"""
Core FUSE operations module.
"""

import errno
import logging
import stat
import time
from itertools import islice

import pyfuse3

from fuse4dbricks.api.errors import (
    UcBadRequest,
    UcConflict,
    UcError,
    UcNotFound,
    UcPermissionDenied,
    UcPreconditionFailed,
    UcRateLimited,
    UcUnavailable,
)
from fuse4dbricks.api.uc_client import UnityCatalogClient
from fuse4dbricks.fs.auth_manager import AuthManager
from fuse4dbricks.fs.data_manager import DataManager
from fuse4dbricks.fs.inode_manager import InodeEntry, InodeEntryAttr, InodeManager
from fuse4dbricks.fs.metadata_manager import MetadataManager
from fuse4dbricks.fs.securable_filter import SecurableFilter
from fuse4dbricks.fs.utils import fs_to_uc_path
from fuse4dbricks.fs.write_buffer import WriteBuffer

logger = logging.getLogger(__name__)

# Open-flag constants (POSIX values)
_O_WRONLY = 1
_O_RDWR = 2
_O_TRUNC = 0o1000  # 512

_R_OK = 4
_W_OK = 2

# Synthetic filesystem capacity reported by statfs(). Unity Catalog volumes
# expose no quota or free-space figure via the Files API, so — like other
# object-store FUSE drivers (s3fs, gcsfuse, rclone) — we advertise a large
# fixed capacity. This makes `df` work and lets tools that pre-check free
# space before writing proceed.
_STATFS_BLOCK_SIZE = 4096
_STATFS_TOTAL_BLOCKS = (1 << 50) // _STATFS_BLOCK_SIZE  # ~1 PiB total
_STATFS_TOTAL_INODES = 1 << 32
_STATFS_NAME_MAX = 255


class UnityCatalogFS(pyfuse3.Operations):
    def __init__(
        self,
        inode_manager: InodeManager,
        metadata_manager: MetadataManager,
        data_manager: DataManager,
        auth_manager: AuthManager,
        uc_client: UnityCatalogClient,
        writes_dir: str,
        read_only: bool = False,
        securable_filter: SecurableFilter | None = None,
    ):
        super(UnityCatalogFS, self).__init__()
        self.inodes = inode_manager
        self.metadata_manager = metadata_manager
        self.data_manager = data_manager
        self.auth_manager = auth_manager
        self.uc_client = uc_client
        self._writes_dir = writes_dir
        self._read_only = read_only
        # Permissive by default (no allow/deny rules) so callers that don't pass
        # one are unaffected.
        self._securables = securable_filter or SecurableFilter()
        self._readdir_state: dict[int, dict] = {}
        self._readdir_fh_count = 0
        self._open_fh_count = 0
        self._open_state: dict[int, dict] = {}

    def _dispatch(self, fs_path: str) -> str:
        if fs_path.startswith("/.auth/") or fs_path in ("/.auth", "/README.txt"):
            return "auth"
        else:
            return "unity_catalog"

    def _child_fs_path(self, parent_fs_path: str, name: str) -> str:
        if parent_fs_path == "/":
            return f"/{name}"
        return f"{parent_fs_path}/{name}"

    async def _check_permissions(self, inode: int, mode: int, ctx: pyfuse3.RequestContext):
        entry = self.inodes.get_entry(inode)
        if entry is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        # Check permissions
        try:
            if self._dispatch(entry.fs_path) == "auth":
                granted = await self.auth_manager.check_access(entry, mode, ctx)
            else:
                granted = await self.metadata_manager.check_access(entry, mode, ctx)
        except pyfuse3.FUSEError:
            # Already-mapped errno: a genuine denial (EACCES) or a transient
            # failure surfaced as EAGAIN by a coalescing follower. Do not
            # collapse a rate-limit/outage into a permanent "permission denied".
            raise
        except Exception as exc:
            # Map domain errors (transient -> EAGAIN, UcPermissionDenied ->
            # EACCES, ...); falls back to EIO rather than a false EACCES.
            self._raise_fuse_error(exc, fs_path=entry.fs_path, op="check_permissions")
            raise pyfuse3.FUSEError(errno.EIO)  # defensive; _raise_fuse_error always raises
        if not granted:
            raise pyfuse3.FUSEError(errno.EACCES)
        return True

    def _raise_fuse_error(self, exc: Exception, *, fs_path: str | None = None, op: str | None = None):
        # Domain -> errno mapping
        if isinstance(exc, UcPermissionDenied):
            raise pyfuse3.FUSEError(errno.EACCES) from exc
        if isinstance(exc, UcNotFound):
            raise pyfuse3.FUSEError(errno.ENOENT) from exc
        if isinstance(exc, UcBadRequest):
            raise pyfuse3.FUSEError(errno.EINVAL) from exc
        if isinstance(exc, UcConflict):
            raise pyfuse3.FUSEError(errno.EEXIST) from exc
        if isinstance(exc, UcPreconditionFailed):
            # The file changed under us mid-read: the cached size/mtime is stale.
            raise pyfuse3.FUSEError(errno.ESTALE) from exc
        if isinstance(exc, (UcRateLimited, UcUnavailable)):
            # transient
            raise pyfuse3.FUSEError(errno.EAGAIN) from exc
        if isinstance(exc, UcError):
            raise pyfuse3.FUSEError(errno.EIO) from exc

        # fall back
        raise pyfuse3.FUSEError(errno.EIO) from exc

    async def access(self, inode: int, mode: int, ctx: pyfuse3.RequestContext) -> bool:
        return await self._check_permissions(inode, mode, ctx)

    async def getattr(
        self, inode: int, ctx: pyfuse3.RequestContext
    ) -> pyfuse3.EntryAttributes:
        entry = self.inodes.get_entry(inode)
        if entry is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        try:
            if self._dispatch(entry.fs_path) == "auth":
                attr = await self.auth_manager.get_attributes(entry, ctx)
            else:
                attr = await self.metadata_manager.get_attributes(entry, ctx)
        except pyfuse3.FUSEError:
            # Already-mapped errno (e.g. EACCES from a permission check): propagate as-is.
            raise
        except Exception as exc:
            # Map domain errors (UcNotFound -> ENOENT, etc.); falls back to EIO.
            self._raise_fuse_error(exc, fs_path=entry.fs_path, op="getattr")
            raise pyfuse3.FUSEError(errno.EIO)

        if attr is None:
            # File was deleted or is now a folder or... inode not valid anyway
            raise pyfuse3.FUSEError(errno.ENOENT)
        # Apply the refreshed attributes to the inode in-place so the reply
        # reflects the current size/mtime instead of whatever was cached at
        # add_entry time.
        entry.attr.update(attr)
        return self._entry_to_fuse_attr(entry)

    async def lookup(
        self, parent_inode: int, name_b: bytes, ctx: pyfuse3.RequestContext
    ) -> pyfuse3.EntryAttributes:
        await self._check_permissions(parent_inode, mode=4, ctx=ctx)  # R_OK
        name = name_b.decode("utf-8")
        parent_entry = self.inodes.get_entry(parent_inode)
        if not parent_entry:
            raise pyfuse3.FUSEError(errno.ENOENT)

        # 1. Check Local Inodes
        if parent_entry.fs_path == "/":
            full_path = f"/{name}"
        else:
            full_path = f"{parent_entry.fs_path}/{name}"

        # Securable allow/deny: a filtered-out path is reported as nonexistent so
        # it is neither listable nor reachable (the auth overlay is exempt).
        if (self._dispatch(full_path) == "unity_catalog"
                and not self._securables.is_path_allowed(full_path)):
            raise pyfuse3.FUSEError(errno.ENOENT)

        existing = self.inodes.get_inode_by_path(full_path)
        if existing:
            # Every lookup reply increments the kernel lookup count; mirror it
            # by pinning the inode BEFORE the getattr await. The get+increment
            # pair is synchronous (atomic under trio's cooperative scheduler),
            # so a forget() racing during the await decrements from 2->1 instead
            # of 1->0 and cannot free a still-referenced inode.
            self.inodes.increment_lookup_count(existing)
            try:
                return await self.getattr(existing, ctx=ctx)
            except BaseException:
                # The reply never reached the kernel, so no matching forget will
                # come; release the pin to avoid leaking the inode.
                self.inodes.forget(existing, 1)
                raise

        # 2. Ask Cache/API
        try:
            if self._dispatch(full_path) == "auth":
                attr = await self.auth_manager.lookup_child(parent_entry, name, ctx)
            else:
                attr = await self.metadata_manager.lookup_child(parent_entry, name, ctx)
        except pyfuse3.FUSEError:
            raise
        except Exception as exc:
            self._raise_fuse_error(exc, fs_path=full_path, op="lookup")

        if attr is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        # 3. Create the inode (or pick up one a concurrent lookup created during
        # the awaits above). Pin before the getattr await, same as the existing
        # path: add_entry + increment are synchronous and therefore atomic.
        entry = self.inodes.add_entry(parent_inode, name, attr)
        self.inodes.increment_lookup_count(entry.inode)
        try:
            return await self.getattr(entry.inode, ctx)
        except BaseException:
            self.inodes.forget(entry.inode, 1)
            raise

    async def opendir(self, inode: int, ctx: pyfuse3.RequestContext):
        logger.debug(f"opendir(inode={inode})")
        R_OK = 4
        await self._check_permissions(inode, mode=R_OK, ctx=ctx)
        entry = self.inodes.get_entry(inode)
        if not entry:
            raise pyfuse3.FUSEError(errno.ENOENT)

        if not entry.is_dir:
            raise pyfuse3.FUSEError(errno.ENOTDIR)

        fh = self._readdir_fh_count
        self._readdir_fh_count += 1
        self._readdir_state[fh] = {
            "inode": inode,
            "ctx": ctx,
        }
        return pyfuse3.FileHandleT(fh)

    async def releasedir(self, fh: pyfuse3.FileHandleT) -> None:
        logger.debug(f"releasedir(fh={fh})")
        if fh in self._readdir_state:
            del self._readdir_state[fh]

    async def readdir(self, fh: pyfuse3.FileHandleT, start_id: int, token) -> None:
        logger.debug(f"readdir(fh={fh}, start_id={start_id})")
        if fh not in self._readdir_state:
            raise pyfuse3.FUSEError(errno.EIO)

        inode = self._readdir_state[fh]["inode"]
        ctx = self._readdir_state[fh]["ctx"]

        entry = self.inodes.get_entry(inode)
        if not entry:
            raise pyfuse3.FUSEError(errno.ENOENT)

        # Yield . and ..
        if start_id <= 0:
            logger.debug(" - .")
            attr = await self.getattr(inode, ctx)
            ret = pyfuse3.readdir_reply(token, b".", attr, 1)  # type:ignore[arg-type]
            if not ret:
                return

        if start_id <= 1:
            logger.debug(" - ..")
            p_attr = await self.getattr(entry.parent_inode, ctx)
            ret = pyfuse3.readdir_reply(token, b"..", p_attr, 2)  # type:ignore[arg-type]
            if not ret:
                return

        # Virtual entries overlaid on the root (the auth files). The auth
        # manager owns their names/attributes; readdir only handles the
        # incremental-offset bookkeeping. They follow . (next_id 1) and
        # .. (next_id 2), so overlay entry i sits at next_id = 3 + i and is
        # emitted only once the cursor (start_id) has reached it.
        if inode == pyfuse3.ROOT_INODE:
            overlay = self.auth_manager.root_overlay()
        else:
            overlay = {}

        for i, (overlay_name, overlay_attr) in enumerate(overlay.items()):
            threshold = 2 + i
            if start_id > threshold:
                continue
            logger.debug(f" - {overlay_name}")
            child_entry = self.inodes.add_entry(
                parent_inode=inode,
                name=overlay_name,
                attr=overlay_attr,
            )
            ret = pyfuse3.readdir_reply(
                token,
                name=child_entry.name.encode("utf-8"),
                attr=self._entry_to_fuse_attr(child_entry),
                next_id=threshold + 1,
            )
            if ret:
                self.inodes.increment_lookup_count(child_entry.inode)
            else:
                return

        meta_attr = 2 + len(overlay)

        # Get Children from Cache/API
        if start_id <= meta_attr:
            if self._dispatch(entry.fs_path) == "auth":
                items = await self.auth_manager.list_directory(entry, ctx)
            else:
                try:
                    items = await self.metadata_manager.list_directory(entry, ctx)
                except pyfuse3.FUSEError as exc:
                    if exc.errno == errno.EACCES and inode == pyfuse3.ROOT_INODE:
                        return
                    raise
                except Exception as exc:
                    self._raise_fuse_error(exc, fs_path=entry.fs_path, op="readdir")
                    raise
            if items is None:
                raise pyfuse3.FUSEError(errno.EIO)
            # Hide securables excluded by the allow/deny rules (auth overlay is
            # served separately above and is never filtered).
            if self._securables.is_active and self._dispatch(entry.fs_path) == "unity_catalog":
                items = {
                    name: attr
                    for name, attr in items.items()
                    if self._securables.is_path_allowed(
                        self._child_fs_path(entry.fs_path, name)
                    )
                }
            self._readdir_state[fh]["children"] = items
        else:
            items = self._readdir_state[fh]["children"]
        logger.debug(f"items: {list(items.keys())}")

        to_skip = max(0, start_id - meta_attr)
        for i, (name, attr) in enumerate(islice(items.items(), to_skip, None)):
            child_entry = self.inodes.add_entry(
                parent_inode=inode,
                name=name,
                attr=attr,
            )
            ret = pyfuse3.readdir_reply(
                token,
                name=child_entry.name.encode("utf-8"),
                attr=self._entry_to_fuse_attr(child_entry),
                next_id=meta_attr + 1 + to_skip + i,
            )
            if ret:
                self.inodes.increment_lookup_count(child_entry.inode)
            else:
                return

    async def open(self, inode, flags, ctx):
        if flags & _O_RDWR:
            required_mode = _R_OK | _W_OK
        elif flags & _O_WRONLY:
            required_mode = _W_OK
        else:
            required_mode = _R_OK
        if not await self._check_permissions(inode, required_mode, ctx):
            raise pyfuse3.FUSEError(errno.EACCES)
        entry = self.inodes.get_entry(inode)
        if entry is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        if entry.is_dir:
            raise pyfuse3.FUSEError(errno.EISDIR)
        if self._read_only and (flags & (_O_WRONLY | _O_RDWR)) and self._dispatch(entry.fs_path) == "unity_catalog":
            raise pyfuse3.FUSEError(errno.EROFS)

        writable = bool(flags & (_O_WRONLY | _O_RDWR))
        write_buffer: WriteBuffer | None = None

        if writable and self._dispatch(entry.fs_path) == "unity_catalog":
            write_buffer = await WriteBuffer.create(self._writes_dir)
            # Without O_TRUNC, POSIX preserves any bytes the caller does not
            # overwrite, so the buffer must start as a copy of the current
            # remote file. Otherwise a partial write (e.g. rewriting only the
            # first few bytes of a large file) would upload a truncated file on
            # release and silently destroy the rest. This applies to BOTH
            # O_WRONLY and O_RDWR; only O_TRUNC (or a brand-new file via
            # create()) legitimately starts from an empty buffer. Stream the
            # existing content in one chunk at a time so memory stays at
            # O(chunk_size) regardless of file size.
            if not (flags & _O_TRUNC) and entry.attr.st_size > 0:
                logger.info(
                    "writable open on %s (size=%d) without O_TRUNC: pre-loading existing content",
                    entry.fs_path, entry.attr.st_size,
                )
                try:
                    file_size = entry.attr.st_size
                    pos = 0
                    chunk_size = self.data_manager.chunk_size
                    while pos < file_size:
                        length = min(chunk_size, file_size - pos)
                        chunk = await self.data_manager.read(
                            entry.fs_path, pos, length,
                            entry.attr.st_mtime, file_size, ctx=ctx,
                        )
                        if len(chunk) == 0:
                            break
                        await write_buffer.write(pos, chunk)
                        pos += len(chunk)
                except Exception as e:
                    await write_buffer.close()
                    self._raise_fuse_error(e, fs_path=entry.fs_path, op="open/preload")

        fh = self._open_fh_count
        self._open_fh_count += 1
        self._open_state[fh] = {
            "inode": inode,
            "ctx": ctx,
            "write_buffer": write_buffer,
            "writable": writable,
            "dirty": False,
        }
        return pyfuse3.FileInfo(fh=fh)

    async def create(self, parent_inode, name, mode, flags, ctx):
        if self._read_only:
            raise pyfuse3.FUSEError(errno.EROFS)
        await self._check_permissions(parent_inode, _W_OK, ctx)

        parent_entry = self.inodes.get_entry(parent_inode)
        if parent_entry is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        if self._dispatch(parent_entry.fs_path) != "unity_catalog":
            raise pyfuse3.FUSEError(errno.EACCES)

        name_str = name.decode("utf-8")
        if not self._securables.is_path_allowed(
            self._child_fs_path(parent_entry.fs_path, name_str)
        ):
            raise pyfuse3.FUSEError(errno.EACCES)
        now = time.time()
        attr = InodeEntryAttr(
            st_mode=(stat.S_IFREG | 0o644),
            st_nlink=1,
            st_size=0,
            st_ctime=now,
            st_mtime=now,
            st_atime=now,
            st_uid=ctx.uid,
            st_gid=ctx.gid,
        )
        entry = self.inodes.add_entry(parent_inode, name_str, attr)
        # create() returns an EntryAttributes to the kernel just like lookup(),
        # so the kernel holds a reference and will send a matching forget().
        # Mirror that with a lookup-count increment, otherwise the new inode
        # sits at ref_count 0 while still referenced and can be freed early.
        self.inodes.increment_lookup_count(entry.inode)

        fh = self._open_fh_count
        self._open_fh_count += 1
        self._open_state[fh] = {
            "inode": entry.inode,
            "ctx": ctx,
            "write_buffer": await WriteBuffer.create(self._writes_dir),
            "writable": True,
            # Mark dirty immediately: create() semantics guarantee the file
            # exists after release(), even if nothing is written into it.
            "dirty": True,
        }
        return (pyfuse3.FileInfo(fh=fh), self._entry_to_fuse_attr(entry))

    async def _upload_if_dirty(self, state: dict) -> None:
        """Upload the write buffer to Unity Catalog if it has unsaved writes.

        Shared by ``flush`` and ``release``. Uploads, then marks the handle
        clean and busts the caches so the new content is immediately visible.
        Raises ``FUSEError(EIO)`` on upload failure (the caller decides whether
        that error reaches userspace). A no-op when the buffer is clean, so a
        second ``flush`` — or the ``release`` fallback after a successful
        ``flush`` — does not re-upload.
        """
        write_buffer: WriteBuffer | None = state.get("write_buffer")
        if write_buffer is None or not state.get("dirty", False):
            return

        entry = self.inodes.get_entry(state["inode"])
        if entry is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        uc_path = fs_to_uc_path(entry.fs_path)
        try:
            # Push buffered writes to the OS so the upload's separate reader sees
            # the complete file (writes smaller than ~8 KB would otherwise read
            # back as zero bytes). Keep the handle open: flush may be delivered
            # again with more writes before release.
            await write_buffer.flush_to_disk()
            await self.uc_client.upload_file(uc_path, write_buffer.path, ctx=state["ctx"])
        except Exception as e:
            logger.error("Upload failed for %s: %s", entry.fs_path, e)
            # Remote file is unchanged: do NOT invalidate the cache.
            raise pyfuse3.FUSEError(errno.EIO) from e
        # Clean before invalidating so a concurrent reader never sees "dirty".
        state["dirty"] = False
        # Bust caches so subsequent reads see the new file: metadata (size/mtime)
        # and the chunk content cache (whose mtime key cannot tell apart two
        # writes in the same second).
        self.metadata_manager.invalidate(entry.fs_path, is_dir=False)
        await self.data_manager.invalidate_path(entry.fs_path)

    async def flush(self, fh: pyfuse3.FileHandleT) -> None:
        # flush handles the close() syscall: the kernel waits for it and returns
        # its error to userspace. Uploading here (rather than only in release(),
        # which runs asynchronously and whose errors the kernel discards) makes a
        # write durable and immediately visible by the time close() returns, and
        # lets a failed upload surface as a close() error instead of being lost.
        state = self._open_state.get(fh)
        if state is None:
            return
        await self._upload_if_dirty(state)

    async def release(self, fh: pyfuse3.FileHandleT) -> None:
        if fh not in self._open_state:
            # This should not happen, but we want to be resilient to it. Just ignore.
            return

        state = self._open_state[fh]
        inode = state["inode"]
        ctx = state["ctx"]
        write_buffer: WriteBuffer | None = state.get("write_buffer")

        entry = self.inodes.get_entry(inode)
        if entry is None:
            if write_buffer is not None:
                await write_buffer.close()
            del self._open_state[fh]
            raise pyfuse3.FUSEError(errno.ENOENT)

        try:
            if write_buffer is not None:
                # Normally flush() already uploaded and cleared the dirty flag,
                # making this a no-op; it stays as a fallback for the rare path
                # where release arrives without a preceding flush.
                await self._upload_if_dirty(state)
            elif self._dispatch(entry.fs_path) == "auth":
                try:
                    await self.auth_manager.release(entry.fs_path, ctx=ctx)
                except Exception as e:
                    logger.error(f"release/close error on {entry.fs_path}: {e}")
                    raise pyfuse3.FUSEError(errno.EIO)
        finally:
            if write_buffer is not None:
                await write_buffer.close()
            if fh in self._open_state:
                del self._open_state[fh]

    async def read(self, fh, offset, length):
        if fh not in self._open_state:
            raise pyfuse3.FUSEError(errno.EIO)

        state = self._open_state[fh]
        inode = state["inode"]
        ctx = state["ctx"]

        entry = self.inodes.get_entry(inode)
        if entry is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        if entry.is_dir:
            raise pyfuse3.FUSEError(errno.EISDIR)

        # O_RDWR handles: serve reads from the local write buffer, which holds
        # the full file content downloaded at open() plus any in-progress writes.
        write_buffer: WriteBuffer | None = state.get("write_buffer")
        if state.get("writable") and write_buffer is not None:
            return await write_buffer.read(offset, length)

        try:
            if self._dispatch(entry.fs_path) == "auth":
                return await self.auth_manager.read(
                    entry.fs_path,
                    offset,
                    length,
                    entry.attr.st_mtime,
                    entry.attr.st_size,
                    ctx=ctx,
                )
            else:
                return await self.data_manager.read(
                    entry.fs_path,
                    offset,
                    length,
                    entry.attr.st_mtime,
                    entry.attr.st_size,
                    ctx=ctx,
                )
        except pyfuse3.FUSEError:
            raise
        except Exception as e:
            logger.exception("read failed op=read path=%s uid=%s. %s", entry.fs_path, getattr(ctx, "uid", None), e)
            self._raise_fuse_error(e, fs_path=entry.fs_path, op="read")
            raise pyfuse3.FUSEError(errno.EIO)

    async def write(self, fh, offset, buffer) -> int:
        if fh not in self._open_state:
            raise pyfuse3.FUSEError(errno.EIO)

        state = self._open_state[fh]
        inode = state["inode"]
        ctx = state["ctx"]

        entry = self.inodes.get_entry(inode)
        if entry is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        if entry.is_dir:
            raise pyfuse3.FUSEError(errno.EISDIR)

        try:
            # Auth files are handled by auth_manager (no write_buffer involved).
            if self._dispatch(entry.fs_path) == "auth":
                return await self.auth_manager.write(
                    entry.fs_path,
                    offset,
                    buffer,
                    ctx=ctx,
                )

            write_buffer: WriteBuffer | None = state.get("write_buffer")
            if write_buffer is None:
                # File was opened read-only.
                raise pyfuse3.FUSEError(errno.EACCES)

            n = await write_buffer.write(offset, buffer)
            state["dirty"] = True
            # Keep st_size current so getattr() reflects the in-progress size.
            entry.attr.st_size = write_buffer.size()
            return n
        except pyfuse3.FUSEError:
            raise
        except Exception as e:
            logger.error("write error on %s: %s", entry.fs_path, e)
            raise pyfuse3.FUSEError(errno.EIO)

    async def mkdir(self, parent_inode, name, mode, ctx):
        if self._read_only:
            raise pyfuse3.FUSEError(errno.EROFS)
        await self._check_permissions(parent_inode, _W_OK, ctx)

        parent_entry = self.inodes.get_entry(parent_inode)
        if parent_entry is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        if self._dispatch(parent_entry.fs_path) != "unity_catalog":
            raise pyfuse3.FUSEError(errno.EACCES)

        name_str = name.decode("utf-8")
        child_fs_path = f"{parent_entry.fs_path}/{name_str}".replace("//", "/")
        if not self._securables.is_path_allowed(child_fs_path):
            raise pyfuse3.FUSEError(errno.EACCES)
        uc_path = fs_to_uc_path(child_fs_path)

        try:
            await self.uc_client.create_directory(uc_path, ctx=ctx)
        except pyfuse3.FUSEError:
            raise
        except Exception as e:
            self._raise_fuse_error(e, fs_path=child_fs_path, op="mkdir")

        now = time.time()
        attr = InodeEntryAttr(
            st_mode=(stat.S_IFDIR | 0o755),
            st_nlink=2,
            st_size=4096,
            st_ctime=now,
            st_mtime=now,
            st_atime=now,
            st_uid=ctx.uid,
            st_gid=ctx.gid,
        )
        entry = self.inodes.add_entry(parent_inode, name_str, attr)
        # Invalidate child path: deletes child's stale attr entry and the parent's
        # dir listing cache so the new directory is visible on the next readdir.
        self.metadata_manager.invalidate(entry.fs_path, is_dir=True)
        return self._entry_to_fuse_attr(entry)

    async def unlink(self, parent_inode, name, ctx):
        if self._read_only:
            raise pyfuse3.FUSEError(errno.EROFS)
        await self._check_permissions(parent_inode, _W_OK, ctx)

        parent_entry = self.inodes.get_entry(parent_inode)
        if parent_entry is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        name_str = name.decode("utf-8")
        child_fs_path = f"{parent_entry.fs_path}/{name_str}".replace("//", "/")
        uc_path = fs_to_uc_path(child_fs_path)

        try:
            await self.uc_client.delete_file(uc_path, ctx=ctx)
        except pyfuse3.FUSEError:
            raise
        except Exception as e:
            self._raise_fuse_error(e, fs_path=child_fs_path, op="unlink")

        self.inodes.detach_path(child_fs_path)
        self.metadata_manager.invalidate(child_fs_path, is_dir=False)
        # Drop any cached content so a later same-named file is not served stale.
        await self.data_manager.invalidate_path(child_fs_path)

    async def rmdir(self, parent_inode, name, ctx):
        if self._read_only:
            raise pyfuse3.FUSEError(errno.EROFS)
        await self._check_permissions(parent_inode, _W_OK, ctx)

        parent_entry = self.inodes.get_entry(parent_inode)
        if parent_entry is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        name_str = name.decode("utf-8")
        child_fs_path = f"{parent_entry.fs_path}/{name_str}".replace("//", "/")
        uc_path = fs_to_uc_path(child_fs_path)

        try:
            await self.uc_client.delete_directory(uc_path, ctx=ctx)
        except UcBadRequest as e:
            # The Files API returns 400 when the directory is non-empty.
            raise pyfuse3.FUSEError(errno.ENOTEMPTY) from e
        except pyfuse3.FUSEError:
            raise
        except Exception as e:
            self._raise_fuse_error(e, fs_path=child_fs_path, op="rmdir")

        self.inodes.detach_path(child_fs_path)
        self.metadata_manager.invalidate(child_fs_path, is_dir=True)

    async def rename(self, parent_inode_old, name_old, parent_inode_new, name_new, flags, ctx):
        if self._read_only:
            raise pyfuse3.FUSEError(errno.EROFS)
        # We don't implement the atomic-exchange extension.
        if flags & pyfuse3.RENAME_EXCHANGE:
            raise pyfuse3.FUSEError(errno.EINVAL)

        await self._check_permissions(parent_inode_old, _W_OK, ctx)
        await self._check_permissions(parent_inode_new, _W_OK, ctx)

        old_parent = self.inodes.get_entry(parent_inode_old)
        new_parent = self.inodes.get_entry(parent_inode_new)
        if old_parent is None or new_parent is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        # The auth overlay is virtual and read-only; renames stay in UC space.
        if (self._dispatch(old_parent.fs_path) != "unity_catalog"
                or self._dispatch(new_parent.fs_path) != "unity_catalog"):
            raise pyfuse3.FUSEError(errno.EACCES)

        old_name = name_old.decode("utf-8")
        new_name = name_new.decode("utf-8")
        old_fs_path = f"{old_parent.fs_path}/{old_name}".replace("//", "/")
        new_fs_path = f"{new_parent.fs_path}/{new_name}".replace("//", "/")
        # Securable rules: a filtered-out source is invisible (ENOENT); renaming
        # into a forbidden destination is refused (EACCES).
        if not self._securables.is_path_allowed(old_fs_path):
            raise pyfuse3.FUSEError(errno.ENOENT)
        if not self._securables.is_path_allowed(new_fs_path):
            raise pyfuse3.FUSEError(errno.EACCES)
        if old_fs_path == new_fs_path:
            return  # no-op

        # Resolve the source so we know its type and size. Prefer the inode map;
        # fall back to a metadata lookup if it isn't cached yet.
        src_inode = self.inodes.get_inode_by_path(old_fs_path)
        src_entry = self.inodes.get_entry(src_inode) if src_inode is not None else None
        if src_entry is None:
            try:
                attr = await self.metadata_manager.lookup_child(old_parent, old_name, ctx)
            except pyfuse3.FUSEError:
                raise
            except Exception as exc:
                self._raise_fuse_error(exc, fs_path=old_fs_path, op="rename")
            if attr is None:
                raise pyfuse3.FUSEError(errno.ENOENT)
            src_entry = self.inodes.add_entry(parent_inode_old, old_name, attr)
            src_inode = src_entry.inode

        # The Files API has no directory move; a recursive copy would be heavy
        # and non-atomic. Report cross-device so `mv` falls back to copy+remove.
        if src_entry.is_dir:
            raise pyfuse3.FUSEError(errno.EXDEV)

        # Refresh the source's metadata from the server before copying. A file
        # just created/written through this mount carries a locally-stamped
        # st_mtime (the create()-time wall clock), which is earlier than the
        # server's Last-Modified (assigned when the upload completed). The copy
        # below sends that mtime as If-Unmodified-Since; a too-early value makes
        # the server report 412 (file changed since) and the read fails with
        # EIO, so `mv` of a freshly-written file breaks. getattr() refreshes the
        # inode on the ordinary read path but rename() never does, so do it here.
        try:
            refreshed = await self.metadata_manager.get_attributes(src_entry, ctx)
        except pyfuse3.FUSEError:
            raise
        except Exception as exc:
            self._raise_fuse_error(exc, fs_path=old_fs_path, op="rename")
        if refreshed is None:
            raise pyfuse3.FUSEError(errno.ENOENT)
        src_entry.attr.update(refreshed)

        # Inspect the destination for NOREPLACE / file-over-dir rules.
        try:
            dst_attr = await self.metadata_manager.lookup_child(new_parent, new_name, ctx)
        except pyfuse3.FUSEError:
            raise
        except Exception as exc:
            self._raise_fuse_error(exc, fs_path=new_fs_path, op="rename")
        if dst_attr is not None:
            if flags & pyfuse3.RENAME_NOREPLACE:
                raise pyfuse3.FUSEError(errno.EEXIST)
            if dst_attr.is_dir:
                raise pyfuse3.FUSEError(errno.EISDIR)

        # No server-side move: download source -> upload to dest -> delete source.
        src_uc = fs_to_uc_path(old_fs_path)
        dst_uc = fs_to_uc_path(new_fs_path)
        write_buffer = await WriteBuffer.create(self._writes_dir)
        try:
            file_size = src_entry.attr.st_size
            pos = 0
            chunk_size = self.data_manager.chunk_size
            while pos < file_size:
                length = min(chunk_size, file_size - pos)
                chunk = await self.data_manager.read(
                    old_fs_path, pos, length,
                    src_entry.attr.st_mtime, file_size, ctx=ctx,
                )
                if len(chunk) == 0:
                    break
                await write_buffer.write(pos, chunk)
                pos += len(chunk)
            await write_buffer.finalize()
            await self.uc_client.upload_file(dst_uc, write_buffer.path, ctx=ctx)
        except pyfuse3.FUSEError:
            await write_buffer.close()
            raise
        except Exception as exc:
            await write_buffer.close()
            self._raise_fuse_error(exc, fs_path=new_fs_path, op="rename/upload")
        else:
            await write_buffer.close()

        # Drop the source only after the copy is durably uploaded.
        try:
            await self.uc_client.delete_file(src_uc, ctx=ctx)
        except pyfuse3.FUSEError:
            raise
        except Exception as exc:
            self._raise_fuse_error(exc, fs_path=old_fs_path, op="rename/delete")

        # Re-point the inode and bust caches for both ends (and their parents).
        self.inodes.move_inode(src_inode, parent_inode_new, new_name)
        self.metadata_manager.invalidate(old_fs_path, is_dir=False)
        self.metadata_manager.invalidate(new_fs_path, is_dir=False)
        # The source is gone and the destination has new content; drop both from
        # the chunk content cache.
        await self.data_manager.invalidate_path(old_fs_path)
        await self.data_manager.invalidate_path(new_fs_path)

    async def setattr(self, inode, attr, fields, fh, ctx):
        entry = self.inodes.get_entry(inode)
        if entry is None:
            raise pyfuse3.FUSEError(errno.ENOENT)

        is_uc = self._dispatch(entry.fs_path) == "unity_catalog"

        # Size change (truncate / ftruncate) is the only field backed by remote
        # storage. The rest (mode/uid/gid/times) has no Unity Catalog
        # representation, so we accept them in-memory only to keep tools that do
        # chmod/chown/utime (tar, cp -p, editors) from failing.
        if fields.update_size:
            if entry.is_dir:
                raise pyfuse3.FUSEError(errno.EISDIR)
            if not is_uc:
                raise pyfuse3.FUSEError(errno.EACCES)
            if self._read_only:
                raise pyfuse3.FUSEError(errno.EROFS)
            await self._truncate_uc_file(entry, attr.st_size, fh, ctx)

        if fields.update_mode:
            # Preserve the file-type bits; only the permission bits change.
            entry.attr.st_mode = stat.S_IFMT(entry.attr.st_mode) | stat.S_IMODE(
                attr.st_mode
            )
        if fields.update_uid:
            entry.attr.st_uid = attr.st_uid
        if fields.update_gid:
            entry.attr.st_gid = attr.st_gid
        if fields.update_atime:
            entry.attr.st_atime = attr.st_atime_ns / 1e9
        if fields.update_mtime:
            entry.attr.st_mtime = attr.st_mtime_ns / 1e9
        # Metadata changed: bump ctime like a real filesystem.
        entry.attr.st_ctime = time.time()

        return self._entry_to_fuse_attr(entry)

    def _find_open_writer(self, inode: int, fh) -> dict | None:
        """Resolve the open write state a truncate should act on.

        Prefer the handle named by an ftruncate call. Otherwise fall back to any
        open writable handle on the same inode: WSL's 9P layer issues a file's
        final size as a *path-based* setattr (fh is None or not the writer), and
        the open writer's buffer -- not the remote object -- holds the
        authoritative bytes. Reading the remote there would upload a zero-filled
        file of the right size.
        """
        if fh is not None:
            state = self._open_state.get(int(fh))
            if state and state.get("writable") and state.get("write_buffer") is not None:
                return state
        for state in self._open_state.values():
            if (state.get("inode") == inode
                    and state.get("writable")
                    and state.get("write_buffer") is not None):
                return state
        return None

    async def _truncate_uc_file(self, entry, new_size, fh, ctx):
        """Resize a Unity Catalog file to ``new_size`` bytes.

        If a writable handle is open for this inode (ftruncate, or the
        path-based setattr WSL emits while a writer is open), operate on its
        buffer and defer the upload to release(). Otherwise (path-based
        truncate of a closed file) materialize the kept bytes into a temp
        buffer, resize it and upload immediately.
        """
        # Resize the open writer's buffer; release() will upload the result.
        writer = self._find_open_writer(entry.inode, fh)
        if writer is not None:
            write_buffer: WriteBuffer = writer["write_buffer"]
            await write_buffer.truncate(new_size)
            writer["dirty"] = True
            entry.attr.st_size = write_buffer.size()
            return

        # Path-based truncate of a closed file: copy the bytes we keep, resize,
        # upload now.
        write_buffer = await WriteBuffer.create(self._writes_dir)
        try:
            old_size = entry.attr.st_size
            to_copy = min(new_size, old_size)
            pos = 0
            chunk_size = self.data_manager.chunk_size
            while pos < to_copy:
                length = min(chunk_size, to_copy - pos)
                chunk = await self.data_manager.read(
                    entry.fs_path, pos, length,
                    entry.attr.st_mtime, old_size, ctx=ctx,
                )
                if len(chunk) == 0:
                    # Short read before reaching the bytes we must preserve: the
                    # remote file isn't what we expected. Fail rather than
                    # silently zero-fill the tail.
                    raise pyfuse3.FUSEError(errno.EIO)
                await write_buffer.write(pos, chunk)
                pos += len(chunk)
            await write_buffer.truncate(new_size)  # zero-extends when growing
            await write_buffer.finalize()
            uc_path = fs_to_uc_path(entry.fs_path)
            await self.uc_client.upload_file(uc_path, write_buffer.path, ctx=ctx)
        except pyfuse3.FUSEError:
            raise
        except Exception as exc:
            self._raise_fuse_error(exc, fs_path=entry.fs_path, op="setattr/truncate")
        finally:
            await write_buffer.close()
        entry.attr.st_size = new_size
        self.metadata_manager.invalidate(entry.fs_path, is_dir=False)
        await self.data_manager.invalidate_path(entry.fs_path)

    async def forget(self, inode_list):
        for inode, nlookup in inode_list:
            self.inodes.forget(inode, nlookup)

    async def statfs(self, ctx: pyfuse3.RequestContext) -> pyfuse3.StatvfsData:
        # Unity Catalog has no capacity API, so report a large synthetic volume
        # (see _STATFS_* constants) so `df` works and free-space pre-checks
        # pass. On a read-only mount, advertise zero availability so writes are
        # clearly unavailable while the total size still shows.
        stat = pyfuse3.StatvfsData()
        stat.f_bsize = _STATFS_BLOCK_SIZE
        stat.f_frsize = _STATFS_BLOCK_SIZE
        stat.f_blocks = _STATFS_TOTAL_BLOCKS
        stat.f_files = _STATFS_TOTAL_INODES
        free_blocks = 0 if self._read_only else _STATFS_TOTAL_BLOCKS
        free_inodes = 0 if self._read_only else _STATFS_TOTAL_INODES
        stat.f_bfree = free_blocks
        stat.f_bavail = free_blocks
        stat.f_ffree = free_inodes
        stat.f_favail = free_inodes
        stat.f_namemax = _STATFS_NAME_MAX
        return stat

    def _entry_to_fuse_attr(self, entry: InodeEntry) -> pyfuse3.EntryAttributes:
        attr = pyfuse3.EntryAttributes()
        attr.st_mode = entry.attr.st_mode
        attr.st_nlink = entry.attr.st_nlink
        attr.st_uid = entry.attr.st_uid
        attr.st_gid = entry.attr.st_gid
        attr.st_size = entry.attr.st_size
        attr.st_atime_ns = int(entry.attr.st_atime * 1e9)
        attr.st_ctime_ns = int(entry.attr.st_ctime * 1e9)
        attr.st_mtime_ns = int(entry.attr.st_mtime * 1e9)
        attr.st_ino = entry.inode
        attr.st_blksize = 4096
        attr.st_blocks = (attr.st_size + 511) // 512
        return attr
