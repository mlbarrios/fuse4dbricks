"""
Tests for fuse4dbricks.fs.operations.UnityCatalogFS.

Covers:
  - _dispatch              (path → "auth" | "unity_catalog")
  - _raise_fuse_error      (UcError subtype → correct errno)
  - _entry_to_fuse_attr    (InodeEntry → pyfuse3.EntryAttributes)
  - getattr                (ENOENT when inode missing; delegates to managers)
  - open                   (permission flags; EISDIR for directories)
  - release                (state cleanup; delegates auth release)
  - read                   (routes to auth or data manager)
  - write                  (auth files writable; read-only catalog → EACCES)
  - readdir                (root overlay merge + offset bookkeeping)
  - forget                 (delegates to inode_manager)
"""

import errno
import stat
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
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
from fuse4dbricks.fs.inode_manager import InodeEntry, InodeEntryAttr, InodeManager
from fuse4dbricks.fs.operations import UnityCatalogFS
from fuse4dbricks.fs.securable_filter import SecurableFilter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_attr(is_dir: bool, size: int = 512) -> InodeEntryAttr:
    mode = (stat.S_IFDIR | 0o755) if is_dir else (stat.S_IFREG | 0o644)
    return InodeEntryAttr(
        st_mode=mode,
        st_nlink=2 if is_dir else 1,
        st_size=size,
        st_ctime=1000.0,
        st_mtime=2000.0,
        st_atime=3000.0,
        st_uid=1000,
        st_gid=1000,
    )


def _make_entry(inode: int, parent: int, name: str, fs_path: str, is_dir: bool, size: int = 512) -> InodeEntry:
    return InodeEntry(
        inode=inode,
        parent_inode=parent,
        name=name,
        fs_path=fs_path,
        attr=_make_attr(is_dir, size=size),
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def ctx():
    return SimpleNamespace(uid=1000, pid=5678, gid=1000)


@pytest.fixture
def inode_manager():
    return InodeManager()


@pytest.fixture
def metadata_manager():
    mgr = MagicMock()
    mgr.check_access = AsyncMock(return_value=True)
    mgr.get_attributes = AsyncMock(return_value=_make_attr(False))
    mgr.lookup_child = AsyncMock(return_value=_make_attr(False))
    mgr.list_directory = AsyncMock(return_value={})
    return mgr


@pytest.fixture
def data_manager():
    mgr = MagicMock()
    mgr.read = AsyncMock(return_value=b"hello")
    mgr.invalidate_path = AsyncMock()
    mgr.chunk_size = 8 * 1024 * 1024  # 8 MB, same as the real DataManager
    return mgr


@pytest.fixture
def auth_manager():
    mgr = MagicMock()
    mgr.check_access = AsyncMock(return_value=True)
    mgr.get_attributes = AsyncMock(return_value=_make_attr(False))
    mgr.lookup_child = AsyncMock(return_value=_make_attr(False))
    mgr.list_directory = AsyncMock(return_value={})
    mgr.read = AsyncMock(return_value=b"README")
    mgr.write = AsyncMock(return_value=5)
    mgr.release = AsyncMock()
    return mgr


@pytest.fixture
def uc_client():
    mgr = MagicMock()
    mgr.upload_file = AsyncMock()
    mgr.delete_file = AsyncMock()
    mgr.delete_directory = AsyncMock()
    mgr.create_directory = AsyncMock()
    return mgr


@pytest.fixture
def writes_dir(tmp_path):
    d = tmp_path / "writes"
    d.mkdir()
    return str(d)


@pytest.fixture
def fs(inode_manager, metadata_manager, data_manager, auth_manager, uc_client, writes_dir):
    return UnityCatalogFS(
        inode_manager=inode_manager,
        metadata_manager=metadata_manager,
        data_manager=data_manager,
        auth_manager=auth_manager,
        uc_client=uc_client,
        writes_dir=writes_dir,
    )


@pytest.fixture
def fs_ro(inode_manager, metadata_manager, data_manager, auth_manager, uc_client, writes_dir):
    return UnityCatalogFS(
        inode_manager=inode_manager,
        metadata_manager=metadata_manager,
        data_manager=data_manager,
        auth_manager=auth_manager,
        uc_client=uc_client,
        writes_dir=writes_dir,
        read_only=True,
    )


@pytest.fixture
def fs_builder(inode_manager, metadata_manager, data_manager, auth_manager, uc_client, writes_dir):
    """Build a UnityCatalogFS with a custom securable_filter, sharing the
    standard manager fixtures (so inode tree / mocks line up with other tests)."""
    def _build(securable_filter=None, read_only=False):
        return UnityCatalogFS(
            inode_manager=inode_manager,
            metadata_manager=metadata_manager,
            data_manager=data_manager,
            auth_manager=auth_manager,
            uc_client=uc_client,
            writes_dir=writes_dir,
            read_only=read_only,
            securable_filter=securable_filter,
        )
    return _build


# ---------------------------------------------------------------------------
# _dispatch
# ---------------------------------------------------------------------------


def test_dispatch_auth_dir(fs):
    assert fs._dispatch("/.auth") == "auth"


def test_dispatch_auth_subpath(fs):
    assert fs._dispatch("/.auth/personal_access_token") == "auth"


def test_dispatch_readme(fs):
    assert fs._dispatch("/README.txt") == "auth"


def test_dispatch_catalog(fs):
    assert fs._dispatch("/my_catalog") == "unity_catalog"


def test_dispatch_deep_path(fs):
    assert fs._dispatch("/cat/sch/vol/folder/file.txt") == "unity_catalog"


def test_dispatch_root(fs):
    assert fs._dispatch("/") == "unity_catalog"


# ---------------------------------------------------------------------------
# _raise_fuse_error
# ---------------------------------------------------------------------------


def test_raise_fuse_error_permission_denied(fs):
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        fs._raise_fuse_error(UcPermissionDenied("denied"))
    assert exc_info.value.errno == errno.EACCES


def test_raise_fuse_error_not_found(fs):
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        fs._raise_fuse_error(UcNotFound("not found"))
    assert exc_info.value.errno == errno.ENOENT


def test_raise_fuse_error_bad_request(fs):
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        fs._raise_fuse_error(UcBadRequest("bad"))
    assert exc_info.value.errno == errno.EINVAL


def test_raise_fuse_error_conflict(fs):
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        fs._raise_fuse_error(UcConflict("conflict"))
    assert exc_info.value.errno == errno.EEXIST


def test_raise_fuse_error_precondition_failed(fs):
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        fs._raise_fuse_error(UcPreconditionFailed("changed under us", status_code=412))
    assert exc_info.value.errno == errno.ESTALE


def test_raise_fuse_error_rate_limited(fs):
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        fs._raise_fuse_error(UcRateLimited("rate limited"))
    assert exc_info.value.errno == errno.EAGAIN


def test_raise_fuse_error_unavailable(fs):
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        fs._raise_fuse_error(UcUnavailable("unavailable"))
    assert exc_info.value.errno == errno.EAGAIN


def test_raise_fuse_error_generic_uc_error(fs):
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        fs._raise_fuse_error(UcError("generic", status_code=500))
    assert exc_info.value.errno == errno.EIO


def test_raise_fuse_error_unknown_exception(fs):
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        fs._raise_fuse_error(RuntimeError("unexpected"))
    assert exc_info.value.errno == errno.EIO


# ---------------------------------------------------------------------------
# _entry_to_fuse_attr
# ---------------------------------------------------------------------------


def test_entry_to_fuse_attr_file(fs):
    entry = _make_entry(10, pyfuse3.ROOT_INODE, "file.txt", "/cat/sch/vol/file.txt", is_dir=False, size=1024)
    attr = fs._entry_to_fuse_attr(entry)
    assert attr.st_ino == 10
    assert attr.st_size == 1024
    assert attr.st_atime_ns == int(3000.0 * 1e9)
    assert attr.st_mtime_ns == int(2000.0 * 1e9)
    assert attr.st_ctime_ns == int(1000.0 * 1e9)
    assert attr.st_uid == 1000
    assert attr.st_gid == 1000
    assert attr.st_blksize == 4096
    assert attr.st_blocks == (1024 + 511) // 512


def test_entry_to_fuse_attr_directory(fs):
    entry = _make_entry(11, pyfuse3.ROOT_INODE, "subdir", "/cat/sch/vol/subdir", is_dir=True)
    attr = fs._entry_to_fuse_attr(entry)
    assert bool(attr.st_mode & stat.S_IFDIR)


# ---------------------------------------------------------------------------
# getattr
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_getattr_missing_inode_raises_enoent(fs, ctx):
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        await fs.getattr(9999, ctx)
    assert exc_info.value.errno == errno.ENOENT


@pytest.mark.trio
async def test_getattr_unity_catalog_file(fs, inode_manager, metadata_manager, ctx):
    # Create an inode under a realistic UC path
    cat = inode_manager.add_entry(pyfuse3.ROOT_INODE, "cat", attr=_make_attr(True))
    sch = inode_manager.add_entry(cat.inode, "sch", attr=_make_attr(True))
    vol = inode_manager.add_entry(sch.inode, "vol", attr=_make_attr(True))
    f = inode_manager.add_entry(vol.inode, "data.txt", attr=_make_attr(False, size=42))

    metadata_manager.get_attributes.return_value = _make_attr(False, size=42)

    attr = await fs.getattr(f.inode, ctx)
    assert attr.st_size == 42


@pytest.mark.trio
async def test_getattr_auth_file_delegates_to_auth_manager(fs, inode_manager, auth_manager, ctx):
    readme = inode_manager.add_entry(pyfuse3.ROOT_INODE, "README.txt", attr=_make_attr(False))
    auth_manager.get_attributes.return_value = _make_attr(False, size=200)

    attr = await fs.getattr(readme.inode, ctx)
    auth_manager.get_attributes.assert_awaited_once()
    assert attr is not None


@pytest.mark.trio
async def test_getattr_raises_enoent_when_manager_returns_none(fs, inode_manager, metadata_manager, ctx):
    cat = inode_manager.add_entry(pyfuse3.ROOT_INODE, "cat", attr=_make_attr(True))
    metadata_manager.get_attributes.return_value = None

    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        await fs.getattr(cat.inode, ctx)
    assert exc_info.value.errno in (errno.ENOENT, errno.EIO)


# ---------------------------------------------------------------------------
# open / release
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_open_read_only_creates_file_handle(fs, inode_manager, metadata_manager, ctx):
    cat = inode_manager.add_entry(pyfuse3.ROOT_INODE, "cat", attr=_make_attr(True))
    sch = inode_manager.add_entry(cat.inode, "sch", attr=_make_attr(True))
    vol = inode_manager.add_entry(sch.inode, "vol", attr=_make_attr(True))
    f = inode_manager.add_entry(vol.inode, "data.txt", attr=_make_attr(False))

    O_RDONLY = 0
    file_info = await fs.open(f.inode, O_RDONLY, ctx)
    assert file_info is not None
    fh = int(file_info.fh)
    assert fh in fs._open_state


@pytest.mark.trio
async def test_open_directory_raises_eisdir(fs, inode_manager, metadata_manager, ctx):
    cat = inode_manager.add_entry(pyfuse3.ROOT_INODE, "cat", attr=_make_attr(True))

    O_RDONLY = 0
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        await fs.open(cat.inode, O_RDONLY, ctx)
    assert exc_info.value.errno == errno.EISDIR


@pytest.mark.trio
async def test_open_missing_inode_raises_enoent(fs, ctx, metadata_manager):
    metadata_manager.check_access.return_value = False
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        await fs.open(9999, 0, ctx)
    assert exc_info.value.errno in (errno.EACCES, errno.ENOENT)


@pytest.mark.trio
async def test_check_permissions_transient_error_surfaces_eagain(fs, inode_manager, metadata_manager, ctx):
    """A transient failure during the permission check must surface EAGAIN
    (retryable), not be collapsed into a permanent EACCES."""
    cat = inode_manager.add_entry(pyfuse3.ROOT_INODE, "cat", attr=_make_attr(True))
    metadata_manager.check_access = AsyncMock(side_effect=UcUnavailable("503"))
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        await fs.access(cat.inode, mode=4, ctx=ctx)
    assert exc_info.value.errno == errno.EAGAIN


@pytest.mark.trio
async def test_check_permissions_rate_limited_surfaces_eagain(fs, inode_manager, metadata_manager, ctx):
    cat = inode_manager.add_entry(pyfuse3.ROOT_INODE, "cat", attr=_make_attr(True))
    metadata_manager.check_access = AsyncMock(side_effect=UcRateLimited("429"))
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        await fs.access(cat.inode, mode=4, ctx=ctx)
    assert exc_info.value.errno == errno.EAGAIN


@pytest.mark.trio
async def test_check_permissions_denied_surfaces_eacces(fs, inode_manager, metadata_manager, ctx):
    """A genuine denial (UcPermissionDenied, or granted=False) stays EACCES."""
    cat = inode_manager.add_entry(pyfuse3.ROOT_INODE, "cat", attr=_make_attr(True))
    metadata_manager.check_access = AsyncMock(side_effect=UcPermissionDenied("403"))
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        await fs.access(cat.inode, mode=4, ctx=ctx)
    assert exc_info.value.errno == errno.EACCES


@pytest.mark.trio
async def test_check_permissions_granted_false_surfaces_eacces(fs, inode_manager, metadata_manager, ctx):
    cat = inode_manager.add_entry(pyfuse3.ROOT_INODE, "cat", attr=_make_attr(True))
    metadata_manager.check_access = AsyncMock(return_value=False)
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        await fs.access(cat.inode, mode=4, ctx=ctx)
    assert exc_info.value.errno == errno.EACCES


@pytest.mark.trio
async def test_release_removes_file_handle_state(fs, inode_manager, metadata_manager, ctx):
    cat = inode_manager.add_entry(pyfuse3.ROOT_INODE, "cat", attr=_make_attr(True))
    sch = inode_manager.add_entry(cat.inode, "sch", attr=_make_attr(True))
    vol = inode_manager.add_entry(sch.inode, "vol", attr=_make_attr(True))
    f = inode_manager.add_entry(vol.inode, "f.txt", attr=_make_attr(False))

    file_info = await fs.open(f.inode, 0, ctx)
    fh = int(file_info.fh)
    assert fh in fs._open_state

    await fs.release(pyfuse3.FileHandleT(fh))
    assert fh not in fs._open_state


@pytest.mark.trio
async def test_release_unknown_fh_is_noop(fs):
    await fs.release(pyfuse3.FileHandleT(9999))  # Must not raise


@pytest.mark.trio
async def test_release_auth_file_calls_auth_manager_release(fs, inode_manager, auth_manager, ctx):
    readme = inode_manager.add_entry(pyfuse3.ROOT_INODE, "README.txt", attr=_make_attr(False))
    file_info = await fs.open(readme.inode, 0, ctx)
    fh = int(file_info.fh)
    await fs.release(pyfuse3.FileHandleT(fh))
    auth_manager.release.assert_awaited_once()


# ---------------------------------------------------------------------------
# read
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_read_uc_file_delegates_to_data_manager(fs, inode_manager, data_manager, ctx):
    cat = inode_manager.add_entry(pyfuse3.ROOT_INODE, "cat", attr=_make_attr(True))
    sch = inode_manager.add_entry(cat.inode, "sch", attr=_make_attr(True))
    vol = inode_manager.add_entry(sch.inode, "vol", attr=_make_attr(True))
    f = inode_manager.add_entry(vol.inode, "data.txt", attr=_make_attr(False, size=100))

    file_info = await fs.open(f.inode, 0, ctx)
    fh = int(file_info.fh)

    data_manager.read.return_value = b"hello world"
    result = await fs.read(fh, offset=0, length=100)
    assert result == b"hello world"
    data_manager.read.assert_awaited_once()


@pytest.mark.trio
async def test_read_auth_file_delegates_to_auth_manager(fs, inode_manager, auth_manager, ctx):
    readme = inode_manager.add_entry(pyfuse3.ROOT_INODE, "README.txt", attr=_make_attr(False, size=500))
    file_info = await fs.open(readme.inode, 0, ctx)
    fh = int(file_info.fh)

    auth_manager.read.return_value = b"README content"
    result = await fs.read(fh, offset=0, length=500)
    assert result == b"README content"
    auth_manager.read.assert_awaited_once()


@pytest.mark.trio
async def test_read_unknown_fh_raises_eio(fs):
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        await fs.read(9999, offset=0, length=100)
    assert exc_info.value.errno == errno.EIO


# ---------------------------------------------------------------------------
# write
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_write_auth_file_delegates_to_auth_manager(fs, inode_manager, auth_manager, ctx):
    readme = inode_manager.add_entry(pyfuse3.ROOT_INODE, "README.txt", attr=_make_attr(False))
    file_info = await fs.open(readme.inode, 0, ctx)
    fh = int(file_info.fh)

    auth_manager.write.return_value = 5
    result = await fs.write(fh, offset=0, buffer=b"hello")
    assert result == 5
    auth_manager.write.assert_awaited_once()


@pytest.mark.trio
async def test_write_uc_file_raises_eacces(fs, inode_manager, metadata_manager, ctx):
    """
    The Unity Catalog filesystem is read-only: writing to a non-auth file must
    surface EACCES. (Previously this errno was incorrectly collapsed to EIO by a
    catch-all `except Exception`.)
    """
    cat = inode_manager.add_entry(pyfuse3.ROOT_INODE, "cat", attr=_make_attr(True))
    sch = inode_manager.add_entry(cat.inode, "sch", attr=_make_attr(True))
    vol = inode_manager.add_entry(sch.inode, "vol", attr=_make_attr(True))
    f = inode_manager.add_entry(vol.inode, "data.txt", attr=_make_attr(False))

    file_info = await fs.open(f.inode, 0, ctx)
    fh = int(file_info.fh)

    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        await fs.write(fh, offset=0, buffer=b"data")
    assert exc_info.value.errno == errno.EACCES


@pytest.mark.trio
async def test_write_unknown_fh_raises_eio(fs):
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        await fs.write(9999, offset=0, buffer=b"x")
    assert exc_info.value.errno == errno.EIO


# ---------------------------------------------------------------------------
# forget
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_forget_decrements_ref_count(fs, inode_manager):
    entry = inode_manager.add_entry(pyfuse3.ROOT_INODE, "tmp", attr=_make_attr(False))
    inode_manager.increment_lookup_count(entry.inode)  # ref = 1
    assert inode_manager.get_entry(entry.inode) is not None

    await fs.forget([(entry.inode, 1)])

    # ref = 0 → inode should be cleaned up
    assert inode_manager.get_entry(entry.inode) is None


# ---------------------------------------------------------------------------
# readdir
# ---------------------------------------------------------------------------


def _capture_readdir_replies(monkeypatch):
    """Patch pyfuse3.readdir_reply to record (name, next_id) and always accept."""
    calls: list = []

    def fake_reply(token, name, attr, next_id):
        calls.append((name, next_id))
        return True

    monkeypatch.setattr(pyfuse3, "readdir_reply", fake_reply)
    return calls


@pytest.mark.trio
async def test_readdir_root_merges_auth_overlay_then_catalogs(fs, metadata_manager, auth_manager, ctx, monkeypatch):
    # The auth manager owns the overlay names; readdir places them after . and ..
    auth_manager.root_overlay = MagicMock(return_value={
        ".auth": _make_attr(True),
        "README.txt": _make_attr(False, size=100),
    })
    metadata_manager.list_directory = AsyncMock(return_value={"my_catalog": _make_attr(True)})

    calls = _capture_readdir_replies(monkeypatch)

    fh = await fs.opendir(pyfuse3.ROOT_INODE, ctx)
    await fs.readdir(fh, 0, token=object())

    assert calls == [
        (b".", 1),
        (b"..", 2),
        (b".auth", 3),
        (b"README.txt", 4),
        (b"my_catalog", 5),
    ]


@pytest.mark.trio
async def test_readdir_root_resumes_midway(fs, metadata_manager, auth_manager, ctx, monkeypatch):
    # start_id=3 means . , .. and the first overlay entry (.auth) were already
    # delivered; readdir must resume at README.txt without repeating them.
    auth_manager.root_overlay = MagicMock(return_value={
        ".auth": _make_attr(True),
        "README.txt": _make_attr(False, size=100),
    })
    metadata_manager.list_directory = AsyncMock(return_value={"my_catalog": _make_attr(True)})

    calls = _capture_readdir_replies(monkeypatch)

    fh = await fs.opendir(pyfuse3.ROOT_INODE, ctx)
    await fs.readdir(fh, 3, token=object())

    assert calls == [
        (b"README.txt", 4),
        (b"my_catalog", 5),
    ]


@pytest.mark.trio
async def test_readdir_non_root_has_no_overlay(fs, inode_manager, metadata_manager, auth_manager, ctx, monkeypatch):
    cat = inode_manager.add_entry(pyfuse3.ROOT_INODE, "cat", attr=_make_attr(True))
    sch = inode_manager.add_entry(cat.inode, "sch", attr=_make_attr(True))
    vol = inode_manager.add_entry(sch.inode, "vol", attr=_make_attr(True))
    metadata_manager.list_directory = AsyncMock(return_value={"file.txt": _make_attr(False)})

    calls = _capture_readdir_replies(monkeypatch)

    fh = await fs.opendir(vol.inode, ctx)
    await fs.readdir(fh, 0, token=object())

    # No overlay for non-root dirs: children start right after . (1) and .. (2).
    assert calls == [
        (b".", 1),
        (b"..", 2),
        (b"file.txt", 3),
    ]
    auth_manager.root_overlay.assert_not_called()


# ---------------------------------------------------------------------------
# write path — O_WRONLY
# ---------------------------------------------------------------------------


def _make_vol_file(inode_manager, filename="data.txt", size=512):
    """Add cat/sch/vol/<filename> to the inode tree and return the file entry."""
    cat = inode_manager.add_entry(pyfuse3.ROOT_INODE, "cat", attr=_make_attr(True))
    sch = inode_manager.add_entry(cat.inode, "sch", attr=_make_attr(True))
    vol = inode_manager.add_entry(sch.inode, "vol", attr=_make_attr(True))
    f = inode_manager.add_entry(vol.inode, filename, attr=_make_attr(False, size=size))
    return vol, f


@pytest.mark.trio
async def test_open_wronly_creates_write_buffer(fs, inode_manager, ctx):
    _vol, f = _make_vol_file(inode_manager, size=0)
    O_WRONLY = 1
    file_info = await fs.open(f.inode, O_WRONLY, ctx)
    fh = int(file_info.fh)
    state = fs._open_state[fh]
    assert state["writable"] is True
    assert state["write_buffer"] is not None
    assert state["dirty"] is False
    await fs.release(pyfuse3.FileHandleT(fh))


@pytest.mark.trio
async def test_write_updates_st_size(fs, inode_manager, ctx):
    _vol, f = _make_vol_file(inode_manager, size=0)
    O_WRONLY = 1
    file_info = await fs.open(f.inode, O_WRONLY, ctx)
    fh = int(file_info.fh)
    data = b"x" * 1000
    n = await fs.write(fh, offset=0, buffer=data)
    assert n == 1000
    assert f.attr.st_size == 1000
    await fs.release(pyfuse3.FileHandleT(fh))


@pytest.mark.trio
async def test_write_at_nonzero_offset_extends_buffer_size(fs, inode_manager, ctx):
    _vol, f = _make_vol_file(inode_manager, size=0)
    O_WRONLY = 1
    file_info = await fs.open(f.inode, O_WRONLY, ctx)
    fh = int(file_info.fh)
    await fs.write(fh, offset=100, buffer=b"X")
    state = fs._open_state[fh]
    assert state["write_buffer"].size() == 101
    await fs.release(pyfuse3.FileHandleT(fh))


@pytest.mark.trio
async def test_write_sets_dirty_flag(fs, inode_manager, ctx):
    _vol, f = _make_vol_file(inode_manager, size=0)
    O_WRONLY = 1
    file_info = await fs.open(f.inode, O_WRONLY, ctx)
    fh = int(file_info.fh)
    assert fs._open_state[fh]["dirty"] is False
    await fs.write(fh, offset=0, buffer=b"data")
    assert fs._open_state[fh]["dirty"] is True
    await fs.release(pyfuse3.FileHandleT(fh))


@pytest.mark.trio
async def test_release_no_upload_when_not_dirty(fs, inode_manager, uc_client, ctx):
    """Open O_WRONLY without writing: release must skip the upload."""
    _vol, f = _make_vol_file(inode_manager, size=0)
    O_WRONLY = 1
    file_info = await fs.open(f.inode, O_WRONLY, ctx)
    fh = int(file_info.fh)
    await fs.release(pyfuse3.FileHandleT(fh))
    uc_client.upload_file.assert_not_awaited()


@pytest.mark.trio
async def test_release_upload_on_dirty(fs, inode_manager, uc_client, ctx):
    """Write then release: upload_file is called with the correct uc_path."""
    _vol, f = _make_vol_file(inode_manager, size=0)
    O_WRONLY = 1
    file_info = await fs.open(f.inode, O_WRONLY, ctx)
    fh = int(file_info.fh)
    await fs.write(fh, offset=0, buffer=b"content")
    await fs.release(pyfuse3.FileHandleT(fh))
    uc_client.upload_file.assert_awaited_once()
    call_args = uc_client.upload_file.call_args
    assert call_args.args[0] == "/Volumes/cat/sch/vol/data.txt"


@pytest.mark.trio
async def test_flush_uploads_on_dirty(fs, inode_manager, uc_client, metadata_manager, ctx):
    """flush() handles close(): it must upload, invalidate and clear dirty so the
    write is durable and visible by the time close() returns (release() used to
    do this asynchronously, after close() had already returned)."""
    _vol, f = _make_vol_file(inode_manager, size=0)
    metadata_manager.invalidate = MagicMock()
    O_WRONLY = 1
    file_info = await fs.open(f.inode, O_WRONLY, ctx)
    fh = int(file_info.fh)
    await fs.write(fh, offset=0, buffer=b"content")

    await fs.flush(pyfuse3.FileHandleT(fh))

    uc_client.upload_file.assert_awaited_once()
    assert uc_client.upload_file.call_args.args[0] == "/Volumes/cat/sch/vol/data.txt"
    metadata_manager.invalidate.assert_called_once_with(f.fs_path, is_dir=False)
    assert fs._open_state[fh]["dirty"] is False


@pytest.mark.trio
async def test_flush_then_release_does_not_reupload(fs, inode_manager, uc_client, ctx):
    """After flush() uploads, the release() fallback must be a no-op (dirty was
    cleared) so the file is uploaded exactly once per close()."""
    _vol, f = _make_vol_file(inode_manager, size=0)
    O_WRONLY = 1
    file_info = await fs.open(f.inode, O_WRONLY, ctx)
    fh = int(file_info.fh)
    await fs.write(fh, offset=0, buffer=b"content")

    await fs.flush(pyfuse3.FileHandleT(fh))
    await fs.release(pyfuse3.FileHandleT(fh))

    uc_client.upload_file.assert_awaited_once()


@pytest.mark.trio
async def test_flush_upload_failure_returns_eio(fs, inode_manager, uc_client, metadata_manager, ctx):
    """A failed upload in flush() surfaces as EIO to close() and must NOT
    invalidate the cache (the remote file is unchanged)."""
    _vol, f = _make_vol_file(inode_manager, size=0)
    uc_client.upload_file = AsyncMock(side_effect=UcUnavailable("503"))
    metadata_manager.invalidate = MagicMock()
    O_WRONLY = 1
    file_info = await fs.open(f.inode, O_WRONLY, ctx)
    fh = int(file_info.fh)
    await fs.write(fh, offset=0, buffer=b"data")

    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        await fs.flush(pyfuse3.FileHandleT(fh))
    assert exc_info.value.errno == errno.EIO
    metadata_manager.invalidate.assert_not_called()
    # Still dirty, so the release() fallback retries the upload.
    assert fs._open_state[fh]["dirty"] is True


@pytest.mark.trio
async def test_flush_unknown_fh_is_noop(fs):
    await fs.flush(pyfuse3.FileHandleT(9999))  # Must not raise


@pytest.mark.trio
async def test_flush_not_dirty_no_upload(fs, inode_manager, uc_client, ctx):
    """Open O_WRONLY without writing, then flush: nothing to upload."""
    _vol, f = _make_vol_file(inode_manager, size=0)
    O_WRONLY = 1
    file_info = await fs.open(f.inode, O_WRONLY, ctx)
    fh = int(file_info.fh)
    await fs.flush(pyfuse3.FileHandleT(fh))
    uc_client.upload_file.assert_not_awaited()


@pytest.mark.trio
async def test_release_invalidates_cache_on_success(fs, inode_manager, metadata_manager, ctx):
    _vol, f = _make_vol_file(inode_manager, size=0)
    metadata_manager.invalidate = MagicMock()
    O_WRONLY = 1
    file_info = await fs.open(f.inode, O_WRONLY, ctx)
    fh = int(file_info.fh)
    await fs.write(fh, offset=0, buffer=b"new content")
    await fs.release(pyfuse3.FileHandleT(fh))
    metadata_manager.invalidate.assert_called_once_with(f.fs_path, is_dir=False)


@pytest.mark.trio
async def test_release_upload_failure_returns_eio(fs, inode_manager, uc_client, metadata_manager, ctx):
    """Upload failure: release raises EIO and does NOT invalidate the cache."""
    _vol, f = _make_vol_file(inode_manager, size=0)
    uc_client.upload_file = AsyncMock(side_effect=UcUnavailable("503"))
    metadata_manager.invalidate = MagicMock()
    O_WRONLY = 1
    file_info = await fs.open(f.inode, O_WRONLY, ctx)
    fh = int(file_info.fh)
    await fs.write(fh, offset=0, buffer=b"data")
    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        await fs.release(pyfuse3.FileHandleT(fh))
    assert exc_info.value.errno == errno.EIO
    metadata_manager.invalidate.assert_not_called()


@pytest.mark.trio
async def test_release_cleans_tempfile_on_failure(fs, inode_manager, uc_client, ctx):
    """Tempfile must be deleted even when the upload raises."""
    import os
    _vol, f = _make_vol_file(inode_manager, size=0)
    uc_client.upload_file = AsyncMock(side_effect=RuntimeError("network error"))
    O_WRONLY = 1
    file_info = await fs.open(f.inode, O_WRONLY, ctx)
    fh = int(file_info.fh)
    await fs.write(fh, offset=0, buffer=b"data")
    tmppath = fs._open_state[fh]["write_buffer"].path
    assert os.path.exists(tmppath)
    with pytest.raises(pyfuse3.FUSEError):
        await fs.release(pyfuse3.FileHandleT(fh))
    assert not os.path.exists(tmppath)


@pytest.mark.trio
async def test_wronly_preloads_existing_file_on_open(fs, inode_manager, data_manager, ctx):
    """open(O_WRONLY) without O_TRUNC must pre-load the existing remote content,
    exactly like O_RDWR — otherwise a partial write would truncate the file."""
    _vol, f = _make_vol_file(inode_manager, size=5)
    data_manager.read = AsyncMock(return_value=b"hello")
    O_WRONLY = 1
    file_info = await fs.open(f.inode, O_WRONLY, ctx)
    fh = int(file_info.fh)
    data_manager.read.assert_awaited_once()
    assert fs._open_state[fh]["write_buffer"].size() == 5
    await fs.release(pyfuse3.FileHandleT(fh))


@pytest.mark.trio
async def test_wronly_trunc_skips_preload(fs, inode_manager, data_manager, ctx):
    """open(O_WRONLY | O_TRUNC) must NOT pre-load — buffer starts empty."""
    _vol, f = _make_vol_file(inode_manager, size=5)
    O_WRONLY = 1
    O_TRUNC = 0o1000
    file_info = await fs.open(f.inode, O_WRONLY | O_TRUNC, ctx)
    fh = int(file_info.fh)
    data_manager.read.assert_not_awaited()
    assert fs._open_state[fh]["write_buffer"].size() == 0
    await fs.release(pyfuse3.FileHandleT(fh))


@pytest.mark.trio
async def test_wronly_partial_write_preserves_tail(fs, inode_manager, data_manager, uc_client, ctx):
    """Regression: a partial O_WRONLY write must keep the bytes it did not touch.

    Opening O_WRONLY (no O_TRUNC) and rewriting only the leading bytes used to
    upload a truncated file, silently destroying the tail. Pre-loading makes the
    buffer start as a copy of the remote file, so the untouched tail survives.
    """
    _vol, f = _make_vol_file(inode_manager, size=10)
    data_manager.read = AsyncMock(return_value=b"0123456789")
    O_WRONLY = 1
    file_info = await fs.open(f.inode, O_WRONLY, ctx)
    fh = int(file_info.fh)
    # Overwrite only the first 3 bytes; the remaining 7 must be preserved.
    await fs.write(fh, offset=0, buffer=b"AAA")
    wb = fs._open_state[fh]["write_buffer"]
    assert wb.size() == 10
    assert await wb.read(0, 10) == b"AAA3456789"
    await fs.release(pyfuse3.FileHandleT(fh))
    uc_client.upload_file.assert_awaited_once()


# ---------------------------------------------------------------------------
# write path — create()
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_create_returns_attrs_and_file_handle(fs, inode_manager, ctx):
    cat = inode_manager.add_entry(pyfuse3.ROOT_INODE, "cat", attr=_make_attr(True))
    sch = inode_manager.add_entry(cat.inode, "sch", attr=_make_attr(True))
    vol = inode_manager.add_entry(sch.inode, "vol", attr=_make_attr(True))
    file_info, attrs = await fs.create(vol.inode, b"newfile.txt", 0o644, 0, ctx)
    assert attrs.st_size == 0
    assert int(file_info.fh) in fs._open_state


@pytest.mark.trio
async def test_create_pins_inode_like_lookup(fs, inode_manager, ctx):
    """create() returns an EntryAttributes, so the kernel holds a reference and
    will send a matching forget(). The new inode must be pinned (ref_count == 1)
    so that single forget brings it to 0 — not below — and it isn't freed while
    still referenced."""
    cat = inode_manager.add_entry(pyfuse3.ROOT_INODE, "cat", attr=_make_attr(True))
    sch = inode_manager.add_entry(cat.inode, "sch", attr=_make_attr(True))
    vol = inode_manager.add_entry(sch.inode, "vol", attr=_make_attr(True))
    _file_info, _attrs = await fs.create(vol.inode, b"pinned.txt", 0o644, 0, ctx)

    inode = inode_manager.get_inode_by_path("/cat/sch/vol/pinned.txt")
    assert inode is not None
    assert inode_manager.get_entry(inode).ref_count == 1

    # The kernel's matching forget for the create reply takes it to 0.
    inode_manager.forget(inode, 1)
    assert inode_manager.get_entry(inode) is None


@pytest.mark.trio
async def test_create_dirty_true_uploads_on_release(fs, inode_manager, uc_client, ctx):
    """create() marks the handle dirty so release() always uploads (even with no writes)."""
    cat = inode_manager.add_entry(pyfuse3.ROOT_INODE, "cat", attr=_make_attr(True))
    sch = inode_manager.add_entry(cat.inode, "sch", attr=_make_attr(True))
    vol = inode_manager.add_entry(sch.inode, "vol", attr=_make_attr(True))
    file_info, _attrs = await fs.create(vol.inode, b"empty.txt", 0o644, 0, ctx)
    fh = int(file_info.fh)
    # No explicit write — but dirty=True from create()
    await fs.release(pyfuse3.FileHandleT(fh))
    uc_client.upload_file.assert_awaited_once()
    call_args = uc_client.upload_file.call_args
    assert call_args.args[0] == "/Volumes/cat/sch/vol/empty.txt"


@pytest.mark.trio
async def test_create_then_write_then_release(fs, inode_manager, uc_client, ctx):
    cat = inode_manager.add_entry(pyfuse3.ROOT_INODE, "cat", attr=_make_attr(True))
    sch = inode_manager.add_entry(cat.inode, "sch", attr=_make_attr(True))
    vol = inode_manager.add_entry(sch.inode, "vol", attr=_make_attr(True))
    file_info, _attrs = await fs.create(vol.inode, b"out.txt", 0o644, 0, ctx)
    fh = int(file_info.fh)
    await fs.write(fh, offset=0, buffer=b"hello world")
    await fs.release(pyfuse3.FileHandleT(fh))
    uc_client.upload_file.assert_awaited_once()
    call_args = uc_client.upload_file.call_args
    assert call_args.args[0] == "/Volumes/cat/sch/vol/out.txt"


# ---------------------------------------------------------------------------
# write path — O_RDWR
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_rdwr_downloads_existing_file_on_open(fs, inode_manager, data_manager, ctx):
    """open(O_RDWR) without O_TRUNC streams the existing file into the buffer one chunk
    at a time so memory usage is O(chunk_size), not O(file_size)."""
    _vol, f = _make_vol_file(inode_manager, size=5)
    data_manager.read = AsyncMock(return_value=b"hello")
    O_RDWR = 2
    file_info = await fs.open(f.inode, O_RDWR, ctx)
    fh = int(file_info.fh)
    # File fits in one chunk → read called exactly once
    data_manager.read.assert_awaited_once()
    assert fs._open_state[fh]["write_buffer"] is not None
    assert fs._open_state[fh]["write_buffer"].size() == 5
    await fs.release(pyfuse3.FileHandleT(fh))


@pytest.mark.trio
async def test_rdwr_multi_chunk_streams_in_pieces(fs, inode_manager, data_manager, ctx):
    """open(O_RDWR) on a file larger than one chunk calls data_manager.read once per chunk."""
    chunk_size = data_manager.chunk_size  # 8 MB
    file_size = chunk_size * 3  # three full chunks
    chunk_data = b"x" * chunk_size
    _vol, f = _make_vol_file(inode_manager, size=file_size)
    data_manager.read = AsyncMock(return_value=chunk_data)
    O_RDWR = 2
    file_info = await fs.open(f.inode, O_RDWR, ctx)
    fh = int(file_info.fh)
    # Three chunks → three separate read() calls, each getting chunk_size bytes
    assert data_manager.read.await_count == 3
    assert fs._open_state[fh]["write_buffer"].size() == file_size
    await fs.release(pyfuse3.FileHandleT(fh))


@pytest.mark.trio
async def test_rdwr_trunc_skips_download(fs, inode_manager, data_manager, ctx):
    """open(O_RDWR | O_TRUNC) must NOT download — buffer starts empty."""
    _vol, f = _make_vol_file(inode_manager)
    O_RDWR = 2
    O_TRUNC = 0o1000
    file_info = await fs.open(f.inode, O_RDWR | O_TRUNC, ctx)
    fh = int(file_info.fh)
    data_manager.read.assert_not_awaited()
    assert fs._open_state[fh]["write_buffer"].size() == 0
    await fs.release(pyfuse3.FileHandleT(fh))


@pytest.mark.trio
async def test_rdwr_read_served_from_buffer(fs, inode_manager, data_manager, ctx):
    """Reads on an O_RDWR handle are served from the local buffer, not DataManager."""
    _vol, f = _make_vol_file(inode_manager, size=5)
    data_manager.read = AsyncMock(return_value=b"hello")
    O_RDWR = 2
    file_info = await fs.open(f.inode, O_RDWR, ctx)
    fh = int(file_info.fh)
    data_manager.read.reset_mock()
    result = await fs.read(fh, offset=0, length=5)
    assert result == b"hello"
    data_manager.read.assert_not_awaited()
    await fs.release(pyfuse3.FileHandleT(fh))


@pytest.mark.trio
async def test_rdwr_sees_own_writes(fs, inode_manager, data_manager, ctx):
    """A write followed by a read on the same O_RDWR handle returns the new content."""
    _vol, f = _make_vol_file(inode_manager, size=5)
    data_manager.read = AsyncMock(return_value=b"hello")
    O_RDWR = 2
    file_info = await fs.open(f.inode, O_RDWR, ctx)
    fh = int(file_info.fh)
    await fs.write(fh, offset=0, buffer=b"world")
    result = await fs.read(fh, offset=0, length=5)
    assert result == b"world"
    await fs.release(pyfuse3.FileHandleT(fh))


@pytest.mark.trio
async def test_rdwr_not_dirty_no_upload(fs, inode_manager, data_manager, uc_client, ctx):
    """Open O_RDWR, only read, release: no upload should happen."""
    _vol, f = _make_vol_file(inode_manager, size=5)
    data_manager.read = AsyncMock(return_value=b"hello")
    O_RDWR = 2
    file_info = await fs.open(f.inode, O_RDWR, ctx)
    fh = int(file_info.fh)
    await fs.read(fh, offset=0, length=5)
    await fs.release(pyfuse3.FileHandleT(fh))
    uc_client.upload_file.assert_not_awaited()


@pytest.mark.trio
async def test_rdwr_dirty_uploads(fs, inode_manager, data_manager, uc_client, ctx):
    """Open O_RDWR, write something, release: upload is called."""
    _vol, f = _make_vol_file(inode_manager, size=5)
    data_manager.read = AsyncMock(return_value=b"hello")
    O_RDWR = 2
    file_info = await fs.open(f.inode, O_RDWR, ctx)
    fh = int(file_info.fh)
    await fs.write(fh, offset=0, buffer=b"world")
    await fs.release(pyfuse3.FileHandleT(fh))
    uc_client.upload_file.assert_awaited_once()


# ---------------------------------------------------------------------------
# mkdir
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_mkdir_calls_create_directory(fs, inode_manager, uc_client, metadata_manager, ctx):
    cat = inode_manager.add_entry(pyfuse3.ROOT_INODE, "cat", attr=_make_attr(True))
    sch = inode_manager.add_entry(cat.inode, "sch", attr=_make_attr(True))
    vol = inode_manager.add_entry(sch.inode, "vol", attr=_make_attr(True))
    metadata_manager.invalidate = MagicMock()

    attrs = await fs.mkdir(vol.inode, b"newdir", 0o755, ctx)

    assert bool(attrs.st_mode & stat.S_IFDIR)
    uc_client.create_directory.assert_awaited_once()
    call_args = uc_client.create_directory.call_args
    assert call_args.args[0] == "/Volumes/cat/sch/vol/newdir"
    # Invalidate child path so parent's dir listing is refreshed.
    metadata_manager.invalidate.assert_called_once_with("/cat/sch/vol/newdir", is_dir=True)


@pytest.mark.trio
async def test_mkdir_eacces_without_write_permission(fs, inode_manager, metadata_manager, ctx):
    cat = inode_manager.add_entry(pyfuse3.ROOT_INODE, "cat", attr=_make_attr(True))
    sch = inode_manager.add_entry(cat.inode, "sch", attr=_make_attr(True))
    vol = inode_manager.add_entry(sch.inode, "vol", attr=_make_attr(True))
    metadata_manager.check_access = AsyncMock(side_effect=pyfuse3.FUSEError(errno.EACCES))

    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        await fs.mkdir(vol.inode, b"newdir", 0o755, ctx)
    assert exc_info.value.errno == errno.EACCES


# ---------------------------------------------------------------------------
# unlink
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_unlink_calls_delete_file(fs, inode_manager, uc_client, metadata_manager, ctx):
    _vol, f = _make_vol_file(inode_manager)
    metadata_manager.invalidate = MagicMock()

    await fs.unlink(f.parent_inode, b"data.txt", ctx)

    uc_client.delete_file.assert_awaited_once()
    call_args = uc_client.delete_file.call_args
    assert call_args.args[0] == "/Volumes/cat/sch/vol/data.txt"
    metadata_manager.invalidate.assert_called_once_with("/cat/sch/vol/data.txt", is_dir=False)
    # Inode should be pruned
    assert inode_manager.get_inode_by_path("/cat/sch/vol/data.txt") is None


@pytest.mark.trio
async def test_unlink_notfound_returns_enoent(fs, inode_manager, uc_client, ctx):
    _vol, f = _make_vol_file(inode_manager)
    uc_client.delete_file = AsyncMock(side_effect=UcNotFound("404"))

    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        await fs.unlink(f.parent_inode, b"data.txt", ctx)
    assert exc_info.value.errno == errno.ENOENT


# ---------------------------------------------------------------------------
# rmdir
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_rmdir_calls_delete_directory(fs, inode_manager, uc_client, metadata_manager, ctx):
    cat = inode_manager.add_entry(pyfuse3.ROOT_INODE, "cat", attr=_make_attr(True))
    sch = inode_manager.add_entry(cat.inode, "sch", attr=_make_attr(True))
    vol = inode_manager.add_entry(sch.inode, "vol", attr=_make_attr(True))
    subdir = inode_manager.add_entry(vol.inode, "subdir", attr=_make_attr(True))
    metadata_manager.invalidate = MagicMock()

    await fs.rmdir(vol.inode, b"subdir", ctx)

    uc_client.delete_directory.assert_awaited_once()
    call_args = uc_client.delete_directory.call_args
    assert call_args.args[0] == "/Volumes/cat/sch/vol/subdir"
    metadata_manager.invalidate.assert_called_once_with("/cat/sch/vol/subdir", is_dir=True)
    assert inode_manager.get_inode_by_path("/cat/sch/vol/subdir") is None


@pytest.mark.trio
async def test_rmdir_nonempty_returns_enotempty(fs, inode_manager, uc_client, ctx):
    cat = inode_manager.add_entry(pyfuse3.ROOT_INODE, "cat", attr=_make_attr(True))
    sch = inode_manager.add_entry(cat.inode, "sch", attr=_make_attr(True))
    vol = inode_manager.add_entry(sch.inode, "vol", attr=_make_attr(True))
    inode_manager.add_entry(vol.inode, "subdir", attr=_make_attr(True))
    uc_client.delete_directory = AsyncMock(side_effect=UcBadRequest("400 non-empty"))

    with pytest.raises(pyfuse3.FUSEError) as exc_info:
        await fs.rmdir(vol.inode, b"subdir", ctx)
    assert exc_info.value.errno == errno.ENOTEMPTY


# ---------------------------------------------------------------------------
# POSIX behaviour documentation tests
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_posix_write_hidden_until_release(fs, inode_manager, data_manager, uc_client, ctx):
    """
    A reader that opened the file before the writer's release() sees the pre-upload
    content. The write is not visible until release() completes the upload.

    This is the guaranteed behaviour: readers always see a complete file — either the
    pre-write or the post-write version, never a partial upload.
    """
    _vol, f = _make_vol_file(inode_manager, size=5)
    data_manager.read = AsyncMock(return_value=b"old  ")

    # Reader opens the file (O_RDONLY)
    read_info = await fs.open(f.inode, 0, ctx)
    rfh = int(read_info.fh)

    # Writer opens O_WRONLY and writes new content
    O_WRONLY = 1
    write_info = await fs.open(f.inode, O_WRONLY, ctx)
    wfh = int(write_info.fh)
    await fs.write(wfh, offset=0, buffer=b"new!!")

    # Reader still sees old content via DataManager (writer hasn't released yet)
    read_data = await fs.read(rfh, offset=0, length=5)
    assert read_data == b"old  "
    uc_client.upload_file.assert_not_awaited()

    # Writer releases — upload happens now
    await fs.release(pyfuse3.FileHandleT(wfh))
    uc_client.upload_file.assert_awaited_once()

    await fs.release(pyfuse3.FileHandleT(rfh))


@pytest.mark.trio
async def test_posix_concurrent_writers_last_wins(fs, inode_manager, uc_client, ctx):
    """
    Two independent writable handles both call release(). The second upload overwrites
    the first silently. No error is returned to either writer. This matches object-store
    (S3/ADLS/GCS) semantics and is documented as an accepted deviation from POSIX.
    """
    # O_TRUNC: both writers fully overwrite, so no pre-load of existing content.
    _vol, f = _make_vol_file(inode_manager)
    O_WRONLY = 1
    O_TRUNC = 0o1000
    info_a = await fs.open(f.inode, O_WRONLY | O_TRUNC, ctx)
    info_b = await fs.open(f.inode, O_WRONLY | O_TRUNC, ctx)
    fha, fhb = int(info_a.fh), int(info_b.fh)

    await fs.write(fha, offset=0, buffer=b"writer-A")
    await fs.write(fhb, offset=0, buffer=b"writer-B")

    # Both release without error
    await fs.release(pyfuse3.FileHandleT(fha))
    await fs.release(pyfuse3.FileHandleT(fhb))

    assert uc_client.upload_file.await_count == 2


# ---------------------------------------------------------------------------
# --read-only mount flag
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_read_only_open_wronly_raises_erofs(fs_ro, inode_manager, ctx):
    """open(O_WRONLY) on a UC file when read_only=True raises EROFS."""
    _vol, f = _make_vol_file(inode_manager)
    with pytest.raises(pyfuse3.FUSEError) as exc:
        await fs_ro.open(f.inode, 1, ctx)  # O_WRONLY = 1
    assert exc.value.errno == errno.EROFS


@pytest.mark.trio
async def test_read_only_open_rdwr_raises_erofs(fs_ro, inode_manager, ctx):
    """open(O_RDWR) on a UC file when read_only=True raises EROFS."""
    _vol, f = _make_vol_file(inode_manager)
    with pytest.raises(pyfuse3.FUSEError) as exc:
        await fs_ro.open(f.inode, 2, ctx)  # O_RDWR = 2
    assert exc.value.errno == errno.EROFS


@pytest.mark.trio
async def test_read_only_open_rdonly_succeeds(fs_ro, inode_manager, ctx):
    """open(O_RDONLY) on a UC file is still allowed when read_only=True."""
    _vol, f = _make_vol_file(inode_manager)
    file_info = await fs_ro.open(f.inode, 0, ctx)  # O_RDONLY = 0
    await fs_ro.release(pyfuse3.FileHandleT(int(file_info.fh)))


@pytest.mark.trio
async def test_read_only_create_raises_erofs(fs_ro, inode_manager, ctx):
    """create() raises EROFS when read_only=True."""
    cat = inode_manager.add_entry(pyfuse3.ROOT_INODE, "cat", attr=_make_attr(True))
    sch = inode_manager.add_entry(cat.inode, "sch", attr=_make_attr(True))
    vol = inode_manager.add_entry(sch.inode, "vol", attr=_make_attr(True))
    with pytest.raises(pyfuse3.FUSEError) as exc:
        await fs_ro.create(vol.inode, b"new.txt", 0o644, 0, ctx)
    assert exc.value.errno == errno.EROFS


@pytest.mark.trio
async def test_read_only_mkdir_raises_erofs(fs_ro, inode_manager, ctx):
    """mkdir() raises EROFS when read_only=True."""
    cat = inode_manager.add_entry(pyfuse3.ROOT_INODE, "cat", attr=_make_attr(True))
    sch = inode_manager.add_entry(cat.inode, "sch", attr=_make_attr(True))
    vol = inode_manager.add_entry(sch.inode, "vol", attr=_make_attr(True))
    with pytest.raises(pyfuse3.FUSEError) as exc:
        await fs_ro.mkdir(vol.inode, b"newdir", 0o755, ctx)
    assert exc.value.errno == errno.EROFS


@pytest.mark.trio
async def test_read_only_unlink_raises_erofs(fs_ro, inode_manager, ctx):
    """unlink() raises EROFS when read_only=True."""
    vol, f = _make_vol_file(inode_manager)
    with pytest.raises(pyfuse3.FUSEError) as exc:
        await fs_ro.unlink(vol.inode, f.name.encode(), ctx)
    assert exc.value.errno == errno.EROFS


@pytest.mark.trio
async def test_read_only_rmdir_raises_erofs(fs_ro, inode_manager, ctx):
    """rmdir() raises EROFS when read_only=True."""
    cat = inode_manager.add_entry(pyfuse3.ROOT_INODE, "cat", attr=_make_attr(True))
    sch = inode_manager.add_entry(cat.inode, "sch", attr=_make_attr(True))
    vol = inode_manager.add_entry(sch.inode, "vol", attr=_make_attr(True))
    subdir = inode_manager.add_entry(vol.inode, "subdir", attr=_make_attr(True))
    with pytest.raises(pyfuse3.FUSEError) as exc:
        await fs_ro.rmdir(vol.inode, subdir.name.encode(), ctx)
    assert exc.value.errno == errno.EROFS


@pytest.mark.trio
async def test_read_only_auth_write_still_allowed(fs_ro, inode_manager, auth_manager, ctx):
    """Writing to .auth is still allowed even when read_only=True."""
    auth_root = inode_manager.add_entry(pyfuse3.ROOT_INODE, ".auth", attr=_make_attr(True))
    token_file = inode_manager.add_entry(auth_root.inode, "token", attr=_make_attr(False))
    # open O_WRONLY on an auth file — must NOT raise EROFS
    file_info = await fs_ro.open(token_file.inode, 1, ctx)  # O_WRONLY = 1
    await fs_ro.release(pyfuse3.FileHandleT(int(file_info.fh)))


# ---------------------------------------------------------------------------
# rename
# ---------------------------------------------------------------------------


def _fields(**kw):
    """A SetattrFields stand-in (the real type is immutable). setattr only
    reads these update_* booleans."""
    base = dict(
        update_size=False, update_mode=False, update_uid=False,
        update_gid=False, update_atime=False, update_mtime=False,
        update_ctime=False,
    )
    base.update(kw)
    return SimpleNamespace(**base)


@pytest.mark.trio
async def test_rename_file_copies_then_deletes_and_moves_inode(
    fs, inode_manager, uc_client, metadata_manager, data_manager, ctx
):
    vol, f = _make_vol_file(inode_manager, size=5)  # data_manager.read -> b"hello"
    metadata_manager.lookup_child = AsyncMock(return_value=None)  # dest is free
    metadata_manager.invalidate = MagicMock()

    await fs.rename(vol.inode, b"data.txt", vol.inode, b"renamed.txt", 0, ctx)

    # Uploaded to the new path, deleted from the old.
    uc_client.upload_file.assert_awaited_once()
    assert uc_client.upload_file.call_args.args[0] == "/Volumes/cat/sch/vol/renamed.txt"
    uc_client.delete_file.assert_awaited_once()
    assert uc_client.delete_file.call_args.args[0] == "/Volumes/cat/sch/vol/data.txt"

    # Inode moved: new path resolves to the same inode, old path is gone.
    assert inode_manager.get_inode_by_path("/cat/sch/vol/data.txt") is None
    moved = inode_manager.get_inode_by_path("/cat/sch/vol/renamed.txt")
    assert moved == f.inode
    assert inode_manager.get_entry(moved).name == "renamed.txt"

    # Both ends invalidated.
    metadata_manager.invalidate.assert_any_call("/cat/sch/vol/data.txt", is_dir=False)
    metadata_manager.invalidate.assert_any_call("/cat/sch/vol/renamed.txt", is_dir=False)


@pytest.mark.trio
async def test_rename_refreshes_source_mtime_before_copy(
    fs, inode_manager, uc_client, metadata_manager, data_manager, ctx
):
    """Regression: a file just written through the mount carries a locally
    stamped st_mtime (create()-time wall clock) that is earlier than the
    server's Last-Modified. rename() must refresh the source from the server
    before copying, so the If-Unmodified-Since the copy sends matches the
    server and does not 412. The copy read must use the refreshed mtime/size,
    not the stale inode values."""
    vol, f = _make_vol_file(inode_manager, size=512)
    # Stale, locally-stamped attributes left on the inode after create()/write.
    f.attr.st_mtime = 9999.0
    f.attr.st_size = 512
    # The server's authoritative view, returned by the refresh.
    server_attr = _make_attr(False, size=5)  # _make_attr -> st_mtime=2000.0
    metadata_manager.get_attributes = AsyncMock(return_value=server_attr)
    metadata_manager.lookup_child = AsyncMock(return_value=None)  # dest is free
    metadata_manager.invalidate = MagicMock()

    await fs.rename(vol.inode, b"data.txt", vol.inode, b"renamed.txt", 0, ctx)

    # Source was refreshed from the server before the copy.
    metadata_manager.get_attributes.assert_awaited_once()
    # The copy read used the server mtime (2000.0) and size (5), not the stale
    # local values (9999.0 / 512).
    read_args = data_manager.read.call_args.args
    assert read_args[3] == 2000.0           # mtime -> If-Unmodified-Since
    assert read_args[4] == 5                # file_size


@pytest.mark.trio
async def test_rename_source_deleted_during_refresh_raises_enoent(
    fs, inode_manager, metadata_manager, ctx
):
    """If the source vanished by the time we refresh its metadata, rename
    surfaces ENOENT rather than copying with stale attributes."""
    vol, f = _make_vol_file(inode_manager, size=5)
    metadata_manager.get_attributes = AsyncMock(return_value=None)  # gone
    metadata_manager.lookup_child = AsyncMock(return_value=None)

    with pytest.raises(pyfuse3.FUSEError) as ei:
        await fs.rename(vol.inode, b"data.txt", vol.inode, b"renamed.txt", 0, ctx)
    assert ei.value.errno == errno.ENOENT


@pytest.mark.trio
async def test_rename_into_other_directory_reparents_inode(
    fs, inode_manager, uc_client, metadata_manager, ctx
):
    cat = inode_manager.add_entry(pyfuse3.ROOT_INODE, "cat", attr=_make_attr(True))
    sch = inode_manager.add_entry(cat.inode, "sch", attr=_make_attr(True))
    vol = inode_manager.add_entry(sch.inode, "vol", attr=_make_attr(True))
    sub = inode_manager.add_entry(vol.inode, "sub", attr=_make_attr(True))
    f = inode_manager.add_entry(vol.inode, "data.txt", attr=_make_attr(False, size=5))
    metadata_manager.lookup_child = AsyncMock(return_value=None)

    await fs.rename(vol.inode, b"data.txt", sub.inode, b"data.txt", 0, ctx)

    assert uc_client.upload_file.call_args.args[0] == "/Volumes/cat/sch/vol/sub/data.txt"
    moved = inode_manager.get_inode_by_path("/cat/sch/vol/sub/data.txt")
    assert moved == f.inode
    assert inode_manager.get_entry(moved).parent_inode == sub.inode


@pytest.mark.trio
async def test_rename_directory_raises_exdev(fs, inode_manager, metadata_manager, ctx):
    cat = inode_manager.add_entry(pyfuse3.ROOT_INODE, "cat", attr=_make_attr(True))
    sch = inode_manager.add_entry(cat.inode, "sch", attr=_make_attr(True))
    vol = inode_manager.add_entry(sch.inode, "vol", attr=_make_attr(True))
    inode_manager.add_entry(vol.inode, "adir", attr=_make_attr(True))
    with pytest.raises(pyfuse3.FUSEError) as exc:
        await fs.rename(vol.inode, b"adir", vol.inode, b"bdir", 0, ctx)
    assert exc.value.errno == errno.EXDEV


@pytest.mark.trio
async def test_rename_noreplace_existing_raises_eexist(
    fs, inode_manager, metadata_manager, uc_client, ctx
):
    vol, f = _make_vol_file(inode_manager, size=5)
    # Destination exists (default lookup_child returns a file attr).
    metadata_manager.lookup_child = AsyncMock(return_value=_make_attr(False))
    with pytest.raises(pyfuse3.FUSEError) as exc:
        await fs.rename(vol.inode, b"data.txt", vol.inode, b"taken.txt",
                        pyfuse3.RENAME_NOREPLACE, ctx)
    assert exc.value.errno == errno.EEXIST
    uc_client.upload_file.assert_not_awaited()


@pytest.mark.trio
async def test_rename_over_existing_directory_raises_eisdir(
    fs, inode_manager, metadata_manager, uc_client, ctx
):
    vol, f = _make_vol_file(inode_manager, size=5)
    metadata_manager.lookup_child = AsyncMock(return_value=_make_attr(True))  # dest is a dir
    with pytest.raises(pyfuse3.FUSEError) as exc:
        await fs.rename(vol.inode, b"data.txt", vol.inode, b"adir", 0, ctx)
    assert exc.value.errno == errno.EISDIR
    uc_client.upload_file.assert_not_awaited()


@pytest.mark.trio
async def test_rename_exchange_flag_raises_einval(fs, inode_manager, ctx):
    vol, f = _make_vol_file(inode_manager, size=5)
    with pytest.raises(pyfuse3.FUSEError) as exc:
        await fs.rename(vol.inode, b"data.txt", vol.inode, b"other.txt",
                        pyfuse3.RENAME_EXCHANGE, ctx)
    assert exc.value.errno == errno.EINVAL


@pytest.mark.trio
async def test_rename_read_only_raises_erofs(fs_ro, inode_manager, ctx):
    vol, f = _make_vol_file(inode_manager, size=5)
    with pytest.raises(pyfuse3.FUSEError) as exc:
        await fs_ro.rename(vol.inode, b"data.txt", vol.inode, b"renamed.txt", 0, ctx)
    assert exc.value.errno == errno.EROFS


@pytest.mark.trio
async def test_rename_upload_failure_does_not_delete_source(
    fs, inode_manager, uc_client, metadata_manager, ctx
):
    vol, f = _make_vol_file(inode_manager, size=5)
    metadata_manager.lookup_child = AsyncMock(return_value=None)
    uc_client.upload_file = AsyncMock(side_effect=UcUnavailable("503"))
    with pytest.raises(pyfuse3.FUSEError) as exc:
        await fs.rename(vol.inode, b"data.txt", vol.inode, b"renamed.txt", 0, ctx)
    assert exc.value.errno == errno.EAGAIN
    # Source must survive a failed copy.
    uc_client.delete_file.assert_not_awaited()
    assert inode_manager.get_inode_by_path("/cat/sch/vol/data.txt") == f.inode


@pytest.mark.trio
async def test_rename_same_path_is_noop(fs, inode_manager, uc_client, ctx):
    vol, f = _make_vol_file(inode_manager, size=5)
    await fs.rename(vol.inode, b"data.txt", vol.inode, b"data.txt", 0, ctx)
    uc_client.upload_file.assert_not_awaited()
    uc_client.delete_file.assert_not_awaited()


# ---------------------------------------------------------------------------
# setattr
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_setattr_ftruncate_shrinks_open_buffer(fs, inode_manager, ctx):
    _vol, f = _make_vol_file(inode_manager, size=0)
    file_info = await fs.open(f.inode, 1, ctx)  # O_WRONLY
    fh = int(file_info.fh)
    await fs.write(fh, offset=0, buffer=b"0123456789")

    attr = pyfuse3.EntryAttributes()
    attr.st_size = 4
    result = await fs.setattr(f.inode, attr, _fields(update_size=True),
                              pyfuse3.FileHandleT(fh), ctx)

    assert result.st_size == 4
    assert f.attr.st_size == 4
    # The open writable handle now serves the truncated content.
    assert await fs.read(fh, 0, 100) == b"0123"


@pytest.mark.trio
async def test_setattr_ftruncate_marks_dirty_and_uploads_on_release(
    fs, inode_manager, uc_client, metadata_manager, ctx
):
    _vol, f = _make_vol_file(inode_manager, size=0)
    metadata_manager.invalidate = MagicMock()
    file_info = await fs.open(f.inode, 1, ctx)  # O_WRONLY
    fh = int(file_info.fh)
    await fs.write(fh, offset=0, buffer=b"0123456789")
    attr = pyfuse3.EntryAttributes()
    attr.st_size = 4
    await fs.setattr(f.inode, attr, _fields(update_size=True),
                     pyfuse3.FileHandleT(fh), ctx)
    await fs.release(pyfuse3.FileHandleT(fh))
    uc_client.upload_file.assert_awaited_once()
    assert uc_client.upload_file.call_args.args[0] == "/Volumes/cat/sch/vol/data.txt"


@pytest.mark.trio
async def test_setattr_path_truncate_downloads_resizes_uploads(
    fs, inode_manager, uc_client, metadata_manager, data_manager, ctx
):
    _vol, f = _make_vol_file(inode_manager, size=5)  # read -> b"hello"
    metadata_manager.invalidate = MagicMock()
    attr = pyfuse3.EntryAttributes()
    attr.st_size = 3
    # No fh -> path-based truncate: download, resize, upload immediately.
    result = await fs.setattr(f.inode, attr, _fields(update_size=True), None, ctx)
    uc_client.upload_file.assert_awaited_once()
    assert uc_client.upload_file.call_args.args[0] == "/Volumes/cat/sch/vol/data.txt"
    assert result.st_size == 3
    assert f.attr.st_size == 3
    metadata_manager.invalidate.assert_called_once_with("/cat/sch/vol/data.txt", is_dir=False)


@pytest.mark.trio
async def test_setattr_path_truncate_with_open_writer_uploads_real_data(
    fs, inode_manager, uc_client, metadata_manager, ctx
):
    """WSL/9P regression: a path-based setattr(size) arriving while a writable
    handle is open must resize that handle's buffer, not read the (empty)
    remote object and upload a zero-filled file of the right size."""
    metadata_manager.invalidate = MagicMock()
    _vol, f = _make_vol_file(inode_manager, size=0)  # brand-new file, empty remote

    uploaded = {}

    async def _capture(uc_path, local_path, *, ctx):
        with open(local_path, "rb") as fh_:
            uploaded["bytes"] = fh_.read()

    uc_client.upload_file.side_effect = _capture

    file_info = await fs.open(f.inode, 1, ctx)  # O_WRONLY
    fh = int(file_info.fh)
    await fs.write(fh, offset=0, buffer=b"0123456789")

    # Path-based setattr (fh=None) -- the WSL SetEndOfFile pattern.
    attr = pyfuse3.EntryAttributes()
    attr.st_size = 10
    await fs.setattr(f.inode, attr, _fields(update_size=True), None, ctx)

    # The path-based branch must NOT have uploaded a zero-filled file here.
    uc_client.upload_file.assert_not_awaited()

    await fs.release(pyfuse3.FileHandleT(fh))

    uc_client.upload_file.assert_awaited_once()
    assert uploaded["bytes"] == b"0123456789"  # real data, not b"\x00" * 10


@pytest.mark.trio
async def test_setattr_truncate_read_only_raises_erofs(fs_ro, inode_manager, ctx):
    _vol, f = _make_vol_file(inode_manager, size=5)
    attr = pyfuse3.EntryAttributes()
    attr.st_size = 0
    with pytest.raises(pyfuse3.FUSEError) as exc:
        await fs_ro.setattr(f.inode, attr, _fields(update_size=True), None, ctx)
    assert exc.value.errno == errno.EROFS


@pytest.mark.trio
async def test_setattr_chmod_chown_noop_updates_inode_only(
    fs, inode_manager, uc_client, ctx
):
    _vol, f = _make_vol_file(inode_manager, size=5)
    original_type = stat.S_IFMT(f.attr.st_mode)
    attr = pyfuse3.EntryAttributes()
    attr.st_mode = stat.S_IFREG | 0o600
    attr.st_uid = 4242
    attr.st_gid = 4343
    result = await fs.setattr(
        f.inode, attr, _fields(update_mode=True, update_uid=True, update_gid=True),
        None, ctx,
    )
    # No remote calls for metadata-only changes.
    uc_client.upload_file.assert_not_awaited()
    # Permission bits applied; file-type bits preserved.
    assert result.st_mode & 0o777 == 0o600
    assert stat.S_IFMT(result.st_mode) == original_type
    assert result.st_uid == 4242
    assert result.st_gid == 4343


@pytest.mark.trio
async def test_setattr_utimes_noop_updates_mtime(fs, inode_manager, uc_client, ctx):
    _vol, f = _make_vol_file(inode_manager, size=5)
    attr = pyfuse3.EntryAttributes()
    attr.st_mtime_ns = 1_500_000_000 * 10**9
    result = await fs.setattr(f.inode, attr, _fields(update_mtime=True), None, ctx)
    uc_client.upload_file.assert_not_awaited()
    assert result.st_mtime_ns == 1_500_000_000 * 10**9


@pytest.mark.trio
async def test_setattr_missing_inode_raises_enoent(fs, ctx):
    attr = pyfuse3.EntryAttributes()
    attr.st_size = 0
    with pytest.raises(pyfuse3.FUSEError) as exc:
        await fs.setattr(999999, attr, _fields(update_size=True), None, ctx)
    assert exc.value.errno == errno.ENOENT


# ---------------------------------------------------------------------------
# statfs (df)
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_statfs_reports_synthetic_capacity(fs, ctx):
    st = await fs.statfs(ctx)
    assert st.f_bsize == 4096
    assert st.f_frsize == 4096
    assert st.f_namemax == 255
    # Large synthetic total, fully available on a writable mount.
    assert st.f_blocks > 0
    assert st.f_bavail == st.f_blocks
    assert st.f_bfree == st.f_blocks
    assert st.f_files > 0
    assert st.f_favail == st.f_files
    assert st.f_ffree == st.f_files


@pytest.mark.trio
async def test_statfs_read_only_reports_zero_availability(fs_ro, ctx):
    st = await fs_ro.statfs(ctx)
    # Total size still shows, but nothing is available to write.
    assert st.f_blocks > 0
    assert st.f_bavail == 0
    assert st.f_bfree == 0
    assert st.f_favail == 0
    assert st.f_ffree == 0


# ---------------------------------------------------------------------------
# securable allow/deny filtering
# ---------------------------------------------------------------------------


def _make_cat_sch(inode_manager):
    """Build /cat/sch and return (cat, sch) entries."""
    cat = inode_manager.add_entry(pyfuse3.ROOT_INODE, "cat", attr=_make_attr(True))
    sch = inode_manager.add_entry(cat.inode, "sch", attr=_make_attr(True))
    return cat, sch


@pytest.mark.trio
async def test_lookup_denied_securable_raises_enoent(fs_builder, inode_manager, ctx):
    fs = fs_builder(SecurableFilter(denylist=["cat.sch.vol"]))
    _cat, sch = _make_cat_sch(inode_manager)
    with pytest.raises(pyfuse3.FUSEError) as exc:
        await fs.lookup(sch.inode, b"vol", ctx)
    assert exc.value.errno == errno.ENOENT


@pytest.mark.trio
async def test_lookup_allowlist_blocks_off_chain_but_permits_target(
    fs_builder, inode_manager, metadata_manager, ctx
):
    fs = fs_builder(SecurableFilter(allowlist=["cat.sch.vol"]))
    _cat, sch = _make_cat_sch(inode_manager)

    # Sibling off the allowed chain is hidden.
    with pytest.raises(pyfuse3.FUSEError) as exc:
        await fs.lookup(sch.inode, b"other_vol", ctx)
    assert exc.value.errno == errno.ENOENT

    # The allowlisted target resolves normally (lookup_child mock returns a file).
    attr = await fs.lookup(sch.inode, b"vol", ctx)
    assert attr is not None


@pytest.mark.trio
async def test_readdir_filters_denied_children(
    fs_builder, inode_manager, metadata_manager, ctx, monkeypatch
):
    fs = fs_builder(SecurableFilter(denylist=["cat.sch.deny_vol"]))
    _cat, sch = _make_cat_sch(inode_manager)
    metadata_manager.list_directory = AsyncMock(return_value={
        "ok_vol": _make_attr(True),
        "deny_vol": _make_attr(True),
    })
    calls = _capture_readdir_replies(monkeypatch)

    fh = await fs.opendir(sch.inode, ctx)
    await fs.readdir(fh, 0, token=object())

    names = [name for name, _next in calls]
    assert b"ok_vol" in names
    assert b"deny_vol" not in names


@pytest.mark.trio
async def test_readdir_allowlist_shows_only_on_chain(
    fs_builder, inode_manager, metadata_manager, ctx, monkeypatch
):
    fs = fs_builder(SecurableFilter(allowlist=["cat.sch.vol"]))
    _cat, sch = _make_cat_sch(inode_manager)
    metadata_manager.list_directory = AsyncMock(return_value={
        "vol": _make_attr(True),
        "other_vol": _make_attr(True),
    })
    calls = _capture_readdir_replies(monkeypatch)

    fh = await fs.opendir(sch.inode, ctx)
    await fs.readdir(fh, 0, token=object())

    names = [name for name, _next in calls]
    assert b"vol" in names
    assert b"other_vol" not in names


@pytest.mark.trio
async def test_create_denied_path_raises_eacces(
    fs_builder, inode_manager, uc_client, ctx
):
    fs = fs_builder(SecurableFilter(denylist=["cat.sch.vol.secret"]))
    cat = inode_manager.add_entry(pyfuse3.ROOT_INODE, "cat", attr=_make_attr(True))
    sch = inode_manager.add_entry(cat.inode, "sch", attr=_make_attr(True))
    vol = inode_manager.add_entry(sch.inode, "vol", attr=_make_attr(True))

    with pytest.raises(pyfuse3.FUSEError) as exc:
        await fs.create(vol.inode, b"secret", 0o644, 0, ctx)
    assert exc.value.errno == errno.EACCES

    # A non-denied sibling still creates fine.
    file_info, _attrs = await fs.create(vol.inode, b"ok.txt", 0o644, 0, ctx)
    assert int(file_info.fh) in fs._open_state


@pytest.mark.trio
async def test_mkdir_denied_path_raises_eacces(
    fs_builder, inode_manager, uc_client, ctx
):
    fs = fs_builder(SecurableFilter(denylist=["cat.sch.vol.private"]))
    cat = inode_manager.add_entry(pyfuse3.ROOT_INODE, "cat", attr=_make_attr(True))
    sch = inode_manager.add_entry(cat.inode, "sch", attr=_make_attr(True))
    vol = inode_manager.add_entry(sch.inode, "vol", attr=_make_attr(True))

    with pytest.raises(pyfuse3.FUSEError) as exc:
        await fs.mkdir(vol.inode, b"private", 0o755, ctx)
    assert exc.value.errno == errno.EACCES
    uc_client.create_directory.assert_not_awaited()


@pytest.mark.trio
async def test_rename_into_denied_path_raises_eacces(
    fs_builder, inode_manager, uc_client, metadata_manager, ctx
):
    fs = fs_builder(SecurableFilter(denylist=["cat.sch.vol.locked"]))
    cat = inode_manager.add_entry(pyfuse3.ROOT_INODE, "cat", attr=_make_attr(True))
    sch = inode_manager.add_entry(cat.inode, "sch", attr=_make_attr(True))
    vol = inode_manager.add_entry(sch.inode, "vol", attr=_make_attr(True))
    inode_manager.add_entry(vol.inode, "data.txt", attr=_make_attr(False, size=5))

    with pytest.raises(pyfuse3.FUSEError) as exc:
        await fs.rename(vol.inode, b"data.txt", vol.inode, b"locked", 0, ctx)
    assert exc.value.errno == errno.EACCES
    uc_client.upload_file.assert_not_awaited()


@pytest.mark.trio
async def test_no_filter_is_permissive(fs_builder, inode_manager, ctx):
    # No securable_filter -> default permissive -> lookup behaves normally.
    fs = fs_builder(None)
    _cat, sch = _make_cat_sch(inode_manager)
    attr = await fs.lookup(sch.inode, b"vol", ctx)
    assert attr is not None
