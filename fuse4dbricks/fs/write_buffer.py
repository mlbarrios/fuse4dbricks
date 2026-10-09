"""
Tempfile-backed write buffer for a single open writable file handle.
"""

import os
import tempfile

import trio


class WriteBuffer:
    """
    Tempfile-backed write buffer for a single open writable file handle.

    Uses a named tempfile in {cache_dir}/writes/ so memory usage stays flat
    regardless of file size — a 50 GB CRAM costs the same RAM as a 1 KB file.
    The buffer also supports read() so O_RDWR handles can serve reads from the
    local copy (which reflects in-progress writes) rather than from DataManager.

    All file I/O is offloaded via ``trio.to_thread.run_sync``, the same pattern
    ``storage/persistence.py`` uses for its own disk operations: every FUSE
    request in this process runs as a cooperatively-scheduled trio task on a
    single OS thread, so an un-awaited blocking call here would stall every
    other concurrent request in the mount, not just the file being written
    (see docs/performance/P12-analysis.md).
    """

    def __init__(self, tmp: "tempfile._TemporaryFileWrapper", size: int):
        """Not awaitable; use :meth:`create` to construct an instance."""
        self._tmp = tmp
        self._size = size

    @classmethod
    async def create(cls, writes_dir: str, initial_data: bytes = b"") -> "WriteBuffer":
        tmp = await trio.to_thread.run_sync(
            lambda: tempfile.NamedTemporaryFile(
                delete=False, prefix="fuse4dbricks_write_", dir=writes_dir
            )
        )
        size = 0
        if initial_data:
            await trio.to_thread.run_sync(tmp.write, initial_data)
            size = len(initial_data)
        return cls(tmp, size)

    def _write_at(self, offset: int, data: bytes) -> None:
        self._tmp.seek(offset)
        self._tmp.write(data)

    def _read_at(self, offset: int, length: int) -> bytes:
        self._tmp.seek(offset)
        return self._tmp.read(length)

    async def write(self, offset: int, data: bytes) -> int:
        await trio.to_thread.run_sync(self._write_at, offset, data)
        end = offset + len(data)
        if end > self._size:
            self._size = end
        return len(data)

    async def read(self, offset: int, length: int) -> bytes:
        return await trio.to_thread.run_sync(self._read_at, offset, length)

    async def truncate(self, size: int) -> None:
        """Resize the buffer to exactly ``size`` bytes.

        Shrinks (dropping the tail) or grows (zero-filling, POSIX hole
        semantics) as needed. ``file.truncate`` flushes the buffered writer
        before resizing, so a subsequent ``read``/``finalize`` sees the new
        length.
        """
        await trio.to_thread.run_sync(self._tmp.truncate, size)
        self._size = size

    async def flush_to_disk(self) -> None:
        """Push buffered writes to the OS so a separate reader sees them.

        Like ``finalize()`` this makes the full content visible to the upload's
        own ``open(self.path)``, but it keeps the write handle open so ``read()``
        and ``write()`` may continue afterwards. Use this when the upload happens
        on ``flush`` (the ``close()`` syscall), which the kernel may deliver more
        than once for a single open file (e.g. a duplicated fd) with further
        writes in between. Idempotent; a no-op once the handle is closed.
        """
        if not self._tmp.closed:
            await trio.to_thread.run_sync(self._tmp.flush)

    async def finalize(self) -> None:
        """Flush and close the write handle, keeping the file on disk.

        Writes go through a buffered file object, so data may sit in Python's
        in-memory buffer (~8 KB) rather than on disk. The upload path opens
        ``self.path`` with a *separate* handle and streams it; without closing
        the write handle first, a write smaller than the buffer would be read
        back as an empty file and silently uploaded as zero bytes. Closing the
        handle flushes the buffer to disk (and frees the fd for the duration of
        the upload) while ``self.path`` stays valid. Idempotent; ``read()`` and
        ``write()`` must not be called afterwards.
        """
        if not self._tmp.closed:
            await trio.to_thread.run_sync(self._tmp.close)

    def size(self) -> int:
        return self._size

    @property
    def path(self) -> str:
        return self._tmp.name

    async def close(self) -> None:
        """Close the handle (if still open) and delete the tempfile. Always
        call this, even on error. Safe to call after ``finalize()``."""
        try:
            if not self._tmp.closed:
                await trio.to_thread.run_sync(self._tmp.close)
        finally:
            try:
                await trio.to_thread.run_sync(os.unlink, self._tmp.name)
            except OSError:
                pass
