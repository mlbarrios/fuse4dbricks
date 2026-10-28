"""
Tests for fuse4dbricks.fs.write_buffer.WriteBuffer.

The critical invariant: after finalize(), the full content is readable from
self.path via a *separate* open() — this is what the upload path does. Writes
go through a buffered file object, so a write smaller than the buffer (~8 KB)
would be read back as an empty file and silently uploaded as zero bytes unless
the handle is flushed/closed first.

All WriteBuffer methods are async (file I/O is offloaded via
trio.to_thread.run_sync; see docs/performance/P12-analysis.md), so every test
here runs under trio.
"""

import os

import pytest

from fuse4dbricks.fs.write_buffer import WriteBuffer


@pytest.fixture
def writes_dir(tmp_path):
    d = tmp_path / "writes"
    d.mkdir()
    return str(d)


@pytest.mark.trio
@pytest.mark.parametrize(
    "data",
    [
        b"",                         # empty
        b"hello world",             # 11 bytes, well under the buffer
        bytes(range(256)) * 8,      # 2048 bytes, still under the default ~8 KB buffer
        b"x" * 100_000,             # larger than the buffer
    ],
)
async def test_finalize_makes_full_content_readable_from_path(writes_dir, data):
    """After finalize(), a fresh open(path) (as the upload does) sees every byte.

    This is the regression guard for the zero-byte-upload bug: small writes
    must not be lost in the buffered write handle.
    """
    wb = await WriteBuffer.create(writes_dir)
    await wb.write(0, data)
    assert wb.size() == len(data)

    await wb.finalize()

    with open(wb.path, "rb") as f:
        on_disk = f.read()
    assert on_disk == data

    await wb.close()


@pytest.mark.trio
@pytest.mark.parametrize(
    "data",
    [
        b"",
        b"hello world",              # well under the buffer
        bytes(range(256)) * 8,       # 2048 bytes, under the default ~8 KB buffer
        b"x" * 100_000,              # larger than the buffer
    ],
)
async def test_flush_to_disk_makes_full_content_readable_from_path(writes_dir, data):
    """After flush_to_disk(), a fresh open(path) (as the upload does) sees every
    byte -- same guarantee as finalize() but without closing the handle, which
    is what the flush()-time upload relies on."""
    wb = await WriteBuffer.create(writes_dir)
    await wb.write(0, data)

    await wb.flush_to_disk()

    with open(wb.path, "rb") as f:
        assert f.read() == data
    await wb.close()


@pytest.mark.trio
async def test_flush_to_disk_keeps_handle_writable(writes_dir):
    """Unlike finalize(), flush_to_disk() leaves the handle open so further
    writes succeed -- flush() may be delivered more than once for one open file
    (e.g. a duplicated fd) with writes in between."""
    wb = await WriteBuffer.create(writes_dir)
    await wb.write(0, b"first")
    await wb.flush_to_disk()
    with open(wb.path, "rb") as f:
        assert f.read() == b"first"

    # Handle still usable: append more, flush again, full content visible.
    await wb.write(5, b"-second")
    await wb.flush_to_disk()
    with open(wb.path, "rb") as f:
        assert f.read() == b"first-second"
    await wb.close()


@pytest.mark.trio
async def test_flush_to_disk_is_idempotent_and_safe_after_close(writes_dir):
    wb = await WriteBuffer.create(writes_dir)
    await wb.write(0, b"data")
    await wb.flush_to_disk()
    await wb.flush_to_disk()  # must not raise
    await wb.finalize()
    await wb.flush_to_disk()  # no-op once closed; must not raise
    await wb.close()


@pytest.mark.trio
async def test_finalize_is_idempotent(writes_dir):
    wb = await WriteBuffer.create(writes_dir)
    await wb.write(0, b"data")
    await wb.finalize()
    await wb.finalize()  # must not raise
    with open(wb.path, "rb") as f:
        assert f.read() == b"data"
    await wb.close()


@pytest.mark.trio
async def test_close_deletes_tempfile(writes_dir):
    wb = await WriteBuffer.create(writes_dir)
    await wb.write(0, b"data")
    path = wb.path
    assert os.path.exists(path)
    await wb.close()
    assert not os.path.exists(path)


@pytest.mark.trio
async def test_close_after_finalize_still_deletes(writes_dir):
    wb = await WriteBuffer.create(writes_dir)
    await wb.write(0, b"data")
    path = wb.path
    await wb.finalize()
    assert os.path.exists(path)
    await wb.close()
    assert not os.path.exists(path)


@pytest.mark.trio
async def test_partial_overwrite_preserves_tail_on_disk(writes_dir):
    """Mirror of the FUSE partial-write case at the buffer level: overwriting
    the leading bytes must leave the rest intact in the uploaded file."""
    wb = await WriteBuffer.create(writes_dir)
    await wb.write(0, b"0123456789")
    await wb.write(0, b"AAA")
    await wb.finalize()
    with open(wb.path, "rb") as f:
        assert f.read() == b"AAA3456789"
    await wb.close()


@pytest.mark.trio
async def test_truncate_shrinks(writes_dir):
    wb = await WriteBuffer.create(writes_dir)
    await wb.write(0, b"0123456789")
    await wb.truncate(4)
    assert wb.size() == 4
    await wb.finalize()
    with open(wb.path, "rb") as f:
        assert f.read() == b"0123"
    await wb.close()


@pytest.mark.trio
async def test_truncate_grows_with_zeros(writes_dir):
    wb = await WriteBuffer.create(writes_dir)
    await wb.write(0, b"abc")
    await wb.truncate(6)
    assert wb.size() == 6
    await wb.finalize()
    with open(wb.path, "rb") as f:
        assert f.read() == b"abc\x00\x00\x00"
    await wb.close()


@pytest.mark.trio
async def test_truncate_to_zero(writes_dir):
    wb = await WriteBuffer.create(writes_dir)
    await wb.write(0, b"data")
    await wb.truncate(0)
    assert wb.size() == 0
    await wb.finalize()
    with open(wb.path, "rb") as f:
        assert f.read() == b""
    await wb.close()
