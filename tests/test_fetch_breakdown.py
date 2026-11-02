"""
End-to-end tests for the remote_fetch phase breakdown: real
DataManager._fetch_chunk -> real UnityCatalogClient.download_chunk_stream
(httpx mocked with controlled delays) -> real DiskPersistence on tmp_path.
"""

import json
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
import trio

from fuse4dbricks.api.uc_client import UnityCatalogClient
from fuse4dbricks.fs.data_manager import DataManager, _ChunkRequest
from fuse4dbricks.perf import profiling
from fuse4dbricks.storage.persistence import DiskPersistence

TTFB_S = 0.05
GAP_S = 0.01
N_PIECES = 4
PIECE = b"x" * 1024

EXCLUSIVE_PHASES = {
    "disk_prepare",
    "download_request_latency",
    "download_ttfb",
    "download_stream_net_wait",
    "disk_write_time",
    "download_response_close",
    "disk_close",
    "cache_finalize",
    "chunk_materialize",
}
DETAIL = {
    "download_stream_transfer",
    "download_consumer_time",
    "cache_rename",
    "cache_evict",
    "cache_lock_wait",
    "cache_metadata_update",
}


@pytest.fixture(autouse=True)
def _fresh_registry():
    profiling.registry.reset()
    yield
    profiling.registry.reset()


@pytest.fixture
def enabled(monkeypatch):
    monkeypatch.setattr(profiling, "ENABLED", True)
    monkeypatch.setattr(profiling, "now_ms", profiling._clock_ms)


def _make_manager(tmp_path) -> DataManager:
    auth = MagicMock()
    auth.get_access_token = AsyncMock(return_value="tok")
    uc = UnityCatalogClient("https://test-workspace.net", auth)
    uc.client = AsyncMock(spec=httpx.AsyncClient)
    uc.client.build_request.return_value = MagicMock(spec=httpx.Request)

    response = AsyncMock(spec=httpx.Response)
    response.status_code = 206

    async def aiter_bytes(chunk_size=None):
        await trio.sleep(TTFB_S)
        for i in range(N_PIECES):
            if i:
                await trio.sleep(GAP_S)
            yield PIECE

    response.aiter_bytes = aiter_bytes
    uc.client.send.return_value = response

    persistence = DiskPersistence(str(tmp_path / "cache"), max_size_gb=0.01, max_age_days=7)
    return DataManager(uc_client=uc, persistence=persistence, ram_cache_mb=1, num_workers=1)


def _request(dm: DataManager) -> _ChunkRequest:
    return _ChunkRequest(
        fs_path="/Volumes/c/s/v/f.bin", chunk_id=0, mtime=0.0, gen=0,
        chunk_size=dm.chunk_size, ctx=SimpleNamespace(uid=0, pid=1, gid=0),
    )


def _events(caplog, schema):
    out = []
    for r in caplog.records:
        if r.name != "fuse4dbricks.perf":
            continue
        ev = json.loads(r.getMessage())
        if ev.get("schema") == schema:
            out.append(ev)
    return out


@pytest.mark.trio
async def test_remote_fetch_breakdown_accounts_for_total(enabled, tmp_path, caplog):
    dm = _make_manager(tmp_path)
    with caplog.at_level(logging.INFO, logger="fuse4dbricks.perf"):
        data = await dm._fetch_chunk(_request(dm))
    assert data == PIECE * N_PIECES

    (bd,) = _events(caplog, "breakdown.v1")
    assert bd["operation"] == "remote_fetch"
    assert bd["status"] == "success"
    assert bd["bytes"] == len(PIECE) * N_PIECES
    assert bd["chunk_id"] == 0
    assert set(bd["phases"]) == EXCLUSIVE_PHASES
    assert set(bd["detail"]) == DETAIL
    assert bd["net_chunks"] == N_PIECES
    assert bd["disk_writes"] == N_PIECES
    assert bd["http_status"] == 206

    # Controlled delays land in the right phases.
    assert bd["phases"]["download_ttfb"] >= TTFB_S * 1000 * 0.9
    assert bd["phases"]["download_stream_net_wait"] >= (N_PIECES - 1) * GAP_S * 1000 * 0.9
    assert bd["detail"]["download_stream_transfer"] >= (N_PIECES - 1) * GAP_S * 1000 * 0.9

    # Exclusive phases + residual reconstruct the total, and the residual is
    # a small fraction of it (phases don't overlap and nothing big is missed).
    assert sum(bd["phases"].values()) + bd["residual_ms"] == pytest.approx(bd["total_ms"], abs=0.01)
    assert 0 <= bd["residual_ms"] < 0.2 * bd["total_ms"]

    # Per-phase JSON lines are folded into the single breakdown event...
    assert not [e for e in _events(caplog, "span.v1") if e["operation"] in EXCLUSIVE_PHASES]
    # ...but every phase still feeds the aggregate summary.
    ops = profiling.registry.operations_snapshot()
    for name in EXCLUSIVE_PHASES | DETAIL | {"remote_fetch", "remote_fetch_residual"}:
        assert ops[name]["count"] == 1, name


@pytest.mark.trio
async def test_phases_do_not_double_count_disk_and_network(enabled, tmp_path, caplog, monkeypatch):
    """Slow disk writes and slow network by distinct known amounts: each delay
    must land in exactly one exclusive phase. If download_stream_net_wait
    included the consumer's write time (the original download_chunk_stream
    span did), it would exceed the injected network gaps by ~N*WRITE_S."""
    write_s = 0.02
    real_open = trio.open_file

    class _SlowFile:
        def __init__(self, f):
            self._f = f

        async def write(self, b):
            await trio.sleep(write_s)
            return await self._f.write(b)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            await self._f.aclose()

    async def slow_open(*a, **k):
        return _SlowFile(await real_open(*a, **k))

    monkeypatch.setattr(trio, "open_file", slow_open)
    dm = _make_manager(tmp_path)
    with caplog.at_level(logging.INFO, logger="fuse4dbricks.perf"):
        await dm._fetch_chunk(_request(dm))

    (bd,) = _events(caplog, "breakdown.v1")
    ph, det = bd["phases"], bd["detail"]
    write_ms, gaps_ms = N_PIECES * write_s * 1000, (N_PIECES - 1) * GAP_S * 1000

    assert ph["disk_write_time"] >= write_ms * 0.9
    assert gaps_ms * 0.9 <= ph["download_stream_net_wait"] < gaps_ms + write_ms * 0.5
    assert ph["download_ttfb"] < TTFB_S * 1000 + write_ms * 0.5
    # Consumer time (overlapping detail) contains the writes, not the network.
    assert det["download_consumer_time"] >= ph["disk_write_time"]
    assert det["download_consumer_time"] < write_ms + gaps_ms * 0.5
    # The legacy span = ttfb + net_wait + consumer time (i.e. includes disk).
    (stream_span,) = [e for e in _events(caplog, "span.v1") if e["operation"] == "download_chunk_stream"]
    assert stream_span["duration_ms"] == pytest.approx(
        ph["download_ttfb"] + ph["download_stream_net_wait"] + det["download_consumer_time"], abs=2.0
    )
    assert bd["residual_ms"] >= 0
    assert sum(ph.values()) <= bd["total_ms"]


@pytest.mark.trio
async def test_breakdown_is_noop_when_disabled(tmp_path, caplog):
    assert profiling.ENABLED is False
    dm = _make_manager(tmp_path)
    with caplog.at_level(logging.INFO, logger="fuse4dbricks.perf"):
        data = await dm._fetch_chunk(_request(dm))
    assert data == PIECE * N_PIECES
    assert [r for r in caplog.records if r.name == "fuse4dbricks.perf"] == []
    assert profiling.registry.operations_snapshot() == {}
    assert profiling.now_ms() == 0.0


def test_record_phase_without_breakdown_emits_span(enabled, caplog):
    with caplog.at_level(logging.INFO, logger="fuse4dbricks.perf"):
        profiling.record_phase("disk_write_time", 12.5, path="/p", category="cache", nbytes=7)
    (ev,) = _events(caplog, "span.v1")
    assert ev["operation"] == "disk_write_time"
    assert ev["duration_ms"] == 12.5
    assert ev["bytes"] == 7
