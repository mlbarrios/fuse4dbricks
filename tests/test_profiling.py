"""
Tests for fuse4dbricks.perf.profiling.

These tests exercise the module's *enabled* code path directly (bypassing the
module-level ENABLED flag, which is fixed at import time from the
ENABLE_PERF_PROFILING environment variable) by constructing Span/_Registry
objects explicitly, so they don't need to fork a subprocess with the env var
set just to test the real (non-no-op) logic.
"""

import json
import logging

import pytest

from fuse4dbricks.perf import profiling


@pytest.fixture(autouse=True)
def _fresh_registry():
    """Each test gets a clean set of aggregates."""
    profiling.registry.reset()
    yield
    profiling.registry.reset()


def test_disabled_by_default():
    """Without ENABLE_PERF_PROFILING set, the module must come up disabled."""
    assert profiling.ENABLED is False


def test_start_returns_noop_span_when_disabled():
    span = profiling.start("read", path="/a/b", category="fuse_read")
    assert isinstance(span, profiling._NoopSpan)
    # Must not raise, and must not touch the registry.
    span.finish(status="success", nbytes=1234)
    assert profiling.registry.operations_snapshot() == {}


def test_cache_event_and_eviction_are_noop_when_disabled():
    profiling.cache_event("ram", hit=True)
    profiling.cache_eviction("disk")
    assert profiling.registry.cache_snapshot() == {}


def test_summaries_are_noop_when_disabled(caplog):
    with caplog.at_level(logging.INFO, logger="fuse4dbricks.perf"):
        profiling.cache_summary()
        profiling.metadata_summary()
        profiling.transfer_summary("/a/b", "download", 1024, 1.0)
    assert caplog.records == []


def test_real_span_records_to_registry_and_emits_json(caplog):
    with caplog.at_level(logging.INFO, logger="fuse4dbricks.perf"):
        span = profiling.Span("read", "/cat/sch/vol/f.bin", "fuse_read")
        span.finish(status="success", nbytes=8_388_608)

    snapshot = profiling.registry.operations_snapshot()
    assert snapshot["read"]["count"] == 1
    assert snapshot["read"]["bytes"] == 8_388_608
    assert snapshot["read"]["errors"] == 0

    assert len(caplog.records) == 1
    event = json.loads(caplog.records[0].message)
    assert event["schema"] == "span.v1"
    assert event["operation"] == "read"
    assert event["path"] == "/cat/sch/vol/f.bin"
    assert event["category"] == "fuse_read"
    assert event["bytes"] == 8_388_608
    assert event["status"] == "success"
    assert isinstance(event["duration_ms"], float)


def test_real_span_records_error_status():
    span = profiling.Span("write", "/f", "fuse_write")
    span.finish(status="error", nbytes=0)
    snapshot = profiling.registry.operations_snapshot()
    assert snapshot["write"]["errors"] == 1
    assert snapshot["write"]["count"] == 1


def test_percentiles_and_averages():
    for duration_ms in (10.0, 20.0, 30.0, 40.0, 50.0):
        profiling.registry.record("getattr", duration_ms, 0, "success")
    snap = profiling.registry.operations_snapshot()["getattr"]
    assert snap["count"] == 5
    assert snap["cumulative_ms"] == pytest.approx(150.0)
    assert snap["avg_ms"] == pytest.approx(30.0)
    # p95/p99 fall on the slowest sample for this small a population.
    assert snap["p95_ms"] == pytest.approx(50.0)
    assert snap["p99_ms"] == pytest.approx(50.0)


def test_cache_hit_ratio():
    profiling.registry.record_cache("ram", hit=True)
    profiling.registry.record_cache("ram", hit=True)
    profiling.registry.record_cache("ram", hit=False)
    snap = profiling.registry.cache_snapshot()["ram"]
    assert snap["hits"] == 2
    assert snap["misses"] == 1
    assert snap["hit_ratio"] == pytest.approx(2 / 3, abs=1e-4)


def test_cache_eviction_counter():
    profiling.registry.record_eviction("disk")
    profiling.registry.record_eviction("disk", count=3)
    snap = profiling.registry.cache_snapshot()["disk"]
    assert snap["evictions"] == 4


def test_transfer_summary_computes_throughput(caplog):
    with caplog.at_level(logging.INFO, logger="fuse4dbricks.perf"):
        # 100 MB in 1 second -> 100 MB/s.
        profiling.ENABLED, prev = True, profiling.ENABLED
        try:
            profiling.transfer_summary("/f", "upload", 100 * 1024 * 1024, 1.0)
        finally:
            profiling.ENABLED = prev

    assert len(caplog.records) == 1
    event = json.loads(caplog.records[0].message)
    assert event["schema"] == "transfer_summary.v1"
    assert event["direction"] == "upload"
    assert event["avg_throughput_mb_s"] == pytest.approx(100.0)
