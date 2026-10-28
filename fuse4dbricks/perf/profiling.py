"""
Lightweight, opt-in performance profiling for Fuse4Databricks.

Disabled by default. Enable with:

    ENABLE_PERF_PROFILING=true

Design (see docs/performance/profiling-strategy.md):
  - Near-zero overhead when disabled: the enabled/disabled choice is made once
    at import time -- ``start()`` returns a shared no-op handle whose
    ``finish()`` is a single no-op call, with no clock read, no allocation,
    and no JSON/logging work -- rather than re-checking a flag on every call.
  - When enabled, every instrumented operation emits one structured JSON log
    line (operation, path, bytes, duration_ms, status) via the dedicated
    "fuse4dbricks.perf" logger, kept separate (non-propagating, own handler)
    from the application's regular logger so profiling output is never
    interleaved with normal log lines. In-memory aggregates feed periodic
    cache/metadata/transfer summaries.
"""

from __future__ import annotations

import json
import logging
import os
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field

_LOGGER_NAME = "fuse4dbricks.perf"

ENABLED = os.environ.get("ENABLE_PERF_PROFILING", "").strip().lower() in (
    "1", "true", "yes", "on",
)


def _make_logger() -> logging.Logger:
    logger = logging.getLogger(_LOGGER_NAME)
    logger.setLevel(logging.INFO)
    # Independent of main.setup_logging()'s root handler: profiling output is
    # structured JSON and must not be interleaved with/prefixed by the regular
    # text log format, and must not be silently dropped if the regular logger
    # is configured at a level above INFO.
    logger.propagate = False
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    return logger


_perf_logger = _make_logger() if ENABLED else logging.getLogger(_LOGGER_NAME)


def _emit(event: dict) -> None:
    _perf_logger.info(json.dumps(event, separators=(",", ":")))


@dataclass
class _Stat:
    """Aggregate for one named operation. Samples are capped (bounded deque)
    so percentile computation never costs unbounded memory on a long-running
    mount; percentiles are only computed when a summary is emitted, never on
    the hot path."""

    count: int = 0
    total_ms: float = 0.0
    total_bytes: int = 0
    errors: int = 0
    samples_ms: deque = field(default_factory=lambda: deque(maxlen=2000))

    def record(self, duration_ms: float, nbytes: int, status: str) -> None:
        self.count += 1
        self.total_ms += duration_ms
        self.total_bytes += nbytes
        if status != "success":
            self.errors += 1
        self.samples_ms.append(duration_ms)

    def percentile(self, p: float) -> float:
        if not self.samples_ms:
            return 0.0
        ordered = sorted(self.samples_ms)
        idx = min(len(ordered) - 1, int(len(ordered) * p))
        return ordered[idx]

    def to_dict(self) -> dict:
        avg = self.total_ms / self.count if self.count else 0.0
        return {
            "count": self.count,
            "avg_ms": round(avg, 3),
            "p95_ms": round(self.percentile(0.95), 3),
            "p99_ms": round(self.percentile(0.99), 3),
            "cumulative_ms": round(self.total_ms, 3),
            "bytes": self.total_bytes,
            "errors": self.errors,
        }


class _Registry:
    """Process-wide in-memory aggregates. Only ever touched when profiling is
    enabled; every update is a plain dict/deque mutation (no I/O)."""

    def __init__(self) -> None:
        self._stats: dict[str, _Stat] = defaultdict(_Stat)
        self._cache: dict[str, dict[str, int]] = defaultdict(
            lambda: {"hits": 0, "misses": 0, "evictions": 0}
        )
        self._retries: dict[str, dict] = defaultdict(
            lambda: {"count": 0, "cumulative_delay_s": 0.0, "reasons": defaultdict(int)}
        )

    def record(self, name: str, duration_ms: float, nbytes: int, status: str) -> None:
        self._stats[name].record(duration_ms, nbytes, status)

    def record_cache(self, tier: str, hit: bool) -> None:
        self._cache[tier]["hits" if hit else "misses"] += 1

    def record_eviction(self, tier: str, count: int = 1) -> None:
        self._cache[tier]["evictions"] += count

    def record_retry(self, scope: str, reason: str, delay_s: float) -> None:
        r = self._retries[scope]
        r["count"] += 1
        r["cumulative_delay_s"] += delay_s
        r["reasons"][reason] += 1

    def operations_snapshot(self) -> dict:
        return {name: stat.to_dict() for name, stat in self._stats.items()}

    def cache_snapshot(self) -> dict:
        snap = {}
        for tier, c in self._cache.items():
            total = c["hits"] + c["misses"]
            snap[tier] = {**c, "hit_ratio": round(c["hits"] / total, 4) if total else 0.0}
        return snap

    def retry_snapshot(self) -> dict:
        snap = {}
        for scope, r in self._retries.items():
            snap[scope] = {
                "count": r["count"],
                "cumulative_delay_s": round(r["cumulative_delay_s"], 3),
                "reasons": dict(r["reasons"]),
            }
        return snap

    def reset(self) -> None:
        self._stats.clear()
        self._cache.clear()
        self._retries.clear()


registry = _Registry()


class Span:
    """Handle returned by :func:`start` when profiling is enabled. Call
    :meth:`finish` exactly once, from every exit path (success and error)."""

    __slots__ = ("_start_ms", "category", "operation", "path")

    def __init__(self, operation: str, path: str | None, category: str):
        self.operation = operation
        self.path = path
        self.category = category
        self._start_ms = time.monotonic() * 1000.0

    def finish(self, *, status: str = "success", nbytes: int = 0, **extra) -> None:
        duration_ms = time.monotonic() * 1000.0 - self._start_ms
        registry.record(self.operation, duration_ms, nbytes, status)
        event = {
            "schema": "span.v1",
            "ts": time.time(),
            "category": self.category,
            "operation": self.operation,
            "path": self.path,
            "bytes": nbytes,
            "duration_ms": round(duration_ms, 3),
            "status": status,
        }
        if extra:
            event.update(extra)
        _emit(event)


class _NoopSpan:
    """Returned by :func:`start` when profiling is disabled. ``finish`` is a
    single attribute-lookup-and-call with no clock read, no allocation (one
    shared singleton), no JSON, no I/O."""

    __slots__ = ()

    def finish(self, *, status: str = "success", nbytes: int = 0, **extra) -> None:
        return None


_NOOP_SPAN = _NoopSpan()


def start(operation: str, path: str | None = None, category: str = "fuse") -> Span | _NoopSpan:
    """Begin timing ``operation``. Always call ``.finish()`` on the result,
    exactly once, from every exit path (success and error)."""
    if not ENABLED:
        return _NOOP_SPAN
    return Span(operation, path, category)


def cache_event(tier: str, hit: bool) -> None:
    """Record a cache lookup outcome (e.g. ``tier="ram"`` or ``tier="disk"``)."""
    if ENABLED:
        registry.record_cache(tier, hit)


def cache_eviction(tier: str, count: int = 1) -> None:
    if ENABLED:
        registry.record_eviction(tier, count)


def record_duration(
    operation: str, duration_ms: float, *, path: str | None = None,
    category: str = "fuse", nbytes: int = 0, status: str = "success", **extra,
) -> None:
    """Record a duration measured externally -- e.g. the gap between a
    timestamp stamped in one trio task (enqueue time) and a later moment
    observed in a different task (dequeue time) -- rather than timing it live
    via :func:`start`/``Span.finish``. No-op when profiling is disabled."""
    if not ENABLED:
        return
    registry.record(operation, duration_ms, nbytes, status)
    event = {
        "schema": "span.v1",
        "ts": time.time(),
        "category": category,
        "operation": operation,
        "path": path,
        "bytes": nbytes,
        "duration_ms": round(duration_ms, 3),
        "status": status,
    }
    if extra:
        event.update(extra)
    _emit(event)


def record_retry(scope: str, reason: str, delay_s: float, *, path: str | None = None) -> None:
    """Record one retry/backoff attempt (e.g. inside ``_with_retry`` or
    ``_request``'s own retry loop). ``scope`` identifies which retry loop
    this came from (e.g. "http_request" vs "with_retry"), so cumulative
    retry time can be attributed to the right layer."""
    if not ENABLED:
        return
    registry.record_retry(scope, reason, delay_s)
    _emit({
        "schema": "retry.v1",
        "ts": time.time(),
        "scope": scope,
        "reason": reason,
        "path": path,
        "delay_s": round(delay_s, 3),
    })


def cache_summary() -> None:
    """Emit a ``cache_summary.v1`` JSON line from current aggregates."""
    if not ENABLED:
        return
    _emit({"schema": "cache_summary.v1", "ts": time.time(), "cache": registry.cache_snapshot()})


def metadata_summary() -> None:
    """Emit a ``metadata_summary.v1`` JSON line for getattr/lookup/readdir/open."""
    if not ENABLED:
        return
    ops = registry.operations_snapshot()
    metadata_ops = {k: v for k, v in ops.items() if k in ("getattr", "lookup", "readdir", "open")}
    _emit({"schema": "metadata_summary.v1", "ts": time.time(), "operations": metadata_ops})


def operations_summary() -> None:
    """Emit an ``operations_summary.v1`` JSON line: cumulative stats for
    *every* instrumented operation (read, write, getattr, lookup, readdir,
    upload_file, download_chunk_stream, data_manager_read, and every internal
    phase-level operation), so benchmark analysis can rank top runtime
    contributors without guessing which names to filter for."""
    if not ENABLED:
        return
    _emit({"schema": "operations_summary.v1", "ts": time.time(), "operations": registry.operations_snapshot()})


def retry_summary() -> None:
    """Emit a ``retry_summary.v1`` JSON line: retry count/cumulative delay per
    scope (``http_request``, ``with_retry``), answering how much benchmark
    time went to waiting on retries/backoff."""
    if not ENABLED:
        return
    _emit({"schema": "retry_summary.v1", "ts": time.time(), "retries": registry.retry_snapshot()})


def transfer_summary(path: str, direction: str, size_bytes: int, duration_s: float) -> None:
    """Emit a ``transfer_summary.v1`` JSON line for one completed file transfer
    (one open-to-release lifecycle of a read-only or write-only handle)."""
    if not ENABLED:
        return
    throughput_mb_s = (size_bytes / (1024 * 1024)) / duration_s if duration_s > 0 else 0.0
    _emit({
        "schema": "transfer_summary.v1",
        "ts": time.time(),
        "path": path,
        "direction": direction,
        "size_bytes": size_bytes,
        "duration_s": round(duration_s, 3),
        "avg_throughput_mb_s": round(throughput_mb_s, 2),
    })
