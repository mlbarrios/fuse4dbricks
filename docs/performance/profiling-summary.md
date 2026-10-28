# Performance Profiling — Change Summary

Wiki-ready summary of the opt-in performance profiling feature added to Fuse4Databricks. Covers every file created or modified for this feature, and why.

**Enable with:** `ENABLE_PERF_PROFILING=true` (disabled by default, zero behavior change when off).

---

## Purpose

Before this change, there was no way to see *where* time went during a large download/upload or a slow `find`/`ls` — only generic, unstructured log lines. This feature adds structured, opt-in instrumentation across the read path, write path, metadata calls, cache layers, the Databricks HTTP client, and the download scheduler, so a benchmark run can be decomposed into phases (backend network time, disk/RAM cache time, scheduler queueing, retries/backoff, etc.) instead of a single opaque wall-clock number.

---

## New files created

| File | Purpose |
|---|---|
| `fuse4dbricks/perf/__init__.py` | New package marker for the profiling module. |
| `fuse4dbricks/perf/profiling.py` | The profiling engine itself: the `ENABLE_PERF_PROFILING` on/off switch, timed "spans" (start/finish), cache hit/miss counters, retry counters, and the summary functions that emit structured JSON. Designed so that when disabled, there is effectively no runtime cost (no clock reads, no allocations, no logging). |
| `tests/test_profiling.py` | Unit tests for the profiling module itself (enabled/disabled behavior, JSON shape, hit ratios, retry counters, percentiles). |

---

## Files updated

| File | What was added | Why |
|---|---|---|
| `fuse4dbricks/fs/operations.py` | Timing around `getattr`, `lookup`, `readdir`, `read`, `write`; per-file-handle byte/time tracking in `open`/`create`; a "transfer summary" and cache/metadata/operations/retry summaries emitted when a file handle is released. | This is the FUSE-facing layer — the one place that sees every real user-visible operation end-to-end, and the natural place to emit periodic summaries (no new background timer needed). |
| `fuse4dbricks/fs/data_manager.py` | End-to-end timing for a single read (`data_manager_read`), split into RAM cache check, disk cache check, and remote network fetch; new timing for the download scheduler's queue-wait time, worker execution time, and total "chunk ready" latency. | Answers "why did this read take X ms" by breaking it into cache-hit vs. cache-miss vs. network-wait vs. queued-behind-other-downloads. |
| `fuse4dbricks/api/uc_client.py` | Timing around every outgoing HTTP request (method, endpoint, status code, retry count), around file uploads, around chunk downloads, and a counter for every retry/backoff attempt (reason + delay). | Answers "how much of the time was the Databricks backend itself, vs. retries/backoff, vs. our own upload/download handling." |
| `fuse4dbricks/fs/ram_cache.py` | Hit/miss counters on the in-memory chunk cache. | Gives a RAM cache hit ratio, independent from the disk cache. |
| `fuse4dbricks/storage/persistence.py` | Hit/miss counters and store-latency timing on the on-disk chunk cache. | Gives a disk cache hit ratio and shows how much time is spent writing new chunks to disk (including cache cleanup). |

---

## What you get when it's enabled

One structured JSON line per event (never mixed with normal application logs), plus periodic summary lines:

- **Per-operation timing** — count, average, p95/p99, total time, for every instrumented operation (`read`, `write`, `getattr`, `lookup`, `readdir`, `upload_file`, `download_chunk_stream`, `data_manager_read`, and the internal cache/scheduler/network phases).
- **Cache summary** — RAM and disk hit ratios, eviction counts.
- **Retry summary** — how many retries happened, why, and how much cumulative time was spent waiting on backoff.
- **Transfer summary** — per-file throughput (MB/s) for completed uploads/downloads.

This is the data needed to answer, with evidence instead of guesswork, questions like "is this slow download a network problem, a cache problem, or a scheduler problem?"

---

## Safety notes

- Disabled by default; must be explicitly turned on.
- No existing behavior, return values, or error handling was changed anywhere — only timing/counting was added around existing calls.
- Full test suite (399 tests) passes unchanged with profiling off, and passes with profiling on (exercising the real, non-no-op code paths).
