# Performance Profiling Strategy — Fuse4Databricks

**Status: proposal only — nothing in this document has been implemented.** Per the request, this is an architecture analysis and design, not code. No source file was modified to produce this document.

This builds on the prior reviews in this folder ([architecture-review.md](architecture-review.md), [optimization-review.md](optimization-review.md), [performance-improvement-roadmap.md](performance-improvement-roadmap.md), [P01](P01-analysis.md)/[P02](P02-analysis.md)/[P03](P03-analysis.md)/[P12](P12-analysis.md)-analysis.md) and is now grounded against a real benchmark run for the first time.

---

## 0. The benchmark data this proposal is built to explain

| Metric | Value | Derived rate |
|---|---|---|
| Dataset | 49 GB, 554 files, 104 directories | avg file size ≈ **90.6 MB** |
| Download (Databricks → Posit) | ~463 s | **≈ 108.3 MB/s** average |
| Upload (Posit → Databricks) | ~1482 s | **≈ 33.9 MB/s** average |
| `find` during concurrent upload | ~33 s | n/a (pure metadata) |
| `ls` during concurrent upload | ~32 s | n/a (pure metadata) |

Upload is roughly **3.2× slower** than download, and metadata operations degrade heavily (tens of seconds for what should be near-instant directory scans) while an upload is in flight. Everything in this proposal is designed to turn these four numbers into a per-phase, per-component time breakdown instead of a single opaque wall-clock total — this is exactly the gap the current logging (plain `logger.info`/`logger.debug` calls, scattered, unstructured, not timed, not aggregated) cannot fill.

---

## 1. Analysis of the current architecture (profiling-relevant facts)

Re-stated here only to the extent they shape *where* instrumentation must go; full architecture detail is in [architecture-review.md](architecture-review.md).

### Read path (Databricks → Posit)
`UnityCatalogFS.read()` → `DataManager.read()` splits the requested range into 8 MB chunks (`DataManager.chunk_size`, [fs/data_manager.py](../../fuse4dbricks/fs/data_manager.py)) → each chunk goes through `_read_chunk()`: RAM cache → disk cache (`DiskPersistence.retrieve_chunk`) → network (`DownloadScheduler` → `UnityCatalogClient.download_chunk_stream`) → `DiskPersistence.store_chunk_from_stream` (write to disk, evict, bookkeeping) → RAM cache populated → waiting reader released. Up to 10 chunks download concurrently via a fixed worker pool; up to 10 chunks are also spec­ulatively prefetched ahead of the current read position.

With an average file of 90.6 MB, each file is ~11 chunks — large enough that per-chunk overhead (not per-file overhead) is the right unit of measurement.

### Write path (Posit → Databricks)
`UnityCatalogFS.write()` writes directly into a per-handle `WriteBuffer` (a local tempfile, [fs/write_buffer.py](../../fuse4dbricks/fs/write_buffer.py)) — synchronously, with **no thread offload today** (see [P12-analysis.md](P12-analysis.md); this review's earlier P12 fix is not present in the current checkout). Nothing is sent to Databricks until `flush`/`release`, at which point the **entire** file is handed to the Databricks SDK (`UnityCatalogClient.upload_file`, [api/uc_client.py](../../fuse4dbricks/api/uc_client.py)) as a single call, constructing a **new** `WorkspaceClient` per call, with no application-level parallelism below the SDK's own ~5 GB automatic multipart threshold — and every file here (avg 90.6 MB) is far below that threshold.

### Metadata path
`getattr`/`lookup`/`readdir` route through `MetadataManager` ([fs/metadata_manager.py](../../fuse4dbricks/fs/metadata_manager.py)): an in-memory attribute/dir/permission/principal cache with TTLs, request coalescing (`InflightCoalescer`), and on miss, one HTTP call via the **same shared `UnityCatalogClient.client`** pool used by chunk downloads (but *not* used by uploads — uploads bypass it entirely via the SDK, see [P03-analysis.md](P03-analysis.md) §1.3).

### The single-OS-thread fact that changes how this benchmark should be read
Confirmed in [P12-analysis.md](P12-analysis.md) by reading `pyfuse3`'s own request-dispatch loop: **every concurrent FUSE request in this process — reads, writes, metadata lookups, download-worker continuations — is a `trio` task cooperatively scheduled on one OS thread.** A blocking call anywhere stalls everything. This is directly relevant to the `find`/`ls`-during-upload numbers: those metadata calls do **not** share a connection pool with uploads (uploads use a separate SDK client), so the leading hypothesis for their 32-33s stalls is **not** the P03 pool-contention mechanism — it is the event loop itself being occupied by the write path's blocking local I/O. Profiling needs to be able to distinguish these two mechanisms, which is why "Locking/Waiting" and "FUSE Overhead" are tracked as distinct categories below rather than folded into "Backend."

---

## 2. Best instrumentation points

Ranked by diagnostic value per the stated questions (backend vs. FUSE vs. cache vs. metadata vs. locking).

| # | Location | Class/Method | What it tells us |
|---|---|---|---|
| 1 | [api/uc_client.py](../../fuse4dbricks/api/uc_client.py) | `UnityCatalogClient._request` (wraps every HTTP call), `download_chunk_stream`, `upload_file` | **Backend time**, request/response sizes, retry counts, failures — answers "is it the Databricks backend?" directly, for every operation type (metadata GET/HEAD, chunk GET, file PUT). |
| 2 | [fs/data_manager.py](../../fuse4dbricks/fs/data_manager.py) | `DataManager.read`, `_read_chunk`, `_process_request`, `_fetch_chunk`, `DownloadScheduler.enqueue_request`/`_downloader` | Chunk-level timing split into cache-check vs. coalescing-wait vs. network-fetch; worker queue depth/wait time — answers "is the bottleneck in scheduling/concurrency, not the network call itself?" |
| 3 | [storage/persistence.py](../../fuse4dbricks/storage/persistence.py) | `retrieve_chunk`, `chunk_exists`, `store_chunk_from_stream`, `evict` | **Disk cache** hit/miss, store latency, and — critically — how much of `store_chunk_from_stream`'s time is the unbounded `evict()` pass (the exact mechanism [P01-analysis.md](P01-analysis.md) flagged). |
| 4 | [fs/ram_cache.py](../../fuse4dbricks/fs/ram_cache.py) | `RamCache.get`/`put` | RAM cache hit/miss, essentially free to instrument (in-memory, sub-microsecond), but needed for the hit-ratio metric explicitly requested. |
| 5 | [fs/write_buffer.py](../../fuse4dbricks/fs/write_buffer.py) | `write`, `read`, `flush_to_disk`, `finalize`, `close` | **Local buffering time** — directly tests the P12 hypothesis; if this dominates write-path time, it also explains the metadata-during-upload stalls (same thread, same blocking calls). |
| 6 | [fs/operations.py](../../fuse4dbricks/fs/operations.py) | `read`, `write`, `open`, `create`, `getattr`, `lookup`, `readdir`, `release`, `flush` | Top-level span per FUSE request; **FUSE overhead** = this span's duration minus every instrumented child span's duration (see §3.4). This is the only layer that sees the complete, real end-to-end latency the kernel/application actually experiences. |
| 7 | [fs/metadata_manager.py](../../fuse4dbricks/fs/metadata_manager.py) | `get_attributes`, `lookup_child`, `list_directory`, `check_access`, `_get_principal` | Cache hit/coalesce/miss breakdown per metadata call type, plus — explicitly requested — p95/p99/cumulative per op. |
| 8 | `trio.Lock` acquisitions and `InflightCoalescer.join_or_lead`/`wait_event.wait()` sites (`fs/metadata_manager.py`, `fs/data_manager.py`, `storage/persistence.py`, `fs/utils.py`) | n/a (cross-cutting) | **Locking/contention** time — time spent waiting to acquire a lock or waiting on a coalesced leader, separated from the leader's own useful work. |

Deliberately *not* instrumented in the first iteration: `fs/inode_manager.py` (pure in-memory dict operations, no I/O, negligible cost) and `fs/securable_filter.py` (pure string matching). Adding spans there would only add overhead for no diagnostic value.

---

## 3. Proposed profiling implementation

### 3.1 New module: `fuse4dbricks/perf/profiling.py`

A single, small, dependency-free module providing the instrumentation primitives. Nothing here touches existing modules' logic — it is purely additive instrumentation wrapped around existing calls.

### 3.2 Enable/disable mechanism (near-zero overhead when disabled)

```python
# Read once at import time, not per-call.
_ENABLED = os.environ.get("ENABLE_PERF_PROFILING", "").lower() in ("1", "true", "yes")
```

The critical design choice for "near-zero overhead when disabled": **select the implementation once, at import time, via a function-pointer swap — not an `if` branch inside the hot path.**

```python
if _ENABLED:
    span = _real_span          # records start time, emits/aggregates on exit
    counter = _real_counter     # increments an in-memory counter
else:
    span = _noop_span           # a bare contextmanager that does nothing
    counter = _noop_counter     # a function that does nothing
```

When disabled, every instrumentation call site (`with span(...): ...`) costs exactly one no-op context-manager `__enter__`/`__exit__` pair — no clock read, no allocation, no dict lookup, no JSON. This is the same zero-cost-abstraction pattern as Python's own `contextlib.nullcontext`, applied via a module-level swap so call sites never need to check a flag themselves.

### 3.3 Hot-path cost when enabled

Two different instrumentation densities, matched to call frequency:

- **Per-request spans** (`operations.py` handlers, `data_manager.py` chunk fetches, `uc_client.py` HTTP calls): one `time.monotonic()` read at start and end, plus an in-memory aggregation update (sum/count/min/max into a pre-allocated structure keyed by operation name — no dict-of-dicts allocation per call, no JSON serialization on the hot path). For a 49 GB / ~6,272-chunk transfer, this is ~12,500 extra clock reads and ~6,250 aggregation updates total — microseconds of aggregate overhead, not seconds.
- **Percentiles (p95/p99) without unbounded memory**: each named metric keeps a small fixed-size reservoir (e.g. the most recent 2,000 samples in a pre-allocated `array.array`, overwritten circularly). Percentiles are computed **only when a summary is emitted** (sorting 2,000 floats is sub-millisecond), never on the hot path.

### 3.4 Inclusive vs. exclusive time (so percentages sum to ~100%, not more)

Every span records both its own wall-clock duration and, via a per-task span stack (using `trio`'s existing task-local-friendly structure — a simple stack stored on the task, pushed/popped around nested spans), how much of that duration was spent inside a *child* span of a known category. The Transfer Summary's "FUSE Overhead" bucket is computed as:

```
fuse_overhead = top_level_span.duration - sum(child_span.duration for child_span in top_level_span.children)
```

This is the same technique profilers like `cProfile`/flame graphs use (inclusive vs. self time) and is what makes the category percentages in the Transfer Summary meaningful rather than double-counted.

### 3.5 Where locking/contention is measured

Each `trio.Lock.acquire()` and each `InflightCoalescer` follower's `await wait_event.wait()` is wrapped in a `span(..., category="locking")`. For a coalescing **leader**, the time spent doing the real work is attributed to `category="backend"`/`"cache"` as appropriate — only a **follower's** wait is "locking/waiting," which correctly reflects that the leader is doing useful work while followers are blocked on it.

### 3.6 Output: structured JSON, two tiers

1. **Aggregate summaries** (always, when enabled): emitted periodically (e.g. every N seconds) and once per completed "transfer" (a heuristic: a burst of chunk/file activity on the same path with no idle gap longer than a few seconds) — one JSON object per summary, written as a single line to a dedicated logger (`logging.getLogger("fuse4dbricks.perf")`, independent of `setup_logging()`'s regular logger so enabling profiling never interleaves with or floods the normal debug log).
2. **Raw span stream** (optional, separate, more verbose flag — e.g. `ENABLE_PERF_PROFILING=trace` instead of `true`): every individual span as its own JSON line, for deep-dive debugging of a single slow transfer. Given the volume this can reach (thousands of lines for one large transfer), this tier is written via a bounded `trio.open_memory_channel` drained by a single background task that does the actual (threaded) disk write — mirroring the exact wake-channel pattern `DownloadScheduler` already uses in this codebase ([fs/data_manager.py](../../fuse4dbricks/fs/data_manager.py)) — so even verbose tracing never performs blocking I/O on a request-handling task.

### 3.7 Correlation

Every span carries a `trace_id` (a monotonic counter assigned once per top-level FUSE request) and, where applicable, the `fs_path` and `chunk_id`. This lets every backend call, cache lookup, and lock wait belonging to one logical `read()`/`write()` be reconstructed and summed, which is required for both the per-request "FUSE overhead" computation (§3.4) and the Transfer Summary.

---

## 4. Metrics schema design

### 4.1 Span event (raw tier only)

```json
{
  "schema": "span.v1",
  "ts": "2026-10-28T09:14:02.481331Z",
  "trace_id": 48213,
  "span_id": 7,
  "parent_span_id": 3,
  "category": "backend",
  "name": "download_chunk_stream",
  "duration_ms": 71.4,
  "attrs": {
    "fs_path": "/cat/sch/vol/GSE212450_RAW.tar",
    "chunk_id": 42,
    "request_bytes": 0,
    "response_bytes": 8388608,
    "retries": 0,
    "status": "success"
  }
}
```

### 4.2 Cache metrics (aggregate tier)

```json
{
  "schema": "cache_summary.v1",
  "ts": "2026-10-28T09:14:10.002Z",
  "window_s": 10,
  "tier": "ram",
  "hits": 812,
  "misses": 94,
  "hit_ratio": 0.896,
  "lookup_latency_us": {"p50": 3.1, "p95": 9.8, "p99": 22.4},
  "store_latency_us": {"p50": 4.0, "p95": 11.2, "p99": 28.0},
  "evictions": 0,
  "size_entries": 64,
  "max_entries": 64
}
```

```json
{
  "schema": "cache_summary.v1",
  "ts": "2026-10-28T09:14:10.002Z",
  "window_s": 10,
  "tier": "disk",
  "hits": 94,
  "misses": 23,
  "hit_ratio": 0.803,
  "lookup_latency_ms": {"p50": 1.8, "p95": 6.4, "p99": 14.1},
  "store_latency_ms": {"p50": 42.3, "p95": 118.7, "p99": 203.5},
  "evictions": 11,
  "size_bytes": 10200547328,
  "max_size_bytes": 10737418240
}
```

Note `store_latency_ms` for the disk tier is exactly the metric [P01-analysis.md](P01-analysis.md) predicted would be elevated near the cache's size limit (`evictions: 11` in the same window is the corroborating signal).

### 4.3 Metadata operation metrics (aggregate tier)

```json
{
  "schema": "metadata_summary.v1",
  "ts": "2026-10-28T09:14:10.002Z",
  "window_s": 10,
  "operations": {
    "getattr":  {"count": 1240, "avg_ms": 0.9,  "p95_ms": 3.1,  "p99_ms": 8.4,   "cumulative_ms": 1116.0},
    "lookup":   {"count": 980,  "avg_ms": 1.2,  "p95_ms": 4.0,  "p99_ms": 9.9,   "cumulative_ms": 1176.0},
    "readdir":  {"count": 104,  "avg_ms": 118.3,"p95_ms": 310.5,"p99_ms": 480.2, "cumulative_ms": 12303.2},
    "open":     {"count": 554,  "avg_ms": 2.1,  "p95_ms": 6.5,  "p99_ms": 15.0,  "cumulative_ms": 1163.4}
  }
}
```

### 4.4 Backend call metrics (aggregate tier)

```json
{
  "schema": "backend_summary.v1",
  "ts": "2026-10-28T09:14:10.002Z",
  "window_s": 10,
  "operations": {
    "chunk_read":   {"count": 420, "avg_ms": 68.2,  "p95_ms": 142.0, "retries": 2, "failures": 0, "bytes_in": 0,          "bytes_out": 3523215360},
    "file_upload":  {"count": 9,   "avg_ms": 2381.0,"p95_ms": 4012.0,"retries": 0, "failures": 0, "bytes_in": 816467968, "bytes_out": 0},
    "list_dir":     {"count": 14,  "avg_ms": 210.5, "p95_ms": 455.0, "retries": 0, "failures": 0, "bytes_in": 0,          "bytes_out": 0},
    "get_metadata": {"count": 310, "avg_ms": 45.1,  "p95_ms": 98.0,  "retries": 1, "failures": 0, "bytes_in": 0,          "bytes_out": 0}
  }
}
```

(`bytes_in`/`bytes_out` are from the server's perspective: upload bytes-in to Databricks, download bytes-out from Databricks — named to avoid the ambiguity of "request/response size" when the payload direction flips between reads and writes.)

### 4.5 Lock/contention metrics (aggregate tier)

```json
{
  "schema": "contention_summary.v1",
  "ts": "2026-10-28T09:14:10.002Z",
  "window_s": 10,
  "sites": {
    "metadata_manager._cache_lock":        {"wait_count": 12,  "avg_wait_ms": 0.4,  "max_wait_ms": 2.1},
    "inflight_coalescer.chunk_download":   {"wait_count": 318, "avg_wait_ms": 61.8, "max_wait_ms": 420.3},
    "write_buffer.event_loop_blocking":    {"wait_count": 980, "avg_wait_ms": 12.9, "max_wait_ms": 88.0}
  }
}
```

The third entry is the specific metric that would confirm or refute the P12 hypothesis for the `find`/`ls`-during-upload stalls: it measures how long *other* tasks were unable to run while a `WriteBuffer` call was in progress, by timing the gap between when a task became runnable and when it actually resumed.

---

## 5. Example JSON logs (realistic, drawn from the benchmark scenario)

A chunk download backend call (raw tier):

```json
{"schema":"span.v1","ts":"2026-10-28T09:10:00.102Z","trace_id":100231,"span_id":2,"parent_span_id":1,"category":"backend","name":"download_chunk_stream","duration_ms":64.7,"attrs":{"fs_path":"/cat/sch/vol/GSE212450_RAW.tar","chunk_id":0,"response_bytes":8388608,"retries":0,"status":"success"}}
```

A RAM cache hit avoiding a backend call entirely (raw tier):

```json
{"schema":"span.v1","ts":"2026-10-28T09:10:00.998Z","trace_id":100235,"span_id":2,"parent_span_id":1,"category":"cache_ram","name":"ram_cache_get","duration_ms":0.004,"attrs":{"fs_path":"/cat/sch/vol/GSE212450_RAW.tar","chunk_id":1,"hit":true}}
```

A follower blocked behind an in-flight leader download (raw tier) — the direct evidence for "is it locking/contention?":

```json
{"schema":"span.v1","ts":"2026-10-28T09:10:01.040Z","trace_id":100240,"span_id":2,"parent_span_id":1,"category":"locking","name":"inflight_coalescer_wait","duration_ms":58.2,"attrs":{"fs_path":"/cat/sch/vol/GSE212450_RAW.tar","chunk_id":1,"role":"follower"}}
```

A write-path local buffering call (raw tier) — the direct evidence for "is it FUSE/local-I/O, not the backend?":

```json
{"schema":"span.v1","ts":"2026-10-28T09:25:11.511Z","trace_id":220110,"span_id":1,"parent_span_id":null,"category":"fuse_write","name":"write_buffer.write","duration_ms":9.8,"attrs":{"fs_path":"/cat/sch/vol2/sample_042.fastq.gz","offset":8388608,"bytes":131072}}
```

A directory listing slowed down during concurrent upload (aggregate tier, 10s window captured during the `find`):

```json
{"schema":"metadata_summary.v1","ts":"2026-10-28T09:30:00.000Z","window_s":10,"operations":{"readdir":{"count":104,"avg_ms":298.1,"p95_ms":890.4,"p99_ms":1420.0,"cumulative_ms":31002.4}}}
```

(Contrast this `avg_ms: 298.1` against the idle-mount baseline in §4.3's `avg_ms: 118.3` for the same operation — this comparison, available for free once both windows exist, is exactly what would confirm upload-induced metadata degradation quantitatively instead of anecdotally.)

---

## 6. Example transfer summary output

### 6.1 Human-readable (as specified in the request)

```
--------------------------------------------------
TRANSFER SUMMARY
--------------------------------------------------

Path:
GSE212450_RAW.tar

Size:
14 GB

Duration:
97 seconds

Average Throughput:
145 MB/s

Breakdown:

Backend Time:
71%

Cache Operations:
8%

Metadata:
2%

FUSE Overhead:
5%

Locking/Waiting:
4%

Other:
10%

Top Bottleneck:
Backend download

--------------------------------------------------
```

### 6.2 Equivalent structured JSON (what actually gets written; the above is rendered from this for humans)

```json
{
  "schema": "transfer_summary.v1",
  "ts": "2026-10-28T09:31:40.118Z",
  "path": "/cat/sch/vol/GSE212450_RAW.tar",
  "direction": "download",
  "size_bytes": 15032385536,
  "duration_s": 97.0,
  "avg_throughput_mb_s": 145.2,
  "chunk_count": 1792,
  "breakdown_pct": {
    "backend": 71,
    "cache": 8,
    "metadata": 2,
    "fuse_overhead": 5,
    "locking": 4,
    "other": 10
  },
  "breakdown_ms": {
    "backend": 68870,
    "cache": 7760,
    "metadata": 1940,
    "fuse_overhead": 4850,
    "locking": 3880,
    "other": 9700
  },
  "cache": {"ram_hit_ratio": 0.12, "disk_hit_ratio": 0.04},
  "backend": {"retries": 3, "failures": 0},
  "top_bottleneck": "backend_download"
}
```

### 6.3 Example for an *upload*, using the actual benchmark's shape (hypothesis-illustrative, not a real measurement)

```json
{
  "schema": "transfer_summary.v1",
  "path": "/cat/sch/vol2/sample_batch_07",
  "direction": "upload",
  "size_bytes": 5452595200,
  "duration_s": 164.9,
  "avg_throughput_mb_s": 31.5,
  "file_count": 60,
  "breakdown_pct": {
    "backend": 38,
    "cache": 0,
    "metadata": 3,
    "fuse_overhead": 47,
    "locking": 9,
    "other": 3
  },
  "top_bottleneck": "fuse_overhead_write_buffer_blocking"
}
```

This illustrates the diagnostic value directly: if real measurement shows `fuse_overhead` (dominated by un-threaded `WriteBuffer` I/O) this large relative to `backend`, that is concrete, quantified confirmation of the [P12](P12-analysis.md) hypothesis rather than an inference from reading code — and it is a fundamentally different finding than "the Databricks upload API is slow," which would instead show up as a large `backend` percentage.

---

## 7. Identification of the most likely bottlenecks (hypotheses to be confirmed by the profiling above)

Stated as hypotheses, each with what specific metric from §4 would confirm or refute it — per the instruction not to assume gains without evidence, nothing here is claimed as fact.

### Download (~108 MB/s average)
- **Hypothesis A — client-side concurrency-bound, not backend-bound**: 10 download workers each sustaining roughly ~11 MB/s would reproduce the observed ~108 MB/s aggregate. *Confirm via*: `backend_summary.chunk_read.avg_ms` combined with chunk size (8 MB) gives effective per-connection throughput; if per-chunk time is roughly constant regardless of worker count and close to a single connection's realistic throughput ceiling, this hypothesis holds, and `DownloadScheduler` queue depth (not yet in a schema above — recommend adding a `scheduler_summary.v1` with queued/active worker counts) would show workers saturated essentially 100% of the time.
- **Hypothesis B — disk-cache store overhead on the critical path**: per [P01-analysis.md](P01-analysis.md), every cache-miss chunk's bytes are held up by `evict()` before being released to the reader. *Confirm via*: `cache_summary.disk.store_latency_ms.p95`/`p99` and whether it correlates with `evictions > 0` in the same window; if store latency is small relative to `backend.chunk_read.avg_ms`, this is not the dominant factor for *this* benchmark (cache likely has headroom for one 49 GB run against a 10 GB+ configured budget), but would become dominant on repeated/larger runs that push the cache near its limit.
- **Less likely given the architecture, but checkable for free**: metadata interference — the same `getattr`/`lookup` calls needed to discover 554 files/104 dirs share the download's HTTP pool. *Confirm via*: `backend_summary.get_metadata`/`list_dir` cumulative time as a fraction of the 463 s total; with only 104 directories and 554 files, this is architecturally bounded and unlikely to be more than a few percent, but the profiling will state this as a number rather than an assumption.

### Upload (~34 MB/s average, 3.2× slower than download)
- **Leading hypothesis — `WriteBuffer`'s un-threaded local I/O blocking the single event-loop thread** ([P12-analysis.md](P12-analysis.md)): every `write()` syscall for every one of 554 files pays a blocking local-disk cost that also prevents any other task (including other files' uploads and metadata calls) from making progress. *Confirm via*: `contention_summary.write_buffer.event_loop_blocking` wait counts/durations, and the `fuse_overhead` share of upload Transfer Summaries.
- **Second hypothesis — no write/upload overlap and no per-file parallelism**: the whole-file-buffer-then-upload design (U-2) means local-write-time and network-upload-time never overlap, and the SDK's automatic multipart never engages below ~5 GB (every file here, avg 90.6 MB, is far under that). *Confirm via*: `backend_summary.file_upload.avg_ms` vs. the local-buffering time recorded in `write_buffer.*` spans for the same file — if upload-phase time and write-phase time are comparable and sequential (not overlapping) per file, this is directly visible in the per-file trace reconstruction (§3.7).
- **Third, smaller hypothesis — fresh SDK client per upload** ([P03-analysis.md](P03-analysis.md) §1.3, item U-4/P13): 554 separate `WorkspaceClient` constructions, each a fresh connection setup. *Confirm via*: whether `file_upload.avg_ms` includes a roughly-constant per-call floor independent of file size (a fixed-overhead signature) across files of varying size.

### Metadata during concurrent upload (`find` ~33 s, `ls` ~32 s)
- **Leading hypothesis — single-thread starvation by the write path, not HTTP-pool contention.** This is a *refinement*, using the new benchmark data, of the original P03 analysis: P03's pool-contention mechanism specifically requires metadata and the contending traffic to **share `UnityCatalogClient.client`** — but uploads bypass that pool entirely via the SDK (confirmed in [P03-analysis.md](P03-analysis.md) §1.3). So a 32-33 s stall on `find`/`ls` *during upload specifically* is better explained by the same single-OS-thread mechanism as the upload hypothesis above, not by connection-pool queuing. *Confirm via*: direct comparison of `contention_summary.write_buffer.event_loop_blocking` activity against the `metadata_summary.readdir`/`lookup`/`getattr` latency spike window — if they overlap in time, this hypothesis is confirmed; if metadata latency instead correlates with `backend_summary` queue/pool-wait metrics with no corresponding write-buffer blocking signal, P03's original pool-contention mechanism would be the better explanation after all (relevant if this benchmark included concurrent *downloads*, not just uploads, during the `find`/`ls` — worth re-confirming which concurrent operation was actually running).

### Direct answers to the specific questions asked
- **Is the bottleneck in the Databricks backend?** Partially, for download (Hypothesis A above suggests per-connection/worker-count limits more than Databricks-side slowness, but this needs the `backend_summary` data to confirm either way). Unlikely to be the primary upload bottleneck given the architecture (see below).
- **Is the bottleneck in FUSE itself?** Likely yes for upload, specifically the `WriteBuffer`/event-loop-blocking mechanism — this is "FUSE-adjacent" in the sense that it's this project's own handler code, not the kernel's FUSE protocol overhead itself.
- **Is the bottleneck in the disk cache?** Possible contributor for download under cache pressure (P01), not expected to be the primary factor for a single 49 GB run against a larger configured cache budget — needs confirmation.
- **Is the bottleneck caused by metadata operations?** Not for the bulk transfer times (directory/file count is modest relative to data volume); yes for the *concurrent* `find`/`ls` degradation specifically, though as a *victim* of thread starvation rather than a *cause* of the slow upload itself.
- **Is the bottleneck caused by locking or contention?** Likely yes, in the specific form of event-loop monopolization (a form of contention not reducible to `trio.Lock` wait time alone) during uploads — directly measurable via the proposed `contention_summary` schema.
- **Which area would give the highest throughput improvement if optimized?** Based on the architecture alone (to be confirmed by profiling): **the write/upload path**, because it is both the slower of the two transfers in absolute terms *and* has a plausible single root cause (`WriteBuffer` blocking I/O) that, per [P12-analysis.md](P12-analysis.md), has the broadest blast radius of any issue in this codebase — fixing it could plausibly improve upload throughput *and* metadata responsiveness-during-upload simultaneously.

---

## 8. Recommendations for optimizations most likely to improve each metric

These map directly onto the already-cataloged items in [performance-improvement-roadmap.md](performance-improvement-roadmap.md); this benchmark data sharpens their relative priority but does not introduce new catalog items.

### Download throughput (MB/s)
1. **Profile first** — §7's Hypothesis A/B determine whether P04 (more workers), P06 (bigger chunks), or P01 (unblock disk-cache store from the critical path) is the correct lever; implementing any of them before measuring risks tuning the wrong knob.
2. If client-concurrency-bound: **P04** (configurable/higher worker count), paired with **P03** (separate HTTP pools) so raising worker count doesn't newly starve metadata.
3. If cache-store-bound under pressure: **P01** (stop `evict()` from gating the reader).

### Upload throughput (MB/s)
1. **P12** (move `WriteBuffer` I/O off the event loop) — now the best-evidenced single upload fix available, since its predicted signature (broad, cross-cutting stalls) matches the observed metadata-during-upload degradation, not just a theoretical concern.
2. **P10** (enable kernel write-back caching) — compounds with P12 by reducing how many times the (now non-blocking, but still non-free) write path is invoked.
3. **P13** (reuse the upload SDK client) — smaller, but removes a per-file fixed cost across 554 files.
4. **P11** (overlap local write with network upload) — the highest theoretical ceiling (per the roadmap, this is explicitly the highest-risk/highest-complexity item), worth revisiting only after 1–3 are measured, since they may already close most of the gap at far lower risk.

### Metadata performance
1. **Confirm the mechanism first** (§7's metadata hypothesis) — if event-loop starvation by the write path is confirmed, **P12 is also the primary metadata fix**, not a separate pool/caching change.
2. If, after P12, metadata is still slow specifically *during concurrent downloads* (not uploads), **P03** (separate HTTP pools) remains the correct, already-analyzed fix for that distinct mechanism.
3. **P08** (RAM cache sizing guidance) and the existing attribute/dir TTL caching are already reasonably effective per the architecture review; no change recommended there without evidence of a cache-miss-driven metadata problem specifically.

---

## Summary

This proposal adds a dedicated, opt-in (`ENABLE_PERF_PROFILING`), structured-JSON profiling layer — not generic logging — purpose-built to answer the eight bottleneck-location questions this review was asked to address, by: (a) instrumenting the specific, named methods in `operations.py`, `data_manager.py`, `metadata_manager.py`, `persistence.py`, `ram_cache.py`, `write_buffer.py`, and `uc_client.py` that each correspond to one candidate bottleneck category; (b) separating inclusive vs. exclusive (self) time so a Transfer Summary's percentages are meaningful; (c) treating lock/coalescer wait time as its own first-class category, distinct from useful backend/cache work; and (d) keeping the disabled-by-default cost at effectively zero via a function-pointer swap rather than per-call branching. Nothing here has been implemented; the next step, if approved, is to build the `fuse4dbricks/perf/profiling.py` module and wire it into the instrumentation points in §2, then re-run this exact benchmark to replace every hypothesis in §7 with a measured number.
