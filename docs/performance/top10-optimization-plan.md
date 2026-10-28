# Top 10 Optimization Plan — Fuse4Databricks

**Status: analysis and prioritization only — nothing in this document has been implemented.** This is the implementation-planning follow-on to [profiling-strategy.md](profiling-strategy.md), built on the same benchmark and the full prior review set ([architecture-review.md](architecture-review.md), [optimization-review.md](optimization-review.md), [performance-improvement-roadmap.md](performance-improvement-roadmap.md), [P01](P01-analysis.md)/[P02](P02-analysis.md)/[P03](P03-analysis.md)/[P12](P12-analysis.md)-analysis.md).

## Benchmark this plan is built against

| Metric | Value | Rate |
|---|---|---|
| Dataset | 49 GB, 554 files, 104 dirs | avg file ≈ 90.6 MB |
| Download | 463 s | ~108 MB/s |
| Upload | 1482 s | ~34 MB/s |
| `find` during upload | 32.6 s | — |
| `ls` during upload | 32.4 s | — |

No profiling instrumentation exists yet (that is what [profiling-strategy.md](profiling-strategy.md) proposes), so every "expected impact" / "confidence" rating below is an architecture-grounded hypothesis, not a measurement — consistent with that document's conclusion that profiling should land *before or alongside* the first few items here, not after all ten.

### Component attribution key

Used in each entry's "Currently limiting" field: **Databricks backend**, **FUSE event loop**, **WriteBuffer**, **RAM cache**, **Disk cache**, **Download scheduler**, **Metadata layer**, **HTTP client**, **Lock contention**.

---

## The 10 optimizations

### 1. Offload `WriteBuffer`'s blocking I/O to a thread (P12)

- **Description**: Route every `WriteBuffer` file operation (`write`, `read`, `truncate`, `flush_to_disk`, `finalize`, `close`, and construction) through `trio.to_thread.run_sync`, matching the pattern `storage/persistence.py` already uses. Today these calls block the single OS thread the whole mount runs on.
- **Files/modules involved**: [fuse4dbricks/fs/write_buffer.py](../../fuse4dbricks/fs/write_buffer.py) (`WriteBuffer` class), [fuse4dbricks/fs/operations.py](../../fuse4dbricks/fs/operations.py) (every call site: `open`, `create`, `write`, `read`, `release`, `_upload_if_dirty`, `rename`, `setattr`/`_truncate_uc_file`).
- **Why it helps**: Confirmed via `pyfuse3`'s own request-dispatch loop ([P12-analysis.md](P12-analysis.md) §1.2) that every concurrent FUSE request — reads, writes, metadata lookups — is a `trio` task on **one** OS thread. A blocking `WriteBuffer` call during upload stalls *everything else*, which is the single best-evidenced explanation for the 32-33s `find`/`ls`-during-upload stalls (uploads bypass the shared HTTP pool entirely via the SDK, so pool contention — the P03 mechanism — cannot explain this specific symptom; event-loop starvation can).
- **Expected impact**: **Very High** — the only item in this list with a plausible mechanism to move *both* upload throughput *and* metadata responsiveness simultaneously.
- **Complexity**: **Small** — mechanical conversion, no design change; a prior session already prototyped this exact change and the full non-live test suite (389 tests) passed unmodified in behavior.
- **Risk**: **Low** — same semantics, different execution context; the existing test suite is the regression guard.
- **Confidence**: **High** — grounded in primary-source evidence (library's own dispatch loop), not inference.
- **Currently limiting**: WriteBuffer, FUSE event loop.

### 2. Verify network placement (region/latency/bandwidth) between Posit Workbench and the Databricks workspace (P09)

- **Description**: Before any further code work, measure the actual network path's latency and raw bandwidth between the Posit Workbench host and the Databricks workspace (e.g. are they in the same cloud region? what does a raw `iperf`-style or large-object HTTP transfer achieve outside this codebase entirely?).
- **Files/modules involved**: None — infrastructure/deployment, not code.
- **Why it helps**: 108 MB/s download and 34 MB/s upload are each consistent with a network-path ceiling as easily as with a software one; no amount of code optimization can exceed a hard network ceiling, and if one exists, it should reprioritize everything else in this list (e.g. P04's extra workers would show no gain if the link itself is the cap).
- **Expected impact**: **Very High** — potentially dwarfs every code change here, in either direction (confirms headroom exists, or rules out large gains from client-side concurrency work).
- **Complexity**: **Small** — a measurement task, not an engineering effort (the *consequence*, e.g. redeploying infrastructure, could be larger, but that is explicitly out of this codebase's scope).
- **Risk**: **Low** — measurement only, no behavior change.
- **Confidence**: **Medium** — the *need* to check this is High confidence; the *outcome* is unknown until measured.
- **Currently limiting**: Databricks backend (network path specifically, not the API itself).

### 3. Overlap local write with network upload for large sequential writes (P11)

- **Description**: Detect a purely sequential (non-overwriting) write pattern and begin streaming data to Databricks before the application finishes writing the file, instead of the current fully-serialized "write everything locally, then upload the whole file."
- **Files/modules involved**: [fuse4dbricks/fs/write_buffer.py](../../fuse4dbricks/fs/write_buffer.py), [fuse4dbricks/fs/operations.py](../../fuse4dbricks/fs/operations.py) (`write`, `_upload_if_dirty`), [fuse4dbricks/api/uc_client.py](../../fuse4dbricks/api/uc_client.py) (`upload_file`).
- **Why it helps**: Today, local-write-time and upload-time are strictly additive per file (U-2 in the roadmap); for 554 files averaging 90.6 MB, eliminating that serialization removes a potentially large fraction of the 1482 s total — the single highest theoretical ceiling of any item in this document.
- **Expected impact**: **Very High** (theoretical ceiling) — but explicitly **unconfirmed**, see confidence below.
- **Complexity**: **Large** — requires write-pattern detection, a fallback path for non-sequential writes, and either a custom incremental-upload mechanism or confirmation that the Files API/SDK supports one below its automatic ~5 GB multipart threshold.
- **Risk**: **High** — correctness risk (a misdetected "sequential" pattern could corrupt an upload); depends on unconfirmed Databricks API capabilities.
- **Confidence**: **Low** — feasibility itself (does the Files API support incremental upload at all below 5 GB?) is not yet established; this should start as a feasibility spike, not an implementation, per the roadmap's original framing.
- **Currently limiting**: WriteBuffer, Databricks backend (API shape).

### 4. Increase/make configurable the chunk-download worker count (P04)

- **Description**: Expose `DataManager`'s hardcoded `num_workers=10` as a tunable (CLI flag), and re-evaluate the default once profiling data (per [profiling-strategy.md](profiling-strategy.md) §7, Hypothesis A) confirms whether download throughput is client-concurrency-bound.
- **Files/modules involved**: [fuse4dbricks/fs/data_manager.py](../../fuse4dbricks/fs/data_manager.py) (`DataManager.__init__`, `DownloadScheduler`), [fuse4dbricks/main.py](../../fuse4dbricks/main.py) (`parse_args`, `async_main`).
- **Why it helps**: If ~108 MB/s reflects 10 workers each sustaining ~11 MB/s (a plausible per-connection ceiling), more workers directly raises aggregate download throughput, up to whatever the network path (item 2) or Databricks' own per-connection/per-account limits allow.
- **Expected impact**: **High** — directly targets the download number explicitly, with a clear, singular mechanism.
- **Complexity**: **Small** — a constructor/CLI parameter, no structural change.
- **Risk**: **Medium** — unsafe to raise in isolation: more download connections checked out for longer directly shrinks the headroom available to metadata calls sharing the same HTTP pool (the original P03 mechanism), so this should not ship without item 9 (P03) or at least awareness of the trade-off.
- **Confidence**: **Medium** — the underlying mechanism (more concurrent connections → more aggregate throughput, up to a ceiling) is well understood, but whether 10 is actually the limiting factor for *this* benchmark (vs. backend/network) is unconfirmed.
- **Currently limiting**: Download scheduler, Databricks backend.

### 5. Bounded concurrent upload pipeline (new — mirrors `DownloadScheduler`)

- **Description**: Introduce an explicit, bounded-concurrency upload worker pool (structurally analogous to the existing `DownloadScheduler`) so multiple files' `flush`/`release`-triggered uploads are deliberately scheduled and capped, rather than relying on incidental `trio`-task concurrency that (today) is largely moot anyway because of item 1's event-loop blocking.
- **Files/modules involved**: New component alongside [fuse4dbricks/fs/data_manager.py](../../fuse4dbricks/fs/data_manager.py)'s existing pattern; [fuse4dbricks/fs/operations.py](../../fuse4dbricks/fs/operations.py) (`_upload_if_dirty`, `flush`, `release`), [fuse4dbricks/api/uc_client.py](../../fuse4dbricks/api/uc_client.py) (`upload_file`).
- **Why it helps**: With 554 files to upload, true controlled parallelism across files (several uploads in flight at once, each a single stream, bounded to avoid overwhelming the connection pool or local disk I/O) is a direct, additive throughput lever once item 1 removes the event-loop bottleneck that currently makes this concurrency largely theoretical.
- **Expected impact**: **High** — directly multiplies effective upload throughput by the achievable concurrency factor, once the event-loop blocker is gone.
- **Complexity**: **Medium** — new scheduling component, though with a clear existing pattern to follow (`DownloadScheduler`).
- **Risk**: **Medium** — needs coordination with HTTP pool sizing (item 9) and local disk I/O capacity (many simultaneous `WriteBuffer` flushes) to avoid trading one bottleneck for another.
- **Confidence**: **Medium** — the mechanism is sound, but the achievable concurrency factor (how many simultaneous uploads before *something else* becomes the new ceiling) is unmeasured.
- **Currently limiting**: WriteBuffer, FUSE event loop (today); Download-scheduler-equivalent gap (no analogous upload throttling exists yet).

### 6. Reuse one Databricks SDK client across uploads instead of one per file (P13)

- **Description**: Construct the `WorkspaceClient` used by `upload_file` once (or per-process, with token-refresh handling) and reuse it, instead of creating a fresh instance — and a fresh underlying connection — for every one of the 554 files.
- **Files/modules involved**: [fuse4dbricks/api/uc_client.py](../../fuse4dbricks/api/uc_client.py) (`UnityCatalogClient.upload_file`, `_do_upload`, `_attempt`).
- **Why it helps**: Removes a fixed per-file connection-setup cost that repeats 554 times in this benchmark; the cost is small per call but additive across the whole dataset.
- **Expected impact**: **Medium** — real but bounded by how large the per-call fixed cost actually is (unmeasured; a few tens of ms per call would still only total single-digit seconds across 554 files, whereas a larger fixed cost would matter more).
- **Complexity**: **Small** — construct once, reuse; the main subtlety is preserving today's "fresh client → always-current token" behavior on 401.
- **Risk**: **Medium** — must explicitly re-implement the token-invalidate-and-retry behavior that today's fresh-client-per-call approach gets "for free."
- **Confidence**: **High** — the current behavior (fresh client per call) is directly confirmed in code; the fix is mechanical.
- **Currently limiting**: HTTP client (in the sense of connection/session reuse, not the shared pool used by metadata/downloads — this is the SDK's own, separate client).

### 7. Enable the kernel's write-back caching feature (P10)

- **Description**: Set `enable_writeback_cache = True` on `UnityCatalogFS`, letting the Linux kernel batch small/frequent `write()` calls into fewer, larger ones before forwarding them to this process.
- **Files/modules involved**: [fuse4dbricks/fs/operations.py](../../fuse4dbricks/fs/operations.py) (`UnityCatalogFS` class attribute).
- **Why it helps**: Confirmed via `pyfuse3`'s actual `Operations` base class that this is a real, exposed, currently-unused setting (unlike P02's `max_read`, which is not exposed at all). Fewer, larger `write()` calls compounds with item 1: even a non-blocking write path still has a per-call cost, and this reduces how many times it's paid.
- **Expected impact**: **Medium** — real, but secondary to item 1 (removing the *blocking* nature of each call matters more than reducing the *count* of calls).
- **Complexity**: **Small** — a one-line class attribute.
- **Risk**: **Medium** — behavior-changing: changes the timing/frequency assumptions the existing `WriteBuffer` pre-load-on-open logic was written against; needs a full write-path regression pass (same existing test suite, but must be re-run and scrutinized specifically for this change, not assumed to just work).
- **Confidence**: **Medium** — the mechanism is confirmed available; its interaction with this project's specific buffering design is untested.
- **Currently limiting**: FUSE event loop, WriteBuffer (call frequency, not blocking nature).

### 8. Stop the disk-cache cleanup pass from gating a finished download (P01)

- **Description**: Release a downloaded chunk to the waiting reader as soon as its bytes are known (already fully in memory), instead of waiting for `os.rename` + the LRU `evict()` pass + lock-protected bookkeeping to finish first. (Two options exist, see [P01-analysis.md](P01-analysis.md) §4: a low-risk "defer only `evict()`" version, or a more aggressive full decoupling.)
- **Files/modules involved**: [fuse4dbricks/storage/persistence.py](../../fuse4dbricks/storage/persistence.py) (`store_chunk_from_stream`, `evict`), [fuse4dbricks/fs/data_manager.py](../../fuse4dbricks/fs/data_manager.py) (`_fetch_chunk`, `_process_request`).
- **Why it helps**: `evict()`'s cost is unbounded (can delete an arbitrary number of old cache entries in a loop) and today sits squarely on the critical path of every cache-miss chunk fetch; this matters most when the disk cache is near its configured size limit.
- **Expected impact**: **Medium** — likely small for *this specific* 49 GB run if the configured cache budget has headroom, but potentially significant for repeated runs or larger datasets that push the cache to its limit — exactly the "TB-scale" regime named in the broader project goals.
- **Complexity**: **Medium** — the minimal ("defer only `evict()`") version is a contained change; the full version requires a new bounded background-task pattern.
- **Risk**: **Low** (minimal version) — reordering, not a new concurrency primitive.
- **Confidence**: **Medium** — the mechanism is certain; its contribution to *this specific* 463 s is unmeasured and plausibly small unless the cache is near its limit.
- **Currently limiting**: Disk cache.

### 9. Separate the HTTP connection pool used for metadata calls from the one used for chunk downloads (P03)

- **Description**: Give `get_path_metadata`/`get_path_contents`/`check_permissions`/`get_current_user_info` their own `httpx.AsyncClient`/pool, distinct from the one `download_chunk_stream` uses.
- **Files/modules involved**: [fuse4dbricks/api/uc_client.py](../../fuse4dbricks/api/uc_client.py) (`UnityCatalogClient.__init__`, `_request`, `close`).
- **Why it helps**: Prevents a long-held download connection from queuing a metadata call behind it ([P03-analysis.md](P03-analysis.md) quantifies a worst case of minutes under pool exhaustion). **Important recalibration from this benchmark**: this mechanism explains metadata contention *during concurrent downloads*, not the specific `find`/`ls`-during-**upload** stalls observed here, because uploads bypass this pool entirely via the SDK (item 6's client). It remains valuable, but ranks below item 1 for *this specific* symptom, and becomes more important specifically if item 4 (more download workers) is pursued.
- **Expected impact**: **Medium** — valuable and well-quantified, but not the mechanism behind the specific metadata numbers in this benchmark (those point to item 1 instead).
- **Complexity**: **Medium** — touches every metadata-plane call site plus `close()`.
- **Risk**: **Medium** — must preserve the shared retry/auth/backoff logic identically across both pools.
- **Confidence**: **High** — the contention mechanism itself is rigorously quantified; the scope correction (it's about downloads, not uploads) is also high-confidence, straight from reading which calls use which client.
- **Currently limiting**: HTTP client, Metadata layer (lock-free but connection-queue contention, when downloads are the contending traffic).

### 10. Make chunk size configurable / re-tune the 8 MB default (P06)

- **Description**: Expose `DataManager.chunk_size` (currently a hardcoded 8 MB constant used for download granularity, disk-cache file granularity, and the RAM-cache unit) as a tunable, and benchmark alternative values (e.g. 4/8/16 MB).
- **Files/modules involved**: [fuse4dbricks/fs/data_manager.py](../../fuse4dbricks/fs/data_manager.py), [fuse4dbricks/storage/persistence.py](../../fuse4dbricks/storage/persistence.py) (`_get_chunk_path`).
- **Why it helps**: A larger chunk size means fewer, larger network requests per file (less per-request overhead) but a longer connection hold time per chunk (interacts with item 9's contention mechanism) and coarser cache granularity; the right value is a genuine trade-off that depends on the network path (item 2) more than on this codebase alone.
- **Expected impact**: **Medium** — direction and magnitude are both uncertain without dedicated benchmarking.
- **Complexity**: **Medium** — touches cache-key format, on-disk file naming, and every existing chunk-math test.
- **Risk**: **Medium** — a wrong default could simultaneously increase memory use and worsen item 9's contention risk.
- **Confidence**: **Low** — explicitly the lowest-confidence item in this list; this is a "needs dedicated benchmarking before implementing" item, not a "likely to help" one.
- **Currently limiting**: Download scheduler, RAM cache, Disk cache (granularity of all three is tied to this one constant).

---

## Prioritized roadmap

Sorted strictly per the requested criteria: **(1) highest expected impact, (2) lowest implementation complexity, (3) lowest regression risk.** Where two items tie on impact and complexity, risk breaks the tie; where all three tie, the better-evidenced (higher-confidence) item is listed first, noted explicitly.

| Rank | Optimization | Impact | Complexity | Risk | Confidence |
|---|---|---|---|---|---|
| 1 | **P12** — Offload `WriteBuffer` I/O to a thread | Very High | Small | Low | High |
| 2 | **P09** — Verify network placement | Very High | Small | Low | Medium |
| 3 | **P11** — Overlap write with upload (feasibility spike first) | Very High | Large | High | Low |
| 4 | **P04** — Increase/configure download worker count | High | Small | Medium | Medium |
| 5 | **New** — Bounded concurrent upload pipeline | High | Medium | Medium | Medium |
| 6 | **P13** — Reuse SDK client across uploads | Medium | Small | Medium | High |
| 7 | **P10** — Enable FUSE write-back caching | Medium | Small | Medium | Medium |
| 8 | **P01** — Stop disk-cache cleanup gating a finished download | Medium | Medium | Low | Medium |
| 9 | **P03** — Separate HTTP pools (metadata vs. downloads) | Medium | Medium | Medium | High |
| 10 | **P06** — Configurable/re-tuned chunk size | Medium | Medium | Medium | Low |

**Note on rank 1 vs. 2**: both are "Very High / Small / Low"; P12 is listed first because it is a direct code change with High confidence and immediately actionable (a prior prototype already exists and passed the full test suite), whereas P09 is a measurement task whose *outcome* — not its cost — is what's uncertain (Medium confidence). Both should realistically happen in parallel, not strictly sequentially, since neither blocks the other.

**Note on rank 3 (P11)**: ranked by raw impact score as instructed, but its "Large/High/Low-confidence" profile means it should not literally be the third thing *built* — per its own entry, it should begin as a feasibility spike (confirm what the Files API actually supports) rather than a direct implementation, and realistically follows ranks 1, 4-6 in actual build order even though it out-ranks them on impact alone.

---

## Answering the governing question

> **"If the goal is to increase transfer throughput (MB/s), which changes should be implemented first?"**

Restricting to the items that directly move download/upload MB/s (as opposed to pure metadata responsiveness), in build order:

1. **P12** (WriteBuffer thread offload) — do this first, unconditionally. It is the only Small/Low-risk item with a plausible mechanism to move upload throughput at all, and it is a prerequisite for item 5 (bounded upload pipeline) to have any effect, since today's event-loop blocking makes multi-file upload concurrency largely moot regardless of how it's scheduled.
2. **P09** (network placement check) — do this in parallel with (1), not after. It is nearly free and determines how much further investment in download-side concurrency (P04) or upload-side parallelism (P11, the new upload pipeline) can realistically pay off.
3. **P04** (download worker count) — the most direct, lowest-cost lever on the download number specifically, once paired with awareness of P03's trade-off (or P03 itself, if download concurrency is raised significantly).
4. **The new bounded upload pipeline + P13 (client reuse)** — build together once (1) is done; this is where upload throughput moves from "incidentally better" to "deliberately scaled."
5. **P11** (overlap write/upload) — the long-term, highest-ceiling item, pursued only after a feasibility spike and only if 1-4 don't already close most of the upload-throughput gap at far lower risk.

Metadata responsiveness during upload (goal 3) is **not** primarily a throughput item — per this benchmark, it is best explained as a side effect of the same mechanism as upload throughput itself (P12). Implementing P12 is therefore expected to be the single highest-leverage item across **all three** stated goals simultaneously, which is why it is ranked first regardless of which of the three goals is weighted most heavily.
