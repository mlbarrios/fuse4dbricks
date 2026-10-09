# Optimization Review — P01, P02, P03

Static-analysis review of the three highest-priority entries in the optimization tracker ([.github/prompts/fuse4dbx-performance-engineer.md](../../.github/prompts/fuse4dbx-performance-engineer.md)). No code was modified or executed; `pyfuse3` is not installed in this environment, so FUSE-kernel-negotiation claims are verified against documented libfuse/kernel behavior rather than a live mount, and are flagged accordingly.

| ID | Optimization | Issue verified? |
|---|---|---|
| P01 | Async disk-cache write | **Yes** — the read-cache write path genuinely blocks the waiting reader behind disk I/O + eviction. A related, likely more impactful bug was also found in the write path (see §P01 "Related finding"). |
| P02 | FUSE `max_read` negotiation | **Partially** — the code fact (no `max_read`/`max_write` mount option is set) is confirmed. Whether this is an actual bottleneck depends on kernel/libfuse auto-negotiation behavior that cannot be confirmed without a live mount. |
| P03 | Separate HTTP pools | **Yes, but narrower than the tracker title implies** — metadata and chunk-download traffic do share one pool; uploads already bypass it entirely via a separate SDK-internal client. |

---

## P01 — Async disk-cache write

### Issue verification

Confirmed. Trace of a cache-miss read:

1. `DataManager._process_request` ([fuse4dbricks/fs/data_manager.py](../../fuse4dbricks/fs/data_manager.py)) calls `await self._fetch_chunk(chunk_request)` for a "high" priority (on-demand) request.
2. `_fetch_chunk` calls `await self.persistence.store_chunk_from_stream(...)`.
3. `DiskPersistence.store_chunk_from_stream` ([fuse4dbricks/storage/persistence.py](../../fuse4dbricks/storage/persistence.py)) does, **in sequence, all before returning the bytes it already has in memory**:
   - stream the chunk to a `.tmp` file (`async with await trio.open_file(...)`),
   - `os.rename` the tmp file into place,
   - `await self.evict(bytes_written)` — a loop that can pop and delete multiple LRU entries from disk if the cache is near its size budget,
   - acquire `self.lock` and update `current_size`/`access_map`/`access_log`.
4. Only after all of that does `store_chunk_from_stream` return `bytes(result)` back up through `_fetch_chunk` → `_process_request`.
5. `_process_request`'s `finally: await self._inflight_coalescer.notify_done(cache_key)` — which wakes every reader blocked in `_read_chunk`'s `await wait_event.wait()` — does not fire until step 4 completes.

So every waiting reader (the leader *and* any coalesced followers) is held up by the full disk-persist-and-evict sequence even though the chunk's bytes were already fully available in the in-memory `result` bytearray as soon as the network stream finished. The `evict()` loop in particular can perform an unbounded number of synchronous-ish delete operations (via `trio.to_thread.run_sync(os.remove, ...)`) on the critical path when the cache is near its configured size limit — a cold TB-scale sequential transfer against a tightly-sized disk cache is exactly the scenario where this matters most.

Note: the write itself does not block the trio *event loop* (it's `trio.open_file`, which offloads blocking syscalls to a thread), but it does block the *task the reader is waiting on*, which is the latency that matters to the FUSE caller.

### Affected files
- [fuse4dbricks/storage/persistence.py](../../fuse4dbricks/storage/persistence.py) — `DiskPersistence.store_chunk_from_stream`, `DiskPersistence.evict`.
- [fuse4dbricks/fs/data_manager.py](../../fuse4dbricks/fs/data_manager.py) — `DataManager._fetch_chunk`, `DataManager._process_request`, `DataManager._read_chunk` (consumer of the coalescing wake-up).

### Affected methods
`DiskPersistence.store_chunk_from_stream`, `DiskPersistence.evict`, `DataManager._fetch_chunk`, `DataManager._process_request`, `DataManager._read_chunk`, `DataManager.run_services`/`close` (if a background-writer task pool is introduced).

### Implementation complexity
**Medium.** The read-serving half (populate `RamCache` and release waiting readers) must be split from the persist-to-disk half (rename + evict + bookkeeping), which requires:
- Returning/caching the assembled bytes and calling `notify_done` as soon as the stream finishes, *before* `evict`/rename/bookkeeping run.
- Moving the rename+evict+bookkeeping sequence into a background task, which needs a nursery handle inside `DataManager`/`DiskPersistence` (today only `DownloadScheduler` holds worker tasks; `DiskPersistence` has no background-task pool of its own beyond `_graceful_init`/`_background_maintenance`).
- Bounding the number of in-flight background persists (unbounded fire-and-forget tasks risk memory/disk-I/O pressure under a very high prefetch/worker count).
- Keeping `chunk_exists`/`retrieve_chunk` correct for any other caller that queries the same path while the background persist is still in flight (they must not see a half-written `.tmp` file; the existing tmp+rename atomicity already protects this as long as the rename itself isn't skipped).
- Isolating background-persist failures the same way `DownloadScheduler._downloader` already isolates download failures, so a failed disk write never crashes the nursery.

### Risks
- **Eviction/size-budget drift**: `current_size` is only updated after a background persist lands; multiple in-flight background writes could let the cache temporarily exceed `max_size_gb` by up to (in-flight count × chunk_size) before eviction catches up.
- **More complex failure semantics**: a background persist failure must be swallowed/logged without affecting the read that already succeeded — a new error-isolation contract to get right and test (none of today's tests cover this path, see [docs/performance/test-gap-analysis.md](test-gap-analysis.md) §3/§7).
- **No correctness risk to the coalescing guarantee itself**: `InflightCoalescer` still gates one fetch per `(fs_path, chunk_id, mtime, gen)`, so decoupling disk-persist timing doesn't introduce duplicate downloads.
- **Process-crash window**: if the process dies after serving bytes to a reader but before the background persist lands, the chunk is simply absent from disk on restart (safe — it just re-downloads later), not a correctness bug.

### Expected performance gain
**Low–Medium, workload-dependent.** The portion of latency this removes is the disk-write + eviction + lock-bookkeeping time, which is a small fraction of total chunk-fetch latency when: the cache directory is on fast local storage, and the cache has headroom (no eviction triggered). It becomes **more significant** when: the cache directory sits on slower/networked storage, the cache is operating near its configured size limit (frequent multi-item eviction loops on the hot path), or the network path to Databricks is very fast (high-bandwidth link), in which case the disk-write/evict overhead becomes a comparatively larger fraction of an otherwise-short chunk fetch. Expect the clearest win specifically on the **time-to-first-byte of a cold multi-chunk sequential read near the cache's size limit**, not on steady-state throughput (RAM-cache hits and already-on-disk chunks are unaffected).

### Related finding (same theme, not in the tracker)

`fs/write_buffer.py`'s `WriteBuffer` class is **entirely synchronous** — `write()`, `read()`, `truncate()`, `flush_to_disk()`, and `finalize()` all call the underlying `tempfile` object's blocking methods directly, with no `trio.to_thread.run_sync` wrapper (unlike every disk operation in `DiskPersistence`, which is careful to use `trio.open_file`/`trio.to_thread.run_sync` throughout). `WriteBuffer` is invoked directly from async FUSE handlers in [fuse4dbricks/fs/operations.py](../../fuse4dbricks/fs/operations.py) — `write()` (every `write` syscall), `open()`'s not-`O_TRUNC` pre-load loop, and `_upload_if_dirty()`'s `flush_to_disk()`/upload trigger. Each such call performs a **blocking disk syscall directly on the trio event-loop thread**, stalling every other concurrent coroutine in the whole process (all other FUSE requests, all chunk downloads, all HTTP I/O) for the syscall's duration. This is arguably a more impactful instance of "disk write is not async" than the read-cache path above, since it affects the *entire event loop*, not just the waiting reader, and triggers on every single `write()` call during a large upload (e.g. `cp` into the mount). If pursuing P01, consider including this in the same unit of work — same theme, same inferred intent, same test strategy class (needs background-thread offload + tests asserting the event loop stays responsive during a write-heavy workload).

---

## P02 — FUSE `max_read` negotiation

### Issue verification

**Code fact confirmed.** [fuse4dbricks/main.py](../../fuse4dbricks/main.py)'s `start_fuse` builds:

```python
mount_options = ["fsname=fuse4dbricks", "noatime"]
if allow_other: mount_options.append("allow_other")
if debug_mode: mount_options.append("debug")
```

No `max_read=`/`max_write=` option is ever added, and there is no corresponding CLI flag in `parse_args`. The filesystem therefore relies entirely on whatever default the kernel/libfuse/pyfuse3 stack negotiates.

**Cannot be fully confirmed as a bottleneck without a live mount.** The practical effect of this gap depends on facts this sandbox cannot check (no `pyfuse3`/libfuse/kernel available to introspect):
- Historically, FUSE's low-level kernel driver caps a single read request at 32 pages (128 KiB on x86). The legacy `max_read` mount option can only *lower* this cap for the (now largely unused) high-level `fuse_main` API — for the low-level API `pyfuse3` uses, the request-size ceiling is governed by the kernel's negotiated `max_write`/`max_pages` during `FUSE_INIT`, not by an `-o max_read=` string.
- On kernels ≥4.20 with libfuse ≥3.1 (the `FUSE_CAP_MAX_PAGES` capability), this negotiation can already raise the effective max request size up to ~1 MiB automatically, with no explicit mount option required — in which case adding a `max_read` option would be a no-op on modern stacks.
- On older kernels/libfuse, the effective cap stays at 128 KiB regardless of what's requested.

Given `DataManager.chunk_size = 8 * 1024 * 1024` (8 MiB), a sequential read of one logical chunk is currently split into either ~8 (at a hypothetical 1 MiB negotiated size) or ~64 (at the legacy 128 KiB cap) separate kernel `read()` requests reaching `UnityCatalogFS.read()` — each a distinct async task dispatch, lock-touch on `_open_state`, and (for `O_RDWR` handles) a `WriteBuffer.read()` call. The *data* itself is unaffected (it's already in RAM after the first sub-read triggers the chunk fetch), so this is purely a per-request-overhead question, not a network-efficiency one.

**Verdict:** the configuration gap is real; the magnitude of its impact requires runtime instrumentation (e.g., counting `UnityCatalogFS.read()` invocations per MB transferred, or `strace -e read` on the mount) on the actual deployment target (Posit Workbench kernel/libfuse combination) before investing further.

### Affected files
- [fuse4dbricks/main.py](../../fuse4dbricks/main.py) — `start_fuse` (mount option list), `parse_args` (if a new CLI flag is added).

### Affected methods
`start_fuse`, `parse_args`. No changes needed in `fs/operations.py` — `UnityCatalogFS.read()` already handles whatever offset/length the kernel sends; it has no fixed assumption about request size.

### Implementation complexity
**Low** for the code change itself (append one more string to `mount_options`, optionally gate it behind a new CLI flag with validation). **Medium effort to validate** — requires instrumented, live-mount testing to determine whether the option has any effect at all on the target kernel/libfuse version, and to measure the actual request-size reduction (see required tests in [docs/performance/test-gap-analysis.md](test-gap-analysis.md) §7, P02).

### Risks
**Low.**
- Setting an unsupported or out-of-range value is typically clamped/ignored by the kernel rather than causing a mount failure, but this should be verified on the target platform before rollout (unknown behavior on very old kernels is an unknown, not a confirmed risk).
- A larger negotiated request size modestly increases per-request kernel-side buffer allocation; negligible at the concurrency levels this project runs at.
- No interaction with correctness: chunk assembly in `DataManager.read()` is already request-size-agnostic.

### Expected performance gain
**Low–Medium, and conditional.** If the deployment's kernel/libfuse combination already auto-negotiates a larger request size (likely on any reasonably modern Linux + libfuse ≥3.1), this change yields **~0 additional gain** — the capability is already in effect. If the deployment is capped at the legacy 128 KiB, explicitly raising it reduces `read()` syscall/task-dispatch count proportionally (e.g., 128 KiB → 1 MiB is an ~8x reduction in requests per chunk), which mainly reduces CPU/context-switch overhead for sequential throughput workloads — unlikely to be the dominant cost compared to network transfer time for large files, but plausibly measurable for the "many sequential files" and "random read" benchmark workloads named in the performance-engineer prompt. **Recommend instrumenting before implementing** (the prompt's own "Performance First Rules" call for this when data is missing).

---

## P03 — Separate HTTP pools

### Issue verification

**Confirmed, with an important scoping correction.** [fuse4dbricks/api/uc_client.py](../../fuse4dbricks/api/uc_client.py) constructs exactly one `httpx.AsyncClient`:

```python
self.client = httpx.AsyncClient(
    base_url=self.base_url,
    timeout=httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=30.0),
    limits=httpx.Limits(max_keepalive_connections=20, max_connections=50),
    follow_redirects=True,
)
```

This single client/pool is shared, via the internal `_request()` helper, by **both**:
- metadata/control-plane calls: `get_path_metadata`, `get_path_contents`, `check_permissions`, `get_current_user_info`, `_get_catalogs`/`_get_schemas`/`_get_volumes` and their singular counterparts, `delete_file`, `delete_directory`, `create_directory`;
- the data-plane streaming call: `download_chunk_stream`.

One `UnityCatalogClient` instance is created in `main.py` and handed to **both** `DataManager` and `MetadataManager`, confirming metadata and chunk-download traffic genuinely contend for the same 50-connection/20-keepalive pool.

**Scoping correction:** `upload_file` does **not** go through `self.client` at all — it constructs a brand-new `databricks.sdk.WorkspaceClient` (with its own internal HTTP session) per call and runs it in `trio.to_thread.run_sync`. Uploads are therefore **already isolated** from the shared pool. This is a double-edged finding: it validates part of the "separate pools" intuition for the write path, but also surfaces a **separate, unaddressed inefficiency** — each upload constructs a fresh SDK client (and thus a fresh TCP+TLS session, no keep-alive reuse) rather than reusing one across calls, which is its own (different) cost worth tracking if large-file-upload throughput work continues.

So the actual, verified contention surface for P03 is narrower than "separate pools" suggests: it's specifically **metadata calls vs. chunk downloads**, not metadata vs. uploads. At today's defaults (`num_workers=10` download workers, `max_connections=50`), there is headroom (≥40 connections free for metadata even if every download worker is busy), so contention is plausible but not clearly severe *at default settings*. It becomes a sharper concern if a future optimization (P04, configurable worker count) raises `num_workers` toward or past 50 for TB-scale transfers, or under workloads that combine a large sequential transfer with heavy concurrent metadata traffic (e.g., `find`/`du`/`rsync --dry-run` scans of a large tree during a copy).

### Affected files
- [fuse4dbricks/api/uc_client.py](../../fuse4dbricks/api/uc_client.py) — client construction, `_request`, `download_chunk_stream`, all metadata-plane methods, `close`.
- [fuse4dbricks/main.py](../../fuse4dbricks/main.py) — constructs the single `UnityCatalogClient` instance (would need, at most, additional constructor parameters/CLI flags for per-pool sizing).

### Affected methods
`UnityCatalogClient.__init__`, `UnityCatalogClient._request`, `UnityCatalogClient.download_chunk_stream`, `UnityCatalogClient.close`, and (by routing, not by code change) every metadata-plane method listed above.

### Implementation complexity
**Medium.** Requires:
- Introducing a second `httpx.AsyncClient` with its own `httpx.Limits`.
- Parameterizing `_request()` (and the methods that call it) by which client to use — currently `_request` hardcodes `self.client`.
- Routing every call site to the correct pool (straightforward but must be done exhaustively and kept correct as new methods are added).
- Updating `close()` to close both clients.
- Re-validating that the shared 401/retry/backoff logic in `_request`/`_with_retry` behaves identically regardless of which pool is used (it should, since it's pool-agnostic logic, but needs test coverage per pool — see [docs/performance/test-gap-analysis.md](test-gap-analysis.md) §7, P03).

### Risks
- **Resource duplication**: two pools mean two sets of idle keep-alive sockets/DNS caches — small in absolute terms (tens of connections) but non-zero.
- **Pool-sizing miscalibration**: splitting a 50-connection budget into, say, 20 (metadata) + 30 (data) without load data could under-provision one side relative to actual usage; needs a load test to size correctly rather than guessing.
- **`close()` omission bug**: a missed `aclose()` on one of the two clients would leak connections — mechanical but real risk, mitigated by the dedicated test in the gap-analysis doc.
- **Divergence risk over time**: with two code paths for "send a request," future methods could accidentally use the wrong pool unless routing is centralized/enforced (e.g., via a single parameterized `_request(..., pool="data"|"metadata")` rather than two near-duplicate call chains).
- **Interacts with rate limiting**: Databricks-side throttling is typically per-token/per-workspace, not per-connection, so splitting pools does not change the retry/backoff-on-429 behavior, but worth confirming it doesn't change effective request concurrency in a way that trips rate limits sooner.

### Expected performance gain
**Low at today's default settings, Medium-to-High as a preventive/scaling measure.** With `num_workers=10` and `max_connections=50`, outright pool exhaustion is unlikely under default configuration — so an isolated before/after benchmark at defaults may show little difference. The real value is in **removing a scaling ceiling**: it directly unblocks safely raising download concurrency (P04) without risking metadata-call starvation, and it protects latency-sensitive `getattr`/`readdir` calls during sustained large transfers regardless of worker count. Recommend pairing this work with a concurrent-load benchmark (metadata p50/p99 latency during an in-progress large download, before/after) rather than a sequential-throughput benchmark, since the latter will not surface the contention this change addresses.

---

## Summary

| ID | Issue confirmed | Primary affected files | Complexity | Risk | Expected gain |
|---|---|---|---|---|---|
| P01 | Yes (read-cache path); related write-path bug also found | `storage/persistence.py`, `fs/data_manager.py` (+ `fs/write_buffer.py` for the related finding) | Medium | Low–Medium (eviction-accounting races, new failure-isolation surface) | Low–Medium, workload-dependent (bigger on slow cache storage or near size limit) |
| P02 | Partially (config gap confirmed; impact unconfirmed without live mount) | `main.py` | Low (change) / Medium (validation) | Low | Low–Medium, conditional on kernel/libfuse auto-negotiation already in effect or not |
| P03 | Yes, narrower scope (downloads vs. metadata; uploads already isolated) | `api/uc_client.py`, `main.py` | Medium | Low–Medium (resource duplication, pool-sizing, `close()` completeness) | Low now / Medium–High as a scaling enabler for P04 |
