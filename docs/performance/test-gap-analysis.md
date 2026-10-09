# Test Gap Analysis — fuse4dbricks

Static analysis of the test suite (`tests/`, 409 test functions across 15 files), focused on the four modules named in the performance-engineering effort:

- `fuse4dbricks/main.py`
- `fuse4dbricks/fs/data_manager.py`
- `fuse4dbricks/api/uc_client.py`
- `fuse4dbricks/storage/persistence.py`

No code was modified or executed to produce this report (the sandbox has no project virtualenv installed); findings are derived from reading every test file in full and cross-referencing against the corresponding source modules analyzed in [docs/performance/architecture-review.md](../performance/architecture-review.md).

P01/P02/P03 refer to the optimization-tracker entries defined in [.github/prompts/fuse4dbx-performance-engineer.md](../../.github/prompts/fuse4dbx-performance-engineer.md):

| ID | Optimization |
|---|---|
| P01 | Async disk-cache write |
| P02 | FUSE `max_read` negotiation |
| P03 | Separate HTTP pools |

---

## 1. Existing tests

### `main.py` — `tests/test_main.py` (8 tests, all sync/pure)

| Test | Covers |
|---|---|
| `test_parse_args_defaults` | default CLI values (mountpoint, unified_auth, allow_other, ram_cache_mb, ttl options) |
| `test_parse_args_no_unified_auth` | `--no-unified-auth` flag |
| `test_parse_args_allow_other_and_workspace` | `--allow-other`, `--workspace` |
| `test_is_system_account_true_for_low_uid` / `_false_for_user_uid` | `_is_system_account()` uid heuristic |
| `test_default_cache_dir_system_account` | `/var/cache/fuse4dbricks` branch |
| `test_default_cache_dir_xdg` | `XDG_CACHE_HOME` branch |
| `test_default_cache_dir_home_fallback` | `~/.cache/fuse4dbricks` fallback |

Indirect coverage: `tests/test_e2e_mount.py` spawns `python -m fuse4dbricks.main` as a real subprocess and exercises the full `async_main`/`start_fuse` path end-to-end (mount, readdir, getattr, read, write, rename, truncate, securable filters, single-principal mode, read-only mode). **This file is gated behind live Databricks credentials (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `FUSE4DBRICKS_TEST_VOLUME`) and FUSE availability, and is skipped in vanilla CI.**

### `fs/data_manager.py` — `tests/test_data_manager.py` (23 tests, trio)

| Area | Tests |
|---|---|
| Trivial/edge cases | `test_read_offset_at_or_beyond_file_size_returns_empty`, `test_read_offset_beyond_file_size_returns_empty` |
| Single-chunk read math | `test_read_single_chunk_full_file`, `test_read_single_chunk_partial_from_middle` |
| Multi-chunk read math | `test_read_exact_two_chunks`, `test_read_spanning_chunk_boundary`, `test_read_last_chunk_smaller_than_chunk_size`, `test_read_file_size_exact_multiple_of_chunk_size` |
| Error propagation | `test_read_raises_eio_when_chunk_is_none` |
| Prefetch window | `test_read_prefetch_not_triggered_at_start_of_first_chunk`, `test_read_prefetch_triggered_after_first_chunk`, `test_read_prefetch_stops_at_eof` |
| `_process_request` priority semantics | `test_process_request_high_priority_disk_hit_caches_in_ram`, `test_process_request_high_priority_miss_downloads_and_caches_in_ram`, `test_process_request_prefetch_skips_download_when_already_on_disk`, `test_process_request_prefetch_downloads_when_missing_but_not_cached_in_ram` |
| Cache-generation invalidation | `test_invalidate_path_bumps_generation`, `test_invalidate_path_changes_the_chunk_cache_key` |

Indirect coverage: `tests/test_operations.py` exercises `UnityCatalogFS.read/open/write` with `DataManager` **fully mocked** (`AsyncMock`), so it validates the operations-layer call contract (how many times/with what args `data_manager.read` is invoked per chunk-spanning `open(O_RDWR)`) but never runs real `DataManager` code.

### `api/uc_client.py` — three files, trio

**`tests/test_uc_client.py` (~30 tests, mocked `httpx.AsyncClient`):**
auth header injection, 401 → invalidate-and-retry, `_quote_path`, `_get_file_metadata` parsing (incl. RFC 7231 `Last-Modified`) and 404→`None`, `download_chunk_stream` Range header + 412→`UcPreconditionFailed`, 429/5xx retry+backoff (incl. `Retry-After` header honoring and exhaustion), transport-error (`httpx.ConnectError`) retry and exhaustion, `UcRateLimited` dataclass contract, `delete_file`/`delete_directory`/`create_directory` success and REST-error-to-domain-exception mapping (400/404/409), `_parse_retry_after` (seconds form, HTTP-date form, past-date clamp, unparseable), and `upload_file` 401-refresh-and-retry-once via the SDK path.

**`tests/test_uc_client_consistency.py` (3 tests, `respx`-mocked HTTP):**
412 precondition-failed on download, successful conditional download, download without a conditional header.

**`tests/test_uc_client_live.py` (10 tests, gated behind live Databricks credentials — skipped in vanilla CI):**
catalog listing smoke test, upload+delete round trip, create/delete directory, immediate read-after-write consistency for metadata/parent-listing/content, and real-API error-mapping documentation tests (`rmdir` on non-empty dir, `delete_directory` on a file path, `mkdir` conflicts).

### `storage/persistence.py` — `tests/test_persistence.py` (23 tests, trio)

Core store/retrieve round-trip, 256-shard path layout, generation-suffixed cache keys (differ-by-generation, no-collision-on-same-mtime), missing-chunk `None` returns, `chunk_exists` true/false **and** the "must not read file content" guarantee, `_get_chunk_path` purity (no directory side effect) and the corresponding "no empty shard dir left behind" tests, atomic tmp-file cleanup on a mid-stream exception, byte-budget LRU eviction, lazy-promotion LRU ordering (access-time bumped in the map without re-heapifying), startup discovery (`_graceful_init`) rebuilding `current_size`/`access_map`/`access_log`, stale-`.tmp` cleanup on init, age-based GC and disk-critical panic-mode GC (**both reimplemented inline in the test** rather than invoking the real `_background_maintenance` loop — see §3), the lock-reentrancy regression guard (`test_concurrent_store_triggers_eviction`), and self-healing re-indexing of a chunk found on disk but missing from `access_map`.

---

## 2. Coverage assessment

| Module | Assessment |
|---|---|
| `main.py` | **Weak in CI.** Only pure helper functions (arg parsing, cache-dir heuristics) are unit-tested. All orchestration logic (`async_main`, `start_fuse`, `_shutdown_on_signal`, `cli_entry_point`, `setup_logging`) is exercised only by the live-gated e2e suite, which does not run in standard CI and does not assert on the orchestration internals (signal handling, nursery composition, cache-dir permissions) — only on externally observable mount behavior. |
| `fs/data_manager.py` | **Strong for `DataManager.read()`'s byte-range/chunk-math and prefetch-window logic; weak-to-absent for everything below it.** `_read_chunk` (the coalescing + cache-tier fallthrough) is never exercised directly — every `read()` test patches `_read_chunk` out. `DownloadScheduler` (priority queue, promotion, wake channel, worker loop, error isolation, `close()`) has **zero** test coverage. `_fetch_chunk` (the real network-fetch-to-disk path) is untested except transitively via live e2e. |
| `api/uc_client.py` | **Strong for the HTTP retry/auth/error-mapping plumbing in `_request`; weak for the UC-metadata discovery layer and pagination.** `_get_catalogs/_get_catalog/_get_schemas/_get_schema/_get_volumes/_get_volume`, `get_current_user_info`, `check_permissions`, `_fetch_all_pages` (multi-page loop), and the path-depth dispatch in `get_path_metadata`/`get_path_contents` have no mocked unit test — they are only (optionally) exercised by the live-gated suite. |
| `storage/persistence.py` | **Strong for the data-path (store/retrieve/evict/shard/atomicity); weak for the background-maintenance path.** Both GC-related tests manually re-implement the age-expiry and disk-critical logic inline instead of invoking `_background_maintenance` itself, so a regression inside that method's actual code would not be caught by either test. `run_services()` (the nursery wiring that starts both background tasks) is never invoked. |

---

## 3. Missing coverage

### `main.py`
- `async_main`'s own logic is untested in CI: cache directory creation/`chmod 0o700` for `root/auth/data/writes`, `--clear-cache` invoking `clear_cache()`, the `writes_dir` startup sweep (`os.unlink` of leftover write-buffer tempfiles), workspace resolution precedence (`--workspace` vs `DATABRICKS_HOST` vs missing-both → `sys.exit(1)`), and component wiring (does `set_token_invalidation_callback` actually get attached before use, etc.).
- `start_fuse`: mount-option assembly (`fsname`, `noatime`, `allow_other`, `debug`), `pyfuse3.init` failure path, and the `finally: pyfuse3.close(unmount=True)` guarantee are untested outside the live e2e subprocess (where failures are hard to inject deterministically).
- `_shutdown_on_signal`: no test simulates SIGTERM/SIGINT/SIGHUP delivery and asserts the nursery's `cancel_scope` is cancelled and `stopped_evt` unblocks `async_main`'s `await stopped_evt.wait()`.
- `ExceptionGroup` logging branch in `async_main`'s `except ExceptionGroup as eg` is never triggered by a test.
- `cli_entry_point`: `KeyboardInterrupt` swallow and generic-`Exception` → `sys.exit(1)` branches are untested.
- Mountpoint-missing (`not os.path.isdir(mountpoint)` → `sys.exit(1)`) path is untested.

### `fs/data_manager.py`
- `DownloadScheduler`: no test of priority-queue ordering (priority vs. regular), promotion of a queued regular request to priority (`enqueue_request(high_priority=True)` while the same key is in `_requests_regular`), de-duplication (`if cache_key not in self._requests_priority`), the wake channel (`_poke_worker`/`_wake_recv`) actually waking an idle worker without polling, per-worker exception isolation in `_downloader` (a raised exception inside `_process_request` must be logged and NOT crash the worker/nursery), or `close()`'s `EndOfChannel` shutdown of idle workers.
- `_read_chunk`: the real coalescing path (`InflightCoalescer.join_or_lead` for a chunk id) is never driven directly — no test proves that two concurrent `_read_chunk` calls for the identical `(fs_path, chunk_id, mtime, gen)` result in exactly one `DownloadScheduler` enqueue and both callers observing the result.
- `_fetch_chunk`: no unit test asserts the `uc_client.download_chunk_stream` call parameters (`offset = chunk_size * chunk_id`, `length`, `if_unmodified_since=mtime`) or that the resulting bytes are what `persistence.store_chunk_from_stream` receives/returns.
- `DataManager.run_services`/`close()`: no test asserts the configured `num_workers` are actually started, or that `close()` unblocks all workers.
- `_get_chunk_from_cache_or_disk`: RAM-cache population-from-disk-hit path (`await self._ram_cache.put(...)` after a disk hit) is reached only incidentally inside `test_invalidate_path_changes_the_chunk_cache_key`; there's no dedicated test for the RAM-miss/disk-hit/RAM-promotion sequence.
- `fs/ram_cache.py`'s `RamCache` (the RAM tier `DataManager` depends on) has **no dedicated test file at all** (`get`/`put`/`delete`/`clear`/`stats`, LRU eviction ordering, `max_entries=0` no-op behavior) — it is only incidentally touched by `test_data_manager.py`'s `_process_request` tests, which construct a throwaway `RamCache(max_entries=8)`.

### `api/uc_client.py`
- `_fetch_all_pages`: no test drives a multi-page response (`next_page_token` present then absent) to confirm pages are concatenated and the loop terminates correctly.
- `_get_catalogs`/`_get_schemas`/`_get_volumes` (and their singular `_get_*` counterparts): field mapping (`created_at`/`updated_at` ms→s conversion, `uc_path` construction) is untested except via the live suite.
- `get_current_user_info` and `check_permissions` (effective-permissions endpoint, privilege-set union across paginated `privilege_assignments`) have no mocked unit test; `MetadataManager`'s own tests mock `uc_client` entirely, so this logic is exercised nowhere in CI.
- `get_path_metadata`/`get_path_contents` path-depth dispatch (root → catalog → schema → volume → file/dir) and the file/directory disambiguation fallback (`_get_directory_metadata` → fallback to `_get_file_metadata` and vice versa) are untested as a unit; only the leaf HEAD helpers are.
- `_with_retry`'s SDK-transient-error branch (`TooManyRequests`/`TemporarilyUnavailable`/`InternalError`/`DeadlineExceeded`) used by `upload_file` is never triggered by a test — only `Unauthenticated` (401-equivalent) is covered for uploads.
- Connection-pool configuration (`httpx.Limits(max_keepalive_connections=20, max_connections=50)`, single shared `httpx.AsyncClient`) has no assertion anywhere — relevant baseline for P03.
- No HTTP/2 configuration or test exists — relevant baseline for P07 (not in scope here, noted for completeness).

### `storage/persistence.py`
- `_background_maintenance`'s actual loop body (including its `trio.sleep(3600)` cadence and the real lock-scoped expiry/panic logic) is never invoked by a test — both GC tests copy the logic inline instead of calling the method, so a bug introduced inside `_background_maintenance` itself would not be caught.
- `run_services()` (nursery wiring of `_graceful_init` + `_background_maintenance`) is never exercised.
- `clear_cache()` (module-level function wiping `*.tmp`/`*.bin` recursively, used by `--clear-cache`) has no test.
- Concurrent writers: no test has two `trio` tasks call `store_chunk_from_stream` for the **same** cache key concurrently (potential double-counting of `current_size`, or a `.tmp.{pid}` collision if ever run multi-process) — existing concurrency test (`test_concurrent_store_triggers_eviction`) only covers sequential calls for **different** keys that trigger eviction.
- No test exercises real concurrent `trio` tasks (via a nursery) racing `store_chunk_from_stream` / `retrieve_chunk` / `evict` against each other — all existing tests are sequential `await` calls within a single task.

---

## 4. Concurrency coverage

| Concurrency primitive | Where used | Test coverage |
|---|---|---|
| `InflightCoalescer` (generic) | `fs/utils.py`, used by `MetadataManager` and `DataManager` | **Covered generically** in `tests/test_utils.py` (`join_or_lead`/`notify_done`, a follower spawned via `trio.open_nursery()` and awaiting the leader) — the only test in the suite that runs genuinely concurrent `trio` tasks against shared state. |
| `InflightCoalescer` applied to chunk downloads | `DataManager._read_chunk`, `_request_fetch_ahead_chunks` | **Not covered** — no test drives two concurrent readers of the same chunk through `DataManager.read()` (real, unmocked) to prove exactly one download occurs. |
| `DownloadScheduler` (priority/regular queues, worker pool, wake channel) | `fs/data_manager.py` | **Not covered at all.** No test starts workers via `run_services`, enqueues requests, and asserts worker concurrency/priority ordering/exception isolation. |
| `trio.Lock` (`MetadataManager._cache_lock`/`_permissions_lock`/`_principal_cache_lock`) | `fs/metadata_manager.py` | Out of this report's module scope, but noted: only exercised sequentially in `test_metadata_manager.py` (no concurrent-task races). |
| `trio.Lock` (`DiskPersistence.lock`) | `storage/persistence.py` | Exercised only by sequential `await` calls in a single task; `test_concurrent_store_triggers_eviction` is a regression test for a **reentrancy** bug (lock-held-during-evict), not a true multi-task race. |
| `trio.Lock` (`DownloadScheduler._requests_lock`, `RamCache._lock`) | `fs/data_manager.py`, `fs/ram_cache.py` | **Not covered** — neither is driven by any test. |
| Top-level `trio.Nursery` (persistence services, download workers, FUSE loop, signal task) | `main.py` | **Not covered in CI** — only implicitly exercised by the live-gated e2e subprocess, which cannot assert on internal task composition or clean-cancellation ordering. |
| Per-read `trio.Nursery` (`DataManager.read`, one task per chunk for multi-chunk reads) | `fs/data_manager.py` | Exercised **functionally** (multi-chunk tests assert correct byte assembly) via a mocked `_read_chunk`, but never asserts the chunks actually download **concurrently** (e.g., via timing/ordering instrumentation) rather than sequentially — the mock makes both indistinguishable. |
| `httpx.AsyncClient` concurrent in-flight requests / pool limits | `api/uc_client.py` | **Not covered** — all `uc_client` tests issue one request at a time against a mocked client; no test asserts behavior under concurrent requests or pool exhaustion. |

**Summary:** genuine multi-task concurrency testing exists only for the generic `InflightCoalescer` primitive. Every concurrency-sensitive component built on top of it (`DownloadScheduler`, chunk-level coalescing in `DataManager`, the HTTP connection pool, and the top-level nursery in `main.py`) is validated only through sequential/mocked unit tests or the live-gated e2e suite.

---

## 5. Cache coverage

| Cache tier | Module | Test coverage |
|---|---|---|
| RAM chunk cache (`RamCache`) | `fs/ram_cache.py` | **No dedicated test file.** Indirectly touched by `test_data_manager.py`'s `_process_request` tests (which replace the tiny default instance with `RamCache(max_entries=8)` to make `get`/`put` non-no-op). LRU eviction ordering, `max_entries=0` behavior, `delete`/`clear`/`stats` are entirely untested. |
| Disk chunk cache (`DiskPersistence`) | `storage/persistence.py` | **Well covered** for the data path: store/retrieve, sharding, generation-keyed collision avoidance, byte-budget eviction, lazy-LRU promotion, atomicity on stream failure, startup discovery/self-healing. **Not covered** for the background maintenance loop as actually implemented (see §3). |
| Cache-generation invalidation (`DataManager._write_generation`) | `fs/data_manager.py` | Covered (`test_invalidate_path_bumps_generation`, `test_invalidate_path_changes_the_chunk_cache_key`). |
| Metadata attribute/dir/negative/permission/principal caches | `fs/metadata_manager.py` | Out of this report's four-module scope; `test_metadata_manager.py` exists and is extensive, but was not analyzed in depth here. |
| Write buffer (tempfile-backed, per open handle) | `fs/write_buffer.py` | Out of scope; `test_write_buffer.py` exists separately. |
| End-to-end cache coherency (RAM → disk → network, all real) | cross-cutting | **Not covered by any mocked unit test** — `test_data_manager.py` always mocks `_read_chunk`, so a read that should hit RAM, then disk, then network, through the *real* `DataManager`, is never exercised end-to-end outside live e2e. `test_persistence.py` exercises disk-only behavior with no `DataManager`/`RamCache` involved. |

---

## 6. Performance optimization coverage

Cross-referencing the optimization tracker (P01–P09) against existing tests:

| ID | Optimization | Current test coverage of the *affected* code |
|---|---|---|
| P01 | Async disk-cache write | `store_chunk_from_stream` is already `async` (uses `trio.open_file`/`trio.to_thread`); existing tests validate correctness (atomicity, eviction) but **none measure or assert on blocking/latency characteristics** of the write relative to serving the read — no baseline exists to compare against. |
| P02 | FUSE `max_read` negotiation | **No test touches `pyfuse3.init`'s `mount_options`** beyond `fsname`/`noatime`/`allow_other`/`debug` presence (and that only via live e2e, not asserted explicitly). No test measures request sizes/counts reaching `UnityCatalogFS.read`. |
| P03 | Separate HTTP pools | **No test asserts on `httpx.Limits`/connection-pool configuration at all**; the single shared `AsyncClient` is constructed once per `UnityCatalogClient` and mocked out entirely in `test_uc_client.py`, so pool-sizing or multi-pool behavior cannot regress-test against today's suite. |
| P04 | Configurable worker count | `DataManager(num_workers=...)` constructor arg exists and is passed `num_workers=1` in test fixtures, but no test asserts the configured count of workers is actually started by `run_services`. |
| P05 | Configurable prefetch window | The *current* fixed window (1 vs. 10 chunks) is well covered (§1), giving a solid regression baseline to build a configurable version against. |
| P06 | Configurable chunk size | `chunk_size = 8 * 1024 * 1024` is hardcoded; all chunk-math tests implicitly assume this constant (`CHUNK = 8 * 1024 * 1024` is redefined in the test file) rather than parametrizing over it — a configurable chunk size would need the existing tests re-parametrized. |
| P07 | HTTP/2 experiment | No coverage; `httpx.AsyncClient` construction has no test. |
| P08 | RAM cache sizing documentation | N/A for tests (documentation-only). |
| P09 | Network placement documentation | N/A for tests (documentation-only). |

---

## 7. Tests required for P01, P02, P03

### P01 — Async disk-cache write

Goal (inferred): decouple the on-disk persistence of a freshly-downloaded chunk from the critical path that unblocks waiting readers, so a cache-miss read is satisfied as soon as the chunk is in RAM, while the disk write completes in the background — reducing time-to-first-byte on cold reads without weakening durability of the disk cache.

Required tests (none exist today):

1. **Latency/ordering unit test** — a fake `persistence.store_chunk_from_stream` that blocks (via a `trio.Event`) until explicitly released; assert `DataManager._process_request`/`_fetch_chunk` allows the waiting reader (`InflightCoalescer.notify_done` → follower wake-up) to proceed **before** the disk write completes, once RAM is populated.
2. **Durability test** — after an async-disk-write read completes, poll/await until the background write finishes and assert the chunk is present on disk via `persistence.chunk_exists`/`retrieve_chunk` (so nothing is silently dropped).
3. **Crash/failure-isolation test** — simulate the background disk write raising (e.g., disk full / `OSError`) after bytes were already served to the reader from RAM; assert the read still succeeded, the error is logged (not propagated to the FUSE reply), and the chunk is **not** falsely recorded as cached on disk (no partial/corrupt `.bin` file left — reuse the existing tmp+rename atomicity guarantee).
4. **Coalescing-still-correct test** — two concurrent readers request the same cache-miss chunk while the (now-async) disk write is still in flight; assert only one network fetch occurs and both readers receive identical bytes.
5. **Eviction-accounting race test** — a background disk write completes out of order relative to a concurrent `evict()` call; assert `current_size`/`access_map`/`access_log` remain consistent (no double-counting, no eviction of a chunk whose write hasn't registered yet, no negative/incorrect `current_size`).
6. **`DiskPersistence` background-task lifecycle test** — if implemented via a nursery-spawned task per write (or a bounded background-writer pool), test that `DataManager.close()`/process shutdown drains or cancels in-flight background writes without leaking tasks or leaving `.tmp` files behind (extend `test_init_cleans_old_tmp_files`-style assertions).
7. **Benchmark harness** (per the performance-engineer workflow's "Benchmark Required" step) — a microbenchmark comparing time-to-first-byte and total throughput for a cold multi-chunk sequential read, before/after, across the 1 GB / 10 GB / 100 GB workloads defined in [.github/PERFORMANCE_NOTES.md](../../.github/PERFORMANCE_NOTES.md).

### P02 — FUSE `max_read` negotiation

Goal (inferred): negotiate a larger kernel-side `max_read` (and/or `max_write`) mount option so sequential `cp`/read workloads issue fewer, larger FUSE requests, reducing per-request overhead relative to the 8 MiB chunk size.

Required tests (none exist today):

1. **Mount-option construction unit test** — extend `tests/test_main.py` (or a new `test_start_fuse_options` test) to assert `start_fuse`'s `mount_options` list contains the negotiated `max_read=<N>` (and/or `max_write=<N>`) value when configured, in addition to the already-implicit `fsname`/`noatime`/`allow_other`/`debug` options.
2. **CLI plumbing test** — if a new flag (e.g. `--max-read-kb`) is introduced, a `test_main.py`-style test for its default value, custom value, and validation (reject non-positive or absurdly large values) — mirroring the existing `test_parse_args_*` pattern.
3. **Bounds/guard-rail test** — assert the implementation clamps or rejects a configured value above the kernel/`pyfuse3` ceiling (historically 128 KiB on older kernels; verify against the `pyfuse3` version pinned in `pyproject.toml`) rather than passing an invalid mount option straight to `pyfuse3.init`.
4. **Request-size/throughput e2e test** — extend `tests/test_e2e_mount.py` (live-gated) with a test that performs a large sequential read and asserts on either (a) the number of `UnityCatalogFS.read()` invocations observed (e.g., via a counting wrapper/log) relative to file size, or (b) wall-clock throughput improvement versus the pre-change baseline — since kernel-level `max_read` negotiation cannot be verified without a real mount.
5. **Regression test for existing mount options** — a test confirming `allow_other`/`debug` options are still appended correctly alongside the new negotiated option (guards against accidental list-construction bugs during the change).
6. **Benchmark harness** — sequential read throughput and request-count comparison (via `strace -e trace=read` or an internal counter) for 1 GB/10 GB/100 GB files before/after, per the performance-engineer workflow's benchmark requirement.

### P03 — Separate HTTP pools

Goal (inferred): split the single shared `httpx.AsyncClient` in `UnityCatalogClient` into two independently-pooled clients — one tuned for small/fast control-plane calls (catalog/schema/volume listing, permission checks, HEAD metadata) and one for large data-plane streaming calls (chunk download, upload) — so large transfers cannot starve metadata-latency-sensitive operations via connection-pool contention.

Required tests (none exist today):

1. **Construction unit test** — assert `UnityCatalogClient.__init__` creates two distinct `httpx.AsyncClient` instances with independently configurable `httpx.Limits` (e.g., `metadata_max_connections`, `data_max_connections`), replacing/extending the current single-client assumption baked into the `client` fixture in `tests/test_uc_client.py`.
2. **Routing unit tests** — for each metadata-plane method (`get_path_metadata`, `get_path_contents`, `check_permissions`, `get_current_user_info`, `_get_catalogs`/`_get_schemas`/`_get_volumes`), assert the request is sent via the metadata-pool client (e.g., by mocking both clients separately and asserting which one's `.send` was called). Mirror for data-plane methods (`download_chunk_stream`, `upload_file`).
3. **`close()` completeness test** — assert `UnityCatalogClient.close()` calls `aclose()` on **both** underlying clients (extend/replace any implicit single-client close assumption); a regression here would leak connections from whichever pool isn't closed.
4. **401/retry/backoff parity test** — re-run the existing `test_request_401_retry_success`, `test_request_429_retries_then_succeeds`, `test_request_5xx_retries_then_succeeds`, and transport-error retry tests against **both** pools (parametrize the existing tests over "metadata client" and "data client") to confirm the shared `_request`/`_with_retry` logic still applies uniformly after the split.
5. **Non-interference / head-of-line-blocking test** — using a fake transport where a data-plane request blocks on a `trio.Event` (simulating a large in-flight download), assert a concurrent metadata-plane request still completes without waiting on the blocked data request — the core behavioral claim of this optimization. This requires a true concurrent `trio` test (two tasks in a nursery), which the current suite has no precedent for against `uc_client`.
6. **Pool-sizing configuration test** — if CLI flags are added for either pool's `max_connections`/`max_keepalive_connections`, add `test_main.py`-style parsing/default/validation tests.
7. **Backward-compatibility test** — run the full existing `test_uc_client.py`/`test_uc_client_consistency.py` suites unmodified against the new dual-pool client (via the existing `client` fixture updated to the new constructor) to confirm no behavioral regression for callers that don't care which pool served the request.
8. **Benchmark harness** — concurrent-load benchmark measuring metadata-call (e.g. `getattr`) p50/p99 latency while a large sequential transfer is in progress, before/after the split, per the performance-engineer workflow's benchmark requirement.
