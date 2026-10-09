# P12 Implementation — `WriteBuffer` Async/Thread Offload

Implements catalog item P12 from [performance-improvement-roadmap.md](performance-improvement-roadmap.md), following the determination in [P12-analysis.md](P12-analysis.md). Scope strictly limited to P12, per the request this work was done under — no other roadmap item was touched.

---

## What changed

`WriteBuffer` ([fuse4dbricks/fs/write_buffer.py](../../fuse4dbricks/fs/write_buffer.py)) previously performed plain, blocking file I/O (`seek`/`read`/`write`/`truncate`/`flush`/`close` on a `tempfile.NamedTemporaryFile`) directly inside its methods, with no `await` anywhere. Per [P12-analysis.md](P12-analysis.md), this blocked the single OS thread the entire mount runs on for the duration of every call, stalling all concurrent FUSE requests (any file, any user) and all in-progress downloads — not just the write in progress.

The fix brings `WriteBuffer` in line with the pattern [storage/persistence.py](../../fuse4dbricks/storage/persistence.py) already uses for the read-side disk cache: every blocking file operation is now offloaded via `trio.to_thread.run_sync`, and every method that does I/O is now `async def`.

### API changes

| Before | After |
|---|---|
| `WriteBuffer(writes_dir, initial_data=b"")` (sync constructor) | `await WriteBuffer.create(writes_dir, initial_data=b"")` (async factory); `__init__` now takes an already-open file handle and size, and is not meant to be called directly |
| `wb.write(offset, data) -> int` | `await wb.write(offset, data) -> int` |
| `wb.read(offset, length) -> bytes` | `await wb.read(offset, length) -> bytes` |
| `wb.truncate(size) -> None` | `await wb.truncate(size) -> None` |
| `wb.flush_to_disk() -> None` | `await wb.flush_to_disk() -> None` |
| `wb.finalize() -> None` | `await wb.finalize() -> None` |
| `wb.close() -> None` | `await wb.close() -> None` |
| `wb.size() -> int` | unchanged (pure in-memory accessor, no I/O) |
| `wb.path -> str` | unchanged (pure in-memory accessor, no I/O) |

A constructor was deliberately *not* kept synchronous-and-blocking: opening a tempfile is itself a blocking syscall, so it is routed through `trio.to_thread.run_sync` too, via the new `create()` classmethod — this fully eliminates blocking I/O from `WriteBuffer`, not just the hot-path methods, per the explicit instruction to move *all* blocking operations to the established pattern.

### Call sites updated

[fuse4dbricks/fs/operations.py](../../fuse4dbricks/fs/operations.py) — every call site that constructed or called a `WriteBuffer` was updated to `await` it:

- `open()` — the not-`O_TRUNC` pre-load loop (construction + per-chunk `write` + error-path `close`).
- `create()` — construction (inline in the `_open_state` dict).
- `_upload_if_dirty()` — `flush_to_disk()`.
- `release()` — both `close()` call sites (entry-missing branch and the `finally`).
- `read()` — `O_RDWR` handles served from the buffer.
- `write()` — the per-syscall `write()` call.
- `rename()` (cross-securable copy path) — construction, per-chunk `write`, `finalize`, and all three `close()` branches.
- `setattr`/`_truncate_uc_file` — `truncate()` on an already-open writer's buffer, and the closed-file path-based truncate's construction/`write`/`truncate`/`finalize`/`close` sequence.

No other module in the repository constructs or calls `WriteBuffer` (confirmed by a repository-wide search); `fs/auth_manager.py` has its own, unrelated, separate in-memory write-buffering dict for the virtual `.auth` files and was not touched.

### Tests updated

- [tests/test_write_buffer.py](../../tests/test_write_buffer.py) — every test converted to `async def` with `@pytest.mark.trio`, and every `WriteBuffer(...)`/`.write(...)`/`.read(...)`/`.truncate(...)`/`.flush_to_disk()`/`.finalize()`/`.close()` call updated to use `await WriteBuffer.create(...)` / `await ...`. No test's assertions or scenarios were changed — this is a mechanical async conversion of the existing coverage (construction, finalize/flush-to-disk content-visibility guarantees, idempotency, partial-overwrite, truncate shrink/grow/zero, tempfile cleanup).
- [tests/test_operations.py](../../tests/test_operations.py) — one direct call site (`wb.read(0, 10)` inside `test_open_wronly_preloads_existing_content_when_not_truncating`-style test) updated to `await wb.read(0, 10)`. All other `write_buffer`-related assertions in this file only touch `.size()`/`.path` (unchanged, non-async) or exercise the buffer indirectly through `fs.open()`/`fs.write()`/`fs.release()`, which already run under `@pytest.mark.trio` and needed no changes beyond the `operations.py` call-site updates themselves.

---

## Verification

Static checks (`get_errors`) were run on all four modified files with no issues reported.

The user approved installing the missing system build dependency (`libfuse3-dev`, via `sudo apt-get install`, a one-time system package install — no project files touched) so that `pyfuse3` could be compiled and the real test suite executed in this sandbox, rather than relying on static review alone.

**Targeted run** (the two directly-affected test files):

```
$ pytest tests/test_write_buffer.py tests/test_operations.py -q
....................................................................
.... [ 54%]
...........................................................
     [100%]
131 passed in 3.38s
```

**Full non-live suite** (everything except the live-Databricks-gated tests, which require real credentials and are unaffected by this change):

```
$ pytest -q -k "not live"
....................................................................
.... [ 17%]
...........ssssssssssssssss.........................................
.... [ 35%]
....................................................................
.... [ 53%]
....................................................................
.... [ 71%]
....................................................................
.... [ 88%]
.............................................
     [100%]
389 passed, 16 skipped, 10 deselected in 4.96s
```

All 389 collected non-live tests pass; the 16 skips are pre-existing (unrelated, environment-gated) and the 10 deselected are the live-Databricks `test_uc_client_live.py` tests excluded by the `-k "not live"` filter, consistent with this repository's documented CI behavior. **No regressions observed anywhere in the suite as a result of this change.**

---

## What this change does and does not establish

- **Confirmed**: the previously-blocking calls are now routed through `trio.to_thread.run_sync`, matching the established pattern elsewhere in the codebase; the full existing behavioral test suite (content correctness, idempotency, partial overwrites, truncate semantics, tempfile cleanup, and every operations-layer write/read/rename/truncate scenario) continues to pass unchanged.
- **Not established by this change alone**: the actual magnitude of the responsiveness improvement under concurrent load. Per [P12-analysis.md](P12-analysis.md) §3.3, that requires a live benchmark (measuring an unrelated concurrent operation's latency, e.g. `getattr` on a different file, while a large sequential write is in progress, before/after) against a real mount — which is the benchmark already specified in the roadmap's P12 catalog entry and is out of scope for this change (no benchmark infrastructure exists in this repository today; this sandbox also lacks a live Databricks workspace to mount against).

## Scope discipline

Per the instructions this work was performed under, only P12 was implemented. No other roadmap item (P01, P03–P11, P13) was touched, and no code outside `fs/write_buffer.py`, `fs/operations.py`, and the two test files listed above was modified.
