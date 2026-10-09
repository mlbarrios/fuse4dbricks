# P02 Deep-Dive — FUSE `max_read` Negotiation

Detailed review of tracker item P02 ([.github/prompts/fuse4dbx-performance-engineer.md](../../.github/prompts/fuse4dbx-performance-engineer.md)). No code was modified. This review goes one level deeper than [docs/performance/optimization-review.md](optimization-review.md)'s P02 section by inspecting the **actual pinned `pyfuse3==3.4.2` source** (downloaded read-only via `pip download --no-binary` for inspection) and the **locally installed `fuse(8)` man page** (`libfuse3 3.10.5`, Ubuntu/WSL2, kernel `5.15.133.1-microsoft-standard-WSL2`), rather than relying on general knowledge of FUSE. This environment has `/dev/fuse` but no `libfuse3-dev`/compiler toolchain for building `pyfuse3` from source, so no live mount was performed here.

---

## 1. Current FUSE read size

### 1.1 What fuse4dbricks sets today

[fuse4dbricks/main.py](../../fuse4dbricks/main.py) (`start_fuse`, lines ~168–177):

```python
mount_options = ["fsname=fuse4dbricks", "noatime"]
if allow_other:
    mount_options.append("allow_other")
if debug_mode:
    mount_options.append("debug")
...
pyfuse3.init(ops, mountpoint, mount_options)
```

No `max_read`, `max_write`, or any buffer-size-related option is ever added. `parse_args` has no corresponding CLI flag. This part of the earlier review is confirmed as written.

### 1.2 What actually governs the request size — ground-truthed against pyfuse3 3.4.2 source

`pyfuse3.init()`'s docstring (`src/pyfuse3/__init__.pyx`) states mount options are forwarded verbatim to `fuse_session_new()` and references libfuse's own `fuse_mount_opts[]`/`fuse_ll_opts[]` tables — i.e. these are genuine kernel/libfuse mount options, not pyfuse3-invented ones.

Crucially, pyfuse3 registers a native (non-Python) FUSE `init` callback that runs during the `FUSE_INIT` handshake, **before** any Python code sees a request (`src/pyfuse3/handlers.pxi`):

```c
cdef void fuse_init (void *userdata, fuse_conn_info *conn):
    if not conn.capable & FUSE_CAP_READDIRPLUS:
        raise RuntimeError('Kernel too old, pyfuse3 requires kernel 3.9 or newer!')
    conn.want &= ~(<unsigned> FUSE_CAP_READDIRPLUS_AUTO)

    if (operations.supports_dot_lookup and conn.capable & FUSE_CAP_EXPORT_SUPPORT):
        conn.want |= FUSE_CAP_EXPORT_SUPPORT
    if (operations.enable_writeback_cache and conn.capable & FUSE_CAP_WRITEBACK_CACHE):
        conn.want |= FUSE_CAP_WRITEBACK_CACHE
    if (operations.enable_acl and conn.capable & FUSE_CAP_POSIX_ACL):
        conn.want |= FUSE_CAP_POSIX_ACL

    # Blocking rather than async, in case we decide to let the
    # init handler modify `conn` in the future.
    operations.init()
```

This is the **only** place `conn->max_write`/`conn->max_readahead` (the fields that actually govern negotiated request size per the FUSE protocol) could be adjusted, and pyfuse3 **never touches them**. The comment "*in case we decide to let the init handler modify `conn` in the future*" confirms that pyfuse3's public Python hook, `Operations.init()`, receives **no reference to `conn` at all** — there is currently no way, through pyfuse3's public API, for `UnityCatalogFS` to request a larger negotiated `max_write`/`max_read`/`max_pages`.

The installed `fuse(8)` man page (libfuse3 3.10.5) independently confirms the mount-option route is a dead end for *increasing* the size:

> **max_read=N** — With this option the maximum size of read operations can be set. The default is infinite, but **typically the kernel enforces its own limit in addition to this one**. [...] **This option should not be specified by the filesystem owner.** [...] **This mount option is deprecated in favor of direct negotiation over the device fd** (as done for e.g. the maximum size of write operations).

In other words: `max_read=N` can only **cap/lower** the kernel's own limit, never raise it — the man page says so explicitly, and names the real mechanism ("direct negotiation over the device fd", i.e. `conn->max_write` during `FUSE_INIT`) as the only way to raise it, which pyfuse3 doesn't expose.

### 1.3 Resulting bound on today's effective read size

Because pyfuse3 doesn't touch `conn->max_write`/`max_readahead`/the `FUSE_CAP_MAX_PAGES`/`FUSE_CAP_BIG_WRITES` capability bits, and doesn't remove them from `conn->want` either, the effective negotiated size is **whatever libfuse3's own internal default negotiates for this kernel**, bounded as follows:
- **Historical/legacy floor**: 32 pages = 128 KiB (pre-`FUSE_CAP_MAX_PAGES`, kernels < 4.20 or old libfuse).
- **Modern ceiling**: up to ~1 MiB (256 pages) on kernel ≥ 4.20 with libfuse ≥ 3.1, if `FUSE_CAP_MAX_PAGES` is negotiated — plausible here since pyfuse3 never masks it out of `conn->want`, and this sandbox's kernel (`5.15.133.1`) and installed libfuse (`3.10.5`) both postdate that capability.

**This cannot be pinned to an exact number without a live mount and either kernel instrumentation or counting actual `UnityCatalogFS.read()` call sizes** (this sandbox has `/dev/fuse` but lacks `libfuse3-dev`/a C toolchain to build `pyfuse3` from source, so a live mount was not attempted here). The honest answer is: **somewhere between 128 KiB and ~1 MiB, most likely at the upper end on any current Linux target, and identical with or without the tracker's proposed `max_read` mount option** (since that option cannot raise it past whatever this negotiation already produced).

---

## 2. Current request amplification

`DataManager.__init__` ([fuse4dbricks/fs/data_manager.py](../../fuse4dbricks/fs/data_manager.py), line 126): `self.chunk_size = 8 * 1024 * 1024` — 8 MiB, hardcoded.

`DataManager.read()` (same file) computes `start_chunk`/`end_chunk` from the kernel-supplied `offset`/`length` and either awaits a single `_read_chunk` or spawns one `trio` task per chunk in a nursery — it is **request-size-agnostic**: whatever `length` the kernel sends in a given `read()` call, `DataManager.read()` just maps it onto 8 MiB chunk boundaries.

`UnityCatalogFS.read()` ([fuse4dbricks/fs/operations.py](../../fuse4dbricks/fs/operations.py)) passes the kernel's `offset`/`length` straight through to `data_manager.read(...)` — no batching, coalescing, or internal re-chunking happens above `DataManager`.

**Amplification factor = `chunk_size / effective_max_read`,** i.e. how many separate kernel `read()` requests (each a distinct `UnityCatalogFS.read()` call, each its own `trio` task dispatched by pyfuse3's request loop) are needed to traverse one 8 MiB logical chunk during a sequential read:

| Effective negotiated max read/write | Requests per 8 MiB chunk | Requests per 1 GB file | Requests per 100 GB file |
|---|---|---|---|
| 128 KiB (legacy floor) | 64 | ~8,192 | ~819,200 |
| 1 MiB (modern ceiling, `FUSE_CAP_MAX_PAGES`) | 8 | ~1,024 | ~102,400 |

Important nuance: this amplification is **pure per-request dispatch overhead**, not redundant network or disk I/O. Only the *first* sub-read of a given chunk triggers `DataManager._read_chunk`'s coalescing path (network fetch via `DownloadScheduler` → `DiskPersistence`); every subsequent sub-read for the same chunk is served from `DataManager._ram_cache` (an in-memory hit) because the whole 8 MiB chunk was cached after the first miss. So the cost of amplification is: one `trio` task spawn + `UnityCatalogFS.read()` call + `RamCache.get()` lock/LRU-touch, repeated 8–64 times per chunk, not 8–64x the network/disk cost.

---

## 3. Expected gain from `max_read`

**Essentially none, and potentially negative, if implemented as literally named.** Two independent, primary-source-grounded facts both point the same way:

1. The `fuse(8)` man page states `max_read=N` can only **lower** the kernel's own enforced limit — never raise it. Setting it to a value at or above the kernel's current negotiated limit is a no-op; setting it below is actively harmful (it would *increase* amplification, the opposite of the tracker's goal).
2. The actual mechanism that *could* raise the limit — `conn->max_write`/`max_readahead` negotiation inside the native `fuse_init` callback — exists in pyfuse3's C layer but is **not exposed to pyfuse3's Python API** (`Operations.init()` receives no `conn` reference; pyfuse3 3.4.2 never modifies these fields itself).

**Conclusion: the optimization as named in the tracker (add a `max_read` mount option) is not viable and should not be implemented as described.** There is no code change available at the `fuse4dbricks` application layer, through pyfuse3's current public API, that increases the kernel-negotiated read/write size beyond whatever libfuse3's own default already negotiates for the running kernel.

The only remaining, real uncertainty is **whether the current (zero-config) default is already "good enough" (likely ~1 MiB on any modern kernel+libfuse, i.e. no actionable gap) or still capped at the legacy 128 KiB (meaningful gap, but unreachable without upstream changes)**. That is an empirical question requiring a live mount, not a code-level one.

---

## 4. Exact code changes required

### 4.1 What should change in `fuse4dbricks` right now: nothing functional

Given §3, there is **no safe, effective application-level code change** that raises the negotiated read/write size today. Specifically:
- **Do not add `max_read=N`** to `mount_options` in `start_fuse` — per the man page, this can only cap the size downward; adding it with any value below the kernel's actual negotiated limit would regress amplification, and adding it at or above that limit does nothing.
- **Do not add a corresponding CLI flag** (`--max-read-kb` or similar) for the same reason — it would expose a user-facing knob that cannot do what its name implies.

### 4.2 What a real fix would require (out of reach without upstream work)

To actually influence `conn->max_write`/`max_readahead`/`FUSE_CAP_MAX_PAGES`, one of the following would be needed — both are **outside this repository's code** and are not being proposed as work items here, only documented for completeness:
- A patch to `pyfuse3` itself (upstream `libfuse/pyfuse3` project) to pass `conn` (or a subset of its fields) into `Operations.init()`, letting a filesystem request a larger `max_write`/`max_readahead` and opt into `FUSE_CAP_MAX_PAGES`/`FUSE_CAP_BIG_WRITES`. This is a non-trivial upstream contribution (Cython/C-level API change, new pyfuse3 release, a version bump in `pyproject.toml`'s `pyfuse3>=3.4.2` pin) and is not something `fuse4dbricks` can do unilaterally.
- Alternatively, switching away from `pyfuse3` to a lower-level FUSE binding that does expose `conn_info` — a disproportionate change relative to the expected gain, not recommended.

### 4.3 The only recommended code change: measurement instrumentation (not a performance fix)

If the team wants to close the empirical gap identified in §1.3 (is the deployment target already at ~1 MiB, or stuck at 128 KiB?), the minimal, low-risk, *diagnostic-only* change would be a temporary debug-log counter in `UnityCatalogFS.read()` ([fuse4dbricks/fs/operations.py](../../fuse4dbricks/fs/operations.py)) recording the distribution of `length` values actually received from the kernel over a real sequential-read benchmark run, e.g. logging `max(length)` seen per file descriptor at `release()` time. This requires no mount-option change, carries no behavioral risk, and directly answers whether there is any gap left to close. This is instrumentation for the benchmark step of the performance-engineer workflow, not an optimization itself, and should not be left in permanently.

---

## 5. Recommendation for the optimization tracker

Based on the above, recommend updating the tracker entry for P02:

| ID | Optimization | Current Status | Recommended Status | Rationale |
|---|---|---|---|---|
| P02 | FUSE `max_read` negotiation | TODO | **REJECTED** (as literally named) *or* **TODO, rescoped** to "measure effective kernel-negotiated read/write size; revisit only if confirmed capped at 128 KiB and only via an upstream pyfuse3 change" | `max_read` mount option cannot raise the limit (man page); the only mechanism that could is not exposed by pyfuse3's public API (verified against the pinned `pyfuse3==3.4.2` source). No in-repo code change achieves the stated goal. |

If the team still wants data before closing it out, treat it as a **benchmark-only** task (§4.3) rather than an implementation task, and gate any further investment on those measurements showing an actual 128 KiB cap in the real Posit Workbench → Databricks deployment environment.
