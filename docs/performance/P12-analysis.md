# P12 Deep-Dive — `WriteBuffer` Synchronous I/O

Validation of catalog item P12 from [performance-improvement-roadmap.md](performance-improvement-roadmap.md). Scope: [fuse4dbricks/fs/write_buffer.py](../../fuse4dbricks/fs/write_buffer.py) and [fuse4dbricks/fs/operations.py](../../fuse4dbricks/fs/operations.py). This analysis was written before any code change; the implementation that follows it is described separately in [P12-implementation.md](P12-implementation.md).

---

## 1. Whether synchronous `WriteBuffer` operations block the trio event loop

**Yes — confirmed, with a stronger and more precise evidence chain than previously documented.**

### 1.1 `WriteBuffer` performs plain, unwrapped blocking I/O

[fs/write_buffer.py](../../fuse4dbricks/fs/write_buffer.py) backs every writable file handle with a `tempfile.NamedTemporaryFile`, and every method that touches it calls the file object's blocking methods **directly**, with no `await`, no `trio.to_thread.run_sync`, and no `trio.open_file`:

| Method | Blocking call(s) |
|---|---|
| `__init__` | `tempfile.NamedTemporaryFile(...)` (opens/creates a file), `self._tmp.write(initial_data)` |
| `write` | `self._tmp.seek(offset)`, `self._tmp.write(data)` |
| `read` | `self._tmp.seek(offset)`, `self._tmp.read(length)` |
| `truncate` | `self._tmp.truncate(size)` |
| `flush_to_disk` | `self._tmp.flush()` |
| `finalize` | `self._tmp.close()` |
| `close` | `self._tmp.close()`, `os.unlink(self._tmp.name)` |

None of these go through an awaitable. This is a direct, visible inconsistency against the rest of the codebase: [storage/persistence.py](../../fuse4dbricks/storage/persistence.py) — which does the equivalent job for the *read*-side disk cache — routes every one of its blocking file operations through either `trio.open_file` (for the streamed chunk write) or `trio.to_thread.run_sync` (for `os.rename`, `os.remove`, `os.path.exists`, and its own `_read_file` helper). `WriteBuffer` is the one place in this codebase that does not follow that established pattern.

### 1.2 Confirming the actual threading/concurrency model (new evidence, obtained by reading the installed `pyfuse3` library's own source)

To answer "does this actually matter" with certainty rather than general `trio` knowledge, this review inspected the exact mechanism `pyfuse3` (the version range pinned in [pyproject.toml](../../pyproject.toml), `pyfuse3>=3.4.2`) uses to dispatch FUSE requests, by downloading its source distribution for inspection (read-only; not installed into the project).

`pyfuse3`'s `main()` coroutine (`src/pyfuse3/__init__.pyx`) starts a single `_session_loop` task in a `trio` nursery; that loop (`src/pyfuse3/internal.pxi`) spawns **additional `_session_loop` trio tasks** — not OS threads — on demand, up to a configurable cap (`max_tasks`, default 99), each one cooperatively reading the next kernel request and awaiting its handler coroutine:

```python
# pyfuse3/internal.pxi (library source, not part of this repository)
async def _session_loop(nursery, int min_tasks, int max_tasks):
    while not fuse_session_exited(session):
        ...
        if not worker_data.active_readers and worker_data.task_count < max_tasks:
            worker_data.task_count += 1
            nursery.start_soon(_session_loop, nursery, min_tasks, max_tasks, ...)
        ...
        fuse_session_process_buf(session, &buf)
        if py_retval is not None:
            await py_retval
```

This confirms, from the library's own implementation rather than general assumption: **every concurrent FUSE request this process handles — every `read`, every `write`, every `getattr`, every chunk download in progress — is a `trio` task cooperatively scheduled on a single OS thread.** There is no worker-thread-per-request model here. `trio`'s cooperative scheduler only switches between tasks at `await` points. A call that never awaits anything — exactly what every `WriteBuffer` method does today — holds that one OS thread for its entire duration, and **no other task in the entire process can run until it returns**, including `_session_loop` itself (so the kernel's *next* request, from any file, any user, isn't even picked up off the queue until the blocking call finishes).

### 1.3 Conclusion for Question 1

Confirmed on two independent grounds: (a) direct reading of `WriteBuffer`'s own code shows no thread-offloading anywhere, unlike every comparable disk operation elsewhere in this codebase; (b) direct reading of `pyfuse3`'s actual request-dispatch loop confirms this process has exactly one OS thread available to make progress on anything, FUSE-related or not, while any one blocking call is in flight.

---

## 2. Whether this affects uploads only, or the entire filesystem

**The entire filesystem — not just the file being written, and not just uploads.**

### 2.1 Every caller of `WriteBuffer` is itself an `async` FUSE handler with no intervening `await`

[fs/operations.py](../../fuse4dbricks/fs/operations.py) calls `WriteBuffer` methods synchronously from inside several of its `async def` handlers, meaning the handler coroutine itself does not yield control to the scheduler while the call is in progress:

| Call site (method) | What it does |
|---|---|
| `open()` — pre-load loop | Streams the existing remote file into the buffer, calling `write_buffer.write(pos, chunk)` once per downloaded piece, without `O_TRUNC` |
| `create()` | Constructs a new `WriteBuffer` (opens a tempfile) |
| `write()` | `write_buffer.write(offset, buffer)` on every single kernel `write()` request |
| `read()` | `write_buffer.read(offset, length)` for `O_RDWR` handles |
| `_upload_if_dirty()` (called by `flush`/`release`) | `write_buffer.flush_to_disk()` before uploading |
| `release()` | `write_buffer.close()` |
| `rename()` (cross-securable copy path) | Constructs a `WriteBuffer`, streams source content into it, `finalize()`, `close()` |
| `setattr`/`_truncate_uc_file` | `write_buffer.truncate(...)`, and (for a closed-file path-based truncate) a full construct/write/finalize/upload/close sequence |

Every one of these is reached from a different kernel request (a different syscall from a possibly different process entirely), and per §1.2, every one of them is a separate `trio` task sharing the same single OS thread as everything else in the mount.

### 2.2 Why this is not scoped to "uploads" or even to "the file being written"

Because `trio`'s scheduler cannot interrupt a non-awaiting call, a blocking `WriteBuffer.write()` inside one task does not just delay that task's own upload — it delays:
- Every other open file's `read()`/`write()`/`getattr()` on this same mount, for any user.
- Every in-progress chunk download: `DataManager`'s download-worker tasks (themselves `trio` tasks on the same thread) cannot make progress — including the network I/O they're waiting on, since even *receiving* the data they already requested requires this same thread to resume and run their `await` continuation.
- The FUSE session loop itself picking up the *next* kernel request from the queue (per the `_session_loop` trace in §1.2), so even unrelated filesystem activity that hasn't started yet is delayed from starting.
- Background maintenance tasks (e.g. `DiskPersistence`'s hourly cleanup, or `DownloadScheduler`'s idle workers waking up) are equally paused, though these are low-frequency enough that the practical impact there is minor compared to interactive read/write latency.

### 2.3 Conclusion for Question 2

This affects the **entire process**, for the **entire duration of the mount**, whenever **any** writable-file operation is in flight — not a narrow "uploads are slow" issue. A user downloading a large file can be stalled by a *different* user's unrelated small write elsewhere in the same mount, and vice versa.

---

## 3. Expected impact

### 3.1 Severity

**High.** This is the single bottleneck in this entire review with the broadest blast radius: every other finding in the performance review degrades *its own* operation (a slow download is slow to download; a contended connection pool delays other network calls); this one can degrade operations that have **nothing to do with the write in progress** — a property no other item in [performance-improvement-roadmap.md](performance-improvement-roadmap.md) shares.

### 3.2 What determines how bad it is in practice

- **Frequency**: how many `write()` calls the kernel forwards per second (which — per the roadmap's P10 finding — is currently *every single* application `write()` syscall individually, since this project does not enable the kernel's write-back batching).
- **Duration per call**: dominated by the local disk/cache-directory's actual write latency per call, which is typically small (local SSD) but non-zero, and is paid, unamortized, on **every** call — including the three largest-blast-radius call sites: the `open()` pre-load loop (one blocking call per 8 MB chunk when re-opening an existing file for partial write), the `rename()` cross-securable copy path (one blocking call per 8 MB chunk of the whole file being copied), and ordinary `write()` (one blocking call per kernel write request).
- **Concurrency**: the busier the mount (more simultaneous users/files/downloads), the more total waiting time this single mechanism imposes system-wide, since it's a shared bottleneck, not a per-file one.

### 3.3 What this review can and cannot claim

Per this review's evidence standard: the **mechanism** is confirmed with certainty (two independent code-reading exercises, no speculation). The **magnitude** in milliseconds/throughput is not claimed here, because it depends on local disk/filesystem performance in the actual deployment (e.g. the configured disk-cache directory's underlying storage), which this static review cannot measure. The benchmark needed to quantify it is: measure the latency of an unrelated concurrent operation (e.g. `getattr` on a different, idle file) while a large sequential write is in progress, before and after this fix — exactly as already specified in the roadmap's P12 catalog entry.

### 3.4 Conclusion

The hypothesis is **validated**: `WriteBuffer`'s synchronous I/O is confirmed, on primary evidence, to block the single OS thread this entire mount runs on, for the duration of every writable-file operation, affecting all concurrent activity in the process — not only uploads, and not only the file being written. This justifies treating it as the highest-priority item in the roadmap, which is why it is the one implemented below.
