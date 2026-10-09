# P03 Deep-Dive — Separate HTTP Pools

Detailed review of tracker item P03 ([.github/prompts/fuse4dbx-performance-engineer.md](../../.github/prompts/fuse4dbx-performance-engineer.md)). No code was modified. Scope: [fuse4dbricks/api/uc_client.py](../../fuse4dbricks/api/uc_client.py), [fuse4dbricks/fs/metadata_manager.py](../../fuse4dbricks/fs/metadata_manager.py), [fuse4dbricks/fs/data_manager.py](../../fuse4dbricks/fs/data_manager.py). This builds on the P03 section of [docs/performance/optimization-review.md](optimization-review.md) with exact call-site tracing and a quantitative worst-case analysis.

---

## 1. How connections are shared

### 1.1 One pool, constructed once

[fuse4dbricks/api/uc_client.py](../../fuse4dbricks/api/uc_client.py), `UnityCatalogClient.__init__` (lines 135–140):

```python
self.client = httpx.AsyncClient(
    base_url=self.base_url,
    timeout=httpx.Timeout(connect=10.0, read=60.0, write=10.0, pool=30.0),
    limits=httpx.Limits(max_keepalive_connections=20, max_connections=50),
    follow_redirects=True,
)
```

Exactly one `httpx.AsyncClient` — and therefore one connection pool, capped at **50 concurrent connections / 20 kept alive** — exists per `UnityCatalogClient` instance. `main.py`'s `async_main` constructs exactly one `UnityCatalogClient` for the whole process and hands the **same instance** to both `DataManager` and `MetadataManager`, so both components genuinely share this one pool.

### 1.2 Who routes through it

Every one of these methods eventually calls `self.client.send(...)` inside `_request()` (lines 227–256ish) — i.e. checks out a connection from the single shared pool:

| Caller | Method | uc_client call | Plane |
|---|---|---|---|
| `MetadataManager._get_principal` (line 224) | — | `get_current_user_info` (line 370) | metadata |
| `MetadataManager.check_access` (line 292) | — | `check_permissions` (line 375) | metadata |
| `MetadataManager._refresh_entry_metadata` / `lookup_child` (line 397) | — | `get_path_metadata` (line 538) | metadata |
| `MetadataManager.list_directory` (line 443) | — | `get_path_contents` (line 624) | metadata |
| `MetadataManager.rename`'s refresh (operations.py, via `get_attributes`) (line 637) | — | `get_path_metadata` (line 538) | metadata |
| `DataManager._fetch_chunk` (data_manager.py line 189) | — | `download_chunk_stream` (uc_client.py line 710) | **data (streaming)** |

Both `_get_catalogs`/`_get_schemas`/`_get_volumes` (listing) and `delete_file`/`delete_directory`/`create_directory` (small mutations) also go through `_request`/`self.client`, but are low-frequency compared to the two dominant traffic classes above (attribute/permission lookups and chunk downloads).

### 1.3 One caller already bypasses the pool entirely

`UnityCatalogClient.upload_file` (line 650) does **not** call `_request`/`self.client` at all — it constructs a fresh `databricks.sdk.WorkspaceClient` (its own independent HTTP session) per call and runs the actual upload via `trio.to_thread.run_sync`. Uploads are therefore already isolated from the shared pool today — confirming and narrowing the earlier [optimization-review.md](optimization-review.md) finding: the real shared-pool contention that P03 can address is specifically **metadata calls vs. chunk downloads**, not metadata vs. uploads.

### 1.4 How long a connection is held per call

This matters because HTTP/1.1 (the default; no explicit HTTP/2 configuration exists anywhere in `uc_client.py`) allows exactly **one in-flight request per physical connection** — a connection checked out by one request is completely unavailable to any other request, regardless of total pool size, until that request finishes.

- **Metadata calls** (`get_path_metadata`, `get_path_contents`, `check_permissions`, `get_current_user_info`): single request/response, non-streamed (`stream=False`). Connection hold time ≈ one network round-trip (tens to a few hundred ms typically).
- **Chunk downloads** (`download_chunk_stream`, lines 710–743): `stream=True`; the connection is held by `DataManager._fetch_chunk`'s caller for the **entire duration of reading the stream** — the `async for chunk in response.aiter_bytes(chunk_size=65536)` loop runs until the whole chunk (up to `DataManager.chunk_size = 8 MiB`, data_manager.py line 128) has been received, only then does the `finally: await response.aclose()` release the connection. Over a real WAN link to a Databricks workspace, this can plausibly be hundreds of milliseconds to multiple seconds per chunk — orders of magnitude longer than a metadata call's hold time.

---

## 2. Whether metadata starvation is possible

**Yes, possible and bounded-but-severe in its worst case — not indefinite starvation, but a multi-attempt stall that can run into minutes.** This is a concrete, quantifiable failure mode, not just a theoretical risk, because of how the configured timeouts and retry logic interact:

### 2.1 The mechanism

1. `DownloadScheduler` ([fs/data_manager.py](../../fuse4dbricks/fs/data_manager.py) line 41) defaults to `num_workers=10` concurrent download workers (line 119 constructor default, wired unmodified from `main.py` — no CLI flag exists to change it today, confirming the earlier gap-analysis finding that P04 "configurable worker count" is not yet implemented).
2. Each active download worker can hold one connection checked out for as long as its current chunk streams in (§1.4).
3. The pool's hard cap is `max_connections=50`. httpx's underlying connection pool (`httpcore`) **queues** a new request when the pool is already at capacity, rather than failing immediately.
4. The configured `httpx.Timeout(..., pool=30.0)` means a queued request waits **up to 30 seconds** for a connection to free up before httpx raises `httpx.PoolTimeout`.
5. `httpx.PoolTimeout` is a subclass of `httpx.TimeoutException`, which is a subclass of `httpx.TransportError` — and `_request()`'s `except httpx.TransportError as e:` (uc_client.py, around line 256) **catches and retries it** with the same exponential-backoff logic used for connection resets (`_backoff_delay`, base 0.5s, capped 20s, up to `_DEFAULT_MAX_RETRIES = 4` attempts).

### 2.2 Worst-case latency this produces for a metadata call

If the pool is saturated (all 50 connections checked out) when a `getattr`/`lookup`/`readdir` call tries to send its request:

```
attempt 0: wait up to 30s for a connection (pool timeout) -> PoolTimeout -> caught, backoff ~0.5-1s
attempt 1: wait up to 30s again -> PoolTimeout -> caught, backoff ~1-2s
attempt 2: wait up to 30s again -> PoolTimeout -> caught, backoff ~2-4s
attempt 3: wait up to 30s again -> PoolTimeout -> caught, backoff ~4-8s
attempt 4 (final, max_retries exhausted): wait up to 30s -> PoolTimeout raised to caller
```

Worst case: **up to ~5 × 30s pool-waits + ~15-20s of cumulative backoff ≈ 2.5–3 minutes** before a single `getattr` call fails outright (surfacing to the FUSE layer as an uncaught exception → `_raise_fuse_error`'s fallback `EIO`, since `httpx.PoolTimeout` isn't one of the mapped `Uc*` domain exceptions). Even the *typical* case — the pool frees up well before 30s because a download finishes — still means the metadata call's actual latency is dictated by **whichever download happens to finish first**, not by the metadata call's own (normally sub-second) cost. This is textbook head-of-line blocking: a cheap, latency-sensitive request queued behind expensive, long-held connections, made worse by HTTP/1.1's one-request-per-connection model.

### 2.3 How likely is this at today's defaults?

- **Default settings** (`num_workers=10`, `max_connections=50`) leave 40 connections nominally free for metadata even if every download worker is simultaneously busy — so *outright pool exhaustion* requires roughly 40+ concurrent metadata requests in flight **at the same moment** as all 10 download workers are saturated. This is plausible under a combined workload (e.g. a recursive `find`/`rsync --dry-run`/parallel `stat` scan of a large tree running concurrently with an in-progress large sequential transfer) but not a near-certainty at these defaults.
- **`max_keepalive_connections=20`** is a secondary, lower-severity effect worth noting even without hitting the 50-connection hard cap: once concurrent connection usage exceeds 20, excess connections are closed after use rather than kept warm, so metadata calls arriving during a burst of concurrent traffic pay full TCP+TLS handshake cost more often than they would with a larger or dedicated keep-alive budget.
- **The risk scales directly with `num_workers`.** Any future increase in download concurrency (P04, "configurable worker count" — currently unimplemented per §2.1) directly erodes the headroom between downloads and the 50-connection cap, making the scenario in §2.2 proportionally more likely. At `num_workers=40+`, the entire pool could realistically be saturated by downloads alone during a sustained large transfer, leaving no slack for metadata at all.
- **Sustained TB-scale sequential transfers** (an explicitly named workload in the performance-engineer prompt's "Primary objective") are exactly the scenario that maximizes both the *duration* a download holds its connection (long chunk streams back-to-back) and the *likelihood* of concurrent metadata traffic (users still browsing/stat-ing the mount, or the copy tool itself issuing periodic `stat` calls).

### 2.4 Does today's code have any mitigation already?

No prioritization exists between metadata and data traffic at the connection-pool level — `httpx`'s pool has no concept of request priority; it is a simple FIFO/fairness queue over whichever requests are waiting. `DownloadScheduler`'s own priority queue (high vs. regular, data_manager.py lines 42–44) only prioritizes **among downloads**, and has no visibility into or influence over metadata requests at all — the two subsystems only interact by sharing `uc_client.client`'s connection pool, with no coordination.

---

## 3. Expected gains from separate pools

### 3.1 What splitting the pool would and would not fix

Giving metadata calls (`get_path_metadata`, `get_path_contents`, `check_permissions`, `get_current_user_info`, and the low-frequency listing/mutation calls) their **own** `httpx.AsyncClient`/pool, separate from the one used by `download_chunk_stream`, would:

- **Eliminate** the §2 failure mode entirely: a metadata request would never queue behind a download's connection checkout, because they'd no longer share a connection budget. Its latency would return to being dominated by its own round-trip time and the Databricks API's own responsiveness, not by how many downloads happen to be in flight.
- **Not** increase raw download throughput — the data-plane pool's own size (however it's configured) still bounds how many chunks can download in parallel; this change only removes *metadata's* exposure to *that* bound, not the bound itself.
- **Not** address the upload path, which (per §1.3) is already isolated, albeit inefficiently (a fresh SDK client/session per upload call, no connection reuse across uploads — a separate, already-isolated inefficiency, not something this change touches).

### 3.2 Magnitude

- **At today's defaults** (`num_workers=10`): low-to-moderate measured impact expected in isolated benchmarking, since §2.3 shows outright starvation needs a fairly specific combined workload to trigger at these settings. The *tail latency* (p99) of metadata calls during a concurrent large transfer is the metric most likely to show a visible improvement; median/typical-case latency may look similar before and after.
- **As a scaling enabler**: the gain becomes substantial and increasingly necessary if `num_workers` is ever raised (P04) toward or past the current 50-connection ceiling, or if multiple large transfers run concurrently (multiple users/mounts sharing a workspace token's rate limit, each independently running download workers against their own `UnityCatalogClient`... though note each *process* gets its own instance, so this specific contention is per-mount, not cross-mount). Framed against the performance-engineer prompt's stated objective of "Sequential TB-scale transfers," this is squarely the regime where §2's worst case becomes a realistic, not merely theoretical, operational issue.
- **Best available metric to benchmark**: `getattr`/`lookup`/`readdir` p50/p99 latency measured *while* a large sequential download is in progress, before vs. after the split — a sequential-throughput-only benchmark (as flagged in the prior review) will not surface this change's value, since it does not touch download throughput at all.

### 3.3 Net assessment

Confirmed, real, and well-isolated contention surface (metadata vs. downloads specifically, not metadata vs. uploads, which are already separate). The fix is conceptually simple (a second `httpx.AsyncClient` + routing) but its value is primarily **protective/tail-latency-focused** rather than a throughput win, and its urgency is directly tied to whether/when download concurrency (`num_workers`) is increased beyond today's conservative default of 10.
