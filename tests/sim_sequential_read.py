#!/usr/bin/env python3
"""
Simulator: sequential read of a large, uncached file through fuse4dbricks.

Scenario
--------
A program runs ``cp /mnt/uc/.../big.bin /local/``. The kernel splits cp's
reads into FUSE READ requests that reach ``DataManager.read``. Nothing is
cached, so every byte comes from Databricks over a link that saturates at
``global_mib_s`` (default 200 MiB/s = 10 streams x 20 MiB/s).

What is real and what is simulated
----------------------------------
* REAL:      ``DataManager`` (chunking, RAM LRU, InflightCoalescer, the
             DownloadScheduler with its priority/regular queues and workers,
             prefetch policy). This is the code under study.
* SIMULATED: the kernel (request generator), the Unity Catalog client
             (time-to-first-byte + bandwidth-limited body), and the disk
             cache (``DiskPersistence`` replaced by a bandwidth-limited fake).

Bandwidth model
---------------
Network and disk are event-driven "fluid" links (exact, no time steps). Every
active flow gets a fair share ``min(per_flow_cap, total_cap / n_active)``
(water-filling: capacity a flow cannot use is redistributed). A network
flow only consumes bandwidth while it has room in its socket buffer, so a
consumer that stalls (e.g. on a slow disk write) eventually stalls the
transfer, like TCP flow control. Disk reads and writes share one disk link.

Time
----
By default the run uses trio's MockClock with autojump: simulated seconds
pass instantly and results are deterministic, but Python CPU cost counts as
zero. ``--real-clock`` runs in wall time, which also exposes event-loop CPU
overhead (a 3 GiB run then takes at least ~16 s). Use it with ``--no-verify``:
the fakes then return cached zero buffers so the simulator's own CPU stays
small, although it still shares the trio loop with DataManager. Not modelled:
pyfuse3 request dispatch, httpx parsing and the thread hops of the real
DiskPersistence, so real-clock results are an optimistic bound.

Usage
-----
    uv run python tests/sim_sequential_read.py --out run.json --plot run.png
    uv run python tests/sim_sequential_read.py --sweep --out sweep.json --plot sweep.png
    uv run python tests/sim_sequential_read.py --help

Units are binary: 1 MiB = 2**20 bytes, 1 GiB = 2**30 bytes.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import math
import random
import struct
import sys
import time
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any

import trio
import trio.testing

from fuse4dbricks.fs.data_manager import DataManager
from fuse4dbricks.fs.ram_cache import RamCache

KiB = 1024
MiB = 1024 * KiB
GiB = 1024 * MiB
PAGE = 4 * KiB
NET_PIECE = 64 * KiB  # uc_client yields the HTTP body in 64 KiB pieces
FS_PATH = "/catalog/schema/volume/big.bin"
MTIME = 1_700_000_000.0


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass
class SimConfig:
    # File
    file_mib: float = 3 * 1024
    # fuse4dbricks (DataManager) knobs
    num_workers: int = 10
    prefetch_mb: int = 80
    ram_cache_mb: int = 512
    chunk_mib: float = 8
    prefetch_to_ram: bool = True
    # Kernel / cp model
    request_kib: int = 128
    kernel_concurrency: int = 1
    dest_mib_s: float = 0.0  # cp destination write speed; 0 = infinitely fast
    # Network model
    per_stream_mib_s: float = 20.0
    global_mib_s: float = 200.0
    ttfb_ms: float = 50.0
    ttfb_jitter_ms: float = 10.0  # std-dev of a normal, truncated at 0; >0 avoids lock-step artefacts
    socket_buffer_kib: int = 4096
    # Disk cache model (fake DiskPersistence)
    disk_mib_s: float = 500.0  # shared by reads and writes; 0 = infinite
    disk_op_latency_ms: float = 0.0
    # Simulation
    sample_ms: float = 100.0
    real_clock: bool = False
    seed: int = 42
    verify: bool = True

    @property
    def file_size(self) -> int:
        return int(self.file_mib * MiB)

    @property
    def chunk_size(self) -> int:
        return int(self.chunk_mib * MiB)


# ---------------------------------------------------------------------------
# File content: every 4 KiB page starts with its own offset (big endian u64).
# Lets us verify returned bytes without keeping 3 GiB in memory.
# ---------------------------------------------------------------------------


def make_content(offset: int, length: int) -> bytes:
    buf = bytearray(length)
    first_page = (offset // PAGE) * PAGE
    for page in range(first_page, offset + length, PAGE):
        header = struct.pack(">Q", page)
        for i in range(8):
            pos = page + i - offset
            if 0 <= pos < length:
                buf[pos] = header[i]
    return bytes(buf)


_ZEROS: dict[int, bytes] = {}


def content_for(cfg: "SimConfig", offset: int, length: int) -> bytes:
    """Real pattern when verifying; otherwise cached zeros, so that in
    --real-clock runs the simulator itself burns as little CPU as possible."""
    if cfg.verify:
        return make_content(offset, length)
    z = _ZEROS.get(length)
    if z is None:
        z = _ZEROS[length] = bytes(length)
    return z


def verify_content(offset: int, data: bytes) -> bool:
    first_page = -(-offset // PAGE) * PAGE  # first page start >= offset
    for page in range(first_page, offset + len(data) - 7, PAGE):
        if struct.unpack_from(">Q", data, page - offset)[0] != page:
            return False
    # bytes outside headers must be zero: spot-check the last byte if not a header
    return True


# ---------------------------------------------------------------------------
# Fluid bandwidth model
# ---------------------------------------------------------------------------


_EPS = 0.5  # bytes; below this a flow counts as finished / a wait as satisfied


class Flow:
    __slots__ = ("remaining", "buffered", "buffer_cap", "rate", "last", "want", "scope")

    def __init__(self, size: int, buffer_cap: float, now: float):
        self.remaining = float(size)  # bytes not yet transferred over the link
        self.buffered = 0.0  # transferred, not yet consumed
        self.buffer_cap = buffer_cap
        self.rate = 0.0  # bytes/s currently allocated
        self.last = now  # time up to which this flow has been settled
        self.want = 0.0  # buffered level the waiter needs
        self.scope: trio.CancelScope | None = None  # set while a consumer waits

    def headroom(self) -> float:
        return self.buffer_cap - self.buffered


class FluidLink:
    """Processor-sharing link with per-flow and total caps, exact and cheap.

    Rates are piecewise constant: they are only recomputed when a flow opens
    or closes, or a socket buffer fills/drains. Between those events every
    flow advances analytically, and a waiting consumer simply sleeps until the
    computed arrival time of the bytes it needs (a CancelScope deadline that
    is moved if the rates change). No time steps, and in --real-clock runs the
    model costs about one wake-up per 64 KiB piece, like a real socket read.
    """

    def __init__(self, name: str, total_bps: float, per_flow_bps: float):
        self.name = name
        self.total_bps = total_bps if total_bps > 0 else math.inf
        self.per_flow_bps = per_flow_bps if per_flow_bps > 0 else math.inf
        self.flows: set[Flow] = set()
        self._n_active = 0
        self._timer: trio.CancelScope | None = None  # buffer-full events
        self._next_full = math.inf
        self._stats_last: float | None = None
        # stats
        self.bytes_moved = 0.0  # updated when flows are settled
        self.busy_time = 0.0  # time with >= 1 flow transferring
        self.flow_time = 0.0  # integral of number of transferring flows

    @property
    def unlimited(self) -> bool:
        return self.total_bps == math.inf and self.per_flow_bps == math.inf

    def transferring(self) -> int:
        return self._n_active

    def sync(self) -> None:
        """Bring byte/time counters up to date (they are updated lazily)."""
        self._reallocate()

    # -- model ---------------------------------------------------------------

    def _settle(self, f: Flow, now: float) -> None:
        dt = now - f.last
        f.last = now
        if dt <= 0 or f.rate <= 0:
            return
        give = min(f.rate * dt, f.remaining, f.headroom())
        if f.remaining - give < _EPS:
            give = f.remaining
        f.remaining -= give
        f.buffered += give
        self.bytes_moved += give

    def _deadline(self, f: Flow, now: float) -> float:
        need = min(f.want - f.buffered, f.remaining)
        if need <= _EPS:
            return now
        if f.rate <= 0:
            return math.inf
        return now + need / f.rate

    def _reallocate(self) -> None:
        now = trio.current_time()
        if self._stats_last is not None and self._n_active:
            dt = now - self._stats_last
            self.busy_time += dt
            self.flow_time += self._n_active * dt
        self._stats_last = now
        for f in self.flows:
            self._settle(f, now)
        eligible = [f for f in self.flows if f.remaining > 0 and f.headroom() > _EPS]
        self._n_active = len(eligible)
        rate = min(self.per_flow_bps, self.total_bps / len(eligible)) if eligible else 0.0
        next_full = math.inf
        for f in self.flows:
            f.rate = rate if f in eligible else 0.0
            if f.rate > 0 and f.buffer_cap != math.inf and f.headroom() < f.remaining:
                next_full = min(next_full, now + f.headroom() / f.rate)
            if f.scope is not None:
                f.scope.deadline = self._deadline(f, now)
        self._next_full = next_full
        if self._timer is not None:
            self._timer.deadline = next_full

    async def run(self) -> None:
        """Only needed to react to a socket buffer filling up."""
        while True:
            with trio.CancelScope(deadline=self._next_full) as scope:
                self._timer = scope
                await trio.sleep_forever()
            self._timer = None
            self._reallocate()

    async def _wait(self, f: Flow, want: float) -> None:
        now = trio.current_time()
        self._settle(f, now)
        while f.buffered < want - _EPS and f.remaining > 0:
            f.want = want
            with trio.CancelScope(deadline=self._deadline(f, now)) as scope:
                f.scope = scope
                await trio.sleep_forever()
            f.scope = None
            now = trio.current_time()
            self._settle(f, now)

    # -- API -----------------------------------------------------------------

    def open(self, size: int, buffer_cap: float = math.inf) -> Flow:
        f = Flow(size, buffer_cap, trio.current_time())
        self.flows.add(f)
        self._reallocate()
        return f

    def close(self, f: Flow) -> None:
        self.flows.discard(f)
        self._reallocate()

    async def take(self, f: Flow, n: int) -> int:
        """Consume up to n bytes from a flow's buffer, waiting for them."""
        await self._wait(f, n)
        was_full = f.rate <= 0 and f.remaining > 0
        k = int(min(n, f.buffered + _EPS))
        f.buffered = max(0.0, f.buffered - k)
        if was_full:  # buffer drained: the flow competes for bandwidth again
            self._reallocate()
        return k

    async def transfer(self, n: int) -> None:
        """Move n bytes (no buffering semantics), e.g. a disk read/write."""
        if n <= 0 or self.unlimited:
            return
        f = self.open(n)
        try:
            await self._wait(f, float(n))
        finally:
            self.close(f)


# ---------------------------------------------------------------------------
# Fakes: Unity Catalog client and disk persistence
# ---------------------------------------------------------------------------


class Recorder:
    """Per-chunk timeline and counters gathered from the instrumented run."""

    def __init__(self, t0: float):
        self.t0 = t0
        self.chunks: dict[int, dict[str, Any]] = {}
        self.downloads = 0
        self.downloaded_bytes = 0
        self.workers_busy = 0
        self.worker_busy_time = 0.0
        self.ttfb_time = 0.0  # summed over downloads (worker time spent waiting)
        self.disk_write_bytes = 0
        self.disk_read_bytes = 0
        self.errors: list[str] = []

    def now(self) -> float:
        return trio.current_time() - self.t0

    def chunk(self, cid: int) -> dict[str, Any]:
        return self.chunks.setdefault(cid, {"chunk": cid})

    def mark(self, cid: int, key: str, value: Any = None) -> None:
        c = self.chunk(cid)
        if key not in c:
            c[key] = self.now() if value is None else value


class FakeUCClient:
    def __init__(self, cfg: SimConfig, net: FluidLink, rec: Recorder, rng: random.Random):
        self.cfg = cfg
        self.net = net
        self.rec = rec
        self.rng = rng

    def _ttfb(self) -> float:
        t = self.cfg.ttfb_ms
        if self.cfg.ttfb_jitter_ms > 0:
            t = self.rng.gauss(t, self.cfg.ttfb_jitter_ms)
        return max(0.0, t) / 1000.0

    async def download_chunk_stream(self, path, offset, length, *, ctx, if_unmodified_since=None):
        cid = offset // self.cfg.chunk_size
        self.rec.downloads += 1
        self.rec.mark(cid, "net_request")
        ttfb = self._ttfb()
        self.rec.ttfb_time += ttfb
        await trio.sleep(ttfb)
        self.rec.mark(cid, "net_first_byte")
        flow = self.net.open(length, buffer_cap=self.cfg.socket_buffer_kib * KiB)
        try:
            pos = 0
            while pos < length:
                k = await self.net.take(flow, min(NET_PIECE, length - pos))
                if k <= 0:
                    break
                self.rec.downloaded_bytes += k
                yield content_for(self.cfg, offset + pos, k)
                pos += k
        finally:
            self.net.close(flow)
            self.rec.mark(cid, "net_done")


class FakePersistence:
    """Bandwidth-limited stand-in for DiskPersistence. Stores only sizes;
    content is regenerated on read (it is a pure function of the offset)."""

    def __init__(self, cfg: SimConfig, disk: FluidLink, rec: Recorder):
        self.cfg = cfg
        self.disk = disk
        self.rec = rec
        self.stored: dict[tuple, int] = {}

    async def _latency(self) -> None:
        if self.cfg.disk_op_latency_ms > 0:
            await trio.sleep(self.cfg.disk_op_latency_ms / 1000.0)

    async def retrieve_chunk(self, fs_path, chunk_index, mtime, gen=0):
        key = (fs_path, chunk_index, mtime, gen)
        await self._latency()
        size = self.stored.get(key)
        if size is None:
            return None
        await self.disk.transfer(size)
        self.rec.disk_read_bytes += size
        return content_for(self.cfg, chunk_index * self.cfg.chunk_size, size)

    def has_chunk(self, fs_path, chunk_index, mtime, gen=0):
        return (fs_path, chunk_index, mtime, gen) in self.stored

    async def chunk_exists(self, fs_path, chunk_index, mtime, gen=0):
        await self._latency()
        return (fs_path, chunk_index, mtime, gen) in self.stored

    async def store_chunk_from_stream(self, fs_path, chunk_index, mtime, stream, gen=0):
        await self._latency()
        written = 0
        parts = []
        async for piece in stream:
            await self.disk.transfer(len(piece))  # write each piece as it arrives
            written += len(piece)
            parts.append(piece)
        self.stored[(fs_path, chunk_index, mtime, gen)] = written
        self.rec.disk_write_bytes += written
        self.rec.mark(chunk_index, "disk_written")
        return b"".join(parts)


# ---------------------------------------------------------------------------
# DataManager construction + instrumentation (no changes to the real code)
# ---------------------------------------------------------------------------


def build_data_manager(cfg: SimConfig, uc: FakeUCClient, pers: FakePersistence, rec: Recorder) -> DataManager:
    dm = DataManager(uc, pers, ram_cache_mb=cfg.ram_cache_mb, num_workers=cfg.num_workers, prefetch_mb=cfg.prefetch_mb,
                     prefetch_to_ram=cfg.prefetch_to_ram)
    if cfg.chunk_size != dm.chunk_size:
        # chunk_size is hard-coded to 8 MiB; override and recompute what __init__ derived from it.
        dm.chunk_size = cfg.chunk_size
        dm._ram_cache = RamCache(max_entries=int(cfg.ram_cache_mb * MiB / cfg.chunk_size))
        dm._chunks_to_prefetch = max(0, int(cfg.prefetch_mb * MiB / cfg.chunk_size))

    sched = dm._download_scheduler
    orig_enqueue = sched.enqueue_request
    orig_process = sched._process_request

    async def enqueue_request(chunk_request, high_priority):
        cid = chunk_request.chunk_id
        rec.mark(cid, "enqueued")
        rec.mark(cid, "enqueued_as", "high" if high_priority else "regular")
        if high_priority:
            rec.mark(cid, "enqueued_high")
        await orig_enqueue(chunk_request, high_priority)

    async def process_request(chunk_request, priority):
        cid = chunk_request.chunk_id
        rec.mark(cid, "worker_start")
        rec.mark(cid, "processed_as", priority)
        rec.workers_busy += 1
        t = trio.current_time()
        try:
            await orig_process(chunk_request, priority)
        finally:
            rec.workers_busy -= 1
            rec.worker_busy_time += trio.current_time() - t
            rec.mark(cid, "worker_done")

    sched.enqueue_request = enqueue_request
    sched._process_request = process_request
    return dm


# ---------------------------------------------------------------------------
# Kernel model + one simulation run
# ---------------------------------------------------------------------------


def percentile(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * p
    lo, hi = math.floor(k), math.ceil(k)
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (k - lo)


def analytic_estimate(cfg: SimConfig) -> dict[str, float]:
    """Back-of-the-envelope steady state, for comparison with the simulation."""
    chunk = cfg.chunk_size
    prefetch_chunks = max(0, int(cfg.prefetch_mb * MiB / chunk))
    parallel = max(1, min(cfg.num_workers, prefetch_chunks + 1))
    per = min(cfg.per_stream_mib_s, cfg.global_mib_s / parallel) * MiB
    t_chunk = cfg.ttfb_ms / 1000 + chunk / per
    net = parallel * chunk / t_chunk
    est = min(net, cfg.global_mib_s * MiB)
    # Every byte is written to the disk cache once. It is also read back once,
    # unless prefetch keeps chunks in RAM and the RAM cache can hold the whole
    # prefetch window (plus the chunk being read) until the reader gets there.
    ram_entries = int(cfg.ram_cache_mb * MiB / chunk)
    read_back = not (cfg.prefetch_to_ram and ram_entries >= prefetch_chunks + 1)
    disk_passes = 2 if read_back else 1
    if cfg.disk_mib_s > 0:
        est = min(est, cfg.disk_mib_s * MiB / disk_passes)
    if cfg.dest_mib_s > 0:
        est = min(est, cfg.dest_mib_s * MiB)
    return {"parallel_downloads": parallel, "chunk_time_s": t_chunk, "disk_passes": disk_passes,
            "throughput_mib_s": est / MiB}


async def _simulate(cfg: SimConfig) -> dict[str, Any]:
    rng = random.Random(cfg.seed)
    t0 = trio.current_time()
    rec = Recorder(t0)
    net = FluidLink("net", cfg.global_mib_s * MiB, cfg.per_stream_mib_s * MiB)
    disk = FluidLink("disk", cfg.disk_mib_s * MiB, 0)
    uc = FakeUCClient(cfg, net, rec, rng)
    pers = FakePersistence(cfg, disk, rec)
    dm = build_data_manager(cfg, uc, pers, rec)
    ctx = SimpleNamespace(uid=1000, gid=1000, pid=4242)

    size = cfg.file_size
    req = cfg.request_kib * KiB
    reads: list[dict[str, float]] = []
    progress = {"bytes": 0, "next_offset": 0, "max_chunk": -1}
    timeseries: list[dict[str, float]] = []
    sched = dm._download_scheduler

    async def kernel_slot():
        while True:
            off = progress["next_offset"]
            if off >= size:
                return
            length = min(req, size - off)
            progress["next_offset"] = off + length
            first, last = off // cfg.chunk_size, (off + length - 1) // cfg.chunk_size
            for cid in range(first, last + 1):
                rec.mark(cid, "first_read_request")
            t = trio.current_time()
            data = await dm.read(FS_PATH, off, length, MTIME, size, ctx)
            lat = trio.current_time() - t
            for cid in range(first, last + 1):
                rec.mark(cid, "first_read_done")
            if len(data) != length:
                rec.errors.append(f"short read at {off}: {len(data)} != {length}")
            elif cfg.verify and not verify_content(off, data):
                rec.errors.append(f"bad content at {off}")
            reads.append({"t": t - t0, "offset": off, "latency": lat})
            progress["bytes"] += len(data)
            progress["max_chunk"] = max(progress["max_chunk"], last)
            if cfg.dest_mib_s > 0:  # cp writing to the destination file
                await trio.sleep(length / (cfg.dest_mib_s * MiB))

    async def sampler():
        last_t, last_net, last_disk, last_read = 0.0, 0.0, 0.0, 0
        while True:
            await trio.sleep(cfg.sample_ms / 1000)
            net.sync()
            disk.sync()
            t = trio.current_time() - t0
            dt = t - last_t
            cur = progress["max_chunk"]
            ready_ahead = sum(1 for (_, cid, _, _) in pers.stored if cid > cur)
            timeseries.append({
                "t": round(t, 4),
                "read_mib": progress["bytes"] / MiB,
                "read_mib_s": (progress["bytes"] - last_read) / dt / MiB,
                "net_mib_s": (net.bytes_moved - last_net) / dt / MiB,
                "disk_mib_s": (disk.bytes_moved - last_disk) / dt / MiB,
                "net_flows": net.transferring(),
                "workers_busy": rec.workers_busy,
                "queue_high": len(sched._requests_priority),
                "queue_regular": len(sched._requests_regular),
                "inflight": len(dm._inflight_coalescer._inflight),
                "chunks_ready_ahead": ready_ahead,
            })
            last_t, last_net, last_disk, last_read = t, net.bytes_moved, disk.bytes_moved, progress["bytes"]

    wall0 = time.perf_counter()
    async with trio.open_nursery() as services:
        services.start_soon(net.run)
        services.start_soon(disk.run)
        services.start_soon(sampler)
        dm.run_services(services)
        async with trio.open_nursery() as readers:
            for _ in range(cfg.kernel_concurrency):
                readers.start_soon(kernel_slot)
        total = trio.current_time() - t0
        net.sync()
        disk.sync()
        # Let pending prefetches drain so byte counters are final, then stop.
        with trio.move_on_after(5):
            while rec.workers_busy or len(sched._requests_priority) or len(sched._requests_regular):
                await trio.sleep(0.01)
        dm.close()
        services.cancel_scope.cancel()
    wall = time.perf_counter() - wall0

    # ---- summary ----
    lats = sorted(r["latency"] for r in reads)
    blocked = [x for x in lats if x > 0.001]
    chunk_list = [rec.chunks[k] for k in sorted(rec.chunks)]
    stalls = []
    for c in chunk_list:
        if "first_read_request" in c and "first_read_done" in c:
            c["stall_s"] = c["first_read_done"] - c["first_read_request"]
            stalls.append(c["stall_s"])
        if "enqueued" in c and "worker_start" in c:
            c["queue_wait_s"] = c["worker_start"] - c["enqueued"]
    n_chunks = math.ceil(size / cfg.chunk_size)
    ideal = size / (cfg.global_mib_s * MiB)
    est = analytic_estimate(cfg)
    first_read_latency = reads[0]["latency"] if reads else 0.0
    high_downloads = sum(1 for c in chunk_list if c.get("processed_as") == "high" and "net_request" in c)
    summary = {
        "total_s": total,
        "throughput_mib_s": size / total / MiB if total > 0 else 0.0,
        "ideal_s": ideal,
        "efficiency_vs_link": ideal / total if total > 0 else 0.0,
        "lost_s_vs_link": total - ideal,
        "analytic_estimate_mib_s": est["throughput_mib_s"],
        "first_read_latency_s": first_read_latency,
        "reads": len(reads),
        "reads_blocked": len(blocked),
        "blocked_time_s": sum(blocked),
        "read_latency_ms": {
            "p50": percentile(lats, 0.5) * 1000,
            "p95": percentile(lats, 0.95) * 1000,
            "p99": percentile(lats, 0.99) * 1000,
            "max": (lats[-1] if lats else 0.0) * 1000,
        },
        "chunks": n_chunks,
        "downloads": rec.downloads,
        "duplicate_downloads": rec.downloads - len([c for c in chunk_list if "net_request" in c]),
        "downloads_by_reader_high_prio": high_downloads,
        "downloads_by_prefetch": rec.downloads - high_downloads,
        "overfetch_ratio": rec.downloaded_bytes / size if size else 0.0,
        "avg_net_mib_s": net.bytes_moved / total / MiB if total else 0.0,
        "net_link_utilization": net.bytes_moved / total / (cfg.global_mib_s * MiB) if total else 0.0,
        "avg_transferring_flows": net.flow_time / total if total else 0.0,
        "avg_workers_busy": rec.worker_busy_time / total if total else 0.0,
        "worker_time_in_ttfb_frac": rec.ttfb_time / rec.worker_busy_time if rec.worker_busy_time else 0.0,
        "disk_write_mib": rec.disk_write_bytes / MiB,
        "disk_read_mib": rec.disk_read_bytes / MiB,
        "disk_utilization": disk.bytes_moved / total / (cfg.disk_mib_s * MiB) if total and cfg.disk_mib_s > 0 else 0.0,
        "chunk_stall_s": {
            "sum": sum(stalls),
            "p50": percentile(sorted(stalls), 0.5),
            "max": max(stalls) if stalls else 0.0,
            "chunks_with_stall_gt_10ms": sum(1 for s in stalls if s > 0.01),
        },
        "errors": rec.errors[:10],
        "error_count": len(rec.errors),
        "wall_clock_s": wall,
    }
    return {"config": dataclasses.asdict(cfg), "summary": summary, "timeseries": timeseries, "chunks": chunk_list}


def run(cfg: SimConfig) -> dict[str, Any]:
    if cfg.real_clock:
        return trio.run(_simulate, cfg)
    clock = trio.testing.MockClock(autojump_threshold=0)
    return trio.run(_simulate, cfg, clock=clock)


# ---------------------------------------------------------------------------
# Sensitivity sweep (one factor at a time around the baseline)
# ---------------------------------------------------------------------------

SWEEP: dict[str, list[Any]] = {
    "prefetch_mb": [0, 8, 16, 40, 80, 160, 320],
    "num_workers": [1, 2, 5, 10, 15, 20, 40],
    "ttfb_ms": [0, 25, 50, 100, 200, 400],
    "chunk_mib": [1, 2, 4, 8, 16, 32],
    "disk_mib_s": [100, 200, 300, 500, 1000, 0],
    "request_kib": [128, 1024],
    "kernel_concurrency": [1, 2, 12],
    "dest_mib_s": [100, 150, 300, 1000, 0],
}


def _run_summary(cfg: SimConfig) -> dict[str, Any]:
    return run(cfg)["summary"]


def run_sweep(base: SimConfig, factors: list[str], log, jobs: int = 1) -> dict[str, Any]:
    from concurrent.futures import ProcessPoolExecutor

    tasks = [(factor, value) for factor in factors for value in SWEEP[factor]]
    cfgs = [dataclasses.replace(base, **{factor: value}) for factor, value in tasks]
    baseline = run(base)
    log(f"baseline: {baseline['summary']['throughput_mib_s']:.1f} MiB/s in {baseline['summary']['total_s']:.2f} s")
    if jobs > 1:
        with ProcessPoolExecutor(max_workers=jobs, max_tasks_per_child=1) as ex:
            summaries = list(ex.map(_run_summary, cfgs))
    else:
        summaries = [_run_summary(c) for c in cfgs]
    results: dict[str, list[dict[str, Any]]] = {f: [] for f in factors}
    for (factor, value), s in zip(tasks, summaries):
        log(f"  {factor}={value}: {s['throughput_mib_s']:.1f} MiB/s ({s['total_s']:.2f} s, wall {s['wall_clock_s']:.1f} s)")
        results[factor].append({"value": value, "summary": s})
    ranking = []
    for factor, rows in results.items():
        tps = [r["summary"]["throughput_mib_s"] for r in rows]
        ranking.append({
            "factor": factor,
            "min_mib_s": min(tps),
            "max_mib_s": max(tps),
            "range_mib_s": max(tps) - min(tps),
            "best_value": rows[tps.index(max(tps))]["value"],
            "worst_value": rows[tps.index(min(tps))]["value"],
        })
    ranking.sort(key=lambda x: -x["range_mib_s"])
    return {"baseline": baseline, "sweep": results, "ranking": ranking, "sweep_values": {f: SWEEP[f] for f in factors}}


# ---------------------------------------------------------------------------
# Plots (optional; requires matplotlib)
# ---------------------------------------------------------------------------


def plot_run(result: dict[str, Any], path: str) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    ts = result["timeseries"]
    cfg = result["config"]
    t = [p["t"] for p in ts]
    fig, axes = plt.subplots(4, 1, figsize=(11, 11), sharex=True)
    ax = axes[0]
    ax.plot(t, [p["net_mib_s"] for p in ts], label="network")
    ax.plot(t, [p["read_mib_s"] for p in ts], label="delivered to cp", alpha=0.8)
    ax.plot(t, [p["disk_mib_s"] for p in ts], label="disk cache (r+w)", alpha=0.6)
    ax.axhline(cfg["global_mib_s"], color="grey", ls="--", lw=1, label="link cap")
    ax.set_ylabel("MiB/s")
    ax.legend(loc="upper right", fontsize=8)
    ax = axes[1]
    ax.plot(t, [p["net_flows"] for p in ts], label="flows transferring")
    ax.plot(t, [p["workers_busy"] for p in ts], label="workers busy", alpha=0.8)
    ax.set_ylabel("count")
    ax.legend(loc="upper right", fontsize=8)
    ax = axes[2]
    ax.plot(t, [p["queue_high"] for p in ts], label="queue high")
    ax.plot(t, [p["queue_regular"] for p in ts], label="queue regular")
    ax.plot(t, [p["chunks_ready_ahead"] for p in ts], label="chunks on disk ahead of reader")
    ax.set_ylabel("chunks")
    ax.legend(loc="upper right", fontsize=8)
    ax = axes[3]
    ax.plot(t, [p["read_mib"] for p in ts], label="read progress")
    ideal = [min(x * cfg["global_mib_s"], cfg["file_mib"]) for x in t]
    ax.plot(t, ideal, ls="--", color="grey", label="ideal (link cap)")
    ax.set_ylabel("MiB read")
    ax.set_xlabel("time (s)")
    ax.legend(loc="lower right", fontsize=8)
    s = result["summary"]
    fig.suptitle(f"{s['throughput_mib_s']:.1f} MiB/s, {s['total_s']:.2f} s "
                 f"(ideal {s['ideal_s']:.2f} s) - workers={cfg['num_workers']} prefetch={cfg['prefetch_mb']}MB "
                 f"ttfb={cfg['ttfb_ms']}ms disk={cfg['disk_mib_s']}MiB/s")
    fig.tight_layout()
    fig.savefig(path, dpi=110)
    plt.close(fig)


def plot_sweep(result: dict[str, Any], path: str) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    sweep = result["sweep"]
    base = result["baseline"]
    bcfg, bsum = base["config"], base["summary"]
    n = len(sweep)
    cols = 3
    rows = math.ceil((n + 1) / cols)
    fig, axes = plt.subplots(rows, cols, figsize=(5 * cols, 3.6 * rows))
    axes = list(axes.flat)
    for ax, (factor, data) in zip(axes, sweep.items()):
        labels = [("inf" if (v == 0 and factor in ("disk_mib_s", "dest_mib_s")) else str(v)) for v in (d["value"] for d in data)]
        tps = [d["summary"]["throughput_mib_s"] for d in data]
        est = [d["summary"]["analytic_estimate_mib_s"] for d in data]
        x = range(len(labels))
        ax.plot(x, tps, marker="o", label="simulated")
        ax.plot(x, est, ls=":", marker=".", color="tab:orange", label="analytic estimate")
        ax.axhline(bcfg["global_mib_s"], color="grey", ls="--", lw=1)
        bval = bcfg[factor]
        if bval in [d["value"] for d in data]:
            i = [d["value"] for d in data].index(bval)
            ax.plot([i], [tps[i]], marker="*", ms=14, color="tab:red", label="baseline")
        ax.set_xticks(list(x))
        ax.set_xticklabels(labels)
        ax.set_title(factor)
        ax.set_ylim(0, bcfg["global_mib_s"] * 1.1)
        ax.set_ylabel("MiB/s")
        ax.legend(fontsize=7)
    ax = axes[n]
    rk = result["ranking"]
    ax.barh([r["factor"] for r in rk][::-1], [r["range_mib_s"] for r in rk][::-1])
    ax.set_title("impact: throughput range (MiB/s)")
    for a in axes[n + 1:]:
        a.axis("off")
    fig.suptitle(f"Sensitivity around baseline ({bsum['throughput_mib_s']:.1f} MiB/s)")
    fig.tight_layout()
    fig.savefig(path, dpi=110)
    plt.close(fig)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    defaults = SimConfig()
    for f in dataclasses.fields(SimConfig):
        name = "--" + f.name.replace("_", "-")
        default = getattr(defaults, f.name)
        if isinstance(default, bool):
            p.add_argument(name, action=argparse.BooleanOptionalAction, default=default)
        else:
            p.add_argument(name, type=type(default), default=default, help=f"(default: {default})")
    p.add_argument("--sweep", action="store_true", help="one-factor-at-a-time sensitivity sweep around the given config")
    p.add_argument("--factors", default=",".join(SWEEP), help="comma-separated factors for --sweep")
    p.add_argument("--jobs", type=int, default=1, help="parallel processes for --sweep")
    p.add_argument("--out", default="-", help="JSON output file ('-' = stdout)")
    p.add_argument("--plot", default=None, help="PNG output file (requires matplotlib)")
    p.add_argument("--no-chunks", action="store_true", help="omit per-chunk timeline from JSON")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    cfg = SimConfig(**{f.name: getattr(args, f.name) for f in dataclasses.fields(SimConfig)})

    def log(msg: str) -> None:
        print(msg, file=sys.stderr, flush=True)

    if args.sweep:
        factors = [f for f in args.factors.split(",") if f]
        unknown = [f for f in factors if f not in SWEEP]
        if unknown:
            raise SystemExit(f"unknown factors: {unknown}")
        result = run_sweep(cfg, factors, log, jobs=args.jobs)
        for rows in result["sweep"].values():
            for row in rows:
                row["summary"].pop("errors", None)
        if args.no_chunks:
            result["baseline"].pop("chunks", None)
        if args.plot:
            plot_sweep(result, args.plot)
        log("ranking (throughput range across tested values):")
        for r in result["ranking"]:
            log(f"  {r['factor']:<20} {r['min_mib_s']:6.1f} .. {r['max_mib_s']:6.1f} MiB/s  (best={r['best_value']}, worst={r['worst_value']})")
    else:
        result = run(cfg)
        if args.no_chunks:
            result.pop("chunks", None)
        if args.plot:
            plot_run(result, args.plot)
        s = result["summary"]
        log(f"{s['throughput_mib_s']:.1f} MiB/s, {s['total_s']:.2f} s (ideal {s['ideal_s']:.2f} s, "
            f"analytic {s['analytic_estimate_mib_s']:.1f} MiB/s), errors={s['error_count']}")

    text = json.dumps(result, indent=1, default=lambda o: None if o in (math.inf,) else str(o))
    if args.out == "-":
        print(text)
    else:
        with open(args.out, "w") as fh:
            fh.write(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
