"""
Disk Persistence Layer for FUSE.
Implements streaming writes, single-level sharding, and lazy LRU eviction.
"""

import hashlib
import logging
import os
import shutil
import time
from heapq import heappop, heappush
from typing import AsyncGenerator, Tuple

import trio

from fuse4dbricks.perf import profiling

logger = logging.getLogger(__name__)


def clear_cache(cache_dir: str):
    """Removes *.tmp and *.bin files from cache_dir recursively"""
    logging.info("Clearing cache... %s", cache_dir)
    for root, _, files in os.walk(cache_dir):
        for f in files:
            path = os.path.join(root, f)
            if path.endswith(".tmp") or path.endswith(".bin"):
                os.unlink(path)


class DiskPersistence:
    def __init__(self, cache_dir: str, max_size_gb: int = 2048, max_age_days: int = 7):
        self.cache_dir = cache_dir
        self.max_size_bytes = float(max_size_gb) * 1024**3
        self.max_age_seconds = max_age_days * 86400
        self.start_time = time.time()

        self.current_size = 0
        self.lock = trio.Lock()
        self.access_log: list[Tuple[float, str, int]] = []  # Heap: (now, cache_path, bytes_written)
        self.access_map: dict[str, float] = {}  # {cache_path: latest_use_timestamp}

    def run_services(self, nursery):
        """Starts background maintenance and discovery."""
        nursery.start_soon(self._graceful_init)
        nursery.start_soon(self._background_maintenance)

    def _get_chunk_path(self, fs_path: str, chunk_index: int, mtime: float, gen: int = 0) -> str:
        """Compute the cache path for a chunk (256-shard distribution).

        Pure: does NOT touch the filesystem. ``retrieve_chunk`` runs on every
        read (cache hits included), so creating the shard dir here meant a
        blocking ``os.makedirs`` syscall in the async hot path plus empty dirs
        for chunks that are never written. Directory creation now lives on the
        write path only (see ``store_chunk_from_stream`` /
        ``_ensure_parent_dir``); a read of a missing chunk simply hits
        FileNotFoundError and returns None.
        """
        sha256_hash = hashlib.sha256(fs_path.encode("utf-8")).hexdigest()
        shard1 = sha256_hash[:2]
        shard2 = f"{(chunk_index // 1000):07d}"  # Secondary shard to prevent too many files in one dir
        shard_dir = os.path.join(self.cache_dir, shard1, shard2)
        mtime_ms = int(mtime * 1000)
        # gen is a local cache epoch (see DataManager.invalidate_path); it keeps
        # a same-second overwrite from reusing a stale chunk file on disk.
        return os.path.join(
            shard_dir, f"{sha256_hash}_{mtime_ms}_g{gen}_{chunk_index:07d}.bin"
        )

    @staticmethod
    def _ensure_parent_dir(path: str) -> None:
        """Create the parent directory of ``path`` (idempotent). Runs off the
        event loop via ``trio.to_thread`` from the write path."""
        os.makedirs(os.path.dirname(path), exist_ok=True)

    async def _graceful_init(self):
        """Non-blocking cache discovery."""
        logger.info("Starting graceful cache discovery...")
        await trio.to_thread.run_sync(self._sync_scan)
        logger.info(f"Discovery complete. Initial size: {self.current_size/1e9:.2f} GB")

    def _sync_scan(self):
        """Synchronous recursive walk to rebuild state."""
        for root, _, files in os.walk(self.cache_dir):
            for f in files:
                cache_path = os.path.join(root, f)
                try:
                    stat = os.stat(cache_path)

                    if f.endswith(".tmp"):
                        if stat.st_mtime < self.start_time:
                            os.remove(cache_path)
                        continue

                    self.current_size += stat.st_size
                    self.access_map[cache_path] = stat.st_atime
                    heappush(self.access_log, (stat.st_atime, cache_path, stat.st_size))
                except OSError:
                    continue

    async def retrieve_chunk(
        self, fs_path: str, chunk_index: int, mtime: float, gen: int = 0
    ) -> bytes | None:
        """
        Retrieves a chunk from disk.
        Updates LRU access time BEFORE reading to prevent concurrent eviction.
        """
        cache_path = self._get_chunk_path(fs_path, chunk_index, mtime, gen)

        # 1. OPTIMISTIC PROMOTION (Pinning)
        # Mark as accessed so GC doesn't delete it while we read.
        # Double-check pattern is not strictly needed here for safety,
        # but the lock is needed to update the map safely.
        async with self.lock:
            if cache_path in self.access_map:
                self.access_map[cache_path] = time.time()

        try:
            # 2. READ (Safe now)
            data = await trio.to_thread.run_sync(self._read_file, cache_path)

            # 3. SELF-HEALING
            # If file exists but wasn't in map (race condition or init miss)
            if cache_path not in self.access_map:
                async with self.lock:
                    if cache_path not in self.access_map:
                        self.current_size += len(data)
                        now = time.time()
                        self.access_map[cache_path] = now
                        heappush(self.access_log, (now, cache_path, len(data)))

            profiling.cache_event("disk", hit=True)
            return data

        except FileNotFoundError:
            profiling.cache_event("disk", hit=False)
            return None

    def _read_file(self, path: str) -> bytes:
        with open(path, "rb") as f:
            return f.read()

    async def chunk_exists(
        self, fs_path: str, chunk_index: int, mtime: float, gen: int = 0
    ) -> bool:
        """Whether a chunk is already cached on disk, without reading its
        content. For a prefetch, the caller only needs to know whether a
        download is required; reading the full chunk back (as
        ``retrieve_chunk`` does) would cost as much I/O as the read it is
        trying to save.
        """
        cache_path = self._get_chunk_path(fs_path, chunk_index, mtime, gen)
        async with self.lock:
            if cache_path in self.access_map:
                self.access_map[cache_path] = time.time()
        return await trio.to_thread.run_sync(os.path.exists, cache_path)

    async def store_chunk_from_stream(
        self,
        fs_path: str,
        chunk_index: int,
        mtime: float,
        stream: AsyncGenerator[bytes, None],
        gen: int = 0,
    ) -> bytes:
        """Consumes a stream and writes it to disk. Returns the bytes written."""
        cache_path = self._get_chunk_path(fs_path, chunk_index, mtime, gen)
        temp_path = f"{cache_path}.{os.getpid()}.tmp"
        result = bytearray()
        bytes_written = 0
        span = profiling.start("disk_cache_store", path=fs_path, category="cache")
        try:
            t_prep = profiling.now_ms()
            # The shard dir is created lazily here (off-thread), only when we
            # actually write a chunk — not on every read in _get_chunk_path.
            await trio.to_thread.run_sync(self._ensure_parent_dir, cache_path)

            # Write to .tmp (No lock needed)
            async with await trio.open_file(temp_path, "wb") as f:
                profiling.record_phase(
                    "disk_prepare", profiling.now_ms() - t_prep, path=fs_path, category="cache",
                )
                # Note: the download (request, TTFB, network) runs lazily
                # inside this ``async for``; it is timed by the generator.
                write_ms = 0.0
                n_writes = 0
                async for chunk in stream:
                    t_w = profiling.now_ms()
                    await f.write(chunk)
                    write_ms += profiling.now_ms() - t_w
                    n_writes += 1
                    bytes_written += len(chunk)
                    result.extend(chunk)
                profiling.record_phase(
                    "disk_write_time", write_ms, path=fs_path, category="cache",
                    nbytes=bytes_written, disk_writes=n_writes,
                )
                t_close = profiling.now_ms()
            profiling.record_phase(
                "disk_close", profiling.now_ms() - t_close, path=fs_path, category="cache",
            )

            t_fin = profiling.now_ms()
            # Atomic Rename (No lock needed)
            await trio.to_thread.run_sync(os.rename, temp_path, cache_path)
            t_renamed = profiling.now_ms()

            # 1. EVICT (Manages Lock Internally)
            await self.evict(bytes_written)
            t_evicted = profiling.now_ms()

            # 2. UPDATE METADATA (Acquire Lock)
            async with self.lock:
                t_locked = profiling.now_ms()
                # If overwriting, logic could go here to subtract old size
                self.current_size += bytes_written
                now = time.time()
                self.access_map[cache_path] = now
                heappush(self.access_log, (now, cache_path, bytes_written))
            t_done = profiling.now_ms()
            profiling.record_phase("cache_rename", t_renamed - t_fin, path=fs_path, category="cache", exclusive=False)
            profiling.record_phase("cache_evict", t_evicted - t_renamed, path=fs_path, category="cache", exclusive=False)
            profiling.record_phase("cache_lock_wait", t_locked - t_evicted, path=fs_path, category="cache", exclusive=False)
            profiling.record_phase("cache_metadata_update", t_done - t_locked, path=fs_path, category="cache", exclusive=False)
            profiling.record_phase("cache_finalize", t_done - t_fin, path=fs_path, category="cache")
            span.finish(status="success", nbytes=bytes_written)
            t_mat = profiling.now_ms()
            data = bytes(result)
            profiling.record_phase(
                "chunk_materialize", profiling.now_ms() - t_mat, path=fs_path,
                category="cache", nbytes=bytes_written,
            )
            return data
        except Exception as e:
            if os.path.exists(temp_path):
                await trio.to_thread.run_sync(os.remove, temp_path)
            span.finish(status="error", nbytes=bytes_written)
            raise e

    async def evict(self, required_space: int):
        """
        Free up space removing old items.
        THREAD-SAFE: Uses lock for state, releases for I/O.
        """
        while True:
            # 1. DECISION PHASE (Lock Held)
            async with self.lock:
                if self.current_size + required_space <= self.max_size_bytes:
                    return

                if not self.access_log:
                    return

                ts_log, path, size = heappop(self.access_log)

                if path not in self.access_map:
                    continue

                # Lazy check: if map has newer timestamp, repush
                if self.access_map[path] > ts_log:
                    heappush(self.access_log, (self.access_map[path], path, size))
                    continue

                # Remove from index immediately
                del self.access_map[path]
                self.current_size -= size
                profiling.cache_eviction("disk")

            # 2. I/O PHASE (Lock Released)
            try:
                await trio.to_thread.run_sync(os.remove, path)
                logger.debug(f"Evicted: {path}")
            except OSError:
                pass

    async def _background_maintenance(self):
        """
        Hourly cleanup. Uses snapshot approach to avoid blocking.
        """
        while True:
            await trio.sleep(3600)
            now = time.time()

            # 1. Fast Snapshot (Lock Held)
            async with self.lock:
                try:
                    usage = await trio.to_thread.run_sync(
                        shutil.disk_usage, self.cache_dir
                    )
                    disk_critical = (usage.free / usage.total) < 0.05
                except OSError:
                    disk_critical = False  # Disk might be unmounted or weird error

                expired_candidates = [
                    path
                    for path, ts in self.access_map.items()
                    if (now - ts > self.max_age_seconds) or disk_critical
                ]

            # 2. Slow Deletion (Lock Released)
            for path in expired_candidates:
                await trio.sleep(0)  # Cooperative yield

                try:
                    stat = await trio.to_thread.run_sync(os.stat, path)
                    await trio.to_thread.run_sync(os.remove, path)

                    # 3. State Update (Lock Held)
                    async with self.lock:
                        if path not in self.access_map:
                            continue

                        del self.access_map[path]
                        self.current_size -= stat.st_size
                except OSError:
                    pass
