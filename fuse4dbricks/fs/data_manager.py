from dataclasses import dataclass
import errno
import logging
from typing import Tuple
from collections import OrderedDict
import trio

import pyfuse3

from fuse4dbricks.fs.utils import fs_to_uc_path, InflightCoalescer

from fuse4dbricks.api.uc_client import UnityCatalogClient
from fuse4dbricks.storage.persistence import DiskPersistence
from fuse4dbricks.fs.ram_cache import RamCache

logger = logging.getLogger(__name__)


@dataclass
class _ChunkRequest:
    fs_path: str
    "path to download"
    chunk_id: int
    "piece to download"
    mtime: float
    "last modified, to ensure chunks are not altered between reads"
    gen: int
    "cache generation: bumped on a local write so a same-second overwrite (mtime has 1s resolution) is not served stale"
    chunk_size: int
    "size of this chunk. It may be smaller than the typical chunk_size on the last chunk of o a file"
    ctx: pyfuse3.RequestContext
    "request context, used for auth when downloading"


# Chunk cache key: (fs_path, chunk_id, mtime, gen). mtime guards against the
# remote file changing under us; gen is a local epoch bumped on every write so a
# stale chunk is unreachable even when the server mtime (1s resolution) collides.
_CacheKey = Tuple[str, int, float, int]

class DownloadScheduler:
    def __init__(self, process_request, num_workers=10):
        self._requests_lock = trio.Lock()
        self._requests_priority: OrderedDict[_CacheKey, _ChunkRequest] = OrderedDict()
        self._requests_regular: OrderedDict[_CacheKey, _ChunkRequest] = OrderedDict()
        self._requests_num_workers = num_workers
        self._process_request = process_request

        self._wake_send, self._wake_recv = trio.open_memory_channel[None](max_buffer_size=1)

    def run_services(self, nursery):
        for i in range(self._requests_num_workers):
            nursery.start_soon(self._downloader)

    async def _dequeue_one(self) -> tuple[_CacheKey | None, _ChunkRequest | None, str]:
        async with self._requests_lock:
            try:
                cache_key, chunk_request = self._requests_priority.popitem(last=False)
                return cache_key, chunk_request, "high"
            except KeyError:
                pass
            try:
                cache_key, chunk_request = self._requests_regular.popitem(last=False)
                return cache_key, chunk_request, "regular"
            except KeyError:
                return None, None, "regular"

    def close(self) -> None:
        # Closing send channel will eventually make receivers get EndOfChannel
        self._wake_send.close()

    async def _downloader(self):
        while True:
            cache_key, chunk_request, priority = await self._dequeue_one()
            if cache_key is None or chunk_request is None:
                # Nothing to do: block until someone enqueues work (no polling).
                try:
                    await self._wake_recv.receive()
                except trio.EndOfChannel:
                    return
                continue
            # A failed download must never escape the worker: otherwise the
            # exception would propagate to the nursery and tear down the whole
            # mount for every user. _process_request always wakes waiters (via
            # notify_done in its finally), so they observe a missing chunk and
            # the read fails with EIO in isolation. trio.Cancelled is a
            # BaseException and still propagates, so shutdown is unaffected.
            try:
                await self._process_request(chunk_request, priority)
            except Exception:
                logger.exception(
                    "Chunk download worker failed for %s (chunk %s, mtime %s)",
                    chunk_request.fs_path, chunk_request.chunk_id, chunk_request.mtime,
                )

    async def _poke_worker(self) -> None:
        # Non-blocking "poke"; if buffer is full, a worker is already awake.
        try:
            self._wake_send.send_nowait(None)
        except trio.WouldBlock:
            pass

    async def enqueue_request(self, chunk_request: _ChunkRequest, high_priority: bool):
        cache_key = (chunk_request.fs_path, chunk_request.chunk_id, chunk_request.mtime, chunk_request.gen)
        async with self._requests_lock:
            if high_priority:
                # If request for that chunk was present as a regular request, promote it:
                if cache_key in self._requests_regular:
                    self._requests_regular.pop(cache_key)
                self._requests_priority[cache_key] = chunk_request
                await self._poke_worker()
            else:
                # If request for that chunk was already a priority request, ignore it, otherwise add it
                if cache_key not in self._requests_priority:
                    self._requests_regular[cache_key] = chunk_request
                    await self._poke_worker()


class DataManager:
    def __init__(self, uc_client: UnityCatalogClient, persistence: DiskPersistence, ram_cache_mb = 512, num_workers=10, prefetch_mb=80, prefetch_to_ram: bool = True):
        """
        uc_client: client to download chunks from UC
        persistence: where to store downloaded chunks on disk
        ram_cache_mb: how much RAM to use for caching chunks in memory (LRU eviction)
        num_workers: how many concurrent downloads to allow
        prefetch_to_ram: also keep freshly prefetched chunks in the RAM cache,
            so the read that reaches them does not have to read them back from
            disk (and concurrent reads of the same chunk do not each do so)
        """
        self.uc_client = uc_client
        self.persistence = persistence
        self.chunk_size = 8 * 1024 * 1024
        self._inflight_coalescer: InflightCoalescer[_CacheKey] = InflightCoalescer()
        "Inflight coalescer for downloads identified by (fs_path, chunk_id, mtime, gen)"

        if num_workers < 1:
            raise ValueError("num_workers must be >= 1")

        self._download_scheduler = DownloadScheduler(process_request=self._process_request, num_workers=num_workers)

        # Per-path cache generation. Bumped on a local write so previously cached
        # chunks become unreachable without scanning. Absent path => generation 0.
        self._write_generation: dict[str, int] = {}

        # max number of chunks to cache in memory
        max_ram_chunks = int(ram_cache_mb*1024*1024 / self.chunk_size)
        self._ram_cache: RamCache[_CacheKey] = RamCache(max_entries = max_ram_chunks)
        prefetch_bytes = prefetch_mb * 1024 * 1024
        chunks_to_prefetch = int(prefetch_bytes / self.chunk_size)
        if chunks_to_prefetch < 0:
            chunks_to_prefetch = 0
        self._chunks_to_prefetch = chunks_to_prefetch
        self._prefetch_to_ram = prefetch_to_ram

    def close(self) -> None:
        self._download_scheduler.close()

    def _generation(self, fs_path: str) -> int:
        return self._write_generation.get(fs_path, 0)

    async def invalidate_path(self, fs_path: str) -> None:
        """Drop the cached content of ``fs_path`` after a local write.

        O(1): bumps the path's cache generation rather than scanning RAM or
        globbing disk. Subsequent reads compute a new key, so the old chunks are
        unreachable and age out via the RAM/disk LRUs. Necessary because chunks
        are keyed by the server ``mtime``, which has only 1-second resolution and
        so cannot distinguish two writes made within the same second.
        """
        self._write_generation[fs_path] = self._generation(fs_path) + 1

    async def _process_request(self, chunk_request: _ChunkRequest, priority: str):
        try:
            cache_key = (chunk_request.fs_path, chunk_request.chunk_id, chunk_request.mtime, chunk_request.gen)
            if priority == "high":
                # A real read needs the bytes now, so fetch them (from disk,
                # falling back to network) and cache in RAM.
                chunk = await self.persistence.retrieve_chunk(fs_path=chunk_request.fs_path, chunk_index=chunk_request.chunk_id, mtime=chunk_request.mtime, gen=chunk_request.gen)
                if chunk is None:
                    chunk = await self._fetch_chunk(chunk_request)
                await self._ram_cache.put(cache_key, chunk)
            else:
                # A prefetch only needs the chunk to end up on disk. Reading
                # it back (as retrieve_chunk does) would cost as much disk
                # I/O as the read it's trying to save, for a chunk that may
                # not even be read; a plain existence check is enough to
                # decide whether a download is needed.
                if not await self.persistence.chunk_exists(fs_path=chunk_request.fs_path, chunk_index=chunk_request.chunk_id, mtime=chunk_request.mtime, gen=chunk_request.gen):
                    chunk = await self._fetch_chunk(chunk_request)
                    # The bytes are already in memory (store_chunk_from_stream
                    # returns them), so keeping them costs no extra I/O or copy
                    # and saves the reader an 8 MiB read-back from disk. Chunks
                    # that were already on disk are NOT loaded: that read-back
                    # is exactly what the existence check avoids.
                    if self._prefetch_to_ram:
                        await self._ram_cache.put(cache_key, chunk)
        finally:
            await self._inflight_coalescer.notify_done(cache_key)


    def run_services(self, nursery):
        """Starts download workers"""
        self._download_scheduler.run_services(nursery)

    async def _fetch_chunk(self, chunk_request: _ChunkRequest):
        uc_path = fs_to_uc_path(chunk_request.fs_path)
        offset = self.chunk_size * chunk_request.chunk_id
        length = chunk_request.chunk_size
        stream = self.uc_client.download_chunk_stream(
            path=uc_path,
            offset=offset,
            length=length,
            ctx=chunk_request.ctx,
            if_unmodified_since=chunk_request.mtime,
        )
        chunk = await self.persistence.store_chunk_from_stream(
            fs_path=chunk_request.fs_path,
            chunk_index=chunk_request.chunk_id,
            mtime=chunk_request.mtime,
            gen=chunk_request.gen,
            stream=stream,
        )
        return chunk

    async def _request_fetch_ahead_chunks(self, fs_path: str, chunks_to_prefetch: list[Tuple[int, int]], mtime: float, gen: int, ctx: pyfuse3.RequestContext):
        for (chunk_id, chunk_size) in chunks_to_prefetch:
            # Already on disk: nothing to prefetch. Without this check, a chunk
            # that finished downloading (and so left the coalescer) was
            # re-enqueued on every read until the reader reached it: 64 reads
            # per 8 MiB chunk x up to N prefetch chunks, each costing a worker
            # wake-up and a chunk_exists() thread hop. has_chunk() is a
            # synchronous in-RAM lookup.
            if self.persistence.has_chunk(fs_path, chunk_id, mtime, gen):
                continue
            cache_key = (fs_path, chunk_id, mtime, gen)
            (wait_event, leader) = await self._inflight_coalescer.join_or_lead(cache_key)
            if not leader:
                continue  # already being fetched, we don't care about it
            chunk_request = _ChunkRequest(fs_path=fs_path, chunk_id=chunk_id, mtime=mtime, gen=gen, chunk_size=chunk_size, ctx=ctx)
            await self._download_scheduler.enqueue_request(chunk_request, high_priority=False)
        return None

    async def _get_chunk_from_cache_or_disk(self, fs_path: str, chunk_id: int, mtime: float, gen: int) -> bytes | None:
        cache_key = (fs_path, chunk_id, mtime, gen)
        # get chunk from ram
        cached = await self._ram_cache.get(cache_key)
        if cached is not None:
            return cached
        # get chunk from disk
        chunk = await self.persistence.retrieve_chunk(fs_path, chunk_id, mtime, gen)
        if chunk is not None:
            await self._ram_cache.put(cache_key, chunk)
        return chunk


    async def _read_chunk(
        self, fs_path: str, chunk_id: int, mtime: float, gen: int, chunk_size: int,
        ctx: pyfuse3.RequestContext, out_dict: dict[int, bytes|None]
    ):
        cache_key = (fs_path, chunk_id, mtime, gen)
        chunk = await self._get_chunk_from_cache_or_disk(fs_path=fs_path, chunk_id=chunk_id, mtime=mtime, gen=gen)
        if chunk is not None:
            out_dict[chunk_id] = chunk
            return

        # 2. Coalescing
        (wait_event, leader) = await self._inflight_coalescer.join_or_lead(cache_key)

        if leader:
            # get chunk from network
            chunk_request = _ChunkRequest(fs_path=fs_path, chunk_id=chunk_id, mtime=mtime, gen=gen, chunk_size=chunk_size, ctx=ctx)
            await self._download_scheduler.enqueue_request(chunk_request, high_priority=True)

        # Now we wait for the download to complete and fetch it from RAM or Disk
        await wait_event.wait()
        chunk = await self._get_chunk_from_cache_or_disk(fs_path=fs_path, chunk_id=chunk_id, mtime=mtime, gen=gen)
        # here if chunk is none, we will return None and let the caller decide what to do (probably return EIO)
        out_dict[chunk_id] = chunk
        return

    async def read(
        self, fs_path: str, offset: int, length: int, mtime: float, file_size: int,
        ctx: pyfuse3.RequestContext
    ) -> bytes:
        """
        Reads file contents
        Raises on error raise pyfuse3.FUSEError(errno.EIO)
        """
        # Resolve the cache generation once for the whole read so every chunk
        # (and the prefetch below) shares a consistent key.
        gen = self._generation(fs_path)
        # Determine path and chunks that need to be read
        if offset >= file_size:
            return bytes()
        last_chunk_size = file_size % self.chunk_size
        num_chunks_in_file = file_size // self.chunk_size
        if last_chunk_size > 0:
            num_chunks_in_file += 1
        else:
            last_chunk_size = self.chunk_size
        start_chunk = offset // self.chunk_size
        end_chunk = (offset + length - 1) // self.chunk_size
        if end_chunk >= num_chunks_in_file:
            end_chunk = num_chunks_in_file - 1
        chunks_to_read = list(range(start_chunk, end_chunk + 1))
        # Download all required chunks
        chunks: dict[int, bytes|None] = {}
        if len(chunks_to_read) == 1:
            chunk_id = chunks_to_read[0]
            if chunk_id == num_chunks_in_file - 1:
                chunk_size = last_chunk_size
            else:
                chunk_size = self.chunk_size
            await self._read_chunk(fs_path, chunk_id, mtime, gen, chunk_size, ctx, chunks)
        else:
            # dictionary: chunk_id -> bytes
            async with trio.open_nursery() as nursery:
                for chunk_id in chunks_to_read:
                    if chunk_id == num_chunks_in_file - 1:
                        chunk_size = last_chunk_size
                    else:
                        chunk_size = self.chunk_size
                    nursery.start_soon(
                        self._read_chunk, fs_path, chunk_id, mtime, gen, chunk_size, ctx, chunks
                    )
        # fetch ahead next 10 chunks once we are beyond chunk 0 (avoid prefetching too much on `head *` command)
        num_chunks_to_prefetch = self._chunks_to_prefetch if end_chunk > 0 else min(1, self._chunks_to_prefetch)
        chunks_to_prefetch = []
        for i in range(num_chunks_to_prefetch):
            chunk_to_prefetch = end_chunk + 1 + i
            if chunk_to_prefetch >= num_chunks_in_file:
                # chunk_to_prefetch beyond EOF, stop
                break
            # chunk_size: Last chunk in file may be shorter
            if chunk_to_prefetch == num_chunks_in_file -1:
                chunk_size = last_chunk_size
            else:
                chunk_size = self.chunk_size
            # Add to list to fetch:
            chunks_to_prefetch.append((chunk_to_prefetch, chunk_size))
        await self._request_fetch_ahead_chunks(fs_path, chunks_to_prefetch, mtime, gen, ctx)
        # Assemble chunks
        result = bytearray()
        for chunk_id in chunks_to_read:
            chunk_data = chunks.get(chunk_id)
            if chunk_data is None:
                raise pyfuse3.FUSEError(errno.EIO)
            # Calculate the start and end within the chunk
            chunk_start = max(0, offset - chunk_id * self.chunk_size)
            chunk_end = min(
                len(chunk_data), offset + length - chunk_id * self.chunk_size
            )
            result.extend(chunk_data[chunk_start:chunk_end])
        return bytes(result)
