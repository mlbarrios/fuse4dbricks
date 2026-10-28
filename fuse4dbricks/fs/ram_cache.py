import trio
from collections import OrderedDict
from typing import TypeVar, Generic

from fuse4dbricks.perf import profiling

CacheKey = TypeVar("CacheKey")

class RamCache(Generic[CacheKey]):
    """
    Simple in-memory LRU cache.

    Concurrency:
      - guarded by a trio.Lock
      - get() marks the entry as recently used
      - put() inserts/updates and evicts LRU items to stay under max_entries
    """

    def __init__(self, *, max_entries: int) -> None:
        if max_entries < 0:
            raise ValueError("max_entries must be >= 0")
        self._max_entries = max_entries
        self._lock = trio.Lock()
        self._lru: OrderedDict[CacheKey, bytes] = OrderedDict()

    async def get(self, key: CacheKey) -> bytes | None:
        if self._max_entries == 0:
            profiling.cache_event("ram", hit=False)
            return None
        async with self._lock:
            value = self._lru.get(key)
            if value is None:
                profiling.cache_event("ram", hit=False)
                return None
            # mark as recently used
            self._lru.move_to_end(key, last=True)
            profiling.cache_event("ram", hit=True)
            return value

    async def put(self, key: CacheKey, value: bytes) -> None:
        if self._max_entries == 0:
            return
        async with self._lock:
            self._lru[key] = value
            self._lru.move_to_end(key, last=True)
            # evict LRU until within limit (handles max_entries shrinking or bursts)
            while len(self._lru) > self._max_entries:
                self._lru.popitem(last=False)

    async def delete(self, key: CacheKey) -> None:
        async with self._lock:
            self._lru.pop(key, None)

    async def clear(self) -> None:
        async with self._lock:
            self._lru.clear()

    async def stats(self) -> tuple[int, int]:
        """Returns (items, max_entries)."""
        async with self._lock:
            return (len(self._lru), self._max_entries)
