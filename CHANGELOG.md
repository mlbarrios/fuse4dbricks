# 0.7.9 (2026-10-31)

- Fix prefetch re-queueing chunks that had already been downloaded. A
  finished prefetch was forgotten, so every following read requested it
  again until the reader reached it: up to 64 reads per 8 MiB chunk for
  each chunk in the prefetch window. Each one woke a download worker and
  checked the disk from a thread. Reading a large file sequentially was
  CPU-bound, and raising `--prefetch_mb` made it slower. The disk cache
  now keeps an in-memory index of the chunks it holds, and prefetch skips
  them.
- Keep prefetched chunks in the RAM cache as well as on disk, so the read
  that reaches a chunk no longer reads 8 MiB back from disk first, and
  concurrent reads of the same chunk no longer each read it back.
  Enabled by default; `--no-prefetch-to-ram` restores disk-only prefetch.
- Add `tests/sim_sequential_read.py`, a simulator that drives the real
  `DataManager` against a bandwidth-limited fake Databricks and disk
  cache. It reports where time goes in a large sequential read.

# 0.7.8 (2026-10-15)

- Fix typo in DataManager

# 0.7.7 (2026-10-01)

- Add --num-download-workers and --prefetch-mb to control prefetching
  and parallel downloads.

# 0.7.6 (2026-08-07)

- Force push. Bump for traceability.

# 0.7.5 (2026-08-06)

- Fix fatal startup/runtime errors exiting with status 0 instead of 1.
- Fix a race in Databricks config file validation where ownership was
  checked on a path and the file was read from that same path separately.
  Ownership is now checked on the open file descriptor that is read.
- Fix prefetch reading already-downloaded chunks back from disk on every
  read, instead of just checking they exist. Reading a large file
  sequentially could pull roughly an order of magnitude more bytes off
  local disk than the file actually contains.

# 0.7.4 (2026-06-20)

- Make writes durable and immediately visible on `close()`. The upload ran only
  in `release()`, which the kernel does not wait for and whose errors it
  discards, so `close()` returned before the data was uploaded — a file written
  through the mount was invisible (and write errors were silent) for the
  duration of the asynchronous upload. The upload now runs in `flush()`, which
  handles the `close()` syscall: the kernel blocks on it, the upload completes
  before `close()` returns, and a failed upload surfaces as a `close()` error.
  `release()` keeps the upload as a fallback for the rare close-without-flush.
- Fix a stale read after overwriting a file within the same second. Cached
  chunks are keyed by the server `mtime`, but the Files API `Last-Modified`
  header has only 1-second resolution (verified against the live API: no
  sub-second timestamp and no `ETag`), so two writes in the same second shared a
  key and a read could return the previous content. The chunk cache key now
  carries a per-path generation counter (RAM key and on-disk chunk filename)
  that `DataManager.invalidate_path()` bumps O(1) on every local write (wired
  into write/flush, rename, truncate and unlink); old chunks become unreachable
  and age out via the existing LRUs. Exposed by the durable-`close()` fix above,
  which made same-second overwrites reachable.
- Fix cache invalidation not reaching the kernel. Mutating ops route through
  `invalidate()`, which evicted the in-process attribute/dir/negative caches but
  never told the FUSE kernel module, so the kernel kept serving stale `stat`
  results from its own attribute and dentry caches for up to its
  `attr_timeout` / `entry_timeout` (the pyfuse3 default, 300s) — a deleted file
  kept stat-ing OK and a recreated one stayed hidden. `invalidate()` now also
  resolves the path through the inode manager and calls
  `pyfuse3.invalidate_entry_async()` (drop the parent dentry; the async variant
  avoids deadlocking against the unlink/rename/create request in flight) and
  `pyfuse3.invalidate_inode()` (drop the inode's cached attributes and data).

# 0.7.3 (2026-06-19)

- Fix `mv` of a freshly written file failing with an I/O error. `rename()`
  copies the source by downloading it with an `If-Unmodified-Since` guard built
  from the inode's `st_mtime`. A file just created/written through the mount
  carries a locally-stamped mtime (the `create()`-time wall clock), which is
  earlier than the server's `Last-Modified` (assigned when the upload
  completed), so the server reported `412 Precondition Failed` and the copy read
  failed with `EIO`. `rename()` now refreshes the source's metadata from the
  server before copying, so the guard matches the server's view. (Ordinary reads
  were unaffected: they refresh via `getattr` before reading; only `rename`
  bypassed that.)
- Stop logging a missing Databricks config file at `ERROR`. A uid without a
  Databricks setup (e.g. `root` or a system daemon probing the mount) produced
  three error lines per request, flooding the journal. A missing default
  `~/.databrickscfg` and the resulting "no token" outcome are now logged at
  `debug`; genuinely unexpected stat failures (e.g. permission denied) are
  logged at `warning`.

# 0.7.2 (2026-06-19)

- Fix a just-created file or directory being reported as nonexistent for up to
  the negative-cache TTL (~5s) when its path had been stat'd before it existed.
  Probing a not-yet-existing path records a negative (not-found) cache entry,
  and `invalidate()` — called on every mutating op (create/write, `mkdir`,
  `unlink`/`rmdir`, `rename`, truncate) — cleared the attribute and directory
  caches but left that negative entry in place, so a subsequent `stat` kept
  returning `ENOENT` until the entry expired. `invalidate()` now also evicts the
  path's negative-cache entries (for every principal).

# 0.7.1 (2026-06-17)

- Unmount cleanly on `SIGTERM`/`SIGHUP` (e.g. `systemctl stop`). trio only turns
  `SIGINT` (Ctrl-C) into a clean shutdown; other termination signals killed the
  process before `pyfuse3.close()` ran, leaving the kernel with a stale mount
  that showed up as `d?????????` in `ls`/`df`. All termination signals now run
  the same orderly unwind that unmounts the filesystem.

# 0.7.0 (2026-06-17)

- Fix files written from a Windows application through WSL being uploaded at
  the correct size but filled with zeros. A path-based `setattr(size)` (which
  WSL's 9P layer emits instead of an `ftruncate` carrying the handle) now
  resizes the open writer's buffer instead of reading the not-yet-uploaded
  remote object. A short read while preserving bytes in a true path-based
  truncate now fails with `EIO` instead of silently zero-filling.
- Add `--single-principal`: use one Databricks identity for every request
  regardless of the requesting uid, resolving the token from the fuse4dbricks
  process's own credentials (its environment and the launching user's
  `~/.databrickscfg`) or from a token written to `.auth`. Intended for
  single-user machines; combine with `--allow-other` so a request carrying an
  undocumented uid (e.g. the Windows file explorer over WSL) is served by the
  one token without fuse4dbricks needing root to read the requesting process.
- Add `--securable-allowlist` and `--securable-denylist` to restrict which
  Unity Catalog securables are listable and accessible. Each takes a
  comma-separated list of dotted securables (`catalog`, `catalog.schema` or
  `catalog.schema.volume`). A denylist hides the listed securables and
  everything beneath them; an allowlist exposes only the listed securables
  (their parent catalog/schema stay navigable). Deny wins when both are given.
  This is a mount-wide visibility filter, not a per-user boundary, and does not
  replace Unity Catalog's own permission checks.
- Implement `rename` and `setattr`. Files can be moved/renamed (via
  copy-then-delete, since the Files API has no server-side move) and truncated;
  directory rename returns `EXDEV` so `mv` falls back to a recursive copy.
  `chmod`/`chown`/`utimes` are accepted as no-ops (Unity Catalog has no POSIX
  metadata) so tools such as `tar` and `cp -p` don't fail.
- Implement `statfs` so `df` works and tools that pre-check free space before
  writing succeed. A large synthetic capacity is reported (Unity Catalog
  exposes no quota), with zero availability on a `--read-only` mount.
- Fix silent data loss on writes: opening a file `O_WRONLY` without `O_TRUNC`
  now pre-loads the existing content so untouched bytes are preserved, and
  small buffered writes are flushed before upload (they could previously be
  uploaded as zero bytes).
- Security: validate that the default `~/.databrickscfg` is a regular file
  owned by the requesting uid, the same way the env-supplied config path is
  already checked. Prevents a symlink planted by one user (under `--allow-other`
  + root) from making root read and cache another user's token.
- Fix `create` not incrementing the kernel lookup count: it returns an entry
  the kernel will later `forget`, so the new inode could be freed while still
  referenced.
- Map a 412 (the file changed under an `If-Unmodified-Since` read) to `ESTALE`
  instead of `EIO`, so a mid-read modification is reported accurately.
- Honor the HTTP-date form of the `Retry-After` header (previously only the
  integer-seconds form was used).
- Refresh the access token on a 401 during an SDK file upload; the upload path
  bypassed the request-layer 401 retry and so failed permanently on an expired
  token.
- Don't let an inode-side `st_size` mutation (from a write or truncate) corrupt
  the shared metadata cache: `add_entry` now stores a copy of the attributes.
- A coalescing `lookup_child` follower that wakes to a cache miss now surfaces a
  retryable `EAGAIN` instead of running its own lookup and notifying a coalescer
  key it does not own.
- Performance: stop running a blocking `os.makedirs` on every chunk read; cache
  shard directories are created only on the write path, off the event loop.
- Security: A user could use DATABRICKS_CONFIG_PROFILE to make fuse4dbricks read any user profile if it ran as root.
- Retry transient API failures (429 rate limits, 5xx server errors and
  connection errors) with exponential backoff and jitter, honoring the
  `Retry-After` header on 429 responses.
- Robustness: a failed chunk download no longer escapes the download worker.
  Previously the exception propagated to the trio nursery and unmounted the
  whole filesystem for every user; now the worker logs and survives, and the
  affected read fails in isolation with `EIO`.
- Fix `getattr` swallowing `ENOENT` (deleted files) and other domain errors
  into `EIO`; already-mapped errnos now propagate and `getattr` updates the
  inode attributes in place so replies reflect the current size/mtime.
- Fix writes to read-only Unity Catalog paths returning `EIO` instead of
  `EACCES` (the catch-all `except` was masking the intended errno). `read`
  likewise preserves already-mapped errnos.
- Fix `lookup` not incrementing the kernel lookup count, which could let
  `forget` free a still-referenced inode.
- Refactor: the auth virtual entries overlaid at the root (`.auth`,
  `README.txt`) are now defined by `AuthManager.root_overlay()` instead of
  being hardcoded in `readdir`, removing the names/offset coupling. Dead,
  type-confused root branch in `AuthManager.list_directory` removed.
- Security: don't turn transient authorization failures into a permanent
  `EACCES`. A rate limit or server outage during a permission check (and a
  coalescing follower that woke to no cached decision) previously looked like
  "permission denied"; transient failures now surface a retryable `EAGAIN`,
  reserving `EACCES` for genuine denials.
- Re-evaluate cached identity when a user's token changes: writing a new token
  refreshes the cached principal and drops that user's cached permission grants
  only if the principal actually changed.
- Cache catalog/schema permission grants with the longer catalog TTL (volume
  grants keep the shorter TTL for faster revocation).
- Key the permission cache by principal (not uid) and drop the cached principal
  whenever a token is invalidated (auth-file write or 401), so a uid switching
  tokens to a different principal can never read the previous principal's
  cached grants.
- Add a short-lived, per-principal negative (not-found) cache so repeated
  lookups of missing paths are answered without re-hitting Unity Catalog, and a
  coalescing follower can tell "genuinely absent" (ENOENT) from "the lookup
  failed" (retryable). TTL via `--metadata-cache-ttl-negative-sec` (5s default).
- Surface a retryable `EAGAIN` (instead of a permanent `EACCES`) when identity
  resolution fails transiently and a coalescing follower wakes to no cached
  principal.
- Clear the listing principal's stale negative entries when a directory is
  listed, so a freshly-created file shown by `ls` is no longer reported as
  missing by a subsequent `stat` until the negative TTL expires.
- Fix a `lookup` race where a `forget` arriving during the `getattr` await
  could free a still-referenced inode (spurious `ENOENT`); the inode is now
  pinned before the await.
- Robustness: read `/proc/<pid>/environ` as bytes so a single non-UTF-8
  variable no longer discards the requesting process's whole environment.
- CLI: replace the inert `--unified-auth` flag (it defaulted to on and could
  never be turned off) with `--no-unified-auth`.
- Raise a clear error for an unknown auth inode instead of an
  `UnboundLocalError`; drop a stray `print(traceback)` in the directory-listing
  error path in favor of `logger.exception`.
- Document known limitations: the attribute cache is global (names, sizes and
  modification times may leak between users, but never file contents), users
  map to tokens 1-to-1, and reported file owner/group (`st_uid`/`st_gid`) are
  meaningless.
- Improve test coverage

# 0.6.1 (2026-05-07)

- Fix type issue
- Depend on pyfuse3.4.2 to make sure 3.4.1 is not used (it's yanked)

# 0.6.0 (2026-05-07)

- Allow setting long access tokens (when token splits in more than one
  write syscall).
- Add Databricks Unified Auth support (Needs root permissions if running multiuser).


# 0.5.3 (2026-02-24)

- Improved documentation
- Fixed tests

# 0.5.2 (2026-02-24)

- Improved error handling.

# 0.5.1 (2026-02-23)

- Extended README.
- Better handling closing of running services.

# 0.5.0 (2026-02-20)

- Initial release
