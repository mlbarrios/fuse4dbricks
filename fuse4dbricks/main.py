#!/usr/bin/env python3
import argparse
import logging
import os
import signal
import traceback
import sys

import trio
import pyfuse3

from fuse4dbricks.api.uc_client import UnityCatalogClient
from fuse4dbricks.fs.data_manager import DataManager
from fuse4dbricks.fs.inode_manager import InodeManager
from fuse4dbricks.fs.metadata_manager import MetadataManager
from fuse4dbricks.fs.auth_manager import AuthManager
from fuse4dbricks.fs.operations import UnityCatalogFS
from fuse4dbricks.fs.securable_filter import SecurableFilter
from fuse4dbricks.auth.provider import AuthProvider
from fuse4dbricks.storage.persistence import DiskPersistence, clear_cache

logger = logging.getLogger(__name__)

def _is_system_account() -> bool:
    try:
        uid = os.geteuid()
        return uid < 1000
    except Exception as exc:
        logger.error("_is_system_account failed to determine if uid is a system account, assuming it is a user.\nException: %s\n%s", exc, traceback.format_exc())
        return False

def _get_default_cache_dir():
    # Assume POSIX system. if uid is root return /var/cache/fuse4dbricks else return something xdg compliant else $HOME/.cache/fuse4dbricks
    if _is_system_account():
        return "/var/cache/fuse4dbricks"
    xdg_cache_home = os.getenv("XDG_CACHE_HOME")
    if xdg_cache_home:
        return os.path.join(xdg_cache_home, "fuse4dbricks")
    return os.path.expanduser("~/.cache/fuse4dbricks")

def parse_args():
    parser = argparse.ArgumentParser(
        description="Mount Databricks Unity Catalog volumes."
    )
    parser.add_argument(
        "--workspace", default="", help="https://adb-xxxx.azuredatabricks.net"
    )
    parser.add_argument("mountpoint", help="Local directory to mount")
    parser.add_argument(
        "--disk-cache-dir", default=_get_default_cache_dir(), help="Local disk cache location"
    )
    
    parser.add_argument("--allow-other", action="store_true",
        help="Use -o allow_other only if /etc/fuse.conf has user_allow_other"
    )
    parser.add_argument("--no-unified-auth", action="store_false", dest="unified_auth",
        help="Disable Databricks Unified Authentication; users must provide a token manually"
    )
    parser.add_argument("--single-principal", action="store_true",
        help=(
            "Use one Databricks identity for every request regardless of the "
            "requesting user. The token is resolved from this process's own "
            "credentials (its environment plus the launching user's "
            "~/.databrickscfg) or from a token written to .auth. Intended for "
            "single-user machines; combine with --allow-other so a request "
            "carrying a different/undocumented uid (e.g. Windows Explorer over "
            "WSL) is served by the one token without needing root to read its "
            "environment."
        ),
    )
    parser.add_argument(
        "--clear-cache", action="store_true", help="Clear disk cache on startup"
    )
    parser.add_argument(
        "--disk-cache-gb",
        type=float,
        default=10,
        help="Maximum disk cache size in GB",
    )
    parser.add_argument(
        "--disk-cache-max-days",
        type=int,
        default=30,
        help="Maximum age of disk cache entries in days",
    )
    parser.add_argument(
        "--ram-cache-mb",
        type=int,
        default=512,
        help="In-memory data cache size in MB",
    )
    parser.add_argument(
        "--metadata-cache-ttl-sec",
        type=int,
        default=30,
        help="Default TTL for metadata cache entries in seconds",
    )
    parser.add_argument(
        "--prefetch_mb",
        type=int,
        default=80,
        help="Amount of data to prefetch in MB for each file",
    )
    parser.add_argument(
        "--prefetch-to-ram",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Keep prefetched chunks in the RAM cache as well as on disk, so "
        "sequential reads do not read them back from disk "
        "(--no-prefetch-to-ram: disk only)",
    )
    parser.add_argument(
        "--num-download-workers",
        type=int,
        default=10,
        help="Number of concurrent download workers",
    )
    parser.add_argument(
        "--metadata-cache-ttl-catalog-sec",
        type=int,
        default=600,
        help="TTL for catalog metadata cache entries in seconds",
    )
    parser.add_argument(
        "--metadata-cache-ttl-negative-sec",
        type=int,
        default=5,
        help="TTL for negative (not-found) metadata cache entries in seconds",
    )
    parser.add_argument(
        "--metadata-cache-max-entries",
        type=int,
        default=20000,
        help="Maximum number of metadata cache entries",
    )
    parser.add_argument("--debug", action="store_true", help="Enable verbose logging")
    parser.add_argument(
        "--read-only", action="store_true",
        help="Disallow all writes to Unity Catalog volumes (token writes to .auth still work)",
    )
    parser.add_argument(
        "--securable-allowlist", default="",
        help=(
            "Comma-separated securables (e.g. cat.schema.vol1,cat.schema2) that "
            "are the ONLY ones listable/accessible; ancestors stay navigable. "
            "Empty means all are allowed."
        ),
    )
    parser.add_argument(
        "--securable-denylist", default="",
        help=(
            "Comma-separated securables that are hidden and inaccessible. "
            "Deny wins over the allowlist."
        ),
    )
    return parser.parse_args()


def setup_logging(debug_mode):
    level = logging.DEBUG if debug_mode else logging.INFO
    logging.basicConfig(
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s", level=level
    )
    if not debug_mode:
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("pyfuse3").setLevel(logging.INFO)


async def _shutdown_on_signal(cancel_scope: trio.CancelScope):
    """Translate termination signals into a clean trio shutdown.

    Without this, trio only converts SIGINT (Ctrl-C) into a KeyboardInterrupt;
    SIGTERM (e.g. ``systemctl stop``) and SIGHUP kill the process outright, so
    ``start_fuse``'s ``finally`` never runs and ``pyfuse3.close()`` never gets
    a chance to unmount. The kernel is then left with a stale mount that shows
    up as ``d?????????`` in ``ls``/``df``. Cancelling the nursery here makes
    every signal follow the same orderly unwind that unmounts the filesystem.
    """
    with trio.open_signal_receiver(signal.SIGTERM, signal.SIGINT, signal.SIGHUP) as signals:
        async for signum in signals:
            logging.info(
                "Received %s, unmounting and shutting down...",
                signal.Signals(signum).name,
            )
            cancel_scope.cancel()
            return


async def start_fuse(ops, mountpoint, allow_other, debug_mode, stopped_evt: trio.Event | None = None):
    mount_options = ["fsname=fuse4dbricks", "noatime"]
    if allow_other:
        mount_options.append("allow_other")
    if debug_mode:
        mount_options.append("debug")

    try:
        pyfuse3.init(ops, mountpoint, mount_options)
    except Exception as e:
        logging.error(f"Failed to initialize FUSE: {e}")
        raise  # allow nursery to handle cancellation/propagation

    logging.info(f"Mounted at {mountpoint}")
    try:
        await pyfuse3.main()
    except KeyboardInterrupt:
        pass
    finally:
        try:
            pyfuse3.close(unmount=True)
        finally:
            if stopped_evt is not None:
                stopped_evt.set()


async def async_main():
    args = parse_args()
    setup_logging(args.debug)

    mountpoint = os.path.abspath(args.mountpoint)
    if not os.path.isdir(mountpoint):
        logging.error(f"Mountpoint does not exist: {mountpoint}")
        sys.exit(1)

    root_cache_dir = os.path.abspath(args.disk_cache_dir)
    os.makedirs(root_cache_dir, exist_ok=True, mode=0o700)
    os.chmod(root_cache_dir, mode=0o700)
    # auth cache and data cache
    auth_cache_dir = os.path.join(root_cache_dir, "auth")
    data_cache_dir = os.path.join(root_cache_dir, "data")
    if args.clear_cache:
        clear_cache(data_cache_dir)
    os.makedirs(auth_cache_dir, exist_ok=True, mode=0o700)
    os.chmod(auth_cache_dir, mode=0o700)
    os.makedirs(data_cache_dir, exist_ok=True, mode=0o700)
    os.chmod(data_cache_dir, mode=0o700)
    writes_dir = os.path.join(root_cache_dir, "writes")
    os.makedirs(writes_dir, exist_ok=True, mode=0o700)
    os.chmod(writes_dir, mode=0o700)
    for _fname in os.listdir(writes_dir):
        try:
            os.unlink(os.path.join(writes_dir, _fname))
        except OSError:
            pass

    if args.workspace != "":
        workspace = args.workspace
    else:
        workspace = os.getenv("DATABRICKS_HOST")
        if workspace is None:
            logging.critical("Mising databricks workspace. Either pass --workspace or set DATABRICKS_HOST to 'https://adb-xxxx.azuredatabricks.net'")
            sys.exit(1)

    # Auth
    logging.info(f"Initializing Authentication... with {args.unified_auth=}")
    auth_provider: AuthProvider = AuthProvider(
        unified_auth=args.unified_auth, single_principal=args.single_principal
    )
    if args.single_principal:
        logging.info("Single-principal mode: one Databricks identity serves all requests")
    # Init Components
    uc_client = UnityCatalogClient(workspace, auth_provider)
    persistence = DiskPersistence(data_cache_dir, max_size_gb=args.disk_cache_gb, max_age_days=args.disk_cache_max_days)

    inode_manager = InodeManager()
    data_manager = DataManager(uc_client, persistence, ram_cache_mb=args.ram_cache_mb, num_workers=args.num_download_workers, prefetch_mb=args.prefetch_mb, prefetch_to_ram=args.prefetch_to_ram)
    metadata_manager = MetadataManager(uc_client, ttl=args.metadata_cache_ttl_sec, max_entries=args.metadata_cache_max_entries, ttl_catalog=args.metadata_cache_ttl_catalog_sec, ttl_negative=args.metadata_cache_ttl_negative_sec, inode_manager=inode_manager)
    # On a 401, the token is invalidated and re-resolved (possibly to a
    # different principal); drop the cached principal so authz re-derives it.
    auth_provider.set_token_invalidation_callback(metadata_manager.forget_principal)
    auth_manager = AuthManager(uc_client, auth_provider, metadata_manager, workspace=workspace)

    def _split_csv(value: str) -> list[str]:
        return [s.strip() for s in value.split(",") if s.strip()]

    securable_filter = SecurableFilter(
        allowlist=_split_csv(args.securable_allowlist),
        denylist=_split_csv(args.securable_denylist),
    )
    if securable_filter.is_active:
        logging.info(
            "Securable filtering active (allowlist=%r, denylist=%r)",
            _split_csv(args.securable_allowlist),
            _split_csv(args.securable_denylist),
        )
    operations = UnityCatalogFS(
        inode_manager, metadata_manager, data_manager, auth_manager,
        uc_client=uc_client, writes_dir=writes_dir, read_only=args.read_only,
        securable_filter=securable_filter,
    )
    stopped_evt = trio.Event()
    try:
        async with trio.open_nursery() as nursery:
            persistence.run_services(nursery)
            data_manager.run_services(nursery)
            nursery.start_soon(_shutdown_on_signal, nursery.cancel_scope)
            nursery.start_soon(start_fuse, operations, mountpoint, args.allow_other, args.debug, stopped_evt)

            await stopped_evt.wait()
            nursery.cancel_scope.cancel()

    except ExceptionGroup as eg:
        # eg is the top-level ExceptionGroup
        logger.error("Top-level group message: %s", eg)
        for i, exc in enumerate(eg.exceptions, 1):
            logger.error("[%d] type=%s -> %s\n%s", i, type(exc).__name__, exc, traceback.format_exc())
        raise
    finally:
        data_manager.close()
        await uc_client.close()


def cli_entry_point():
    try:
        trio.run(async_main)
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logging.critical(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    cli_entry_point()
