"""
Tests for the pure helpers in fuse4dbricks.main (argument parsing and the
cache-dir / system-account heuristics). The async mount path is exercised by
test_e2e_mount.
"""

import sys

import pytest

from fuse4dbricks import main as main_mod


def _run_parse(argv, monkeypatch):
    monkeypatch.setattr(sys, "argv", ["fuse4dbricks"] + argv)
    return main_mod.parse_args()


# ---------------------------------------------------------------------------
# parse_args
# ---------------------------------------------------------------------------


def test_parse_args_defaults(monkeypatch):
    args = _run_parse(["/mnt/point"], monkeypatch)
    assert args.mountpoint == "/mnt/point"
    assert args.unified_auth is True
    assert args.allow_other is False
    assert args.ram_cache_mb == 512
    assert args.metadata_cache_ttl_sec == 30
    assert args.metadata_cache_ttl_catalog_sec == 600


def test_parse_args_prefetch_to_ram(monkeypatch):
    assert _run_parse(["/mnt/point"], monkeypatch).prefetch_to_ram is True
    assert _run_parse(["/mnt/point", "--no-prefetch-to-ram"], monkeypatch).prefetch_to_ram is False


def test_parse_args_no_unified_auth(monkeypatch):
    args = _run_parse(["/mnt/point", "--no-unified-auth"], monkeypatch)
    assert args.unified_auth is False


def test_parse_args_allow_other_and_workspace(monkeypatch):
    args = _run_parse(
        ["--workspace", "https://adb-x.azuredatabricks.net", "--allow-other", "/mnt/point"],
        monkeypatch,
    )
    assert args.allow_other is True
    assert args.workspace == "https://adb-x.azuredatabricks.net"


# ---------------------------------------------------------------------------
# _is_system_account
# ---------------------------------------------------------------------------


def test_is_system_account_true_for_low_uid(monkeypatch):
    monkeypatch.setattr(main_mod.os, "geteuid", lambda: 0)
    assert main_mod._is_system_account() is True


def test_is_system_account_false_for_user_uid(monkeypatch):
    monkeypatch.setattr(main_mod.os, "geteuid", lambda: 1000)
    assert main_mod._is_system_account() is False


# ---------------------------------------------------------------------------
# _get_default_cache_dir
# ---------------------------------------------------------------------------


def test_default_cache_dir_system_account(monkeypatch):
    monkeypatch.setattr(main_mod, "_is_system_account", lambda: True)
    assert main_mod._get_default_cache_dir() == "/var/cache/fuse4dbricks"


def test_default_cache_dir_xdg(monkeypatch):
    monkeypatch.setattr(main_mod, "_is_system_account", lambda: False)
    monkeypatch.setenv("XDG_CACHE_HOME", "/tmp/xdgcache")
    assert main_mod._get_default_cache_dir() == "/tmp/xdgcache/fuse4dbricks"


def test_default_cache_dir_home_fallback(monkeypatch):
    monkeypatch.setattr(main_mod, "_is_system_account", lambda: False)
    monkeypatch.delenv("XDG_CACHE_HOME", raising=False)
    monkeypatch.setenv("HOME", "/home/someone")
    assert main_mod._get_default_cache_dir() == "/home/someone/.cache/fuse4dbricks"
