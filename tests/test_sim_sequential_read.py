"""Smoke tests for the sequential-read simulator (tests/sim_sequential_read.py)."""

import importlib.util
import pathlib
import sys

import pytest
import trio
import trio.testing

_SPEC = importlib.util.spec_from_file_location(
    "sim_sequential_read", pathlib.Path(__file__).with_name("sim_sequential_read.py")
)
sim = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = sim  # dataclasses need the module registered
_SPEC.loader.exec_module(sim)


def _link_rate(n_flows: int) -> float:
    """Aggregate MiB/s achieved by n parallel 10 MiB flows on a 20/200 MiB/s link."""

    async def main():
        link = sim.FluidLink("net", 200 * sim.MiB, 20 * sim.MiB)
        async with trio.open_nursery() as n:
            n.start_soon(link.run)
            t0 = trio.current_time()
            async with trio.open_nursery() as flows:
                for _ in range(n_flows):
                    flows.start_soon(link.transfer, 10 * sim.MiB)
            elapsed = trio.current_time() - t0
            n.cancel_scope.cancel()
        return n_flows * 10 / elapsed

    return trio.run(main, clock=trio.testing.MockClock(autojump_threshold=0))


@pytest.mark.parametrize("n_flows, expected", [(1, 20), (5, 100), (10, 200), (20, 200)])
def test_fluid_link_respects_per_flow_and_global_caps(n_flows, expected):
    assert _link_rate(n_flows) == pytest.approx(expected, rel=0.02)


def test_content_roundtrip():
    data = sim.make_content(3 * sim.PAGE, 5 * sim.PAGE)
    assert sim.verify_content(3 * sim.PAGE, data)
    assert not sim.verify_content(4 * sim.PAGE, data)


def test_small_simulation_is_consistent():
    cfg = sim.SimConfig(file_mib=64, sample_ms=50)
    s = sim.run(cfg)["summary"]
    assert s["error_count"] == 0
    assert s["reads"] == 64 * 1024 // cfg.request_kib
    assert s["duplicate_downloads"] == 0
    assert s["overfetch_ratio"] == pytest.approx(1.0)
    # Can never beat the link, and the first read pays at least TTFB + one chunk.
    assert s["throughput_mib_s"] <= cfg.global_mib_s * 1.001
    assert s["first_read_latency_s"] >= cfg.ttfb_ms / 1000 + cfg.chunk_mib / cfg.per_stream_mib_s - 0.01


@pytest.mark.parametrize(
    "kw, passes, expected",
    [
        # prefetch-to-RAM: the disk cache is only written, so a 100 MiB/s disk caps at 100.
        (dict(disk_mib_s=100), 1, 100.0),
        # disk-only prefetch: every byte is written and read back -> half the disk speed.
        (dict(disk_mib_s=100, prefetch_to_ram=False), 2, 50.0),
        # RAM cache too small for the prefetch window: chunks are evicted and read back.
        (dict(disk_mib_s=100, ram_cache_mb=40), 2, 50.0),
    ],
)
def test_analytic_estimate_disk_passes(kw, passes, expected):
    est = sim.analytic_estimate(sim.SimConfig(**kw))
    assert est["disk_passes"] == passes
    assert est["throughput_mib_s"] == pytest.approx(expected)
