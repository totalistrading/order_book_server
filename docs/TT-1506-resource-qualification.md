# TT-1506 book resource qualification

## Measured hotspot, 2026-09-11

A read-only 30-second Mainnet CPU-clock profile (49 Hz, no lost samples) of
book PID 999042, binary SHA256
`d738ae80fe86dfcbd58f6d8e2841a984a952f0348069b8dc585e2d374a5bd57b`, found:

| Self CPU samples | Symbol |
| --- | --- |
| 30.38% | `map_to_l2_levels` |
| 12.75% | `build_l2_level` |
| 4.14% | `__log10_finite` |
| 2.94% | `floor` |

This identifies L2 construction as a current hotspot. It does not prove it was
the sole cause of the earlier outage or establish a memory leak.

Previously every completed update rebuilt every coin's precision variants, and
an immediate subscription repeated that global computation. Published L2 views
now share immutable per-coin caches. Applied diffs invalidate their coin; a new
authoritative state starts with all coins dirty. Every completed block still
advances global time/height, including quiet blocks and empty HIP-4 views.
Full L4 authority is unchanged. Truncation now clones only requested levels.

The deterministic release benchmark (200 coins, 1,000 price levels each, 100
turns, one dirty coin) took 599.8 ms rebuilding all views versus 8.52 ms updating
the dirty coin on the development host. This is a synthetic CPU comparison,
not an end-to-end production speedup. Run it with:

```sh
cargo test --release benchmark_incremental_l2_views -- --ignored --nocapture
```

Tests verify quiet-block provenance, old-view immutability, unchanged-coin
sharing, real removal invalidation, equality against a full rebuild for every
precision variant, and the number of cloned levels during truncation.

## Remaining qualification

TT-1506 remains in progress. The optimization does not yet bound filesystem
notifications, record processing, unmatched pairs, or snapshot reconciliation
caches. Fault injection, long-snapshot recovery, sustained heap measurements,
and before/after live CPU/ingestion latency remain required.

The latest 64 MiB Mainnet samples contained maximum complete records of
7,717,816 bytes (order statuses), 1,592,279 bytes (raw diffs), and 387,125 bytes
(fills). These are observations, not protocol maxima: small arbitrary record
ceilings would reject valid traffic. Per-turn work limits must allow partial
records and distinguish byte backlog from source-height gaps.
