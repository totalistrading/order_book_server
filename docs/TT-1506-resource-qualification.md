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

## Bounded ingestion and recovery

Filesystem notifications now coalesce into a bounded FIFO of dirty paths.
Each path keeps its own file descriptor, byte offset and partial record. A turn
reads at most 1 MiB before other dirty paths and snapshot completion can run.
Complete records are parsed only after their newline arrives, including records
larger than a read turn. Rotation retains independent cursors; replacement or
truncation produces an explicit gap. Removed, drained files release their cursors.

Defaults are configurable through positive integer environment values:

| Variable | Default | Meaning |
| --- | --- | --- |
| `BOOK_MAX_DIRTY_FILES` | 32 | Dirty paths and retained file cursors |
| `BOOK_READ_TURN_BYTES` | 1048576 | Bytes read per scheduling turn |
| `BOOK_MAX_RECORD_BYTES` | 67108864 | Maximum encoded record, including newline |
| `BOOK_MAX_QUEUE_BYTES` | 1073741824 | Encoded bytes retained per unmatched or validation queue |
| `BOOK_MAX_QUEUE_HEIGHTS` | 4096 | Queued batches and unmatched height span |
| `BOOK_MAX_QUEUE_AGE_SECONDS` | 120 | Wall-clock residence of unmatched/validation work |

These bound retained encoded input, not total process heap: the full authoritative
L4 state, decoded object overhead, published views and one reconciliation clone
also consume memory. The 64 MiB record default is over eight times the sampled
7.7 MB status record. It is not a claim about a protocol maximum. A record that
exceeds the configured ceiling explicitly fences the stream and is skipped once
in favor of a fresh snapshot, rather than rereading the same invalid interval.

A source gap closes existing consumers, clears unmatched work and invalidates
older snapshot attempts. Consumers must obtain a new authoritative snapshot.
There is still only one snapshot owner. Validation-cache overflow discards that
validation attempt while preserving healthy live state; the existing job must
finish before another starts. No error path restarts the node or book process.
A continuously oversized or unavailable source can remain fenced and requires
resource/configuration intervention; publication is never resumed with guessed
state. Fills are deduplicated and broadcasts no longer spawn one detached task
per batch.

The private book server exposes `GET /resources` for local qualification. It
reports source height, read/processing/lock-wait counters, file backlog, queue
bytes, gaps and latest snapshot clone/parse/reconciliation durations. This is a
diagnostic snapshot, not an end-to-end health guarantee.

Unit tests cover byte/height/age limits, duplicate accounting, queue release,
100,000 coalesced notifications, fair requeue order, partial UTF-8 records,
replacement/truncation, explicit malformed-record recovery, stale snapshot
fencing and validation overflow without discarding live state. The Linux
integration test holds a snapshot request beyond the normal interval, floods
bounded ingestion, verifies there is only one request owner, and then verifies
resnapshot recovery and resumed block progress. Live sustained qualification is
still required before closing TT-1506.
