# Local WebSocket Server

## Disclaimer

This was a standalone project, not written by the Hyperliquid Labs core team. It is made available "as is", without warranty of any kind, express or implied, including but not limited to warranties of merchantability, fitness for a particular purpose, or noninfringement. Use at your own risk. It is intended for educational or illustrative purposes only and may be incomplete, insecure, or incompatible with future systems. No commitment is made to maintain, update, or fix any issues in this repository.

## Functionality

This server provides the `l2book` and `trades` endpoints from [Hyperliquid’s official API](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions), with roughly the same API.

- The `l2book` subscription now includes an optional field:
  `n_levels`, which can be up to `100` and defaults to `20`.
- This server also introduces a new endpoint: `l4book`.

The `l4book` subscription first sends a snapshot of the entire book and then forwards order diffs by block. The subscription format is:

```json
{
  "method": "subscribe",
  "subscription": {
    "type": "l4Book",
    "coin": "<coin_symbol>"
  }
}
```

## Setup

1. Run a non-validating node (from [`hyperliquid-dex/node`](https://github.com/hyperliquid-dex/node)). Requires batching by block. Requires recording fills, order statuses, and raw book diffs. Requires handling info requests. 

2. Then run this local server:

```bash
cargo run --release --bin websocket_server -- --address 0.0.0.0 --port 8000
```

If this local server does not detect the node writing down any new events, it will automatically exit after some amount of time (currently set to 5 seconds).
In addition, the local server periodically fetches order book snapshots from the node, and compares to its own internal state. If a difference is detected, it will exit.

If you want logging, prepend the command with `RUST_LOG=info`.

The WebSocket server comes with compression built-in. The compression ratio can be tuned using the `--websocket-compression-level` flag.

## Caveats

- This server does **not** show untriggered trigger orders.
- It currently **does not** support spot order books.
- The current implementation batches node outputs by block, making the order book a few milliseconds slower than a streaming implementation.

## Bounded subscriptions (TT-1508)

The shared native server admits at most 256 WebSocket connections and 2048
subscriptions per connection. L2 depths must be 1–100 except explicit 20 (use null for that default); zero is rejected.
Coin identifiers are limited to 64 bytes. Commands are limited to 4KiB and encoded output frames to 16MiB. Attempting to send source
positions more than three seconds from wall clock terminates the connection;
clients must reconnect and install a new snapshot. This send-time check does not
claim that an idle source is periodically probed; the gateway owns that liveness check. The existing two-second write
deadline and explicit source-gap/lag disconnect behavior remain enforced. Trade
and L4 event loss is never hidden by coalescing.

A shared 2MiB/1024-entry encoded L2 cache reuses identical subscribed views across
sockets. Its key binds the immutable snapshot allocation, height, time and all
subscription precision/depth parameters. Source snapshots remain dirty-coin cached
from TT-1506; wire-cache entries cannot survive a source/position change.

Run `cargo +1.89.0 test --locked --workspace` on Linux (production toolchain).
The existing directory-notification integration test can duplicate file events on
macOS; the Linux CI runs it along with source recovery and transport tests.

## Snapshot audit cadence (TT-1708)

`BOOK_SNAPSHOT_INTERVAL_SECONDS` controls the delay after a completed routine
snapshot audit (10–300 seconds, default 10). `/resources` exposes the configured
interval, snapshot request count, latest completed request start/completion
timestamps and request-phase duration. Duration includes output-file/client setup
and the HTTP request (including node snapshot generation), and excludes local
snapshot parsing/reconciliation. The completed timing fields update atomically;
`snapshot_request_inflight_started_at_ms` separately identifies an active request
and is zero when none is running.

Startup still requests its first snapshot after five seconds. A fenced listener
uses the one-second maintenance loop and a ten-second retry delay after its last
completed request, independently of the healthy audit interval. There is still
only one snapshot owner, including while a timed-out or invalidated request is
finishing. Queue bounds, consistency checks and source freshness are unchanged.

The proposed production trial is 60 seconds, compared with the current 10-second
setting. A longer healthy interval reduces repeated full L4 exports and local
cloning/reconciliation, but also increases the interval between independent
full-state consistency checks. Continuous source ordering/gap checks remain
active. This is a targeted contention mitigation to measure, not a claim that it
eliminates Hyperliquid's own periodic ABCI checkpoint pauses. Do not raise the
three-second freshness limit to make the comparison pass.
