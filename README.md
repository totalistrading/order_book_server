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
subscriptions per connection. L2 depths must be 1–100; zero is rejected.
Commands are limited to 4KiB and encoded output frames to 16MiB. Source/queued
positions more than three seconds from wall clock terminate the connection;
clients must reconnect and install a new snapshot. The existing two-second write
deadline and explicit source-gap/lag disconnect behavior remain enforced. Trade
and L4 event loss is never hidden by coalescing.

A shared 2MiB/1024-entry encoded L2 cache reuses identical subscribed views across
sockets. Its key binds the immutable snapshot allocation, height, time and all
subscription precision/depth parameters. Source snapshots remain dirty-coin cached
from TT-1506; wire-cache entries cannot survive a source/position change.

Run `cargo +1.89.0 test --locked --workspace` on Linux (production toolchain).
The existing directory-notification integration test can duplicate file events on
macOS; the Linux CI runs it along with source recovery and transport tests.
