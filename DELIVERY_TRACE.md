# Connection delivery evidence (TT-1741)

Each accepted WebSocket handshake includes `x-source-connection-id`, generated
locally from time, process ID and a process-wide sequence. It contains no peer
address and is diagnostic only; it is not an authentication or continuity token.

On connection task exit (including cancellation), `book_delivery_trace` logs
at most the last 128 broadcast snapshot batches. Each records source height/time,
local batch start, local completion if reached, and successfully flushed L2 frame
count. A partial batch has null completion. Zero frames is possible with no L2
subscriptions or duplicate positions. Immediate subscription replies, trades and
L4 updates are not traced. No payloads or subscription identifiers are logged.

A completed flush means the local sink accepted/flushed the bytes. It does not
mean TCP acknowledgement or remote application receipt. Compare the handshake ID
with the gateway's connection diagnostics and last parsed book position, and
bound host clocks before interpreting cross-host time differences. The gateway's
parsed position is not a validated/applied watermark. A pending unparsed frame
may be newer. The ring is count-bounded, not guaranteed to cover a time interval.

This locates source batch progress for an exact application connection. It does
not map that connection to nginx/TLS TCP socket inodes, and it does not prove a
network cause. It cannot retrospectively explain failures on older binaries.

A source batch beginning late points toward ingestion/fan-out delay; a batch
beginning promptly but completing late points toward serialization/local send.
Prompt completion combined with late gateway parsing leaves proxy/transport or
client buffering to resolve with matched transport observations. Source export
must be checked separately: September 24's 15:16 incident already had raw export
age above the three-second freshness fence, unlike the 13:11 and 13:38 incidents.

Trade and L4 dispatch is scoped to active subscription types: a connection with
no subscriber to that type does not convert or freshness-check that broadcast.
Subscribed streams retain their three-second checks. Trade/L4 source-age errors
include stream type, height and source time, without payloads. A completed final
L2 batch does not identify which other message type later failed; use the typed
error rather than attributing every disconnect to L2 delivery.

The trace's `source_peer_port` is the numeric TCP peer port provided by Axum's
accepted connection metadata, never a request header. It identifies the
proxy-to-book hop (book listener port 8000); the gateway's separate
`edge_peer_port` identifies the gateway-to-proxy hop (edge listener port 8443).
Match each only within the trace's observed connection lifetime and reject
ambiguous port reuse. No peer address is retained. Existing source socket
captures already collect both hops, so a uniquely matched trace can distinguish
book-to-proxy queuing from proxy-to-gateway queuing. ACKs still acknowledge TCP
bytes, not application consumption.
