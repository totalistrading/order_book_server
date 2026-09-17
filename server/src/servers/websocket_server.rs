use crate::{
    listeners::order_book::{
        InternalMessage, L2SnapshotParams, L2Snapshots, OrderBookListener, TimedSnapshots, hl_listen,
    },
    order_book::Coin,
    prelude::*,
    types::{
        L2Book, L4Book, L4BookUpdates, L4Order, Trade,
        node_data::{Batch, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
        subscription::{
            ClientMessage, DEFAULT_LEVELS, MAX_SUBSCRIPTIONS, ServerResponse, Subscription, SubscriptionManager,
            is_hip4_coin,
        },
    },
};
use axum::{Router, response::IntoResponse, routing::get};
use futures_util::{Sink, SinkExt, StreamExt};
use log::{error, info};
use std::{
    collections::{HashMap, HashSet},
    env::home_dir,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::select;
use tokio::{
    net::TcpListener,
    sync::{
        Mutex, Semaphore,
        broadcast::{Sender, channel},
    },
};
use yawc::{FrameView, OpCode, WebSocket};

pub async fn run_websocket_server(address: &str, ignore_spot: bool, compression_level: u32) -> Result<()> {
    let (internal_message_tx, _) = channel::<Arc<InternalMessage>>(100);
    let connections = Arc::new(Semaphore::new(256));
    let wire_cache = Arc::new(std::sync::Mutex::new(BookWireCache::default()));

    // Central task: listen to messages and forward them for distribution
    let home_dir = home_dir().ok_or("Could not find home directory")?;
    let listener = {
        let internal_message_tx = internal_message_tx.clone();
        OrderBookListener::new(Some(internal_message_tx), ignore_spot)
    };
    let listener = Arc::new(Mutex::new(listener));
    {
        let listener = listener.clone();
        tokio::spawn(async move {
            if let Err(err) = hl_listen(listener, home_dir).await {
                error!("Listener fatal error: {err}");
                std::process::exit(1);
            }
        });
    }

    let websocket_opts =
        yawc::Options::default().with_compression_level(yawc::CompressionLevel::new(compression_level));
    let resources = listener.clone();
    let app = Router::new()
        .route(
            "/resources",
            get(move || {
                let resources = resources.clone();
                async move { axum::Json(resources.lock().await.resource_status()) }
            }),
        )
        .route(
            "/ws",
            get({
                let internal_message_tx = internal_message_tx.clone();
                async move |ws_upgrade| {
                    ws_handler(
                        ws_upgrade,
                        internal_message_tx.clone(),
                        listener.clone(),
                        ignore_spot,
                        websocket_opts,
                        connections.clone(),
                        wire_cache.clone(),
                    )
                }
            }),
        );

    let listener = TcpListener::bind(address).await?;
    info!("WebSocket server running at ws://{address}");

    if let Err(err) = axum::serve(listener, app.into_make_service()).await {
        error!("Server fatal error: {err}");
        std::process::exit(2);
    }

    Ok(())
}

fn ws_handler(
    incoming: yawc::IncomingUpgrade,
    internal_message_tx: Sender<Arc<InternalMessage>>,
    listener: Arc<Mutex<OrderBookListener>>,
    ignore_spot: bool,
    websocket_opts: yawc::Options,
    connections: Arc<Semaphore>,
    wire_cache: Arc<std::sync::Mutex<BookWireCache>>,
) -> axum::response::Response {
    let Ok(permit) = connections.try_acquire_owned() else {
        return (axum::http::StatusCode::TOO_MANY_REQUESTS, "Connection capacity reached").into_response();
    };
    let (resp, fut) = incoming.upgrade(websocket_opts).unwrap();
    tokio::spawn(async move {
        let _permit = permit;
        let ws = match tokio::time::timeout(std::time::Duration::from_secs(5), fut).await {
            Ok(Ok(ok)) => ok,
            Ok(Err(err)) => {
                log::error!("failed to upgrade websocket connection: {err}");
                return;
            }
            Err(_) => {
                log::warn!("websocket upgrade deadline exceeded");
                return;
            }
        };

        if let Err(err) = handle_socket(ws, internal_message_tx, listener, ignore_spot, wire_cache).await {
            error!("Book stream connection terminated: {err}");
        }
    });

    resp.into_response()
}

async fn handle_socket(
    mut socket: WebSocket,
    internal_message_tx: Sender<Arc<InternalMessage>>,
    listener: Arc<Mutex<OrderBookListener>>,
    ignore_spot: bool,
    wire_cache: Arc<std::sync::Mutex<BookWireCache>>,
) -> Result<()> {
    let mut internal_message_rx = internal_message_tx.subscribe();
    let is_ready = listener.lock().await.is_ready();
    let mut manager = SubscriptionManager::default();
    let mut sent_positions = HashMap::<String, u64>::new();
    let mut universe = listener.lock().await.universe().into_iter().map(|c| c.value()).collect();
    if !is_ready {
        let msg = ServerResponse::Error("Order book not ready for streaming (waiting for snapshot)".to_string());
        send_socket_message(&mut socket, msg).await?;
        return Ok(());
    }
    loop {
        select! {
            recv_result = internal_message_rx.recv() => {
                match recv_result {
                    Ok(msg) => {
                        match msg.as_ref() {
                            InternalMessage::Gap => {
                                // A consumer must resubscribe to an authoritative snapshot.
                                send_socket_message(&mut socket, ServerResponse::Error("Source gap; resnapshot required".into())).await?;
                                return Ok(());
                            }
                            InternalMessage::Snapshot{ l2_snapshots, time, height } => {
                                universe = new_universe(l2_snapshots, ignore_spot);
                                for sub in manager.subscriptions() {
                                    send_ws_data_from_snapshot(&mut socket, sub, l2_snapshots, *time, *height, &mut sent_positions, &wire_cache).await?;
                                }
                            },
                            InternalMessage::Fills{ batch } => {
                                require_recent_source(batch.block_time())?;
                                let mut trades = coin_to_trades(batch);
                                for sub in manager.subscriptions() {
                                    require_recent_source(batch.block_time())?;
                                    send_ws_data_from_trades(&mut socket, sub, &mut trades).await?;
                                }
                            },
                            InternalMessage::L4BookUpdates{ diff_batch, status_batch } => {
                                require_recent_source(diff_batch.block_time())?;
                                let mut book_updates = coin_to_book_updates(diff_batch, status_batch);
                                for sub in manager.subscriptions() {
                                    require_recent_source(diff_batch.block_time())?;
                                    send_ws_data_from_book_updates(&mut socket, sub, &mut book_updates).await?;
                                }
                            },
                        }

                    }
                    Err(err) => {
                        error!("Receiver error: {err}");
                        return Ok(());
                    }
                }
            }

            msg = socket.next() => {
                if let Some(frame) = msg {
                    match frame.opcode {
                        OpCode::Text => {
                            if frame.payload.len() > 4096 {
                                return Err("Subscription request exceeds 4096 bytes".into());
                            }
                            let text = match std::str::from_utf8(&frame.payload) {
                                Ok(text) => text,
                                Err(err) => {
                                    log::warn!("unable to parse websocket content: {err}: {:?}", frame.payload.as_ref());
                                    // deserves to close the connection because the payload is not a valid utf8 string.
                                    return Ok(());
                                }
                            };

                            info!("Client message: {text}");

                            if let Ok(value) = serde_json::from_str::<ClientMessage>(text) {
                                receive_client_message(&mut socket, &mut manager, value, &universe, listener.clone(), &mut sent_positions, &wire_cache).await?;
                            }
                            else {
                                let msg = ServerResponse::Error(format!("Error parsing JSON into valid websocket request: {text}"));
                                send_socket_message(&mut socket, msg).await?;
                            }
                        }
                        OpCode::Close => {
                            info!("Client disconnected");
                            return Ok(());
                        }
                        _ => {}
                    }
                } else {
                    info!("Client connection closed");
                    return Ok(());
                }
            }
        }
    }
}

async fn receive_client_message(
    socket: &mut WebSocket,
    manager: &mut SubscriptionManager,
    client_message: ClientMessage,
    universe: &HashSet<String>,
    listener: Arc<Mutex<OrderBookListener>>,
    sent_positions: &mut HashMap<String, u64>,
    wire_cache: &std::sync::Mutex<BookWireCache>,
) -> Result<()> {
    let subscription = match &client_message {
        ClientMessage::Unsubscribe { subscription } | ClientMessage::Subscribe { subscription } => subscription.clone(),
    };
    // this is used for display purposes only, hence unwrap_or_default. It also shouldn't fail
    let sub = serde_json::to_string(&subscription).unwrap_or_default();
    if matches!(client_message, ClientMessage::Subscribe { .. })
        && manager.subscriptions().len() >= MAX_SUBSCRIPTIONS
        && !manager.subscriptions().contains(&subscription)
    {
        send_socket_message(socket, ServerResponse::Error("Subscription capacity reached".into())).await?;
        return Ok(());
    }
    if !subscription.validate(universe) {
        let msg = ServerResponse::Error(format!("Invalid subscription: {sub}"));
        send_socket_message(socket, msg).await?;
        return Ok(());
    }
    let (word, success) = match &client_message {
        ClientMessage::Subscribe { .. } => ("", manager.subscribe(subscription)),
        ClientMessage::Unsubscribe { .. } => ("un", manager.unsubscribe(subscription)),
    };
    if success {
        if let ClientMessage::Subscribe { subscription: selection @ Subscription::L2Book { .. } } = &client_message {
            let snapshot = listener.lock().await.compute_l2_snapshot();
            if let Some((time, height, snapshots)) = snapshot {
                if require_recent_source(time).is_ok() {
                    send_socket_message(socket, ServerResponse::SubscriptionResponse(client_message)).await?;
                    let selection: Subscription = serde_json::from_str(&sub)?;
                    send_ws_data_from_snapshot(
                        socket,
                        &selection,
                        &snapshots,
                        time,
                        height,
                        sent_positions,
                        wire_cache,
                    )
                    .await?;
                    return Ok(());
                }
            }
            manager.unsubscribe(selection.clone());
            send_socket_message(socket, ServerResponse::Error("Unable to grab fresh order book snapshot".into()))
                .await?;
            return Ok(());
        }
        let snapshot_msg = if let ClientMessage::Subscribe { subscription } = &client_message {
            let msg = subscription.handle_immediate_snapshot(listener).await;
            match msg {
                Ok(msg) => msg,
                Err(err) => {
                    manager.unsubscribe(subscription.clone());
                    let msg = ServerResponse::Error(format!("Unable to grab order book snapshot: {err}"));
                    send_socket_message(socket, msg).await?;
                    return Ok(());
                }
            }
        } else {
            sent_positions.remove(&sub);
            None
        };
        let msg = ServerResponse::SubscriptionResponse(client_message);
        send_socket_message(socket, msg).await?;
        if let Some(snapshot_msg) = snapshot_msg {
            if let ServerResponse::L2Book(book) = &snapshot_msg {
                sent_positions.insert(sub, book.height());
            }
            send_socket_message(socket, snapshot_msg).await?;
        }
    } else {
        let msg = ServerResponse::Error(format!("Already {word}subscribed: {sub}"));
        send_socket_message(socket, msg).await?;
    }
    Ok(())
}

// A cancelled send may have written part of a frame. Its caller must drop
// the connection on any error; never continue publishing on that socket.
const SOCKET_WRITE_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_WIRE_BYTES: usize = 16 * 1024 * 1024;

fn require_recent_source(time_ms: u64) -> Result<()> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    if now.abs_diff(u128::from(time_ms)) > 3000 {
        return Err("Source/queue age exceeded 3 seconds; resnapshot required".into());
    }
    Ok(())
}

async fn send_socket_message<S>(socket: &mut S, msg: ServerResponse) -> Result<()>
where
    S: Sink<FrameView> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    let msg = serde_json::to_string(&msg)?;
    if msg.len() > MAX_WIRE_BYTES {
        return Err("Book stream frame exceeds 16 MiB; resnapshot required".into());
    }
    tokio::time::timeout(SOCKET_WRITE_TIMEOUT, socket.send(FrameView::text(msg)))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Book stream write exceeded 2 seconds"))??;
    Ok(())
}

// derive it from l2_snapshots because thats convenient
fn new_universe(l2_snapshots: &L2Snapshots, ignore_spot: bool) -> HashSet<String> {
    l2_snapshots
        .as_ref()
        .iter()
        .filter_map(|(c, _)| if !c.is_spot() || !ignore_spot { Some(c.clone().value()) } else { None })
        .collect()
}

#[derive(Default)]
struct BookWireCache {
    position: Option<(usize, u64, u64)>,
    source: Option<L2Snapshots>,
    entries: HashMap<Subscription, Arc<str>>,
    bytes: usize,
}

impl BookWireCache {
    fn get(
        &mut self,
        subscription: &Subscription,
        snapshots: &L2Snapshots,
        time: u64,
        height: u64,
    ) -> Result<Option<Arc<str>>> {
        let Subscription::L2Book { coin, n_sig_figs, n_levels, mantissa } = subscription else {
            return Ok(None);
        };
        let position = (std::ptr::from_ref(snapshots.as_ref()) as usize, time, height);
        let cacheable =
            self.position.is_none_or(|(_, cached_time, cached_height)| height >= cached_height && time >= cached_time);
        if cacheable && self.position != Some(position) {
            self.position = Some(position);
            // Retain the source Arc so an allocation address cannot be reused
            // as another view with the same height while cached bytes survive.
            self.source = Some(snapshots.clone());
            self.entries.clear();
            self.bytes = 0;
        }
        if self.position == Some(position) {
            if let Some(encoded) = self.entries.get(subscription) {
                return Ok(Some(encoded.clone()));
            }
        }
        let levels = match snapshots
            .as_ref()
            .get(&Coin::new(coin))
            .and_then(|v| v.get(&L2SnapshotParams::new(*n_sig_figs, *mantissa)))
        {
            Some(snapshot) => snapshot.truncate(n_levels.unwrap_or(DEFAULT_LEVELS)).export_inner_snapshot(),
            None if is_hip4_coin(coin) => [Vec::new(), Vec::new()],
            None => return Ok(None),
        };
        let encoded: Arc<str> = serde_json::to_string(&ServerResponse::L2Book(L2Book::from_l2_snapshot(
            coin.clone(),
            levels,
            time,
            height,
        )))?
        .into();
        if encoded.len() > MAX_WIRE_BYTES {
            return Err("Book frame exceeds byte bound".into());
        }
        // One cache shared by all sockets, not a cache per subscriber or block.
        if cacheable && self.bytes + encoded.len() <= 2 * 1024 * 1024 && self.entries.len() < 1024 {
            self.bytes += encoded.len();
            self.entries.insert(subscription.clone(), encoded.clone());
        }
        Ok(Some(encoded))
    }
}

async fn send_ws_data_from_snapshot(
    socket: &mut WebSocket,
    subscription: &Subscription,
    snapshots: &L2Snapshots,
    time: u64,
    height: u64,
    sent_positions: &mut HashMap<String, u64>,
    cache: &std::sync::Mutex<BookWireCache>,
) -> Result<()> {
    if !matches!(subscription, Subscription::L2Book { .. }) {
        return Ok(());
    }
    require_recent_source(time)?;
    let key = serde_json::to_string(subscription)?;
    if sent_positions.get(&key).is_some_and(|previous| height <= *previous) {
        return Ok(());
    }
    let encoded = {
        let mut cache = cache.lock().map_err(|_| "Book wire cache poisoned")?;
        cache.get(subscription, snapshots, time, height)?
    };
    if let Some(encoded) = encoded {
        tokio::time::timeout(SOCKET_WRITE_TIMEOUT, socket.send(FrameView::text(encoded.to_string())))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Book stream write timeout"))??;
        sent_positions.insert(key, height);
    }
    Ok(())
}

fn coin_to_trades(batch: &Batch<NodeDataFill>) -> HashMap<String, Vec<Trade>> {
    let mut fills = batch.clone().events();
    let mut trades = HashMap::new();
    while fills.len() >= 2 {
        let f2 = fills.pop();
        let f1 = fills.pop();
        if let Some(f1) = f1 {
            if let Some(f2) = f2 {
                let mut fills = HashMap::new();
                fills.insert(f1.1.side, f1);
                fills.insert(f2.1.side, f2);
                if let Some(trade) = Trade::from_fills(fills) {
                    let coin = trade.coin.clone();
                    trades.entry(coin).or_insert_with(Vec::new).push(trade);
                }
            }
        }
    }
    for list in trades.values_mut() {
        list.reverse();
    }
    trades
}

fn coin_to_book_updates(
    diff_batch: &Batch<NodeDataOrderDiff>,
    status_batch: &Batch<NodeDataOrderStatus>,
) -> HashMap<String, L4BookUpdates> {
    let diffs = diff_batch.clone().events();
    let statuses = status_batch.clone().events();
    let time = diff_batch.block_time();
    let height = diff_batch.block_number();
    let mut updates = HashMap::new();
    for diff in diffs {
        let coin = diff.coin().value();
        updates.entry(coin).or_insert_with(|| L4BookUpdates::new(time, height)).book_diffs.push(diff);
    }
    for status in statuses {
        let coin = status.order.coin.clone();
        updates.entry(coin).or_insert_with(|| L4BookUpdates::new(time, height)).order_statuses.push(status);
    }
    updates
}

async fn send_ws_data_from_book_updates(
    socket: &mut WebSocket,
    subscription: &Subscription,
    book_updates: &mut HashMap<String, L4BookUpdates>,
) -> Result<()> {
    if let Subscription::L4Book { coin } = subscription {
        if let Some(updates) = book_updates.remove(coin) {
            let msg = ServerResponse::L4Book(L4Book::Updates(updates));
            send_socket_message(socket, msg).await?;
        }
    }
    Ok(())
}

async fn send_ws_data_from_trades(
    socket: &mut WebSocket,
    subscription: &Subscription,
    trades: &mut HashMap<String, Vec<Trade>>,
) -> Result<()> {
    if let Subscription::Trades { coin } = subscription {
        if let Some(trades) = trades.remove(coin) {
            let msg = ServerResponse::Trades(trades);
            send_socket_message(socket, msg).await?;
        }
    }
    Ok(())
}

impl Subscription {
    // snapshots that begin a stream
    async fn handle_immediate_snapshot(
        &self,
        listener: Arc<Mutex<OrderBookListener>>,
    ) -> Result<Option<ServerResponse>> {
        if let Self::L2Book { coin, n_sig_figs, n_levels, mantissa } = self {
            if let Some((time, height, snapshots)) = listener.lock().await.compute_l2_snapshot() {
                require_recent_source(time)?;
                if let Some(snapshot) = snapshots
                    .as_ref()
                    .get(&Coin::new(coin))
                    .and_then(|value| value.get(&L2SnapshotParams::new(*n_sig_figs, *mantissa)))
                {
                    let levels = snapshot.truncate(n_levels.unwrap_or(DEFAULT_LEVELS)).export_inner_snapshot();
                    return Ok(Some(ServerResponse::L2Book(L2Book::from_l2_snapshot(
                        coin.clone(),
                        levels,
                        time,
                        height,
                    ))));
                }
                if is_hip4_coin(coin) {
                    return Ok(Some(ServerResponse::L2Book(L2Book::from_l2_snapshot(
                        coin.clone(),
                        [Vec::new(), Vec::new()],
                        time,
                        height,
                    ))));
                }
            }
            return Err("Snapshot Failed".into());
        }
        if let Self::L4Book { coin } = self {
            let snapshot = listener.lock().await.compute_snapshot();
            if let Some(TimedSnapshots { time, height, snapshot }) = snapshot {
                require_recent_source(time)?;
                let snapshot =
                    snapshot.value().into_iter().filter(|(c, _)| *c == Coin::new(coin)).collect::<Vec<_>>().pop();
                if let Some((coin, snapshot)) = snapshot {
                    let snapshot =
                        snapshot.as_ref().clone().map(|orders| orders.into_iter().map(L4Order::from).collect());
                    return Ok(Some(ServerResponse::L4Book(L4Book::Snapshot {
                        coin: coin.value(),
                        time,
                        height,
                        levels: snapshot,
                    })));
                }
            }
            return Err("Snapshot Failed".into());
        }
        Ok(None)
    }
}

#[cfg(test)]
mod transport_tests {
    use super::*;

    #[test]
    fn shared_wire_cache_reuses_exact_view_and_advances_position() {
        let mut cache = BookWireCache::default();
        let snapshot = L2Snapshots::default();
        let subscription =
            Subscription::L2Book { coin: "#10".into(), n_sig_figs: None, n_levels: None, mantissa: None };
        let first = cache.get(&subscription, &snapshot, 1000, 1).unwrap().unwrap();
        let repeated = cache.get(&subscription, &snapshot, 1000, 1).unwrap().unwrap();
        assert!(Arc::ptr_eq(&first, &repeated));
        let next = cache.get(&subscription, &snapshot, 1001, 2).unwrap().unwrap();
        assert!(!Arc::ptr_eq(&first, &next));
        let value: serde_json::Value = serde_json::from_str(&next).unwrap();
        assert_eq!(value["data"]["height"], 2);
        assert_eq!(cache.entries.len(), 1);
        let old = cache.get(&subscription, &snapshot, 1000, 1).unwrap().unwrap();
        assert_eq!(serde_json::from_str::<serde_json::Value>(&old).unwrap()["data"]["height"], 1);
        let still_current = cache.get(&subscription, &snapshot, 1001, 2).unwrap().unwrap();
        assert!(Arc::ptr_eq(&next, &still_current));
    }

    #[test]
    fn wire_cache_is_bounded_across_subscribed_views() {
        let mut cache = BookWireCache::default();
        let snapshot = L2Snapshots::default();
        for id in 0..1100 {
            let subscription =
                Subscription::L2Book { coin: format!("#{id}"), n_sig_figs: None, n_levels: None, mantissa: None };
            assert!(cache.get(&subscription, &snapshot, 1000, 1).unwrap().is_some());
        }
        assert_eq!(cache.entries.len(), 1024);
        assert!(cache.bytes <= 2 * 1024 * 1024);
    }

    #[test]
    fn source_age_rejects_stale_and_future_positions() {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        assert!(require_recent_source(now).is_ok());
        assert!(require_recent_source(now - 4000).is_err());
        assert!(require_recent_source(now + 4000).is_err());
    }
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };

    #[derive(Default)]
    struct TestSocket {
        blocked: bool,
        broken: bool,
        sent: usize,
    }

    impl Sink<FrameView> for TestSocket {
        type Error = io::Error;
        fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn start_send(mut self: Pin<&mut Self>, _: FrameView) -> io::Result<()> {
            self.sent += 1;
            Ok(())
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            if self.broken {
                Poll::Ready(Err(io::Error::new(io::ErrorKind::BrokenPipe, "test disconnected peer")))
            } else if self.blocked {
                Poll::Pending
            } else {
                Poll::Ready(Ok(()))
            }
        }
        fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.poll_flush(cx)
        }
    }

    #[tokio::test]
    async fn broken_write_propagates_instead_of_continuing_the_batch() {
        let mut socket = TestSocket { broken: true, ..Default::default() };
        let batch = async {
            for _ in 0..10 {
                send_socket_message(&mut socket, ServerResponse::Error("test".into())).await?;
            }
            Ok::<(), Error>(())
        }
        .await;
        assert!(batch.is_err());
        assert_eq!(socket.sent, 1);
    }

    #[tokio::test]
    async fn blocked_flush_expires_while_another_connection_keeps_working() {
        let mut blocked = TestSocket { blocked: true, ..Default::default() };
        let mut healthy = TestSocket::default();
        let (stalled_result, healthy_result) = tokio::join!(
            send_socket_message(&mut blocked, ServerResponse::Error("blocked".into())),
            tokio::time::timeout(Duration::from_millis(100), async {
                for _ in 0..10 {
                    send_socket_message(&mut healthy, ServerResponse::Error("healthy".into())).await?;
                }
                Ok::<(), Error>(())
            })
        );
        let error = stalled_result.unwrap_err();
        assert_eq!(error.downcast_ref::<io::Error>().unwrap().kind(), io::ErrorKind::TimedOut);
        assert!(healthy_result.unwrap().is_ok());
        assert_eq!(healthy.sent, 10);
        assert_eq!(blocked.sent, 1);
    }
}
