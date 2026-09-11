use crate::{
    listeners::{directory::DirectoryListener, order_book::state::OrderBookState},
    order_book::{
        Coin, Snapshot,
        multi_book::{Snapshots, load_snapshots_from_json},
    },
    prelude::*,
    types::{
        L4Order,
        inner::{InnerL4Order, InnerLevel},
        node_data::{Batch, EventSource, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
    },
};
use alloy::primitives::Address;
use fs::File;
use log::{error, info, warn};
use notify::{Event, RecursiveMode, Watcher, recommended_watcher};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet, VecDeque},
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{Mutex, broadcast::Sender, mpsc::unbounded_channel},
    task::JoinSet,
    time::{Instant, interval_at, sleep},
};
use utils::{BatchQueue, EventBatch, process_rmp_file, validate_snapshot_consistency};

mod state;
mod utils;

// WARNING - this code assumes no other file system operations are occurring in the watched directories
// if there are scripts running, this may not work as intended
pub(crate) async fn hl_listen(listener: Arc<Mutex<OrderBookListener>>, dir: PathBuf) -> Result<()> {
    let order_statuses_dir = EventSource::OrderStatuses.event_source_dir(&dir).canonicalize()?;
    let fills_dir = EventSource::Fills.event_source_dir(&dir).canonicalize()?;
    let order_diffs_dir = EventSource::OrderDiffs.event_source_dir(&dir).canonicalize()?;
    info!("Monitoring order status directory: {}", order_statuses_dir.display());
    info!("Monitoring order diffs directory: {}", order_diffs_dir.display());
    info!("Monitoring fills directory: {}", fills_dir.display());

    // monitoring the directory via the notify crate (gives file system events)
    let (fs_event_tx, mut fs_event_rx) = unbounded_channel();
    let mut watcher = recommended_watcher(move |res| {
        let fs_event_tx = fs_event_tx.clone();
        if let Err(err) = fs_event_tx.send(res) {
            error!("Error sending fs event to processor via channel: {err}");
        }
    })?;

    // One owner for snapshot/reconciliation and its update cache. Dropping the
    // listener loop aborts the task instead of leaving a detached reconciler.
    let mut snapshots = JoinSet::new();

    watcher.watch(&order_statuses_dir, RecursiveMode::Recursive)?;
    watcher.watch(&fills_dir, RecursiveMode::Recursive)?;
    watcher.watch(&order_diffs_dir, RecursiveMode::Recursive)?;
    let start = Instant::now() + Duration::from_secs(5);
    let mut ticker = interval_at(start, Duration::from_secs(10));
    loop {
        tokio::select! {
            event = fs_event_rx.recv() =>  match event {
                Some(Ok(event)) => {
                    if event.kind.is_create() || event.kind.is_modify() {
                        let new_path = &event.paths[0];
                        if new_path.starts_with(&order_statuses_dir) && new_path.is_file() {
                            listener
                                .lock()
                                .await
                                .process_update(&event, new_path, EventSource::OrderStatuses)
                                .map_err(|err| format!("Order status processing error: {err}"))?;
                        } else if new_path.starts_with(&fills_dir) && new_path.is_file() {
                            listener
                                .lock()
                                .await
                                .process_update(&event, new_path, EventSource::Fills)
                                .map_err(|err| format!("Fill update processing error: {err}"))?;
                        } else if new_path.starts_with(&order_diffs_dir) && new_path.is_file() {
                            listener
                                .lock()
                                .await
                                .process_update(&event, new_path, EventSource::OrderDiffs)
                                .map_err(|err| format!("Book diff processing error: {err}"))?;
                        }
                    }
                }
                Some(Err(err)) => {
                    error!("Watcher error: {err}");
                    return Err(format!("Watcher error: {err}").into());
                }
                None => {
                    error!("Channel closed. Listener exiting");
                    return Err("Channel closed.".into());
                }
            },
            snapshot_fetch_res = snapshots.join_next(), if !snapshots.is_empty() => {
                match snapshot_fetch_res {
                    Some(Ok(Ok(()))) => ticker.reset(),
                    Some(Ok(Err(err))) => return Err(format!("Abci state reading error: {err}").into()),
                    Some(Err(err)) => return Err(format!("Snapshot task failed: {err}").into()),
                    None => return Err("Snapshot task disappeared".into()),
                }
            }
            _ = ticker.tick(), if snapshots.is_empty() => {
                snapshots.spawn(fetch_snapshot(dir.clone(), listener.clone()));
            }
        }
    }
}

async fn fetch_snapshot(dir: PathBuf, listener: Arc<Mutex<OrderBookListener>>) -> Result<()> {
    match process_rmp_file(&dir).await {
        Ok(output_fln) => {
            let state = {
                let mut listener = listener.lock().await;
                listener.begin_caching();
                listener.clone_state()
            };
            let snapshot = load_snapshots_from_json::<InnerL4Order, (Address, L4Order)>(output_fln.path()).await;
            info!("Snapshot fetched");
            // sleep to let some updates build up.
            sleep(Duration::from_secs(1)).await;
            match snapshot {
                Ok((height, expected_snapshot)) => {
                    let mut listener = listener.lock().await;
                    let cache = listener.take_cache();
                    info!("Cache has {} elements", cache.len());
                    listener.reconcile_snapshot(state, expected_snapshot, height, cache)
                }
                Err(err) => {
                    listener.lock().await.take_cache();
                    Err(err)
                }
            }
        }
        Err(err) => Err(err),
    }
}

pub(crate) struct OrderBookListener {
    ignore_spot: bool,
    fill_status_file: Option<File>,
    order_status_file: Option<File>,
    order_diff_file: Option<File>,
    // None if we haven't seen a valid snapshot yet
    order_book_state: Option<OrderBookState>,
    last_fill: Option<u64>,
    order_diff_cache: BatchQueue<NodeDataOrderDiff>,
    order_status_cache: BatchQueue<NodeDataOrderStatus>,
    // Only Some when we want it to collect updates
    fetched_snapshot_cache: Option<VecDeque<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)>>,
    internal_message_tx: Option<Sender<Arc<InternalMessage>>>,
}

impl OrderBookListener {
    pub(crate) const fn new(internal_message_tx: Option<Sender<Arc<InternalMessage>>>, ignore_spot: bool) -> Self {
        Self {
            ignore_spot,
            fill_status_file: None,
            order_status_file: None,
            order_diff_file: None,
            order_book_state: None,
            last_fill: None,
            fetched_snapshot_cache: None,
            internal_message_tx,
            order_diff_cache: BatchQueue::new(),
            order_status_cache: BatchQueue::new(),
        }
    }

    fn clone_state(&self) -> Option<OrderBookState> {
        self.order_book_state.clone()
    }

    pub(crate) const fn is_ready(&self) -> bool {
        self.order_book_state.is_some()
    }

    pub(crate) fn universe(&self) -> HashSet<Coin> {
        self.order_book_state.as_ref().map_or_else(HashSet::new, OrderBookState::compute_universe)
    }

    #[allow(clippy::type_complexity)]
    // pops earliest pair of cached updates that have the same timestamp if possible
    fn pop_cache(&mut self) -> Option<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)> {
        // synchronize to same block
        while let Some(t) = self.order_diff_cache.front() {
            if let Some(s) = self.order_status_cache.front() {
                match t.block_number().cmp(&s.block_number()) {
                    Ordering::Less => {
                        self.order_diff_cache.pop_front();
                    }
                    Ordering::Equal => {
                        return self
                            .order_status_cache
                            .pop_front()
                            .and_then(|t| self.order_diff_cache.pop_front().map(|s| (t, s)));
                    }
                    Ordering::Greater => {
                        self.order_status_cache.pop_front();
                    }
                }
            } else {
                break;
            }
        }
        None
    }

    fn receive_batch(&mut self, updates: EventBatch) -> Result<()> {
        match updates {
            EventBatch::Orders(batch) => {
                self.order_status_cache.push(batch);
            }
            EventBatch::BookDiffs(batch) => {
                self.order_diff_cache.push(batch);
            }
            EventBatch::Fills(batch) => {
                if self.last_fill.is_none_or(|height| height < batch.block_number()) {
                    // send fill updates if we received a new update
                    if let Some(tx) = &self.internal_message_tx {
                        let tx = tx.clone();
                        tokio::spawn(async move {
                            let snapshot = Arc::new(InternalMessage::Fills { batch });
                            let _unused = tx.send(snapshot);
                        });
                    }
                }
            }
        }
        if self.is_ready() {
            if let Some((order_statuses, order_diffs)) = self.pop_cache() {
                self.order_book_state
                    .as_mut()
                    .map(|book| book.apply_updates(order_statuses.clone(), order_diffs.clone()))
                    .transpose()?;
                if let Some(cache) = &mut self.fetched_snapshot_cache {
                    cache.push_back((order_statuses.clone(), order_diffs.clone()));
                }
                if let Some(tx) = &self.internal_message_tx {
                    let tx = tx.clone();
                    tokio::spawn(async move {
                        let updates = Arc::new(InternalMessage::L4BookUpdates {
                            diff_batch: order_diffs,
                            status_batch: order_statuses,
                        });
                        let _unused = tx.send(updates);
                    });
                }
            }
        }
        Ok(())
    }

    fn begin_caching(&mut self) {
        self.fetched_snapshot_cache = Some(VecDeque::new());
    }

    // tkae the cached updates and stop collecting updates
    fn take_cache(&mut self) -> VecDeque<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)> {
        self.fetched_snapshot_cache.take().unwrap_or_default()
    }

    fn init_from_snapshot(&mut self, snapshot: Snapshots<InnerL4Order>, height: u64) {
        info!("No existing snapshot");
        let mut new_order_book = OrderBookState::from_snapshot(snapshot, height, 0, true, self.ignore_spot);
        let mut retry = false;
        while let Some((order_statuses, order_diffs)) = self.pop_cache() {
            if new_order_book.apply_updates(order_statuses, order_diffs).is_err() {
                info!(
                    "Failed to apply updates to this book (likely missing older updates). Waiting for next snapshot."
                );
                retry = true;
                break;
            }
        }
        if !retry {
            self.order_book_state = Some(new_order_book);
            info!("Order book ready");
        }
    }

    fn reconcile_snapshot(
        &mut self,
        state: Option<OrderBookState>,
        expected_snapshot: Snapshots<InnerL4Order>,
        height: u64,
        mut cache: VecDeque<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)>,
    ) -> Result<()> {
        let Some(mut state) = state else {
            self.init_from_snapshot(expected_snapshot, height);
            return Ok(());
        };

        while state.height() < height {
            if let Some((order_statuses, order_diffs)) = cache.pop_front() {
                state.apply_updates(order_statuses, order_diffs)?;
            } else {
                warn!("Snapshot at height {height} is ahead of cached updates; skipping validation");
                return Ok(());
            }
        }
        if state.height() > height {
            warn!("Fetched snapshot at height {height} lags stored state at {}; skipping validation", state.height());
            return Ok(());
        }

        info!("Validating snapshot");
        if let Err(err) =
            validate_snapshot_consistency(&state.compute_snapshot().snapshot, &expected_snapshot, self.ignore_spot)
        {
            warn!("Snapshot diverged ({err}); reloading authoritative node snapshot");
            let mut recovered = OrderBookState::from_snapshot(expected_snapshot, height, 0, true, self.ignore_spot);
            while let Some((order_statuses, order_diffs)) = cache.pop_front() {
                recovered.apply_updates(order_statuses, order_diffs)?;
            }
            self.order_book_state = Some(recovered);
            info!("Order book recovered from node snapshot");
        }
        Ok(())
    }

    // forcibly grab current snapshot
    pub(crate) fn compute_snapshot(&mut self) -> Option<TimedSnapshots> {
        self.order_book_state.as_mut().map(|o| o.compute_snapshot())
    }

    pub(crate) fn compute_l2_snapshot(&mut self) -> Option<(u64, u64, L2Snapshots)> {
        self.order_book_state.as_mut().map(|o| o.compute_l2_snapshot())
    }

    // prevent snapshotting mutiple times at the same height
    fn l2_snapshots(&mut self, prevent_future_snaps: bool) -> Option<(u64, u64, L2Snapshots)> {
        self.order_book_state.as_mut().and_then(|o| o.l2_snapshots(prevent_future_snaps))
    }
}

impl OrderBookListener {
    fn process_update(&mut self, event: &Event, new_path: &PathBuf, event_source: EventSource) -> Result<()> {
        if event.kind.is_create() {
            info!("-- Event: {} created --", new_path.display());
            self.on_file_creation(new_path.clone(), event_source)?;
        }
        // Check for `Modify` event (only if the file is already initialized)
        else {
            // If we are not tracking anything right now, we treat a file update as declaring that it has been created.
            // Unfortunately, we miss the update that occurs at this time step.
            // We go to the end of the file to read for updates after that.
            if self.is_reading(event_source) {
                self.on_file_modification(event_source)?;
            } else {
                info!("-- Event: {} modified, tracking it now --", new_path.display());
                let file = self.file_mut(event_source);
                let mut new_file = File::open(new_path)?;
                new_file.seek(SeekFrom::End(0))?;
                *file = Some(new_file);
            }
        }
        Ok(())
    }
}

impl DirectoryListener for OrderBookListener {
    fn is_reading(&self, event_source: EventSource) -> bool {
        match event_source {
            EventSource::Fills => self.fill_status_file.is_some(),
            EventSource::OrderStatuses => self.order_status_file.is_some(),
            EventSource::OrderDiffs => self.order_diff_file.is_some(),
        }
    }

    fn file_mut(&mut self, event_source: EventSource) -> &mut Option<File> {
        match event_source {
            EventSource::Fills => &mut self.fill_status_file,
            EventSource::OrderStatuses => &mut self.order_status_file,
            EventSource::OrderDiffs => &mut self.order_diff_file,
        }
    }

    fn on_file_creation(&mut self, new_file: PathBuf, event_source: EventSource) -> Result<()> {
        if let Some(file) = self.file_mut(event_source).as_mut() {
            let mut buf = String::new();
            file.read_to_string(&mut buf)?;
            if !buf.is_empty() {
                self.process_data(buf, event_source)?;
            }
        }
        *self.file_mut(event_source) = Some(File::open(new_file)?);
        Ok(())
    }

    fn process_data(&mut self, data: String, event_source: EventSource) -> Result<()> {
        for record in data.split_inclusive('\n') {
            if !record.ends_with('\n') {
                let unread = i64::try_from(record.len())?;
                if let Some(file) = self.file_mut(event_source).as_mut() {
                    file.seek_relative(-unread)?;
                }
                break;
            }
            let line = record.trim_end_matches(['\r', '\n']);
            if line.is_empty() {
                continue;
            }
            let res = match event_source {
                EventSource::Fills => serde_json::from_str::<Batch<NodeDataFill>>(line).map(|batch| {
                    let height = batch.block_number();
                    (height, EventBatch::Fills(batch))
                }),
                EventSource::OrderStatuses => serde_json::from_str(line)
                    .map(|batch: Batch<NodeDataOrderStatus>| (batch.block_number(), EventBatch::Orders(batch))),
                EventSource::OrderDiffs => serde_json::from_str(line)
                    .map(|batch: Batch<NodeDataOrderDiff>| (batch.block_number(), EventBatch::BookDiffs(batch))),
            };
            let (height, event_batch) = match res {
                Ok(data) => data,
                Err(err) => {
                    error!(
                        "{event_source} serialization error {err}, height: {:?}, line: {:?}",
                        self.order_book_state.as_ref().map(OrderBookState::height),
                        line.get(..100).unwrap_or(line),
                    );
                    continue;
                }
            };
            if height % 100 == 0 {
                info!("{event_source} block: {height}");
            }
            if let Err(err) = self.receive_batch(event_batch) {
                self.order_book_state = None;
                return Err(err);
            }
        }
        let snapshot = self.l2_snapshots(true);
        if let Some(snapshot) = snapshot {
            if let Some(tx) = &self.internal_message_tx {
                // Preserve completed snapshot order; detached tasks can race.
                let snapshot = Arc::new(InternalMessage::Snapshot {
                    l2_snapshots: snapshot.2,
                    time: snapshot.0,
                    height: snapshot.1,
                });
                let _unused = tx.send(snapshot);
            }
        }
        Ok(())
    }
}

pub(crate) type CoinL2Snapshots = HashMap<L2SnapshotParams, Snapshot<InnerLevel>>;
pub(crate) type L2SnapshotMap = HashMap<Coin, Arc<CoinL2Snapshots>>;

#[derive(Clone, Default)]
pub(crate) struct L2Snapshots(Arc<L2SnapshotMap>);

impl L2Snapshots {
    pub(crate) fn as_ref(&self) -> &L2SnapshotMap {
        &self.0
    }
}

pub(crate) struct TimedSnapshots {
    pub(crate) time: u64,
    pub(crate) height: u64,
    pub(crate) snapshot: Snapshots<InnerL4Order>,
}

// Messages sent from node data listener to websocket dispatch to support streaming
pub(crate) enum InternalMessage {
    Snapshot { l2_snapshots: L2Snapshots, time: u64, height: u64 },
    Fills { batch: Batch<NodeDataFill> },
    L4BookUpdates { diff_batch: Batch<NodeDataOrderDiff>, status_batch: Batch<NodeDataOrderStatus> },
}

#[derive(Eq, PartialEq, Hash)]
pub(crate) struct L2SnapshotParams {
    n_sig_figs: Option<u32>,
    mantissa: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order_book::multi_book::load_snapshots_from_str;

    #[test]
    fn reloads_snapshot_when_a_market_is_added() -> Result<()> {
        let mut listener = OrderBookListener::new(None, false);
        listener.order_book_state =
            Some(OrderBookState::from_snapshot(Snapshots::new(HashMap::new()), 100, 0, true, false));
        let (_, expected) =
            load_snapshots_from_str::<InnerL4Order, (Address, L4Order)>(r#"[100, [["NEW", [[], []]]]]"#)?;

        listener.reconcile_snapshot(listener.clone_state(), expected, 100, VecDeque::new())?;

        assert!(listener.universe().contains(&Coin::new("NEW")));
        Ok(())
    }

    #[test]
    fn ignores_snapshot_older_than_live_state() -> Result<()> {
        let mut listener = OrderBookListener::new(None, false);
        listener.order_book_state =
            Some(OrderBookState::from_snapshot(Snapshots::new(HashMap::new()), 101, 0, true, false));

        listener.reconcile_snapshot(listener.clone_state(), Snapshots::new(HashMap::new()), 100, VecDeque::new())?;

        assert!(listener.is_ready());
        Ok(())
    }

    #[test]
    fn incomplete_record_rewinds_only_that_record() -> Result<()> {
        let path = std::env::temp_dir().join(format!("order-book-partial-{}", std::process::id()));
        let data = format!("\n{{\"value\":\"{}", "x".repeat(120));
        fs::write(&path, &data)?;
        let mut file = File::open(&path)?;
        file.seek(SeekFrom::End(0))?;
        let mut listener = OrderBookListener::new(None, false);
        listener.order_status_file = Some(file);

        listener.process_data(data, EventSource::OrderStatuses)?;

        let position = listener.order_status_file.as_mut().unwrap().stream_position()?;
        fs::remove_file(path)?;
        assert_eq!(position, 1);
        Ok(())
    }

    #[test]
    fn malformed_complete_record_is_skipped() -> Result<()> {
        let path = std::env::temp_dir().join(format!("order-book-malformed-{}", std::process::id()));
        let data = "not-json\n";
        fs::write(&path, data)?;
        let mut file = File::open(&path)?;
        file.seek(SeekFrom::End(0))?;
        let mut listener = OrderBookListener::new(None, false);
        listener.order_status_file = Some(file);

        listener.process_data(data.to_string(), EventSource::OrderStatuses)?;

        let position = listener.order_status_file.as_mut().unwrap().stream_position()?;
        fs::remove_file(path)?;
        assert_eq!(position, data.len() as u64);
        Ok(())
    }

    #[test]
    fn snapshot_ahead_of_cache_is_retried_later() -> Result<()> {
        let mut listener = OrderBookListener::new(None, false);
        listener.order_book_state =
            Some(OrderBookState::from_snapshot(Snapshots::new(HashMap::new()), 100, 0, true, false));

        listener.reconcile_snapshot(listener.clone_state(), Snapshots::new(HashMap::new()), 101, VecDeque::new())?;

        assert!(listener.is_ready());
        Ok(())
    }
}
