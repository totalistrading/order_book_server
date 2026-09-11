use crate::{
    listeners::order_book::state::OrderBookState,
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
use log::{error, info, warn};
use notify::{Event, RecursiveMode, Watcher, recommended_watcher};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet, VecDeque},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{Mutex, broadcast::Sender},
    task::JoinSet,
    time::{Instant, interval_at, sleep},
};
use utils::{BatchQueue, EventBatch, process_rmp_file, validate_snapshot_consistency};

mod ingestion;
mod state;
mod utils;

pub(crate) async fn hl_listen(listener: Arc<Mutex<OrderBookListener>>, dir: PathBuf) -> Result<()> {
    hl_listen_with_source(listener, dir, "http://localhost:3001/info".into(), ResourceLimits::from_env()?).await
}

async fn hl_listen_with_source(
    listener: Arc<Mutex<OrderBookListener>>,
    dir: PathBuf,
    info_url: String,
    limits: ResourceLimits,
) -> Result<()> {
    use ingestion::{DirtyFiles, FileCursor};
    use std::sync::Mutex as StdMutex;
    use tokio::sync::Notify;

    let roots = [EventSource::OrderStatuses, EventSource::OrderDiffs, EventSource::Fills]
        .into_iter()
        .map(|source| Ok((source.event_source_dir(&dir).canonicalize()?, source)))
        .collect::<Result<Vec<_>>>()?;
    listener.lock().await.limits = limits;
    let dirty = Arc::new(StdMutex::new(DirtyFiles::new(limits.dirty_files)));
    let wake = Arc::new(Notify::new());
    let mut watcher = recommended_watcher({
        let dirty = dirty.clone();
        let wake = wake.clone();
        move |res: notify::Result<Event>| {
            let mut queue = dirty.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            match res {
                Ok(event)
                    if (event.kind.is_create() || event.kind.is_modify())
                        && !matches!(event.kind, notify::EventKind::Modify(notify::event::ModifyKind::Metadata(_))) =>
                {
                    for path in event.paths {
                        queue.push(path, event.kind.is_create());
                    }
                }
                Ok(_) => return,
                Err(err) => {
                    error!("Filesystem watcher gap: {err}");
                    queue.mark_overflow();
                }
            }
            wake.notify_one();
        }
    })?;
    for (root, _) in &roots {
        watcher.watch(root, RecursiveMode::Recursive)?;
    }
    let mut cursors = HashMap::<PathBuf, FileCursor>::new();
    let mut snapshots = JoinSet::new();
    let mut ticker = interval_at(Instant::now() + Duration::from_secs(5), Duration::from_secs(10));
    let mut maintenance = interval_at(Instant::now() + Duration::from_secs(1), Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = maintenance.tick() => {
                let pending_file_bytes = cursors.values().map(FileCursor::backlog_bytes).sum();
                let dirty_files = dirty.lock().unwrap_or_else(std::sync::PoisonError::into_inner).len();
                let mut state = listener.lock().await;
                state.stats.pending_file_bytes = pending_file_bytes;
                state.stats.dirty_files = dirty_files;
                if state.snapshot_cache_started.is_some_and(|at| at.elapsed() > limits.queue_age) {
                    state.recovery_epoch = state.recovery_epoch.wrapping_add(1);
                    state.take_cache();
                    warn!("Snapshot validation cache exceeded age budget; awaiting its owner before retry");
                }
                if state.order_status_cache.expired(limits) || state.order_diff_cache.expired(limits) {
                    state.fence("unmatched source queue exceeded age limit");
                }
            }
            _ = wake.notified() => {
                let (overflow, next) = {
                    let mut queue = dirty.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                    (queue.take_overflow(), queue.pop())
                };
                if overflow {
                    listener.lock().await.fence("filesystem notification capacity exceeded");
                    cursors.clear();
                }
                if let Some((path, created)) = next {
                    if let Some((root, source)) = roots.iter().find(|(root, _)| path.starts_with(root)) {
                        if path.is_file() {
                            if !cursors.contains_key(&path) {
                                cursors.retain(|path, cursor| path.exists() || !cursor.drained());
                                if cursors.len() >= limits.dirty_files {
                                    listener.lock().await.fence("file cursor capacity exceeded");
                                    cursors.clear();
                                }
                                // At initial attachment, start at the tail and obtain a
                                // fresh authoritative snapshot. A later rotated file starts
                                // at byte zero even if its create notification was coalesced.
                                let from_end = !created && !cursors.keys().any(|known| known.starts_with(root));
                                if from_end {
                                    listener.lock().await.fence("attaching to a file without a known offset");
                                }
                                match FileCursor::open(&path, from_end, limits.record_bytes) {
                                    Ok(cursor) => { cursors.insert(path.clone(), cursor); }
                                    Err(err) => listener.lock().await.fence(&format!("opening source file: {err}")),
                                }
                            }
                            if let Some(cursor) = cursors.get_mut(&path) {
                                let read_started = Instant::now();
                                match cursor.read_turn(&path, limits.turn_bytes) {
                                    Ok((data, more, read_bytes)) => {
                                        let read_time = read_started.elapsed().as_micros() as u64;
                                        let waiting = Instant::now();
                                        {
                                            let mut state = listener.lock().await;
                                            state.stats.lock_wait_us += waiting.elapsed().as_micros() as u64;
                                            state.stats.read_bytes += read_bytes as u64;
                                            state.stats.read_time_us += read_time;
                                            let processing = Instant::now();
                                            if !data.is_empty() {
                                                if let Err(err) = state.process_data(data, *source) {
                                                    state.fence(&format!("source record gap: {err}"));
                                                }
                                            }
                                            state.stats.process_time_us += processing.elapsed().as_micros() as u64;
                                        }
                                        if more {
                                            dirty.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
                                                .push(path.clone(), false);
                                        }
                                    }
                                    Err(err) => {
                                        listener.lock().await.fence(&format!("source file gap: {err}"));
                                        cursors.remove(&path);
                                        // Skip the invalid interval once. Reopening at zero
                                        // would replay the same oversized/malformed interval
                                        // forever instead of allowing a fresh snapshot.
                                        if let Ok(cursor) = FileCursor::open(&path, true, limits.record_bytes) {
                                            cursors.insert(path.clone(), cursor);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                if !dirty.lock().unwrap_or_else(std::sync::PoisonError::into_inner).is_empty() {
                    wake.notify_one();
                }
                // Each path gets at most one byte budget before other paths and
                // snapshot completion can progress, even during a notification burst.
                tokio::task::yield_now().await;
            }
            result = snapshots.join_next(), if !snapshots.is_empty() => {
                match result {
                    Some(Ok(Ok(()))) => {},
                    Some(Ok(Err(err))) => listener.lock().await.fence(&format!("snapshot recovery failed: {err}")),
                    Some(Err(err)) => listener.lock().await.fence(&format!("snapshot task failed: {err}")),
                    None => return Err("Snapshot task disappeared".into()),
                }
                ticker.reset();
            }
            _ = ticker.tick(), if snapshots.is_empty() => {
                snapshots.spawn(fetch_snapshot(dir.clone(), listener.clone(), info_url.clone()));
            }
        }
    }
}

async fn fetch_snapshot(dir: PathBuf, listener: Arc<Mutex<OrderBookListener>>, info_url: String) -> Result<()> {
    let started = Instant::now();
    // Capture the validation baseline before asking the node for its snapshot,
    // so updates during export are available to reach that snapshot's height.
    let (epoch, state, timeout) = {
        let mut listener = listener.lock().await;
        let epoch = listener.recovery_epoch;
        listener.begin_caching();
        let cloning = Instant::now();
        let state = listener.clone_state();
        listener.stats.snapshot_clone_ms = cloning.elapsed().as_millis() as u64;
        (epoch, state, listener.limits.snapshot_timeout)
    };
    match process_rmp_file(&dir, &info_url, timeout).await {
        Ok(output_fln) => {
            if listener.lock().await.recovery_epoch != epoch {
                return Ok(());
            }
            let parsing = Instant::now();
            let snapshot = load_snapshots_from_json::<InnerL4Order, (Address, L4Order)>(output_fln.path()).await;
            listener.lock().await.stats.snapshot_parse_ms = parsing.elapsed().as_millis() as u64;
            info!("Snapshot fetched");
            // sleep to let some updates build up.
            sleep(Duration::from_secs(1)).await;
            match snapshot {
                Ok((height, expected_snapshot)) => {
                    let mut listener = listener.lock().await;
                    info!("Snapshot completed in {:?}", started.elapsed());
                    listener.finish_snapshot(epoch, state, expected_snapshot, height)
                }
                Err(err) => {
                    let mut listener = listener.lock().await;
                    listener.take_cache();
                    listener.stats.snapshot_failures += 1;
                    warn!("Snapshot validation unavailable; preserving current live state: {err}");
                    Ok(())
                }
            }
        }
        Err(err) => {
            let mut listener = listener.lock().await;
            listener.take_cache();
            listener.stats.snapshot_failures += 1;
            warn!("Snapshot request unavailable; preserving current live state: {err}");
            Ok(())
        }
    }
}

#[derive(Clone, Copy)]
struct ResourceLimits {
    dirty_files: usize,
    turn_bytes: usize,
    record_bytes: usize,
    queue_bytes: usize,
    queue_heights: usize,
    queue_age: Duration,
    snapshot_timeout: Duration,
}

impl ResourceLimits {
    const DEFAULT: Self = Self {
        dirty_files: 32,
        turn_bytes: 1024 * 1024,
        record_bytes: 64 * 1024 * 1024,
        queue_bytes: 1024 * 1024 * 1024,
        queue_heights: 4096,
        queue_age: Duration::from_secs(120),
        snapshot_timeout: Duration::from_secs(120),
    };

    fn from_env() -> Result<Self> {
        fn number(name: &str, fallback: usize) -> Result<usize> {
            let value = match std::env::var(name) {
                Ok(value) => value.parse::<usize>().map_err(|err| format!("{name}: {err}"))?,
                Err(std::env::VarError::NotPresent) => fallback,
                Err(err) => return Err(format!("{name}: {err}").into()),
            };
            if value == 0 {
                return Err(format!("{name} must be positive").into());
            }
            Ok(value)
        }
        let defaults = Self::DEFAULT;
        Ok(Self {
            dirty_files: number("BOOK_MAX_DIRTY_FILES", defaults.dirty_files)?,
            turn_bytes: number("BOOK_READ_TURN_BYTES", defaults.turn_bytes)?,
            record_bytes: number("BOOK_MAX_RECORD_BYTES", defaults.record_bytes)?,
            queue_bytes: number("BOOK_MAX_QUEUE_BYTES", defaults.queue_bytes)?,
            queue_heights: number("BOOK_MAX_QUEUE_HEIGHTS", defaults.queue_heights)?,
            queue_age: Duration::from_secs(
                number("BOOK_MAX_QUEUE_AGE_SECONDS", defaults.queue_age.as_secs() as usize)? as u64,
            ),
            snapshot_timeout: Duration::from_secs(number(
                "BOOK_SNAPSHOT_TIMEOUT_SECONDS",
                defaults.snapshot_timeout.as_secs() as usize,
            )? as u64),
        })
    }
}

#[derive(serde::Serialize)]
struct ResourceStats {
    read_bytes: u64,
    read_time_us: u64,
    process_time_us: u64,
    lock_wait_us: u64,
    source_gaps: u64,
    snapshot_failures: u64,
    pending_file_bytes: u64,
    dirty_files: usize,
    snapshot_clone_ms: u64,
    snapshot_parse_ms: u64,
    snapshot_reconcile_ms: u64,
}

impl ResourceStats {
    const EMPTY: Self = Self {
        read_bytes: 0,
        read_time_us: 0,
        process_time_us: 0,
        lock_wait_us: 0,
        source_gaps: 0,
        snapshot_failures: 0,
        pending_file_bytes: 0,
        dirty_files: 0,
        snapshot_clone_ms: 0,
        snapshot_parse_ms: 0,
        snapshot_reconcile_ms: 0,
    };
}

pub(crate) struct OrderBookListener {
    stats: ResourceStats,
    limits: ResourceLimits,
    recovery_epoch: u64,
    snapshot_cache_bytes: usize,
    snapshot_cache_started: Option<Instant>,
    ignore_spot: bool,
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
            stats: ResourceStats::EMPTY,
            limits: ResourceLimits::DEFAULT,
            recovery_epoch: 0,
            snapshot_cache_bytes: 0,
            snapshot_cache_started: None,
            ignore_spot,
            order_book_state: None,
            last_fill: None,
            fetched_snapshot_cache: None,
            internal_message_tx,
            order_diff_cache: BatchQueue::new(),
            order_status_cache: BatchQueue::new(),
        }
    }

    fn fence(&mut self, reason: &str) {
        error!("Book stream fenced; authoritative resnapshot required: {reason}");
        self.stats.source_gaps += 1;
        self.recovery_epoch = self.recovery_epoch.wrapping_add(1);
        self.order_book_state = None;
        self.order_status_cache = BatchQueue::new();
        self.order_diff_cache = BatchQueue::new();
        self.fetched_snapshot_cache = None;
        self.snapshot_cache_started = None;
        self.snapshot_cache_bytes = 0;
        if let Some(tx) = &self.internal_message_tx {
            let _unused = tx.send(Arc::new(InternalMessage::Gap));
        }
    }

    pub(crate) fn resource_status(&self) -> serde_json::Value {
        serde_json::json!({
            "ready": self.is_ready(),
            "source_height": self.order_book_state.as_ref().map(OrderBookState::height),
            "stats": self.stats,
            "unmatched_status_bytes": self.order_status_cache.bytes(),
            "unmatched_diff_bytes": self.order_diff_cache.bytes(),
            "snapshot_cache_bytes": self.snapshot_cache_bytes,
            "snapshot_cache_active": self.fetched_snapshot_cache.is_some(),
            "recovery_epoch": self.recovery_epoch,
        })
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
                self.order_status_cache.push(batch, self.limits)?;
            }
            EventBatch::BookDiffs(batch) => {
                self.order_diff_cache.push(batch, self.limits)?;
            }
            EventBatch::Fills(batch) => {
                if self.last_fill.is_none_or(|height| height < batch.block_number()) {
                    self.last_fill = Some(batch.block_number());
                    // send fill updates if we received a new update
                    if let Some(tx) = &self.internal_message_tx {
                        let snapshot = Arc::new(InternalMessage::Fills { batch });
                        let _unused = tx.send(snapshot);
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
                let pair_bytes = order_statuses.wire_bytes().saturating_add(order_diffs.wire_bytes());
                if self.fetched_snapshot_cache.as_ref().is_some_and(|cache| {
                    cache.len() >= self.limits.queue_heights
                        || self.snapshot_cache_bytes.saturating_add(pair_bytes) > self.limits.queue_bytes
                }) {
                    // The live state remains valid. Discard only this validation
                    // attempt; its sole owner must finish before another starts.
                    self.recovery_epoch = self.recovery_epoch.wrapping_add(1);
                    self.fetched_snapshot_cache = None;
                    self.snapshot_cache_started = None;
                    self.snapshot_cache_bytes = 0;
                    warn!("Snapshot validation cache limit reached; skipping this validation attempt");
                }
                if let Some(cache) = &mut self.fetched_snapshot_cache {
                    self.snapshot_cache_bytes += pair_bytes;
                    cache.push_back((order_statuses.clone(), order_diffs.clone()));
                }
                if let Some(tx) = &self.internal_message_tx {
                    let updates = Arc::new(InternalMessage::L4BookUpdates {
                        diff_batch: order_diffs,
                        status_batch: order_statuses,
                    });
                    let _unused = tx.send(updates);
                }
            }
        }
        Ok(())
    }

    fn begin_caching(&mut self) {
        self.snapshot_cache_started = Some(Instant::now());
        self.snapshot_cache_bytes = 0;
        self.fetched_snapshot_cache = Some(VecDeque::new());
    }

    // tkae the cached updates and stop collecting updates
    fn take_cache(&mut self) -> VecDeque<(Batch<NodeDataOrderStatus>, Batch<NodeDataOrderDiff>)> {
        self.snapshot_cache_started = None;
        self.snapshot_cache_bytes = 0;
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

    fn finish_snapshot(
        &mut self,
        epoch: u64,
        state: Option<OrderBookState>,
        snapshot: Snapshots<InnerL4Order>,
        height: u64,
    ) -> Result<()> {
        if self.recovery_epoch != epoch {
            return Ok(());
        }
        let cache = self.take_cache();
        let started = Instant::now();
        let result = self.reconcile_snapshot(state, snapshot, height, cache);
        self.stats.snapshot_reconcile_ms = started.elapsed().as_millis() as u64;
        result
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
            if let Some(tx) = &self.internal_message_tx {
                let _unused = tx.send(Arc::new(InternalMessage::Gap));
            }
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
    fn process_data(&mut self, data: String, event_source: EventSource) -> Result<()> {
        for record in data.split_inclusive('\n') {
            if !record.ends_with('\n') {
                return Err("incomplete record escaped the bounded file cursor".into());
            }
            let line = record.trim_end_matches(['\r', '\n']);
            if line.is_empty() {
                continue;
            }
            let res = match event_source {
                EventSource::Fills => serde_json::from_str::<Batch<NodeDataFill>>(line).map(|batch| {
                    let height = batch.block_number();
                    (height, EventBatch::Fills(batch.with_wire_bytes(line.len())))
                }),
                EventSource::OrderStatuses => serde_json::from_str(line).map(|batch: Batch<NodeDataOrderStatus>| {
                    (batch.block_number(), EventBatch::Orders(batch.with_wire_bytes(line.len())))
                }),
                EventSource::OrderDiffs => serde_json::from_str(line).map(|batch: Batch<NodeDataOrderDiff>| {
                    (batch.block_number(), EventBatch::BookDiffs(batch.with_wire_bytes(line.len())))
                }),
            };
            let (height, event_batch) = match res {
                Ok(data) => data,
                Err(err) => {
                    error!(
                        "{event_source} serialization error {err}, height: {:?}, line: {:?}",
                        self.order_book_state.as_ref().map(OrderBookState::height),
                        line.get(..100).unwrap_or(line),
                    );
                    return Err(format!("malformed {event_source} record: {err}").into());
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
    Gap,
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
    fn malformed_and_incomplete_records_require_recovery() {
        let mut listener = OrderBookListener::new(None, false);
        assert!(listener.process_data("not-json\n".into(), EventSource::OrderStatuses).is_err());
        assert!(listener.process_data("unfinished".into(), EventSource::OrderStatuses).is_err());
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
    fn empty_batch<T: serde::de::DeserializeOwned>(height: u64) -> Batch<T> {
        serde_json::from_value::<Batch<T>>(serde_json::json!({
            "local_time":"2026-09-11T00:00:00", "block_time":"2026-09-11T00:00:00",
            "block_number":height, "events":[]
        }))
        .unwrap()
        .with_wire_bytes(10)
    }

    #[test]
    fn old_snapshot_cannot_reopen_a_fenced_stream() {
        let mut listener = OrderBookListener::new(None, false);
        let old_epoch = listener.recovery_epoch;
        listener.fence("test missing block");
        listener.finish_snapshot(old_epoch, None, Snapshots::new(HashMap::new()), 100).unwrap();
        assert!(!listener.is_ready());
        listener.finish_snapshot(listener.recovery_epoch, None, Snapshots::new(HashMap::new()), 101).unwrap();
        assert!(listener.is_ready());
    }

    #[test]
    fn validation_cache_overflow_drops_only_validation_and_preserves_live_progress() {
        let mut listener = OrderBookListener::new(None, false);
        listener.limits.queue_bytes = 30;
        listener.order_book_state =
            Some(OrderBookState::from_snapshot(Snapshots::new(HashMap::new()), 100, 0, true, false));
        let old_epoch = listener.recovery_epoch;
        listener.begin_caching();
        for height in 101..=102 {
            listener.receive_batch(EventBatch::Orders(empty_batch(height))).unwrap();
            listener.receive_batch(EventBatch::BookDiffs(empty_batch(height))).unwrap();
        }
        assert!(listener.is_ready());
        assert_eq!(listener.order_book_state.as_ref().unwrap().height(), 102);
        assert_eq!(listener.snapshot_cache_bytes, 0);
        assert!(listener.fetched_snapshot_cache.is_none());
        assert_ne!(listener.recovery_epoch, old_epoch);
        listener.finish_snapshot(old_epoch, None, Snapshots::new(HashMap::new()), 100).unwrap();
        assert_eq!(listener.order_book_state.as_ref().unwrap().height(), 102);
    }

    #[test]
    fn a_fence_closes_consumers_and_clears_unmatched_work() {
        let (tx, mut rx) = tokio::sync::broadcast::channel(8);
        let mut listener = OrderBookListener::new(Some(tx), false);
        listener.receive_batch(EventBatch::Orders(empty_batch(101))).unwrap();
        listener.fence("test");
        assert!(matches!(rx.try_recv().unwrap().as_ref(), InternalMessage::Gap));
        assert!(listener.order_status_cache.front().is_none());
        assert!(!listener.is_ready());
    }

    #[test]
    fn fills_are_deduplicated_without_detached_tasks() {
        let (tx, mut rx) = tokio::sync::broadcast::channel(8);
        let mut listener = OrderBookListener::new(Some(tx), false);
        listener.receive_batch(EventBatch::Fills(empty_batch(101))).unwrap();
        listener.receive_batch(EventBatch::Fills(empty_batch(101))).unwrap();
        assert!(matches!(rx.try_recv().unwrap().as_ref(), InternalMessage::Fills { .. }));
        assert!(rx.try_recv().is_err());
    }
    #[tokio::test]
    async fn unavailable_validation_does_not_discard_healthy_live_state() {
        let app = axum::Router::new()
            .route("/info", axum::routing::post(|| async { axum::http::StatusCode::SERVICE_UNAVAILABLE }))
            .route("/hang", axum::routing::post(|| async { std::future::pending::<axum::http::StatusCode>().await }));
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", socket.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(socket, app).await.unwrap();
        });
        let listener = Arc::new(Mutex::new(OrderBookListener::new(None, false)));
        {
            let mut state = listener.lock().await;
            state.order_book_state =
                Some(OrderBookState::from_snapshot(Snapshots::new(HashMap::new()), 100, 1, true, false));
            state.limits.snapshot_timeout = Duration::from_millis(25);
        }
        for path in ["info", "hang"] {
            tokio::time::timeout(
                Duration::from_secs(2),
                fetch_snapshot(std::env::temp_dir(), listener.clone(), format!("{base}/{path}")),
            )
            .await
            .unwrap()
            .unwrap();
        }
        let state = listener.lock().await;
        assert!(state.is_ready());
        assert!(state.fetched_snapshot_cache.is_none());
        assert_eq!(state.stats.source_gaps, 0);
        assert_eq!(state.stats.snapshot_failures, 2);
        server.abort();
        let _unused = server.await;
    }
}

#[cfg(all(test, target_os = "linux"))]
mod recovery_integration_tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
    use tokio::{io::AsyncWriteExt, sync::Notify};

    #[tokio::test]
    async fn long_snapshot_and_notification_burst_recover_without_overlapping_jobs() {
        let dir = std::env::temp_dir().join(format!("tt1506-recovery-{}", std::process::id()));
        for source in [EventSource::OrderStatuses, EventSource::OrderDiffs, EventSource::Fills] {
            fs::create_dir_all(source.event_source_dir(&dir).join("hourly/20260911")).unwrap();
        }
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let requests = Arc::new(AtomicUsize::new(0));
        let height = Arc::new(AtomicU64::new(100));
        let app = axum::Router::new().route(
            "/info",
            axum::routing::post({
                let entered = entered.clone();
                let release = release.clone();
                let requests = requests.clone();
                let height = height.clone();
                move |axum::Json(request): axum::Json<serde_json::Value>| {
                    let entered = entered.clone();
                    let release = release.clone();
                    let requests = requests.clone();
                    let height = height.clone();
                    async move {
                        let captured = height.load(AtomicOrdering::SeqCst);
                        if requests.fetch_add(1, AtomicOrdering::SeqCst) == 0 {
                            entered.notify_one();
                            release.notified().await;
                        }
                        tokio::fs::write(request["outPath"].as_str().unwrap(), format!("[{captured}, []]"))
                            .await
                            .unwrap();
                        axum::Json(serde_json::json!({}))
                    }
                }
            }),
        );
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/info", socket.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(socket, app).await.unwrap();
        });
        let listener = Arc::new(Mutex::new(OrderBookListener::new(None, false)));
        let limits = ResourceLimits {
            turn_bytes: 128,
            record_bytes: 4096,
            queue_bytes: 2048,
            queue_heights: 8,
            ..ResourceLimits::DEFAULT
        };
        let task = tokio::spawn(hl_listen_with_source(listener.clone(), dir.clone(), url, limits));
        tokio::time::timeout(Duration::from_secs(10), entered.notified()).await.unwrap();
        let mut records = String::new();
        for number in 101..=164 {
            records += &format!(
                "{{\"local_time\":\"2026-09-11T00:00:00\",\"block_time\":\"2026-09-11T00:00:00\",\"block_number\":{number},\"events\":[]}}\n"
            );
        }
        for source in [EventSource::OrderStatuses, EventSource::OrderDiffs] {
            tokio::fs::write(source.event_source_dir(&dir).join("hourly/20260911/0"), &records).await.unwrap();
        }
        height.store(164, AtomicOrdering::SeqCst);
        // Longer than the regular snapshot interval: still only one owner.
        sleep(Duration::from_secs(12)).await;
        assert_eq!(requests.load(AtomicOrdering::SeqCst), 1);
        {
            let state = listener.lock().await;
            assert!(state.stats.source_gaps > 0);
            assert!(!state.is_ready());
            assert!(state.order_status_cache.bytes() <= limits.queue_bytes);
            assert!(state.order_diff_cache.bytes() <= limits.queue_bytes);
        }
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if listener.lock().await.is_ready() {
                    break;
                }
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(requests.load(AtomicOrdering::SeqCst), 2);
        assert_eq!(listener.lock().await.order_book_state.as_ref().unwrap().height(), 164);
        let record = "{\"local_time\":\"2026-09-11T00:00:01\",\"block_time\":\"2026-09-11T00:00:01\",\"block_number\":165,\"events\":[]}\n";
        for source in [EventSource::OrderStatuses, EventSource::OrderDiffs] {
            let mut file = tokio::fs::OpenOptions::new()
                .append(true)
                .open(source.event_source_dir(&dir).join("hourly/20260911/0"))
                .await
                .unwrap();
            file.write_all(record.as_bytes()).await.unwrap();
            file.flush().await.unwrap();
        }
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if listener.lock().await.order_book_state.as_ref().is_some_and(|state| state.height() == 165) {
                    break;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert!(!task.is_finished());
        task.abort();
        let _ = task.await;
        server.abort();
        let _ = server.await;
        fs::remove_dir_all(dir).unwrap();
    }
}
