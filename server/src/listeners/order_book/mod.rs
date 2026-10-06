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
        subscription::CoinScope,
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
use utils::{BatchQueue, EventBatch, SnapshotConsistency, process_rmp_file, validate_snapshot_consistency};

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
    use ingestion::{DirtyFiles, FileCursor, evict_retired_cursor};
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
    let first_snapshot = Instant::now() + Duration::from_secs(5);
    let mut ticker = interval_at(first_snapshot, limits.snapshot_interval);
    let mut recovery_retry_at = first_snapshot;
    let mut maintenance = interval_at(Instant::now() + Duration::from_secs(1), Duration::from_secs(1));
    maintenance.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = maintenance.tick() => {
                cursors.retain(|path, cursor| path.exists() || !cursor.drained());
                let pending_file_bytes = cursors.values().map(FileCursor::backlog_bytes).sum();
                // Notifications are only hints. Periodically revisit retained
                // offsets so a missed/coalesced final notification cannot strand data.
                let dirty_files = {
                    let mut queue = dirty.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                    for (path, cursor) in &cursors {
                        if cursor.needs_attention(path) { queue.push(path.clone(), false); }
                    }
                    queue.len()
                };
                if dirty_files > 0 { wake.notify_one(); }
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
                let needs_recovery = !state.is_ready();
                drop(state);
                // Routine full-node snapshot audits can be spaced out without
                // making a fenced book wait for the next healthy audit.
                if needs_recovery && snapshots.is_empty() && Instant::now() >= recovery_retry_at {
                    snapshots.spawn(fetch_snapshot(dir.clone(), listener.clone(), info_url.clone()));
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
                        if !path.is_file() {
                            if cursors.get(&path).is_some_and(|cursor| !cursor.drained()) {
                                listener.lock().await.fence("source file removed with unread work");
                            }
                            cursors.remove(&path);
                        }
                        if path.is_file() {
                            if !cursors.contains_key(&path) {
                                let tracking_source = cursors.keys().any(|known| known.starts_with(root));
                                cursors.retain(|path, cursor| path.exists() || !cursor.drained());
                                if cursors.len() >= limits.dirty_files {
                                    evict_retired_cursor(&mut cursors, root, &path);
                                }
                                if cursors.len() >= limits.dirty_files {
                                    listener.lock().await.fence("file cursor capacity exceeded");
                                    cursors.clear();
                                }
                                // At initial attachment, start at the tail and obtain a
                                // fresh authoritative snapshot. A later rotated file starts
                                // at byte zero even if its create notification was coalesced.
                                let from_end = !created && !tracking_source;
                                if from_end {
                                    listener.lock().await.fence("attaching to a file without a known offset");
                                }
                                match FileCursor::open(&path, from_end, limits.record_bytes).map(|cursor| cursor.with_max_age(limits.queue_age)) {
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
                                        if let Ok(cursor) = FileCursor::open(&path, true, limits.record_bytes).map(|cursor| cursor.with_max_age(limits.queue_age)) {
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
                recovery_retry_at = Instant::now() + Duration::from_secs(10);
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
    let (epoch, state, timeout, request_started_at_ms, scope) = {
        let mut listener = listener.lock().await;
        let epoch = listener.recovery_epoch;
        listener.begin_caching();
        let cloning = Instant::now();
        let state = listener.clone_state();
        listener.stats.snapshot_clone_ms = cloning.elapsed().as_millis() as u64;
        listener.stats.snapshot_requests += 1;
        let request_started_at_ms = chrono::Utc::now().timestamp_millis();
        listener.stats.snapshot_request_inflight_started_at_ms = request_started_at_ms;
        (epoch, state, listener.limits.snapshot_timeout, request_started_at_ms, listener.scope.clone())
    };
    let request_started = Instant::now();
    let response = process_rmp_file(&dir, &info_url, timeout).await;
    let request_ms = request_started.elapsed().as_millis() as u64;
    let completed_at_ms = chrono::Utc::now().timestamp_millis();
    {
        let mut listener = listener.lock().await;
        listener.stats.snapshot_request_started_at_ms = request_started_at_ms;
        listener.stats.snapshot_request_ms = request_ms;
        listener.stats.snapshot_request_completed_at_ms = completed_at_ms;
        listener.stats.snapshot_request_inflight_started_at_ms = 0;
    }
    match response {
        Ok(output_fln) => {
            if listener.lock().await.recovery_epoch != epoch {
                return Ok(());
            }
            let parsing = Instant::now();
            let snapshot =
                load_snapshots_from_json::<InnerL4Order, (Address, L4Order)>(output_fln.path(), &scope).await;
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
    snapshot_interval: Duration,
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
        snapshot_interval: Duration::from_secs(10),
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
        let snapshot_interval =
            number("BOOK_SNAPSHOT_INTERVAL_SECONDS", defaults.snapshot_interval.as_secs() as usize)?;
        if !(10..=300).contains(&snapshot_interval) {
            return Err("BOOK_SNAPSHOT_INTERVAL_SECONDS must be between 10 and 300".into());
        }
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
            snapshot_interval: Duration::from_secs(snapshot_interval as u64),
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
    snapshot_reloads: u64,
    empty_book_refreshes: u64,
    last_snapshot_reload_reason: Option<String>,
    pending_file_bytes: u64,
    dirty_files: usize,
    snapshot_clone_ms: u64,
    snapshot_parse_ms: u64,
    snapshot_reconcile_ms: u64,
    snapshot_requests: u64,
    snapshot_request_started_at_ms: i64,
    snapshot_request_completed_at_ms: i64,
    snapshot_request_ms: u64,
    snapshot_request_inflight_started_at_ms: i64,
}

impl ResourceStats {
    const EMPTY: Self = Self {
        read_bytes: 0,
        read_time_us: 0,
        process_time_us: 0,
        lock_wait_us: 0,
        source_gaps: 0,
        snapshot_failures: 0,
        snapshot_reloads: 0,
        empty_book_refreshes: 0,
        last_snapshot_reload_reason: None,
        pending_file_bytes: 0,
        dirty_files: 0,
        snapshot_clone_ms: 0,
        snapshot_parse_ms: 0,
        snapshot_reconcile_ms: 0,
        snapshot_requests: 0,
        snapshot_request_started_at_ms: 0,
        snapshot_request_completed_at_ms: 0,
        snapshot_request_ms: 0,
        snapshot_request_inflight_started_at_ms: 0,
    };
}

pub(crate) struct OrderBookListener {
    stats: ResourceStats,
    limits: ResourceLimits,
    recovery_epoch: u64,
    snapshot_cache_bytes: usize,
    snapshot_cache_started: Option<Instant>,
    ignore_spot: bool,
    // Both the live state and every validation snapshot are restricted to this
    // scope at parse time, so consistency checks compare like with like.
    scope: CoinScope,
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
    pub(crate) const fn new(
        internal_message_tx: Option<Sender<Arc<InternalMessage>>>,
        ignore_spot: bool,
        scope: CoinScope,
    ) -> Self {
        Self {
            stats: ResourceStats::EMPTY,
            limits: ResourceLimits::DEFAULT,
            recovery_epoch: 0,
            snapshot_cache_bytes: 0,
            snapshot_cache_started: None,
            ignore_spot,
            scope,
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
            "coin_scope": self.scope.describe(),
            "book_count": self.order_book_state.as_ref().map(OrderBookState::book_count),
            "stats": self.stats,
            "unmatched_status_bytes": self.order_status_cache.bytes(),
            "unmatched_diff_bytes": self.order_diff_cache.bytes(),
            "snapshot_cache_bytes": self.snapshot_cache_bytes,
            "snapshot_cache_active": self.fetched_snapshot_cache.is_some(),
            "recovery_epoch": self.recovery_epoch,
            "snapshot_interval_seconds": self.limits.snapshot_interval.as_secs(),
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
                if self.order_book_state.as_ref().is_some_and(|book| order_statuses.block_number() <= book.height()) {
                    return Ok(());
                }
                self.order_book_state
                    .as_mut()
                    .map(|book| book.apply_updates(order_statuses.clone(), order_diffs.clone()))
                    .transpose()?;
                let pair_bytes = order_statuses
                    .wire_bytes()
                    .checked_add(order_diffs.wire_bytes())
                    .ok_or("paired batch byte counter overflow")?;
                let next_cache_bytes = self.snapshot_cache_bytes.checked_add(pair_bytes);
                if self.fetched_snapshot_cache.as_ref().is_some_and(|cache| {
                    cache.len() >= self.limits.queue_heights
                        || next_cache_bytes.is_none_or(|bytes| bytes > self.limits.queue_bytes)
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
                    self.snapshot_cache_bytes = next_cache_bytes.ok_or("validation byte counter overflow")?;
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
        let baseline = state.compute_snapshot();
        let consistency = validate_snapshot_consistency(&baseline.snapshot, &expected_snapshot, self.ignore_spot);
        if matches!(consistency, Ok(SnapshotConsistency::Equal)) {
            return Ok(());
        }
        let empty_additions = matches!(consistency, Ok(SnapshotConsistency::EmptyBooksAdded));
        if let Err(err) = &consistency {
            self.stats.last_snapshot_reload_reason = Some(err.to_string().chars().take(512).collect());
            warn!("Snapshot diverged ({err}); reloading authoritative node snapshot");
        }
        // Empty additions preserve every existing order at the same proven height.
        // Retain its source time; replay later cached updates before committing.
        let time = if empty_additions { baseline.time } else { 0 };
        let mut recovered = OrderBookState::from_snapshot(expected_snapshot, height, time, true, self.ignore_spot);
        while let Some((order_statuses, order_diffs)) = cache.pop_front() {
            recovered.apply_updates(order_statuses, order_diffs)?;
        }
        self.order_book_state = Some(recovered);
        if empty_additions {
            self.stats.empty_book_refreshes += 1;
            if let Some((time, height, l2_snapshots)) = self.l2_snapshots(true) {
                if let Some(tx) = &self.internal_message_tx {
                    let _unused = tx.send(Arc::new(InternalMessage::Snapshot { l2_snapshots, time, height }));
                }
            }
        } else {
            self.stats.snapshot_reloads += 1;
            if let Some(tx) = &self.internal_message_tx {
                let _unused = tx.send(Arc::new(InternalMessage::Gap));
            }
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
                EventSource::Fills => Batch::<NodeDataFill>::parse_in_scope(line, &self.scope)
                    .map(|batch| (batch.block_number(), EventBatch::Fills(batch))),
                EventSource::OrderStatuses => Batch::<NodeDataOrderStatus>::parse_in_scope(line, &self.scope)
                    .map(|batch| (batch.block_number(), EventBatch::Orders(batch))),
                EventSource::OrderDiffs => Batch::<NodeDataOrderDiff>::parse_in_scope(line, &self.scope)
                    .map(|batch| (batch.block_number(), EventBatch::BookDiffs(batch))),
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
    use crate::{order_book::multi_book::load_snapshots_from_str, types::subscription::Subscription};

    #[test]
    fn adds_empty_market_without_resetting_existing_consumers() -> Result<()> {
        let (tx, mut rx) = tokio::sync::broadcast::channel(16);
        let mut listener = OrderBookListener::new(Some(tx), false, CoinScope::all());
        listener.order_book_state =
            Some(OrderBookState::from_snapshot(Snapshots::new(HashMap::new()), 100, 1234, true, false));
        let (_, expected) = load_snapshots_from_str::<InnerL4Order, (Address, L4Order)>(
            r#"[100, [["NEW", [[], []]]]]"#,
            &CoinScope::all(),
        )?;

        listener.reconcile_snapshot(listener.clone_state(), expected, 100, VecDeque::new())?;

        assert!(listener.universe().contains(&Coin::new("NEW")));
        assert_eq!(listener.compute_snapshot().unwrap().time, 1234);
        assert_eq!(listener.stats.empty_book_refreshes, 1);
        assert_eq!(listener.stats.snapshot_reloads, 0);
        let update = rx.try_recv().unwrap();
        let InternalMessage::Snapshot { l2_snapshots, time, height } = update.as_ref() else {
            panic!("empty additions must publish a snapshot, not a gap");
        };
        assert!(l2_snapshots.as_ref().contains_key(&Coin::new("NEW")));
        assert_eq!((*height, *time), (100, 1234));
        assert!(rx.try_recv().is_err());
        Ok(())
    }

    #[test]
    fn empty_addition_replays_updates_before_preserving_consumers() -> Result<()> {
        let (tx, mut rx) = tokio::sync::broadcast::channel(16);
        let mut listener = OrderBookListener::new(Some(tx), false, CoinScope::all());
        let baseline = OrderBookState::from_snapshot(Snapshots::new(HashMap::new()), 100, 1234, true, false);
        let record =
            r#"{"local_time":"2026-09-11T00:00:01","block_time":"2026-09-11T00:00:01","block_number":101,"events":[]}"#;
        let statuses: Batch<NodeDataOrderStatus> = serde_json::from_str(record)?;
        let diffs: Batch<NodeDataOrderDiff> = serde_json::from_str(record)?;
        let mut live = baseline.clone();
        live.apply_updates(statuses.clone(), diffs.clone())?;
        let expected_time = live.compute_snapshot().time;
        listener.order_book_state = Some(live);
        let (_, expected) = load_snapshots_from_str::<InnerL4Order, (Address, L4Order)>(
            r#"[100, [["NEW", [[], []]]]]"#,
            &CoinScope::all(),
        )?;
        listener.reconcile_snapshot(Some(baseline), expected, 100, VecDeque::from([(statuses, diffs)]))?;
        let result = listener.compute_snapshot().unwrap();
        assert_eq!((result.height, result.time), (101, expected_time));
        assert!(listener.universe().contains(&Coin::new("NEW")));
        assert_eq!(listener.stats.empty_book_refreshes, 1);
        let update = rx.try_recv().unwrap();
        let InternalMessage::Snapshot { l2_snapshots, time, height } = update.as_ref() else {
            panic!("empty additions must publish a snapshot, not a gap");
        };
        assert!(l2_snapshots.as_ref().contains_key(&Coin::new("NEW")));
        assert_eq!((*height, *time), (101, expected_time));
        assert!(rx.try_recv().is_err());
        Ok(())
    }

    #[test]
    fn ignores_snapshot_older_than_live_state() -> Result<()> {
        let mut listener = OrderBookListener::new(None, false, CoinScope::all());
        listener.order_book_state =
            Some(OrderBookState::from_snapshot(Snapshots::new(HashMap::new()), 101, 0, true, false));

        listener.reconcile_snapshot(listener.clone_state(), Snapshots::new(HashMap::new()), 100, VecDeque::new())?;

        assert!(listener.is_ready());
        Ok(())
    }

    #[test]
    fn malformed_and_incomplete_records_require_recovery() {
        let mut listener = OrderBookListener::new(None, false, CoinScope::all());
        assert!(listener.process_data("not-json\n".into(), EventSource::OrderStatuses).is_err());
        assert!(listener.process_data("unfinished".into(), EventSource::OrderStatuses).is_err());
    }

    #[test]
    fn snapshot_ahead_of_cache_is_retried_later() -> Result<()> {
        let mut listener = OrderBookListener::new(None, false, CoinScope::all());
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
        let mut listener = OrderBookListener::new(None, false, CoinScope::all());
        let old_epoch = listener.recovery_epoch;
        listener.fence("test missing block");
        listener.finish_snapshot(old_epoch, None, Snapshots::new(HashMap::new()), 100).unwrap();
        assert!(!listener.is_ready());
        listener.finish_snapshot(listener.recovery_epoch, None, Snapshots::new(HashMap::new()), 101).unwrap();
        assert!(listener.is_ready());
    }

    #[test]
    fn validation_cache_overflow_drops_only_validation_and_preserves_live_progress() {
        let mut listener = OrderBookListener::new(None, false, CoinScope::all());
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

    fn scoped_order(coin: &str, oid: u64, side: &str, px: &str, sz: &str) -> serde_json::Value {
        serde_json::json!({
            "coin": coin, "side": side, "limitPx": px, "sz": sz, "oid": oid, "timestamp": 1000,
            "triggerCondition": "N/A", "isTrigger": false, "triggerPx": "0.0", "isPositionTpsl": false,
            "reduceOnly": false, "orderType": "Limit", "tif": "Gtc", "cloid": null
        })
    }

    fn scoped_book(coin: &str, bids: &[serde_json::Value], asks: &[serde_json::Value]) -> serde_json::Value {
        let side = |orders: &[serde_json::Value]| {
            orders.iter().map(|order| serde_json::json!([Address::ZERO, order])).collect::<Vec<_>>()
        };
        serde_json::json!([coin, [side(bids), side(asks)]])
    }

    fn scoped_block(height: u64, events: serde_json::Value) -> String {
        format!(
            "{}\n",
            serde_json::json!({
                "local_time":"2026-09-11T00:00:01", "block_time":"2026-09-11T00:00:01",
                "block_number":height, "events":events
            })
        )
    }

    /// Node snapshot at 100, one mixed block at 101, and the node snapshot at 101.
    fn mixed_market_fixture() -> (String, String, String, String) {
        let before = serde_json::json!([
            100,
            [
                scoped_book("BTC", &[scoped_order("BTC", 1, "B", "100.0", "1.0")], &[]),
                scoped_book("#10", &[scoped_order("#10", 2, "B", "0.5", "10.0")], &[]),
                scoped_book("@1", &[], &[scoped_order("@1", 3, "A", "30.0", "2.0")]),
            ]
        ]);
        let open = |order: serde_json::Value| serde_json::json!({"time":"2026-09-11T00:00:01", "user":Address::ZERO, "status":"open", "order":order});
        let statuses = scoped_block(
            101,
            serde_json::json!([
                open(scoped_order("BTC", 4, "A", "101.0", "2.0")),
                open(scoped_order("#10", 5, "A", "0.6", "3.0")),
            ]),
        );
        let diff = |coin: &str, oid: u64, px: &str, diff: serde_json::Value| serde_json::json!({"user":Address::ZERO, "oid":oid, "px":px, "coin":coin, "raw_book_diff":diff});
        let diffs = scoped_block(
            101,
            serde_json::json!([
                diff("BTC", 4, "101.0", serde_json::json!({"new":{"sz":"2.0"}})),
                diff("#10", 5, "0.6", serde_json::json!({"new":{"sz":"3.0"}})),
                diff("BTC", 1, "100.0", serde_json::json!("remove")),
                diff("#10", 2, "0.5", serde_json::json!({"update":{"origSz":"10.0","newSz":"7.0"}})),
            ]),
        );
        let after = serde_json::json!([
            101,
            [
                scoped_book("BTC", &[], &[scoped_order("BTC", 4, "A", "101.0", "2.0")]),
                scoped_book(
                    "#10",
                    &[scoped_order("#10", 2, "B", "0.5", "7.0")],
                    &[scoped_order("#10", 5, "A", "0.6", "3.0")]
                ),
                scoped_book("@1", &[], &[scoped_order("@1", 3, "A", "30.0", "2.0")]),
            ]
        ]);
        (before.to_string(), statuses, diffs, after.to_string())
    }

    fn mixed_market_listener(scope: &CoinScope) -> Result<OrderBookListener> {
        let (before, statuses, diffs, _) = mixed_market_fixture();
        // Production runs with ignore_spot = true.
        let mut listener = OrderBookListener::new(None, true, scope.clone());
        let (height, snapshot) = load_snapshots_from_str::<InnerL4Order, (Address, L4Order)>(&before, scope)?;
        listener.finish_snapshot(listener.recovery_epoch, None, snapshot, height)?;
        listener.process_data(statuses, EventSource::OrderStatuses)?;
        listener.process_data(diffs, EventSource::OrderDiffs)?;
        assert_eq!(listener.order_book_state.as_ref().unwrap().height(), 101);
        Ok(listener)
    }

    #[test]
    fn hip4_scope_skips_other_markets_and_keeps_hip4_books_identical() -> Result<()> {
        let mut full = mixed_market_listener(&CoinScope::all())?;
        let mut hip4 = mixed_market_listener(&CoinScope::default())?;
        assert!(full.universe().contains(&Coin::new("BTC")));
        assert_eq!(hip4.universe(), HashSet::from([Coin::new("#10")]));
        assert_eq!(hip4.order_book_state.as_ref().unwrap().book_count(), 1);

        let (full_time, full_height, full_l2) = full.compute_l2_snapshot().unwrap();
        let (hip4_time, hip4_height, hip4_l2) = hip4.compute_l2_snapshot().unwrap();
        assert_eq!((hip4_time, hip4_height), (full_time, full_height));
        assert_eq!(hip4_l2.as_ref().len(), 1);
        let coin = Coin::new("#10");
        let views = &hip4_l2.as_ref()[&coin];
        assert_eq!(views.len(), full_l2.as_ref()[&coin].len());
        for (params, view) in views.as_ref() {
            assert_eq!(
                view.clone().export_inner_snapshot(),
                full_l2.as_ref()[&coin][params].clone().export_inner_snapshot()
            );
        }
        let full_l4 = full.compute_snapshot().unwrap().snapshot;
        let hip4_l4 = hip4.compute_snapshot().unwrap().snapshot;
        assert_eq!(hip4_l4.as_ref()[&coin].as_ref(), full_l4.as_ref()[&coin].as_ref());

        let universe: HashSet<String> = hip4.universe().into_iter().map(|coin| coin.value()).collect();
        let l2 =
            |coin: &str| Subscription::L2Book { coin: coin.into(), n_sig_figs: None, n_levels: None, mantissa: None };
        assert!(l2("#10").validate(&universe));
        assert!(!l2("BTC").validate(&universe));
        assert!(!Subscription::Trades { coin: "BTC".into() }.validate(&universe));
        assert!(!Subscription::L4Book { coin: "BTC".into() }.validate(&universe));
        Ok(())
    }

    #[tokio::test]
    async fn filtered_snapshot_audit_validates_without_reloading() {
        let (_, _, _, after) = mixed_market_fixture();
        let app = axum::Router::new().route(
            "/info",
            axum::routing::post(move |axum::Json(request): axum::Json<serde_json::Value>| {
                let after = after.clone();
                async move {
                    tokio::fs::write(request["outPath"].as_str().unwrap(), after).await.unwrap();
                    axum::Json(serde_json::json!({}))
                }
            }),
        );
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/info", socket.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(socket, app).await.unwrap();
        });
        for scope in [CoinScope::default(), CoinScope::all()] {
            let (tx, mut rx) = tokio::sync::broadcast::channel(16);
            let mut listener = mixed_market_listener(&scope).unwrap();
            listener.internal_message_tx = Some(tx);
            let listener = Arc::new(Mutex::new(listener));
            fetch_snapshot(std::env::temp_dir(), listener.clone(), url.clone()).await.unwrap();
            let state = listener.lock().await;
            assert!(state.is_ready(), "{}", scope.describe());
            assert_eq!(state.stats.snapshot_reloads, 0, "{:?}", state.stats.last_snapshot_reload_reason);
            assert_eq!(state.stats.empty_book_refreshes, 0);
            assert_eq!(state.stats.source_gaps, 0);
            assert!(rx.try_recv().is_err(), "a passing audit must not publish a gap or snapshot");
        }
        server.abort();
        let _unused = server.await;
    }

    #[test]
    #[ignore = "manual resource benchmark; run with --release --ignored --nocapture"]
    fn benchmark_hip4_scope() {
        fn rss_mib() -> u64 {
            std::fs::read_to_string("/proc/self/statm")
                .ok()
                .and_then(|statm| statm.split_whitespace().nth(1)?.parse::<u64>().ok())
                .map_or(0, |pages| pages * 4096 / (1024 * 1024))
        }
        // Synthetic mainnet-like shape: 400 large books, 200 small HIP-4 books.
        let mut books = Vec::new();
        let mut oid = 0;
        for (coins, prefix, orders) in [(400, "C", 2500), (200, "#", 20)] {
            for coin in 0..coins {
                let coin = format!("{prefix}{coin}0");
                let mut bids = Vec::new();
                for level in 0..orders {
                    oid += 1;
                    bids.push(scoped_order(&coin, oid, "B", &format!("{}.0", 100_000 - level), "1.0"));
                }
                books.push(scoped_book(&coin, &bids, &[]));
            }
        }
        let snapshot = serde_json::json!([100, books]).to_string();
        let open = |order: serde_json::Value| serde_json::json!({"time":"2026-09-11T00:00:01", "user":Address::ZERO, "status":"open", "order":order});
        // Alternate blocks: 5,000 resting orders open, then are removed (1% HIP-4).
        let coin_of =
            |index: u64| if index % 100 == 0 { format!("#{}0", index % 200) } else { format!("C{}0", index % 400) };
        let mut blocks = Vec::new();
        for height in 101..=140_u64 {
            let (statuses, diffs): (Vec<_>, Vec<_>) = (0..5000_u64)
                .map(|index| {
                    let coin = coin_of(index);
                    let oid = 10_000_000 + index;
                    let diff = |raw: serde_json::Value| {
                        serde_json::json!({"user":Address::ZERO, "oid":oid, "px":"200000.0", "coin":coin, "raw_book_diff":raw})
                    };
                    if height % 2 == 1 {
                        (Some(open(scoped_order(&coin, oid, "A", "200000.0", "1.0"))), diff(serde_json::json!({"new":{"sz":"1.0"}})))
                    } else {
                        (None, diff(serde_json::json!("remove")))
                    }
                })
                .unzip();
            let statuses: Vec<_> = statuses.into_iter().flatten().collect();
            blocks.push((
                scoped_block(height, serde_json::json!(statuses)),
                scoped_block(height, serde_json::json!(diffs)),
            ));
        }
        println!("snapshot {} MiB, open-block statuses {} KiB", snapshot.len() >> 20, blocks[0].0.len() >> 10);
        for scope in [CoinScope::all(), CoinScope::default()] {
            let baseline = rss_mib();
            let (tx, _rx) = tokio::sync::broadcast::channel(1024);
            let mut listener = OrderBookListener::new(Some(tx), true, scope.clone());
            let started = std::time::Instant::now();
            let (height, parsed) =
                load_snapshots_from_str::<InnerL4Order, (Address, L4Order)>(&snapshot, &scope).unwrap();
            let load = started.elapsed();
            listener.finish_snapshot(listener.recovery_epoch, None, parsed, height).unwrap();
            let started = std::time::Instant::now();
            std::hint::black_box(listener.compute_l2_snapshot());
            let l2 = started.elapsed();
            let resident = rss_mib().saturating_sub(baseline);
            let started = std::time::Instant::now();
            let audit = listener.clone_state().unwrap().compute_snapshot();
            let audit_time = started.elapsed();
            drop(audit);
            let started = std::time::Instant::now();
            for (statuses, diffs) in &blocks {
                listener.process_data(statuses.clone(), EventSource::OrderStatuses).unwrap();
                listener.process_data(diffs.clone(), EventSource::OrderDiffs).unwrap();
            }
            let per_block = started.elapsed() / blocks.len() as u32;
            assert_eq!(listener.order_book_state.as_ref().unwrap().height(), 140);
            println!(
                "scope={} books={} snapshot_load={load:?} initial_l2={l2:?} audit_clone+snapshot={audit_time:?} \
                 per_block(parse+apply+l2+publish)={per_block:?} book_rss={resident}MiB",
                scope.describe(),
                listener.order_book_state.as_ref().unwrap().book_count(),
            );
        }
    }

    #[test]
    fn filtered_snapshot_audit_still_detects_hip4_divergence() -> Result<()> {
        let scope = CoinScope::default();
        let (tx, mut rx) = tokio::sync::broadcast::channel(16);
        let mut listener = mixed_market_listener(&scope)?;
        listener.internal_message_tx = Some(tx);
        let (_, _, _, after) = mixed_market_fixture();
        let diverged = after.replace(r#""sz":"7.0""#, r#""sz":"6.0""#);
        assert_ne!(diverged, after);
        let (height, expected) = load_snapshots_from_str::<InnerL4Order, (Address, L4Order)>(&diverged, &scope)?;
        listener.finish_snapshot(listener.recovery_epoch, listener.clone_state(), expected, height)?;
        assert_eq!(listener.stats.snapshot_reloads, 1);
        assert!(listener.stats.last_snapshot_reload_reason.as_ref().unwrap().contains("Orders do not match"));
        assert!(matches!(rx.try_recv().unwrap().as_ref(), InternalMessage::Gap));
        Ok(())
    }

    #[test]
    fn a_fence_closes_consumers_and_clears_unmatched_work() {
        let (tx, mut rx) = tokio::sync::broadcast::channel(8);
        let mut listener = OrderBookListener::new(Some(tx), false, CoinScope::all());
        listener.receive_batch(EventBatch::Orders(empty_batch(101))).unwrap();
        listener.fence("test");
        assert!(matches!(rx.try_recv().unwrap().as_ref(), InternalMessage::Gap));
        assert!(listener.order_status_cache.front().is_none());
        assert!(!listener.is_ready());
    }

    #[test]
    fn fills_are_deduplicated_without_detached_tasks() {
        let (tx, mut rx) = tokio::sync::broadcast::channel(8);
        let mut listener = OrderBookListener::new(Some(tx), false, CoinScope::all());
        listener.receive_batch(EventBatch::Fills(empty_batch(101))).unwrap();
        listener.receive_batch(EventBatch::Fills(empty_batch(101))).unwrap();
        assert!(matches!(rx.try_recv().unwrap().as_ref(), InternalMessage::Fills { .. }));
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn replayed_pre_snapshot_batches_are_not_published_or_cached() {
        let (tx, mut rx) = tokio::sync::broadcast::channel(8);
        let mut listener = OrderBookListener::new(Some(tx), false, CoinScope::all());
        listener.order_book_state =
            Some(OrderBookState::from_snapshot(Snapshots::new(HashMap::new()), 100, 1, true, false));
        listener.begin_caching();
        listener.receive_batch(EventBatch::Orders(empty_batch(99))).unwrap();
        listener.receive_batch(EventBatch::BookDiffs(empty_batch(99))).unwrap();
        assert!(rx.try_recv().is_err());
        assert!(listener.fetched_snapshot_cache.as_ref().unwrap().is_empty());
        assert_eq!(listener.order_book_state.as_ref().unwrap().height(), 100);
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
        let listener = Arc::new(Mutex::new(OrderBookListener::new(None, false, CoinScope::all())));
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
        let listener = Arc::new(Mutex::new(OrderBookListener::new(None, false, CoinScope::all())));
        let limits = ResourceLimits {
            turn_bytes: 128,
            record_bytes: 4096,
            queue_bytes: 2048,
            queue_heights: 8,
            snapshot_interval: Duration::from_secs(60),
            ..ResourceLimits::DEFAULT
        };
        let task = tokio::spawn(hl_listen_with_source(listener.clone(), dir.clone(), url, limits));
        tokio::time::timeout(Duration::from_secs(10), entered.notified()).await.unwrap();
        {
            let state = listener.lock().await;
            assert!(state.stats.snapshot_request_inflight_started_at_ms > 0);
            assert_eq!(state.stats.snapshot_request_started_at_ms, 0);
            assert_eq!(state.stats.snapshot_request_completed_at_ms, 0);
        }
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
        // Longer than the recovery retry interval: still only one owner.
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
        // Recovery did not wait for the configured 60-second healthy audit.
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
        sleep(Duration::from_secs(12)).await;
        assert_eq!(requests.load(AtomicOrdering::SeqCst), 2, "healthy audits must respect their interval");
        height.store(165, AtomicOrdering::SeqCst);
        listener.lock().await.fence("test recovery during long healthy audit interval");
        tokio::time::timeout(Duration::from_secs(5), async {
            while !listener.lock().await.is_ready() {
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(requests.load(AtomicOrdering::SeqCst), 3);
        {
            let state = listener.lock().await;
            assert_eq!(state.stats.snapshot_request_inflight_started_at_ms, 0);
            assert!(state.stats.snapshot_request_started_at_ms > 0);
            assert!(state.stats.snapshot_request_completed_at_ms >= state.stats.snapshot_request_started_at_ms);
        }
        task.abort();
        let _ = task.await;
        server.abort();
        let _ = server.await;
        fs::remove_dir_all(dir).unwrap();
    }
}
