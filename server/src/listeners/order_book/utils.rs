use crate::{
    listeners::order_book::{L2SnapshotParams, L2Snapshots, ResourceLimits},
    order_book::{
        Coin, Snapshot,
        multi_book::{OrderBooks, Snapshots},
        types::InnerOrder,
    },
    prelude::*,
    types::{
        inner::InnerLevel,
        node_data::{Batch, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
    },
};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use reqwest::Client;
use serde_json::json;
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

pub(super) struct SnapshotFile(PathBuf);

impl SnapshotFile {
    pub(super) fn path(&self) -> &Path {
        &self.0
    }

    fn create(dir: &Path) -> Result<Self> {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = dir.join(format!(
            "book-snapshot-{}-{}-{}.json",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::OpenOptions::new().write(true).create_new(true).open(&path)?;
        Ok(Self(path))
    }
}

impl Drop for SnapshotFile {
    fn drop(&mut self) {
        if let Err(err) = fs::remove_file(&self.0) {
            if err.kind() != io::ErrorKind::NotFound {
                log::warn!("Cannot remove snapshot {}: {err}", self.0.display());
            }
        }
    }
}

pub(super) async fn process_rmp_file(dir: &Path, info_url: &str, timeout: std::time::Duration) -> Result<SnapshotFile> {
    let output = SnapshotFile::create(dir)?;
    let output_path = output.path();
    let payload = json!({
        "type": "fileSnapshot",
        "request": {
            "type": "l4Snapshots",
            "includeUsers": true,
            "includeTriggerOrders": false
        },
        "outPath": output_path,
        "includeHeightInOutput": true
    });

    let client = Client::builder().timeout(timeout).build()?;
    client.post(info_url).header("Content-Type", "application/json").json(&payload).send().await?.error_for_status()?;
    Ok(output)
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum SnapshotConsistency {
    Equal,
    EmptyBooksAdded,
}

pub(super) fn validate_snapshot_consistency<O: Clone + PartialEq + Debug>(
    snapshot: &Snapshots<O>,
    expected: &Snapshots<O>,
    ignore_spot: bool,
) -> Result<SnapshotConsistency> {
    let mut snapshot_map: HashMap<_, _> =
        expected.as_ref().iter().filter(|(c, _)| !c.is_spot() || !ignore_spot).collect();

    for (coin, book) in snapshot.as_ref() {
        if ignore_spot && coin.is_spot() {
            continue;
        }
        let book1 = book.as_ref();
        if let Some(book2) = snapshot_map.remove(coin) {
            if book1.iter().map(Vec::len).ne(book2.as_ref().iter().map(Vec::len)) {
                return Err(format!("Order counts do not match for {}", coin.value()).into());
            }
            for (orders1, orders2) in book1.as_ref().iter().zip(book2.as_ref()) {
                for (order1, order2) in orders1.iter().zip(orders2.iter()) {
                    if *order1 != *order2 {
                        return Err(
                            format!("Orders do not match, expected: {:?} received: {:?}", *order2, *order1).into()
                        );
                    }
                }
            }
        } else if !book1[0].is_empty() || !book1[1].is_empty() {
            return Err(format!("Missing {} book", coin.value()).into());
        }
    }
    if snapshot_map.values().any(|book| book.as_ref().iter().any(|orders| !orders.is_empty())) {
        let samples: Vec<_> = snapshot_map
            .iter()
            .take(8)
            .map(|(coin, book)| format!("{coin:?}:{} bids/{} asks", book.as_ref()[0].len(), book.as_ref()[1].len()))
            .collect();
        return Err(format!("Extra orderbooks detected: {samples:?}").into());
    }
    Ok(if snapshot_map.is_empty() { SnapshotConsistency::Equal } else { SnapshotConsistency::EmptyBooksAdded })
}

impl L2SnapshotParams {
    pub(crate) const fn new(n_sig_figs: Option<u32>, mantissa: Option<u64>) -> Self {
        Self { n_sig_figs, mantissa }
    }
}

pub(super) fn refresh_l2_snapshots<O: InnerOrder + Send + Sync>(
    order_books: &OrderBooks<O>,
    cached: &mut L2Snapshots,
    dirty: &HashSet<Coin>,
) {
    if dirty.is_empty() {
        return;
    }
    let refreshed: Vec<_> = order_books
        .as_ref()
        .par_iter()
        .filter(|(coin, _)| dirty.contains(*coin))
        .map(|(coin, order_book)| {
            let mut entries = Vec::new();
            let snapshot = order_book.to_l2_snapshot(None, None, None);
            entries.push((L2SnapshotParams { n_sig_figs: None, mantissa: None }, snapshot));
            let mut add_new_snapshot = |n_sig_figs: Option<u32>, mantissa: Option<u64>, idx: usize| {
                if let Some((_, last_snapshot)) = &entries.get(entries.len() - idx) {
                    let snapshot = last_snapshot.to_l2_snapshot(None, n_sig_figs, mantissa);
                    entries.push((L2SnapshotParams { n_sig_figs, mantissa }, snapshot));
                }
            };
            for n_sig_figs in (2..=5).rev() {
                if n_sig_figs == 5 {
                    for mantissa in [None, Some(2), Some(5)] {
                        if mantissa == Some(5) {
                            // Some(2) is NOT a superset of this info!
                            add_new_snapshot(Some(n_sig_figs), mantissa, 2);
                        } else {
                            add_new_snapshot(Some(n_sig_figs), mantissa, 1);
                        }
                    }
                } else {
                    add_new_snapshot(Some(n_sig_figs), None, 1);
                }
            }
            (coin.clone(), Arc::new(entries.into_iter().collect::<HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>()))
        })
        .collect();
    // Published views remain immutable; only the map of coin references is copied.
    Arc::make_mut(&mut cached.0).extend(refreshed);
}

pub(super) enum EventBatch {
    Orders(Batch<NodeDataOrderStatus>),
    BookDiffs(Batch<NodeDataOrderDiff>),
    Fills(Batch<NodeDataFill>),
}

pub(super) struct BatchQueue<T> {
    deque: VecDeque<(Batch<T>, std::time::Instant)>,
    last_ts: Option<u64>,
    bytes: usize,
}

impl<T> BatchQueue<T> {
    pub(super) const fn new() -> Self {
        Self { deque: VecDeque::new(), last_ts: None, bytes: 0 }
    }

    pub(super) fn push(&mut self, block: Batch<T>, limits: ResourceLimits) -> Result<bool> {
        if self.last_ts.is_some_and(|last| last >= block.block_number()) {
            return Ok(false);
        }
        let next_bytes = self.bytes.checked_add(block.wire_bytes()).ok_or("unmatched queue byte counter overflow")?;
        if next_bytes > limits.queue_bytes
            || self.deque.len() >= limits.queue_heights
            || self.deque.front().is_some_and(|(first, at)| {
                at.elapsed() > limits.queue_age
                    || block.block_number().saturating_sub(first.block_number()) > limits.queue_heights as u64
            })
        {
            return Err(format!("unmatched queue limit: bytes={}, heights={}", self.bytes, self.deque.len()).into());
        }
        self.last_ts = Some(block.block_number());
        self.bytes = next_bytes;
        self.deque.push_back((block, std::time::Instant::now()));
        Ok(true)
    }

    pub(super) const fn bytes(&self) -> usize {
        self.bytes
    }

    pub(super) fn expired(&self, limits: ResourceLimits) -> bool {
        self.deque.front().is_some_and(|(_, at)| at.elapsed() > limits.queue_age)
    }

    pub(super) fn pop_front(&mut self) -> Option<Batch<T>> {
        self.deque.pop_front().map(|(batch, _)| {
            self.bytes -= batch.wire_bytes();
            batch
        })
    }

    pub(super) fn front(&self) -> Option<&Batch<T>> {
        self.deque.front().map(|(batch, _)| batch)
    }
}

#[cfg(test)]
mod snapshot_file_tests {
    use super::SnapshotFile;

    #[test]
    fn snapshots_have_distinct_paths_and_cleanup_only_their_own_file() {
        let directory = std::env::temp_dir();
        let first = SnapshotFile::create(&directory).unwrap();
        let second = SnapshotFile::create(&directory).unwrap();
        assert_ne!(first.path(), second.path());
        let first_path = first.path().to_owned();
        let second_path = second.path().to_owned();
        std::fs::write(&second_path, b"other snapshot").unwrap();
        drop(first);
        assert!(!first_path.exists());
        assert_eq!(std::fs::read(&second_path).unwrap(), b"other snapshot");
        drop(second);
        assert!(!second_path.exists());
    }

    #[tokio::test]
    async fn aborting_an_owned_snapshot_task_cleans_up_its_output() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(async move {
            let file = SnapshotFile::create(&std::env::temp_dir()).unwrap();
            tx.send(file.path().to_owned()).unwrap();
            std::future::pending::<()>().await;
            drop(file);
        });
        let path = rx.await.unwrap();
        assert!(path.exists());
        tasks.shutdown().await;
        assert!(!path.exists());
    }
}

#[cfg(test)]
mod queue_limit_tests {
    use super::*;

    fn batch(height: u64, bytes: usize) -> Batch<NodeDataOrderDiff> {
        serde_json::from_value::<Batch<NodeDataOrderDiff>>(serde_json::json!({
            "local_time":"2026-09-11T00:00:00", "block_time":"2026-09-11T00:00:00",
            "block_number":height, "events":[]
        }))
        .unwrap()
        .with_wire_bytes(bytes)
    }

    #[test]
    fn unmatched_bytes_are_released_and_duplicates_do_not_consume_budget() {
        let limits = ResourceLimits { queue_bytes: 10, ..ResourceLimits::DEFAULT };
        let mut queue = BatchQueue::new();
        assert!(queue.push(batch(1, 10), limits).unwrap());
        assert!(!queue.push(batch(1, 10), limits).unwrap());
        assert!(queue.push(batch(2, 1), limits).is_err());
        queue.pop_front().unwrap();
        assert_eq!(queue.bytes, 0);
        assert!(queue.push(batch(2, 10), limits).unwrap());
    }

    #[test]
    fn unmatched_height_and_age_limits_are_independent_of_bytes() {
        let limits = ResourceLimits { queue_heights: 2, ..ResourceLimits::DEFAULT };
        let mut queue = BatchQueue::new();
        queue.push(batch(1, 0), limits).unwrap();
        assert!(queue.push(batch(4, 0), limits).is_err());
        queue.push(batch(2, 0), limits).unwrap();
        assert!(queue.push(batch(3, 0), limits).is_err());
        queue.deque.front_mut().unwrap().1 =
            std::time::Instant::now() - limits.queue_age - std::time::Duration::from_secs(1);
        assert!(queue.expired(limits));
        assert!(queue.push(batch(3, 0), limits).is_err());
    }
    #[test]
    fn unrepresentable_queue_bytes_fail_without_advancing_accounting() {
        let limits = ResourceLimits { queue_bytes: usize::MAX, ..ResourceLimits::DEFAULT };
        let mut queue = BatchQueue::new();
        queue.push(batch(1, usize::MAX), limits).unwrap();
        assert!(queue.push(batch(2, 1), limits).is_err());
        assert_eq!(queue.bytes, usize::MAX);
        assert_eq!(queue.last_ts, Some(1));
        queue.pop_front().unwrap();
        assert!(queue.push(batch(2, 1), limits).unwrap());
        assert_eq!(queue.bytes, 1);
    }
}

#[cfg(test)]
mod consistency_tests {
    use super::*;
    use crate::order_book::multi_book::load_snapshots_from_str;

    #[derive(Clone, Debug, PartialEq)]
    struct Order(u64);
    impl TryFrom<u64> for Order {
        type Error = crate::prelude::Error;
        fn try_from(value: u64) -> Result<Self> {
            Ok(Self(value))
        }
    }
    fn snapshot(json: &str) -> Snapshots<Order> {
        load_snapshots_from_str::<Order, u64>(json).unwrap().1
    }

    #[test]
    fn empty_addition_requires_exact_existing_order_equality() {
        let baseline = snapshot(r#"[100, [["BTC", [[1], [2]]]]]"#);
        let same = snapshot(r#"[100, [["BTC", [[1], [2]]]]]"#);
        assert_eq!(validate_snapshot_consistency(&baseline, &same, false).unwrap(), SnapshotConsistency::Equal);
        let added = snapshot(r#"[100, [["BTC", [[1], [2]]], ["NEW", [[], []]]]]"#);
        assert_eq!(
            validate_snapshot_consistency(&baseline, &added, false).unwrap(),
            SnapshotConsistency::EmptyBooksAdded
        );
        for json in [
            r#"[100, [["BTC", [[3], [2]]], ["NEW", [[], []]]]]"#,
            r#"[100, [["BTC", [[1, 3], [2]]], ["NEW", [[], []]]]]"#,
            r#"[100, [["BTC", [[1], [2]]], ["NEW", [[3], []]]]]"#,
            r#"[100, [["NEW", [[], []]]]]"#,
        ] {
            assert!(validate_snapshot_consistency(&baseline, &snapshot(json), false).is_err(), "{json}");
        }
    }
}
