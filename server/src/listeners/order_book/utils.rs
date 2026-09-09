use crate::{
    listeners::order_book::{L2SnapshotParams, L2Snapshots},
    order_book::{
        Snapshot,
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
use std::collections::VecDeque;
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

pub(super) async fn process_rmp_file(dir: &Path) -> Result<SnapshotFile> {
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

    let client = Client::new();
    client
        .post("http://localhost:3001/info")
        .header("Content-Type", "application/json")
        .json(&payload)
        .send()
        .await?
        .error_for_status()?;
    Ok(output)
}

pub(super) fn validate_snapshot_consistency<O: Clone + PartialEq + Debug>(
    snapshot: &Snapshots<O>,
    expected: &Snapshots<O>,
    ignore_spot: bool,
) -> Result<()> {
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
    if !snapshot_map.is_empty() {
        return Err("Extra orderbooks detected".to_string().into());
    }
    Ok(())
}

impl L2SnapshotParams {
    pub(crate) const fn new(n_sig_figs: Option<u32>, mantissa: Option<u64>) -> Self {
        Self { n_sig_figs, mantissa }
    }
}

pub(super) fn compute_l2_snapshots<O: InnerOrder + Send + Sync>(order_books: &OrderBooks<O>) -> L2Snapshots {
    L2Snapshots(
        order_books
            .as_ref()
            .par_iter()
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
                (coin.clone(), entries.into_iter().collect::<HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>())
            })
            .collect(),
    )
}

pub(super) enum EventBatch {
    Orders(Batch<NodeDataOrderStatus>),
    BookDiffs(Batch<NodeDataOrderDiff>),
    Fills(Batch<NodeDataFill>),
}

pub(super) struct BatchQueue<T> {
    deque: VecDeque<Batch<T>>,
    last_ts: Option<u64>,
}

impl<T> BatchQueue<T> {
    pub(super) const fn new() -> Self {
        Self { deque: VecDeque::new(), last_ts: None }
    }

    pub(super) fn push(&mut self, block: Batch<T>) -> bool {
        if let Some(last_ts) = self.last_ts {
            if last_ts >= block.block_number() {
                return false;
            }
        }
        self.last_ts = Some(block.block_number());
        self.deque.push_back(block);
        true
    }

    pub(super) fn pop_front(&mut self) -> Option<Batch<T>> {
        self.deque.pop_front()
    }

    pub(super) fn front(&self) -> Option<&Batch<T>> {
        self.deque.front()
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
