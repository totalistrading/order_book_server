use crate::{
    listeners::order_book::{L2Snapshots, TimedSnapshots, utils::refresh_l2_snapshots},
    order_book::{
        Coin, InnerOrder, Oid,
        multi_book::{OrderBooks, Snapshots},
    },
    prelude::*,
    types::{
        inner::{InnerL4Order, InnerOrderDiff},
        node_data::{Batch, NodeDataOrderDiff, NodeDataOrderStatus},
    },
};
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Clone)]
pub(super) struct OrderBookState {
    order_book: OrderBooks<InnerL4Order>,
    height: u64,
    time: u64,
    snapped: bool,
    ignore_spot: bool,
    l2_cache: L2Snapshots,
    dirty_coins: HashSet<Coin>,
}

impl OrderBookState {
    pub(super) fn from_snapshot(
        snapshot: Snapshots<InnerL4Order>,
        height: u64,
        time: u64,
        ignore_triggers: bool,
        ignore_spot: bool,
    ) -> Self {
        let order_book = OrderBooks::from_snapshots(snapshot, ignore_triggers);
        let dirty_coins = order_book.as_ref().keys().cloned().collect();
        Self { ignore_spot, time, height, order_book, dirty_coins, l2_cache: L2Snapshots::default(), snapped: false }
    }

    pub(super) const fn height(&self) -> u64 {
        self.height
    }

    // forcibly take snapshot - (time, height, snapshot)
    pub(super) fn compute_snapshot(&self) -> TimedSnapshots {
        TimedSnapshots { time: self.time, height: self.height, snapshot: self.order_book.to_snapshots_par() }
    }

    pub(super) fn compute_l2_snapshot(&mut self) -> (u64, u64, L2Snapshots) {
        refresh_l2_snapshots(&self.order_book, &mut self.l2_cache, &self.dirty_coins);
        self.dirty_coins.clear();
        (self.time, self.height, self.l2_cache.clone())
    }

    // (time, height, snapshot)
    pub(super) fn l2_snapshots(&mut self, prevent_future_snaps: bool) -> Option<(u64, u64, L2Snapshots)> {
        if self.snapped {
            None
        } else {
            self.snapped = prevent_future_snaps || self.snapped;
            Some(self.compute_l2_snapshot())
        }
    }

    pub(super) fn compute_universe(&self) -> HashSet<Coin> {
        self.order_book.as_ref().keys().cloned().collect()
    }

    pub(super) fn apply_updates(
        &mut self,
        order_statuses: Batch<NodeDataOrderStatus>,
        order_diffs: Batch<NodeDataOrderDiff>,
    ) -> Result<()> {
        let height = order_statuses.block_number();
        let time = order_statuses.block_time();
        assert_eq!(order_statuses.block_number(), order_diffs.block_number());
        if height > self.height + 1 {
            return Err(format!("Expecting block {}, got block {}", self.height + 1, height).into());
        } else if height <= self.height {
            // This is not an error in case we started caching long before a snapshot is fetched
            return Ok(());
        }
        let mut diffs = order_diffs.events().into_iter().collect::<VecDeque<_>>();
        let mut order_map = order_statuses
            .events()
            .into_iter()
            .filter_map(|order_status| {
                if order_status.is_inserted_into_book() {
                    Some((Oid::new(order_status.order.oid), order_status))
                } else {
                    None
                }
            })
            .collect::<HashMap<_, _>>();
        while let Some(diff) = diffs.pop_front() {
            let oid = diff.oid();
            let coin = diff.coin();
            if coin.is_spot() && self.ignore_spot {
                continue;
            }
            self.dirty_coins.insert(coin.clone());
            let inner_diff = diff.diff().try_into()?;
            match inner_diff {
                InnerOrderDiff::New { sz, insert_before } => {
                    if let Some(order) = order_map.remove(&oid) {
                        let time = order.time.and_utc().timestamp_millis();
                        let mut inner_order: InnerL4Order = order.try_into()?;
                        // The status retains a trigger's original limit. The paired book
                        // diff gives its actual resting price and size after activation.
                        inner_order.limit_px = diff.price()?;
                        inner_order.modify_sz(sz);
                        // must replace time with time of entering book, which is the timestamp of the order status update
                        #[allow(clippy::unwrap_used)]
                        inner_order.convert_trigger(time.try_into().unwrap());
                        if !self.order_book.add_order_before(inner_order, insert_before) {
                            return Err(format!("Unable to find insertBefore order on the book {diff:?}").into());
                        }
                    } else {
                        return Err(format!("Unable to find order opening status {diff:?}").into());
                    }
                }
                InnerOrderDiff::Update { new_sz, .. } => {
                    if !self.order_book.modify_sz(oid, coin, new_sz) {
                        return Err(format!("Unable to find order on the book {diff:?}").into());
                    }
                }
                InnerOrderDiff::Remove => {
                    if !self.order_book.cancel_order(oid, coin) {
                        return Err(format!("Unable to find order on the book {diff:?}").into());
                    }
                }
            }
        }
        self.height += 1;
        self.time = time;
        self.snapped = false;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order_book::multi_book::load_snapshots_from_str;
    use crate::types::L4Order;
    use alloy::primitives::Address;
    use std::sync::Arc;

    #[test]
    fn cached_views_preserve_published_values_and_advance_quiet_heights() {
        let (_, snapshot) = load_snapshots_from_str::<InnerL4Order, (Address, L4Order)>(
            r#"[100, [["BTC", [[], []]], ["ETH", [[], []]]]]"#,
        )
        .unwrap();
        let mut state = OrderBookState::from_snapshot(snapshot, 100, 1000, true, false);
        let (_, _, first) = state.compute_l2_snapshot();
        let (_, _, repeated) = state.compute_l2_snapshot();
        assert!(Arc::ptr_eq(&first.0, &repeated.0));

        // A quiet completed block must advance provenance without rebuilding views.
        let empty =
            r#"{"local_time":"2026-09-11T00:00:00","block_time":"2026-09-11T00:00:00","block_number":101,"events":[]}"#;
        state.apply_updates(serde_json::from_str(empty).unwrap(), serde_json::from_str(empty).unwrap()).unwrap();
        let (time, height, quiet) = state.l2_snapshots(true).unwrap();
        assert_eq!(height, 101);
        assert!(time > 1000);
        assert!(Arc::ptr_eq(&first.0, &quiet.0));
        assert!(state.l2_snapshots(true).is_none());

        // Refreshing one coin must not mutate a previously published snapshot.
        state.dirty_coins.insert(Coin::new("BTC"));
        let (_, _, updated) = state.compute_l2_snapshot();
        assert!(!Arc::ptr_eq(&first.0, &updated.0));
        assert!(!Arc::ptr_eq(&first.as_ref()[&Coin::new("BTC")], &updated.as_ref()[&Coin::new("BTC")]));
        assert!(Arc::ptr_eq(&first.as_ref()[&Coin::new("ETH")], &updated.as_ref()[&Coin::new("ETH")]));
    }
    #[test]
    fn triggered_market_order_uses_resting_diff_price_and_matches_snapshot() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!("fixtures/trigger_repriced.json")).unwrap();
        let mut state = OrderBookState::from_snapshot(Snapshots::new(HashMap::new()), 100, 0, true, false);
        state
            .apply_updates(
                serde_json::from_value(fixture["order_statuses"].clone()).unwrap(),
                serde_json::from_value(fixture["book_diffs"].clone()).unwrap(),
            )
            .unwrap();
        let (_, expected) =
            load_snapshots_from_str::<InnerL4Order, (Address, L4Order)>(&fixture["snapshot"].to_string()).unwrap();
        let actual = state.compute_snapshot();
        super::super::utils::validate_snapshot_consistency(&actual.snapshot, &expected, false).unwrap();
        let (_, _, l2) = state.compute_l2_snapshot();
        let views = &l2.as_ref()[&Coin::new("UNI")];
        let raw = views[&super::super::L2SnapshotParams::new(None, None)].as_ref();
        assert_eq!(raw[0][0].px, crate::order_book::Px::parse_from_str("7.917").unwrap());
        assert_eq!(raw[0][0].sz, crate::order_book::Sz::parse_from_str("4.6").unwrap());
        // Later removal must find the order at its authoritative resting level.
        let mut orders = fixture["order_statuses"].clone();
        orders["block_number"] = serde_json::json!(102);
        orders["events"] = serde_json::json!([]);
        let mut diffs = fixture["book_diffs"].clone();
        diffs["block_number"] = serde_json::json!(102);
        diffs["events"][0]["raw_book_diff"] = serde_json::json!("remove");
        state.apply_updates(serde_json::from_value(orders).unwrap(), serde_json::from_value(diffs).unwrap()).unwrap();
        let after = state.compute_snapshot();
        assert!(after.snapshot.as_ref()[&Coin::new("UNI")].as_ref()[0].is_empty());
    }

    fn fixture_state(coins: usize, levels: usize) -> OrderBookState {
        use crate::order_book::{Px, Side, Sz};
        let mut state = OrderBookState::from_snapshot(Snapshots::new(HashMap::new()), 100, 1000, true, false);
        for coin in 0..coins {
            let coin = Coin::new(&format!("COIN{coin}"));
            for level in 0..levels {
                let order = InnerL4Order {
                    user: Address::ZERO,
                    coin: coin.clone(),
                    side: Side::Bid,
                    limit_px: Px::new(10_000_000_000 + level as u64 * 1_000_000),
                    sz: Sz::new(100_000_000),
                    oid: level as u64 + 1,
                    timestamp: 1000,
                    trigger_condition: String::new(),
                    is_trigger: false,
                    trigger_px: "0".into(),
                    is_position_tpsl: false,
                    reduce_only: false,
                    order_type: "Limit".into(),
                    tif: None,
                    cloid: None,
                };
                assert!(state.order_book.add_order_before(order, None));
            }
            state.dirty_coins.insert(coin);
        }
        state
    }

    #[test]
    fn applied_removal_invalidates_only_affected_coin_and_matches_full_rebuild() {
        let mut state = fixture_state(2, 10);
        let (_, _, before) = state.compute_l2_snapshot();
        let batch = |events: serde_json::Value| {
            serde_json::json!({
                "local_time":"2026-09-11T00:00:00", "block_time":"2026-09-11T00:00:00",
                "block_number":101, "events":events
            })
        };
        let orders = serde_json::from_value(batch(serde_json::json!([]))).unwrap();
        let diffs = serde_json::from_value(batch(serde_json::json!([{
            "user":Address::ZERO, "oid":1, "px":"100", "coin":"COIN0", "raw_book_diff":"remove"
        }])))
        .unwrap();
        state.apply_updates(orders, diffs).unwrap();
        let (_, _, after) = state.compute_l2_snapshot();
        let coin = Coin::new("COIN0");
        assert!(!Arc::ptr_eq(&before.as_ref()[&coin], &after.as_ref()[&coin]));
        assert!(Arc::ptr_eq(&before.as_ref()[&Coin::new("COIN1")], &after.as_ref()[&Coin::new("COIN1")]));
        let mut full = L2Snapshots::default();
        refresh_l2_snapshots(&state.order_book, &mut full, &state.compute_universe());
        for (coin, views) in after.as_ref() {
            for (params, view) in views.as_ref() {
                assert_eq!(
                    view.clone().export_inner_snapshot(),
                    full.as_ref()[coin][params].clone().export_inner_snapshot()
                );
            }
        }
    }

    #[test]
    #[ignore = "manual resource benchmark; run with --release --ignored --nocapture"]
    fn benchmark_incremental_l2_views() {
        let mut state = fixture_state(200, 1000);
        state.compute_l2_snapshot();
        let all = state.compute_universe();
        let started = std::time::Instant::now();
        for _ in 0..100 {
            state.dirty_coins = all.clone();
            std::hint::black_box(state.compute_l2_snapshot());
        }
        let full = started.elapsed();
        let started = std::time::Instant::now();
        for _ in 0..100 {
            state.dirty_coins.insert(Coin::new("COIN0"));
            std::hint::black_box(state.compute_l2_snapshot());
        }
        let incremental = started.elapsed();
        println!("L2 benchmark 200 coins x 1000 levels x 100 turns: full={full:?}, one_dirty={incremental:?}");
    }
}
