use std::{
    borrow::Cow,
    path::{Path, PathBuf},
};

use alloy::primitives::Address;
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize, de::DeserializeOwned, de::IgnoredAny};
use serde_json::value::RawValue;

use crate::{
    order_book::{Coin, Oid, Px},
    types::{Fill, L4Order, OrderDiff, subscription::CoinScope},
};

#[derive(Deserialize)]
struct CoinProbe<'a> {
    #[serde(borrow)]
    coin: Cow<'a, str>,
}

#[derive(Deserialize)]
struct OrderStatusProbe<'a> {
    #[serde(borrow)]
    order: CoinProbe<'a>,
}

/// A node event whose coin can be read without materialising the event.
pub(crate) trait ScopedEvent: DeserializeOwned {
    fn in_scope(raw: &RawValue, scope: &CoinScope) -> serde_json::Result<bool>;
}

impl ScopedEvent for NodeDataOrderDiff {
    fn in_scope(raw: &RawValue, scope: &CoinScope) -> serde_json::Result<bool> {
        Ok(scope.contains(&serde_json::from_str::<CoinProbe<'_>>(raw.get())?.coin))
    }
}

impl ScopedEvent for NodeDataOrderStatus {
    fn in_scope(raw: &RawValue, scope: &CoinScope) -> serde_json::Result<bool> {
        Ok(scope.contains(&serde_json::from_str::<OrderStatusProbe<'_>>(raw.get())?.order.coin))
    }
}

impl ScopedEvent for NodeDataFill {
    fn in_scope(raw: &RawValue, scope: &CoinScope) -> serde_json::Result<bool> {
        Ok(scope.contains(&serde_json::from_str::<(IgnoredAny, CoinProbe<'_>)>(raw.get())?.1.coin))
    }
}

#[derive(Deserialize)]
struct RawBatch<'a> {
    local_time: NaiveDateTime,
    block_time: NaiveDateTime,
    block_number: u64,
    #[serde(borrow)]
    events: Vec<&'a RawValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct NodeDataOrderDiff {
    user: Address,
    oid: u64,
    px: String,
    coin: String,
    pub(crate) raw_book_diff: OrderDiff,
}

impl NodeDataOrderDiff {
    pub(crate) fn diff(&self) -> OrderDiff {
        self.raw_book_diff.clone()
    }
    pub(crate) const fn oid(&self) -> Oid {
        Oid::new(self.oid)
    }

    pub(crate) fn price(&self) -> crate::prelude::Result<Px> {
        Px::parse_from_str(&self.px)
    }

    pub(crate) fn coin(&self) -> Coin {
        Coin::new(&self.coin)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct NodeDataFill(pub Address, pub Fill);

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct NodeDataOrderStatus {
    pub time: NaiveDateTime,
    pub user: Address,
    pub status: String,
    pub order: L4Order,
}

impl NodeDataOrderStatus {
    pub(crate) fn is_inserted_into_book(&self) -> bool {
        (self.status == "open" && !self.order.is_trigger && (self.order.tif != Some("Ioc".to_string())))
            || (self.order.is_trigger && self.status == "triggered")
    }
}

#[derive(Clone, Copy, strum_macros::Display)]
pub(crate) enum EventSource {
    Fills,
    OrderStatuses,
    OrderDiffs,
}

impl EventSource {
    #[must_use]
    pub(crate) fn event_source_dir(self, dir: &Path) -> PathBuf {
        match self {
            Self::Fills => dir.join("hl/data/node_fills_by_block"),
            Self::OrderStatuses => dir.join("hl/data/node_order_statuses_by_block"),
            Self::OrderDiffs => dir.join("hl/data/node_raw_book_diffs_by_block"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct Batch<E> {
    local_time: NaiveDateTime,
    block_time: NaiveDateTime,
    block_number: u64,
    events: Vec<E>,
    #[serde(skip)]
    wire_bytes: usize,
}

impl<E: ScopedEvent> Batch<E> {
    /// Parses one block record, keeping only in-scope events. Out-of-scope
    /// events are only probed for their coin. The block position is always
    /// retained, so an empty result still advances every in-scope book.
    /// `wire_bytes` stays the full record length for queue accounting.
    pub(crate) fn parse_in_scope(line: &str, scope: &CoinScope) -> serde_json::Result<Self> {
        if scope.is_all() {
            return serde_json::from_str::<Self>(line).map(|batch| batch.with_wire_bytes(line.len()));
        }
        let raw: RawBatch<'_> = serde_json::from_str(line)?;
        let mut events = Vec::new();
        for event in raw.events {
            if E::in_scope(event, scope)? {
                events.push(serde_json::from_str(event.get())?);
            }
        }
        Ok(Self {
            local_time: raw.local_time,
            block_time: raw.block_time,
            block_number: raw.block_number,
            events,
            wire_bytes: line.len(),
        })
    }
}

impl<E> Batch<E> {
    pub(crate) fn with_wire_bytes(mut self, bytes: usize) -> Self {
        self.wire_bytes = bytes;
        self
    }

    pub(crate) const fn wire_bytes(&self) -> usize {
        self.wire_bytes
    }

    #[allow(clippy::unwrap_used)]
    pub(crate) fn block_time(&self) -> u64 {
        self.block_time.and_utc().timestamp_millis().try_into().unwrap()
    }

    pub(crate) const fn block_number(&self) -> u64 {
        self.block_number
    }

    pub(crate) fn events(self) -> Vec<E> {
        self.events
    }
}

#[cfg(test)]
mod scope_tests {
    use super::*;

    fn block(events: &str) -> String {
        format!(
            r#"{{"local_time":"2026-09-11T00:00:00","block_time":"2026-09-11T00:00:01","block_number":7,"events":[{events}]}}"#
        )
    }

    #[test]
    fn out_of_scope_events_are_dropped_without_being_materialised() {
        // The BTC event is not a valid diff; it must only be probed for its coin.
        let line = block(
            r##"{"coin":"BTC","oid":"not-a-number"},
               {"user":"0x0000000000000000000000000000000000000000","oid":5,"px":"0.6","coin":"#10","raw_book_diff":"remove"}"##,
        );
        let scoped = Batch::<NodeDataOrderDiff>::parse_in_scope(&line, &CoinScope::default()).unwrap();
        assert_eq!((scoped.block_number(), scoped.wire_bytes()), (7, line.len()));
        let full_time = serde_json::from_str::<Batch<NodeDataOrderDiff>>(&block("")).unwrap().block_time();
        assert_eq!(scoped.block_time(), full_time);
        let events = scoped.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].coin(), Coin::new("#10"));
        assert!(Batch::<NodeDataOrderDiff>::parse_in_scope(&line, &CoinScope::all()).is_err());
        // An event without a readable coin is still a malformed source record.
        assert!(Batch::<NodeDataOrderDiff>::parse_in_scope(&block(r#"{"oid":1}"#), &CoinScope::default()).is_err());
    }

    #[test]
    fn statuses_and_fills_are_scoped_by_their_coin() {
        let status = |coin: &str| {
            format!(
                r#"{{"time":"2026-09-11T00:00:00","user":"0x0000000000000000000000000000000000000000","status":"open","order":{{"coin":"{coin}","side":"B","limitPx":"0.5","sz":"1.0","oid":1,"timestamp":0,"triggerCondition":"N/A","isTrigger":false,"triggerPx":"0.0","isPositionTpsl":false,"reduceOnly":false,"orderType":"Limit","tif":"Gtc","cloid":null}}}}"#
            )
        };
        let line = block(&[status("@107"), status("#11"), status("ETH")].join(","));
        let statuses = Batch::<NodeDataOrderStatus>::parse_in_scope(&line, &CoinScope::default()).unwrap().events();
        assert_eq!(statuses.iter().map(|s| s.order.coin.as_str()).collect::<Vec<_>>(), ["#11"]);
        let all = Batch::<NodeDataOrderStatus>::parse_in_scope(&line, &CoinScope::all()).unwrap().events();
        assert_eq!(all, serde_json::from_str::<Batch<NodeDataOrderStatus>>(&line).unwrap().events());

        let fill = |coin: &str, side: &str| {
            format!(
                r#"["0x0000000000000000000000000000000000000000",{{"coin":"{coin}","px":"1","sz":"1","side":"{side}","time":0,"startPosition":"0","dir":"Buy","closedPnl":"0","hash":"0x0","oid":1,"crossed":true,"fee":"0","tid":9,"feeToken":"USDC","liquidation":null}}]"#
            )
        };
        let line = block(&[fill("BTC", "A"), fill("BTC", "B"), fill("#11", "A"), fill("#11", "B")].join(","));
        let fills = Batch::<NodeDataFill>::parse_in_scope(&line, &CoinScope::default()).unwrap().events();
        assert_eq!(fills.iter().map(|NodeDataFill(_, fill)| fill.coin.as_str()).collect::<Vec<_>>(), ["#11", "#11"]);
    }
}
