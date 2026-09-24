//! Bounded, connection-local observations. A completed batch means local Sink
//! flushes completed, never that the remote application acknowledged receipt.
use serde::Serialize;
use std::{
    collections::VecDeque,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

const HISTORY: usize = 128;
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

pub(super) fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

#[derive(Clone, Serialize)]
pub(super) struct BatchTrace {
    height: u64,
    source_time_ms: u64,
    started_at_ms: u64,
    completed_at_ms: Option<u64>,
    flushed_frames: u64,
}

#[derive(Serialize)]
pub(super) struct DeliveryTrace {
    pub id: String,
    opened_at_ms: u64,
    closed_at_ms: Option<u64>,
    batches: VecDeque<BatchTrace>,
}

impl DeliveryTrace {
    pub fn new() -> Self {
        Self {
            id: format!("{:x}-{:x}-{:x}", now_ms(), std::process::id(), NEXT_ID.fetch_add(1, Ordering::Relaxed)),
            opened_at_ms: now_ms(),
            closed_at_ms: None,
            batches: VecDeque::with_capacity(HISTORY),
        }
    }
    pub fn begin(&mut self, height: u64, source_time_ms: u64) {
        if self.batches.len() == HISTORY {
            self.batches.pop_front();
        }
        self.batches.push_back(BatchTrace {
            height,
            source_time_ms,
            started_at_ms: now_ms(),
            completed_at_ms: None,
            flushed_frames: 0,
        });
    }
    pub fn sent(&mut self) {
        if let Some(batch) = self.batches.back_mut() {
            batch.flushed_frames += 1;
        }
    }
    pub fn complete(&mut self) {
        if let Some(batch) = self.batches.back_mut() {
            batch.completed_at_ms = Some(now_ms());
        }
    }
}

impl Drop for DeliveryTrace {
    fn drop(&mut self) {
        self.closed_at_ms = Some(now_ms());
        // Fixed number of numeric fields, no peer addresses, payloads or errors.
        // Includes an unfinished batch when cancellation/error interrupts send.
        if let Ok(value) = serde_json::to_string(self) {
            log::info!("book_delivery_trace {value}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn identity_is_connection_local_and_header_safe() {
        let a = DeliveryTrace::new();
        let b = DeliveryTrace::new();
        assert_ne!(a.id, b.id);
        assert!(a.id.bytes().all(|c| c.is_ascii_hexdigit() || c == b'-'));
    }
    #[test]
    fn history_is_bounded_and_incomplete_batch_is_not_acknowledged() {
        let mut trace = DeliveryTrace::new();
        for height in 0..200 {
            trace.begin(height, height * 100);
            trace.complete();
        }
        trace.begin(200, 20000);
        trace.sent();
        assert_eq!(trace.batches.len(), HISTORY);
        assert_eq!(trace.batches.front().unwrap().height, 73);
        assert!(trace.batches.back().unwrap().completed_at_ms.is_none());
        assert_eq!(trace.batches.back().unwrap().flushed_frames, 1);
        assert!(trace.batches[HISTORY - 2].completed_at_ms.is_some());
        let value = serde_json::to_value(&trace).unwrap();
        assert_eq!(value["batches"][HISTORY - 1]["source_time_ms"], 20000);
    }
}
