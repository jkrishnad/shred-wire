//! Every update goes through the hub, which numbers it and hands a copy to each subscriber.
//!
//! The ingest thread and the heartbeat task both send, so the seq is taken and the update
//! sent under one lock. Otherwise seq 6 could reach the channel before seq 5.
//! Each update is encoded once, outside the lock, and its bytes are shared by every subscriber.
//! Sending never waits: a subscriber that falls more than the buffer behind is lagged,
//! and the server closes it.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use prost::bytes::Bytes;
use proto::encoded::Encoded;
use proto::subscribe_response::Kind;
use tokio::sync::broadcast;

use crate::filter::TxKeys;

/// One numbered update, with what filters look at when it is a tx.
#[derive(Debug)]
pub struct Update {
    pub seq: u64,
    /// The tx or heartbeat, already encoded.
    pub body: Bytes,
    pub keys: Option<TxKeys>,
}

impl Update {
    /// The response for one subscriber, with the names of its filters that matched.
    pub fn response(&self, filters: Vec<String>) -> Encoded {
        Encoded {
            seq: self.seq,
            filters,
            body: self.body.clone(),
        }
    }
}

pub struct Hub {
    /// The next seq, with the channel it goes out on.
    next: Mutex<(u64, broadcast::Sender<Arc<Update>>)>,
    /// Newest slot seen, for the heartbeat.
    last_slot: AtomicU64,
}

impl Hub {
    /// A hub keeping the last buffer updates for subscribers that are behind.
    pub fn new(buffer: usize) -> Self {
        let (sender, _) = broadcast::channel(buffer);
        Self {
            next: Mutex::new((0, sender)),
            last_slot: AtomicU64::new(0),
        }
    }

    /// Numbers the update and hands it to every subscriber.
    pub fn send(&self, kind: Kind, keys: Option<TxKeys>) {
        let body = Encoded::body(&kind);
        // a panic elsewhere while holding the lock leaves the seq and channel whole, so carry on
        let mut next = self.next.lock().unwrap_or_else(PoisonError::into_inner);
        let (seq, sender) = &mut *next;
        let update = Update {
            seq: *seq,
            body,
            keys,
        };
        *seq += 1;
        // with no subscribers the update is dropped, and its seq is still used up
        let _ = sender.send(Arc::new(update));
    }

    /// A new reader that sees every update sent from now on.
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<Update>> {
        let next = self.next.lock().unwrap_or_else(PoisonError::into_inner);
        next.1.subscribe()
    }

    pub fn saw_slot(&self, slot: u64) {
        self.last_slot.fetch_max(slot, Ordering::Relaxed);
    }

    pub fn last_slot(&self) -> u64 {
        self.last_slot.load(Ordering::Relaxed)
    }
}
