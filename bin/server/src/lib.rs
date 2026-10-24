//! The shredwire server. It checks the API key, then streams every update to every subscriber.
//!
//! One producer task numbers each update with a global seq and puts it on a broadcast channel.
//! Every subscriber reads its own copy, and one that falls too far behind is closed with
//! RESOURCE_EXHAUSTED instead of slowing the others down.
//! The transactions are made up, at a fixed rate, with a heartbeat every second.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use proto::feed_service_server::{FeedService, FeedServiceServer};
use proto::subscribe_response::Kind;
use proto::{EPOCH_HEADER, TOKEN_HEADER};
use proto::{Heartbeat, SubscribeRequest, SubscribeResponse, Tx};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::wrappers::{BroadcastStream, TcpListenerStream};
use tokio_stream::{Stream, StreamExt};
use tonic::metadata::{AsciiMetadataValue, MetadataValue};
use tonic::transport::Server;
use tonic::{Request, Response, Status};

/// Updates a subscriber may fall behind before it is closed.
const BUFFER: usize = 4096;
const HEARTBEAT: Duration = Duration::from_secs(1);
/// The producer wakes this often and sends every tx that came due since.
const TICK: Duration = Duration::from_millis(1);
const SLOT_NS: u128 = 200_000_000;

pub struct Config {
    /// API keys allowed to subscribe.
    pub tokens: Vec<String>,
    /// Made-up transactions per second.
    pub tx_per_sec: u64,
}

/// Serves the feed on the listener until it fails.
pub async fn serve(listener: TcpListener, config: Config) -> Result<(), tonic::transport::Error> {
    let (updates, _) = broadcast::channel(BUFFER);
    let producer = tokio::spawn(produce(updates.clone(), config.tx_per_sec));

    let feed = Feed {
        updates,
        epoch: boot_epoch(),
    };
    let tokens = Arc::new(config.tokens);
    let service =
        FeedServiceServer::with_interceptor(feed, move |request| check_token(&tokens, request));

    let result = Server::builder()
        .add_service(service)
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await;
    producer.abort();
    result
}

struct Feed {
    updates: broadcast::Sender<SubscribeResponse>,
    epoch: AsciiMetadataValue,
}

#[tonic::async_trait]
impl FeedService for Feed {
    type SubscribeStream = Pin<Box<dyn Stream<Item = Result<SubscribeResponse, Status>> + Send>>;

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        // filters are not checked against the transactions yet, so every tx matches all of them
        let names: Vec<String> = request.into_inner().filters.into_keys().collect();
        let stream =
            BroadcastStream::new(self.updates.subscribe()).map(move |update| match update {
                Ok(mut update) => {
                    if matches!(update.kind, Some(Kind::Tx(_))) {
                        update.filters = names.clone();
                    }
                    Ok(update)
                }
                // tonic ends the stream after the first error
                Err(BroadcastStreamRecvError::Lagged(missed)) => Err(Status::resource_exhausted(
                    format!("client fell {missed} updates behind"),
                )),
            });

        let mut response = Response::new(Box::pin(stream) as Self::SubscribeStream);
        response
            .metadata_mut()
            .insert(EPOCH_HEADER, self.epoch.clone());
        Ok(response)
    }
}

/// Lets the call through only with one of the configured keys, before any data is sent.
fn check_token(tokens: &[String], request: Request<()>) -> Result<Request<()>, Status> {
    let token = request
        .metadata()
        .get(TOKEN_HEADER)
        .and_then(|value| value.to_str().ok());
    match token {
        Some(token) if tokens.iter().any(|key| key == token) => Ok(request),
        _ => Err(Status::unauthenticated("missing or wrong API key")),
    }
}

/// Numbers every update and hands it to the subscribers.
async fn produce(updates: broadcast::Sender<SubscribeResponse>, tx_per_sec: u64) {
    let start = Instant::now();
    let mut ticks = tokio::time::interval(TICK);
    let mut seq = 0;
    let mut txs_sent: u64 = 0;
    let mut next_heartbeat = Duration::ZERO;
    let mut slot = 0;
    let mut in_slot = 0;

    loop {
        ticks.tick().await;
        let elapsed = start.elapsed();
        let now_slot = (elapsed.as_nanos() / SLOT_NS) as u64;
        if now_slot != slot {
            slot = now_slot;
            in_slot = 0;
        }

        let due = (elapsed.as_nanos() * u128::from(tx_per_sec) / 1_000_000_000) as u64;
        while txs_sent < due {
            let tx = made_up_tx(slot, in_slot, seq);
            send(&updates, &mut seq, Kind::Tx(tx));
            txs_sent += 1;
            in_slot += 1;
        }

        if elapsed >= next_heartbeat {
            let heartbeat = Heartbeat {
                sent_ns: wall_ns(),
                last_slot: slot,
            };
            send(&updates, &mut seq, Kind::Heartbeat(heartbeat));
            next_heartbeat += HEARTBEAT;
        }
    }
}

fn send(updates: &broadcast::Sender<SubscribeResponse>, seq: &mut u64, kind: Kind) {
    let update = SubscribeResponse {
        seq: *seq,
        filters: Vec::new(),
        kind: Some(kind),
    };
    *seq += 1;
    // with no subscribers the update is dropped, and its seq is still used up
    let _ = updates.send(update);
}

/// A tx with the right shape and no real contents. The signature holds the seq.
fn made_up_tx(slot: u64, position: u32, seq: u64) -> Tx {
    let mut signature = vec![0; 64];
    signature[..8].copy_from_slice(&seq.to_le_bytes());
    let now = wall_ns();
    Tx {
        slot,
        entry: 0,
        position,
        signature,
        transaction: Vec::new(),
        rebuilt: false,
        first_shred_ns: now,
        decoded_ns: now,
        emitted_ns: now,
    }
}

/// A new random value every boot, so a client can tell a restart from a dropped connection.
fn boot_epoch() -> AsciiMetadataValue {
    // RandomState is seeded randomly per process and per call, so no rand crate is needed
    let mut hasher = RandomState::new().build_hasher();
    hasher.write_u64(wall_ns());
    let epoch = format!("{:016x}", hasher.finish());
    // sixteen hex digits are always a valid header value
    epoch
        .parse()
        .unwrap_or_else(|_| MetadataValue::from_static("0"))
}

/// Wall clock, Unix epoch nanoseconds.
fn wall_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_nanos() as u64)
}
