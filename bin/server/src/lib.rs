//! The shredwire server. It checks the API key, then streams every update to every subscriber.
//!
//! source fills the hub: verified transactions from shreds, live or from a pcap file, or made-up ones.
//! hub numbers every update with a global seq and hands a copy to each subscriber.
//! A heartbeat goes out every second. A subscriber that falls too far behind is closed with
//! RESOURCE_EXHAUSTED instead of slowing the others down.

pub mod hub;
pub mod source;

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use proto::feed_service_server::{FeedService, FeedServiceServer};
use proto::subscribe_response::Kind;
use proto::{EPOCH_HEADER, TOKEN_HEADER};
use proto::{Heartbeat, SubscribeRequest, SubscribeResponse};
use tokio::net::TcpListener;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::wrappers::{BroadcastStream, TcpListenerStream};
use tokio_stream::{Stream, StreamExt};
use tonic::metadata::{AsciiMetadataValue, MetadataValue};
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use hub::Hub;
pub use source::Source;

/// Updates a subscriber may fall behind before it is closed.
const BUFFER: usize = 4096;
const HEARTBEAT: Duration = Duration::from_secs(1);

pub struct Config {
    /// API keys allowed to subscribe.
    pub tokens: Vec<String>,
    /// Where the transactions come from.
    pub source: Source,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Reading shreds failed, shown exactly as shred-core words it.
    #[error(transparent)]
    Core(#[from] shred_core::error::Error),
    #[error("no leader schedule in {0}, needed to check every shred")]
    NoLeaders(PathBuf),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("grpc: {0}")]
    Transport(#[from] tonic::transport::Error),
}

/// Serves the feed on the listener until it fails or the shred source stops with an error.
pub async fn serve(listener: TcpListener, config: Config) -> Result<(), Error> {
    let hub = Arc::new(Hub::new(BUFFER));
    let mut running = source::start(config.source, hub.clone())?;
    let heartbeat = tokio::spawn(heartbeat(hub.clone()));

    let feed = Feed {
        hub,
        epoch: boot_epoch(),
    };
    let tokens = Arc::new(config.tokens);
    let service =
        FeedServiceServer::with_interceptor(feed, move |request| check_token(&tokens, request));
    let server = Server::builder()
        .add_service(service)
        .serve_with_incoming(TcpListenerStream::new(listener));

    // a source that ends cleanly, like a pcap file, leaves the server running with heartbeats
    let failed = async {
        match (&mut running.done).await {
            Ok(Err(e)) => e,
            _ => std::future::pending().await,
        }
    };
    let result = tokio::select! {
        result = server => result.map_err(Error::from),
        e = failed => Err(e),
    };
    heartbeat.abort();
    result
}

struct Feed {
    hub: Arc<Hub>,
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
        let stream = BroadcastStream::new(self.hub.subscribe()).map(move |update| match update {
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

/// Sends a heartbeat every second, the first one right away.
async fn heartbeat(hub: Arc<Hub>) {
    let mut ticks = tokio::time::interval(HEARTBEAT);
    loop {
        ticks.tick().await;
        hub.send(Kind::Heartbeat(Heartbeat {
            sent_ns: wall_ns(),
            last_slot: hub.last_slot(),
        }));
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
pub(crate) fn wall_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_nanos() as u64)
}
