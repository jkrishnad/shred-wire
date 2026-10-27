//! The shredwire client. It subscribes, hands over every update and reconnects when the stream ends.
//!
//! Every update carries the server's global seq, and every connection starts with the server's
//! boot epoch. After a reconnect to the same epoch, the seqs between the last update before the
//! drop and the first one after it are reported as a gap. A new epoch means the server restarted
//! and its seq began again, so that is reported as a reset instead.
//! With filters, seqs of txs that did not match are inside a gap too.
//! Each time it can't connect or the stream ends, it says why and when it tries again.
//! A wrong API key or a bad filter can't be fixed by retrying, so then it stops.

use std::collections::HashMap;
use std::error::Error;
use std::time::Duration;

use shredwire_proto::feed_service_client::FeedServiceClient;
use shredwire_proto::{EPOCH_HEADER, TOKEN_HEADER};
use shredwire_proto::{SubscribeRequest, SubscribeResponse, TxFilter};
use tokio::sync::mpsc;
use tonic::{Code, Request, Status, Streaming};

/// The message types, so a user needs only this crate.
pub use shredwire_proto as proto;

const FIRST_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(5);

pub struct Config {
    /// Server address, like http://127.0.0.1:50051.
    pub url: String,
    /// API key, sent in the x-token header.
    pub token: String,
    /// Named filters, sent on every subscribe.
    pub filters: HashMap<String, TxFilter>,
}

/// What the client hands over.
#[derive(Debug)]
pub enum Event {
    Update(SubscribeResponse),
    /// Seqs from..=to of this server boot were sent while the client was away.
    Gap {
        from: u64,
        to: u64,
    },
    /// The server restarted, seq began again.
    Reset {
        old: String,
        new: String,
    },
    /// Subscribed, with the server's boot epoch.
    Connected {
        epoch: String,
    },
    /// Not connected, trying again after the wait.
    Retrying {
        reason: String,
        after: Duration,
    },
    /// Refused for a reason retrying can't fix, like a wrong API key. Nothing comes after it.
    Stopped {
        reason: String,
    },
}

/// Runs until the receiving side of events is dropped.
pub async fn run(config: Config, events: mpsc::UnboundedSender<Event>) {
    let mut backoff = FIRST_BACKOFF;
    let mut epoch: Option<String> = None;
    let mut last_seq: Option<u64> = None;

    loop {
        let reason = match subscribe(&config).await {
            Ok((new_epoch, mut stream)) => {
                backoff = FIRST_BACKOFF;
                if events
                    .send(Event::Connected {
                        epoch: new_epoch.clone(),
                    })
                    .is_err()
                {
                    return;
                }
                if let Some(old) = epoch.replace(new_epoch.clone())
                    && old != new_epoch
                {
                    last_seq = None;
                    if events
                        .send(Event::Reset {
                            old,
                            new: new_epoch,
                        })
                        .is_err()
                    {
                        return;
                    }
                }

                // only the first update of a connection can follow a gap, the rest come in order
                let mut first = true;
                loop {
                    let update = match stream.message().await {
                        Ok(Some(update)) => update,
                        Ok(None) => break "the server ended the stream".to_string(),
                        Err(status) => break status.message().to_string(),
                    };
                    if first
                        && let Some(last) = last_seq
                        && update.seq > last + 1
                    {
                        let gap = Event::Gap {
                            from: last + 1,
                            to: update.seq - 1,
                        };
                        if events.send(gap).is_err() {
                            return;
                        }
                    }
                    first = false;
                    last_seq = Some(update.seq);
                    if events.send(Event::Update(update)).is_err() {
                        return;
                    }
                }
            }
            Err(status)
                if matches!(status.code(), Code::Unauthenticated | Code::InvalidArgument) =>
            {
                let reason = status.message().to_string();
                let _ = events.send(Event::Stopped { reason });
                return;
            }
            Err(status) => status.message().to_string(),
        };

        let retry = Event::Retrying {
            reason,
            after: backoff,
        };
        if events.send(retry).is_err() {
            return;
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// Connects and subscribes, returning the server's boot epoch and the update stream.
async fn subscribe(config: &Config) -> Result<(String, Streaming<SubscribeResponse>), Status> {
    let mut client = FeedServiceClient::connect(config.url.clone())
        .await
        .map_err(|e| Status::unavailable(with_causes(&e)))?;

    let token = config
        .token
        .parse()
        .map_err(|_| Status::invalid_argument("API key is not a valid header value"))?;
    let mut request = Request::new(SubscribeRequest {
        filters: config.filters.clone(),
    });
    request.metadata_mut().insert(TOKEN_HEADER, token);

    let response = client.subscribe(request).await?;
    let epoch = response
        .metadata()
        .get(EPOCH_HEADER)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| Status::internal("server sent no boot epoch"))?
        .to_string();
    Ok((epoch, response.into_inner()))
}

/// The error and what caused it, like "transport error, connection refused".
fn with_causes(error: &dyn Error) -> String {
    let mut parts = vec![error.to_string()];
    let mut cause = error.source();
    while let Some(next) = cause {
        let part = next.to_string();
        // some layers repeat the message of the one below
        if parts.last() != Some(&part) {
            parts.push(part);
        }
        cause = next.source();
    }
    parts.join(", ")
}
