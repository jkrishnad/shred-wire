//! The shredwire contract, generated from feed.proto.

tonic::include_proto!("shredwire");

/// Response header carrying the server's boot epoch.
pub const EPOCH_HEADER: &str = "x-shredwire-epoch";
/// Request header carrying the API key.
pub const TOKEN_HEADER: &str = "x-token";
