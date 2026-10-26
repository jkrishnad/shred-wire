//! The server side of FeedService, sending responses whose kind is already protobuf bytes.
//!
//! A tx or heartbeat is encoded once when it reaches the hub. Every client stream then writes
//! only the seq and its own filter names in front of those shared bytes.
//! The result is the same bytes SubscribeResponse would encode to, so clients read it as usual.

use prost::bytes::{Buf, BufMut, Bytes};
use prost::encoding::{self, DecodeContext, WireType};
use prost::{DecodeError, Message};

use crate::subscribe_response::Kind;

include!(concat!(env!("OUT_DIR"), "/encoded/shredwire.rs"));

/// A SubscribeResponse put together from parts.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Encoded {
    pub seq: u64,
    pub filters: Vec<String>,
    /// The kind field, tx or heartbeat, encoded once and shared by every client.
    pub body: Bytes,
}

impl Encoded {
    /// Encodes the kind field on its own, ready to be shared.
    pub fn body(kind: &Kind) -> Bytes {
        let mut buf = Vec::with_capacity(kind.encoded_len());
        kind.encode(&mut buf);
        Bytes::from(buf)
    }
}

impl Message for Encoded {
    // fields in tag order and a zero seq left out, the same as prost does for SubscribeResponse
    fn encode_raw(&self, buf: &mut impl BufMut) {
        if self.seq != 0 {
            encoding::uint64::encode(1, &self.seq, buf);
        }
        encoding::string::encode_repeated(2, &self.filters, buf);
        buf.put_slice(&self.body);
    }

    // the server only sends these, it never reads one
    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: WireType,
        buf: &mut impl Buf,
        ctx: DecodeContext,
    ) -> Result<(), DecodeError> {
        encoding::skip_field(wire_type, tag, buf, ctx)
    }

    fn encoded_len(&self) -> usize {
        let seq = if self.seq != 0 {
            encoding::uint64::encoded_len(1, &self.seq)
        } else {
            0
        };
        seq + encoding::string::encoded_len_repeated(2, &self.filters) + self.body.len()
    }

    fn clear(&mut self) {
        *self = Self::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Heartbeat, SubscribeResponse, Tx};

    #[test]
    fn test_same_bytes_as_subscribe_response() {
        let tx = Kind::Tx(Tx {
            slot: 457012312,
            signature: vec![7; 64],
            transaction: vec![1; 300],
            first_shred_ns: 1,
            ..Tx::default()
        });
        let heartbeat = Kind::Heartbeat(Heartbeat {
            sent_ns: 2,
            last_slot: 457012312,
        });
        for (seq, filters, kind) in [
            (0, vec![], heartbeat.clone()),
            (5, vec![], heartbeat),
            (300, vec!["all".to_string(), "jup".to_string()], tx),
        ] {
            let parts = Encoded {
                seq,
                filters: filters.clone(),
                body: Encoded::body(&kind),
            };
            let whole = SubscribeResponse {
                seq,
                filters,
                kind: Some(kind),
            };
            assert_eq!(parts.encode_to_vec(), whole.encode_to_vec());
        }
    }
}
