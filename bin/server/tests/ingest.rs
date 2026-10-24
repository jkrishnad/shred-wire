//! Real shreds in, every transaction out whole, in order, once each.
//! Runs only when SHREDWIRE_PCAP names a capture and SHREDWIRE_LEADERS its leader schedule folder,
//! so no capture or schedule lives in the repo. SHREDWIRE_TXS, if set, is the tx count getBlock reports.

use std::collections::HashSet;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use proto::subscribe_response::Kind;
use server::hub::Hub;
use shred_core::pcap::PcapReader;
use solana_transaction::versioned::VersionedTransaction;

#[test]
fn ingest_real_capture() {
    let (Ok(pcap), Ok(leaders)) = (
        std::env::var("SHREDWIRE_PCAP"),
        std::env::var("SHREDWIRE_LEADERS"),
    ) else {
        eprintln!("skipped, set SHREDWIRE_PCAP and SHREDWIRE_LEADERS to run it");
        return;
    };
    let file = File::open(pcap).unwrap();
    let mut source = PcapReader::new(BufReader::new(file)).unwrap();
    let hub = Hub::new(8192);
    let mut updates = hub.subscribe();
    server::source::ingest(&mut source, Path::new(&leaders), &hub).unwrap();

    let mut seqs = Vec::new();
    let mut signatures = HashSet::new();
    while let Ok(update) = updates.try_recv() {
        seqs.push(update.seq);
        let Some(Kind::Tx(tx)) = update.kind else {
            panic!("only txs come from ingest");
        };
        // the bytes sent decode back to the transaction, its first signature the one sent with it
        let transaction: VersionedTransaction = wincode::deserialize(&tx.transaction).unwrap();
        assert_eq!(transaction.signatures[0].as_ref(), tx.signature.as_slice());
        assert!(tx.first_shred_ns > 0 && tx.first_shred_ns <= tx.decoded_ns);
        assert!(signatures.insert(tx.signature));
    }
    assert!(!signatures.is_empty());
    if let Ok(expected) = std::env::var("SHREDWIRE_TXS") {
        assert_eq!(signatures.len(), expected.parse::<usize>().unwrap());
    }
    assert!(
        seqs.windows(2).all(|w| w[1] == w[0] + 1),
        "seq has no holes"
    );
    assert!(hub.last_slot() > 0);
}

#[test]
fn ingest_refuses_without_leaders() {
    let empty = std::env::temp_dir().join(format!("shredwire-no-leaders-{}", std::process::id()));
    std::fs::create_dir_all(&empty).unwrap();
    // a capture header with no packets is enough, the schedule is checked first
    let header = [
        0x4d, 0x3c, 0xb2, 0xa1, 2, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0,
    ];
    let mut source = PcapReader::new(&header[..]).unwrap();
    let result = server::source::ingest(&mut source, &empty, &Hub::new(16));
    assert!(matches!(result, Err(server::Error::NoLeaders(_))));
    std::fs::remove_dir_all(&empty).unwrap();
}
