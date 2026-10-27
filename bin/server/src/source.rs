//! What fills the hub: shreds from a network interface or a pcap file, or made-up txs.
//!
//! Shreds go through shred-core's assembler in emit_early mode on a thread of their own,
//! so every transaction is sent the moment its batch decodes. Each one is checked first:
//! its shred's Merkle proof and the slot leader's signature, so a leader schedule is required.
//! The schedule folder is read again every minute of capture time, so a new epoch needs no restart.

use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use shred_core::assembler::{Assembler, Event};
use shred_core::leaders::LeaderDir;
use shred_core::live::LiveCapture;
use shred_core::pcap::{PacketSource, PcapReader, udp_payload};
use shredwire_proto::Tx;
use shredwire_proto::subscribe_response::Kind;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::filter::TxKeys;
use crate::hub::Hub;
use crate::{Error, wall_ns};

/// How often the schedule folder is looked at, on the capture clock.
const LEADER_CHECK_NS: u64 = 60_000_000_000;
/// The made-up producer wakes this often and sends every tx that came due since.
const TICK: Duration = Duration::from_millis(1);
const SLOT_NS: u128 = 200_000_000;

pub enum Source {
    /// Shreds read live from a network interface. Needs root or cap_net_raw and cap_net_admin.
    Live { interface: String, leaders: PathBuf },
    /// Shreds read from a pcap file as fast as it reads, then nothing more.
    File { path: PathBuf, leaders: PathBuf },
    /// Made-up transactions at a fixed rate, with no real contents.
    Made { tx_per_sec: u64, tx_bytes: usize },
}

/// A running source. Dropping it stops made-up txs, a shred thread runs until the process ends.
pub(crate) struct Running {
    /// Gets the shred thread's error if it stops with one.
    pub(crate) done: oneshot::Receiver<Result<(), Error>>,
    made: Option<JoinHandle<()>>,
}

impl Drop for Running {
    fn drop(&mut self) {
        if let Some(made) = &self.made {
            made.abort();
        }
    }
}

/// Opens the source and starts filling the hub.
pub(crate) fn start(source: Source, hub: Arc<Hub>) -> Result<Running, Error> {
    let (done_tx, done) = oneshot::channel();
    match source {
        Source::Live { interface, leaders } => {
            let capture = LiveCapture::open(&interface)?;
            spawn_ingest(capture, leaders, hub, done_tx)?;
        }
        Source::File { path, leaders } => {
            let reader = PcapReader::new(BufReader::new(File::open(path)?))?;
            spawn_ingest(reader, leaders, hub, done_tx)?;
        }
        Source::Made {
            tx_per_sec,
            tx_bytes,
        } => {
            let made = tokio::spawn(made_up(hub, tx_per_sec, tx_bytes));
            return Ok(Running {
                done,
                made: Some(made),
            });
        }
    }
    Ok(Running { done, made: None })
}

fn spawn_ingest(
    mut source: impl PacketSource + Send + 'static,
    leaders: PathBuf,
    hub: Arc<Hub>,
    done: oneshot::Sender<Result<(), Error>>,
) -> Result<(), Error> {
    std::thread::Builder::new()
        .name("ingest".into())
        .spawn(move || {
            let result = ingest(&mut source, &leaders, &hub);
            let _ = done.send(result);
        })?;
    Ok(())
}

/// Feeds every packet through the assembler and sends each transaction, until the source ends.
pub fn ingest(source: &mut dyn PacketSource, leaders: &Path, hub: &Hub) -> Result<(), Error> {
    let mut dir = LeaderDir::new(leaders);
    let schedule = dir
        .changed()?
        .filter(|schedule| !schedule.is_empty())
        .ok_or_else(|| Error::NoLeaders(leaders.to_path_buf()))?;
    eprintln!("leader schedule: {} slots", schedule.len());

    let mut assembler = Assembler::new().with_leaders(schedule).emit_early();

    let mut next_leader_check_ns = 0;
    while let Some(record) = source.next_record()? {
        if next_leader_check_ns == 0 {
            next_leader_check_ns = record.recv_ns + LEADER_CHECK_NS;
        } else if record.recv_ns >= next_leader_check_ns {
            next_leader_check_ns = record.recv_ns + LEADER_CHECK_NS;
            reload_leaders(&mut dir, &mut assembler);
        }
        if let Some(payload) = udp_payload(&record.frame) {
            for event in assembler.push(payload, record.recv_ns) {
                publish(hub, event);
            }
        }
    }
    for event in assembler.finish() {
        publish(hub, event);
    }
    Ok(())
}

/// Loads the folder again if it changed. A folder that can't be read keeps the schedule loaded.
fn reload_leaders(dir: &mut LeaderDir, assembler: &mut Assembler) {
    match dir.changed() {
        Ok(Some(leaders)) if !leaders.is_empty() => {
            eprintln!("leader schedule: {} slots", leaders.len());
            assembler.set_leaders(leaders);
        }
        Ok(_) => {}
        Err(e) => eprintln!("leader schedule: {e}, keeping the one loaded"),
    }
}

/// Sends a transaction to the hub. Markers, sets and slots are not sent.
fn publish(hub: &Hub, event: Event) {
    // every slot starts with a header marker, so a slot with no txs still moves last_slot
    if let Event::Marker { slot, .. } = event {
        hub.saw_slot(slot);
        return;
    }
    let Event::Tx {
        slot,
        signature,
        transaction,
        raw,
        entry,
        position,
        timing,
        rebuilt,
    } = event
    else {
        return;
    };
    hub.saw_slot(slot);
    let keys = TxKeys::of(&transaction);
    let tx = Tx {
        slot,
        entry,
        position,
        signature: signature.as_ref().to_vec(),
        // the bytes as the leader encoded them, no need to encode again
        transaction: raw,
        rebuilt,
        first_shred_ns: timing.first_shred_ns,
        decoded_ns: timing.decoded_ns,
        emitted_ns: wall_ns(),
    };
    hub.send(Kind::Tx(tx), Some(keys));
}

/// Sends made-up txs at a fixed rate, forever.
async fn made_up(hub: Arc<Hub>, tx_per_sec: u64, tx_bytes: usize) {
    let start = Instant::now();
    let mut ticks = tokio::time::interval(TICK);
    let mut txs_sent: u64 = 0;
    let mut slot = 0;
    let mut in_slot = 0;

    loop {
        ticks.tick().await;
        let elapsed = start.elapsed();
        let now_slot = (elapsed.as_nanos() / SLOT_NS) as u64;
        if now_slot != slot {
            slot = now_slot;
            in_slot = 0;
            hub.saw_slot(slot);
        }

        let due = (elapsed.as_nanos() * u128::from(tx_per_sec) / 1_000_000_000) as u64;
        while txs_sent < due {
            let tx = made_up_tx(slot, in_slot, txs_sent, tx_bytes);
            hub.send(Kind::Tx(tx), Some(TxKeys::default()));
            txs_sent += 1;
            in_slot += 1;
        }
    }
}

/// A tx with the right shape and no real contents. The signature holds its number.
fn made_up_tx(slot: u64, position: u32, number: u64, tx_bytes: usize) -> Tx {
    let mut signature = vec![0; 64];
    signature[..8].copy_from_slice(&number.to_le_bytes());
    let now = wall_ns();
    Tx {
        slot,
        entry: 0,
        position,
        signature,
        transaction: vec![0; tx_bytes],
        rebuilt: false,
        first_shred_ns: now,
        decoded_ns: now,
        emitted_ns: now,
    }
}
