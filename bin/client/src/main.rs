use std::io::IsTerminal;
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::Parser;
use shredwire_proto::subscribe_response::Kind;
use shredwire_proto::{Heartbeat, Tx, TxFilter};
use tokio::sync::mpsc;

use shredwire_client::Event;

/// Subscribes to a shredwire server and prints what arrives.
#[derive(Parser)]
struct Args {
    /// Server address.
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    url: String,
    /// API key.
    #[arg(long)]
    token: String,
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    let config = shredwire_client::Config {
        url: args.url,
        token: args.token,
        filters: [("all".into(), TxFilter::default())].into(),
    };
    let (events, mut received) = mpsc::unbounded_channel();
    tokio::spawn(shredwire_client::run(config, events));

    // colors only on a terminal, so a file or pipe gets plain text
    let paint = Paint(std::io::stdout().is_terminal() && std::env::var_os("NO_COLOR").is_none());
    while let Some(event) = received.recv().await {
        let now = paint.dim(&clock());
        match event {
            Event::Update(update) => match update.kind {
                Some(Kind::Tx(tx)) => println!(
                    "{now}  {}  {}",
                    paint.dim(&format!("{:>8}", update.seq)),
                    tx_line(&paint, &tx)
                ),
                Some(Kind::Heartbeat(beat)) => println!(
                    "{now}  {}  {}",
                    paint.dim(&format!("{:>8}", update.seq)),
                    heartbeat_line(&paint, &beat)
                ),
                None => {}
            },
            Event::Connected { epoch } => println!(
                "{now}  {}",
                paint.color(GREEN, &format!("connected, server boot {epoch}"))
            ),
            Event::Gap { from, to } => println!(
                "{now}  {}",
                paint.color(
                    RED,
                    &format!("missed {} updates, seq {from} to {to}", to - from + 1)
                )
            ),
            Event::Reset { old, new } => println!(
                "{now}  {}",
                paint.color(
                    MAGENTA,
                    &format!("server restarted, boot {old} is now {new}")
                )
            ),
            Event::Retrying { reason, after } => {
                eprintln!(
                    "{now}  {}",
                    paint.color(
                        YELLOW,
                        &format!("not connected, {reason}, retrying in {after:?}")
                    )
                )
            }
            Event::Stopped { reason } => {
                eprintln!("{now}  {}", paint.color(RED, &format!("stopped, {reason}")));
                return ExitCode::FAILURE;
            }
        }
    }
    ExitCode::SUCCESS
}

fn tx_line(paint: &Paint, tx: &Tx) -> String {
    let signature = bs58::encode(&tx.signature).into_string();
    let short = signature.get(..16).unwrap_or(&signature);
    // how long the batch waited for its shreds, both packet arrival times, so a replay reads the same
    let ms = tx.decoded_ns.saturating_sub(tx.first_shred_ns) as f64 / 1e6;
    let speed = match ms {
        ms if ms < 50.0 => GREEN,
        ms if ms < 200.0 => YELLOW,
        _ => RED,
    };
    let rebuilt = if tx.rebuilt {
        // most batches decode with backup shreds, so this is the usual case and stays quiet
        paint.dim("rebuilt")
    } else {
        paint.wrap("1;34", "on wire")
    };
    format!(
        "{}  {}  {}  {}  {}  {}  {}",
        slot(paint, tx.slot),
        paint.color(CYAN, "tx"),
        paint.bold(&format!("{short}..")),
        paint.dim(&format!("entry {:>4} pos {:>3}", tx.entry, tx.position)),
        paint.dim(&format!("{:>5} B", tx.transaction.len())),
        rebuilt,
        paint.color(speed, &format!("{ms:>7.1} ms to decode")),
    )
}

fn heartbeat_line(paint: &Paint, beat: &Heartbeat) -> String {
    // the server sends 0 until its first slot
    let slot = match beat.last_slot {
        0 => paint.dim("no slot yet   "),
        last => slot(paint, last),
    };
    format!("{slot}  {}", paint.dim("heartbeat"))
}

/// Each slot gets its own color, so the txs of one slot stand together.
fn slot(paint: &Paint, slot: u64) -> String {
    const SLOT_COLORS: [u8; 6] = [BLUE, MAGENTA, GREEN, YELLOW, CYAN, RED];
    let color = SLOT_COLORS[(slot % SLOT_COLORS.len() as u64) as usize];
    paint.color(color, &format!("slot {slot}"))
}

/// Wall clock as HH:MM:SS.mmm in UTC.
fn clock() -> String {
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_millis());
    let secs = ms / 1000 % 86_400;
    format!(
        "{:02}:{:02}:{:02}.{:03}",
        secs / 3600,
        secs / 60 % 60,
        secs % 60,
        ms % 1000
    )
}

const RED: u8 = 31;
const GREEN: u8 = 32;
const YELLOW: u8 = 33;
const BLUE: u8 = 34;
const MAGENTA: u8 = 35;
const CYAN: u8 = 36;

/// ANSI colors, or plain text when off.
struct Paint(bool);

impl Paint {
    fn color(&self, code: u8, text: &str) -> String {
        self.wrap(&code.to_string(), text)
    }

    fn bold(&self, text: &str) -> String {
        self.wrap("1", text)
    }

    fn dim(&self, text: &str) -> String {
        self.wrap("2", text)
    }

    fn wrap(&self, code: &str, text: &str) -> String {
        if self.0 {
            format!("\x1b[{code}m{text}\x1b[0m")
        } else {
            text.to_string()
        }
    }
}
