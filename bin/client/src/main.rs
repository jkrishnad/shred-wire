use std::process::ExitCode;

use clap::Parser;
use shredwire_proto::TxFilter;
use shredwire_proto::subscribe_response::Kind;
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

    while let Some(event) = received.recv().await {
        match event {
            Event::Update(update) => match update.kind {
                Some(Kind::Tx(tx)) => println!(
                    "{} tx slot {} position {}",
                    update.seq, tx.slot, tx.position
                ),
                Some(Kind::Heartbeat(beat)) => {
                    println!("{} heartbeat slot {}", update.seq, beat.last_slot)
                }
                None => {}
            },
            Event::Retrying { reason, after } => {
                eprintln!("not connected, {reason}, retrying in {after:?}")
            }
            Event::Stopped { reason } => {
                eprintln!("stopped, {reason}");
                return ExitCode::FAILURE;
            }
            other => println!("{other:?}"),
        }
    }
    ExitCode::SUCCESS
}
