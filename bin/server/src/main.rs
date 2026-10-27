use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use shredwire_server::Source;
use tokio::net::TcpListener;

/// Streams shredwire updates over gRPC.
#[derive(Parser)]
struct Args {
    /// Address to listen on.
    #[arg(long, default_value = "127.0.0.1:50051")]
    listen: SocketAddr,
    /// An API key allowed to subscribe, repeat for more.
    #[arg(long = "token", required = true)]
    tokens: Vec<String>,
    /// Read shreds live from this network interface.
    #[arg(long, conflicts_with = "pcap", requires = "leaders")]
    interface: Option<String>,
    /// Read shreds from this pcap file.
    #[arg(long, requires = "leaders")]
    pcap: Option<PathBuf>,
    /// Folder of leader schedules, one solana leader-schedule output per epoch, ending in .txt.
    #[arg(long)]
    leaders: Option<PathBuf>,
    /// Without --interface or --pcap, made-up transactions per second.
    #[arg(long, default_value_t = 1000)]
    tx_per_sec: u64,
    /// Size of each made-up transaction.
    #[arg(long, default_value_t = 0)]
    tx_bytes: usize,
}

#[tokio::main]
async fn main() -> ExitCode {
    match run(Args::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let (source, from) = match (args.interface, args.pcap, args.leaders) {
        (Some(interface), _, Some(leaders)) => {
            let from = format!("capturing {interface}");
            (Source::Live { interface, leaders }, from)
        }
        (_, Some(path), Some(leaders)) => {
            let from = format!("reading {}", path.display());
            (Source::File { path, leaders }, from)
        }
        _ => {
            let from = format!("{} made-up txs a second", args.tx_per_sec);
            let source = Source::Made {
                tx_per_sec: args.tx_per_sec,
                tx_bytes: args.tx_bytes,
            };
            (source, from)
        }
    };
    let listener = TcpListener::bind(args.listen).await?;
    eprintln!("listening on {}, {from}", args.listen);
    let config = shredwire_server::Config {
        tokens: args.tokens,
        source,
    };
    shredwire_server::serve(listener, config).await?;
    Ok(())
}
