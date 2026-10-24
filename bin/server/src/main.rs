use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use server::Source;
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
    let source = match (args.interface, args.pcap, args.leaders) {
        (Some(interface), _, Some(leaders)) => Source::Live { interface, leaders },
        (_, Some(path), Some(leaders)) => Source::File { path, leaders },
        _ => Source::Made {
            tx_per_sec: args.tx_per_sec,
            tx_bytes: args.tx_bytes,
        },
    };
    let listener = TcpListener::bind(args.listen).await?;
    let config = server::Config {
        tokens: args.tokens,
        source,
    };
    server::serve(listener, config).await?;
    Ok(())
}
