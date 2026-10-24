use std::net::SocketAddr;

use clap::Parser;
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
    /// Made-up transactions per second.
    #[arg(long, default_value_t = 1000)]
    tx_per_sec: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let listener = TcpListener::bind(args.listen).await?;
    let config = server::Config {
        tokens: args.tokens,
        tx_per_sec: args.tx_per_sec,
    };
    server::serve(listener, config).await?;
    Ok(())
}
