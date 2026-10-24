//! T2. Reconnect, gaps and resets.
//! (1) The connection drops, the server keeps running: every seq is either received
//!     or inside a reported gap, exactly once. Nothing missing, nothing invented.
//! (2) The server restarts: the client reports a reset, never a bogus backwards gap.

use std::net::SocketAddr;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::Duration;

use client::Event;
use proto::TxFilter;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, mpsc};
use tokio::time::timeout;

const KEY: &str = "right-key";

#[tokio::test]
async fn t2_connection_drop_reports_exact_gap() {
    let server_addr = free_addr();
    let _server = Server::start(server_addr);
    let proxy = Proxy::start(server_addr).await;
    let mut events = start_client(proxy.addr);

    let mut seqs = Vec::new();
    let mut gaps: Vec<RangeInclusive<u64>> = Vec::new();
    let run = async {
        while let Some(event) = events.recv().await {
            match event {
                Event::Update(update) => {
                    seqs.push(update.seq);
                    if seqs.len() == 50 {
                        proxy.cut();
                    }
                    if !gaps.is_empty() && seqs.len() >= 150 {
                        break;
                    }
                }
                Event::Gap { from, to } => gaps.push(from..=to),
                Event::Reset { .. } => panic!("same server, same epoch, no reset"),
                _ => {}
            }
        }
    };
    timeout(Duration::from_secs(10), run)
        .await
        .expect("client must reconnect and report the gap");

    // the client waits out its backoff before reconnecting, and the server keeps producing meanwhile
    assert_eq!(gaps.len(), 1, "one drop, one gap");
    assert!(seqs.windows(2).all(|w| w[0] < w[1]), "seq must only grow");
    let mut covered = seqs.clone();
    covered.extend(gaps.iter().flat_map(|gap| gap.clone()));
    covered.sort_unstable();
    let expected: Vec<u64> = (seqs[0]..=*seqs.last().unwrap()).collect();
    assert_eq!(
        covered, expected,
        "every seq received or inside the reported gap, exactly once"
    );
}

#[tokio::test]
async fn t2_server_restart_reports_reset_not_gap() {
    let addr = free_addr();
    let server = Server::start(addr);
    let mut events = start_client(addr);

    let mut epochs = Vec::new();
    let mut resets = Vec::new();
    let mut after_reset = 0;
    let mut server = Some(server);
    let run = async {
        let mut updates = 0;
        while let Some(event) = events.recv().await {
            match event {
                Event::Connected { epoch } => epochs.push(epoch),
                Event::Update(_) => {
                    updates += 1;
                    if updates == 20 {
                        drop(server.take());
                        server = Some(Server::start(addr));
                    }
                    if !resets.is_empty() {
                        after_reset += 1;
                        if after_reset == 20 {
                            break;
                        }
                    }
                }
                Event::Reset { old, new } => resets.push((old, new)),
                Event::Gap { from, to } => panic!("a restart is a reset, not a gap {from}..={to}"),
            }
        }
    };
    timeout(Duration::from_secs(10), run)
        .await
        .expect("client must reconnect to the new server");

    assert_eq!(epochs.len(), 2, "two servers, two connects");
    assert_ne!(epochs[0], epochs[1], "a new boot gets a new epoch");
    assert_eq!(resets, vec![(epochs[0].clone(), epochs[1].clone())]);
}

/// Subscribes to everything, so heartbeats and txs together use every seq.
fn start_client(addr: SocketAddr) -> mpsc::UnboundedReceiver<Event> {
    let (tx, rx) = mpsc::unbounded_channel();
    let config = client::Config {
        url: format!("http://{addr}"),
        token: KEY.into(),
        filters: [("all".into(), TxFilter::default())].into(),
    };
    tokio::spawn(client::run(config, tx));
    rx
}

fn free_addr() -> SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}

/// A server on its own runtime, so dropping it kills it and every connection, like a crash.
struct Server(Option<tokio::runtime::Runtime>);

impl Server {
    fn start(addr: SocketAddr) -> Self {
        // the old server's socket may take a moment to close after a restart
        let listener = (0..100)
            .find_map(|_| {
                std::net::TcpListener::bind(addr)
                    .inspect_err(|_| std::thread::sleep(Duration::from_millis(10)))
                    .ok()
            })
            .expect("port must free up");
        listener.set_nonblocking(true).unwrap();
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        runtime.spawn(async move {
            let config = server::Config {
                tokens: vec![KEY.into()],
                tx_per_sec: 1000,
            };
            server::serve(TcpListener::from_std(listener).unwrap(), config).await
        });
        Server(Some(runtime))
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(runtime) = self.0.take() {
            runtime.shutdown_background();
        }
    }
}

/// A TCP proxy in front of the server. cut() kills the live connections, new ones are still accepted.
struct Proxy {
    addr: SocketAddr,
    cut: Arc<Notify>,
}

impl Proxy {
    async fn start(upstream: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let cut = Arc::new(Notify::new());
        let signal = cut.clone();
        tokio::spawn(async move {
            while let Ok((mut downstream, _)) = listener.accept().await {
                let signal = signal.clone();
                tokio::spawn(async move {
                    let mut up = TcpStream::connect(upstream).await.unwrap();
                    tokio::select! {
                        _ = tokio::io::copy_bidirectional(&mut downstream, &mut up) => {}
                        _ = signal.notified() => {}
                    }
                });
            }
        });
        Proxy { addr, cut }
    }

    fn cut(&self) {
        self.cut.notify_waiters();
    }
}
