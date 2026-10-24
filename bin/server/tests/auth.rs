//! T1. Auth: no key and a wrong key are refused before any data, the right key streams.

use std::time::Duration;

use proto::TOKEN_HEADER;
use proto::feed_service_client::FeedServiceClient;
use proto::{SubscribeRequest, TxFilter};
use tonic::{Code, Request};

const KEY: &str = "right-key";

#[tokio::test]
async fn t1_auth() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let config = server::Config {
        tokens: vec![KEY.into()],
        tx_per_sec: 1000,
    };
    tokio::spawn(server::serve(listener, config));
    let mut client = FeedServiceClient::connect(url).await.unwrap();

    // refused at the call itself: no response headers, no stream, so no data can leak
    for token in [None, Some("wrong-key")] {
        let status = client
            .subscribe(subscribe_all(token))
            .await
            .expect_err("must be refused");
        assert_eq!(status.code(), Code::Unauthenticated, "token {token:?}");
    }

    let mut stream = client
        .subscribe(subscribe_all(Some(KEY)))
        .await
        .unwrap()
        .into_inner();
    let first = tokio::time::timeout(Duration::from_secs(2), stream.message()).await;
    assert!(
        matches!(first, Ok(Ok(Some(_)))),
        "right key must stream, got {first:?}"
    );
}

fn subscribe_all(token: Option<&str>) -> Request<SubscribeRequest> {
    let filters = [("all".into(), TxFilter::default())].into();
    let mut request = Request::new(SubscribeRequest { filters });
    if let Some(token) = token {
        request
            .metadata_mut()
            .insert(TOKEN_HEADER, token.parse().unwrap());
    }
    request
}
