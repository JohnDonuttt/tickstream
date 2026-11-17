//! End-to-end tests against a real server on a random local port.
//!
//! The simulator is not started here: the tests publish ticks into the hub
//! themselves so they control exactly what is sent and when.

use std::net::SocketAddr;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tickstream::ticker::{default_symbols, Tick};
use tickstream::{router, Config, Hub};
use tokio::net::{TcpSocket, TcpStream};
use tokio::time::{sleep, timeout, Instant};
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type Client = WebSocketStream<MaybeTlsStream<TcpStream>>;

const WAIT: Duration = Duration::from_secs(10);

async fn start(config: Config) -> (Hub, SocketAddr) {
    let symbols = default_symbols().iter().map(|s| s.symbol).collect();
    let hub = Hub::new(config, symbols);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = router(hub.clone());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (hub, addr)
}

async fn connect(addr: SocketAddr) -> Client {
    let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();
    ws
}

/// Next text message as JSON, skipping control frames.
async fn next_json(ws: &mut Client) -> Value {
    loop {
        let msg = timeout(WAIT, ws.next())
            .await
            .expect("timed out waiting for a message")
            .expect("stream ended")
            .expect("websocket error");
        if let Message::Text(text) = msg {
            return serde_json::from_str(text.as_str()).unwrap();
        }
    }
}

async fn expect_hello(ws: &mut Client) {
    let hello = next_json(ws).await;
    assert_eq!(hello["type"], "hello");
    assert_eq!(
        hello["symbols"],
        serde_json::json!(["BTC-USD", "ETH-USD", "SOL-USD"])
    );
}

fn tick(symbol: &'static str, seq: u64) -> Tick {
    Tick {
        symbol,
        price: 100.0 + seq as f64 / 100.0,
        seq,
        ts: seq,
    }
}

async fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + WAIT;
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test]
async fn every_client_gets_every_tick_in_order() {
    let (hub, addr) = start(Config::default()).await;
    let mut a = connect(addr).await;
    let mut b = connect(addr).await;
    // The server subscribes to the feed before sending hello, so once both
    // hellos are in, nothing published from here on can be missed.
    expect_hello(&mut a).await;
    expect_hello(&mut b).await;

    let symbols = ["BTC-USD", "ETH-USD", "SOL-USD"];
    for seq in 1..=150u64 {
        assert_eq!(hub.publish(&tick(symbols[(seq % 3) as usize], seq)), 2);
    }

    for client in [&mut a, &mut b] {
        for seq in 1..=150u64 {
            let msg = next_json(client).await;
            assert_eq!(msg["type"], "tick");
            assert_eq!(msg["seq"], seq);
            assert_eq!(msg["symbol"], symbols[(seq % 3) as usize]);
        }
    }

    let stats = hub.stats();
    assert_eq!(stats.connected, 2);
    assert_eq!(stats.slow_disconnects(), 0);
}

#[tokio::test]
async fn subscribe_filters_per_client() {
    let (hub, addr) = start(Config::default()).await;
    let mut eth_only = connect(addr).await;
    let mut everything = connect(addr).await;
    expect_hello(&mut eth_only).await;
    expect_hello(&mut everything).await;

    eth_only
        .send(Message::text(
            r#"{"type":"subscribe","symbols":["ETH-USD"]}"#,
        ))
        .await
        .unwrap();
    let ack = next_json(&mut eth_only).await;
    assert_eq!(
        ack,
        serde_json::json!({"type":"subscribed","symbols":["ETH-USD"]})
    );

    hub.publish(&tick("BTC-USD", 1));
    hub.publish(&tick("ETH-USD", 2));
    hub.publish(&tick("SOL-USD", 3));
    hub.publish(&tick("ETH-USD", 4));

    assert_eq!(next_json(&mut eth_only).await["seq"], 2);
    assert_eq!(next_json(&mut eth_only).await["seq"], 4);
    for seq in 1..=4 {
        assert_eq!(next_json(&mut everything).await["seq"], seq);
    }

    // Unknown symbols are rejected and leave the filter alone.
    eth_only
        .send(Message::text(
            r#"{"type":"subscribe","symbols":["DOGE-USD"]}"#,
        ))
        .await
        .unwrap();
    let err = next_json(&mut eth_only).await;
    assert_eq!(err["type"], "error");
    assert_eq!(err["message"], "unknown symbol: DOGE-USD");
    hub.publish(&tick("BTC-USD", 5));
    hub.publish(&tick("ETH-USD", 6));
    assert_eq!(next_json(&mut eth_only).await["seq"], 6);
}

/// A client that stops reading must be cut off with a policy close frame,
/// while a client that keeps up stays connected and keeps receiving.
#[tokio::test]
async fn slow_consumer_is_disconnected_and_others_carry_on() {
    let config = Config {
        client_queue: 256,
        broadcast_capacity: 65_536,
        close_grace: Duration::from_secs(10),
        ..Config::default()
    };
    let (hub, addr) = start(config).await;

    // The slow client asks for a small receive buffer so the TCP path fills
    // quickly. It reads the hello and then stops reading.
    let socket = TcpSocket::new_v4().unwrap();
    socket.set_recv_buffer_size(4096).unwrap();
    let stream = socket.connect(addr).await.unwrap();
    let (mut slow, _) =
        tokio_tungstenite::client_async(format!("ws://{addr}/ws"), MaybeTlsStream::Plain(stream))
            .await
            .unwrap();
    expect_hello(&mut slow).await;

    let mut healthy = connect(addr).await;
    expect_hello(&mut healthy).await;

    const MARKER: u64 = u64::MAX;
    let reader = tokio::spawn(async move {
        let mut ticks = 0u64;
        loop {
            let msg = next_json(&mut healthy).await;
            if msg["type"] == "tick" {
                ticks += 1;
                if msg["seq"] == MARKER {
                    return ticks;
                }
            }
        }
    });

    // Publish at a steady pace until the server reports a slow disconnect.
    let started = Instant::now();
    let mut seq = 0u64;
    while hub.stats().slow_disconnects() == 0 {
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "slow client was never disconnected"
        );
        for _ in 0..16 {
            seq += 1;
            hub.publish(&tick("BTC-USD", seq));
        }
        sleep(Duration::from_millis(1)).await;
    }
    let stats = hub.stats();
    assert_eq!(stats.dropped_queue_full, 1, "{stats:?}");
    assert_eq!(stats.dropped_lagged, 0, "{stats:?}");

    // Now drain the slow client: buffered ticks, then the close frame.
    let close = timeout(WAIT, async {
        while let Some(msg) = slow.next().await {
            match msg {
                Ok(Message::Close(frame)) => return frame,
                Ok(_) => continue,
                Err(e) => panic!("connection broke before the close frame: {e}"),
            }
        }
        panic!("stream ended without a close frame");
    })
    .await
    .expect("timed out draining the slow client")
    .expect("close frame has a code");
    assert_eq!(close.code, CloseCode::Policy);
    assert_eq!(close.reason.as_str(), "slow consumer: send queue full");

    // The healthy client is still connected and still receiving.
    wait_until("slow client to be gone", || hub.stats().connected == 1).await;
    hub.publish(&tick("ETH-USD", MARKER));
    let ticks = timeout(WAIT, reader).await.unwrap().unwrap();
    assert_eq!(ticks, seq + 1, "healthy client missed ticks");
    assert_eq!(hub.stats().slow_disconnects(), 1);
}

#[tokio::test]
async fn serves_the_browser_client_and_stats() {
    let (hub, addr) = start(Config::default()).await;
    hub.publish(&tick("BTC-USD", 1));

    let index = http_get(addr, "/").await;
    assert!(index.starts_with("HTTP/1.1 200"), "{index}");
    assert!(index.contains("<canvas id=\"chart\""));

    let stats = http_get(addr, "/stats").await;
    assert!(stats.starts_with("HTTP/1.1 200"), "{stats}");
    let body = stats.split("\r\n\r\n").nth(1).unwrap();
    let v: Value = serde_json::from_str(body).unwrap();
    assert_eq!(v["published"], 1);
    assert_eq!(v["connected"], 0);
}

/// Minimal HTTP/1.1 GET so the tests don't need an HTTP client crate.
async fn http_get(addr: SocketAddr, path: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut s = TcpStream::connect(addr).await.unwrap();
    let req = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
    s.write_all(req.as_bytes()).await.unwrap();
    let mut buf = String::new();
    s.read_to_string(&mut buf).await.unwrap();
    buf
}
