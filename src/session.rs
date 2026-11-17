//! One WebSocket connection = three futures in one task:
//!
//! - forwarder: broadcast feed -> symbol filter -> `try_send` into the
//!   client's bounded queue. Never waits on the client.
//! - writer: bounded queue -> socket. The only part that waits on the network.
//! - reader: handles `subscribe` and notices when the client goes away.
//!
//! Whichever finishes first decides how the connection ends.

use std::collections::BTreeSet;
use std::sync::Arc;

use axum::extract::ws::{close_code, CloseFrame, Message, Utf8Bytes, WebSocket};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{broadcast, mpsc, watch};
use tokio::time::timeout;

use crate::hub::{Hub, Published, SlowReason};
use crate::protocol::{ClientMessage, ServerMessage};

/// Which symbols a client wants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Filter {
    All,
    Only(BTreeSet<&'static str>),
}

impl Filter {
    pub fn matches(&self, symbol: &str) -> bool {
        match self {
            Filter::All => true,
            Filter::Only(set) => set.contains(symbol),
        }
    }
}

/// How the forwarder stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForwardEnd {
    Slow(SlowReason),
    ClientGone,
    FeedClosed,
}

/// Moves ticks from the shared feed into one client's bounded queue.
///
/// This is the backpressure policy in one place: it never awaits the client.
/// If the client's queue is full, or the client fell so far behind that the
/// broadcast ring buffer overwrote unread ticks, the client is cut off rather
/// than slowing down the publisher or eating memory.
pub async fn forward(
    feed: &mut broadcast::Receiver<Arc<Published>>,
    filter: &watch::Receiver<Filter>,
    out: &mpsc::Sender<Utf8Bytes>,
) -> ForwardEnd {
    loop {
        let msg = tokio::select! {
            biased;
            () = out.closed() => return ForwardEnd::ClientGone,
            msg = feed.recv() => msg,
        };
        match msg {
            Ok(published) => {
                if !filter.borrow().matches(published.symbol) {
                    continue;
                }
                match out.try_send(published.payload.clone()) {
                    Ok(()) => {}
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        return ForwardEnd::Slow(SlowReason::QueueFull)
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => return ForwardEnd::ClientGone,
                }
            }
            Err(broadcast::error::RecvError::Lagged(missed)) => {
                return ForwardEnd::Slow(SlowReason::Lagged(missed))
            }
            Err(broadcast::error::RecvError::Closed) => return ForwardEnd::FeedClosed,
        }
    }
}

/// Turns a `subscribe` request into a filter, rejecting unknown symbols.
pub fn resolve_filter(
    requested: Option<Vec<String>>,
    known: &[&'static str],
) -> Result<Filter, String> {
    let Some(requested) = requested else {
        return Ok(Filter::All);
    };
    let mut set = BTreeSet::new();
    for name in requested {
        match known.iter().find(|k| **k == name) {
            Some(k) => {
                set.insert(*k);
            }
            None => return Err(format!("unknown symbol: {name}")),
        }
    }
    Ok(Filter::Only(set))
}

fn close_reason(reason: SlowReason) -> String {
    match reason {
        SlowReason::Lagged(n) => format!("slow consumer: fell {n} messages behind the feed"),
        SlowReason::QueueFull => "slow consumer: send queue full".to_string(),
    }
}

/// Drives one upgraded WebSocket until it ends.
pub async fn run(socket: WebSocket, hub: Hub) {
    let _guard = hub.connection_guard();
    let cfg = hub.config().clone();
    let (mut sink, mut stream) = socket.split();

    // subscribe before hello so nothing published after hello can be missed
    let mut feed = hub.subscribe();
    let (out_tx, mut out_rx) = mpsc::channel::<Utf8Bytes>(cfg.client_queue);
    let (filter_tx, filter_rx) = watch::channel(Filter::All);

    let hello = ServerMessage::Hello {
        symbols: hub.symbols(),
    }
    .to_json();
    if sink.send(Message::Text(hello.into())).await.is_err() {
        return;
    }

    let writer = async {
        while let Some(text) = out_rx.recv().await {
            if sink.send(Message::Text(text)).await.is_err() {
                break;
            }
        }
    };

    let reader = async {
        while let Some(Ok(msg)) = stream.next().await {
            match msg {
                Message::Text(text) => {
                    let reply = match serde_json::from_str::<ClientMessage>(text.as_str()) {
                        Ok(ClientMessage::Subscribe { symbols }) => {
                            match resolve_filter(symbols, hub.symbols()) {
                                Ok(filter) => {
                                    let symbols = match &filter {
                                        Filter::All => hub.symbols().to_vec(),
                                        Filter::Only(set) => set.iter().copied().collect(),
                                    };
                                    filter_tx.send_replace(filter);
                                    ServerMessage::Subscribed { symbols }
                                }
                                Err(message) => ServerMessage::Error { message },
                            }
                        }
                        Err(e) => ServerMessage::Error {
                            message: format!("bad message: {e}"),
                        },
                    };
                    // a full queue means the forwarder is about to drop us anyway
                    let _ = out_tx.try_send(reply.to_json().into());
                }
                Message::Close(_) => break,
                // pings are answered by tungstenite
                _ => {}
            }
        }
    };

    let forwarder = forward(&mut feed, &filter_rx, &out_tx);

    let ended = tokio::select! {
        end = forwarder => Some(end),
        () = reader => None,
        () = writer => None,
    };

    // the select dropped all three futures, so the sink is free again
    let frame = match ended {
        Some(ForwardEnd::Slow(reason)) => {
            hub.record_slow(reason);
            tracing::warn!(?reason, "disconnecting slow consumer");
            Some(CloseFrame {
                code: close_code::POLICY,
                reason: close_reason(reason).into(),
            })
        }
        Some(ForwardEnd::FeedClosed) => Some(CloseFrame {
            code: close_code::AWAY,
            reason: "feed closed".into(),
        }),
        Some(ForwardEnd::ClientGone) | None => None,
    };

    // A slow client may have stopped reading entirely, so the close frame
    // (and anything still buffered ahead of it) gets a deadline. After that we
    // just drop the TCP connection.
    let goodbye = async {
        if let Some(frame) = frame {
            sink.send(Message::Close(Some(frame))).await?;
        }
        sink.close().await
    };
    let _ = timeout(cfg.close_grace, goodbye).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::ticker::Tick;

    const SYMBOLS: &[&str] = &["BTC-USD", "ETH-USD", "SOL-USD"];

    fn tick(symbol: &'static str, seq: u64) -> Tick {
        Tick {
            symbol,
            price: 1.0,
            seq,
            ts: 0,
        }
    }

    fn hub(broadcast_capacity: usize) -> Hub {
        let cfg = Config {
            broadcast_capacity,
            ..Config::default()
        };
        Hub::new(cfg, SYMBOLS.to_vec())
    }

    #[tokio::test]
    async fn forwards_matching_ticks_in_order() {
        let hub = hub(16);
        let mut feed = hub.subscribe();
        let (_ftx, frx) = watch::channel(Filter::All);
        let (tx, mut rx) = mpsc::channel(16);
        for seq in 1..=3 {
            hub.publish(&tick("BTC-USD", seq));
        }
        drop(hub); // closes the feed once drained, so `forward` returns
        assert_eq!(forward(&mut feed, &frx, &tx).await, ForwardEnd::FeedClosed);
        for seq in 1..=3 {
            let text = rx.recv().await.unwrap();
            assert!(text.as_str().contains(&format!(r#""seq":{seq}"#)));
        }
    }

    #[tokio::test]
    async fn filter_drops_other_symbols() {
        let hub = hub(16);
        let mut feed = hub.subscribe();
        let filter = resolve_filter(Some(vec!["ETH-USD".into()]), SYMBOLS).unwrap();
        let (_ftx, frx) = watch::channel(filter);
        let (tx, mut rx) = mpsc::channel(16);
        hub.publish(&tick("BTC-USD", 1));
        hub.publish(&tick("ETH-USD", 2));
        hub.publish(&tick("SOL-USD", 3));
        drop(hub);
        forward(&mut feed, &frx, &tx).await;
        drop(tx);
        let got: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
        assert_eq!(got.len(), 1);
        assert!(got[0].as_str().contains("ETH-USD"));
    }

    #[tokio::test]
    async fn full_client_queue_disconnects() {
        let hub = hub(64);
        let mut feed = hub.subscribe();
        let (_ftx, frx) = watch::channel(Filter::All);
        let (tx, _rx) = mpsc::channel(4); // nobody drains this
        for seq in 1..=5 {
            hub.publish(&tick("BTC-USD", seq));
        }
        assert_eq!(
            forward(&mut feed, &frx, &tx).await,
            ForwardEnd::Slow(SlowReason::QueueFull)
        );
    }

    #[tokio::test]
    async fn lagging_behind_the_broadcast_buffer_disconnects() {
        let hub = hub(8);
        let mut feed = hub.subscribe();
        let (_ftx, frx) = watch::channel(Filter::All);
        let (tx, _rx) = mpsc::channel(1024);
        // 20 messages into an 8-slot ring before the receiver reads anything.
        for seq in 1..=20 {
            hub.publish(&tick("BTC-USD", seq));
        }
        assert_eq!(
            forward(&mut feed, &frx, &tx).await,
            ForwardEnd::Slow(SlowReason::Lagged(12))
        );
    }

    #[tokio::test]
    async fn stops_when_the_writer_goes_away() {
        let hub = hub(8);
        let mut feed = hub.subscribe();
        let (_ftx, frx) = watch::channel(Filter::All);
        let (tx, rx) = mpsc::channel(8);
        drop(rx);
        // No ticks at all: must still notice the client is gone.
        assert_eq!(forward(&mut feed, &frx, &tx).await, ForwardEnd::ClientGone);
    }

    #[test]
    fn resolve_filter_cases() {
        assert_eq!(resolve_filter(None, SYMBOLS), Ok(Filter::All));
        assert_eq!(
            resolve_filter(Some(vec![]), SYMBOLS),
            Ok(Filter::Only(BTreeSet::new()))
        );
        let f = resolve_filter(Some(vec!["SOL-USD".into(), "BTC-USD".into()]), SYMBOLS).unwrap();
        assert!(f.matches("SOL-USD") && f.matches("BTC-USD") && !f.matches("ETH-USD"));
        assert_eq!(
            resolve_filter(Some(vec!["DOGE-USD".into()]), SYMBOLS),
            Err("unknown symbol: DOGE-USD".into())
        );
    }

    #[test]
    fn close_reasons_fit_in_a_close_frame() {
        // RFC 6455 limits the reason to 123 bytes.
        assert!(close_reason(SlowReason::Lagged(u64::MAX)).len() <= 123);
        assert!(close_reason(SlowReason::QueueFull).len() <= 123);
    }
}
