//! One broadcast channel shared by every client, plus the `/stats` counters.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use axum::extract::ws::Utf8Bytes;
use serde::Serialize;
use tokio::sync::broadcast;

use crate::config::Config;
use crate::protocol::ServerMessage;
use crate::ticker::Tick;

/// A tick serialised once by the publisher. Each subscriber only clones the
/// `Utf8Bytes`, which is a refcount bump, so fan-out cost doesn't grow with
/// message size.
#[derive(Debug)]
pub struct Published {
    pub symbol: &'static str,
    pub payload: Utf8Bytes,
}

#[derive(Clone)]
pub struct Hub {
    inner: Arc<Inner>,
}

struct Inner {
    tx: broadcast::Sender<Arc<Published>>,
    symbols: Vec<&'static str>,
    config: Config,
    stats: Counters,
}

#[derive(Default)]
struct Counters {
    published: AtomicU64,
    connected: AtomicU64,
    connections_total: AtomicU64,
    dropped_lagged: AtomicU64,
    dropped_queue_full: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct Stats {
    pub published: u64,
    pub connected: u64,
    pub connections_total: u64,
    pub dropped_lagged: u64,
    pub dropped_queue_full: u64,
}

impl Stats {
    pub fn slow_disconnects(&self) -> u64 {
        self.dropped_lagged + self.dropped_queue_full
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlowReason {
    /// fell further behind than the broadcast ring buffer holds; carries the
    /// number of messages that were overwritten
    Lagged(u64),
    /// the client's own bounded queue was full
    QueueFull,
}

impl Hub {
    pub fn new(config: Config, symbols: Vec<&'static str>) -> Self {
        let (tx, _) = broadcast::channel(config.broadcast_capacity);
        Self {
            inner: Arc::new(Inner {
                tx,
                symbols,
                config,
                stats: Counters::default(),
            }),
        }
    }

    pub fn config(&self) -> &Config {
        &self.inner.config
    }

    pub fn symbols(&self) -> &[&'static str] {
        &self.inner.symbols
    }

    /// Returns the number of subscribers the tick was queued for.
    pub fn publish(&self, tick: &Tick) -> usize {
        let payload = ServerMessage::Tick(tick).to_json();
        self.inner.stats.published.fetch_add(1, Ordering::Relaxed);
        let msg = Arc::new(Published {
            symbol: tick.symbol,
            payload: payload.into(),
        });
        // only errors when nobody is subscribed
        self.inner.tx.send(msg).unwrap_or(0)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Arc<Published>> {
        self.inner.tx.subscribe()
    }

    pub fn stats(&self) -> Stats {
        let s = &self.inner.stats;
        Stats {
            published: s.published.load(Ordering::Relaxed),
            connected: s.connected.load(Ordering::Relaxed),
            connections_total: s.connections_total.load(Ordering::Relaxed),
            dropped_lagged: s.dropped_lagged.load(Ordering::Relaxed),
            dropped_queue_full: s.dropped_queue_full.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn connection_guard(&self) -> ConnectionGuard {
        self.inner.stats.connected.fetch_add(1, Ordering::Relaxed);
        self.inner
            .stats
            .connections_total
            .fetch_add(1, Ordering::Relaxed);
        ConnectionGuard { hub: self.clone() }
    }

    pub(crate) fn record_slow(&self, reason: SlowReason) {
        let counter = match reason {
            SlowReason::Lagged(_) => &self.inner.stats.dropped_lagged,
            SlowReason::QueueFull => &self.inner.stats.dropped_queue_full,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

pub(crate) struct ConnectionGuard {
    hub: Hub,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.hub
            .inner
            .stats
            .connected
            .fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tick(seq: u64) -> Tick {
        Tick {
            symbol: "BTC-USD",
            price: 100.0,
            seq,
            ts: 0,
        }
    }

    #[test]
    fn publish_reaches_every_subscriber() {
        let hub = Hub::new(Config::default(), vec!["BTC-USD"]);
        let mut a = hub.subscribe();
        let mut b = hub.subscribe();
        assert_eq!(hub.publish(&tick(1)), 2);
        let (ma, mb) = (a.try_recv().unwrap(), b.try_recv().unwrap());
        // Same allocation: serialised once, shared by both receivers.
        assert!(Arc::ptr_eq(&ma, &mb));
        assert!(ma.payload.as_str().contains(r#""seq":1"#));
        assert_eq!(hub.stats().published, 1);
    }

    #[test]
    fn publish_with_no_subscribers_is_not_an_error() {
        let hub = Hub::new(Config::default(), vec!["BTC-USD"]);
        assert_eq!(hub.publish(&tick(1)), 0);
    }

    #[test]
    fn connection_guard_tracks_live_count() {
        let hub = Hub::new(Config::default(), vec![]);
        let g1 = hub.connection_guard();
        let g2 = hub.connection_guard();
        assert_eq!(hub.stats().connected, 2);
        drop(g1);
        assert_eq!(hub.stats().connected, 1);
        drop(g2);
        let s = hub.stats();
        assert_eq!((s.connected, s.connections_total), (0, 2));
    }
}
