use std::net::SocketAddr;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct Config {
    pub addr: SocketAddr,
    /// one tick per symbol per interval
    pub tick_interval: Duration,
    pub broadcast_capacity: usize,
    pub client_queue: usize,
    /// how long a client being dropped gets to receive its close frame
    pub close_grace: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            addr: SocketAddr::from(([127, 0, 0, 1], 3000)),
            tick_interval: Duration::from_millis(100),
            broadcast_capacity: 1024,
            client_queue: 256,
            close_grace: Duration::from_secs(2),
        }
    }
}

impl Config {
    /// Defaults, overridden by any `TICKSTREAM_*` variables that are set.
    pub fn from_env() -> Result<Self, String> {
        let mut cfg = Self::default();
        if let Some(addr) = var("TICKSTREAM_ADDR") {
            cfg.addr = addr
                .parse()
                .map_err(|e| format!("TICKSTREAM_ADDR={addr:?}: {e}"))?;
        }
        if let Some(ms) = parse_positive("TICKSTREAM_TICK_MS")? {
            cfg.tick_interval = Duration::from_millis(ms as u64);
        }
        if let Some(n) = parse_positive("TICKSTREAM_BROADCAST_CAPACITY")? {
            cfg.broadcast_capacity = n;
        }
        if let Some(n) = parse_positive("TICKSTREAM_CLIENT_QUEUE")? {
            cfg.client_queue = n;
        }
        Ok(cfg)
    }
}

fn var(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.trim().is_empty())
}

fn parse_positive(name: &str) -> Result<Option<usize>, String> {
    match var(name) {
        None => Ok(None),
        Some(raw) => match raw.trim().parse::<usize>() {
            Ok(n) if n > 0 => Ok(Some(n)),
            _ => Err(format!("{name} must be a positive integer, got {raw:?}")),
        },
    }
}
