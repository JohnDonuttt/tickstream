//! Simulated market-data feed over WebSockets. See the README for the design.

pub mod config;
pub mod hub;
pub mod protocol;
pub mod session;
pub mod ticker;

use axum::extract::ws::WebSocketUpgrade;
use axum::extract::State;
use axum::http::header;
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use axum::{Json, Router};

pub use config::Config;
pub use hub::{Hub, Stats};

pub const INDEX_HTML: &str = include_str!("../static/index.html");

// clients only ever send small subscribe messages
const MAX_CLIENT_MESSAGE: usize = 4 * 1024;

pub fn router(hub: Hub) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/ws", get(ws_upgrade))
        .route("/stats", get(stats))
        .with_state(hub)
}

async fn index() -> impl IntoResponse {
    ([(header::CACHE_CONTROL, "no-cache")], Html(INDEX_HTML))
}

async fn ws_upgrade(ws: WebSocketUpgrade, State(hub): State<Hub>) -> impl IntoResponse {
    ws.max_message_size(MAX_CLIENT_MESSAGE)
        .max_frame_size(MAX_CLIENT_MESSAGE)
        .on_upgrade(move |socket| session::run(socket, hub))
}

async fn stats(State(hub): State<Hub>) -> Json<Stats> {
    Json(hub.stats())
}
