use tickstream::ticker::{self, TickGenerator};
use tickstream::{router, Config, Hub};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = match Config::from_env() {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("config error: {e}");
            std::process::exit(2);
        }
    };

    let generator = TickGenerator::new(ticker::default_symbols());
    let hub = Hub::new(config.clone(), generator.symbols().collect());
    tokio::spawn(ticker::run(hub.clone(), generator, config.tick_interval));

    let listener = match tokio::net::TcpListener::bind(config.addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("cannot listen on {}: {e}", config.addr);
            std::process::exit(1);
        }
    };
    tracing::info!(
        "tickstream listening on http://{}  (tick every {:?}, client queue {}, broadcast buffer {})",
        config.addr,
        config.tick_interval,
        config.client_queue,
        config.broadcast_capacity,
    );

    let shutdown = async {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("shutting down");
    };
    if let Err(e) = axum::serve(listener, router(hub))
        .with_graceful_shutdown(shutdown)
        .await
    {
        eprintln!("server error: {e}");
        std::process::exit(1);
    }
}
