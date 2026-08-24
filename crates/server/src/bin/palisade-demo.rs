//! Palisade web demo binary: HTTP + WebSocket UI over live Redis.
//!
//! Complements (never replaces) the gRPC server — same backend, same Lua
//! scripts, same safety argument; this binary exists so the system can be
//! *seen* working in a browser.

use std::net::SocketAddr;

use clap::Parser;

#[derive(Parser)]
#[command(
    name = "palisade-demo",
    about = "Browser demo of Palisade distributed locking"
)]
struct Args {
    /// Redis endpoint backing the demo.
    #[arg(long, default_value = "redis://127.0.0.1:6379")]
    redis_url: String,

    /// Listen address for the HTTP + WebSocket server.
    #[arg(long, default_value = "0.0.0.0:8080")]
    listen: SocketAddr,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    palisade_server::demo::run(args.listen, &args.redis_url).await?;
    tracing::info!("demo shutdown complete");
    Ok(())
}
