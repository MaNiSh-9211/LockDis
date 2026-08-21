//! Palisade gRPC server binary.

use std::net::SocketAddr;
use std::time::Duration;

use clap::Parser;
use tonic::transport::Server;

use palisade_proto::lock_service_server::LockServiceServer;
use palisade_redis::RedisConfig;
use palisade_server::{PalisadeService, ServiceConfig};

#[derive(Parser)]
#[command(name = "palisade-server", about = "Palisade distributed lock service")]
struct Args {
    /// Redis endpoint to back the service.
    #[arg(long, default_value = "redis://127.0.0.1:6379")]
    redis_url: String,

    /// Listen address for the gRPC server.
    #[arg(long, default_value = "0.0.0.0:50051")]
    listen: SocketAddr,

    /// Maximum lease the server will grant or extend to (seconds).
    #[arg(long, default_value_t = 600)]
    max_ttl_secs: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = Args::parse();
    let manager = palisade_redis::RedisLockManager::connect(RedisConfig::new(&args.redis_url))
        .await
        .map_err(|e| format!("redis connect failed: {e}"))?;
    let service = PalisadeService::new(
        manager,
        ServiceConfig {
            max_ttl: Duration::from_secs(args.max_ttl_secs),
            ..ServiceConfig::default()
        },
    );

    tracing::info!(addr = %args.listen, "palisade-server listening");
    Server::builder()
        .add_service(LockServiceServer::new(service))
        .serve(args.listen)
        .await?;

    Ok(())
}
