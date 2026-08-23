//! Palisade gRPC server binary.

use std::net::SocketAddr;
use std::time::Duration;

use clap::Parser;
use tonic::transport::{Server, ServerTlsConfig};

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

    /// PEM server certificate (enables TLS; requires --tls-key).
    #[arg(long, requires = "tls_key")]
    tls_cert: Option<String>,

    /// PEM server private key (requires --tls-cert).
    #[arg(long, requires = "tls_cert")]
    tls_key: Option<String>,

    /// PEM CA used to verify clients (enables mTLS with --tls-cert/--tls-key).
    #[arg(long, requires = "tls_cert")]
    client_ca: Option<String>,

    /// Seconds between readiness flip and listener shutdown on drain.
    #[arg(long, default_value_t = 10)]
    drain_grace_secs: u64,

    /// Serve Prometheus metrics at this address (e.g. 0.0.0.0:9100).
    #[arg(long)]
    metrics_addr: Option<SocketAddr>,

    /// JSON ACL file; omit for open mode (dev only).
    #[arg(long)]
    acl_file: Option<std::path::PathBuf>,

    /// Identity source: file (bearer tokens from the ACL) or trusted-header
    /// (gateway/UAM vouches via x-palisade-principal; requires --acl-file).
    #[arg(long, default_value = "file", requires = "acl_file")]
    auth_mode: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let json_logs = std::env::var("PALISADE_LOG_JSON").as_deref() == Ok("1");
    if json_logs {
        tracing_subscriber::fmt().json().init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| "info".into()),
            )
            .init();
    }

    let args = Args::parse();
    let manager = palisade_redis::RedisLockManager::connect(RedisConfig::new(&args.redis_url))
        .await
        .map_err(|e| format!("redis connect failed: {e}"))?;
    let mut service = PalisadeService::new(
        manager,
        ServiceConfig {
            max_ttl: Duration::from_secs(args.max_ttl_secs),
            ..ServiceConfig::default()
        },
    );
    if let Some(acl_path) = &args.acl_file {
        let mut acl = palisade_server::Acl::load_file(acl_path)?;
        if args.auth_mode == "trusted-header" {
            acl = acl.with_mode(palisade_server::AuthMode::TrustedHeader);
            tracing::info!("auth mode: trusted gateway/UAM header");
        }
        service = service.with_acl(acl);
    }
    let _sweeper = palisade_server::start_session_sweeper(&service);

    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut builder = Server::builder();
    if let (Some(cert), Some(key)) = (&args.tls_cert, &args.tls_key) {
        let cert = std::fs::read(cert)?;
        let key = std::fs::read(key)?;
        let mut tls =
            ServerTlsConfig::new().identity(tonic::transport::Identity::from_pem(cert, key));
        if let Some(ca) = &args.client_ca {
            let ca = std::fs::read(ca)?;
            tls = tls.client_ca_root(tonic::transport::Certificate::from_pem(ca));
            tracing::info!("mTLS enabled: client certificates required");
        }
        builder = builder.tls_config(tls)?;
    }

    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<LockServiceServer<PalisadeService>>()
        .await;

    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    tracing::info!(addr = %args.listen, "palisade-server listening");

    if let Some(metrics_addr) = args.metrics_addr {
        let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().install_recorder()?;
        tracing::info!(addr = %metrics_addr, "serving prometheus metrics");
        tokio::spawn(serve_metrics(metrics_addr, recorder));
    }

    // Drain: flip readiness + health first, wait out in-flight retries,
    // then let the listener shut down. Held leases are unaffected by design.
    let ready_flag = service.ready_handle();
    let shutdown = async move {
        drain_signal().await;
        tracing::info!("drain signal received: refusing new grants");
        ready_flag.store(false, std::sync::atomic::Ordering::Release);
        health_reporter
            .set_not_serving::<LockServiceServer<PalisadeService>>()
            .await;
        tokio::time::sleep(Duration::from_secs(args.drain_grace_secs)).await;
    };

    builder
        .add_service(health_service)
        .add_service(LockServiceServer::new(service))
        .serve_with_incoming_shutdown(TcpListenerStreamCompat::new(listener), shutdown)
        .await?;

    tracing::info!("shutdown complete");
    Ok(())
}

async fn drain_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = term.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Minimal HTTP responder for the Prometheus text format on one path —
/// avoids pulling a web framework for a single endpoint.
async fn serve_metrics(
    addr: SocketAddr,
    recorder: metrics_exporter_prometheus::PrometheusHandle,
) -> std::io::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind(addr).await?;
    loop {
        let (mut sock, _) = listener.accept().await?;
        let recorder = recorder.clone();
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            if sock.read(&mut buf).await.is_err() {
                return;
            }
            let body = recorder.render();
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/plain; version=0.0.4\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = sock.write_all(response.as_bytes()).await;
            let _ = sock.shutdown().await;
        });
    }
}

/// `TcpListenerStream` wrapper keeping the import surface small.
type TcpListenerStreamCompat = tokio_stream::wrappers::TcpListenerStream;
