//! mTLS end-to-end: CA-generated certs, server requiring client identity,
//! SDK connecting over mutual TLS. Skips silently without a Redis.

use std::net::SocketAddr;
use std::time::Duration;

use palisade_client::PalisadeClient;
use palisade_core::{LockOptions, OwnerId};
use palisade_proto::lock_service_server::LockServiceServer;
use palisade_redis::RedisConfig;
use palisade_server::{PalisadeService, ServiceConfig};
use rcgen::{CertificateParams, KeyPair, SanType};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};

struct CertMaterial {
    ca_pem: String,
    server_cert_pem: String,
    server_key_pem: String,
    client_cert_pem: String,
    client_key_pem: String,
}

fn ca_material() -> std::result::Result<CertMaterial, rcgen::Error> {
    let ca_key = KeyPair::generate()?;
    let mut ca_params = CertificateParams::new(vec![])?;
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_cert = ca_params.self_signed(&ca_key)?;

    // Server leaf: valid for localhost and 127.0.0.1.
    let server_key = KeyPair::generate()?;
    let mut server_params = CertificateParams::new(vec!["localhost".to_string()])?;
    server_params
        .subject_alt_names
        .push(SanType::IpAddress(std::net::IpAddr::from([127, 0, 0, 1])));
    let server_cert = server_params.signed_by(&server_key, &ca_cert, &ca_key)?;

    // Client leaf.
    let client_key = KeyPair::generate()?;
    let client_params = CertificateParams::new(vec!["palisade-client".to_string()])?;
    let client_cert = client_params.signed_by(&client_key, &ca_cert, &ca_key)?;

    Ok(CertMaterial {
        ca_pem: ca_cert.pem(),
        server_cert_pem: server_cert.pem(),
        server_key_pem: server_key.serialize_pem(),
        client_cert_pem: client_cert.pem(),
        client_key_pem: client_key.serialize_pem(),
    })
}

async fn spawn_mtls_stack(material: &CertMaterial) -> Option<SocketAddr> {
    let url =
        std::env::var("PALISADE_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let manager = match palisade_redis::RedisLockManager::connect(RedisConfig::new(&url)).await {
        Ok(m) => m,
        Err(e) => {
            eprintln!("skipping mtls test: no redis at {url}: {e}");
            return None;
        }
    };
    let service = PalisadeService::new(manager, ServiceConfig::default());

    let tls = ServerTlsConfig::new()
        .identity(Identity::from_pem(
            material.server_cert_pem.clone(),
            material.server_key_pem.clone(),
        ))
        .client_ca_root(Certificate::from_pem(material.ca_pem.clone()));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = Server::builder()
            .tls_config(tls)
            .expect("server tls")
            .add_service(LockServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    Some(addr)
}

#[tokio::test]
async fn mtls_roundtrip_and_plaintext_rejection() {
    let material = match ca_material() {
        Ok(m) => m,
        Err(e) => panic!("cert generation failed: {e}"),
    };
    let Some(addr) = spawn_mtls_stack(&material).await else {
        return;
    };

    let client = PalisadeClient::connect_mtls(
        format!("https://{addr}"),
        material.ca_pem.clone(),
        material.client_cert_pem.clone(),
        material.client_key_pem.clone(),
    )
    .await
    .expect("mtls connect");

    let key = format!("palisade-mtls-test:{}", OwnerId::generate().as_uuid());
    let opts = LockOptions::default()
        .with_ttl(Duration::from_secs(10))
        .with_watchdog(false);

    let h = client.try_lock(&key, &opts).await.expect("grant over mtls");
    h.release().await.expect("release over mtls");

    // Plaintext against a TLS-only listener must fail.
    let plain = PalisadeClient::connect(format!("http://{addr}")).await;
    assert!(plain.is_err(), "plaintext connection should be rejected");
}
