use std::net::SocketAddr;
use std::path::PathBuf;

use open_conductor::routing::RoutingConfig;
use open_conductor::tls::TlsConfig;

const BIND_ADDR_ENV_VAR: &str = "OPEN_CONDUCTOR_ADDR";
const DEFAULT_BIND_ADDR: &str = "127.0.0.1:8080";
const BASE_DOMAIN_ENV_VAR: &str = "OPEN_CONDUCTOR_BASE_DOMAIN";
const TLS_CERT_CHAIN_ENV_VAR: &str = "OPEN_CONDUCTOR_TLS_CERT_CHAIN";
const TLS_PRIVATE_KEY_ENV_VAR: &str = "OPEN_CONDUCTOR_TLS_PRIVATE_KEY";
const TLS_CLIENT_CA_ENV_VAR: &str = "OPEN_CONDUCTOR_TLS_CLIENT_CA";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    open_conductor::logging::init();

    let addr: SocketAddr = std::env::var(BIND_ADDR_ENV_VAR)
        .unwrap_or_else(|_| DEFAULT_BIND_ADDR.to_string())
        .parse()?;

    let routing_config = RoutingConfig {
        base_domain: std::env::var(BASE_DOMAIN_ENV_VAR).ok(),
    };

    let handle = match (
        std::env::var(TLS_CERT_CHAIN_ENV_VAR).ok(),
        std::env::var(TLS_PRIVATE_KEY_ENV_VAR).ok(),
    ) {
        (Some(cert_chain_path), Some(private_key_path)) => {
            let tls_config = TlsConfig {
                cert_chain_path: PathBuf::from(cert_chain_path),
                private_key_path: PathBuf::from(private_key_path),
                client_ca_path: std::env::var(TLS_CLIENT_CA_ENV_VAR).ok().map(PathBuf::from),
            };
            open_conductor::serve_tls(addr, routing_config, tls_config).await?
        }
        _ => open_conductor::serve(addr, routing_config).await?,
    };

    tracing::info!(addr = %handle.addr(), "listening");

    tokio::signal::ctrl_c().await?;
    tracing::info!("ctrl-c received, shutting down");
    handle.shutdown().await;

    Ok(())
}
