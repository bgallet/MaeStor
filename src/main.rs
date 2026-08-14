use std::net::SocketAddr;

/// Overrides the bind address; falls back to `DEFAULT_BIND_ADDR` when unset.
const BIND_ADDR_ENV_VAR: &str = "OPEN_CONDUCTOR_ADDR";
const DEFAULT_BIND_ADDR: &str = "127.0.0.1:8080";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    open_conductor::logging::init();

    let addr: SocketAddr = std::env::var(BIND_ADDR_ENV_VAR)
        .unwrap_or_else(|_| DEFAULT_BIND_ADDR.to_string())
        .parse()?;

    let handle = open_conductor::serve(addr).await?;
    tracing::info!(addr = %handle.addr(), "listening");

    tokio::signal::ctrl_c().await?;
    tracing::info!("ctrl-c received, shutting down");
    handle.shutdown().await;

    Ok(())
}
