use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    open_conductor::logging::init();

    let addr: SocketAddr = "127.0.0.1:8080".parse()?;
    let bound = open_conductor::serve(addr).await?;
    tracing::info!(%bound, "listening");

    std::future::pending::<()>().await;
    Ok(())
}
