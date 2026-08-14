pub mod error;
pub mod operation;
pub mod auth;
pub mod routing;
pub mod handlers;
pub mod logging;

use std::net::SocketAddr;

use bytes::Bytes;
use http::{Request, Response};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use tokio::net::TcpListener;

pub async fn handle_request(
    req: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, std::convert::Infallible> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let query = req.uri().query().map(str::to_string);
    let headers = req.headers().clone();

    let user = auth::extract_user(&headers);

    let response = match routing::parse_request(&method, &path, query.as_deref(), &headers) {
        Ok(op) => logging::log_dispatch(&user, op).await,
        Err(err) => {
            let status = err.status_code().as_u16();
            tracing::info!(
                user = %user,
                operation = "ParseError",
                duration_ms = 0u64,
                bytes = 0u64,
                status,
                "s3_request"
            );
            Err(err)
        }
    };

    Ok(response.unwrap_or_else(|err| err.to_response()))
}

pub async fn serve(addr: SocketAddr) -> std::io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;

    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(pair) => pair,
                Err(err) => {
                    tracing::error!(error = %err, "accept error");
                    continue;
                }
            };
            let io = TokioIo::new(stream);
            tokio::spawn(async move {
                let builder = ConnBuilder::new(TokioExecutor::new());
                if let Err(err) = builder.serve_connection(io, service_fn(handle_request)).await {
                    tracing::error!(error = %err, "connection error");
                }
            });
        }
    });

    Ok(local_addr)
}
