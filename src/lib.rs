pub mod error;
pub mod operation;
pub mod auth;
pub mod routing;
pub mod handlers;
pub mod logging;

use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use http::{Request, Response};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use tokio::net::TcpListener;

/// How long to wait after a failed `accept()` before trying again, so that a
/// persistent error (e.g. file-descriptor exhaustion) does not spin the CPU.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);

pub async fn handle_request(
    req: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, std::convert::Infallible> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let query = req.uri().query().map(str::to_string);
    let headers = req.headers().clone();

    let user = auth::extract_user(&headers);
    let parsed = routing::parse_request(&method, &path, query.as_deref(), &headers);

    // Handlers are stubs and do not read the body yet, but HTTP/1.1 keep-alive
    // requires the request body to be fully consumed before the connection can
    // be reused. Drain and discard it.
    if let Err(err) = req.into_body().collect().await {
        tracing::warn!(error = %err, "failed to drain request body");
    }

    Ok(logging::log_request(&user, parsed).await)
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
                    // Back off so a persistent condition (EMFILE/ENFILE under
                    // load) does not turn into a 100%-CPU busy loop.
                    tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
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
