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
use hyper_util::server::graceful::GracefulShutdown;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

/// How long to wait after a failed `accept()` before trying again, so that a
/// persistent error (e.g. file-descriptor exhaustion) does not spin the CPU.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);

/// How long to wait for in-flight connections to finish after a shutdown is
/// requested before dropping them unconditionally.
const GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

pub async fn handle_request(
    req: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, std::convert::Infallible> {
    // Borrow directly from `req` rather than cloning method/path/query/headers
    // up front — nothing here needs to outlive the borrow, since `user` and
    // `parsed` are both owned before `req` is consumed below.
    let user = auth::extract_user(req.headers());
    let parsed = routing::parse_request(
        req.method(),
        req.uri().path(),
        req.uri().query(),
        req.headers(),
    );

    // Handlers are stubs and do not read the body yet, but HTTP/1.1 keep-alive
    // requires the request body to be fully consumed before the connection can
    // be reused. Drain and discard it.
    if let Err(err) = req.into_body().collect().await {
        tracing::warn!(error = %err, "failed to drain request body");
    }

    Ok(logging::log_request(&user, parsed).await)
}

/// Handle to a running server, returned by [`serve`].
///
/// Dropping this without calling [`shutdown`](ServerHandle::shutdown) leaves
/// the accept loop running in the background for the rest of the process —
/// that's the normal case for `main`, which runs until the process is
/// killed. Callers that need an orderly stop (tests, or `main`'s own
/// signal handling) call `shutdown()` explicitly.
pub struct ServerHandle {
    addr: SocketAddr,
    shutdown_tx: oneshot::Sender<()>,
    join_handle: tokio::task::JoinHandle<()>,
}

impl ServerHandle {
    /// The address the server is actually bound to (useful when `serve` was
    /// called with port `0` for an ephemeral port).
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Stop accepting new connections and wait for in-flight connections to
    /// finish, up to [`GRACEFUL_SHUTDOWN_TIMEOUT`], then wait for the accept
    /// loop itself to exit.
    pub async fn shutdown(self) {
        // A send error here means the accept loop's task has already ended
        // on its own — nothing to do about it.
        let _ = self.shutdown_tx.send(());
        let _ = self.join_handle.await;
    }
}

pub async fn serve(addr: SocketAddr) -> std::io::Result<ServerHandle> {
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
    // Not Clone by design (see hyper_util's docs): stays owned by this task
    // for its whole life, and is consumed exactly once by `.shutdown()`
    // below. Each connection instead gets an owned `Watcher` (see
    // `graceful.watcher()` below), which is the type meant to be sent onto
    // another task.
    let graceful = GracefulShutdown::new();

    let join_handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    let (stream, _) = match accept_result {
                        Ok(pair) => pair,
                        Err(err) => {
                            tracing::error!(error = %err, "accept error");
                            // Back off so a persistent condition (EMFILE/ENFILE
                            // under load) does not turn into a 100%-CPU busy loop.
                            tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
                            continue;
                        }
                    };
                    let io = TokioIo::new(stream);
                    let watcher = graceful.watcher();
                    tokio::spawn(async move {
                        // Built inside this task (rather than shared/cloned in)
                        // so the connection future it produces doesn't need to
                        // borrow anything from outside this task.
                        let builder = ConnBuilder::new(TokioExecutor::new());
                        let conn = builder.serve_connection(io, service_fn(handle_request));
                        let conn = watcher.watch(conn);
                        if let Err(err) = conn.await {
                            tracing::error!(error = %err, "connection error");
                        }
                    });
                }
                _ = &mut shutdown_rx => {
                    tracing::info!("shutdown requested; no longer accepting new connections");
                    break;
                }
            }
        }

        if tokio::time::timeout(GRACEFUL_SHUTDOWN_TIMEOUT, graceful.shutdown())
            .await
            .is_err()
        {
            tracing::warn!("graceful shutdown timed out; dropping remaining connections");
        } else {
            tracing::info!("all connections closed gracefully");
        }
    });

    Ok(ServerHandle {
        addr: local_addr,
        shutdown_tx,
        join_handle,
    })
}
