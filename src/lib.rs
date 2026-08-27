pub mod error;
pub mod operation;
pub mod auth;
pub mod routing;
pub mod handlers;
pub mod logging;
pub mod metadata;
pub mod tls;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::{Request, Response};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use hyper_util::server::graceful::GracefulShutdown;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

/// How long to wait after a failed `accept()` before trying again, so that a
/// persistent error (e.g. file-descriptor exhaustion) does not spin the CPU.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);

/// How long to wait for in-flight connections to finish after a shutdown is
/// requested before dropping them unconditionally.
const GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

pub async fn handle_request(
    req: Request<Incoming>,
    routing_config: &routing::RoutingConfig,
    peer_identity: Option<&str>,
) -> Result<Response<Full<Bytes>>, std::convert::Infallible> {
    // Borrow directly from `req` rather than cloning method/path/query/headers
    // up front — nothing here needs to outlive the borrow, since `identity`
    // and `parsed` are both owned before `req` is consumed below.
    let identity = auth::extract_user(req.headers(), peer_identity);
    // Prefer the request-target's own authority over the `Host` header.
    // HTTP/2 carries the authority as the `:authority` pseudo-header, which
    // hyper surfaces via `req.uri().authority()` — never in the header map —
    // so a Host-header-only lookup silently sees no host at all for h2
    // requests. The same `uri().authority()` path also covers HTTP/1.1
    // absolute-form request-targets (RFC 9112 §3.2.2). Falling back to the
    // `Host` header keeps today's HTTP/1.1 origin-form behavior unchanged.
    let host = req
        .uri()
        .authority()
        .map(|a| a.as_str())
        .or_else(|| req.headers().get(http::header::HOST).and_then(|v| v.to_str().ok()));
    let parsed = routing::parse_request(
        req.method(),
        req.uri().path(),
        req.uri().query(),
        req.headers(),
        host,
        routing_config.base_domain.as_deref(),
    );

    // Handlers are stubs and do not read the body yet, but HTTP/1.1 keep-alive
    // requires the request body to be fully consumed before the connection can
    // be reused. Drain and discard it.
    if let Err(err) = req.into_body().collect().await {
        tracing::warn!(error = %err, "failed to drain request body");
    }

    Ok(logging::log_request(&identity.user, identity.method, parsed).await)
}

/// Handle to a running server, returned by [`serve`] or [`serve_tls`].
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

/// Runs the accept loop shared by [`serve`] and [`serve_tls`]: bind already
/// happened in the caller (the two entry points fail differently — plain
/// `io::Error` vs. `TlsError` — so they map bind errors themselves), and this
/// owns accepting connections, spawning one task per connection, and
/// graceful shutdown.
///
/// `connect` turns a raw `TcpStream` into the transport-specific IO to hand
/// to hyper (a plain passthrough for `serve`, a TLS handshake for
/// `serve_tls`) plus that connection's resolved peer identity, if any.
/// Returning `None` drops the connection (e.g. a failed TLS handshake)
/// without tearing down the loop.
async fn run_accept_loop<C, Fut, IO>(
    listener: TcpListener,
    routing_config: routing::RoutingConfig,
    connect: C,
) -> ServerHandle
where
    C: Fn(TcpStream) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Option<(IO, Option<Arc<str>>)>> + Send + 'static,
    IO: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let local_addr = listener
        .local_addr()
        .expect("a bound listener always has a local address");
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
    // Not Clone by design (see hyper_util's docs): stays owned by this task
    // for its whole life, and is consumed exactly once by `.shutdown()`
    // below. Each connection instead gets an owned `Watcher` (see
    // `graceful.watcher()` below), which is the type meant to be sent onto
    // another task.
    let graceful = GracefulShutdown::new();
    let routing_config = Arc::new(routing_config);
    let connect = Arc::new(connect);

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
                    let watcher = graceful.watcher();
                    let routing_config = Arc::clone(&routing_config);
                    let connect = Arc::clone(&connect);
                    tokio::spawn(async move {
                        let Some((io, peer_identity)) = connect(stream).await else {
                            return;
                        };
                        // Built inside this task (rather than shared/cloned in)
                        // so the connection future it produces doesn't need to
                        // borrow anything from outside this task.
                        let io = TokioIo::new(io);
                        let builder = ConnBuilder::new(TokioExecutor::new());
                        let service = service_fn(move |req| {
                            let routing_config = Arc::clone(&routing_config);
                            // `Arc<str>` clone is a refcount bump, not an
                            // allocation — cheap even though this runs once
                            // per request on a keep-alive connection.
                            let peer_identity = peer_identity.clone();
                            async move {
                                handle_request(req, &routing_config, peer_identity.as_deref()).await
                            }
                        });
                        let conn = builder.serve_connection(io, service);
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

    ServerHandle {
        addr: local_addr,
        shutdown_tx,
        join_handle,
    }
}

pub async fn serve(
    addr: SocketAddr,
    routing_config: routing::RoutingConfig,
) -> std::io::Result<ServerHandle> {
    let listener = TcpListener::bind(addr).await?;
    Ok(run_accept_loop(listener, routing_config, |stream: TcpStream| async move {
        Some((stream, None::<Arc<str>>))
    })
    .await)
}

pub async fn serve_tls(
    addr: SocketAddr,
    routing_config: routing::RoutingConfig,
    tls_config: tls::TlsConfig,
) -> Result<ServerHandle, tls::TlsError> {
    let listener = TcpListener::bind(addr).await.map_err(tls::TlsError::Io)?;
    let reloadable = Arc::new(tls::ReloadableConfig::start(tls_config)?);

    let connect = move |stream: TcpStream| {
        let reloadable = Arc::clone(&reloadable);
        async move {
            let acceptor = tokio_rustls::TlsAcceptor::from(reloadable.current());
            let tls_stream = match acceptor.accept(stream).await {
                Ok(stream) => stream,
                Err(err) => {
                    tracing::warn!(error = %err, "TLS handshake failed");
                    return None;
                }
            };
            let peer_identity = tls::resolve_peer_identity(tls_stream.get_ref().1);
            Some((tls_stream, peer_identity))
        }
    };

    Ok(run_accept_loop(listener, routing_config, connect).await)
}
