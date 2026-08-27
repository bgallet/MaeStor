mod support;

use std::path::PathBuf;
use std::sync::Arc;

use maestore::routing::RoutingConfig;
use maestore::tls::{load_server_config, ReloadableConfig, TlsConfig};
use rcgen::Issuer;
use rustls::pki_types::ServerName;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tracing_test::traced_test;

fn build_root_store(ca_pem: &str) -> rustls::RootCertStore {
    let mut roots = rustls::RootCertStore::empty();
    let mut reader = ca_pem.as_bytes();
    for cert in rustls_pemfile::certs(&mut reader) {
        roots.add(cert.expect("valid CA cert")).expect("add CA to root store");
    }
    roots
}

fn client_config_trusting(ca_pem: &str) -> rustls::ClientConfig {
    rustls::ClientConfig::builder()
        .with_root_certificates(build_root_store(ca_pem))
        .with_no_client_auth()
}

/// Retries `attempt` (one full handshake/connection attempt, returning
/// whether it succeeded) until it succeeds or `deadline` passes — file
/// watcher event delivery timing isn't guaranteed, so a reload test can't
/// just try once.
async fn poll_until_ok<F, Fut>(deadline: std::time::Instant, mut attempt: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    loop {
        if attempt().await {
            return;
        }
        assert!(std::time::Instant::now() < deadline, "reload was not observed within the timeout");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

async fn handshake_ok(server_config: Arc<rustls::ServerConfig>, client_config: rustls::ClientConfig, sni: &str) {
    let (client_io, server_io) = tokio::io::duplex(8192);
    let acceptor = TlsAcceptor::from(server_config);
    let connector = TlsConnector::from(Arc::new(client_config));
    let name = ServerName::try_from(sni.to_string()).expect("valid server name");

    let (server_result, client_result) =
        tokio::join!(acceptor.accept(server_io), connector.connect(name, client_io));

    server_result.expect("server-side handshake should succeed");
    client_result.expect("client-side handshake should succeed");
}

#[tokio::test]
async fn loads_a_wildcard_chain_and_handshakes_for_any_matching_sni() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (ca_pem, ca_params, ca_key) = support::generate_ca();
    let issuer = Issuer::from_params(&ca_params, ca_key);
    let leaf = support::issue_server_cert(&issuer, "*.s3.test");
    let (chain_path, key_path) = support::write_chain_and_key(dir.path(), &ca_pem, &leaf);
    let tls_config = TlsConfig {
        cert_chain_path: chain_path,
        private_key_path: key_path,
        client_ca_path: None,
    };

    for sni in ["bucket-one.s3.test", "bucket-two.s3.test"] {
        let server_config = Arc::new(load_server_config(&tls_config).expect("load server config"));
        handshake_ok(server_config, client_config_trusting(&ca_pem), sni).await;
    }
}

#[tokio::test]
async fn chain_file_sends_the_full_chain_to_the_client() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (ca_pem, ca_params, ca_key) = support::generate_ca();
    let issuer = Issuer::from_params(&ca_params, ca_key);
    let leaf = support::issue_server_cert(&issuer, "*.s3.test");
    let (chain_path, key_path) = support::write_chain_and_key(dir.path(), &ca_pem, &leaf);

    let server_config = load_server_config(&TlsConfig {
        cert_chain_path: chain_path,
        private_key_path: key_path,
        client_ca_path: None,
    })
    .expect("load server config");

    let (client_io, server_io) = tokio::io::duplex(8192);
    let acceptor = TlsAcceptor::from(Arc::new(server_config));
    let connector = TlsConnector::from(Arc::new(client_config_trusting(&ca_pem)));
    let name = ServerName::try_from("bucket.s3.test".to_string()).expect("valid server name");

    let (server_result, client_result) =
        tokio::join!(acceptor.accept(server_io), connector.connect(name, client_io));
    server_result.expect("server-side handshake should succeed");
    let client_stream = client_result.expect("client-side handshake should succeed");

    let (_, connection) = client_stream.get_ref();
    let chain = connection.peer_certificates().expect("client should observe the server's chain");
    assert_eq!(chain.len(), 2, "leaf + CA should both be sent");
}

fn client_config_with_cert(ca_pem: &str, cert_pem: &str, key_pem: &str) -> rustls::ClientConfig {
    let roots = build_root_store(ca_pem);
    let mut cert_reader = cert_pem.as_bytes();
    let client_chain: Vec<_> = rustls_pemfile::certs(&mut cert_reader)
        .collect::<Result<_, _>>()
        .expect("valid client cert");
    let mut key_reader = key_pem.as_bytes();
    let client_key = rustls_pemfile::private_key(&mut key_reader)
        .expect("read client key")
        .expect("client key present");

    rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(client_chain, client_key)
        .expect("valid client auth cert")
}

#[tokio::test]
async fn mtls_optional_accepts_connection_without_client_cert() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (ca_pem, ca_params, ca_key) = support::generate_ca();
    let issuer = Issuer::from_params(&ca_params, ca_key);
    let leaf = support::issue_server_cert(&issuer, "*.s3.test");
    let (chain_path, key_path) = support::write_chain_and_key(dir.path(), &ca_pem, &leaf);
    let client_ca_path = support::write_pem(dir.path(), "client_ca.pem", &ca_pem);

    let server_config = load_server_config(&TlsConfig {
        cert_chain_path: chain_path,
        private_key_path: key_path,
        client_ca_path: Some(client_ca_path),
    })
    .expect("load server config");

    handshake_ok(Arc::new(server_config), client_config_trusting(&ca_pem), "bucket.s3.test").await;
}

#[tokio::test]
async fn mtls_accepts_a_client_cert_signed_by_the_configured_ca() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (ca_pem, ca_params, ca_key) = support::generate_ca();
    let issuer = Issuer::from_params(&ca_params, ca_key);
    let leaf = support::issue_server_cert(&issuer, "*.s3.test");
    let client_cert = support::issue_client_cert(&issuer, "alice@example.com");
    let (chain_path, key_path) = support::write_chain_and_key(dir.path(), &ca_pem, &leaf);
    let client_ca_path = support::write_pem(dir.path(), "client_ca.pem", &ca_pem);

    let server_config = load_server_config(&TlsConfig {
        cert_chain_path: chain_path,
        private_key_path: key_path,
        client_ca_path: Some(client_ca_path),
    })
    .expect("load server config");

    let client_config = client_config_with_cert(&ca_pem, &client_cert.cert_pem, &client_cert.key_pem);
    handshake_ok(Arc::new(server_config), client_config, "bucket.s3.test").await;
}

#[tokio::test]
async fn mtls_rejects_a_client_cert_signed_by_an_untrusted_ca() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (ca_pem, ca_params, ca_key) = support::generate_ca();
    let issuer = Issuer::from_params(&ca_params, ca_key);
    let leaf = support::issue_server_cert(&issuer, "*.s3.test");
    let (chain_path, key_path) = support::write_chain_and_key(dir.path(), &ca_pem, &leaf);
    let client_ca_path = support::write_pem(dir.path(), "client_ca.pem", &ca_pem);

    let (_other_ca_pem, other_ca_params, other_ca_key) = support::generate_ca();
    let other_issuer = Issuer::from_params(&other_ca_params, other_ca_key);
    let untrusted_client_cert = support::issue_client_cert(&other_issuer, "mallory@example.com");

    let server_config = load_server_config(&TlsConfig {
        cert_chain_path: chain_path,
        private_key_path: key_path,
        client_ca_path: Some(client_ca_path),
    })
    .expect("load server config");

    // The client trusts the *real* server CA (`ca_pem`) so that a handshake
    // failure here can only be attributed to the server's mTLS verifier
    // rejecting the client's certificate — not to the client failing to
    // trust the server.
    let client_config = client_config_with_cert(
        &ca_pem,
        &untrusted_client_cert.cert_pem,
        &untrusted_client_cert.key_pem,
    );

    let (client_io, server_io) = tokio::io::duplex(8192);
    let acceptor = TlsAcceptor::from(Arc::new(server_config));
    let connector = TlsConnector::from(Arc::new(client_config));
    let name = ServerName::try_from("bucket.s3.test".to_string()).expect("valid server name");

    let (server_result, _client_result) =
        tokio::join!(acceptor.accept(server_io), connector.connect(name, client_io));
    assert!(server_result.is_err(), "handshake should fail for an untrusted client cert");
}

#[tokio::test]
async fn reload_picks_up_a_rotated_certificate() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (ca_pem, ca_params, ca_key) = support::generate_ca();
    let issuer = Issuer::from_params(&ca_params, ca_key);
    let first_leaf = support::issue_server_cert(&issuer, "*.s3.test");
    let (chain_path, key_path) = support::write_chain_and_key(dir.path(), &ca_pem, &first_leaf);

    let (other_ca_pem, other_ca_params, other_ca_key) = support::generate_ca();
    let other_issuer = Issuer::from_params(&other_ca_params, other_ca_key);
    let second_leaf = support::issue_server_cert(&other_issuer, "*.s3.test");

    let reloadable = ReloadableConfig::start(TlsConfig {
        cert_chain_path: chain_path.clone(),
        private_key_path: key_path.clone(),
        client_ca_path: None,
    })
    .expect("start reloadable config");

    // Initially, only the first CA's client trusts the served cert.
    handshake_ok(reloadable.current(), client_config_trusting(&ca_pem), "bucket.s3.test").await;

    // Rewrite the cert file with a leaf signed by a *different* CA.
    std::fs::write(&chain_path, format!("{}{}", second_leaf.cert_pem, other_ca_pem)).expect("rewrite chain file");
    std::fs::write(&key_path, &second_leaf.key_pem).expect("rewrite key file");

    // Poll until the swap is observed — file watcher delivery timing isn't guaranteed.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    poll_until_ok(deadline, || async {
        let (client_io, server_io) = tokio::io::duplex(8192);
        let acceptor = TlsAcceptor::from(reloadable.current());
        let connector = TlsConnector::from(Arc::new(client_config_trusting(&other_ca_pem)));
        let name = ServerName::try_from("bucket.s3.test".to_string()).expect("valid server name");
        let (server_result, client_result) =
            tokio::join!(acceptor.accept(server_io), connector.connect(name, client_io));
        server_result.is_ok() && client_result.is_ok()
    })
    .await;
}

/// Restores the process's current directory on drop, even if the test body
/// panics — used only by
/// `reloadable_config_starts_with_bare_filename_paths`, the single test in
/// this crate that touches process-wide current-directory state. No other
/// test (here or elsewhere in the crate) resolves a path relative to the
/// current directory — `tempfile::tempdir()` always yields absolute paths —
/// so this is safe under parallel test execution within this binary.
struct CwdGuard(PathBuf);

impl CwdGuard {
    fn change_to(dir: &std::path::Path) -> Self {
        let original = std::env::current_dir().expect("read current dir");
        std::env::set_current_dir(dir).expect("chdir into tempdir");
        CwdGuard(original)
    }
}

impl Drop for CwdGuard {
    fn drop(&mut self) {
        let _ = std::env::set_current_dir(&self.0);
    }
}

#[tokio::test]
async fn reloadable_config_starts_with_bare_filename_paths() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (ca_pem, ca_params, ca_key) = support::generate_ca();
    let issuer = Issuer::from_params(&ca_params, ca_key);
    let leaf = support::issue_server_cert(&issuer, "*.s3.test");
    let (chain_path, key_path) = support::write_chain_and_key(dir.path(), &ca_pem, &leaf);

    // Bare filenames (no directory component) reproduce the bug directly:
    // `Path::parent()` on a name with no directory returns `Some("")`, and
    // watching "" used to fail outright, so `ReloadableConfig::start` (and
    // thus the whole server) refused to start. Change into the tempdir so
    // the bare filenames below resolve to the real cert/key files.
    let _cwd_guard = CwdGuard::change_to(dir.path());
    let reloadable = ReloadableConfig::start(TlsConfig {
        cert_chain_path: PathBuf::from(chain_path.file_name().expect("chain path has a filename")),
        private_key_path: PathBuf::from(key_path.file_name().expect("key path has a filename")),
        client_ca_path: None,
    })
    .expect("start should succeed with bare filename paths (no directory component)");

    handshake_ok(reloadable.current(), client_config_trusting(&ca_pem), "bucket.s3.test").await;
}

async fn start_tls_server(tls_config: TlsConfig, routing: RoutingConfig) -> maestore::ServerHandle {
    maestore::serve_tls("127.0.0.1:0".parse().unwrap(), routing, tls_config)
        .await
        .expect("server should bind")
}

async fn send_tls_request(
    addr: std::net::SocketAddr,
    client_config: Arc<rustls::ClientConfig>,
    sni: &str,
    request: &str,
) -> String {
    let tcp = tokio::net::TcpStream::connect(addr).await.expect("connect");
    let connector = TlsConnector::from(client_config);
    let name = ServerName::try_from(sni.to_string()).expect("valid server name");
    let mut tls = connector.connect(name, tcp).await.expect("tls handshake");

    tls.write_all(request.as_bytes()).await.expect("write");
    let mut response = Vec::new();
    tls.read_to_end(&mut response).await.expect("read");
    String::from_utf8_lossy(&response).into_owned()
}

#[tokio::test]
async fn serve_tls_handshakes_and_answers_over_a_wildcard_cert() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (ca_pem, ca_params, ca_key) = support::generate_ca();
    let issuer = Issuer::from_params(&ca_params, ca_key);
    let leaf = support::issue_server_cert(&issuer, "*.s3.test");
    let (chain_path, key_path) = support::write_chain_and_key(dir.path(), &ca_pem, &leaf);

    let handle = start_tls_server(
        TlsConfig { cert_chain_path: chain_path, private_key_path: key_path, client_ca_path: None },
        RoutingConfig::default(),
    )
    .await;

    let response = send_tls_request(
        handle.addr(),
        Arc::new(client_config_trusting(&ca_pem)),
        "bucket.s3.test",
        "GET / HTTP/1.1\r\nHost: bucket.s3.test\r\nConnection: close\r\n\r\n",
    )
    .await;

    assert!(response.starts_with("HTTP/1.1 200"));
    assert!(response.contains("ListAllMyBucketsResult"));
}

#[traced_test]
#[tokio::test]
async fn serve_tls_with_client_cert_resolves_identity_from_the_certificate() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (ca_pem, ca_params, ca_key) = support::generate_ca();
    let issuer = Issuer::from_params(&ca_params, ca_key);
    let leaf = support::issue_server_cert(&issuer, "*.s3.test");
    let client_cert = support::issue_client_cert(&issuer, "alice@example.com");
    let (chain_path, key_path) = support::write_chain_and_key(dir.path(), &ca_pem, &leaf);
    let client_ca_path = support::write_pem(dir.path(), "client_ca.pem", &ca_pem);

    let handle = start_tls_server(
        TlsConfig { cert_chain_path: chain_path, private_key_path: key_path, client_ca_path: Some(client_ca_path) },
        RoutingConfig::default(),
    )
    .await;

    let client_config = Arc::new(client_config_with_cert(&ca_pem, &client_cert.cert_pem, &client_cert.key_pem));

    // The Authorization header names a *different* user; if the cert
    // identity weren't taking priority, that's who would be logged.
    let request = "GET / HTTP/1.1\r\nHost: bucket.s3.test\r\nAuthorization: AWS4-HMAC-SHA256 Credential=AKIAOTHER/20260814/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc123\r\nConnection: close\r\n\r\n";
    let response = send_tls_request(handle.addr(), client_config, "bucket.s3.test", request).await;

    assert!(response.starts_with("HTTP/1.1 200"));
    assert!(logs_contain("alice@example.com"));
    assert!(!logs_contain("AKIAOTHER"));
}

#[traced_test]
#[tokio::test]
async fn serve_tls_with_verified_cert_but_no_email_san_warns_and_falls_back() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (ca_pem, ca_params, ca_key) = support::generate_ca();
    let issuer = Issuer::from_params(&ca_params, ca_key);
    let leaf = support::issue_server_cert(&issuer, "*.s3.test");
    // A DNS-SAN-only cert, signed by the trusted client CA: it passes chain
    // validation as a client certificate, but has no rfc822Name SAN for
    // `extract_email_identity` to find — the "verified but no usable
    // identity" case, distinct from "no client cert presented at all".
    let client_cert_with_no_email_san = support::issue_server_cert(&issuer, "no-email.example.com");
    let (chain_path, key_path) = support::write_chain_and_key(dir.path(), &ca_pem, &leaf);
    let client_ca_path = support::write_pem(dir.path(), "client_ca.pem", &ca_pem);

    let handle = start_tls_server(
        TlsConfig { cert_chain_path: chain_path, private_key_path: key_path, client_ca_path: Some(client_ca_path) },
        RoutingConfig::default(),
    )
    .await;

    let client_config = Arc::new(client_config_with_cert(
        &ca_pem,
        &client_cert_with_no_email_san.cert_pem,
        &client_cert_with_no_email_san.key_pem,
    ));

    let request = "GET / HTTP/1.1\r\nHost: bucket.s3.test\r\nConnection: close\r\n\r\n";
    let response = send_tls_request(handle.addr(), client_config, "bucket.s3.test", request).await;

    // The connection succeeds and the request falls back to the (here,
    // header-less) SigV4/anonymous path rather than failing or picking up an
    // identity.
    assert!(response.starts_with("HTTP/1.1 200"), "expected 200, got: {response}");
    assert!(response.contains("ListAllMyBucketsResult"));
    assert!(logs_contain("client certificate verified but has no email SAN"));
}

#[tokio::test]
async fn serve_tls_host_based_routing_resolves_bucket_from_host_header() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (ca_pem, ca_params, ca_key) = support::generate_ca();
    let issuer = Issuer::from_params(&ca_params, ca_key);
    let leaf = support::issue_server_cert(&issuer, "*.s3.test");
    let (chain_path, key_path) = support::write_chain_and_key(dir.path(), &ca_pem, &leaf);

    let handle = start_tls_server(
        TlsConfig { cert_chain_path: chain_path, private_key_path: key_path, client_ca_path: None },
        RoutingConfig { base_domain: Some("s3.test".to_string()) },
    )
    .await;

    let response = send_tls_request(
        handle.addr(),
        Arc::new(client_config_trusting(&ca_pem)),
        "mybucket.s3.test",
        "GET / HTTP/1.1\r\nHost: mybucket.s3.test\r\nConnection: close\r\n\r\n",
    )
    .await;

    // A bucket-level GET (ListObjects, a stub) is 501 — distinct from the 200
    // ListAllMyBucketsResult that plain path-style routing on "/" would
    // produce if the Host header (and SNI) were ignored. Same discriminator
    // tests/integration_test.rs's host-routing test uses, now proven over a
    // real TLS connection (SNI `mybucket.s3.test` against a `*.s3.test`
    // wildcard cert, `Host: mybucket.s3.test`).
    assert!(response.starts_with("HTTP/1.1 501"), "expected 501, got: {response}");
    assert!(response.contains("NotImplemented"));
}

#[tokio::test]
async fn serve_tls_rejects_a_client_cert_from_an_untrusted_ca() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (ca_pem, ca_params, ca_key) = support::generate_ca();
    let issuer = Issuer::from_params(&ca_params, ca_key);
    let leaf = support::issue_server_cert(&issuer, "*.s3.test");
    let (chain_path, key_path) = support::write_chain_and_key(dir.path(), &ca_pem, &leaf);
    let client_ca_path = support::write_pem(dir.path(), "client_ca.pem", &ca_pem);

    let handle = start_tls_server(
        TlsConfig { cert_chain_path: chain_path, private_key_path: key_path, client_ca_path: Some(client_ca_path) },
        RoutingConfig::default(),
    )
    .await;

    let (_other_ca_pem, other_ca_params, other_ca_key) = support::generate_ca();
    let other_issuer = Issuer::from_params(&other_ca_params, other_ca_key);
    let untrusted_client_cert = support::issue_client_cert(&other_issuer, "mallory@example.com");
    // The client trusts the *real* server CA (`ca_pem`), not the untrusted
    // cert's own issuer (`other_ca_pem`). If the client instead trusted
    // `other_ca_pem`, it would reject the *server's* certificate (signed by
    // `ca_pem`) as untrusted, and the handshake would fail on the client's
    // root-store check before the server's mTLS verifier ever evaluated the
    // client's certificate — the assertion below would then pass for the
    // wrong reason. Trusting `ca_pem` isolates the failure to the server's
    // mTLS verifier rejecting the client cert, which is what this test
    // claims to prove.
    let client_config = Arc::new(client_config_with_cert(
        &ca_pem,
        &untrusted_client_cert.cert_pem,
        &untrusted_client_cert.key_pem,
    ));

    let tcp = tokio::net::TcpStream::connect(handle.addr()).await.expect("connect");
    let connector = TlsConnector::from(client_config);
    let name = ServerName::try_from("bucket.s3.test".to_string()).expect("valid server name");

    // In TLS 1.3, client-certificate auth completes the *client's* side of
    // the handshake as soon as it has sent its own Finished message — the
    // client does not wait to learn whether the server accepted its
    // certificate before `connect()` resolves. So `connect()` can return
    // `Ok` even though the server is about to reject the connection: the
    // rejection only surfaces once the client tries to exchange data and
    // gets the server's fatal alert (verified empirically: `connect()`
    // succeeds here, and the following read fails with
    // `AlertReceived(DecryptError)`). A bare `connector.connect(...).is_err()`
    // check (as an earlier draft of this test used) is therefore not
    // sufficient — it can pass on some runs and fail on others depending on
    // exactly how much of the handshake has completed when `connect()`
    // resolves. Instead, treat either an immediate handshake error or a
    // failed request/response round trip as proof of rejection.
    match connector.connect(name, tcp).await {
        Err(_) => {}
        Ok(mut tls) => {
            let request = "GET / HTTP/1.1\r\nHost: bucket.s3.test\r\nConnection: close\r\n\r\n";
            let mut response = Vec::new();
            let outcome = async {
                tls.write_all(request.as_bytes()).await?;
                tls.read_to_end(&mut response).await
            }
            .await;
            assert!(
                outcome.is_err() || response.is_empty(),
                "expected the connection to be rejected, but got a response: {}",
                String::from_utf8_lossy(&response)
            );
        }
    }
}

#[tokio::test]
async fn serve_tls_reloads_a_rotated_certificate_without_restarting() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (ca_pem, ca_params, ca_key) = support::generate_ca();
    let issuer = Issuer::from_params(&ca_params, ca_key);
    let first_leaf = support::issue_server_cert(&issuer, "*.s3.test");
    let (chain_path, key_path) = support::write_chain_and_key(dir.path(), &ca_pem, &first_leaf);

    let (other_ca_pem, other_ca_params, other_ca_key) = support::generate_ca();
    let other_issuer = Issuer::from_params(&other_ca_params, other_ca_key);
    let second_leaf = support::issue_server_cert(&other_issuer, "*.s3.test");

    let handle = start_tls_server(
        TlsConfig { cert_chain_path: chain_path.clone(), private_key_path: key_path.clone(), client_ca_path: None },
        RoutingConfig::default(),
    )
    .await;

    let response = send_tls_request(
        handle.addr(),
        Arc::new(client_config_trusting(&ca_pem)),
        "bucket.s3.test",
        "GET / HTTP/1.1\r\nHost: bucket.s3.test\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 200"));

    std::fs::write(&chain_path, format!("{}{}", second_leaf.cert_pem, other_ca_pem)).expect("rewrite chain file");
    std::fs::write(&key_path, &second_leaf.key_pem).expect("rewrite key file");

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    poll_until_ok(deadline, || async {
        let tcp = tokio::net::TcpStream::connect(handle.addr()).await.expect("connect");
        let connector = TlsConnector::from(Arc::new(client_config_trusting(&other_ca_pem)));
        let name = ServerName::try_from("bucket.s3.test".to_string()).expect("valid server name");
        connector.connect(name, tcp).await.is_ok()
    })
    .await;
}
