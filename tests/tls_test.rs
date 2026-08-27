mod support;

use std::sync::Arc;

use open_conductor::tls::{load_server_config, ReloadableConfig, TlsConfig};
use rcgen::Issuer;
use rustls::pki_types::ServerName;
use tokio_rustls::{TlsAcceptor, TlsConnector};

fn client_config_trusting(ca_pem: &str) -> rustls::ClientConfig {
    let mut roots = rustls::RootCertStore::empty();
    let mut reader = ca_pem.as_bytes();
    for cert in rustls_pemfile::certs(&mut reader) {
        roots.add(cert.expect("valid CA cert")).expect("add CA to root store");
    }
    rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth()
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
    let mut roots = rustls::RootCertStore::empty();
    let mut ca_reader = ca_pem.as_bytes();
    for cert in rustls_pemfile::certs(&mut ca_reader) {
        roots.add(cert.expect("valid CA cert")).expect("add CA to root store");
    }
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
    loop {
        let (client_io, server_io) = tokio::io::duplex(8192);
        let acceptor = TlsAcceptor::from(reloadable.current());
        let connector = TlsConnector::from(Arc::new(client_config_trusting(&other_ca_pem)));
        let name = ServerName::try_from("bucket.s3.test".to_string()).expect("valid server name");
        let (server_result, client_result) =
            tokio::join!(acceptor.accept(server_io), connector.connect(name, client_io));
        if server_result.is_ok() && client_result.is_ok() {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "reload was not observed within the timeout");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}
