mod support;

use std::sync::Arc;

use open_conductor::tls::{load_server_config, TlsConfig};
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
