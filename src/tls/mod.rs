use std::fs::File;
use std::io::{self, BufReader};
use std::path::{Path, PathBuf};

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::ServerConfig;

mod identity;
pub use identity::extract_email_identity;

mod reload;
pub use reload::ReloadableConfig;

#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub cert_chain_path: PathBuf,
    pub private_key_path: PathBuf,
    pub client_ca_path: Option<PathBuf>,
}

#[derive(Debug)]
pub enum TlsError {
    Io(io::Error),
    NoCertificates,
    NoPrivateKey,
    Rustls(rustls::Error),
    ClientVerifier(rustls::server::VerifierBuilderError),
}

impl std::fmt::Display for TlsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TlsError::Io(err) => write!(f, "I/O error loading TLS material: {err}"),
            TlsError::NoCertificates => write!(f, "no certificates found in chain file"),
            TlsError::NoPrivateKey => write!(f, "no private key found in key file"),
            TlsError::Rustls(err) => write!(f, "TLS configuration error: {err}"),
            TlsError::ClientVerifier(err) => write!(f, "client certificate verifier error: {err}"),
        }
    }
}

impl std::error::Error for TlsError {}

impl From<io::Error> for TlsError {
    fn from(err: io::Error) -> Self {
        TlsError::Io(err)
    }
}

impl From<rustls::Error> for TlsError {
    fn from(err: rustls::Error) -> Self {
        TlsError::Rustls(err)
    }
}

impl From<rustls::server::VerifierBuilderError> for TlsError {
    fn from(err: rustls::server::VerifierBuilderError) -> Self {
        TlsError::ClientVerifier(err)
    }
}

fn load_cert_chain(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let mut reader = BufReader::new(File::open(path)?);
    let certs = rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>()?;
    if certs.is_empty() {
        return Err(TlsError::NoCertificates);
    }
    Ok(certs)
}

fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsError> {
    let mut reader = BufReader::new(File::open(path)?);
    rustls_pemfile::private_key(&mut reader)?.ok_or(TlsError::NoPrivateKey)
}

/// Loads a TLS server config from the configured chain/key files. Every
/// certificate found in `cert_chain_path` is sent to the peer, in file
/// order — this is what lets a chain be supplied as one concatenated PEM
/// file (leaf then intermediates), and what makes a wildcard leaf work: the
/// server never inspects SNI to pick among multiple certs, so the same
/// chain is presented regardless of the hostname the client dialed.
pub fn load_server_config(config: &TlsConfig) -> Result<ServerConfig, TlsError> {
    // Idempotent: a process-level crypto provider must be installed before
    // `ServerConfig::builder()` works. `install_default` returns `Err` if
    // one is already installed (e.g. a prior reload) — expected, not a
    // failure.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let chain = load_cert_chain(&config.cert_chain_path)?;
    let key = load_private_key(&config.private_key_path)?;

    let builder = ServerConfig::builder();
    let builder = match &config.client_ca_path {
        Some(client_ca_path) => {
            let ca_certs = load_cert_chain(client_ca_path)?;
            let mut roots = rustls::RootCertStore::empty();
            for cert in ca_certs {
                roots.add(cert)?;
            }
            let verifier = rustls::server::WebPkiClientVerifier::builder(std::sync::Arc::new(roots))
                .allow_unauthenticated()
                .build()?;
            builder.with_client_cert_verifier(verifier)
        }
        None => builder.with_no_client_auth(),
    };

    Ok(builder.with_single_cert(chain, key)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_cert_file_is_an_io_error() {
        let config = TlsConfig {
            cert_chain_path: PathBuf::from("/nonexistent/chain.pem"),
            private_key_path: PathBuf::from("/nonexistent/key.pem"),
            client_ca_path: None,
        };
        assert!(matches!(load_server_config(&config), Err(TlsError::Io(_))));
    }
}
