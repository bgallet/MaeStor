use std::sync::Arc;

use x509_parser::certificate::X509Certificate;
use x509_parser::extensions::GeneralName;
use x509_parser::prelude::FromDer;

/// Extracts the first rfc822Name (email) Subject Alternative Name from a
/// DER-encoded certificate, if present. Returns `None` — never an error —
/// for a cert with no SAN extension or no email entry: a peer cert lacking
/// an email identity is a normal case the caller falls back from, not a
/// failure.
pub fn extract_email_identity(cert_der: &[u8]) -> Option<String> {
    let (_, cert) = X509Certificate::from_der(cert_der).ok()?;
    let san = cert.subject_alternative_name().ok()??;
    san.value.general_names.iter().find_map(|name| match name {
        GeneralName::RFC822Name(email) => Some(email.to_string()),
        _ => None,
    })
}

/// Resolves the identity to use for requests on a TLS connection from its
/// verified peer certificate, if any. Distinguishes "no client cert
/// presented" (silent — mTLS client certs are always optional) from "a cert
/// was presented and passed chain validation, but has no email SAN" (logged
/// at `warn`, since it's a verified identity that still falls back to header
/// auth). Returns `Arc<str>` rather than `String` so cloning it once per
/// request on a keep-alive connection is a refcount bump, not an allocation.
pub fn resolve_peer_identity(connection: &rustls::ServerConnection) -> Option<Arc<str>> {
    let cert = connection.peer_certificates()?.first()?;
    let identity = extract_email_identity(cert);
    if identity.is_none() {
        tracing::warn!("client certificate verified but has no email SAN; falling back to header auth");
    }
    identity.map(|email| Arc::from(email.as_str()))
}

#[cfg(test)]
mod tests {
    use super::*;

    use rcgen::string::Ia5String;
    use rcgen::{CertificateParams, KeyPair, SanType};

    #[test]
    fn extracts_email_san_from_a_self_signed_cert() {
        let key = KeyPair::generate().expect("generate key");
        let mut params = CertificateParams::new(Vec::<String>::new()).expect("params");
        params.subject_alt_names = vec![SanType::Rfc822Name(
            Ia5String::try_from("alice@example.com").expect("ia5"),
        )];
        let cert = params.self_signed(&key).expect("self-sign");

        assert_eq!(extract_email_identity(cert.der()), Some("alice@example.com".to_string()));
    }

    #[test]
    fn returns_none_when_there_is_no_email_san() {
        let key = KeyPair::generate().expect("generate key");
        let mut params = CertificateParams::new(Vec::<String>::new()).expect("params");
        params.subject_alt_names = vec![SanType::DnsName(
            Ia5String::try_from("host.example.com").expect("ia5"),
        )];
        let cert = params.self_signed(&key).expect("self-sign");

        assert_eq!(extract_email_identity(cert.der()), None);
    }
}
