// This module is a shared test fixture library that later TLS tasks will
// keep extending; not every helper is used by every test binary that
// includes it.
#![allow(dead_code)]

use std::path::{Path, PathBuf};

use rcgen::string::Ia5String;
use rcgen::{BasicConstraints, CertificateParams, Issuer, IsCa, KeyPair, SanType};

pub struct GeneratedCert {
    pub cert_pem: String,
    pub key_pem: String,
}

/// Returns the new CA's own cert PEM plus the params/key needed to build an
/// `Issuer` from it (`Issuer::from_params(&params, key)`) — returned
/// separately, not as a struct, so callers don't hit partial-move errors
/// when they need the cert PEM after moving the key into an `Issuer`.
pub fn generate_ca() -> (String, CertificateParams, KeyPair) {
    let key = KeyPair::generate().expect("generate CA key");
    let mut params = CertificateParams::new(Vec::<String>::new()).expect("CA params");
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let cert = params.self_signed(&key).expect("self-sign CA");
    (cert.pem(), params, key)
}

/// A leaf certificate signed by `issuer`, with `dns_name` as its only SAN
/// (e.g. a wildcard like `"*.s3.test"`).
pub fn issue_server_cert(issuer: &Issuer<'_, KeyPair>, dns_name: &str) -> GeneratedCert {
    let key = KeyPair::generate().expect("generate leaf key");
    let mut params = CertificateParams::new(Vec::<String>::new()).expect("leaf params");
    params.subject_alt_names = vec![SanType::DnsName(
        Ia5String::try_from(dns_name).expect("valid IA5 DNS name"),
    )];
    let cert = params.signed_by(&key, issuer).expect("sign leaf cert");
    GeneratedCert { cert_pem: cert.pem(), key_pem: key.serialize_pem() }
}

/// Writes `leaf` followed by `ca_cert_pem` as a two-entry chain file (the
/// "give the chain" case), plus `leaf`'s key, into `dir`. Returns their paths.
pub fn write_chain_and_key(dir: &Path, ca_cert_pem: &str, leaf: &GeneratedCert) -> (PathBuf, PathBuf) {
    let chain_path = dir.join("chain.pem");
    let key_path = dir.join("key.pem");
    std::fs::write(&chain_path, format!("{}{}", leaf.cert_pem, ca_cert_pem)).expect("write chain file");
    std::fs::write(&key_path, &leaf.key_pem).expect("write key file");
    (chain_path, key_path)
}

pub fn write_pem(dir: &Path, filename: &str, pem: &str) -> PathBuf {
    let path = dir.join(filename);
    std::fs::write(&path, pem).expect("write PEM file");
    path
}
