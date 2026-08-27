use std::net::SocketAddr;
use std::path::PathBuf;

use maestore::routing::RoutingConfig;
use maestore::tls::TlsConfig;

const BIND_ADDR_ENV_VAR: &str = "OPEN_CONDUCTOR_ADDR";
const DEFAULT_BIND_ADDR: &str = "127.0.0.1:8080";
const BASE_DOMAIN_ENV_VAR: &str = "OPEN_CONDUCTOR_BASE_DOMAIN";
const TLS_CERT_CHAIN_ENV_VAR: &str = "OPEN_CONDUCTOR_TLS_CERT_CHAIN";
const TLS_PRIVATE_KEY_ENV_VAR: &str = "OPEN_CONDUCTOR_TLS_PRIVATE_KEY";
const TLS_CLIENT_CA_ENV_VAR: &str = "OPEN_CONDUCTOR_TLS_CLIENT_CA";

/// Treats an empty (or all-whitespace) string as "not configured". Without
/// this, `OPEN_CONDUCTOR_BASE_DOMAIN=` (set but empty) would produce
/// `RoutingConfig { base_domain: Some("") }`, and `resolve_virtual_hosted_bucket`
/// would then match against the suffix `"."` — i.e. any `Host` ending in a
/// trailing dot — silently turning the entire hostname into a bucket name.
fn normalize_base_domain(value: Option<String>) -> Option<String> {
    value.filter(|v| !v.trim().is_empty())
}

/// Resolves the two/three TLS env vars into either "run plain HTTP" (`Ok(None)`)
/// or "run HTTPS with this config" (`Ok(Some(_))`).
///
/// The cert chain and private key vars are a matched pair: either both are
/// set (TLS) or neither is (plain HTTP). Any other combination is a startup
/// error naming which var is set and which is missing, rather than silently
/// falling back to plaintext — a half-configured TLS setup (e.g. a typo'd env
/// var name) is far more likely to be an operator mistake than an intentional
/// "serve plaintext" request.
///
/// A client-CA bundle configured without TLS itself (cert/key both unset) is
/// also an error: a client-CA bundle has no effect unless it turns on mTLS
/// for the HTTPS listener, so silently ignoring it would mask a
/// misconfiguration the same way silently downgrading to HTTP would.
fn resolve_tls_config(
    cert_chain_path: Option<String>,
    private_key_path: Option<String>,
    client_ca_path: Option<String>,
) -> Result<Option<TlsConfig>, String> {
    match (cert_chain_path, private_key_path) {
        (Some(cert_chain_path), Some(private_key_path)) => Ok(Some(TlsConfig {
            cert_chain_path: PathBuf::from(cert_chain_path),
            private_key_path: PathBuf::from(private_key_path),
            client_ca_path: client_ca_path.map(PathBuf::from),
        })),
        (None, None) => {
            if client_ca_path.is_some() {
                Err(format!(
                    "{TLS_CLIENT_CA_ENV_VAR} is set but TLS is not configured: \
                     set both {TLS_CERT_CHAIN_ENV_VAR} and {TLS_PRIVATE_KEY_ENV_VAR} to enable TLS, \
                     or unset {TLS_CLIENT_CA_ENV_VAR} to run plain HTTP"
                ))
            } else {
                Ok(None)
            }
        }
        (Some(_), None) => Err(half_configured_tls_error(TLS_CERT_CHAIN_ENV_VAR, TLS_PRIVATE_KEY_ENV_VAR)),
        (None, Some(_)) => Err(half_configured_tls_error(TLS_PRIVATE_KEY_ENV_VAR, TLS_CERT_CHAIN_ENV_VAR)),
    }
}

fn half_configured_tls_error(set: &str, missing: &str) -> String {
    format!("{set} is set but {missing} is not; TLS requires both to be set")
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    maestore::logging::init();

    let addr: SocketAddr = std::env::var(BIND_ADDR_ENV_VAR)
        .unwrap_or_else(|_| DEFAULT_BIND_ADDR.to_string())
        .parse()?;

    let routing_config = RoutingConfig {
        base_domain: normalize_base_domain(std::env::var(BASE_DOMAIN_ENV_VAR).ok()),
    };

    let tls_config = resolve_tls_config(
        std::env::var(TLS_CERT_CHAIN_ENV_VAR).ok(),
        std::env::var(TLS_PRIVATE_KEY_ENV_VAR).ok(),
        std::env::var(TLS_CLIENT_CA_ENV_VAR).ok(),
    )?;

    let handle = match tls_config {
        Some(tls_config) => maestore::serve_tls(addr, routing_config, tls_config).await?,
        None => maestore::serve(addr, routing_config).await?,
    };

    tracing::info!(addr = %handle.addr(), "listening");

    tokio::signal::ctrl_c().await?;
    tracing::info!("ctrl-c received, shutting down");
    handle.shutdown().await;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_tls_vars_set_produces_tls_config() {
        let result = resolve_tls_config(
            Some("chain.pem".to_string()),
            Some("key.pem".to_string()),
            None,
        );
        let config = result.expect("should not error").expect("should be Some");
        assert_eq!(config.cert_chain_path, PathBuf::from("chain.pem"));
        assert_eq!(config.private_key_path, PathBuf::from("key.pem"));
        assert_eq!(config.client_ca_path, None);
    }

    #[test]
    fn both_tls_vars_set_with_client_ca_produces_tls_config_with_client_ca() {
        let result = resolve_tls_config(
            Some("chain.pem".to_string()),
            Some("key.pem".to_string()),
            Some("client_ca.pem".to_string()),
        );
        let config = result.expect("should not error").expect("should be Some");
        assert_eq!(config.client_ca_path, Some(PathBuf::from("client_ca.pem")));
    }

    #[test]
    fn neither_tls_var_set_and_no_client_ca_is_plain_http() {
        let result = resolve_tls_config(None, None, None).expect("should not error");
        assert!(result.is_none());
    }

    #[test]
    fn only_cert_chain_set_is_an_error_naming_both_vars() {
        let result = resolve_tls_config(Some("chain.pem".to_string()), None, None);
        let err = result.expect_err("should error on half-configured TLS");
        assert!(err.contains(TLS_CERT_CHAIN_ENV_VAR), "error should name the set var: {err}");
        assert!(err.contains(TLS_PRIVATE_KEY_ENV_VAR), "error should name the missing var: {err}");
    }

    #[test]
    fn only_private_key_set_is_an_error_naming_both_vars() {
        let result = resolve_tls_config(None, Some("key.pem".to_string()), None);
        let err = result.expect_err("should error on half-configured TLS");
        assert!(err.contains(TLS_PRIVATE_KEY_ENV_VAR), "error should name the set var: {err}");
        assert!(err.contains(TLS_CERT_CHAIN_ENV_VAR), "error should name the missing var: {err}");
    }

    #[test]
    fn client_ca_set_without_tls_is_an_error() {
        let result = resolve_tls_config(None, None, Some("client_ca.pem".to_string()));
        let err = result.expect_err("should error: client CA has no effect without TLS");
        assert!(err.contains(TLS_CLIENT_CA_ENV_VAR), "error should name the client CA var: {err}");
    }

    #[test]
    fn empty_base_domain_is_treated_as_unset() {
        assert_eq!(normalize_base_domain(Some(String::new())), None);
        assert_eq!(normalize_base_domain(Some("   ".to_string())), None);
    }

    #[test]
    fn non_empty_base_domain_is_kept() {
        assert_eq!(
            normalize_base_domain(Some("s3.test".to_string())),
            Some("s3.test".to_string())
        );
    }

    #[test]
    fn absent_base_domain_stays_none() {
        assert_eq!(normalize_base_domain(None), None);
    }
}
