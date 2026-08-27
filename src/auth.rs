use http::HeaderMap;

pub const ANONYMOUS_USER: &str = "anonymous";

/// How a request's identity was established. Threaded into the audit log
/// (`logging::log_request`) as `auth_method`, distinct from the `user` string
/// itself, so a log reader can tell a cryptographically-verified client-cert
/// identity apart from a bare, unverified claim in a SigV4 header (today's
/// SigV4 signature checking is still a stub — see the SSL/TLS design doc).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMethod {
    /// Identity came from a client certificate that passed TLS chain
    /// validation and had an email SAN.
    ClientCert,
    /// Identity came from parsing a SigV4 `Authorization` header's
    /// `Credential=` field. The signature itself is not yet verified.
    SigV4Header,
    /// No identity could be established (no client cert, no/unparseable
    /// `Authorization` header).
    Anonymous,
}

impl AuthMethod {
    pub fn as_str(&self) -> &'static str {
        match self {
            AuthMethod::ClientCert => "client_cert",
            AuthMethod::SigV4Header => "sigv4_header",
            AuthMethod::Anonymous => "anonymous",
        }
    }
}

/// A request's resolved identity: who it's from, and how that was decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    pub user: String,
    pub method: AuthMethod,
}

pub fn extract_user(headers: &HeaderMap, peer_identity: Option<&str>) -> Identity {
    if let Some(identity) = peer_identity {
        return Identity {
            user: identity.to_string(),
            method: AuthMethod::ClientCert,
        };
    }
    let Some(value) = headers.get(http::header::AUTHORIZATION) else {
        return Identity {
            user: ANONYMOUS_USER.to_string(),
            method: AuthMethod::Anonymous,
        };
    };
    let Ok(value) = value.to_str() else {
        return Identity {
            user: ANONYMOUS_USER.to_string(),
            method: AuthMethod::Anonymous,
        };
    };
    match parse_access_key(value) {
        Some(user) => Identity {
            user,
            method: AuthMethod::SigV4Header,
        },
        None => Identity {
            user: ANONYMOUS_USER.to_string(),
            method: AuthMethod::Anonymous,
        },
    }
}

fn parse_access_key(auth_header: &str) -> Option<String> {
    let credential_marker = "Credential=";
    let start = auth_header.find(credential_marker)? + credential_marker.len();
    let rest = &auth_header[start..];
    let end = rest.find(',').unwrap_or(rest.len());
    let credential = &rest[..end];
    let access_key = credential.split('/').next()?;
    if access_key.is_empty() {
        None
    } else {
        Some(access_key.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, HeaderValue};

    #[test]
    fn missing_header_falls_back_to_anonymous() {
        let headers = HeaderMap::new();
        let identity = extract_user(&headers, None);
        assert_eq!(identity.user, ANONYMOUS_USER);
        assert_eq!(identity.method, AuthMethod::Anonymous);
    }

    #[test]
    fn valid_sigv4_header_extracts_access_key() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_static(
                "AWS4-HMAC-SHA256 Credential=AKIAEXAMPLE/20260814/us-east-1/s3/aws4_request, \
                 SignedHeaders=host;x-amz-date, Signature=abc123",
            ),
        );
        let identity = extract_user(&headers, None);
        assert_eq!(identity.user, "AKIAEXAMPLE");
        assert_eq!(identity.method, AuthMethod::SigV4Header);
    }

    #[test]
    fn malformed_header_falls_back_to_anonymous() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_static("not-a-sigv4-header"),
        );
        let identity = extract_user(&headers, None);
        assert_eq!(identity.user, ANONYMOUS_USER);
        assert_eq!(identity.method, AuthMethod::Anonymous);
    }

    #[test]
    fn client_cert_identity_takes_priority_over_signature_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_static(
                "AWS4-HMAC-SHA256 Credential=AKIAEXAMPLE/20260814/us-east-1/s3/aws4_request, \
                 SignedHeaders=host;x-amz-date, Signature=abc123",
            ),
        );
        let identity = extract_user(&headers, Some("alice@example.com"));
        assert_eq!(identity.user, "alice@example.com");
        assert_eq!(identity.method, AuthMethod::ClientCert);
    }

    #[test]
    fn missing_peer_identity_falls_back_to_header_parsing() {
        let empty = HeaderMap::new();
        let identity = extract_user(&empty, None);
        assert_eq!(identity.user, ANONYMOUS_USER);
        assert_eq!(identity.method, AuthMethod::Anonymous);
    }
}
