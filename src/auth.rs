use http::HeaderMap;

pub const ANONYMOUS_USER: &str = "anonymous";

pub fn extract_user(headers: &HeaderMap, peer_identity: Option<&str>) -> String {
    if let Some(identity) = peer_identity {
        return identity.to_string();
    }
    let Some(value) = headers.get(http::header::AUTHORIZATION) else {
        return ANONYMOUS_USER.to_string();
    };
    let Ok(value) = value.to_str() else {
        return ANONYMOUS_USER.to_string();
    };
    parse_access_key(value).unwrap_or_else(|| ANONYMOUS_USER.to_string())
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
        assert_eq!(extract_user(&headers, None), ANONYMOUS_USER);
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
        assert_eq!(extract_user(&headers, None), "AKIAEXAMPLE");
    }

    #[test]
    fn malformed_header_falls_back_to_anonymous() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_static("not-a-sigv4-header"),
        );
        assert_eq!(extract_user(&headers, None), ANONYMOUS_USER);
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
        assert_eq!(extract_user(&headers, Some("alice@example.com")), "alice@example.com");
    }

    #[test]
    fn missing_peer_identity_falls_back_to_header_parsing() {
        let empty = HeaderMap::new();
        assert_eq!(extract_user(&empty, None), ANONYMOUS_USER);
    }
}
