use bytes::Bytes;
use http::{Response, StatusCode};
use http_body_util::Full;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum S3Error {
    NoSuchBucket,
    NoSuchKey,
    AccessDenied,
    InvalidRequest,
    MethodNotAllowed,
    NotImplemented,
    Internal,
}

impl S3Error {
    pub fn code(&self) -> &'static str {
        match self {
            S3Error::NoSuchBucket => "NoSuchBucket",
            S3Error::NoSuchKey => "NoSuchKey",
            S3Error::AccessDenied => "AccessDenied",
            S3Error::InvalidRequest => "InvalidRequest",
            S3Error::MethodNotAllowed => "MethodNotAllowed",
            S3Error::NotImplemented => "NotImplemented",
            S3Error::Internal => "InternalError",
        }
    }

    pub fn message(&self) -> &'static str {
        match self {
            S3Error::NoSuchBucket => "The specified bucket does not exist.",
            S3Error::NoSuchKey => "The specified key does not exist.",
            S3Error::AccessDenied => "Access Denied.",
            S3Error::InvalidRequest => "The request was invalid.",
            S3Error::MethodNotAllowed => {
                "The specified method is not allowed against this resource."
            }
            S3Error::NotImplemented => "This operation is not implemented yet.",
            S3Error::Internal => "We encountered an internal error. Please try again.",
        }
    }

    pub fn status_code(&self) -> StatusCode {
        match self {
            S3Error::NoSuchBucket => StatusCode::NOT_FOUND,
            S3Error::NoSuchKey => StatusCode::NOT_FOUND,
            S3Error::AccessDenied => StatusCode::FORBIDDEN,
            S3Error::InvalidRequest => StatusCode::BAD_REQUEST,
            S3Error::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            S3Error::NotImplemented => StatusCode::NOT_IMPLEMENTED,
            S3Error::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub fn to_xml(&self) -> String {
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>{}</Code><Message>{}</Message></Error>",
            self.code(),
            self.message()
        )
    }

    pub fn to_response(&self) -> Response<Full<Bytes>> {
        Response::builder()
            .status(self.status_code())
            .header("Content-Type", "application/xml")
            .body(Full::new(Bytes::from(self.to_xml())))
            .expect("building an S3Error response should never fail")
    }
}

impl fmt::Display for S3Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code(), self.message())
    }
}

impl std::error::Error for S3Error {}

#[cfg(test)]
mod tests {
    use super::*;
    use http::StatusCode;
    use http_body_util::BodyExt;

    #[test]
    fn no_such_bucket_maps_to_404() {
        assert_eq!(S3Error::NoSuchBucket.status_code(), StatusCode::NOT_FOUND);
        assert_eq!(S3Error::NoSuchBucket.code(), "NoSuchBucket");
    }

    #[test]
    fn not_implemented_maps_to_501() {
        assert_eq!(S3Error::NotImplemented.status_code(), StatusCode::NOT_IMPLEMENTED);
    }

    #[test]
    fn to_xml_contains_code_and_message() {
        let xml = S3Error::AccessDenied.to_xml();
        assert!(xml.contains("<Code>AccessDenied</Code>"));
        assert!(xml.contains("<Message>"));
    }

    #[tokio::test]
    async fn to_response_renders_expected_status_and_body() {
        let response = S3Error::NoSuchBucket.to_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);

        let body = response
            .into_body()
            .collect()
            .await
            .expect("collect body")
            .to_bytes();
        let body_str = String::from_utf8(body.to_vec()).expect("valid utf8");
        assert!(body_str.contains("NoSuchBucket"));
    }
}
