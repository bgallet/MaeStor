use std::time::Instant;

use bytes::Bytes;
use http::Response;
use http_body::Body as _;
use http_body_util::Full;
use tracing::info;

use crate::error::S3Error;
use crate::operation::{dispatch, S3Operation};

pub fn init() {
    tracing_subscriber::fmt()
        .json()
        .with_current_span(false)
        .init();
}

/// Operation name reported when the request could not be parsed into an
/// `S3Operation` at all.
const PARSE_ERROR_OPERATION: &str = "ParseError";

/// The single owner of the `s3_request` log event.
///
/// Takes the output of `routing::parse_request` and drives it all the way to a
/// final `Response`:
///
/// * `Ok(op)` — times the dispatch, converting a handler error into its XML
///   error response.
/// * `Err(err)` — no dispatch happens (duration is ~0), the parse error is
///   converted into its XML error response.
///
/// Either way exactly one `s3_request` event is emitted, and `bytes` measures
/// the body of the response that actually goes out over the wire.
pub async fn log_request(
    user: &str,
    parsed: Result<S3Operation, S3Error>,
) -> Response<Full<Bytes>> {
    let start = Instant::now();

    let (operation, result) = match parsed {
        Ok(op) => {
            let name = op.name();
            (name, dispatch(&op).await)
        }
        Err(err) => (PARSE_ERROR_OPERATION, Err(err)),
    };

    let duration_ms = start.elapsed().as_millis() as u64;

    let response = match result {
        Ok(response) => response,
        Err(err) => err.to_response(),
    };

    let status = response.status().as_u16();
    let bytes = response.body().size_hint().exact().unwrap_or(0);

    info!(
        user,
        operation,
        duration_ms,
        bytes,
        status,
        "s3_request"
    );

    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::StatusCode;
    use http_body_util::BodyExt;

    async fn body_string(response: Response<Full<Bytes>>) -> String {
        let bytes = response
            .into_body()
            .collect()
            .await
            .expect("collect body")
            .to_bytes();
        String::from_utf8(bytes.to_vec()).expect("valid utf8")
    }

    #[tokio::test]
    async fn log_request_forwards_success_result() {
        let response = log_request("test-user", Ok(S3Operation::ListBuckets)).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(body_string(response).await.contains("ListAllMyBucketsResult"));
    }

    #[tokio::test]
    async fn log_request_converts_dispatch_error_to_response() {
        let op = S3Operation::CreateBucket {
            bucket: "b".to_string(),
        };
        let response = log_request("test-user", Ok(op)).await;
        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
        assert!(body_string(response).await.contains("NotImplemented"));
    }

    #[tokio::test]
    async fn log_request_converts_parse_error_to_response() {
        let response = log_request("test-user", Err(S3Error::MethodNotAllowed)).await;
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert!(body_string(response).await.contains("MethodNotAllowed"));
    }

    #[tokio::test]
    async fn error_responses_have_a_non_empty_measurable_body() {
        let response = log_request("test-user", Err(S3Error::MethodNotAllowed)).await;
        let bytes = response.body().size_hint().exact().expect("exact size hint");
        assert!(bytes > 0, "error responses must report real byte counts");
    }
}
