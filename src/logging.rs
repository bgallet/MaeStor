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

pub async fn log_dispatch(
    user: &str,
    op: S3Operation,
) -> Result<Response<Full<Bytes>>, S3Error> {
    let start = Instant::now();
    let op_name = op.name();

    let result = dispatch(&op).await;
    let duration_ms = start.elapsed().as_millis();

    let (status, bytes) = match &result {
        Ok(resp) => (
            resp.status().as_u16(),
            resp.body().size_hint().exact().unwrap_or(0),
        ),
        Err(err) => (err.status_code().as_u16(), 0),
    };

    info!(
        user,
        operation = op_name,
        duration_ms,
        bytes,
        status,
        "s3_request"
    );

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::StatusCode;

    #[tokio::test]
    async fn log_dispatch_forwards_success_result() {
        let result = log_dispatch("test-user", S3Operation::ListBuckets).await;
        assert_eq!(result.expect("should succeed").status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn log_dispatch_forwards_error_result() {
        let op = S3Operation::CreateBucket {
            bucket: "b".to_string(),
        };
        let result = log_dispatch("test-user", op).await;
        assert_eq!(
            result.unwrap_err(),
            crate::error::S3Error::NotImplemented
        );
    }
}
