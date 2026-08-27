use std::time::Instant;

use bytes::Bytes;
use http::Response;
use http_body::Body as _;
use http_body_util::Full;
use tracing::info;

use crate::auth::AuthMethod;
use crate::error::S3Error;
use crate::handlers::dispatch;
use crate::operation::S3Operation;

/// Initializes the global JSON tracing subscriber. Honors `RUST_LOG` (falling
/// back to `info` when unset). Safe to call more than once — later calls are
/// no-ops rather than panicking, since a second `main`-style init would
/// otherwise crash a process that only wanted to log a warning.
pub fn init() {
    let _ = tracing_subscriber::fmt()
        .json()
        .with_current_span(false)
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(tracing::level_filters::LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .try_init();
}

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
/// the body of the response that actually goes out over the wire. `operation`
/// is `None` when routing itself failed, since there is no `S3Operation` to
/// report in that case. `auth_method` records how `user` was established
/// (`"client_cert"`, `"sigv4_header"`, or `"anonymous"`) — distinct
/// information from `user` itself, since a bare header-derived `user` string
/// is an unverified claim (SigV4 signature checking is still a stub) while a
/// `client_cert` identity has passed TLS chain validation.
pub async fn log_request(
    user: &str,
    auth_method: AuthMethod,
    parsed: Result<S3Operation, S3Error>,
) -> Response<Full<Bytes>> {
    let start = Instant::now();

    let (operation, result) = match parsed {
        Ok(op) => {
            let name = op.name();
            (Some(name), dispatch(&op).await)
        }
        Err(err) => (None, Err(err)),
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
        auth_method = auth_method.as_str(),
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
    use tracing_test::traced_test;

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
        let response = log_request("test-user", AuthMethod::Anonymous, Ok(S3Operation::ListBuckets)).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert!(body_string(response).await.contains("ListAllMyBucketsResult"));
    }

    #[tokio::test]
    async fn log_request_converts_dispatch_error_to_response() {
        let op = S3Operation::CreateBucket {
            bucket: "b".to_string(),
        };
        let response = log_request("test-user", AuthMethod::Anonymous, Ok(op)).await;
        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
        assert!(body_string(response).await.contains("NotImplemented"));
    }

    #[tokio::test]
    async fn log_request_converts_parse_error_to_response() {
        let response = log_request("test-user", AuthMethod::Anonymous, Err(S3Error::MethodNotAllowed)).await;
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert!(body_string(response).await.contains("MethodNotAllowed"));
    }

    #[tokio::test]
    async fn error_responses_have_a_non_empty_measurable_body() {
        let response = log_request("test-user", AuthMethod::Anonymous, Err(S3Error::MethodNotAllowed)).await;
        let bytes = response.body().size_hint().exact().expect("exact size hint");
        assert!(bytes > 0, "error responses must report real byte counts");
    }

    // The tests above only ever assert on the `Response` `log_request` hands
    // back — none of them look at what actually got logged. The tests below
    // close that gap.

    #[traced_test]
    #[tokio::test]
    async fn log_request_emits_s3_request_with_operation_name_on_success() {
        let _ = log_request("test-user", AuthMethod::Anonymous, Ok(S3Operation::ListBuckets)).await;
        assert!(logs_contain("s3_request"));
        assert!(logs_contain("ListBuckets"));
    }

    #[traced_test]
    #[tokio::test]
    async fn log_request_emits_s3_request_with_operation_name_on_dispatch_error() {
        let op = S3Operation::CreateBucket {
            bucket: "b".to_string(),
        };
        let _ = log_request("test-user", AuthMethod::Anonymous, Ok(op)).await;
        assert!(logs_contain("s3_request"));
        assert!(logs_contain("CreateBucket"));
    }

    #[traced_test]
    #[tokio::test]
    async fn log_request_emits_s3_request_without_an_operation_name_on_parse_error() {
        let _ = log_request("test-user", AuthMethod::Anonymous, Err(S3Error::MethodNotAllowed)).await;
        assert!(logs_contain("s3_request"));
        // `operation` is `Option<&str>`; tracing's blanket `Value` impl for
        // `Option<T>` skips the visitor entirely when the value is `None`, so
        // the field should not appear in the log output at all — not even as
        // an empty or null value. Checked as "operation=" (its rendered
        // key=value form), not the bare word "operation", since this test's
        // own name is included in the captured output as a span name and
        // would otherwise trivially match a plain substring check.
        assert!(!logs_contain("operation="));
    }

    /// A `MakeWriter` that clones share the same underlying buffer, so a test
    /// can hand a writer to a subscriber while keeping a handle to read back
    /// whatever it wrote.
    #[derive(Clone, Default)]
    struct SharedBuffer(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for SharedBuffer {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().write(buf)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for SharedBuffer {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// Runs `log_request` under a subscriber built with our actual production
    /// formatter config (`.json().with_current_span(false)`, same as
    /// `init()`), captures its output, and returns the JSON object for the
    /// `s3_request` event. Unlike `#[traced_test]` (which uses its own
    /// text-based formatter), this exercises the exact JSON shape `init()`
    /// configures — the level `tracing-test`'s `logs_contain` can't reach.
    async fn s3_request_json(
        user: &str,
        auth_method: AuthMethod,
        parsed: Result<S3Operation, S3Error>,
    ) -> serde_json::Value {
        let buffer = SharedBuffer::default();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_current_span(false)
            .with_writer(buffer.clone())
            .finish();

        {
            let _guard = tracing::subscriber::set_default(subscriber);
            let _ = log_request(user, auth_method, parsed).await;
        }

        let captured = buffer.0.lock().unwrap();
        String::from_utf8_lossy(&captured)
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("each log line should be valid JSON"))
            .find(|line| line["fields"]["message"] == "s3_request")
            .expect("s3_request event should have been logged")
    }

    #[tokio::test]
    async fn s3_request_event_serializes_numeric_fields_as_json_numbers() {
        let event = s3_request_json("test-user", AuthMethod::Anonymous, Ok(S3Operation::ListBuckets)).await;

        assert!(
            event["fields"]["duration_ms"].is_number(),
            "duration_ms should serialize as a JSON number, not a string: {event}"
        );
        assert!(
            event["fields"]["bytes"].is_number(),
            "bytes should serialize as a JSON number: {event}"
        );
        assert!(
            event["fields"]["status"].is_number(),
            "status should serialize as a JSON number: {event}"
        );
    }

    #[tokio::test]
    async fn s3_request_event_omits_operation_field_on_parse_error() {
        let event = s3_request_json("test-user", AuthMethod::Anonymous, Err(S3Error::MethodNotAllowed)).await;

        assert!(
            event["fields"].get("operation").is_none(),
            "operation field should be omitted entirely for parse errors: {event}"
        );
    }

    #[tokio::test]
    async fn s3_request_event_records_client_cert_auth_method() {
        let event = s3_request_json(
            "alice@example.com",
            AuthMethod::ClientCert,
            Ok(S3Operation::ListBuckets),
        )
        .await;

        assert_eq!(event["fields"]["auth_method"], "client_cert", "{event}");
        assert_eq!(event["fields"]["user"], "alice@example.com", "{event}");
    }

    #[tokio::test]
    async fn s3_request_event_records_sigv4_header_auth_method() {
        let event = s3_request_json(
            "AKIAEXAMPLE",
            AuthMethod::SigV4Header,
            Ok(S3Operation::ListBuckets),
        )
        .await;

        assert_eq!(event["fields"]["auth_method"], "sigv4_header", "{event}");
        assert_eq!(event["fields"]["user"], "AKIAEXAMPLE", "{event}");
    }

    #[tokio::test]
    async fn s3_request_event_records_anonymous_auth_method() {
        let event = s3_request_json(
            "anonymous",
            AuthMethod::Anonymous,
            Ok(S3Operation::ListBuckets),
        )
        .await;

        assert_eq!(event["fields"]["auth_method"], "anonymous", "{event}");
    }
}
