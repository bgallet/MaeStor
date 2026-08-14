# Routing Layer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the routing layer for `open-conductor`: parse S3 wire-format HTTP requests into a typed `S3Operation`, dispatch to async handler stubs, extract a best-effort user identity, render S3-shaped XML errors, and log every request as one structured event.

**Architecture:** A single Rust binary+library crate built directly on `hyper` 1.x (`hyper-util` for the server loop, `http-body-util`/`http-body` for bodies) — no axum/tower. Request flow: TCP accept → hyper connection → `service_fn` → parse (method+path+query → `S3Operation`) → auth extract (best-effort) → logging-wrapped dispatch (match → handler stub) → error mapping (→ XML) → response.

**Tech Stack:** Rust, tokio, hyper 1.x, hyper-util, http, http-body, http-body-util, bytes, tracing/tracing-subscriber (JSON), url (form_urlencoded).

**Spec:** `docs/superpowers/specs/2026-08-14-routing-layer-design.md`

## Global Constraints

- Path-style bucket/key addressing only (`/{bucket}/{key}`); no virtual-hosted-style addressing.
- Built directly on raw `hyper` 1.x + `hyper-util` + `http-body-util`; no axum/tower/other web framework.
- Broad operation surface up front (bucket CRUD, object CRUD, ACL, tagging, versioning, multipart) — most handlers are stubs returning `S3Error::NotImplemented`; `ListBuckets` returns a real empty-list success response to prove the path end-to-end.
- `Authorization` header is parsed best-effort for a SigV4 access key (no signature verification); missing/unparseable auth falls back to user `"anonymous"` and the request is still routed.
- Errors render as real S3 XML shape: `<Error><Code>...</Code><Message>...</Message></Error>`, with the matching HTTP status code.
- Every request emits one `tracing` JSON log event: `user`, `operation`, `duration_ms`, `bytes`.
- Single crate (`open-conductor`), modules under `src/`, no workspace split yet.

---

### Task 1: Project scaffolding

**Files:**
- Create: `Cargo.toml`
- Create: `src/lib.rs`
- Create: `src/main.rs`

**Interfaces:**
- Consumes: nothing (first task)
- Produces: a compiling, empty `open_conductor` library crate and a runnable `open-conductor` binary that later tasks build on by adding `pub mod ...;` lines to `src/lib.rs` and replacing `src/main.rs`.

- [ ] **Step 1: Create `Cargo.toml`**

```toml
[package]
name = "open-conductor"
version = "0.1.0"
edition = "2021"

[dependencies]
tokio = { version = "1", features = ["full"] }
hyper = { version = "1", features = ["server", "http1", "http2"] }
hyper-util = { version = "0.1", features = ["full"] }
http = "1"
http-body = "1"
http-body-util = "0.1"
bytes = "1"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["json", "env-filter"] }
url = "2"
```

- [ ] **Step 2: Create an empty library root**

`src/lib.rs`:

```rust
```

(An empty file — this is the crate root that later tasks populate with `pub mod` declarations.)

- [ ] **Step 3: Create a minimal binary entry point**

`src/main.rs`:

```rust
fn main() {
    println!("open-conductor");
}
```

- [ ] **Step 4: Verify the crate builds and runs**

Run: `cargo build && cargo run`
Expected: build succeeds, prints `open-conductor`.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml Cargo.lock src/lib.rs src/main.rs
git commit -m "chore: scaffold open-conductor crate"
```

---

### Task 2: S3 error type and XML rendering

**Files:**
- Create: `src/error.rs`
- Modify: `src/lib.rs` (add `pub mod error;`)

**Interfaces:**
- Consumes: `http::{Response, StatusCode}`, `http_body_util::Full`, `bytes::Bytes` (external crates only)
- Produces: `pub enum S3Error { NoSuchBucket, NoSuchKey, AccessDenied, InvalidRequest, MethodNotAllowed, NotImplemented, Internal }` implementing `Debug + Clone + PartialEq + Eq + std::error::Error + std::fmt::Display`, with methods `code(&self) -> &'static str`, `message(&self) -> &'static str`, `status_code(&self) -> http::StatusCode`, `to_xml(&self) -> String`, `to_response(&self) -> http::Response<http_body_util::Full<bytes::Bytes>>`. Later tasks (routing, handlers, logging) construct and match on these variants.

- [ ] **Step 1: Write the failing tests**

`src/error.rs`:

```rust
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib error`
Expected: FAIL to compile — `S3Error` is not defined yet.

- [ ] **Step 3: Implement `S3Error`**

Prepend this above the `#[cfg(test)]` block in `src/error.rs`:

```rust
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
```

- [ ] **Step 4: Add the module to the crate root**

Modify `src/lib.rs` to contain:

```rust
pub mod error;
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test --lib error`
Expected: PASS (4 tests).

- [ ] **Step 6: Commit**

```bash
git add src/error.rs src/lib.rs
git commit -m "feat: add S3Error type with XML rendering"
```

---

### Task 3: S3Operation enum

**Files:**
- Create: `src/operation.rs`
- Modify: `src/lib.rs` (add `pub mod operation;`)

**Interfaces:**
- Consumes: nothing external beyond `std`
- Produces: `pub enum S3Operation` with these variants (exact names and fields — later tasks depend on this precisely):
  - `ListBuckets`
  - `CreateBucket { bucket: String }`
  - `DeleteBucket { bucket: String }`
  - `HeadBucket { bucket: String }`
  - `ListObjects { bucket: String }`
  - `GetBucketAcl { bucket: String }`
  - `PutBucketAcl { bucket: String }`
  - `GetBucketTagging { bucket: String }`
  - `PutBucketTagging { bucket: String }`
  - `DeleteBucketTagging { bucket: String }`
  - `GetBucketVersioning { bucket: String }`
  - `PutBucketVersioning { bucket: String }`
  - `ListMultipartUploads { bucket: String }`
  - `GetObject { bucket: String, key: String }`
  - `PutObject { bucket: String, key: String }`
  - `DeleteObject { bucket: String, key: String }`
  - `HeadObject { bucket: String, key: String }`
  - `CopyObject { bucket: String, key: String, source: String }`
  - `GetObjectAcl { bucket: String, key: String }`
  - `PutObjectAcl { bucket: String, key: String }`
  - `GetObjectTagging { bucket: String, key: String }`
  - `PutObjectTagging { bucket: String, key: String }`
  - `DeleteObjectTagging { bucket: String, key: String }`
  - `CreateMultipartUpload { bucket: String, key: String }`
  - `UploadPart { bucket: String, key: String, part_number: u32, upload_id: String }`
  - `CompleteMultipartUpload { bucket: String, key: String, upload_id: String }`
  - `AbortMultipartUpload { bucket: String, key: String, upload_id: String }`
  - `ListParts { bucket: String, key: String, upload_id: String }`

  Plus `pub fn name(&self) -> &'static str` and a `Display` impl that writes `name()`. Derives `Debug, Clone, PartialEq, Eq`.

- [ ] **Step 1: Write the failing tests**

`src/operation.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_returns_pascal_case_operation() {
        assert_eq!(S3Operation::ListBuckets.name(), "ListBuckets");
        assert_eq!(
            S3Operation::GetObject {
                bucket: "b".to_string(),
                key: "k".to_string(),
            }
            .name(),
            "GetObject"
        );
    }

    #[test]
    fn display_matches_name() {
        let op = S3Operation::CreateBucket {
            bucket: "b".to_string(),
        };
        assert_eq!(format!("{op}"), "CreateBucket");
    }

    #[test]
    fn variants_carry_expected_fields() {
        let op = S3Operation::UploadPart {
            bucket: "b".to_string(),
            key: "k".to_string(),
            part_number: 3,
            upload_id: "u1".to_string(),
        };
        assert_eq!(
            op,
            S3Operation::UploadPart {
                bucket: "b".to_string(),
                key: "k".to_string(),
                part_number: 3,
                upload_id: "u1".to_string(),
            }
        );
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib operation`
Expected: FAIL to compile — `S3Operation` is not defined yet.

- [ ] **Step 3: Implement `S3Operation`**

Prepend this above the `#[cfg(test)]` block in `src/operation.rs`:

```rust
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum S3Operation {
    ListBuckets,
    CreateBucket { bucket: String },
    DeleteBucket { bucket: String },
    HeadBucket { bucket: String },
    ListObjects { bucket: String },
    GetBucketAcl { bucket: String },
    PutBucketAcl { bucket: String },
    GetBucketTagging { bucket: String },
    PutBucketTagging { bucket: String },
    DeleteBucketTagging { bucket: String },
    GetBucketVersioning { bucket: String },
    PutBucketVersioning { bucket: String },
    ListMultipartUploads { bucket: String },
    GetObject { bucket: String, key: String },
    PutObject { bucket: String, key: String },
    DeleteObject { bucket: String, key: String },
    HeadObject { bucket: String, key: String },
    CopyObject { bucket: String, key: String, source: String },
    GetObjectAcl { bucket: String, key: String },
    PutObjectAcl { bucket: String, key: String },
    GetObjectTagging { bucket: String, key: String },
    PutObjectTagging { bucket: String, key: String },
    DeleteObjectTagging { bucket: String, key: String },
    CreateMultipartUpload { bucket: String, key: String },
    UploadPart {
        bucket: String,
        key: String,
        part_number: u32,
        upload_id: String,
    },
    CompleteMultipartUpload {
        bucket: String,
        key: String,
        upload_id: String,
    },
    AbortMultipartUpload {
        bucket: String,
        key: String,
        upload_id: String,
    },
    ListParts {
        bucket: String,
        key: String,
        upload_id: String,
    },
}

impl S3Operation {
    pub fn name(&self) -> &'static str {
        match self {
            S3Operation::ListBuckets => "ListBuckets",
            S3Operation::CreateBucket { .. } => "CreateBucket",
            S3Operation::DeleteBucket { .. } => "DeleteBucket",
            S3Operation::HeadBucket { .. } => "HeadBucket",
            S3Operation::ListObjects { .. } => "ListObjects",
            S3Operation::GetBucketAcl { .. } => "GetBucketAcl",
            S3Operation::PutBucketAcl { .. } => "PutBucketAcl",
            S3Operation::GetBucketTagging { .. } => "GetBucketTagging",
            S3Operation::PutBucketTagging { .. } => "PutBucketTagging",
            S3Operation::DeleteBucketTagging { .. } => "DeleteBucketTagging",
            S3Operation::GetBucketVersioning { .. } => "GetBucketVersioning",
            S3Operation::PutBucketVersioning { .. } => "PutBucketVersioning",
            S3Operation::ListMultipartUploads { .. } => "ListMultipartUploads",
            S3Operation::GetObject { .. } => "GetObject",
            S3Operation::PutObject { .. } => "PutObject",
            S3Operation::DeleteObject { .. } => "DeleteObject",
            S3Operation::HeadObject { .. } => "HeadObject",
            S3Operation::CopyObject { .. } => "CopyObject",
            S3Operation::GetObjectAcl { .. } => "GetObjectAcl",
            S3Operation::PutObjectAcl { .. } => "PutObjectAcl",
            S3Operation::GetObjectTagging { .. } => "GetObjectTagging",
            S3Operation::PutObjectTagging { .. } => "PutObjectTagging",
            S3Operation::DeleteObjectTagging { .. } => "DeleteObjectTagging",
            S3Operation::CreateMultipartUpload { .. } => "CreateMultipartUpload",
            S3Operation::UploadPart { .. } => "UploadPart",
            S3Operation::CompleteMultipartUpload { .. } => "CompleteMultipartUpload",
            S3Operation::AbortMultipartUpload { .. } => "AbortMultipartUpload",
            S3Operation::ListParts { .. } => "ListParts",
        }
    }
}

impl fmt::Display for S3Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}
```

- [ ] **Step 4: Add the module to the crate root**

Modify `src/lib.rs` to add the line:

```rust
pub mod operation;
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test --lib operation`
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
git add src/operation.rs src/lib.rs
git commit -m "feat: add S3Operation enum"
```

---

### Task 4: Best-effort auth extraction

**Files:**
- Create: `src/auth.rs`
- Modify: `src/lib.rs` (add `pub mod auth;`)

**Interfaces:**
- Consumes: `http::HeaderMap`
- Produces: `pub const ANONYMOUS_USER: &str = "anonymous";` and `pub fn extract_user(headers: &http::HeaderMap) -> String`. Used by `src/lib.rs`'s `handle_request` (Task 8) and by `logging.rs`'s tests (Task 7).

- [ ] **Step 1: Write the failing tests**

`src/auth.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, HeaderValue};

    #[test]
    fn missing_header_falls_back_to_anonymous() {
        let headers = HeaderMap::new();
        assert_eq!(extract_user(&headers), ANONYMOUS_USER);
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
        assert_eq!(extract_user(&headers), "AKIAEXAMPLE");
    }

    #[test]
    fn malformed_header_falls_back_to_anonymous() {
        let mut headers = HeaderMap::new();
        headers.insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_static("not-a-sigv4-header"),
        );
        assert_eq!(extract_user(&headers), ANONYMOUS_USER);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib auth`
Expected: FAIL to compile — `extract_user` is not defined yet.

- [ ] **Step 3: Implement `extract_user`**

Prepend this above the `#[cfg(test)]` block in `src/auth.rs`:

```rust
use http::HeaderMap;

pub const ANONYMOUS_USER: &str = "anonymous";

pub fn extract_user(headers: &HeaderMap) -> String {
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
```

- [ ] **Step 4: Add the module to the crate root**

Modify `src/lib.rs` to add the line:

```rust
pub mod auth;
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test --lib auth`
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
git add src/auth.rs src/lib.rs
git commit -m "feat: add best-effort SigV4 user extraction"
```

---

### Task 5: Request routing (path/query parser)

**Files:**
- Create: `src/routing.rs`
- Modify: `src/lib.rs` (add `pub mod routing;`)

**Interfaces:**
- Consumes: `S3Operation` (Task 3, all variants), `S3Error::{InvalidRequest, MethodNotAllowed}` (Task 2), `http::{Method, HeaderMap}`
- Produces: `pub fn parse_request(method: &http::Method, path: &str, query: Option<&str>, headers: &http::HeaderMap) -> Result<S3Operation, S3Error>`. Used by `handle_request` in `src/lib.rs` (Task 8).

- [ ] **Step 1: Write the failing tests**

`src/routing.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::S3Error;
    use crate::operation::S3Operation;
    use http::{HeaderMap, HeaderValue, Method};

    fn headers_with_copy_source(source: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-amz-copy-source",
            HeaderValue::from_str(source).unwrap(),
        );
        headers
    }

    #[test]
    fn table_driven_parse_cases() {
        let empty = HeaderMap::new();
        let cases: Vec<(Method, &str, Option<&str>, S3Operation)> = vec![
            (Method::GET, "/", None, S3Operation::ListBuckets),
            (
                Method::PUT,
                "/my-bucket",
                None,
                S3Operation::CreateBucket {
                    bucket: "my-bucket".to_string(),
                },
            ),
            (
                Method::DELETE,
                "/my-bucket",
                None,
                S3Operation::DeleteBucket {
                    bucket: "my-bucket".to_string(),
                },
            ),
            (
                Method::HEAD,
                "/my-bucket",
                None,
                S3Operation::HeadBucket {
                    bucket: "my-bucket".to_string(),
                },
            ),
            (
                Method::GET,
                "/my-bucket",
                None,
                S3Operation::ListObjects {
                    bucket: "my-bucket".to_string(),
                },
            ),
            (
                Method::GET,
                "/my-bucket",
                Some("acl"),
                S3Operation::GetBucketAcl {
                    bucket: "my-bucket".to_string(),
                },
            ),
            (
                Method::PUT,
                "/my-bucket",
                Some("acl"),
                S3Operation::PutBucketAcl {
                    bucket: "my-bucket".to_string(),
                },
            ),
            (
                Method::PUT,
                "/my-bucket",
                Some("versioning"),
                S3Operation::PutBucketVersioning {
                    bucket: "my-bucket".to_string(),
                },
            ),
            (
                Method::GET,
                "/my-bucket",
                Some("uploads"),
                S3Operation::ListMultipartUploads {
                    bucket: "my-bucket".to_string(),
                },
            ),
            (
                Method::GET,
                "/my-bucket/my-key",
                None,
                S3Operation::GetObject {
                    bucket: "my-bucket".to_string(),
                    key: "my-key".to_string(),
                },
            ),
            (
                Method::HEAD,
                "/my-bucket/my-key",
                None,
                S3Operation::HeadObject {
                    bucket: "my-bucket".to_string(),
                    key: "my-key".to_string(),
                },
            ),
            (
                Method::DELETE,
                "/my-bucket/my-key",
                None,
                S3Operation::DeleteObject {
                    bucket: "my-bucket".to_string(),
                    key: "my-key".to_string(),
                },
            ),
            (
                Method::PUT,
                "/my-bucket/my-key",
                None,
                S3Operation::PutObject {
                    bucket: "my-bucket".to_string(),
                    key: "my-key".to_string(),
                },
            ),
            (
                Method::GET,
                "/my-bucket/my-key",
                Some("acl"),
                S3Operation::GetObjectAcl {
                    bucket: "my-bucket".to_string(),
                    key: "my-key".to_string(),
                },
            ),
            (
                Method::PUT,
                "/my-bucket/my-key",
                Some("tagging"),
                S3Operation::PutObjectTagging {
                    bucket: "my-bucket".to_string(),
                    key: "my-key".to_string(),
                },
            ),
            (
                Method::POST,
                "/my-bucket/my-key",
                Some("uploads"),
                S3Operation::CreateMultipartUpload {
                    bucket: "my-bucket".to_string(),
                    key: "my-key".to_string(),
                },
            ),
            (
                Method::PUT,
                "/my-bucket/my-key",
                Some("partNumber=2&uploadId=up1"),
                S3Operation::UploadPart {
                    bucket: "my-bucket".to_string(),
                    key: "my-key".to_string(),
                    part_number: 2,
                    upload_id: "up1".to_string(),
                },
            ),
            (
                Method::POST,
                "/my-bucket/my-key",
                Some("uploadId=up1"),
                S3Operation::CompleteMultipartUpload {
                    bucket: "my-bucket".to_string(),
                    key: "my-key".to_string(),
                    upload_id: "up1".to_string(),
                },
            ),
            (
                Method::DELETE,
                "/my-bucket/my-key",
                Some("uploadId=up1"),
                S3Operation::AbortMultipartUpload {
                    bucket: "my-bucket".to_string(),
                    key: "my-key".to_string(),
                    upload_id: "up1".to_string(),
                },
            ),
            (
                Method::GET,
                "/my-bucket/my-key",
                Some("uploadId=up1"),
                S3Operation::ListParts {
                    bucket: "my-bucket".to_string(),
                    key: "my-key".to_string(),
                    upload_id: "up1".to_string(),
                },
            ),
        ];

        for (method, path, query, expected) in cases {
            let result = parse_request(&method, path, query, &empty);
            assert_eq!(
                result,
                Ok(expected.clone()),
                "method={method:?} path={path} query={query:?}"
            );
        }
    }

    #[test]
    fn put_object_with_copy_source_header_is_copy_object() {
        let headers = headers_with_copy_source("/src-bucket/src-key");
        let result = parse_request(&Method::PUT, "/dst-bucket/dst-key", None, &headers);
        assert_eq!(
            result,
            Ok(S3Operation::CopyObject {
                bucket: "dst-bucket".to_string(),
                key: "dst-key".to_string(),
                source: "/src-bucket/src-key".to_string(),
            })
        );
    }

    #[test]
    fn upload_part_without_part_number_is_invalid_request() {
        let empty = HeaderMap::new();
        let result = parse_request(
            &Method::PUT,
            "/my-bucket/my-key",
            Some("uploadId=up1"),
            &empty,
        );
        assert_eq!(result, Err(S3Error::InvalidRequest));
    }

    #[test]
    fn unsupported_bucket_method_is_method_not_allowed() {
        let empty = HeaderMap::new();
        let result = parse_request(&Method::POST, "/my-bucket", None, &empty);
        assert_eq!(result, Err(S3Error::MethodNotAllowed));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib routing`
Expected: FAIL to compile — `parse_request` is not defined yet.

- [ ] **Step 3: Implement the parser**

Prepend this above the `#[cfg(test)]` block in `src/routing.rs`:

```rust
use std::collections::HashMap;

use http::{HeaderMap, Method};

use crate::error::S3Error;
use crate::operation::S3Operation;

pub fn parse_request(
    method: &Method,
    path: &str,
    query: Option<&str>,
    headers: &HeaderMap,
) -> Result<S3Operation, S3Error> {
    let params = parse_query(query);
    let trimmed = path.trim_start_matches('/');

    if trimmed.is_empty() {
        return match *method {
            Method::GET => Ok(S3Operation::ListBuckets),
            _ => Err(S3Error::MethodNotAllowed),
        };
    }

    match trimmed.split_once('/') {
        Some((bucket, key)) if !key.is_empty() => {
            parse_object_operation(method, bucket, key, &params, headers)
        }
        Some((bucket, _)) => parse_bucket_operation(method, bucket, &params),
        None => parse_bucket_operation(method, trimmed, &params),
    }
}

fn parse_query(query: Option<&str>) -> HashMap<String, String> {
    match query {
        None => HashMap::new(),
        Some(q) => url::form_urlencoded::parse(q.as_bytes())
            .into_owned()
            .collect(),
    }
}

fn parse_bucket_operation(
    method: &Method,
    bucket: &str,
    params: &HashMap<String, String>,
) -> Result<S3Operation, S3Error> {
    let bucket = bucket.to_string();

    if params.contains_key("acl") {
        return match *method {
            Method::GET => Ok(S3Operation::GetBucketAcl { bucket }),
            Method::PUT => Ok(S3Operation::PutBucketAcl { bucket }),
            _ => Err(S3Error::MethodNotAllowed),
        };
    }
    if params.contains_key("tagging") {
        return match *method {
            Method::GET => Ok(S3Operation::GetBucketTagging { bucket }),
            Method::PUT => Ok(S3Operation::PutBucketTagging { bucket }),
            Method::DELETE => Ok(S3Operation::DeleteBucketTagging { bucket }),
            _ => Err(S3Error::MethodNotAllowed),
        };
    }
    if params.contains_key("versioning") {
        return match *method {
            Method::GET => Ok(S3Operation::GetBucketVersioning { bucket }),
            Method::PUT => Ok(S3Operation::PutBucketVersioning { bucket }),
            _ => Err(S3Error::MethodNotAllowed),
        };
    }
    if params.contains_key("uploads") {
        return match *method {
            Method::GET => Ok(S3Operation::ListMultipartUploads { bucket }),
            _ => Err(S3Error::MethodNotAllowed),
        };
    }

    match *method {
        Method::GET => Ok(S3Operation::ListObjects { bucket }),
        Method::PUT => Ok(S3Operation::CreateBucket { bucket }),
        Method::DELETE => Ok(S3Operation::DeleteBucket { bucket }),
        Method::HEAD => Ok(S3Operation::HeadBucket { bucket }),
        _ => Err(S3Error::MethodNotAllowed),
    }
}

fn parse_object_operation(
    method: &Method,
    bucket: &str,
    key: &str,
    params: &HashMap<String, String>,
    headers: &HeaderMap,
) -> Result<S3Operation, S3Error> {
    let bucket = bucket.to_string();
    let key = key.to_string();

    if params.contains_key("acl") {
        return match *method {
            Method::GET => Ok(S3Operation::GetObjectAcl { bucket, key }),
            Method::PUT => Ok(S3Operation::PutObjectAcl { bucket, key }),
            _ => Err(S3Error::MethodNotAllowed),
        };
    }
    if params.contains_key("tagging") {
        return match *method {
            Method::GET => Ok(S3Operation::GetObjectTagging { bucket, key }),
            Method::PUT => Ok(S3Operation::PutObjectTagging { bucket, key }),
            Method::DELETE => Ok(S3Operation::DeleteObjectTagging { bucket, key }),
            _ => Err(S3Error::MethodNotAllowed),
        };
    }
    if params.contains_key("uploads") {
        return match *method {
            Method::POST => Ok(S3Operation::CreateMultipartUpload { bucket, key }),
            _ => Err(S3Error::MethodNotAllowed),
        };
    }
    if let Some(upload_id) = params.get("uploadId") {
        let upload_id = upload_id.clone();
        return match *method {
            Method::PUT => {
                let part_number = params
                    .get("partNumber")
                    .ok_or(S3Error::InvalidRequest)?
                    .parse::<u32>()
                    .map_err(|_| S3Error::InvalidRequest)?;
                Ok(S3Operation::UploadPart {
                    bucket,
                    key,
                    part_number,
                    upload_id,
                })
            }
            Method::POST => Ok(S3Operation::CompleteMultipartUpload {
                bucket,
                key,
                upload_id,
            }),
            Method::DELETE => Ok(S3Operation::AbortMultipartUpload {
                bucket,
                key,
                upload_id,
            }),
            Method::GET => Ok(S3Operation::ListParts {
                bucket,
                key,
                upload_id,
            }),
            _ => Err(S3Error::MethodNotAllowed),
        };
    }

    match *method {
        Method::GET => Ok(S3Operation::GetObject { bucket, key }),
        Method::HEAD => Ok(S3Operation::HeadObject { bucket, key }),
        Method::DELETE => Ok(S3Operation::DeleteObject { bucket, key }),
        Method::PUT => {
            if let Some(source) = headers.get("x-amz-copy-source") {
                let source = source
                    .to_str()
                    .map_err(|_| S3Error::InvalidRequest)?
                    .to_string();
                Ok(S3Operation::CopyObject { bucket, key, source })
            } else {
                Ok(S3Operation::PutObject { bucket, key })
            }
        }
        _ => Err(S3Error::MethodNotAllowed),
    }
}
```

- [ ] **Step 4: Add the module to the crate root**

Modify `src/lib.rs` to add the line:

```rust
pub mod routing;
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test --lib routing`
Expected: PASS (4 tests, including the 20-case table-driven test).

- [ ] **Step 6: Commit**

```bash
git add src/routing.rs src/lib.rs
git commit -m "feat: add S3 request routing/parsing"
```

---

### Task 6: Handler stubs and dispatch

**Files:**
- Create: `src/handlers/mod.rs`
- Create: `src/handlers/bucket.rs`
- Create: `src/handlers/object.rs`
- Modify: `src/operation.rs` (add `dispatch`)
- Modify: `src/lib.rs` (add `pub mod handlers;`)

**Interfaces:**
- Consumes: `S3Operation` (Task 3, all variants), `S3Error` (Task 2)
- Produces: `pub async fn dispatch(op: &S3Operation) -> Result<http::Response<http_body_util::Full<bytes::Bytes>>, S3Error>` in `src/operation.rs`. Used by `logging::log_dispatch` (Task 7).

- [ ] **Step 1: Write the failing tests**

`src/handlers/bucket.rs`:

```rust
use bytes::Bytes;
use http::{Response, StatusCode};
use http_body_util::Full;

use crate::error::S3Error;

pub async fn list_buckets() -> Result<Response<Full<Bytes>>, S3Error> {
    let body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
<ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
<Owner><ID>anonymous</ID><DisplayName>anonymous</DisplayName></Owner>\
<Buckets></Buckets>\
</ListAllMyBucketsResult>";

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/xml")
        .body(Full::new(Bytes::from(body)))
        .expect("building ListBuckets response should never fail"))
}

pub async fn create_bucket(_bucket: &str) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn delete_bucket(_bucket: &str) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn head_bucket(_bucket: &str) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn list_objects(_bucket: &str) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn get_bucket_acl(_bucket: &str) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn put_bucket_acl(_bucket: &str) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn get_bucket_tagging(_bucket: &str) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn put_bucket_tagging(_bucket: &str) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn delete_bucket_tagging(_bucket: &str) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn get_bucket_versioning(_bucket: &str) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn put_bucket_versioning(_bucket: &str) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn list_multipart_uploads(_bucket: &str) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn list_buckets_returns_ok() {
        let response = list_buckets().await.expect("should succeed");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn create_bucket_is_not_yet_implemented() {
        let result = create_bucket("my-bucket").await;
        assert_eq!(result.unwrap_err(), S3Error::NotImplemented);
    }
}
```

`src/handlers/object.rs`:

```rust
use bytes::Bytes;
use http::Response;
use http_body_util::Full;

use crate::error::S3Error;

pub async fn get_object(_bucket: &str, _key: &str) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn put_object(_bucket: &str, _key: &str) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn delete_object(_bucket: &str, _key: &str) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn head_object(_bucket: &str, _key: &str) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn copy_object(
    _bucket: &str,
    _key: &str,
    _source: &str,
) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn get_object_acl(_bucket: &str, _key: &str) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn put_object_acl(_bucket: &str, _key: &str) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn get_object_tagging(
    _bucket: &str,
    _key: &str,
) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn put_object_tagging(
    _bucket: &str,
    _key: &str,
) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn delete_object_tagging(
    _bucket: &str,
    _key: &str,
) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn create_multipart_upload(
    _bucket: &str,
    _key: &str,
) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn upload_part(
    _bucket: &str,
    _key: &str,
    _part_number: u32,
    _upload_id: &str,
) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn complete_multipart_upload(
    _bucket: &str,
    _key: &str,
    _upload_id: &str,
) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn abort_multipart_upload(
    _bucket: &str,
    _key: &str,
    _upload_id: &str,
) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

pub async fn list_parts(
    _bucket: &str,
    _key: &str,
    _upload_id: &str,
) -> Result<Response<Full<Bytes>>, S3Error> {
    Err(S3Error::NotImplemented)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn get_object_is_not_yet_implemented() {
        let result = get_object("my-bucket", "my-key").await;
        assert_eq!(result.unwrap_err(), S3Error::NotImplemented);
    }
}
```

`src/handlers/mod.rs`:

```rust
pub mod bucket;
pub mod object;
```

Add a dispatch test to `src/operation.rs`'s existing `#[cfg(test)] mod tests` block (append this test function inside that block):

```rust
    #[tokio::test]
    async fn dispatch_list_buckets_succeeds() {
        let response = dispatch(&S3Operation::ListBuckets).await;
        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn dispatch_create_bucket_is_not_implemented() {
        let op = S3Operation::CreateBucket {
            bucket: "b".to_string(),
        };
        let result = dispatch(&op).await;
        assert_eq!(result.unwrap_err(), crate::error::S3Error::NotImplemented);
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib handlers operation`
Expected: FAIL to compile — `handlers` module and `dispatch` function don't exist yet.

- [ ] **Step 3: Wire up the handlers module and dispatch function**

Add the module declaration to `src/lib.rs`:

```rust
pub mod handlers;
```

Append this to `src/operation.rs`, above the existing `#[cfg(test)]` block:

```rust
use bytes::Bytes;
use http::Response;
use http_body_util::Full;

use crate::error::S3Error;
use crate::handlers;

pub async fn dispatch(op: &S3Operation) -> Result<Response<Full<Bytes>>, S3Error> {
    match op {
        S3Operation::ListBuckets => handlers::bucket::list_buckets().await,
        S3Operation::CreateBucket { bucket } => handlers::bucket::create_bucket(bucket).await,
        S3Operation::DeleteBucket { bucket } => handlers::bucket::delete_bucket(bucket).await,
        S3Operation::HeadBucket { bucket } => handlers::bucket::head_bucket(bucket).await,
        S3Operation::ListObjects { bucket } => handlers::bucket::list_objects(bucket).await,
        S3Operation::GetBucketAcl { bucket } => handlers::bucket::get_bucket_acl(bucket).await,
        S3Operation::PutBucketAcl { bucket } => handlers::bucket::put_bucket_acl(bucket).await,
        S3Operation::GetBucketTagging { bucket } => {
            handlers::bucket::get_bucket_tagging(bucket).await
        }
        S3Operation::PutBucketTagging { bucket } => {
            handlers::bucket::put_bucket_tagging(bucket).await
        }
        S3Operation::DeleteBucketTagging { bucket } => {
            handlers::bucket::delete_bucket_tagging(bucket).await
        }
        S3Operation::GetBucketVersioning { bucket } => {
            handlers::bucket::get_bucket_versioning(bucket).await
        }
        S3Operation::PutBucketVersioning { bucket } => {
            handlers::bucket::put_bucket_versioning(bucket).await
        }
        S3Operation::ListMultipartUploads { bucket } => {
            handlers::bucket::list_multipart_uploads(bucket).await
        }
        S3Operation::GetObject { bucket, key } => handlers::object::get_object(bucket, key).await,
        S3Operation::PutObject { bucket, key } => handlers::object::put_object(bucket, key).await,
        S3Operation::DeleteObject { bucket, key } => {
            handlers::object::delete_object(bucket, key).await
        }
        S3Operation::HeadObject { bucket, key } => {
            handlers::object::head_object(bucket, key).await
        }
        S3Operation::CopyObject { bucket, key, source } => {
            handlers::object::copy_object(bucket, key, source).await
        }
        S3Operation::GetObjectAcl { bucket, key } => {
            handlers::object::get_object_acl(bucket, key).await
        }
        S3Operation::PutObjectAcl { bucket, key } => {
            handlers::object::put_object_acl(bucket, key).await
        }
        S3Operation::GetObjectTagging { bucket, key } => {
            handlers::object::get_object_tagging(bucket, key).await
        }
        S3Operation::PutObjectTagging { bucket, key } => {
            handlers::object::put_object_tagging(bucket, key).await
        }
        S3Operation::DeleteObjectTagging { bucket, key } => {
            handlers::object::delete_object_tagging(bucket, key).await
        }
        S3Operation::CreateMultipartUpload { bucket, key } => {
            handlers::object::create_multipart_upload(bucket, key).await
        }
        S3Operation::UploadPart {
            bucket,
            key,
            part_number,
            upload_id,
        } => handlers::object::upload_part(bucket, key, *part_number, upload_id).await,
        S3Operation::CompleteMultipartUpload {
            bucket,
            key,
            upload_id,
        } => handlers::object::complete_multipart_upload(bucket, key, upload_id).await,
        S3Operation::AbortMultipartUpload {
            bucket,
            key,
            upload_id,
        } => handlers::object::abort_multipart_upload(bucket, key, upload_id).await,
        S3Operation::ListParts {
            bucket,
            key,
            upload_id,
        } => handlers::object::list_parts(bucket, key, upload_id).await,
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib handlers operation`
Expected: PASS (4 handler tests + 2 new dispatch tests + the 3 existing operation tests from Task 3).

- [ ] **Step 5: Commit**

```bash
git add src/handlers/ src/operation.rs src/lib.rs
git commit -m "feat: add handler stubs and operation dispatch"
```

---

### Task 7: Structured request logging

**Files:**
- Create: `src/logging.rs`
- Modify: `src/lib.rs` (add `pub mod logging;`)

**Interfaces:**
- Consumes: `S3Operation` (Task 3), `S3Error` (Task 2), `operation::dispatch` (Task 6)
- Produces: `pub fn init()` (sets up a JSON `tracing` subscriber) and `pub async fn log_dispatch(user: &str, op: S3Operation) -> Result<http::Response<http_body_util::Full<bytes::Bytes>>, S3Error>`. Used by `handle_request` in `src/lib.rs` (Task 8).

- [ ] **Step 1: Write the failing tests**

`src/logging.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::operation::S3Operation;
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib logging`
Expected: FAIL to compile — `log_dispatch` is not defined yet.

- [ ] **Step 3: Implement logging setup and the dispatch wrapper**

Prepend this above the `#[cfg(test)]` block in `src/logging.rs`:

```rust
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
```

- [ ] **Step 4: Add the module to the crate root**

Modify `src/lib.rs` to add the line:

```rust
pub mod logging;
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test --lib logging`
Expected: PASS (2 tests).

- [ ] **Step 6: Commit**

```bash
git add src/logging.rs src/lib.rs
git commit -m "feat: add structured JSON request logging"
```

---

### Task 8: Server wiring and integration test

**Files:**
- Modify: `src/lib.rs` (add `handle_request` and `serve`)
- Modify: `src/main.rs` (replace with real server bootstrap)
- Create: `tests/integration_test.rs`

**Interfaces:**
- Consumes: `auth::extract_user` (Task 4), `routing::parse_request` (Task 5), `logging::{init, log_dispatch}` (Task 7), `error::S3Error` (Task 2)
- Produces: `pub async fn handle_request(req: http::Request<hyper::body::Incoming>) -> Result<http::Response<http_body_util::Full<bytes::Bytes>>, std::convert::Infallible>` and `pub async fn serve(addr: std::net::SocketAddr) -> std::io::Result<std::net::SocketAddr>` in `src/lib.rs`. `serve` binds and returns the bound address (letting callers/tests use port `0` for an ephemeral port), then serves connections in a background task for the life of the process/test.

- [ ] **Step 1: Write the failing integration test**

`tests/integration_test.rs`:

```rust
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};

async fn start_server() -> SocketAddr {
    open_conductor::serve("127.0.0.1:0".parse().unwrap())
        .await
        .expect("server should bind")
}

fn send_request(addr: SocketAddr, request: &str) -> String {
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream.write_all(request.as_bytes()).expect("write");
    let mut response = String::new();
    stream.read_to_string(&mut response).expect("read");
    response
}

#[tokio::test]
async fn list_buckets_returns_ok() {
    let addr = start_server().await;
    let request = "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n".to_string();

    let response = tokio::task::spawn_blocking(move || send_request(addr, &request))
        .await
        .expect("blocking task should not panic");

    assert!(response.starts_with("HTTP/1.1 200"));
    assert!(response.contains("ListAllMyBucketsResult"));
}

#[tokio::test]
async fn put_object_acl_returns_not_implemented() {
    let addr = start_server().await;
    let request = "PUT /my-bucket/my-key?acl HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: 0\r\n\r\n".to_string();

    let response = tokio::task::spawn_blocking(move || send_request(addr, &request))
        .await
        .expect("blocking task should not panic");

    assert!(response.starts_with("HTTP/1.1 501"));
    assert!(response.contains("NotImplemented"));
}

#[tokio::test]
async fn unknown_bucket_method_returns_method_not_allowed() {
    let addr = start_server().await;
    let request =
        "POST /my-bucket HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
            .to_string();

    let response = tokio::task::spawn_blocking(move || send_request(addr, &request))
        .await
        .expect("blocking task should not panic");

    assert!(response.starts_with("HTTP/1.1 405"));
    assert!(response.contains("MethodNotAllowed"));
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test --test integration_test`
Expected: FAIL to compile — `open_conductor::serve` does not exist yet.

- [ ] **Step 3: Implement `handle_request` and `serve` in `src/lib.rs`**

Append this to `src/lib.rs` (after the existing `pub mod` lines):

```rust
use std::net::SocketAddr;

use bytes::Bytes;
use http::{Request, Response};
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use tokio::net::TcpListener;

pub async fn handle_request(
    req: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, std::convert::Infallible> {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let query = req.uri().query().map(str::to_string);
    let headers = req.headers().clone();

    let user = auth::extract_user(&headers);

    let response = match routing::parse_request(&method, &path, query.as_deref(), &headers) {
        Ok(op) => logging::log_dispatch(&user, op).await,
        Err(err) => Err(err),
    };

    Ok(response.unwrap_or_else(|err| err.to_response()))
}

pub async fn serve(addr: SocketAddr) -> std::io::Result<SocketAddr> {
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;

    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(pair) => pair,
                Err(err) => {
                    tracing::error!(error = %err, "accept error");
                    continue;
                }
            };
            let io = TokioIo::new(stream);
            tokio::spawn(async move {
                let builder = ConnBuilder::new(TokioExecutor::new());
                if let Err(err) = builder.serve_connection(io, service_fn(handle_request)).await {
                    tracing::error!(error = %err, "connection error");
                }
            });
        }
    });

    Ok(local_addr)
}
```

- [ ] **Step 4: Replace `src/main.rs` with the real server bootstrap**

`src/main.rs`:

```rust
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    open_conductor::logging::init();

    let addr: SocketAddr = "127.0.0.1:8080".parse()?;
    let bound = open_conductor::serve(addr).await?;
    tracing::info!(%bound, "listening");

    std::future::pending::<()>().await;
    Ok(())
}
```

- [ ] **Step 5: Run all tests to verify everything passes**

Run: `cargo test`
Expected: PASS — all unit tests from Tasks 2-7 plus the 3 new integration tests.

- [ ] **Step 6: Manually verify the server runs**

Run: `cargo run` (in one terminal), then in another: `curl -i http://127.0.0.1:8080/`
Expected: HTTP 200 response containing `ListAllMyBucketsResult`. Stop the server with Ctrl+C.

- [ ] **Step 7: Commit**

```bash
git add src/lib.rs src/main.rs tests/integration_test.rs
git commit -m "feat: wire up hyper server and add routing integration tests"
```

---

## Self-Review Notes

- **Spec coverage:** path-style parsing + sub-resource query matching (Task 5), broad operation surface (Tasks 3/5/6), best-effort auth with anonymous fallback (Task 4), S3 XML error rendering (Task 2), structured JSON logging with user/operation/duration/bytes (Task 7), hand-rolled router on raw hyper/hyper-util/http-body-util (Tasks 1, 8), single crate layout (all tasks) — all covered.
- **Type consistency:** `S3Operation` variant names/fields (Task 3) match exactly what `routing.rs` (Task 5) constructs and what `operation::dispatch` (Task 6) matches on and what `handlers::*` (Task 6) accept. `S3Error` variants (Task 2) match what `routing.rs` and `handlers::*` return.
- **No placeholders:** every step contains complete, real code; stub handlers return real `S3Error::NotImplemented` values rather than TODO comments.
