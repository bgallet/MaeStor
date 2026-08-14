# Routing Layer Design

Date: 2026-08-14
Status: Approved

## Context

`open-conductor` will eventually be a proxy that emulates the AWS S3 API in
front of pluggable storage backends, with caching and a pluggable metadata
store (SQLite first), optimized for AI training workloads. Every action will
be logged with the acting user, the action performed, how long it took, and
the number of bytes transferred.

That full system is too large for a single design/spec. This document covers
only the first sub-project: **the routing layer** — parsing an incoming HTTP
request into a typed S3 operation and dispatching it to an async handler.
Storage backends, caching, and the metadata system are out of scope here and
will each get their own design pass later. Handlers built in this phase are
stubs; they prove the routing/dispatch/logging/error-mapping path end to end
but do not talk to real storage yet.

## Goals

- Parse S3 wire-format requests (path-style bucket/key addressing, plus
  query-string sub-resource markers like `?acl`, `?uploads`, `?tagging`,
  `?versioning`) into a typed `S3Operation`.
- Dispatch each `S3Operation` to a dedicated async handler function.
- Cover a broad surface of S3 operations up front (core object/bucket CRUD,
  ACL, multipart upload, tagging, versioning), even though most handlers
  will initially return "not implemented."
- Extract a best-effort user identity from the `Authorization` header
  (SigV4 shape) without verifying the signature.
- Map errors to S3-shaped XML error responses with correct HTTP status
  codes.
- Log every request as one structured event: user, operation, duration,
  bytes transferred.

## Non-goals (deferred to later sub-projects)

- Actual signature verification / access control.
- Talking to real storage backends.
- Caching.
- The metadata system (SQLite-backed).
- Virtual-hosted-style bucket addressing (`bucket.s3.amazonaws.com`) —
  path-style only for now.

## Architecture

Built on raw `hyper` 1.x + `hyper-util` (server loop/executor) +
`http-body-util` (body helpers) + `tokio`. No axum/tower — the router is
hand-rolled, per the requirement to build directly on Hyper.

Request flow:

```
TCP accept
  -> hyper connection
  -> service_fn entry point
  -> parse: method + path + query -> S3Operation
  -> auth extract: Authorization header -> user (best-effort, "anonymous" fallback)
  -> dispatch: match on S3Operation -> async handler
  -> handler returns Result<Response, S3Error>
  -> error mapping: S3Error -> XML error body + status code
  -> logging: tracing JSON event {user, operation, duration_ms, bytes}
     wraps parse-through-response
```

## Components

1. **`routing.rs`** — path/query parser. Path-style addressing:
   `/{bucket}` and `/{bucket}/{key...}`. Inspects the query string for
   sub-resource markers (`?acl`, `?uploads`, `?tagging`, `?versioning`,
   etc.) and combines them with the HTTP method to produce an
   `S3Operation` value.

2. **`operation.rs`** — the `S3Operation` enum (e.g. `ListBuckets`,
   `GetObject { bucket, key }`, `PutObject { bucket, key }`,
   `SetAcl { bucket, key }`, `CreateMultipartUpload { bucket, key }`, ...)
   plus a `dispatch(op, req) -> Result<Response<...>, S3Error>` function
   with one match arm per variant, calling into `handlers::*`.

3. **`handlers/`** — one async fn stub per operation (grouped by
   object/bucket/acl/multipart modules as needed). Most return
   `Err(S3Error::NotImplemented)`. A couple of trivial ones (e.g.
   `ListBuckets` returning an empty list) return real success responses to
   prove the path end-to-end.

4. **`auth.rs`** — best-effort SigV4 `Authorization` header parse to pull
   out the access-key/user identity. No signature verification. Falls back
   to `"anonymous"` when the header is missing or unparseable; the request
   is still routed.

5. **`error.rs`** — `S3Error` enum (`NoSuchBucket`, `NoSuchKey`,
   `AccessDenied`, `InvalidRequest`, `NotImplemented`, `Internal`, ...)
   with a `to_response()` that renders the S3 XML error shape
   (`<Error><Code>...</Code><Message>...</Message></Error>`) and the
   corresponding HTTP status code.

6. **`logging.rs`** — `tracing` setup with a JSON subscriber, plus a
   wrapper around the dispatch call that records start time and, on
   completion, emits one event with `user`, `operation` (from the
   `S3Operation`'s Display/Debug), `duration_ms`, and `bytes` (response
   body length / content-length; 0 for stub responses).

7. **`main.rs`** — hyper server bootstrap
   (`hyper_util::server::conn::auto` + `TcpListener`), wiring a
   `service_fn` that runs parse → auth → logging-wrapped-dispatch → error
   mapping.

## Testing

- Unit tests for the path/query parser: table-driven, method + path +
  query -> expected `S3Operation`.
- Unit tests for `S3Error::to_response()`: verify XML shape and status
  code per variant.
- One integration test that starts the server on an ephemeral port and
  fires real HTTP requests (e.g. `GET /` -> `ListBuckets` stub,
  `PUT /bucket/key?acl` -> `SetAcl` stub) to prove the full chain wires
  together.

## Open questions for future sub-projects

- How the metadata abstraction trait is shaped, and what the SQLite
  implementation looks like.
- How storage backends are selected/configured and what their trait
  interface looks like.
- Caching policy and where it sits in the request flow.
- Virtual-hosted-style addressing and real SigV4 verification.
