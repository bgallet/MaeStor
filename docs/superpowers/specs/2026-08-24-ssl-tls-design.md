# SSL/TLS Design

Date: 2026-08-24
Status: Approved

## Context

`open-conductor` currently only serves plain HTTP (`serve()` in `src/lib.rs`, over a bare `TcpListener`). Bucket addressing is path-style only (`/bucket/key`) — there is no Host-header parsing anywhere in the crate.

This document covers adding TLS termination with four requirements: certificates must be reloadable without restarting the server; tests need a way to supply a full certificate chain (leaf + intermediates), not just a single leaf cert; wildcard certificates must work, in support of virtual-hosted-style ("host-based") bucket addressing; and client certificates (mTLS) must be supported as an alternative to SigV4 — when a client cert is presented and verified, its identity is used and SigV4 signature checking is skipped.

Scope for this cut: TLS termination (rustls), hot cert reload via file watching, optional client-cert auth with identity extraction, and virtual-hosted-style bucket routing (needed to make "wildcard cert for host-based buckets" a meaningful, testable feature rather than just a cert-loading detail). Not in scope: actual SigV4 signature verification (still a stub today — untouched by this work), ACME/automatic certificate issuance, per-tenant/multi-domain SNI cert selection (this cut is single-cert-per-listener), and rate limiting or other connection-level hardening.

## Architecture overview

Two server entry points live side by side in `src/lib.rs`:

- `serve(addr, RoutingConfig)` — today's plain HTTP listener, extended only to accept routing config.
- `serve_tls(addr, RoutingConfig, TlsConfig)` — a new HTTPS listener. Same accept-loop/graceful-shutdown shape as `serve`, but each accepted `TcpStream` goes through a TLS handshake (via `tokio-rustls`) before being handed to hyper.

An operator can run either, or both (e.g. HTTP on one port, HTTPS on another) — nothing in `TlsConfig` disables the plain listener. This mirrors today's single-`serve()`-call usage in `main.rs`, just with a second optional call.

Three new/changed modules:

- `src/tls.rs` (new) — cert/key loading, the hot-reload watcher, and the mTLS client verifier.
- `src/auth.rs` (changed) — accepts an optional cert-derived identity, which takes priority over SigV4 header parsing.
- `src/routing.rs` (changed) — accepts an optional `Host` header + configured base domain, to resolve virtual-hosted-style requests to a bucket.

## `src/tls.rs`: config and loading

```rust
pub struct TlsConfig {
    pub cert_chain_path: PathBuf,
    pub private_key_path: PathBuf,
    pub client_ca_path: Option<PathBuf>,
}
```

All three fields are file paths — deliberately no in-memory-bytes constructor. Tests generate certs with `rcgen` and write them to a tempdir, then point `TlsConfig` at those paths, exercising the exact same loading code as production rather than a parallel test-only path.

`client_ca_path` being `Some` is what turns on mTLS for that listener; there's no separate boolean, since a client-CA bundle with no verifier attached would be a config with no effect.

```rust
fn load_server_config(config: &TlsConfig) -> Result<rustls::ServerConfig, TlsError>
```

- Reads `cert_chain_path` via `rustls-pemfile`, collecting every `CERTIFICATE` block in the file in order (leaf first, then any intermediates) — this is the "give the chain" requirement: the file is simply a concatenated PEM chain, and every cert in it is sent to the client during the handshake.
- Reads `private_key_path`, one `PRIVATE KEY` block.
- If `client_ca_path` is set: builds a `RootCertStore` from it and constructs a `WebPkiClientVerifier` with `.allow_unauthenticated()` — a connection with no client cert is still accepted, but a connection that *does* present one must chain to this store or the handshake fails. If unset, uses `WebPkiClientVerifier::no_client_auth()` (today's implicit behavior).
- No SNI-based cert selection — `with_single_cert` presents the same chain on every handshake regardless of the requested SNI name. This is what makes wildcard certs "just work": a `*.s3.example.com` leaf is valid for any subdomain a client dials, and the server never needs to know which subdomain that was.

`TlsError` wraps file I/O and parse failures (`io::Error`, PEM/DER parse errors); `serve_tls` returns it from its `Result` at startup instead of panicking.

## Hot reload

```rust
struct ReloadableConfig(Arc<ArcSwap<rustls::ServerConfig>>);
```

On construction, `serve_tls` calls `load_server_config` once (a failure here is a startup error, returned to the caller) and wraps the result in `ReloadableConfig`. It then spawns a background task that watches the parent directories of `cert_chain_path`, `private_key_path`, and (if set) `client_ca_path` using the `notify` crate — watching directories rather than the files directly, since common rotation tools (certbot, `kubectl cp`, symlink swaps) replace a file via rename rather than in-place write, which an inode-level watch would miss.

On any change event, the watcher calls `load_server_config` again:

- **Success** → the new `rustls::ServerConfig` is swapped into the `ArcSwap`, effective for the next accepted connection. In-flight connections keep using whatever config they started their handshake with; nothing is forcibly torn down.
- **Failure** (e.g. cert file caught mid-rewrite, malformed replacement) → logged at `warn` and discarded. The last-known-good config keeps serving. A reload is never allowed to degrade a running listener into "serves nothing" or "serves garbage."

The whole `ServerConfig` — cert, key, and client-CA verifier — is rebuilt and swapped together, rather than reloading the leaf cert alone via rustls's `ResolvesServerCert` hook. This costs one extra `Arc` clone per accepted connection (to snapshot the current config before building that connection's `TlsAcceptor`), which is negligible, and it means client-CA rotation is reloadable through the exact same path as cert rotation instead of needing a second mechanism.

## Client-cert (mTLS) identity

After a successful handshake, `serve_tls`'s connection-handling task reads `peer_certificates()` off the `rustls::ServerConnection`. If present and non-empty, the leaf cert's DER is parsed with `x509-parser` to pull the first `rfc822Name` (email) Subject Alternative Name entry. That string, if found, becomes this connection's `peer_identity: Option<String>` — captured once per connection (not per request) and passed to every request on that connection via the closure given to `service_fn`.

A cert that passed chain validation but has no email SAN yields `peer_identity: None` for that connection (logged at `warn` once) — chain validity doesn't guarantee an email SAN exists, and the fallback path (SigV4 header, see below) is always safe to take.

`src/auth.rs` changes:

```rust
pub fn extract_user(headers: &HeaderMap, peer_identity: Option<&str>) -> String
```

If `peer_identity` is `Some`, it's returned directly — the header is never inspected, which is the literal "signature can be skipped" requirement. Otherwise, behavior is unchanged from today (parse SigV4 `Credential=`, fall back to `ANONYMOUS_USER`).

`handle_request` (in `lib.rs`) gains a `peer_identity: Option<&str>` parameter threaded from the connection into `auth::extract_user`. Plain HTTP connections (via `serve`) always pass `None`.

## Virtual-hosted-style (host-based) bucket routing

```rust
pub struct RoutingConfig {
    pub base_domain: Option<String>,
}
```

Passed to both `serve` and `serve_tls` — this is independent of TLS (real S3 supports virtual-hosted addressing over plain HTTP too). `base_domain: None` (or omitted) preserves today's path-only behavior exactly.

`routing::parse_request` gains a `host: Option<&str>` parameter (the request's `Host` header). Both `host` and `base_domain` are lowercased before comparison, so configuration and incoming headers can differ in case. Resolution order:

1. If `base_domain` is configured and `host` is `Some` and `host` ends with `.{base_domain}` with a non-empty label before it (i.e. `host != base_domain` itself), that label is the bucket. The entire path (after stripping the leading `/`) is then routed as the key/operation *under that bucket* — reusing `parse_object_operation`/`parse_bucket_operation` as-is, since they already take bucket and key/path as separate arguments; only what supplies the bucket argument changes.
2. Otherwise, fall through to today's logic: split the path on its first `/` to get bucket and key.

Both styles are live simultaneously whenever `base_domain` is set, matching real S3 (a request can address the same bucket either way).

## `lib.rs` / `main.rs` wiring

- `serve(addr: SocketAddr, routing: RoutingConfig) -> io::Result<ServerHandle>` — existing accept loop, unchanged except for threading `routing` into `handle_request` and always passing `peer_identity: None`.
- `serve_tls(addr: SocketAddr, routing: RoutingConfig, tls: TlsConfig) -> Result<ServerHandle, TlsError>` — same accept loop shape, but wraps each accepted stream: TLS handshake via a `tokio_rustls::TlsAcceptor` built from the current `ArcSwap` snapshot, extract `peer_identity`, then hand the resulting `TlsStream` to hyper exactly as `serve` hands off the raw `TcpStream` today.
- `ServerHandle` is unchanged and used by both.
- `main.rs` reads new optional env vars (cert chain path, key path, client CA path, base domain). Their absence reproduces exactly today's behavior: plain HTTP, path-style routing only.

## Error handling summary

| Situation | Behavior |
|---|---|
| Cert/key file missing or unparseable at startup | `serve_tls` returns `Err(TlsError)`; process fails to start (caller's choice how to handle, matching how `serve`'s bind error is handled today) |
| Reloaded cert/key file missing or unparseable | Logged at `warn`, old config keeps serving |
| Client presents a cert that doesn't chain to `client_ca_path` | TLS handshake fails, connection rejected (rustls behavior under `allow_unauthenticated`) |
| Client presents a valid cert with no email SAN | Connection succeeds, `peer_identity: None`, falls back to SigV4 header parsing |
| `Host` header present but doesn't match `base_domain` suffix | Falls back to path-style routing, not an error |

## Testing

- **Fixtures**: `rcgen` (dev-dependency) generates, in a tempdir per test: a root CA, a wildcard server leaf (e.g. `*.s3.test`) signed by it — written as a chain file (leaf + root) to exercise multi-cert-in-one-file loading — and a client cert with an email SAN signed by a (possibly separate) client CA.
- **Unit tests**:
  - `routing.rs`: table-driven cases for host-based resolution (matching subdomain, non-matching domain, bare base domain with no bucket label, no `base_domain` configured), in the same style as the existing path-based table.
  - `auth.rs`: `extract_user` with `peer_identity: Some(...)` (header ignored even if present and well-formed) vs `None` (today's tests unchanged).
- **Integration tests** (`tests/integration_test.rs`):
  - `serve_tls` with the chain file handshakes successfully against a client trusting the root CA, for multiple SNI/Host values matching the wildcard.
  - Reload: start `serve_tls`, connect and note the served cert, rewrite the cert file (new leaf, new key) at its path, wait for the watcher to pick it up, reconnect, and confirm the new cert is served.
  - mTLS: no client cert → connection succeeds, request resolves to SigV4/anonymous path; valid client cert → connection succeeds, request resolves to the cert's email identity; client cert signed by an untrusted CA → handshake fails.
  - Host-based routing end-to-end: request with `Host: mybucket.s3.test` against a server configured with `base_domain: "s3.test"` resolves the same as `/mybucket/...` path-style would.

## Non-goals (deferred)

- Real SigV4 signature verification (unrelated to this work — `auth.rs` remains a stub for the header-parsing path).
- ACME/automatic certificate issuance or renewal.
- Per-tenant or multi-domain SNI cert selection (one cert per listener in this cut).
- Connection-level hardening (rate limiting, handshake timeouts beyond what rustls/tokio-rustls provide by default).
