use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};

use maestore::routing::RoutingConfig;
use maestore::ServerHandle;

async fn start_server() -> ServerHandle {
    start_server_with_routing(RoutingConfig::default()).await
}

async fn start_server_with_routing(routing: RoutingConfig) -> ServerHandle {
    maestore::serve("127.0.0.1:0".parse().unwrap(), routing)
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
    let handle = start_server().await;
    let addr = handle.addr();
    let request = "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n".to_string();

    let response = tokio::task::spawn_blocking(move || send_request(addr, &request))
        .await
        .expect("blocking task should not panic");

    assert!(response.starts_with("HTTP/1.1 200"));
    assert!(response.contains("ListAllMyBucketsResult"));
}

#[tokio::test]
async fn put_object_acl_returns_not_implemented() {
    let handle = start_server().await;
    let addr = handle.addr();
    let request = "PUT /my-bucket/my-key?acl HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: 0\r\n\r\n".to_string();

    let response = tokio::task::spawn_blocking(move || send_request(addr, &request))
        .await
        .expect("blocking task should not panic");

    assert!(response.starts_with("HTTP/1.1 501"));
    assert!(response.contains("NotImplemented"));
}

#[tokio::test]
async fn unknown_bucket_method_returns_method_not_allowed() {
    let handle = start_server().await;
    let addr = handle.addr();
    let request =
        "POST /my-bucket HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
            .to_string();

    let response = tokio::task::spawn_blocking(move || send_request(addr, &request))
        .await
        .expect("blocking task should not panic");

    assert!(response.starts_with("HTTP/1.1 405"));
    assert!(response.contains("MethodNotAllowed"));
}

#[tokio::test]
async fn shutdown_stops_accepting_new_connections() {
    let handle = start_server().await;
    let addr = handle.addr();

    // Confirm the server actually answers before shutting it down.
    let request = "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n".to_string();
    let response = tokio::task::spawn_blocking(move || send_request(addr, &request))
        .await
        .expect("blocking task should not panic");
    assert!(response.starts_with("HTTP/1.1 200"));

    handle.shutdown().await;

    let connect_result = tokio::task::spawn_blocking(move || TcpStream::connect(addr))
        .await
        .expect("blocking task should not panic");
    assert!(
        connect_result.is_err(),
        "server should no longer accept connections after shutdown"
    );
}

#[tokio::test]
async fn host_based_bucket_routing_resolves_bucket_from_host_header() {
    let handle = start_server_with_routing(RoutingConfig {
        base_domain: Some("s3.test".to_string()),
    })
    .await;
    let addr = handle.addr();
    let request = "GET / HTTP/1.1\r\nHost: mybucket.s3.test\r\nConnection: close\r\n\r\n".to_string();

    let response = tokio::task::spawn_blocking(move || send_request(addr, &request))
        .await
        .expect("blocking task should not panic");

    // A bucket-level GET (ListObjects, a stub) is 501 — distinct from the
    // 200 ListAllMyBucketsResult that would come back if the Host header
    // were ignored and "/" fell through to path-style ListBuckets.
    assert!(response.starts_with("HTTP/1.1 501"));
    assert!(response.contains("NotImplemented"));
}

#[tokio::test]
async fn absolute_form_request_target_authority_is_used_for_host_based_routing() {
    let handle = start_server_with_routing(RoutingConfig {
        base_domain: Some("s3.test".to_string()),
    })
    .await;
    let addr = handle.addr();
    // Absolute-form request-target (RFC 9112 §3.2.2) carries its own
    // authority, "mybucket.s3.test" — deliberately different from the `Host`
    // header's "unrelated.example.com". hyper surfaces a request-target's
    // authority via `req.uri().authority()`, which handle_request now
    // prefers over the `Host` header (the same code path an HTTP/2 request's
    // `:authority` pseudo-header takes, since neither is ever present in the
    // header map). If handle_request only ever looked at the `Host` header,
    // this would fall through to path-style routing on "unrelated.example.com"
    // (which doesn't match the base domain) and return 200 ListAllMyBucketsResult.
    let request =
        "GET http://mybucket.s3.test/ HTTP/1.1\r\nHost: unrelated.example.com\r\nConnection: close\r\n\r\n"
            .to_string();

    let response = tokio::task::spawn_blocking(move || send_request(addr, &request))
        .await
        .expect("blocking task should not panic");

    assert!(response.starts_with("HTTP/1.1 501"), "expected 501, got: {response}");
    assert!(response.contains("NotImplemented"));
}

#[tokio::test]
async fn host_not_matching_base_domain_still_uses_path_style_routing() {
    let handle = start_server_with_routing(RoutingConfig {
        base_domain: Some("s3.test".to_string()),
    })
    .await;
    let addr = handle.addr();
    let request = "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n".to_string();

    let response = tokio::task::spawn_blocking(move || send_request(addr, &request))
        .await
        .expect("blocking task should not panic");

    assert!(response.starts_with("HTTP/1.1 200"));
    assert!(response.contains("ListAllMyBucketsResult"));
}
