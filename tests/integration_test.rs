use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};

use open_conductor::ServerHandle;

async fn start_server() -> ServerHandle {
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
