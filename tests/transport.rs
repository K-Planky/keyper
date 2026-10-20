//! Real TCP tests: owned loopback servers, bounded I/O, cleanup on failure.

mod support;

use std::io;

use keyper::{config::Config, resp, server};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::JoinSet;

use support::{TestServer, bounded};

const PING: &[u8] = b"*1\r\n$4\r\nPING\r\n";

async fn send(stream: &mut TcpStream, bytes: &[u8]) {
    bounded(stream.write_all(bytes)).await.unwrap();
}

async fn expect(stream: &mut TcpStream, expected: &[u8]) {
    let mut bytes = vec![0; expected.len()];
    bounded(stream.read_exact(&mut bytes)).await.unwrap();
    assert_eq!(bytes, expected);
}

async fn ping(stream: &mut TcpStream) {
    send(stream, PING).await;
    expect(stream, b"+PONG\r\n").await;
}

fn echo_request(message: &[u8]) -> Vec<u8> {
    let mut request = format!("*2\r\n$4\r\nPING\r\n${}\r\n", message.len()).into_bytes();
    request.extend_from_slice(message);
    request.extend_from_slice(b"\r\n");
    request
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ping_binary_empty_case_and_recoverable_command_errors() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;
    ping(&mut client).await;
    send(&mut client, b"*1\r\n$4\r\npInG\r\n").await;
    expect(&mut client, b"+PONG\r\n").await;
    for message in [&b""[..], b"\0\xff\r\n", b"hello world"] {
        send(&mut client, &echo_request(message)).await;
        expect(
            &mut client,
            &resp::Reply::Bulk(Some(message.to_vec())).encode(),
        )
        .await;
    }
    for (request, reply) in [
        (
            &b"*3\r\n$4\r\nPING\r\n$0\r\n\r\n$0\r\n\r\n"[..],
            &b"-ERR wrong number of arguments for 'ping' command\r\n"[..],
        ),
        (b"*1\r\n$7\r\nUNKNOWN\r\n", b"-ERR unknown command\r\n"),
        (b"*1\r\n$0\r\n\r\n", b"-ERR unknown command\r\n"),
        (b"*0\r\n", b"-ERR empty command\r\n"),
    ] {
        send(&mut client, request).await;
        expect(&mut client, reply).await;
        ping(&mut client).await;
    }
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn segmented_and_coalesced_frames_preserve_boundaries() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;
    let wire = echo_request(b"\0\xff\r\n");
    // Leave a bulk unfinished while a different connection makes progress.
    let split = wire.len() - 3;
    send(&mut client, &wire[..split]).await;
    ping(&mut server.connect().await).await;
    for byte in &wire[split..] {
        send(&mut client, &[*byte]).await;
    }
    expect(&mut client, b"$4\r\n\0\xff\r\n\r\n").await;
    // Coalescing must not discard bytes belonging to the next request. This
    // tests framing, not the future blocking-command pipeline contract.
    let mut coalesced = PING.to_vec();
    coalesced.extend_from_slice(&echo_request(b"next"));
    send(&mut client, &coalesced).await;
    expect(&mut client, b"+PONG\r\n$4\r\nnext\r\n").await;
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn protocol_errors_reply_then_close_and_truncation_never_executes() {
    let server = TestServer::start().await;
    for wire in [
        &b"*1\r\n+PING\r\n"[..],
        b"PING\r\n",
        b"*-1\r\n",
        b"*1\r\n$-1\r\n",
        b"*1\r\n$4\r\nPINGxx",
        b"*1025\r\n",
        b"*1\r\n$8388609\r\n",
    ] {
        let mut client = server.connect().await;
        send(&mut client, wire).await;
        let mut reply = vec![];
        bounded(client.read_to_end(&mut reply)).await.unwrap();
        assert!(reply.starts_with(b"-ERR Protocol error: "), "{reply:?}");
        assert!(reply.ends_with(b"\r\n"));
    }
    for wire in [&b""[..], b"*", b"*1\r\n$4\r\nPIN", b"*1\r\n$4\r\nPING\r"] {
        let mut client = server.connect().await;
        send(&mut client, wire).await;
        bounded(client.shutdown()).await.unwrap();
        let mut reply = vec![];
        bounded(client.read_to_end(&mut reply)).await.unwrap();
        assert!(reply.is_empty(), "truncation must not execute PING");
    }
    ping(&mut server.connect().await).await;
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_clients_progress_despite_idle_partial_and_nonreading_peers() {
    let server = TestServer::start().await;
    let _idle = server.connect().await;
    let mut partial = server.connect().await;
    send(&mut partial, b"*2\r\n$4\r\nPING\r\n$8388608\r\n").await;
    let mut nonreading = server.connect().await;
    send(
        &mut nonreading,
        &echo_request(&vec![b'x'; resp::MAX_BULK_BYTES]),
    )
    .await;
    let mut clients = JoinSet::new();
    for _ in 0..24 {
        let address = server.address;
        clients.spawn(async move {
            let mut stream = bounded(TcpStream::connect(address)).await.unwrap();
            for _ in 0..8 {
                ping(&mut stream).await;
            }
        });
    }
    bounded(async {
        while let Some(result) = clients.join_next().await {
            result.unwrap();
        }
    })
    .await;
    // A disappearing reply receiver is a connection failure, not service failure.
    drop(nonreading);
    ping(&mut server.connect().await).await;
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_closes_idle_and_partial_sessions_and_drops_listener() {
    let server = TestServer::start().await;
    let address = server.address;
    let mut idle = server.connect().await;
    let mut partial = server.connect().await;
    send(&mut partial, b"*1\r\n").await;
    // A completed command is also a readiness fence for this server instance.
    ping(&mut server.connect().await).await;
    server.stop().await;
    let mut byte = [0];
    assert_eq!(bounded(idle.read(&mut byte)).await.unwrap(), 0);
    // Depending on whether unread data reached the socket, closure may be a reset.
    match bounded(partial.read(&mut byte)).await {
        Ok(0) => (),
        Err(error) if error.kind() == io::ErrorKind::ConnectionReset => (),
        other => panic!("unexpected shutdown result: {other:?}"),
    }
    assert!(bounded(TcpStream::connect(address)).await.is_err());
}

#[tokio::test]
async fn explicit_port_and_occupied_bind_diagnostics() {
    let occupied = server::bind(Config { port: 0 }).await.unwrap();
    let port = occupied.local_addr().unwrap().port();
    assert!(occupied.local_addr().unwrap().ip().is_loopback());
    let error = server::bind(Config { port }).await.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
    drop(occupied);
    let alternate = server::bind(Config { port }).await.unwrap();
    assert_eq!(alternate.local_addr().unwrap().port(), port);
}
