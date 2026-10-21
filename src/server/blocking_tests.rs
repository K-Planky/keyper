//! Real sockets and the real owner; a test-only watch reports waiter counts.

use tokio::io::{AsyncReadExt, duplex};

use super::*;

struct TestServer {
    address: std::net::SocketAddr,
    waiters: tokio::sync::watch::Receiver<usize>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<io::Result<()>>,
}

const LIMIT: Duration = Duration::from_secs(5);

async fn bounded<F: Future>(future: F) -> F::Output {
    timeout(LIMIT, future).await.expect("test deadline")
}

impl TestServer {
    async fn start() -> Self {
        let listener = bind(Config { port: 0 }).await.unwrap();
        let address = listener.local_addr().unwrap();
        let (client, owner, waiters) = state::observed().unwrap();
        let (stop, signal) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(serve_with_state(
            listener,
            async {
                let _ = signal.await;
                Ok(())
            },
            (client, owner),
        ));
        Self {
            address,
            waiters,
            stop: Some(stop),
            task,
        }
    }

    async fn connect(&self) -> TcpStream {
        bounded(TcpStream::connect(self.address)).await.unwrap()
    }

    async fn count(&mut self, expected: usize) {
        bounded(self.waiters.wait_for(|&count| count == expected))
            .await
            .unwrap();
    }

    async fn stop(mut self) {
        self.stop.take().unwrap().send(()).unwrap();
        bounded(&mut self.task).await.unwrap().unwrap();
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn request(args: &[&[u8]]) -> Vec<u8> {
    let mut bytes = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        bytes.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        bytes.extend_from_slice(arg);
        bytes.extend_from_slice(b"\r\n");
    }
    bytes
}

async fn send(client: &mut TcpStream, args: &[&[u8]]) {
    bounded(client.write_all(&request(args))).await.unwrap();
}

async fn line(client: &mut TcpStream) -> Vec<u8> {
    let mut bytes = Vec::new();
    while bytes.len() < 128 {
        bytes.push(client.read_u8().await.unwrap());
        if bytes.ends_with(b"\r\n") {
            bytes.truncate(bytes.len() - 2);
            return bytes;
        }
    }
    panic!("oversized test response header")
}

async fn decode(client: &mut TcpStream) -> Reply {
    let header = line(client).await;
    match header[0] {
        b'+' => Reply::Simple(header[1..].to_vec()),
        b'-' => Reply::Error(header[1..].to_vec()),
        b':' => Reply::Integer(std::str::from_utf8(&header[1..]).unwrap().parse().unwrap()),
        b'$' | b'*' => {
            let count: i64 = std::str::from_utf8(&header[1..]).unwrap().parse().unwrap();
            if header[0] == b'$' {
                if count == -1 {
                    return Reply::Bulk(None);
                }
                let count = usize::try_from(count).unwrap();
                assert!(count <= crate::resp::MAX_BULK_BYTES);
                let mut bytes = vec![0; count + 2];
                client.read_exact(&mut bytes).await.unwrap();
                assert_eq!(bytes.split_off(count), b"\r\n");
                Reply::Bulk(Some(bytes))
            } else {
                if count == -1 {
                    return Reply::Array(None);
                }
                assert!((0..=1024).contains(&count));
                let mut items = Vec::new();
                for _ in 0..count {
                    items.push(Box::pin(decode(client)).await);
                }
                Reply::Array(Some(items))
            }
        }
        _ => panic!("unexpected test reply: {header:?}"),
    }
}

async fn receive(client: &mut TcpStream) -> Reply {
    bounded(decode(client)).await
}

async fn call(client: &mut TcpStream, args: &[&[u8]]) -> Reply {
    send(client, args).await;
    receive(client).await
}

fn pair(key: &[u8], value: &[u8]) -> Reply {
    Reply::Array(Some(vec![
        Reply::Bulk(Some(key.to_vec())),
        Reply::Bulk(Some(value.to_vec())),
    ]))
}

fn ok() -> Reply {
    Reply::Simple(b"OK".to_vec())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn socket_pops_preserve_end_key_type_and_binary_priority() {
    let mut server = TestServer::start().await;
    let mut client = server.connect().await;
    let key = b"\0\xff\r\n";
    assert_eq!(
        call(&mut client, &[b"RPUSH", key, b"", b"\0\xff\r\n", b"tail"]).await,
        Reply::Integer(3)
    );
    assert_eq!(
        call(&mut client, &[b"bLpOp", b"absent", key, key, b"0"]).await,
        pair(key, b"")
    );
    assert_eq!(
        call(&mut client, &[b"bRpOp", key, b"0"]).await,
        pair(key, b"tail")
    );
    assert_eq!(
        call(&mut client, &[b"BLPOP", key, b"0"]).await,
        pair(key, b"\0\xff\r\n")
    );
    assert_eq!(
        call(&mut client, &[b"EXISTS", key]).await,
        Reply::Integer(0)
    );
    assert_eq!(call(&mut client, &[b"SET", b"str", b"value"]).await, ok());
    assert_eq!(
        call(&mut client, &[b"RPUSH", key, b"a", b"b"]).await,
        Reply::Integer(2)
    );
    assert_eq!(
        call(&mut client, &[b"BLPOP", key, b"str", b"0"]).await,
        pair(key, b"a")
    );
    assert!(
        matches!(call(&mut client, &[b"BRPOP", b"str", key, b"0"]).await,
        Reply::Error(error) if error.starts_with(b"WRONGTYPE"))
    );
    assert_eq!(
        call(&mut client, &[b"BRPOP", key, b"str", b"0"]).await,
        pair(key, b"b")
    );
    server.count(0).await;
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn socket_validation_and_fractional_timeouts_never_consume_on_error_or_expiry() {
    let mut server = TestServer::start().await;
    let mut client = server.connect().await;
    assert_eq!(
        call(&mut client, &[b"RPUSH", b"k", b"retained"]).await,
        Reply::Integer(1)
    );
    for name in [&b"BLPOP"[..], b"BRPOP"] {
        for args in [
            vec![name],
            vec![name, b"0"],
            vec![name, b"k", b"k", b"k", b"k", b"k", b"k", b"0"],
        ] {
            assert!(matches!(call(&mut client, &args).await, Reply::Error(_)));
        }
        for value in [
            &b"-1"[..],
            b"NaN",
            b"inf",
            b"1e999",
            b"1e-999",
            b"bad",
            b"",
            b"\xff",
        ] {
            assert!(matches!(
                call(&mut client, &[name, b"k", value]).await,
                Reply::Error(_)
            ));
        }
    }
    assert_eq!(
        call(&mut client, &[b"BRPOP", b"k", b"0"]).await,
        pair(b"k", b"retained")
    );
    for name in [&b"BLPOP"[..], b"BRPOP"] {
        let start = tokio::time::Instant::now();
        assert_eq!(
            call(
                &mut client,
                &[name, b"a", b"b", b"c", b"d", b"e", b"0.0011"]
            )
            .await,
            Reply::Array(None)
        );
        assert!(start.elapsed() >= Duration::from_millis(2));
        server.count(0).await;
        assert_eq!(
            call(&mut client, &[b"RPUSH", b"e", b"later"]).await,
            Reply::Integer(1)
        );
        assert_eq!(
            call(&mut client, &[name, b"e", b"0"]).await,
            pair(b"e", b"later")
        );
    }
    let mut fresh = server.connect().await;
    assert!(matches!(
        call(&mut fresh, &[b"BLPOP", b"k", b"invalid"]).await,
        Reply::Error(_)
    ));
    assert!(matches!(
        call(&mut fresh, &[b"SELECT", b"15"]).await,
        Reply::Error(_)
    ));
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overlapping_fifo_waiters_observe_whole_pushes_while_other_clients_progress() {
    let mut server = TestServer::start().await;
    let mut first = server.connect().await;
    send(&mut first, &[b"BLPOP", b"a", b"b", b"a", b"-0"]).await;
    server.count(1).await;
    let mut second = server.connect().await;
    send(&mut second, &[b"BRPOP", b"b", b"c", b"0"]).await;
    server.count(2).await;
    let mut other = server.connect().await;
    assert_eq!(
        call(&mut other, &[b"PING"]).await,
        Reply::Simple(b"PONG".to_vec())
    );
    let mut db15 = server.connect().await;
    assert_eq!(call(&mut db15, &[b"SELECT", b"15"]).await, ok());
    assert_eq!(
        call(&mut db15, &[b"RPUSH", b"b", b"independent"]).await,
        Reply::Integer(1)
    );
    assert_eq!(
        call(&mut db15, &[b"BRPOP", b"b", b"0"]).await,
        pair(b"b", b"independent")
    );
    server.count(2).await;
    assert_eq!(
        call(&mut other, &[b"SET", b"unrelated", b"value"]).await,
        ok()
    );
    assert_eq!(
        call(&mut other, &[b"LPUSH", b"b", b"x", b"y", b"z"]).await,
        Reply::Integer(3)
    );
    assert_eq!(receive(&mut first).await, pair(b"b", b"z"));
    assert_eq!(receive(&mut second).await, pair(b"b", b"x"));
    assert_eq!(
        call(&mut other, &[b"BLPOP", b"b", b"0"]).await,
        pair(b"b", b"y")
    );
    server.count(0).await;
    send(&mut second, &[b"BRPOP", b"c", b"0"]).await;
    server.count(1).await;
    send(&mut first, &[b"BLPOP", b"c", b"0"]).await;
    server.count(2).await;
    assert_eq!(
        call(&mut other, &[b"RPUSH", b"c", b"one", b"two"]).await,
        Reply::Integer(2)
    );
    assert_eq!(receive(&mut second).await, pair(b"c", b"two"));
    assert_eq!(receive(&mut first).await, pair(b"c", b"one"));
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn competing_socket_consumers_account_for_each_item_once() {
    let mut server = TestServer::start().await;
    let mut consumers = Vec::new();
    for count in 1..=32 {
        let mut client = server.connect().await;
        send(&mut client, &[b"BLPOP", b"k", b"0"]).await;
        server.count(count).await;
        consumers.push(client);
    }
    let gate = std::sync::Arc::new(tokio::sync::Barrier::new(8));
    let mut producers = tokio::task::JoinSet::new();
    for batch in 0..8 {
        let mut client = server.connect().await;
        let gate = gate.clone();
        producers.spawn(async move {
            let values: Vec<_> = (batch * 4..batch * 4 + 4).map(|n| n.to_string()).collect();
            let mut args = vec![&b"RPUSH"[..], b"k"];
            args.extend(values.iter().map(|v| v.as_bytes()));
            bounded(gate.wait()).await;
            assert_eq!(call(&mut client, &args).await, Reply::Integer(4));
        });
    }
    while let Some(result) = bounded(producers.join_next()).await {
        result.unwrap();
    }
    let mut values = std::collections::BTreeSet::new();
    for consumer in &mut consumers {
        let Reply::Array(Some(parts)) = receive(consumer).await else {
            panic!("expected pair")
        };
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], Reply::Bulk(Some(b"k".to_vec())));
        let Reply::Bulk(Some(value)) = &parts[1] else {
            panic!("expected value")
        };
        assert!(
            values.insert(
                std::str::from_utf8(value)
                    .unwrap()
                    .parse::<usize>()
                    .unwrap()
            )
        );
    }
    assert_eq!(values, (0..32).collect());
    server.count(0).await;
    assert_eq!(
        call(&mut consumers[0], &[b"EXISTS", b"k"]).await,
        Reply::Integer(0)
    );
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eof_cleans_dormant_waits_and_discards_queued_commands_without_a_producer() {
    let mut server = TestServer::start().await;
    let mut observer = server.connect().await;
    for n in 0..32 {
        let mut client = server.connect().await;
        send(&mut client, &[b"BLPOP", b"a", b"b", b"c", b"d", b"e", b"0"]).await;
        server.count(1).await;
        if n % 2 == 0 {
            // EOF must not hide behind more than one read buffer of queued input.
            let mut ahead = request(&[b"SET", b"must-not-run", b"bad"]);
            ahead.extend(request(&[b"PING", &vec![b'x'; 32 * 1024]]));
            bounded(client.write_all(&ahead)).await.unwrap();
            bounded(client.shutdown()).await.unwrap();
            let mut bytes = Vec::new();
            bounded(client.read_to_end(&mut bytes)).await.unwrap();
            assert!(bytes.is_empty());
        } else {
            drop(client);
        }
        server.count(0).await;
        assert_eq!(
            call(&mut observer, &[b"EXISTS", b"must-not-run"]).await,
            Reply::Integer(0)
        );
    }
    for key in [&b"a"[..], b"b", b"c", b"d", b"e"] {
        assert_eq!(
            call(&mut observer, &[b"RPUSH", key, b"retained"]).await,
            Reply::Integer(1)
        );
        assert_eq!(
            call(&mut observer, &[b"BRPOP", key, b"0"]).await,
            pair(key, b"retained")
        );
    }
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disconnect_and_producer_races_allow_ambiguous_commit_but_never_stale_consumption() {
    let mut server = TestServer::start().await;
    let mut producer = server.connect().await;
    for _ in 0..32 {
        let mut client = server.connect().await;
        send(&mut client, &[b"BRPOP", b"k", b"0"]).await;
        server.count(1).await;
        let (closed, pushed) = bounded(async {
            tokio::join!(
                client.shutdown(),
                call(&mut producer, &[b"RPUSH", b"k", b"item"])
            )
        })
        .await;
        closed.unwrap();
        assert_eq!(pushed, Reply::Integer(1));
        drop(client);
        server.count(0).await;
        match call(&mut producer, &[b"EXISTS", b"k"]).await {
            Reply::Integer(0) => (), // A pop may have committed before EOF detection.
            Reply::Integer(1) => assert_eq!(
                call(&mut producer, &[b"BLPOP", b"k", b"0"]).await,
                pair(b"k", b"item")
            ),
            other => panic!("invalid accounting: {other:?}"),
        }
        assert_eq!(
            call(&mut producer, &[b"RPUSH", b"k", b"later"]).await,
            Reply::Integer(1)
        );
        assert_eq!(
            call(&mut producer, &[b"BLPOP", b"k", b"0"]).await,
            pair(b"k", b"later")
        );
    }
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn timeout_and_producer_races_preserve_item_accounting() {
    let mut server = TestServer::start().await;
    let mut client = server.connect().await;
    let mut producer = server.connect().await;
    for _ in 0..32 {
        send(&mut client, &[b"BLPOP", b"k", b"0.000001"]).await;
        let (popped, pushed) = tokio::join!(
            receive(&mut client),
            call(&mut producer, &[b"RPUSH", b"k", b"item"])
        );
        assert_eq!(pushed, Reply::Integer(1));
        server.count(0).await;
        if popped == Reply::Array(None) {
            assert_eq!(
                call(&mut producer, &[b"BLPOP", b"k", b"0"]).await,
                pair(b"k", b"item")
            );
        } else {
            assert_eq!(popped, pair(b"k", b"item"));
        }
        assert_eq!(
            call(&mut producer, &[b"EXISTS", b"k"]).await,
            Reply::Integer(0)
        );
    }
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lost_pop_reply_does_not_requeue_and_shutdown_drains_pending_sockets() {
    let mut server = TestServer::start().await;
    let mut observer = server.connect().await;
    let mut lost = server.connect().await;
    send(&mut lost, &[b"BLPOP", b"large", b"0"]).await;
    server.count(1).await;
    assert_eq!(
        call(
            &mut observer,
            &[b"RPUSH", b"large", &vec![b'x'; crate::resp::MAX_BULK_BYTES]]
        )
        .await,
        Reply::Integer(1)
    );
    // A reply prefix proves commitment. A stalled writer cannot retain ownership.
    let mut prefix = [0; 4];
    bounded(lost.read_exact(&mut prefix)).await.unwrap();
    assert_eq!(&prefix, b"*2\r\n");
    assert_eq!(
        call(&mut observer, &[b"EXISTS", b"large"]).await,
        Reply::Integer(0)
    );
    drop(lost);
    assert_eq!(
        call(&mut observer, &[b"RPUSH", b"large", b"after-loss"]).await,
        Reply::Integer(1)
    );
    assert_eq!(
        call(&mut observer, &[b"BRPOP", b"large", b"0"]).await,
        pair(b"large", b"after-loss")
    );

    let mut blocked = Vec::new();
    for n in 0..12 {
        let mut client = server.connect().await;
        let timeout = if n % 2 == 0 { &b"0"[..] } else { b"1000.5" };
        send(&mut client, &[b"BLPOP", b"pending", timeout]).await;
        server.count(n + 1).await;
        let mut queued = request(&[b"SET", b"queued-at-shutdown", b"must-not-run"]);
        queued.extend(request(&[b"RPUSH", b"pending", b"cannot-self-release"]));
        bounded(client.write_all(&queued)).await.unwrap();
        blocked.push(client);
    }
    let mut idle = server.connect().await;
    let mut partial = server.connect().await;
    bounded(partial.write_all(b"*3\r\n$3\r\nSET\r\n"))
        .await
        .unwrap();
    assert_eq!(
        call(&mut observer, &[b"SET", b"old-state", b"lost-on-restart"]).await,
        ok()
    );
    let address = server.address;
    server.stop().await;
    for client in blocked.iter_mut().chain([&mut idle, &mut partial]) {
        let mut byte = [0];
        match bounded(client.read(&mut byte)).await {
            Ok(0) => (),
            Err(error) if error.kind() == io::ErrorKind::ConnectionReset => (),
            other => panic!("unexpected shutdown result: {other:?}"),
        }
    }
    assert!(bounded(TcpStream::connect(address)).await.is_err());
    let fresh = TestServer::start().await;
    assert_eq!(
        call(
            &mut fresh.connect().await,
            &[b"EXISTS", b"old-state", b"large"]
        )
        .await,
        Reply::Integer(0)
    );
    fresh.stop().await;
}

#[tokio::test]
async fn failed_pop_write_keeps_the_committed_removal() {
    use crate::command::{DatabaseCommand, ListEnd};
    let (client, owner) = state::spawn().unwrap();
    let db = state::DatabaseIndex::default();
    bounded(client.execute(
        db,
        DatabaseCommand::Push {
            end: ListEnd::Right,
            key: b"k".to_vec(),
            values: vec![b"value".to_vec()],
        },
    ))
    .await
    .unwrap();
    let reply = bounded(client.execute(
        db,
        DatabaseCommand::Pop {
            end: ListEnd::Left,
            keys: vec![b"k".to_vec()],
            timeout: b"0".to_vec(),
        },
    ))
    .await
    .unwrap();
    assert_eq!(reply, pair(b"k", b"value"));
    let (mut writer, reader) = duplex(1);
    drop(reader);
    assert_eq!(
        write_reply(&mut writer, &reply).await.unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    assert_eq!(
        bounded(client.execute(db, DatabaseCommand::Exists(vec![b"k".to_vec()])))
            .await
            .unwrap(),
        Reply::Integer(0)
    );
    drop(client);
    bounded(owner).await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn excessive_queued_input_closes_and_cancels_instead_of_hiding_eof() {
    let mut server = TestServer::start().await;
    let mut client = server.connect().await;
    send(&mut client, &[b"BLPOP", b"k", b"0"]).await;
    server.count(1).await;
    let mut bytes = request(&[b"SET", b"queued-mutation", b"must-not-run"]);
    bytes.resize(input::MAX_READ_AHEAD_BYTES + 1, b'x');
    if let Err(error) = bounded(client.write_all(&bytes)).await {
        assert!(matches!(
            error.kind(),
            io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset
        ));
    }
    server.count(0).await;
    let mut output = Vec::new();
    match bounded(client.read_to_end(&mut output)).await {
        Ok(_) => assert!(output.is_empty()),
        Err(error) => assert_eq!(error.kind(), io::ErrorKind::ConnectionReset),
    }
    let mut other = server.connect().await;
    assert_eq!(
        call(&mut other, &[b"EXISTS", b"queued-mutation"]).await,
        Reply::Integer(0)
    );
    assert_eq!(
        call(&mut other, &[b"RPUSH", b"k", b"retained"]).await,
        Reply::Integer(1)
    );
    assert_eq!(
        call(&mut other, &[b"BLPOP", b"k", b"0"]).await,
        pair(b"k", b"retained")
    );
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocking_pipeline_cannot_release_itself_and_resumes_through_errors_and_partial_input() {
    let mut server = TestServer::start().await;
    let mut client = server.connect().await;
    let mut other = server.connect().await;
    let mut bytes = request(&[b"SET", b"interleaved", b"before"]);
    bytes.extend(request(&[b"BLPOP", b"k", b"0"]));
    bytes.extend(request(&[b"PING", b"after-first"]));
    bytes.extend(request(&[b"GET", b"interleaved"]));
    bytes.extend(request(&[b"UNKNOWN"]));
    bytes.extend(request(&[b"RPUSH", b"k", b"self-push"]));
    bytes.extend(request(&[b"BRPOP", b"k", b"0"]));
    bytes.extend(request(&[b"SET", b"partial", b"\0\xff\r\n"]));
    // Leave the SET terminator incomplete across read-ahead and socket input.
    let split = bytes.len() - 1;
    bounded(client.write_all(&bytes[..split])).await.unwrap();
    server.count(1).await;
    assert_eq!(receive(&mut client).await, ok());
    // A pipeline orders this connection but is NOT a transaction. Its prefix
    // is visible and another client can intervene before the suffix executes.
    assert_eq!(
        call(&mut other, &[b"GET", b"interleaved"]).await,
        Reply::Bulk(Some(b"before".to_vec()))
    );
    assert_eq!(
        call(&mut other, &[b"SET", b"interleaved", b"between"]).await,
        ok()
    );
    assert_eq!(
        call(&mut other, &[b"EXISTS", b"k", b"partial"]).await,
        Reply::Integer(0)
    );
    assert_eq!(
        call(&mut other, &[b"RPUSH", b"k", b"external"]).await,
        Reply::Integer(1)
    );
    assert_eq!(receive(&mut client).await, pair(b"k", b"external"));
    assert_eq!(
        receive(&mut client).await,
        Reply::Bulk(Some(b"after-first".to_vec()))
    );
    assert_eq!(
        receive(&mut client).await,
        Reply::Bulk(Some(b"between".to_vec()))
    );
    assert!(
        matches!(receive(&mut client).await, Reply::Error(error) if error == b"ERR unknown command")
    );
    assert_eq!(receive(&mut client).await, Reply::Integer(1));
    assert_eq!(receive(&mut client).await, pair(b"k", b"self-push"));
    assert_eq!(
        call(&mut other, &[b"GET", b"partial"]).await,
        Reply::Bulk(None)
    );
    let mut tail = bytes[split..].to_vec();
    tail.extend(request(&[b"GET", b"partial"]));
    tail.extend(request(&[b"BRPOP", b"empty", b"0.001"]));
    tail.extend(request(&[b"PING"]));
    bounded(client.write_all(&tail)).await.unwrap();
    assert_eq!(receive(&mut client).await, ok());
    assert_eq!(
        receive(&mut client).await,
        Reply::Bulk(Some(b"\0\xff\r\n".to_vec()))
    );
    assert_eq!(receive(&mut client).await, Reply::Array(None));
    assert_eq!(receive(&mut client).await, Reply::Simple(b"PONG".to_vec()));
    server.count(0).await;
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_input_cannot_overtake_a_pop_and_retains_binary_frame_boundaries() {
    let mut server = TestServer::start().await;
    let mut client = server.connect().await;
    let mut observer = server.connect().await;
    let value: Vec<u8> = (0..32 * 1024).map(|n| (n % 256) as u8).collect();
    let mut bytes = request(&[b"BLPOP", b"k", b"0"]);
    bytes.extend(request(&[b"SET", b"after-pop", &value]));
    bytes.extend(request(&[b"PING"]));
    bounded(client.write_all(&bytes)).await.unwrap();
    server.count(1).await;
    assert_eq!(
        call(&mut observer, &[b"GET", b"after-pop"]).await,
        Reply::Bulk(None)
    );
    assert_eq!(
        call(&mut observer, &[b"RPUSH", b"k", b"release"]).await,
        Reply::Integer(1)
    );
    assert_eq!(receive(&mut client).await, pair(b"k", b"release"));
    assert_eq!(receive(&mut client).await, ok());
    assert_eq!(receive(&mut client).await, Reply::Simple(b"PONG".to_vec()));
    assert_eq!(
        call(&mut observer, &[b"GET", b"after-pop"]).await,
        Reply::Bulk(Some(value))
    );
    server.stop().await;
}
