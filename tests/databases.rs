//! Public database/session contracts and small concurrent histories over TCP.

mod support;

use std::sync::atomic::{AtomicUsize, Ordering};

use keyper::resp::Reply;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::Barrier;

use support::{TestServer, bounded};

struct Client(BufReader<TcpStream>);

impl Client {
    async fn connect(server: &TestServer) -> Self {
        Self(BufReader::new(server.connect().await))
    }

    async fn command(&mut self, args: &[&[u8]]) -> Reply {
        bounded(self.0.get_mut().write_all(&request(args)))
            .await
            .unwrap();
        bounded(async {
            // This test client decodes only this step's public reply shapes.
            let mut line = vec![];
            self.0.read_until(b'\n', &mut line).await.unwrap();
            assert!(line.ends_with(b"\r\n"), "truncated reply: {line:?}");
            line.truncate(line.len() - 2);
            match line[0] {
                b'+' => Reply::Simple(line[1..].to_vec()),
                b'-' => Reply::Error(line[1..].to_vec()),
                b':' => Reply::Integer(std::str::from_utf8(&line[1..]).unwrap().parse().unwrap()),
                b'$' if line == b"$-1" => Reply::Bulk(None),
                b'$' => {
                    let length: usize = std::str::from_utf8(&line[1..]).unwrap().parse().unwrap();
                    assert!(length <= keyper::resp::MAX_BULK_BYTES);
                    let mut value = vec![0; length];
                    self.0.read_exact(&mut value).await.unwrap();
                    let mut end = [0; 2];
                    self.0.read_exact(&mut end).await.unwrap();
                    assert_eq!(&end, b"\r\n");
                    Reply::Bulk(Some(value))
                }
                _ => panic!("unexpected reply: {line:?}"),
            }
        })
        .await
    }

    async fn expect(&mut self, args: &[&[u8]], expected: Reply) {
        assert_eq!(self.command(args).await, expected);
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

fn ok() -> Reply {
    Reply::Simple(b"OK".to_vec())
}

fn bulk(value: &[u8]) -> Reply {
    Reply::Bulk(Some(value.to_vec()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_sixteen_namespaces_and_independent_default_sessions() {
    let server = TestServer::start().await;
    let mut clients = vec![];
    for index in 0..16 {
        let mut client = Client::connect(&server).await;
        let text = index.to_string();
        client.expect(&[b"sElEcT", text.as_bytes()], ok()).await;
        client.expect(&[b"GET", b"shared"], Reply::Bulk(None)).await;
        client
            .expect(&[b"SET", b"shared", text.as_bytes()], ok())
            .await;
        clients.push(client);
    }
    for (index, client) in clients.iter_mut().enumerate() {
        client
            .expect(&[b"GET", b"shared"], bulk(index.to_string().as_bytes()))
            .await;
        client
            .expect(
                &[b"EXISTS", b"shared", b"absent", b"shared"],
                Reply::Integer(2),
            )
            .await;
    }
    let mut default = Client::connect(&server).await;
    default.expect(&[b"GET", b"shared"], bulk(b"0")).await;
    default.expect(&[b"SET", b"shared", b"changed"], ok()).await;
    clients[0]
        .expect(&[b"GET", b"shared"], bulk(b"changed"))
        .await;
    clients[15].expect(&[b"GET", b"shared"], bulk(b"15")).await;
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selection_is_first_complete_attempt_only_and_errors_leave_database_unchanged() {
    let server = TestServer::start().await;
    let mut default = Client::connect(&server).await;
    default.expect(&[b"SET", b"which", b"zero"], ok()).await;
    let mut other = Client::connect(&server).await;
    other.expect(&[b"SELECT", b"15"], ok()).await;
    other.expect(&[b"SET", b"which", b"fifteen"], ok()).await;
    for index in [
        &b"-1"[..],
        b"16",
        b"01",
        b"-0",
        b"+0",
        b" 1",
        b"1 ",
        b"1.0",
        b"1e0",
        b"\xff",
        b"1\0",
        b"2147483648",
        b"9223372036854775808",
    ] {
        let mut client = Client::connect(&server).await;
        assert!(matches!(
            client.command(&[b"SELECT", index]).await,
            Reply::Error(_)
        ));
        assert!(matches!(
            client.command(&[b"SELECT", b"15"]).await,
            Reply::Error(_)
        ));
        client.expect(&[b"GET", b"which"], bulk(b"zero")).await;
    }
    for first in [
        vec![&b"PING"[..]],
        vec![b"PING", b"a", b"b"],
        vec![b"UNKNOWN"],
        vec![],
        vec![b"SELECT"],
        vec![b"SELECT", b"15", b"0"],
        vec![b"GET"],
        vec![b"SET", b"key"],
        vec![b"EXISTS"],
    ] {
        let mut client = Client::connect(&server).await;
        client.command(&first).await;
        assert!(matches!(
            client.command(&[b"SELECT", b"15"]).await,
            Reply::Error(_)
        ));
        client.expect(&[b"GET", b"which"], bulk(b"zero")).await;
    }
    assert!(matches!(
        other.command(&[b"SELECT", b"0"]).await,
        Reply::Error(_)
    ));
    other.expect(&[b"GET", b"which"], bulk(b"fifteen")).await;
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn string_key_semantics_arity_and_rejected_options_are_binary_safe_and_atomic() {
    let server = TestServer::start().await;
    let mut client = Client::connect(&server).await;
    let key = b"\0\xff\r\n";
    client.expect(&[b"gEt", key], Reply::Bulk(None)).await;
    client
        .expect(&[b"EXISTS", key, b""], Reply::Integer(0))
        .await;
    client.expect(&[b"sEt", key, b""], ok()).await;
    client.expect(&[b"GET", key], bulk(b"")).await;
    client.expect(&[b"SET", b"", key], ok()).await;
    client.expect(&[b"GET", b""], bulk(key)).await;
    client.expect(&[b"SET", key, b"second\0\xff"], ok()).await;
    client.expect(&[b"GET", key], bulk(b"second\0\xff")).await;
    client
        .expect(&[b"eXiStS", key, key, b"missing", b""], Reply::Integer(3))
        .await;
    for args in [
        vec![&b"GET"[..]],
        vec![b"GET", key, b"extra"],
        vec![b"SET"],
        vec![b"SET", key],
        vec![b"SET", key, b"bad", b"NX"],
        vec![b"SET", key, b"bad", b"EX", b"1"],
        vec![b"SET", key, b"bad", b"GET"],
        vec![b"EXISTS"],
    ] {
        assert!(matches!(client.command(&args).await, Reply::Error(_)));
        client.expect(&[b"GET", key], bulk(b"second\0\xff")).await;
    }
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partial_or_truncated_mutations_never_apply_and_fragmented_select_keeps_its_window() {
    let server = TestServer::start().await;
    let mut observer = Client::connect(&server).await;
    let mut partial = server.connect().await;
    let wire = request(&[b"SET", b"key", b"value"]);
    bounded(partial.write_all(&wire[..wire.len() - 1]))
        .await
        .unwrap();
    observer.expect(&[b"GET", b"key"], Reply::Bulk(None)).await;
    bounded(partial.shutdown()).await.unwrap();
    let mut discarded = vec![];
    bounded(partial.read_to_end(&mut discarded)).await.unwrap();
    assert!(discarded.is_empty());
    observer.expect(&[b"GET", b"key"], Reply::Bulk(None)).await;

    let mut selected = Client::connect(&server).await;
    let wire = request(&[b"SELECT", b"15"]);
    bounded(selected.0.get_mut().write_all(&wire[..wire.len() - 1]))
        .await
        .unwrap();
    observer.expect(&[b"SET", b"which", b"zero"], ok()).await;
    // Finish SELECT and check the reply directly, without a second command.
    bounded(selected.0.get_mut().write_all(&wire[wire.len() - 1..]))
        .await
        .unwrap();
    let mut reply = [0; 5];
    bounded(selected.0.read_exact(&mut reply)).await.unwrap();
    assert_eq!(&reply, b"+OK\r\n");
    selected
        .expect(&[b"GET", b"which"], Reply::Bulk(None))
        .await;
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_set_get_histories_and_duplicate_exists_have_atomic_outcomes() {
    let server = TestServer::start().await;
    let mut a = Client::connect(&server).await;
    let mut b = Client::connect(&server).await;
    let mut observer = Client::connect(&server).await;

    async fn writer(
        client: &mut Client,
        key: &[u8],
        value: &[u8],
        gate: &Barrier,
        events: &AtomicUsize,
    ) -> (Reply, usize, usize) {
        gate.wait().await;
        let invoked = events.fetch_add(1, Ordering::SeqCst);
        client.expect(&[b"SET", key, value], ok()).await;
        let responded = events.fetch_add(1, Ordering::SeqCst);
        let read = client.command(&[b"GET", key]).await;
        (read, invoked, responded)
    }

    for round in 0..64 {
        let key = format!("round-{round}").into_bytes();
        let gate = Barrier::new(3);
        let events = AtomicUsize::new(0);
        let observe = async {
            gate.wait().await;
            let mut args = vec![&b"EXISTS"[..]];
            args.extend(std::iter::repeat_n(key.as_slice(), 512));
            observer.command(&args).await
        };
        let ((read_a, start_a, end_a), (read_b, start_b, end_b), exists) = bounded(async {
            tokio::join!(
                writer(&mut a, &key, b"a", &gate, &events),
                writer(&mut b, &key, b"b", &gate, &events),
                observe
            )
        })
        .await;
        // One EXISTS must not observe a fraction of the repeated identical keys.
        assert!(exists == Reply::Integer(0) || exists == Reply::Integer(512));
        let final_value = observer.command(&[b"GET", &key]).await;
        // Exhaustive possible outcomes of the six sequential interleavings of
        // A: SET a; GET and B: SET b; GET. A final read follows both responses.
        assert!(
            (final_value == bulk(b"a")
                && read_a == bulk(b"a")
                && [bulk(b"a"), bulk(b"b")].contains(&read_b))
                || (final_value == bulk(b"b")
                    && read_b == bulk(b"b")
                    && [bulk(b"a"), bulk(b"b")].contains(&read_a)),
            "non-atomic history: {read_a:?}, {read_b:?}, {final_value:?}"
        );
        // If invocations did not overlap, response-before-invocation fixes order.
        if end_a < start_b {
            assert_eq!(final_value, bulk(b"b"));
        }
        if end_b < start_a {
            assert_eq!(final_value, bulk(b"a"));
        }
    }
    // An explicit cross-connection real-time fence, independent of scheduling.
    a.expect(&[b"SET", b"fence", b"first"], ok()).await;
    b.expect(&[b"GET", b"fence"], bulk(b"first")).await;
    b.expect(&[b"SET", b"fence", b"second"], ok()).await;
    a.expect(&[b"GET", b"fence"], bulk(b"second")).await;
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stalled_database_reply_does_not_retain_state_and_restart_is_empty() {
    let server = TestServer::start().await;
    let mut writer = Client::connect(&server).await;
    writer
        .expect(
            &[b"SET", b"large", &vec![b'x'; keyper::resp::MAX_BULK_BYTES]],
            ok(),
        )
        .await;
    let mut nonreading = server.connect().await;
    bounded(nonreading.write_all(&request(&[b"GET", b"large"])))
        .await
        .unwrap();
    // Fence on reply-writing having begun, then leave the large body unread.
    let mut header = [0; 10];
    bounded(nonreading.read_exact(&mut header)).await.unwrap();
    assert_eq!(&header, b"$8388608\r\n");
    let mut other = Client::connect(&server).await;
    other.expect(&[b"SELECT", b"15"], ok()).await;
    other.expect(&[b"SET", b"key", b"independent"], ok()).await;
    writer.expect(&[b"GET", b"absent"], Reply::Bulk(None)).await;
    drop(nonreading);
    server.stop().await;

    let server = TestServer::start().await;
    let mut fresh = Client::connect(&server).await;
    fresh.expect(&[b"GET", b"large"], Reply::Bulk(None)).await;
    server.stop().await;
}
