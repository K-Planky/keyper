//! Public database/session contracts and small concurrent histories over TCP.

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use keyper::resp::Reply;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::Barrier;
use tokio::task::JoinSet;

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
        vec![b"LPUSH", b"key"],
        vec![b"RPUSH", b"key", b"value"],
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

fn wrong_type() -> Reply {
    Reply::Error(b"WRONGTYPE Operation against a key holding the wrong kind of value".to_vec())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pushes_are_binary_safe_and_type_or_arity_errors_preserve_values() {
    let server = TestServer::start().await;
    let mut client = Client::connect(&server).await;
    for key in [&b""[..], b"\0\xff\r\n"] {
        client.expect(&[b"EXISTS", key], Reply::Integer(0)).await;
        client
            .expect(&[b"lPuSh", key, b"", b"\0\xff", b""], Reply::Integer(3))
            .await;
        client
            .expect(&[b"rPuSh", key, b"c", b"d"], Reply::Integer(5))
            .await;
        client.expect(&[b"GET", key], wrong_type()).await;
        client
            .expect(&[b"EXISTS", key, b"absent", key], Reply::Integer(2))
            .await;
        for name in [&b"LPUSH"[..], b"RPUSH"] {
            let error = Reply::Error(
                format!(
                    "ERR wrong number of arguments for '{}' command",
                    std::str::from_utf8(name).unwrap().to_ascii_lowercase()
                )
                .into_bytes(),
            );
            // Admission fails before a list is created or an old one is touched.
            client.expect(&[name], error).await;
            assert!(matches!(
                client.command(&[name, key]).await,
                Reply::Error(_)
            ));
            client
                .expect(&[b"EXISTS", b"absent"], Reply::Integer(0))
                .await;
            assert!(matches!(
                client.command(&[name, b"absent"]).await,
                Reply::Error(_)
            ));
            client
                .expect(&[b"EXISTS", b"absent"], Reply::Integer(0))
                .await;
        }
        assert!(matches!(
            client.command(&[b"SET", key, b"bad", b"NX"]).await,
            Reply::Error(_)
        ));
        client.expect(&[b"GET", key], wrong_type()).await;
        client
            .expect(&[b"RPUSH", key, b"still-a-list"], Reply::Integer(6))
            .await;
        for value in [&b""[..], b"replacement\0\xff"] {
            client.expect(&[b"SET", key, value], ok()).await;
            client.expect(&[b"GET", key], bulk(value)).await;
            for name in [&b"LPUSH"[..], b"RPUSH"] {
                client.expect(&[name, key, b"a", b"b"], wrong_type()).await;
                client.expect(&[b"GET", key], bulk(value)).await;
            }
        }
    }
    // All 1022 values allowed by the request argument limit are inserted once.
    let mut args = vec![&b"RPUSH"[..], b"many"];
    args.extend(std::iter::repeat_n(
        &b""[..],
        keyper::resp::MAX_ARGUMENTS - 2,
    ));
    client.expect(&args, Reply::Integer(1022)).await;
    args[0] = b"LPUSH";
    client.expect(&args, Reply::Integer(2044)).await;
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lists_in_all_sixteen_databases_have_independent_types_and_lengths() {
    let server = TestServer::start().await;
    let mut clients = vec![];
    for index in 0..16 {
        let mut client = Client::connect(&server).await;
        client
            .expect(&[b"SELECT", index.to_string().as_bytes()], ok())
            .await;
        let mut args = vec![&b"RPUSH"[..], b"shared"];
        args.extend(std::iter::repeat_n(&b"item"[..], index + 1));
        client.expect(&args, Reply::Integer(index as i64 + 1)).await;
        clients.push(client);
    }
    for (index, client) in clients.iter_mut().enumerate() {
        client
            .expect(
                &[b"LPUSH", b"shared", b"front"],
                Reply::Integer(index as i64 + 2),
            )
            .await;
    }
    let mut default = Client::connect(&server).await;
    default
        .expect(&[b"RPUSH", b"shared", b"default"], Reply::Integer(3))
        .await;
    default
        .expect(&[b"SET", b"shared", b"now-string"], ok())
        .await;
    clients[0]
        .expect(&[b"GET", b"shared"], bulk(b"now-string"))
        .await;
    clients[15].expect(&[b"GET", b"shared"], wrong_type()).await;
    clients[15]
        .expect(&[b"RPUSH", b"shared", b"last"], Reply::Integer(18))
        .await;
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_variadic_pushes_return_whole_batch_lengths_without_lost_updates() {
    const WRITERS: usize = 8;
    const ROUNDS: usize = 32;
    let server = TestServer::start().await;
    let gate = Arc::new(Barrier::new(WRITERS));
    let mut tasks = JoinSet::new();
    for writer in 0..WRITERS {
        let mut client = Client::connect(&server).await;
        let gate = gate.clone();
        tasks.spawn(async move {
            let mut replies = vec![];
            let name = if writer % 2 == 0 {
                &b"LPUSH"[..]
            } else {
                b"RPUSH"
            };
            let identity = writer.to_string();
            for round in 0..ROUNDS {
                let key = format!("round-{round}");
                gate.wait().await;
                replies.push(
                    client
                        .command(&[name, key.as_bytes(), identity.as_bytes(), b"", b"\0\xff"])
                        .await,
                );
            }
            replies
        });
    }
    let mut lengths: Vec<Vec<i64>> = (0..ROUNDS).map(|_| vec![]).collect();
    bounded(async {
        while let Some(result) = tasks.join_next().await {
            for (round, reply) in result.unwrap().into_iter().enumerate() {
                let Reply::Integer(length) = reply else {
                    panic!("push returned {reply:?}");
                };
                lengths[round].push(length);
            }
        }
    })
    .await;
    let mut observer = Client::connect(&server).await;
    for (round, lengths) in lengths.iter_mut().enumerate() {
        lengths.sort_unstable();
        assert_eq!(
            *lengths,
            (1..=WRITERS as i64).map(|n| n * 3).collect::<Vec<_>>()
        );
        // This invocation follows every writer response: real-time order fixes
        // the complete list length, regardless of the overlapping push order.
        observer
            .expect(
                &[b"RPUSH", format!("round-{round}").as_bytes(), b"fence"],
                Reply::Integer(WRITERS as i64 * 3 + 1),
            )
            .await;
    }
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn racing_set_and_push_have_only_valid_atomic_type_outcomes() {
    let server = TestServer::start().await;
    let mut setter = Client::connect(&server).await;
    let mut pusher = Client::connect(&server).await;
    let gate = Barrier::new(2);
    for round in 0..32 {
        let key = format!("race-{round}");
        let setting = async {
            gate.wait().await;
            setter
                .expect(&[b"SET", key.as_bytes(), b"string"], ok())
                .await;
        };
        let pushing = async {
            gate.wait().await;
            pusher
                .command(&[b"LPUSH", key.as_bytes(), b"a", b"b", b"c"])
                .await
        };
        let ((), push) = bounded(async { tokio::join!(setting, pushing) }).await;
        assert!(push == Reply::Integer(3) || push == wrong_type());
        setter
            .expect(&[b"GET", key.as_bytes()], bulk(b"string"))
            .await;
    }
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incomplete_variadic_pushes_never_create_or_partially_extend_a_list() {
    let server = TestServer::start().await;
    let mut observer = Client::connect(&server).await;
    observer
        .expect(&[b"RPUSH", b"existing", b"original"], Reply::Integer(1))
        .await;
    for (key, length) in [(&b"existing"[..], 1), (&b"absent"[..], 0)] {
        for name in [&b"LPUSH"[..], b"RPUSH"] {
            let mut partial = server.connect().await;
            let wire = request(&[name, key, b"one", b"two"]);
            bounded(partial.write_all(&wire[..wire.len() - 1]))
                .await
                .unwrap();
            observer
                .expect(&[b"EXISTS", key], Reply::Integer(i64::from(length > 0)))
                .await;
            bounded(partial.shutdown()).await.unwrap();
            let mut reply = vec![];
            bounded(partial.read_to_end(&mut reply)).await.unwrap();
            assert!(reply.is_empty());
        }
        observer
            .expect(&[b"RPUSH", key, b"fence"], Reply::Integer(length + 1))
            .await;
    }
    server.stop().await;
}
