//! Real RESP2 transaction queues, error state transitions, and isolated batches.

mod support;

use std::collections::HashMap;
use std::sync::Arc;

use keyper::resp::Reply;
use redis::{Value, aio::MultiplexedConnection};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::Barrier;
use tokio::task::JoinSet;

use support::{TestServer, bounded};

struct Client(BufReader<TcpStream>);

fn encode(args: &[&[u8]]) -> Vec<u8> {
    let mut bytes = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        bytes.extend(format!("${}\r\n", arg.len()).as_bytes());
        bytes.extend(*arg);
        bytes.extend(b"\r\n");
    }
    bytes
}

impl Client {
    async fn connect(server: &TestServer) -> Self {
        Self(BufReader::new(server.connect().await))
    }

    async fn read(&mut self) -> Reply {
        let mut header = String::new();
        self.0.read_line(&mut header).await.unwrap();
        assert!(header.ends_with("\r\n"));
        let value = &header.as_bytes()[1..header.len() - 2];
        match header.as_bytes()[0] {
            b'+' => Reply::Simple(value.to_vec()),
            b'-' => Reply::Error(value.to_vec()),
            b':' => Reply::Integer(std::str::from_utf8(value).unwrap().parse().unwrap()),
            b'$' | b'*' => {
                let count: i64 = std::str::from_utf8(value).unwrap().parse().unwrap();
                if header.starts_with('$') {
                    if count == -1 {
                        return Reply::Bulk(None);
                    }
                    let mut bytes = vec![0; count as usize];
                    self.0.read_exact(&mut bytes).await.unwrap();
                    assert_eq!(self.0.read_u16().await.unwrap(), 0x0d0a);
                    Reply::Bulk(Some(bytes))
                } else if count == -1 {
                    Reply::Array(None)
                } else {
                    let mut replies = Vec::new();
                    for _ in 0..count {
                        replies.push(Box::pin(self.read()).await);
                    }
                    Reply::Array(Some(replies))
                }
            }
            _ => panic!("invalid reply header"),
        }
    }

    async fn command(&mut self, args: &[&[u8]]) -> Reply {
        bounded(async {
            self.0.get_mut().write_all(&encode(args)).await.unwrap();
            self.read().await
        })
        .await
    }

    async fn expect(&mut self, args: &[&[u8]], expected: Reply) {
        assert_eq!(self.command(args).await, expected, "{:?}", args[0]);
    }

    async fn queue(&mut self, args: &[&[u8]]) {
        self.expect(args, simple(b"QUEUED")).await;
    }
}

fn simple(bytes: &[u8]) -> Reply {
    Reply::Simple(bytes.to_vec())
}

fn bulk(bytes: &[u8]) -> Reply {
    Reply::Bulk(Some(bytes.to_vec()))
}

fn error(bytes: &[u8]) -> Reply {
    Reply::Error(bytes.to_vec())
}

fn array(replies: Vec<Reply>) -> Reply {
    Reply::Array(Some(replies))
}

fn abort() -> Reply {
    error(b"EXECABORT Transaction discarded because of previous errors.")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fragmented_transaction_pipeline_preserves_all_nested_reply_shapes_and_database_selection()
{
    let server = TestServer::start().await;
    for db in 0..16 {
        let mut client = Client::connect(&server).await;
        let mut wire = Vec::new();
        for args in [
            vec![b"SELECT".as_slice(), db.to_string().as_bytes()],
            vec![b"mUlTi"],
            vec![b"SET", b"\0\xff", b""],
            vec![b"GET", b"\0\xff"],
            vec![b"PING"],
            vec![b"PING", b"\0\xff\r\n"],
            vec![b"GET", b"absent"],
            vec![b"LPOP", b"absent", b"1"],
            vec![b"SADD", b"set", b""],
            vec![b"SMEMBERS", b"set"],
            vec![b"SELECT", b"0"],
            vec![b"eXeC"],
            vec![b"EXEC"],
            vec![b"GET", b"\0\xff"],
        ] {
            wire.extend(encode(&args));
        }
        // Fragment requests independently of command boundaries. No reply is
        // read until the complete pipeline is sent.
        bounded(async {
            for part in wire.chunks(3) {
                client.0.get_mut().write_all(part).await.unwrap();
            }
            assert_eq!(client.read().await, simple(b"OK"));
            assert_eq!(client.read().await, simple(b"OK"));
            for _ in 0..9 {
                assert_eq!(client.read().await, simple(b"QUEUED"));
            }
            assert_eq!(
                client.read().await,
                array(vec![
                    simple(b"OK"),
                    bulk(b""),
                    simple(b"PONG"),
                    bulk(b"\0\xff\r\n"),
                    Reply::Bulk(None),
                    Reply::Array(None),
                    Reply::Integer(1),
                    array(vec![bulk(b"")]),
                    error(b"ERR SELECT is only allowed as the first command"),
                ])
            );
            assert_eq!(client.read().await, error(b"ERR EXEC without MULTI"));
            assert_eq!(client.read().await, bulk(b""));
        })
        .await;
    }
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admission_errors_abort_but_nested_multi_keeps_the_original_queue() {
    let server = TestServer::start().await;
    let mut client = Client::connect(&server).await;
    client
        .expect(&[b"EXEC"], error(b"ERR EXEC without MULTI"))
        .await;
    client
        .expect(&[b"DISCARD"], error(b"ERR DISCARD without MULTI"))
        .await;
    for bad in [
        vec![b"GET".as_slice()],
        vec![b"MULTI", b"extra"],
        vec![b"DISCARD", b"extra"],
        vec![b"SET", b"k", b"v", b"NX"],
        vec![b"HSET", b"h", b"f"],
        vec![b"UNKNOWN"],
        vec![b"BLPOP", b"a", b"b", b"c", b"d", b"e", b"f", b"0"],
        vec![],
    ] {
        client.expect(&[b"MULTI"], simple(b"OK")).await;
        client.queue(&[b"SET", b"never", b"applied"]).await;
        assert!(matches!(client.command(&bad).await, Reply::Error(_)));
        client.queue(&[b"INCR", b"never"]).await;
        client.expect(&[b"EXEC"], abort()).await;
        client.expect(&[b"GET", b"never"], Reply::Bulk(None)).await;
    }
    client.expect(&[b"MULTI"], simple(b"OK")).await;
    client.queue(&[b"SET", b"kept", b"1"]).await;
    client
        .expect(&[b"MULTI"], error(b"ERR MULTI calls can not be nested"))
        .await;
    client.queue(&[b"INCR", b"kept"]).await;
    client
        .expect(&[b"EXEC"], array(vec![simple(b"OK"), Reply::Integer(2)]))
        .await;
    client.expect(&[b"MULTI"], simple(b"OK")).await;
    assert!(matches!(client.command(&[b"GET"]).await, Reply::Error(_)));
    client.expect(&[b"DISCARD"], simple(b"OK")).await;
    client.expect(&[b"MULTI"], simple(b"OK")).await;
    client.expect(&[b"EXEC"], array(vec![])).await;
    let arity_abort =
        b"EXECABORT Transaction discarded because of: wrong number of arguments for 'exec' command";
    client
        .expect(&[b"EXEC", b"extra"], error(arity_abort))
        .await;
    for dirty in [false, true] {
        client.expect(&[b"MULTI"], simple(b"OK")).await;
        client.queue(&[b"SET", b"never", b"applied"]).await;
        if dirty {
            assert!(matches!(client.command(&[b"GET"]).await, Reply::Error(_)));
        }
        client
            .expect(&[b"EXEC", b"extra"], error(arity_abort))
            .await;
        client
            .expect(&[b"EXEC"], error(b"ERR EXEC without MULTI"))
            .await;
        client.expect(&[b"GET", b"never"], Reply::Bulk(None)).await;
    }
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn variable_arity_form_errors_are_deferred_despite_their_wrong_arity_error_text() {
    let server = TestServer::start().await;
    let mut client = Client::connect(&server).await;
    client.expect(&[b"MULTI"], simple(b"OK")).await;
    client.queue(&[b"INCR", b"counter"]).await;
    for args in [
        vec![b"PING".as_slice(), b"a", b"b"],
        vec![b"HSET", b"h", b"f", b"v", b"unpaired"],
        vec![b"LPOP", b"list", b"1", b"extra"],
    ] {
        client.queue(&args).await;
    }
    client.queue(&[b"INCR", b"counter"]).await;
    client
        .expect(
            &[b"EXEC"],
            array(vec![
                Reply::Integer(1),
                error(b"ERR wrong number of arguments for 'ping' command"),
                error(b"ERR wrong number of arguments for 'hset' command"),
                error(b"ERR wrong number of arguments for 'lpop' command"),
                Reply::Integer(2),
            ]),
        )
        .await;
    client
        .expect(&[b"EXISTS", b"h", b"list"], Reply::Integer(0))
        .await;
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_errors_retain_writes_and_execute_later_commands_without_waiting() {
    let server = TestServer::start().await;
    let mut client = Client::connect(&server).await;
    client.expect(&[b"MULTI"], simple(b"OK")).await;
    for args in [
        vec![b"SET".as_slice(), b"key", b"not-integer"],
        vec![b"INCR", b"key"],
        vec![b"RPUSH", b"key", b"bad"],
        vec![b"BLPOP", b"missing", b"0"],
        vec![b"BRPOP", b"missing", b"9999"],
        vec![b"BLPOP", b"missing", b"nan"],
        vec![b"LPOP", b"missing", b"-1"],
        vec![b"LINDEX", b"missing", b"bad"],
        vec![b"RPUSH", b"list", b"a", b"b"],
        vec![b"LINDEX", b"list", b"bad"],
        vec![b"BRPOP", b"list", b"0"],
        vec![b"GET", b"key"],
        vec![b"SET", b"key", b"7"],
        vec![b"INCR", b"key"],
    ] {
        client.queue(&args).await;
    }
    client
        .expect(
            &[b"EXEC"],
            array(vec![
                simple(b"OK"),
                error(b"ERR value is not an integer or out of range"),
                error(b"WRONGTYPE Operation against a key holding the wrong kind of value"),
                Reply::Array(None),
                Reply::Array(None),
                error(b"ERR timeout is not a float or out of range"),
                error(b"ERR value is out of range, must be positive"),
                Reply::Bulk(None),
                Reply::Integer(2),
                error(b"ERR value is not an integer or out of range"),
                array(vec![bulk(b"list"), bulk(b"b")]),
                bulk(b"not-integer"),
                simple(b"OK"),
                Reply::Integer(8),
            ]),
        )
        .await;
    client.expect(&[b"GET", b"key"], bulk(b"8")).await;
    client.expect(&[b"LPOP", b"list"], bulk(b"a")).await;
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn discard_disconnect_and_truncated_exec_drop_unexecuted_queues_without_locking_state() {
    let server = TestServer::start().await;
    let mut observer = Client::connect(&server).await;
    for discard in [true, false] {
        let mut client = Client::connect(&server).await;
        client.expect(&[b"MULTI"], simple(b"OK")).await;
        client.queue(&[b"SET", b"absent", b"never"]).await;
        observer
            .expect(&[b"GET", b"absent"], Reply::Bulk(None))
            .await;
        observer
            .expect(
                &[b"INCR", b"independent"],
                Reply::Integer(if discard { 1 } else { 2 }),
            )
            .await;
        if discard {
            client.expect(&[b"dIsCaRd"], simple(b"OK")).await;
            client
                .expect(&[b"EXEC"], error(b"ERR EXEC without MULTI"))
                .await;
        } else {
            let wire = encode(&[b"EXEC"]);
            bounded(client.0.get_mut().write_all(&wire[..wire.len() - 1]))
                .await
                .unwrap();
        }
        drop(client);
        observer
            .expect(&[b"GET", b"absent"], Reply::Bulk(None))
            .await;
    }
    let mut unexecuted = Client::connect(&server).await;
    unexecuted.expect(&[b"MULTI"], simple(b"OK")).await;
    unexecuted.queue(&[b"BLPOP", b"missing", b"0"]).await;
    unexecuted.queue(&[b"SET", b"absent", b"never"]).await;
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queue_command_and_byte_limits_dirty_the_transaction_and_allow_recovery() {
    let server = TestServer::start().await;
    let mut client = Client::connect(&server).await;
    for large in [false, true] {
        client.expect(&[b"MULTI"], simple(b"OK")).await;
        let payload = vec![b'x'; if large { 8 * 1024 * 1024 } else { 0 }];
        for _ in 0..if large { 1 } else { 1024 } {
            client.queue(&[b"SET", b"never", &payload]).await;
        }
        client
            .expect(
                &[b"SET", b"never", &payload],
                error(b"ERR transaction queue limit exceeded"),
            )
            .await;
        client.queue(&[b"SET", b"never", b"later"]).await;
        client.expect(&[b"EXEC"], abort()).await;
        client.expect(&[b"GET", b"never"], Reply::Bulk(None)).await;
    }
    client.expect(&[b"MULTI"], simple(b"OK")).await;
    client.queue(&[b"SET", b"after", b"ok"]).await;
    client.expect(&[b"EXEC"], array(vec![simple(b"OK")])).await;
    server.stop().await;
}

async fn redis_client(server: &TestServer, db: u8) -> MultiplexedConnection {
    bounded(
        redis::Client::open(format!("redis://{}/{db}", server.address))
            .unwrap()
            .get_multiplexed_async_connection(),
    )
    .await
    .unwrap()
}

type Snapshot = (
    HashMap<String, String>,
    usize,
    usize,
    usize,
    HashMap<String, String>,
    HashMap<String, String>,
);

async fn snapshot(client: &mut MultiplexedConnection) -> Snapshot {
    // Exact unmodified crawler read() composition and RESP2 decoding shape.
    bounded(
        redis::pipe()
            .atomic()
            .cmd("HGETALL")
            .arg("meta")
            .cmd("SCARD")
            .arg("seen")
            .cmd("LLEN")
            .arg("frontier")
            .cmd("HLEN")
            .arg("in-flight")
            .cmd("HGETALL")
            .arg("stats")
            .cmd("HGETALL")
            .arg("extensions")
            .query_async(client),
    )
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crawler_snapshot_is_coherent_during_writes_and_database_isolated() {
    let server = TestServer::start().await;
    for db in 0..16u8 {
        let mut client = redis_client(&server, db).await;
        let state = snapshot(&mut client).await;
        assert!(state.0.is_empty() && state.4.is_empty() && state.5.is_empty());
        assert_eq!((state.1, state.2, state.3), (0, 0, 0));
    }
    let barrier = Arc::new(Barrier::new(5));
    let mut tasks = JoinSet::new();
    let mut writer = redis_client(&server, 15).await;
    let start = barrier.clone();
    tasks.spawn(async move {
        start.wait().await;
        for n in 1..=80 {
            let _: Value = bounded(
                redis::pipe()
                    .atomic()
                    .cmd("HSET")
                    .arg("meta")
                    .arg("epoch")
                    .arg(n)
                    .cmd("SADD")
                    .arg("seen")
                    .arg(n)
                    .cmd("RPUSH")
                    .arg("frontier")
                    .arg(n)
                    .cmd("HSET")
                    .arg("in-flight")
                    .arg(n)
                    .arg("owner")
                    .cmd("HSET")
                    .arg("stats")
                    .arg("epoch")
                    .arg(n)
                    .cmd("HSET")
                    .arg("extensions")
                    .arg("epoch")
                    .arg(n)
                    .query_async(&mut writer),
            )
            .await
            .unwrap();
        }
    });
    for _ in 0..4 {
        let mut reader = redis_client(&server, 15).await;
        let start = barrier.clone();
        tasks.spawn(async move {
            start.wait().await;
            for _ in 0..100 {
                let (meta, seen, frontier, inflight, stats, extensions) =
                    snapshot(&mut reader).await;
                let epoch = meta
                    .get("epoch")
                    .map(|s| s.parse::<usize>().unwrap())
                    .unwrap_or(0);
                assert_eq!((seen, frontier, inflight), (epoch, epoch, epoch));
                assert_eq!(stats.get("epoch"), meta.get("epoch"));
                assert_eq!(extensions.get("epoch"), meta.get("epoch"));
            }
        });
    }
    bounded(async {
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    })
    .await;
    let mut client = redis_client(&server, 15).await;
    assert_eq!(snapshot(&mut client).await.1, 80);
    let mut zero = redis_client(&server, 0).await;
    assert_eq!(snapshot(&mut zero).await.1, 0);
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn competing_exec_batches_and_ordinary_increments_cannot_interleave_inside_a_batch() {
    let server = TestServer::start().await;
    let barrier = Arc::new(Barrier::new(9));
    let mut tasks = JoinSet::new();
    for task in 0..9 {
        let mut client = redis_client(&server, 0).await;
        let start = barrier.clone();
        tasks.spawn(async move {
            start.wait().await;
            let mut ids = Vec::new();
            for _ in 0..40 {
                if task == 0 {
                    let id: i64 =
                        bounded(redis::cmd("INCR").arg("counter").query_async(&mut client))
                            .await
                            .unwrap();
                    ids.push(id);
                } else {
                    let mut pipe = redis::pipe();
                    pipe.atomic();
                    for _ in 0..8 {
                        pipe.cmd("INCR").arg("counter");
                    }
                    let batch: Vec<i64> = bounded(pipe.query_async(&mut client)).await.unwrap();
                    assert!(batch.windows(2).all(|w| w[1] == w[0] + 1));
                    ids.extend(batch);
                }
            }
            ids
        });
    }
    let mut ids = Vec::new();
    bounded(async {
        while let Some(result) = tasks.join_next().await {
            ids.extend(result.unwrap());
        }
    })
    .await;
    ids.sort_unstable();
    assert_eq!(ids, (1..=2600).collect::<Vec<_>>());
    let mut fence = redis_client(&server, 0).await;
    let next: i64 = bounded(redis::cmd("INCR").arg("counter").query_async(&mut fence))
        .await
        .unwrap();
    assert_eq!(next, 2601);
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unread_exec_reply_does_not_lock_state_or_roll_back_committed_writes() {
    let server = TestServer::start().await;
    let mut client = Client::connect(&server).await;
    let mut observer = Client::connect(&server).await;
    let large = vec![b'x'; 8 * 1024 * 1024];
    observer
        .expect(&[b"SET", b"large", &large], simple(b"OK"))
        .await;
    client.expect(&[b"MULTI"], simple(b"OK")).await;
    client.queue(&[b"SET", b"marker", b"committed"]).await;
    for _ in 0..3 {
        client.queue(&[b"GET", b"large"]).await;
    }
    bounded(client.0.get_mut().write_all(&encode(&[b"EXEC"])))
        .await
        .unwrap();
    // Reading the first array byte proves owner execution without draining the
    // large reply. Other clients progress while this writer is backpressured.
    assert_eq!(bounded(client.0.read_u8()).await.unwrap(), b'*');
    observer
        .expect(&[b"GET", b"marker"], bulk(b"committed"))
        .await;
    drop(client);
    observer
        .expect(&[b"GET", b"marker"], bulk(b"committed"))
        .await;
    observer
        .expect(&[b"INCR", b"counter"], Reply::Integer(1))
        .await;
    server.stop().await;
}
