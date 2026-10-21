//! Raw outstanding requests plus the crawler's exact Redis client/configuration.
//! No crawler checkout, reference server, or client patches are needed.

mod support;

use std::time::Duration;

use redis::{AsyncConnectionConfig, ErrorKind, Value, aio::MultiplexedConnection};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Barrier;
use tokio::task::JoinSet;

use support::{TestServer, bounded};

fn request(args: &[&[u8]]) -> Vec<u8> {
    let mut bytes = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        bytes.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
        bytes.extend_from_slice(arg);
        bytes.extend_from_slice(b"\r\n");
    }
    bytes
}

async fn expect(stream: &mut TcpStream, expected: &[u8]) {
    let mut bytes = vec![0; expected.len()];
    bounded(stream.read_exact(&mut bytes)).await.unwrap();
    assert_eq!(bytes, expected);
}

async fn connect(server: &TestServer, db: u8) -> redis::RedisResult<MultiplexedConnection> {
    // Same RESP2 URL path, features, and timeout ownership as swarmcrawl's
    // check/store setup. Do NOT disable the library's CLIENT SETINFO pipeline.
    let client = redis::Client::open(format!("redis://{}/{db}", server.address)).unwrap();
    let config = AsyncConnectionConfig::new()
        .set_connection_timeout(None)
        .set_response_timeout(None);
    bounded(client.get_multiplexed_async_connection_with_config(&config)).await
}

async fn wait_for_key(client: &mut MultiplexedConnection, key: &str) {
    bounded(async {
        loop {
            if redis::cmd("GET")
                .arg(key)
                .query_async::<Option<String>>(client)
                .await
                .unwrap()
                .is_some()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coalesced_and_fragmented_pipelines_preserve_effects_errors_and_reply_types() {
    let server = TestServer::start().await;
    // Repeat the same observable history with coalesced, bytewise, and uneven
    // writes. Include an >8 KiB binary bulk so framing crosses read buffers.
    let value: Vec<u8> = (0..20_000).map(|n| (n % 256) as u8).collect();
    for chunk in [usize::MAX, 1, 137] {
        let mut client = server.connect().await;
        let mut wire = Vec::new();
        for args in [
            vec![&b"SELECT"[..], b"15"],
            vec![b"SET", b"\0\xff", &value],
            vec![b"GET", b"\0\xff"],
            vec![b"UNKNOWN"],
            vec![b"SET", b"\0\xff", b"not-applied", b"NX"],
            vec![b"LPUSH", b"\0\xff", b"wrong-type"],
            vec![b"GET", b"\0\xff"],
            vec![b"SET", b"\0\xff", b""],
            vec![b"GET", b"\0\xff"],
            vec![b"GET", b"absent"],
            vec![b"EXISTS", b"\0\xff", b"absent", b"\0\xff"],
            vec![b"SELECT", b"0"],
            vec![b"RPUSH", b"list", b"a", b"b"],
            vec![b"BRPOP", b"list", b"0"],
            vec![b"BLPOP", b"list", b"0"],
            vec![b"BRPOP", b"list", b"0.001"],
            vec![b"PING", b"end"],
        ] {
            wire.extend(request(&args));
        }
        bounded(async {
            for bytes in wire.chunks(chunk) {
                client.write_all(bytes).await.unwrap();
            }
        })
        .await;
        expect(&mut client, b"+OK\r\n+OK\r\n$20000\r\n").await;
        expect(&mut client, &value).await;
        expect(
            &mut client,
            concat!(
                "\r\n",
                "-ERR unknown command\r\n",
                "-ERR wrong number of arguments for 'set' command\r\n",
                "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n",
                "$20000\r\n",
            )
            .as_bytes(),
        )
        .await;
        expect(&mut client, &value).await;
        expect(
            &mut client,
            concat!(
                "\r\n+OK\r\n$0\r\n\r\n$-1\r\n:2\r\n",
                "-ERR SELECT is only allowed as the first command\r\n",
                ":2\r\n",
                "*2\r\n$4\r\nlist\r\n$1\r\nb\r\n",
                "*2\r\n$4\r\nlist\r\n$1\r\na\r\n",
                "*-1\r\n$3\r\nend\r\n",
            )
            .as_bytes(),
        )
        .await;
    }
    let mut default_db = connect(&server, 0).await.unwrap();
    assert_eq!(
        bounded(
            redis::cmd("EXISTS")
                .arg(b"\0\xff")
                .query_async::<i64>(&mut default_db)
        )
        .await
        .unwrap(),
        0
    );
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_metadata_errors_are_honest_ordered_and_do_not_extend_selection() {
    let server = TestServer::start().await;
    for db in [0, 15] {
        let mut client = server.connect().await;
        let mut wire = Vec::new();
        if db != 0 {
            wire.extend(request(&[b"SELECT", b"15"]));
        }
        wire.extend(request(&[b"CLIENT", b"SETINFO", b"LIB-NAME", b"redis-rs"]));
        wire.extend(request(&[b"CLIENT", b"SETINFO", b"LIB-VER", b"1.7.1"]));
        wire.extend(request(&[b"SELECT", b"1"]));
        wire.extend(request(&[b"PING"]));
        bounded(client.write_all(&wire)).await.unwrap();
        if db != 0 {
            expect(&mut client, b"+OK\r\n").await;
        }
        expect(
            &mut client,
            concat!(
                "-ERR unknown command\r\n",
                "-ERR unknown command\r\n",
                "-ERR SELECT is only allowed as the first command\r\n",
                "+PONG\r\n",
            )
            .as_bytes(),
        )
        .await;
    }
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fatal_framing_in_a_pipeline_stops_the_suffix_without_undoing_the_prefix() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;
    let mut wire = request(&[b"SET", b"prefix", b"retained"]);
    wire.extend_from_slice(b"*1\r\n+PING\r\n");
    wire.extend(request(&[b"SET", b"suffix", b"must-not-run"]));
    bounded(client.write_all(&wire)).await.unwrap();
    let mut output = Vec::new();
    bounded(client.read_to_end(&mut output)).await.unwrap();
    assert_eq!(
        output,
        b"+OK\r\n-ERR Protocol error: expected non-null bulk string\r\n"
    );
    let mut observer = connect(&server, 0).await.unwrap();
    let values: (Option<String>, Option<String>) = bounded(
        redis::pipe()
            .cmd("GET")
            .arg("prefix")
            .cmd("GET")
            .arg("suffix")
            .query_async(&mut observer),
    )
    .await
    .unwrap();
    assert_eq!(values, (Some("retained".into()), None));
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_reply_and_queued_batches_resume_in_order_while_other_clients_progress() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;
    let payload = vec![b'x'; keyper::resp::MAX_BULK_BYTES];
    let mut wire = request(&[b"PING", &payload]);
    // Do not read replies yet. More than one read buffer of outstanding input
    // must survive cancellation of read-ahead as large writes finish.
    for n in 0..1024 {
        let value = n.to_string();
        wire.extend(request(&[b"SET", b"ordered", value.as_bytes()]));
        wire.extend(request(&[b"GET", b"ordered"]));
    }
    bounded(client.write_all(&wire)).await.unwrap();
    let mut observer = connect(&server, 15).await.unwrap();
    assert_eq!(
        bounded(redis::cmd("PING").query_async::<String>(&mut observer))
            .await
            .unwrap(),
        "PONG"
    );
    expect(&mut client, b"$8388608\r\n").await;
    let mut received = vec![0; payload.len()];
    bounded(client.read_exact(&mut received)).await.unwrap();
    assert_eq!(received, payload);
    expect(&mut client, b"\r\n").await;
    for n in 0..1024 {
        let value = n.to_string();
        expect(
            &mut client,
            format!("+OK\r\n${}\r\n{value}\r\n", value.len()).as_bytes(),
        )
        .await;
    }
    // A new request is a fence against any stray/duplicated pipeline reply.
    bounded(client.write_all(&request(&[b"PING", b"fence"])))
        .await
        .unwrap();
    expect(&mut client, b"$5\r\nfence\r\n").await;
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_client_startup_selects_all_databases_and_rejects_invalid_selection() {
    let server = TestServer::start().await;
    for db in 0..16 {
        let mut client = connect(&server, db).await.unwrap();
        let result: (String, String, i64) = bounded(
            redis::pipe()
                .cmd("SET")
                .arg("same-key")
                .arg(db)
                .ignore()
                .cmd("PING")
                .cmd("GET")
                .arg("same-key")
                .cmd("EXISTS")
                .arg("same-key")
                .arg("same-key")
                .query_async(&mut client),
        )
        .await
        .unwrap();
        assert_eq!(result, ("PONG".into(), db.to_string(), 2));
        assert!(
            bounded(redis::cmd("SELECT").arg(db).query_async::<()>(&mut client))
                .await
                .is_err()
        );
    }
    for db in 0..16 {
        let mut client = connect(&server, db).await.unwrap();
        assert_eq!(
            bounded(
                redis::cmd("GET")
                    .arg("same-key")
                    .query_async::<u8>(&mut client)
            )
            .await
            .unwrap(),
            db
        );
    }
    // The setup pipeline must propagate SELECT errors, not ignore them like
    // metadata errors. No fallback connection/retry is supplied by this test.
    assert!(connect(&server, 16).await.is_err());
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cloned_multiplexed_client_routes_concurrent_binary_successes_and_errors() {
    let server = TestServer::start().await;
    let client = connect(&server, 15).await.unwrap();
    let barrier = std::sync::Arc::new(Barrier::new(33));
    let mut tasks = JoinSet::new();
    for n in 0..32u8 {
        let mut client = client.clone();
        let barrier = barrier.clone();
        tasks.spawn(async move {
            let key = vec![0, 255, n];
            bounded(barrier.wait()).await;
            for iteration in 0..8u8 {
                let value = vec![n, iteration, 0, 255, b'\r', b'\n'];
                let result: (Vec<u8>, i64, Vec<u8>) = bounded(
                    redis::pipe()
                        .cmd("SET")
                        .arg(&key)
                        .arg(&value)
                        .ignore()
                        .cmd("GET")
                        .arg(&key)
                        .cmd("EXISTS")
                        .arg(&key)
                        .arg(&key)
                        .cmd("PING")
                        .arg(&value)
                        .query_async(&mut client),
                )
                .await
                .unwrap();
                assert_eq!(result, (value.clone(), 2, value.clone()));
                // A failing pipeline still executes its suffix. The crate must
                // drain every reply before resolving later multiplexed work.
                let mut suffix = value.clone();
                suffix.push(b'!');
                let error = bounded(
                    redis::pipe()
                        .cmd("GET")
                        .arg(&key)
                        .cmd("LPUSH")
                        .arg(&key)
                        .arg("wrong-type")
                        .cmd("SET")
                        .arg(&key)
                        .arg(&suffix)
                        .query_async::<Vec<Value>>(&mut client),
                )
                .await
                .unwrap_err();
                let errors = error.into_server_errors().unwrap();
                assert_eq!(errors.len(), 1);
                assert_eq!(errors[0].0, 1);
                assert_eq!(errors[0].1.code(), "WRONGTYPE");
                let stored: Vec<u8> = bounded(redis::cmd("GET").arg(&key).query_async(&mut client))
                    .await
                    .unwrap();
                assert_eq!(stored, suffix);
                let echo: Vec<u8> =
                    bounded(redis::cmd("PING").arg(&value).query_async(&mut client))
                        .await
                        .unwrap();
                assert_eq!(echo, value);
            }
        });
    }
    bounded(barrier.wait()).await;
    bounded(async {
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    })
    .await;
    let mut default_db = connect(&server, 0).await.unwrap();
    assert_eq!(
        bounded(
            redis::cmd("EXISTS")
                .arg(&[0u8, 255, 0])
                .query_async::<i64>(&mut default_db)
        )
        .await
        .unwrap(),
        0
    );
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canceling_a_client_future_does_not_retract_dispatched_work_or_misroute_replies() {
    let server = TestServer::start().await;
    let mut client = connect(&server, 0).await.unwrap();
    let (cancel, signal) = tokio::sync::oneshot::channel();
    let (canceled, cancellation_observed) = tokio::sync::oneshot::channel();
    let mut commands = redis::pipe();
    commands
        .cmd("SET")
        .arg("entered")
        .arg("yes")
        .cmd("BLPOP")
        .arg("queue")
        .arg(0)
        .cmd("SET")
        .arg("suffix")
        .arg("applied");
    let waiting = async {
        tokio::select! {
            biased;
            _ = signal => (),
            result = commands.query_async::<Vec<Value>>(&mut client) => panic!("completed before release: {result:?}"),
        }
        canceled.send(()).unwrap();
    };
    let producer = async {
        let mut other = connect(&server, 0).await.unwrap();
        wait_for_key(&mut other, "entered").await;
        cancel.send(()).unwrap();
        bounded(cancellation_observed).await.unwrap();
        // The client driver still owns its socket/in-flight replies. Canceling
        // the caller's future is NOT EOF or cancellation of server-side work.
        assert_eq!(
            bounded(
                redis::cmd("RPUSH")
                    .arg("queue")
                    .arg("once")
                    .query_async::<i64>(&mut other)
            )
            .await
            .unwrap(),
            1
        );
    };
    bounded(async { tokio::join!(waiting, producer) }).await;
    let result: (String, String, i64) = bounded(
        redis::pipe()
            .cmd("PING")
            .cmd("GET")
            .arg("suffix")
            .cmd("EXISTS")
            .arg("queue")
            .query_async(&mut client),
    )
    .await
    .unwrap();
    assert_eq!(result, ("PONG".into(), "applied".into(), 0));
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multiplexed_blocking_pipeline_releases_then_routes_null_error_and_later_work() {
    let server = TestServer::start().await;
    let mut client = connect(&server, 0).await.unwrap();
    let blocked = async {
        type Pop = Option<(String, String)>;
        let result: (Pop, String, Pop, String) = bounded(
            redis::pipe()
                .cmd("SET")
                .arg("entered")
                .arg("yes")
                .ignore()
                .cmd("BLPOP")
                .arg("queue")
                .arg(0)
                .cmd("PING")
                .arg("after-pop")
                .cmd("BRPOP")
                .arg("empty")
                .arg("0.001")
                .cmd("SET")
                .arg("after")
                .arg("committed")
                .query_async(&mut client),
        )
        .await
        .unwrap();
        assert_eq!(
            result,
            (
                Some(("queue".into(), "released".into())),
                "after-pop".into(),
                None,
                "OK".into()
            )
        );
        let error = bounded(redis::cmd("UNKNOWN").query_async::<()>(&mut client))
            .await
            .unwrap_err();
        assert_eq!(
            error.kind(),
            ErrorKind::Server(redis::ServerErrorKind::ResponseError)
        );
        assert_eq!(
            bounded(
                redis::cmd("GET")
                    .arg("after")
                    .query_async::<String>(&mut client)
            )
            .await
            .unwrap(),
            "committed"
        );
    };
    let producer = async {
        let mut other = connect(&server, 0).await.unwrap();
        wait_for_key(&mut other, "entered").await;
        assert_eq!(
            bounded(
                redis::cmd("GET")
                    .arg("after")
                    .query_async::<Option<String>>(&mut other)
            )
            .await
            .unwrap(),
            None
        );
        assert_eq!(
            bounded(
                redis::cmd("RPUSH")
                    .arg("queue")
                    .arg("released")
                    .query_async::<i64>(&mut other)
            )
            .await
            .unwrap(),
            1
        );
    };
    bounded(async { tokio::join!(blocked, producer) }).await;
    server.stop().await;
}
