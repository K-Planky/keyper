//! Audited list/key wire behavior and whole-command concurrency (not EXEC/EVAL).

mod support;

use std::sync::Arc;

use redis::{Value, aio::MultiplexedConnection};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Barrier;
use tokio::task::JoinSet;

use support::{TestServer, bounded};

async fn connect(server: &TestServer, db: u8) -> MultiplexedConnection {
    let client = redis::Client::open(format!("redis://{}/{db}", server.address)).unwrap();
    bounded(client.get_multiplexed_async_connection())
        .await
        .unwrap()
}

async fn command(client: &mut MultiplexedConnection, args: &[&[u8]]) -> redis::RedisResult<Value> {
    let mut command = redis::cmd(std::str::from_utf8(args[0]).unwrap());
    for arg in &args[1..] {
        command.arg(*arg);
    }
    bounded(command.query_async(client)).await
}

async fn expect(client: &mut MultiplexedConnection, args: &[&[u8]], expected: Value) {
    assert_eq!(command(client, args).await.unwrap(), expected, "{args:?}");
}

fn array(values: &[&[u8]]) -> Value {
    Value::Array(
        values
            .iter()
            .map(|v| Value::BulkString(v.to_vec()))
            .collect(),
    )
}

fn bulks(reply: Value) -> Vec<Vec<u8>> {
    let Value::Array(values) = reply else {
        panic!("expected array")
    };
    values
        .into_iter()
        .map(|v| {
            let Value::BulkString(bytes) = v else {
                panic!("expected bulk string")
            };
            bytes
        })
        .collect()
}

fn encode(args: &[&[u8]]) -> Vec<u8> {
    let mut wire = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        wire.extend(format!("${}\r\n", arg.len()).as_bytes());
        wire.extend(*arg);
        wire.extend(b"\r\n");
    }
    wire
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fragmented_pipeline_distinguishes_status_bulk_null_array_and_empty_replies() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;
    let mut wire = Vec::new();
    for args in [
        vec![b"tYpE".as_slice(), b"\0\xff"],
        vec![b"lLeN", b"\0\xff"],
        vec![b"lInDeX", b"\0\xff", b"0"],
        vec![b"lPoP", b"\0\xff"],
        vec![b"LPOP", b"\0\xff", b"0"],
        vec![b"RPUSH", b"\0\xff", b"", b"\0\xff\r\n", b"tail"],
        vec![b"TYPE", b"\0\xff"],
        vec![b"LLEN", b"\0\xff"],
        vec![b"LINDEX", b"\0\xff", b"-2"],
        vec![b"LPOP", b"\0\xff", b"0"],
        vec![b"LPOP", b"\0\xff"],
        vec![b"LPOP", b"\0\xff", b"1"],
        vec![b"LPOP", b"\0\xff", b"100"],
        vec![b"LPOP", b"\0\xff", b"1"],
        vec![b"TYPE", b"\0\xff"],
    ] {
        wire.extend(encode(&args));
    }
    bounded(async {
        for byte in wire {
            client.write_all(&[byte]).await.unwrap();
        }
    })
    .await;
    let expected = b"+none\r\n:0\r\n$-1\r\n$-1\r\n*-1\r\n:3\r\n+list\r\n:3\r\n$4\r\n\0\xff\r\n\r\n*0\r\n$0\r\n\r\n*1\r\n$4\r\n\0\xff\r\n\r\n*1\r\n$4\r\ntail\r\n*-1\r\n+none\r\n";
    let mut actual = vec![0; expected.len()];
    bounded(client.read_exact(&mut actual)).await.unwrap();
    assert_eq!(actual, expected);
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn indices_pop_forms_type_transitions_and_binary_keys_are_isolated_in_all_databases() {
    let server = TestServer::start().await;
    let mut clients = Vec::new();
    for n in 0..16u8 {
        let mut client = connect(&server, n).await;
        expect(
            &mut client,
            &[b"TYPE", b""],
            Value::SimpleString("none".into()),
        )
        .await;
        expect(&mut client, &[b"LLEN", b""], Value::Int(0)).await;
        expect(
            &mut client,
            &[b"RPUSH", b"", b"", &[n], b"\0\xff\r\n"],
            Value::Int(3),
        )
        .await;
        clients.push(client);
    }
    for (n, client) in clients.iter_mut().enumerate() {
        for (index, value) in [
            (b"0".as_slice(), b"".as_slice()),
            (b"1", &[n as u8]),
            (b"2", b"\0\xff\r\n"),
            (b"-1", b"\0\xff\r\n"),
            (b"-2", &[n as u8]),
            (b"-3", b""),
        ] {
            expect(
                client,
                &[b"LINDEX", b"", index],
                Value::BulkString(value.to_vec()),
            )
            .await;
        }
        for index in [
            b"3".as_slice(),
            b"-4",
            b"9223372036854775807",
            b"-9223372036854775808",
        ] {
            expect(client, &[b"LINDEX", b"", index], Value::Nil).await;
        }
        expect(client, &[b"TYPE", b""], Value::SimpleString("list".into())).await;
        expect(client, &[b"LPOP", b"", b"0"], array(&[])).await;
        expect(client, &[b"LLEN", b""], Value::Int(3)).await;
        expect(client, &[b"LPOP", b""], Value::BulkString(vec![])).await;
        expect(client, &[b"LPOP", b"", b"1"], array(&[&[n as u8]])).await;
        expect(
            client,
            &[b"LPOP", b"", b"9223372036854775807"],
            array(&[b"\0\xff\r\n"]),
        )
        .await;
        expect(client, &[b"TYPE", b""], Value::SimpleString("none".into())).await;
        expect(client, &[b"EXISTS", b"", b""], Value::Int(0)).await;
        expect(client, &[b"LPOP", b""], Value::Nil).await;
        expect(client, &[b"LPOP", b"", b"0"], Value::Nil).await;
        expect(client, &[b"SET", b"", b"0"], Value::Okay).await;
        expect(
            client,
            &[b"TYPE", b""],
            Value::SimpleString("string".into()),
        )
        .await;
        expect(client, &[b"INCR", b""], Value::Int(1)).await;
        expect(client, &[b"SADD", b"set", b"m"], Value::Int(1)).await;
        expect(
            client,
            &[b"TYPE", b"set"],
            Value::SimpleString("set".into()),
        )
        .await;
        expect(client, &[b"SREM", b"set", b"m"], Value::Int(1)).await;
        expect(
            client,
            &[b"TYPE", b"set"],
            Value::SimpleString("none".into()),
        )
        .await;
        expect(client, &[b"HSET", b"hash", b"f", b"v"], Value::Int(1)).await;
        expect(
            client,
            &[b"TYPE", b"hash"],
            Value::SimpleString("hash".into()),
        )
        .await;
        expect(client, &[b"HDEL", b"hash", b"f"], Value::Int(1)).await;
        expect(
            client,
            &[b"TYPE", b"hash"],
            Value::SimpleString("none".into()),
        )
        .await;
        expect(client, &[b"RPUSH", b"hash", b"v"], Value::Int(1)).await;
        expect(client, &[b"BRPOP", b"hash", b"0"], array(&[b"hash", b"v"])).await;
        expect(
            client,
            &[b"TYPE", b"hash"],
            Value::SimpleString("none".into()),
        )
        .await;
    }
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn arity_integer_and_type_errors_follow_redis_precedence_without_mutation() {
    let server = TestServer::start().await;
    let mut client = connect(&server, 0).await;
    expect(&mut client, &[b"RPUSH", b"list", b"v"], Value::Int(1)).await;
    expect(&mut client, &[b"SET", b"string", b"42"], Value::Okay).await;
    expect(&mut client, &[b"SADD", b"set", b"m"], Value::Int(1)).await;
    expect(&mut client, &[b"HSET", b"hash", b"f", b"v"], Value::Int(1)).await;
    for name in [b"TYPE".as_slice(), b"LLEN", b"LINDEX", b"LPOP"] {
        for count in 0..6 {
            let valid = match name {
                b"LINDEX" => count == 2,
                b"LPOP" => (1..=2).contains(&count),
                _ => count == 1,
            };
            if valid {
                continue;
            }
            let mut args = vec![name];
            args.resize(count + 1, b"list");
            let error = command(&mut client, &args).await.unwrap_err();
            assert_eq!(error.code(), Some("ERR"));
            assert_eq!(
                error.detail(),
                Some(
                    format!(
                        "wrong number of arguments for '{}' command",
                        std::str::from_utf8(name).unwrap().to_lowercase()
                    )
                    .as_str()
                )
            );
        }
    }
    for key in [b"string".as_slice(), b"set", b"hash"] {
        for args in [
            vec![b"LLEN".as_slice(), key],
            vec![b"LINDEX", key, b"0"],
            vec![b"LINDEX", key, b"invalid"],
            vec![b"LPOP", key],
            vec![b"LPOP", key, b"0"],
        ] {
            assert_eq!(
                command(&mut client, &args).await.unwrap_err().code(),
                Some("WRONGTYPE")
            );
        }
    }
    for text in [
        b"".as_slice(),
        b"-0",
        b"+1",
        b"01",
        b"-01",
        b" 1",
        b"1 ",
        b"1.0",
        b"1e0",
        b"1\0",
        b"\xff",
        b"9223372036854775808",
        b"-9223372036854775809",
        &[b'9'; 100],
    ] {
        expect(&mut client, &[b"LINDEX", b"missing", text], Value::Nil).await;
        let error = command(&mut client, &[b"LINDEX", b"list", text])
            .await
            .unwrap_err();
        assert_eq!(
            error.detail(),
            Some("value is not an integer or out of range")
        );
        for key in [b"missing".as_slice(), b"string", b"set", b"hash", b"list"] {
            let error = command(&mut client, &[b"LPOP", key, text])
                .await
                .unwrap_err();
            assert_eq!(error.code(), Some("ERR"));
            assert_eq!(
                error.detail(),
                Some("value is out of range, must be positive")
            );
        }
    }
    for text in [b"-1".as_slice(), b"-9223372036854775808"] {
        assert!(
            command(&mut client, &[b"LPOP", b"list", text])
                .await
                .is_err()
        );
    }
    expect(&mut client, &[b"LLEN", b"list"], Value::Int(1)).await;
    expect(
        &mut client,
        &[b"LPOP", b"list"],
        Value::BulkString(b"v".to_vec()),
    )
    .await;
    expect(
        &mut client,
        &[b"GET", b"string"],
        Value::BulkString(b"42".to_vec()),
    )
    .await;
    expect(&mut client, &[b"SMEMBERS", b"set"], array(&[b"m"])).await;
    expect(&mut client, &[b"HGETALL", b"hash"], array(&[b"f", b"v"])).await;
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_binary_reads_and_counted_pops_work_and_incomplete_pops_do_not_execute() {
    let server = TestServer::start().await;
    let mut client = connect(&server, 0).await;
    let large: Vec<_> = (0..keyper::resp::MAX_BULK_BYTES)
        .map(|n| (n % 256) as u8)
        .collect();
    expect(
        &mut client,
        &[b"RPUSH", b"list", &large, b"tail"],
        Value::Int(2),
    )
    .await;
    expect(
        &mut client,
        &[b"LINDEX", b"list", b"0"],
        Value::BulkString(large.clone()),
    )
    .await;
    let mut raw = server.connect().await;
    let mut wire = encode(&[b"LPOP", b"list", b"2"]);
    wire.pop();
    bounded(raw.write_all(&wire)).await.unwrap();
    bounded(raw.shutdown()).await.unwrap();
    let mut end = Vec::new();
    bounded(raw.read_to_end(&mut end)).await.unwrap();
    assert!(end.is_empty());
    expect(&mut client, &[b"LLEN", b"list"], Value::Int(2)).await;
    expect(
        &mut client,
        &[b"LPOP", b"list", b"9223372036854775807"],
        array(&[&large, b"tail"]),
    )
    .await;
    expect(
        &mut client,
        &[b"TYPE", b"list"],
        Value::SimpleString("none".into()),
    )
    .await;
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn counted_pops_are_atomic_and_concurrent_ranges_are_disjoint() {
    let server = TestServer::start().await;
    let mut observer = connect(&server, 0).await;
    let values: Vec<_> = (0..768u32).map(|n| n.to_be_bytes().to_vec()).collect();
    let mut args = vec![b"RPUSH".as_slice(), b"list"];
    args.extend(values.iter().map(Vec::as_slice));
    expect(&mut observer, &args, Value::Int(768)).await;
    let gate = Arc::new(Barrier::new(13));
    let mut tasks = JoinSet::new();
    for _ in 0..12 {
        let mut client = connect(&server, 0).await;
        let gate = gate.clone();
        tasks.spawn(async move {
            bounded(gate.wait()).await;
            let values = bulks(
                command(&mut client, &[b"LPOP", b"list", b"64"])
                    .await
                    .unwrap(),
            );
            let Value::Int(length) = command(&mut client, &[b"LLEN", b"list"]).await.unwrap()
            else {
                panic!("integer")
            };
            // This read follows its own completed pop, but may follow others too.
            assert!(length <= 704 && length % 64 == 0);
            values
        });
    }
    bounded(gate.wait()).await;
    for _ in 0..32 {
        let Value::Int(length) = command(&mut observer, &[b"LLEN", b"list"]).await.unwrap() else {
            panic!("integer")
        };
        assert!((0..=768).contains(&length) && length % 64 == 0);
        match command(&mut observer, &[b"LINDEX", b"list", b"0"])
            .await
            .unwrap()
        {
            Value::BulkString(bytes) => {
                assert_eq!(u32::from_be_bytes(bytes.try_into().unwrap()) % 64, 0)
            }
            Value::Nil => {}
            reply => panic!("unexpected head reply: {reply:?}"),
        }
    }
    let mut ranges = Vec::new();
    while let Some(result) = bounded(tasks.join_next()).await {
        let range = result.unwrap();
        assert_eq!(range.len(), 64);
        ranges.push(range);
    }
    ranges.sort();
    assert_eq!(ranges.concat(), values);
    expect(&mut observer, &[b"EXISTS", b"list"], Value::Int(0)).await;
    expect(
        &mut observer,
        &[b"TYPE", b"list"],
        Value::SimpleString("none".into()),
    )
    .await;
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_pushes_scalar_counted_and_blocking_pops_preserve_exact_item_accounting() {
    let server = TestServer::start().await;
    let mut observer = connect(&server, 0).await;
    let values: Vec<_> = (0..512u32).map(|n| n.to_be_bytes().to_vec()).collect();
    let gate = Arc::new(Barrier::new(9));
    let mut tasks = JoinSet::new();
    for writer in 0..4 {
        let mut client = connect(&server, 0).await;
        let gate = gate.clone();
        let values = values[writer * 128..(writer + 1) * 128].to_vec();
        tasks.spawn(async move {
            bounded(gate.wait()).await;
            for batch in values.chunks(16) {
                let mut args = vec![b"RPUSH".as_slice(), b"list"];
                args.extend(batch.iter().map(Vec::as_slice));
                let Value::Int(length) = command(&mut client, &args).await.unwrap() else {
                    panic!("integer")
                };
                assert!(length >= 16);
            }
            Vec::new()
        });
    }
    for reader in 0..4 {
        let mut client = connect(&server, 0).await;
        let gate = gate.clone();
        tasks.spawn(async move {
            bounded(gate.wait()).await;
            let mut consumed = Vec::new();
            for _ in 0..32 {
                let args: Vec<&[u8]> = match reader {
                    0 => vec![b"LPOP", b"list"],
                    1 => vec![b"LPOP", b"list", b"7"],
                    2 => vec![b"BLPOP", b"list", b"0.001"],
                    _ => vec![b"BRPOP", b"list", b"0.001"],
                };
                match command(&mut client, &args).await.unwrap() {
                    Value::Nil => {}
                    Value::BulkString(v) => consumed.push(v),
                    reply => {
                        let mut values = bulks(reply);
                        if reader >= 2 {
                            assert_eq!(values.remove(0), b"list");
                        }
                        consumed.extend(values);
                    }
                }
            }
            consumed
        });
    }
    bounded(gate.wait()).await;
    let mut consumed = Vec::new();
    while let Some(result) = bounded(tasks.join_next()).await {
        consumed.extend(result.unwrap());
    }
    match command(&mut observer, &[b"LPOP", b"list", b"9223372036854775807"])
        .await
        .unwrap()
    {
        reply @ Value::Array(_) => consumed.extend(bulks(reply)),
        Value::Nil => {}
        reply => panic!("unexpected drain reply: {reply:?}"),
    }
    consumed.sort();
    assert_eq!(consumed, values);
    expect(&mut observer, &[b"LLEN", b"list"], Value::Int(0)).await;
    // No completed/timed-out blocking request may consume a later item.
    expect(&mut observer, &[b"RPUSH", b"list", b"later"], Value::Int(1)).await;
    expect(
        &mut observer,
        &[b"LPOP", b"list"],
        Value::BulkString(b"later".to_vec()),
    )
    .await;
    server.stop().await;
}
