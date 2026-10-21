//! Hash wire semantics and concurrent whole-record operations, not EXEC snapshots.

mod support;

use std::collections::BTreeMap;
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

async fn snapshot(client: &mut MultiplexedConnection, key: &[u8]) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let flat = bulks(command(client, &[b"HGETALL", key]).await.unwrap());
    assert_eq!(flat.len() % 2, 0);
    let pairs: BTreeMap<_, _> = flat
        .as_chunks::<2>()
        .0
        .iter()
        .map(|p| (p[0].clone(), p[1].clone()))
        .collect();
    assert_eq!(pairs.len() * 2, flat.len(), "unique fields");
    pairs
}

async fn values(client: &mut MultiplexedConnection, key: &[u8]) -> Vec<Vec<u8>> {
    let mut values = bulks(command(client, &[b"HVALS", key]).await.unwrap());
    values.sort();
    values
}

fn encode(args: &[&[u8]]) -> Vec<u8> {
    let mut bytes = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        bytes.extend(format!("${}\r\n", arg.len()).as_bytes());
        bytes.extend(*arg);
        bytes.extend(b"\r\n");
    }
    bytes
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fragmented_hash_pipeline_has_flat_binary_resp2_replies_and_ordered_errors() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;
    let mut bytes = Vec::new();
    for args in [
        vec![&b"hSeT"[..], b"h", b"\0\xff", b""],
        vec![b"hGeT", b"h", b"\0\xff"],
        vec![b"HGET", b"h", b"missing"],
        vec![b"hExIsTs", b"h", b"\0\xff"],
        vec![b"hLeN", b"h"],
        vec![b"hGeTaLl", b"h"],
        vec![b"hVaLs", b"h"],
        vec![b"HSET", b"h", b"\0\xff", b"bad", b"dangling"],
        vec![b"GET", b"h"],
        vec![b"hDeL", b"h", b"\0\xff", b"\0\xff"],
        vec![b"HGETALL", b"h"],
        vec![b"HVALS", b"h"],
        vec![b"EXISTS", b"h"],
    ] {
        bytes.extend(encode(&args));
    }
    bounded(async {
        for byte in bytes {
            client.write_all(&[byte]).await.unwrap();
        }
    })
    .await;
    let expected = b":1\r\n$0\r\n\r\n$-1\r\n:1\r\n:1\r\n*2\r\n$2\r\n\0\xff\r\n$0\r\n\r\n*1\r\n$0\r\n\r\n-ERR wrong number of arguments for 'hset' command\r\n-WRONGTYPE Operation against a key holding the wrong kind of value\r\n:1\r\n*0\r\n*0\r\n:0\r\n";
    let mut actual = vec![0; expected.len()];
    bounded(client.read_exact(&mut actual)).await.unwrap();
    assert_eq!(actual, expected);
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binary_hashes_duplicates_empty_values_and_deletion_are_isolated_in_all_databases() {
    let server = TestServer::start().await;
    let mut clients = Vec::new();
    for index in 0..16u8 {
        let mut client = connect(&server, index).await;
        expect(&mut client, &[b"HGET", b"", b""], Value::Nil).await;
        expect(&mut client, &[b"HEXISTS", b"", b""], Value::Int(0)).await;
        expect(&mut client, &[b"HLEN", b""], Value::Int(0)).await;
        expect(&mut client, &[b"HGETALL", b""], Value::Array(vec![])).await;
        expect(&mut client, &[b"HVALS", b""], Value::Array(vec![])).await;
        expect(&mut client, &[b"HDEL", b"", b""], Value::Int(0)).await;
        expect(&mut client, &[b"EXISTS", b""], Value::Int(0)).await;
        expect(
            &mut client,
            &[
                b"HSET",
                b"",
                b"",
                b"first",
                b"\0\xff\r\n",
                &[index],
                b"",
                b"",
                b"same",
                b"",
            ],
            Value::Int(3),
        )
        .await;
        clients.push(client);
    }
    for (index, client) in clients.iter_mut().enumerate() {
        let expected = BTreeMap::from([
            (vec![], vec![]),
            (b"\0\xff\r\n".to_vec(), vec![index as u8]),
            (b"same".to_vec(), vec![]),
        ]);
        assert_eq!(snapshot(client, b"").await, expected);
        assert_eq!(
            values(client, b"").await,
            vec![vec![], vec![], vec![index as u8]]
        );
        expect(client, &[b"HGET", b"", b""], Value::BulkString(vec![])).await;
        expect(client, &[b"HGET", b"", b"missing"], Value::Nil).await;
        expect(client, &[b"HEXISTS", b"", b"missing"], Value::Int(0)).await;
        expect(client, &[b"HEXISTS", b"", b""], Value::Int(1)).await;
        expect(client, &[b"HLEN", b""], Value::Int(3)).await;
        expect(
            client,
            &[b"HSET", b"", b"", b"new", b"", b"last"],
            Value::Int(0),
        )
        .await;
        expect(
            client,
            &[b"HGET", b"", b""],
            Value::BulkString(b"last".to_vec()),
        )
        .await;
        expect(client, &[b"EXISTS", b"", b""], Value::Int(2)).await;
        expect(client, &[b"HDEL", b"", b"", b"", b"missing"], Value::Int(1)).await;
        expect(
            client,
            &[b"HDEL", b"", b"same", b"\0\xff\r\n"],
            Value::Int(2),
        )
        .await;
        expect(client, &[b"EXISTS", b""], Value::Int(0)).await;
        expect(client, &[b"HLEN", b""], Value::Int(0)).await;
        expect(client, &[b"HGETALL", b""], Value::Array(vec![])).await;
        expect(client, &[b"HVALS", b""], Value::Array(vec![])).await;
        // Last-field deletion removes the key/type, allowing recreation as a list.
        expect(client, &[b"RPUSH", b"", b"list"], Value::Int(1)).await;
    }
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_hash_arities_and_cross_type_errors_are_nonmutating_and_connections_remain_usable() {
    let server = TestServer::start().await;
    let mut client = connect(&server, 0).await;
    expect(&mut client, &[b"HSET", b"h", b"f", b"v"], Value::Int(1)).await;
    for name in [
        &b"HSET"[..],
        b"HGET",
        b"HDEL",
        b"HEXISTS",
        b"HLEN",
        b"HGETALL",
        b"HVALS",
    ] {
        for count in 0..8 {
            let valid = match name {
                b"HSET" => count >= 3 && count % 2 == 1,
                b"HDEL" => count >= 2,
                b"HGET" | b"HEXISTS" => count == 2,
                _ => count == 1,
            };
            if valid {
                continue;
            }
            let mut args = vec![name, &b"h"[..]];
            args.resize(count + 1, b"f");
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
    for args in [
        vec![&b"GET"[..], b"h"],
        vec![b"INCR", b"h"],
        vec![b"LPUSH", b"h", b"v"],
        vec![b"RPUSH", b"h", b"v"],
        vec![b"BLPOP", b"h", b"0"],
        vec![b"BRPOP", b"h", b"0"],
        vec![b"SADD", b"h", b"m"],
        vec![b"SREM", b"h", b"m"],
        vec![b"SISMEMBER", b"h", b"m"],
        vec![b"SCARD", b"h"],
        vec![b"SMEMBERS", b"h"],
    ] {
        assert_eq!(
            command(&mut client, &args).await.unwrap_err().code(),
            Some("WRONGTYPE")
        );
    }
    assert_eq!(
        snapshot(&mut client, b"h").await,
        BTreeMap::from([(b"f".to_vec(), b"v".to_vec())])
    );
    expect(&mut client, &[b"SET", b"string", b"42"], Value::Okay).await;
    expect(&mut client, &[b"RPUSH", b"list", b"v"], Value::Int(1)).await;
    expect(&mut client, &[b"SADD", b"set", b"m"], Value::Int(1)).await;
    for key in [&b"string"[..], b"list", b"set"] {
        for args in [
            vec![&b"HSET"[..], key, b"f", b"v"],
            vec![b"HGET", key, b"f"],
            vec![b"HDEL", key, b"f"],
            vec![b"HEXISTS", key, b"f"],
            vec![b"HLEN", key],
            vec![b"HGETALL", key],
            vec![b"HVALS", key],
        ] {
            assert_eq!(
                command(&mut client, &args).await.unwrap_err().code(),
                Some("WRONGTYPE")
            );
        }
    }
    expect(
        &mut client,
        &[b"GET", b"string"],
        Value::BulkString(b"42".to_vec()),
    )
    .await;
    expect(
        &mut client,
        &[b"SMEMBERS", b"set"],
        Value::Array(vec![Value::BulkString(b"m".to_vec())]),
    )
    .await;
    // Immediate list priority still stops before a later wrong-type hash.
    expect(
        &mut client,
        &[b"BLPOP", b"list", b"h", b"0"],
        Value::Array(vec![
            Value::BulkString(b"list".to_vec()),
            Value::BulkString(b"v".to_vec()),
        ]),
    )
    .await;
    expect(&mut client, &[b"SET", b"h", b"0"], Value::Okay).await;
    expect(&mut client, &[b"INCR", b"h"], Value::Int(1)).await;
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maximum_hash_pairs_large_binary_fields_values_and_incomplete_input() {
    let server = TestServer::start().await;
    let mut client = connect(&server, 0).await;
    let fields: Vec<_> = (0..(keyper::resp::MAX_ARGUMENTS - 2) / 2)
        .map(|n| n.to_be_bytes().to_vec())
        .collect();
    let mut args = vec![&b"HSET"[..], b"h"];
    for field in &fields {
        args.extend([field.as_slice(), field.as_slice()]);
    }
    expect(&mut client, &args, Value::Int(fields.len() as i64)).await;
    assert_eq!(
        snapshot(&mut client, b"h").await,
        fields.iter().cloned().map(|f| (f.clone(), f)).collect()
    );
    let mut args = vec![&b"HDEL"[..], b"h"];
    args.extend(fields.iter().map(Vec::as_slice));
    expect(&mut client, &args, Value::Int(fields.len() as i64)).await;
    let large: Vec<_> = (0..keyper::resp::MAX_BULK_BYTES)
        .map(|n| (n % 256) as u8)
        .collect();
    // Separate requests stay within the encoded 16 MiB request limit.
    expect(
        &mut client,
        &[b"HSET", b"large", &large, b"v"],
        Value::Int(1),
    )
    .await;
    expect(
        &mut client,
        &[b"HSET", b"large", b"small", &large],
        Value::Int(1),
    )
    .await;
    expect(
        &mut client,
        &[b"HGET", b"large", &large],
        Value::BulkString(b"v".to_vec()),
    )
    .await;
    expect(
        &mut client,
        &[b"HGET", b"large", b"small"],
        Value::BulkString(large.clone()),
    )
    .await;
    assert_eq!(
        snapshot(&mut client, b"large").await,
        BTreeMap::from([(large.clone(), b"v".to_vec()), (b"small".to_vec(), large)])
    );
    let mut raw = server.connect().await;
    let mut frame = encode(&[b"HSET", b"h", b"f", b"would-write", b"g", b"incomplete"]);
    frame.truncate(frame.len() - 3);
    bounded(raw.write_all(&frame)).await.unwrap();
    bounded(raw.shutdown()).await.unwrap();
    let mut end = Vec::new();
    bounded(raw.read_to_end(&mut end)).await.unwrap();
    assert!(end.is_empty());
    expect(&mut client, &[b"EXISTS", b"h"], Value::Int(0)).await;
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_hash_creation_and_deletion_count_only_whole_unique_changes() {
    let server = TestServer::start().await;
    let fields: Vec<_> = (0..64u32).map(|n| n.to_be_bytes().to_vec()).collect();
    let expected: BTreeMap<_, _> = fields.iter().cloned().map(|f| (f.clone(), f)).collect();
    let mut observer = connect(&server, 0).await;
    for name in ["HSET", "HDEL"] {
        let gate = Arc::new(Barrier::new(13));
        let mut tasks = JoinSet::new();
        for _ in 0..12 {
            let mut client = connect(&server, 0).await;
            let gate = gate.clone();
            let fields = fields.clone();
            tasks.spawn(async move {
                let mut args = vec![name.as_bytes(), &b"h"[..]];
                for field in fields.iter().chain(fields.iter()) {
                    args.push(field);
                    if name == "HSET" {
                        args.push(field);
                    }
                }
                bounded(gate.wait()).await;
                command(&mut client, &args).await.unwrap()
            });
        }
        bounded(gate.wait()).await;
        for _ in 0..24 {
            let actual = snapshot(&mut observer, b"h").await;
            assert!(actual.is_empty() || actual == expected);
            let actual = values(&mut observer, b"h").await;
            assert!(actual.is_empty() || actual == fields);
            assert!(
                [Value::Int(0), Value::Int(64)]
                    .contains(&command(&mut observer, &[b"HLEN", b"h"]).await.unwrap())
            );
        }
        let mut counts = Vec::new();
        while let Some(result) = bounded(tasks.join_next()).await {
            let Value::Int(n) = result.unwrap() else {
                panic!("expected integer")
            };
            counts.push(n);
        }
        counts.sort();
        assert_eq!(counts, [vec![0; 11], vec![64]].concat());
        assert_eq!(
            snapshot(&mut observer, b"h").await,
            if name == "HSET" {
                expected.clone()
            } else {
                BTreeMap::new()
            }
        );
        expect(
            &mut observer,
            &[b"EXISTS", b"h"],
            Value::Int(i64::from(name == "HSET")),
        )
        .await;
    }
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_record_replacements_never_expose_mixed_fields_within_one_read() {
    let server = TestServer::start().await;
    let mut observer = connect(&server, 0).await;
    let fields: Vec<_> = (0..64u32).map(|n| n.to_be_bytes().to_vec()).collect();
    let mut initialize = vec![&b"HSET"[..], b"h"];
    for field in &fields {
        initialize.extend([field.as_slice(), b"initial"]);
    }
    expect(&mut observer, &initialize, Value::Int(64)).await;
    let gate = Arc::new(Barrier::new(5));
    let mut tasks = JoinSet::new();
    for writer in 0..4 {
        let mut client = connect(&server, 0).await;
        let fields = fields.clone();
        let gate = gate.clone();
        tasks.spawn(async move {
            bounded(gate.wait()).await;
            for sequence in 0..32 {
                let value = format!("{writer}:{sequence}").into_bytes();
                let mut args = vec![&b"HSET"[..], b"h"];
                for field in &fields {
                    args.extend([field.as_slice(), &value]);
                }
                expect(&mut client, &args, Value::Int(0)).await;
                // A read on this connection cannot precede its completed write,
                // but may observe any subsequent whole writer's replacement.
                let actual = snapshot(&mut client, b"h").await;
                assert_eq!(actual.len(), 64);
                assert!(
                    actual
                        .values()
                        .all(|v| v == actual.first_key_value().unwrap().1)
                );
                assert!(actual.values().all(|v| v != b"initial"));
            }
        });
    }
    bounded(gate.wait()).await;
    for _ in 0..64 {
        let actual = values(&mut observer, b"h").await;
        assert_eq!(actual.len(), 64);
        assert!(actual.iter().all(|v| v == &actual[0]));
    }
    while let Some(result) = bounded(tasks.join_next()).await {
        result.unwrap();
    }
    let final_record = snapshot(&mut observer, b"h").await;
    assert_eq!(final_record.len(), 64);
    assert!(
        final_record
            .values()
            .all(|v| v == final_record.first_key_value().unwrap().1)
    );
    assert!(final_record.values().all(|v| v.ends_with(b":31")));
    server.stop().await;
}
