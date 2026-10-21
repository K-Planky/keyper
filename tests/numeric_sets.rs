//! Public numeric/set semantics, RESP2 shapes, and concurrent atomic histories.

mod support;

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

async fn integer(client: &mut MultiplexedConnection, args: &[&[u8]]) -> i64 {
    let Value::Int(value) = command(client, args).await.unwrap() else {
        panic!("expected integer")
    };
    value
}

async fn members(client: &mut MultiplexedConnection, key: &[u8]) -> Vec<Vec<u8>> {
    let Value::Array(reply) = command(client, &[b"SMEMBERS", key]).await.unwrap() else {
        panic!("expected array")
    };
    let mut members: Vec<_> = reply
        .into_iter()
        .map(|value| {
            let Value::BulkString(bytes) = value else {
                panic!("expected bulk member")
            };
            bytes
        })
        .collect();
    members.sort();
    members
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fragmented_numeric_set_pipeline_has_exact_ordered_resp2_shapes() {
    let server = TestServer::start().await;
    let mut client = server.connect().await;
    let mut bytes = Vec::new();
    for args in [
        vec![&b"INCR"[..], b"n"],
        vec![b"SADD", b"s", b"\0\xff"],
        vec![b"sMeMbErS", b"s"],
        vec![b"SCARD", b"s"],
        vec![b"SISMEMBER", b"s", b"\0\xff"],
        vec![b"GET", b"s"],
        vec![b"SREM", b"s", b"\0\xff"],
        vec![b"SMEMBERS", b"s"],
        vec![b"GET", b"n"],
    ] {
        bytes.extend(format!("*{}\r\n", args.len()).as_bytes());
        for arg in args {
            bytes.extend(format!("${}\r\n", arg.len()).as_bytes());
            bytes.extend(arg);
            bytes.extend(b"\r\n");
        }
    }
    bounded(async {
        for byte in bytes {
            client.write_all(&[byte]).await.unwrap();
        }
    })
    .await;
    let expected = b":1\r\n:1\r\n*1\r\n$2\r\n\0\xff\r\n:1\r\n:1\r\n-WRONGTYPE Operation against a key holding the wrong kind of value\r\n:1\r\n*0\r\n$1\r\n1\r\n";
    let mut actual = vec![0; expected.len()];
    bounded(client.read_exact(&mut actual)).await.unwrap();
    assert_eq!(actual, expected);
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exact_integer_boundaries_and_nonmutating_errors_over_resp2() {
    let server = TestServer::start().await;
    let mut client = connect(&server, 0).await;
    expect(&mut client, &[b"iNcR", b""], Value::Int(1)).await;
    expect(
        &mut client,
        &[b"GET", b""],
        Value::BulkString(b"1".to_vec()),
    )
    .await;
    for (before, after) in [
        (i64::MIN, i64::MIN + 1),
        (-1, 0),
        (0, 1),
        (9_007_199_254_740_992, 9_007_199_254_740_993),
        (i64::MAX - 1, i64::MAX),
    ] {
        expect(
            &mut client,
            &[b"SET", b"\0\xff", before.to_string().as_bytes()],
            Value::Okay,
        )
        .await;
        expect(&mut client, &[b"INCR", b"\0\xff"], Value::Int(after)).await;
        expect(
            &mut client,
            &[b"GET", b"\0\xff"],
            Value::BulkString(after.to_string().into_bytes()),
        )
        .await;
    }
    let large = vec![b'9'; keyper::resp::MAX_BULK_BYTES];
    for value in [
        &b""[..],
        b"+1",
        b"01",
        b"-0",
        b"-01",
        b" 1",
        b"1 ",
        b"1.0",
        b"1e0",
        b"1\0",
        b"\xff",
        b"9223372036854775808",
        b"-9223372036854775809",
        b"9223372036854775807",
        &large,
    ] {
        expect(&mut client, &[b"SET", b"bad", value], Value::Okay).await;
        let error = command(&mut client, &[b"INCR", b"bad"]).await.unwrap_err();
        assert_eq!(error.code(), Some("ERR"));
        let expected = if value == b"9223372036854775807" {
            "increment or decrement would overflow"
        } else {
            "value is not an integer or out of range"
        };
        assert_eq!(error.detail(), Some(expected));
        expect(
            &mut client,
            &[b"GET", b"bad"],
            Value::BulkString(value.to_vec()),
        )
        .await;
    }
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn set_members_are_binary_unique_and_empty_sets_disappear_in_sixteen_databases() {
    let server = TestServer::start().await;
    let mut clients = Vec::new();
    for index in 0..16 {
        let mut client = connect(&server, index).await;
        expect(&mut client, &[b"SCARD", b""], Value::Int(0)).await;
        expect(&mut client, &[b"SISMEMBER", b"", b""], Value::Int(0)).await;
        expect(&mut client, &[b"SMEMBERS", b""], Value::Array(vec![])).await;
        expect(&mut client, &[b"SREM", b"", b"absent"], Value::Int(0)).await;
        expect(&mut client, &[b"EXISTS", b""], Value::Int(0)).await;
        expect(
            &mut client,
            &[b"sAdD", b"", b"", b"\0\xff\r\n", &[index], b""],
            Value::Int(3),
        )
        .await;
        expect(&mut client, &[b"INCR", b"counter"], Value::Int(1)).await;
        clients.push(client);
    }
    for (index, client) in clients.iter_mut().enumerate() {
        expect(client, &[b"sCaRd", b""], Value::Int(3)).await;
        expect(client, &[b"sIsMeMbEr", b"", &[index as u8]], Value::Int(1)).await;
        expect(client, &[b"SISMEMBER", b"", b"no"], Value::Int(0)).await;
        let mut expected = vec![vec![], vec![index as u8], b"\0\xff\r\n".to_vec()];
        expected.sort();
        assert_eq!(members(client, b"").await, expected);
        expect(client, &[b"SADD", b"", b"", &[index as u8]], Value::Int(0)).await;
        expect(client, &[b"sReM", b"", b"", b"", b"absent"], Value::Int(1)).await;
        expect(client, &[b"EXISTS", b"", b""], Value::Int(2)).await;
        expect(
            client,
            &[b"SREM", b"", &[index as u8], b"\0\xff\r\n"],
            Value::Int(2),
        )
        .await;
        expect(client, &[b"SCARD", b""], Value::Int(0)).await;
        expect(client, &[b"SMEMBERS", b""], Value::Array(vec![])).await;
        expect(client, &[b"EXISTS", b""], Value::Int(0)).await;
        // An emptied set is absent, not an empty value of the old type.
        expect(client, &[b"RPUSH", b"", b"list"], Value::Int(1)).await;
        expect(
            client,
            &[b"GET", b"counter"],
            Value::BulkString(b"1".to_vec()),
        )
        .await;
    }
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_new_arities_and_type_errors_keep_the_connection_and_values_intact() {
    let server = TestServer::start().await;
    let mut client = connect(&server, 0).await;
    expect(
        &mut client,
        &[b"SADD", b"set", b"one", b"two"],
        Value::Int(2),
    )
    .await;
    for (name, required, variadic) in [
        (&b"INCR"[..], 1, false),
        (b"SADD", 2, true),
        (b"SREM", 2, true),
        (b"SISMEMBER", 2, false),
        (b"SCARD", 1, false),
        (b"SMEMBERS", 1, false),
    ] {
        for count in 0..=required + 1 {
            if count == required || (variadic && count > required) {
                continue;
            }
            let mut args = vec![name];
            args.extend(vec![&b"set"[..]; count]);
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
        vec![&b"GET"[..], b"set"],
        vec![b"INCR", b"set"],
        vec![b"LPUSH", b"set", b"v"],
        vec![b"RPUSH", b"set", b"v"],
        vec![b"BLPOP", b"set", b"0"],
        vec![b"BRPOP", b"set", b"0"],
    ] {
        assert_eq!(
            command(&mut client, &args).await.unwrap_err().code(),
            Some("WRONGTYPE")
        );
    }
    assert_eq!(
        members(&mut client, b"set").await,
        vec![b"one".to_vec(), b"two".to_vec()]
    );
    expect(&mut client, &[b"SET", b"string", b"42"], Value::Okay).await;
    expect(&mut client, &[b"RPUSH", b"list", b"v"], Value::Int(1)).await;
    for key in [&b"string"[..], b"list"] {
        for args in [
            vec![&b"SADD"[..], key, b"v"],
            vec![b"SREM", key, b"v"],
            vec![b"SISMEMBER", key, b"v"],
            vec![b"SCARD", key],
            vec![b"SMEMBERS", key],
        ] {
            assert_eq!(
                command(&mut client, &args).await.unwrap_err().code(),
                Some("WRONGTYPE")
            );
        }
    }
    assert_eq!(
        command(&mut client, &[b"INCR", b"list"])
            .await
            .unwrap_err()
            .code(),
        Some("WRONGTYPE")
    );
    expect(
        &mut client,
        &[b"GET", b"string"],
        Value::BulkString(b"42".to_vec()),
    )
    .await;
    expect(
        &mut client,
        &[b"BLPOP", b"list", b"0"],
        Value::Array(vec![
            Value::BulkString(b"list".to_vec()),
            Value::BulkString(b"v".to_vec()),
        ]),
    )
    .await;
    expect(&mut client, &[b"SET", b"set", b"0"], Value::Okay).await;
    expect(&mut client, &[b"INCR", b"set"], Value::Int(1)).await;
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maximum_variadic_sets_and_large_members_round_trip() {
    let server = TestServer::start().await;
    let mut client = connect(&server, 0).await;
    let values: Vec<_> = (0..keyper::resp::MAX_ARGUMENTS - 2)
        .map(|n| n.to_be_bytes().to_vec())
        .collect();
    let mut args = vec![&b"SADD"[..], b"\0\xff"];
    args.extend(values.iter().map(Vec::as_slice));
    expect(&mut client, &args, Value::Int(values.len() as i64)).await;
    let mut expected = values.clone();
    expected.sort();
    assert_eq!(members(&mut client, b"\0\xff").await, expected);
    args[0] = b"SREM";
    expect(&mut client, &args, Value::Int(values.len() as i64)).await;
    expect(&mut client, &[b"EXISTS", b"\0\xff"], Value::Int(0)).await;
    let large: Vec<_> = (0..keyper::resp::MAX_BULK_BYTES)
        .map(|n| (n % 256) as u8)
        .collect();
    expect(&mut client, &[b"SADD", b"large", &large], Value::Int(1)).await;
    expect(
        &mut client,
        &[b"SISMEMBER", b"large", &large],
        Value::Int(1),
    )
    .await;
    assert_eq!(members(&mut client, b"large").await, vec![large]);
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_increments_allocate_unique_exact_ids_and_respect_realtime_fences() {
    let server = TestServer::start().await;
    let gate = Barrier::new(12);
    let base = 9_007_199_254_740_992i64;
    let mut observer = connect(&server, 0).await;
    expect(
        &mut observer,
        &[b"SET", b"ids", base.to_string().as_bytes()],
        Value::Okay,
    )
    .await;
    let mut tasks = JoinSet::new();
    // Independent connections share a start gate, not a multiplexed socket.
    let gate = std::sync::Arc::new(gate);
    for _ in 0..12 {
        let mut client = connect(&server, 0).await;
        let gate = gate.clone();
        tasks.spawn(async move {
            bounded(gate.wait()).await;
            let mut ids = Vec::new();
            for _ in 0..64 {
                ids.push(integer(&mut client, &[b"INCR", b"ids"]).await);
            }
            assert!(ids.windows(2).all(|w| w[0] < w[1]));
            ids
        });
    }
    let mut ids = Vec::new();
    while let Some(result) = bounded(tasks.join_next()).await {
        ids.extend(result.unwrap());
    }
    ids.sort();
    assert_eq!(ids, (base + 1..=base + 12 * 64).collect::<Vec<_>>());
    // These requests begin only after every earlier response has been observed.
    expect(
        &mut observer,
        &[b"GET", b"ids"],
        Value::BulkString((base + 12 * 64).to_string().into_bytes()),
    )
    .await;
    expect(
        &mut observer,
        &[b"INCR", b"ids"],
        Value::Int(base + 12 * 64 + 1),
    )
    .await;
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overflow_racers_have_exactly_the_remaining_successes() {
    let server = TestServer::start().await;
    let mut observer = connect(&server, 0).await;
    expect(
        &mut observer,
        &[b"SET", b"ids", (i64::MAX - 7).to_string().as_bytes()],
        Value::Okay,
    )
    .await;
    let gate = std::sync::Arc::new(Barrier::new(24));
    let mut tasks = JoinSet::new();
    for _ in 0..24 {
        let mut client = connect(&server, 0).await;
        let gate = gate.clone();
        tasks.spawn(async move {
            bounded(gate.wait()).await;
            command(&mut client, &[b"INCR", b"ids"]).await
        });
    }
    let mut successes = Vec::new();
    let mut errors = 0;
    while let Some(result) = bounded(tasks.join_next()).await {
        match result.unwrap() {
            Ok(Value::Int(value)) => successes.push(value),
            Err(error) => {
                assert_eq!(
                    error.detail(),
                    Some("increment or decrement would overflow")
                );
                errors += 1;
            }
            other => panic!("unexpected result {other:?}"),
        }
    }
    successes.sort();
    assert_eq!(successes, (i64::MAX - 6..=i64::MAX).collect::<Vec<_>>());
    assert_eq!(errors, 17);
    expect(
        &mut observer,
        &[b"GET", b"ids"],
        Value::BulkString(i64::MAX.to_string().into_bytes()),
    )
    .await;
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_variadic_dedup_and_removal_are_whole_operations_not_partial_snapshots() {
    let server = TestServer::start().await;
    let values: Vec<_> = (0..64u32).map(|n| n.to_be_bytes().to_vec()).collect();
    let mut observer = connect(&server, 0).await;
    for name in ["SADD", "SREM"] {
        let gate = std::sync::Arc::new(Barrier::new(13));
        let mut tasks = JoinSet::new();
        for _ in 0..12 {
            let mut client = connect(&server, 0).await;
            let gate = gate.clone();
            let values = values.clone();
            tasks.spawn(async move {
                let mut args = vec![name.as_bytes(), &b"seen"[..]];
                args.extend(values.iter().chain(values.iter()).map(Vec::as_slice));
                bounded(gate.wait()).await;
                integer(&mut client, &args).await
            });
        }
        bounded(gate.wait()).await;
        for _ in 0..24 {
            let snapshot = members(&mut observer, b"seen").await;
            assert!(snapshot.is_empty() || snapshot == values);
            assert!([0, 64].contains(&integer(&mut observer, &[b"SCARD", b"seen"]).await));
        }
        let mut results = Vec::new();
        while let Some(result) = bounded(tasks.join_next()).await {
            results.push(result.unwrap());
        }
        results.sort();
        assert_eq!(results, [vec![0; 11], vec![64]].concat());
        let expected = if name == "SADD" {
            values.clone()
        } else {
            vec![]
        };
        assert_eq!(members(&mut observer, b"seen").await, expected);
        expect(
            &mut observer,
            &[b"EXISTS", b"seen"],
            Value::Int(i64::from(name == "SADD")),
        )
        .await;
    }
    server.stop().await;
}
