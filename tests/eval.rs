//! Genuine owner-side EVAL: wire phases, command contexts, isolation and errors.

mod support;

use std::sync::Arc;

use keyper::resp::Reply;
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

    async fn eval(&mut self, source: &[u8]) -> Reply {
        self.command(&[b"EVAL", source, b"0"]).await
    }
}

fn bulk(bytes: &[u8]) -> Reply {
    Reply::Bulk(Some(bytes.to_vec()))
}

fn simple(bytes: &[u8]) -> Reply {
    Reply::Simple(bytes.to_vec())
}

fn error(bytes: &[u8]) -> Reply {
    Reply::Error(bytes.to_vec())
}

fn array(replies: Vec<Reply>) -> Reply {
    Reply::Array(Some(replies))
}

fn lua_error(reply: Reply, contains: &[u8]) {
    let Reply::Error(message) = reply else {
        panic!("expected Lua error, got {reply:?}");
    };
    assert!(
        message.windows(contains.len()).any(|s| s == contains),
        "{message:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eval_admission_and_numkeys_are_distinct_from_execution_errors() {
    let server = TestServer::start().await;
    let mut client = Client::connect(&server).await;
    for args in [vec![b"EVAL".as_slice()], vec![b"EVAL", b"return 1"]] {
        client
            .expect(
                &args,
                error(b"ERR wrong number of arguments for 'eval' command"),
            )
            .await;
    }
    let source = b"redis.call('SET','never','written'); return 1";
    for (count, expected) in [
        (
            b"-1".as_slice(),
            b"ERR Number of keys can't be negative".as_slice(),
        ),
        (
            b"1",
            b"ERR Number of keys can't be greater than number of args",
        ),
        (
            b"9223372036854775807",
            b"ERR Number of keys can't be greater than number of args",
        ),
        (
            b"9223372036854775808",
            b"ERR value is not an integer or out of range",
        ),
        (
            b"-9223372036854775808",
            b"ERR Number of keys can't be negative",
        ),
        (b"00", b"ERR value is not an integer or out of range"),
        (b"-0", b"ERR value is not an integer or out of range"),
        (b"+0", b"ERR value is not an integer or out of range"),
        (b"1.0", b"ERR value is not an integer or out of range"),
        (b" 0", b"ERR value is not an integer or out of range"),
        (b"0\0", b"ERR value is not an integer or out of range"),
        (b"\xff", b"ERR value is not an integer or out of range"),
    ] {
        client
            .expect(&[b"EVAL", source, count], error(expected))
            .await;
        // numkeys validation precedes compilation, even for invalid source.
        client
            .expect(&[b"EVAL", b"!invalid", count], error(expected))
            .await;
    }
    client.expect(&[b"GET", b"never"], Reply::Bulk(None)).await;
    assert_eq!(client.eval(b"").await, Reply::Bulk(None));
    assert_eq!(
        client.eval(b"return {#KEYS,#ARGV}").await,
        array(vec![Reply::Integer(0), Reply::Integer(0)])
    );
    // EVAL also consumes the project's first-command SELECT window.
    client
        .expect(
            &[b"SELECT", b"1"],
            error(b"ERR SELECT is only allowed as the first command"),
        )
        .await;
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keys_args_derived_keys_binary_values_and_fresh_environments_in_every_database() {
    let server = TestServer::start().await;
    for db in 0..16 {
        let mut client = Client::connect(&server).await;
        client
            .expect(&[b"SELECT", db.to_string().as_bytes()], simple(b"OK"))
            .await;
        let source = b"local k=KEYS[1]..ARGV[1]; local before=redis.call('GET',k); redis.call('SET',k,ARGV[2]); return {before,redis.call('GET',k),KEYS,ARGV}";
        let key = b"\0\xff\r\n";
        let suffix = b"\0derived";
        let value = db.to_string();
        client
            .expect(
                &[b"eVaL", source, b"1", key, suffix, value.as_bytes()],
                array(vec![
                    Reply::Bulk(None),
                    bulk(value.as_bytes()),
                    array(vec![bulk(key)]),
                    array(vec![bulk(suffix), bulk(value.as_bytes())]),
                ]),
            )
            .await;
        // Seven KEYS is valid: only blocking pops have a five-key limit.
        client
            .expect(
                &[
                    b"EVAL",
                    b"return {#KEYS,#ARGV,KEYS[7],ARGV[1]}",
                    b"7",
                    b"a",
                    b"b",
                    b"c",
                    b"d",
                    b"e",
                    b"f",
                    b"",
                    b"\0\xff",
                ],
                array(vec![
                    Reply::Integer(7),
                    Reply::Integer(1),
                    bulk(b""),
                    bulk(b"\0\xff"),
                ]),
            )
            .await;
        let mutate =
            b"KEYS[1]='stale'; ARGV[1]='stale'; table.insert=nil; redis.call=nil; return true";
        assert_eq!(client.eval(mutate).await, Reply::Integer(1));
        assert_eq!(
            client
                .eval(b"local t={}; table.insert(t,redis.call('PING').ok); return {#KEYS,#ARGV,t}")
                .await,
            array(vec![
                Reply::Integer(0),
                Reply::Integer(0),
                array(vec![bulk(b"PONG")])
            ])
        );
        lua_error(client.eval(b"unwanted_global=1").await, b"modify a global");
        lua_error(
            client.eval(b"return os.execute('false')").await,
            b"nonexistent global",
        );
    }
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supported_commands_have_real_database_semantics_and_resp2_conversions() {
    let server = TestServer::start().await;
    let mut client = Client::connect(&server).await;
    let source = br#"
        local out={}
        local function call(...) table.insert(out,redis.call(...)) end
        call('PING'); call('PING',string.char(0,255,13,10))
        call('GET','absent'); call('SET','s','9007199254740992')
        redis.call('INCR','s'); call('GET','s')
        call('EXISTS','s','absent','s'); call('TYPE','s')
        call('SADD','set','m','m'); call('SISMEMBER','set','m')
        call('SCARD','set'); call('SMEMBERS','set'); call('SREM','set','m')
        call('HSET','h','f','v','f','last'); call('HGET','h','f')
        call('HEXISTS','h','f'); call('HLEN','h'); call('HGETALL','h')
        call('HVALS','h'); call('HDEL','h','f')
        call('LPUSH','l','a','b'); call('RPUSH','l','c'); call('LLEN','l')
        call('LINDEX','l',-1); call('LPOP','l',1)
        call('BLPOP','l',0); call('BRPOP','l',0)
        call('BLPOP','l',0); call('BRPOP','l',1000)
        table.insert(out,{redis.call('TYPE','l'),redis.status_reply('READY'),false,true,{1,nil,2}})
        return out
    "#;
    assert_eq!(
        client.eval(source).await,
        array(vec![
            simple(b"PONG"),
            bulk(b"\0\xff\r\n"),
            Reply::Bulk(None),
            simple(b"OK"),
            bulk(b"9007199254740993"),
            Reply::Integer(2),
            simple(b"string"),
            Reply::Integer(1),
            Reply::Integer(1),
            Reply::Integer(1),
            array(vec![bulk(b"m")]),
            Reply::Integer(1),
            Reply::Integer(1),
            bulk(b"last"),
            Reply::Integer(1),
            Reply::Integer(1),
            array(vec![bulk(b"f"), bulk(b"last")]),
            array(vec![bulk(b"last")]),
            Reply::Integer(1),
            Reply::Integer(2),
            Reply::Integer(3),
            Reply::Integer(3),
            bulk(b"c"),
            array(vec![bulk(b"b")]),
            array(vec![bulk(b"l"), bulk(b"a")]),
            array(vec![bulk(b"l"), bulk(b"c")]),
            Reply::Bulk(None),
            Reply::Bulk(None),
            array(vec![
                simple(b"none"),
                simple(b"READY"),
                Reply::Bulk(None),
                Reply::Integer(1),
                array(vec![Reply::Integer(1)])
            ]),
        ])
    );
    client
        .expect(&[b"SET", b"s", b"9223372036854775807"], simple(b"OK"))
        .await;
    assert_eq!(
        client
            .eval(b"return {redis.pcall('INCR','s'),redis.call('GET','s')}")
            .await,
        array(vec![
            error(b"ERR increment or decrement would overflow"),
            bulk(b"9223372036854775807")
        ])
    );
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn script_context_errors_preserve_call_pcall_and_project_restrictions() {
    let server = TestServer::start().await;
    let mut client = Client::connect(&server).await;
    for (call, expected) in [
        (
            "'GET'",
            "ERR Wrong number of args calling Redis command from script",
        ),
        (
            "'EVAL'",
            "ERR Wrong number of args calling Redis command from script",
        ),
        (
            "'MULTI','extra'",
            "ERR Wrong number of args calling Redis command from script",
        ),
        (
            "'GET','a','b'",
            "ERR Wrong number of args calling Redis command from script",
        ),
        ("'UNKNOWN'", "ERR Unknown Redis command called from script"),
        (
            "'EVAL','return 1',0",
            "ERR This Redis command is not allowed from script",
        ),
        (
            "'MULTI'",
            "ERR This Redis command is not allowed from script",
        ),
        (
            "'EXEC'",
            "ERR This Redis command is not allowed from script",
        ),
        (
            "'DISCARD'",
            "ERR This Redis command is not allowed from script",
        ),
        (
            "'SELECT',1",
            "ERR SELECT is only allowed as the first command",
        ),
        (
            "'SET','k','v','NX'",
            "ERR Wrong number of args calling Redis command from script",
        ),
        (
            "'BLPOP','a','b','c','d','e','f',0",
            "ERR blocking pop supports at most 5 keys",
        ),
        (
            "'PING',1,2",
            "ERR wrong number of arguments for 'ping' command",
        ),
        (
            "'HSET','h','f','v','unpaired'",
            "ERR wrong number of arguments for 'hset' command",
        ),
        (
            "'LPOP','l',1,2",
            "ERR wrong number of arguments for 'lpop' command",
        ),
        (
            "'BLPOP','l','invalid'",
            "ERR timeout is not a float or out of range",
        ),
        ("'BRPOP','l',-1", "ERR timeout is negative"),
        (
            "",
            "ERR Please specify at least one argument for this redis lib call",
        ),
        (
            "'SET','k',false",
            "ERR Lua redis lib command arguments must be strings or integers",
        ),
    ] {
        assert_eq!(
            client
                .eval(format!("return redis.pcall({call})").as_bytes())
                .await,
            error(expected.as_bytes()),
            "{call}"
        );
        assert_eq!(
            client
                .eval(format!("return redis.call({call})").as_bytes())
                .await,
            error(expected.as_bytes()),
            "{call}"
        );
        assert_eq!(
            client
                .eval(
                    format!("local ok,e=pcall(function() redis.call({call}) end); return {{ok,e}}")
                        .as_bytes()
                )
                .await,
            array(vec![Reply::Bulk(None), bulk(expected.as_bytes())]),
            "{call}"
        );
    }
    assert_eq!(
        client
            .eval(b"return redis.error_reply('BUSY controlled')")
            .await,
        error(b"BUSY controlled")
    );
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn syntax_runtime_command_and_return_conversion_errors_have_no_automatic_rollback() {
    let server = TestServer::start().await;
    let mut client = Client::connect(&server).await;
    lua_error(
        client.eval(b"redis.call('SET','key','not-run'); !").await,
        b"ERR Lua script",
    );
    client.expect(&[b"GET", b"key"], Reply::Bulk(None)).await;
    for tail in [
        "error('intentional')",
        "redis.call('LPUSH','key','bad')",
        "return redis.error_reply('ERR controlled')",
        "local t={}; t[1]=t; return t",
    ] {
        client
            .expect(&[b"SET", b"key", b"before"], simple(b"OK"))
            .await;
        // A return must be last in its block in Lua 5.1.
        let source = format!(
            "redis.call('SET','key','retained'); do {tail} end; redis.call('SET','after','never')"
        );
        assert!(
            matches!(client.eval(source.as_bytes()).await, Reply::Error(_)),
            "{tail}"
        );
        client.expect(&[b"GET", b"key"], bulk(b"retained")).await;
        client.expect(&[b"GET", b"after"], Reply::Bulk(None)).await;
    }
    let source = br#"
        redis.call('SET','key','retained')
        local e=redis.pcall('LPUSH','key','bad')
        redis.call('SET','after','continued')
        return e
    "#;
    assert_eq!(
        client.eval(source).await,
        error(b"WRONGTYPE Operation against a key holding the wrong kind of value")
    );
    client.expect(&[b"GET", b"after"], bulk(b"continued")).await;
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn eval_inside_exec_defers_validation_keeps_partial_writes_and_continues() {
    let server = TestServer::start().await;
    let mut client = Client::connect(&server).await;
    client.expect(&[b"MULTI"], simple(b"OK")).await;
    let script = b"redis.call('INCR','c'); redis.call('LPUSH','c','bad')";
    for args in [
        vec![b"EVAL".as_slice(), script, b"0"],
        vec![b"EVAL", b"invalid", b"-1"],
        vec![
            b"EVAL",
            b"return {redis.call('GET','c'),redis.call('BLPOP','empty',0)}",
            b"0",
        ],
        vec![b"INCR", b"c"],
    ] {
        client.expect(&args, simple(b"QUEUED")).await;
    }
    client
        .expect(
            &[b"EXEC"],
            array(vec![
                error(b"WRONGTYPE Operation against a key holding the wrong kind of value"),
                error(b"ERR Number of keys can't be negative"),
                array(vec![bulk(b"1"), Reply::Bulk(None)]),
                Reply::Integer(2),
            ]),
        )
        .await;
    client.expect(&[b"MULTI"], simple(b"OK")).await;
    client
        .expect(&[b"SET", b"never", b"queued"], simple(b"QUEUED"))
        .await;
    client
        .expect(
            &[b"EVAL", b"return 1"],
            error(b"ERR wrong number of arguments for 'eval' command"),
        )
        .await;
    client
        .expect(
            &[b"EXEC"],
            error(b"EXECABORT Transaction discarded because of previous errors."),
        )
        .await;
    client.expect(&[b"GET", b"never"], Reply::Bulk(None)).await;
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn competing_scripts_execs_and_ordinary_commands_have_no_intermediate_observations() {
    let server = TestServer::start().await;
    let barrier = Arc::new(Barrier::new(9));
    let mut tasks = JoinSet::new();
    let source = b"local r={}; for i=1,8 do r[i]=redis.call('INCR','counter') end; return r";
    for task in 0..9 {
        let mut client = Client::connect(&server).await;
        let start = barrier.clone();
        tasks.spawn(async move {
            start.wait().await;
            let mut ids = Vec::new();
            for _ in 0..40 {
                let result = if task == 0 {
                    client.command(&[b"INCR", b"counter"]).await
                } else if task % 2 == 0 {
                    client.expect(&[b"MULTI"], simple(b"OK")).await;
                    client
                        .expect(&[b"EVAL", source, b"0"], simple(b"QUEUED"))
                        .await;
                    client
                        .expect(&[b"INCR", b"counter"], simple(b"QUEUED"))
                        .await;
                    let Reply::Array(Some(mut replies)) = client.command(&[b"EXEC"]).await else {
                        panic!("EXEC array")
                    };
                    let last = replies.pop().unwrap();
                    let Reply::Array(Some(mut batch)) = replies.pop().unwrap() else {
                        panic!("EVAL array")
                    };
                    batch.push(last);
                    array(batch)
                } else {
                    client.eval(source).await
                };
                let batch = match result {
                    Reply::Integer(n) => vec![n],
                    Reply::Array(Some(batch)) => batch
                        .into_iter()
                        .map(|r| match r {
                            Reply::Integer(n) => n,
                            _ => panic!("integer"),
                        })
                        .collect(),
                    _ => panic!("unexpected reply"),
                };
                assert!(batch.windows(2).all(|w| w[1] == w[0] + 1));
                ids.extend(batch);
            }
            ids
        });
    }
    let mut ids = Vec::new();
    bounded(async {
        while let Some(task) = tasks.join_next().await {
            ids.extend(task.unwrap());
        }
    })
    .await;
    ids.sort_unstable();
    assert_eq!(ids, (1..=2760).collect::<Vec<_>>());
    let mut fence = Client::connect(&server).await;
    fence.expect(&[b"GET", b"counter"], bulk(b"2760")).await;
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fragmented_pipeline_and_incomplete_eval_preserve_reply_order_and_nonmutation() {
    let server = TestServer::start().await;
    let mut client = Client::connect(&server).await;
    let mut wire = encode(&[
        b"EVAL",
        b"return {redis.call('SET',KEYS[1],ARGV[1]),redis.call('GET',KEYS[1])}",
        b"1",
        b"k\0",
        b"\0\xff\r\n",
    ]);
    wire.extend(encode(&[b"EVAL", b"return redis.call('GET')", b"0"]));
    wire.extend(encode(&[b"GET", b"k\0"]));
    for chunk in wire.chunks(7) {
        bounded(client.0.get_mut().write_all(chunk)).await.unwrap();
    }
    assert_eq!(
        bounded(client.read()).await,
        array(vec![simple(b"OK"), bulk(b"\0\xff\r\n")])
    );
    assert_eq!(
        bounded(client.read()).await,
        error(b"ERR Wrong number of args calling Redis command from script")
    );
    assert_eq!(bounded(client.read()).await, bulk(b"\0\xff\r\n"));
    let mut partial = Client::connect(&server).await;
    let wire = encode(&[b"EVAL", b"redis.call('SET','never','applied')", b"0"]);
    bounded(partial.0.get_mut().write_all(&wire[..wire.len() - 1]))
        .await
        .unwrap();
    drop(partial);
    client.expect(&[b"GET", b"never"], Reply::Bulk(None)).await;
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unread_script_reply_releases_ownership_and_lost_reply_does_not_undo_writes() {
    let server = TestServer::start().await;
    let mut client = Client::connect(&server).await;
    let mut observer = Client::connect(&server).await;
    let source = br#"
        redis.call('SET','committed','yes')
        return {string.rep('x',8388608),string.rep('x',8388608)}
    "#;
    bounded(
        client
            .0
            .get_mut()
            .write_all(&encode(&[b"EVAL", source, b"0"])),
    )
    .await
    .unwrap();
    assert_eq!(bounded(client.0.read_u8()).await.unwrap(), b'*');
    observer.expect(&[b"GET", b"committed"], bulk(b"yes")).await;
    drop(client);
    observer.expect(&[b"GET", b"committed"], bulk(b"yes")).await;
    observer
        .expect(&[b"INCR", b"progress"], Reply::Integer(1))
        .await;
    server.stop().await;
}
