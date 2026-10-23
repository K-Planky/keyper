//! Opt-in language/conversion comparison, not Keyper's future TCP EVAL bridge.
//! Requires an explicit redis-server binary; never connects to an existing DB.

use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use keyper::{resp::Reply, scripting::execute};

struct Reference {
    child: Child,
    scratch: PathBuf,
}

impl Drop for Reference {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.scratch);
    }
}

fn start() -> (Reference, redis::Connection) {
    let binary = std::env::var_os("KEYPER_REFERENCE_REDIS_SERVER")
        .expect("set KEYPER_REFERENCE_REDIS_SERVER to a Redis 7.4 redis-server binary");
    // Release an ephemeral reservation immediately before binding the child.
    // A bind race fails the test; it never falls back to a preexisting server.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let scratch = std::env::temp_dir().join(format!(
        "keyper-lua-reference-{}-{unique}",
        std::process::id()
    ));
    std::fs::create_dir(&scratch).unwrap();
    drop(listener);
    let child = Command::new(binary)
        .args([
            "--bind",
            "127.0.0.1",
            "--port",
            &port.to_string(),
            "--save",
            "",
            "--appendonly",
            "no",
            "--dir",
        ])
        .arg(&scratch)
        .args(["--logfile", "redis.log"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .spawn();
    let child = child.unwrap_or_else(|error| {
        let _ = std::fs::remove_dir_all(&scratch);
        panic!("cannot start reference Redis: {error}")
    });
    let mut owner = Reference { child, scratch };
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}/0")).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        assert!(
            owner.child.try_wait().unwrap().is_none(),
            "reference Redis exited: {:?}",
            std::fs::read_to_string(owner.scratch.join("redis.log"))
        );
        if let Ok(mut connection) = client.get_connection_with_timeout(Duration::from_millis(100)) {
            connection
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            connection
                .set_write_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            // Match the child's process ID before doing any fixture work. A
            // port reservation race must not test/write another user's server.
            let info: String = redis::cmd("INFO")
                .arg("server")
                .query(&mut connection)
                .unwrap();
            assert!(
                info.lines()
                    .any(|line| line == format!("process_id:{}", owner.child.id()))
            );
            return (owner, connection);
        }
        assert!(Instant::now() < deadline, "reference startup deadline");
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn error_reply(code: &str, detail: Option<&str>) -> Reply {
    Reply::Error(
        match detail {
            Some(detail) => format!("{code} {detail}"),
            None => code.to_string(),
        }
        .into_bytes(),
    )
}

fn reply(value: redis::Value) -> Reply {
    match value {
        redis::Value::Nil => Reply::Bulk(None),
        redis::Value::Int(n) => Reply::Integer(n),
        redis::Value::BulkString(bytes) => Reply::Bulk(Some(bytes)),
        redis::Value::SimpleString(text) => Reply::Simple(text.into_bytes()),
        redis::Value::Okay => Reply::Simple(b"OK".to_vec()),
        redis::Value::Array(items) => Reply::Array(Some(items.into_iter().map(reply).collect())),
        redis::Value::ServerError(error) => error_reply(error.code(), error.details()),
        other => panic!("unexpected RESP3 value {other:?}"),
    }
}

#[test]
#[ignore = "requires KEYPER_REFERENCE_REDIS_SERVER; starts only its own disposable Redis"]
fn lua51_general_language_and_resp2_match_reference() {
    let (_owner, mut reference) = start();
    let mut sources: Vec<String> = [
        "return _VERSION", "return {KEYS,ARGV}", "return 'a' .. string.char(0,255,13,10)",
        "return nil", "return false", "return true", "return function() end", "return {}",
        "return {1,false,{2,nil,3},[5]=5,ignore=9}", "return 1,2,3",
        "return {ok='OK',err='ERR preferred'}", "return {ok=3,7}",
        "return {ok='A\\r\\nB\\000suffix'}", "return {err='ERR A\\r\\nB\\000suffix'}",
        "return redis.status_reply('READY')", "return redis.error_reply('-BUSY later')",
        "return redis.error_reply('refused')", "return redis.error_reply('two words')",
        "return redis.error_reply('ERR \\r\\ntext\\r\\n')", "return redis.error_reply('ERR spaced  ')",
        "return redis.status_reply(1)", "return redis.error_reply()",
        "return {redis.call('PING').ok,redis.pcall('GET').err}",
        "return {redis.call('TYPE',KEYS[1]).ok, redis.call('EXISTS',KEYS[1])}",
        "return {pcall(function() redis.call('GET') end)}",
        "return {xpcall(function() redis.call('GET') end, function(e) return e.err end)}",
        "local function f() return 1,nil,3 end; local function count(...) return select('#',...) end; return count(pcall(f))",
        "local n=0; for _,v in ipairs({1,2,3}) do n=n+v end; return n",
        "local function outer(n) return function(x) return n+x end end; return outer(12)(5)",
        "return {string.match('abc-123','%d+'),string.sub('abcdef',2,4),string.format('%s:%d','v',42)}",
        "local a={1,2,3}; table.insert(a,4); return {unpack(a,2,3)}",
        "return {redis.pcall().err,redis.pcall('GET',true).err}",
    ].into_iter().map(str::to_owned).collect();
    for n in [
        "0",
        "-0.0",
        "1.25",
        "-1.25",
        "0/0",
        "math.huge",
        "-math.huge",
        "9007199254740992",
        "9007199254740993",
        "9223372036854775807",
        "-9223372036854775808",
        "1e20",
        "1e-7",
        "1.2345678901234567",
        "0.00001",
    ] {
        sources.push(format!("return {n}"));
        // PING echoes its argument as bulk data, so the test callback exercises
        // number->command-argument formatting without a database implementation.
        sources.push(format!("return redis.call('PING',{n})"));
    }
    for n in 0..128 {
        sources.push(format!(
            "local n={n}; local a={{}}; for i=1,8 do a[i]=n+i/3 end; return a"
        ));
    }
    for n in [
        f64::MIN_POSITIVE,
        f64::from_bits(1),
        f64::MAX,
        -0.000_000_1,
        4_611_686_018_427_387_904.0,
        4_611_686_018_427_388_928.0,
        -4_611_686_018_427_388_928.0,
        9_223_372_036_854_774_784.0,
    ] {
        sources.push(format!("return redis.call('PING',{n:e})"));
    }
    let mut bits = 0x1729_3141_5926_5358u64;
    for _ in 0..1024 {
        bits ^= bits << 13;
        bits ^= bits >> 7;
        bits ^= bits << 17;
        let n = f64::from_bits(bits);
        if n.is_finite() {
            sources.push(format!("return redis.call('PING',{n:e})"));
        }
    }
    let keys = vec![b"\0\xffabsent".to_vec(), vec![]];
    let args = vec![b"\0\xff\r\n".to_vec(), b"18446744073709551615".to_vec()];
    for source in &sources {
        let mut command = redis::cmd("EVAL");
        command.arg(source).arg(keys.len()).arg(&keys).arg(&args);
        let expected = match command.query::<redis::Value>(&mut reference) {
            Ok(value) => reply(value),
            Err(error) => error_reply(error.code().expect("server error code"), error.detail()),
        };
        let actual = execute(source.as_bytes(), &keys, &args, |request| {
            match request[0].as_slice() {
                b"PING" if request.len() == 1 => Reply::Simple(b"PONG".to_vec()),
                b"PING" => Reply::Bulk(Some(request[1].clone())),
                b"GET" => Reply::Error(
                    b"ERR Wrong number of args calling Redis command from script".to_vec(),
                ),
                b"TYPE" => Reply::Simple(b"none".to_vec()),
                b"EXISTS" => Reply::Integer(0),
                _ => panic!("unexpected test callback command"),
            }
        });
        assert_eq!(actual, expected, "source: {source}");
    }
    // Diagnostic strings include engine-specific source/line/hash decorations;
    // check the failure category, not an invented exact Redis stack trace.
    for source in [
        "local =",
        "error('ordinary')",
        "return missing_global",
        "new_global=1",
    ] {
        assert!(
            redis::cmd("EVAL")
                .arg(source)
                .arg(0)
                .query::<redis::Value>(&mut reference)
                .is_err()
        );
        assert!(matches!(
            execute(source.as_bytes(), &[], &[], |_| unreachable!()),
            Reply::Error(_)
        ));
    }
    println!(
        "matched {} exact language/conversion results and 4 error categories",
        sources.len()
    );
}
