//! Executable configuration, startup, and occupied-port diagnostics.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

const LIMIT: Duration = Duration::from_secs(5);

struct Process(Option<Child>);

impl Process {
    fn spawn(args: &[&str]) -> Self {
        Self(Some(
            Command::new(env!("CARGO_BIN_EXE_keyper"))
                .args(args)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .expect("start keyper"),
        ))
    }

    fn output(self) -> Output {
        self.output_with_limit(LIMIT)
    }

    fn output_with_limit(mut self, limit: Duration) -> Output {
        let deadline = Instant::now() + limit;
        loop {
            match self.0.as_mut().unwrap().try_wait() {
                Ok(Some(_)) => return self.0.take().unwrap().wait_with_output().unwrap(),
                Ok(None) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(10))
                }
                result => panic!("keyper did not exit within {limit:?}: {result:?}"),
            }
        }
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        if let Some(child) = &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[test]
fn help_succeeds_and_describes_the_actual_boundary() {
    let output = Process::spawn(&["--help"]).output();
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let help = String::from_utf8(output.stdout).unwrap();
    assert!(help.contains("--port NUMBER"));
    assert!(help.contains("default: 6379"));
    assert!(
        help.contains(
            "RESP2 PING, SELECT, GET, SET, EXISTS, LPUSH, RPUSH, BLPOP, BRPOP,\nINCR, SADD, SREM, SISMEMBER, SCARD, SMEMBERS,\nHSET, HGET, HDEL, HEXISTS, HLEN, HGETALL, HVALS,\nTYPE, LLEN, LINDEX, LPOP (with optional count), MULTI, EXEC, DISCARD, EVAL on 127.0.0.1"
        )
    );
    assert!(help.contains("SELECT is first-command-only; SET has no options"));
    assert!(help.contains("Transactions and Lua 5.1 EVAL execute atomically, without rollback"));
    assert!(help.contains("blocking pops accept 1–5 keys"));
    assert!(help.contains("scripts have no time/heap quota"));
}

#[test]
fn startup_announces_an_actual_loopback_endpoint_and_serves_ping() {
    let mut process = Process::spawn(&["--port", "0"]);
    let stdout = process.0.as_mut().unwrap().stdout.take().unwrap();
    let (send, receive) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        let mut line = String::new();
        BufReader::new(stdout).read_line(&mut line).unwrap();
        let _ = send.send(line);
    });
    let line = receive.recv_timeout(LIMIT).expect("startup deadline");
    reader.join().unwrap();
    let address = line
        .trim()
        .strip_prefix("keyper: listening on ")
        .unwrap()
        .parse()
        .unwrap();
    let mut client = TcpStream::connect_timeout(&address, LIMIT).unwrap();
    client.set_read_timeout(Some(LIMIT)).unwrap();
    client.set_write_timeout(Some(LIMIT)).unwrap();
    client.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
    let mut reply = [0; 7];
    client.read_exact(&mut reply).unwrap();
    assert_eq!(&reply, b"+PONG\r\n");
    // Process guard terminates this owned server even on assertion failure.
}

/// Linux /proc CPU accounting gives a bounded start gate without adding a host
/// callback to Lua or relying on a sleep to guess that an infinite script began.
#[cfg(target_os = "linux")]
#[test]
fn a_running_infinite_eval_keeps_network_workers_live_and_shutdown_is_fail_stop() {
    let mut process = Process::spawn(&["--port", "0"]);
    let pid = process.0.as_ref().unwrap().id();
    let stdout = process.0.as_mut().unwrap().stdout.take().unwrap();
    let (send, receive) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        let mut line = String::new();
        BufReader::new(stdout).read_line(&mut line).unwrap();
        let _ = send.send(line);
    });
    let line = receive.recv_timeout(LIMIT).expect("startup deadline");
    reader.join().unwrap();
    let address = line
        .trim()
        .strip_prefix("keyper: listening on ")
        .unwrap()
        .parse()
        .unwrap();
    let mut client = TcpStream::connect_timeout(&address, LIMIT).unwrap();
    client.set_write_timeout(Some(LIMIT)).unwrap();
    let source = b"redis.call('SET','started','yes'); while true do end";
    client
        .write_all(format!("*3\r\n$4\r\nEVAL\r\n${}\r\n", source.len()).as_bytes())
        .unwrap();
    client.write_all(source).unwrap();
    client.write_all(b"\r\n$1\r\n0\r\n").unwrap();
    let deadline = Instant::now() + LIMIT;
    let mut baseline = None;
    loop {
        let running = std::fs::read_dir(format!("/proc/{pid}/task"))
            .unwrap()
            .any(|task| {
                let path = task.unwrap().path();
                if std::fs::read_to_string(path.join("comm"))
                    .unwrap_or_default()
                    .trim()
                    != "keyper-state"
                {
                    return false;
                }
                let stat = std::fs::read_to_string(path.join("stat")).unwrap();
                // Fields after the final ')' start at field 3 (state); utime=14,
                // stime=15. Require CPU ticks, not mere thread existence.
                let fields: Vec<_> = stat
                    .rsplit_once(')')
                    .unwrap()
                    .1
                    .split_whitespace()
                    .collect();
                let ticks = fields[11].parse::<u64>().unwrap() + fields[12].parse::<u64>().unwrap();
                let before = *baseline.get_or_insert(ticks);
                ticks >= before + 2
            });
        if running {
            break;
        }
        assert!(Instant::now() < deadline, "script start deadline");
        std::thread::sleep(Duration::from_millis(10));
    }
    // Disconnect cannot interrupt a started atomic unit or reset its state.
    drop(client);
    // This is a responsiveness probe, not part of a linearizability history.
    let mut ping = TcpStream::connect_timeout(&address, LIMIT).unwrap();
    ping.set_read_timeout(Some(LIMIT)).unwrap();
    ping.set_write_timeout(Some(LIMIT)).unwrap();
    ping.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
    let mut pong = [0; 7];
    ping.read_exact(&mut pong).unwrap();
    assert_eq!(&pong, b"+PONG\r\n");
    assert!(
        Command::new("kill")
            .args(["-INT", &pid.to_string()])
            .status()
            .unwrap()
            .success()
    );
    let output = process.output_with_limit(Duration::from_secs(8));
    assert_eq!(output.status.code(), Some(1));
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("service shutdown exceeded five seconds; process must exit")
    );
}

#[test]
fn occupied_port_is_an_actionable_startup_failure() {
    let occupied = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = occupied.local_addr().unwrap().port().to_string();
    let output = Process::spawn(&["--port", &port]).output();
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    let error = String::from_utf8(output.stderr).unwrap();
    assert!(error.contains(&format!("bind 127.0.0.1:{port}")));
    assert!(!error.contains("listening on"));
}

#[test]
fn invalid_configuration_has_an_actionable_error_and_exit_status() {
    for args in [vec!["--port", "65536"], vec!["--port"], vec!["--unknown"]] {
        let output = Process::spawn(&args).output();
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        let error = String::from_utf8(output.stderr).unwrap();
        assert!(error.contains("keyper:"));
        assert!(error.contains("Try 'keyper --help'."));
        assert!(!error.contains("listening on"));
    }
}
