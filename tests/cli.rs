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

    fn output(mut self) -> Output {
        let deadline = Instant::now() + LIMIT;
        loop {
            match self.0.as_mut().unwrap().try_wait() {
                Ok(Some(_)) => return self.0.take().unwrap().wait_with_output().unwrap(),
                Ok(None) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(10))
                }
                result => panic!("keyper did not exit within five seconds: {result:?}"),
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
            "RESP2 PING, SELECT, GET, SET, EXISTS, LPUSH, RPUSH, BLPOP, BRPOP,\nINCR, SADD, SREM, SISMEMBER, SCARD, SMEMBERS,\nHSET, HGET, HDEL, HEXISTS, HLEN, HGETALL, HVALS,\nTYPE, LLEN, LINDEX, LPOP (with optional count), MULTI, EXEC, DISCARD on 127.0.0.1"
        )
    );
    assert!(help.contains("SELECT is first-command-only; SET has no options"));
    assert!(help.contains("Transactions execute atomically, without rollback"));
    assert!(help.contains("blocking pops accept 1–5 keys"));
    assert!(help.contains("Lua and other Redis commands are not implemented yet"));
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
