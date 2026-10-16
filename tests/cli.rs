//! Process-level checks of setup diagnostics, not server/RESP coverage.

use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

fn invoke(args: &[&str]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_keyper"))
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start keyper");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return child.wait_with_output().expect("collect keyper output"),
            Ok(None) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            result => {
                let _ = child.kill();
                let _ = child.wait();
                panic!("keyper did not exit within five seconds: {result:?}");
            }
        }
    }
}

#[test]
fn help_succeeds_and_describes_the_actual_boundary() {
    let output = invoke(&["--help"]);
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let help = String::from_utf8(output.stdout).unwrap();
    assert!(help.contains("--port NUMBER"));
    assert!(help.contains("default: 6379"));
    assert!(help.contains("not implemented yet"));
    assert!(help.contains("No listener is started"));
}

#[test]
fn startup_is_an_honest_failure_until_transport_exists() {
    for args in [vec![], vec!["--port", "6380"]] {
        let output = invoke(&args);
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stdout.is_empty());
        let error = String::from_utf8(output.stderr).unwrap();
        assert!(error.contains("TCP service is not implemented yet"));
        assert!(error.contains(if args.is_empty() { "6379" } else { "6380" }));
        assert!(error.contains("no listener was started"));
    }
}

#[test]
fn invalid_configuration_has_an_actionable_error_and_exit_status() {
    for args in [vec!["--port", "65536"], vec!["--port"], vec!["--unknown"]] {
        let output = invoke(&args);
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        let error = String::from_utf8(output.stderr).unwrap();
        assert!(error.contains("keyper:"));
        assert!(error.contains("Try 'keyper --help'."));
        assert!(!error.contains("no listener was started"));
    }
}
