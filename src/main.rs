use std::process::ExitCode;

use keyper::config::{Cli, parse_args};

const HELP: &str = "\
Keyper — in-memory Redis-compatible service (setup only)

Usage: keyper [--port NUMBER]
       keyper --help

Options:
  --port NUMBER  Intended loopback listening port (default: 6379; 0: OS-assigned)
  -h, --help     Print this help

TCP service and Redis commands are not implemented yet.
No listener is started by this version.";

#[tokio::main(worker_threads = 2)]
async fn main() -> ExitCode {
    match parse_args(std::env::args_os().skip(1)) {
        Ok(Cli::Help) => {
            println!("{HELP}");
            ExitCode::SUCCESS
        }
        Ok(Cli::Serve(config)) => {
            eprintln!(
                "keyper: TCP service is not implemented yet (requested port {}); no listener was started",
                config.port
            );
            ExitCode::FAILURE
        }
        Err(error) => {
            eprintln!("keyper: {error}\nTry 'keyper --help'.");
            ExitCode::from(2)
        }
    }
}
