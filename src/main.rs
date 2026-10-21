use std::io;
use std::process::ExitCode;

use keyper::config::{Cli, Config, parse_args};
use keyper::server;

const HELP: &str = "\
Keyper — in-memory Redis-compatible service

Usage: keyper [--port NUMBER]
       keyper --help

Options:
  --port NUMBER  Loopback listening port (default: 6379; 0: OS-assigned)
  -h, --help     Print this help

Serves RESP2 PING, SELECT, GET, SET, EXISTS, LPUSH, RPUSH, BLPOP, BRPOP,
INCR, SADD, SREM, SISMEMBER, SCARD, SMEMBERS,
HSET, HGET, HDEL, HEXISTS, HLEN, HGETALL, HVALS,
TYPE, LLEN, LINDEX, LPOP (with optional count) on 127.0.0.1.
SELECT is first-command-only; SET has no options; blocking pops accept 1–5 keys.
Press Ctrl-C to stop. Transactions, Lua, and other Redis commands are not implemented yet.";

async fn run(config: Config) -> io::Result<()> {
    let listener = server::bind(config).await.map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("bind 127.0.0.1:{}: {error}", config.port),
        )
    })?;
    println!("keyper: listening on {}", listener.local_addr()?);
    server::serve(listener, tokio::signal::ctrl_c()).await
}

#[tokio::main(worker_threads = 2)]
async fn main() -> ExitCode {
    match parse_args(std::env::args_os().skip(1)) {
        Ok(Cli::Help) => {
            println!("{HELP}");
            ExitCode::SUCCESS
        }
        Ok(Cli::Serve(config)) => match run(config).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("keyper: {error}");
                ExitCode::FAILURE
            }
        },
        Err(error) => {
            eprintln!("keyper: {error}\nTry 'keyper --help'.");
            ExitCode::from(2)
        }
    }
}
