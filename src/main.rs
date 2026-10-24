use std::io;
use std::net::SocketAddr;
use std::process::ExitCode;

use keyper::config::{Cli, Config, parse_args};
use keyper::server;

const HELP: &str = "\
Keyper — in-memory Redis-compatible service

Usage: keyper [--bind IP] [--port NUMBER]
       keyper --help

Options:
  --bind IP      Listening IPv4 or IPv6 address (default: 127.0.0.1)
  --port NUMBER  Listening port (default: 6379; 0: OS-assigned)
  -h, --help     Print this help

Serves RESP2 PING, SELECT, GET, SET, EXISTS, LPUSH, RPUSH, BLPOP, BRPOP,
INCR, SADD, SREM, SISMEMBER, SCARD, SMEMBERS,
HSET, HGET, HDEL, HEXISTS, HLEN, HGETALL, HVALS,
TYPE, LLEN, LINDEX, LPOP (with optional count), MULTI, EXEC, DISCARD, EVAL.
SELECT is first-command-only; SET has no options; blocking pops accept 1–5 keys.
Transactions and Lua 5.1 EVAL execute atomically, without rollback.
Other Redis commands are unsupported; scripts have no time/heap quota.
No authentication or TLS. Use only trusted clients and scripts.
For LAN access, bind a specific local IP and restrict the port with a firewall.
Wildcard binds (0.0.0.0 or ::) expose all interfaces; never expose publicly.
Press Ctrl-C to stop.";

async fn run(config: Config) -> io::Result<()> {
    let listener = server::bind(config).await.map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "bind {}: {error}",
                SocketAddr::new(config.bind, config.port)
            ),
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
