# Keyper

A small, in-memory Redis-compatible service being built in Rust on Tokio.

**Current boundary: RESP2 TCP transport and PING.** Multiple clients can connect
and exchange binary-safe PING requests. Database commands, blocking pops,
transactions, Lua, and crawler interoperability are **not implemented yet**.

## Prerequisites and build

Install Rust **1.99.0**, including rustfmt and Clippy, with
[rustup](https://rustup.rs/):

```sh
rustup toolchain install 1.99.0 --profile minimal --component rustfmt --component clippy
```

A platform linker is also required (for example, the system C toolchain on Linux).
The checked-in `rust-toolchain.toml` selects this toolchain automatically.
`Cargo.lock` is tracked; use `--locked` to avoid unintended dependency updates.
The first build needs crates.io access unless the locked dependencies are cached.

```sh
cargo build --locked
cargo run --locked -- --help
```

The only direct dependency is Tokio **1.53.1**, pinned exactly, with default
features disabled. Enabled features provide the multithreaded runtime/macros,
TCP, buffered asynchronous I/O, signals, and timers. Tests also use `sync` for
shutdown channels and `test-util` for controlled-clock deadline checks. No Redis installation,
sibling repository, Lua toolchain, or local planning documents are needed for
the basic build/tests.

## Start the server and talk to it

In one terminal:

```sh
cargo run --locked -- --port 6380
# keyper: listening on 127.0.0.1:6380
```

In another, using an installed [redis-cli](https://redis.io/docs/latest/develop/tools/cli/):

```sh
redis-cli -2 -h 127.0.0.1 -p 6380 PING
# PONG
redis-cli -2 -h 127.0.0.1 -p 6380 PING 'hello world'
# "hello world"
redis-cli -2 -h 127.0.0.1 -p 6380 PING ''
# ""
```

The `-2` explicitly requests RESP2. Several terminals can use this endpoint at
once. Unknown commands (including client metadata commands) return honest errors,
not fake success. `redis-cli` **7.4.0** was verified with the examples above.
Interactive `redis-cli` may also attempt unsupported commands such as COMMAND;
those errors do not close the connection. Raw inline/telnet commands are not
supported.

Stop the owned server with **Ctrl-C**. It stops accepting clients, cancels pending
connection I/O, and drains tasks with a five-second deadline. PING has no data to
commit. This does not yet certify shutdown of future database operations or Lua.
There is no persistence; restarting will not restore data.

### Configuration and exposure

- No arguments binds **127.0.0.1:6379**.
- `--port NUMBER` accepts decimal integers from 0 through 65535. Port **0** asks
  the OS for an available port; the startup line prints the actual endpoint.
- There is no bind-address option. This is a **trusted local/demo subset**, not
  an authenticated or hardened public Redis replacement.
- Missing, invalid, repeated, or unknown options exit with status **2**.
  Only the separate `--port NUMBER` spelling is accepted.
- `-h` / `--help` prints help and exits successfully, short-circuiting parsing.
- A bind/accept/task failure exits with status **1**, with contextual diagnostics;
  an occupied port never prints a successful listening message.
- Ctrl-C normally exits **0**; a signal-handler or shutdown failure exits **1**.
  There is no automatic service restart after an unexpected failure.

## Implemented command and transport contracts

| Command | RESP2 result |
| --- | --- |
| `PING` | Simple string `PONG` |
| `PING message` | Bulk string containing the exact message, including empty/binary bytes |

Command names are ASCII case-insensitive. Wrong PING arity, an empty command
array, and unknown commands return `ERR` replies; the connection remains usable.
PING is exempt from the project's linearizability requirement and excluded from
operation histories. **No database linearizability claim is made at this stage.**

Requests must be arrays of **non-null bulk strings**. The decoder consumes one
complete request at a time, preserving bytes following it. Partial TCP reads and
coalesced frames work without losing boundaries. The encoder supports simple
strings, errors, signed 64-bit integers, bulk/null bulk strings, and nested/null
arrays; only PING/error replies are currently exposed as commands. Null and empty
values remain distinct. CR/LF in status/error text are replaced with spaces to
prevent reply injection; bulk data is never altered.

Malformed framing or a resource-limit violation produces a best-effort
`ERR Protocol error` reply and closes that connection without resynchronizing.
EOF within a request discards the incomplete request and closes silently; EOF
between requests closes normally. Socket/write failures affect only that client;
a connection-task panic stops the service. Half-close continuation is not a
supported client mode.

### Resource limits and ordering

These explicit local-service limits bound untrusted allocations and connection
work; they are not the real Redis server's defaults:

- **256** active connection tasks; further connections wait in the OS backlog
  until a slot becomes available (the OS may refuse excess connections).
- **1024** bulk-string arguments, including the command name, per request.
- **8 MiB** per bulk string; **16 MiB** per encoded request including headers.
- At most **20 decimal digits** in each length header; sizes are checked before
  allocation. Lengths are unsigned decimal; negative/null request lengths fail.
- One request/reply in execution per connection, an **8 KiB** read-ahead buffer,
  and a **five-second** deadline for writing each complete reply.

Idle/partial request reads have no deadline, but consume a connection slot and
bounded memory. A slow reader cannot retain shared database ownership (none
exists yet) or block another connection's I/O. This is bounded per connection,
not protection against a hostile local user exhausting the overall memory cap.

The present read/execute/write loop preserves order for simple coalesced PINGs;
full pipelining/backpressure, client startup, disconnect-during-blocking, and
transaction guarantees remain later work. Do not infer crawler compatibility
from these transport tests.

## Development checks

Run all four gates after code changes:

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --locked
cargo build --locked
```

Use `cargo fmt --all` to apply formatting. Core tests need no running Redis or
external client. They cover configuration/CLI errors, all reply encodings,
request limits, malformed/truncated frames, partial I/O, PING semantics,
concurrent/slow/disconnected clients, occupied ports, and bounded shutdown.
Socket tests own loopback servers on ephemeral ports, bound their waits, and
clean up on failure. Controlled-clock tests check write deadlines; injected
broken/short writes check delivery failures without depending on TCP timing.
These tests are not proof of future state concurrency or crawler behavior.

### Layout

- `src/config.rs`: configuration and argument validation, without network I/O.
- `src/resp.rs`: request framing, resource bounds, and RESP2 reply encoding.
- `src/command.rs`: command validation and PING behavior.
- `src/server.rs`: loopback listener, connection tasks, writes, and supervision.
- `src/main.rs`: arguments, two-worker Tokio entry point, signals, diagnostics.
- `tests/cli.rs`, `tests/transport.rs`: process and real-socket checks.

## Intended state contracts (not implemented)

The target has sixteen independent databases (0–15), database 0 by default, and
SELECT allowed only as the first complete command attempt. Further core commands
will be SELECT, GET, bare SET, EXISTS, LPUSH, RPUSH, BLPOP, and BRPOP; blocking pops
accept one to five keys and a double-precision timeout.

The planned linearizability boundary is one dedicated state-owner thread for all
databases and waiters, separate from Tokio networking. A command, transaction, or
Lua script runs as one non-interleaved state unit. Blocking pops atomically check
and register, then release the owner while waiting; they recheck keys left to
right after complete atomic units, with one terminal outcome per waiter. No state
ownership may span socket I/O or a blocking wait. These are design commitments,
**not a verified implementation or proof**. Owner failure must stop the service,
not silently replace state. A lost response may follow an applied write; no
automatic rollback or retry is promised.

Later work includes ordered pipelining, audited extra commands, genuine Lua 5.1,
and MULTI/EXEC/DISCARD for an **unmodified** external crawler. Crawler sources and
Git state must not be changed; compatibility code and harnesses belong here.
Neither crawler interoperability nor any live demo has been verified. Broad
Redis compatibility, RESP3, authentication, pub/sub, and public-network hardening
are not claimed.
