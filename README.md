# Keyper

A small, in-memory Redis-compatible service being built in Rust on Tokio.

**Current boundary: RESP2, PING, SELECT, GET, bare SET, and EXISTS.** Sixteen
in-memory databases are owned by one dedicated state thread, separate from
asynchronous networking. Lists/blocking pops, transactions, Lua, and crawler
interoperability are **not implemented yet**.

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
TCP, buffered asynchronous I/O, signals, timers, and `sync` for bounded work and
one-shot reply channels. Tests add `test-util` for controlled-clock deadlines.
No Redis installation, sibling repository, Lua toolchain, or local planning
documents are needed for the basic build/tests.

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
redis-cli -2 -h 127.0.0.1 -p 6380 SET greeting hello
# OK
redis-cli -2 -h 127.0.0.1 -p 6380 GET greeting
# "hello"
redis-cli -2 -h 127.0.0.1 -p 6380 EXISTS greeting missing greeting
# (integer) 2
redis-cli -2 -h 127.0.0.1 -p 6380 -n 15 SET greeting independent
# OK
redis-cli -2 -h 127.0.0.1 -p 6380 -n 15 GET greeting
# "independent"
redis-cli -2 -h 127.0.0.1 -p 6380 GET greeting
# "hello"
```

Each invocation opens a new connection. `-n 15` selects database 15 at startup;
without it, the connection uses database 0. Unlike real Redis, **SELECT is allowed
only as the first complete command attempt**: PING, invalid/unknown commands,
and failed SELECT attempts consume that window too. Repeated/late SELECT fails
without changing databases. Reconnect to choose another database.

The `-2` explicitly requests RESP2. Several terminals can use this endpoint at
once. Unknown commands (including client metadata commands) return honest errors,
not fake success. `redis-cli` **7.4.0** was verified with the examples above.
Interactive `redis-cli` may also attempt unsupported commands such as COMMAND;
those errors do not close the connection. Raw inline/telnet commands are not
supported.

Stop the owned server with **Ctrl-C**. It stops accepting clients, cancels
connection I/O, skips canceled work that has not started, and lets an already
executing state operation finish. Connection and owner draining share a
five-second deadline. Failure to drain is fatal: the executable exits with
status 1, never resets/replaces state and resumes. Embedders of `server::serve`
must likewise exit on failure rather than restart while an owner might still be
running. Future blocking/Lua shutdown behavior is not yet certified. There is no
persistence; every explicit restart starts empty.

### Configuration and exposure

- No arguments binds **127.0.0.1:6379**.
- `--port NUMBER` accepts decimal integers from 0 through 65535. Port **0** asks
  the OS for an available port; the startup line prints the actual endpoint.
- There is no bind-address option. This is a **trusted local/demo subset**, not
  an authenticated or hardened public Redis replacement.
- Missing, invalid, repeated, or unknown options exit with status **2**.
  Only the separate `--port NUMBER` spelling is accepted.
- `-h` / `--help` prints help and exits successfully, short-circuiting parsing.
- A bind/accept/task/state-owner failure exits with status **1**, with contextual
  diagnostics; an occupied port never prints a successful listening message.
- Ctrl-C normally exits **0**; a signal-handler or shutdown failure exits **1**.
  There is no automatic service restart after an unexpected failure.

## Implemented command and transport contracts

| Command | RESP2 result |
| --- | --- |
| `PING` | Simple string `PONG` |
| `PING message` | Bulk string containing the exact message, including empty/binary bytes |
| `SELECT index` | Simple string `OK`; only the first complete command attempt, index 0–15 |
| `GET key` | Bulk string value, or null bulk if absent |
| `SET key value` | Simple string `OK`; overwrites the value; **no options** |
| `EXISTS key [key ...]` | Integer count; repeated existing keys count repeatedly |

Command names are ASCII case-insensitive; keys/values are opaque bytes, including
empty, NUL, CR/LF, and non-UTF-8 data. Empty and absent values are distinct.
Indices use canonical decimal integer syntax (no `+`, whitespace, leading zeros,
`-0`, or fractions). Invalid indices, wrong arity, SET options, empty command
arrays, and unsupported commands return `ERR` replies without data mutation; the
connection remains usable. GET's wrong-type behavior and SET's cross-type
overwrite tests will arrive with list storage; currently only strings exist.
PING is exempt from linearizability and excluded from operation histories.

Requests must be arrays of **non-null bulk strings**. The decoder consumes one
complete request at a time, preserving bytes following it. Partial TCP reads and
coalesced frames work without losing boundaries. The encoder supports simple
strings, errors, signed 64-bit integers, bulk/null bulk strings, and nested/null
arrays; array replies are not yet exposed as commands. Null and empty values
remain distinct. CR/LF in status/error text are replaced with spaces to
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
- A shared work channel with **256** queued operations and one one-shot reply
  channel per operation. Full-queue sends await capacity without blocking Tokio.
  Stored database contents have no total size limit or eviction policy.

Idle/partial request reads have no deadline, but consume a connection slot and
bounded memory. A slow reader cannot retain database ownership or block another
connection's I/O. This is bounded per connection, not protection against a
hostile local user exhausting available memory.

The present read/execute/write loop preserves order for coalesced commands;
full pipeline lifecycle, actual client startup, disconnect-during-blocking, and
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
Database tests cover all sixteen namespaces, selection-window errors, binary
values, rejected SET options, atomic repeated-key EXISTS, and concurrent SET/GET
outcomes with cross-client real-time fences. Owner tests exercise asynchronous
backpressure, cancellation/lost replies, and fail-stop panic supervision.
Socket tests own loopback servers on ephemeral ports, bound their waits, and
clean up on failure. Controlled-clock tests check write deadlines; injected
broken/short writes check delivery failures without depending on TCP timing.
Repeated schedules supplement the ownership argument below, not a proof or
certification of future blocking/batch behavior or crawler compatibility.

### Layout

- `src/config.rs`: configuration and argument validation, without network I/O.
- `src/resp.rs`: request framing, resource bounds, and RESP2 reply encoding.
- `src/command.rs`: command admission, PING, and connection-local selection.
- `src/state.rs`: typed database indices, sixteen maps, bounded channels, owner thread.
- `src/server.rs`: loopback listener, connection tasks, writes, and supervision.
- `src/main.rs`: arguments, two-worker Tokio entry point, signals, diagnostics.
- `tests/cli.rs`, `tests/transport.rs`, `tests/databases.rs`: process/socket checks.
- `tests/support/mod.rs`: owned ephemeral servers and bounded socket-test waits.

## Linearizability and failure contract

Under the single-machine/correct-OS-and-hardware assumptions, the dedicated owner
alone accesses the sixteen maps. Every GET, SET, or entire variadic EXISTS runs
without interleaving. GET linearizes at its lookup, SET at insertion/replacement,
and EXISTS during its indivisible scan; it never samples different moments for
repeated arguments. SELECT linearizes at the connection-local state transition;
input-validation errors linearize at validation without touching data. Other
clients cannot change a connection's selected database. PING is not in histories.

The owner receives owned commands and returns owned replies. A reply is created
after the operation and before asynchronous socket writing. Thus a response
received before a later invocation fixes their state order; overlapping calls
can take either valid order. No map reference or database ownership survives into
network I/O. Hash-map lookup/insertion is average O(1); EXISTS is O(N) in supplied
key arguments, as in the [Redis command reference](https://redis.io/docs/latest/commands/).
Hashing/copying/serialization also costs the number of bytes involved. Databases
are isolated namespaces, not independently executing owner threads.

A dropped reply receiver lets the owner skip an operation that has not begun.
Cancellation racing with execution can instead lose the response to an applied
write. Disconnect/write failure is **ambiguous**, never automatic rollback or
retry. An owner panic/channel failure stops serving rather than replacing state;
an explicit restart loses all data. This is not exactly-once delivery, persistence,
or crash recovery.

## Remaining work (not implemented)

Further core commands will be LPUSH, RPUSH, BLPOP, and BRPOP; blocking pops accept
one to five keys and a double-precision timeout. The planned owner will atomically
check/register waits, then remain available to other clients. Waiters recheck
keys left to right after complete atomic units, with FIFO priority and one
terminal outcome each. No state ownership may span a blocking wait. Blocking
linearizability, cancellation races, and atomic-batch integration remain unverified.

Later work includes ordered pipelining, audited extra commands, genuine Lua 5.1,
and MULTI/EXEC/DISCARD for an **unmodified** external crawler. Crawler sources and
Git state must not be changed; compatibility code and harnesses belong here.
Neither crawler interoperability nor any live demo has been verified. Broad
Redis compatibility, RESP3, authentication, pub/sub, and public-network hardening
are not claimed.
