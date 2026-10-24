# Keyper

A small, in-memory Redis-compatible service built with Rust and Tokio. Keyper
provides RESP2, 16 independent databases, ordered pipelining, and linearizable
database operations. Genuine Lua 5.1 scripting and transactions support the
unchanged [swarmcrawl](https://github.com/K-Planky/swarmcrawl) crawler.

Keyper implements a focused Redis subset, not a production Redis replacement.

## Prerequisites and build

- Rust **1.99.0** and Cargo; the toolchain is pinned in `rust-toolchain.toml`.
- A native C compiler, archiver, and linker to build vendored Lua.
- `redis-cli` for the examples below; no Redis server is needed to run Keyper.

From the repository root:

```sh
cargo build --release --locked
```

## Run and connect

Start the server in one terminal:

```sh
./target/release/keyper --port 6380
```

Keyper binds only to **127.0.0.1**. Omitting `--port` uses **6379**;
`--port 0` selects an available port and prints the address. Use `--help` for usage.

In another terminal:

```sh
redis-cli -2 -h 127.0.0.1 -p 6380 PING                 # PONG
redis-cli -2 -h 127.0.0.1 -p 6380 SET greeting hello   # OK
redis-cli -2 -h 127.0.0.1 -p 6380 GET greeting         # "hello"
redis-cli -2 -h 127.0.0.1 -p 6380 -n 15 GET greeting   # (nil): separate database
```

To try blocking operations, run this in the client terminal:

```sh
redis-cli -2 -p 6380 BLPOP queue 30
```

Before its 30-second timeout, push from a third terminal:

```sh
redis-cli -2 -p 6380 RPUSH queue first second
```

The waiting client receives `queue` and `first`; `second` remains in the list.
Other connections can keep working while a client waits.

Stop Keyper with **Ctrl-C**. All data is lost when the service stops; restarting
creates empty databases.

## Implemented command and transport contracts

| Area | Supported commands |
| --- | --- |
| Connection | `PING [message]`, `SELECT index` |
| Strings and keys | `GET`, `SET`, `INCR`, `EXISTS`, `TYPE` |
| Lists | `LPUSH`, `RPUSH`, `LLEN`, `LINDEX`, `LPOP key [count]`, `BLPOP`, `BRPOP` |
| Sets | `SADD`, `SREM`, `SISMEMBER`, `SCARD`, `SMEMBERS` |
| Hashes | `HSET`, `HGET`, `HDEL`, `HEXISTS`, `HLEN`, `HGETALL`, `HVALS` |
| Atomic execution | `MULTI`, `EXEC`, `DISCARD`, `EVAL script numkeys [key ...] [arg ...]` |

These 30 commands follow the [Redis command reference](https://redis.io/docs/latest/commands/)
within this subset, with these project-specific restrictions:

- **SELECT:** only the first complete command attempt on a connection may select
  database **0–15**. Any first attempt, including an invalid command or PING,
  consumes that opportunity. The default database is **0**.
- **SET:** only `SET key value`; no options.
- **BLPOP / BRPOP:** **1–5 key arguments**, including duplicates. Timeouts are
  floating-point seconds; zero waits indefinitely. Negative, nonfinite, malformed,
  and out-of-range timeouts are rejected. Positive durations round up to milliseconds.

Requests are RESP2 arrays of bulk strings; keys and values are binary-safe.
Pipelined commands execute and reply in connection order, but a pipeline is not
atomic. A blocking command delays later commands on that connection.

EVAL exposes `KEYS`, `ARGV`, `redis.call`, `redis.pcall`, and status/error reply
helpers in an isolated Lua 5.1 environment, not the full Redis scripting API.

Unsupported commands return errors, including `CLIENT SETINFO`; the audited
credential-free RESP2 redis-rs startup tolerates those metadata errors.
RESP3, inline requests, authentication/TLS, pub/sub, WATCH, script-cache APIs,
expiration, persistence, replication, and Redis Cluster are not supported.

## Linearizability and failure contract

Tokio handles network I/O on multiple threads. A **single dedicated state-owner
thread** exclusively owns all databases, waiter registrations, and Lua execution.
Bounded channels carry commands and owned replies; database access never spans
socket I/O or a client's blocking wait.

- Ordinary reads, writes, and execution errors linearize within one indivisible
  owner operation. SELECT and transaction queue changes are ordered session-state
  transitions. **PING is exempt** from linearizability and excluded from histories.
- A blocking pop checks keys **left to right** and, if all are empty, registers its
  wait in the same owner operation. No producer can slip between the empty check
  and registration, so there is no lost wakeup. A successful pop linearizes at
  item removal; eligible waiters are considered in registration order.
- A whole variadic push, EXEC, or EVAL finishes before affected waiters run.
  Waiters recheck their original key order against the **final state**, never an
  intermediate batch result. For example, a waiting `BLPOP a b 0` chooses `a` if
  one atomic batch fills `b` and then `a`.
- Finite waits use monotonic deadlines and never time out early. A null timeout
  result has an empty observation during the pending interval as its linearization
  witness. The owner serializes timeout, producer, and disconnect handling;
  terminal cleanup removes every registration, preventing later consumption.
- EXEC and EVAL execute without interleaving across any database. Transaction
  admission errors abort execution; runtime errors **do not roll back** prior
  writes, and EXEC continues with its remaining commands. Blocking pops inside
  either atomic context return null immediately when no item is available.

A lost reply may follow an applied mutation: Keyper does not retry it or promise
exactly-once delivery. Unexpected owner/task panics stop the service rather than
reset uncertain state. The design assumes a single machine with correctly
functioning hardware and OS; it does not provide crash recovery.

## Operational limits

Use only trusted local clients and scripts. Lua has a fresh, host-restricted
environment per invocation, but **no time or heap quota**. A long or infinite
script stalls database work across all namespaces.

- 256 active connections and a 256-entry owner work queue.
- 1,024 arguments per request (including the command name), 8 MiB per bulk string,
  16 MiB per request frame, and 16 MiB of pending read-ahead per connection.
  Excess pending input closes the connection.
- Transactions: 1,024 queued commands and a 16 MiB accounted queue budget.
- Five-second reply-write and shutdown deadlines; a stuck shutdown exits with
  failure rather than attempting partial recovery.

These limits are not a global memory bound. Command costs include payload copying,
reply serialization, destruction of overwritten collections, and waiter bookkeeping;
transactions do not copy the whole database.

## Verification

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --locked
cargo build --locked
```

Core tests start isolated servers and need no external Redis. They cover framing,
database isolation, command semantics, ordered pipelines, blocking races,
transactions, Lua, and shutdown. Tests are evidence, not a formal linearizability
proof; automatic history checking is not included.

See [VERIFICATION.md](VERIFICATION.md) for recorded results, verification boundaries,
and opt-in Redis **7.4.x** comparisons. Linux x86_64 is the verified platform.

<a id="optional-unchanged-crawler-script-components"></a>
<a id="optional-full-process-crawler-verification"></a>

## Crawler integration

The audited unchanged crawler revision is
`f2c3a591af66e9a424178d9fbc5836740a71e896`. All nodes and CLI processes must share
a credential-free `SWARMCRAWL_REDIS_URL` (for example,
`redis://127.0.0.1:6380/15`) and `SWARMCRAWL_JOB_NAMESPACE`. Point
`SWARMCRAWL_CONFIG_DIR` at an absent disposable directory to avoid saved deployment
settings. Build outputs and runtime state stay outside the crawler checkout.

Follow the [reproduction guide](VERIFICATION.md#reproduce-from-fresh-source) for
source acquisition, exact script checks, and full-process one-/three-node
verification against Redis. External checks require Python **3.9+**, Unix signals,
and a Redis **7.4.x** executable. For an interactive walkthrough, use the
[multi-client and crawler demo](demo/README.md); crawler cluster-management commands
do not manage Keyper.
