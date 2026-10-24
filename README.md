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

Keyper binds to **127.0.0.1** by default. Omitting `--port` uses **6379**;
`--port 0` selects an available port and prints the address. `--bind IP` changes
only the listening address; it does not change port behavior. Both options take
separate values, may appear in either order, and may each be specified only once.
Use `--help` for usage.

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

### Trusted LAN access

- **No authentication, authorization, or TLS:** anyone who can connect can read
  and modify any database and execute Lua. Traffic is plaintext. Never expose
  Keyper to the Internet or configure public port forwarding.
- **Before binding beyond loopback, configure the host/network firewall** to
  allow the chosen TCP port only from trusted client IPs, denying all others.
  Apply this to IPv4 and IPv6 as applicable. A private IP alone is not access control.

On the Keyper host, bind its **specific local LAN IP** (replace the example with
an address assigned to that host):

```sh
./target/release/keyper --bind 192.168.1.10 --port 6380
```

From a trusted client on that LAN:

```sh
redis-cli -2 -h 192.168.1.10 -p 6380 PING
```

- `--bind` accepts one literal IPv4 or IPv6 address, not a hostname, interface
  name, address list, or `IP:port`. IPv6 literals are unbracketed here
  (for example, `--bind ::1`); scoped addresses such as `fe80::1%eth0` are not supported.
- `--bind 0.0.0.0` listens on all IPv4 interfaces; `--bind ::` listens on all
  IPv6 interfaces and may also accept IPv4 depending on the OS. Prefer a specific
  LAN IP to these wildcard addresses. Clients connect to the host's actual IP,
  not a wildcard address. An IPv6 listener does not promise dual-stack access.
- Invalid arguments, missing option values, or duplicate options exit with status
  **2**. A bind failure exits with status **1**, without falling back to loopback
  or another port. Successful startup prints the actual listening endpoint.

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

Use only trusted clients and scripts, locally or on a
[firewall-restricted trusted LAN](#trusted-lan-access). Lua has a fresh, host-restricted
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

Core tests require working **IPv4 and IPv6 loopback**, but no external Redis.
They cover bind/port validation and startup diagnostics, framing,
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

For nodes or CLI processes on other trusted LAN machines, follow the
[LAN firewall/security guidance](#trusted-lan-access) above and set the same
credential-free endpoint on **every** crawler client, for example:

```sh
export SWARMCRAWL_REDIS_URL=redis://192.168.1.10:6380/15
```

Use the Keyper host's actual reachable IP and selected port, retaining the shared
database and namespace. IPv6 URLs require brackets, for example
`redis://[fd00::10]:6380/15`. Existing crawler flags/environment settings suffice;
do not use its authenticated Docker `cluster init/join` path to manage Keyper.

Follow the [reproduction guide](VERIFICATION.md#reproduce-from-fresh-source) for
source acquisition, exact script checks, and full-process one-/three-node
verification against Redis. External checks require Python **3.9+**, Unix signals,
and a Redis **7.4.x** executable. For an interactive walkthrough, use the
[multi-client and crawler demo](demo/README.md); crawler cluster-management commands
do not manage Keyper.
