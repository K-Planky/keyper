# Keyper

A small, in-memory Redis-compatible service being built in Rust on Tokio.

**Current boundary: RESP2, PING, SELECT, GET, bare SET, EXISTS, LPUSH, RPUSH,
BLPOP, BRPOP, INCR, SADD, SREM, SISMEMBER, SCARD, SMEMBERS,
HSET, HGET, HDEL, HEXISTS, HLEN, HGETALL, HVALS, TYPE, LLEN, LINDEX, LPOP,
MULTI, EXEC, DISCARD, and EVAL (30 commands).**
Sixteen in-memory databases hold strings, lists, sets, or hashes and belong to one
dedicated state thread, separate from asynchronous networking. Blocking
pops include checked deadlines, disconnect cancellation, and bounded shutdown.
Ordered pipelining and the unchanged crawler's Redis 1.7.1 RESP2 client setup
are verified, as are isolated transactions and the crawler's six-read snapshot
composition. Genuine sandboxed Lua 5.1 EVAL executes on that same owner,
including inside EXEC, with no intermediate database or waiter visibility.
The five unchanged crawler script compositions pass component checks against
Redis 7.4.0. Full-process checks also pass with the pinned unchanged crawler:
one/three nodes, concurrent jobs, exact local HTTP counts and statistics,
progress/follow, retained results, abort and failure. See the
[process suite](#optional-full-process-crawler-verification) and
[interactive demo](demo/README.md). This is a tested subset, not full Redis.

## Prerequisites and build

Install Rust **1.99.0**, including rustfmt and Clippy, with
[rustup](https://rustup.rs/):

```sh
rustup toolchain install 1.99.0 --profile minimal --component rustfmt --component clippy
```

A native C compiler, archiver, and platform linker are also required (for example,
`build-essential` on Debian/Ubuntu or Xcode command-line tools on macOS). Cargo
builds and statically links genuine Lua **5.1.5** from the locked `lua-src` crate;
no system Lua installation is used. Linux x86_64 is the verified build target.
The checked-in `rust-toolchain.toml` selects this toolchain automatically.
`Cargo.lock` is tracked; use `--locked` to avoid unintended dependency updates.
The first build needs crates.io access unless the locked dependencies are cached.

```sh
cargo build --locked
cargo run --locked -- --help
```

Direct runtime dependencies are Tokio **1.53.1** and mlua **0.12.2**, pinned
exactly with default features disabled. mlua enables only `lua51` and `vendored`;
it provides the synchronous Lua boundary, not a Redis backend. Tokio's enabled
features provide the multithreaded runtime/macros,
TCP, buffered asynchronous I/O, signals, timers, and `sync` for bounded work and
one-shot reply channels. The lock also includes mlua's native build helpers
(including `luajit-src`), but the selected runtime is Lua 5.1, not LuaJIT.
Tests add `test-util` for controlled-clock deadlines
and Redis **1.7.1** (exact, default features off, `tokio-comp`) to exercise the
crawler's actual client library against owned Keyper servers. The Redis crate
and its locked transitive dependencies are test-only, not a server/proxy backend.
No Redis installation, sibling repository, separate Lua interpreter/development
package, or local planning documents are needed for the basic build/tests.

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
redis-cli -2 -h 127.0.0.1 -p 6380 LPUSH queue a b
# (integer) 2
redis-cli -2 -h 127.0.0.1 -p 6380 RPUSH queue c d
# (integer) 4
# The list is now [b, a, c, d].
redis-cli -2 -h 127.0.0.1 -p 6380 BRPOP queue 0
# 1) "queue"
# 2) "d"
redis-cli -2 -h 127.0.0.1 -p 6380 BLPOP queue 0
# 1) "queue"
# 2) "b"
redis-cli -2 -h 127.0.0.1 -p 6380 GET queue
# (error) WRONGTYPE Operation against a key holding the wrong kind of value
redis-cli -2 -h 127.0.0.1 -p 6380 SET queue replaced
# OK
redis-cli -2 -h 127.0.0.1 -p 6380 GET queue
# "replaced"
redis-cli -2 -h 127.0.0.1 -p 6380 INCR next-id
# (integer) 1
redis-cli -2 -h 127.0.0.1 -p 6380 SADD seen page-a page-b page-a
# (integer) 2
redis-cli -2 -h 127.0.0.1 -p 6380 SISMEMBER seen page-a
# (integer) 1
redis-cli -2 -h 127.0.0.1 -p 6380 SCARD seen
# (integer) 2
redis-cli -2 -h 127.0.0.1 -p 6380 SMEMBERS seen
# "page-a" and "page-b", in unspecified order
redis-cli -2 -h 127.0.0.1 -p 6380 SREM seen page-a page-a missing
# (integer) 1
redis-cli -2 -h 127.0.0.1 -p 6380 HSET record state running processed 0 state done
# (integer) 2; repeated fields count once, and the last value wins
redis-cli -2 -h 127.0.0.1 -p 6380 HGET record state
# "done"
redis-cli -2 -h 127.0.0.1 -p 6380 HEXISTS record processed
# (integer) 1
redis-cli -2 -h 127.0.0.1 -p 6380 HLEN record
# (integer) 2
redis-cli -2 -h 127.0.0.1 -p 6380 HGETALL record
# Alternating field/value strings: state, done, processed, 0 (pair order unspecified)
redis-cli -2 -h 127.0.0.1 -p 6380 HVALS record
# "done" and "0", in unspecified order
redis-cli -2 -h 127.0.0.1 -p 6380 HDEL record processed processed missing
# (integer) 1
redis-cli -2 -h 127.0.0.1 -p 6380 RPUSH list-demo one two three
# (integer) 3
redis-cli -2 -h 127.0.0.1 -p 6380 TYPE list-demo
# list
redis-cli -2 -h 127.0.0.1 -p 6380 LLEN list-demo
# (integer) 3
redis-cli -2 -h 127.0.0.1 -p 6380 LINDEX list-demo -1
# "three"
redis-cli -2 -h 127.0.0.1 -p 6380 LPOP list-demo 0
# (empty array); the list is unchanged
redis-cli -2 -h 127.0.0.1 -p 6380 LPOP list-demo
# "one"
redis-cli -2 -h 127.0.0.1 -p 6380 LPOP list-demo 10
# 1) "two"
# 2) "three"
redis-cli -2 -h 127.0.0.1 -p 6380 TYPE list-demo
# none; removing the last element deletes the key
```

To demonstrate blocking, run this in another terminal:

```sh
redis-cli -2 -h 127.0.0.1 -p 6380 BLPOP blocking-demo 0
# Waits indefinitely (Ctrl-C cancels this client).
```

While it waits, the original terminal can still PING, access other databases,
and release it:

```sh
redis-cli -2 -h 127.0.0.1 -p 6380 PING
# PONG
redis-cli -2 -h 127.0.0.1 -p 6380 RPUSH blocking-demo delivered
# (integer) 1; the waiting terminal prints ["blocking-demo", "delivered"].
redis-cli -2 -h 127.0.0.1 -p 6380 BRPOP missing-list 0.05
# (nil) after at least 50 ms (plus scheduling/transport time).
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
running. Pending finite/indefinite blocking requests are canceled without waiting
for a producer. Running scripts are not interrupted by disconnect/shutdown;
a nonterminating script reaches the same fatal drain deadline (process-tested on
Linux). There is no persistence; every explicit restart starts empty.

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
| `GET key` | Bulk string value, null bulk if absent, or `WRONGTYPE` for a non-string |
| `SET key value` | Simple string `OK`; overwrites any type with a string; **no options** |
| `EXISTS key [key ...]` | Integer count for any type; repeated existing keys count repeatedly |
| `LPUSH key value [value ...]` | Integer length after inserting all values at the head, in argument order |
| `RPUSH key value [value ...]` | Integer length after inserting all values at the tail, in argument order |
| `BLPOP key [key ...] timeout` | Two-bulk-string array `[key, head]`, or null array on timeout; 1–5 keys |
| `BRPOP key [key ...] timeout` | Two-bulk-string array `[key, tail]`, or null array on timeout; 1–5 keys |
| `INCR key` | Signed 64-bit integer after increment; absent starts at zero; stores exact decimal text |
| `SADD key member [member ...]` | Integer count of newly added members, ignoring duplicates |
| `SREM key member [member ...]` | Integer count of removed members; deletes an emptied set key |
| `SISMEMBER key member` | Integer 1 if present, otherwise 0 (including absent key) |
| `SCARD key` | Integer member count, or 0 for absent key |
| `SMEMBERS key` | Unordered array of bulk-string members; empty array for absent key |
| `HSET key field value [field value ...]` | Integer count of new fields; existing fields are overwritten; last repeated value wins |
| `HGET key field` | Bulk string value; null bulk for an absent field/key |
| `HDEL key field [field ...]` | Integer count of removed fields; repeated fields count once; deletes an emptied hash key |
| `HEXISTS key field` | Integer 1 if the field exists (even with an empty value), otherwise 0 |
| `HLEN key` | Integer field count, or 0 for an absent key |
| `HGETALL key` | Flat array of alternating bulk-string fields/values; empty array for an absent key; pair order unspecified |
| `HVALS key` | Unordered bulk-string value array, retaining duplicates; empty array for an absent key |
| `MULTI` | Simple `OK`; begin a connection-local transaction queue |
| `EXEC` | Array of queued replies, or EXECABORT for a rejected transaction |
| `DISCARD` | Simple `OK`; discard the open transaction without execution |
| `EVAL script numkeys [key ...] [arg ...]` | Lua 5.1 return converted to RESP2; isolated execution, including retained writes on runtime errors |
| `TYPE key` | Simple string `none`, `string`, `list`, `set`, or `hash` |
| `LLEN key` | Integer list length, or 0 for an absent key |
| `LINDEX key index` | Bulk string at a zero-based index; negative indices count from the tail; null bulk if absent/out of bounds |
| `LPOP key [count]` | Without count: head bulk string or null bulk; with count: array of up to count head elements, or null array if the key is absent |

Command names are ASCII case-insensitive; keys/values are opaque bytes, including
empty, NUL, CR/LF, and non-UTF-8 data. Empty and absent values are distinct.
Integer arguments use canonical decimal syntax (no `+`, whitespace, leading zeros,
`-0`, or fractions). Invalid numeric arguments (subject to LINDEX's lookup order
below), wrong arity, SET options, empty command arrays, and unsupported commands
return `ERR` replies without that command's data mutation; the connection remains usable.
An enclosing script/transaction does **not** roll back earlier successful commands.
Pushes create a list when the key is absent, and return `WRONGTYPE` without changing an existing non-list. GET rejects non-strings
without mutation; bare SET replaces any supported type with a string. LPUSH
reverses its supplied values at the head (`LPUSH k a b c` yields `[c, b, a]`); RPUSH appends them
(`RPUSH k a b c` yields `[a, b, c]`). Duplicates and empty/binary elements are
retained. The entire variadic push, including its returned length, is atomic.
PING is exempt from linearizability and excluded from operation histories.

Requests must be arrays of **non-null bulk strings**. The decoder consumes one
complete request at a time, preserving bytes following it. Partial TCP reads and
coalesced frames work without losing boundaries. The encoder supports simple
strings, errors, signed 64-bit integers, bulk/null bulk strings, and nested/null
arrays; blocking pops expose pair/null arrays. Null and empty values remain
distinct. CR/LF in status/error text are replaced with spaces to
prevent reply injection; bulk data is never altered.

Malformed framing or a request-framing limit violation produces a best-effort
`ERR Protocol error` reply and closes that connection without resynchronizing.
EOF within a request discards the incomplete request and closes silently; EOF
between requests closes normally. Socket/write failures affect only that client;
a connection-task panic stops the service. Half-close continuation is not a
supported client mode.

INCR accepts only canonical signed-64-bit decimal strings, with no floating-point
conversion (including beyond 2^53). Invalid stored text returns `ERR value is not
an integer or out of range`; incrementing 9223372036854775807 returns `ERR
increment or decrement would overflow`. Both errors preserve the old value;
non-string keys return `WRONGTYPE`. A successful INCR remains a string for GET.

Set commands reject non-set values without mutation. SADD creates a set on a
missing key; missing-key reads/SREM do not create one. Members are opaque binary
strings, not numbers: `1`, `01`, and an empty member are distinct. SADD/SREM
apply their whole variadic operation before any other command observes it;
SMEMBERS is a single atomic snapshot, not ordered output. SET can replace a set
with a string; EXISTS counts any supported type. Set creation/deletion does not
wake blocked list pops; subsequent list activity still rechecks key/type priority.

Hash commands likewise reject non-hash types without mutation. HSET creates a
hash only after all field/value pairs pass arity validation; an unmatched field
returns an error without applying even the valid prefix. HGET distinguishes a
missing field (null) from a present empty value. Fields and values are opaque
bytes; decimal counters are never converted to numbers by hash storage. HSET
and HDEL apply their whole argument list atomically. Each HGETALL or HVALS is
one indivisible snapshot of that hash, not several independent field reads.
**Separate commands, even pipelined, are not a coherent multi-key snapshot**;
use MULTI/EXEC for a coherent multi-key snapshot. SET replaces hashes;
EXISTS counts them; hash creation/deletion does not wake blocked list pops.

TYPE never creates a key and reports only the types implemented here. LLEN,
LINDEX, and LPOP reject non-lists without mutation. LINDEX accepts signed-64-bit
indices: 0 is the head, -1 the tail, and any out-of-bounds index returns null.
As in Redis 7.4, it checks existence/type **before** converting the index: an
absent key returns null and a non-list returns WRONGTYPE even for invalid text.
LPOP instead validates its optional **nonnegative signed-64-bit count first**,
returning `ERR value is out of range, must be positive` for invalid/negative
counts (zero is valid). `LPOP key 0` returns an empty array for a list, null array
for an absent key, and WRONGTYPE for a non-list. Supplying count 1 still returns
an array, not a scalar. Huge valid counts are capped by the current list length,
not used as allocation sizes. All counted removals and last-element key deletion
are one atomic operation. List reads and empty LPOP do not wake/reorder blocked
waiters; existing waiters get their post-push service before a later LPOP runs.
Separate LINDEX and LPOP calls do **not** form an atomic claim; that requires the
EVAL boundary used by the crawler.

### Transactions

`MULTI` starts a connection-local queue, without locking or changing the database.
Supported commands, including PING, return **QUEUED** until `EXEC` executes them
in order as one isolated owner operation. EXEC returns an array containing one
reply per queued command (nested arrays and errors retain their RESP2 types);
an empty transaction returns an empty array. `DISCARD` drops the queue and exits
transaction mode. Both EXEC and DISCARD restore normal command processing.

Use **one connection**, for example on the disposable endpoint above:

```sh
redis-cli -2 -h 127.0.0.1 -p 6380 <<'EOF'
MULTI
SET tx-demo 0
INCR tx-demo
GET tx-demo
EXEC
EOF
# OK, three QUEUED replies, then the EXEC array [OK, 1, "1"].
```

- Unknown commands and admission-arity errors dirty the transaction: EXEC returns
  `EXECABORT` and executes nothing. Bare SET's no-options rule and the five-key
  pop bound remain admission restrictions. Further valid requests still receive
  QUEUED, but a dirty queue never runs. DISCARD clears dirty state too.
- Redis distinguishes admission arity from execution-time form checks: excessive
  PING/LPOP arguments and an unpaired HSET field after its minimum arguments
  **queue**, then return errors inside EXEC. Numeric/type errors are likewise
  deferred. Runtime errors retain earlier writes and **do not skip later commands**.
  Transactions provide isolation, **not rollback**.
- Nested MULTI errors immediately without replacing/dirtying the original queue.
  EXEC/DISCARD without MULTI error. Wrong-arity EXEC is special: it immediately
  returns `EXECABORT` and discards/exits the transaction, even if already dirty.
  Wrong-arity MULTI/DISCARD dirty an open transaction instead.
- SELECT inside MULTI queues a late-selection error; EXEC cannot change the
  database. MULTI itself consumes the first-command selection opportunity.
- BLPOP/BRPOP inside EXEC never wait: they immediately pop if possible, otherwise
  return a null array. Existing blocked clients run **after the entire batch and
  its reply capture**, rechecking final lists and original key order. A list
  pushed then emptied/replaced cannot leak intermediate items to a waiter.
- Disconnect before EXEC drops unexecuted queue contents. Cancellation before
  owner execution can skip EXEC; once it starts, the entire batch completes even
  if its reply is lost. This is an ambiguous outcome, never automatic retry or
  rollback. Shutdown drops queues and drains an already-running owner unit.

The unchanged crawler's `redis::pipe().atomic()` snapshot composition is verified:
HGETALL(meta), SCARD(seen), LLEN(frontier), HLEN(in-flight), HGETALL(stats),
HGETALL(extensions), in that order. EVAL also queues real owner work, with
numkeys/source validation deferred until EXEC; script errors are array entries,
and later queued commands still execute. WATCH/CAS and persistence are not provided.
See the [official transaction semantics](https://redis.io/docs/latest/develop/using-commands/transactions/).

### Resource limits and ordering

These explicit local-service limits bound untrusted allocations and connection
work; they are not the real Redis server's defaults:

- **256** active connection tasks; further connections wait in the OS backlog
  until a slot becomes available (the OS may refuse excess connections).
- **1024** bulk-string arguments, including the command name, per request.
- **8 MiB** per bulk string; **16 MiB** per encoded request including headers.
- At most **20 decimal digits** in each length header; sizes are checked before
  allocation. Lengths are unsigned decimal; negative/null request lengths fail.
- One request/reply in execution per connection, an **8 KiB** socket read buffer,
  and a **five-second** deadline for writing each complete reply.
- While execution or writing waits, up to **16 MiB** of additional input is queued
  without execution. Reads continue to detect EOF even behind pipelined bytes.
  Exceeding this queue closes the connection without a guaranteed error reply,
  cancels unstarted/pending work, and discards queued commands. This explicit
  overload policy avoids an unbounded buffer or EOF hidden behind a full queue.
- Each transaction retains at most **1024 commands** and **16 MiB** charged as
  argument payload bytes plus 32 bytes per argument (including command names).
  Exceeding either limit returns `ERR transaction queue limit exceeded`, dirties
  the transaction, and frees its queued payloads; EXEC then returns EXECABORT.
  These bounds are separate from unread pipelined input. EXEC replies are fully
  materialized; repeated large reads can still use substantial memory.
- A shared work channel with **256** queued operations and one one-shot reply
  channel per operation. Full-queue sends await capacity without blocking Tokio.
  Stored database contents have no total size limit or eviction policy.

Idle/partial request reads have no deadline, but consume a connection slot and
bounded memory. A slow reader cannot retain database ownership or block another
connection's I/O. This is bounded per connection, not protection against a
hostile local user exhausting available memory.

The read/execute/write loop preserves connection order, including across a
blocking pop: queued commands cannot unblock their own connection or overtake
its current command/reply. Separate connections progress independently. EOF
ends the session, discarding unexecuted input rather than continuing a half-closed
pipeline. A pipeline is **not atomic**: its prefix is visible and other clients
may intervene between its commands. Explicit MULTI/EXEC batches and individual
EVAL scripts execute atomically; queuing alone does not acquire database ownership.

### RESP2 client startup and pipelining

Clients may send multiple outstanding requests without waiting for replies.
Within the connection/resource limits above, each command receives one reply in
connection order, including command errors; malformed framing instead closes
the connection after a best-effort protocol error. Binary values, nulls, and
empty strings keep their RESP2 types.
The implementation intentionally processes/writes one command at a time, not
parallel tasks or an unbounded reply queue. Slow readers and blocking pops cause
**head-of-line blocking on that connection**, including PING and other cloned
multiplexed handles. Use a separate connection for the producer of a blocked pop;
a queued push cannot release its own connection. Other connections can progress.
Pipeline in bounded batches and drain replies; the input/write/work limits above
still apply. There is no promise of Redis-equivalent pipeline throughput.

The unmodified Redis crate **1.7.1**, with default features disabled and
`tokio-comp`, works with credential-free `redis://127.0.0.1:6380/0` (default
RESP2) or `/1` through `/15`. Its normal startup pipeline is:

| Database | Requests, in order |
| --- | --- |
| 0 | `CLIENT SETINFO LIB-NAME redis-rs`, `CLIENT SETINFO LIB-VER 1.7.1` |
| 1–15 | `SELECT index`, then those two metadata requests |

Keyper returns honest unsupported-command errors for **both** CLIENT requests.
The library consumes/ignores those responses as documented by
[CLIENT SETINFO](https://redis.io/docs/latest/commands/client-setinfo/), but
propagates SELECT failures. No metadata-suppression option or fake success is
needed. Metadata still consumes the startup selection window on DB 0; reconnect
to choose a database. AUTH, HELLO/RESP3, TLS, and broad CLIENT support are not
provided. Normal `redis-cli -2` commands work; its `--pipe` bulk-loader mode uses
unsupported ECHO/leading separators and is not claimed.

The audited external crawler is `K-Planky/swarmcrawl` revision
`f2c3a591af66e9a424178d9fbc5836740a71e896`. Use its existing
`--redis-url` / `SWARMCRAWL_REDIS_URL` for the credential-free RESP2 URL, and
`--namespace` / `SWARMCRAWL_JOB_NAMESPACE` consistently across CLI/nodes. Set
`SWARMCRAWL_CONFIG_DIR` to an absolute **nonexistent disposable path outside its
checkout**, so saved deployment metadata cannot interfere. Do not use its
`cluster` lifecycle commands to manage Keyper or change any crawler files.
Its connection setup uses `AsyncConnectionConfig::new()` with both
`set_connection_timeout(None)` and `set_response_timeout(None)`, wrapped in its
own bounded operation timeout. Our tests exercise that exact setup independently:

```sh
cargo test --locked --test pipelining
```

Dropping a caller's Redis-client query future is **not** retracting a dispatched
request: the multiplexed driver can keep its connection/work alive and drain the
reply. Later cloned queries remain behind it. No retry or rollback is implied;
mutations with lost responses remain ambiguous. Actual socket closure follows
the EOF cancellation policy above. General EVAL is implemented; client, transaction,
and general-script compatibility alone do not certify the unchanged crawler jobs.

## Atomic EVAL and Lua 5.1

`EVAL script numkeys [key ...] [arg ...]` runs genuine Lua **5.1.5**, via mlua
**0.12.2**, on the dedicated state owner. No known-script parsing/hash dispatch,
native crawler substitutes, or Redis proxy is used. The callback-based library
`keyper::scripting::execute` owns language/environment/conversion; the owner
supplies the same real database command semantics used by ordinary clients.
Ownership lasts through the **whole script and return capture**, including when
EVAL is inside EXEC. No other database operation, timer/cancellation decision,
or waiter consumption can observe intermediate effects. No waits or I/O occur
in a script callback.

```sh
redis-cli -2 -h 127.0.0.1 -p 6380 EVAL \
  "redis.call('SET',KEYS[1],ARGV[1]); return {redis.call('TYPE',KEYS[1]),redis.call('GET',KEYS[1])}" \
  1 script-demo hello
# 1) string (status reply); 2) "hello" (bulk string)
redis-cli -2 -h 127.0.0.1 -p 6380 EVAL \
  "redis.call('SET','script-kept','yes'); return redis.call('LPUSH','script-kept','bad')" 0
# WRONGTYPE; the successful SET is retained, not rolled back.
redis-cli -2 -h 127.0.0.1 -p 6380 GET script-kept
# "yes"
```

- `numkeys` is a canonical nonnegative signed-64-bit integer no greater than the
  remaining argument count. Validation occurs before source compilation, at
  execution time (also in EXEC). Zero keys and more than five keys are valid.
  KEYS contains that prefix; ARGV contains the remainder. Bad EVAL admission
  arity dirties a transaction, but bad numkeys/source does not prevent queuing.
- All data access stays in the connection's selected database. Derived or
  undeclared key names work, matching standalone Redis's actual behavior (not
  Redis Cluster's explicit-key guidance). No key whitelist is invented.
- Script calls support the listed command forms. Unknown commands and admission
  arity use Redis's script-specific errors. EVAL and MULTI/EXEC/DISCARD are
  NOSCRIPT; SELECT always returns the project's late-selection error. Form,
  numeric and type errors keep the ordinary command semantics. Bare SET and
  the one-to-five-key pop restriction remain in force inside scripts.
- BLPOP/BRPOP inside EVAL never suspend: pop immediately or return null. That
  null becomes Lua false and returns as a **null bulk**, not a null array.
  Pushes record readiness, but waiters run only after the **outermost** EVAL or
  EXEC captures its result, even on a runtime/return-conversion error. A push
  consumed or replaced within that unit never leaks an intermediate item.
- Compilation errors have no effects. Runtime, command, explicit error replies,
  and return-conversion errors retain earlier writes. `redis.call` stops on an
  uncaught command error; `redis.pcall` allows the script to continue. An EXEC
  containing a failed EVAL continues its later commands. Atomicity is not rollback.
  Error codes/messages follow command semantics; source hash/stack decoration
  and engine-specific Lua diagnostics are not byte-identical to Redis.
- Each invocation creates and drops its own VM, with fresh binary-safe indexed
  KEYS/ARGV. There is no cache or cross-request Lua state. This favors a simple
  isolation argument over VM reuse/performance that has not been required.
- Allowed libraries are base language functions, `string`, `table`, and `math`:
  closures, loops, `ipairs`, `unpack` (including ranges), patterns, formatting,
  concatenation, and digit arithmetic work normally. Direct global assignment
  and undefined global reads fail; library/argument table mutations are local to
  that invocation and cannot survive even an error.
- No filesystem, processes, network, host printing, debug, package/module loader,
  environment-changing functions, `rawset`, or binary chunks are
  exposed. `load`/`loadstring`, `string.dump`, coroutines, and `math.random`/
  `randomseed` are omitted. Lua 5.1's PRNG would otherwise affect process-global
  C state. Redis extensions such as cjson/cmsgpack/struct/bit, script flags,
  caching, replication helpers, and SCRIPT KILL are outside this boundary.
- `redis.call` raises command errors; `redis.pcall` returns `{err=...}`. Language
  `pcall` returns an error string for those errors, while `xpcall` can inspect the
  error table, matching Redis 7.4. `redis.status_reply` and `redis.error_reply`
  construct the corresponding tables. These are general conversions, not fixed
  crawler replies. Ordinary errors do not undo callback effects. Rust callback
  panics **cannot** be swallowed by any of the protected-call functions.
- RESP2 bulks stay byte strings; integers become Lua doubles; null bulk/array
  becomes false; status/error becomes `.ok`/`.err`; arrays nest. Returning Lua
  strings/numbers/true/false/nil gives bulk/integer/1/null/null. Numeric fractions
  truncate; indexed tables end at the first nil and ignore associative fields;
  `.err`/`.ok` string fields take precedence. Functions become null. Nonfinite or
  out-of-i64 numeric returns become i64::MIN, matching Redis 7.4/x86_64; keep such
  data as strings instead. Status/error
  lines truncate at NUL and sanitize CR/LF; bulk data is never sanitized.
- Whole counters must remain **decimal byte strings**, not pass through doubles.
  Exact IDs beyond 2^53 use INCR followed by GET; individual-digit arithmetic can
  preserve u64 totals. Number arguments use Redis's conservative integer range
  and fpconv rounding/notation, not Lua `tostring` or Rust Display. The small
  safe-Rust adaptation in `src/scripting/fpconv.rs` retains the upstream Boost
  license and source attribution; reference tests found observable differences
  with both Rust Display and the separate dtoa crate (not adopted).
- Conversion is limited to **128 levels / 1,048,576 nodes** in each direction;
  cycles and excessive replies error instead of overflowing Rust's stack.
  These are not byte or heap quotas. There is no script instruction/time/memory
  quota, preemption, or rollback. A nonterminating script stalls **all database
  work in every namespace**, though independent network workers can still PING.
  Disconnect does not interrupt it. Shutdown waits up to the shared five-second
  deadline, then the process exits nonzero without resetting or resuming state.
  Large scripts/replies can exhaust memory; request/queue limits do not bound
  script-created command arguments or stored data.
  This trusted-local subset is **not** a hostile-code sandbox or public service.

Self-contained checks: `cargo test --locked --test lua_runtime` (language) and
`cargo test --locked --test eval` (real TCP/database bridge). Numeric unit checks
also cover all binary exponents and 100,000 seeded bit patterns. The optional
unchanged-script component suite below adds crawler-specific verification;
neither alone certifies actual crawler processes or HTTP behavior. The separate
[process suite](#optional-full-process-crawler-verification) covers that boundary.

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
values, rejected SET options, atomic repeated-key EXISTS, concurrent SET/GET
outcomes with cross-client real-time fences, list creation/lengths in all sixteen
databases, type errors, SET-over-list, incomplete pushes, and concurrent variadic
push/SET outcomes. State tests inspect internal left/right list order and binary
contents. Owner-level BLPOP/BRPOP tests cover both ends, key/type priority,
FIFO/overlapping/duplicate keys, empty-list deletion, checked double timeouts,
all sixteen namespaces, whole-push ordering, item accounting, and serialized
producer/expiry outcomes. Controlled clocks check deadlines, rearming, indefinite
waits, and no renewal after an empty recheck; a real dedicated-owner test checks
its timer driver and bounded draining. Real-socket blocking tests also cover
unblocking, overlapping FIFO waiters/reentry, independent databases, 32 consumers
with concurrent producers, queued-input ordering, prompt EOF cleanup without
producers, finite timeout/producer/disconnect races, oversized queued input,
large unread/lost pop replies, pending shutdown, and empty restart. A test-only
watch reports actual owner registration counts for synchronization; it does not
alter replies or execution. Pipeline tests cover coalesced/bytewise/uneven
requests, mixed reply types and errors, fatal framing with an unapplied suffix,
large replies with queued input, non-atomic cross-client interleaving, and
blocking/error/partial-frame resumption. The actual Redis client tests cover all
sixteen startup databases, rejected selection, 32 concurrent clones, failing
pipelines that still apply their suffix, and cancellation without reply
misrouting. Overflow/EOF/shutdown checks include queued mutations that must not
self-release a blocked connection. Owner tests inspect all cleanup indexes and
verify cancellation notification without any subsequent commands. Owner tests also
exercise asynchronous backpressure, lost replies, and fail-stop panic supervision.
Numeric/set tests cover signed boundaries and canonical parsing, exact IDs beyond
2^53, overflow/nonmutation, all arities/types, binary/empty/large members,
duplicates, empty-set deletion/recreation, all sixteen databases, dense-index
churn, waiter type priority, fragmented ordered replies, concurrent unique ID
allocation/overflow, and atomic variadic deduplication/removal/member snapshots.
Hash tests cover all seven commands, binary/empty/large fields and values,
missing/null distinctions, duplicate and overwrite counts, duplicate HVALS,
flat RESP2 pairs, arity/nonmutation, type interactions, all sixteen databases,
last-field deletion/recreation, maximum variadic pairs, incomplete framing,
index churn against an independent ordered map, waiter type rechecking, and
concurrent whole-record creation/deletion/replacement with real-time fences.
List/key tests cover all four added commands, exact RESP2 status/scalar/array/null
forms, signed index/count bounds, numeric/type-error precedence, binary/empty/large
values, all sixteen databases, deletion/type transitions, incomplete pop framing,
independent-vector churn, waiter priority, concurrent disjoint counted ranges,
whole-batch length/head observations, and mixed producer/scalar/counted/blocking
pop item accounting with real-time fences.
Transaction tests cover fragmented pipelines/all sixteen DBs, nested reply shapes,
queue control and limits, admission versus runtime form/numeric/type errors,
retained writes, nonwaiting batch pops, pre-execution cancellation, discarded and
truncated queues, unread/lost EXEC replies, and competing EXEC/ordinary increments.
The actual Redis 1.7.1 atomic pipeline decodes the crawler's six-read snapshot
while concurrent batches update all six keys. Synchronized blocked-socket checks
verify final-state service, FIFO/key priority, and no intermediate delivery.
Socket tests own loopback servers on ephemeral ports, bound their waits, and
clean up on failure. Controlled-clock tests check write deadlines; injected
broken/short writes check delivery failures without depending on TCP timing.
EVAL wire tests cover validation phases, all supported command calls, binary
KEYS/ARGV and derived keys across sixteen databases, fresh environments, error
contexts, exact decimal counters, retained writes, EVAL inside EXEC, nonwaiting
pops, fragmented pipelines, incomplete input, and unread/lost replies. Competing
scripts, EXECs and ordinary increments have contiguous batch results and exact
item accounting. Synchronized sockets check deferred EVAL/EXEC waiter service,
including after script errors. On Linux, an owned-process test observes the
state thread's CPU progress during an infinite script, disconnects its client,
checks independent PING, and verifies fatal five-second shutdown; all waits are
bounded and only the owned child is signaled.
Combined owner/socket regressions cover left-to-right multi-key rechecking after
EVAL/EXEC/mixed batches, overlapping FIFO waiters, replacement/type rechecks, and
both explicit timeout/disconnect contender orders without stale consumption.
Repeated schedules supplement the ownership argument below, not a proof or
certification of full-process crawler compatibility.

### Optional numeric/set reference comparison

With Python 3 and a separately installed/built Redis **7.4.x** server on
Linux/macOS/WSL2, run from the repository root:

```sh
cargo build --locked
python3 tests/compare_numeric_sets.py --redis-server /absolute/path/to/redis-server
```

The script starts its own ephemeral Keyper TCP endpoint and a scratch Redis Unix
socket (TCP/persistence disabled), compares **2,512** sequential commands including
errors and deterministic set churn, and stops only its owned processes. SMEMBERS
ordering is normalized; other reply types/values/errors must match exactly.
Startup/I/O/shutdown waits are bounded; no existing Redis or crawler is touched.
This opt-in check is separate from `cargo test` and is not a full Redis/crawler
certification. Verified with Redis **7.4.0**; concurrent contracts are tested by
the Rust suites, not this sequential comparison.

### Optional hash reference comparison

Using the same Python/Redis prerequisites and owned-endpoint policy:

```sh
cargo build --locked
python3 tests/compare_hashes.py --redis-server /absolute/path/to/redis-server
```

Verified with Redis **7.4.0**: **3,101** commands including all hash arity/type
errors, binary/empty fields and values, duplicate updates/deletes, exact decimal
strings, and deterministic churn. Only HGETALL **pair** order and HVALS/SMEMBERS
element order are normalized; duplicate values and field/value associations
must remain intact. The script reuses the numeric/set harness's bounded process
runner. Neither comparison substitutes for concurrent tests, transactions,
Lua, or a full crawler run; missing prerequisites or mismatches exit nonzero.

### Optional list/key reference comparison

Using the same Python/Redis prerequisites and owned-endpoint policy:

```sh
cargo build --locked
python3 tests/compare_lists.py --redis-server /absolute/path/to/redis-server
```

Verified with Redis **7.4.0**: **2,734** commands covering TYPE, LLEN, LINDEX,
LPOP (including optional count), all arities/types, signed boundaries, error
precedence, binary/empty data, deletion/recreation, and deterministic list churn.
Replies are compared **exactly**, without normalization; scalar/null bulk and
array/null array must remain distinct. This reuses the same bounded process
runner; missing prerequisites or mismatches exit nonzero. It does not verify
transactions, scripts, or full crawler jobs. The crawler's original Redis suites
also require test-only SCAN/DEL/LRANGE/LSET/TTL, which remain unsupported; these
Keyper-owned checks are not a claim those suites pass on Keyper.

### Optional transaction reference comparison

Using the same Python/Redis prerequisites and owned-endpoint policy:

```sh
cargo build --locked
python3 tests/compare_transactions.py --redis-server /absolute/path/to/redis-server
```

Verified with Redis **7.4.0**: queue/EXEC/DISCARD state transitions, admission and
runtime errors (including wrong-arity EXEC and variable-arity form checks),
retained writes, nonwaiting pops, nested/binary replies, and the crawler snapshot
composition. Replies match exactly except unknown-command diagnostics: Keyper
intentionally does not reflect potentially sensitive input, so that error's
category is normalized. Project-specific selection/queue/key limits are tested
in Rust, not compared to Redis's different policies. This sequential comparison
does not replace concurrent checks or verify Lua/full crawler jobs.

### Optional Lua language reference comparison

With an explicitly supplied Redis **7.4.x** server (verified: **7.4.0**):

```sh
KEYPER_REFERENCE_REDIS_SERVER=/absolute/path/to/redis-server \
  cargo test --locked --test lua_reference -- --ignored --nocapture
```

This otherwise-ignored test starts only its own disposable loopback Redis,
checks its process identity before fixture work, bounds startup and socket I/O,
and cleans up on failure. Missing tools/mismatches fail, not silently skip. It
compares **1,222** exact general-language/conversion results (including binary
values and seeded double arguments) plus four error categories; engine-specific
stack/source diagnostics are not compared as identical strings. Explicit test
callbacks supply PING/type/integer/error replies; this is **not** Keyper TCP EVAL
or the database bridge. Sandbox exclusions/limits are tested locally rather than
compared with Redis's broader API. No existing Redis or crawler is touched.

### Optional EVAL/database reference comparison

Using the same Python/Redis prerequisites and owned-endpoint policy:

```sh
cargo build --locked
python3 tests/compare_eval.py --redis-server /absolute/path/to/redis-server
```

Verified with Redis **7.4.0**: **8,468** commands, including the general
numeric/set/hash/list corpora through real Lua calls, validation/error phases,
call/pcall/NOSCRIPT, exact decimal counters, derived/binary keys, nonwaiting pops,
retained writes and EXEC-contained EVAL. Scripts normalize only unordered
collection order. Comparison removes Redis's source hash/line suffix from
uncaught command errors; codes/messages otherwise match exactly. Explicit
syntax/runtime/denied-global/host-execution cases compare their error categories,
not engine-specific diagnostics (Redis exposes a limited os table; Keyper none).
Project SELECT, bare SET, five-key and stricter sandbox rules are checked in Rust,
not treated as reference-compatible. Missing prerequisites/mismatches fail.
This is general-script evidence, not unchanged-crawler script/job certification.

### Optional unchanged-crawler script components

Requires Python 3, a Redis **7.4.x** server executable, and a clean, unmodified
checkout of [K-Planky/swarmcrawl](https://github.com/K-Planky/swarmcrawl) at
`f2c3a591af66e9a424178d9fbc5836740a71e896` (repository access may require owner
permission). Obtain that revision in a separate checkout, not by switching or
modifying an existing working tree:

```sh
git clone --no-checkout https://github.com/K-Planky/swarmcrawl /absolute/scratch/swarmcrawl
git -C /absolute/scratch/swarmcrawl checkout --detach f2c3a591af66e9a424178d9fbc5836740a71e896
cargo build --locked
python3 tests/compare_crawler_scripts.py \
  --crawler-checkout /absolute/scratch/swarmcrawl \
  --redis-server /absolute/path/to/redis-server
```

The harness only **reads** this checkout. It checks revision, clean Git status
(with optional locks disabled), and all six Lua source SHA-256s before/after;
it consumes the five exact runtime compositions: submit, worker identity, and
shared protocol plus claim, complete/fail, or abort. No source rewriting, known
script substitutes, crawler build, or crawler runtime state is involved. Both
servers and all data are owned/disposable; existing deployments are never used.
Sockets, barriers, futures, polling, and child cleanup have bounded waits.
A missing prerequisite, changed source, mismatch, or server failure exits nonzero.

On **DB 0 and 15**, the same expected-result/invariant checks run on Keyper and
Redis. They cover concurrent submission/deduplication, independent derived job
keys, exact unique node/job IDs above 2^53 through signed-i64 exhaustion, unique
claims, empty-frontier/nonempty-ownership progress, shared-child deduplication,
concurrent publication with coherent six-read EXEC snapshots, duplicate/invalid
completion, abort/publication races, frozen failure diagnostics, immediately
readable retained final statistics, exact u64 totals/overflow and a small usize
limit, orphan-ID gaps, and malformed/type-corrupt storage without resets.
Unordered hash enumeration is canonicalized; script reply shapes and decimal
strings are exact. Deterministic retained snapshots match the Redis baseline;
nondeterministic race winners are checked against their permitted serial orders,
not forced to match across separate runs. No infeasibly huge collection fixture
is claimed for cardinalities above 2^53.

The publication fixture processes **26 URLs**, counts **13 files**, **1 extension**
(`html`) and **31 words**. Claims move queue entries into ownership within one
EVAL; publication deduplicates children, merges one contribution, releases the
owner and decides completion within another. A concurrent six-read EXEC is
ordered wholly before or after publication, so it cannot observe new statistics
with old ownership or a done marker before final statistics. Each batch uses the
same exclusive state owner as ordinary commands; PING is not a history witness.

The reference-only priority probe uses CLIENT LIST to confirm a blocked waiter
before EVAL/EXEC/mixed batches fill b then a: Redis **7.4.0** selects **a** for
`BLPOP a b 0`, matching this project's left-to-right contract. CLIENT LIST is
not a Keyper command. Default Rust tests independently gate actual Keyper waiter
registration and verify this boundary, including FIFO, timeout and disconnect
cleanup. Other Redis versions are not certified by this result.

These are **component tests**, not full crawler processes, HTTP request counts,
or the original crawler Redis suites. Those suites still require excluded
SCAN/DEL/LRANGE/LSET/TTL helpers; they are not silently skipped and called passes.
The separate process suite below covers actual CLI/nodes/HTTP, not those helpers.

### Optional full-process crawler verification

Requires **Linux/Unix signals**, Python **3.9+**, Rust **1.99.0**/native build tools,
a Redis **7.4.x** executable, and the pinned clean crawler checkout obtained
[above](#optional-unchanged-crawler-script-components). Verified on Linux x86_64
with Python 3.12.3 and Redis 7.4.0. No Docker daemon, source patch, saved deployment
or existing Redis endpoint is used. The first crawler build needs cached crates
or crates.io access; reserve several GB for release/debug build output.

From the Keyper root:

```sh
cargo build --locked
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p 'test_crawler*.py' -v
# --output must not exist and must be outside the crawler checkout.
PYTHONDONTWRITEBYTECODE=1 python3 tests/crawler_process.py \
  --crawler-checkout /absolute/scratch/swarmcrawl \
  --redis-server /absolute/path/to/redis-server \
  --output /absolute/scratch/keyper-process-run \
  --crawler-tests
```

The suite checks the pinned clean source and six Lua digests before/after,
builds the unchanged crawler with `cargo +1.99.0 build --release --locked
--manifest-path ... --target-dir <output>/crawler-target` from outside its
checkout, and owns both loopback database servers, HTTP fixtures and processes.
Keyper uses an OS-selected port; Redis uses a reserved loopback port and must
report its own successful listen (a conflict fails rather than using another
server). Persistence is disabled. All startup, request, body-gate, crawl and
drain waits are bounded. Cleanup releases bodies and interrupts only owned
processes; it never clears an existing database. Logs/builds remain in `--output`
for inspection, including on failure; remove only that owned directory afterward.
Do not run Python with `-O`: the executable test oracles use assertions.

Each endpoint runs **one node / DB 0** and **three nodes / DB 15**, with fresh
namespaces. The actual `check`, `submit`, `jobs`, `status`, `status -f`, `stats`,
and `abort` CLI commands verify:

- Two broad jobs plus a simultaneous narrow job seeded at `/a/leaf-0.html`;
  the shared leaf is fetched **once per job**, not globally skipped. Its narrow
  scope counts one file/two words. Fragment/repeated submission returns retained
  IDs without CLI HTTP traffic. **64 main graph paths** receive 65 GETs (the
  shared leaf twice), including redirect/404; no out-of-scope or extra HEADs.
- Each main job finishes at **32 crawled/discovered, zero frontier/in-flight,
  30 files, four extensions, 52 words**: `html 27`, `jpeg 1`, `jpg 1`, `pdf 1`.
  One/three-node results and exact GET counts equal the real-Redis baseline.
- Body gates hold **10 requests with one node**, and **at least 21 across three
  nodes**, before release. Existing opt-in diagnostics check each node's HTTP
  peak **≤10** (one-node peak exactly 10), drained HTTP/owned/CPU gauges,
  owned-task peak ≤20 and CPU peak ≤4. This verifies real concurrency and each
  cap without assuming equal work allocation. Aggregate fixture counts alone
  do not prove the individual cap.
- A held parent with **empty frontier / one in-flight** remains running, rejects
  partial final stats and publishes its child before done. Follow histories are
  coherent/monotonic and exit; final stats are immediately readable and retained
  from fresh CLI processes after all nodes exit.
- Aborting held work freezes diagnostics and discards late links; a controlled
  503 fails a job and exits its node nonzero, without retry or partial final
  stats. Resubmission does not restart terminal jobs. Namespaces and DBs isolate.

Each case makes **69 GETs across 68 paths**, including delayed, aborted and failed jobs.
`report.json` records final states/counts, node diagnostics, tool/revision and
binary/lock SHA-256s; per-case `cli.jsonl`, `result.json`, and child/server logs
retain the observations. Failures/missing prerequisites exit nonzero, not a skip.

`--crawler-tests` additionally runs the **unmodified default crawler tests**,
its opt-in PING test on **both** servers, and the original pipeline/Redis/job/
node/CLI/distributed suites on the owned **real Redis only**. Omit the flag for
just the four black-box process cases. The original suites' SCAN/DEL cleanup and
LRANGE/LSET/TTL fixtures remain incompatible with Keyper; reference-only passes
are **not** Keyper passes. Docker managed-cluster lifecycle tests and a live
human-assisted demo are not certified here. Core `cargo test` remains independent
of this opt-in suite and external crawler.

For the same fixture, sample results, helper roles, manual commands, reset and
cleanup, use the tracked [interactive demo runbook](demo/README.md).

### Layout

- `src/config.rs`: configuration and argument validation, without network I/O.
- `src/resp.rs`: request framing, resource bounds, and RESP2 reply encoding.
- `src/command.rs`: command admission/form checks, PING, selection and transaction queues.
- `src/scripting.rs`, `src/scripting/`: isolated Lua boundary, RESP2 conversions,
  and Redis-compatible numeric formatting; called synchronously by owner-side EVAL.
- `src/state.rs`: typed database indices, sixteen maps, bounded channels, owner thread.
- `src/state/set.rs`: dense set members with a hash index for constant-time membership/removal.
- `src/state/numeric_sets_tests.rs`: numeric/set semantics and index churn checks.
- `src/state/hash.rs`: dense field/value pairs with a lookup index and unspecified ordering.
- `src/state/hashes_tests.rs`: hash semantics, type interactions, and independent-map churn checks.
- `src/state/lists_tests.rs`: list/key semantics, numeric precedence, and independent-vector churn.
- `src/state/blocking.rs`: pop eligibility, waiter indexes, owner loop, and deadlines.
- `src/state/blocking/tests.rs`: deterministic owner-level blocking tests.
- `src/server.rs`: loopback listener, ordered connection tasks, writes, supervision.
- `src/server/input.rs`: bounded input queue and EOF detection during waits/writes.
- `src/server/tests.rs`, `src/server/blocking_tests.rs`: transport/failure and real-socket blocking checks.
- `src/main.rs`: arguments, two-worker Tokio entry point, signals, diagnostics.
- `tests/cli.rs`, `tests/transport.rs`, `tests/databases.rs`: process/socket checks.
- `tests/pipelining.rs`: raw pipelines and actual Redis 1.7.1 startup/multiplexing.
- `tests/numeric_sets.rs`: numeric/set wire, boundary, and concurrent allocation/dedup tests.
- `tests/hashes.rs`: hash wire shapes, boundaries, and concurrent whole-record operations.
- `tests/lists.rs`: list/key wire shapes, boundaries, counted-pop atomicity and item accounting.
- `tests/compare_numeric_sets.py`: opt-in numeric/set comparison and shared owned-endpoint runner.
- `tests/compare_hashes.py`: opt-in hash comparison with an owned reference Redis.
- `tests/compare_lists.py`: opt-in exact list/key comparison with an owned reference Redis.
- `tests/transactions.rs`: raw transaction semantics and actual-client atomic snapshot/concurrency tests.
- `tests/compare_transactions.py`: opt-in transaction comparison with an owned reference Redis.
- `tests/eval.rs`: EVAL wire semantics, error phases, isolation and concurrent atomic units.
- `tests/compare_eval.py`: opt-in general EVAL/database comparison with owned servers.
- `tests/compare_crawler_scripts.py`: opt-in unchanged-script components and retained-state reference checks.
- `tests/crawler_process.py`: opt-in unchanged crawler build/CLI/node/reference orchestration.
- `tests/crawler_fixture.py`, `tests/test_crawler_fixture.py`: owned gated HTTP oracle/demo and self-contained fixture checks.
- `demo/README.md`: interactive server/client/crawler demo, expectations and owned cleanup.
- `tests/lua_runtime.rs`, `tests/lua_reference.rs`: language/conversion isolation and opt-in references.
- `tests/support/mod.rs`: owned ephemeral servers and bounded socket-test waits.

## Linearizability and failure contract

Under the single-machine/correct-OS-and-hardware assumptions, the dedicated owner
alone accesses the sixteen maps. Every GET, SET, entire variadic EXISTS, or entire
LPUSH/RPUSH runs without interleaving. GET (including a type error) linearizes at
its lookup, SET at insertion/replacement, and EXISTS during its indivisible scan;
it never samples different moments for repeated arguments. A push linearizes
after all insertions, when its resulting length is captured; a wrong-type push
linearizes at the type check without mutation. No other operation can observe a
partial push. Waiter service runs after this complete unit and its reply capture,
not between elements. INCR linearizes at its checked replacement; invalid text,
overflow, and wrong-type outcomes linearize at the indivisible check without
mutation. Entire SADD/SREM operations (including empty-key removal) linearize
after all arguments; SISMEMBER/SCARD at lookup, and SMEMBERS during its indivisible
member snapshot. A concurrent deduplication race adds each member once, and
successful increments allocate distinct exact IDs until signed overflow.
HSET/HDEL linearize after all fields, including last-field key deletion; HGET,
HEXISTS, and HLEN at lookup; HGETALL/HVALS during their indivisible hash snapshot.
Wrong-type outcomes linearize at the type check without mutation. A reader
cannot observe a partially updated variadic record, but separate reads can see
different moments. TYPE, LLEN, and LINDEX linearize at their owner lookup/index
check; LPOP at its entire removal and any empty-key deletion, never between
returned elements. Missing/type/numeric errors are indivisible owner decisions
with no mutation. These individual operations alone are not a substitute for
script/EXEC atomicity. MULTI/QUEUED/DISCARD linearize at connection-local queue
transitions, not database mutations. EXEC submits one owner unit, with no other
client operation or waiter consumption between commands; its entire result and
retained runtime-error writes linearize as that batch. Waiter service follows
reply capture. Cost is the sum of enclosed work plus replies/readiness service,
not a copy of the database. MULTI is O(1); DISCARD frees O(queued commands/bytes).
EVAL (also within EXEC) is likewise one non-interleaved unit: fresh VM setup,
source compilation, all command calls and return capture share the owner order.
Runtime errors retain preceding effects at this same boundary; waiters run after
capture, never inside the script or between an EXEC-contained EVAL and other
queued commands.
Its cost depends on setup, source, executed Lua/commands and conversion; no
whole-database copy, waits or external service are involved.
SELECT linearizes at the connection-local
state transition; input-validation errors linearize at validation without
touching data. Other clients cannot change a connection's selected database.
PING is not in histories.

The owner receives owned commands and returns owned replies. A reply is created
after the operation and before asynchronous socket writing. Thus a response
received before a later invocation fixes their state order; overlapping calls
can take either valid order. No map reference or database ownership survives into
network I/O. Hash-map lookup/insertion is average O(1); EXISTS is O(N) in supplied
key arguments, as in the [Redis command reference](https://redis.io/docs/latest/commands/).
Lists use `VecDeque`, with amortized O(1) insertion at either end and amortized
O(N) for N inserted elements, without shifting the list on every LPUSH. No whole-database
scan or copy is involved. TYPE and LLEN are average O(1); VecDeque provides
O(1) LINDEX access (at least as good as Redis's O(distance) bound, including O(1)
at either end). LPOP is O(number returned), moving only the requested head prefix,
not shifting/copying the remaining list. Huge indices/counts cannot wrap or cause
count-sized preallocation. INCR parses at most 20 bytes and uses checked i64
arithmetic. Sets use a dense member vector plus a hash index: average O(1)
membership/cardinality/insert/remove, amortized O(N) for N SADD/SREM arguments,
and O(current cardinality) SMEMBERS even after heavy removal (rather than scanning
a sparse hash table's historical capacity). Swap-removal leaves member order
unspecified. This direct layout stores each member's bytes twice; no additional
dependency is needed. Hashes similarly use dense field/value pairs and a field
index: average O(1) HGET/HEXISTS/HLEN and field insertion/update/removal, amortized
O(N) HSET/HDEL for N supplied pairs/fields, and O(current field count)
HGETALL/HVALS after churn. Hash fields are stored twice, values once; swap-removal
leaves pair/value order unspecified. Hashing/copying/serialization also costs the
number of bytes involved; replacing a collection additionally frees its old elements.
Databases are isolated namespaces, not independently executing owner threads.

A dropped reply receiver lets the owner skip an operation that has not begun.
Cancellation racing with execution can instead lose the response to an applied
write. Disconnect/write failure is **ambiguous**, never automatic rollback or
retry. An owner panic/channel failure stops serving rather than replacing state;
an explicit restart loses all data. This is not exactly-once delivery, persistence,
or crash recovery.

### Blocking-pop contract

BLPOP/BRPOP accept **one to five key arguments** and a double-precision timeout
in seconds. Duplicate keys count toward five, but are
registered once in their original priority order. Both directions scan keys
left to right: an earlier nonempty list wins over a later wrong type; an earlier
wrong type fails without consuming from a later list. BLPOP removes the head,
BRPOP the tail. Success returns a two-bulk-string array `[key, value]`, removes
an emptied list key, and linearizes at the indivisible eligibility/removal check.

Timeout conversion and the checked monotonic deadline precede any removal.
Signed zero means indefinite waiting; positive fractions round up to milliseconds
(e.g. `0.0011` seconds becomes 2 ms). Negative, malformed, nonfinite, overflowing,
and nonzero-underflow-to-zero inputs fail without mutation. Duration and platform
Instant range are checked. The original deadline starts when the owner begins
that command, not when queued bytes arrive, and never renews after rechecks.

An empty attempt and waiter registration are one owner unit: a producer cannot
slip between them and cause a lost wakeup. The owner then releases execution,
parks on work or its next deadline, and remains available for other commands
and databases. A current-thread Tokio timer driver lives on the dedicated owner
thread; synchronous state work stays off network workers. No map reference or
execution ownership spans a socket or list wait.

After a complete successful push and captured length reply, eligible waiters
run in registration order. Each rechecks its own keys left to right against the
committed state, so variadic LPUSH exposes its final head, never its first
intermediate insertion. Reentering creates a new FIFO position. Only committed
nonempty lists trigger service: SET, failed pushes, or a list filled then
emptied/replaced within one unit do not themselves wake a waiter. The service
boundary is tested with actual EXEC, EVAL, and EVAL inside EXEC, including
multi-key/FIFO, replacement, and explicit timeout/disconnect contender orders.
If an atomic batch fills b
then a for a waiter on `[a, b]`, the recheck must select a; this project's explicit
linearizability requirement takes precedence over Redis prose describing
first-pushed-key priority. No caller may observe a partial atomic batch.

The owner alone decides terminal outcomes and removes every key/deadline index
before replying. Expiry returns a **null array**, never before the original
deadline; scheduling may delay it. A timeout linearization witness is the empty
observation at registration during its pending interval. At an owner-loop
boundary, an already-due timer takes priority over queued work; an executing
producer completes its whole unit and waiter service before the next timer
check. Tests explicitly exercise both serialized orders instead of guessing a
race winner from sleeps. Stale expiry cannot consume or undo a committed item.

Eligibility is O(K) for K supplied keys (at most five). Per-key waiter indexes
avoid scanning unrelated databases/keys; ordered waiter/deadline bookkeeping
adds logarithmic costs in pending waiters, plus eligible rechecks and byte costs.
No polling or task per timeout is needed. Empty rechecks preserve FIFO position
and deadline; terminal cleanup removes indexes rather than leaving tombstones.

Socket EOF, read failure, queued-input overflow, or task cancellation drops the
pending execution future. Its guard first closes the reply receiver, then wakes
the owner through a coalescing notification. The owner removes closed waits and
all their key/deadline indexes even with no producer or finite timer. Cancellation
sweeps are O(W) in pending waiters (at most 256 TCP clients); ordinary readiness
still uses per-key indexes. Notifications contain no payload queue and create
no per-waiter task. Cleanup runs between atomic units, never interrupts one.

Before admission/owner execution, cancellation can skip the operation entirely.
Racing a pop/producer may instead lose an already-committed response. The owner
checks receiver closure before eligibility/removal, but no check can promise
that a client stays alive through delivery. A reply lost after removal **never
restores/requeues** the item; clients must not automatically retry ambiguous
mutations. This is not a reliable work queue or exactly-once processing. Closing
the last work sender also drops all pending waits, including indefinite ones;
normal shutdown never waits for a producer.

## Remaining work

Next: final requirements/readability audit, fresh-checkout acquisition and full
demo rehearsal. General EVAL, transactions, the six-read snapshot, unchanged
scripts, atomic-batch/waiter regressions and four actual crawler/HTTP process
cases pass. This does not certify a live human-assisted demo, post-demo visibility,
grading tag or course submission; those require the owner's participation.
Crawler sources and Git state must not be changed; compatibility code and
harnesses belong here. Broad Redis compatibility, RESP3, authentication, pub/sub,
and public-network hardening are not claimed.
