# Keyper

A small, in-memory Redis-compatible service being built in Rust on Tokio.

**Current boundary: RESP2, PING, SELECT, GET, bare SET, EXISTS, LPUSH, RPUSH,
BLPOP, BRPOP, INCR, SADD, SREM, SISMEMBER, SCARD, and SMEMBERS.**
Sixteen in-memory databases hold strings, lists, or sets and belong to one
dedicated state thread, separate from asynchronous networking. Blocking
pops include checked deadlines, disconnect cancellation, and bounded shutdown.
Ordered pipelining and the unchanged crawler's Redis 1.7.1 RESP2 client setup
are verified. Transactions, Lua, and full crawler interoperability are not
implemented yet.

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

The only direct runtime dependency is Tokio **1.53.1**, pinned exactly, with default
features disabled. Enabled features provide the multithreaded runtime/macros,
TCP, buffered asynchronous I/O, signals, timers, and `sync` for bounded work and
one-shot reply channels. Tests add `test-util` for controlled-clock deadlines
and Redis **1.7.1** (exact, default features off, `tokio-comp`) to exercise the
crawler's actual client library against owned Keyper servers. The Redis crate
and its locked transitive dependencies are test-only, not a server/proxy backend.
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
for a producer; future Lua shutdown behavior is not yet certified. There is no
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

Command names are ASCII case-insensitive; keys/values are opaque bytes, including
empty, NUL, CR/LF, and non-UTF-8 data. Empty and absent values are distinct.
Indices use canonical decimal integer syntax (no `+`, whitespace, leading zeros,
`-0`, or fractions). Invalid indices, wrong arity, SET options, empty command
arrays, and unsupported commands return `ERR` replies without data mutation; the
connection remains usable. Pushes create a list when the key is absent, and
return `WRONGTYPE` without changing an existing non-list. GET rejects non-strings
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
may intervene between its commands. Only each supported command is atomic;
transactions/scripts are not implemented yet.

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
the EOF cancellation policy above. Full crawler commands still need the audited
extra commands, transactions, and genuine Lua; client compatibility alone does
not certify crawler jobs.

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
Socket tests own loopback servers on ephemeral ports, bound their waits, and
clean up on failure. Controlled-clock tests check write deadlines; injected
broken/short writes check delivery failures without depending on TCP timing.
Repeated schedules supplement the ownership argument below, not a proof or
certification of future batch behavior or crawler compatibility.

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

### Layout

- `src/config.rs`: configuration and argument validation, without network I/O.
- `src/resp.rs`: request framing, resource bounds, and RESP2 reply encoding.
- `src/command.rs`: command admission, PING, and connection-local selection.
- `src/state.rs`: typed database indices, sixteen maps, bounded channels, owner thread.
- `src/state/set.rs`: dense set members with a hash index for constant-time membership/removal.
- `src/state/numeric_sets_tests.rs`: numeric/set semantics and index churn checks.
- `src/state/blocking.rs`: pop eligibility, waiter indexes, owner loop, and deadlines.
- `src/state/blocking/tests.rs`: deterministic owner-level blocking tests.
- `src/server.rs`: loopback listener, ordered connection tasks, writes, supervision.
- `src/server/input.rs`: bounded input queue and EOF detection during waits/writes.
- `src/server/tests.rs`, `src/server/blocking_tests.rs`: transport/failure and real-socket blocking checks.
- `src/main.rs`: arguments, two-worker Tokio entry point, signals, diagnostics.
- `tests/cli.rs`, `tests/transport.rs`, `tests/databases.rs`: process/socket checks.
- `tests/pipelining.rs`: raw pipelines and actual Redis 1.7.1 startup/multiplexing.
- `tests/numeric_sets.rs`: numeric/set wire, boundary, and concurrent allocation/dedup tests.
- `tests/compare_numeric_sets.py`: opt-in comparison with an owned reference Redis.
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
successful increments allocate distinct exact IDs until signed overflow. These
are individual operations, not a substitute for future script/EXEC atomicity.
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
scan or copy is involved. INCR parses at most 20 bytes and uses checked i64
arithmetic. Sets use a dense member vector plus a hash index: average O(1)
membership/cardinality/insert/remove, amortized O(N) for N SADD/SREM arguments,
and O(current cardinality) SMEMBERS even after heavy removal (rather than scanning
a sparse hash table's historical capacity). Swap-removal leaves member order
unspecified. This direct layout stores each member's bytes twice; no additional
dependency is needed. Hashing/copying/serialization also costs the number of
bytes involved; replacing a collection additionally frees its old elements.
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
boundary is tested with simulated multi-command units, **not yet EXEC/EVAL**.
Future batch integration must preserve this rule. If an atomic batch fills b
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

Next: the audited hash commands, followed by the remaining list/key commands.
Actual atomic-batch integration remains unimplemented/unverified.

Later work includes genuine Lua 5.1 and MULTI/EXEC/DISCARD for an **unmodified**
external crawler. Crawler sources and Git state must not be changed;
compatibility code and harnesses belong here.
Neither crawler interoperability nor any live demo has been verified. Broad
Redis compatibility, RESP3, authentication, pub/sub, and public-network hardening
are not claimed.
