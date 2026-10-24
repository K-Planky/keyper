# Keyper walkthrough

Run Keyper as a normal local service, then try it from several clients.
Connecting swarmcrawl is optional; Keyper needs neither a crawler nor a separate
Redis server.

Use the [build prerequisites](../README.md#prerequisites-and-build) and install
`redis-cli` for the client examples. All commands start from the **Keyper root**.
Examples assume fresh `demo:*` keys; use a different prefix when repeating them.

## 1. Start Keyper

In the server terminal:

```sh
cargo build --release --locked
./target/release/keyper
```

Wait for `keyper: listening on 127.0.0.1:6379`. This walkthrough uses loopback.
For `--bind IP` LAN access, first follow the
[firewall and no-authentication warnings](../README.md#trusted-lan-access), then
add `-h <Keyper-host-IP>` to every `redis-cli` command and use that IP instead of
`127.0.0.1` in the crawler URL below (bracket IPv6 addresses in URLs).

If 6379 is occupied, use `--port 6380`, add `-p 6380` to client commands, and update
the crawler URL. Do not stop another service to free the port.

## 2. Read, write, and select a database

In a client terminal:

```sh
redis-cli -2 PING                         # PONG
redis-cli -2 SET demo:message hello       # OK
redis-cli -2 GET demo:message             # "hello"
redis-cli -2 EXISTS demo:message          # 1
redis-cli -2 -n 15 GET demo:message        # (nil)
redis-cli -2 GET demo:message             # "hello": DB 0 is unchanged
```

There are 16 independent databases, numbered **0–15**. Each command above opens
a fresh connection; `-n 15` selects DB 15 at connection startup. An interactive
client cannot SELECT after sending another command. SET accepts no options.

Try both ends of a list:

```sh
redis-cli -2 LPUSH demo:list left right   # 2
redis-cli -2 BLPOP demo:list 1            # demo:list / right
redis-cli -2 BRPOP demo:list 1            # demo:list / left
```

## 3. Block one client, keep another working

In the first client terminal:

```sh
redis-cli -2 BLPOP demo:queue 30
```

Before the 30-second timeout, use a second client terminal:

```sh
redis-cli -2 PING                         # PONG while the other client waits
redis-cli -2 RPUSH demo:queue first second # 2
redis-cli -2 BRPOP demo:queue 1            # demo:queue / second
redis-cli -2 BLPOP demo:empty 0.25         # (nil), after at least 0.25 seconds
```

The first client receives `demo:queue / first`. RPUSH returns **2** because the
entire push finishes before the waiting pop consumes an item. A waiting client
does not hold database access; other connections can progress.

If the first wait expires before the push, repeat with a new queue key. Blocking
pops accept at most **five key arguments**; timeout **0** waits indefinitely.

## 4. Try transactions and Lua

Send the transaction on one connection:

```sh
redis-cli -2 <<'COMMANDS'
MULTI
SET demo:counter 10
INCR demo:counter
GET demo:counter
EXEC
COMMANDS

redis-cli -2 EVAL "return redis.call('INCR', KEYS[1])" 1 demo:counter
```

EXEC returns the three results: `OK`, `11`, and `"11"`. EVAL then returns **12**.
Each EXEC or EVAL runs without interleaving with another client's database work.
Runtime errors do not roll back earlier writes. Only run trusted scripts: Lua
has no time or heap quota.

## Optional: connect swarmcrawl

Keep Keyper running. Use a clean, unchanged swarmcrawl checkout at
`f2c3a591af66e9a424178d9fbc5836740a71e896`; see
[source acquisition](../VERIFICATION.md#reproduce-from-fresh-source) if needed.
Replace the source path below. Build outputs stay outside the crawler checkout;
skip the Cargo build if a matching `swarmcrawl` binary is already on PATH:

```sh
export CRAWLER_SRC=/absolute/path/to/unmodified/swarmcrawl
DEMO_WORK=$(mktemp -d /tmp/keyper-demo-XXXXXX)
printf 'Use this directory in every crawler terminal: %s\n' "$DEMO_WORK"
cargo +1.99.0 build --release --locked \
  --manifest-path "$CRAWLER_SRC/Cargo.toml" --target-dir "$DEMO_WORK/crawler-target"
```

Stop if the build fails. In **each crawler terminal**, set `DEMO_WORK` to that
same printed path, then run:

```sh
export PATH="$DEMO_WORK/crawler-target/release:$PATH"
export SWARMCRAWL_REDIS_URL=redis://127.0.0.1:6379/15
export SWARMCRAWL_JOB_NAMESPACE="${DEMO_WORK##*/}"
export SWARMCRAWL_CONFIG_DIR="$DEMO_WORK/absent-config"
swarmcrawl check
```

Expect `Redis connectivity: OK (PONG)`. The absent config directory avoids loading
saved deployment settings; do not create it. All crawler processes must use the
same endpoint, database, and namespace. This setup uses DB **15**, separate from
the Keyper examples in DB 0, and a fresh namespace for each new `DEMO_WORK`.

Start a worker in one configured terminal:

```sh
swarmcrawl node
```

In another configured terminal, submit a site you are authorized to crawl.
Replace the example URL and use the printed job ID instead of `1` if different:

```sh
swarmcrawl submit https://your-site.example/docs/
swarmcrawl status -f 1
swarmcrawl stats 1
```

Start additional local nodes the same way. For other machines, follow the
[LAN setup](../README.md#crawler-integration) and keep the namespace identical,
even when build/config paths differ. No Docker, authentication, or crawler
`cluster` commands are needed; those commands do not manage Keyper.
For deterministic local HTTP fixtures and full one-/three-node checks, see the
[verification guide](../VERIFICATION.md#crawler-checks).

## Shut down

If using swarmcrawl, press **Ctrl-C in each node terminal** and wait for it to
drain. Final statistics remain readable with `swarmcrawl stats <job-id>` while
Keyper is running. Then press **Ctrl-C in the Keyper terminal**.

Stopping Keyper loses **all data in every database**. Starting it again creates
empty databases; `redis-cli -2 GET demo:message` returns `(nil)` after restart.
Remove only the scratch directory you created, after its processes have stopped;
never clear another deployment or use crawler cluster-management commands for cleanup.
