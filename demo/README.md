# Keyper walkthrough

Run Keyper as a normal local service, then try it from several clients.
Connecting swarmcrawl is optional; Keyper needs neither a crawler nor a separate
Redis server.

Use the [build prerequisites](../README.md#prerequisites-and-build) and install
`redis-cli` for the client examples. Start each terminal in the **Keyper root**;
the crawler section switches to the sibling **swarmcrawl directory**. Examples
assume fresh `demo:*` keys; use a different prefix when repeating them.

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

Keep Keyper running. With swarmcrawl already built, run this in **each new
terminal opened in the Keyper directory**:

```sh
cd ../swarmcrawl
export PATH="$PWD/target/release:$PATH"
swarmcrawl check
```

Expect `Redis connectivity: OK (PONG)`. No Redis URL export is needed if your
existing settings already point to Keyper. All terminals must use the same
endpoint, database, and namespace.

If needed, run `export SWARMCRAWL_REDIS_URL=redis://127.0.0.1:6379/15` before
`swarmcrawl check` to use Keyper on port 6379 with DB **15**, separate from the
examples in DB 0.

Start a worker in one terminal:

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

Start more nodes the same way. No Docker or crawler `cluster` commands are
needed; those commands do not manage Keyper. For LAN access and the tested
crawler revision, see [crawler integration](../README.md#crawler-integration).

For a fresh crawl, optionally run `export SWARMCRAWL_JOB_NAMESPACE=keyper-demo-2`
in every terminal before starting nodes or submitting jobs. Otherwise,
resubmitting the same URL reuses its job.

<details>
<summary>Optional: use a temporary directory</summary>

To ignore saved settings and use a fresh namespace, run
`mktemp -d /tmp/keyper-demo-XXXXXX` once. In every crawler terminal, replace the
path below with its output before starting nodes or submitting jobs:

```sh
DEMO_WORK=/tmp/keyper-demo-XXXXXX
export SWARMCRAWL_REDIS_URL=redis://127.0.0.1:6379/15
export SWARMCRAWL_JOB_NAMESPACE="${DEMO_WORK##*/}"
export SWARMCRAWL_CONFIG_DIR="$DEMO_WORK/absent-config"
```

Leave `absent-config` uncreated so the crawler does not load saved settings.

</details>

## Shut down

If using swarmcrawl, press **Ctrl-C in each node terminal** and wait for it to
drain. Final statistics remain readable with `swarmcrawl stats <job-id>` while
Keyper is running. Then press **Ctrl-C in the Keyper terminal**.

Stopping Keyper loses **all data in every database**. Starting it again creates
empty databases; `redis-cli -2 GET demo:message` returns `(nil)` after restart.
If you used a temporary directory, remove only that directory after its processes
have stopped. Never clear another deployment or use crawler cluster-management
commands for cleanup.
