# Keyper + unchanged swarmcrawl demo

A local, repeatable multi-client/multi-node demonstration. The site and all
compatibility assets live in **Keyper**, not the crawler. Use only trusted local
code/endpoints. This runbook does not authorize a grading tag, public visibility,
or course submission; arrange the live helper/demo with the owner separately.

## Prerequisites and roles

- Keyper's [build tools](../README.md#prerequisites-and-build), Python **3.9+**,
  `curl`, and `redis-cli` (RESP2; verified with 7.4.0).
- A clean, unmodified [swarmcrawl checkout](https://github.com/K-Planky/swarmcrawl)
  at **f2c3a591af66e9a424178d9fbc5836740a71e896**. See the
  [acquisition commands](../README.md#optional-unchanged-crawler-script-components).
  Access may require owner permission. Never switch/edit an existing checkout to
  prepare this demo; obtain a separate matching checkout if needed.
- On one machine, reserve loopback **6380** (Keyper) and **8000** (HTTP). If either
  is occupied, choose another port and update every command. Never stop an
  unrelated deployment to free a port.
- **Presenter:** server/CLI and first node. **Helper:** second/third nodes,
  blocking-pop producer and HTTP gate release. Use separate terminals; all
  commands below start from the **Keyper root**, never the crawler checkout.

The automated [process suite](../README.md#optional-full-process-crawler-verification)
checks this graph plus delayed completion, abort/failure and per-node diagnostics
against an owned Redis baseline. It is the correctness check; manual observations
supplement it, not a proof or a claim the live demo already occurred.

## 1. Prepare external build/runtime space

In the setup terminal (replace the source path):

```sh
export CRAWLER_SRC=/absolute/path/to/unmodified/swarmcrawl
export DEMO_WORK=$(mktemp -d /tmp/keyper-demo-XXXXXX)
printf 'Share this DEMO_WORK with every terminal: %s\n' "$DEMO_WORK"
GIT_OPTIONAL_LOCKS=0 git -C "$CRAWLER_SRC" status --porcelain=v1 --untracked-files=all
git -C "$CRAWLER_SRC" rev-parse HEAD
# Require no status output and exactly the revision above.
cargo build --locked
cargo +1.99.0 build --release --locked \
  --manifest-path "$CRAWLER_SRC/Cargo.toml" --target-dir "$DEMO_WORK/crawler-target"
```

The crawler manifest/lockfile/source remain untouched; builds are outside its
checkout. In **every CLI/node terminal**, paste the same actual path and a new,
matching namespace (use another name for a repeat):

```sh
export DEMO_WORK=/tmp/keyper-demo-REPLACE_WITH_PRINTED_NAME
export PATH="$DEMO_WORK/crawler-target/release:$PATH"
export SWARMCRAWL_CONFIG_DIR="$DEMO_WORK/absent-config"
export SWARMCRAWL_REDIS_URL=redis://127.0.0.1:6380/15
export SWARMCRAWL_JOB_NAMESPACE=keyper-demo-three-1
export SWARMCRAWL_REDIS_TIMEOUT_SECS=5
export SWARMCRAWL_FETCH_TIMEOUT_SECS=300
unset SWARMCRAWL_DIAGNOSTICS
```

`absent-config` must not exist: this prevents loading unrelated saved deployment
metadata. No credentials, AUTH, RESP3, Docker, or crawler `cluster` commands are
used. Those lifecycle commands manage a different deployment, not Keyper.

## 2. Start servers and show several clients

In the server terminal:

```sh
./target/debug/keyper --port 6380
```

In the HTTP terminal:

```sh
python3 tests/crawler_fixture.py --port 8000
```

Wait for both startup messages. HTTP binds only to loopback and holds leaf HTML
bodies until release; **gates fail after 300 seconds**, so release promptly.
Do not browse or curl crawl URLs: extra requests change the request oracle.
The `/_fixture/` control endpoints do not count as crawl requests.

Show independent databases and startup selection using fresh clients:

```sh
redis-cli -2 -p 6380 SET demo:key zero
redis-cli -2 -p 6380 -n 15 GET demo:key       # (nil)
redis-cli -2 -p 6380 GET demo:key             # "zero"
```

In one client terminal, block (five-second timeout):

```sh
redis-cli -2 -p 6380 BLPOP demo:queue 5
```

The helper uses another terminal promptly:

```sh
redis-cli -2 -p 6380 PING
redis-cli -2 -p 6380 RPUSH demo:queue first second
redis-cli -2 -p 6380 BRPOP demo:queue 1
redis-cli -2 -p 6380 BLPOP demo:empty 0.25    # (nil), no earlier than timeout
```

The blocked client receives `demo:queue / first`, push length is **2**, and
BRPOP receives `second`. PING/other connections progress while a client waits.
If the five-second wait expired before the push, repeat with a new queue name;
do not mislabel that schedule as a server failure. Core demo keys are in DB 0;
the crawler below uses DB 15.

## 3. Submit two jobs before starting nodes

In the configured CLI terminal:

```sh
swarmcrawl check
swarmcrawl submit http://127.0.0.1:8000/a/ http://127.0.0.1:8000/b/
swarmcrawl submit 'http://127.0.0.1:8000/a/#duplicate'
swarmcrawl jobs
swarmcrawl stats 1
```

On a fresh namespace IDs are **1 and 2**; substitute the printed IDs otherwise.
The duplicate returns job 1's **existing** ID. Initially each job is running with
0 crawled, frontier 1, in-flight 0, discovered 1. `stats` must exit nonzero with
an unfinished-job message, not print partial totals. CLI commands fetch no HTTP.

## 4. Launch three nodes; inspect and follow

In **three separate configured node terminals**:

```sh
swarmcrawl node --diagnostics
```

Wait for each node-ready message. Nodes remain available for later jobs. In CLI:

```sh
swarmcrawl jobs
swarmcrawl status 1
swarmcrawl stats 1
curl --fail --silent --show-error --max-time 5 http://127.0.0.1:8000/_fixture/state | python3 -m json.tool
swarmcrawl status -f 1
```

The two jobs remain running while leaf bodies are held. HTTP `held` counts can
reach **30 across three nodes** (ten with one node). Aggregate counts do not
independently establish each node's cap; the automated suite checks per-node
HTTP diagnostics, body-held concurrency and request counts together. Follow
prints coherent changes and waits; it must not invent final results.

The helper releases the main gate in a different terminal:

```sh
curl --fail --silent --show-error --max-time 5 -X POST http://127.0.0.1:8000/_fixture/release/main
```

Then in CLI:

```sh
swarmcrawl status -f 2
swarmcrawl stats 1
swarmcrawl stats 2
curl --fail --silent --show-error --max-time 5 http://127.0.0.1:8000/_fixture/state | python3 -m json.tool
```

Each job finishes with **32 crawled/discovered, 0 frontier/in-flight, 30 files**.
Both final statistics are immediately readable:

```text
files: 30   extensions: 4   words: 52
  html 27
  jpeg 1
  jpg 1
  pdf 1
```

Hand-countable contributions per job:

| Content | Files | HTML words |
| --- | ---: | ---: |
| Index (`Alpha beta`) | 1 | 2 |
| 24 gated leaves (`Leaf words`) | 24 | 48 |
| Shared page (`Shared end`) | 1 | 2 |
| JPG, JPEG, PDF, extensionless binary | 4 | 0 |
| Redirect to an existing leaf and 404 | 0 | 0 |
| **Total** | **30** | **52** |

The extensionless binary has extension `html` but no HTML words. Empty anchors,
comments and attributes add no words. Duplicate/fragment links, cycles, a
redirect, cross-job/out-of-scope links and convergent shared-page discovery test
the scope/dedup boundary. `get_counts` should show **64 paths, each once**, no
`/outside.html`; `errors` empty, `active` and all `held` counts zero.

Optional empty-frontier demonstration (nodes still running):

```sh
swarmcrawl submit http://127.0.0.1:8000/delay/  # next ID, normally 3
swarmcrawl status 3
swarmcrawl stats 3
# Wait for /delay/ held=1: frontier=0, in-flight=1 must remain running.
curl --fail --silent --show-error --max-time 5 -X POST http://127.0.0.1:8000/_fixture/release/delay
swarmcrawl status -f 3
swarmcrawl stats 3
```

Its final result is **2 files, html 2, 4 words**; only the held parent can reveal
the child. A momentary empty queue is not completion.

## 5. Drain, retain, reset and clean up

Release any remaining gates first (also use this after a failed/abandoned run):

```sh
curl --fail --silent --show-error --max-time 5 -X POST http://127.0.0.1:8000/_fixture/release/all
```

Press **Ctrl-C in every node terminal** and wait for `drained; exiting` and the
numeric `swarmcrawl_metrics` line. Each HTTP peak should be ≤10 and all active
gauges zero after drain; startup/idle time affects timing fields. While Keyper
still runs, read `swarmcrawl stats 1` again from a fresh CLI process: results
remain retained. Then Ctrl-C the HTTP fixture and finally Keyper. No persistent
data survive stopping Keyper; no recovery or exactly-once crash semantics are
promised.

To compare **one node**, restart the HTTP fixture (resets counts/re-arms gates),
use a **new matching namespace in every CLI/node terminal**, and repeat steps
3–5 with one node. The same two jobs must produce the same totals and 64 unique
GETs; at most ten bodies are held at once. A namespace does not reset HTTP
counts. Resubmitting a retained done/failed/aborted URL does not restart it.

After all owned processes have stopped, verify the crawler revision/clean status
again with step 1's read-only Git commands. Inspect and remove only the exact
`DEMO_WORK` directory you created if no longer needed. Never use FLUSHALL,
`swarmcrawl cluster stop/remove`, `pkill`, or cleanup against another deployment.
If a node cannot drain, preserve its logs and report the failure; force-killing
an owned node is not a successful graceful demo and can strand work.
