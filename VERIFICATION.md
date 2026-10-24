# Verification

Commands for checking Keyper locally, comparing its supported subset with Redis,
and exercising the unchanged swarmcrawl crawler. Recorded results are historical;
rerun the checks when runtime or test sources change.

## Core checks

Use the [build prerequisites](README.md#prerequisites-and-build): Rust **1.99.0**,
rustfmt, Clippy, and native C build tools. From the Keyper root:

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --locked
cargo build --locked
```

No external Redis is required. Coverage includes RESP2 framing, CLI errors,
all 16 databases, command semantics, ordered pipelines, blocking-pop priority and
cleanup, timeout/disconnect races, atomic EXEC/EVAL, and fail-stop shutdown.

The HTTP fixture and crawler-oracle tests need Python **3.9+**, but no Redis:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p 'test_crawler*.py' -v
```

## Reproduce from fresh source

For an isolated run, start in a scratch workspace and obtain new checkouts.
GitHub access may require owner permission. Never switch or modify an existing
crawler checkout.

```sh
set -eu
git clone git@github.com:K-Planky/keyper.git keyper-review
git clone --no-checkout https://github.com/K-Planky/swarmcrawl crawler-review
git -C crawler-review checkout --detach f2c3a591af66e9a424178d9fbc5836740a71e896
CRAWLER_SRC="$(cd crawler-review && pwd)"
REVIEW_WORK=$(mktemp -d /tmp/keyper-review-XXXXXX)
export CRAWLER_SRC REVIEW_WORK
cd keyper-review
```

Run the core checks above in the new Keyper checkout. Continue below in the same
shell; stop if any check fails. External suites require Python **3.9+**, Unix
signals, and a Redis **7.4.x** server executable:

```sh
export REDIS_SERVER=/absolute/path/to/redis-server
```

The suites start their own disposable local servers; they never use an
existing Redis deployment or proxy Keyper commands to Redis. Do not use Python
`-O`: the test oracles rely on assertions.

## Reference Redis checks

```sh
KEYPER_REFERENCE_REDIS_SERVER="$REDIS_SERVER" \
  cargo test --locked --test lua_reference -- --ignored --nocapture

for suite in numeric_sets hashes lists transactions eval; do
  PYTHONDONTWRITEBYTECODE=1 python3 "tests/compare_${suite}.py" \
    --redis-server "$REDIS_SERVER" || exit 1
done
```

The Lua reference test is ignored by default and must be run explicitly.
Comparisons preserve data values and RESP2 shapes; only the collection ordering
and error-diagnostic differences documented in each suite are normalized.
Project-specific restrictions are checked by Rust tests, not against Redis's
different policies.

## Crawler checks

```sh
PYTHONDONTWRITEBYTECODE=1 python3 tests/compare_crawler_scripts.py \
  --crawler-checkout "$CRAWLER_SRC" --redis-server "$REDIS_SERVER"

PYTHONDONTWRITEBYTECODE=1 python3 tests/crawler_process.py \
  --crawler-checkout "$CRAWLER_SRC" --redis-server "$REDIS_SERVER" \
  --output "$REVIEW_WORK/process" --crawler-tests
```

The harnesses check the crawler revision, clean worktree, and six Lua-file
digests before and after execution. The process suite builds the crawler outside
its checkout and runs four cases: **Keyper and Redis**, each with **one node in
DB 0** and **three nodes in DB 15**. It checks per-job deduplication, independent
jobs, each node's ten-request cap, coherent progress, delayed discovery,
termination, retained final statistics, abort, and honest HTTP failure handling.

`--output` must be a new directory outside the crawler checkout. Inspect its
`report.json`, per-case `result.json` and `cli.jsonl`, build/test logs, and child
diagnostics. Remove only owned scratch paths after their processes have stopped.

## Recorded results

Audit: **2026-10-24**, runtime/test revision
`3dd5108246c8ee409e223f9e64636128d6491f88`.
Environment: Linux x86_64 / WSL2, Rust **1.99.0**, Python **3.12.3**, GCC **13.3.0**,
Redis **7.4.0**, vendored Lua **5.1.5** through mlua **0.12.2**.

| Check | Recorded outcome |
| --- | --- |
| Rust quality gates | Formatting, warning-free Clippy, and build passed; **167 tests passed**, zero failed. One default-ignored Lua reference test passed separately. |
| Python fixture/oracles | **8 tests passed**. |
| Sequential Redis comparisons | **19,079 commands**: numeric/set 2,512; hash 3,101; list/key 2,734; transactions 2,264; EVAL 8,468. |
| Lua language and conversion | **1,222 exact results** and **4 error categories** matched. |
| Blocking/concurrency repetitions | **1,470 passing executions** across ten additional owner/socket suite runs. Final-state-only waiter service and left-to-right key rechecking also matched Redis priority probes. |
| Unchanged crawler scripts | **4,760 commands**, five script compositions, DBs **0 and 15**; concurrent ownership/publication, error states, and integer boundaries checked. |
| Full crawler processes | All **four cases passed**; **69 GETs / 68 paths per case**, matching final states/statistics, no extra HEAD or out-of-scope requests. Broad jobs each produced **30 files / 4 extensions / 52 words**. |
| Original crawler tests | **81 passed / 48 ignored** by default; PING passed once on each server. **3 pipeline** and **42 legacy integration** tests passed on **real Redis only**. |

Fresh-clone builds and checks passed without local planning files or existing
build outputs. Locked dependencies were cached; this was not an offline
acquisition test. Crawler source checkouts remained clean at the pinned revision;
the recorded Cargo.lock SHA-256 was
`01fd4498796a39a25f77853f0c369b436a9dec3861a6746bfe55b76a2eb6cc58`.

## Scope and remaining gaps

- These results cover the [documented subset](README.md#implemented-command-and-transport-contracts),
  not full Redis compatibility or other platforms. Tests and repetitions are not
  a formal proof; automatic linearizability checking is not included. See the
  [ownership and failure argument](README.md#linearizability-and-failure-contract).
- Original crawler integration helpers need unsupported `SCAN`, `DEL`, `LRANGE`,
  `LSET`, and `TTL`. Their Redis-only passes are not Keyper passes. Two
  Docker/interactive-PTY managed-cluster tests remain unrun.
- The [manual demo](demo/README.md) was rehearsed using scripted processes,
  `redis-cli` **7.4.0**, and `curl`: multiple clients, blocking pops, DB isolation,
  transactions/Lua, one-/three-node crawling, retained results, and empty restart.
  The main graph produced **64 GET paths, each once**. This was not a live
  human-assisted demo.
- Live demo/helper coordination, post-demo public visibility, crawler access for
  grading, the graded `1.0.0` tag, and LMS submission remain owner actions;
  verification does not authorize or complete them.
