# Technical readiness and verification

Final technical audit: **2026-10-24**, Linux x86_64 / WSL2. Runtime and test
sources audited at `3dd5108246c8ee409e223f9e64636128d6491f88`; the readiness
changes add documentation only. This is evidence for the supported subset,
**not** full Redis compatibility, a formal proof, or completion of the live
course demo/release.

## Reproduce from fresh source

Use the [build prerequisites](README.md#prerequisites-and-build): Rust/Cargo
**1.99.0**, rustfmt, Clippy, native C compiler/archiver/linker. External checks
also need Python **3.9+**, Unix signals and a Redis **7.4.x** server executable
(verified **7.4.0**). Manual rehearsal additionally uses `redis-cli` and `curl`.
The verified environment used Python **3.12.3**, GCC **13.3.0**, and genuine
vendored Lua **5.1.5** through locked mlua **0.12.2**.

Obtain separate checkouts in new scratch paths; never switch or patch an existing
crawler checkout. GitHub access may require owner permission. Dependencies can
come from Cargo's cache or crates.io; neither project needs existing build output.
The commands below start from an existing parent scratch directory:

```sh
git clone git@github.com:K-Planky/keyper.git keyper-review
git clone --no-checkout https://github.com/K-Planky/swarmcrawl crawler-review
git -C crawler-review checkout --detach f2c3a591af66e9a424178d9fbc5836740a71e896
export CRAWLER_SRC="$(cd crawler-review && pwd)"
cd keyper-review
export REVIEW_WORK=$(mktemp -d /tmp/keyper-review-XXXXXX)
export REDIS_SERVER=/absolute/path/to/redis-server

cargo fmt --all -- --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --locked
cargo build --locked
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p 'test_crawler*.py' -v

KEYPER_REFERENCE_REDIS_SERVER="$REDIS_SERVER" \
  cargo test --locked --test lua_reference -- --ignored --nocapture
for suite in numeric_sets hashes lists transactions eval; do
  PYTHONDONTWRITEBYTECODE=1 python3 "tests/compare_${suite}.py" \
    --redis-server "$REDIS_SERVER" || exit 1
done
PYTHONDONTWRITEBYTECODE=1 python3 tests/compare_crawler_scripts.py \
  --crawler-checkout "$CRAWLER_SRC" --redis-server "$REDIS_SERVER"
PYTHONDONTWRITEBYTECODE=1 python3 tests/crawler_process.py \
  --crawler-checkout "$CRAWLER_SRC" --redis-server "$REDIS_SERVER" \
  --output "$REVIEW_WORK/process" --crawler-tests
```

Run each check successfully before proceeding (or use a fail-fast shell).
Do not use Python `-O`: the reference/process oracles use assertions. No running
Redis service is needed for core Rust tests. External suites start disposable
owned endpoints, fail on missing prerequisites/mismatches, and never proxy Keyper
commands to Redis. `--output` must not already exist and must be outside the
crawler checkout. Builds, runtime state and logs stay outside that checkout.

This exact sequence passed from fresh remote clones, without local planning
files, a sibling checkout, shared target directories, or patched crawler sources.
Fresh native builds used the installed toolchain and cached locked crates; this
was not a network-empty/offline dependency-acquisition test. The process suite
built the crawler anew in its own output directory. Its `report.json`, per-case
`result.json`/`cli.jsonl`, build/test logs and child diagnostics are retained for
inspection. After stopping owned processes, remove only your owned scratch paths.

## Results and coverage

| Boundary | Passing evidence |
| --- | --- |
| Rust/Tokio, async/multithreaded lifecycle | Formatter, warning-free all-target/all-feature Clippy, build; **167 Rust tests passed**, zero failed. One opt-in Lua reference test was ignored by default and **separately passed**, not counted as a default pass. |
| Sixteen namespaces, default DB 0, startup-only SELECT | `tests/databases.rs`, `tests/pipelining.rs`: all DBs 0–15, rejected DB/late/invalid attempts, unchanged client startup and isolation. |
| RESP2, binary/empty/null/error distinctions | `src/resp.rs`, `tests/transport.rs`, `tests/pipelining.rs`: bounded framing, partial/coalesced input, malformed/truncated nonmutation, nested replies and ordered errors. |
| Port default/configuration and failures | `src/config.rs`, `tests/cli.rs`, `tests/transport.rs`: default **6379**, explicit/ephemeral ports, invalid arguments, occupied-port diagnostics. Manual run used **6380**. |
| GET, bare SET, variadic EXISTS, PING | `tests/databases.rs`, `tests/transport.rs`: missing/binary/type/overwrite/arity cases, duplicate key counting and real-time fences. PING is excluded from linearizability histories. |
| LPUSH/RPUSH | `tests/databases.rs`, owner tests: variadic order/length atomicity, wrong types, concurrent pushes/replacements and all namespaces. |
| BLPOP/BRPOP | `src/state/blocking/tests.rs`, `src/server/blocking_tests.rs`: both ends, 1–5/too-many/duplicate keys, type/key/FIFO priority, floating/zero/invalid timeouts, no lost wakeup, one terminal outcome and complete cleanup. |
| Failure/concurrency integration | Socket/owner/process tests cover canceled admission, timeout/producer/disconnect contenders, contention and item accounting, slow/broken/lost writes, pending shutdown, empty restart, owner panic and stuck-Lua fail-stop exit. |
| Numeric/set/hash/list compatibility | Wire/unit boundaries plus Redis comparisons: **2,512 numeric/set**, **3,101 hash**, **2,734 list/key** commands. Decimal integers remain exact; collection order alone is normalized where unspecified. |
| MULTI/EXEC/DISCARD | **2,264** reference commands plus `tests/transactions.rs`: admission abort versus retained runtime writes, later-command continuation, queue cleanup/budgets, coherent six-read crawler snapshots and no interleaving. |
| Genuine EVAL/Lua | **8,468** database-bridge comparisons; **1,222** exact language/conversion results plus **4 error categories**. `tests/eval.rs`/`tests/lua_runtime.rs` also check denied host access, fresh environments, binary/derived keys, error effects and EXEC-contained EVAL. |
| Combined batch/waiter semantics | Deterministic owner/socket tests cover final-state-only service, left-to-right key rechecking, FIFO, push-then-empty/replace and timeout/disconnect orders. Reference Redis 7.4.0 priority probes agree. |
| Exact unchanged crawler scripts | All five runtime compositions, read unchanged from six digest-checked Lua files, passed on Keyper and Redis in DB **0 and 15**; final run **4,760 commands**. Includes concurrent dedup/claim/publication, coherent snapshots, corrupt/error states, IDs above 2^53 and i64/u64 overflow. |
| Actual unchanged crawler | **Four full-process cases**: Keyper/Redis × **one node/DB 0** and **three nodes/DB 15**. Per case: **69 GETs / 68 paths**, identical final states/statistics, no extra HEAD/out-of-scope requests. |
| Mini 1 guarantees on Keyper | Concurrent independent/overlapping jobs, once-per-URL-per-job fetching, each node HTTP peak **≤10**, real body-held concurrency, coherent follow, delayed discovery before termination, immediately readable/retained final stats; abort and honest 503 failure/no retry. Broad jobs each yield **30 files / 4 extensions / 52 words** with either node count. |
| Fixture/oracle checks | **8 Python tests passed**, including gates, hand-counted graph, invalid metrics/progress rejection, extra-request visibility and failure cleanup ordering. |
| Original crawler tests | Unmodified default suite **81 passed / 48 explicitly ignored**. Original PING **1 passed on each server**. Original pipeline **3** and legacy Redis/job/node/CLI/distributed **42** passed on **real Redis only**, not Keyper. |
| Self-contained deliverable | Both repositories acquired from GitHub into separate new checkouts. Keyper built/tested/ran using tracked README/tests/demo only; no local `docs/` or `AGENTS.md` required. |
| Crawler immutability | Both the existing external checkout and new source checkout remained clean at **f2c3a591af66e9a424178d9fbc5836740a71e896** before/after. Lock SHA-256: `01fd4498796a39a25f77853f0c369b436a9dec3861a6746bfe55b76a2eb6cc58`; script digests checked by the harness. |

The five sequential command comparison suites total **19,079 commands**. Ten
additional owner/socket suite runs passed **1,470 test executions**. These
are not concurrent-history proofs; controlled concurrent tests and the
[ownership argument](README.md#linearizability-and-failure-contract) address that
separate obligation. Lua error-source/engine diagnostics and unordered collections
have only the documented normalizations, not relaxed data-value comparisons.

## Execution-path and readability review

Reviewed configuration → TCP connection → RESP decoding → session validation →
bounded owner work → response → cleanup, including errors and nested EVAL/EXEC:

- `Session::prepare` consumes selection once, owns clean/dirty transaction state,
  and separates admission from runtime errors. Commands remain typed and byte-safe.
- `Client::submit` owns the queued request/reply. Cancellation closes its receiver
  before coalescing owner notification; unapplied work can be skipped, committed
  writes cannot be retried or undone because delivery failed.
- `Owner::process` / `execute_atomic` alone access state. Whole commands/batches
  capture replies before `serve_ready`; no map access spans network I/O or a wait.
- Pop empty-check/registration is indivisible. Terminal removal clears all key
  and deadline indexes; finite deadlines are monotonic and never renewed. FIFO
  candidates recheck their original key order against the completed outer unit.
- VM creation, script calls and return conversion stay on that same dedicated
  thread. General Lua executes actual commands; there is no script-text/hash
  dispatch, crawler-specific native replacement or external Redis backend.
- Tokio/mlua are the two justified runtime dependencies; redis-rs is test-only.
  Dense set/hash indexes and VecDeque meet the documented command complexity
  bounds without whole-database scans/copies. Byte copying, collection destruction
  and waiter bookkeeping costs are disclosed in the README.
- Supervision is fail-stop for uncertain owner/task failure. Ordinary bad client
  commands are errors, not panics. Logs do not include raw request payloads.

No material correctness or readability defect was found in these paths during
this audit. No production/test/dependency rewrite was justified. Documentation
now removes the stale pre-audit stopping point and exposes this repeatable
coverage/release boundary directly from the README.

## Demo rehearsal and remaining owner actions

The tracked [presenter/helper runbook](demo/README.md) was rehearsed with real
`redis-cli` **7.4.0**, `curl`, standalone HTTP fixture, fresh-built Keyper and the
fresh-built unchanged crawler. Scripted separate processes performed the roles;
**no live human-assisted demo is claimed**.

Observed: multiple clients; DB 0/15 isolation; blocked BLPOP released by another
client; whole-push length **2** and opposite-end order; PING progress; a **0.25 s**
empty timeout no earlier than its deadline; actual MULTI/EXEC and general Lua;
three-node and one-node independent runs with identical **52-word** results and
**64 main GET paths, each once**; held-parent empty-frontier progress; successful
follow/drain, retained stats after nodes exit and empty same-port server restart.
All owned servers/nodes/fixture processes exited normally in the successful
rehearsal. An earlier disposable rehearsal launcher inherited ignored SIGINT
from a background shell, preventing its Python fixture from handling Ctrl-C;
that launcher was corrected to restore foreground signal semantics and the
whole sequence rerun successfully. No server/crawler/fixture source was changed
to mask the orchestration error.

Known boundaries remain intentional, not failed requirements hidden as passes:

- This is loopback-only, credential-free RESP2 with the
  [30 documented commands](README.md#implemented-command-and-transport-contracts),
  startup-only SELECT, bare SET and at most five blocking-pop keys.
- No persistence/recovery, exactly-once delivery, rollback, authentication,
  RESP3, pub/sub, WATCH, script cache APIs, or public-network hardening. Pipelines
  are ordered, non-atomic and head-of-line blocked; EVAL/EXEC are isolated.
- Lua has no time/heap quota or preemption. Long scripts serialize all databases;
  shutdown has a five-second fail-stop deadline. Queue/conversion limits are not
  a global memory bound. Only trusted local scripts/endpoints are appropriate.
- The original crawler Redis suites need excluded SCAN/DEL/LRANGE/LSET/TTL helpers;
  their reference-only passes are not Keyper passes. Two managed-cluster
  Docker/interactive-PTY tests remain unrun and do not manage Keyper.
- Automatic linearizability checking is optional and deferred; Linux x86_64 is
  the verified platform, not a claim of tested cross-platform behavior.

**Owner-coordinated work remains:** arrange a helper and live multi-client/crawler
demo, keep this project's repository private until afterward, authorize/confirm
public visibility and crawler availability for grading, select and push the exact
graded `1.0.0` tag, and submit the URL on the LMS. None of these release actions
were performed or authorized by this technical audit.
