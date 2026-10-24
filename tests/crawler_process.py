#!/usr/bin/env python3
"""Opt-in full-process verification of the pinned, unchanged swarmcrawl.

Builds into a new external output directory, owns both TCP database servers and
all HTTP/CLI/node processes, and retains logs/report on success or failure.
Requires Unix signals, Python 3, Rust 1.99.0/native tools and Redis 7.4.x.
"""

import argparse
import contextlib
import hashlib
import json
import os
import pathlib
import re
import signal
import socket
import subprocess
import sys
import time

sys.dont_write_bytecode = True
from compare_crawler_scripts import REVISION, checked_scripts
from crawler_fixture import EXPECTED_STATS, Fixture


class Child:
    def __init__(self, args, log, env=None):
        self.log = log
        self.output = log.open("w")
        try:
            self.process = subprocess.Popen(args, stdout=self.output, stderr=subprocess.STDOUT,
                                            env=env, cwd=log.parent)
        except BaseException:
            self.output.close()
            raise

    def text(self):
        return self.log.read_text()

    def finish(self, expected=0, interrupt=False, timeout=15):
        if interrupt and self.process.poll() is None:
            self.process.send_signal(signal.SIGINT)
        self.process.wait(timeout=timeout)
        assert self.process.returncode == expected, (self.log, self.process.returncode, self.text())
        return self.text()

    def cleanup(self):
        try:
            if self.process.poll() is None:
                self.process.send_signal(signal.SIGINT)
                try:
                    self.process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=5)
                    raise AssertionError(f"owned child would not drain: {self.log}")
        finally:
            self.output.close()


def wait_for(check, description, children=(), timeout=30):
    deadline = time.monotonic() + timeout
    while True:
        result = check()
        if result:
            return result
        for child in children:
            assert child.process.poll() is None, (description, child.log, child.text())
        assert time.monotonic() < deadline, f"deadline waiting for {description}"
        time.sleep(0.02)  # Bounded polling of observable gates, never a race winner.


def clean_env(output):
    env = {key: value for key, value in os.environ.items() if not key.startswith("SWARMCRAWL_")}
    env.update(SWARMCRAWL_CONFIG_DIR=str(output / "absent-config"),
               SWARMCRAWL_REDIS_TIMEOUT_SECS="5", SWARMCRAWL_FETCH_TIMEOUT_SECS="120",
               NO_PROXY="127.0.0.1,localhost", no_proxy="127.0.0.1,localhost")
    return env


@contextlib.contextmanager
def servers(args, output, env):
    with contextlib.ExitStack() as stack:
        def start(command, name):
            child = Child(command, output / f"{name}.log", env)
            stack.callback(child.cleanup)
            return child

        keyper = start([str(args.keyper_binary), "--port", "0"], "keyper")
        line = wait_for(lambda: next((line for line in keyper.text().splitlines()
                                    if line.startswith("keyper: listening on ")), None),
                        "Keyper listening", [keyper])
        keyper_url = "redis://" + line.rsplit(" ", 1)[1]
        # Redis cannot choose port 0 for TCP (it disables TCP). Release an owned
        # reservation, then demand the owned process's successful listen log. A
        # bind conflict fails; we never fall back to an existing deployment.
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        reference = start([str(args.redis_server), "--bind", "127.0.0.1", "--port", str(port),
                           "--save", "", "--appendonly", "no", "--dir", str(output)], "redis")
        wait_for(lambda: "Ready to accept connections tcp" in reference.text(),
                 "owned reference TCP listen", [reference])
        yield [("keyper", keyper_url), ("redis", f"redis://127.0.0.1:{port}")]
        keyper.finish(interrupt=True)
        reference.finish(interrupt=True)


SNAPSHOT = re.compile(
    r"job (\d+)  crawled (\d+)  frontier (\d+)  in flight (\d+)  files (\d+)  discovered (\d+)  (.+)"
)


def snapshot(line):
    match = SNAPSHOT.fullmatch(line.strip())
    assert match, f"invalid status line: {line!r}"
    job, processed, queued, owned, files, discovered = map(int, match.groups()[:6])
    state = match[7]
    assert files <= processed <= discovered
    if state in ("running", "done"):
        assert discovered == processed + queued + owned, line
        if state == "running":
            assert queued + owned > 0, line
        else:
            assert queued == owned == 0, line
    return dict(job=job, processed=processed, queued=queued, owned=owned, files=files,
                discovered=discovered, state=state)


class Run:
    def __init__(self, binary, output, env, stack):
        self.binary, self.output, self.env, self.stack = binary, output, env, stack
        self.journal = []
        self.serial = 0

    def cli(self, *args, expected=0):
        result = subprocess.run([str(self.binary), *map(str, args)], env=self.env,
                                cwd=self.output, capture_output=True, text=True, timeout=10)
        record = dict(args=args, code=result.returncode, stdout=result.stdout, stderr=result.stderr)
        self.journal.append(record)
        # Persist each outcome even if an assertion or later process fails.
        with (self.output / "cli.jsonl").open("a") as log:
            log.write(json.dumps(record) + "\n")
        assert result.returncode == expected, record
        return result.stdout

    def child(self, *args):
        self.serial += 1
        child = Child([str(self.binary), *map(str, args)],
                      self.output / f"child-{self.serial}.log", self.env)
        self.stack.callback(child.cleanup)
        return child

    def submit(self, *urls):
        lines = self.cli("submit", *urls).splitlines()
        assert len(lines) == len(urls), lines
        matches = [re.fullmatch(r"job (\d+)  input (\d+)  (created|existing)", line) for line in lines]
        assert all(matches), lines
        assert [int(m[2]) for m in matches] == list(range(1, len(urls) + 1))
        return [(int(m[1]), m[3]) for m in matches]

    def status(self, job, expected=0):
        return snapshot(self.cli("status", job, expected=expected))

    def unavailable_stats(self, job, reason="still running"):
        assert self.cli("stats", job, expected=1) == "", "partial final statistics printed"
        error = self.journal[-1]["stderr"]
        assert reason in error and "final statistics are not available" in error, error

    def done(self, job, children, expected_stats):
        def completed():
            state = self.status(job)
            assert state["state"] in ("running", "done"), state
            return state if state["state"] == "done" else None
        final = wait_for(completed, f"job {job} done", children)
        # Do not wait for another job, node drain or a later poll before the
        # first final-stats read: done itself must guarantee their availability.
        assert self.cli("stats", job) == expected_stats
        return final

    def nodes(self, count):
        nodes = [self.child("node", "--diagnostics") for _ in range(count)]
        for node in nodes:
            wait_for(lambda: "ready; polling Redis" in node.text(), "node ready", nodes)
        return nodes


def metrics(text):
    lines = [line.removeprefix("swarmcrawl_metrics ") for line in text.splitlines()
             if line.startswith("swarmcrawl_metrics ")]
    assert len(lines) == 1, text
    result = json.loads(lines[0])
    assert result["http"]["active"] == result["owned"]["active"] == result["cpu"]["active"] == 0
    assert 1 <= result["http"]["peak"] <= 10, result
    assert result["owned"]["peak"] <= 20 and result["cpu"]["peak"] <= 4, result
    return result


@contextlib.contextmanager
def draining_children(fixture):
    with contextlib.ExitStack() as stack:
        try:
            yield stack
        finally:
            # Also on an assertion/timeout: release bodies BEFORE interrupting
            # nodes, otherwise graceful cleanup waits behind our own gate.
            fixture.release()


def run_case(binary, output, env, node_count):
    output.mkdir()
    with Fixture(gate_timeout=120) as fixture, draining_children(fixture) as stack:
        run = Run(binary, output, env, stack)
        assert run.cli("check") == "Redis connectivity: OK (PONG)\n"
        assert run.cli("jobs") == "No jobs in this database/namespace.\n"
        urls = [fixture.url + path for path in ("/a/", "/b/")]
        submitted = run.submit(*urls)
        assert submitted == [(1, "created"), (2, "created")], submitted
        assert run.submit(urls[0] + "#fragment") == [(1, "existing")]
        # The narrow overlapping job owns the same leaf URL independently. A
        # global (rather than per-job) visited set would incorrectly skip it.
        assert run.submit(fixture.url + "/a/leaf-0.html") == [(3, "created")]
        # Submission and all pre-node commands must not fetch any HTTP URL.
        for job in (1, 2, 3):
            assert run.status(job) == dict(job=job, processed=0, queued=1, owned=0,
                                           files=0, discovered=1, state="running")
            run.unavailable_stats(job)
        assert fixture.snapshot()["get_counts"] == {}
        assert [snapshot(line)["job"] for line in run.cli("jobs").splitlines()] == [1, 2, 3]
        follows = [run.child("status", "-f", job) for job in (1, 2)]
        for follow in follows:
            wait_for(lambda: "running" in follow.text(), "follow initial snapshot", follows)
        nodes = run.nodes(node_count)

        # A node can own at most 20 tasks. With 49 held leaf fetches, uneven
        # ownership may leave one node with fewer than ten even while others
        # have HTTP-admission waiters. Exactly 30 would assume load balancing,
        # not test the cap. More than 20 establishes three active nodes; the
        # single-node case still fills all ten slots. Diagnostics check each cap.
        required_held = 10 * (node_count - 1) + 1 if node_count > 1 else 10

        def concurrent_bodies():
            state = fixture.snapshot()
            held = state["held"]
            assert not state["errors"], state
            assert sum(held.values()) <= 10 * node_count, state
            return (sum(held.values()) >= required_held and
                    all(any(path.startswith(base) and count for path, count in held.items())
                        for base in ("/a/", "/b/")))

        wait_for(concurrent_bodies, "body-held concurrency across jobs and nodes", nodes)
        held_state = fixture.snapshot()
        for job in (1, 2):
            state = run.status(job)
            assert state["state"] == "running" and state["owned"] > 0
            run.unavailable_stats(job)
        assert all(fixture.snapshot()["get_counts"].get(f"/{job}/shared.html", 0) == 0
                   for job in ("a", "b")), "discovery escaped the held HTML body"
        fixture.release("main")
        finals = [run.done(job, nodes, EXPECTED_STATS) for job in (1, 2)]
        for job, final, follow in zip((1, 2), finals, follows):
            assert final == dict(job=job, processed=32, queued=0, owned=0, files=30,
                                 discovered=32, state="done"), final
            assert run.cli("stats", job) == EXPECTED_STATS
            lines = follow.finish().splitlines()
            history = [snapshot(line) for line in lines]
            assert history[0]["state"] == "running" and history[-1] == final, history
            assert all(row["state"] in ("running", "done") for row in history), history
            for field in ("processed", "files", "discovered"):
                assert all(a[field] <= b[field] for a, b in zip(history, history[1:])), history
        overlap_stats = "files: 1   extensions: 1   words: 2\n  html 1\n"
        overlap = run.done(3, nodes, overlap_stats)
        assert overlap == dict(job=3, processed=1, queued=0, owned=0, files=1,
                               discovered=1, state="done"), overlap
        assert run.cli("stats", 3) == overlap_stats
        main_counts = fixture.snapshot()["get_counts"]
        expected_paths = {path for path in fixture.routes if path.startswith(("/a/", "/b/"))}
        expected_main_counts = dict.fromkeys(expected_paths, 1)
        expected_main_counts["/a/leaf-0.html"] = 2  # Once in each of two distinct jobs.
        assert main_counts == expected_main_counts, main_counts
        assert run.submit(*urls) == [(1, "existing"), (2, "existing")]

        # Empty frontier while one held parent can still discover a child. Other
        # nodes may poll, but no completion or partial stats may become visible.
        assert run.submit(fixture.url + "/delay/") == [(4, "created")]
        wait_for(lambda: fixture.snapshot()["held"].get("/delay/") == 1, "late parent held", nodes)
        delayed = run.status(4)
        assert delayed == dict(job=4, processed=0, queued=0, owned=1, files=0,
                               discovered=1, state="running"), delayed
        run.unavailable_stats(4)
        follower = run.child("status", "-f", 4)
        wait_for(lambda: "running" in follower.text(), "late-parent follow", [*nodes, follower])
        fixture.release("delay")
        delayed_stats = "files: 2   extensions: 1   words: 4\n  html 2\n"
        assert run.done(4, nodes, delayed_stats) == dict(job=4, processed=2, queued=0, owned=0, files=2,
                                                       discovered=2, state="done")
        assert "done" in follower.finish()
        assert run.cli("stats", 4) == delayed_stats

        assert run.submit(fixture.url + "/abort/") == [(5, "created")]
        wait_for(lambda: fixture.snapshot()["held"].get("/abort/") == 1, "abort parent held", nodes)
        assert run.cli("abort", 5) == "job 5  aborted\n"
        aborted = run.status(5, expected=1)
        assert aborted["state"] == "aborted"
        run.cli("status", "-f", 5, expected=1)
        run.unavailable_stats(5, "aborted")
        assert run.submit(fixture.url + "/abort/") == [(5, "existing")]
        # Abort is idempotent; it must not restart or change a terminal job.
        assert run.cli("abort", 5) == "job 5  unchanged (already terminal)\n"
        fixture.release("abort")
        node_metrics = [metrics(node.finish(interrupt=True)) for node in nodes]
        if node_count == 1:
            assert node_metrics[0]["http"]["peak"] == 10, node_metrics
        assert run.status(5, expected=1) == aborted, "late publication changed frozen abort"
        assert "/abort/child.html" not in fixture.snapshot()["get_counts"]
        # Retained stats remain readable from fresh CLI processes without nodes.
        for job in (1, 2):
            assert run.cli("stats", job) == EXPECTED_STATS
        assert run.cli("stats", 3) == overlap_stats
        assert run.cli("stats", 4) == delayed_stats

        # A normal HTTP operational failure retains a failed job, no partial
        # final stats, no automatic request replay, and an honest node exit.
        assert run.submit(fixture.url + "/failure/") == [(6, "created")]
        failing = run.child("node", "--diagnostics")
        failure_log = failing.finish(expected=1)
        failure_metrics = metrics(failure_log)
        failed = run.status(6, expected=1)
        assert failed["state"] == "failed (fetch)", failed
        run.cli("status", "-f", 6, expected=1)
        run.unavailable_stats(6, "failed (Fetch)")
        assert run.submit(fixture.url + "/failure/") == [(6, "existing")]
        assert run.status(6, expected=1) == failed
        assert run.cli("abort", "--all") == "No running jobs in this database/namespace.\n"
        listed = [snapshot(line) for line in run.cli("jobs").splitlines()]
        assert [row["job"] for row in listed] == list(range(1, 7)), listed
        assert [row["state"] for row in listed] == ["done"] * 4 + ["aborted", "failed (fetch)"]
        # Per-job and namespace/database isolation; no reused job identities.
        other_env = dict(env, SWARMCRAWL_JOB_NAMESPACE=env["SWARMCRAWL_JOB_NAMESPACE"] + ":other")
        other = Run(binary, output, other_env, stack)
        assert other.cli("jobs") == "No jobs in this database/namespace.\n"
        db = int(env["SWARMCRAWL_REDIS_URL"].rsplit("/", 1)[1])
        other.env = dict(env, SWARMCRAWL_REDIS_URL=env["SWARMCRAWL_REDIS_URL"].rsplit("/", 1)[0] +
                         f"/{15 if db == 0 else 0}")
        assert other.cli("jobs") == "No jobs in this database/namespace.\n"
        wait_for(lambda: fixture.snapshot()["active"] == 0, "all HTTP handlers drained")
        state = fixture.snapshot()
        expected_counts = dict(main_counts, **{path: 1 for path in (
            "/delay/", "/delay/child.html", "/abort/", "/failure/",
        )})
        assert state["get_counts"] == expected_counts, state
        assert not state["errors"] and state["active"] == sum(state["held"].values()) == 0, state
        report = dict(nodes=node_count, required_held=required_held, final=finals, stats=EXPECTED_STATS,
                      overlap=overlap, overlap_stats=overlap_stats, delayed_stats=delayed_stats,
                      aborted=aborted, failed=failed, http=state, held_observation=held_state,
                      node_metrics=node_metrics, failure_metrics=failure_metrics)
        (output / "result.json").write_text(json.dumps(report, indent=2) + "\n")
        return report


def external_command(command, output, name, env, timeout=900):
    with (output / f"{name}.log").open("w") as log:
        process = subprocess.Popen(command, cwd=output, env=env, stdout=log,
                                   stderr=subprocess.STDOUT, start_new_session=True)
        try:
            process.wait(timeout=timeout)
            assert process.returncode == 0, f"{name} failed; see {output / (name + '.log')}"
        except BaseException:
            # Cargo/test subprocesses share this owned session. A deadline must
            # not orphan a compiler or crawler node after killing just Cargo.
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=5)
            raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--crawler-checkout", type=pathlib.Path, required=True)
    parser.add_argument("--redis-server", type=pathlib.Path, required=True)
    parser.add_argument("--keyper-binary", type=pathlib.Path, default=pathlib.Path("target/debug/keyper"))
    parser.add_argument("--output", type=pathlib.Path, required=True, help="new external directory for build/logs")
    parser.add_argument("--crawler-tests", action="store_true", help=(
        "also run unmodified default tests, PING on both servers, and legacy Redis suites on Redis only"))
    args = parser.parse_args()
    for name in ("crawler_checkout", "redis_server", "keyper_binary", "output"):
        setattr(args, name, getattr(args, name).resolve())
    assert not args.output.is_relative_to(args.crawler_checkout), "output must be outside crawler checkout"
    checked_scripts(args.crawler_checkout)
    version = subprocess.check_output([str(args.redis_server), "--version"], text=True, timeout=5).strip()
    assert "v=7.4." in version, version
    assert args.keyper_binary.is_file(), "build Keyper first"
    args.output.mkdir()  # Refuse existing directories; never overwrite user state.
    env = clean_env(args.output)
    manifest = str(args.crawler_checkout / "Cargo.toml")
    target = str(args.output / "crawler-target")
    cargo = ["cargo", "+1.99.0"]
    try:
        external_command([*cargo, "build", "--release", "--locked", "--manifest-path", manifest,
                          "--target-dir", target], args.output, "crawler-build", env)
        binary = pathlib.Path(target) / "release" / "swarmcrawl"
        if args.crawler_tests:
            external_command([*cargo, "test", "--locked", "--manifest-path", manifest,
                              "--target-dir", target], args.output, "crawler-default-tests", env)
        results = {}
        with servers(args, args.output, env) as endpoints:
            for name, url in endpoints:
                if args.crawler_tests:
                    test_env = dict(env, SWARMCRAWL_REDIS_URL=url + "/15")
                    for key in ("SWARMCRAWL_FETCH_TIMEOUT_SECS", "SWARMCRAWL_REDIS_TIMEOUT_SECS"):
                        test_env.pop(key, None)
                    external_command([*cargo, "test", "--locked", "--manifest-path", manifest,
                                      "--target-dir", target, "--test", "redis_connectivity", "--", "--ignored"],
                                     args.output, name + "-crawler-ping", test_env)
                for nodes, db in ((1, 0), (3, 15)):
                    label = f"{name}-{nodes}-db{db}"
                    print(f"Running {label}", flush=True)
                    case_env = dict(env, SWARMCRAWL_REDIS_URL=f"{url}/{db}",
                                    SWARMCRAWL_JOB_NAMESPACE=f"keyper:process:{nodes}")
                    results[label] = run_case(binary, args.output / label, case_env, nodes)
                if name == "redis" and args.crawler_tests:
                    # Original helpers need SCAN/DEL/LRANGE/LSET/TTL. Run them
                    # unchanged on the owned reference, never label that Keyper evidence.
                    base = [*cargo, "test", "--locked", "--manifest-path", manifest, "--target-dir", target]
                    external_command([*base, "--lib", "--", "--ignored"],
                                     args.output, "redis-crawler-pipeline-tests", test_env, timeout=180)
                    external_command([*base, "--test", "redis_jobs", "--test", "redis_frontier",
                                      "--test", "node_process", "--test", "cli_jobs", "--", "--ignored"],
                                     args.output, "redis-crawler-legacy-tests", test_env, timeout=180)
        # Scheduling, port and node-local timing differ. Expected final states,
        # WebStats and exact HTTP GET counts must not depend on server or N.
        oracle = results["redis-1-db0"]
        for result in results.values():
            for field in ("final", "stats", "overlap", "overlap_stats", "delayed_stats", "aborted", "failed"):
                assert result[field] == oracle[field], (field, result[field], oracle[field])
            assert result["http"]["get_counts"] == oracle["http"]["get_counts"]
        report = dict(crawler_revision=REVISION, redis_version=version, cases=results,
                      crawler_tests=args.crawler_tests,
                      crawler_lock_sha256=hashlib.sha256((args.crawler_checkout / "Cargo.lock").read_bytes()).hexdigest(),
                      crawler_binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                      keyper_binary_sha256=hashlib.sha256(args.keyper_binary.read_bytes()).hexdigest(),
                      rustc=subprocess.check_output(["rustc", "+1.99.0", "--version"], text=True, timeout=5).strip(),
                      python=sys.version, platform=list(os.uname()),
                      legacy_keyper_gap="SCAN/DEL cleanup; LRANGE/LSET/TTL fixtures; not run on Keyper",
                      release_gap="Docker managed-cluster lifecycle and live demo not verified")
        (args.output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
        print(f"PASS: four full-process cases; 69 GETs / 68 paths per case; logs/report in {args.output}")
    finally:
        checked_scripts(args.crawler_checkout)


if __name__ == "__main__":
    main()
