#!/usr/bin/env python3
"""Opt-in unchanged swarmcrawl script components, not a crawler process test.

Reads the pinned clean external checkout; never builds or writes inside it.
Runs the same invariant/expected-result checks against two owned endpoints.
Only unordered hash/set enumeration is canonicalized, not script replies.
"""

import argparse
import concurrent.futures
import contextlib
import hashlib
import os
import pathlib
import subprocess
import sys
import threading
import time

sys.dont_write_bytecode = True
from compare_numeric_sets import command, connection, owned_endpoints

REVISION = "f2c3a591af66e9a424178d9fbc5836740a71e896"
DIGESTS = {
    "abort": "35e08e6e8a798730127a1a908f27c74a6153dbddd92d53655b1794bcf64cf7a6",
    "claim": "c04e63f2a4d2441b1d1dce09c3b9d4f0e36ef3de269f0ca810bb89ed9cf3ffe2",
    "complete": "0a09d1bc753c1cfad65aed342a4fb962a7ec19d265026021578d52ef401d8758",
    "protocol": "3232bd7f015089abb047053ea0837d6b137896b61b045f41a2ce088a82026c09",
    "submit": "e0673470872a0067ea854501f923ba30f836bc75d3cd1fa8ffa6dbf6bdc9d673",
    "worker": "e139fdba7240477733721245f5c6e04d6fb389d40baf5d535cefa4448a052997",
}
MAXIMUM = b"18446744073709551615"  # Audited 64-bit crawler usize/u64.
BASE = b"http://fixture.invalid/docs/"
SUFFIXES = [b"meta", b"seen", b"frontier", b"in-flight", b"stats", b"extensions"]


def checked_scripts(checkout):
    env = dict(os.environ, GIT_OPTIONAL_LOCKS="0")

    def git(*args):
        return subprocess.check_output(
            ["git", "-C", str(checkout), *args], env=env, timeout=10
        ).strip()

    assert git("rev-parse", "HEAD").decode() == REVISION, "wrong crawler revision"
    assert not git("status", "--porcelain=v1", "--untracked-files=all"), "crawler is not clean"
    scripts = {}
    for name, digest in DIGESTS.items():
        source = (checkout / "src" / "jobs" / f"{name}.lua").read_bytes()
        assert hashlib.sha256(source).hexdigest() == digest, f"changed {name}.lua"
        scripts[name] = source
    # Exactly frontier.rs's concat!(protocol, "\n", operation); no wrappers.
    return {
        name: scripts[name] if name in ("submit", "worker") else
        scripts["protocol"] + b"\n" + scripts[name]
        for name in ("submit", "worker", "claim", "complete", "abort")
    }


def decode(reply):
    kind, value = reply
    assert kind != b"-", f"unexpected Redis error: {value!r}"
    if kind == b"*" and value is not None:
        return [decode(item) for item in value]
    return value


def hash_map(pairs):
    assert len(pairs) % 2 == 0
    result = dict(zip(pairs[::2], pairs[1::2]))
    assert len(result) * 2 == len(pairs), "duplicate hash fields"
    return result


class Suite:
    def __init__(self, endpoint, scripts, database):
        self.endpoint, self.scripts, self.database = endpoint, scripts, database
        self.commands = 0
        self.lock = threading.Lock()
        self.serial = 0

    @contextlib.contextmanager
    def client(self):
        with connection(self.endpoint) as client:
            assert self.call(client, b"SELECT", str(self.database).encode()) == b"OK"
            yield client

    def request(self, client, *args):
        reply = command(*client, list(args))
        with self.lock:
            self.commands += 1
        return reply

    def call(self, client, *args):
        return decode(self.request(client, *args))

    def eval(self, client, name, keys, args=()):
        reply = self.request(client, b"EVAL", self.scripts[name], str(len(keys)).encode(), *keys, *args)
        # All five compositions return arrays of bulk strings, not statuses or
        # rounded numeric replies. Preserve that wire contract as well as bytes.
        assert reply[0] == b"*" and reply[1] is not None, reply
        assert all(kind == b"$" and value is not None for kind, value in reply[1]), reply
        return decode(reply)

    def namespace(self):
        self.serial += 1
        return f"components:db{self.database}:case{self.serial}".encode()

    def keys(self, ns, job=b"1"):
        return [ns + b":job:" + job + b":" + suffix for suffix in SUFFIXES] + [ns + b":active"]

    def submit(self, client, ns, base=BASE):
        return self.eval(client, "submit", [ns + b":" + suffix for suffix in
                         (b"submissions", b"next-job-id", b"active")], [ns, base])

    def transition(self, client, ns, name, args=(), job=b"1", maximum=MAXIMUM):
        return self.eval(client, name, self.keys(ns, job), [job, maximum, *args])

    def claim(self, client, ns, worker=b"node-1"):
        return self.transition(client, ns, "claim", [worker])

    def complete(self, client, ns, parent=BASE, children=(), worker=b"node-1",
                 outcome=b"file", words=b"1", extension=b"html", maximum=MAXIMUM):
        return self.transition(client, ns, "complete",
                               [worker, parent, BASE, outcome, words, extension, *children],
                               maximum=maximum)

    def snapshot(self, client, ns, job=b"1"):
        keys = self.keys(ns, job)
        assert self.call(client, b"MULTI") == b"OK"
        for op, key in zip([b"HGETALL", b"SCARD", b"LLEN", b"HLEN", b"HGETALL", b"HGETALL"], keys):
            assert self.call(client, op, key) == b"QUEUED"
        result = self.call(client, b"EXEC")
        assert len(result) == 6
        for i in (0, 4, 5):
            result[i] = hash_map(result[i])
        return result

    def coherent(self, snapshot):
        meta, seen, queued, owned, stats, extensions = snapshot
        processed = int(meta[b"processed"])
        assert seen == processed + queued + owned, snapshot
        assert sum(map(int, extensions.values())) == int(stats[b"num_files"])
        assert len(extensions) == int(stats[b"num_exts"])
        assert int(stats[b"num_files"]) <= processed
        if meta[b"state"] == b"done":
            assert queued == owned == 0
        else:
            assert meta[b"state"] == b"running" and queued + owned > 0

    def parallel(self, count, operation):
        gate = threading.Barrier(count)

        def run(i):
            with self.client() as client:
                gate.wait(timeout=10)
                return operation(client, i)

        with concurrent.futures.ThreadPoolExecutor(max_workers=count) as pool:
            futures = [pool.submit(run, i) for i in range(count)]
            return [future.result(timeout=20) for future in futures]

    def identity(self, client):
        ns = self.namespace()
        replies = self.parallel(16, lambda c, i: self.submit(c, ns))
        assert replies.count([b"created", b"1"]) == 1
        assert replies.count([b"existing", b"1"]) == 15
        self.coherent(self.snapshot(client, ns))
        replies = self.parallel(12, lambda c, i: self.submit(c, ns, BASE + str(i).encode()))
        assert all(r[0] == b"created" for r in replies)
        assert sorted(int(r[1]) for r in replies) == list(range(2, 14))
        assert sorted(self.call(client, b"HVALS", ns + b":submissions")) == sorted(
            str(i).encode() for i in range(1, 14))
        for job in range(1, 14):
            self.coherent(self.snapshot(client, ns, str(job).encode()))

        ns = self.namespace()
        assert self.eval(client, "worker", [ns + b":next-worker-id"]) == [b"worker", b"node-1"]
        for initial in (b"9007199254740992", b"9223372036854775791"):
            ns = self.namespace()
            self.call(client, b"SET", ns + b":next-worker-id", initial)
            replies = self.parallel(16, lambda c, i: self.eval(c, "worker", [ns + b":next-worker-id"]))
            expected = {b"node-" + str(int(initial) + n).encode() for n in range(1, 17)}
            assert all(r[0] == b"worker" for r in replies)
            assert {r[1] for r in replies} == expected
            assert self.call(client, b"GET", ns + b":next-worker-id") == str(int(initial) + 16).encode()
            if initial.startswith(b"922"):
                assert self.eval(client, "worker", [ns + b":next-worker-id"]) == [b"exhausted"]

        for initial in (b"9007199254740992", b"9223372036854775806"):
            ns = self.namespace()
            self.call(client, b"SET", ns + b":next-job-id", initial)
            job = str(int(initial) + 1).encode()
            assert self.submit(client, ns) == [b"created", job]
            assert self.submit(client, ns) == [b"existing", job]
            assert self.snapshot(client, ns, job)[0][b"base"] == BASE
            if initial.startswith(b"922"):
                assert self.submit(client, ns, BASE + b"other") == [b"exhausted"]

    def publication(self, client):
        ns = self.namespace()
        assert self.submit(client, ns) == [b"created", b"1"]
        assert self.claim(client, ns) == [b"claimed", BASE, BASE]
        assert self.claim(client, ns, b"node-other") == [b"idle"]
        assert self.snapshot(client, ns)[0][b"state"] == b"running"  # Empty frontier != done.
        children = [BASE + str(i).encode() for i in range(12)]
        assert self.complete(client, ns, children=children * 2 + [BASE], words=b"7") == [b"published", b"running"]
        claims = self.parallel(24, lambda c, i: self.claim(c, ns, f"node-{i}".encode()))
        owned = [(f"node-{i}".encode(), r[2]) for i, r in enumerate(claims) if r[0] == b"claimed"]
        assert sorted(url for _, url in owned) == sorted(children)
        assert sum(r == [b"idle"] for r in claims) == 12
        before = self.snapshot(client, ns)
        assert self.complete(client, ns, parent=owned[0][1], worker=b"not-owner") == [b"ownership"]
        assert self.snapshot(client, ns) == before
        assert self.complete(client, ns) == [b"already"]

        # A reader starts before publication and checks the actual six-read EXEC
        # shape repeatedly while writers race. No sleeps select an ordering.
        started, finished = threading.Event(), threading.Event()

        def observe():
            with self.client() as reader:
                count = 0
                deadline = time.monotonic() + 20
                while not finished.is_set() or count < 2:
                    self.coherent(self.snapshot(reader, ns))
                    started.set()
                    count += 1
                    assert time.monotonic() < deadline, "snapshot observation deadline"
                return count

        common = BASE + b"shared"
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            reader = pool.submit(observe)
            try:
                assert started.wait(timeout=10)

                def publish(c, i):
                    worker, url = owned[i]
                    return self.complete(c, ns, parent=url, worker=worker,
                                         children=[common, common, BASE, url + b"/leaf"], words=b"2")

                assert self.parallel(12, publish) == [[b"published", b"running"]] * 12
            finally:
                finished.set()
            assert reader.result(timeout=20) >= 2
        after = self.snapshot(client, ns)
        self.coherent(after)
        assert after[1:4] == [26, 13, 0]
        assert after[0][b"processed"] == b"13"
        assert after[4] == {b"num_files": b"13", b"num_exts": b"1", b"total_word_count": b"31"}
        assert self.complete(client, ns, parent=owned[0][1], worker=owned[0][0]) == [b"already"]
        assert self.snapshot(client, ns) == after
        remaining = []
        for i in range(13):
            reply = self.claim(client, ns)
            assert reply[:2] == [b"claimed", BASE]
            remaining.append(reply[2])
            assert self.complete(client, ns, parent=reply[2], outcome=b"no-file") == [
                b"published", b"done" if i == 12 else b"running"]
        assert set(remaining) == {common, *(url + b"/leaf" for url in children)}
        final = self.snapshot(client, ns)
        self.coherent(final)
        assert final[0][b"processed"] == b"26" and final[0][b"state"] == b"done"
        assert final[4] == after[4]  # Immediately readable, retained final stats.
        assert self.call(client, b"SISMEMBER", ns + b":active", b"1") == 0
        assert self.transition(client, ns, "abort") == [b"unchanged"]
        assert self.submit(client, ns) == [b"existing", b"1"]
        assert self.claim(client, ns) == [b"idle"]
        assert self.snapshot(client, ns) == final

        ns = self.namespace()
        self.submit(client, ns)
        self.claim(client, ns)
        # Duplicate completion racing itself must merge exactly one contribution.
        replies = self.parallel(16, lambda c, i: self.complete(c, ns, words=b"9007199254740993"))
        assert replies.count([b"published", b"done"]) == 1
        assert replies.count([b"already"]) == 15
        retained = self.snapshot(client, ns)
        self.coherent(retained)
        assert retained[0][b"processed"] == b"1"
        assert retained[4] == {b"num_files": b"1", b"num_exts": b"1",
                              b"total_word_count": b"9007199254740993"}
        return final, retained

    def failures(self, client):
        results = []
        for outcome in (b"abort", b"fetch", b"statistics", b"protocol"):
            ns = self.namespace()
            assert self.submit(client, ns) == [b"created", b"1"]
            assert self.claim(client, ns) == [b"claimed", BASE, BASE]
            before = self.snapshot(client, ns)
            expected = [b"aborted"] if outcome == b"abort" else [b"failed", outcome]
            if outcome == b"abort":
                assert self.transition(client, ns, "abort") == expected
            else:
                assert self.complete(client, ns, outcome=outcome) == expected
            terminal = self.snapshot(client, ns)
            assert terminal[1:] == before[1:]
            assert terminal[0][b"state"] == (b"aborted" if outcome == b"abort" else b"failed")
            assert self.call(client, b"SISMEMBER", ns + b":active", b"1") == 0
            assert self.complete(client, ns, children=[BASE + b"late"]) == expected
            assert self.claim(client, ns) == [b"idle"]
            assert self.transition(client, ns, "abort") == [b"unchanged"]
            assert self.submit(client, ns) == [b"existing", b"1"]
            assert self.snapshot(client, ns) == terminal
            results.append(terminal)
        # Abort vs publication may choose either serialization, never a hybrid.
        ns = self.namespace()
        self.submit(client, ns)
        self.claim(client, ns)
        raced = self.parallel(2, lambda c, i: self.transition(c, ns, "abort") if i == 0 else self.complete(c, ns))
        final = self.snapshot(client, ns)
        if raced[0] == [b"aborted"]:
            assert raced[1] == [b"aborted"] and final[3] == 1 and final[0][b"processed"] == b"0"
        else:
            assert raced == [[b"unchanged"], [b"published", b"done"]]
            assert final[3] == 0 and final[0][b"processed"] == b"1"
        return results  # Exclude the deliberately nondeterministic race winner.

    def boundaries(self, client):
        ns = self.namespace()
        self.submit(client, ns)
        self.claim(client, ns)
        assert self.complete(client, ns, words=MAXIMUM) == [b"published", b"done"]
        assert self.snapshot(client, ns)[4][b"total_word_count"] == MAXIMUM
        for maximum, words in [(MAXIMUM, b"18446744073709551614"), (b"1", b"0")]:
            ns = self.namespace()
            self.submit(client, ns)
            self.claim(client, ns)
            assert self.complete(client, ns, children=[BASE + b"a", BASE + b"b"], words=words,
                                 maximum=maximum) == [b"published", b"running"]
            self.claim(client, ns)
            if maximum == MAXIMUM:
                assert self.complete(client, ns, parent=BASE + b"a", words=b"1") == [b"published", b"running"]
                assert self.snapshot(client, ns)[4][b"total_word_count"] == MAXIMUM
                self.claim(client, ns)
                parent = BASE + b"b"
            else:
                parent = BASE + b"a"
            before = self.snapshot(client, ns)
            assert self.complete(client, ns, parent=parent, words=b"1", children=[BASE + b"not-added"],
                                 maximum=maximum) == [b"failed", b"statistics"]
            after = self.snapshot(client, ns)
            assert after[1:] == before[1:] and after[0][b"processed"] == before[0][b"processed"]
            assert self.complete(client, ns, parent=parent) == [b"failed", b"statistics"]

    def invalid(self, client):
        for text in (b"", b"01", b"-1", b"+1", b"1.0", b"1e2", b"9223372036854775808", b"9" * 80):
            ns = self.namespace()
            for suffix, script in [(b"next-job-id", "submit"), (b"next-worker-id", "worker")]:
                key = ns + b":" + suffix
                self.call(client, b"SET", key, text)
                reply = self.submit(client, ns) if script == "submit" else self.eval(client, script, [key])
                assert reply == [b"invalid"]
                assert self.call(client, b"GET", key) == text
            assert self.call(client, b"EXISTS", ns + b":submissions", ns + b":active") == 0
        for suffix in (b"submissions", b"next-job-id", b"active"):
            ns = self.namespace()
            key = ns + b":" + suffix
            self.call(client, b"RPUSH", key, b"wrong-type")
            assert self.submit(client, ns) == [b"invalid"]
            assert self.call(client, b"LINDEX", key, b"0") == b"wrong-type"
            assert self.call(client, b"EXISTS", self.keys(ns)[0]) == 0
        ns = self.namespace()
        self.call(client, b"SADD", ns + b":next-worker-id", b"wrong-type")
        assert self.eval(client, "worker", [ns + b":next-worker-id"]) == [b"invalid"]
        for orphan in range(6):
            ns = self.namespace()
            key = self.keys(ns)[orphan]
            self.call(client, b"SET", key, b"orphan")
            assert self.submit(client, ns) == [b"invalid"]
            assert self.call(client, b"GET", ns + b":next-job-id") == b"1"  # Retained ID gap, not rollback.
            assert self.call(client, b"EXISTS", ns + b":submissions", ns + b":active") == 0
            assert self.call(client, b"GET", key) == b"orphan"
            assert self.submit(client, ns) == [b"created", b"2"]
        for existing in (b"01", b"9223372036854775808", b"1"):
            ns = self.namespace()
            self.call(client, b"HSET", ns + b":submissions", BASE, existing)
            assert self.submit(client, ns) == [b"invalid"]  # Missing/malformed retained metadata.
        ns = self.namespace()
        assert self.transition(client, ns, "claim", [b"node-1"]) == [b"unknown"]
        self.call(client, b"SADD", self.keys(ns)[1], BASE)
        assert self.transition(client, ns, "abort") == [b"invalid"]  # Orphan without meta.
        # Every protocol key's type preflight rejects corruption, without reset.
        for index in range(7):
            ns = self.namespace()
            self.submit(client, ns)
            key = self.keys(ns)[index]
            self.call(client, b"SET", key, b"corrupt")
            for name, args in [("claim", [b"node-1"]), ("abort", []),
                               ("complete", [b"node-1", BASE, BASE, b"file", b"1", b"html"])]:
                assert self.transition(client, ns, name, args) == [b"invalid"]
                assert self.call(client, b"GET", key) == b"corrupt"
        for index, fields in [(0, [b"schema", b"2"]), (0, [b"processed", b"01"]),
                              (0, [b"state", b"done"]), (0, [b"failure", b"fetch"]),
                              (0, [b"extra", b"field"]), (4, [b"num_files", b"1"]),
                              (4, [b"total_word_count", b"18446744073709551616"]),
                              (5, [b"html", b"0"])]:
            ns = self.namespace()
            self.submit(client, ns)
            self.call(client, b"HSET", self.keys(ns)[index], *fields)
            before = self.snapshot(client, ns)
            assert self.claim(client, ns) == [b"invalid"]
            assert self.transition(client, ns, "abort") == [b"invalid"]
            assert self.snapshot(client, ns) == before
        ns = self.namespace()
        self.submit(client, ns)
        self.claim(client, ns)
        for changes in [dict(children=[BASE + b"valid", b"http://outside.invalid/"]),
                        dict(words=b"01"), dict(words=b"18446744073709551616"),
                        dict(extension=b""), dict(outcome=b"bad")]:
            before = self.snapshot(client, ns)
            assert self.complete(client, ns, **changes) == [b"invalid"]
            assert self.snapshot(client, ns) == before
        assert self.complete(client, ns, parent=BASE + b"never-seen") == [b"ownership"]
        before = self.snapshot(client, ns)
        assert self.transition(client, ns, "complete",
                               [b"node-1", BASE, b"wrong-base", b"file", b"1", b"html"]) == [b"invalid"]
        assert self.snapshot(client, ns) == before
        for url in (b"http://outside.invalid/", BASE + b"unseen"):
            ns = self.namespace()
            self.submit(client, ns)
            frontier = self.keys(ns)[2]
            assert self.call(client, b"LPOP", frontier) == BASE
            self.call(client, b"RPUSH", frontier, url)
            before = self.snapshot(client, ns)
            assert self.claim(client, ns) == [b"invalid"]
            assert self.snapshot(client, ns) == before

    def run(self):
        with self.client() as client:
            self.identity(client)
            final = self.publication(client)
            failures = self.failures(client)
            self.boundaries(client)
            self.invalid(client)
        return final, failures


def reference_key_priority(endpoint):
    """Live 7.4 priority check; CLIENT LIST is reference-only synchronization."""
    for mode in range(3):
        with connection(endpoint) as writer, connection(endpoint) as waiter:
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                pending = pool.submit(command, *waiter, [b"BLPOP", b"priority:a", b"priority:b", b"0"])
                deadline = time.monotonic() + 3
                while True:
                    clients = decode(command(*writer, [b"CLIENT", b"LIST"]))
                    if any(b"cmd=blpop" in row and b"flags=b" in row for row in clients.splitlines()):
                        break
                    assert time.monotonic() < deadline, "reference waiter did not register"
                    time.sleep(0.005)
                if mode == 0:
                    reply = command(*writer, [b"EVAL", b"redis.call('RPUSH','priority:b','B'); return redis.call('RPUSH','priority:a','A')", b"0"])
                    assert decode(reply) == 1
                else:
                    assert decode(command(*writer, [b"MULTI"])) == b"OK"
                    first = [b"RPUSH", b"priority:b", b"B"] if mode == 1 else [
                        b"EVAL", b"return redis.call('RPUSH','priority:b','B')", b"0"]
                    for args in (first, [b"RPUSH", b"priority:a", b"A"]):
                        assert decode(command(*writer, args)) == b"QUEUED"
                    assert decode(command(*writer, [b"EXEC"])) == [1, 1]
                assert decode(pending.result(timeout=5)) == [b"priority:a", b"A"]
                assert decode(command(*writer, [b"LPOP", b"priority:b"])) == b"B"
    print("PASS: reference EVAL/EXEC/mixed-batch blocked-pop priority is left-to-right")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--keyper", default="target/debug/keyper")
    parser.add_argument("--redis-server", required=True)
    parser.add_argument("--crawler-checkout", type=pathlib.Path, required=True)
    args = parser.parse_args()
    scripts = checked_scripts(args.crawler_checkout)
    try:
        with owned_endpoints(args.keyper, args.redis_server) as endpoints:
            reference_key_priority(endpoints[1])
            for database in (0, 15):
                results = []
                for name, endpoint in zip(("Keyper", "Redis"), endpoints):
                    suite = Suite(endpoint, scripts, database)
                    results.append(suite.run())
                    print(f"PASS: {name} DB {database}: unchanged-script invariants ({suite.commands} commands)")
                assert results[0] == results[1], "retained snapshots differ from reference"
    finally:
        assert checked_scripts(args.crawler_checkout) == scripts, "crawler changed during verification"
    print(f"PASS: five exact script compositions; retained snapshots match; crawler unchanged at {REVISION}")


if __name__ == "__main__":
    main()
