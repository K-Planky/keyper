#!/usr/bin/env python3
"""Opt-in sequential numeric/set comparison; owns both endpoints and all state.

Requires Python 3 and a separately supplied Redis 7.4.x server executable.
Core Rust tests do not depend on this script or an external Redis service.
"""

import argparse
import contextlib
import pathlib
import random
import signal
import socket
import subprocess
import tempfile
import time


def read_reply(stream):
    line = stream.readline()
    assert line.endswith(b"\r\n"), "truncated RESP reply"
    kind, value = line[:1], line[1:-2]
    if kind in (b"+", b"-"):
        return kind, value
    if kind == b":":
        return kind, int(value)
    if kind == b"$":
        length = int(value)
        if length == -1:
            return kind, None
        data = stream.read(length)
        assert len(data) == length and stream.read(2) == b"\r\n"
        return kind, data
    if kind == b"*":
        length = int(value)
        return kind, None if length == -1 else [read_reply(stream) for _ in range(length)]
    raise AssertionError(f"unknown RESP reply type: {kind!r}")


def command(sock, stream, args):
    wire = b"*%d\r\n" % len(args)
    for arg in args:
        wire += b"$%d\r\n" % len(arg) + arg + b"\r\n"
    sock.sendall(wire)
    return read_reply(stream)


@contextlib.contextmanager
def process(args, log_path):
    # Keep logs off a pipe so a verbose reference process cannot deadlock.
    with log_path.open("w") as log:
        child = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT)
        try:
            yield child
        finally:
            if child.poll() is None:
                child.send_signal(signal.SIGINT)
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=5)
                raise AssertionError("owned server did not stop within five seconds")
            if child.returncode != 0:
                raise AssertionError(
                    f"owned server exited {child.returncode}: {log_path.read_text()!r}"
                )


def corpus():
    yield [b"SELECT", b"15"]  # Only first-command selection is shared behavior.
    yield [b"INCR", b"counter"]
    for text in [
        b"-9223372036854775808", b"-1", b"0", b"9007199254740992",
        b"9223372036854775806", b"9223372036854775807", b"", b"+1", b"01",
        b"-0", b"-01", b" 1", b"1 ", b"1.0", b"1e0", b"1\0", b"\xff",
        b"9223372036854775808", b"-9223372036854775809", b"9" * 8192,
    ]:
        yield [b"SET", b"counter", text]
        yield [b"INCR", b"counter"]
        yield [b"GET", b"counter"]
    for name, required, variadic in [
        (b"INCR", 1, False), (b"SADD", 2, True), (b"SREM", 2, True),
        (b"SISMEMBER", 2, False), (b"SCARD", 1, False), (b"SMEMBERS", 1, False),
    ]:
        for count in range(required + 2):
            if count != required and not (variadic and count > required):
                yield [name] + [b"set"] * count
    for key in [b"missing", b"set", b"string", b"list"]:
        if key == b"set":
            yield [b"SADD", key, b"", b"\0\xff\r\n", b"01", b"1", b""]
        elif key == b"string":
            yield [b"SET", key, b"42"]
        elif key == b"list":
            yield [b"RPUSH", key, b"v"]
        for args in [
            [b"SCARD"], [b"SMEMBERS"], [b"SISMEMBER", b""],
            [b"SADD", b"new", b"new"], [b"SREM", b"new", b"new"], [b"INCR"], [b"GET"],
        ]:
            yield [args[0], key] + args[1:]
    # Deterministic churn, duplicate arguments, byte identity and empty deletion.
    rng = random.Random(9041)
    pool = [b"", b"1", b"01", b"\0\xff\r\n"] + [str(n).encode() for n in range(20)]
    for _ in range(600):
        name = rng.choice([b"sAdD", b"sReM"])
        yield [name, b"churn"] + rng.choices(pool, k=rng.randrange(1, 12))
        yield [b"SCARD", b"churn"]
        yield [b"SMEMBERS", b"churn"]
        yield [b"SISMEMBER", b"churn", rng.choice(pool)]
    yield [b"SREM", b"churn"] + pool
    yield [b"EXISTS", b"churn"]
    yield [b"SMEMBERS", b"churn"]
    yield [b"RPUSH", b"churn", b"now-a-list"]
    yield [b"SET", b"set", b"0"]
    yield [b"INCR", b"set"]


def compare(corpus_factory, normalize, description):
    """Shared owned-endpoint runner for the numeric/set and hash corpora."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--keyper", default="target/debug/keyper")
    parser.add_argument("--redis-server", required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="keyper-command-reference-") as scratch:
        # Redis uses an owned Unix socket, with TCP/persistence disabled. Keyper
        # binds its own OS-assigned loopback port. No existing server is touched.
        redis_socket = str(pathlib.Path(scratch) / "redis.sock")
        keyper_log = pathlib.Path(scratch) / "keyper.log"
        with contextlib.ExitStack() as stack:
            reference = stack.enter_context(process(
                [args.redis_server, "--port", "0", "--unixsocket", redis_socket,
                 "--save", "", "--appendonly", "no", "--dir", scratch],
                pathlib.Path(scratch) / "redis.log",
            ))
            keyper = stack.enter_context(process(
                [str(pathlib.Path(args.keyper).resolve()), "--port", "0"], keyper_log,
            ))
            deadline = time.monotonic() + 10
            while True:
                line = keyper_log.read_text().strip()
                if line.startswith("keyper: listening on "):
                    # A socket pathname alone can appear before listen(). Probe
                    # actual readiness, retrying only these startup failures.
                    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    probe.settimeout(5)
                    try:
                        probe.connect(redis_socket)
                    except (FileNotFoundError, ConnectionRefusedError):
                        probe.close()
                    except BaseException:
                        probe.close()
                        raise
                    else:
                        redis_sock = stack.enter_context(probe)
                        break
                assert keyper.poll() is None, f"Keyper failed: {line!r}"
                assert reference.poll() is None, "reference Redis failed at startup"
                assert time.monotonic() < deadline, f"server startup deadline: {line!r}"
                time.sleep(0.01)
            host, port = line.rsplit(" ", 1)[1].split(":")
            keyper_sock = stack.enter_context(
                socket.create_connection((host, int(port)), timeout=5)
            )
            sockets = [keyper_sock, redis_sock]
            streams = [stack.enter_context(sock.makefile("rb")) for sock in sockets]
            count = 0
            for args in corpus_factory():
                replies = [
                    normalize(args, command(sock, stream, args))
                    for sock, stream in zip(sockets, streams)
                ]
                assert replies[0] == replies[1], f"comparison {count}, command {args[0]!r}: {replies!r}"
                count += 1
    print(f"PASS: {count} commands match reference Redis (only unordered collections normalized)")


def normalize(args, reply):
    kind, value = reply
    if args[0].upper() == b"SMEMBERS" and kind == b"*":
        return kind, sorted(value)
    return reply


if __name__ == "__main__":
    compare(corpus, normalize, __doc__)
