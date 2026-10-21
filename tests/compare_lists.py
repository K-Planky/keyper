#!/usr/bin/env python3
"""Opt-in TYPE/LLEN/LINDEX/LPOP comparison against supplied Redis 7.4.x.

Reuses the bounded owned-endpoint runner; compares exact RESP2 shapes and order,
including numeric-error precedence. No existing Redis or crawler is touched.
"""

import random
import sys

sys.dont_write_bytecode = True
from compare_numeric_sets import compare


def corpus():
    yield [b"SELECT", b"15"]
    for name in [b"TYPE", b"LLEN", b"LINDEX", b"LPOP"]:
        for count in range(6):
            valid = count == (2 if name == b"LINDEX" else 1)
            valid |= name == b"LPOP" and count == 2
            if not valid:
                yield [name] + [b"list"] * count
    yield [b"SET", b"string", b"42"]
    yield [b"SADD", b"set", b"m"]
    yield [b"HSET", b"hash", b"f", b"v"]
    yield [b"RPUSH", b"list", b"", b"\0\xff\r\n", b"tail"]
    numbers = [
        b"0", b"-0", b"1", b"-1", b"3", b"-3", b"4", b"-4", b"",
        b"+1", b"01", b"-01", b" 1", b"1 ", b"1.0", b"1e0", b"1\0", b"\xff",
        b"9223372036854775807", b"-9223372036854775808",
        b"9223372036854775808", b"-9223372036854775809", b"9" * 8192,
    ]
    for key in [b"missing", b"string", b"set", b"hash", b"list"]:
        yield [b"TYPE", key]
        yield [b"LLEN", key]
        for n in numbers:
            yield [b"LINDEX", key, n]
        for n in numbers:
            yield [b"LPOP", key, n]
        yield [b"LPOP", key]
        yield [b"TYPE", key]
    # Empty vs absent count replies, scalar vs count=1, exact byte identity,
    # signed bounds, pop order, deletion/recreation and deque wraparound churn.
    rng = random.Random(9093)
    pool = [b"", b"\0\xff\r\n", b"1", b"01", b"-0", b"same"]
    for _ in range(600):
        key = rng.choice([b"", b"\0\xff\r\n", b"churn"])
        if rng.randrange(3):
            yield [rng.choice([b"lPuSh", b"rPuSh"]), key] + rng.choices(pool, k=rng.randrange(1, 12))
        else:
            yield [b"lPoP", key] + ([rng.choice([b"0", b"1", b"4", b"9223372036854775807"])] if rng.randrange(2) else [])
        yield [b"tYpE", key]
        yield [b"lLeN", key]
        yield [b"lInDeX", key, rng.choice(numbers)]
    for key in [b"", b"\0\xff\r\n", b"churn"]:
        yield [b"LPOP", key, b"9223372036854775807"]
        yield [b"TYPE", key]
        yield [b"EXISTS", key]
        yield [b"LPOP", key, b"0"]
        yield [b"LPOP", key]
        yield [b"RPUSH", key, b"one", b"two", b"three"]
        yield [b"LPOP", key, b"0"]
        yield [b"LPOP", key]
        yield [b"LPOP", key, b"1"]
        yield [b"LPOP", key, b"2"]
        yield [b"SADD", key, b"m"]
        yield [b"TYPE", key]
        yield [b"SREM", key, b"m"]
        yield [b"TYPE", key]
        yield [b"HSET", key, b"f", b"v"]
        yield [b"TYPE", key]
        yield [b"HDEL", key, b"f"]
        yield [b"TYPE", key]
        yield [b"SET", key, b""]
        yield [b"TYPE", key]


if __name__ == "__main__":
    compare(corpus, lambda args, reply: reply, __doc__)
