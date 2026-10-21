#!/usr/bin/env python3
"""Opt-in hash comparison against a separately supplied Redis 7.4.x executable.

Owns both endpoints through the numeric/set harness's bounded process runner.
Only HGETALL pair order and HVALS/SMEMBERS element order are normalized.
"""

import random
import sys

# Import the existing runner without leaving artifacts in the source tree.
sys.dont_write_bytecode = True
from compare_numeric_sets import compare


def corpus():
    yield [b"SELECT", b"15"]
    for name in [b"HSET", b"HGET", b"HDEL", b"HEXISTS", b"HLEN", b"HGETALL", b"HVALS"]:
        for count in range(8):
            if name == b"HSET":
                valid = count >= 3 and count % 2 == 1
            elif name == b"HDEL":
                valid = count >= 2
            elif name in (b"HGET", b"HEXISTS"):
                valid = count == 2
            else:
                valid = count == 1
            if not valid:
                yield [name] + [b"h"] * count
    for key in [b"missing", b"string", b"list", b"set", b"hash"]:
        if key == b"string":
            yield [b"SET", key, b"42"]
        elif key == b"list":
            yield [b"RPUSH", key, b"v"]
        elif key == b"set":
            yield [b"SADD", key, b"m"]
        elif key == b"hash":
            yield [b"HSET", key, b"", b"", b"\0\xff\r\n", b"9007199254740993", b"dup", b""]
        for tail in [
            [b"HGET", b""], [b"HEXISTS", b""], [b"HLEN"], [b"HGETALL"], [b"HVALS"],
            [b"HSET", b"f", b"v", b"f", b"last"], [b"HDEL", b"f", b"f", b"absent"],
        ]:
            yield [tail[0], key] + tail[1:]
    for tail in [
        [b"GET"], [b"INCR"], [b"LPUSH", b"v"], [b"RPUSH", b"v"],
        [b"BLPOP", b"0"], [b"BRPOP", b"0"], [b"SADD", b"m"], [b"SREM", b"m"],
        [b"SISMEMBER", b"m"], [b"SCARD"], [b"SMEMBERS"],
    ]:
        yield [tail[0], b"hash"] + tail[1:]
    rng = random.Random(9092)
    fields = [b"", b"\0\xff\r\n", b"1", b"01"] + [str(n).encode() for n in range(30)]
    values = [b"", b"\xff\0\r\n", b"9007199254740993", b"18446744073709551615", b"same"]
    for _ in range(500):
        if rng.randrange(2):
            args = [b"hSeT", b"churn"]
            for field in rng.choices(fields, k=rng.randrange(1, 16)):
                args += [field, rng.choice(values)]
            yield args
        else:
            yield [b"hDeL", b"churn"] + rng.choices(fields, k=rng.randrange(1, 16))
        yield [b"HLEN", b"churn"]
        yield [b"HGETALL", b"churn"]
        yield [b"HVALS", b"churn"]
        yield [b"HGET", b"churn", rng.choice(fields)]
        yield [b"HEXISTS", b"churn", rng.choice(fields)]
    yield [b"HDEL", b"churn"] + fields + fields
    yield [b"EXISTS", b"churn"]
    yield [b"HGETALL", b"churn"]
    yield [b"HVALS", b"churn"]
    yield [b"RPUSH", b"churn", b"now-list"]
    yield [b"SET", b"hash", b"replaced"]
    yield [b"HGETALL", b"hash"]
    yield [b"GET", b"hash"]


def normalize(args, reply):
    kind, value = reply
    name = args[0].upper()
    if kind == b"*" and name == b"HGETALL":
        assert value is not None and len(value) % 2 == 0, "flat field/value array"
        assert all(kind == b"$" and data is not None for kind, data in value)
        pairs = list(zip(value[::2], value[1::2]))
        assert len({field for field, _ in pairs}) == len(pairs), "unique fields"
        return kind, sorted(pairs)
    if kind == b"*" and name in (b"HVALS", b"SMEMBERS"):
        assert value is not None
        assert all(kind == b"$" and data is not None for kind, data in value)
        return kind, sorted(value)  # Retain duplicate values, not a set.
    return reply


if __name__ == "__main__":
    compare(corpus, normalize, __doc__)
