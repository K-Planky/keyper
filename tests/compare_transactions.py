#!/usr/bin/env python3
"""Opt-in MULTI/EXEC/DISCARD RESP2 comparison with owned Redis 7.4.x.

Queue control, admission/runtime errors, retained writes, and nested replies are
compared exactly. Only unknown-command diagnostics omit reflected user input in
Keyper; that error's category is normalized. Project queue/SELECT/key limits
are covered by Rust tests, not compared to Redis's different policies.
"""

import random
import sys

sys.dont_write_bytecode = True
from compare_numeric_sets import compare


def corpus():
    yield [b"SELECT", b"15"]
    for name in [b"MULTI", b"EXEC", b"DISCARD"]:
        yield [name, b"extra"]
    yield [b"EXEC"]
    yield [b"DISCARD"]
    yield [b"MULTI"]
    yield [b"EXEC"]
    for bad in [
        [b"GET"], [b"MULTI", b"extra"],
        [b"DISCARD", b"extra"],
        [b"HSET", b"h", b"f"], [b"UNKNOWN"],
    ]:
        for end in [b"EXEC", b"DISCARD"]:
            yield [b"MULTI"]
            yield [b"SET", b"absent", b"must-not-apply"]
            yield bad
            yield [b"INCR", b"absent"]
            yield [end]
            yield [b"GET", b"absent"]
    for dirty in [False, True]:
        yield [b"MULTI"]
        yield [b"SET", b"absent", b"must-not-apply"]
        if dirty:
            yield [b"GET"]
        yield [b"EXEC", b"extra"]
        yield [b"EXEC"]
        yield [b"GET", b"absent"]
    yield [b"MULTI"]
    yield [b"SET", b"counter", b"0"]
    yield [b"MULTI"]  # Does not dirty or replace the existing queue.
    yield [b"INCR", b"counter"]
    yield [b"EXEC"]
    # All supported command families, empty/binary replies and EXEC arrays.
    yield [b"MULTI"]
    for args in [
        [b"PING"], [b"PING", b""], [b"PING", b"\0\xff\r\n"],
        [b"SET", b"binary", b"\0\xff\r\n"], [b"GET", b"binary"],
        [b"EXISTS", b"binary", b"missing", b"binary"], [b"TYPE", b"binary"],
        [b"PING", b"one", b"two"], [b"HSET", b"hash", b"f", b"v", b"unpaired"],
        [b"LPOP", b"list", b"1", b"extra"],
        [b"LPUSH", b"list", b"one", b"two"], [b"RPUSH", b"list", b"three"],
        [b"LLEN", b"list"], [b"LINDEX", b"list", b"-1"], [b"LPOP", b"list", b"0"],
        [b"BRPOP", b"list", b"0"], [b"BLPOP", b"list", b"0"], [b"LPOP", b"list", b"9"],
        [b"LPOP", b"list"], [b"LPOP", b"list", b"1"],
        [b"BLPOP", b"missing", b"0"], [b"BRPOP", b"missing", b"10"],
        [b"SADD", b"set", b"", b""], [b"SISMEMBER", b"set", b""],
        [b"SCARD", b"set"], [b"SMEMBERS", b"set"], [b"SREM", b"set", b""],
        [b"HSET", b"hash", b"", b"\0\xff"], [b"HGET", b"hash", b""],
        [b"HEXISTS", b"hash", b""], [b"HLEN", b"hash"], [b"HGETALL", b"hash"],
        [b"HVALS", b"hash"], [b"HDEL", b"hash", b""],
        [b"SET", b"counter", b"9223372036854775807"], [b"INCR", b"counter"],
        [b"GET", b"counter"], [b"SET", b"counter", b"9007199254740992"],
        [b"INCR", b"counter"], [b"GET", b"counter"],
    ]:
        yield args
    yield [b"EXEC"]
    rng = random.Random(10101)
    operations = [
        [b"SET", b"value", b"bad"], [b"SET", b"value", b"0"], [b"INCR", b"value"],
        [b"PING", b"one", b"two"], [b"HSET", b"value", b"f", b"v", b"unpaired"],
        [b"LPOP", b"value", b"1", b"extra"],
        [b"RPUSH", b"value", b"a", b"b"], [b"LPOP", b"value", b"1"],
        [b"BLPOP", b"value", b"0"], [b"BRPOP", b"missing", b"1"],
        [b"BLPOP", b"value", b"nan"], [b"BLPOP", b"value", b"-1"],
        [b"LPOP", b"value", b"-1"], [b"LINDEX", b"value", b"bad"],
        [b"SADD", b"value", b"member"], [b"SCARD", b"value"],
        [b"HSET", b"value", b"field", b"data"], [b"HGETALL", b"value"],
        [b"PING", b"\0\xff\r\n"], [b"GET", b"value"],
    ]
    for _ in range(150):
        yield [b"mUlTi"]
        for args in rng.choices(operations, k=rng.randrange(1, 20)):
            yield args
        yield [b"dIsCaRd"] if rng.randrange(5) == 0 else [b"eXeC"]
        yield [b"TYPE", b"value"]
        yield [b"GET", b"value"]
    # The crawler's exact six-read snapshot composition (one field per hash
    # avoids pretending unspecified hash enumeration order is a difference).
    for key in [b"meta", b"stats", b"extensions"]:
        yield [b"HSET", key, b"field", b"value"]
    yield [b"SADD", b"seen", b"url"]
    yield [b"RPUSH", b"frontier", b"url"]
    yield [b"HSET", b"in-flight", b"url", b"worker"]
    yield [b"MULTI"]
    for args in [
        [b"HGETALL", b"meta"], [b"SCARD", b"seen"], [b"LLEN", b"frontier"],
        [b"HLEN", b"in-flight"], [b"HGETALL", b"stats"], [b"HGETALL", b"extensions"],
    ]:
        yield args
    yield [b"EXEC"]


def normalize(args, reply):
    if args[0].upper() == b"UNKNOWN":
        assert reply[0] == b"-" and reply[1].startswith(b"ERR unknown command")
        return b"-", b"ERR unknown command"
    return reply


if __name__ == "__main__":
    compare(corpus, normalize, __doc__)
