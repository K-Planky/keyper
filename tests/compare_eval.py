#!/usr/bin/env python3
"""Opt-in real TCP EVAL/database comparison with owned Redis 7.4.x.

Uses general scripts, not crawler scripts. Collection order alone is normalized
by the scripts; error hash/location decoration and Lua engine diagnostics are
normalized only for the explicitly enumerated error cases below. Project SELECT,
bare SET, blocking-key, and sandbox policies are tested separately in Rust.
"""

import re
import sys

sys.dont_write_bytecode = True
from compare_numeric_sets import compare
from compare_numeric_sets import corpus as numeric_corpus
from compare_hashes import corpus as hash_corpus
from compare_lists import corpus as list_corpus


def eval_(source, keys=(), args=()):
    return [b"EVAL", source, str(len(keys)).encode(), *keys, *args]


def corpus():
    yield [b"SELECT", b"15"]
    yield [b"EVAL"]
    yield [b"EVAL", b"return 1"]
    for numkeys in [b"-1", b"1", b"9223372036854775807", b"-9223372036854775808",
                    b"9223372036854775808", b"00", b"-0", b"+0", b"1.0", b" 0", b"0\0", b"\xff"]:
        for source in [b"return 1", b"!invalid"]:
            yield [b"EVAL", source, numkeys]
    for source in [
        b"", b"return {KEYS,ARGV}", b"return {false,true,nil,3}",
        b"return {{ok='READY'},{err='BUSY controlled'},string.char(0,255,13,10)}",
        b"return redis.call('TYPE','missing').ok",
        b"local t={}; table.insert(t,'v'); return table.concat(t,',')",
    ]:
        yield eval_(source, [b"\0\xff", b"", b"1", b"2", b"3", b"4", b"5"], [b"\0\xff\r\n", b""])
    yield eval_(b"local k=KEYS[1]..ARGV[1]; return {redis.call('SET',k,ARGV[2]),redis.call('GET',k)}", [b"derived"], [b"\0", b"\0\xff\r\n"])
    # Every supported database command's general direct-wire corpus now executes
    # through the Lua bridge, including numeric, type and arity errors.
    bridge = b"""
        local r=redis.pcall(unpack(ARGV))
        if ARGV[1]=='SMEMBERS' or ARGV[1]=='smembers' or ARGV[1]=='HVALS' then
            if not r.err then table.sort(r) end
        elseif ARGV[1]=='HGETALL' and not r.err then
            local p={}; for i=1,#r,2 do p[#p+1]={r[i],r[i+1]} end
            table.sort(p,function(a,b) return a[1]<b[1] end); return p
        end
        return r
    """
    for factory in [numeric_corpus, hash_corpus, list_corpus]:
        for command in factory():
            if command[0].upper() != b"SELECT":
                yield eval_(bridge, args=command)
    for call in [
        b"'GET'", b"'GET','a','b'", b"'UNKNOWN'", b"'MULTI'", b"'EXEC'", b"'DISCARD'",
        b"'EVAL','return 1',0", b"'EVAL'", b"'MULTI','extra'", b"'PING',1,2",
        b"'HSET','h','f','v','unpaired'", b"'LPOP','l',1,2", b"'BLPOP','l','invalid'",
        b"'BRPOP','empty',-1", b"", b"'SET','k',false",
    ]:
        for kind in [b"call", b"pcall"]:
            yield eval_(b"return redis." + kind + b"(" + call + b")")
        yield eval_(b"local ok,e=pcall(function() redis.call(" + call + b") end); return {ok,e}")
    for name in [b"BLPOP", b"BRPOP"]:
        yield eval_(b"return {redis.call(ARGV[1],'empty',0),redis.call(ARGV[1],'empty',1000)}", args=[name])
        yield [b"RPUSH", b"ready", b"a", b"b"]
        yield eval_(b"return {redis.call(ARGV[1],'ready',0),redis.call(ARGV[1],'ready',0),redis.call(ARGV[1],'ready',0)}", args=[name])
    for text in [b"9007199254740992", b"9223372036854775806", b"9223372036854775807"]:
        yield [b"SET", b"exact", text]
        yield eval_(b"local r=redis.pcall('INCR','exact'); return {r,redis.call('GET','exact')}")
    for source in [
        b"redis.call('SET','kept','value'); return redis.call('LPUSH','kept','bad')",
        b"redis.call('SET','kept','value'); return redis.error_reply('ERR controlled')",
        b"redis.call('SET','kept','value'); error('intentional')",
        b"redis.call('SET','not-parsed','value'); !invalid",
        b"unexpected_global=1", b"return os.execute('false')",
    ]:
        yield eval_(source)
        yield [b"GET", b"kept"]
        yield [b"GET", b"not-parsed"]
    # Scripts are actual queued owner operations, not eagerly evaluated replies.
    yield [b"MULTI"]
    yield eval_(b"redis.call('SET','in-exec','yes'); return {redis.call('GET','in-exec'),false,{ok='OK'}}")
    yield [b"EVAL", b"!invalid", b"-1"]
    yield eval_(b"redis.call('INCR','tx-counter'); return redis.call('LPUSH','tx-counter','bad')")
    yield [b"INCR", b"tx-counter"]
    yield [b"EXEC"]
    yield [b"GET", b"in-exec"]
    yield [b"MULTI"]
    yield [b"SET", b"abort", b"never"]
    yield [b"EVAL", b"return 1"]
    yield [b"EXEC"]
    yield [b"GET", b"abort"]


def normalize(args, reply):
    source = args[1] if args[0].upper() == b"EVAL" and len(args) > 1 else b""

    def visit(reply):
        kind, value = reply
        if kind == b"*" and value is not None:
            return kind, [visit(item) for item in value]
        if kind == b"-":
            # Redis adds source hash/line decoration to uncaught redis.call
            # errors, including those nested in EXEC. Do not normalize category
            # or message text: these are compared exactly after decoration.
            value = re.sub(rb" script: [0-9a-f]{40}, on @user_script:\d+\.$", b"", value)
            for trigger, messages in [
                (b"intentional", [b"intentional"]),
                (b"unexpected_global=", [b"modify a global", b"modify a readonly table"]),
                (b"os.execute", [b"nonexistent global", b"attempt to call field 'execute' (a nil value)"]),
            ]:
                if trigger in source:
                    assert any(message in value for message in messages), value
                    return kind, trigger
            if b"!invalid" in source and len(args) > 2 and args[2] == b"0":
                assert b"compil" in value or b"syntax" in value or b"unexpected symbol" in value, value
                return kind, b"compilation error"
        return kind, value

    return visit(reply)


if __name__ == "__main__":
    compare(corpus, normalize, __doc__)
