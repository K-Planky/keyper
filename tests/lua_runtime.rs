//! Runtime-only checks: callbacks below are explicit test doubles, not a Redis
//! database or EVAL substitute. TCP/owner integration is deliberately separate.

use keyper::{resp::Reply, scripting::execute};

fn bulk(bytes: impl AsRef<[u8]>) -> Reply {
    Reply::Bulk(Some(bytes.as_ref().to_vec()))
}

fn array(items: Vec<Reply>) -> Reply {
    Reply::Array(Some(items))
}

fn run(source: &str) -> Reply {
    execute(source.as_bytes(), &[], &[], |_| {
        panic!("unexpected command")
    })
}

fn assert_error(reply: Reply, contains: &str) {
    let Reply::Error(bytes) = reply else {
        panic!("expected error, got {reply:?}")
    };
    assert!(
        String::from_utf8_lossy(&bytes).contains(contains),
        "{bytes:?}"
    );
}

#[test]
fn genuine_lua51_closures_loops_patterns_tables_and_varargs() {
    assert_eq!(
        run(r#"
        local function make(n)
            return function(...) local t = {...}; local sum = n
                for _, x in ipairs(t) do sum = sum + x end; return sum end
        end
        local values = {3, 4, 5}; table.insert(values, 6)
        local text = string.format('%s:%d', string.sub('abcdef', 2, 4), make(2)(unpack(values, 2, 3)))
        local _, _, digits = string.find('tag-123-end', '%-(%d+)%-')
        return {_VERSION, text, string.match('v42', '%d+'), digits,
                table.concat(values, ','), math.floor(7/2), 3/2, #values}
    "#),
        array(vec![
            bulk("Lua 5.1"),
            bulk("bcd:11"),
            bulk("42"),
            bulk("123"),
            bulk("3,4,5,6"),
            Reply::Integer(3),
            Reply::Integer(1),
            Reply::Integer(4)
        ])
    );
    assert_error(run("local x = 1 // 2; return x"), "Lua script");
}

#[test]
fn keys_and_args_are_binary_distinct_indexed_and_fresh_each_time() {
    let keys = vec![b"\0key\xff".to_vec(), vec![]];
    let args = vec![b"\r\n\0\xff".to_vec(), b"18446744073709551615".to_vec()];
    for _ in 0..3 {
        assert_eq!(
            execute(
                b"return {KEYS, ARGV, #KEYS, #ARGV}",
                &keys,
                &args,
                |_| unreachable!()
            ),
            array(vec![
                array(vec![bulk(&keys[0]), bulk("")]),
                array(vec![bulk(&args[0]), bulk(&args[1])]),
                Reply::Integer(2),
                Reply::Integer(2)
            ])
        );
        assert_eq!(
            execute(
                b"KEYS[1]='changed'; ARGV[9]='changed'; return true",
                &keys,
                &args,
                |_| unreachable!()
            ),
            Reply::Integer(1)
        );
        assert_eq!(
            run("return {#KEYS, #ARGV, KEYS[1], ARGV[1]}"),
            array(vec![Reply::Integer(0), Reply::Integer(0)])
        );
    }
}

#[test]
fn resp2_language_results_and_raw_table_conversion() {
    for (source, expected) in [
        ("return nil", Reply::Bulk(None)),
        ("return false", Reply::Bulk(None)),
        ("return true", Reply::Integer(1)),
        ("return ''", bulk("")),
        ("return -12.99", Reply::Integer(-12)),
        ("return function() end", Reply::Bulk(None)),
        ("return {}", array(vec![])),
        ("return {a='ignored'}", array(vec![])),
        ("return 1,2,3", Reply::Integer(1)),
        (
            "return {1, false, {2, nil, 3}, [5]=5}",
            array(vec![
                Reply::Integer(1),
                Reply::Bulk(None),
                array(vec![Reply::Integer(2)]),
            ]),
        ),
        (
            "return setmetatable({}, {__index=function() error('must not run') end})",
            array(vec![]),
        ),
        (
            "return {ok=3, err=false, 7}",
            array(vec![Reply::Integer(7)]),
        ),
        (
            "return {ok='OK', err='ERR preferred', 3}",
            Reply::Error(b"ERR preferred".to_vec()),
        ),
        (
            "return 'a' .. string.char(0,255,13,10)",
            bulk(b"a\0\xff\r\n"),
        ),
    ] {
        assert_eq!(run(source), expected, "{source}");
    }
}

#[test]
fn status_error_helpers_and_line_injection_boundaries() {
    for (source, expected) in [
        (
            "return redis.status_reply('READY')",
            Reply::Simple(b"READY".to_vec()),
        ),
        (
            "return redis.error_reply('ERR refused')",
            Reply::Error(b"ERR refused".to_vec()),
        ),
        (
            "return redis.error_reply('-BUSY later')",
            Reply::Error(b"BUSY later".to_vec()),
        ),
        (
            "return redis.error_reply('refused')",
            Reply::Error(b"ERR refused".to_vec()),
        ),
        (
            "return {ok='A\\r\\nB\\000ignored'}",
            Reply::Simple(b"A  B".to_vec()),
        ),
        (
            "return {err='ERR A\\r\\nB\\000ignored'}",
            Reply::Error(b"ERR A  B".to_vec()),
        ),
        (
            "return redis.status_reply(1)",
            Reply::Error(b"ERR wrong number or type of arguments".to_vec()),
        ),
        (
            "return redis.error_reply()",
            Reply::Error(b"ERR wrong number or type of arguments".to_vec()),
        ),
        (
            "return redis.status_reply('one', 'two')",
            Reply::Error(b"ERR wrong number or type of arguments".to_vec()),
        ),
    ] {
        assert_eq!(run(source), expected, "{source}");
    }
    assert_eq!(
        run("return {redis.status_reply('TYPE').ok, redis.error_reply('ERR no').err}"),
        array(vec![bulk("TYPE"), bulk("ERR no")])
    );
}

#[test]
fn callback_replies_convert_null_status_integers_nested_errors_and_binary() {
    let reply = execute(
        br#"
        local r = redis.call('fixture')
        assert(type(r[1]) == 'number')
        assert(r[2] == false and r[3] == false)
        assert(r[4].ok == 'hash' and r[7][1].err == 'WRONGTYPE nested')
        return r
    "#,
        &[],
        &[],
        |args| {
            assert_eq!(args, vec![b"fixture".to_vec()]);
            array(vec![
                Reply::Integer(42),
                Reply::Bulk(None),
                Reply::Array(None),
                Reply::Simple(b"hash".to_vec()),
                bulk(b"\0\xff\r\n"),
                array(vec![]),
                array(vec![Reply::Error(b"WRONGTYPE nested".to_vec())]),
            ])
        },
    );
    assert_eq!(
        reply,
        array(vec![
            Reply::Integer(42),
            Reply::Bulk(None),
            Reply::Bulk(None),
            Reply::Simple(b"hash".to_vec()),
            bulk(b"\0\xff\r\n"),
            array(vec![]),
            array(vec![Reply::Error(b"WRONGTYPE nested".to_vec())])
        ])
    );
}

#[test]
fn call_raises_pcall_returns_error_and_language_pcall_keeps_all_results() {
    let mut calls = 0;
    let result = execute(
        br#"
        local a = redis.pcall('bad')
        local ok, message = pcall(function() redis.call('bad') end)
        local xok, xerr = xpcall(function() redis.call('bad') end, function(e) return e.err end)
        local function multi() return 1,nil,3 end
        local function count(...) return select('#',...) end
        return {a.err, ok, message, xok, xerr, count(pcall(multi)), {pcall(multi)}}
    "#,
        &[],
        &[],
        |_| {
            calls += 1;
            Reply::Error(b"WRONGTYPE fixture".to_vec())
        },
    );
    assert_eq!(calls, 3);
    assert_eq!(
        result,
        array(vec![
            bulk("WRONGTYPE fixture"),
            Reply::Bulk(None),
            bulk("WRONGTYPE fixture"),
            Reply::Bulk(None),
            bulk("WRONGTYPE fixture"),
            Reply::Integer(4),
            array(vec![Reply::Integer(1), Reply::Integer(1)])
        ])
    );
    assert_eq!(
        execute(b"redis.call('bad'); return 'unreachable'", &[], &[], |_| {
            Reply::Error(b"ERR stopped".to_vec())
        }),
        Reply::Error(b"ERR stopped".to_vec())
    );
}

#[test]
fn bad_call_arguments_are_catchable_and_do_not_reach_callback() {
    for call in [
        "redis.pcall()",
        "redis.pcall('x', true)",
        "redis.pcall('x', {})",
        "redis.pcall('x', nil)",
        "redis.pcall(function() end)",
    ] {
        assert_error(run(&format!("return {call}")), "ERR");
    }
    assert_eq!(
        run("return {pcall(function() redis.call('x',false) end)}"),
        array(vec![
            Reply::Bulk(None),
            bulk("ERR Lua redis lib command arguments must be strings or integers")
        ])
    );
}

#[test]
fn byte_arguments_exact_integer_formatting_and_numeric_reply_precision() {
    let mut observed = Vec::new();
    assert_eq!(execute(br#"
        redis.call('capture', KEYS[1], ARGV[1], 9007199254740992, 1e9, -0.0, 1.25, 1e-7, 1e20, 0/0, math.huge)
        return true
    "#, &[b"\0\xff".to_vec()], &[b"18446744073709551615".to_vec()], |args| { observed = args; Reply::Simple(b"OK".to_vec()) }), Reply::Integer(1));
    assert_eq!(
        observed,
        [
            b"capture".as_slice(),
            b"\0\xff",
            b"18446744073709551615",
            b"9007199254740992",
            b"1000000000",
            b"0",
            b"1.25",
            b"1e-7",
            b"1e+20",
            b"-nan",
            b"inf"
        ]
        .map(Vec::from)
    );
    assert_eq!(
        execute(
            b"return {redis.call('number'), redis.call('text')}",
            &[],
            &[],
            |args| if args[0] == b"number" {
                Reply::Integer(9_007_199_254_740_993)
            } else {
                bulk("9007199254740993")
            }
        ),
        array(vec![
            Reply::Integer(9_007_199_254_740_992),
            bulk("9007199254740993")
        ])
    );
    for source in [
        "return 0/0",
        "return math.huge",
        "return -math.huge",
        "return 9223372036854775808",
        "return -9223372036854775808",
    ] {
        assert_eq!(run(source), Reply::Integer(i64::MIN));
    }
}

#[test]
fn decimal_string_arithmetic_uses_only_individual_digits_as_numbers() {
    // Independent general snippet, not a copied crawler script or native shortcut.
    let source = br#"
        local text = ARGV[1]; local carry = 1; local out = {}
        for i = #text, 1, -1 do
            local d = tonumber(string.sub(text,i,i)) + carry
            carry = math.floor(d/10); table.insert(out, 1, tostring(d%10))
        end
        if carry > 0 then table.insert(out, 1, tostring(carry)) end
        return table.concat(out)
    "#;
    for (input, output) in [
        ("9007199254740992", "9007199254740993"),
        ("18446744073709551614", "18446744073709551615"),
        ("999999999999999999999999", "1000000000000000000000000"),
    ] {
        assert_eq!(
            execute(
                source,
                &[],
                &[input.as_bytes().to_vec()],
                |_| unreachable!()
            ),
            bulk(output)
        );
    }
}

#[test]
fn host_access_loaders_environment_escape_and_global_writes_are_denied() {
    for name in [
        "os",
        "io",
        "debug",
        "package",
        "require",
        "module",
        "load",
        "loadstring",
        "loadfile",
        "dofile",
        "getfenv",
        "setfenv",
        "rawset",
        "newproxy",
        "collectgarbage",
        "print",
        "jit",
        "ffi",
    ] {
        assert_error(run(&format!("return {name}")), "nonexistent global");
    }
    for source in [
        "leaked = 42",
        "function leaked() end",
        "ARGV = {}",
        "_G.KEYS = {}",
        "string = {}",
        "_G = {}",
    ] {
        assert_error(run(source), "modify a global");
    }
    assert_error(
        run("setmetatable(_G, {}); return true"),
        "protected metatable",
    );
    assert_eq!(
        run(
            "return {getmetatable(_G), type(string.dump), type(math.randomseed), type(math.random)}"
        ),
        array(vec![
            Reply::Bulk(None),
            bulk("nil"),
            bulk("nil"),
            bulk("nil")
        ])
    );
}

#[test]
fn library_table_and_error_path_mutations_never_survive_an_invocation() {
    assert_eq!(
        run(
            "math.floor = function() return 999 end; string.sub = nil; redis.call=nil; table.insert(_G, 'local only'); return math.floor(1.5)"
        ),
        Reply::Integer(999)
    );
    assert_error(
        run("math.floor=nil; error('ordinary failure')"),
        "ordinary failure",
    );
    assert_eq!(
        run("return {math.floor(1.5), string.sub('abc',2), type(redis.call), rawget(_G,1)}"),
        array(vec![Reply::Integer(1), bulk("bc"), bulk("function")])
    );
    assert_error(run("return leaked"), "nonexistent global");
}

#[test]
fn compilation_and_runtime_errors_do_not_rollback_callback_effects() {
    let mut seen = Vec::new();
    assert_error(
        execute(b"redis.call('write'); local =", &[], &[], |args| {
            seen.push(args);
            Reply::Integer(1)
        }),
        "Lua script",
    );
    assert!(seen.is_empty());
    assert_error(
        execute(
            b"redis.call('write'); error('after write')",
            &[],
            &[],
            |args| {
                seen.push(args);
                Reply::Integer(1)
            },
        ),
        "after write",
    );
    assert_eq!(seen, vec![vec![b"write".to_vec()]]);
    assert_error(
        execute(b"\x1bLua\x51\x00", &[], &[], |_| unreachable!()),
        "binary",
    );
    assert_error(
        execute(b"return 1\0return 2", &[], &[], |_| unreachable!()),
        "Lua script",
    );
    for source in ["error({})", "error(false)", "error(nil)"] {
        assert_error(run(source), "non-string error");
    }
    assert_eq!(run("return 'still works'"), bulk("still works"));
}

#[test]
fn cyclic_deep_and_oversized_replies_are_bounded_without_metamethods() {
    for source in [
        "local a={}; a[1]=a; return a",
        "local a=1; for i=1,129 do a={a} end; return a",
        "local a={}; for i=1,1048576 do a[i]=false end; return a",
    ] {
        assert_error(run(source), "conversion limit");
    }
    let mut expected = Reply::Integer(1);
    for _ in 0..128 {
        expected = array(vec![expected]);
    }
    assert_eq!(
        run("local a=1; for i=1,128 do a={a} end; return a"),
        expected
    );
    let mut value = Reply::Integer(1);
    for _ in 0..130 {
        value = array(vec![value]);
    }
    assert_error(
        execute(b"return redis.call('deep')", &[], &[], |_| {
            std::mem::replace(&mut value, Reply::Bulk(None))
        }),
        "conversion limit",
    );
}

#[test]
fn rust_panics_cannot_be_swallowed_by_any_protected_lua_call() {
    for source in [
        "redis.call('panic')",
        "redis.pcall('panic')",
        "pcall(function() redis.call('panic') end); return 'swallowed'",
        "pcall(function() redis.pcall('panic') end); return 'swallowed'",
        "xpcall(function() redis.call('panic') end, function() return 'swallowed' end); return 'swallowed'",
        "pcall(function() xpcall(function() redis.pcall('panic') end, function() end) end); return 'swallowed'",
    ] {
        let outcome = std::panic::catch_unwind(|| {
            execute(source.as_bytes(), &[], &[], |_| panic!("owner invariant"))
        });
        assert!(outcome.is_err(), "panic swallowed by {source}");
    }
}

#[test]
fn independent_parallel_invocations_have_no_vm_or_argument_leakage() {
    let threads: Vec<_> = (0..8)
        .map(|id| {
            std::thread::spawn(move || {
                for _ in 0..20 {
                    let bytes = id.to_string().into_bytes();
                    assert_eq!(
                        execute(
                            b"return {ARGV[1], redis.call('id'), #KEYS}",
                            &[],
                            std::slice::from_ref(&bytes),
                            |_| bulk(&bytes)
                        ),
                        array(vec![bulk(&bytes), bulk(&bytes), Reply::Integer(0)])
                    );
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
}
