//! Synchronous Lua 5.1 language boundary used by owner-side EVAL.
//!
//! The caller supplies command execution; this module owns language/environment
//! isolation and RESP2 conversion, not database semantics or atomicity. Run it on
//! the dedicated state owner, never a Tokio worker, and never supply a callback
//! that waits or performs network I/O. Each invocation uses a fresh VM: even
//! mutations of library tables cannot become cross-request state.

use mlua::chunk::ChunkMode;
use mlua::{Function, Lua, LuaOptions, MultiValue, StdLib, Table, Value, Variadic};

use crate::resp::{Reply, Request};

mod conversion;
mod fpconv;

/// Execute text in an isolated Lua 5.1 environment with byte-preserving KEYS and
/// ARGV. Commands are passed synchronously to `command` as byte arguments.
///
/// Script/command errors become RESP2 errors; there is no rollback. Rust callback
/// panics propagate, even through Lua pcall/xpcall/redis.pcall, so the state owner
/// can fail-stop rather than silently continue after an invariant failure.
///
/// There is no instruction/time/heap quota or preemption. The caller must keep
/// this off async workers and use process-level fail-stop shutdown if it stalls.
/// Reply conversion is bounded to 128 levels and 1,048,576 nodes in each direction.
/// This is a trusted-local runtime boundary, not a hostile-code isolation service.
pub fn execute(
    source: &[u8],
    keys: &[Vec<u8>],
    args: &[Vec<u8>],
    mut command: impl FnMut(Request) -> Reply,
) -> Reply {
    match run(source, keys, args, &mut command) {
        Ok(reply) => reply,
        Err(error) => Reply::Error(format!("ERR Lua script: {error}").into_bytes()),
    }
}

fn run(
    source: &[u8],
    keys: &[Vec<u8>],
    args: &[Vec<u8>],
    command: &mut impl FnMut(Request) -> Reply,
) -> mlua::Result<Reply> {
    // Do not enable ALL_SAFE: that includes host libraries. Lua::sandbox is
    // Luau-only; the Lua 5.1 environment below is our explicit allowlist.
    let lua = Lua::new_with(
        StdLib::TABLE | StdLib::STRING | StdLib::MATH,
        LuaOptions::new().catch_rust_panics(false),
    )?;
    lua.scope(|scope| {
        let invoke = scope.create_function_mut(|lua, values: Variadic<Value>| {
            let reply = match conversion::command_arguments(values) {
                Ok(request) => command(request),
                Err(message) => Reply::Error(message.into()),
            };
            conversion::to_lua(lua, reply).or_else(|error| {
                Ok(Value::Table(conversion::message_table(
                    lua,
                    "err",
                    format!("ERR Lua reply conversion: {error}").as_bytes(),
                )?))
            })
        })?;
        let redis = lua.create_table()?;
        redis.set("pcall", invoke.clone())?;
        // Lua error objects retain their err table for xpcall and the top-level
        // runner. The public pcall wrapper below flattens err to a string, as in
        // Redis 7.4. Captured functions cannot be replaced through script globals.
        let factory: Function = lua
            .load(
                r#"
                local invoke, raise, kind = ...
                return function(...)
                    local reply = invoke(...)
                    if kind(reply) == 'table' and reply.err then raise(reply, 0) end
                    return reply
                end
                "#,
            )
            .into_function()?;
        let call: Function = factory.call((
            invoke,
            lua.globals().get::<Function>("error")?,
            lua.globals().get::<Function>("type")?,
        ))?;
        redis.set("call", call)?;
        for field in ["ok", "err"] {
            let helper = lua.create_function(move |lua, values: Variadic<Value>| {
                let (field, bytes) = match values.as_slice() {
                    [Value::String(s)] if field == "err" => {
                        (field, conversion::error_message(s.as_bytes().as_ref()))
                    }
                    [Value::String(s)] => (field, s.as_bytes().to_vec()),
                    _ => ("err", b"ERR wrong number or type of arguments".to_vec()),
                };
                conversion::message_table(lua, field, &bytes)
            })?;
            redis.set(
                if field == "ok" {
                    "status_reply"
                } else {
                    "error_reply"
                },
                helper,
            )?;
        }
        let env = environment(&lua, redis, keys, args)?;
        let script = lua
            .load(source)
            .set_name("user_script")
            .set_mode(ChunkMode::Text)
            .set_environment(env)
            .into_function()?;
        // Use the original panic-propagating pcall, not Lua's script-visible
        // wrapper. This preserves error tables without exposing Rust userdata.
        let pcall: Function = lua.globals().get("pcall")?;
        let mut result: MultiValue = pcall.call(script)?;
        let success = matches!(result.pop_front(), Some(Value::Boolean(true)));
        let value = result.pop_front().unwrap_or(Value::Nil);
        if success {
            conversion::from_lua(value)
        } else {
            match value {
                Value::Table(table) => {
                    if let Value::String(message) = table.raw_get::<Value>("err")? {
                        return Ok(Reply::Error(conversion::line(message.as_bytes().as_ref())));
                    }
                    Ok(Reply::Error(
                        b"ERR Lua script raised a non-string error".to_vec(),
                    ))
                }
                Value::Error(error) => Ok(Reply::Error(
                    format!("ERR Lua script: {error}").into_bytes(),
                )),
                other => {
                    let Some(message) = lua.coerce_string(other)? else {
                        return Ok(Reply::Error(
                            b"ERR Lua script raised a non-string error".to_vec(),
                        ));
                    };
                    Ok(Reply::Error(
                        [b"ERR Lua script: ".as_slice(), message.as_bytes().as_ref()].concat(),
                    ))
                }
            }
        }
    })
}

fn environment(lua: &Lua, redis: Table, keys: &[Vec<u8>], args: &[Vec<u8>]) -> mlua::Result<Table> {
    let globals = lua.globals();
    let allowed = lua.create_table()?;
    for name in [
        "assert",
        "error",
        "ipairs",
        "next",
        "pairs",
        "select",
        "tonumber",
        "tostring",
        "type",
        "unpack",
        "xpcall",
        "rawequal",
        "rawget",
        "getmetatable",
        "setmetatable",
        "_VERSION",
        "table",
        "string",
        "math",
    ] {
        allowed.set(name, globals.get::<Value>(name)?)?;
    }
    // These are not needed by the audited surface. In particular Lua 5.1's
    // randomseed affects a process-global C PRNG, not just this fresh VM.
    allowed.get::<Table>("math")?.set("random", Value::Nil)?;
    allowed
        .get::<Table>("math")?
        .set("randomseed", Value::Nil)?;
    allowed.get::<Table>("string")?.set("dump", Value::Nil)?;
    // Preserve multiple successful return values (and nil holes) in pcall.
    let pcall_factory = lua
        .load(
            r#"
            local protected, kind = ...
            local function finish(ok, ...)
                if not ok then
                    local err = ...
                    if kind(err) == 'table' and kind(err.err) == 'string' then
                        return false, err.err
                    end
                end
                return ok, ...
            end
            return function(...) return finish(protected(...)) end
            "#,
        )
        .into_function()?;
    allowed.set(
        "pcall",
        pcall_factory.call::<Function>((
            globals.get::<Function>("pcall")?,
            globals.get::<Function>("type")?,
        ))?,
    )?;
    allowed.set("redis", redis)?;
    for (name, values) in [("KEYS", keys), ("ARGV", args)] {
        let table = lua.create_table()?;
        for (index, bytes) in values.iter().enumerate() {
            table.raw_set(index + 1, lua.create_string(bytes)?)?;
        }
        allowed.set(name, table)?;
    }
    // An empty proxy prevents even overwriting existing globals. No rawset,
    // getfenv/setfenv, loader, debug, package, io, os, or host print is exposed.
    let env = lua.create_table()?;
    allowed.set("_G", env.clone())?;
    let metatable = lua.create_table()?;
    metatable.set("__metatable", false)?;
    let index_factory = lua
        .load(
            r#"
            local allowed, raise = ...
            return function(_, name)
                local value = allowed[name]
                if value == nil then
                    raise('Script attempted to access a nonexistent global variable', 2)
                end
                return value
            end
            "#,
        )
        .into_function()?;
    metatable.set(
        "__index",
        index_factory.call::<Function>((allowed, globals.get::<Function>("error")?))?,
    )?;
    let newindex_factory = lua
        .load(
            r#"
            local raise = ...
            return function() raise('Script attempted to modify a global variable', 2) end
            "#,
        )
        .into_function()?;
    metatable.set(
        "__newindex",
        newindex_factory.call::<Function>(globals.get::<Function>("error")?)?,
    )?;
    env.set_metatable(Some(metatable))?;
    Ok(env)
}
