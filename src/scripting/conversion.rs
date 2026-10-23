//! Explicit RESP2/Lua conversions. Never coerce bulk data through a number.

use mlua::{Lua, Table, Value, Variadic};

use crate::resp::{Reply, Request};

const MAX_DEPTH: usize = 128;
const MAX_NODES: usize = 1_048_576;

struct Budget(usize);

impl Budget {
    fn take(&mut self, depth: usize) -> mlua::Result<()> {
        if depth > MAX_DEPTH || self.0 == 0 {
            return Err(mlua::Error::RuntimeError(
                "Lua reply exceeds conversion limit".into(),
            ));
        }
        self.0 -= 1;
        Ok(())
    }
}

pub(super) fn message_table(lua: &Lua, field: &str, bytes: &[u8]) -> mlua::Result<Table> {
    let table = lua.create_table()?;
    table.raw_set(field, lua.create_string(bytes)?)?;
    Ok(table)
}

pub(super) fn to_lua(lua: &Lua, reply: Reply) -> mlua::Result<Value> {
    reply_to_lua(lua, reply, 0, &mut Budget(MAX_NODES))
}

fn reply_to_lua(lua: &Lua, reply: Reply, depth: usize, budget: &mut Budget) -> mlua::Result<Value> {
    budget.take(depth)?;
    Ok(match reply {
        // Lua 5.1 uses doubles even for RESP integers. IDs/counters that require
        // exactness must be obtained as bulk decimal strings, e.g. INCR then GET.
        Reply::Integer(n) => Value::Number(n as f64),
        Reply::Bulk(None) | Reply::Array(None) => Value::Boolean(false),
        Reply::Bulk(Some(bytes)) => Value::String(lua.create_string(bytes)?),
        Reply::Simple(bytes) => Value::Table(message_table(lua, "ok", &bytes)?),
        Reply::Error(bytes) => Value::Table(message_table(lua, "err", &bytes)?),
        Reply::Array(Some(items)) => {
            let table = lua.create_table()?;
            for (index, item) in items.into_iter().enumerate() {
                table.raw_set(index + 1, reply_to_lua(lua, item, depth + 1, budget)?)?;
            }
            Value::Table(table)
        }
    })
}

pub(super) fn from_lua(value: Value) -> mlua::Result<Reply> {
    lua_to_reply(value, 0, &mut Budget(MAX_NODES))
}

fn lua_to_reply(value: Value, depth: usize, budget: &mut Budget) -> mlua::Result<Reply> {
    budget.take(depth)?;
    Ok(match value {
        Value::String(bytes) => Reply::Bulk(Some(bytes.as_bytes().to_vec())),
        Value::Integer(n) => Reply::Integer(n),
        Value::Number(n) => Reply::Integer(number_reply(n)),
        Value::Boolean(true) => Reply::Integer(1),
        Value::Table(table) => {
            // Raw lookup matches Redis: neither __index nor __len may run while
            // capturing a reply. A cycle is rejected by the same depth budget.
            for field in ["err", "ok"] {
                if let Value::String(bytes) = table.raw_get::<Value>(field)? {
                    let bytes = line(bytes.as_bytes().as_ref());
                    return Ok(if field == "err" {
                        Reply::Error(bytes)
                    } else {
                        Reply::Simple(bytes)
                    });
                }
            }
            let mut items = Vec::new();
            for index in 1usize.. {
                let item = table.raw_get::<Value>(index)?;
                if item == Value::Nil {
                    break;
                }
                items.push(lua_to_reply(item, depth + 1, budget)?);
            }
            Reply::Array(Some(items))
        }
        // Nil, false, and non-RESP language values (functions, threads, etc.)
        // become null, not host handles or serialized debug representations.
        _ => Reply::Bulk(None),
    })
}

fn number_reply(n: f64) -> i64 {
    // Avoid Rust's saturating float cast. Redis 7.4/x86_64 returns LLONG_MIN for
    // nonfinite/out-of-range doubles; finite in-range values truncate to zero.
    if (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&n) {
        n as i64
    } else {
        i64::MIN
    }
}

pub(super) fn command_arguments(values: Variadic<Value>) -> Result<Request, &'static [u8]> {
    if values.is_empty() {
        return Err(b"ERR Please specify at least one argument for this redis lib call");
    }
    values
        .into_iter()
        .map(|value| match value {
            Value::String(s) => Ok(s.as_bytes().to_vec()),
            Value::Integer(n) => Ok(number_argument(n as f64).into_bytes()),
            Value::Number(n) => Ok(number_argument(n).into_bytes()),
            _ => Err(b"ERR Lua redis lib command arguments must be strings or integers".as_slice()),
        })
        .collect()
}

fn number_argument(n: f64) -> String {
    // Redis double2ll uses the conservative inclusive +/-2^62 range for its
    // exact-integer fast path, unlike Lua tostring's lossy %.14g representation.
    if n.is_finite()
        && n.fract() == 0.0
        && (-4_611_686_018_427_387_904.0..=4_611_686_018_427_387_904.0).contains(&n)
    {
        return (n as i64).to_string();
    }
    if n.is_nan() {
        return if n.is_sign_negative() { "-nan" } else { "nan" }.into();
    }
    if n == f64::INFINITY {
        return "inf".into();
    }
    if n == f64::NEG_INFINITY {
        return "-inf".into();
    }
    // Match fpconv rounding and presentation, not just roundtrip equivalence.
    // Decimal-string arguments never enter this path.
    let (digits, shift) = super::fpconv::digits(n.abs());
    let length = digits.len() as i32;
    let exponent = shift + length - 1;
    let sign = if n.is_sign_negative() { "-" } else { "" };
    if shift >= 0 && exponent.abs() < length + 7 {
        format!("{sign}{digits}{}", "0".repeat(shift as usize))
    } else if shift < 0 && (shift > -7 || exponent.abs() < 4) {
        let offset = length + shift;
        if offset <= 0 {
            format!("{sign}0.{}{digits}", "0".repeat((-offset) as usize))
        } else {
            let (left, right) = digits.split_at(offset as usize);
            format!("{sign}{left}.{right}")
        }
    } else if digits.len() == 1 {
        format!("{sign}{digits}e{exponent:+}")
    } else {
        format!("{sign}{}.{}e{exponent:+}", &digits[..1], &digits[1..])
    }
}

/// Status/error replies are C strings in Redis; binary data belongs in bulks.
/// Normalize while capturing replies, before the independent wire encoder.
pub(super) fn line(bytes: &[u8]) -> Vec<u8> {
    bytes
        .iter()
        .take_while(|&&b| b != 0)
        .map(|&b| if b == b'\r' || b == b'\n' { b' ' } else { b })
        .collect()
}

pub(super) fn error_message(bytes: &[u8]) -> Vec<u8> {
    // error_reply accepts a leading '-' but stores only CODE message in .err.
    let bytes = bytes.split(|&b| b == 0).next().unwrap_or_default();
    let bytes = bytes.strip_prefix(b"-").unwrap_or(bytes);
    let (code, message) = match bytes.iter().position(|&b| b == b' ') {
        Some(space) => (&bytes[..space], &bytes[space + 1..]),
        None => (b"ERR".as_slice(), bytes),
    };
    let start = message
        .iter()
        .position(|b| !matches!(b, b'\r' | b'\n'))
        .unwrap_or(message.len());
    let message = &message[start..];
    let end = message
        .iter()
        .rposition(|b| !matches!(b, b'\r' | b'\n'))
        .map_or(0, |i| i + 1);
    [code, b" ", &message[..end]].concat()
}

#[cfg(test)]
mod tests {
    use super::number_argument;

    #[test]
    fn redis_numeric_argument_rounding_and_format_boundaries() {
        for (n, expected) in [
            (1.214_500_052_527_671_3e15, "1214500052527671.2"),
            (1.234_567_890_123_456_7, "1.2345678901234567"),
            (-9_223_372_036_854_775_808.0, "-9223372036854776000"),
            (9_223_372_036_854_775_808.0, "9223372036854776000"),
            (f64::from_bits(1), "5e-324"),
            (f64::MAX, "1.7976931348623157e+308"),
            (1e-6, "0.000001"),
            (1e-7, "1e-7"),
            (1e20, "1e+20"),
            (-0.0, "0"),
        ] {
            assert_eq!(number_argument(n), expected, "{n:e}");
        }
    }

    #[test]
    fn numeric_argument_roundtrips_all_binary_exponents_and_seeded_mantissas() {
        for exponent in 0..2047u64 {
            for fraction in [0, 1, (1u64 << 51) - 1, (1u64 << 52) - 1] {
                let n = f64::from_bits((exponent << 52) | fraction);
                for n in [n, -n] {
                    let parsed: f64 = number_argument(n).parse().unwrap();
                    assert_eq!(parsed, n, "{n:e}"); // signed zero is intentionally normalized
                }
            }
        }
        let mut bits = 0x3141_5926_5358_9793u64;
        for _ in 0..100_000 {
            bits ^= bits << 13;
            bits ^= bits >> 7;
            bits ^= bits << 17;
            let n = f64::from_bits(bits);
            if n.is_finite() {
                assert_eq!(number_argument(n).parse::<f64>().unwrap(), n, "{n:e}");
            }
        }
    }
}
