//! Command admission and connection-local selection, separate from wire/state I/O.

use crate::resp::{Reply, Request};
use crate::state::DatabaseIndex;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ListEnd {
    Left,
    Right,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum DatabaseCommand {
    Get(Vec<u8>),
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Exists(Vec<Vec<u8>>),
    Increment(Vec<u8>),
    SetAdd {
        key: Vec<u8>,
        members: Vec<Vec<u8>>,
    },
    SetRemove {
        key: Vec<u8>,
        members: Vec<Vec<u8>>,
    },
    SetContains {
        key: Vec<u8>,
        member: Vec<u8>,
    },
    SetCardinality(Vec<u8>),
    SetMembers(Vec<u8>),
    Push {
        end: ListEnd,
        key: Vec<u8>,
        values: Vec<Vec<u8>>,
    },
    Pop {
        end: ListEnd,
        keys: Vec<Vec<u8>>,
        timeout: Vec<u8>,
    },
}

#[derive(Debug, PartialEq, Eq)]
enum Command {
    Ping(Option<Vec<u8>>),
    Select(Vec<u8>),
    Database(DatabaseCommand),
}

pub(crate) enum Action {
    Reply(Reply),
    Database(DatabaseIndex, DatabaseCommand),
}

pub(crate) struct Session {
    database: DatabaseIndex,
    first_command: bool,
}

impl Default for Session {
    fn default() -> Self {
        Self {
            database: DatabaseIndex::default(),
            first_command: true,
        }
    }
}

impl Session {
    pub(crate) fn prepare(&mut self, request: Request) -> Action {
        // Even an invalid/unknown/empty command consumes the selection window.
        // Framing only calls us once the entire request has been decoded.
        let first = std::mem::replace(&mut self.first_command, false);
        let reply = match parse(request) {
            Ok(Command::Select(index)) if first => match parse_database(&index) {
                Ok(database) => {
                    self.database = database;
                    Reply::Simple(b"OK".to_vec())
                }
                Err(error) => error,
            },
            Ok(Command::Select(_)) => {
                Reply::Error(b"ERR SELECT is only allowed as the first command".to_vec())
            }
            Ok(Command::Ping(None)) => Reply::Simple(b"PONG".to_vec()),
            Ok(Command::Ping(Some(message))) => Reply::Bulk(Some(message)),
            Ok(Command::Database(command)) => return Action::Database(self.database, command),
            Err(error) => error,
        };
        Action::Reply(reply)
    }
}

fn wrong_arity(command: &str) -> Reply {
    Reply::Error(format!("ERR wrong number of arguments for '{command}' command").into_bytes())
}

// Validate arity before taking arguments. Value conversion stays separate so
// later transaction admission need not confuse execution-time errors with arity.
fn parse(request: Request) -> Result<Command, Reply> {
    let mut args = request.into_iter();
    let Some(name) = args.next() else {
        return Err(Reply::Error(b"ERR empty command".to_vec()));
    };
    if name.eq_ignore_ascii_case(b"PING") {
        if args.len() > 1 {
            return Err(wrong_arity("ping"));
        }
        Ok(Command::Ping(args.next()))
    } else if name.eq_ignore_ascii_case(b"SELECT") {
        if args.len() != 1 {
            return Err(wrong_arity("select"));
        }
        Ok(Command::Select(args.next().unwrap()))
    } else if name.eq_ignore_ascii_case(b"GET") {
        if args.len() != 1 {
            return Err(wrong_arity("get"));
        }
        Ok(Command::Database(DatabaseCommand::Get(
            args.next().unwrap(),
        )))
    } else if name.eq_ignore_ascii_case(b"SET") {
        if args.len() != 2 {
            return Err(wrong_arity("set"));
        }
        Ok(Command::Database(DatabaseCommand::Set {
            key: args.next().unwrap(),
            value: args.next().unwrap(),
        }))
    } else if name.eq_ignore_ascii_case(b"EXISTS") {
        if args.len() == 0 {
            return Err(wrong_arity("exists"));
        }
        Ok(Command::Database(DatabaseCommand::Exists(args.collect())))
    } else if name.eq_ignore_ascii_case(b"INCR") {
        if args.len() != 1 {
            return Err(wrong_arity("incr"));
        }
        Ok(Command::Database(DatabaseCommand::Increment(
            args.next().unwrap(),
        )))
    } else if name.eq_ignore_ascii_case(b"SADD") || name.eq_ignore_ascii_case(b"SREM") {
        let add = name.eq_ignore_ascii_case(b"SADD");
        if args.len() < 2 {
            return Err(wrong_arity(if add { "sadd" } else { "srem" }));
        }
        let key = args.next().unwrap();
        let members = args.collect();
        Ok(Command::Database(if add {
            DatabaseCommand::SetAdd { key, members }
        } else {
            DatabaseCommand::SetRemove { key, members }
        }))
    } else if name.eq_ignore_ascii_case(b"SISMEMBER") {
        if args.len() != 2 {
            return Err(wrong_arity("sismember"));
        }
        Ok(Command::Database(DatabaseCommand::SetContains {
            key: args.next().unwrap(),
            member: args.next().unwrap(),
        }))
    } else if name.eq_ignore_ascii_case(b"SCARD") || name.eq_ignore_ascii_case(b"SMEMBERS") {
        let cardinality = name.eq_ignore_ascii_case(b"SCARD");
        if args.len() != 1 {
            return Err(wrong_arity(if cardinality { "scard" } else { "smembers" }));
        }
        let key = args.next().unwrap();
        Ok(Command::Database(if cardinality {
            DatabaseCommand::SetCardinality(key)
        } else {
            DatabaseCommand::SetMembers(key)
        }))
    } else if name.eq_ignore_ascii_case(b"LPUSH") || name.eq_ignore_ascii_case(b"RPUSH") {
        let (end, command) = if name.eq_ignore_ascii_case(b"LPUSH") {
            (ListEnd::Left, "lpush")
        } else {
            (ListEnd::Right, "rpush")
        };
        if args.len() < 2 {
            return Err(wrong_arity(command));
        }
        Ok(Command::Database(DatabaseCommand::Push {
            end,
            key: args.next().unwrap(),
            values: args.collect(),
        }))
    } else if name.eq_ignore_ascii_case(b"BLPOP") || name.eq_ignore_ascii_case(b"BRPOP") {
        let (end, command) = if name.eq_ignore_ascii_case(b"BLPOP") {
            (ListEnd::Left, "blpop")
        } else {
            (ListEnd::Right, "brpop")
        };
        if args.len() < 2 {
            return Err(wrong_arity(command));
        }
        if args.len() > 6 {
            return Err(Reply::Error(
                b"ERR blocking pop supports at most 5 keys".to_vec(),
            ));
        }
        let timeout = args.next_back().unwrap();
        Ok(Command::Database(DatabaseCommand::Pop {
            end,
            keys: args.collect(),
            timeout,
        }))
    } else {
        // Never reflect arbitrary (possibly binary or sensitive) input.
        Err(Reply::Error(b"ERR unknown command".to_vec()))
    }
}

/// Redis signed integers are canonical decimal, unlike Rust's looser parser.
/// Bound the scan before UTF-8/number conversion, even for huge stored strings.
pub(crate) fn parse_integer(bytes: &[u8]) -> Option<i64> {
    if bytes.len() > 20 {
        return None;
    }
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|text| text.parse::<i64>().ok())
        .filter(|value| value.to_string().as_bytes() == bytes)
}

fn parse_database(bytes: &[u8]) -> Result<DatabaseIndex, Reply> {
    let index = parse_integer(bytes)
        .ok_or_else(|| Reply::Error(b"ERR value is not an integer or out of range".to_vec()))?;
    let index = i32::try_from(index).map_err(|_| {
        Reply::Error(
            b"ERR value is out of range, value must between -2147483648 and 2147483647".to_vec(),
        )
    })?;
    DatabaseIndex::new(index).ok_or_else(|| Reply::Error(b"ERR DB index is out of range".to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(args: &[&[u8]]) -> Request {
        args.iter().map(|arg| arg.to_vec()).collect()
    }

    #[test]
    fn ping_is_ascii_case_insensitive_and_echoes_binary_or_empty_messages() {
        for name in [b"PING", b"ping", b"pInG"] {
            assert_eq!(parse(request(&[name])), Ok(Command::Ping(None)));
            for message in [&b""[..], b"\0\xff\r\n"] {
                assert_eq!(
                    parse(request(&[name, message])),
                    Ok(Command::Ping(Some(message.to_vec())))
                );
            }
        }
    }

    #[test]
    fn admission_rejects_wrong_arity_and_set_options() {
        for args in [
            vec![],
            vec![&b"UNKNOWN\r\n"[..]],
            vec![b""],
            vec![b"PING", b"", b""],
            vec![b"SELECT"],
            vec![b"SELECT", b"0", b"1"],
            vec![b"GET"],
            vec![b"GET", b"a", b"b"],
            vec![b"SET", b"a"],
            vec![b"SET", b"a", b"v", b"NX"],
            vec![b"EXISTS"],
            vec![b"LPUSH"],
            vec![b"LPUSH", b"key"],
            vec![b"RPUSH"],
            vec![b"RPUSH", b"key"],
        ] {
            assert!(matches!(parse(request(&args)), Err(Reply::Error(_))));
        }
    }

    #[test]
    fn numeric_set_admission_preserves_binary_arguments_and_checks_arity() {
        let key = b"\0\xff".to_vec();
        let members = vec![vec![], b"a\r\n".to_vec(), vec![]];
        for (args, command) in [
            (
                vec![&b"iNcR"[..], &key],
                DatabaseCommand::Increment(key.clone()),
            ),
            (
                vec![b"sAdD", &key, b"", b"a\r\n", b""],
                DatabaseCommand::SetAdd {
                    key: key.clone(),
                    members: members.clone(),
                },
            ),
            (
                vec![b"sReM", &key, b"", b"a\r\n", b""],
                DatabaseCommand::SetRemove {
                    key: key.clone(),
                    members,
                },
            ),
            (
                vec![b"sIsMeMbEr", &key, b""],
                DatabaseCommand::SetContains {
                    key: key.clone(),
                    member: vec![],
                },
            ),
            (
                vec![b"sCaRd", &key],
                DatabaseCommand::SetCardinality(key.clone()),
            ),
            (
                vec![b"sMeMbErS", &key],
                DatabaseCommand::SetMembers(key.clone()),
            ),
        ] {
            assert_eq!(parse(request(&args)), Ok(Command::Database(command)));
        }
        for (name, required, variadic) in [
            (&b"INCR"[..], 1, false),
            (b"SADD", 2, true),
            (b"SREM", 2, true),
            (b"SISMEMBER", 2, false),
            (b"SCARD", 1, false),
            (b"SMEMBERS", 1, false),
        ] {
            for count in 0..=required + 1 {
                let mut args = vec![name];
                args.extend(vec![&b""[..]; count]);
                assert_eq!(
                    parse(request(&args)).is_ok(),
                    count == required || (variadic && count > required)
                );
            }
        }
        for value in [i64::MIN, -1, 0, 1, 9_007_199_254_740_993, i64::MAX] {
            assert_eq!(parse_integer(value.to_string().as_bytes()), Some(value));
        }
        for bytes in [
            &b""[..],
            b"+1",
            b"01",
            b"-0",
            b"-01",
            b" 1",
            b"1 ",
            b"1.0",
            b"1e0",
            b"1\0",
            b"\xff",
            b"9223372036854775808",
            b"-9223372036854775809",
        ] {
            assert_eq!(parse_integer(bytes), None);
        }
        assert_eq!(parse_integer(&vec![b'1'; 8192]), None);
    }

    #[test]
    fn push_admission_preserves_end_key_and_all_binary_values() {
        for (name, end) in [(b"lPuSh", ListEnd::Left), (b"rPuSh", ListEnd::Right)] {
            assert_eq!(
                parse(request(&[name, b"\0\xff", b"", b"a\r\n", b"a\r\n"])),
                Ok(Command::Database(DatabaseCommand::Push {
                    end,
                    key: b"\0\xff".to_vec(),
                    values: vec![vec![], b"a\r\n".to_vec(), b"a\r\n".to_vec()],
                }))
            );
        }
    }

    #[test]
    fn pop_admission_preserves_keys_and_defers_timeout_conversion() {
        for (name, end) in [(b"bLpOp", ListEnd::Left), (b"bRpOp", ListEnd::Right)] {
            for count in 1..=5 {
                let mut args = vec![&name[..]];
                args.extend(vec![&b"\0\xff"[..]; count]);
                args.push(b"not-a-float");
                assert_eq!(
                    parse(request(&args)),
                    Ok(Command::Database(DatabaseCommand::Pop {
                        end,
                        keys: vec![b"\0\xff".to_vec(); count],
                        timeout: b"not-a-float".to_vec(),
                    }))
                );
            }
            for args in [
                vec![&name[..]],
                vec![name, b"0"],
                vec![name, b"a", b"b", b"c", b"d", b"e", b"f", b"0"],
            ] {
                assert!(matches!(parse(request(&args)), Err(Reply::Error(_))));
            }
            let mut session = Session::default();
            assert!(matches!(
                session.prepare(request(&[name, b"k", b"0"])),
                Action::Database(_, DatabaseCommand::Pop { .. })
            ));
            assert!(matches!(
                session.prepare(request(&[b"SELECT", b"15"])),
                Action::Reply(Reply::Error(_))
            ));
        }
    }

    #[test]
    fn select_accepts_only_canonical_in_range_indices() {
        for index in 0..16 {
            assert_eq!(
                parse_database(index.to_string().as_bytes()),
                Ok(DatabaseIndex::new(index).unwrap())
            );
        }
        for index in [
            &b""[..],
            b"-1",
            b"16",
            b"00",
            b"01",
            b"-0",
            b"+0",
            b" 1",
            b"1 ",
            b"1.0",
            b"1e0",
            b"1\0",
            b"\xff",
            b"2147483648",
            b"-2147483649",
            b"9223372036854775808",
        ] {
            assert!(matches!(parse_database(index), Err(Reply::Error(_))));
        }
    }

    #[test]
    fn every_complete_attempt_consumes_selection_and_invalid_selection_keeps_default() {
        for first in [
            vec![&b"PING"[..]],
            vec![b"PING", b"a", b"b"],
            vec![b"UNKNOWN"],
            vec![],
            vec![b"SELECT", b"16"],
            vec![b"SELECT"],
            vec![b"GET"],
        ] {
            let mut session = Session::default();
            session.prepare(request(&first));
            assert!(matches!(
                session.prepare(request(&[b"SELECT", b"15"])),
                Action::Reply(Reply::Error(_))
            ));
            assert!(matches!(
                session.prepare(request(&[b"GET", b"key"])),
                Action::Database(db, _) if db == DatabaseIndex::default()
            ));
        }
        let mut session = Session::default();
        assert!(matches!(
            session.prepare(request(&[b"sElEcT", b"15"])),
            Action::Reply(Reply::Simple(_))
        ));
        session.prepare(request(&[b"SELECT", b"0"]));
        assert!(matches!(
            session.prepare(request(&[b"GET", b"key"])),
            Action::Database(db, _) if db == DatabaseIndex::new(15).unwrap()
        ));
    }
}
