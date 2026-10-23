//! Command admission, connection-local selection and transaction queues.

use crate::resp::{Reply, Request};
use crate::state::DatabaseIndex;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ListEnd {
    Left,
    Right,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum DatabaseCommand {
    Eval {
        source: Vec<u8>,
        numkeys: Vec<u8>,
        args: Vec<Vec<u8>>,
    },
    Get(Vec<u8>),
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Exists(Vec<Vec<u8>>),
    Type(Vec<u8>),
    ListLength(Vec<u8>),
    ListIndex {
        key: Vec<u8>,
        index: Vec<u8>,
    },
    ListPop {
        key: Vec<u8>,
        count: Option<Vec<u8>>,
    },
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
    HashSet {
        key: Vec<u8>,
        pairs: Vec<(Vec<u8>, Vec<u8>)>,
    },
    HashGet {
        key: Vec<u8>,
        field: Vec<u8>,
    },
    HashDelete {
        key: Vec<u8>,
        fields: Vec<Vec<u8>>,
    },
    HashContains {
        key: Vec<u8>,
        field: Vec<u8>,
    },
    HashLength(Vec<u8>),
    HashGetAll(Vec<u8>),
    HashValues(Vec<u8>),
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
    Multi,
    Exec,
    Discard,
    RuntimeError(Reply),
    Database(DatabaseCommand),
}

/// Pure replies (PING, rejected late SELECT, form errors) still occupy a slot
/// in EXEC. No command in this queue has executed against the database yet.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum QueuedCommand {
    Reply(Reply),
    Database(DatabaseCommand),
}

pub(crate) enum Action {
    Reply(Reply),
    Database(DatabaseIndex, DatabaseCommand),
    Transaction(DatabaseIndex, Vec<QueuedCommand>),
}

const MAX_TRANSACTION_COMMANDS: usize = 1024;
const MAX_TRANSACTION_BYTES: usize = 16 * 1024 * 1024;

#[derive(Default)]
struct Transaction {
    commands: Vec<QueuedCommand>,
    bytes: usize,
    dirty: bool,
}

impl Transaction {
    fn dirty(&mut self) {
        self.dirty = true;
        // EXEC can never run this queue. Reclaim retained payloads immediately.
        self.commands.clear();
        self.bytes = 0;
    }

    fn queue(&mut self, command: QueuedCommand, bytes: usize) -> Reply {
        if !self.dirty {
            if self.commands.len() == MAX_TRANSACTION_COMMANDS
                || bytes > MAX_TRANSACTION_BYTES - self.bytes
            {
                self.dirty();
                return Reply::Error(b"ERR transaction queue limit exceeded".to_vec());
            }
            self.bytes += bytes;
            self.commands.push(command);
        }
        Reply::Simple(b"QUEUED".to_vec())
    }
}

fn late_select() -> Reply {
    Reply::Error(b"ERR SELECT is only allowed as the first command".to_vec())
}

pub(crate) struct Session {
    database: DatabaseIndex,
    first_command: bool,
    transaction: Option<Transaction>,
}

impl Default for Session {
    fn default() -> Self {
        Self {
            database: DatabaseIndex::default(),
            first_command: true,
            transaction: None,
        }
    }
}

impl Session {
    pub(crate) fn prepare(&mut self, request: Request) -> Action {
        // Even an invalid/unknown/empty command consumes the selection window.
        // Framing only calls us once the entire request has been decoded.
        let first = std::mem::replace(&mut self.first_command, false);
        // Charge payload plus conservative per-argument framing/storage overhead;
        // empty arguments must not evade the byte bound. Wire frames are bounded.
        let bytes = request.iter().map(|arg| arg.len() + 32).sum::<usize>();
        let is_exec = request
            .first()
            .is_some_and(|name| name.eq_ignore_ascii_case(b"EXEC"));
        let command = match parse(request) {
            Ok(command) => command,
            Err(_) if is_exec => {
                // Redis treats a rejected EXEC specially: even arity errors
                // exit MULTI immediately, including when already dirty.
                self.transaction = None;
                return Action::Reply(Reply::Error(
                    b"EXECABORT Transaction discarded because of: wrong number of arguments for 'exec' command".to_vec(),
                ));
            }
            Err(error) => {
                if let Some(transaction) = &mut self.transaction {
                    transaction.dirty();
                }
                return Action::Reply(error);
            }
        };
        let reply = match command {
            Command::Multi => {
                if self.transaction.is_some() {
                    // Nested MULTI is an immediate error, but does not dirty or
                    // replace the original queue (unlike wrong-arity MULTI).
                    Reply::Error(b"ERR MULTI calls can not be nested".to_vec())
                } else {
                    self.transaction = Some(Transaction::default());
                    Reply::Simple(b"OK".to_vec())
                }
            }
            Command::Exec => match self.transaction.take() {
                None => Reply::Error(b"ERR EXEC without MULTI".to_vec()),
                Some(transaction) if transaction.dirty => Reply::Error(
                    b"EXECABORT Transaction discarded because of previous errors.".to_vec(),
                ),
                Some(transaction) => {
                    return Action::Transaction(self.database, transaction.commands);
                }
            },
            Command::Discard => match self.transaction.take() {
                None => Reply::Error(b"ERR DISCARD without MULTI".to_vec()),
                Some(_) => Reply::Simple(b"OK".to_vec()),
            },
            command if self.transaction.is_some() => {
                let queued = match command {
                    Command::Ping(None) => QueuedCommand::Reply(Reply::Simple(b"PONG".to_vec())),
                    Command::Ping(Some(message)) => {
                        QueuedCommand::Reply(Reply::Bulk(Some(message)))
                    }
                    Command::Select(_) => QueuedCommand::Reply(late_select()),
                    Command::RuntimeError(error) => QueuedCommand::Reply(error),
                    Command::Database(command) => QueuedCommand::Database(command),
                    _ => unreachable!("transaction control handled above"),
                };
                self.transaction.as_mut().unwrap().queue(queued, bytes)
            }
            Command::Select(index) if first => match parse_database(&index) {
                Ok(database) => {
                    self.database = database;
                    Reply::Simple(b"OK".to_vec())
                }
                Err(error) => error,
            },
            Command::Select(_) => late_select(),
            Command::RuntimeError(error) => error,
            Command::Ping(None) => Reply::Simple(b"PONG".to_vec()),
            Command::Ping(Some(message)) => Reply::Bulk(Some(message)),
            Command::Database(command) => return Action::Database(self.database, command),
        };
        Action::Reply(reply)
    }
}

fn wrong_arity(command: &str) -> Reply {
    Reply::Error(format!("ERR wrong number of arguments for '{command}' command").into_bytes())
}

// Validate Redis's admission arity before taking arguments. Some variable-arity
// commands check their supported form only at execution (even when the error
// text says wrong arity): keep those errors in the EXEC array, not EXECABORT.
// Numeric/type conversion likewise remains execution-time work.
fn parse(request: Request) -> Result<Command, Reply> {
    parse_in_context(request, false)
}

/// Lua uses the same admission and execution semantics as wire commands, but
/// arity/lookup diagnostics and forbidden contexts follow Redis's script rules.
/// No connection state (especially SELECT or a MULTI queue) is available here.
pub(crate) fn script_command(request: Request) -> QueuedCommand {
    match parse_in_context(request, true) {
        Ok(Command::Database(DatabaseCommand::Eval { .. }))
        | Ok(Command::Multi | Command::Exec | Command::Discard) => QueuedCommand::Reply(
            Reply::Error(b"ERR This Redis command is not allowed from script".to_vec()),
        ),
        Ok(Command::Select(_)) => QueuedCommand::Reply(late_select()),
        Ok(Command::Ping(message)) => QueuedCommand::Reply(match message {
            None => Reply::Simple(b"PONG".to_vec()),
            Some(message) => Reply::Bulk(Some(message)),
        }),
        Ok(Command::RuntimeError(error)) | Err(error) => QueuedCommand::Reply(error),
        Ok(Command::Database(command)) => QueuedCommand::Database(command),
    }
}

fn parse_in_context(request: Request, script: bool) -> Result<Command, Reply> {
    let wrong_arity = |command: &str| {
        if script {
            Reply::Error(b"ERR Wrong number of args calling Redis command from script".to_vec())
        } else {
            wrong_arity(command)
        }
    };
    let mut args = request.into_iter();
    let Some(name) = args.next() else {
        return Err(Reply::Error(b"ERR empty command".to_vec()));
    };
    if name.eq_ignore_ascii_case(b"MULTI")
        || name.eq_ignore_ascii_case(b"EXEC")
        || name.eq_ignore_ascii_case(b"DISCARD")
    {
        let (command, label) = if name.eq_ignore_ascii_case(b"MULTI") {
            (Command::Multi, "multi")
        } else if name.eq_ignore_ascii_case(b"EXEC") {
            (Command::Exec, "exec")
        } else {
            (Command::Discard, "discard")
        };
        if args.len() != 0 {
            return Err(wrong_arity(label));
        }
        Ok(command)
    } else if name.eq_ignore_ascii_case(b"EVAL") {
        if args.len() < 2 {
            return Err(wrong_arity("eval"));
        }
        Ok(Command::Database(DatabaseCommand::Eval {
            source: args.next().unwrap(),
            numkeys: args.next().unwrap(),
            args: args.collect(),
        }))
    } else if name.eq_ignore_ascii_case(b"PING") {
        if args.len() > 1 {
            return Ok(Command::RuntimeError(self::wrong_arity("ping")));
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
    } else if name.eq_ignore_ascii_case(b"TYPE") || name.eq_ignore_ascii_case(b"LLEN") {
        let kind = name.eq_ignore_ascii_case(b"TYPE");
        if args.len() != 1 {
            return Err(wrong_arity(if kind { "type" } else { "llen" }));
        }
        let key = args.next().unwrap();
        Ok(Command::Database(if kind {
            DatabaseCommand::Type(key)
        } else {
            DatabaseCommand::ListLength(key)
        }))
    } else if name.eq_ignore_ascii_case(b"LINDEX") {
        if args.len() != 2 {
            return Err(wrong_arity("lindex"));
        }
        Ok(Command::Database(DatabaseCommand::ListIndex {
            key: args.next().unwrap(),
            index: args.next().unwrap(),
        }))
    } else if name.eq_ignore_ascii_case(b"LPOP") {
        if args.len() == 0 {
            return Err(wrong_arity("lpop"));
        }
        if args.len() > 2 {
            return Ok(Command::RuntimeError(self::wrong_arity("lpop")));
        }
        Ok(Command::Database(DatabaseCommand::ListPop {
            key: args.next().unwrap(),
            count: args.next(),
        }))
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
    } else if name.eq_ignore_ascii_case(b"HSET") {
        if args.len() < 3 {
            return Err(wrong_arity("hset"));
        }
        if args.len() % 2 != 1 {
            return Ok(Command::RuntimeError(self::wrong_arity("hset")));
        }
        let key = args.next().unwrap();
        let mut pairs = Vec::with_capacity(args.len() / 2);
        while let Some(field) = args.next() {
            pairs.push((field, args.next().unwrap()));
        }
        Ok(Command::Database(DatabaseCommand::HashSet { key, pairs }))
    } else if name.eq_ignore_ascii_case(b"HDEL") {
        if args.len() < 2 {
            return Err(wrong_arity("hdel"));
        }
        Ok(Command::Database(DatabaseCommand::HashDelete {
            key: args.next().unwrap(),
            fields: args.collect(),
        }))
    } else if name.eq_ignore_ascii_case(b"HGET") || name.eq_ignore_ascii_case(b"HEXISTS") {
        let get = name.eq_ignore_ascii_case(b"HGET");
        if args.len() != 2 {
            return Err(wrong_arity(if get { "hget" } else { "hexists" }));
        }
        let key = args.next().unwrap();
        let field = args.next().unwrap();
        Ok(Command::Database(if get {
            DatabaseCommand::HashGet { key, field }
        } else {
            DatabaseCommand::HashContains { key, field }
        }))
    } else if name.eq_ignore_ascii_case(b"HLEN")
        || name.eq_ignore_ascii_case(b"HGETALL")
        || name.eq_ignore_ascii_case(b"HVALS")
    {
        let command = if name.eq_ignore_ascii_case(b"HLEN") {
            "hlen"
        } else if name.eq_ignore_ascii_case(b"HGETALL") {
            "hgetall"
        } else {
            "hvals"
        };
        if args.len() != 1 {
            return Err(wrong_arity(command));
        }
        let key = args.next().unwrap();
        Ok(Command::Database(match command {
            "hlen" => DatabaseCommand::HashLength(key),
            "hgetall" => DatabaseCommand::HashGetAll(key),
            _ => DatabaseCommand::HashValues(key),
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
        Err(Reply::Error(if script {
            b"ERR Unknown Redis command called from script".to_vec()
        } else {
            b"ERR unknown command".to_vec()
        }))
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
    fn list_key_admission_checks_arity_but_defers_integer_validation() {
        for (name, required) in [
            (b"tYpE".as_slice(), 1),
            (b"lLeN", 1),
            (b"lInDeX", 2),
            (b"lPoP", 1),
        ] {
            for count in 0..5 {
                let mut args = vec![name];
                args.resize(count + 1, b"\0\xff");
                let valid = count == required || (name == b"lPoP" && count == 2);
                assert_eq!(
                    matches!(parse(request(&args)), Ok(Command::Database(_))),
                    valid
                );
            }
        }
        assert_eq!(
            parse(request(&[b"LINDEX", b"", b"invalid"])),
            Ok(Command::Database(DatabaseCommand::ListIndex {
                key: vec![],
                index: b"invalid".to_vec()
            }))
        );
        assert_eq!(
            parse(request(&[b"LPOP", b"", b"-1"])),
            Ok(Command::Database(DatabaseCommand::ListPop {
                key: vec![],
                count: Some(b"-1".to_vec())
            }))
        );
        assert_eq!(
            parse(request(&[b"LPOP", b"\0\xff"])),
            Ok(Command::Database(DatabaseCommand::ListPop {
                key: b"\0\xff".to_vec(),
                count: None
            }))
        );
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
    fn hash_admission_checks_complete_pairs_and_preserves_binary_arguments() {
        let key = b"\0\xff".to_vec();
        for (args, command) in [
            (
                vec![&b"hSeT"[..], &key, b"", b"\0\xff\r\n", b"", b"last"],
                DatabaseCommand::HashSet {
                    key: key.clone(),
                    pairs: vec![(vec![], b"\0\xff\r\n".to_vec()), (vec![], b"last".to_vec())],
                },
            ),
            (
                vec![b"hDeL", &key, b"", b""],
                DatabaseCommand::HashDelete {
                    key: key.clone(),
                    fields: vec![vec![], vec![]],
                },
            ),
            (
                vec![b"hGeT", &key, b""],
                DatabaseCommand::HashGet {
                    key: key.clone(),
                    field: vec![],
                },
            ),
            (
                vec![b"hExIsTs", &key, b""],
                DatabaseCommand::HashContains {
                    key: key.clone(),
                    field: vec![],
                },
            ),
            (
                vec![b"hLeN", &key],
                DatabaseCommand::HashLength(key.clone()),
            ),
            (
                vec![b"hGeTaLl", &key],
                DatabaseCommand::HashGetAll(key.clone()),
            ),
            (
                vec![b"hVaLs", &key],
                DatabaseCommand::HashValues(key.clone()),
            ),
        ] {
            assert_eq!(parse(request(&args)), Ok(Command::Database(command)));
        }
        for name in [
            &b"HSET"[..],
            b"HDEL",
            b"HGET",
            b"HEXISTS",
            b"HLEN",
            b"HGETALL",
            b"HVALS",
        ] {
            for count in 0..8 {
                let valid = match name {
                    b"HSET" => count >= 3 && count % 2 == 1,
                    b"HDEL" => count >= 2,
                    b"HGET" | b"HEXISTS" => count == 2,
                    _ => count == 1,
                };
                let mut args = vec![name];
                args.extend(vec![&b""[..]; count]);
                assert_eq!(
                    matches!(parse(request(&args)), Ok(Command::Database(_))),
                    valid,
                    "{name:?}, {count}"
                );
            }
        }
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
            vec![b"EXEC"],
            vec![b"DISCARD"],
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
