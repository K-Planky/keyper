//! One dedicated thread owns all databases. No state reference crosses a channel.

use std::collections::{HashMap, VecDeque};
use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::thread;

use tokio::sync::{Notify, mpsc, oneshot};

use crate::command::{DatabaseCommand, ListEnd, QueuedCommand, parse_integer};
use crate::resp::Reply;

mod blocking;
mod hash;
mod set;

#[cfg(test)]
mod hashes_tests;
#[cfg(test)]
mod lists_tests;
#[cfg(test)]
mod numeric_sets_tests;

use blocking::Owner;

const DATABASES: usize = 16;
pub(crate) const WORK_CAPACITY: usize = 256;

/// Construction enforces the namespace bound before work reaches the owner.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct DatabaseIndex(u8);

impl DatabaseIndex {
    pub(crate) fn new(index: i32) -> Option<Self> {
        u8::try_from(index)
            .ok()
            .filter(|index| usize::from(*index) < DATABASES)
            .map(Self)
    }
}

enum Value {
    String(Vec<u8>),
    List(VecDeque<Vec<u8>>),
    Set(set::Set),
    Hash(hash::Hash),
}

#[derive(Default)]
struct Databases([HashMap<Vec<u8>, Value>; DATABASES]);

fn wrong_type() -> Reply {
    Reply::Error(b"WRONGTYPE Operation against a key holding the wrong kind of value".to_vec())
}

impl Databases {
    fn execute(&mut self, database: DatabaseIndex, command: DatabaseCommand) -> Reply {
        let entries = &mut self.0[usize::from(database.0)];
        // This entire match is one non-interleaved operation: its lookup/insert
        // (or full variadic operation / collection snapshot) is the linearization point.
        // Replies own bytes; sending/dropping one cannot retain or undo access.
        match command {
            DatabaseCommand::Eval { .. } => unreachable!("EVAL executes at the owner boundary"),
            DatabaseCommand::Get(key) => match entries.get(&key) {
                None => Reply::Bulk(None),
                Some(Value::String(value)) => Reply::Bulk(Some(value.clone())),
                Some(_) => wrong_type(),
            },
            DatabaseCommand::Set { key, value } => {
                entries.insert(key, Value::String(value));
                Reply::Simple(b"OK".to_vec())
            }
            DatabaseCommand::Exists(keys) => Reply::Integer(
                keys.iter()
                    .map(|key| i64::from(entries.contains_key(key)))
                    .sum(),
            ),
            DatabaseCommand::Type(key) => Reply::Simple(
                match entries.get(&key) {
                    None => &b"none"[..],
                    Some(Value::String(_)) => b"string",
                    Some(Value::List(_)) => b"list",
                    Some(Value::Set(_)) => b"set",
                    Some(Value::Hash(_)) => b"hash",
                }
                .to_vec(),
            ),
            DatabaseCommand::ListLength(key) => match entries.get(&key) {
                None => Reply::Integer(0),
                Some(Value::List(list)) => Reply::Integer(list.len() as i64),
                Some(_) => wrong_type(),
            },
            DatabaseCommand::ListIndex { key, index } => {
                // Redis checks existence/type before converting the index.
                let list = match entries.get(&key) {
                    None => return Reply::Bulk(None),
                    Some(Value::List(list)) => list,
                    Some(_) => return wrong_type(),
                };
                let Some(index) = parse_integer(&index) else {
                    return Reply::Error(b"ERR value is not an integer or out of range".to_vec());
                };
                // unsigned_abs also handles i64::MIN; no wrapping negation or
                // truncation on narrower targets. VecDeque indexing is O(1).
                let offset = if index < 0 {
                    usize::try_from(index.unsigned_abs())
                        .ok()
                        .and_then(|distance| list.len().checked_sub(distance))
                } else {
                    usize::try_from(index).ok()
                };
                Reply::Bulk(offset.and_then(|offset| list.get(offset).cloned()))
            }
            DatabaseCommand::ListPop { key, count } => {
                // Unlike LINDEX, count validation precedes even a missing or
                // wrong-type lookup. Retain presence for scalar/array replies.
                let count = match count {
                    None => None,
                    Some(bytes) => match parse_integer(&bytes).filter(|n| *n >= 0) {
                        Some(n) => Some(n as u64),
                        None => {
                            return Reply::Error(
                                b"ERR value is out of range, must be positive".to_vec(),
                            );
                        }
                    },
                };
                let list = match entries.get_mut(&key) {
                    None if count.is_some() => return Reply::Array(None),
                    None => return Reply::Bulk(None),
                    Some(Value::List(list)) => list,
                    Some(_) => return wrong_type(),
                };
                let result = if let Some(count) = count {
                    // Clamp before conversion/allocation: huge valid counts
                    // cost only the number of elements actually returned.
                    let take = count.min(list.len() as u64) as usize;
                    Reply::Array(Some(
                        list.drain(..take).map(|v| Reply::Bulk(Some(v))).collect(),
                    ))
                } else {
                    Reply::Bulk(list.pop_front())
                };
                if list.is_empty() {
                    entries.remove(&key);
                }
                result
            }
            DatabaseCommand::Increment(key) => {
                let current = match entries.get(&key) {
                    None => 0,
                    Some(Value::String(bytes)) => match parse_integer(bytes) {
                        Some(value) => value,
                        None => {
                            return Reply::Error(
                                b"ERR value is not an integer or out of range".to_vec(),
                            );
                        }
                    },
                    Some(_) => return wrong_type(),
                };
                let Some(next) = current.checked_add(1) else {
                    return Reply::Error(b"ERR increment or decrement would overflow".to_vec());
                };
                entries.insert(key, Value::String(next.to_string().into_bytes()));
                Reply::Integer(next)
            }
            DatabaseCommand::SetAdd { key, members } => {
                let value = entries
                    .entry(key)
                    .or_insert_with(|| Value::Set(set::Set::default()));
                let Value::Set(set) = value else {
                    return wrong_type();
                };
                let added = members
                    .into_iter()
                    .map(|member| i64::from(set.insert(member)))
                    .sum();
                Reply::Integer(added)
            }
            DatabaseCommand::SetRemove { key, members } => {
                let set = match entries.get_mut(&key) {
                    None => return Reply::Integer(0),
                    Some(Value::Set(set)) => set,
                    Some(_) => return wrong_type(),
                };
                let removed = members
                    .iter()
                    .map(|member| i64::from(set.remove(member)))
                    .sum();
                if set.members.is_empty() {
                    entries.remove(&key);
                }
                Reply::Integer(removed)
            }
            DatabaseCommand::SetContains { key, member } => match entries.get(&key) {
                None => Reply::Integer(0),
                Some(Value::Set(set)) => Reply::Integer(i64::from(set.contains(&member))),
                Some(_) => wrong_type(),
            },
            DatabaseCommand::SetCardinality(key) => match entries.get(&key) {
                None => Reply::Integer(0),
                Some(Value::Set(set)) => Reply::Integer(set.members.len() as i64),
                Some(_) => wrong_type(),
            },
            DatabaseCommand::SetMembers(key) => match entries.get(&key) {
                None => Reply::Array(Some(vec![])),
                Some(Value::Set(set)) => Reply::Array(Some(
                    set.members
                        .iter()
                        .map(|member| Reply::Bulk(Some(member.clone())))
                        .collect(),
                )),
                Some(_) => wrong_type(),
            },
            DatabaseCommand::HashSet { key, pairs } => {
                let value = entries
                    .entry(key)
                    .or_insert_with(|| Value::Hash(hash::Hash::default()));
                let Value::Hash(hash) = value else {
                    return wrong_type();
                };
                // Each first insertion counts once; repeated fields overwrite
                // in argument order, so their final supplied value wins.
                let added = pairs
                    .into_iter()
                    .map(|(field, value)| i64::from(hash.set(field, value)))
                    .sum();
                Reply::Integer(added)
            }
            DatabaseCommand::HashGet { key, field } => match entries.get(&key) {
                None => Reply::Bulk(None),
                Some(Value::Hash(hash)) => Reply::Bulk(hash.get(&field).cloned()),
                Some(_) => wrong_type(),
            },
            DatabaseCommand::HashDelete { key, fields } => {
                let hash = match entries.get_mut(&key) {
                    None => return Reply::Integer(0),
                    Some(Value::Hash(hash)) => hash,
                    Some(_) => return wrong_type(),
                };
                let removed = fields
                    .iter()
                    .map(|field| i64::from(hash.remove(field)))
                    .sum();
                if hash.pairs.is_empty() {
                    entries.remove(&key);
                }
                Reply::Integer(removed)
            }
            DatabaseCommand::HashContains { key, field } => match entries.get(&key) {
                None => Reply::Integer(0),
                Some(Value::Hash(hash)) => Reply::Integer(i64::from(hash.get(&field).is_some())),
                Some(_) => wrong_type(),
            },
            DatabaseCommand::HashLength(key) => match entries.get(&key) {
                None => Reply::Integer(0),
                Some(Value::Hash(hash)) => Reply::Integer(hash.pairs.len() as i64),
                Some(_) => wrong_type(),
            },
            DatabaseCommand::HashGetAll(key) => match entries.get(&key) {
                None => Reply::Array(Some(vec![])),
                Some(Value::Hash(hash)) => Reply::Array(Some(
                    hash.pairs
                        .iter()
                        .flat_map(|(field, value)| {
                            [
                                Reply::Bulk(Some(field.clone())),
                                Reply::Bulk(Some(value.clone())),
                            ]
                        })
                        .collect(),
                )),
                Some(_) => wrong_type(),
            },
            DatabaseCommand::HashValues(key) => match entries.get(&key) {
                None => Reply::Array(Some(vec![])),
                Some(Value::Hash(hash)) => Reply::Array(Some(
                    hash.pairs
                        .iter()
                        .map(|(_, value)| Reply::Bulk(Some(value.clone())))
                        .collect(),
                )),
                Some(_) => wrong_type(),
            },
            DatabaseCommand::Push { end, key, values } => {
                let value = entries
                    .entry(key)
                    .or_insert_with(|| Value::List(VecDeque::new()));
                let Value::List(list) = value else {
                    return wrong_type();
                };
                // Complete every insertion and capture the resulting length as
                // one atomic unit. Owner waiter service runs only after it.
                for value in values {
                    match end {
                        ListEnd::Left => list.push_front(value),
                        ListEnd::Right => list.push_back(value),
                    }
                }
                // VecDeque's nonzero-sized elements bound length below i64::MAX
                // on supported 32/64-bit targets, before allocation can succeed.
                Reply::Integer(list.len() as i64)
            }
            DatabaseCommand::Pop { end, keys, timeout } => {
                // Nonblocking execution for an isolated unit. Ordinary owner
                // requests use Owner::process to register an empty attempt.
                match blocking::Pop::new(end, keys, &timeout, tokio::time::Instant::now()) {
                    Ok(pop) => self.pop(database, &pop).unwrap_or(Reply::Array(None)),
                    Err(error) => error,
                }
            }
        }
    }
}

enum Operation {
    Single(DatabaseCommand),
    Transaction(Vec<QueuedCommand>),
}

struct Work {
    database: DatabaseIndex,
    command: Operation,
    reply: oneshot::Sender<Reply>,
}

#[derive(Clone)]
pub(crate) struct Client {
    work: mpsc::Sender<Work>,
    canceled: Arc<Notify>,
}

// Cancellation closes the reply first, then wakes the owner. Notify coalesces
// bursts without an unbounded cleanup queue or a task per pending request.
struct PendingReply {
    receive: oneshot::Receiver<Reply>,
    canceled: Arc<Notify>,
    completed: bool,
}

impl Drop for PendingReply {
    fn drop(&mut self) {
        if !self.completed {
            self.receive.close();
            self.canceled.notify_one();
        }
    }
}

impl Client {
    pub(crate) async fn execute(
        &self,
        database: DatabaseIndex,
        command: DatabaseCommand,
    ) -> io::Result<Reply> {
        self.submit(database, Operation::Single(command)).await
    }

    pub(crate) async fn transaction(
        &self,
        database: DatabaseIndex,
        commands: Vec<QueuedCommand>,
    ) -> io::Result<Reply> {
        self.submit(database, Operation::Transaction(commands))
            .await
    }

    async fn submit(&self, database: DatabaseIndex, command: Operation) -> io::Result<Reply> {
        let (reply, receive) = oneshot::channel();
        let mut pending = PendingReply {
            receive,
            canceled: self.canceled.clone(),
            completed: false,
        };
        self.work
            .send(Work {
                database,
                command,
                reply,
            })
            .await
            .map_err(|_| io::Error::other("state owner work channel closed"))?;
        let result = (&mut pending.receive).await;
        pending.completed = true;
        result.map_err(|_| io::Error::other("state owner reply channel closed"))
    }
}

pub(crate) type Completion = oneshot::Receiver<io::Result<()>>;

pub(crate) fn spawn() -> io::Result<(Client, Completion)> {
    spawn_with(run)
}

fn spawn_with(
    run: impl FnOnce(mpsc::Receiver<Work>, Arc<Notify>) -> io::Result<()> + Send + 'static,
) -> io::Result<(Client, Completion)> {
    let (work, receive) = mpsc::channel(WORK_CAPACITY);
    let canceled = Arc::new(Notify::new());
    let owner_canceled = canceled.clone();
    let (finished, completion) = oneshot::channel();
    thread::Builder::new()
        .name("keyper-state".into())
        .spawn(move || {
            // Report panic only to fail-stop supervision, never to reset state
            // or turn uncertain execution into a normal client error.
            let outcome = catch_unwind(AssertUnwindSafe(|| run(receive, owner_canceled)))
                .unwrap_or_else(|_| {
                    Err(io::Error::other("state owner panicked; service must stop"))
                });
            let _ = finished.send(outcome);
        })
        .map_err(|error| io::Error::new(error.kind(), format!("start state owner: {error}")))?;
    Ok((Client { work, canceled }, completion))
}

fn run(work: mpsc::Receiver<Work>, canceled: Arc<Notify>) -> io::Result<()> {
    run_owner(work, canceled, Owner::default())
}

fn run_owner(work: mpsc::Receiver<Work>, canceled: Arc<Notify>, owner: Owner) -> io::Result<()> {
    // A timer driver on this same dedicated thread parks only between atomic
    // units. Synchronous state work (including Lua) never runs on network workers.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()?;
    runtime.block_on(owner.run(work, canceled));
    Ok(())
}

#[cfg(test)]
pub(crate) fn observed() -> io::Result<(Client, Completion, tokio::sync::watch::Receiver<usize>)> {
    let (count, observe) = tokio::sync::watch::channel(0);
    let (client, completion) =
        spawn_with(move |work, canceled| run_owner(work, canceled, Owner::observed(count)))?;
    Ok((client, completion, observe))
}

#[cfg(test)]
pub(crate) fn failing_owner(fail: oneshot::Receiver<()>) -> io::Result<(Client, Completion)> {
    spawn_with(move |_work, _canceled| {
        fail.blocking_recv().unwrap();
        panic!("injected owner failure");
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::timeout;

    use super::*;

    const LIMIT: Duration = Duration::from_secs(5);

    #[test]
    fn strings_are_binary_safe_overwrite_and_count_each_existing_argument() {
        let mut databases = Databases::default();
        for index in 0..16 {
            let db = DatabaseIndex::new(index).unwrap();
            assert_eq!(
                databases.execute(db, DatabaseCommand::Get(vec![])),
                Reply::Bulk(None)
            );
            let value = vec![0, 255, index as u8];
            assert_eq!(
                databases.execute(db, DatabaseCommand::Set { key: vec![], value }),
                Reply::Simple(b"OK".to_vec())
            );
        }
        for index in 0..16 {
            let db = DatabaseIndex::new(index).unwrap();
            assert_eq!(
                databases.execute(db, DatabaseCommand::Get(vec![])),
                Reply::Bulk(Some(vec![0, 255, index as u8]))
            );
            assert_eq!(
                databases.execute(db, DatabaseCommand::Exists(vec![vec![], vec![1], vec![]])),
                Reply::Integer(2)
            );
            databases.execute(
                db,
                DatabaseCommand::Set {
                    key: vec![],
                    value: vec![],
                },
            );
            assert_eq!(
                databases.execute(db, DatabaseCommand::Get(vec![])),
                Reply::Bulk(Some(vec![]))
            );
        }
    }

    fn push(end: ListEnd, key: &[u8], values: &[&[u8]]) -> DatabaseCommand {
        DatabaseCommand::Push {
            end,
            key: key.to_vec(),
            values: values.iter().map(|value| value.to_vec()).collect(),
        }
    }

    fn assert_list(databases: &Databases, db: DatabaseIndex, key: &[u8], expected: &[&[u8]]) {
        let Some(Value::List(list)) = databases.0[usize::from(db.0)].get(key) else {
            panic!("expected a stored list");
        };
        assert_eq!(list.iter().map(Vec::as_slice).collect::<Vec<_>>(), expected);
    }

    #[test]
    fn pushes_create_lists_and_preserve_left_right_and_variadic_order() {
        let mut databases = Databases::default();
        let db = DatabaseIndex::default();
        for (end, expected) in [
            (ListEnd::Left, [&b"c"[..], b"b", b"a"]),
            (ListEnd::Right, [&b"a"[..], b"b", b"c"]),
        ] {
            let key = match end {
                ListEnd::Left => &b"left"[..],
                ListEnd::Right => &b"right"[..],
            };
            assert_eq!(
                databases.execute(db, push(end, key, &[b"a"])),
                Reply::Integer(1)
            );
            assert_eq!(
                databases.execute(db, push(end, key, &[b"b", b"c"])),
                Reply::Integer(3)
            );
            assert_list(&databases, db, key, &expected);
        }

        let key = b"\0\xff\r\n";
        assert_eq!(
            databases.execute(db, push(ListEnd::Right, key, &[b"", b"\0\xff", b""])),
            Reply::Integer(3)
        );
        assert_eq!(
            databases.execute(db, push(ListEnd::Left, key, &[b"a", b"b"])),
            Reply::Integer(5)
        );
        assert_eq!(
            databases.execute(db, push(ListEnd::Right, key, &[b"c", b"d"])),
            Reply::Integer(7)
        );
        assert_list(
            &databases,
            db,
            key,
            &[b"b", b"a", b"", b"\0\xff", b"", b"c", b"d"],
        );
    }

    #[test]
    fn wrong_types_do_not_mutate_and_set_replaces_lists_with_strings() {
        let mut databases = Databases::default();
        let db = DatabaseIndex::default();
        for value in [&b""[..], b"\0\xff\r\n"] {
            databases.execute(
                db,
                DatabaseCommand::Set {
                    key: vec![],
                    value: value.to_vec(),
                },
            );
            for end in [ListEnd::Left, ListEnd::Right] {
                assert_eq!(
                    databases.execute(db, push(end, b"", &[b"a", b"b"])),
                    wrong_type()
                );
                assert_eq!(
                    databases.execute(db, DatabaseCommand::Get(vec![])),
                    Reply::Bulk(Some(value.to_vec()))
                );
            }
        }
        databases.execute(db, push(ListEnd::Right, b"list", &[b"a", b"b"]));
        assert_eq!(
            databases.execute(db, DatabaseCommand::Get(b"list".to_vec())),
            wrong_type()
        );
        assert_list(&databases, db, b"list", &[b"a", b"b"]);
        assert_eq!(
            databases.execute(
                db,
                DatabaseCommand::Exists(vec![
                    b"list".to_vec(),
                    vec![],
                    b"missing".to_vec(),
                    b"list".to_vec()
                ])
            ),
            Reply::Integer(3)
        );
        assert_eq!(
            databases.execute(
                db,
                DatabaseCommand::Set {
                    key: b"list".to_vec(),
                    value: vec![]
                }
            ),
            Reply::Simple(b"OK".to_vec())
        );
        assert_eq!(
            databases.execute(db, DatabaseCommand::Get(b"list".to_vec())),
            Reply::Bulk(Some(vec![]))
        );
        for end in [ListEnd::Left, ListEnd::Right] {
            assert_eq!(
                databases.execute(db, push(end, b"list", &[b"c"])),
                wrong_type()
            );
        }
    }

    #[tokio::test]
    async fn full_work_queue_backpressures_without_blocking_tokio() {
        let (release, gate) = oneshot::channel();
        let (client, done) = spawn_with(move |work, canceled| {
            gate.blocking_recv().unwrap();
            run(work, canceled)
        })
        .unwrap();
        let mut replies = vec![];
        for _ in 0..WORK_CAPACITY {
            let (reply, receive) = oneshot::channel();
            assert!(
                client
                    .work
                    .try_send(Work {
                        database: DatabaseIndex::default(),
                        command: Operation::Single(DatabaseCommand::Get(vec![])),
                        reply,
                    })
                    .is_ok()
            );
            replies.push(receive);
        }
        {
            let waiting = client.execute(DatabaseIndex::default(), DatabaseCommand::Get(vec![]));
            tokio::pin!(waiting);
            tokio::select! {
                biased;
                result = &mut waiting => panic!("full queue admitted work: {result:?}"),
                _ = tokio::task::yield_now() => (),
            }
            // The yield progressed on a single-thread runtime while send waited.
            release.send(()).unwrap();
            assert_eq!(
                timeout(LIMIT, waiting).await.unwrap().unwrap(),
                Reply::Bulk(None)
            );
        }
        drop(replies);
        drop(client);
        timeout(LIMIT, done).await.unwrap().unwrap().unwrap();
    }

    #[tokio::test]
    async fn cancel_execute_before_admission_or_owner_start_never_mutates() {
        for full_queue in [false, true] {
            let (release, gate) = oneshot::channel();
            let (client, done) = spawn_with(move |work, canceled| {
                gate.blocking_recv().unwrap();
                run(work, canceled)
            })
            .unwrap();
            let mut replies = Vec::new();
            if full_queue {
                for _ in 0..WORK_CAPACITY {
                    let (reply, receive) = oneshot::channel();
                    client
                        .work
                        .try_send(Work {
                            database: DatabaseIndex::default(),
                            command: Operation::Single(DatabaseCommand::Get(vec![])),
                            reply,
                        })
                        .ok()
                        .unwrap();
                    replies.push(receive);
                }
            }
            let mut execution = Box::pin(client.execute(
                DatabaseIndex::default(),
                DatabaseCommand::Set {
                    key: b"canceled".to_vec(),
                    value: b"must-not-apply".to_vec(),
                },
            ));
            tokio::select! {
                biased;
                result = &mut execution => panic!("owner gate did not hold: {result:?}"),
                _ = tokio::task::yield_now() => (),
            }
            assert_eq!(
                client.work.capacity(),
                if full_queue { 0 } else { WORK_CAPACITY - 1 }
            );
            drop(execution);
            release.send(()).unwrap();
            assert_eq!(
                timeout(
                    LIMIT,
                    client.execute(
                        DatabaseIndex::default(),
                        DatabaseCommand::Get(b"canceled".to_vec())
                    )
                )
                .await
                .unwrap()
                .unwrap(),
                Reply::Bulk(None)
            );
            drop(replies);
            drop(client);
            timeout(LIMIT, done).await.unwrap().unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn canceled_exec_before_admission_or_owner_start_skips_the_entire_batch() {
        for full_queue in [false, true] {
            let (release, gate) = oneshot::channel();
            let (client, done) = spawn_with(move |work, canceled| {
                gate.blocking_recv().unwrap();
                run(work, canceled)
            })
            .unwrap();
            let mut replies = Vec::new();
            if full_queue {
                for _ in 0..WORK_CAPACITY {
                    let (reply, receive) = oneshot::channel();
                    client
                        .work
                        .try_send(Work {
                            database: DatabaseIndex::default(),
                            command: Operation::Single(DatabaseCommand::Get(vec![])),
                            reply,
                        })
                        .ok()
                        .unwrap();
                    replies.push(receive);
                }
            }
            let commands = (0..3)
                .map(|_| QueuedCommand::Database(DatabaseCommand::Increment(b"skipped".to_vec())))
                .collect();
            let mut execution = Box::pin(client.transaction(DatabaseIndex::default(), commands));
            tokio::select! {
                biased;
                result = &mut execution => panic!("owner gate did not hold: {result:?}"),
                _ = tokio::task::yield_now() => (),
            }
            assert_eq!(
                client.work.capacity(),
                if full_queue { 0 } else { WORK_CAPACITY - 1 }
            );
            drop(execution);
            release.send(()).unwrap();
            assert_eq!(
                timeout(
                    LIMIT,
                    client.execute(
                        DatabaseIndex::default(),
                        DatabaseCommand::Get(b"skipped".to_vec())
                    )
                )
                .await
                .unwrap()
                .unwrap(),
                Reply::Bulk(None)
            );
            drop(replies);
            drop(client);
            timeout(LIMIT, done).await.unwrap().unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn dropped_reply_before_execution_skips_work_and_after_execution_keeps_writes() {
        // Queue before starting the owner: deterministic cancellation, no sleep.
        let (send, receive) = mpsc::channel(WORK_CAPACITY);
        let db = DatabaseIndex::default();
        let (reply, canceled) = oneshot::channel();
        send.send(Work {
            database: db,
            command: Operation::Single(DatabaseCommand::Set {
                key: b"key".to_vec(),
                value: b"skipped".to_vec(),
            }),
            reply,
        })
        .await
        .unwrap();
        drop(canceled);
        let (reply, applied) = oneshot::channel();
        send.send(Work {
            database: db,
            command: Operation::Single(DatabaseCommand::Set {
                key: b"other".to_vec(),
                value: b"kept".to_vec(),
            }),
            reply,
        })
        .await
        .unwrap();
        let (reply, skipped_value) = oneshot::channel();
        send.send(Work {
            database: db,
            command: Operation::Single(DatabaseCommand::Get(b"key".to_vec())),
            reply,
        })
        .await
        .unwrap();
        let (reply, kept_value) = oneshot::channel();
        send.send(Work {
            database: db,
            command: Operation::Single(DatabaseCommand::Get(b"other".to_vec())),
            reply,
        })
        .await
        .unwrap();
        let (finished, done) = oneshot::channel();
        thread::spawn(move || {
            run(receive, Arc::new(Notify::new())).unwrap();
            finished.send(()).unwrap();
        });
        // GET is a fence after SET, but the SET reply is never read. Dropping
        // that receiver after application must not undo the committed write.
        assert_eq!(
            timeout(LIMIT, skipped_value).await.unwrap().unwrap(),
            Reply::Bulk(None)
        );
        assert_eq!(
            timeout(LIMIT, kept_value).await.unwrap().unwrap(),
            Reply::Bulk(Some(b"kept".to_vec()))
        );
        drop(applied);
        let (reply, after_loss) = oneshot::channel();
        send.send(Work {
            database: db,
            command: Operation::Single(DatabaseCommand::Get(b"other".to_vec())),
            reply,
        })
        .await
        .unwrap();
        assert_eq!(
            timeout(LIMIT, after_loss).await.unwrap().unwrap(),
            Reply::Bulk(Some(b"kept".to_vec()))
        );
        drop(send);
        timeout(LIMIT, done).await.unwrap().unwrap();
    }
}
