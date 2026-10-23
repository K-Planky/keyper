//! Eligibility, waiter indexes, and terminal decisions belong to the state owner.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Notify, mpsc, oneshot};
use tokio::time::{Instant, sleep_until};

use super::{DATABASES, DatabaseIndex, Databases, Operation, Value, Work, wrong_type};
use crate::command::{DatabaseCommand, ListEnd, QueuedCommand, parse_integer, script_command};
use crate::resp::Reply;

#[cfg(test)]
mod tests;

pub(super) struct Pop {
    end: ListEnd,
    // Unique keys in original priority order; duplicates still count on input.
    keys: Vec<Vec<u8>>,
    deadline: Option<Instant>,
}

impl Pop {
    pub(super) fn new(
        end: ListEnd,
        keys: Vec<Vec<u8>>,
        timeout: &[u8],
        now: Instant,
    ) -> Result<Self, Reply> {
        if !(1..=5).contains(&keys.len()) {
            return Err(Reply::Error(
                b"ERR blocking pop requires 1 to 5 keys".to_vec(),
            ));
        }
        let deadline = deadline(timeout, now)?;
        let mut unique = Vec::with_capacity(keys.len());
        for key in keys {
            if !unique.contains(&key) {
                unique.push(key);
            }
        }
        Ok(Self {
            end,
            keys: unique,
            deadline,
        })
    }
}

fn deadline(bytes: &[u8], now: Instant) -> Result<Option<Instant>, Reply> {
    let invalid = || Reply::Error(b"ERR timeout is not a float or out of range".to_vec());
    let text = std::str::from_utf8(bytes).map_err(|_| invalid())?;
    let seconds = text.parse::<f64>().map_err(|_| invalid())?;
    if !seconds.is_finite() {
        return Err(invalid());
    }
    // Rust's parser silently rounds decimal underflow to zero. Only a genuine
    // zero mantissa (including signed zero) may mean an indefinite wait.
    if seconds == 0.0 {
        let mantissa = text.split(['e', 'E']).next().unwrap();
        if mantissa.bytes().any(|b| matches!(b, b'1'..=b'9')) {
            return Err(invalid());
        }
        return Ok(None);
    }
    if seconds < 0.0 {
        return Err(Reply::Error(b"ERR timeout is negative".to_vec()));
    }
    let millis = (seconds * 1000.0).ceil();
    // 2^63 is the f64 representation of i64::MAX, so equality is out of
    // range too. Check before casting (Rust's float casts otherwise saturate).
    if millis >= i64::MAX as f64 {
        return Err(invalid());
    }
    let duration = Duration::from_millis((millis as u64).max(1));
    now.checked_add(duration).map(Some).ok_or_else(invalid)
}

impl Databases {
    /// None means an empty observation. Check and registration are performed
    /// by the same owner unit; no producer can run in between them.
    pub(super) fn pop(&mut self, database: DatabaseIndex, pop: &Pop) -> Option<Reply> {
        let entries = &mut self.0[usize::from(database.0)];
        for key in &pop.keys {
            match entries.get_mut(key) {
                None => continue,
                Some(Value::String(_) | Value::Set(_) | Value::Hash(_)) => {
                    return Some(wrong_type());
                }
                Some(Value::List(list)) => {
                    let value = match pop.end {
                        ListEnd::Left => list.pop_front(),
                        ListEnd::Right => list.pop_back(),
                    };
                    let empty = list.is_empty();
                    if empty {
                        entries.remove(key);
                    }
                    if let Some(value) = value {
                        return Some(Reply::Array(Some(vec![
                            Reply::Bulk(Some(key.clone())),
                            Reply::Bulk(Some(value)),
                        ])));
                    }
                }
            }
        }
        None
    }
}

struct Waiter {
    database: DatabaseIndex,
    pop: Pop,
    reply: oneshot::Sender<Reply>,
}

#[derive(Default)]
pub(super) struct Owner {
    databases: Databases,
    // Monotonic IDs encode FIFO priority across overlapping key sets.
    next_id: u64,
    waiters: BTreeMap<u64, Waiter>,
    watching: [HashMap<Vec<u8>, BTreeSet<u64>>; DATABASES],
    deadlines: BTreeSet<(Instant, u64)>,
    #[cfg(test)]
    waiter_count: Option<tokio::sync::watch::Sender<usize>>,
}

impl Owner {
    #[cfg(test)]
    pub(super) fn observed(count: tokio::sync::watch::Sender<usize>) -> Self {
        Self {
            waiter_count: Some(count),
            ..Self::default()
        }
    }

    pub(super) async fn run(mut self, mut work: mpsc::Receiver<Work>, canceled: Arc<Notify>) {
        loop {
            self.expire(Instant::now());
            let work = if let Some(&(deadline, _)) = self.deadlines.first() {
                tokio::select! {
                    // Expiry wins if both events are ready at this boundary.
                    biased;
                    _ = sleep_until(deadline) => continue,
                    _ = canceled.notified() => {
                        self.reap_closed();
                        continue;
                    }
                    work = work.recv() => work,
                }
            } else {
                tokio::select! {
                    biased;
                    _ = canceled.notified() => {
                        self.reap_closed();
                        continue;
                    }
                    work = work.recv() => work,
                }
            };
            let Some(work) = work else {
                // Closing all clients ends indefinite waits too. Drop reply
                // senders and all indexes together, without waiting on sockets.
                return;
            };
            self.process(work, Instant::now());
        }
    }

    fn process(&mut self, work: Work, now: Instant) {
        let Work {
            database,
            command,
            reply,
        } = work;
        if reply.is_closed() {
            return;
        }
        match command {
            Operation::Transaction(commands) => {
                let mut ready = Vec::new();
                let results = commands
                    .into_iter()
                    .map(|command| match command {
                        QueuedCommand::Reply(reply) => reply,
                        QueuedCommand::Database(command) => {
                            self.execute_atomic(database, command, &mut ready)
                        }
                    })
                    .collect();
                // All replies describe the isolated batch, never subsequent waiter
                // consumption. Runtime errors are array entries, not rollback/exit.
                let _ = reply.send(Reply::Array(Some(results)));
                self.serve_ready(database, &ready);
            }
            Operation::Single(DatabaseCommand::Pop { end, keys, timeout }) => {
                let pop = match Pop::new(end, keys, &timeout, now) {
                    Ok(pop) => pop,
                    Err(error) => {
                        let _ = reply.send(error);
                        return;
                    }
                };
                if let Some(result) = self.databases.pop(database, &pop) {
                    let _ = reply.send(result);
                } else {
                    self.register(Waiter {
                        database,
                        pop,
                        reply,
                    });
                }
            }
            Operation::Single(command) => {
                let mut ready = Vec::new();
                let result = self.execute_atomic(database, command, &mut ready);
                // Failed delivery cannot undo a push or suppress waiter service.
                let _ = reply.send(result);
                self.serve_ready(database, &ready);
            }
        }
    }

    fn execute_atomic(
        &mut self,
        database: DatabaseIndex,
        command: DatabaseCommand,
        ready: &mut Vec<Vec<u8>>,
    ) -> Reply {
        if let DatabaseCommand::Eval {
            source,
            numkeys,
            args,
        } = command
        {
            // Validation is execution-time work, including inside EXEC. Parsing
            // source follows numkeys validation and cannot mutate the database.
            let count = match parse_integer(&numkeys) {
                None => {
                    return Reply::Error(b"ERR value is not an integer or out of range".to_vec());
                }
                Some(n) if n < 0 => {
                    return Reply::Error(b"ERR Number of keys can't be negative".to_vec());
                }
                Some(n) if n as u64 > args.len() as u64 => {
                    return Reply::Error(
                        b"ERR Number of keys can't be greater than number of args".to_vec(),
                    );
                }
                Some(n) => n as usize,
            };
            let (keys, args) = args.split_at(count);
            // The callback borrows this owner for the entire VM lifetime. All
            // calls share the outer ready list; no intermediate waiter service,
            // cancellation, or other client's operation can run inside EVAL.
            // script_command forbids nested EVAL and transaction controls.
            return crate::scripting::execute(&source, keys, args, |request| {
                let command = script_command(request);
                match command {
                    QueuedCommand::Reply(reply) => reply,
                    QueuedCommand::Database(command) => {
                        self.execute_atomic(database, command, ready)
                    }
                }
            });
        }
        let pushed_key = match &command {
            DatabaseCommand::Push { key, .. } => Some(key.clone()),
            _ => None,
        };
        // Databases::execute never registers waits. Batch blocking pops make
        // one immediate attempt and return null if no item is available.
        let result = self.databases.execute(database, command);
        if let Some(key) = pushed_key
            && matches!(result, Reply::Integer(_))
        {
            ready.push(key);
        }
        result
    }

    fn register(&mut self, waiter: Waiter) {
        let id = self.next_id;
        self.next_id = id.checked_add(1).expect("waiter ID exhausted");
        let watching = &mut self.watching[usize::from(waiter.database.0)];
        for key in &waiter.pop.keys {
            watching.entry(key.clone()).or_default().insert(id);
        }
        if let Some(deadline) = waiter.pop.deadline {
            self.deadlines.insert((deadline, id));
        }
        self.waiters.insert(id, waiter);
        #[cfg(test)]
        self.report_waiters();
    }

    fn reap_closed(&mut self) {
        // Disconnects are rare relative to commands, and the network layer caps
        // pending clients at 256. A coalesced O(W) sweep avoids extra request IDs,
        // cleanup channels, and cancellation tasks. Readiness still uses indexes.
        let closed: Vec<_> = self
            .waiters
            .iter()
            .filter(|(_, waiter)| waiter.reply.is_closed())
            .map(|(&id, _)| id)
            .collect();
        for id in closed {
            self.remove(id);
        }
    }

    #[cfg(test)]
    fn report_waiters(&self) {
        if let Some(count) = &self.waiter_count {
            count.send_replace(self.waiters.len());
        }
    }

    /// Every terminal path removes all indexes before delivering an outcome.
    /// No tombstones accumulate after successful pops or expired deadlines.
    fn remove(&mut self, id: u64) -> Waiter {
        let waiter = self.waiters.remove(&id).expect("registered waiter");
        let watching = &mut self.watching[usize::from(waiter.database.0)];
        for key in &waiter.pop.keys {
            let ids = watching.get_mut(key).expect("registered key");
            ids.remove(&id);
            if ids.is_empty() {
                watching.remove(key);
            }
        }
        if let Some(deadline) = waiter.pop.deadline {
            self.deadlines.remove(&(deadline, id));
        }
        #[cfg(test)]
        self.report_waiters();
        waiter
    }

    fn expire(&mut self, now: Instant) {
        while let Some(&(deadline, id)) = self.deadlines.first() {
            if deadline > now {
                break;
            }
            let waiter = self.remove(id);
            let _ = waiter.reply.send(Reply::Array(None));
        }
    }

    fn serve_ready(&mut self, database: DatabaseIndex, keys: &[Vec<u8>]) {
        let index = usize::from(database.0);
        let watching = &self.watching[index];
        let candidates: BTreeSet<u64> = keys
            .iter()
            // Only committed, nonempty lists are ready. A batch may have
            // pushed then emptied/replaced a key before this service boundary.
            .filter(|key| match self.databases.0[index].get(*key) {
                Some(Value::List(list)) => !list.is_empty(),
                _ => false,
            })
            .filter_map(|key| watching.get(key))
            .flat_map(|ids| ids.iter().copied())
            .collect();
        for id in candidates {
            let waiter = self.waiters.get(&id).expect("indexed waiter");
            if waiter.reply.is_closed() {
                self.remove(id);
            } else if let Some(result) = self.databases.pop(database, &waiter.pop) {
                let waiter = self.remove(id);
                let _ = waiter.reply.send(result);
            }
            // Empty rechecks retain the original priority and deadline.
        }
    }
}
