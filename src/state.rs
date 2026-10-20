//! One dedicated thread owns all databases. No state reference crosses a channel.

use std::collections::HashMap;
use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::thread;

use tokio::sync::{mpsc, oneshot};

use crate::command::DatabaseCommand;
use crate::resp::Reply;

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

#[derive(Default)]
struct Databases([HashMap<Vec<u8>, Vec<u8>>; DATABASES]);

impl Databases {
    fn execute(&mut self, database: DatabaseIndex, command: DatabaseCommand) -> Reply {
        let entries = &mut self.0[usize::from(database.0)];
        // This entire match is one non-interleaved operation: its lookup/insert
        // (or full EXISTS scan) is the linearization point. Replies own bytes;
        // sending/dropping one cannot retain or undo database access.
        match command {
            DatabaseCommand::Get(key) => Reply::Bulk(entries.get(&key).cloned()),
            DatabaseCommand::Set { key, value } => {
                entries.insert(key, value);
                Reply::Simple(b"OK".to_vec())
            }
            DatabaseCommand::Exists(keys) => Reply::Integer(
                keys.iter()
                    .map(|key| i64::from(entries.contains_key(key)))
                    .sum(),
            ),
        }
    }
}

struct Work {
    database: DatabaseIndex,
    command: DatabaseCommand,
    reply: oneshot::Sender<Reply>,
}

#[derive(Clone)]
pub(crate) struct Client {
    work: mpsc::Sender<Work>,
}

impl Client {
    pub(crate) async fn execute(
        &self,
        database: DatabaseIndex,
        command: DatabaseCommand,
    ) -> io::Result<Reply> {
        let (reply, receive) = oneshot::channel();
        self.work
            .send(Work {
                database,
                command,
                reply,
            })
            .await
            .map_err(|_| io::Error::other("state owner work channel closed"))?;
        receive
            .await
            .map_err(|_| io::Error::other("state owner reply channel closed"))
    }
}

pub(crate) type Completion = oneshot::Receiver<io::Result<()>>;

pub(crate) fn spawn() -> io::Result<(Client, Completion)> {
    spawn_with(run)
}

fn spawn_with(
    run: impl FnOnce(mpsc::Receiver<Work>) + Send + 'static,
) -> io::Result<(Client, Completion)> {
    let (work, receive) = mpsc::channel(WORK_CAPACITY);
    let (finished, completion) = oneshot::channel();
    thread::Builder::new()
        .name("keyper-state".into())
        .spawn(move || {
            // Report panic only to fail-stop supervision, never to reset state
            // or turn uncertain execution into a normal client error.
            let outcome = catch_unwind(AssertUnwindSafe(|| run(receive)))
                .map_err(|_| io::Error::other("state owner panicked; service must stop"));
            let _ = finished.send(outcome);
        })
        .map_err(|error| io::Error::new(error.kind(), format!("start state owner: {error}")))?;
    Ok((Client { work }, completion))
}

fn run(mut work: mpsc::Receiver<Work>) {
    let mut databases = Databases::default();
    // Only this OS thread blocks. Tokio tasks use asynchronous bounded sends.
    // Dropping all clients closes the queue; remaining unstarted/canceled work
    // is skipped, and an already-started operation is always allowed to finish.
    while let Some(work) = work.blocking_recv() {
        if !work.reply.is_closed() {
            let reply = databases.execute(work.database, work.command);
            let _ = work.reply.send(reply); // Lost reply is not rollback/retry.
        }
    }
}

#[cfg(test)]
pub(crate) fn failing_owner(fail: oneshot::Receiver<()>) -> io::Result<(Client, Completion)> {
    spawn_with(move |_work| {
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

    #[tokio::test]
    async fn full_work_queue_backpressures_without_blocking_tokio() {
        let (release, gate) = oneshot::channel();
        let (client, done) = spawn_with(move |work| {
            gate.blocking_recv().unwrap();
            run(work);
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
                        command: DatabaseCommand::Get(vec![]),
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
    async fn dropped_reply_before_execution_skips_work_and_after_execution_keeps_writes() {
        // Queue before starting the owner: deterministic cancellation, no sleep.
        let (send, receive) = mpsc::channel(WORK_CAPACITY);
        let db = DatabaseIndex::default();
        let (reply, canceled) = oneshot::channel();
        send.send(Work {
            database: db,
            command: DatabaseCommand::Set {
                key: b"key".to_vec(),
                value: b"skipped".to_vec(),
            },
            reply,
        })
        .await
        .unwrap();
        drop(canceled);
        let (reply, applied) = oneshot::channel();
        send.send(Work {
            database: db,
            command: DatabaseCommand::Set {
                key: b"other".to_vec(),
                value: b"kept".to_vec(),
            },
            reply,
        })
        .await
        .unwrap();
        let (reply, skipped_value) = oneshot::channel();
        send.send(Work {
            database: db,
            command: DatabaseCommand::Get(b"key".to_vec()),
            reply,
        })
        .await
        .unwrap();
        let (reply, kept_value) = oneshot::channel();
        send.send(Work {
            database: db,
            command: DatabaseCommand::Get(b"other".to_vec()),
            reply,
        })
        .await
        .unwrap();
        let (finished, done) = oneshot::channel();
        thread::spawn(move || {
            run(receive);
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
            command: DatabaseCommand::Get(b"other".to_vec()),
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
