use super::*;
use crate::state::{Client, WORK_CAPACITY, spawn};
use tokio::sync::oneshot::error::TryRecvError;
use tokio::time::{advance, timeout};

const LIMIT: Duration = Duration::from_secs(5);
const DB: DatabaseIndex = DatabaseIndex(0);

fn pop(end: ListEnd, keys: &[&[u8]], timeout: &[u8]) -> DatabaseCommand {
    DatabaseCommand::Pop {
        end,
        keys: keys.iter().map(|key| key.to_vec()).collect(),
        timeout: timeout.to_vec(),
    }
}

fn push(end: ListEnd, key: &[u8], values: &[&[u8]]) -> DatabaseCommand {
    DatabaseCommand::Push {
        end,
        key: key.to_vec(),
        values: values.iter().map(|value| value.to_vec()).collect(),
    }
}

fn pair(key: &[u8], value: &[u8]) -> Reply {
    Reply::Array(Some(vec![
        Reply::Bulk(Some(key.to_vec())),
        Reply::Bulk(Some(value.to_vec())),
    ]))
}

fn send(
    owner: &mut Owner,
    db: DatabaseIndex,
    command: DatabaseCommand,
    now: Instant,
) -> oneshot::Receiver<Reply> {
    let (reply, receive) = oneshot::channel();
    owner.process(
        Work {
            database: db,
            command: Operation::Single(command),
            reply,
        },
        now,
    );
    receive
}

fn execute(owner: &mut Owner, db: DatabaseIndex, command: DatabaseCommand, now: Instant) -> Reply {
    send(owner, db, command, now)
        .try_recv()
        .expect("immediate reply")
}

fn pending(receive: &mut oneshot::Receiver<Reply>) {
    assert_eq!(receive.try_recv(), Err(TryRecvError::Empty));
}

fn empty_indexes(owner: &Owner) {
    assert!(owner.waiters.is_empty());
    assert!(owner.deadlines.is_empty());
    assert!(owner.watching.iter().all(HashMap::is_empty));
}

#[test]
fn list_reads_and_empty_lpop_do_not_wake_or_reorder_waiters_and_cannot_steal_a_push() {
    let now = Instant::now();
    let mut owner = Owner::default();
    let mut first = send(&mut owner, DB, pop(ListEnd::Left, &[b"list"], b"0"), now);
    let mut second = send(&mut owner, DB, pop(ListEnd::Right, &[b"list"], b"0"), now);
    for (command, expected) in [
        (
            DatabaseCommand::Type(b"list".to_vec()),
            Reply::Simple(b"none".to_vec()),
        ),
        (
            DatabaseCommand::ListLength(b"list".to_vec()),
            Reply::Integer(0),
        ),
        (
            DatabaseCommand::ListIndex {
                key: b"list".to_vec(),
                index: b"-1".to_vec(),
            },
            Reply::Bulk(None),
        ),
        (
            DatabaseCommand::ListPop {
                key: b"list".to_vec(),
                count: None,
            },
            Reply::Bulk(None),
        ),
        (
            DatabaseCommand::ListPop {
                key: b"list".to_vec(),
                count: Some(b"0".to_vec()),
            },
            Reply::Array(None),
        ),
    ] {
        assert_eq!(execute(&mut owner, DB, command, now), expected);
        pending(&mut first);
        pending(&mut second);
        assert_eq!(owner.waiters.len(), 2);
    }
    assert_eq!(
        execute(
            &mut owner,
            DB,
            push(ListEnd::Right, b"list", &[b"a", b"b", b"c", b"d"]),
            now
        ),
        Reply::Integer(4)
    );
    // Prior waiters are subsequent owner work before another client's LPOP.
    assert_eq!(first.try_recv(), Ok(pair(b"list", b"a")));
    assert_eq!(second.try_recv(), Ok(pair(b"list", b"d")));
    assert_eq!(
        execute(
            &mut owner,
            DB,
            DatabaseCommand::ListPop {
                key: b"list".to_vec(),
                count: Some(b"2".to_vec())
            },
            now
        ),
        Reply::Array(Some(vec![
            Reply::Bulk(Some(b"b".to_vec())),
            Reply::Bulk(Some(b"c".to_vec()))
        ]))
    );
    empty_indexes(&owner);
    assert!(owner.databases.0[0].is_empty());
}

#[test]
fn hash_creation_and_deletion_do_not_wake_waiters_but_later_list_activity_rechecks_types() {
    let now = Instant::now();
    let mut owner = Owner::default();
    let mut waiting = send(
        &mut owner,
        DB,
        pop(ListEnd::Left, &[b"h", b"ready"], b"0"),
        now,
    );
    assert_eq!(
        execute(
            &mut owner,
            DB,
            DatabaseCommand::HashSet {
                key: b"h".to_vec(),
                pairs: vec![(b"f".to_vec(), b"v".to_vec())],
            },
            now
        ),
        Reply::Integer(1)
    );
    pending(&mut waiting);
    assert_eq!(
        execute(
            &mut owner,
            DB,
            DatabaseCommand::HashDelete {
                key: b"h".to_vec(),
                fields: vec![b"f".to_vec()],
            },
            now
        ),
        Reply::Integer(1)
    );
    pending(&mut waiting);
    execute(
        &mut owner,
        DB,
        DatabaseCommand::HashSet {
            key: b"h".to_vec(),
            pairs: vec![(b"f".to_vec(), b"v".to_vec())],
        },
        now,
    );
    pending(&mut waiting);
    execute(
        &mut owner,
        DB,
        push(ListEnd::Right, b"ready", &[b"item"]),
        now,
    );
    assert_eq!(waiting.try_recv(), Ok(wrong_type()));
    empty_indexes(&owner);
    // The later ready list was not consumed past the earlier wrong-type hash.
    assert_eq!(
        execute(
            &mut owner,
            DB,
            pop(ListEnd::Left, &[b"ready", b"h"], b"0"),
            now
        ),
        pair(b"ready", b"item")
    );
}

#[test]
fn double_timeouts_are_checked_and_positive_fractions_round_up() {
    let now = Instant::now();
    for text in ["0", "-0", "+0", "0.000", "-0.0e-9999", "0e9999"] {
        assert_eq!(deadline(text.as_bytes(), now), Ok(None), "{text}");
    }
    for (text, millis) in [
        ("0.000000001", 1),
        ("5e-324", 1),
        ("0.0009", 1),
        (".001", 1),
        ("0.0011", 2),
        ("1.0001", 1001),
        ("+1.25e-1", 125),
        ("1e1", 10_000),
    ] {
        assert_eq!(
            deadline(text.as_bytes(), now),
            Ok(Some(now + Duration::from_millis(millis))),
            "{text}"
        );
    }
    for text in [
        &b""[..],
        b" ",
        b" 1",
        b"1 ",
        b"1\n",
        b"\xff",
        b"1\0",
        b"1.2.3",
        b"1e",
        b"--0",
        b"NaN",
        b"inf",
        b"-inf",
        b"Infinity",
        b"-1",
        b"-0.0001",
        b"1e999",
        b"1e-999",
        b"-1e-999",
        b"1e308",
        b"9223372036854776",
    ] {
        assert!(
            matches!(deadline(text, now), Err(Reply::Error(_))),
            "{text:?}"
        );
    }
    // Validate the platform Instant range too, not only duration conversion.
    let (mut low, mut high) = (0, u64::MAX);
    while high - low > 1 {
        let middle = low + (high - low) / 2;
        if now.checked_add(Duration::from_secs(middle)).is_some() {
            low = middle;
        } else {
            high = middle;
        }
    }
    let far = now.checked_add(Duration::from_secs(low)).unwrap();
    assert!(deadline(b"2", far).is_err());
}

#[test]
fn immediate_both_ends_preserve_binary_values_and_delete_empty_lists() {
    let now = Instant::now();
    for push_end in [ListEnd::Left, ListEnd::Right] {
        for pop_end in [ListEnd::Left, ListEnd::Right] {
            let mut owner = Owner::default();
            let key = b"\0\xff\r\n";
            assert_eq!(
                execute(
                    &mut owner,
                    DB,
                    push(push_end, key, &[b"", b"\0\xff", b"last"]),
                    now
                ),
                Reply::Integer(3)
            );
            let expected: &[&[u8]] = if push_end == pop_end {
                &[b"last", b"\0\xff", b""]
            } else {
                &[b"", b"\0\xff", b"last"]
            };
            for value in expected {
                assert_eq!(
                    execute(&mut owner, DB, pop(pop_end, &[key], b"0"), now),
                    pair(key, value)
                );
            }
            assert_eq!(
                execute(
                    &mut owner,
                    DB,
                    DatabaseCommand::Exists(vec![key.to_vec()]),
                    now
                ),
                Reply::Integer(0)
            );
            empty_indexes(&owner);
        }
    }
}

#[test]
fn one_to_five_arguments_and_duplicate_keys_count_before_deduplication() {
    let now = Instant::now();
    let mut owner = Owner::default();
    for count in 1..=5 {
        execute(&mut owner, DB, push(ListEnd::Right, b"k", &[b"v"]), now);
        assert_eq!(
            execute(
                &mut owner,
                DB,
                pop(ListEnd::Left, &vec![&b"k"[..]; count], b"0"),
                now
            ),
            pair(b"k", b"v")
        );
    }
    for count in [0, 6] {
        execute(&mut owner, DB, push(ListEnd::Right, b"k", &[b"kept"]), now);
        assert!(matches!(
            execute(
                &mut owner,
                DB,
                pop(ListEnd::Left, &vec![&b"k"[..]; count], b"0"),
                now
            ),
            Reply::Error(_)
        ));
        assert_eq!(
            execute(&mut owner, DB, pop(ListEnd::Left, &[b"k"], b"0"), now),
            pair(b"k", b"kept")
        );
    }
    let mut waiting = send(
        &mut owner,
        DB,
        pop(ListEnd::Left, &[b"k", b"k", b"k"], b"1"),
        now,
    );
    pending(&mut waiting);
    assert_eq!(owner.watching[0][&b"k"[..]].len(), 1);
    assert_eq!(owner.waiters[&0].pop.keys, vec![b"k".to_vec()]);
    execute(
        &mut owner,
        DB,
        push(ListEnd::Right, b"k", &[b"once", b"left"]),
        now,
    );
    assert_eq!(waiting.try_recv().unwrap(), pair(b"k", b"once"));
    assert_eq!(
        execute(&mut owner, DB, pop(ListEnd::Left, &[b"k"], b"0"), now),
        pair(b"k", b"left")
    );
    empty_indexes(&owner);
}

#[test]
fn invalid_timeouts_are_rejected_even_when_an_item_is_available() {
    let now = Instant::now();
    let mut owner = Owner::default();
    execute(&mut owner, DB, push(ListEnd::Right, b"k", &[b"kept"]), now);
    for invalid in [&b"-1"[..], b"NaN", b"1e-999", b"1e999", b"bad"] {
        assert!(matches!(
            execute(&mut owner, DB, pop(ListEnd::Left, &[b"k"], invalid), now),
            Reply::Error(_)
        ));
        empty_indexes(&owner);
    }
    assert_eq!(
        execute(&mut owner, DB, pop(ListEnd::Left, &[b"k"], b"0"), now),
        pair(b"k", b"kept")
    );
}

#[test]
fn immediate_keys_stop_at_first_item_or_encountered_wrong_type() {
    let now = Instant::now();
    let mut owner = Owner::default();
    execute(
        &mut owner,
        DB,
        DatabaseCommand::Set {
            key: b"string".to_vec(),
            value: b"v".to_vec(),
        },
        now,
    );
    execute(
        &mut owner,
        DB,
        push(ListEnd::Right, b"a", &[b"a1", b"a2"]),
        now,
    );
    execute(&mut owner, DB, push(ListEnd::Right, b"b", &[b"b1"]), now);
    assert_eq!(
        execute(
            &mut owner,
            DB,
            pop(ListEnd::Right, &[b"absent", b"a", b"string", b"b"], b"1"),
            now
        ),
        pair(b"a", b"a2")
    );
    assert_eq!(
        execute(
            &mut owner,
            DB,
            pop(ListEnd::Left, &[b"absent", b"string", b"a"], b"0"),
            now
        ),
        wrong_type()
    );
    assert_eq!(
        execute(&mut owner, DB, pop(ListEnd::Left, &[b"a", b"b"], b"0"), now),
        pair(b"a", b"a1")
    );
    assert_eq!(
        execute(&mut owner, DB, pop(ListEnd::Left, &[b"b"], b"0"), now),
        pair(b"b", b"b1")
    );
    empty_indexes(&owner);
}

#[test]
fn whole_variadic_push_precedes_fifo_pops_and_response_length() {
    let now = Instant::now();
    for push_end in [ListEnd::Left, ListEnd::Right] {
        let mut owner = Owner::default();
        let mut first = send(&mut owner, DB, pop(ListEnd::Left, &[b"k"], b"0"), now);
        let mut second = send(&mut owner, DB, pop(ListEnd::Right, &[b"k"], b"0"), now);
        pending(&mut first);
        pending(&mut second);
        assert_eq!(
            execute(
                &mut owner,
                DB,
                push(push_end, b"k", &[b"a", b"b", b"c"]),
                now
            ),
            Reply::Integer(3)
        );
        let (left, right) = if push_end == ListEnd::Left {
            (b"c", b"a")
        } else {
            (b"a", b"c")
        };
        assert_eq!(first.try_recv().unwrap(), pair(b"k", left));
        assert_eq!(second.try_recv().unwrap(), pair(b"k", right));
        assert_eq!(
            execute(&mut owner, DB, pop(ListEnd::Left, &[b"k"], b"0"), now),
            pair(b"k", b"b")
        );
        empty_indexes(&owner);
    }
}

#[test]
fn overlapping_waiters_recheck_in_order_and_reentry_loses_priority() {
    let now = Instant::now();
    let mut owner = Owner::default();
    let mut first = send(&mut owner, DB, pop(ListEnd::Left, &[b"a", b"b"], b"0"), now);
    let mut second = send(&mut owner, DB, pop(ListEnd::Left, &[b"b", b"c"], b"0"), now);
    execute(&mut owner, DB, push(ListEnd::Right, b"b", &[b"first"]), now);
    assert_eq!(first.try_recv().unwrap(), pair(b"b", b"first"));
    pending(&mut second);
    let mut again = send(&mut owner, DB, pop(ListEnd::Left, &[b"b"], b"0"), now);
    execute(
        &mut owner,
        DB,
        push(ListEnd::Right, b"b", &[b"second", b"again"]),
        now,
    );
    assert_eq!(second.try_recv().unwrap(), pair(b"b", b"second"));
    assert_eq!(again.try_recv().unwrap(), pair(b"b", b"again"));
    empty_indexes(&owner);
}

#[test]
fn readiness_rechecks_original_keys_not_signal_order_and_ignores_intermediate_items() {
    let now = Instant::now();
    let mut owner = Owner::default();
    let mut waiting = send(&mut owner, DB, pop(ListEnd::Left, &[b"a", b"b"], b"1"), now);
    // Simulate a complete owner unit. Actual EXEC/EVAL integration is later.
    owner
        .databases
        .execute(DB, push(ListEnd::Right, b"b", &[b"b"]));
    owner
        .databases
        .execute(DB, push(ListEnd::Right, b"a", &[b"a"]));
    owner.serve_ready(DB, &[b"b".to_vec(), b"a".to_vec()]);
    assert_eq!(waiting.try_recv().unwrap(), pair(b"a", b"a"));
    assert_eq!(
        execute(&mut owner, DB, pop(ListEnd::Left, &[b"b"], b"0"), now),
        pair(b"b", b"b")
    );
    let mut waiting = send(&mut owner, DB, pop(ListEnd::Left, &[b"b"], b"1"), now);
    owner
        .databases
        .execute(DB, push(ListEnd::Right, b"b", &[b"hidden"]));
    owner
        .databases
        .execute(DB, pop(ListEnd::Right, &[b"b"], b"0"));
    owner.serve_ready(DB, &[b"b".to_vec()]);
    pending(&mut waiting);
    assert_eq!(owner.waiters.len(), 1);
    owner
        .databases
        .execute(DB, push(ListEnd::Right, b"b", &[b"also hidden"]));
    owner.databases.execute(
        DB,
        DatabaseCommand::Set {
            key: b"b".to_vec(),
            value: b"replacement".to_vec(),
        },
    );
    owner.serve_ready(DB, &[b"b".to_vec()]);
    pending(&mut waiting);
    owner.databases.0[0].remove(&b"b"[..]);
    execute(
        &mut owner,
        DB,
        push(ListEnd::Right, b"b", &[b"visible"]),
        now,
    );
    assert_eq!(waiting.try_recv().unwrap(), pair(b"b", b"visible"));
    empty_indexes(&owner);
}

#[test]
fn string_replacement_does_not_wake_but_later_list_readiness_rechecks_types() {
    let now = Instant::now();
    let mut owner = Owner::default();
    let mut waiting = send(&mut owner, DB, pop(ListEnd::Left, &[b"a", b"b"], b"1"), now);
    execute(
        &mut owner,
        DB,
        DatabaseCommand::Set {
            key: b"a".to_vec(),
            value: b"v".to_vec(),
        },
        now,
    );
    pending(&mut waiting);
    assert_eq!(
        execute(&mut owner, DB, push(ListEnd::Right, b"a", &[b"bad"]), now),
        wrong_type()
    );
    pending(&mut waiting);
    execute(&mut owner, DB, push(ListEnd::Right, b"b", &[b"kept"]), now);
    assert_eq!(waiting.try_recv().unwrap(), wrong_type());
    assert_eq!(
        execute(&mut owner, DB, pop(ListEnd::Right, &[b"b"], b"0"), now),
        pair(b"b", b"kept")
    );
    empty_indexes(&owner);
}

#[test]
fn deadlines_never_fire_early_or_renew_after_empty_rechecks() {
    let now = Instant::now();
    let mut owner = Owner::default();
    let mut waiting = send(
        &mut owner,
        DB,
        pop(ListEnd::Left, &[b"a", b"b"], b"0.0011"),
        now,
    );
    let expected = now + Duration::from_millis(2);
    assert_eq!(owner.deadlines.first().unwrap().0, expected);
    for _ in 0..3 {
        owner.serve_ready(DB, &[b"b".to_vec()]);
        pending(&mut waiting);
        assert_eq!(owner.deadlines.first().unwrap().0, expected);
        assert_eq!(owner.waiters.first_key_value().unwrap().0, &0);
    }
    owner.expire(expected - Duration::from_nanos(1));
    pending(&mut waiting);
    owner.expire(expected);
    assert_eq!(waiting.try_recv().unwrap(), Reply::Array(None));
    empty_indexes(&owner);
    execute(
        &mut owner,
        DB,
        push(ListEnd::Right, b"a", &[b"later"]),
        expected,
    );
    owner.expire(expected + Duration::from_secs(100));
    assert_eq!(
        execute(&mut owner, DB, pop(ListEnd::Left, &[b"a"], b"0"), expected),
        pair(b"a", b"later")
    );
}

#[test]
fn losing_a_fifo_recheck_does_not_renew_deadline_or_priority() {
    let now = Instant::now();
    let mut owner = Owner::default();
    let mut first = send(&mut owner, DB, pop(ListEnd::Left, &[b"k"], b"0"), now);
    let mut second = send(&mut owner, DB, pop(ListEnd::Left, &[b"k"], b"1"), now);
    execute(
        &mut owner,
        DB,
        push(ListEnd::Right, b"k", &[b"first"]),
        now + Duration::from_millis(900),
    );
    assert_eq!(first.try_recv().unwrap(), pair(b"k", b"first"));
    pending(&mut second);
    assert_eq!(
        owner.deadlines.first(),
        Some(&(now + Duration::from_secs(1), 1))
    );
    assert_eq!(owner.waiters.first_key_value().unwrap().0, &1);
    owner.expire(now + Duration::from_secs(1));
    assert_eq!(second.try_recv().unwrap(), Reply::Array(None));
    empty_indexes(&owner);
}

#[test]
fn producer_and_timer_order_assigns_one_terminal_outcome() {
    let now = Instant::now();
    let end = now + Duration::from_secs(1);
    for timer_first in [false, true] {
        let mut owner = Owner::default();
        let mut waiting = send(&mut owner, DB, pop(ListEnd::Left, &[b"k"], b"1"), now);
        if timer_first {
            owner.expire(end);
        }
        execute(&mut owner, DB, push(ListEnd::Right, b"k", &[b"v"]), end);
        owner.expire(end);
        let expected = if timer_first {
            Reply::Array(None)
        } else {
            pair(b"k", b"v")
        };
        assert_eq!(waiting.try_recv().unwrap(), expected);
        assert_eq!(waiting.try_recv(), Err(TryRecvError::Closed));
        empty_indexes(&owner);
        let remaining = execute(
            &mut owner,
            DB,
            DatabaseCommand::Exists(vec![b"k".to_vec()]),
            end,
        );
        assert_eq!(remaining, Reply::Integer(i64::from(timer_first)));
    }
}

#[test]
fn independent_namespaces_and_equal_deadlines_cleanup_every_registration() {
    let now = Instant::now();
    let mut owner = Owner::default();
    let mut receivers = Vec::new();
    for index in 0..16 {
        let db = DatabaseIndex(index);
        receivers.push(send(
            &mut owner,
            db,
            pop(ListEnd::Left, &[b"a", b"b", b"c", b"d", b"e"], b"1"),
            now,
        ));
    }
    execute(
        &mut owner,
        DatabaseIndex(15),
        push(ListEnd::Right, b"e", &[b"only15"]),
        now,
    );
    assert_eq!(receivers[15].try_recv().unwrap(), pair(b"e", b"only15"));
    for receiver in &mut receivers[..15] {
        pending(receiver);
    }
    owner.expire(now + Duration::from_secs(1));
    for receiver in &mut receivers[..15] {
        assert_eq!(receiver.try_recv().unwrap(), Reply::Array(None));
    }
    empty_indexes(&owner);
}

#[test]
fn canceled_waiter_never_consumes_and_lost_committed_reply_never_restores() {
    let now = Instant::now();
    let mut owner = Owner::default();
    let canceled = send(&mut owner, DB, pop(ListEnd::Left, &[b"a", b"b"], b"1"), now);
    drop(canceled);
    let applied = send(&mut owner, DB, pop(ListEnd::Left, &[b"a", b"b"], b"1"), now);
    execute(
        &mut owner,
        DB,
        push(ListEnd::Right, b"b", &[b"applied", b"kept"]),
        now,
    );
    drop(applied);
    empty_indexes(&owner);
    assert_eq!(
        execute(&mut owner, DB, pop(ListEnd::Left, &[b"b"], b"0"), now),
        pair(b"b", b"kept")
    );
}

#[test]
fn fifo_item_accounting_across_many_waiters_and_push_batches() {
    let now = Instant::now();
    let mut owner = Owner::default();
    let mut waits = (0..64)
        .map(|_| {
            send(
                &mut owner,
                DB,
                pop(ListEnd::Left, &[b"a", b"k", b"k"], b"1"),
                now,
            )
        })
        .collect::<Vec<_>>();
    for batch in 0..8 {
        let values = (batch * 8..batch * 8 + 8)
            .map(|n| n.to_string().into_bytes())
            .collect::<Vec<_>>();
        assert_eq!(
            execute(
                &mut owner,
                DB,
                DatabaseCommand::Push {
                    end: ListEnd::Right,
                    key: b"k".to_vec(),
                    values
                },
                now
            ),
            Reply::Integer(8)
        );
    }
    for (n, receiver) in waits.iter_mut().enumerate() {
        assert_eq!(
            receiver.try_recv().unwrap(),
            pair(b"k", n.to_string().as_bytes())
        );
    }
    empty_indexes(&owner);
    assert_eq!(
        execute(
            &mut owner,
            DB,
            DatabaseCommand::Exists(vec![b"k".to_vec()]),
            now
        ),
        Reply::Integer(0)
    );
}

async fn enqueue(client: &Client, command: DatabaseCommand) -> oneshot::Receiver<Reply> {
    let (reply, receive) = oneshot::channel();
    timeout(
        LIMIT,
        client.work.send(Work {
            database: DB,
            command: Operation::Single(command),
            reply,
        }),
    )
    .await
    .unwrap()
    .unwrap();
    receive
}

async fn fence(client: &Client) {
    assert_eq!(
        timeout(
            LIMIT,
            client.execute(DB, DatabaseCommand::Get(b"fence".to_vec()))
        )
        .await
        .unwrap()
        .unwrap(),
        Reply::Bulk(None)
    );
}

#[tokio::test(start_paused = true)]
async fn owner_timer_driver_expires_without_other_work_and_retains_indefinite_waits() {
    let (work, receive) = mpsc::channel(WORK_CAPACITY);
    let canceled = Arc::new(Notify::new());
    let client = Client {
        work,
        canceled: canceled.clone(),
    };
    let owner = tokio::spawn(Owner::default().run(receive, canceled));
    let mut finite = enqueue(&client, pop(ListEnd::Left, &[b"k"], b"0.0011")).await;
    let mut indefinite = enqueue(&client, pop(ListEnd::Right, &[b"forever"], b"-0")).await;
    fence(&client).await;
    advance(Duration::from_millis(1)).await;
    pending(&mut finite);
    pending(&mut indefinite);
    advance(Duration::from_millis(1)).await;
    assert_eq!(
        timeout(LIMIT, &mut finite).await.unwrap().unwrap(),
        Reply::Array(None)
    );
    advance(Duration::from_secs(1000)).await;
    pending(&mut indefinite);
    let push_reply = timeout(
        LIMIT,
        client.execute(DB, push(ListEnd::Right, b"forever", &[b"v"])),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(push_reply, Reply::Integer(1));
    assert_eq!(
        timeout(LIMIT, indefinite).await.unwrap().unwrap(),
        pair(b"forever", b"v")
    );
    drop(client);
    timeout(LIMIT, owner).await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn new_earliest_timer_rearms_and_popped_deadlines_do_not_fire_again() {
    let (work, receive) = mpsc::channel(WORK_CAPACITY);
    let canceled = Arc::new(Notify::new());
    let client = Client {
        work,
        canceled: canceled.clone(),
    };
    let owner = tokio::spawn(Owner::default().run(receive, canceled));
    let mut long = enqueue(&client, pop(ListEnd::Left, &[b"long"], b"10")).await;
    fence(&client).await;
    advance(Duration::from_secs(1)).await;
    let mut short = enqueue(&client, pop(ListEnd::Right, &[b"short"], b"1")).await;
    fence(&client).await;
    advance(Duration::from_secs(1)).await;
    assert_eq!(
        timeout(LIMIT, &mut short).await.unwrap().unwrap(),
        Reply::Array(None)
    );
    pending(&mut long);
    assert_eq!(
        timeout(
            LIMIT,
            client.execute(DB, push(ListEnd::Right, b"long", &[b"v"]))
        )
        .await
        .unwrap()
        .unwrap(),
        Reply::Integer(1)
    );
    assert_eq!(
        timeout(LIMIT, &mut long).await.unwrap().unwrap(),
        pair(b"long", b"v")
    );
    advance(Duration::from_secs(20)).await;
    let mut fresh = enqueue(&client, pop(ListEnd::Left, &[b"long"], b"0")).await;
    fence(&client).await;
    pending(&mut fresh);
    drop(client);
    timeout(LIMIT, owner).await.unwrap().unwrap();
    assert!(timeout(LIMIT, fresh).await.unwrap().is_err());
}

#[tokio::test]
async fn dedicated_owner_registers_without_lost_wakeup_and_shuts_down_with_pending_waits() {
    let (client, done) = spawn().unwrap();
    let mut waits = Vec::new();
    for _ in 0..32 {
        // FIFO channel order makes the next push a producer directly after the
        // empty check/registration boundary; no scheduling sleeps are needed.
        waits.push(enqueue(&client, pop(ListEnd::Left, &[b"k"], b"0")).await);
    }
    let values = (0..32).map(|n| n.to_string().into_bytes()).collect();
    assert_eq!(
        timeout(
            LIMIT,
            client.execute(
                DB,
                DatabaseCommand::Push {
                    end: ListEnd::Right,
                    key: b"k".to_vec(),
                    values
                }
            )
        )
        .await
        .unwrap()
        .unwrap(),
        Reply::Integer(32)
    );
    for (n, waiter) in waits.into_iter().enumerate() {
        assert_eq!(
            timeout(LIMIT, waiter).await.unwrap().unwrap(),
            pair(b"k", n.to_string().as_bytes())
        );
    }
    let pending = enqueue(&client, pop(ListEnd::Right, &[b"k"], b"0")).await;
    fence(&client).await;
    drop(client);
    timeout(LIMIT, done).await.unwrap().unwrap().unwrap();
    assert!(timeout(LIMIT, pending).await.unwrap().is_err());
}

#[tokio::test]
async fn dedicated_owner_finite_timeout_uses_a_real_monotonic_deadline() {
    let (client, done) = spawn().unwrap();
    let start = Instant::now();
    assert_eq!(
        timeout(
            LIMIT,
            client.execute(DB, pop(ListEnd::Left, &[b"k"], b"0.0011"))
        )
        .await
        .unwrap()
        .unwrap(),
        Reply::Array(None)
    );
    assert!(start.elapsed() >= Duration::from_millis(2));
    drop(client);
    timeout(LIMIT, done).await.unwrap().unwrap().unwrap();
}

#[test]
fn cancellation_sweep_removes_every_key_and_deadline_index() {
    let mut owner = Owner::default();
    let now = Instant::now();
    let mut canceled = Vec::new();
    for timeout in [&b"0"[..], b"5.5"] {
        for keys in [
            vec![&b"a"[..], b"b", b"c", b"d", b"e"],
            vec![&b"a"[..], b"a", b"a", b"a", b"a"],
        ] {
            canceled.push(send(
                &mut owner,
                DB,
                pop(ListEnd::Left, &keys, timeout),
                now,
            ));
        }
    }
    let mut live = send(
        &mut owner,
        DatabaseIndex(15),
        pop(ListEnd::Right, &[b"other"], b"3"),
        now,
    );
    drop(canceled);
    owner.reap_closed();
    assert_eq!(owner.waiters.len(), 1);
    assert_eq!(owner.deadlines.len(), 1);
    assert!(owner.watching[0].is_empty());
    pending(&mut live);
    drop(live);
    owner.reap_closed();
    empty_indexes(&owner);
    owner.reap_closed(); // Repeated notifications are harmless.
    owner.expire(now + Duration::from_secs(100));
    empty_indexes(&owner);
}

#[test]
fn set_creation_and_empty_deletion_do_not_wake_waiters_but_list_readiness_does() {
    let mut owner = Owner::default();
    let now = Instant::now();
    let mut waiting = send(
        &mut owner,
        DB,
        pop(ListEnd::Left, &[b"set", b"list"], b"0"),
        now,
    );
    assert_eq!(
        execute(
            &mut owner,
            DB,
            DatabaseCommand::SetAdd {
                key: b"set".to_vec(),
                members: vec![b"member".to_vec()]
            },
            now
        ),
        Reply::Integer(1)
    );
    pending(&mut waiting);
    // List activity makes the waiter recheck its keys; the earlier set is a
    // wrong type, not a usable list, and the pushed item must remain untouched.
    assert_eq!(
        execute(&mut owner, DB, push(ListEnd::Right, b"list", &[b"v"]), now),
        Reply::Integer(1)
    );
    assert_eq!(waiting.try_recv().unwrap(), wrong_type());
    empty_indexes(&owner);
    assert_eq!(
        execute(
            &mut owner,
            DB,
            pop(ListEnd::Right, &[b"list", b"set"], b"0"),
            now
        ),
        pair(b"list", b"v")
    );
    assert_eq!(
        execute(
            &mut owner,
            DB,
            DatabaseCommand::SetRemove {
                key: b"set".to_vec(),
                members: vec![b"member".to_vec()]
            },
            now
        ),
        Reply::Integer(1)
    );
    let mut waiting = send(&mut owner, DB, pop(ListEnd::Right, &[b"set"], b"0"), now);
    execute(
        &mut owner,
        DB,
        DatabaseCommand::SetAdd {
            key: b"set".to_vec(),
            members: vec![b"member".to_vec()],
        },
        now,
    );
    execute(
        &mut owner,
        DB,
        DatabaseCommand::SetRemove {
            key: b"set".to_vec(),
            members: vec![b"member".to_vec()],
        },
        now,
    );
    pending(&mut waiting);
    execute(
        &mut owner,
        DB,
        push(ListEnd::Right, b"set", &[b"delivered"]),
        now,
    );
    assert_eq!(waiting.try_recv().unwrap(), pair(b"set", b"delivered"));
    empty_indexes(&owner);
}

fn atomic_lists(owner: &mut Owner, in_exec: bool, now: Instant) {
    let command = DatabaseCommand::Eval {
        source: b"redis.call('RPUSH','b','B'); redis.call('RPUSH','a','A'); return {redis.call('LLEN','a'),redis.call('LLEN','b')}".to_vec(),
        numkeys: b"0".to_vec(),
        args: Vec::new(),
    };
    let command = if in_exec {
        Operation::Transaction(vec![QueuedCommand::Database(command)])
    } else {
        Operation::Single(command)
    };
    let (reply, mut receive) = oneshot::channel();
    owner.process(
        Work {
            database: DB,
            command,
            reply,
        },
        now,
    );
    let lengths = Reply::Array(Some(vec![Reply::Integer(1), Reply::Integer(1)]));
    assert_eq!(
        receive.try_recv().unwrap(),
        if in_exec {
            Reply::Array(Some(vec![lengths]))
        } else {
            lengths
        }
    );
}

#[test]
fn script_and_exec_timeout_contenders_have_one_terminal_outcome_and_no_later_consumption() {
    for in_exec in [false, true] {
        for expiry_first in [false, true] {
            let now = Instant::now();
            let deadline = now + Duration::from_millis(1);
            let mut owner = Owner::default();
            let mut waiting = send(
                &mut owner,
                DB,
                pop(ListEnd::Left, &[b"a", b"b", b"a"], b"0.001"),
                now,
            );
            pending(&mut waiting);
            if expiry_first {
                owner.expire(deadline);
                assert_eq!(waiting.try_recv().unwrap(), Reply::Array(None));
            }
            // Explicit contender orders, not sleeping near a deadline. The
            // producer may finish after the deadline without early expiry or
            // an independent timer mutating state halfway through its script.
            atomic_lists(&mut owner, in_exec, deadline);
            if !expiry_first {
                assert_eq!(waiting.try_recv().unwrap(), pair(b"a", b"A"));
            }
            owner.expire(deadline + Duration::from_secs(10));
            empty_indexes(&owner);
            if expiry_first {
                assert_eq!(
                    execute(&mut owner, DB, pop(ListEnd::Left, &[b"a"], b"0"), deadline),
                    pair(b"a", b"A")
                );
            }
            assert_eq!(
                execute(&mut owner, DB, pop(ListEnd::Left, &[b"b"], b"0"), deadline),
                pair(b"b", b"B")
            );
            assert_eq!(
                execute(
                    &mut owner,
                    DB,
                    push(ListEnd::Right, b"a", &[b"later"]),
                    deadline
                ),
                Reply::Integer(1)
            );
            assert_eq!(
                execute(&mut owner, DB, pop(ListEnd::Left, &[b"a"], b"0"), deadline),
                pair(b"a", b"later")
            );
            empty_indexes(&owner);
        }
    }
}

#[test]
fn script_and_exec_disconnect_contenders_skip_closed_waiters_but_never_undo_committed_pops() {
    for in_exec in [false, true] {
        for disconnect_first in [false, true] {
            let now = Instant::now();
            let mut owner = Owner::default();
            let first = send(
                &mut owner,
                DB,
                pop(ListEnd::Left, &[b"a", b"b"], b"1000"),
                now,
            );
            let mut second = send(
                &mut owner,
                DB,
                pop(ListEnd::Left, &[b"a", b"b"], b"1000"),
                now,
            );
            // Option lets us drop the receiver on either side of commitment,
            // without guessing when a socket's EOF happens to be observed.
            let mut first = Some(first);
            if disconnect_first {
                drop(first.take());
            }
            atomic_lists(&mut owner, in_exec, now);
            drop(first.take()); // A queued reply can be lost, not requeued.
            assert_eq!(
                second.try_recv().unwrap(),
                if disconnect_first {
                    pair(b"a", b"A")
                } else {
                    pair(b"b", b"B")
                }
            );
            if disconnect_first {
                assert_eq!(
                    execute(&mut owner, DB, pop(ListEnd::Left, &[b"b"], b"0"), now),
                    pair(b"b", b"B")
                );
            }
            owner.reap_closed();
            owner.expire(now + Duration::from_secs(2000));
            empty_indexes(&owner);
            assert_eq!(
                execute(
                    &mut owner,
                    DB,
                    DatabaseCommand::Exists(vec![b"a".to_vec(), b"b".to_vec()]),
                    now
                ),
                Reply::Integer(0)
            );
            assert_eq!(
                execute(&mut owner, DB, push(ListEnd::Right, b"a", &[b"later"]), now),
                Reply::Integer(1)
            );
            assert_eq!(
                execute(&mut owner, DB, pop(ListEnd::Left, &[b"a"], b"0"), now),
                pair(b"a", b"later")
            );
            empty_indexes(&owner);
        }
    }
}

#[tokio::test]
async fn dropping_execute_notifies_a_dormant_owner_without_any_further_work() {
    let (client, done, mut count) = crate::state::observed().unwrap();
    for timeout_text in [&b"0"[..], b"1000"] {
        let mut execution = Box::pin(client.execute(
            DB,
            pop(ListEnd::Left, &[b"a", b"b", b"c", b"d", b"e"], timeout_text),
        ));
        tokio::select! {
            result = &mut execution => panic!("unexpected completion: {result:?}"),
            observed = timeout(LIMIT, count.wait_for(|&n| n == 1)) => { observed.unwrap().unwrap(); }
        }
        drop(execution);
        timeout(LIMIT, count.wait_for(|&n| n == 0))
            .await
            .unwrap()
            .unwrap();
    }
    drop(client);
    timeout(LIMIT, done).await.unwrap().unwrap().unwrap();
}
