use super::*;

const DB: DatabaseIndex = DatabaseIndex(0);

fn add(key: &[u8], members: &[&[u8]]) -> DatabaseCommand {
    DatabaseCommand::SetAdd {
        key: key.to_vec(),
        members: members.iter().map(|m| m.to_vec()).collect(),
    }
}

fn remove(key: &[u8], members: &[&[u8]]) -> DatabaseCommand {
    DatabaseCommand::SetRemove {
        key: key.to_vec(),
        members: members.iter().map(|m| m.to_vec()).collect(),
    }
}

fn members(databases: &mut Databases, db: DatabaseIndex, key: &[u8]) -> Vec<Vec<u8>> {
    let Reply::Array(Some(reply)) =
        databases.execute(db, DatabaseCommand::SetMembers(key.to_vec()))
    else {
        panic!("expected member array");
    };
    let mut members: Vec<_> = reply
        .into_iter()
        .map(|reply| {
            let Reply::Bulk(Some(bytes)) = reply else {
                panic!("expected bulk member")
            };
            bytes
        })
        .collect();
    members.sort();
    members
}

#[test]
fn increments_are_exact_signed_integers_and_errors_preserve_strings() {
    let mut databases = Databases::default();
    let key = b"\0\xff".to_vec();
    assert_eq!(
        databases.execute(DB, DatabaseCommand::Increment(key.clone())),
        Reply::Integer(1)
    );
    for (before, after) in [
        (i64::MIN, i64::MIN + 1),
        (-1, 0),
        (0, 1),
        (9_007_199_254_740_992, 9_007_199_254_740_993),
        (i64::MAX - 1, i64::MAX),
    ] {
        databases.execute(
            DB,
            DatabaseCommand::Set {
                key: key.clone(),
                value: before.to_string().into_bytes(),
            },
        );
        assert_eq!(
            databases.execute(DB, DatabaseCommand::Increment(key.clone())),
            Reply::Integer(after)
        );
        assert_eq!(
            databases.execute(DB, DatabaseCommand::Get(key.clone())),
            Reply::Bulk(Some(after.to_string().into_bytes()))
        );
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
        &vec![b'9'; 8192],
    ] {
        databases.execute(
            DB,
            DatabaseCommand::Set {
                key: key.clone(),
                value: bytes.to_vec(),
            },
        );
        assert_eq!(
            databases.execute(DB, DatabaseCommand::Increment(key.clone())),
            Reply::Error(b"ERR value is not an integer or out of range".to_vec())
        );
        assert_eq!(
            databases.execute(DB, DatabaseCommand::Get(key.clone())),
            Reply::Bulk(Some(bytes.to_vec()))
        );
    }
    databases.execute(
        DB,
        DatabaseCommand::Set {
            key: key.clone(),
            value: i64::MAX.to_string().into_bytes(),
        },
    );
    assert_eq!(
        databases.execute(DB, DatabaseCommand::Increment(key.clone())),
        Reply::Error(b"ERR increment or decrement would overflow".to_vec())
    );
    assert_eq!(
        databases.execute(DB, DatabaseCommand::Get(key)),
        Reply::Bulk(Some(i64::MAX.to_string().into_bytes()))
    );
}

#[test]
fn sets_are_binary_unique_and_deleted_when_empty_in_all_databases() {
    let mut databases = Databases::default();
    for index in 0..16 {
        let db = DatabaseIndex::new(index).unwrap();
        assert_eq!(
            databases.execute(db, DatabaseCommand::SetCardinality(vec![])),
            Reply::Integer(0)
        );
        assert_eq!(
            databases.execute(
                db,
                DatabaseCommand::SetContains {
                    key: vec![],
                    member: vec![]
                }
            ),
            Reply::Integer(0)
        );
        assert_eq!(
            databases.execute(db, remove(b"", &[b""])),
            Reply::Integer(0)
        );
        assert!(members(&mut databases, db, b"").is_empty());
        assert!(databases.0[index as usize].is_empty());
        assert_eq!(
            databases.execute(db, add(b"", &[b"", b"\0\xff\r\n", b"", &[index as u8]])),
            Reply::Integer(3)
        );
        assert_eq!(
            databases.execute(db, add(b"", &[b"\0\xff\r\n"])),
            Reply::Integer(0)
        );
    }
    for index in 0..16 {
        let db = DatabaseIndex::new(index).unwrap();
        assert_eq!(
            databases.execute(db, DatabaseCommand::SetCardinality(vec![])),
            Reply::Integer(3)
        );
        assert_eq!(
            databases.execute(
                db,
                DatabaseCommand::SetContains {
                    key: vec![],
                    member: vec![index as u8]
                }
            ),
            Reply::Integer(1)
        );
        let mut expected = vec![vec![], vec![index as u8], b"\0\xff\r\n".to_vec()];
        expected.sort();
        assert_eq!(members(&mut databases, db, b""), expected);
        assert_eq!(
            databases.execute(db, remove(b"", &[b"", b"", b"absent"])),
            Reply::Integer(1)
        );
        assert_eq!(
            databases.execute(db, remove(b"", &[&[index as u8], b"\0\xff\r\n"])),
            Reply::Integer(2)
        );
        assert_eq!(
            databases.execute(db, DatabaseCommand::Exists(vec![vec![]])),
            Reply::Integer(0)
        );
        assert!(databases.0[index as usize].is_empty());
    }
}

#[test]
fn set_index_survives_removing_first_middle_last_members_and_churn() {
    let mut databases = Databases::default();
    for cycle in 0..4 {
        for n in 0..1000u32 {
            assert_eq!(
                databases.execute(DB, add(b"set", &[&n.to_be_bytes()])),
                Reply::Integer(1)
            );
        }
        // Nontrivial permutation repeatedly removes an index whose final member
        // is swapped in. All membership lookups/removals must still agree.
        for n in (0..1000u32).map(|n| (n * 317) % 1000) {
            assert_eq!(
                databases.execute(DB, remove(b"set", &[&n.to_be_bytes()])),
                Reply::Integer(1)
            );
            assert_eq!(
                databases.execute(
                    DB,
                    DatabaseCommand::SetContains {
                        key: b"set".to_vec(),
                        member: n.to_be_bytes().to_vec()
                    }
                ),
                Reply::Integer(0)
            );
            if n % 29 == 0 {
                let snapshot = members(&mut databases, DB, b"set");
                assert_eq!(
                    databases.execute(DB, DatabaseCommand::SetCardinality(b"set".to_vec())),
                    Reply::Integer(snapshot.len() as i64)
                );
                for member in snapshot {
                    assert_eq!(
                        databases.execute(
                            DB,
                            DatabaseCommand::SetContains {
                                key: b"set".to_vec(),
                                member
                            }
                        ),
                        Reply::Integer(1)
                    );
                }
            }
        }
        assert!(databases.0[0].is_empty(), "cycle {cycle}");
    }
}

#[test]
fn all_type_errors_preserve_sets_strings_lists_and_set_can_replace_any_type() {
    let mut databases = Databases::default();
    databases.execute(DB, add(b"set", &[b"one", b"two"]));
    for command in [
        DatabaseCommand::Get(b"set".to_vec()),
        DatabaseCommand::Increment(b"set".to_vec()),
        DatabaseCommand::Push {
            end: ListEnd::Left,
            key: b"set".to_vec(),
            values: vec![b"x".to_vec()],
        },
        DatabaseCommand::Pop {
            end: ListEnd::Right,
            keys: vec![b"set".to_vec()],
            timeout: b"0".to_vec(),
        },
    ] {
        assert_eq!(databases.execute(DB, command), wrong_type());
        assert_eq!(
            members(&mut databases, DB, b"set"),
            vec![b"one".to_vec(), b"two".to_vec()]
        );
    }
    databases.execute(
        DB,
        DatabaseCommand::Set {
            key: b"string".to_vec(),
            value: b"42".to_vec(),
        },
    );
    databases.execute(
        DB,
        DatabaseCommand::Push {
            end: ListEnd::Right,
            key: b"list".to_vec(),
            values: vec![b"v".to_vec()],
        },
    );
    for key in [&b"string"[..], b"list"] {
        for command in [
            add(key, &[b"x"]),
            remove(key, &[b"x"]),
            DatabaseCommand::SetContains {
                key: key.to_vec(),
                member: b"x".to_vec(),
            },
            DatabaseCommand::SetCardinality(key.to_vec()),
            DatabaseCommand::SetMembers(key.to_vec()),
        ] {
            assert_eq!(databases.execute(DB, command), wrong_type());
        }
    }
    assert_eq!(
        databases.execute(DB, DatabaseCommand::Increment(b"list".to_vec())),
        wrong_type()
    );
    assert_eq!(
        databases.execute(DB, DatabaseCommand::Get(b"string".to_vec())),
        Reply::Bulk(Some(b"42".to_vec()))
    );
    let Some(Value::List(list)) = databases.0[0].get(b"list".as_slice()) else {
        panic!("expected list")
    };
    assert_eq!(list, &VecDeque::from([b"v".to_vec()]));
    assert_eq!(
        databases.execute(
            DB,
            DatabaseCommand::Exists(vec![b"set".to_vec(), b"set".to_vec()])
        ),
        Reply::Integer(2)
    );
    databases.execute(
        DB,
        DatabaseCommand::Set {
            key: b"set".to_vec(),
            value: b"0".to_vec(),
        },
    );
    assert_eq!(
        databases.execute(DB, DatabaseCommand::Increment(b"set".to_vec())),
        Reply::Integer(1)
    );
}
