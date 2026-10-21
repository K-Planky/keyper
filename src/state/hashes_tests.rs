use std::collections::BTreeMap;

use super::*;

const DB: DatabaseIndex = DatabaseIndex(0);

fn set(key: &[u8], pairs: &[(&[u8], &[u8])]) -> DatabaseCommand {
    DatabaseCommand::HashSet {
        key: key.to_vec(),
        pairs: pairs
            .iter()
            .map(|(f, v)| (f.to_vec(), v.to_vec()))
            .collect(),
    }
}

fn delete(key: &[u8], fields: &[&[u8]]) -> DatabaseCommand {
    DatabaseCommand::HashDelete {
        key: key.to_vec(),
        fields: fields.iter().map(|f| f.to_vec()).collect(),
    }
}

fn array(reply: Reply) -> Vec<Vec<u8>> {
    let Reply::Array(Some(values)) = reply else {
        panic!("expected array")
    };
    values
        .into_iter()
        .map(|reply| {
            let Reply::Bulk(Some(bytes)) = reply else {
                panic!("expected bulk string")
            };
            bytes
        })
        .collect()
}

fn snapshot(
    databases: &mut Databases,
    db: DatabaseIndex,
    key: &[u8],
) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let flat = array(databases.execute(db, DatabaseCommand::HashGetAll(key.to_vec())));
    assert_eq!(flat.len() % 2, 0);
    let pairs: BTreeMap<_, _> = flat
        .as_chunks::<2>()
        .0
        .iter()
        .map(|p| (p[0].clone(), p[1].clone()))
        .collect();
    assert_eq!(pairs.len() * 2, flat.len(), "unique fields");
    pairs
}

#[test]
fn hashes_preserve_binary_fields_duplicate_counts_and_missing_shapes_in_all_databases() {
    let mut databases = Databases::default();
    for index in 0..16 {
        let db = DatabaseIndex(index);
        for command in [
            DatabaseCommand::HashLength(vec![]),
            DatabaseCommand::HashContains {
                key: vec![],
                field: vec![],
            },
            delete(b"", &[b""]),
        ] {
            assert_eq!(databases.execute(db, command), Reply::Integer(0));
        }
        assert_eq!(
            databases.execute(
                db,
                DatabaseCommand::HashGet {
                    key: vec![],
                    field: vec![]
                }
            ),
            Reply::Bulk(None)
        );
        assert!(snapshot(&mut databases, db, b"").is_empty());
        assert_eq!(
            databases.execute(db, DatabaseCommand::HashValues(vec![])),
            Reply::Array(Some(vec![]))
        );
        assert!(databases.0[index as usize].is_empty());
        assert_eq!(
            databases.execute(
                db,
                set(
                    b"",
                    &[(b"", b"first"), (b"\0\xff\r\n", &[index]), (b"", b"")]
                )
            ),
            Reply::Integer(2)
        );
        assert_eq!(
            databases.execute(db, set(b"", &[(b"same", b""), (b"same", b"")])),
            Reply::Integer(1)
        );
    }
    for index in 0..16 {
        let db = DatabaseIndex(index);
        let expected = BTreeMap::from([
            (vec![], vec![]),
            (b"same".to_vec(), vec![]),
            (b"\0\xff\r\n".to_vec(), vec![index]),
        ]);
        assert_eq!(snapshot(&mut databases, db, b""), expected);
        let mut values = array(databases.execute(db, DatabaseCommand::HashValues(vec![])));
        values.sort();
        assert_eq!(values, vec![vec![], vec![], vec![index]]);
        assert_eq!(
            databases.execute(
                db,
                DatabaseCommand::HashGet {
                    key: vec![],
                    field: vec![]
                }
            ),
            Reply::Bulk(Some(vec![]))
        );
        assert_eq!(
            databases.execute(
                db,
                DatabaseCommand::HashGet {
                    key: vec![],
                    field: b"absent".to_vec()
                }
            ),
            Reply::Bulk(None)
        );
        assert_eq!(
            databases.execute(
                db,
                DatabaseCommand::HashContains {
                    key: vec![],
                    field: vec![]
                }
            ),
            Reply::Integer(1)
        );
        assert_eq!(
            databases.execute(db, DatabaseCommand::HashLength(vec![])),
            Reply::Integer(3)
        );
        assert_eq!(
            databases.execute(db, set(b"", &[(b"", b"updated"), (b"", b"last")])),
            Reply::Integer(0)
        );
        assert_eq!(
            databases.execute(
                db,
                DatabaseCommand::HashGet {
                    key: vec![],
                    field: vec![]
                }
            ),
            Reply::Bulk(Some(b"last".to_vec()))
        );
        assert_eq!(
            databases.execute(db, delete(b"", &[b"", b"", b"missing"])),
            Reply::Integer(1)
        );
        assert_eq!(
            databases.execute(db, delete(b"", &[b"same", b"\0\xff\r\n", b"same"])),
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
fn hash_updates_and_swap_removals_agree_with_an_independent_ordered_map() {
    let mut databases = Databases::default();
    let mut expected = BTreeMap::new();
    for cycle in 0..3u32 {
        for n in 0..1000u32 {
            let field = n.to_be_bytes().to_vec();
            let value = (n + cycle).to_be_bytes().to_vec();
            let added = i64::from(expected.insert(field.clone(), value.clone()).is_none());
            assert_eq!(
                databases.execute(DB, set(b"h", &[(&field, &value)])),
                Reply::Integer(added)
            );
        }
        for n in (0..1000u32).map(|n| (n * 317) % 1000) {
            let field = n.to_be_bytes();
            assert_eq!(
                databases.execute(DB, delete(b"h", &[&field, &field])),
                Reply::Integer(1)
            );
            expected.remove(field.as_slice());
            assert_eq!(
                databases.execute(
                    DB,
                    DatabaseCommand::HashGet {
                        key: b"h".to_vec(),
                        field: field.to_vec()
                    }
                ),
                Reply::Bulk(None)
            );
            // Updates after removals exercise moved-field index repair too.
            if let Some((field, value)) = expected
                .first_key_value()
                .map(|(f, v)| (f.clone(), v.clone()))
            {
                assert_eq!(
                    databases.execute(DB, set(b"h", &[(&field, &value)])),
                    Reply::Integer(0)
                );
                assert_eq!(
                    databases.execute(
                        DB,
                        DatabaseCommand::HashGet {
                            key: b"h".to_vec(),
                            field
                        }
                    ),
                    Reply::Bulk(Some(value))
                );
            }
            if n % 29 == 0 {
                assert_eq!(snapshot(&mut databases, DB, b"h"), expected);
                assert_eq!(
                    databases.execute(DB, DatabaseCommand::HashLength(b"h".to_vec())),
                    Reply::Integer(expected.len() as i64)
                );
                let mut actual =
                    array(databases.execute(DB, DatabaseCommand::HashValues(b"h".to_vec())));
                actual.sort();
                let mut values: Vec<_> = expected.values().cloned().collect();
                values.sort();
                assert_eq!(actual, values);
            }
        }
        assert!(databases.0[0].is_empty());
    }
}

#[test]
fn hash_type_errors_preserve_all_types_and_set_can_replace_hashes() {
    let mut databases = Databases::default();
    databases.execute(DB, set(b"h", &[(b"f", b"v")]));
    for command in [
        DatabaseCommand::Get(b"h".to_vec()),
        DatabaseCommand::Increment(b"h".to_vec()),
        DatabaseCommand::SetAdd {
            key: b"h".to_vec(),
            members: vec![vec![]],
        },
        DatabaseCommand::SetRemove {
            key: b"h".to_vec(),
            members: vec![vec![]],
        },
        DatabaseCommand::SetContains {
            key: b"h".to_vec(),
            member: vec![],
        },
        DatabaseCommand::SetCardinality(b"h".to_vec()),
        DatabaseCommand::SetMembers(b"h".to_vec()),
        DatabaseCommand::Push {
            end: ListEnd::Left,
            key: b"h".to_vec(),
            values: vec![vec![]],
        },
        DatabaseCommand::Pop {
            end: ListEnd::Right,
            keys: vec![b"h".to_vec()],
            timeout: b"0".to_vec(),
        },
    ] {
        assert_eq!(databases.execute(DB, command), wrong_type());
        assert_eq!(
            snapshot(&mut databases, DB, b"h"),
            BTreeMap::from([(b"f".to_vec(), b"v".to_vec())])
        );
    }
    databases.execute(
        DB,
        DatabaseCommand::Set {
            key: b"s".to_vec(),
            value: b"42".to_vec(),
        },
    );
    databases.execute(
        DB,
        DatabaseCommand::SetAdd {
            key: b"set".to_vec(),
            members: vec![b"m".to_vec()],
        },
    );
    databases.execute(
        DB,
        DatabaseCommand::Push {
            end: ListEnd::Right,
            key: b"l".to_vec(),
            values: vec![b"v".to_vec()],
        },
    );
    for key in [&b"s"[..], b"set", b"l"] {
        for command in [
            set(key, &[(b"f", b"v")]),
            delete(key, &[b"f"]),
            DatabaseCommand::HashGet {
                key: key.to_vec(),
                field: vec![],
            },
            DatabaseCommand::HashContains {
                key: key.to_vec(),
                field: vec![],
            },
            DatabaseCommand::HashLength(key.to_vec()),
            DatabaseCommand::HashGetAll(key.to_vec()),
            DatabaseCommand::HashValues(key.to_vec()),
        ] {
            assert_eq!(databases.execute(DB, command), wrong_type());
        }
    }
    assert_eq!(
        databases.execute(DB, DatabaseCommand::Get(b"s".to_vec())),
        Reply::Bulk(Some(b"42".to_vec()))
    );
    assert_eq!(
        databases.execute(DB, DatabaseCommand::SetCardinality(b"set".to_vec())),
        Reply::Integer(1)
    );
    assert_eq!(
        databases.execute(
            DB,
            DatabaseCommand::Pop {
                end: ListEnd::Left,
                keys: vec![b"l".to_vec()],
                timeout: b"0".to_vec()
            }
        ),
        Reply::Array(Some(vec![
            Reply::Bulk(Some(b"l".to_vec())),
            Reply::Bulk(Some(b"v".to_vec()))
        ]))
    );
    assert_eq!(
        databases.execute(
            DB,
            DatabaseCommand::Exists(vec![b"h".to_vec(), b"h".to_vec()])
        ),
        Reply::Integer(2)
    );
    databases.execute(
        DB,
        DatabaseCommand::Set {
            key: b"h".to_vec(),
            value: b"0".to_vec(),
        },
    );
    assert_eq!(
        databases.execute(DB, DatabaseCommand::Increment(b"h".to_vec())),
        Reply::Integer(1)
    );
}
