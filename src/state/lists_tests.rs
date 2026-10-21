use super::*;

const DB: DatabaseIndex = DatabaseIndex(0);

fn index(key: &[u8], index: &[u8]) -> DatabaseCommand {
    DatabaseCommand::ListIndex {
        key: key.to_vec(),
        index: index.to_vec(),
    }
}

fn pop(key: &[u8], count: Option<&[u8]>) -> DatabaseCommand {
    DatabaseCommand::ListPop {
        key: key.to_vec(),
        count: count.map(<[u8]>::to_vec),
    }
}

fn push(end: ListEnd, values: Vec<Vec<u8>>) -> DatabaseCommand {
    DatabaseCommand::Push {
        end,
        key: b"list".to_vec(),
        values,
    }
}

#[test]
fn list_indices_counts_and_empty_deletion_match_an_independent_vector_through_churn() {
    let mut databases = Databases::default();
    let mut expected: Vec<Vec<u8>> = Vec::new();
    for cycle in 0..100u32 {
        let values: Vec<_> = (0..13u32)
            .map(|n| (cycle * 13 + n).to_be_bytes().to_vec())
            .collect();
        let end = if cycle % 2 == 0 {
            ListEnd::Left
        } else {
            ListEnd::Right
        };
        if end == ListEnd::Left {
            expected.splice(..0, values.iter().rev().cloned());
        } else {
            expected.extend(values.iter().cloned());
        }
        assert_eq!(
            databases.execute(DB, push(end, values)),
            Reply::Integer(expected.len() as i64)
        );
        for n in -(expected.len() as i64) - 1..=expected.len() as i64 {
            let offset = if n < 0 { expected.len() as i64 + n } else { n };
            let value = usize::try_from(offset)
                .ok()
                .and_then(|i| expected.get(i))
                .cloned();
            assert_eq!(
                databases.execute(DB, index(b"list", n.to_string().as_bytes())),
                Reply::Bulk(value)
            );
        }
        for n in [i64::MIN, i64::MAX] {
            assert_eq!(
                databases.execute(DB, index(b"list", n.to_string().as_bytes())),
                Reply::Bulk(None)
            );
        }
        for count in [0, 1, 7] {
            let take = count.min(expected.len());
            let values = expected
                .drain(..take)
                .map(|v| Reply::Bulk(Some(v)))
                .collect();
            assert_eq!(
                databases.execute(DB, pop(b"list", Some(count.to_string().as_bytes()))),
                Reply::Array(Some(values))
            );
        }
        assert_eq!(
            databases.execute(DB, pop(b"list", None)),
            Reply::Bulk(Some(expected.remove(0)))
        );
        assert_eq!(
            databases.execute(DB, DatabaseCommand::ListLength(b"list".to_vec())),
            Reply::Integer(expected.len() as i64)
        );
    }
    let values = expected.into_iter().map(|v| Reply::Bulk(Some(v))).collect();
    assert_eq!(
        databases.execute(DB, pop(b"list", Some(b"9223372036854775807"))),
        Reply::Array(Some(values))
    );
    assert!(databases.0[0].is_empty());
    assert_eq!(databases.execute(DB, pop(b"list", None)), Reply::Bulk(None));
    for count in [b"0".as_slice(), b"1"] {
        assert_eq!(
            databases.execute(DB, pop(b"list", Some(count))),
            Reply::Array(None)
        );
    }
}

#[test]
fn numeric_error_precedence_matches_each_command_and_failed_pops_preserve_the_list() {
    let mut databases = Databases::default();
    databases.execute(DB, push(ListEnd::Right, vec![vec![], vec![0, 255]]));
    databases.execute(
        DB,
        DatabaseCommand::Set {
            key: b"string".to_vec(),
            value: b"42".to_vec(),
        },
    );
    for text in [
        b"".as_slice(),
        b"-0",
        b"+1",
        b"01",
        b"-01",
        b" 1",
        b"1 ",
        b"1.0",
        b"1e0",
        b"\xff",
        b"1\0",
        b"9223372036854775808",
        b"-9223372036854775809",
        &[b'9'; 100],
    ] {
        assert_eq!(
            databases.execute(DB, index(b"missing", text)),
            Reply::Bulk(None)
        );
        assert_eq!(databases.execute(DB, index(b"string", text)), wrong_type());
        assert_eq!(
            databases.execute(DB, index(b"list", text)),
            Reply::Error(b"ERR value is not an integer or out of range".to_vec())
        );
        for key in [b"missing".as_slice(), b"string", b"list"] {
            assert_eq!(
                databases.execute(DB, pop(key, Some(text))),
                Reply::Error(b"ERR value is out of range, must be positive".to_vec())
            );
        }
    }
    assert_eq!(
        databases.execute(DB, pop(b"list", Some(b"-1"))),
        Reply::Error(b"ERR value is out of range, must be positive".to_vec())
    );
    assert_eq!(
        databases.execute(DB, pop(b"string", Some(b"0"))),
        wrong_type()
    );
    assert_eq!(
        databases.execute(DB, pop(b"list", Some(b"0"))),
        Reply::Array(Some(vec![]))
    );
    assert_eq!(
        databases.execute(DB, pop(b"list", None)),
        Reply::Bulk(Some(vec![]))
    );
    assert_eq!(
        databases.execute(DB, pop(b"list", Some(b"1"))),
        Reply::Array(Some(vec![Reply::Bulk(Some(vec![0, 255]))]))
    );
    assert!(!databases.0[0].contains_key(b"list".as_slice()));
}

#[test]
fn type_reports_every_supported_value_and_never_creates_missing_keys() {
    let mut databases = Databases::default();
    for n in 0..16 {
        let db = DatabaseIndex(n);
        assert_eq!(
            databases.execute(db, DatabaseCommand::Type(vec![])),
            Reply::Simple(b"none".to_vec())
        );
        assert_eq!(
            databases.execute(db, DatabaseCommand::ListLength(vec![])),
            Reply::Integer(0)
        );
        assert!(databases.0[n as usize].is_empty());
        for (value, kind) in [
            (Value::String(vec![]), b"string".as_slice()),
            (Value::List(VecDeque::from([vec![n]])), b"list"),
            (Value::Set(set::Set::default()), b"set"),
            (Value::Hash(hash::Hash::default()), b"hash"),
        ] {
            databases.0[n as usize].insert(vec![], value);
            assert_eq!(
                databases.execute(db, DatabaseCommand::Type(vec![])),
                Reply::Simple(kind.to_vec())
            );
            if kind != b"list" {
                assert_eq!(
                    databases.execute(db, DatabaseCommand::ListLength(vec![])),
                    wrong_type()
                );
                assert_eq!(databases.execute(db, index(b"", b"0")), wrong_type());
                assert_eq!(databases.execute(db, pop(b"", None)), wrong_type());
            }
        }
    }
}
