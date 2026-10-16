//! Command validation stays separate from wire framing and network lifecycle.

use crate::resp::{Reply, Request};

pub fn execute(request: Request) -> Reply {
    let mut args = request.into_iter();
    let Some(name) = args.next() else {
        return Reply::Error(b"ERR empty command".to_vec());
    };
    if !name.eq_ignore_ascii_case(b"PING") {
        // Do not reflect arbitrary (possibly binary or sensitive) input.
        return Reply::Error(b"ERR unknown command".to_vec());
    }
    match (args.next(), args.next()) {
        (None, None) => Reply::Simple(b"PONG".to_vec()),
        (Some(message), None) => Reply::Bulk(Some(message)),
        _ => Reply::Error(b"ERR wrong number of arguments for 'ping' command".to_vec()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ping_is_ascii_case_insensitive_and_echoes_binary_or_empty_messages() {
        for name in [b"PING", b"ping", b"pInG"] {
            assert_eq!(
                execute(vec![name.to_vec()]),
                Reply::Simple(b"PONG".to_vec())
            );
            for message in [vec![], b"\0\xff\r\n".to_vec()] {
                assert_eq!(
                    execute(vec![name.to_vec(), message.clone()]),
                    Reply::Bulk(Some(message))
                );
            }
        }
    }

    #[test]
    fn invalid_or_unsupported_commands_return_errors() {
        for args in [
            vec![],
            vec![b"PING".to_vec(), vec![], vec![]],
            vec![b"UNKNOWN\r\n".to_vec()],
            vec![vec![]],
        ] {
            assert!(matches!(execute(args), Reply::Error(_)));
        }
    }
}
