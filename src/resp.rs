//! RESP2 request framing and reply encoding, independent of command semantics.

use std::io;

use tokio::io::{AsyncBufRead, AsyncReadExt};

pub const MAX_ARGUMENTS: usize = 1024;
pub const MAX_BULK_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
const MAX_LENGTH_DIGITS: usize = 20;

pub type Request = Vec<Vec<u8>>;

#[derive(Debug)]
pub enum ReadError {
    Protocol(&'static str),
    Truncated,
    Io(io::Error),
}

impl From<io::Error> for ReadError {
    fn from(error: io::Error) -> Self {
        if error.kind() == io::ErrorKind::UnexpectedEof {
            Self::Truncated
        } else {
            Self::Io(error)
        }
    }
}

struct Budget(usize);

impl Budget {
    fn take(&mut self, bytes: usize) -> Result<(), ReadError> {
        self.0 = self
            .0
            .checked_sub(bytes)
            .ok_or(ReadError::Protocol("request exceeds byte limit"))?;
        Ok(())
    }
}

// Length headers are consumed once, even when the peer sends one byte at a time.
// Reject untrusted sizes before allocation and never scan/copy a growing frame.
async fn read_length<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    budget: &mut Budget,
) -> Result<usize, ReadError> {
    let mut value = 0usize;
    let mut digits = 0;
    loop {
        budget.take(1)?;
        let byte = reader.read_u8().await?;
        if byte == b'\r' && digits > 0 {
            budget.take(1)?;
            if reader.read_u8().await? == b'\n' {
                return Ok(value);
            }
            return Err(ReadError::Protocol("expected LF after CR"));
        }
        if !byte.is_ascii_digit() || digits == MAX_LENGTH_DIGITS {
            return Err(ReadError::Protocol("invalid length"));
        }
        value = value
            .checked_mul(10)
            .and_then(|n| n.checked_add(usize::from(byte - b'0')))
            .ok_or(ReadError::Protocol("length out of range"))?;
        digits += 1;
    }
}

/// Read one array of non-null bulk strings. EOF before a frame is normal;
/// EOF within it is truncation. Inline commands, nested requests, and RESP3
/// are not accepted. Cancellation requires dropping the reader/connection:
/// this function consumes partial frames and must not be restarted after cancel.
pub async fn read_request<R: AsyncBufRead + Unpin>(
    reader: &mut R,
) -> Result<Option<Request>, ReadError> {
    let mut first = [0];
    if reader.read(&mut first).await? == 0 {
        return Ok(None);
    }
    if first[0] != b'*' {
        return Err(ReadError::Protocol("expected array of bulk strings"));
    }
    let mut budget = Budget(MAX_FRAME_BYTES - 1);
    let count = read_length(reader, &mut budget).await?;
    if count > MAX_ARGUMENTS {
        return Err(ReadError::Protocol("too many arguments"));
    }
    let mut args = Vec::with_capacity(count);
    for _ in 0..count {
        budget.take(1)?;
        if reader.read_u8().await? != b'$' {
            return Err(ReadError::Protocol("expected non-null bulk string"));
        }
        let length = read_length(reader, &mut budget).await?;
        if length > MAX_BULK_BYTES {
            return Err(ReadError::Protocol("bulk string exceeds byte limit"));
        }
        // The bulk bound makes this addition safe on supported platforms.
        budget.take(length + 2)?;
        let mut data = vec![0; length];
        reader.read_exact(&mut data).await?;
        let mut end = [0; 2];
        reader.read_exact(&mut end).await?;
        if end != *b"\r\n" {
            return Err(ReadError::Protocol("expected CRLF after bulk string"));
        }
        args.push(data);
    }
    Ok(Some(args))
}

/// All RESP2 reply types, including nested arrays for future EXEC/EVAL results.
/// Null and empty bulk strings/arrays remain distinct.
#[derive(Debug, Eq, PartialEq)]
pub enum Reply {
    Simple(Vec<u8>),
    Error(Vec<u8>),
    Integer(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Reply>>),
}

impl Reply {
    pub fn encode(&self) -> Vec<u8> {
        let mut output = Vec::new();
        self.encode_into(&mut output);
        output
    }

    fn encode_into(&self, output: &mut Vec<u8>) {
        match self {
            Self::Simple(text) | Self::Error(text) => {
                output.push(if matches!(self, Self::Simple(_)) {
                    b'+'
                } else {
                    b'-'
                });
                // Status/error text (eventually also Lua errors) must not inject
                // extra replies. Binary payloads belong in bulk strings.
                output.extend(
                    text.iter()
                        .map(|&b| if b == b'\r' || b == b'\n' { b' ' } else { b }),
                );
                output.extend_from_slice(b"\r\n");
            }
            Self::Integer(value) => {
                output.push(b':');
                output.extend_from_slice(value.to_string().as_bytes());
                output.extend_from_slice(b"\r\n");
            }
            Self::Bulk(None) => output.extend_from_slice(b"$-1\r\n"),
            Self::Bulk(Some(bytes)) => {
                output.push(b'$');
                output.extend_from_slice(bytes.len().to_string().as_bytes());
                output.extend_from_slice(b"\r\n");
                output.extend_from_slice(bytes);
                output.extend_from_slice(b"\r\n");
            }
            Self::Array(None) => output.extend_from_slice(b"*-1\r\n"),
            Self::Array(Some(items)) => {
                output.push(b'*');
                output.extend_from_slice(items.len().to_string().as_bytes());
                output.extend_from_slice(b"\r\n");
                for item in items {
                    item.encode_into(output);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::BufReader;

    use super::*;

    #[test]
    fn every_reply_type_and_nested_binary_arrays() {
        let reply = Reply::Array(Some(vec![
            Reply::Simple(b"OK".to_vec()),
            Reply::Error(b"ERR bad\r\nreply".to_vec()),
            Reply::Integer(i64::MIN),
            Reply::Integer(i64::MAX),
            Reply::Bulk(None),
            Reply::Bulk(Some(vec![])),
            Reply::Bulk(Some(b"\0\xff\r\n".to_vec())),
            Reply::Array(None),
            Reply::Array(Some(vec![])),
            Reply::Array(Some(vec![Reply::Integer(0)])),
        ]));
        assert_eq!(reply.encode(), b"*10\r\n+OK\r\n-ERR bad  reply\r\n:-9223372036854775808\r\n:9223372036854775807\r\n$-1\r\n$0\r\n\r\n$4\r\n\0\xff\r\n\r\n*-1\r\n*0\r\n*1\r\n:0\r\n");
        assert_eq!(Reply::Simple(b"x\ny".to_vec()).encode(), b"+x y\r\n");
    }

    #[tokio::test]
    async fn binary_empty_and_coalesced_requests() {
        // Force header/payload reads through one-byte buffer boundaries.
        let mut reader = BufReader::with_capacity(
            1,
            &b"*3\r\n$4\r\nPING\r\n$0\r\n\r\n$4\r\n\0\xff\r\n\r\n*0\r\n"[..],
        );
        assert_eq!(
            read_request(&mut reader).await.unwrap(),
            Some(vec![b"PING".to_vec(), vec![], b"\0\xff\r\n".to_vec()])
        );
        assert_eq!(read_request(&mut reader).await.unwrap(), Some(vec![]));
        assert_eq!(read_request(&mut reader).await.unwrap(), None);
    }

    #[tokio::test]
    async fn every_incomplete_prefix_is_truncated_not_a_command() {
        let wire = b"*2\r\n$4\r\nPING\r\n$3\r\nabc\r\n";
        for end in 1..wire.len() {
            let mut reader = BufReader::new(&wire[..end]);
            assert!(
                matches!(read_request(&mut reader).await, Err(ReadError::Truncated)),
                "prefix {end}"
            );
        }
    }

    #[tokio::test]
    async fn malformed_types_lengths_and_terminators_are_rejected() {
        for wire in [
            &b"PING\r\n"[..],
            b"+PING\r\n",
            b"_\r\n",
            b"*-1\r\n",
            b"*\r\n",
            b"*+1\r\n",
            b"*1\n",
            b"*1\rx",
            b"*1\r\n:1\r\n",
            b"*1\r\n*0\r\n",
            b"*1\r\n$-1\r\n",
            b"*1\r\n$x\r\n",
            b"*1\r\n$1\r\naXX",
            b"*999999999999999999999999\r\n",
            b"*1\r\n$18446744073709551616\r\n",
        ] {
            assert!(
                matches!(
                    read_request(&mut BufReader::new(wire)).await,
                    Err(ReadError::Protocol(_))
                ),
                "{wire:?}"
            );
        }
    }

    #[tokio::test]
    async fn declared_limits_are_checked_without_reading_payloads() {
        for header in [
            format!("*{}\r\n", MAX_ARGUMENTS + 1),
            format!("*1\r\n${}\r\n", MAX_BULK_BYTES + 1),
        ] {
            assert!(matches!(
                read_request(&mut BufReader::new(header.as_bytes())).await,
                Err(ReadError::Protocol(_))
            ));
        }
        // The exact argument limit is allowed, not an off-by-one rejection.
        let mut wire = format!("*{MAX_ARGUMENTS}\r\n").into_bytes();
        for _ in 0..MAX_ARGUMENTS {
            wire.extend_from_slice(b"$0\r\n\r\n");
        }
        assert_eq!(
            read_request(&mut BufReader::new(wire.as_slice()))
                .await
                .unwrap()
                .unwrap()
                .len(),
            MAX_ARGUMENTS
        );
    }

    #[tokio::test]
    async fn bulk_and_total_frame_bounds_apply_before_payload_allocation() {
        let mut wire = format!("*1\r\n${MAX_BULK_BYTES}\r\n").into_bytes();
        wire.resize(wire.len() + MAX_BULK_BYTES, b'x');
        wire.extend_from_slice(b"\r\n");
        assert_eq!(
            read_request(&mut BufReader::new(wire.as_slice()))
                .await
                .unwrap()
                .unwrap()[0]
                .len(),
            MAX_BULK_BYTES
        );
        // Each bulk is allowed separately, but their headers plus contents
        // exceed the aggregate bound. No second payload need be transmitted.
        wire[1] = b'2';
        wire.extend_from_slice(format!("${MAX_BULK_BYTES}\r\n").as_bytes());
        assert!(matches!(
            read_request(&mut BufReader::new(wire.as_slice())).await,
            Err(ReadError::Protocol("request exceeds byte limit"))
        ));

        // At the exact aggregate boundary, headers count but bytes are allowed.
        let header = format!("*2\r\n${MAX_BULK_BYTES}\r\n");
        let remaining = MAX_FRAME_BYTES - header.len() - MAX_BULK_BYTES - 2;
        let second_length = remaining - format!("${MAX_BULK_BYTES}\r\n").len() - 2;
        let mut exact = header.into_bytes();
        exact.resize(exact.len() + MAX_BULK_BYTES, b'x');
        exact.extend_from_slice(format!("\r\n${second_length}\r\n").as_bytes());
        exact.resize(exact.len() + second_length, b'y');
        exact.extend_from_slice(b"\r\n");
        assert_eq!(exact.len(), MAX_FRAME_BYTES);
        let request = read_request(&mut BufReader::new(exact.as_slice()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(request[1].len(), second_length);
    }
}
