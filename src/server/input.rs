//! Bounded read-ahead keeps EOF detection alive while execution or writing waits.

use std::collections::VecDeque;
use std::io;

use tokio::io::{AsyncReadExt, BufReader};
use tokio::net::tcp::OwnedReadHalf;

use crate::resp::{self, ReadError, Request};

const READ_BUFFER_BYTES: usize = 8 * 1024;
pub(super) const MAX_READ_AHEAD_BYTES: usize = resp::MAX_FRAME_BYTES;

pub(super) struct Input {
    reader: BufReader<OwnedReadHalf>,
    ahead: VecDeque<u8>,
}

impl Input {
    pub(super) fn new(reader: OwnedReadHalf) -> Self {
        Self {
            reader: BufReader::with_capacity(READ_BUFFER_BYTES, reader),
            ahead: VecDeque::new(),
        }
    }

    pub(super) async fn request(&mut self) -> Result<Option<Request>, ReadError> {
        // Both deque slices implement AsyncBufRead. Chaining borrows, rather
        // than copying/compacting a large queued pipeline on every small frame.
        let original = self.ahead.len();
        let (mut front, mut back) = self.ahead.as_slices();
        let result =
            resp::read_request(&mut (&mut front).chain(&mut back).chain(&mut self.reader)).await;
        let consumed = original - front.len() - back.len();
        self.ahead.drain(..consumed);
        result
    }

    /// Returns only on EOF/error/overflow. Cancellation after any read retains
    /// those bytes in order; no partially decoded future command is executed.
    pub(super) async fn until_closed(&mut self) -> io::Result<()> {
        let mut chunk = [0; READ_BUFFER_BYTES];
        loop {
            // At the limit probe one byte to distinguish EOF from excess data.
            // Never wait on a full queue and thereby hide a disconnect forever.
            let available = (MAX_READ_AHEAD_BYTES - self.ahead.len()).max(1);
            let length = chunk.len().min(available);
            let read = self.reader.read(&mut chunk[..length]).await?;
            if read == 0 {
                return Ok(());
            }
            if self.ahead.len() + read > MAX_READ_AHEAD_BYTES {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "pending input exceeds 16 MiB; closing connection",
                ));
            }
            self.ahead.extend(&chunk[..read]);
        }
    }
}
