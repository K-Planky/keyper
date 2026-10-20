//! Loopback TCP service and supervision. No database state lives in this layer.

use std::future::Future;
use std::io;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::time::Duration;

use tokio::io::{AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tokio::time::timeout;

use crate::command::{Action, Session};
use crate::config::Config;
use crate::resp::{self, ReadError, Reply};
use crate::state;

pub const MAX_CONNECTIONS: usize = 256;
pub const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
pub const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const READ_BUFFER_BYTES: usize = 8 * 1024;

pub async fn bind(config: Config) -> io::Result<TcpListener> {
    TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, config.port)).await
}

/// Serve until shutdown or an unexpected listener/task failure. Normal client
/// I/O failures only close that connection; aborted accepts are retried. A task
/// panic is fatal, not a reason to silently resume with possibly uncertain state.
/// The caller must not restart this service automatically after an error.
///
/// Shutdown cancels connection I/O and unstarted work, then waits for the owner
/// to finish its current atomic operation. A drain timeout is fatal; callers must
/// exit, not restart this service while a detached owner might still be running.
pub async fn serve(
    listener: TcpListener,
    shutdown: impl Future<Output = io::Result<()>>,
) -> io::Result<()> {
    serve_with_state(listener, shutdown, state::spawn()?).await
}

async fn serve_with_state(
    listener: TcpListener,
    shutdown: impl Future<Output = io::Result<()>>,
    (state, mut owner): (state::Client, state::Completion),
) -> io::Result<()> {
    let mut connections = JoinSet::new();
    let mut owner_finished = false;
    tokio::pin!(shutdown);
    let outcome = loop {
        tokio::select! {
            biased;
            result = &mut shutdown => break result,
            result = &mut owner => {
                owner_finished = true;
                break owner_outcome(result).and(Err(io::Error::other(
                    "state owner stopped unexpectedly; service must stop"
                )));
            }
            result = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(error)) = result {
                    break Err(io::Error::other(format!("connection task failed: {error}")));
                }
            }
            accepted = listener.accept(), if connections.len() < MAX_CONNECTIONS => {
                let (stream, peer) = match accepted {
                    Ok(connection) => connection,
                    Err(error) if matches!(error.kind(),
                        io::ErrorKind::Interrupted
                        | io::ErrorKind::ConnectionAborted
                        | io::ErrorKind::ConnectionReset
                    ) => continue,
                    Err(error) => break Err(io::Error::new(
                        error.kind(), format!("accept connection: {error}")
                    )),
                };
                let state = state.clone();
                connections.spawn(async move {
                    if let Err(error) = connection(stream, state).await {
                        eprintln!("keyper: connection {peer}: {error}");
                    }
                });
            }
        }
    };
    drop(listener);
    connections.abort_all();
    let drained = timeout(SHUTDOWN_TIMEOUT, async {
        let mut outcome = Ok(());
        while let Some(result) = connections.join_next().await {
            if let Err(error) = result
                && !error.is_cancelled()
            {
                outcome = Err(io::Error::other(format!(
                    "connection task failed during shutdown: {error}"
                )));
            }
        }
        // Every connection sender/receiver is now gone. Closing the last sender
        // lets the owner discard canceled queued work and finish, without ever
        // interrupting a mutation or waiting on socket I/O.
        drop(state);
        if !owner_finished {
            outcome = outcome.and(owner_outcome(owner.await));
        }
        outcome
    })
    .await
    .unwrap_or_else(|_| {
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "service shutdown exceeded five seconds; process must exit",
        ))
    });
    outcome.and(drained)
}

fn owner_outcome(
    result: Result<io::Result<()>, tokio::sync::oneshot::error::RecvError>,
) -> io::Result<()> {
    result.unwrap_or_else(|_| Err(io::Error::other("state owner completion channel closed")))
}

async fn connection(stream: TcpStream, state: state::Client) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let mut reader = BufReader::with_capacity(READ_BUFFER_BYTES, stream);
    let mut session = Session::default();
    loop {
        let (reply, close) = match resp::read_request(&mut reader).await {
            Ok(Some(request)) => {
                let reply = match session.prepare(request) {
                    Action::Reply(reply) => reply,
                    Action::Database(database, command) => state.execute(database, command).await?,
                };
                (reply, false)
            }
            Ok(None) | Err(ReadError::Truncated) => return Ok(()),
            Err(ReadError::Protocol(message)) => (
                Reply::Error(format!("ERR Protocol error: {message}").into_bytes()),
                true,
            ),
            Err(ReadError::Io(error)) => {
                return Err(io::Error::new(
                    error.kind(),
                    format!("read request: {error}"),
                ));
            }
        };
        // One owned response at a time; never retain shared state across writes.
        // BufReader retains any coalesced bytes for the next request, but full
        // blocking-command/pipeline lifecycle guarantees belong to later steps.
        write_reply(reader.get_mut(), &reply).await?;
        if close {
            return Ok(());
        }
    }
}

async fn write_reply<W: AsyncWrite + Unpin>(writer: &mut W, reply: &Reply) -> io::Result<()> {
    let bytes = reply.encode();
    timeout(WRITE_TIMEOUT, writer.write_all(&bytes))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "write reply exceeded five seconds"))?
        .map_err(|error| io::Error::new(error.kind(), format!("write reply: {error}")))
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, duplex};

    use super::*;

    #[tokio::test]
    async fn owner_panic_stops_even_idle_connections_instead_of_replacing_state() {
        let listener = bind(Config { port: 0 }).await.unwrap();
        let address = listener.local_addr().unwrap();
        let (fail, signal) = tokio::sync::oneshot::channel();
        let owner = state::failing_owner(signal).unwrap();
        let serving = serve_with_state(listener, std::future::pending(), owner);
        let checking = async {
            let mut client = TcpStream::connect(address).await.unwrap();
            client.write_all(b"*1\r\n$4\r\nPING\r\n").await.unwrap();
            let mut pong = [0; 7];
            client.read_exact(&mut pong).await.unwrap();
            assert_eq!(&pong, b"+PONG\r\n");
            fail.send(()).unwrap();
            let mut byte = [0];
            assert_eq!(client.read(&mut byte).await.unwrap(), 0);
        };
        let (result, ()) = timeout(Duration::from_secs(5), async {
            tokio::join!(serving, checking)
        })
        .await
        .unwrap();
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("state owner panicked")
        );
        assert!(TcpStream::connect(address).await.is_err());
    }

    #[tokio::test]
    async fn reply_write_handles_partial_writes_and_broken_connections() {
        let reply = Reply::Bulk(Some(b"\0\xff\r\n".to_vec()));
        let expected = reply.encode();
        let (mut writer, mut reader) = duplex(1);
        let writing = tokio::spawn(async move { write_reply(&mut writer, &reply).await });
        let mut received = vec![0; expected.len()];
        timeout(Duration::from_secs(2), reader.read_exact(&mut received))
            .await
            .unwrap()
            .unwrap();
        writing.await.unwrap().unwrap();
        assert_eq!(received, expected);

        let (mut writer, reader) = duplex(1);
        drop(reader);
        assert_eq!(
            write_reply(&mut writer, &Reply::Integer(1))
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::BrokenPipe
        );
    }

    #[tokio::test(start_paused = true)]
    async fn nonreading_peer_has_a_bounded_write_deadline() {
        let (mut writer, _reader) = duplex(1);
        let start = tokio::time::Instant::now();
        let error = write_reply(&mut writer, &Reply::Integer(1))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert_eq!(start.elapsed(), WRITE_TIMEOUT);
    }
}
