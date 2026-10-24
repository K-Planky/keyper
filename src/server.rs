//! TCP service and supervision. No database state lives in this layer.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tokio::time::timeout;

use crate::command::{Action, Session};
use crate::config::Config;
use crate::resp::{ReadError, Reply};
use crate::state;

mod input;
use input::Input;

pub const MAX_CONNECTIONS: usize = 256;
pub const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
pub const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

pub async fn bind(config: Config) -> io::Result<TcpListener> {
    TcpListener::bind(SocketAddr::new(config.bind, config.port)).await
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
    let (reader, mut writer) = stream.into_split();
    let mut input = Input::new(reader);
    let mut session = Session::default();
    loop {
        let (reply, close) = match input.request().await {
            Ok(Some(request)) => {
                let reply = match session.prepare(request) {
                    Action::Reply(reply) => reply,
                    Action::Transaction(database, commands) => {
                        let Some(reply) =
                            while_connected(&mut input, state.transaction(database, commands))
                                .await?
                        else {
                            return Ok(());
                        };
                        reply
                    }
                    Action::Database(database, command) => {
                        let Some(reply) =
                            while_connected(&mut input, state.execute(database, command)).await?
                        else {
                            return Ok(());
                        };
                        reply
                    }
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
        // EOF also cancels slow writes; an already-committed operation is never
        // undone or retried. Queued commands cannot overtake this owned reply.
        if while_connected(&mut input, write_reply(&mut writer, &reply))
            .await?
            .is_none()
        {
            return Ok(());
        }
        if close {
            return Ok(());
        }
    }
}

async fn while_connected<T>(
    input: &mut Input,
    operation: impl Future<Output = io::Result<T>>,
) -> io::Result<Option<T>> {
    tokio::select! {
        biased;
        closed = input.until_closed() => closed.map(|()| None),
        result = operation => result.map(Some),
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
mod blocking_tests;
#[cfg(test)]
mod tests;
