//! Owned ephemeral servers and bounded waits shared by socket suites.

use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use keyper::{config::Config, server};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;

const LIMIT: Duration = Duration::from_secs(5);

pub async fn bounded<F: Future>(future: F) -> F::Output {
    timeout(LIMIT, future).await.expect("test I/O deadline")
}

pub struct TestServer {
    pub address: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<io::Result<()>>>,
}

impl TestServer {
    pub async fn start() -> Self {
        Self::start_on(Config::default().bind).await
    }

    pub async fn start_on(bind: IpAddr) -> Self {
        let listener = server::bind(Config { bind, port: 0 }).await.unwrap();
        let address = listener.local_addr().unwrap();
        let (send, receive) = oneshot::channel();
        let task = tokio::spawn(server::serve(listener, async {
            let _ = receive.await;
            Ok(())
        }));
        Self {
            address,
            shutdown: Some(send),
            task: Some(task),
        }
    }

    pub async fn connect(&self) -> TcpStream {
        bounded(TcpStream::connect(self.address)).await.unwrap()
    }

    pub async fn stop(mut self) {
        self.shutdown.take().unwrap().send(()).unwrap();
        bounded(self.task.as_mut().unwrap()).await.unwrap().unwrap();
        self.task.take();
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        // Abort-on-drop also cleans up if an assertion or bounded wait fails.
        // Connection tasks and their state senders then drop; the owner exits.
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
