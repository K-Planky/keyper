use tokio::io::{AsyncReadExt, duplex};

use super::*;

#[tokio::test]
async fn owner_panic_stops_even_idle_connections_instead_of_replacing_state() {
    let listener = bind(Config {
        port: 0,
        ..Config::default()
    })
    .await
    .unwrap();
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
