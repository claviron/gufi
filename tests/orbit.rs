use super::*;
use claviron::orbit::transport::read_message;
use std::task::Poll;
use tokio::io::{AsyncReadExt, duplex, split};
use tokio::sync::{mpsc, watch};

struct Inbox {
    messages: mpsc::Sender<Message>,
    ended: watch::Sender<bool>,
}
impl Delivery for Inbox {
    async fn deliver(
        &self,
        message: Message,
    ) -> bool {
        self.messages.send(message).await.is_ok()
    }
    fn end(&self) {
        self.ended.send_replace(true);
    }
}
fn inbox() -> (Inbox, mpsc::Receiver<Message>) {
    let (messages, receiver) = mpsc::channel(1);
    (Inbox { messages, ended: watch::Sender::new(false) }, receiver)
}

#[test]
fn original_protocol_selects_each_message_transport() {
    for (version, method, expected) in [
        (http::Version::HTTP_11, "GET", Some(MessageTransport::WebSocket)),
        (http::Version::HTTP_2, "CONNECT", Some(MessageTransport::WebSocket)),
        (
            http::Version::HTTP_3,
            "CONNECT",
            Some(MessageTransport::WebTransport),
        ),
        (http::Version::HTTP_2, "GET", None),
        (http::Version::HTTP_3, "GET", None),
    ] {
        let request = http::Request::builder()
            .version(version)
            .method(method)
            .body(claviron::runner::RequestBody::empty())
            .unwrap();
        assert_eq!(transport(&request), expected);
    }
}

#[tokio::test]
async fn application_close_ends_a_reader_waiting_on_a_partial_record() {
    let (near, mut peer) = duplex(64);
    let (mut input, mut output) = split(near);
    let (inbox, _messages) = inbox();
    let (tx, mut outgoing) = mpsc::channel(1);
    peer.write_all(&[1, 8, 0, 0, 0, b'p']).await.unwrap();
    let close = Message::Close { code: 1000, reason: "done".into() };
    tx.send(close.clone()).await.unwrap();
    relay(&mut input, &mut output, &inbox, &mut outgoing, 64).await;
    assert_eq!(read_message(&mut peer).await.unwrap(), Some(close));
    assert_eq!(read_message(&mut peer).await.unwrap(), None);
    assert!(*inbox.ended.borrow());
}

#[tokio::test]
async fn one_partial_stream_does_not_stop_another_and_fin_keeps_output_open() {
    let (near_a, mut peer_a) = duplex(128);
    let (near_b, mut peer_b) = duplex(128);
    let (mut input_a, mut output_a) = split(near_a);
    let (mut input_b, mut output_b) = split(near_b);
    let (inbox_a, mut received_a) = inbox();
    let (inbox_b, mut received_b) = inbox();
    let (tx_a, mut sent_a) = mpsc::channel(1);
    let (tx_b, mut sent_b) = mpsc::channel(1);
    peer_a.write_all(&[1, 1, 0, 0, 0]).await.unwrap();
    write_message(&mut peer_b, &Message::Text("B".into())).await.unwrap();
    let scenario = async {
        assert_eq!(received_b.recv().await.unwrap(), Message::Text("B".into()));
        assert!(received_a.try_recv().is_err());
        tx_b.send(Message::Binary(vec![1, 2])).await.unwrap();
        assert_eq!(
            read_message(&mut peer_b).await.unwrap(),
            Some(Message::Binary(vec![1, 2]))
        );
        peer_a.write_all(b"A").await.unwrap();
        assert_eq!(received_a.recv().await.unwrap(), Message::Text("A".into()));
        peer_b.shutdown().await.unwrap();
        let mut ended = inbox_b.ended.subscribe();
        ended.wait_for(|ended| *ended).await.unwrap();
        tx_b.send(Message::Text("after FIN".into())).await.unwrap();
        assert_eq!(
            read_message(&mut peer_b).await.unwrap(),
            Some(Message::Text("after FIN".into()))
        );
        drop(tx_a);
        drop(tx_b);
    };
    tokio::join!(
        relay(&mut input_a, &mut output_a, &inbox_a, &mut sent_a, 64),
        relay(&mut input_b, &mut output_b, &inbox_b, &mut sent_b, 64),
        scenario
    );
}

#[tokio::test]
async fn duplex_writes_progress_while_application_receive_is_backpressured() {
    let (near, mut peer) = duplex(64);
    let (mut input, mut output) = split(near);
    let (inbox, _received) = inbox();
    let (tx, mut sent) = mpsc::channel(1);
    write_message(&mut peer, &Message::Text("one".into())).await.unwrap();
    write_message(&mut peer, &Message::Text("two".into())).await.unwrap();
    let mut relay =
        std::pin::pin!(relay(&mut input, &mut output, &inbox, &mut sent, 64));
    poll_fn(|cx| {
        assert!(relay.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    tx.send(Message::Close { code: 1000, reason: String::new() })
        .await
        .unwrap();
    relay.await;
    assert!(matches!(
        read_message(&mut peer).await.unwrap(),
        Some(Message::Close { code: 1000, .. })
    ));
}

#[tokio::test]
async fn oversized_header_stops_the_session_without_waiting_for_payload() {
    let (near, mut peer) = duplex(64);
    let (mut input, mut output) = split(near);
    let (inbox, _received) = inbox();
    let (_tx, mut sent) = mpsc::channel(1);
    peer.write_all(&[2, 255, 255, 255, 255]).await.unwrap();
    relay(&mut input, &mut output, &inbox, &mut sent, 64).await;
    assert!(*inbox.ended.borrow());
    assert!(valid_message(&Message::Binary(vec![0; 65]), 64, false).is_err());
    assert!(
        valid_message(
            &Message::Close { code: 1006, reason: String::new() },
            64,
            false
        )
        .is_err()
    );
    // Drop the writer as the production caller does; no bytes were published.
    drop(output);
    drop(input);
    let mut bytes = Vec::new();
    peer.read_to_end(&mut bytes).await.unwrap();
    assert!(bytes.is_empty());
}
