use std::fs;

use super::*;

#[test]
fn target_requires_module_and_attribute() {
    let target = ApplicationTarget::parse(" package.application:app ").unwrap();
    assert_eq!(target.module(), "package.application");
    assert_eq!(target.attribute(), "app");
    assert!(ApplicationTarget::parse("application").is_err());
    assert!(ApplicationTarget::parse(":app").is_err());
    assert!(ApplicationTarget::parse("main:").is_err());
}

#[test]
fn lifespan_is_started_and_shutdown() {
    let directory = tempfile::tempdir().unwrap();
    fs::write(
        directory.path().join("lifespan_fixture.py"),
        include_str!("fixtures/lib/lifespan.py"),
    )
    .unwrap();

    let running = PythonApplication::new("lifespan_fixture:app")
        .unwrap()
        .module_path(directory.path())
        .boot()
        .unwrap();
    running.shutdown().unwrap();
}

#[tokio::test]
async fn shutdown_closes_admission_but_keeps_drain_notifications() {
    let (commands, doorbell) = commands().unwrap();
    let handle = PythonApplicationHandle { commands: commands.clone() };
    commands.send(ApplicationCommand::Shutdown).unwrap();
    let request = || HttpRequest {
        method: "GET".to_owned(),
        scheme: "http".to_owned(),
        http_version: "1.1".to_owned(),
        path: "/".to_owned(),
        raw_path: b"/".to_vec(),
        query_string: Vec::new(),
        headers: Vec::new(),
        client: None,
        server: None,
    };
    assert!(matches!(
        handle.handle_websocket(request()),
        Err(Error::ApplicationStopped)
    ));
    assert!(matches!(
        handle
            .handle_http(request(), http_body_util::Full::new(Bytes::new()))
            .await,
        Err(Error::ApplicationStopped)
    ));
    commands.send(ApplicationCommand::Ready(7)).unwrap();
    // Drop/shutdown may repeat; it must not interpose another stop in drain.
    commands.send(ApplicationCommand::Shutdown).unwrap();
    assert!(matches!(
        doorbell.queue.try_recv(),
        Ok(ApplicationCommand::Shutdown)
    ));
    assert!(matches!(
        doorbell.queue.try_recv(),
        Ok(ApplicationCommand::Ready(7))
    ));
    assert!(matches!(
        doorbell.queue.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
}

#[tokio::test]
async fn http_request_is_dispatched_on_the_application_loop() {
    let directory = tempfile::tempdir().unwrap();
    fs::write(
        directory.path().join("http_fixture.py"),
        include_str!("fixtures/lib/http.py"),
    )
    .unwrap();

    let running = PythonApplication::new("http_fixture:app")
        .unwrap()
        .module_path(directory.path())
        .boot()
        .unwrap();
    let response = running
        .handle()
        .handle_http(
            HttpRequest {
                method: "POST".to_owned(),
                scheme: "http".to_owned(),
                http_version: "1.1".to_owned(),
                path: "/hello".to_owned(),
                raw_path: b"/hello".to_vec(),
                query_string: Vec::new(),
                headers: Vec::new(),
                client: None,
                server: None,
            },
            http_body_util::Full::new(Bytes::from_static(b"request")),
        )
        .await
        .unwrap();
    assert_eq!(response.status, 201);
    assert_eq!(
        response.headers,
        vec![(b"content-type".to_vec(), b"text/plain".to_vec())]
    );
    let body = http_body_util::BodyExt::collect(response.body)
        .await
        .unwrap()
        .to_bytes();
    assert_eq!(body.as_ref(), b"response");
    running.shutdown().unwrap();
}

/// A body the test writes a part at a time.
struct Parts(tokio::sync::mpsc::Receiver<Bytes>);

impl http_body::Body for Parts {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<
        Option<std::result::Result<http_body::Frame<Bytes>, Self::Error>>,
    > {
        self.0
            .poll_recv(context)
            .map(|part| part.map(|part| Ok(http_body::Frame::data(part))))
    }
}

/// The body reaches `receive()` as it arrives rather than collected, and a
/// `receive()` past the body waits for the reply to complete instead of
/// answering `http.disconnect` at once — the early disconnect that ended
/// sse-starlette's streams.
#[tokio::test]
async fn a_request_body_streams_and_disconnect_waits_for_the_reply() {
    let directory = tempfile::tempdir().unwrap();
    fs::write(
        directory.path().join("stream_fixture.py"),
        include_str!("fixtures/lib/stream.py"),
    )
    .unwrap();
    let running = PythonApplication::new("stream_fixture:app")
        .unwrap()
        .module_path(directory.path())
        .boot()
        .unwrap();

    let (parts, body) = tokio::sync::mpsc::channel(1);
    parts.send(Bytes::from_static(b"first")).await.unwrap();
    let handle = running.handle();
    let answering = tokio::spawn(async move {
        handle
            .handle_http(
                HttpRequest {
                    method: "POST".to_owned(),
                    scheme: "http".to_owned(),
                    http_version: "1.1".to_owned(),
                    path: "/upload".to_owned(),
                    raw_path: b"/upload".to_vec(),
                    query_string: Vec::new(),
                    headers: Vec::new(),
                    client: None,
                    server: None,
                },
                Parts(body),
            )
            .await
    });
    // The second part is written only after the first could have been read.
    tokio::task::yield_now().await;
    parts.send(Bytes::from_static(b"second")).await.unwrap();
    drop(parts);

    let response = answering.await.unwrap().unwrap();
    let body = http_body_util::BodyExt::collect(response.body)
        .await
        .unwrap()
        .to_bytes();
    // Two messages carried data, and the receive past the body was still
    // waiting when the reply began.
    assert_eq!(body.as_ref(), b"first|second;waiting");
    running.shutdown().unwrap();
}

fn fixes_request() -> HttpRequest {
    HttpRequest {
        method: "GET".into(),
        scheme: "http".into(),
        http_version: "1.1".into(),
        path: "/".into(),
        raw_path: b"/".to_vec(),
        query_string: vec![],
        headers: vec![],
        client: None,
        server: None,
    }
}

fn fixes_exchange() -> (HttpExchange, oneshot::Receiver<Result<HttpResponse>>) {
    let (commands, _doorbell) = commands().unwrap();
    let feed = BodyFeed {
        id: 100,
        shared: Arc::new(BodyInbound::default()),
        commands,
    };
    let (response, received) = oneshot::channel();
    (HttpExchange::new(fixes_request(), feed, response), received)
}

#[tokio::test]
async fn fixes_stream_terminal_survives_data_pressure_and_exchange_drop() {
    use http_body_util::BodyExt;
    for full in [false, true] {
        for explicit in [false, true] {
            let (exchange, _received) = fixes_exchange();
            let (tx, rx) = chunks::channel(2);
            let terminal = tx.clone().try_reserve_owned().unwrap();
            if full {
                tx.try_send(Ok(Bytes::from_static(b"partial"))).unwrap();
                assert!(matches!(
                    tx.try_send(Ok(Bytes::new())),
                    Err(chunks::error::TrySendError::Full(_))
                ));
            }
            let retained_sender = tx.clone();
            {
                let mut state = exchange.response.lock().unwrap();
                state.sender.take();
                state.status = Some(200);
                state.rest = Some(tx);
                state.terminal = Some(terminal);
            }
            let mut body =
                ResponseStream { first: None, rest: Some(rx), request: None };
            if explicit {
                exchange.fail("synthetic stream failure".into());
            }
            drop(exchange);
            if full {
                assert_eq!(
                    body.frame().await.unwrap().unwrap().into_data().unwrap(),
                    "partial"
                );
            }
            let error = body.frame().await.unwrap().unwrap_err();
            if explicit {
                assert!(matches!(error, Error::InvalidHttpResponse(_)));
            } else {
                assert!(matches!(error, Error::ResponseChannelClosed));
            }
            assert!(http_body::Body::is_end_stream(&body));
            assert!(retained_sender.is_closed());
            assert!(body.frame().await.is_none());
        }
    }
}

#[test]
fn fixes_stream_failure_tolerates_a_departed_reader() {
    let (exchange, _received) = fixes_exchange();
    let (tx, rx) = chunks::channel(2);
    let terminal = tx.clone().try_reserve_owned().unwrap();
    {
        let mut state = exchange.response.lock().unwrap();
        state.sender.take();
        state.status = Some(200);
        state.rest = Some(tx);
        state.terminal = Some(terminal);
    }
    drop(rx);
    exchange.fail("synthetic departed stream".into());
    assert!(exchange.response.lock().unwrap().rest.is_none());
    drop(exchange);
}

#[tokio::test]
async fn fixes_cancelled_final_send_preserves_terminal_failure() {
    let (tx, mut rx) = chunks::channel(2);
    let terminal =
        PendingResponseEnd(Some(tx.clone().try_reserve_owned().unwrap()));
    tx.try_send(Ok(Bytes::from_static(b"partial"))).unwrap();
    drop(terminal);
    drop(tx);
    assert!(rx.recv().await.unwrap().is_ok());
    assert!(matches!(rx.recv().await, Some(Err(Error::ResponseChannelClosed))));
    assert!(rx.recv().await.is_none());
}
