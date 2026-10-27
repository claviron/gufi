use super::*;

#[tokio::test]
async fn stopped_python_service_returns_unavailable_without_booting_python() {
    let mut python = PythonService::new(Arc::new(RwLock::new(None)));
    let request = || Request::new(Full::new(Bytes::new()));
    assert_eq!(
        python.handle(request()).await.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        python.call(request()).await.unwrap().status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test]
async fn fixes_asgi_framing_header_is_ignored_without_losing_application_headers()
 {
    for name in [
        b"transfer-encoding".as_slice(),
        b"Transfer-Encoding",
        b"TRANSFER-ENCODING",
    ] {
        let response = response_from_asgi(
            200,
            vec![
                (name.to_vec(), b"gzip".to_vec()),
                // Even an invalid transfer-encoding value is ignored before parsing.
                (name.to_vec(), vec![0]),
                (b"content-encoding".to_vec(), b"identity".to_vec()),
                (b"set-cookie".to_vec(), b"first=1".to_vec()),
                (b"set-cookie".to_vec(), b"second=2".to_vec()),
            ],
            full_body(Bytes::from_static(b"plain")),
        )
        .unwrap();
        assert!(
            !response.headers().contains_key(http::header::TRANSFER_ENCODING)
        );
        assert_eq!(
            response.headers()[http::header::CONTENT_ENCODING],
            "identity"
        );
        let cookies: Vec<_> = response
            .headers()
            .get_all(http::header::SET_COOKIE)
            .iter()
            .map(|v| v.to_str().unwrap())
            .collect();
        assert_eq!(cookies, ["first=1", "second=2"]);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "plain"
        );
    }
    assert!(
        response_from_asgi(
            200,
            vec![(b"set-cookie".to_vec(), vec![0])],
            full_body(Bytes::new())
        )
        .is_err()
    );
}
