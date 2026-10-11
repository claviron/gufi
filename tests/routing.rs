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
