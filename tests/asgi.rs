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
        include_str!("fixtures/lib/lifespan.py")
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
async fn http_request_is_dispatched_on_the_application_loop() {
    let directory = tempfile::tempdir().unwrap();
    fs::write(directory.path().join("http_fixture.py"), include_str!("fixtures/lib/http.py"))
        .unwrap();

    let running = PythonApplication::new("http_fixture:app")
        .unwrap()
        .module_path(directory.path())
        .boot()
        .unwrap();
    let response = running
        .handle()
        .handle_http(HttpRequest {
            method: "POST".to_owned(),
            scheme: "http".to_owned(),
            http_version: "1.1".to_owned(),
            path: "/hello".to_owned(),
            raw_path: b"/hello".to_vec(),
            query_string: Vec::new(),
            headers: Vec::new(),
            body: b"request".to_vec(),
            client: None,
            server: None
        })
        .await
        .unwrap();
    assert_eq!(response.status, 201);
    assert_eq!(response.headers, vec![(b"content-type".to_vec(), b"text/plain".to_vec())]);
    assert_eq!(response.body, b"response");
    running.shutdown().unwrap();
}
