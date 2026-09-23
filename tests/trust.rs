use super::*;

fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in pairs {
        headers.append(
            HeaderName::from_bytes(name.as_bytes()).unwrap(),
            value.parse::<http::HeaderValue>().unwrap()
        );
    }
    headers
}

fn peer(address: &str) -> Option<SocketAddr> {
    Some(format!("{address}:51000").parse().unwrap())
}

/// The whole point: a client that writes its own `X-Forwarded-For` is not
/// believed, because the socket it came in on is nobody we trust.
#[test]
fn an_untrusted_peer_cannot_choose_its_own_address() {
    let trust = Trust::new(vec!["10.0.0.1".parse().unwrap()]);
    let resolved =
        trust.resolve(peer("203.0.113.9"), &headers(&[("x-forwarded-for", "198.51.100.7")]));
    assert_eq!(resolved.client, peer("203.0.113.9"), "the socket wins");
    assert_eq!(resolved.scheme, None);
}

#[test]
fn a_trusted_proxy_is_believed() {
    let trust = Trust::new(vec!["10.0.0.0/8".parse().unwrap()]);
    let resolved = trust.resolve(
        peer("10.0.0.4"),
        &headers(&[("x-forwarded-for", "198.51.100.7"), ("x-forwarded-proto", "https")])
    );
    assert_eq!(
        resolved.client.map(|client| client.ip().to_string()).as_deref(),
        Some("198.51.100.7")
    );
    assert_eq!(resolved.scheme.as_deref(), Some("https"));
}

/// Two trusted hops append as they go. Walking from the right past our own
/// addresses lands on the client; anything further left was written by
/// whoever spoke first, and that is the client itself.
#[test]
fn trusted_hops_are_stepped_over() {
    let trust = Trust::new(vec!["10.0.0.0/8".parse().unwrap()]);
    let resolved = trust.resolve(
        peer("10.0.0.4"),
        &headers(&[("x-forwarded-for", "198.51.100.7, 10.0.0.9, 10.0.0.4")])
    );
    assert_eq!(
        resolved.client.map(|client| client.ip().to_string()).as_deref(),
        Some("198.51.100.7")
    );
}

/// A Unix socket has no peer address, and reaching it took filesystem
/// permission the operator granted.
#[test]
fn a_unix_socket_is_trusted_by_construction() {
    let trust = Trust::default();
    let resolved = trust.resolve(None, &headers(&[("x-forwarded-for", "198.51.100.7")]));
    assert_eq!(
        resolved.client.map(|client| client.ip().to_string()).as_deref(),
        Some("198.51.100.7")
    );
}

#[test]
fn a_scheme_that_is_not_http_is_ignored() {
    let trust = Trust::default();
    let resolved = trust.resolve(None, &headers(&[("x-forwarded-proto", "gopher")]));
    assert_eq!(resolved.scheme, None);
}
