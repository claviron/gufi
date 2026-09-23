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

/// The host is the reason this matters in practice: without it an application
/// behind a proxy sees the proxy's address in `Host` and rejects its own
/// domain.
#[test]
fn a_trusted_proxy_reports_the_host_the_client_asked_for() {
    let trust = Trust::new(vec!["10.0.0.0/8".parse().unwrap()]);
    let resolved = trust.resolve(
        peer("10.0.0.4"),
        &headers(&[
            ("x-forwarded-for", "198.51.100.7"),
            ("x-forwarded-host", "py.home"),
            ("x-forwarded-port", "443")
        ])
    );
    assert_eq!(resolved.host.as_deref(), Some("py.home"));
    // The port rides on the address: without one it would be the proxy's,
    // which is a number that means something else.
    assert_eq!(resolved.client.map(|client| client.port()), Some(443));
}

/// RFC 7239 is the standard spelling, and a proxy that sends it means it.
#[test]
fn rfc7239_forwarded_wins_over_the_x_family() {
    let trust = Trust::new(vec!["10.0.0.0/8".parse().unwrap()]);
    let resolved = trust.resolve(
        peer("10.0.0.4"),
        &headers(&[
            ("forwarded", "for=198.51.100.7;proto=https;host=py.home"),
            ("x-forwarded-for", "203.0.113.9"),
            ("x-forwarded-proto", "http")
        ])
    );
    assert_eq!(
        resolved.client.map(|client| client.ip().to_string()).as_deref(),
        Some("198.51.100.7")
    );
    assert_eq!(resolved.scheme.as_deref(), Some("https"));
    assert_eq!(resolved.host.as_deref(), Some("py.home"));
}

/// Bracketed IPv6 with a port, as the RFC writes it.
#[test]
fn a_forwarded_ipv6_node_is_read_with_its_port() {
    let trust = Trust::default();
    let resolved = trust.resolve(None, &headers(&[("forwarded", "for=\"[2001:db8::1]:8080\"")]));
    let client = resolved.client.unwrap();
    assert_eq!(client.ip().to_string(), "2001:db8::1");
    assert_eq!(client.port(), 8080);
}

/// `for=_hidden` names no address, so the connection's answer stands.
#[test]
fn an_obfuscated_node_leaves_the_socket_alone() {
    let trust = Trust::new(vec!["10.0.0.0/8".parse().unwrap()]);
    let resolved = trust.resolve(peer("10.0.0.4"), &headers(&[("forwarded", "for=_hidden")]));
    assert_eq!(resolved.client, peer("10.0.0.4"));
}

/// A port with no address to attach it to is left alone: the socket's peer
/// stands, port and all.
#[test]
fn a_forwarded_port_without_an_address_changes_nothing() {
    let trust = Trust::new(vec!["10.0.0.0/8".parse().unwrap()]);
    let resolved = trust.resolve(peer("10.0.0.4"), &headers(&[("x-forwarded-port", "443")]));
    assert_eq!(resolved.client, peer("10.0.0.4"));
}

/// nginx's single-value header, when nothing richer was sent.
#[test]
fn x_real_ip_is_read_when_nothing_else_says() {
    let trust = Trust::new(vec!["10.0.0.0/8".parse().unwrap()]);
    let resolved = trust.resolve(peer("10.0.0.4"), &headers(&[("x-real-ip", "198.51.100.7")]));
    assert_eq!(
        resolved.client.map(|client| client.ip().to_string()).as_deref(),
        Some("198.51.100.7")
    );
}
