//! Which proxy to believe.
//!
//! Behind a reverse proxy the socket's peer is the proxy, and the client's
//! real address, scheme and host arrive in headers — which anyone can send.
//! [`Trust`] names the peers whose headers count. Nothing is believed unless
//! you say so.
//!
//! When the peer is trusted, all of these are read and applied to the request
//! the application sees:
//!
//! | header | effect |
//! |---|---|
//! | `Forwarded` (RFC 7239) | client address, scheme and host; preferred when present |
//! | `X-Forwarded-For` | client address |
//! | `X-Forwarded-Proto` | `https` or `http` in the ASGI scope |
//! | `X-Forwarded-Host` | replaces `Host`, so `ALLOWED_HOSTS` sees the real domain |
//! | `X-Forwarded-Port` | the client's port, when its address is known too |
//! | `X-Real-IP` | client address, when nothing richer was sent |
//!
//! A Unix socket is always trusted: opening it already required the
//! filesystem permission you granted.
//!
//! # Examples
//!
//! From the launch:
//!
//! ```text
//! $ checkout --uds /run/checkout.sock --trusted 10.0.0.0/8
//! ```
//!
//! Or in `configure`, when the list comes from somewhere of your own:
//!
//! ```no_run
//! use std::sync::Arc;
//!
//! use claviron::runtime::{AppState, registry};
//! use gufi::Trust;
//!
//! pub fn configure(state: &AppState) -> registry::Result<()> {
//!     state.registry_ref().insert(Arc::new(Trust::new(vec![
//!         "10.0.0.0/8".parse().expect("network"),
//!         "::1".parse().expect("address"),
//!     ])))?;
//!     Ok(())
//! }
//! ```
//!
//! A `Trust` registered in `configure` replaces whatever the launch asked
//! for.
//!
//! An untrusted peer's forwarding headers are ignored completely — the
//! connection's own address and scheme are reported instead.

use std::net::{IpAddr, SocketAddr};

use claviron::runtime::{AppState, Provider};
use http::HeaderMap;
use http::header::HeaderName;

const FORWARDED: HeaderName = HeaderName::from_static("forwarded");
const X_FORWARDED_FOR: HeaderName = HeaderName::from_static("x-forwarded-for");
const X_FORWARDED_PROTO: HeaderName = HeaderName::from_static("x-forwarded-proto");
const X_FORWARDED_HOST: HeaderName = HeaderName::from_static("x-forwarded-host");
const X_FORWARDED_PORT: HeaderName = HeaderName::from_static("x-forwarded-port");
const X_REAL_IP: HeaderName = HeaderName::from_static("x-real-ip");

/// The peers whose forwarding headers this process believes.
///
/// See the [module documentation](self) for what is read and applied.
#[derive(Clone, Debug, Default)]
pub struct Trust {
    /// Peers whose word is taken, as addresses or CIDR prefixes.
    networks: Vec<Network>
}

impl Trust {
    pub fn new(networks: Vec<Network>) -> Self {
        Self { networks }
    }

    pub fn is_empty(&self) -> bool {
        self.networks.is_empty()
    }

    fn admits(
        &self,
        address: IpAddr
    ) -> bool {
        self.networks.iter().any(|network| network.contains(address))
    }

    /// Resolve the client, scheme and host for one request.
    ///
    /// `peer` is the connection's address, or `None` for a Unix socket. When
    /// the peer is not trusted the headers are ignored and the connection's
    /// own answer is returned.
    pub fn resolve(
        &self,
        peer: Option<SocketAddr>,
        headers: &HeaderMap
    ) -> Resolved {
        let trusted = match peer {
            None => true,
            Some(peer) => self.admits(peer.ip())
        };
        if !trusted {
            return Resolved { client: peer, scheme: None, host: None };
        }

        let hops = parse_forwarded(headers);
        if !hops.is_empty() {
            let nearest = hops
                .iter()
                .rev()
                .find(|hop| hop.address.is_none_or(|address| !self.admits(address)))
                .or_else(|| hops.first());
            return Resolved {
                client: nearest
                    .and_then(|hop| hop.address)
                    .map(|address| {
                        SocketAddr::new(address, nearest.and_then(|hop| hop.port).unwrap_or(0))
                    })
                    .or(peer),
                scheme: hops
                    .iter()
                    .rev()
                    .find_map(|hop| hop.proto.clone())
                    .filter(|proto| is_http(proto)),
                host: hops.iter().rev().find_map(|hop| hop.host.clone())
            };
        }

        Resolved {
            client: self.client(peer, headers),
            scheme: single(headers, &X_FORWARDED_PROTO).filter(|proto| is_http(proto)),
            host: single(headers, &X_FORWARDED_HOST)
        }
    }

    /// Walk `X-Forwarded-For` from the right, stepping over hops this process
    /// trusts. The first address that is not one of ours is the client as the
    /// nearest trusted hop saw it; entries to the left of it were written by
    /// someone we have no reason to believe.
    fn client(
        &self,
        peer: Option<SocketAddr>,
        headers: &HeaderMap
    ) -> Option<SocketAddr> {
        let forwarded: Vec<IpAddr> = headers
            .get_all(X_FORWARDED_FOR)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .flat_map(|value| value.split(','))
            .filter_map(|entry| entry.trim().parse::<IpAddr>().ok())
            .collect();

        let client = forwarded
            .iter()
            .rev()
            .find(|address| !self.admits(**address))
            .or_else(|| forwarded.first())
            .copied();

        // A port only if the proxy said one: `X-Forwarded-Port` is the
        // client's, while the socket's is the proxy's. Zero says "not known"
        // rather than repeating a number that means something else.
        let port = single(headers, &X_FORWARDED_PORT)
            .and_then(|value| value.parse::<u16>().ok())
            .unwrap_or(0);
        client
            .or_else(|| single(headers, &X_REAL_IP).and_then(|value| value.parse().ok()))
            .map(|address| SocketAddr::new(address, port))
            .or(peer)
    }
}

/// What the connection and its headers together say.
pub struct Resolved {
    pub client: Option<SocketAddr>,
    /// `None` leaves the listener's own answer in place.
    pub scheme: Option<String>,
    /// The host the client asked for, when a trusted proxy reported one. The
    /// listener replaces `Host` with it, which is what trusting the proxy
    /// means — an application reads `Host`, and behind a proxy the socket's is
    /// the proxy's own.
    pub host: Option<String>
}

fn is_http(value: &str) -> bool {
    value == "http" || value == "https"
}

/// The first value of a header, lowercased and trimmed of a comma list.
fn single(
    headers: &HeaderMap,
    name: &HeaderName
) -> Option<String> {
    let value = headers.get(name)?.to_str().ok()?;
    let value = value.split(',').next()?.trim();
    (!value.is_empty()).then(|| value.to_ascii_lowercase())
}

/// One hop as RFC 7239 describes it.
#[derive(Debug, Default)]
struct Hop {
    address: Option<IpAddr>,
    port: Option<u16>,
    proto: Option<String>,
    host: Option<String>
}

/// Parse `Forwarded: for=192.0.2.60;proto=http;by=203.0.113.43, for="[2001:db8::1]:8080"`.
///
/// Quoted values, bracketed IPv6 and an optional port, in the order the hops
/// were added — leftmost first, as the header is written.
fn parse_forwarded(headers: &HeaderMap) -> Vec<Hop> {
    let mut hops = Vec::new();
    for value in headers.get_all(FORWARDED) {
        let Ok(value) = value.to_str() else { continue };
        for element in value.split(',') {
            let mut hop = Hop::default();
            let mut seen = false;
            for pair in element.split(';') {
                let Some((key, raw)) = pair.split_once('=') else { continue };
                let key = key.trim().to_ascii_lowercase();
                let raw = raw.trim().trim_matches('"').trim();
                if raw.is_empty() {
                    continue;
                }
                seen = true;
                match key.as_str() {
                    "for" => {
                        let (address, port) = split_node(raw);
                        hop.address = address;
                        hop.port = port;
                    }
                    "proto" => hop.proto = Some(raw.to_ascii_lowercase()),
                    "host" => hop.host = Some(raw.to_ascii_lowercase()),
                    _ => {}
                }
            }
            if seen {
                hops.push(hop);
            }
        }
    }
    hops
}

/// `192.0.2.60`, `192.0.2.60:8080`, `[2001:db8::1]`, `[2001:db8::1]:8080`,
/// and the obfuscated `_hidden` / `unknown` forms, which name no address.
fn split_node(raw: &str) -> (Option<IpAddr>, Option<u16>) {
    if let Some(rest) = raw.strip_prefix('[') {
        let (address, rest) = match rest.split_once(']') {
            Some(parts) => parts,
            None => (rest, "")
        };
        let port = rest.strip_prefix(':').and_then(|port| port.parse().ok());
        return (address.parse().ok(), port);
    }
    match raw.rsplit_once(':') {
        Some((address, port)) if !address.contains(':') => {
            (address.parse().ok(), port.parse().ok())
        }
        _ => (raw.parse().ok(), None)
    }
}

/// One trusted peer, or a network of them.
///
/// Parsed from `10.0.0.1`, `10.0.0.0/8`, `::1` or `fd00::/8`; a bare address
/// means that address alone.
#[derive(Clone, Copy, Debug)]
pub struct Network {
    address: IpAddr,
    prefix: u8
}

impl Network {
    fn contains(
        &self,
        address: IpAddr
    ) -> bool {
        match (self.address, address) {
            (IpAddr::V4(network), IpAddr::V4(candidate)) => {
                matches(&network.octets(), &candidate.octets(), self.prefix)
            }
            (IpAddr::V6(network), IpAddr::V6(candidate)) => {
                matches(&network.octets(), &candidate.octets(), self.prefix)
            }
            _ => false
        }
    }
}

fn matches(
    network: &[u8],
    candidate: &[u8],
    prefix: u8
) -> bool {
    let whole = (prefix / 8) as usize;
    let rest = prefix % 8;
    if network[..whole] != candidate[..whole] {
        return false;
    }
    if rest == 0 {
        return true;
    }
    let mask = 0xffu8 << (8 - rest);
    network[whole] & mask == candidate[whole] & mask
}

impl std::str::FromStr for Network {
    type Err = String;

    /// `10.0.0.1`, `10.0.0.0/8`, `::1`, `fd00::/8`. A bare address is itself.
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let (address, prefix) = match text.split_once('/') {
            Some((address, prefix)) => (
                address,
                Some(prefix.parse::<u8>().map_err(|_| format!("bad prefix length in '{text}'"))?)
            ),
            None => (text, None)
        };
        let address: IpAddr =
            address.parse().map_err(|_| format!("'{text}' is not an address or a network"))?;
        let bits = if address.is_ipv4() { 32 } else { 128 };
        let prefix = prefix.unwrap_or(bits);
        if prefix > bits {
            return Err(format!("prefix /{prefix} does not fit {address}"));
        }
        Ok(Self { address, prefix })
    }
}

impl Provider<AppState> for Trust {
    fn name(&self) -> &'static str {
        "Trust"
    }
}

#[cfg(test)]
#[path = "../tests/trust.rs"]
mod tests;
