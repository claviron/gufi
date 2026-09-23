//! Whether to believe what the server in front says.
//!
//! This is the one policy a policy-free listener cannot avoid. Behind a proxy
//! the socket's peer is the proxy, and the real client is in a header — which
//! anyone can write. Believing it always lets a client choose its own address;
//! believing it never puts the proxy's address in every log and every rate
//! limit. So the launch says whose word counts.
//!
//! Nothing is trusted by default. A Unix socket is the exception: reaching it
//! at all requires the filesystem permission to open it, so the peer is
//! already someone the operator let in.
//!
//! ## Where the answer comes from
//!
//! From the registry, like everything else this runner resolves. The launch
//! can put one there — `--trusted` is a convenience that does exactly that —
//! and an application that wants more than a list of networks registers its
//! own in `configure` before the runner would. Nothing here reads a
//! configuration file: this crate has no configuration format and is not
//! going to grow one, because every policy that needed a flag would need a
//! parser and a document beside it.
//!
//! ## Why this does not exist on the Orbit path
//!
//! There the edge writes `client` and `scheme` as typed fields of the
//! exchange, and the only writer is a process in the fleet. The header dance
//! exists because HTTP has no way to say "this is the real client" that a
//! client cannot also say.

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

/// Whose forwarding headers this process believes.
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

    /// What the connection and its headers together say, given who connected.
    ///
    /// `peer` is `None` for a Unix socket, which is trusted by construction.
    /// An untrusted peer's headers are ignored entirely — not partly, and not
    /// with a warning that nobody reads.
    ///
    /// Every header a proxy is known to send is read, not just the address:
    /// a scheme the application needs for secure cookies and redirects, and a
    /// host it needs for URL generation and for `ALLOWED_HOSTS`. Reading the
    /// address and dropping the host is how an application behind a proxy
    /// starts answering 400 to its own domain.
    ///
    /// `Forwarded` (RFC 7239) wins over the `X-Forwarded-*` family when both
    /// are present: it is the standard one, and a proxy that sends it is
    /// saying so deliberately.
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

/// One trusted peer, or a prefix of them.
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

/// Resolved from the registry by the listener; registered by the launch, or
/// by the application before it.
impl Provider<AppState> for Trust {
    fn name(&self) -> &'static str {
        "Trust"
    }
}

#[cfg(test)]
#[path = "../tests/trust.rs"]
mod tests;
