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
//! ## Why this does not exist on the Orbit path
//!
//! There the edge writes `client` and `scheme` as typed fields of the
//! exchange, and the only writer is a process in the fleet. The header dance
//! exists because HTTP has no way to say "this is the real client" that a
//! client cannot also say.

use std::net::{IpAddr, SocketAddr};

use http::HeaderMap;
use http::header::HeaderName;

const X_FORWARDED_FOR: HeaderName = HeaderName::from_static("x-forwarded-for");
const X_FORWARDED_PROTO: HeaderName = HeaderName::from_static("x-forwarded-proto");

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

    /// The client and scheme to report, given who connected.
    ///
    /// `peer` is `None` for a Unix socket, which is trusted by construction.
    /// An untrusted peer's headers are ignored entirely — not partly, and not
    /// with a warning that nobody reads.
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
            return Resolved { client: peer, scheme: None };
        }

        Resolved { client: self.client(peer, headers), scheme: scheme(headers) }
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

        // The port is the proxy's, not the client's, and nothing reports a
        // forwarded port reliably. Zero says so rather than repeating a number
        // that means something else.
        client.map(|address| SocketAddr::new(address, 0)).or(peer)
    }
}

/// What the connection and its headers together say.
pub struct Resolved {
    pub client: Option<SocketAddr>,
    /// `None` leaves the listener's own answer in place.
    pub scheme: Option<String>
}

fn scheme(headers: &HeaderMap) -> Option<String> {
    let value = headers.get(X_FORWARDED_PROTO)?.to_str().ok()?.trim();
    let value = value.split(',').next()?.trim().to_ascii_lowercase();
    (value == "http" || value == "https").then_some(value)
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

#[cfg(test)]
#[path = "../tests/trust.rs"]
mod tests;
