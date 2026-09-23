//! Serving TLS on a TCP door.
//!
//! A certificate and its key are all that is required; supplying them is what
//! turns a TCP door into an HTTPS one. There is no SNI here and no certificate
//! store: this runner serves one application, so it serves one certificate.
//!
//! # Mutual TLS
//!
//! On a hop between a proxy and the application behind it, encryption alone
//! is usually ceremony — the link is a loopback or a private network, and a
//! Unix socket is already bounded by filesystem permission. What earns its
//! place is the other half: requiring the client to prove who it is, so only
//! your edge can connect at all.
//!
//! Give a CA and every connection must present a certificate it signed:
//!
//! ```text
//! $ checkout --tcp 0.0.0.0:8443 \
//!     --tls-cert /etc/checkout/server.pem \
//!     --tls-key  /etc/checkout/server.key \
//!     --tls-client-ca /etc/checkout/edge-ca.pem
//! ```
//!
//! # Renewal
//!
//! The certificate is read once, at boot. Renewing it means restarting the
//! process — which is what a certificate renewal already does to most
//! upstreams, and is stated here rather than discovered.

use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{RootCertStore, ServerConfig};

/// The certificate a door serves, and optionally the CA a client must be
/// signed by.
#[derive(Clone, Debug)]
pub struct Tls {
    pub certificate: PathBuf,
    pub key: PathBuf,
    /// Require a client certificate signed by this CA. Without it, clients
    /// are not asked for one.
    pub client_ca: Option<PathBuf>
}

impl Tls {
    pub fn new(
        certificate: impl Into<PathBuf>,
        key: impl Into<PathBuf>
    ) -> Self {
        Self { certificate: certificate.into(), key: key.into(), client_ca: None }
    }

    /// Require a client certificate signed by this authority.
    pub fn client_ca(
        mut self,
        path: impl Into<PathBuf>
    ) -> Self {
        self.client_ca = Some(path.into());
        self
    }

    /// Build the acceptor, reading every file named here.
    pub(crate) fn acceptor(&self) -> Result<TlsAcceptor, String> {
        let chain = certificates(&self.certificate)?;
        let key = key(&self.key)?;

        let config = match &self.client_ca {
            None => ServerConfig::builder().with_no_client_auth(),
            Some(path) => {
                let mut roots = RootCertStore::empty();
                for certificate in certificates(path)? {
                    roots
                        .add(certificate)
                        .map_err(|error| format!("{}: {error}", path.display()))?;
                }
                let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
                    .build()
                    .map_err(|error| format!("{}: {error}", path.display()))?;
                ServerConfig::builder().with_client_cert_verifier(verifier)
            }
        };

        let config = config
            .with_single_cert(chain, key)
            .map_err(|error| format!("certificate and key do not match: {error}"))?;
        Ok(TlsAcceptor::from(Arc::new(config)))
    }
}

fn certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>, String> {
    let file = std::fs::File::open(path).map_err(|error| format!("{}: {error}", path.display()))?;
    let certificates: Result<Vec<_>, _> =
        rustls_pemfile::certs(&mut BufReader::new(file)).collect();
    let certificates = certificates.map_err(|error| format!("{}: {error}", path.display()))?;
    if certificates.is_empty() {
        return Err(format!("{}: no certificate in this file", path.display()));
    }
    Ok(certificates)
}

fn key(path: &Path) -> Result<PrivateKeyDer<'static>, String> {
    let file = std::fs::File::open(path).map_err(|error| format!("{}: {error}", path.display()))?;
    rustls_pemfile::private_key(&mut BufReader::new(file))
        .map_err(|error| format!("{}: {error}", path.display()))?
        .ok_or_else(|| format!("{}: no private key in this file", path.display()))
}
