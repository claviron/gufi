//! Options for a launch.
//!
//! [`GufiOptions`] holds what a deployment decides: the application's name,
//! which doors it answers on, and whose forwarding headers it believes. The
//! application's own code — routes, providers, services — is unaffected by
//! any of it.
//!
//! Fill it from the command line, from your own configuration, or by hand.
//!
//! # Examples
//!
//! From the command line. `--help` is generated from the fields:
//!
//! ```no_run
//! use gufi::{GufiOptions, Runner};
//!
//! #[tokio::main]
//! async fn main() -> gufi::Result<()> {
//!     Runner::new("main:app")
//!         .with_options(GufiOptions::from_args())
//!         .run(app::configure)
//!         .await
//! }
//! # mod app {
//! #     use claviron::runtime::{AppState, registry};
//! #     pub fn configure(_: &AppState) -> registry::Result<()> { Ok(()) }
//! # }
//! ```
//!
//! ```text
//! $ checkout --application-name checkout --uds /run/checkout.sock --trusted 10.0.0.0/8
//! $ checkout --orbit                       # reached over shared memory
//! $ checkout --tcp 0.0.0.0:8000 --h1       # HTTP/1 only
//! ```
//!
//! Or set them in code, when the deployment is fixed or comes from elsewhere:
//!
//! ```no_run
//! use gufi::{GufiOptions, Runner};
//!
//! let options = GufiOptions {
//!     application_name: Some("checkout".to_owned()),
//!     orbit: true,
//!     ..Default::default()
//! };
//!
//! let runner = Runner::new("main:app").with_options(options);
//! ```
//!
//! A field left unset keeps the runner's default: the working directory's
//! name, `.venv` beneath it, and a listener on `127.0.0.1:8000` when no door
//! is named. Builder methods called after [`Runner::with_options`] take
//! precedence, so a deployment can accept the command line and still pin one
//! value itself.

use std::net::SocketAddr;
use std::path::PathBuf;

/// Options for a launch.
///
/// Every field is optional; an unset field keeps the runner's default. See
/// the [module documentation](self) for examples.
#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "cli", derive(clap::Parser))]
#[cfg_attr(
    feature = "cli",
    command(
        name = "gufi",
        about = "Run a Python application on Claviron",
        long_about = None
    )
)]
pub struct GufiOptions {
    /// What this application is called. The edge routes to this name.
    #[cfg_attr(feature = "cli", arg(short, long))]
    pub application_name: Option<String>,

    /// What this process is called. Defaults to the application name; a
    /// second process serving the same application states its own.
    #[cfg_attr(feature = "cli", arg(short, long))]
    pub instance_name: Option<String>,

    /// Answer through the shared fleet, over shared memory.
    #[cfg_attr(feature = "cli", arg(long))]
    pub orbit: bool,

    /// Answer on TCP. Without an address, on localhost port 8000.
    #[cfg_attr(
        feature = "cli",
        arg(long, num_args = 0..=1, default_missing_value = "127.0.0.1:8000")
    )]
    pub tcp: Option<SocketAddr>,

    /// Answer on a Unix socket at this path.
    #[cfg_attr(feature = "cli", arg(long))]
    pub uds: Option<PathBuf>,

    /// Accept HTTP/1 on the sockets. Naming neither version accepts both.
    #[cfg_attr(feature = "cli", arg(long))]
    pub h1: bool,

    /// Accept HTTP/2 on the sockets, cleartext or otherwise.
    #[cfg_attr(feature = "cli", arg(long))]
    pub h2: bool,

    /// Peers whose forwarding headers to believe: an address or a CIDR,
    /// repeatable. Nobody, unless stated.
    #[cfg_attr(feature = "cli", arg(long = "trusted"))]
    pub trusted: Vec<crate::trust::Network>,

    /// Serve HTTPS on the TCP door with this certificate chain.
    #[cfg(feature = "tls")]
    #[cfg_attr(feature = "cli", arg(long, requires = "tls_key"))]
    pub tls_cert: Option<PathBuf>,

    /// The private key for `--tls-cert`.
    #[cfg(feature = "tls")]
    #[cfg_attr(feature = "cli", arg(long, requires = "tls_cert"))]
    pub tls_key: Option<PathBuf>,

    /// Require a client certificate signed by this authority.
    #[cfg(feature = "tls")]
    #[cfg_attr(feature = "cli", arg(long, requires = "tls_cert"))]
    pub tls_client_ca: Option<PathBuf>,

    /// The application root. Defaults to the working directory.
    #[cfg_attr(feature = "cli", arg(long))]
    pub root: Option<PathBuf>,

    /// The Python environment. Defaults to `.venv` under the root.
    #[cfg_attr(feature = "cli", arg(long))]
    pub venv: Option<PathBuf>
}

impl GufiOptions {
    /// Read these options from the command line.
    ///
    /// Call it from your `main`; the runner never reads `argv` itself.
    /// `--help` and `--version` are generated from the fields.
    ///
    /// Requires the `cli` feature, which is on in the starter kit.
    #[cfg(feature = "cli")]
    pub fn from_args() -> Self {
        <Self as clap::Parser>::parse()
    }
}
