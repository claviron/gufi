//! Everything a launch can decide, as one struct.
//!
//! A struct rather than a trait: the runner then has exactly one type to read,
//! and an application fills it however it likes — from the command line, from
//! its own configuration, from environment variables, or by hand. The `cli`
//! feature does not change that; it only adds one more way to fill it.
//!
//! ## Who reads the command line
//!
//! The binary does, not this crate. `Runner::run` never looks at `argv`:
//! a library that did could not be used twice in one process, would read
//! `cargo test`'s own arguments in an integration test, and would hide the
//! decision from the code that made it. So [`GufiOptions::from_args`] exists
//! and is called explicitly — one visible line in the application's `main`,
//! which the starter kit ships already written:
//!
//! ```ignore
//! Runner::new("main:app")
//!     .with_options(GufiOptions::from_args())
//!     .run(app::configure)
//!     .await
//! ```
//!
//! Delete that line and the application decides for itself; nothing else
//! changes.

use std::net::SocketAddr;
use std::path::PathBuf;

/// What a launch decided, in the shape the runner reads.
///
/// Every field is optional in the sense that `None` leaves the runner's own
/// default in place — the working directory's name, `.venv` beneath it, the
/// development listener. A launch states what it cares about and nothing
/// else.
#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "cli", derive(clap::Parser))]
#[cfg_attr(feature = "cli", command(name = "gufi", about = "Run a Python application on Claviron"))]
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

    /// The application root. Defaults to the working directory.
    #[cfg_attr(feature = "cli", arg(long))]
    pub root: Option<PathBuf>,

    /// The Python environment. Defaults to `.venv` under the root.
    #[cfg_attr(feature = "cli", arg(long))]
    pub venv: Option<PathBuf>
}

impl GufiOptions {
    /// Read the command line.
    ///
    /// Called by the application, never by the runner. `--help` and `--version`
    /// come with it.
    #[cfg(feature = "cli")]
    pub fn from_args() -> Self {
        <Self as clap::Parser>::parse()
    }
}
