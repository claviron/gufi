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
//! $ checkout --name checkout --uds /run/checkout.sock --trusted 10.0.0.0/8
//! $ checkout --orbit                       # reached over shared memory
//! $ checkout --tcp 0.0.0.0:8000 --h1       # HTTP/1 only
//! ```
//!
//! # From a file
//!
//! A deployment outgrows a command line. Write the same fields into
//! `.gufi/config.yaml` beside your application and start it with no
//! arguments at all:
//!
//! ```yaml
//! name: checkout
//! orbit: true
//! workers: 4
//! trusted:
//!   - 10.0.0.0/8
//! ```
//!
//! ```text
//! $ checkout                        # reads ./.gufi/config.yaml
//! $ checkout --config /etc/checkout # reads it from there instead
//! ```
//!
//! `.gufi` is also where Claviron reads what it owns, so `supervisor.yaml` —
//! rotation, recovery, heartbeat, every deadline a worker fleet runs on —
//! sits beside it. That is the whole reason for a directory rather than a
//! file: asking for `workers: 4` is asking for a supervisor, and a
//! supervisor's policy is not a flag.
//!
//! It is hidden, and beside `.venv` rather than inside it, because a Python
//! project's own `config` may already belong to its framework.
//!
//! The command line wins over the file wherever it said anything, so one
//! deployment can be pinned in the file and probed from the shell.
//!
//! Or set them in code, when the deployment is fixed or comes from elsewhere:
//!
//! ```no_run
//! use gufi::{GufiOptions, Runner};
//!
//! let options = GufiOptions {
//!     name: Some("checkout".to_owned()),
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
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
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
    /// What this application is called. The edge routes to it.
    ///
    /// Defaults to the name of the application root, which for a Python
    /// application is the directory its scripts and `.venv` are in. State it
    /// when the two differ — a directory named for the repository serving an
    /// application named for what it does.
    #[cfg_attr(feature = "cli", arg(short, long))]
    pub name: Option<String>,

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
    /// Carried as the text it was written as, so encoding a launch and
    /// reading it back yields what the operator typed.
    #[cfg_attr(feature = "cli", arg(long = "trusted"))]
    #[serde(with = "networks")]
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
    pub venv: Option<PathBuf>,

    /// How many worker processes serve this application. Zero is standalone.
    ///
    /// Each worker is a process with an interpreter of its own, which is how
    /// Python scales — one interpreter runs one thread of bytecode at a time.
    /// The master serves no traffic: it holds no lane and binds no socket.
    #[cfg_attr(feature = "cli", arg(short, long))]
    pub workers: Option<u8>,

    /// Where this deployment is written down. Defaults to [`CONFIG_FILE`]
    /// beside the application.
    ///
    /// Not part of the launch itself — it is where the launch was read
    /// from — so it is neither written into the file nor carried to a
    /// worker, which is handed the resolved launch instead.
    #[cfg_attr(feature = "cli", arg(short, long))]
    #[serde(skip)]
    pub config: Option<PathBuf>,

    /// How the worker fleet is supervised: rotation, recovery, heartbeat,
    /// and the deadlines a restart runs on.
    ///
    /// Only consulted when `workers` asks for a fleet, and every field has a
    /// built-in value, so most deployments leave the whole section out.
    /// Written here rather than in a `supervisor.yaml` of its own because
    /// one application is one file.
    #[cfg_attr(feature = "cli", arg(skip))]
    pub supervisor: Option<claviron::supervisor::SupervisorConfig>
}

/// What gufi calls its file beside an application.
pub const CONFIG_FILE: &str = "gufi.yaml";

impl GufiOptions {
    /// Read these options from the command line and the file beside the
    /// application, the command line winning.
    ///
    /// Call it from your `main`; the runner never reads `argv` itself.
    /// `--help` and `--version` are generated from the fields, and a
    /// `gufi.yaml` that cannot be read is reported the same way a bad flag
    /// is — a launch states what it wants, and a launch that states
    /// something impossible does not start.
    ///
    /// Requires the `cli` feature, which is on in the starter kit.
    #[cfg(feature = "cli")]
    pub fn from_args() -> Self {
        use clap::CommandFactory;

        match <Self as clap::Parser>::parse().resolve() {
            Ok(options) => options,
            Err(message) => {
                Self::command().error(clap::error::ErrorKind::InvalidValue, message).exit()
            }
        }
    }

    /// Where this deployment is written down: [`CONFIG_FILE`] beside the
    /// application, or the file `--config` names.
    ///
    /// A Python application is not compiled, so its process stands in the
    /// directory its scripts are in — the root is what the application *is*,
    /// not where someone happened to be. One file goes there, beside
    /// `pyproject.toml` and `.venv`.
    ///
    /// Absent, it is simply absent: an application nobody configured runs on
    /// its defaults.
    pub fn config_path(&self) -> PathBuf {
        match &self.config {
            Some(path) => path.clone(),
            None => self.root.clone().unwrap_or_else(|| PathBuf::from(".")).join(CONFIG_FILE)
        }
    }

    /// Read [`CONFIG_FILE`] from the configuration directory and let these
    /// options stand over what it says.
    ///
    /// A missing file is not an error — an application that was never
    /// configured runs on its defaults. A named `--config` directory that
    /// does not exist is one, because it was asked for by name.
    ///
    /// The command line wins wherever it said anything. A flag can open a
    /// door the file left shut; closing one the file opened is an edit to
    /// the file, because a flag that is absent and a flag that is `false`
    /// are the same three characters of argv.
    pub fn resolve(self) -> Result<Self, String> {
        let path = self.config_path();
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && self.config.is_none() => {
                return Ok(self);
            }
            Err(error) => return Err(format!("read {}: {error}", path.display()))
        };
        let written: Self = serde_yaml::from_str(&text)
            .map_err(|error| format!("parse {}: {error}", path.display()))?;
        let mut resolved = self.over(written);
        resolved.config = Some(path);
        Ok(resolved)
    }

    /// These options over those, field by field.
    fn over(
        self,
        written: Self
    ) -> Self {
        Self {
            name: self.name.or(written.name),
            orbit: self.orbit || written.orbit,
            tcp: self.tcp.or(written.tcp),
            uds: self.uds.or(written.uds),
            h1: self.h1 || written.h1,
            h2: self.h2 || written.h2,
            // Stated rather than merged: a list of peers to believe is one
            // decision, and a command line that names any is the whole of it.
            trusted: if self.trusted.is_empty() { written.trusted } else { self.trusted },
            #[cfg(feature = "tls")]
            tls_cert: self.tls_cert.or(written.tls_cert),
            #[cfg(feature = "tls")]
            tls_key: self.tls_key.or(written.tls_key),
            #[cfg(feature = "tls")]
            tls_client_ca: self.tls_client_ca.or(written.tls_client_ca),
            root: self.root.or(written.root),
            venv: self.venv.or(written.venv),
            workers: self.workers.or(written.workers),
            // Policy is written down, never typed: no flag can have stated it.
            supervisor: written.supervisor,
            config: self.config
        }
    }
}

impl GufiOptions {
    /// Encode these options for a worker to inherit.
    ///
    /// A worker is started by the supervisor with no command line — an
    /// embedding binary may define any flags it likes and the supervisor has
    /// no business parsing them — so whatever the launch resolved travels in
    /// the bootstrap envelope instead. The encoding is deterministic: the
    /// same options produce the same bytes, which is what lets a master and
    /// its workers be compared at all.
    pub fn encode(&self) -> Result<Vec<u8>, String> {
        serde_json::to_vec(self).map_err(|error| error.to_string())
    }

    /// Read back what a master resolved.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        serde_json::from_slice(bytes).map_err(|error| error.to_string())
    }
}

/// What this application is called when nobody said: the root's own name.
///
/// Python is not compiled, so the process runs where the scripts are and the
/// root is the application rather than a place it was started from. One
/// directory, one virtual environment, one name.
pub(crate) fn default_name(root: &std::path::Path) -> Option<String> {
    root.file_name()?.to_str().filter(|name| !name.is_empty()).map(ToOwned::to_owned)
}

/// `Network` is parsed from text and prints back to it; carrying the text is
/// both smaller and closer to what was meant than carrying the parsed halves.
mod networks {
    use serde::{Deserialize, Deserializer, Serializer};

    use crate::trust::Network;

    pub(super) fn serialize<S: Serializer>(
        networks: &[Network],
        serializer: S
    ) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(networks.iter().map(ToString::to_string))
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D
    ) -> Result<Vec<Network>, D::Error> {
        Vec::<String>::deserialize(deserializer)?
            .into_iter()
            .map(|text| text.parse().map_err(serde::de::Error::custom))
            .collect()
    }
}

#[cfg(test)]
#[path = "../tests/options.rs"]
mod tests;
