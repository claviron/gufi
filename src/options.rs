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

use std::path::PathBuf;

pub use claviron::runner::RunnerOptions;

/// Options for a launch.
///
/// Every field is optional; an unset field keeps the runner's default. What
/// every Claviron runner's launch says alike is [`RunnerOptions`], flat in the
/// same file beside gufi's own keys. See the [module documentation](self) for
/// examples.
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
    /// The name, doors, trust, request policy, static mounts, watching and
    /// workers.
    #[cfg_attr(feature = "cli", command(flatten))]
    pub runner: RunnerOptions,

    /// The application root. Defaults to the working directory.
    #[cfg_attr(feature = "cli", arg(long))]
    pub root: Option<PathBuf>,

    /// The Python environment. Defaults to `.venv` under the root.
    #[cfg_attr(feature = "cli", arg(long))]
    pub venv: Option<PathBuf>,

    /// Broadcasting in this process: the application `claviron.broadcast`
    /// publishes to, and Django Channels' layer. Written in the file only;
    /// the applications' ids, keys and secrets are `broadcast.yaml`.
    #[cfg(feature = "broadcast")]
    #[cfg_attr(feature = "cli", arg(skip))]
    pub broadcasting: Option<Broadcasting>,

    /// Where this deployment is written down. Defaults to [`CONFIG_FILE`]
    /// beside the application.
    ///
    /// Not part of the launch itself — it is where the launch was read
    /// from — so it is neither written into the file nor carried to a
    /// worker, which is handed the resolved launch instead.
    #[cfg_attr(feature = "cli", arg(short, long))]
    pub config: Option<PathBuf>
}

/// gufi's own keys, beside the runner's in the same flat file.
#[derive(Default, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Own {
    root: Option<PathBuf>,
    venv: Option<PathBuf>,
    #[cfg(feature = "broadcast")]
    broadcasting: Option<Broadcasting>
}

impl serde::Serialize for GufiOptions {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S
    ) -> Result<S::Ok, S::Error> {
        use serde::ser::Error;
        let own = Own {
            root: self.root.clone(),
            venv: self.venv.clone(),
            #[cfg(feature = "broadcast")]
            broadcasting: self.broadcasting.clone()
        };
        let shared = serde_yaml::to_value(&self.runner).map_err(S::Error::custom)?;
        let own = serde_yaml::to_value(&own).map_err(S::Error::custom)?;
        claviron::runner::options::merge(shared, own).serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for GufiOptions {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        let file = serde_yaml::Value::deserialize(deserializer)?;
        let (shared, own) = claviron::runner::options::split(file).map_err(D::Error::custom)?;
        let runner: RunnerOptions = serde_yaml::from_value(shared).map_err(D::Error::custom)?;
        // A key neither side knows lands here; the list of what is known is
        // both halves, or the message would point away from the fix.
        let own: Own = serde_yaml::from_value(own).map_err(|error| {
            D::Error::custom(format!(
                "{error}, or one of the runner's: {}",
                RunnerOptions::KEYS.join(", ")
            ))
        })?;
        Ok(Self {
            runner,
            root: own.root,
            venv: own.venv,
            #[cfg(feature = "broadcast")]
            broadcasting: own.broadcasting,
            config: None
        })
    }
}

/// The `broadcasting:` section of the launch.
#[cfg(feature = "broadcast")]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Broadcasting {
    /// The broadcast application `claviron.broadcast(...)` publishes to when
    /// a call names none.
    pub application: Option<String>,
    /// Django Channels' channel layer, `claviron.channels.OrbitChannelLayer`,
    /// over the broadcast service's groups. Absent, the layer is not
    /// installed.
    #[cfg(feature = "django")]
    pub channels: Option<Channels>
}

/// `broadcasting.channels:` — the limits of every consumer's box. Unwritten
/// means no limit, as Channels' own `CONFIG` would otherwise decide.
#[cfg(feature = "django")]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Channels {
    /// Messages a box holds before a direct send to it is refused
    /// (`ChannelFull`) and a group send passes it over.
    pub capacity: Option<usize>,
    /// How old a message may be when it is received; older ones are dropped
    /// then. Written as a duration, `"60s"`.
    #[serde(
        serialize_with = "serialize_expiry",
        deserialize_with = "claviron::helpers::de::deserialize_optional_duration"
    )]
    pub expiry: Option<std::time::Duration>
}

/// In milliseconds, which the reader parses back without loss.
#[cfg(feature = "django")]
fn serialize_expiry<S: serde::Serializer>(
    value: &Option<std::time::Duration>,
    serializer: S
) -> Result<S::Ok, S::Error> {
    match value {
        Some(expiry) => serializer.collect_str(&format_args!("{}ms", expiry.as_millis())),
        None => serializer.serialize_none()
    }
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
        resolved.runner.validate().map_err(|error| format!("{}: {error}", path.display()))?;
        resolved.config = Some(path);
        Ok(resolved)
    }

    /// These options over those, field by field: the command line wins
    /// wherever it said anything.
    fn over(
        self,
        written: Self
    ) -> Self {
        Self {
            runner: self.runner.over(written.runner),
            root: self.root.or(written.root),
            venv: self.venv.or(written.venv),
            #[cfg(feature = "broadcast")]
            broadcasting: self.broadcasting.or(written.broadcasting),
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

#[cfg(test)]
#[path = "../tests/options.rs"]
mod tests;
