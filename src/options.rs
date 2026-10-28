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

    /// Maximum text/binary message bytes over Link. Defaults to 1 MiB.
    #[cfg_attr(feature = "cli", arg(long, value_name = "BYTES"))]
    pub link_message_limit: Option<std::num::NonZeroUsize>,

    /// Broadcasting in this process: the application `gufi.broadcast`
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
    #[cfg_attr(
        feature = "cli",
        arg(short, long = "config-path", visible_alias = "config")
    )]
    pub config: Option<PathBuf>,
}

/// gufi's own keys, beside the runner's in the same flat file, and what a
/// worker inherits of them.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Python {
    pub(crate) root: Option<PathBuf>,
    pub(crate) venv: Option<PathBuf>,
    pub(crate) link_message_limit: Option<std::num::NonZeroUsize>,
    #[cfg(feature = "broadcast")]
    pub(crate) broadcasting: Option<Broadcasting>,
}

/// A launch as gufi writes and carries it: the runner's options and its own.
pub(crate) type Launch = claviron::runner::launch::Launch<Python>;

impl GufiOptions {
    /// The launch these options state.
    pub(crate) fn into_launch(self) -> Launch {
        Launch {
            runner: self.runner,
            own: Python {
                root: self.root,
                venv: self.venv,
                link_message_limit: self.link_message_limit,
                #[cfg(feature = "broadcast")]
                broadcasting: self.broadcasting,
            },
        }
    }

    fn from_launch(
        launch: Launch,
        config: Option<PathBuf>,
    ) -> Self {
        Self {
            runner: launch.runner,
            root: launch.own.root,
            venv: launch.own.venv,
            link_message_limit: launch.own.link_message_limit,
            #[cfg(feature = "broadcast")]
            broadcasting: launch.own.broadcasting,
            config,
        }
    }
}

/// The `broadcasting:` section of the launch.
#[cfg(feature = "broadcast")]
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Broadcasting {
    /// The broadcast application `gufi.broadcast(...)` publishes to when
    /// a call names none.
    pub application: Option<String>,
    /// Django Channels' channel layer, `gufi.channels.OrbitChannelLayer`,
    /// over the broadcast service's groups. Absent, the layer is not
    /// installed.
    #[cfg(feature = "django")]
    pub channels: Option<Channels>,
}

#[cfg(feature = "django")]
pub use gufi_asgi::config::Channels;

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
            Err(message) => Self::command()
                .error(clap::error::ErrorKind::InvalidValue, message)
                .exit(),
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
            None => self
                .root
                .clone()
                .unwrap_or_else(|| PathBuf::from("."))
                .join(CONFIG_FILE),
        }
    }

    /// Read [`CONFIG_FILE`] from the configuration directory and let these
    /// options stand over what it says.
    ///
    /// A missing file is not an error — an application that was never
    /// configured runs on its defaults. A named `--config-path` file that
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
            Err(error)
                if error.kind() == std::io::ErrorKind::NotFound
                    && self.config.is_none() =>
            {
                return Ok(self);
            }
            Err(error) => {
                return Err(format!("read {}: {error}", path.display()));
            }
        };
        let written: Launch = serde_yaml::from_str(&text)
            .map_err(|error| format!("parse {}: {error}", path.display()))?;
        let written = Self::from_launch(written, None);
        let mut resolved = self.over(written);
        resolved
            .runner
            .validate()
            .map_err(|error| format!("{}: {error}", path.display()))?;
        resolved.config = Some(path);
        Ok(resolved)
    }

    /// These options over those, field by field: the command line wins
    /// wherever it said anything.
    fn over(
        self,
        written: Self,
    ) -> Self {
        Self {
            runner: self.runner.over(written.runner),
            root: self.root.or(written.root),
            venv: self.venv.or(written.venv),
            link_message_limit: self
                .link_message_limit
                .or(written.link_message_limit),
            #[cfg(feature = "broadcast")]
            broadcasting: self.broadcasting.or(written.broadcasting),
            config: self.config,
        }
    }
}

/// What this application is called when nobody said: the root's own name.
///
/// Python is not compiled, so the process runs where the scripts are and the
/// root is the application rather than a place it was started from. One
/// directory, one virtual environment, one name.
pub(crate) fn default_name(root: &std::path::Path) -> Option<String> {
    root.file_name()?
        .to_str()
        .filter(|name| !name.is_empty())
        .map(ToOwned::to_owned)
}

#[cfg(test)]
#[path = "../tests/options.rs"]
mod tests;
