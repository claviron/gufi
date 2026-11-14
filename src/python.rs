use std::path::PathBuf;
use std::sync::Arc;

use claviron::http::trust::Network;
use claviron::runtime::{AppState, Register, registry};
use claviron_runner::Composition;

use crate::application::PythonApplicationService;
use crate::options::Launch;

#[cfg(feature = "axum")]
type Interceptor = Box<dyn FnOnce(&AppState) -> registry::Result<()> + Send>;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Application(#[from] crate::asgi::Error),
    #[error("unable to resolve the application working directory: {0}")]
    CurrentDirectory(#[source] std::io::Error),
    #[error("application working directory has no name")]
    MissingApplicationName,
    #[error(
        "watch rolls the workers over one at a time: set workers to 2 or more"
    )]
    WatchWithoutWorkers,
    #[error(transparent)]
    Claviron(#[from] claviron::Error),
    #[error("registration failed: {0}")]
    Registry(String),
}

type Registration = Box<dyn FnOnce(&AppState) -> registry::Result<()> + Send>;

/// Standalone Python application runner with a local HTTP development ingress.
///
/// By convention the process working directory is the application root and
/// `.venv` beneath it is the Python environment. Builder methods can replace
/// every environment-specific value without changing the application entry
/// point. What they set is the launch itself — the one a worker inherits.
pub struct Runner {
    target: String,
    launch: Launch,
    config_path: Option<PathBuf>,
    registrations: Vec<Registration>,
    #[cfg(feature = "axum")]
    interceptor: Option<Interceptor>,
}

impl Runner {
    /// Declare the framework once for management, overriding pyproject detection.
    /// Python dispatch is unchanged.
    pub fn framework(
        mut self,
        framework: impl Into<String>,
    ) -> Self {
        self.launch.runner.frameworks = Some(vec![framework.into()]);
        self
    }

    /// Declare all informational framework tags; an empty list disables discovery.
    pub fn frameworks(
        mut self,
        frameworks: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.launch.runner.frameworks =
            Some(frameworks.into_iter().map(Into::into).collect());
        self
    }

    /// Choose an admin-client icon by code, independently of Python dispatch.
    pub fn icon(
        mut self,
        icon: impl Into<String>,
    ) -> Self {
        self.launch.runner.icon = Some(icon.into());
        self
    }

    pub fn new(target: impl Into<String>) -> Self {
        Self {
            target: target.into(),
            launch: Launch::default(),
            config_path: None,
            registrations: Vec::new(),
            #[cfg(feature = "axum")]
            interceptor: None,
        }
    }

    /// What this application is called.
    ///
    /// The only name there is. It defaults to the working directory's name,
    /// which is convenient and also the reason to state it: this is an
    /// address. A Claviron edge names it in its own configuration and finds
    /// this process by it in shared memory — so the two spellings have to
    /// match, and a directory rename would silently break that.
    pub fn name(
        mut self,
        name: impl Into<String>,
    ) -> Self {
        self.launch.runner.name = Some(name.into());
        self
    }

    /// The file this launch was read from. Defaults to `gufi.yaml` beside the
    /// application.
    pub fn config_path(
        mut self,
        path: impl Into<PathBuf>,
    ) -> Self {
        self.config_path = Some(path.into());
        self
    }

    pub fn application_root(
        mut self,
        path: impl Into<PathBuf>,
    ) -> Self {
        self.launch.own.root = Some(path.into());
        self
    }

    pub fn virtual_environment(
        mut self,
        path: impl Into<PathBuf>,
    ) -> Self {
        self.launch.own.venv = Some(path.into());
        self
    }

    /// Bound each Link text/binary message in bytes; defaults to 1 MiB.
    pub fn link_message_limit(
        mut self,
        bytes: std::num::NonZeroUsize,
    ) -> Self {
        self.launch.own.link_message_limit = Some(bytes);
        self
    }

    /// Also answer through this application's fleet, reached over shared
    /// memory. The fleet is named after the application, so the application
    /// name is the whole of the address.
    pub fn orbit(
        mut self,
        orbit: bool,
    ) -> Self {
        self.launch.runner.orbit = orbit.then(|| {
            serde_yaml::from_str("{link: {}}")
                .expect("static Orbit configuration")
        });
        self
    }

    /// Whose forwarding headers to believe. Nobody, unless stated. A Unix
    /// socket is trusted whatever this says: opening it already took the
    /// operator's permission.
    pub fn trusted(
        mut self,
        networks: impl IntoIterator<Item = Network>,
    ) -> Self {
        self.launch.runner.trusted.extend(networks);
        self
    }

    /// Serve HTTPS on the TCP socket. A Unix socket is left alone: its
    /// boundary is already the filesystem's.
    #[cfg(feature = "tls")]
    pub fn tls(
        mut self,
        tls: crate::Tls,
    ) -> Self {
        self.launch.runner.tls_cert = Some(tls.certificate);
        self.launch.runner.tls_key = Some(tls.key);
        self.launch.runner.tls_client_ca = tls.client_ca;
        self
    }

    /// How many worker processes serve this application. Zero is standalone:
    /// one process, one interpreter. Above that a master supervises that many
    /// workers, each with an interpreter of its own — which is how Python
    /// scales — and serves no traffic itself.
    pub fn workers(
        mut self,
        workers: u8,
    ) -> Self {
        self.launch.runner.workers = Some(workers);
        self
    }

    /// Take what a launch decided: each field it set stands over the runner's.
    /// Builder calls made after this one win, so an application can accept the
    /// command line and still pin something itself.
    pub fn with_options(
        mut self,
        options: crate::GufiOptions,
    ) -> Self {
        if let Some(path) = options.config.clone() {
            self.config_path = Some(path);
        }
        let given = options.into_launch();
        self.launch.runner = given.runner.over(self.launch.runner);
        self.launch.own.root = given.own.root.or(self.launch.own.root);
        self.launch.own.venv = given.own.venv.or(self.launch.own.venv);
        #[cfg(feature = "broadcast")]
        {
            self.launch.own.broadcasting =
                given.own.broadcasting.or(self.launch.own.broadcasting);
        }
        self
    }

    /// Register application services before the application is composed. Each
    /// type owns how its provider instances are built; tuples work too.
    pub fn with_services<C: Register<AppState> + 'static>(self) -> Self {
        self.with_bootstrap(C::register)
    }

    /// Run registrations of your own before the application is composed.
    pub fn with_bootstrap<F>(
        mut self,
        register: F,
    ) -> Self
    where
        F: FnOnce(&AppState) -> registry::Result<()> + Send + 'static,
    {
        self.registrations.push(Box::new(register));
        self
    }

    /// Rust handles its declared routes before HTTP Routable contributions
    /// and Python. Built after `configure`, so application registrations are
    /// available. Calling this again replaces the selected interceptor.
    #[cfg(feature = "axum")]
    pub fn with_interceptor<F>(
        mut self,
        router: F,
    ) -> Self
    where
        F: FnOnce(AppState) -> claviron::axum::Router + Send + 'static,
    {
        self.interceptor = Some(Box::new(move |state| {
            crate::routing::register_router(state, router)
        }));
        self
    }

    /// Boot the application and serve it.
    ///
    /// The lifecycle is Claviron's, not a sequence written here: providers
    /// boot in dependency order, runnables are spawned once every boot has
    /// completed, shutdown drains them, and finalize runs last. The doors,
    /// routing, trust and supervision are the runner shell's; the interpreter
    /// is this crate's owner among them.
    pub async fn run(self) -> Result<()> {
        #[cfg(feature = "axum")]
        let interceptor = self.interceptor;
        let registrations = self.registrations;
        let mut launch = self.launch;
        let root = match launch.own.root.clone() {
            Some(path) => path,
            None => std::env::current_dir().map_err(Error::CurrentDirectory)?,
        };
        crate::framework::resolve(&root, &mut launch.runner.frameworks);
        // The program's own name, not the directory it was started in: an
        // address must not depend on where someone was standing.
        let name = match launch.runner.name.clone() {
            Some(name) => name,
            None => crate::options::default_name(&root)
                .ok_or(Error::MissingApplicationName)?,
        };
        launch.own.venv =
            Some(launch.own.venv.clone().unwrap_or_else(|| root.join(".venv")));
        launch.own.root = Some(root.clone());
        // A process with no door would boot an interpreter nothing can reach.
        // The first thing a Python developer runs has no arguments, so that
        // case is a local listener rather than an error.
        if launch.runner.socket_listeners().map_err(Error::Registry)?.is_empty()
            && !launch.runner.link_enabled()
        {
            launch.runner.listeners = Some(
                serde_yaml::from_str(
                    "development:\n  transport: tcp\n  listen: 127.0.0.1:8000\n  protocols: [h1, h2]\n",
                )
                .map_err(|error| Error::Registry(error.to_string()))?,
            );
        }
        // An interpreter is not reloaded in place: a change rolls the workers
        // over, so there have to be workers to roll.
        #[cfg(feature = "watch")]
        if launch.runner.watch && launch.runner.workers.unwrap_or(0) < 2 {
            return Err(Error::WatchWithoutWorkers);
        }
        // Named once here so the master can say where the launch came from.
        let configuration = self
            .config_path
            .clone()
            .unwrap_or_else(|| root.join(crate::CONFIG_FILE));
        let config_path = configuration.clone();
        let configuration = configuration.is_file().then_some(configuration);
        let target = self.target;

        Composition::new(
            launch,
            name,
            crate::orbit::ASGI_RUNTIME,
            move |state, launch| {
                if !state.is_worker()
                    && let Some(path) = &configuration
                {
                    tracing::info!("📄 launch read from {}", path.display());
                }
                let runner = &launch.runner;
                let root = launch.own.root.clone().unwrap_or_default();
                let venv = launch
                    .own
                    .venv
                    .clone()
                    .unwrap_or_else(|| root.join(".venv"));

                // The master watches the source and restarts its workers; the
                // workers are what it restarts, so they do not watch.
                #[cfg(feature = "watch")]
                if state.is_master() && runner.watch {
                    state.registry_ref().register(Arc::new(
                        crate::watch::watcher(
                            root.clone(),
                            runner.watch_paths.clone(),
                        ),
                    ))?;
                }

                // The owner: the interpreter, and whatever the application's
                // routes do not claim. The broadcast service itself is its own
                // crate's; what the launch adds is the application
                // `claviron.broadcast` publishes to.
                let application = Arc::new(PythonApplicationService::new(
                    target,
                    root,
                    venv,
                    runner.frameworks.clone().unwrap_or_default(),
                    runner.icon.clone(),
                    #[cfg(feature = "broadcast")]
                    launch.own.broadcasting.clone().unwrap_or_default(),
                ));
                let python = application.service();
                let owner = claviron_runner::Owner::new(
                    claviron::ExecutorSpec::Dedicated(
                        claviron::ExecutorKind::CurrentThread,
                    ),
                    python,
                );
                // A session on any door is the ASGI application's.
                #[cfg(feature = "websocket")]
                let owner = {
                    let handle = application.handle_cell();
                    let message_limit = launch
                        .own
                        .link_message_limit
                        .map(std::num::NonZeroUsize::get)
                        .unwrap_or(crate::orbit::DEFAULT_MESSAGE_LIMIT);
                    owner.upgrades(
                        crate::websocket::Upgrades::new(handle)
                            .message_limit(message_limit),
                    )
                };
                // A Pusher client's channels stand in front of the application.
                #[cfg(feature = "broadcast")]
                let owner = claviron_broadcast::runner::in_front(state, owner)?;
                state.registry_ref().register(application)?;
                Ok(owner)
            },
        )
        .config_path(&config_path)
        .root(|launch| launch.own.root.clone().unwrap_or_default())
        .definitions(|state, _| {
            #[cfg(feature = "broadcast")]
            claviron_broadcast::register(state)?;
            #[cfg(not(feature = "broadcast"))]
            let _ = state;
            Ok(())
        })
        .configure(move |state, _| {
            // The fleet's keyed counters: `claviron.counter(key)` in Python,
            // `CounterProvider` in Rust.
            claviron::orbit::register_counter(state)?;

            // Then the application's own registrations, which may compose
            // the interpreter behind their routes.
            for register in registrations {
                register(state)?;
            }
            #[cfg(feature = "axum")]
            if let Some(interceptor) = interceptor {
                interceptor(state)?;
            }
            Ok(())
        })
        .serve()
        .await?;
        Ok(())
    }
}
