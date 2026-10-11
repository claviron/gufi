use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use claviron::Server;
use claviron::http::trust::Network;
use claviron::runner::{Bind, Protocols, launch};
use claviron::runtime::{AppState, registry};

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
    #[cfg(feature = "axum")]
    interceptor: Option<Interceptor>,
}

impl Runner {
    pub fn new(target: impl Into<String>) -> Self {
        Self {
            target: target.into(),
            launch: Launch::default(),
            config_path: None,
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

    /// Also answer through this application's fleet, reached over shared
    /// memory. The fleet is named after the application, so the application
    /// name is the whole of the address.
    pub fn orbit(
        mut self,
        orbit: bool,
    ) -> Self {
        self.launch.runner.orbit = orbit;
        self
    }

    /// Also answer on a socket. Doors are capabilities, not alternatives: a
    /// process may hold a lane in the fleet and a Unix socket at once.
    pub fn listen(
        mut self,
        bind: Bind,
    ) -> Self {
        match bind {
            Bind::Tcp(address) => self.launch.runner.tcp = Some(address),
            Bind::Uds(path) => self.launch.runner.uds = Some(path),
        }
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

    /// Which HTTP versions the sockets accept. Both, unless told otherwise.
    pub fn protocols(
        mut self,
        protocols: Protocols,
    ) -> Self {
        self.launch.runner.h1 = protocols.h1;
        self.launch.runner.h2 = protocols.h2;
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
    pub async fn run<F>(
        self,
        configure: F,
    ) -> Result<()>
    where
        F: FnOnce(&AppState) -> registry::Result<()> + Send + 'static,
    {
        #[cfg(feature = "axum")]
        let interceptor = self.interceptor;
        let mut launch = self.launch;
        let root = match launch.own.root.clone() {
            Some(path) => path,
            None => std::env::current_dir().map_err(Error::CurrentDirectory)?,
        };
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
        if launch.runner.tcp.is_none()
            && launch.runner.uds.is_none()
            && !launch.runner.orbit
        {
            launch.runner.tcp =
                Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8000));
        }
        // An interpreter is not reloaded in place: a change rolls the workers
        // over, so there have to be workers to roll.
        #[cfg(feature = "watch")]
        if launch.runner.watch && launch.runner.workers.unwrap_or(0) < 2 {
            return Err(Error::WatchWithoutWorkers);
        }
        let production = launch.runner.production;
        let environment = launch.runner.environment.clone();
        let prepared = launch.prepare(name).map_err(Error::Registry)?;
        let definitions = launch.clone();

        // Named once here so the master can say where the launch came from.
        let configuration = self
            .config_path
            .clone()
            .unwrap_or_else(|| root.join(crate::CONFIG_FILE));
        let configuration = configuration.is_file().then_some(configuration);
        let target = self.target;

        // Production only by `--production`; otherwise `--environment`, and
        // without either, `local`.
        let server = match (production, environment) {
            (true, _) => Server::new().production(),
            (false, Some(environment)) => {
                Server::new().environment(environment)
            }
            (false, None) => Server::new(),
        };
        server
            .workers(prepared.workers)
            .launch(prepared.envelope)
            // The instance lock and this process's own fleet, both under the
            // application's own name — never Claviron's `clv`.
            .name(prepared.name.clone())
            .fleet_name(prepared.fleet_name)
            .config_dir(&root)
            .with_definitions(move |state| {
                let definitions = definitions.inherited(state)?;
                claviron::runner::supervisor::register_policy(
                    state,
                    definitions.runner.supervisor.clone().unwrap_or_default(),
                )
            })
            .register(move |state| {
                // A worker serves what its master resolved, and reads it
                // before anything is registered from it: the interpreter's
                // root and environment are in there too.
                let launch = launch.inherited(state)?;
                if !state.is_worker()
                    && let Some(path) = &configuration
                {
                    tracing::info!("📄 launch read from {}", path.display());
                }
                let runner = &launch.runner;
                let root = launch.own.root.clone().unwrap_or_default();
                let venv = launch.own.venv.clone().unwrap_or_else(|| root.join(".venv"));

                // The master watches the source and restarts its workers; the
                // workers are what it restarts, so they do not watch.
                #[cfg(feature = "watch")]
                if state.is_master() && runner.watch {
                    state.registry_ref().register(Arc::new(crate::watch::watcher(
                        root.clone(),
                        runner.watch_paths.clone()
                    )))?;
                }

                // The owner: the interpreter, and whatever the application's
                // routes do not claim. The broadcast service itself is
                // Claviron's; what the launch adds is the application
                // `claviron.broadcast` publishes to.
                let application = Arc::new(PythonApplicationService::new(
                    target,
                    root.clone(),
                    venv,
                    #[cfg(feature = "broadcast")]
                    launch.own.broadcasting.clone().unwrap_or_default()
                ));
                let python = application.service();
                let handle = python.handle_cell();
                #[cfg(feature = "websocket")]
                let upgrades = crate::websocket::Upgrades::new(python.handle_cell());
                // A WebSocket over Orbit that is not a broadcast session: the
                // ASGI application's.
                let owner = claviron::runner::Owner::new(python).orbit_sessions(
                    move |request, endpoint, handshake| {
                        crate::orbit::session(request, endpoint, handshake, Arc::clone(&handle))
                    }
                );
                #[cfg(feature = "websocket")]
                let owner = owner.upgrades(upgrades);
                claviron::runner::register_owner(state, owner)?;
                state.registry_ref().register(application)?;
                launch::register_routing(state, runner, &root)?;
                // The fleet's keyed counters: `claviron.counter(key)` in
                // Python, `CounterProvider` in Rust.
                claviron::orbit::register_counter(state)?;

                // Then the application's own registrations, which may compose
                // the interpreter behind their routes.
                configure(state)?;
                #[cfg(feature = "axum")]
                if let Some(interceptor) = interceptor {
                    interceptor(state)?;
                }
                claviron::runner::ensure_application(state)?;
                launch::register_trust(state, runner)?;
                launch::register_doors(state, runner, crate::orbit::ASGI_RUNTIME, None)
            })
            .serve()
            .await?;
        Ok(())
    }
}
