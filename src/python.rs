use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use claviron::Server;
use claviron::http::trust::{Network, Trust};
use claviron::runtime::{AppState, registry};

use crate::application::PythonApplicationService;
use crate::http::{Bind, HttpIngress, Protocols};
use crate::orbit::OrbitIngress;

pub type Result<T> = std::result::Result<T, Error>;

/// The supervisor's policy, as this launch wrote it down.
///
/// Claviron would otherwise read a `supervisor.yaml` of its own from the
/// config dir. One application is one file, so gufi hands the policy over
/// instead and that file is never looked for. Every field has a built-in
/// value, which is why an application that asks for workers and says nothing
/// else still starts.
struct SupervisorPolicy(claviron::supervisor::SupervisorConfig);

impl claviron::runtime::Provider<AppState> for SupervisorPolicy {
    fn name(&self) -> &'static str {
        "GufiSupervisorPolicy"
    }
}

impl claviron::supervisor::SupervisorConfigSource for SupervisorPolicy {
    fn supervisor_config(
        &self
    ) -> std::result::Result<claviron::supervisor::SupervisorConfig, String> {
        Ok(self.0.clone())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Application(#[from] crate::asgi::Error),
    #[error("unable to resolve the application working directory: {0}")]
    CurrentDirectory(#[source] std::io::Error),
    #[error("application working directory has no name")]
    MissingApplicationName,
    #[error("watch rolls the workers over one at a time: set workers to 2 or more")]
    WatchWithoutWorkers,
    #[error(transparent)]
    Claviron(#[from] claviron::Error),
    #[error("registration failed: {0}")]
    Registry(String)
}

/// Standalone Python application runner with a local HTTP development ingress.
///
/// By convention the process working directory is the application root and
/// `.venv` beneath it is the Python environment. Builder methods can replace
/// every environment-specific value without changing the application entry
/// point.
pub struct Runner {
    target: String,
    application_name: Option<String>,
    application_root: Option<PathBuf>,
    virtual_environment: Option<PathBuf>,
    orbit: bool,
    binds: Vec<Bind>,
    protocols: Protocols,
    trusted: Vec<Network>,
    workers: u8,
    config_path: Option<PathBuf>,
    supervisor: Option<claviron::supervisor::SupervisorConfig>,
    #[cfg(feature = "tls")]
    tls: Option<crate::Tls>,
    #[cfg(feature = "broadcast")]
    broadcasting: crate::options::Broadcasting,
    #[cfg(feature = "files")]
    files: Option<claviron::runner::files::FilesSource>,
    #[cfg(feature = "watch")]
    watch: Option<Vec<PathBuf>>
}

impl Runner {
    pub fn new(target: impl Into<String>) -> Self {
        Self {
            target: target.into(),
            application_name: None,
            application_root: None,
            virtual_environment: None,
            orbit: false,
            binds: Vec::new(),
            protocols: Protocols::default(),
            trusted: Vec::new(),
            workers: 0,
            config_path: None,
            supervisor: None,
            #[cfg(feature = "tls")]
            tls: None,
            #[cfg(feature = "broadcast")]
            broadcasting: crate::options::Broadcasting::default(),
            #[cfg(feature = "files")]
            files: None,
            #[cfg(feature = "watch")]
            watch: None
        }
    }

    /// What this application is called.
    ///
    /// The only name there is. It defaults to the working directory's name,
    /// which is convenient and also the reason to state it: this is an
    /// address. A Claviron edge names it in its own configuration the way it
    /// names a PHP or JavaScript application, and finds this process by it in
    /// shared memory — so the two spellings have to match, and a directory
    /// rename would silently break that.
    ///
    /// Several processes may answer to it. They take a lane each and know
    /// nothing about each other; the edge reads the lanes serving the name and
    /// picks the idlest. Nothing has to be told that a new one started.
    pub fn name(
        mut self,
        name: impl Into<String>
    ) -> Self {
        self.application_name = Some(name.into());
        self
    }

    /// The file this launch was read from.
    ///
    /// Defaults to `gufi.yaml` beside the application. Set it when the
    /// deployment keeps its configuration apart from the code — `/etc/orders`,
    /// or a mounted volume.
    pub fn config_path(
        mut self,
        path: impl Into<PathBuf>
    ) -> Self {
        self.config_path = Some(path.into());
        self
    }

    pub fn application_root(
        mut self,
        path: impl Into<PathBuf>
    ) -> Self {
        self.application_root = Some(path.into());
        self
    }

    pub fn virtual_environment(
        mut self,
        path: impl Into<PathBuf>
    ) -> Self {
        self.virtual_environment = Some(path.into());
        self
    }

    /// Also answer through this application's fleet, reached over shared
    /// memory.
    ///
    /// There is no fleet name to go with it: the fleet is named after the
    /// application, so the application name is the whole of the address.
    pub fn orbit(
        mut self,
        orbit: bool
    ) -> Self {
        self.orbit = orbit;
        self
    }

    /// Also answer on a socket.
    ///
    /// Doors are capabilities, not alternatives: a process may hold a lane in
    /// the fleet and a Unix socket at the same time, which is what a migration
    /// off an old front server looks like. Every one of them ends at the same
    /// application.
    pub fn listen(
        mut self,
        bind: Bind
    ) -> Self {
        self.binds.push(bind);
        self
    }

    /// Whose forwarding headers to believe.
    ///
    /// Nobody, unless stated. A Unix socket is trusted whatever this says:
    /// opening it already took the operator's permission.
    pub fn trusted(
        mut self,
        networks: impl IntoIterator<Item = Network>
    ) -> Self {
        self.trusted.extend(networks);
        self
    }

    /// Serve HTTPS on the TCP doors.
    ///
    /// A Unix socket is left alone: its boundary is already the filesystem's.
    #[cfg(feature = "tls")]
    pub fn tls(
        mut self,
        tls: crate::Tls
    ) -> Self {
        self.tls = Some(tls);
        self
    }

    /// Which HTTP versions the sockets accept. Both, unless told otherwise.
    pub fn protocols(
        mut self,
        protocols: Protocols
    ) -> Self {
        self.protocols = protocols;
        self
    }

    /// How many worker processes serve this application.
    ///
    /// Zero is standalone: one process, one interpreter. Above that a master
    /// supervises that many workers, each with an interpreter of its own —
    /// which is how Python scales, since one interpreter runs one thread of
    /// bytecode at a time.
    ///
    /// The workers inherit this launch rather than re-reading a command line:
    /// a worker is started by the supervisor with no arguments, so whatever
    /// was resolved here travels in the bootstrap envelope. An application
    /// that asks for workers therefore gets the doors it asked for in every
    /// one of them.
    ///
    /// A master serves no traffic itself: it holds no lane and binds no
    /// socket.
    pub fn workers(
        mut self,
        workers: u8
    ) -> Self {
        self.workers = workers;
        self
    }

    /// Take what a launch decided.
    ///
    /// Each field that is set replaces the runner's; each that is not leaves
    /// it alone. Builder calls made after this one win, so an application can
    /// accept the command line and still pin something itself.
    pub fn with_options(
        mut self,
        options: crate::GufiOptions
    ) -> Self {
        let crate::GufiOptions {
            name: named_application,
            orbit,
            tcp,
            uds,
            h1,
            h2,
            trusted,
            workers,
            #[cfg(feature = "tls")]
            tls_cert,
            #[cfg(feature = "tls")]
            tls_key,
            #[cfg(feature = "tls")]
            tls_client_ca,
            root,
            venv,
            config,
            supervisor,
            #[cfg(feature = "broadcast")]
            broadcasting,
            #[cfg(feature = "files")]
            files,
            #[cfg(feature = "watch")]
            watch,
            #[cfg(feature = "watch")]
            watch_paths
        } = options;
        #[cfg(feature = "watch")]
        if watch {
            self.watch = Some(watch_paths);
        }
        #[cfg(feature = "files")]
        if files.is_some() {
            self.files = files;
        }
        #[cfg(feature = "broadcast")]
        if let Some(broadcasting) = broadcasting {
            self.broadcasting = broadcasting;
        }
        if let Some(path) = config {
            self.config_path = Some(path);
        }
        if let Some(policy) = supervisor {
            self.supervisor = Some(policy);
        }
        if let Some(name) = named_application {
            self.application_name = Some(name);
        }
        if orbit {
            self.orbit = true;
        }
        if let Some(address) = tcp {
            self.binds.push(Bind::Tcp(address));
        }
        if let Some(path) = uds {
            self.binds.push(Bind::Uds(path));
        }
        if h1 || h2 {
            self.protocols = Protocols::resolve(h1, h2);
        }
        self.trusted.extend(trusted);
        if let Some(workers) = workers {
            self.workers = workers;
        }
        #[cfg(feature = "tls")]
        if let (Some(certificate), Some(key)) = (tls_cert, tls_key) {
            let mut tls = crate::Tls::new(certificate, key);
            if let Some(authority) = tls_client_ca {
                tls = tls.client_ca(authority);
            }
            self.tls = Some(tls);
        }
        if let Some(root) = root {
            self.application_root = Some(root);
        }
        if let Some(venv) = venv {
            self.virtual_environment = Some(venv);
        }
        self
    }

    /// Boot the application and serve it.
    ///
    /// The lifecycle is Claviron's standalone one, not a sequence written here:
    /// providers boot in dependency order, runnables are spawned once every
    /// boot has completed, shutdown drains them, and finalize runs last. The
    /// Python interpreter and the development ingress are two providers in that
    /// order like any other, which is what lets `configure` register alongside
    /// them.
    pub async fn run<F>(
        self,
        configure: F
    ) -> Result<()>
    where
        F: FnOnce(&AppState) -> registry::Result<()> + Send + 'static
    {
        let application_root = match self.application_root {
            Some(path) => path,
            None => std::env::current_dir().map_err(Error::CurrentDirectory)?
        };
        // The program's own name, not the directory it was started in: an
        // address must not depend on where someone was standing.
        let application_name = match self.application_name {
            Some(name) => name,
            None => crate::options::default_name(&application_root)
                .ok_or(Error::MissingApplicationName)?
        };
        let virtual_environment =
            self.virtual_environment.unwrap_or_else(|| application_root.join(".venv"));
        // The supervisor's policy comes from this launch, not from a file of
        // Claviron's own, so nothing here looks for `supervisor.yaml`.
        let supervisor = self.supervisor.clone().unwrap_or_default();
        let target = self.target;
        let protocols = self.protocols;
        let trusted = self.trusted;
        #[cfg(feature = "tls")]
        let tls = self.tls;
        let orbit = self.orbit;
        let workers = self.workers;
        #[cfg(feature = "watch")]
        let watch = self.watch.clone();
        #[cfg(feature = "watch")]
        if watch.is_some() && workers < 2 {
            return Err(Error::WatchWithoutWorkers);
        }
        #[cfg(feature = "broadcast")]
        let broadcasting = self.broadcasting.clone();
        #[cfg(feature = "files")]
        let files = self.files.clone();
        // A process with no door would boot an interpreter nothing can reach.
        // The first thing a Python developer runs has no arguments, so that
        // case is a local listener rather than an error.
        let binds = if self.binds.is_empty() && !orbit {
            vec![Bind::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8000))]
        } else {
            self.binds
        };

        // Named once here so boot can say where the launch came from. A
        // worker is not told: it did not read a file, it was handed the
        // result, and saying otherwise would invite someone to edit that
        // file and expect a worker to notice.
        let configuration =
            self.config_path.clone().unwrap_or_else(|| application_root.join(crate::CONFIG_FILE));
        let configuration = configuration.is_file().then_some(configuration);

        // What this process resolved, for its workers to inherit. A worker is
        // started with no command line, so the envelope is how a door asked
        // for here reaches every one of them.
        let launch = crate::GufiOptions {
            name: Some(application_name.clone()),
            orbit,
            tcp: binds.iter().find_map(|bind| match bind {
                Bind::Tcp(address) => Some(*address),
                Bind::Uds(_) => None
            }),
            uds: binds.iter().find_map(|bind| match bind {
                Bind::Uds(path) => Some(path.clone()),
                Bind::Tcp(_) => None
            }),
            h1: protocols.h1,
            h2: protocols.h2,
            trusted: trusted.clone(),
            root: Some(application_root.clone()),
            venv: Some(virtual_environment.clone()),
            #[cfg(feature = "tls")]
            tls_cert: tls.as_ref().map(|tls| tls.certificate.clone()),
            #[cfg(feature = "tls")]
            tls_key: tls.as_ref().map(|tls| tls.key.clone()),
            #[cfg(feature = "tls")]
            tls_client_ca: tls.as_ref().and_then(|tls| tls.client_ca.clone()),
            // Not carried: a worker does not start workers.
            workers: None,
            // Not carried: a worker did not read a file, it was handed the
            // result of one.
            config: None,
            // Carried: a worker registers the same policy source its master
            // did, so the fleet is supervised by one written-down decision.
            supervisor: Some(supervisor.clone()),
            #[cfg(feature = "broadcast")]
            broadcasting: Some(broadcasting.clone()),
            #[cfg(feature = "files")]
            files: files.clone(),
            // Not carried: the master watches, a worker is what it restarts.
            #[cfg(feature = "watch")]
            watch: false,
            #[cfg(feature = "watch")]
            watch_paths: Vec::new()
        }
        .encode()
        .map_err(Error::Registry)?;

        Server::new()
            .workers(workers)
            .launch(launch)
            // The instance lock, held under the application's own name: one
            // directory, one virtual environment, one process tree. A second
            // process of the same name is refused rather than left to write
            // over this one's state.
            .name(application_name.clone())
            // This process's own fleet, holding its workers and its metrics —
            // not the shared one the transport meets in, and never Claviron's
            // `clv`: an application has no business mapping a server's
            // internal state, and a server has none mapping an application's.
            .fleet_name(application_name.clone())
            .config_dir(&application_root)
            .register(move |state| {
                // The invocation router is already registered: Claviron's own
                // bootstrap installs it before this callback runs, and the
                // interpreter resolves it at boot.
                // Before the application's own registrations, because the
                // supervisor boots early and must find it there.
                state.registry_ref().insert(Arc::new(SupervisorPolicy(supervisor.clone())))?;
                state
                    .registry_ref()
                    .bind_dyn::<dyn claviron::supervisor::SupervisorConfigSource, SupervisorPolicy>(
                        |source| source
                    )?;

                // The master watches the source and restarts its workers; the
                // workers are what it restarts, so they do not watch.
                #[cfg(feature = "watch")]
                if state.is_master()
                    && let Some(paths) = &watch
                {
                    state.registry_ref().insert(Arc::new(crate::watch::SourceWatcher::new(
                        application_root.clone(),
                        paths.clone()
                    )))?;
                }

                // A worker was started with no command line: whatever this
                // process's `main` parsed, it parsed nothing. What it serves
                // is what its master resolved, and that arrived in the
                // bootstrap envelope. It is read first, before anything is
                // registered from it — the interpreter's own root and virtual
                // environment are in there too, and a worker that fell back
                // to its working directory for those would look for `.venv`
                // wherever the master happened to be started from.
                //
                // Asked of the run mode rather than of the registry, because
                // the process that wrote the envelope also holds it: the
                // question is not whether a launch is there, it is whether
                // this process resolved it or was handed it.
                let inherited = if state.is_worker() {
                    state
                        .registry_ref()
                        .resolve::<claviron::runtime::LaunchBlob>()
                        .filter(|blob| !blob.is_empty())
                        .map(|blob| crate::GufiOptions::decode(blob.as_bytes()))
                        .transpose()
                        .map_err(registry::Error::msg)?
                } else {
                    if let Some(path) = &configuration {
                        tracing::info!("📄 launch read from {}", path.display());
                    }
                    None
                };

                // A worker's is its master's, carried in the launch.
                #[cfg(feature = "broadcast")]
                let broadcasting = match inherited.as_ref() {
                    Some(options) => options.broadcasting.clone().unwrap_or_default(),
                    None => broadcasting.clone()
                };
                #[cfg(feature = "files")]
                let files = match inherited.as_ref() {
                    Some(options) => options.files.clone(),
                    None => files.clone()
                };
                let (orbit, binds, protocols, trusted, application_name, root, venv) =
                    match inherited {
                        None => (
                            orbit,
                            binds,
                            protocols,
                            trusted,
                            application_name,
                            application_root,
                            virtual_environment
                        ),
                        Some(options) => {
                            let mut doors = Vec::new();
                            if let Some(address) = options.tcp {
                                doors.push(Bind::Tcp(address));
                            }
                            if let Some(path) = options.uds.clone() {
                                doors.push(Bind::Uds(path));
                            }
                            (
                                options.orbit,
                                doors,
                                Protocols::resolve(options.h1, options.h2),
                                options.trusted.clone(),
                                options.name.clone().unwrap_or(application_name),
                                options.root.clone().unwrap_or(application_root),
                                options.venv.clone().unwrap_or(virtual_environment)
                            )
                        }
                    };

                // Parsed here, in every process, and anchored at the root.
                #[cfg(feature = "files")]
                let files = files
                    .as_ref()
                    .map(|files| files.parse().map(|files| Arc::new(files.under(&root))))
                    .transpose()
                    .map_err(registry::Error::msg)?;

                // The broadcast service itself is registered by Claviron's own
                // bootstrap whenever the feature is built in; what the launch
                // adds is the application `claviron.broadcast` publishes to.
                let application = Arc::new(PythonApplicationService::new(
                    target,
                    root,
                    venv,
                    #[cfg(feature = "broadcast")]
                    broadcasting
                ));
                // The owner: whatever the application's routes do not claim.
                claviron::runner::register_owner(
                    state,
                    claviron::runner::Owner::new(application.service())
                )?;
                state.registry_ref().insert(application)?;
                // What stands in front of the routes.
                let routing = claviron::runner::Routing::new();
                #[cfg(feature = "files")]
                let routing = routing.files(files);
                state.registry_ref().insert(Arc::new(routing))?;
                // The fleet's keyed counters: `claviron.counter(key)` in
                // Python, `CounterProvider` in Rust.
                claviron::orbit::register_counter(state)?;

                // Then the application's own registrations, which may compose
                // the interpreter behind their routes.
                configure(state)?;
                claviron::runner::ensure_application(state)?;

                // Every door that was asked for, and they do not exclude each
                // other: each is a runnable of its own, and all of them end at
                // the same application.
                if orbit {
                    // The lane advertises what this process *serves*, which is
                    // the one name the edge knows.
                    state.registry_ref().insert(Arc::new(OrbitIngress::new(application_name)))?;
                }
                // The launch's answer, unless `configure` already registered a
                // better one. Same rule as the application service: state what
                // is missing, never overwrite what was stated.
                if state.registry_ref().resolve::<Trust>().is_none() {
                    state.registry_ref().insert(Arc::new(Trust::new(trusted)))?;
                }
                if !binds.is_empty() {
                    state.registry_ref().insert(Arc::new(HttpIngress::new(
                        binds,
                        protocols,
                        #[cfg(feature = "tls")]
                        tls
                    )))?;
                }
                Ok(())
            })
            .serve()
            .await?;
        Ok(())
    }
}
