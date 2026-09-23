use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use claviron::Server;
use claviron::runtime::{AppState, registry};

use crate::application::PythonApplicationService;
use crate::http::{Bind, HttpIngress, Protocols};
use crate::orbit::OrbitIngress;
use crate::trust::{Network, Trust};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Application(#[from] crate::asgi::Error),
    #[error("unable to resolve the application working directory: {0}")]
    CurrentDirectory(#[source] std::io::Error),
    #[error("application working directory has no name")]
    MissingApplicationName,
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
    instance_name: Option<String>,
    application_root: Option<PathBuf>,
    virtual_environment: Option<PathBuf>,
    orbit: bool,
    binds: Vec<Bind>,
    protocols: Protocols,
    trusted: Vec<Network>
}

impl Runner {
    pub fn new(target: impl Into<String>) -> Self {
        Self {
            target: target.into(),
            application_name: None,
            instance_name: None,
            application_root: None,
            virtual_environment: None,
            orbit: false,
            binds: Vec::new(),
            protocols: Protocols::default(),
            trusted: Vec::new()
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
    pub fn application_name(
        mut self,
        name: impl Into<String>
    ) -> Self {
        self.application_name = Some(name.into());
        self
    }

    /// What *this process* is called, as distinct from what it serves.
    ///
    /// Several processes may answer to one application name, and they are
    /// independent: separately started, separately stopped, unaware of each
    /// other. What they cannot share is Claviron's own identity — the
    /// instance lock is held under this name, and so is the fleet carrying
    /// this process's workers, metrics and events. Two processes sharing it
    /// would both be node zero of one fleet and would write over each other's
    /// state, which is what the lock refuses.
    ///
    /// So: the application name is the address, shared; this is the identity,
    /// unique. It defaults to the application name, which is right for the
    /// common case of one process, and a second process states it:
    ///
    /// ```ignore
    /// Runner::new("main:app").application_name("orders").instance_name("orders-2")
    /// ```
    ///
    /// Nothing about the address changes — the edge still writes to `orders`
    /// and both processes are targets for it.
    pub fn instance_name(
        mut self,
        name: impl Into<String>
    ) -> Self {
        self.instance_name = Some(name.into());
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

    /// Also answer through the shared fleet, reached over shared memory.
    ///
    /// There is no fleet name to go with it. The transport's is the same on
    /// every process on the machine, and this process's own is taken from its
    /// instance name. Neither is a question for the application.
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

    /// Which HTTP versions the sockets accept. Both, unless told otherwise.
    pub fn protocols(
        mut self,
        protocols: Protocols
    ) -> Self {
        self.protocols = protocols;
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
            application_name,
            instance_name,
            orbit,
            tcp,
            uds,
            h1,
            h2,
            trusted,
            root,
            venv
        } = options;
        if let Some(name) = application_name {
            self.application_name = Some(name);
        }
        if let Some(name) = instance_name {
            self.instance_name = Some(name);
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
        let application_name = match self.application_name {
            Some(name) => name,
            None => application_root
                .file_name()
                .and_then(|name| name.to_str())
                .filter(|name| !name.is_empty())
                .ok_or(Error::MissingApplicationName)?
                .to_owned()
        };
        let virtual_environment =
            self.virtual_environment.unwrap_or_else(|| application_root.join(".venv"));
        let instance_name = self.instance_name.unwrap_or_else(|| application_name.clone());
        let target = self.target;
        let protocols = self.protocols;
        let trusted = self.trusted;
        let orbit = self.orbit;
        // A process with no door would boot an interpreter nothing can reach.
        // The first thing a Python developer runs has no arguments, so that
        // case is a local listener rather than an error.
        let binds = if self.binds.is_empty() && !orbit {
            vec![Bind::Tcp(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8000))]
        } else {
            self.binds
        };

        Server::new()
            // The instance, not the application: the lock is held under this
            // name, and a second process serving the same application states
            // its own.
            .name(instance_name.clone())
            // This process's own fleet, holding its workers and its metrics —
            // not the shared one the transport meets in, and never Claviron's
            // `clv`: an application has no business mapping a server's
            // internal state, and a server has none mapping an application's.
            // Per instance rather than per application, because two processes
            // in one fleet would both be node zero of it.
            .fleet_name(instance_name)
            .config_dir(&application_root)
            .register(move |state| {
                // The invocation router is already registered: Claviron's own
                // bootstrap installs it before this callback runs, and the
                // interpreter resolves it at boot.
                state.registry_ref().insert(Arc::new(PythonApplicationService::new(
                    target,
                    application_root,
                    virtual_environment
                )))?;

                // Then the application's own registrations, which may compose
                // the interpreter behind their routes.
                configure(state)?;
                crate::routing::ensure_application(state)?;

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
                    state.registry_ref().insert(Arc::new(HttpIngress::new(binds, protocols)))?;
                }
                Ok(())
            })
            .serve()
            .await?;
        Ok(())
    }
}
