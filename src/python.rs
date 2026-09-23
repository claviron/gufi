use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use claviron::Server;
use claviron::runtime::{AppState, registry};

use crate::application::PythonApplicationService;
use crate::fleet::FleetIngress;
use crate::http::DevIngress;

/// Which door this process answers through.
///
/// One decision, taken when the process is started rather than written into
/// the application: the same binary, the same `configure`, the same routes,
/// reached two different ways. Nothing in the application sees this — a
/// request arrives as a request either way.
///
/// This is not Claviron's `RunMode`, which says whether a process is a master,
/// a worker or standalone. That is the process model; this is the door.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RunMode {
    /// A plain HTTP listener on this machine, for writing the application.
    ///
    /// The default, because it is the one that needs nothing else running.
    #[default]
    Dev,
    /// No listener at all. The process joins the shared fleet under its
    /// application name and waits on its lane, and a Claviron edge reaches it
    /// there — over shared memory, with no socket between them.
    Fleet
}

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
    application_root: Option<PathBuf>,
    virtual_environment: Option<PathBuf>,
    mode: RunMode,
    address: SocketAddr
}

impl Runner {
    pub fn new(target: impl Into<String>) -> Self {
        Self {
            target: target.into(),
            application_name: None,
            application_root: None,
            virtual_environment: None,
            mode: RunMode::Dev,
            address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8000)
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

    /// Which door to answer through: a local listener, or the shared fleet.
    ///
    /// There is no fleet name to go with it. The transport's is the same on
    /// every process on the machine — an edge worker and an application find
    /// each other because they opened the same segment — and this process's
    /// own fleet is named after the application, which is a name the runner
    /// already has. Neither is a question for the application to answer.
    pub fn mode(
        mut self,
        mode: RunMode
    ) -> Self {
        self.mode = mode;
        self
    }

    pub fn address(
        mut self,
        address: SocketAddr
    ) -> Self {
        self.address = address;
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
        let target = self.target;
        let address = self.address;
        let mode = self.mode;
        let ingress_name = application_name.clone();

        Server::new()
            .name(application_name.clone())
            // This process's own fleet, holding its workers and its metrics —
            // not the shared one the transport meets in, and never Claviron's
            // `clv`: an application has no business mapping a server's
            // internal state, and a server has none mapping an application's.
            .fleet_name(application_name)
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

                // One door, chosen once. Registering both would put two
                // ingresses on one interpreter, and the second would be a
                // surprise to whoever started the first.
                match mode {
                    RunMode::Dev => {
                        state.registry_ref().insert(Arc::new(DevIngress::new(address)))?;
                    }
                    RunMode::Fleet => {
                        state.registry_ref().insert(Arc::new(FleetIngress::new(ingress_name)))?;
                    }
                }
                Ok(())
            })
            .serve()
            .await?;
        Ok(())
    }
}
