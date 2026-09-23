use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use claviron::Server;
use claviron::runtime::{AppState, registry};

use crate::application::PythonApplicationService;
use crate::http::DevIngress;

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
    fleet_name: Option<String>,
    address: SocketAddr
}

impl Runner {
    pub fn new(target: impl Into<String>) -> Self {
        Self {
            target: target.into(),
            application_name: None,
            application_root: None,
            virtual_environment: None,
            fleet_name: None,
            address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8000)
        }
    }

    /// What this process is called in its fleet.
    ///
    /// It defaults to the working directory's name, which is convenient and
    /// also the reason to state it. This is an address: two processes in one
    /// fleet cannot share it, because the instance lock is held under it, and
    /// a Claviron edge serving this application names it in its own
    /// configuration the way it names a PHP or JavaScript one. The edge
    /// resolves it over shared memory, so the two spellings have to be the
    /// same — and a directory rename would silently break that.
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

    /// The Orbit fleet this process joins.
    ///
    /// It defaults to the application's own name, never to Claviron's `clv`.
    /// Shared memory outlives the process that made it, so a runner that
    /// defaulted into an edge's fleet would map segments belonging to a server
    /// it knows nothing about. Joining one is a decision, and it is stated
    /// here.
    ///
    /// This is where several applications meet: they share a fleet name and
    /// differ by [`Runner::application_name`], which is what lets one edge
    /// reach all of them over shared memory.
    pub fn fleet_name(
        mut self,
        fleet_name: impl Into<String>
    ) -> Self {
        self.fleet_name = Some(fleet_name.into());
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
        let fleet_name = self.fleet_name.unwrap_or_else(|| application_name.clone());
        let target = self.target;
        let address = self.address;

        Server::new()
            .name(application_name)
            .fleet_name(fleet_name)
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

                state.registry_ref().insert(Arc::new(DevIngress::new(address)))?;
                Ok(())
            })
            .serve()
            .await?;
        Ok(())
    }
}
