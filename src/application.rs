//! The Python application as a provider Claviron boots.
//!
//! The interpreter comes up in `boot()`, before any runnable is spawned, so an
//! ingress cannot be reached before the application behind it is ready. It goes
//! down in `finalize()`, after the runnables have drained — the ingress has
//! already stopped accepting and finished what it accepted by then, so
//! shutdown waits for work rather than interrupting it.

use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;
use claviron::app::invoke::InvocationRouter;
use claviron::runtime::{AppState, Finalizable, Provider, ProviderOrder, registry};

use crate::asgi::{PythonApplication, PythonApplicationHandle, RunningPythonApplication};
use crate::routing::PythonService;

pub(crate) struct PythonApplicationService {
    target: String,
    application_root: PathBuf,
    virtual_environment: PathBuf,
    handle: Arc<OnceLock<PythonApplicationHandle>>,
    running: Mutex<Option<RunningPythonApplication>>
}

impl PythonApplicationService {
    pub(crate) fn new(
        target: impl Into<String>,
        application_root: PathBuf,
        virtual_environment: PathBuf
    ) -> Self {
        Self {
            target: target.into(),
            application_root,
            virtual_environment,
            handle: Arc::new(OnceLock::new()),
            running: Mutex::new(None)
        }
    }

    /// The application as a service, for an application to compose behind its
    /// own routes. The handle inside is empty until `boot()` fills it, and no
    /// ingress runs before that.
    pub(crate) fn service(&self) -> PythonService {
        PythonService::new(Arc::clone(&self.handle))
    }
}

#[async_trait]
impl Provider<AppState> for PythonApplicationService {
    fn name(&self) -> &'static str {
        "PythonApplicationService"
    }

    /// `boot()` resolves the invocation router, so it has to exist by then.
    /// Stated as an edge rather than left to a priority: a priority only
    /// breaks ties between providers that are already free of edges, so a
    /// missing one does not look like a mistake anywhere — it compiles, and
    /// the first sign is this boot failing on state that had not been
    /// published yet.
    fn order(&self) -> ProviderOrder {
        ProviderOrder::new().after::<InvocationRouter>()
    }

    async fn boot(
        &self,
        state: &AppState
    ) -> registry::Result<()> {
        // A master with workers serves no traffic, and an interpreter it never
        // calls is a few hundred milliseconds and a copy of the application's
        // memory for nothing. Standalone is not a master, so it still boots.
        if state.is_master() {
            return Ok(());
        }

        let invocations = state
            .registry_ref()
            .resolve::<InvocationRouter>()
            .ok_or_else(|| registry::Error::msg("InvocationRouter is not registered"))?;

        let application = PythonApplication::new(self.target.clone())
            .map_err(|error| registry::Error::msg(error.to_string()))?
            .module_path(&self.application_root)
            .virtual_environment(&self.virtual_environment)
            .invocations(invocations);

        let running =
            application.boot().map_err(|error| registry::Error::msg(error.to_string()))?;
        let _ = self.handle.set(running.handle());
        *self.running.lock().expect("python application lock") = Some(running);
        Ok(())
    }

    fn as_finalizable(&self) -> Option<&dyn Finalizable<AppState>> {
        Some(self)
    }
}

#[async_trait]
impl Finalizable<AppState> for PythonApplicationService {
    async fn finalize(
        &self,
        _state: &AppState
    ) -> registry::Result<()> {
        let running = self.running.lock().expect("python application lock").take();
        match running {
            Some(running) => {
                running.shutdown().map_err(|error| registry::Error::msg(error.to_string()))
            }
            None => Ok(())
        }
    }
}
