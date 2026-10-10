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
use claviron::orbit::CounterProvider;
use claviron::runtime::{AppState, Bootable, Component, Finalizable, registry};

use crate::asgi::{
    PythonApplication, PythonApplicationHandle, RunningPythonApplication,
};
use crate::routing::PythonService;

pub(crate) struct PythonApplicationService {
    target: String,
    application_root: PathBuf,
    virtual_environment: PathBuf,
    handle: Arc<OnceLock<PythonApplicationHandle>>,
    running: Mutex<Option<RunningPythonApplication>>,
    /// The launch's `broadcasting:` block: the application
    /// `claviron.broadcast` publishes to by default, and Channels' layer.
    #[cfg(feature = "broadcast")]
    broadcasting: crate::options::Broadcasting,
}

impl PythonApplicationService {
    pub(crate) fn new(
        target: impl Into<String>,
        application_root: PathBuf,
        virtual_environment: PathBuf,
        #[cfg(feature = "broadcast")]
        broadcasting: crate::options::Broadcasting,
    ) -> Self {
        Self {
            target: target.into(),
            application_root,
            virtual_environment,
            handle: Arc::new(OnceLock::new()),
            running: Mutex::new(None),
            #[cfg(feature = "broadcast")]
            broadcasting,
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
impl Component<AppState> for PythonApplicationService {
    // Masters remain interpreter-free without requiring application handles.
    #[cfg(not(feature = "broadcast"))]
    type Deps = (Option<Arc<InvocationRouter>>, Option<Arc<CounterProvider>>);
    #[cfg(feature = "broadcast")]
    type Deps = (
        Option<Arc<InvocationRouter>>,
        Option<Arc<CounterProvider>>,
        Option<Arc<claviron::broadcast::BroadcastService>>,
    );

    fn name(&self) -> &'static str {
        "PythonApplicationService"
    }

    fn as_bootable(&self) -> Option<&dyn Bootable<AppState, Self::Deps>> {
        Some(self)
    }

    fn as_finalizable(&self) -> Option<&dyn Finalizable<AppState>> {
        Some(self)
    }
}

#[async_trait]
impl Finalizable<AppState> for PythonApplicationService {
    async fn finalize(
        &self,
        _state: &AppState,
    ) -> registry::Result<()> {
        let running =
            self.running.lock().expect("python application lock").take();
        match running {
            Some(running) => running
                .shutdown()
                .map_err(|error| registry::Error::msg(error.to_string())),
            None => Ok(()),
        }
    }
}

#[async_trait]
impl Bootable<AppState, <Self as Component<AppState>>::Deps>
    for PythonApplicationService
{
    async fn boot(
        &self,
        deps: <Self as Component<AppState>>::Deps,
        state: &AppState,
    ) -> registry::Result<()> {
        // A master with workers serves no traffic, and an interpreter it never
        // calls is a few hundred milliseconds and a copy of the application's
        // memory for nothing. Standalone is not a master, so it still boots.
        if state.is_master() {
            return Ok(());
        }

        #[cfg(not(feature = "broadcast"))]
        let (invocations, counters) = deps;
        #[cfg(feature = "broadcast")]
        let (invocations, counters, broadcast) = deps;
        let invocations = invocations.ok_or_else(|| {
            registry::Error::msg("InvocationRouter is not registered")
        })?;

        let mut application = PythonApplication::new(self.target.clone())
            .map_err(|error| registry::Error::msg(error.to_string()))?
            .module_path(&self.application_root)
            .virtual_environment(&self.virtual_environment)
            .invocations(invocations);
        if let Some(table) = counters.and_then(|provider| provider.handle()) {
            application = application.counters(table);
        }

        #[cfg(feature = "broadcast")]
        if let Some(service) = broadcast {
            if let Some(default_application) =
                self.broadcasting.application.clone()
            {
                application = application
                    .broadcast(Arc::clone(&service), default_application);
            }
            #[cfg(feature = "django")]
            if let Some(settings) = &self.broadcasting.channels {
                application = application.channels(
                    crate::django::channels::ChannelLayer::new(
                        service.groups(),
                        settings,
                    ),
                );
            }
        }

        let running = application
            .boot()
            .map_err(|error| registry::Error::msg(error.to_string()))?;
        let _ = self.handle.set(running.handle());
        *self.running.lock().expect("python application lock") = Some(running);
        Ok(())
    }
}
