//! The Python application component Claviron prepares and finalizes.
//!
//! The interpreter comes up in `boot()`, before any runnable is spawned, so an
//! ingress cannot be reached before the application behind it is ready. It goes
//! down in `finalize()`, after the runnables have drained — the ingress has
//! already stopped accepting and finished what it accepted by then, so
//! shutdown waits for work rather than interrupting it.

use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use claviron::app::ApplicationManager;
use claviron::app::invoke::InvocationRouter;
use claviron::orbit::CounterProvider;
use claviron::runtime::{AppState, Bootable, Component, Finalizable, registry};

use crate::asgi::{PythonApplication, PythonApplicationHandle};
use crate::management::PythonApplicationController;
use crate::routing::PythonService;

pub(crate) struct PythonApplicationService {
    description: claviron::app::ApplicationDescription,
    target: String,
    application_root: PathBuf,
    virtual_environment: PathBuf,
    handle: Arc<RwLock<Option<PythonApplicationHandle>>>,
    controller: Mutex<Option<Arc<PythonApplicationController>>>,
    /// The launch's `broadcasting:` block: the application
    /// `gufi.broadcast` publishes to by default, and Channels' layer.
    #[cfg(feature = "broadcast")]
    broadcasting: crate::options::Broadcasting,
}

impl PythonApplicationService {
    pub(crate) fn new(
        target: impl Into<String>,
        application_root: PathBuf,
        virtual_environment: PathBuf,
        frameworks: Vec<String>,
        icon: Option<String>,
        #[cfg(feature = "broadcast")]
        broadcasting: crate::options::Broadcasting,
    ) -> Self {
        Self {
            description: claviron::app::ApplicationDescription {
                frameworks,
                icon,
                ..claviron::app::ApplicationDescription::new("python", "ASGI")
                    .runner("Gufi")
            },
            target: target.into(),
            application_root,
            virtual_environment,
            handle: Arc::new(RwLock::new(None)),
            controller: Mutex::new(None),
            #[cfg(feature = "broadcast")]
            broadcasting,
        }
    }

    /// The application as a service, for an application to compose behind its
    /// own routes. The handle follows the managed generation. No ingress runs
    /// before boot, and a later application stop leaves the ingress alive.
    pub(crate) fn service(&self) -> PythonService {
        PythonService::new(Arc::clone(&self.handle))
    }
}

#[async_trait]
impl Component<AppState> for PythonApplicationService {
    // Masters remain interpreter-free without requiring application handles.
    #[cfg(not(feature = "broadcast"))]
    type Deps = (
        Arc<ApplicationManager>,
        Option<Arc<InvocationRouter>>,
        Option<Arc<CounterProvider>>,
    );
    #[cfg(feature = "broadcast")]
    type Deps = (
        Arc<ApplicationManager>,
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
        let controller =
            self.controller.lock().expect("python controller lock").take();
        match controller {
            Some(controller) => controller
                .stop_application()
                .await
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
        let (manager, invocations, counters) = deps;
        #[cfg(feature = "broadcast")]
        let (manager, invocations, counters, broadcast) = deps;
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

        let controller = Arc::new(
            PythonApplicationController::new(
                application,
                Arc::clone(&self.handle),
                self.description.clone(),
                state,
            )
            .map_err(|error| registry::Error::msg(error.to_string()))?,
        );
        manager
            .add(controller.clone())
            .map_err(|error| registry::Error::msg(error.to_string()))?;
        *self.controller.lock().expect("python controller lock") =
            Some(controller.clone());
        controller
            .start_application()
            .await
            .map_err(|error| registry::Error::msg(error.to_string()))
    }
}
