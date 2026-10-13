//! Python's application lifecycle adapter. Link and fleet command transport
//! belong to the runner; ASGI lifespan and interpreter ownership stay here.

use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use claviron::app::management::{
    ApplicationFailed, ApplicationStarted, ApplicationStopped,
};
use claviron::app::{
    ApplicationControlResult, ApplicationController, ApplicationId,
    ApplicationLifecycle, ApplicationName, ApplicationRuntime,
    ApplicationSnapshot,
};
use claviron::runtime::{AppState, ProcessEventBus};
use tokio_util::sync::CancellationToken;

use crate::asgi::{
    PythonApplication, PythonApplicationHandle, RunningPythonApplication,
};

#[derive(Clone)]
pub(crate) struct PythonApplicationController {
    execution: Arc<Execution>,
}

struct Execution {
    id: ApplicationId,
    description: claviron::app::ApplicationDescription,
    application: PythonApplication,
    handle: Arc<RwLock<Option<PythonApplicationHandle>>>,
    current: Mutex<Current>,
    // Serialize native boot/drain on a blocking worker, never a Tokio reactor.
    operation: Mutex<()>,
    events: ProcessEventBus,
    shutdown: CancellationToken,
}

struct Current {
    running: Option<RunningPythonApplication>,
    lifecycle: ApplicationLifecycle,
    enabled: bool,
    generation: u64,
    last_error: Option<String>,
}

impl PythonApplicationController {
    pub(crate) fn new(
        application: PythonApplication,
        handle: Arc<RwLock<Option<PythonApplicationHandle>>>,
        description: claviron::app::ApplicationDescription,
        state: &AppState,
    ) -> ApplicationControlResult<Self> {
        let id = ApplicationId::new(
            ApplicationRuntime::new(crate::orbit::ASGI_RUNTIME)?,
            ApplicationName::new(state.name())?,
        );
        Ok(Self {
            execution: Arc::new(Execution {
                id,
                description,
                application,
                handle,
                current: Mutex::new(Current {
                    running: None,
                    lifecycle: ApplicationLifecycle::Stopped,
                    enabled: false,
                    generation: 0,
                    last_error: None,
                }),
                operation: Mutex::new(()),
                // Retain only the publication primitive and shutdown signal,
                // not AppState (which owns the manager and this controller).
                events: state.events().bus().clone(),
                shutdown: state.shutdown_token(),
            }),
        })
    }

    pub(crate) async fn start_application(
        &self
    ) -> ApplicationControlResult<()> {
        self.apply(Execution::start).await
    }

    pub(crate) async fn stop_application(
        &self
    ) -> ApplicationControlResult<()> {
        self.apply(Execution::stop).await
    }

    async fn apply(
        &self,
        operation: fn(&Execution) -> ApplicationControlResult<()>,
    ) -> ApplicationControlResult<()> {
        let execution = Arc::clone(&self.execution);
        // The task owns the transition even if its caller drops its future.
        tokio::task::spawn_blocking(move || {
            let _guard =
                execution.operation.lock().expect("python operation lock");
            operation(&execution)
        })
        .await?
    }

    fn require_name(
        &self,
        name: &ApplicationName,
    ) -> ApplicationControlResult<()> {
        if name == self.execution.id.name() {
            Ok(())
        } else {
            Err(format!("Python application '{name}' is not registered").into())
        }
    }
}

impl Execution {
    fn snapshot(&self) -> ApplicationSnapshot {
        // Observe native thread failure without polling, and without waiting
        // for a concurrent lifecycle operation to finish.
        if let Ok(_guard) = self.operation.try_lock() {
            self.reap_finished();
        }
        let current = self.current.lock().expect("python application lock");
        ApplicationSnapshot {
            id: self.id.clone(),
            description: self.description.clone(),
            lifecycle: current.lifecycle,
            enabled: current.enabled,
            generation: current.generation,
            // ASGI has a loop, not a PHP execution pool. No synthetic counters.
            pool: None,
            last_error: current.last_error.clone(),
        }
    }

    // Called only while operation holds the generation transition lock.
    fn start(&self) -> ApplicationControlResult<()> {
        if self.shutdown.is_cancelled() {
            return Err(
                "Python management is unavailable during shutdown".into()
            );
        }
        self.reap_finished();
        {
            let mut current =
                self.current.lock().expect("python application lock");
            if current.running.is_some() {
                return Ok(());
            }
            current.lifecycle = ApplicationLifecycle::Starting;
            current.enabled = true;
            current.generation = current.generation.saturating_add(1);
            current.last_error = None;
        }
        match self.application.boot() {
            Ok(running) => {
                let generation = {
                    let mut current =
                        self.current.lock().expect("python application lock");
                    *self.handle.write().expect("python handle lock") =
                        Some(running.handle());
                    current.running = Some(running);
                    current.lifecycle = ApplicationLifecycle::Running;
                    current.generation
                };
                self.events.emit(ApplicationStarted {
                    id: self.id.clone(),
                    generation,
                });
                Ok(())
            }
            Err(error) => {
                self.failed(error.to_string());
                Err(error.into())
            }
        }
    }

    fn stop(&self) -> ApplicationControlResult<()> {
        let running = {
            let mut current =
                self.current.lock().expect("python application lock");
            current.enabled = false;
            let Some(running) = current.running.take() else {
                return Ok(());
            };
            current.lifecycle = ApplicationLifecycle::Stopping;
            *self.handle.write().expect("python handle lock") = None;
            running
        };
        match running.shutdown() {
            Ok(()) => {
                let generation = {
                    let mut current =
                        self.current.lock().expect("python application lock");
                    current.lifecycle = ApplicationLifecycle::Stopped;
                    current.last_error = None;
                    current.generation
                };
                self.events.emit(ApplicationStopped {
                    id: self.id.clone(),
                    generation,
                });
                Ok(())
            }
            Err(error) => {
                self.failed(error.to_string());
                Err(error.into())
            }
        }
    }

    fn restart(&self) -> ApplicationControlResult<()> {
        if self.shutdown.is_cancelled() {
            return Err(
                "Python management is unavailable during shutdown".into()
            );
        }
        self.stop()?;
        self.start()
    }

    fn failed(
        &self,
        error: String,
    ) {
        let generation = {
            let mut current =
                self.current.lock().expect("python application lock");
            current.lifecycle = ApplicationLifecycle::Failed;
            current.enabled = false;
            current.last_error = Some(error.clone());
            current.generation
        };
        tracing::error!(application = %self.id, %error, "Python application lifecycle failed");
        self.events.emit(ApplicationFailed {
            id: self.id.clone(),
            generation,
            error,
        });
    }

    // operation is held; joining a finished thread cannot block on guest work.
    fn reap_finished(&self) {
        let running = {
            let mut current =
                self.current.lock().expect("python application lock");
            if !current
                .running
                .as_ref()
                .is_some_and(RunningPythonApplication::is_finished)
            {
                return;
            }
            *self.handle.write().expect("python handle lock") = None;
            current.running.take().expect("finished Python application")
        };
        let error = running.shutdown().err().map_or_else(
            || {
                "Python application loop exited without a stop command"
                    .to_owned()
            },
            |error| error.to_string(),
        );
        self.failed(error);
    }
}

#[async_trait]
impl ApplicationController for PythonApplicationController {
    fn runtime(&self) -> &ApplicationRuntime {
        self.execution.id.runtime()
    }

    fn list(&self) -> Vec<ApplicationSnapshot> {
        vec![self.execution.snapshot()]
    }

    fn status(
        &self,
        name: &ApplicationName,
    ) -> Option<ApplicationSnapshot> {
        (name == self.execution.id.name()).then(|| self.execution.snapshot())
    }

    async fn start(
        &self,
        name: &ApplicationName,
    ) -> ApplicationControlResult<()> {
        self.require_name(name)?;
        self.start_application().await
    }

    async fn stop(
        &self,
        name: &ApplicationName,
    ) -> ApplicationControlResult<()> {
        self.require_name(name)?;
        self.stop_application().await
    }

    async fn restart(
        &self,
        name: &ApplicationName,
    ) -> ApplicationControlResult<()> {
        self.require_name(name)?;
        self.apply(Execution::restart).await
    }
}
