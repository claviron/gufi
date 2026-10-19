//! Claviron's broadcast service in a Python application's process.
//!
//! Two things, both thin: `gufi.broadcast(...)`, which publishes from
//! Python straight into the service in this process — the round trip a
//! Pusher client library makes over HTTP to a remote server is not needed
//! when the server is here — and the acceptance of a Pusher WebSocket that
//! arrives over Orbit, which runs as a broadcast session rather than reaching
//! the ASGI application. The protocol, the channels and the sessions are
//! Claviron's.

use std::sync::Arc;

use claviron::broadcast::BroadcastService;
use claviron::broadcast::protocol::pusher::PublishedEvent;
use pyo3::prelude::*;
use pyo3::types::{PyList, PyString};

/// The service and the application a call publishes to when it names none.
#[derive(Clone)]
pub(crate) struct Publisher {
    service: Arc<BroadcastService>,
    default_application: String,
}

impl Publisher {
    pub(crate) fn new(
        service: Arc<BroadcastService>,
        default_application: String,
    ) -> Self {
        Self { service, default_application }
    }
}

/// `gufi.broadcast`, as Python sees it.
#[pyclass(name = "Broadcast", module = "gufi", frozen)]
pub(crate) struct PythonBroadcast {
    publisher: Publisher,
}

impl PythonBroadcast {
    pub(crate) fn new(publisher: Publisher) -> Self {
        Self { publisher }
    }
}

#[pymethods]
impl PythonBroadcast {
    /// Publish `event` on `channel` — a name or a list of names — and return
    /// how many subscribers in this process received it; the rest of the
    /// fleet receives it through Orbit. `data` goes as it is when it is a
    /// string and as JSON otherwise, which is what a Pusher client expects.
    #[pyo3(signature = (channel, event, data = None, *, application = None, socket_id = None))]
    fn __call__(
        &self,
        py: Python<'_>,
        channel: &Bound<'_, PyAny>,
        event: String,
        data: Option<&Bound<'_, PyAny>>,
        application: Option<String>,
        socket_id: Option<String>,
    ) -> PyResult<usize> {
        let channels = if let Ok(name) = channel.cast::<PyString>() {
            vec![name.to_str()?.to_owned()]
        } else if let Ok(names) = channel.cast::<PyList>() {
            names
                .iter()
                .map(|name| name.extract::<String>())
                .collect::<PyResult<Vec<_>>>()?
        } else {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "channel is a name or a list of names",
            ));
        };
        let data = match data {
            None => "{}".to_owned(),
            Some(value) => match value.cast::<PyString>() {
                Ok(text) => text.to_str()?.to_owned(),
                Err(_) => py
                    .import("json")?
                    .call_method1("dumps", (value,))?
                    .extract()?,
            },
        };
        let event = PublishedEvent { name: event, data, channels, socket_id };
        let application = application
            .unwrap_or_else(|| self.publisher.default_application.clone());
        self.publisher.service.publish(&application, &event).map_err(|error| {
            pyo3::exceptions::PyRuntimeError::new_err(error.to_string())
        })
    }
}
