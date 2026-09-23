//! Transport-independent in-process ASGI application engine.

use std::ffi::CString;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};

use claviron::app::invoke::InvocationRouter;
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods, PyList, PyListMethods, PyModule};

const APPLICATION_DRIVER: &str = r#"
import asyncio
import traceback


def _exception_text(error):
    return "".join(traceback.format_exception(error)).strip()


async def _cancel(task):
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


async def drive_http(application, exchange, state):
    request_sent = False
    response_complete = False

    async def receive():
        nonlocal request_sent
        if request_sent:
            return {"type": "http.disconnect"}
        request_sent = True
        return {
            "type": "http.request",
            "body": bytes(exchange.body),
            "more_body": False,
        }

    async def send(message):
        nonlocal response_complete
        message_type = message.get("type")
        if message_type == "http.response.start":
            headers = [
                (bytes(name), bytes(value))
                for name, value in message.get("headers", [])
            ]
            exchange.response_start(message["status"], headers)
            return
        if message_type == "http.response.body":
            more_body = bool(message.get("more_body", False))
            exchange.response_body(bytes(message.get("body", b"")), more_body)
            response_complete = not more_body
            return
        raise RuntimeError(f"unexpected ASGI HTTP message: {message_type!r}")

    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.5"},
        "http_version": exchange.http_version,
        "method": exchange.method,
        "scheme": exchange.scheme,
        "path": exchange.path,
        "raw_path": bytes(exchange.raw_path),
        "query_string": bytes(exchange.query_string),
        "root_path": "",
        "headers": [
            (bytes(name), bytes(value)) for name, value in exchange.headers
        ],
        "client": exchange.client,
        "server": exchange.server,
        "state": state.copy(),
    }
    try:
        await application(scope, receive, send)
        if not response_complete:
            raise RuntimeError("ASGI application returned before completing the HTTP response")
    except BaseException as error:
        exchange.fail(_exception_text(error))


async def drive(application, control):
    incoming = asyncio.Queue()
    outgoing = asyncio.Queue()

    async def receive():
        return await incoming.get()

    async def send(message):
        await outgoing.put(message)

    state = {}
    scope = {
        "type": "lifespan",
        "asgi": {"version": "3.0", "spec_version": "2.0"},
        "state": state,
    }
    task = asyncio.create_task(application(scope, receive, send))
    startup_message = asyncio.create_task(outgoing.get())
    await incoming.put({"type": "lifespan.startup"})

    done, _ = await asyncio.wait(
        (task, startup_message), return_when=asyncio.FIRST_COMPLETED
    )

    # An application that ends the lifespan scope without answering does not
    # implement the protocol. That is allowed, and it is how most of them say
    # so: Django raises "can only handle ASGI/HTTP connections". Refusing is
    # not failing — what fails is `lifespan.startup.failed`, below, which an
    # application sends only after choosing to speak the protocol.
    lifespan = startup_message in done
    if not lifespan:
        error = task.exception()
        startup_message.cancel()
        control.lifespan_absent(
            _exception_text(error)
            if error is not None
            else "the application returned without sending a lifespan message"
        )
    else:
        message = startup_message.result()
        message_type = message.get("type")
        if message_type == "lifespan.startup.failed":
            detail = message.get("message") or "ASGI lifespan startup failed"
            control.startup_failed(detail)
            await _cancel(task)
            raise RuntimeError(detail)
        if message_type != "lifespan.startup.complete":
            detail = f"unexpected ASGI lifespan startup message: {message_type!r}"
            control.startup_failed(detail)
            await _cancel(task)
            raise RuntimeError(detail)

    control.startup_complete()
    requests = set()
    while True:
        exchange = await asyncio.to_thread(control.next_exchange)
        if exchange is None:
            break
        request = asyncio.create_task(drive_http(application, exchange, state))
        requests.add(request)
        request.add_done_callback(requests.discard)

    if requests:
        await asyncio.gather(*requests, return_exceptions=True)

    # An application that never spoke the protocol has nothing to be told the
    # process is ending, and the scope it refused is already over.
    if not lifespan:
        return

    shutdown_message = asyncio.create_task(outgoing.get())
    await incoming.put({"type": "lifespan.shutdown"})

    done, _ = await asyncio.wait(
        (task, shutdown_message), return_when=asyncio.FIRST_COMPLETED
    )
    if shutdown_message not in done:
        error = task.exception()
        if error is not None:
            raise error
        raise RuntimeError(
            "ASGI application returned before lifespan shutdown completed"
        )

    message = shutdown_message.result()
    message_type = message.get("type")
    if message_type == "lifespan.shutdown.failed":
        detail = message.get("message") or "ASGI lifespan shutdown failed"
        await _cancel(task)
        raise RuntimeError(detail)
    if message_type != "lifespan.shutdown.complete":
        await _cancel(task)
        raise RuntimeError(
            f"unexpected ASGI lifespan shutdown message: {message_type!r}"
        )

    await task
"#;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid ASGI target '{target}': expected non-empty module:attribute")]
    InvalidTarget { target: String },
    #[error("Python module path does not exist: {0}")]
    MissingModulePath(PathBuf),
    #[error("Python virtual environment does not exist: {0}")]
    MissingVirtualEnvironment(PathBuf),
    #[error("Python application failed: {0}")]
    Python(String),
    #[error("Python application thread stopped before reporting startup")]
    StartupChannelClosed,
    #[error("Python application thread panicked")]
    ThreadPanicked,
    #[error("Python application is no longer accepting requests")]
    ApplicationStopped,
    #[error("Python application stopped before completing the HTTP response")]
    ResponseChannelClosed,
    #[error("invalid ASGI HTTP response: {0}")]
    InvalidHttpResponse(String)
}

#[derive(Clone, Debug)]
pub struct HttpRequest {
    pub method: String,
    pub scheme: String,
    pub http_version: String,
    pub path: String,
    pub raw_path: Vec<u8>,
    pub query_string: Vec<u8>,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub body: Vec<u8>,
    pub client: Option<SocketAddr>,
    pub server: Option<SocketAddr>
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub body: Vec<u8>
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationTarget {
    module: String,
    attribute: String
}

impl ApplicationTarget {
    pub fn parse(target: impl Into<String>) -> Result<Self> {
        let target = target.into();
        let Some((module, attribute)) = target.split_once(':') else {
            return Err(Error::InvalidTarget { target });
        };
        let module = module.trim();
        let attribute = attribute.trim();
        if module.is_empty() || attribute.is_empty() {
            return Err(Error::InvalidTarget { target });
        }
        Ok(Self { module: module.to_owned(), attribute: attribute.to_owned() })
    }

    pub fn module(&self) -> &str {
        &self.module
    }

    pub fn attribute(&self) -> &str {
        &self.attribute
    }
}

#[derive(Clone)]
pub struct PythonApplication {
    target: ApplicationTarget,
    module_paths: Vec<PathBuf>,
    virtual_environment: Option<PathBuf>,
    invocations: Option<Arc<InvocationRouter>>
}

impl PythonApplication {
    pub fn new(target: impl Into<String>) -> Result<Self> {
        Ok(Self {
            target: ApplicationTarget::parse(target)?,
            module_paths: Vec::new(),
            virtual_environment: None,
            invocations: None
        })
    }

    pub fn module_path(
        mut self,
        path: impl Into<PathBuf>
    ) -> Self {
        self.module_paths.push(path.into());
        self
    }

    pub fn virtual_environment(
        mut self,
        path: impl Into<PathBuf>
    ) -> Self {
        self.virtual_environment = Some(path.into());
        self
    }

    pub fn target(&self) -> &ApplicationTarget {
        &self.target
    }

    pub fn invocations(
        mut self,
        router: Arc<InvocationRouter>
    ) -> Self {
        self.invocations = Some(router);
        self
    }

    pub fn boot(&self) -> Result<RunningPythonApplication> {
        for path in &self.module_paths {
            if !path.exists() {
                return Err(Error::MissingModulePath(path.clone()));
            }
        }
        if let Some(path) = &self.virtual_environment
            && !path.exists()
        {
            return Err(Error::MissingVirtualEnvironment(path.clone()));
        }

        Python::initialize();

        let definition = self.clone();
        let (commands, command_rx) = mpsc::channel();
        let (startup_tx, startup_rx) = mpsc::sync_channel(1);
        let handle = thread::Builder::new()
            .name("claviron-asgi".to_owned())
            .spawn(move || run_application(definition, command_rx, startup_tx))
            .map_err(|error| Error::Python(error.to_string()))?;

        match startup_rx.recv() {
            Ok(Ok(())) => Ok(RunningPythonApplication { commands, handle: Some(handle) }),
            Ok(Err(error)) => {
                join_thread(handle)?;
                Err(Error::Python(error))
            }
            Err(_) => match join_thread(handle) {
                Ok(()) => Err(Error::StartupChannelClosed),
                Err(error) => Err(error)
            }
        }
    }
}

pub struct RunningPythonApplication {
    commands: mpsc::Sender<ApplicationCommand>,
    handle: Option<JoinHandle<Result<()>>>
}

impl RunningPythonApplication {
    pub fn handle(&self) -> PythonApplicationHandle {
        PythonApplicationHandle { commands: self.commands.clone() }
    }

    pub fn shutdown(mut self) -> Result<()> {
        let _ = self.commands.send(ApplicationCommand::Shutdown);
        self.join()
    }

    fn join(&mut self) -> Result<()> {
        match self.handle.take() {
            Some(handle) => join_thread(handle),
            None => Ok(())
        }
    }
}

impl Drop for RunningPythonApplication {
    fn drop(&mut self) {
        let _ = self.commands.send(ApplicationCommand::Shutdown);
        let _ = self.join();
    }
}

#[derive(Clone)]
pub struct PythonApplicationHandle {
    commands: mpsc::Sender<ApplicationCommand>
}

impl PythonApplicationHandle {
    pub fn handle_http(
        &self,
        request: HttpRequest
    ) -> Result<HttpResponse> {
        let (response, response_rx) = mpsc::sync_channel(1);
        self.commands
            .send(ApplicationCommand::Http { request: Box::new(request), response })
            .map_err(|_| Error::ApplicationStopped)?;
        response_rx.recv().map_err(|_| Error::ResponseChannelClosed)?
    }
}

enum ApplicationCommand {
    Http { request: Box<HttpRequest>, response: mpsc::SyncSender<Result<HttpResponse>> },
    Shutdown
}

#[pyclass]
struct LifespanControl {
    startup: Mutex<Option<mpsc::SyncSender<std::result::Result<(), String>>>>,
    commands: Mutex<mpsc::Receiver<ApplicationCommand>>
}

#[pymethods]
impl LifespanControl {
    fn startup_complete(&self) {
        self.report_startup(Ok(()));
    }

    fn startup_failed(
        &self,
        message: String
    ) {
        self.report_startup(Err(message));
    }

    /// The application has no lifespan protocol, and that is allowed.
    ///
    /// ASGI says a server must cope with an application that does not
    /// implement `lifespan`, and the way an application says so is by
    /// refusing the scope — Django's handler raises "can only handle
    /// ASGI/HTTP connections", Flask's does the same. Treating that as a boot
    /// failure would lock out most of the ecosystem for a protocol whose whole
    /// content here is a startup and a shutdown notification.
    ///
    /// Logged rather than swallowed: a genuine crash during startup work
    /// arrives here looking exactly the same, and the only thing that tells
    /// the two apart is the sentence the application raised.
    fn lifespan_absent(
        &self,
        detail: String
    ) {
        tracing::debug!("🐍 application declines the ASGI lifespan protocol: {detail}");
    }

    fn next_exchange(
        &self,
        py: Python<'_>
    ) -> PyResult<Option<HttpExchange>> {
        match py.detach(|| self.commands.lock().unwrap().recv()) {
            Ok(ApplicationCommand::Http { request, response }) => {
                Ok(Some(HttpExchange::new(*request, response)))
            }
            Ok(ApplicationCommand::Shutdown) | Err(_) => Ok(None)
        }
    }
}

impl LifespanControl {
    fn report_startup(
        &self,
        result: std::result::Result<(), String>
    ) {
        if let Some(sender) = self.startup.lock().unwrap().take() {
            let _ = sender.send(result);
        }
    }
}

#[pyclass]
struct HttpExchange {
    request: HttpRequest,
    response: Mutex<HttpResponseState>
}

struct HttpResponseState {
    sender: Option<mpsc::SyncSender<Result<HttpResponse>>>,
    status: Option<u16>,
    headers: Vec<(Vec<u8>, Vec<u8>)>,
    body: Vec<u8>
}

impl HttpExchange {
    fn new(
        request: HttpRequest,
        sender: mpsc::SyncSender<Result<HttpResponse>>
    ) -> Self {
        Self {
            request,
            response: Mutex::new(HttpResponseState {
                sender: Some(sender),
                status: None,
                headers: Vec::new(),
                body: Vec::new()
            })
        }
    }
}

#[pymethods]
impl HttpExchange {
    #[getter]
    fn method(&self) -> String {
        self.request.method.clone()
    }

    #[getter]
    fn scheme(&self) -> String {
        self.request.scheme.clone()
    }

    #[getter]
    fn http_version(&self) -> String {
        self.request.http_version.clone()
    }

    #[getter]
    fn path(&self) -> String {
        self.request.path.clone()
    }

    #[getter]
    fn raw_path(&self) -> Vec<u8> {
        self.request.raw_path.clone()
    }

    #[getter]
    fn query_string(&self) -> Vec<u8> {
        self.request.query_string.clone()
    }

    #[getter]
    fn headers(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.request.headers.clone()
    }

    #[getter]
    fn body(&self) -> Vec<u8> {
        self.request.body.clone()
    }

    #[getter]
    fn client(&self) -> Option<(String, u16)> {
        self.request.client.map(|address| (address.ip().to_string(), address.port()))
    }

    #[getter]
    fn server(&self) -> Option<(String, u16)> {
        self.request.server.map(|address| (address.ip().to_string(), address.port()))
    }

    fn response_start(
        &self,
        status: u16,
        headers: Vec<(Vec<u8>, Vec<u8>)>
    ) -> PyResult<()> {
        if !(100..=999).contains(&status) {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "invalid HTTP response status {status}"
            )));
        }
        let mut response = self.response.lock().unwrap();
        if response.sender.is_none() {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "HTTP response is already complete"
            ));
        }
        if response.status.replace(status).is_some() {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "ASGI application sent http.response.start more than once"
            ));
        }
        response.headers = headers;
        Ok(())
    }

    fn response_body(
        &self,
        body: Vec<u8>,
        more_body: bool
    ) -> PyResult<()> {
        let mut response = self.response.lock().unwrap();
        if response.status.is_none() {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "ASGI application sent a response body before response start"
            ));
        }
        if response.sender.is_none() {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "HTTP response is already complete"
            ));
        }
        response.body.extend_from_slice(&body);
        if !more_body {
            let sender = response.sender.take().unwrap();
            let completed = HttpResponse {
                status: response.status.unwrap(),
                headers: std::mem::take(&mut response.headers),
                body: std::mem::take(&mut response.body)
            };
            let _ = sender.send(Ok(completed));
        }
        Ok(())
    }

    fn fail(
        &self,
        message: String
    ) {
        if let Some(sender) = self.response.lock().unwrap().sender.take() {
            let _ = sender.send(Err(Error::InvalidHttpResponse(message)));
        }
    }
}

impl Drop for HttpExchange {
    fn drop(&mut self) {
        if let Some(sender) = self.response.lock().unwrap().sender.take() {
            let _ = sender.send(Err(Error::ResponseChannelClosed));
        }
    }
}

fn run_application(
    definition: PythonApplication,
    commands: mpsc::Receiver<ApplicationCommand>,
    startup: mpsc::SyncSender<std::result::Result<(), String>>
) -> Result<()> {
    Python::attach(|py| {
        configure_python(py, &definition)?;

        if let Some(router) = definition.invocations {
            install_claviron_module(py, router)?;
        }

        let module =
            py.import(&definition.target.module).map_err(|error| python_error(py, error))?;
        let mut application = module
            .getattr(definition.target.attribute.split('.').next().unwrap())
            .map_err(|error| python_error(py, error))?;
        for attribute in definition.target.attribute.split('.').skip(1) {
            application =
                application.getattr(attribute).map_err(|error| python_error(py, error))?;
        }
        if !application.is_callable() {
            return Err(Error::Python(format!(
                "ASGI target '{}:{}' is not callable",
                definition.target.module, definition.target.attribute
            )));
        }

        let control = Py::new(
            py,
            LifespanControl { startup: Mutex::new(Some(startup)), commands: Mutex::new(commands) }
        )
        .map_err(|error| python_error(py, error))?;
        let code = CString::new(APPLICATION_DRIVER).expect("application driver has no NUL bytes");
        let filename = CString::new("<claviron-asgi>").unwrap();
        let name = CString::new("_claviron_asgi_lifespan").unwrap();
        let driver = PyModule::from_code(py, &code, &filename, &name)
            .map_err(|error| python_error(py, error))?;
        let coroutine = driver
            .getattr("drive")
            .and_then(|drive| drive.call1((application, control)))
            .map_err(|error| python_error(py, error))?;
        py.import("asyncio")
            .and_then(|asyncio| asyncio.call_method1("run", (coroutine,)))
            .map_err(|error| python_error(py, error))?;
        Ok(())
    })
}

#[pyclass]
struct PythonInvocationBridge {
    router: Arc<InvocationRouter>
}

#[pymethods]
impl PythonInvocationBridge {
    fn invoke<'py>(
        &self,
        py: Python<'py>,
        operation: String,
        payload: &Bound<'py, PyAny>
    ) -> PyResult<Bound<'py, PyAny>> {
        let payload = py.import("json")?.call_method1("dumps", (payload,))?.extract::<String>()?;
        let payload = serde_json::from_str(&payload)
            .map_err(|error| pyo3::exceptions::PyValueError::new_err(error.to_string()))?;
        let router = Arc::clone(&self.router);

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let response = router
                .invoke(&operation, payload)
                .await
                .map_err(pyo3::exceptions::PyRuntimeError::new_err)?;
            let response = serde_json::to_string(&response)
                .map_err(|error| pyo3::exceptions::PyRuntimeError::new_err(error.to_string()))?;
            Python::attach(|py| {
                py.import("json")
                    .and_then(|json| json.call_method1("loads", (response,)))
                    .map(Bound::unbind)
            })
        })
    }
}

/// Installs the module the application imports to reach this process.
///
/// The name is `claviron`, not the runner's. This engine installs it, and it
/// is here whether or not a runner sits above: an application that provides
/// its own binary and boots [`PythonApplication`] directly gets the same
/// module. The operations reached through it are Claviron's as well
/// (`claviron.ping.v1` and the rest), so the module is named after what is
/// answering rather than after whoever assembled the process.
fn install_claviron_module(
    py: Python<'_>,
    router: Arc<InvocationRouter>
) -> Result<()> {
    let module = PyModule::new(py, "claviron").map_err(|error| python_error(py, error))?;
    let bridge =
        Py::new(py, PythonInvocationBridge { router }).map_err(|error| python_error(py, error))?;
    let invoke = bridge.bind(py).getattr("invoke").map_err(|error| python_error(py, error))?;
    module.add("invoke", invoke).map_err(|error| python_error(py, error))?;
    module.add("_bridge", bridge).map_err(|error| python_error(py, error))?;

    let modules = py
        .import("sys")
        .and_then(|sys| sys.getattr("modules"))
        .map_err(|error| python_error(py, error))?;
    let modules =
        modules.cast_into::<PyDict>().map_err(|error| Error::Python(error.to_string()))?;
    PyDictMethods::set_item(&modules, "claviron", module)
        .map_err(|error| python_error(py, error))?;
    Ok(())
}

fn configure_python(
    py: Python<'_>,
    definition: &PythonApplication
) -> Result<()> {
    let sys = py.import("sys").map_err(|error| python_error(py, error))?;
    let sys_path = sys.getattr("path").map_err(|error| python_error(py, error))?;
    let sys_path =
        sys_path.cast_into::<PyList>().map_err(|error| Error::Python(error.to_string()))?;
    for path in definition.module_paths.iter().rev() {
        sys_path.insert(0, path_text(path)?).map_err(|error| python_error(py, error))?;
    }

    if let Some(virtual_environment) = &definition.virtual_environment {
        let version = sys.getattr("version_info").map_err(|error| python_error(py, error))?;
        let major: u8 = version
            .getattr("major")
            .and_then(|value| value.extract())
            .map_err(|error| python_error(py, error))?;
        let minor: u8 = version
            .getattr("minor")
            .and_then(|value| value.extract())
            .map_err(|error| python_error(py, error))?;
        let site_packages = if cfg!(windows) {
            virtual_environment.join("Lib").join("site-packages")
        } else {
            virtual_environment
                .join("lib")
                .join(format!("python{major}.{minor}"))
                .join("site-packages")
        };
        if !site_packages.exists() {
            return Err(Error::Python(format!(
                "Python virtual environment has no site-packages directory: {}",
                site_packages.display()
            )));
        }
        let site_packages = path_text(&site_packages)?;
        py.import("site")
            .and_then(|site| site.call_method1("addsitedir", (site_packages,)))
            .map_err(|error| python_error(py, error))?;
    }
    Ok(())
}

fn path_text(path: &Path) -> Result<&str> {
    path.to_str()
        .ok_or_else(|| Error::Python(format!("Python path is not valid UTF-8: {}", path.display())))
}

fn python_error(
    py: Python<'_>,
    error: PyErr
) -> Error {
    let rendered = py
        .import("traceback")
        .and_then(|traceback| traceback.call_method1("format_exception", (error.value(py),)))
        .and_then(|lines| lines.call_method0("__iter__"))
        .and_then(|iterator| {
            let mut rendered = String::new();
            loop {
                match iterator.call_method0("__next__") {
                    Ok(line) => rendered.push_str(&line.extract::<String>()?),
                    Err(stop) if stop.is_instance_of::<pyo3::exceptions::PyStopIteration>(py) => {
                        break;
                    }
                    Err(error) => return Err(error)
                }
            }
            Ok(rendered)
        })
        .unwrap_or_else(|_| error.to_string());
    Error::Python(rendered.trim().to_owned())
}

fn join_thread(handle: JoinHandle<Result<()>>) -> Result<()> {
    handle.join().map_err(|_| Error::ThreadPanicked)?
}

#[cfg(test)]
#[path = "../tests/asgi.rs"]
mod tests;
