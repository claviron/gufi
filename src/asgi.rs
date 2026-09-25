//! Transport-independent in-process ASGI application engine.

use std::ffi::CString;
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};

use bytes::Bytes;
use claviron::app::invoke::InvocationRouter;
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyDict, PyDictMethods, PyList, PyListMethods, PyModule};
use tokio::sync::{mpsc as chunks, oneshot};

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
            waiting = exchange.response_body(bytes(message.get("body", b"")), more_body)
            if waiting is not None:
                await waiting
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
    loop = asyncio.get_running_loop()
    finished = loop.create_future()
    doorbell = control.doorbell()

    # The loop takes requests itself: the doorbell is one more descriptor it
    # watches, and each time it is readable every request waiting is taken at
    # once. No second Python thread asks for work, so the GIL has one owner.
    def answer():
        exchanges, last = control.take_exchanges()
        for exchange in exchanges:
            request = loop.create_task(drive_http(application, exchange, state))
            requests.add(request)
            request.add_done_callback(requests.discard)
        if last and not finished.done():
            loop.remove_reader(doorbell)
            finished.set_result(None)

    loop.add_reader(doorbell, answer)
    answer()
    await finished

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

/// A reply's head, delivered with the first part of its body.
///
/// Not at `http.response.start`: a head that arrives before any body sends
/// its reader to wait on a body not yet written, which is a second wake for
/// every small reply. With the first part in hand, a reply that ends there is
/// whole and says its size, and one that goes on streams.
#[derive(Debug)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub body: ResponseStream
}

/// A reply body as the application sends it: the first part, then whatever
/// follows until `more_body` is false.
#[derive(Debug)]
pub struct ResponseStream {
    first: Option<Bytes>,
    rest: Option<chunks::Receiver<Result<Bytes>>>
}

impl http_body::Body for ResponseStream {
    type Data = Bytes;
    type Error = Error;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>
    ) -> std::task::Poll<Option<Result<http_body::Frame<Bytes>>>> {
        use std::task::Poll;
        if let Some(first) = self.first.take() {
            return Poll::Ready(Some(Ok(http_body::Frame::data(first))));
        }
        let Some(rest) = self.rest.as_mut() else { return Poll::Ready(None) };
        match rest.poll_recv(cx) {
            Poll::Ready(Some(part)) => Poll::Ready(Some(part.map(http_body::Frame::data))),
            Poll::Ready(None) => {
                self.rest = None;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending
        }
    }

    fn is_end_stream(&self) -> bool {
        self.first.is_none() && self.rest.is_none()
    }

    /// Exact when the reply ended with its first part — the whole body is in
    /// hand, and an ingress may hand it over before the head.
    fn size_hint(&self) -> http_body::SizeHint {
        match &self.rest {
            None => {
                http_body::SizeHint::with_exact(self.first.as_ref().map_or(0, |b| b.len() as u64))
            }
            Some(_) => http_body::SizeHint::default()
        }
    }
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

        initialize_interpreter(self.virtual_environment.as_deref())?;

        let definition = self.clone();
        let (commands, doorbell) = commands()?;
        let (startup_tx, startup_rx) = mpsc::sync_channel(1);
        let handle = thread::Builder::new()
            .name("claviron-asgi".to_owned())
            .spawn(move || run_application(definition, doorbell, startup_tx))
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
    commands: Commands,
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
    commands: Commands
}

impl PythonApplicationHandle {
    /// Hand one request to the application and wait for its answer.
    ///
    /// Awaited, not blocked on: the reply comes back on a oneshot the Python
    /// thread completes, so no thread is held for the length of a request and
    /// none has to be woken to deliver it. It used to run on a
    /// `spawn_blocking` thread parked on a channel — one per request in
    /// flight, and a wake on each side of it.
    pub async fn handle_http(
        &self,
        request: HttpRequest
    ) -> Result<HttpResponse> {
        let (response, response_rx) = oneshot::channel();
        self.commands
            .send(ApplicationCommand::Http { request: Box::new(request), response })
            .map_err(|_| Error::ApplicationStopped)?;
        response_rx.await.map_err(|_| Error::ResponseChannelClosed)?
    }
}

/// The way into the application thread: a queue, and a doorbell on the loop.
///
/// A command is queued first and the bell rung after, and only when it is
/// not already ringing: a burst of requests is one byte and one wake-up, and
/// the loop takes all of them when it answers. The loop clears `armed` before
/// it looks at the queue, so a command queued after that look rings again.
///
/// The writing end lives only here. When the last sender is gone the reading
/// end sees end-of-file, which is how the loop learns nothing more can come.
#[derive(Clone)]
struct Commands {
    queue: mpsc::Sender<ApplicationCommand>,
    bell: Arc<UnixStream>,
    armed: Arc<AtomicBool>
}

impl Commands {
    fn send(
        &self,
        command: ApplicationCommand
    ) -> std::result::Result<(), mpsc::SendError<ApplicationCommand>> {
        self.queue.send(command)?;
        if !self.armed.swap(true, Ordering::AcqRel) {
            // Full means a byte is already waiting to be read; either way the
            // loop wakes.
            let _ = (&*self.bell).write(&[1]);
        }
        Ok(())
    }
}

/// The reading side of [`Commands`], held by the application thread.
struct Doorbell {
    queue: mpsc::Receiver<ApplicationCommand>,
    bell: UnixStream,
    armed: Arc<AtomicBool>
}

fn commands() -> Result<(Commands, Doorbell)> {
    let (queue, receiver) = mpsc::channel();
    let (write, read) = UnixStream::pair().map_err(|error| Error::Python(error.to_string()))?;
    for end in [&write, &read] {
        end.set_nonblocking(true).map_err(|error| Error::Python(error.to_string()))?;
    }
    let armed = Arc::new(AtomicBool::new(false));
    Ok((
        Commands { queue, bell: Arc::new(write), armed: Arc::clone(&armed) },
        Doorbell { queue: receiver, bell: read, armed }
    ))
}

enum ApplicationCommand {
    Http { request: Box<HttpRequest>, response: oneshot::Sender<Result<HttpResponse>> },
    Shutdown
}

#[pyclass]
struct LifespanControl {
    startup: Mutex<Option<mpsc::SyncSender<std::result::Result<(), String>>>>,
    doorbell: Mutex<Doorbell>
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

    /// The descriptor the loop watches for requests.
    fn doorbell(&self) -> i32 {
        self.doorbell.lock().unwrap().bell.as_raw_fd()
    }

    /// Every request waiting, and whether this is the last call: the process
    /// is shutting down, or nothing can send any more.
    fn take_exchanges(&self) -> (Vec<HttpExchange>, bool) {
        let doorbell = self.doorbell.lock().unwrap();
        let mut rung = [0u8; 64];
        let mut closed = false;
        loop {
            match (&doorbell.bell).read(&mut rung) {
                Ok(0) => {
                    closed = true;
                    break;
                }
                Ok(_) => {}
                Err(_) => break
            }
        }
        doorbell.armed.store(false, Ordering::SeqCst);

        let mut exchanges = Vec::new();
        loop {
            match doorbell.queue.try_recv() {
                Ok(ApplicationCommand::Http { request, response }) => {
                    exchanges.push(HttpExchange::new(*request, response));
                }
                Ok(ApplicationCommand::Shutdown) | Err(mpsc::TryRecvError::Disconnected) => {
                    return (exchanges, true);
                }
                Err(mpsc::TryRecvError::Empty) => return (exchanges, closed)
            }
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
    /// Until the first body part: the head goes with it.
    sender: Option<oneshot::Sender<Result<HttpResponse>>>,
    status: Option<u16>,
    headers: Vec<(Vec<u8>, Vec<u8>)>,
    /// After it, while `more_body` is true: where the rest goes.
    rest: Option<chunks::Sender<Result<Bytes>>>,
    complete: bool
}

impl HttpExchange {
    fn new(
        request: HttpRequest,
        sender: oneshot::Sender<Result<HttpResponse>>
    ) -> Self {
        Self {
            request,
            response: Mutex::new(HttpResponseState {
                sender: Some(sender),
                status: None,
                headers: Vec::new(),
                rest: None,
                complete: false
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
        if response.complete {
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

    /// One part of the reply body. Returns `None` when the part was taken at
    /// once, or an awaitable when the reader is behind: `send()` then waits
    /// for room, which is how a slow client slows the application rather than
    /// filling memory.
    fn response_body<'py>(
        &self,
        py: Python<'py>,
        body: Vec<u8>,
        more_body: bool
    ) -> PyResult<Option<Bound<'py, PyAny>>> {
        let mut response = self.response.lock().unwrap();
        let Some(status) = response.status else {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "ASGI application sent a response body before response start"
            ));
        };
        if response.complete {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "HTTP response is already complete"
            ));
        }
        response.complete = !more_body;
        let part = Bytes::from(body);

        // The first part: the head goes with it.
        if let Some(sender) = response.sender.take() {
            let rest = if more_body {
                // One part in flight: the reader takes it before the next is
                // accepted. Any more would be memory the client has not asked
                // to spend.
                let (tx, rx) = chunks::channel(1);
                response.rest = Some(tx);
                Some(rx)
            } else {
                None
            };
            let first = (!part.is_empty()).then_some(part);
            let _ = sender.send(Ok(HttpResponse {
                status,
                headers: std::mem::take(&mut response.headers),
                body: ResponseStream { first, rest }
            }));
            return Ok(None);
        }

        // A later part: to the stream the first one opened.
        let tx = if more_body { response.rest.clone() } else { response.rest.take() };
        drop(response);
        let Some(tx) = tx else { return Ok(None) };
        if part.is_empty() {
            return Ok(None);
        }
        match tx.try_send(Ok(part)) {
            Ok(()) => Ok(None),
            // The reader is gone — the client left. Nothing to wait for.
            Err(chunks::error::TrySendError::Closed(_)) => Ok(None),
            Err(chunks::error::TrySendError::Full(part)) => {
                pyo3_async_runtimes::tokio::future_into_py(py, async move {
                    let _ = tx.send(part).await;
                    Ok(())
                })
                .map(Some)
            }
        }
    }

    fn fail(
        &self,
        message: String
    ) {
        let mut response = self.response.lock().unwrap();
        response.complete = true;
        if let Some(sender) = response.sender.take() {
            let _ = sender.send(Err(Error::InvalidHttpResponse(message)));
        } else if let Some(rest) = response.rest.take() {
            let _ = rest.try_send(Err(Error::InvalidHttpResponse(message)));
        }
    }
}

impl Drop for HttpExchange {
    fn drop(&mut self) {
        let mut response = self.response.lock().unwrap();
        if let Some(sender) = response.sender.take() {
            let _ = sender.send(Err(Error::ResponseChannelClosed));
        } else if !response.complete
            && let Some(rest) = response.rest.take()
        {
            let _ = rest.try_send(Err(Error::ResponseChannelClosed));
        }
    }
}

fn run_application(
    definition: PythonApplication,
    doorbell: Doorbell,
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
            LifespanControl { startup: Mutex::new(Some(startup)), doorbell: Mutex::new(doorbell) }
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

/// Start the interpreter as the environment's own `python` would start.
///
/// An embedded interpreter finds its standard library by walking up from the
/// executable it is told it is — and by default that is this binary, which
/// lives nowhere near a Python installation. The walk ends at `/`, and on a
/// merged-/usr system `/lib/python3.x` is the distribution's library: a
/// 3.13.13 interpreter then runs Debian's 3.13.5 stdlib without a word, and
/// only fails when a C module the two builds disagree on is missing.
///
/// Naming the environment's `bin/python` as the executable makes CPython read
/// its `pyvenv.cfg`: `home` gives the installation the library comes from,
/// and the environment becomes `sys.prefix`, exactly as when the application
/// is run by that `python`. Without an environment, CPython's own search
/// stands as before.
fn initialize_interpreter(virtual_environment: Option<&Path>) -> Result<()> {
    let executable = virtual_environment
        .map(|venv| {
            if cfg!(windows) {
                venv.join("Scripts").join("python.exe")
            } else {
                venv.join("bin").join("python")
            }
        })
        .filter(|python| python.exists());
    let Some(executable) = executable else {
        Python::initialize();
        return Ok(());
    };
    let executable =
        CString::new(path_text(&executable)?).map_err(|error| Error::Python(error.to_string()))?;

    // SAFETY: the config is initialised before use and cleared on every path;
    // the interpreter is started at most once, before any other Python call,
    // and the GIL it hands this thread is released as `Python::initialize`
    // releases it.
    unsafe {
        if pyo3::ffi::Py_IsInitialized() != 0 {
            return Ok(());
        }
        let mut config = std::mem::MaybeUninit::<pyo3::ffi::PyConfig>::uninit();
        pyo3::ffi::PyConfig_InitPythonConfig(config.as_mut_ptr());
        let mut config = config.assume_init();
        // What `Py_InitializeEx(0)` leaves alone: the process owns its
        // signals, its argv and its stdio.
        config.install_signal_handlers = 0;
        config.parse_argv = 0;
        config.configure_c_stdio = 0;

        let config_ptr: *mut pyo3::ffi::PyConfig = &mut config;
        let status = pyo3::ffi::PyConfig_SetBytesString(
            config_ptr,
            &raw mut (*config_ptr).executable,
            executable.as_ptr()
        );
        if pyo3::ffi::PyStatus_Exception(status) != 0 {
            pyo3::ffi::PyConfig_Clear(&mut config);
            return Err(Error::Python("could not set the interpreter's executable".to_owned()));
        }
        let status = pyo3::ffi::Py_InitializeFromConfig(&config);
        pyo3::ffi::PyConfig_Clear(&mut config);
        if pyo3::ffi::PyStatus_Exception(status) != 0 {
            return Err(Error::Python("the interpreter failed to start".to_owned()));
        }
        pyo3::ffi::PyEval_SaveThread();
    }
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

        // `addsitedir` appends, and it also reads the `.pth` files that
        // editable installs and namespace packages rely on. Appending alone
        // leaves the environment *behind* the interpreter's own directories,
        // so a package the system happens to ship wins over the one the
        // application installed — which is the opposite of what an
        // environment is for. Everything it added is therefore moved to the
        // front afterwards, keeping the order `site` chose among them.
        let before = sys_path.len();
        py.import("site")
            .and_then(|site| site.call_method1("addsitedir", (site_packages,)))
            .map_err(|error| python_error(py, error))?;

        let added: Vec<_> = (before..sys_path.len())
            .map(|index| sys_path.get_item(index))
            .collect::<std::result::Result<_, _>>()
            .map_err(|error| python_error(py, error))?;
        for index in (before..sys_path.len()).rev() {
            sys_path.del_item(index).map_err(|error| python_error(py, error))?;
        }
        for (offset, entry) in added.into_iter().enumerate() {
            sys_path.insert(offset, entry).map_err(|error| python_error(py, error))?;
        }
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
