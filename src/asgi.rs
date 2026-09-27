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
use claviron::orbit::transport::Message;
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


async def drive_http(application, exchange, state, waiters):
    response_complete = False

    # The body as the ingress reads it, a part per message; once it is all
    # read, the next receive waits for the reply to complete or the client to
    # leave, and then says `http.disconnect`. Waits are resolved by the loop,
    # as a WebSocket's are.
    async def receive():
        event = exchange.try_receive()
        if event is None:
            waiter = asyncio.get_running_loop().create_future()
            waiters[exchange.id] = (exchange, waiter)
            try:
                event = await waiter
            finally:
                waiters.pop(exchange.id, None)
        return event

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
            if response_complete:
                # A receive waiting past the body learns the exchange is over.
                entry = waiters.get(exchange.id)
                if entry is not None and not entry[1].done():
                    event = exchange.try_receive()
                    if event is not None:
                        entry[1].set_result(event)
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


async def drive_websocket(application, socket, state, waiters):
    headers = [(bytes(name), bytes(value)) for name, value in socket.headers]
    subprotocols = []
    for name, value in headers:
        if name.lower() == b"sec-websocket-protocol":
            subprotocols += [
                part.strip() for part in value.decode("latin-1").split(",") if part.strip()
            ]
    scope = {
        "type": "websocket",
        "asgi": {"version": "3.0", "spec_version": "2.4"},
        "http_version": socket.http_version,
        "scheme": "wss" if socket.scheme == "https" else "ws",
        "path": socket.path,
        "raw_path": bytes(socket.raw_path),
        "query_string": bytes(socket.query_string),
        "root_path": "",
        "headers": headers,
        "client": socket.client,
        "server": socket.server,
        "subprotocols": subprotocols,
        "state": state.copy(),
    }
    connected = False

    async def receive():
        nonlocal connected
        if not connected:
            connected = True
            return {"type": "websocket.connect"}
        event = socket.try_receive()
        if event is None:
            waiter = asyncio.get_running_loop().create_future()
            waiters[socket.id] = (socket, waiter)
            try:
                event = await waiter
            finally:
                waiters.pop(socket.id, None)
        return event

    async def send(message):
        kind = message.get("type")
        if kind == "websocket.accept":
            socket.accept(
                message.get("subprotocol"),
                [(bytes(name), bytes(value)) for name, value in message.get("headers", [])],
            )
        elif kind == "websocket.send":
            waiting = socket.send(message.get("text"), message.get("bytes"))
            if waiting is not None:
                await waiting
        elif kind == "websocket.close":
            waiting = socket.close(int(message.get("code", 1000)), message.get("reason") or "")
            if waiting is not None:
                await waiting
        else:
            raise RuntimeError(f"unexpected ASGI websocket message: {kind!r}")

    try:
        await application(scope, receive, send)
    except BaseException as error:
        socket.fail(_exception_text(error))
    finally:
        socket.finish()


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
    # WebSockets waiting for a message and HTTP requests waiting for a part
    # of their body, by id: the doorbell names them and the loop resolves
    # their waits here, on its own thread.
    waiters = {}

    def answer():
        exchanges, ready, last = control.take_exchanges()
        for socket_id in ready:
            entry = waiters.pop(socket_id, None)
            if entry is None:
                continue
            socket, waiter = entry
            event = socket.try_receive()
            if event is None:
                waiters[socket_id] = entry
            elif not waiter.done():
                waiter.set_result(event)
        for exchange in exchanges:
            if exchange.kind == "websocket":
                work = drive_websocket(application, exchange, state, waiters)
            else:
                work = drive_http(application, exchange, state, waiters)
            request = loop.create_task(work)
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
    rest: Option<chunks::Receiver<Result<Bytes>>>,
    /// The request this answers: dropped before its end, this reply tells it
    /// the client is gone.
    request: Option<BodyFeed>
}

/// Whoever reads a reply — a socket's connection, the lane's slot — drops it
/// when the client goes, and a reply dropped before its end is the one signal
/// both ingresses give. It is what `receive()` reads as `http.disconnect`.
impl Drop for ResponseStream {
    fn drop(&mut self) {
        if let Some(request) = self.request.take()
            && !http_body::Body::is_end_stream(self)
        {
            request.client_gone();
        }
    }
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
    invocations: Option<Arc<InvocationRouter>>,
    counters: Option<Arc<claviron::orbit::Counter>>,
    #[cfg(feature = "broadcast")]
    broadcast: Option<crate::broadcast::Publisher>
}

impl PythonApplication {
    pub fn new(target: impl Into<String>) -> Result<Self> {
        Ok(Self {
            target: ApplicationTarget::parse(target)?,
            module_paths: Vec::new(),
            virtual_environment: None,
            invocations: None,
            counters: None,
            #[cfg(feature = "broadcast")]
            broadcast: None
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

    /// The broadcast service, as `claviron.broadcast(channel, event, data)`,
    /// publishing to `default_application` unless a call names another.
    #[cfg(feature = "broadcast")]
    pub fn broadcast(
        mut self,
        service: Arc<claviron::broadcast::BroadcastService>,
        default_application: String
    ) -> Self {
        self.broadcast = Some(crate::broadcast::Publisher::new(service, default_application));
        self
    }

    /// The fleet's counter table, as `claviron.counter(key)`.
    pub fn counters(
        mut self,
        table: Arc<claviron::orbit::Counter>
    ) -> Self {
        self.counters = Some(table);
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
    ///
    /// The body goes as it is read. What is already here when the request
    /// arrives — all of a bodiless or small one — is handed over with it and
    /// costs no wake; the rest follows a part at a time, each taken by
    /// `receive()` before the next is read, so an upload the application reads
    /// slowly is slowed at its source rather than held here.
    pub async fn handle_http<B>(
        &self,
        request: HttpRequest,
        body: B
    ) -> Result<HttpResponse>
    where
        B: http_body::Body<Data = Bytes> + Send + 'static,
        B::Error: std::fmt::Display + Send
    {
        let feed = BodyFeed {
            id: EXCHANGE_IDS.fetch_add(1, Ordering::Relaxed),
            shared: Arc::new(BodyInbound::default()),
            commands: self.commands.clone()
        };
        let mut body = Box::pin(body);
        let more = feed.take_ready(body.as_mut());
        let (response, response_rx) = oneshot::channel();
        self.commands
            .send(ApplicationCommand::Http {
                request: Box::new(request),
                body: feed.clone(),
                response
            })
            .map_err(|_| Error::ApplicationStopped)?;
        if more {
            tokio::spawn(feed.pump(body));
        }
        response_rx.await.map_err(|_| Error::ResponseChannelClosed)?
    }

    /// Hand a WebSocket to the application. It is answered through the
    /// session: first `decision`, then messages either way.
    pub fn handle_websocket(
        &self,
        request: HttpRequest
    ) -> Result<WebSocketSession> {
        let (decision, decided) = oneshot::channel();
        // One message in flight each way: the side that is behind makes the
        // other wait, rather than a buffer growing between them.
        let id = EXCHANGE_IDS.fetch_add(1, Ordering::Relaxed);
        let shared = Arc::new(Inbound::default());
        let (outbound_tx, outbound) = chunks::channel(1);
        let ends =
            WebSocketEnds { id, decision, inbound: Arc::clone(&shared), outbound: outbound_tx };
        self.commands
            .send(ApplicationCommand::WebSocket { request: Box::new(request), ends })
            .map_err(|_| Error::ApplicationStopped)?;
        let inbound = WebSocketInbound { id, shared, commands: self.commands.clone() };
        Ok(WebSocketSession { decision: decided, inbound, outbound })
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
    Http {
        request: Box<HttpRequest>,
        body: BodyFeed,
        response: oneshot::Sender<Result<HttpResponse>>
    },
    WebSocket {
        request: Box<HttpRequest>,
        ends: WebSocketEnds
    },
    /// A WebSocket the application is waiting on has a message, or its client
    /// is gone. The loop resolves the wait itself.
    Ready(u64),
    Shutdown
}

/// How the application answered a WebSocket's `websocket.connect`.
#[derive(Debug)]
pub enum WebSocketDecision {
    /// `websocket.accept`: the headers to add to the 101, a chosen
    /// subprotocol among them.
    Accept { headers: Vec<(Vec<u8>, Vec<u8>)> },
    /// `websocket.close` before accepting, or the application ending first:
    /// the handshake is refused (ASGI says 403).
    Reject
}

/// The Rust side of one WebSocket: the answer to the handshake, then messages
/// both ways until either side closes.
pub struct WebSocketSession {
    pub decision: oneshot::Receiver<WebSocketDecision>,
    /// The client's messages, to the application.
    pub inbound: WebSocketInbound,
    /// The application's messages, to the client.
    pub outbound: chunks::Receiver<Message>
}

/// The application's side of the same, handed to Python.
struct WebSocketEnds {
    id: u64,
    decision: oneshot::Sender<WebSocketDecision>,
    inbound: Arc<Inbound>,
    outbound: chunks::Sender<Message>
}

/// The client's messages waiting for the application.
///
/// Handed over the way requests are: the application's loop takes them
/// itself. A message is queued here, and only when the application is already
/// waiting does the doorbell ring for it — no thread but the loop's touches
/// Python, and a message that arrives before the application asks for it costs
/// no wake at all.
#[derive(Default)]
struct Inbound {
    state: Mutex<InboundState>,
    /// Signalled when the application takes a message: room for the next.
    room: tokio::sync::Notify
}

#[derive(Default)]
struct InboundState {
    queue: std::collections::VecDeque<Message>,
    /// The client is gone; the application reads a disconnect.
    closed: bool,
    /// The application asked and found nothing; the next message rings.
    waiting: bool,
    /// The application is gone; nothing more is worth queueing.
    abandoned: bool
}

/// Where the client's messages go, held by the ingress.
pub struct WebSocketInbound {
    id: u64,
    shared: Arc<Inbound>,
    commands: Commands
}

impl WebSocketInbound {
    /// Queue one message, waiting while the last is still unread: a client
    /// faster than the application is slowed at the slot, not buffered here.
    /// `false` once the application is gone.
    pub async fn deliver(
        &self,
        message: Message
    ) -> bool {
        let mut message = Some(message);
        loop {
            let room = self.shared.room.notified();
            tokio::pin!(room);
            room.as_mut().enable();
            {
                let mut state = self.shared.state.lock().unwrap();
                if state.abandoned {
                    return false;
                }
                if state.queue.is_empty() {
                    state.queue.push_back(message.take().expect("delivered once"));
                    let ring = std::mem::take(&mut state.waiting);
                    drop(state);
                    if ring {
                        let _ = self.commands.send(ApplicationCommand::Ready(self.id));
                    }
                    return true;
                }
            }
            room.await;
        }
    }

    /// The client is gone.
    pub fn end(&self) {
        let mut state = self.shared.state.lock().unwrap();
        state.closed = true;
        let ring = std::mem::take(&mut state.waiting);
        drop(state);
        if ring {
            let _ = self.commands.send(ApplicationCommand::Ready(self.id));
        }
    }
}

/// One numbering for WebSockets and HTTP requests: the loop keeps their waits
/// in one table, by id.
static EXCHANGE_IDS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// A request body on its way to the application.
///
/// Handed over the way a WebSocket's messages are: parts queue here and the
/// application's loop takes them itself, and only when it is already waiting
/// does the doorbell ring. One part is queued at a time — the reader behind
/// the ingress is not read further until the application has taken it.
#[derive(Default)]
struct BodyInbound {
    state: Mutex<BodyState>,
    /// Signalled when the application takes a part, or lets the request go.
    room: tokio::sync::Notify
}

#[derive(Default)]
struct BodyState {
    queue: std::collections::VecDeque<Bytes>,
    /// The ingress read the body's end.
    ended: bool,
    /// The body was cut short: too large, stalled, its client gone.
    failed: bool,
    /// `receive()` has said `more_body: False`.
    delivered: bool,
    /// The application completed its reply.
    replied: bool,
    /// The client left before the reply reached its end.
    gone: bool,
    /// The application asked and found nothing; the next change rings.
    waiting: bool,
    /// The application is done with the request; nothing more is read.
    abandoned: bool
}

/// Both ends of a request body: the ingress feeds it, the exchange reads it.
#[derive(Clone)]
struct BodyFeed {
    id: u64,
    shared: Arc<BodyInbound>,
    commands: Commands
}

impl std::fmt::Debug for BodyFeed {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>
    ) -> std::fmt::Result {
        f.debug_struct("BodyFeed").field("id", &self.id).finish_non_exhaustive()
    }
}

impl BodyFeed {
    /// Take what the body already holds, without waiting: a bodiless request
    /// has ended, a small one is one part and its end. Two looks, not a loop —
    /// a fast upload keeps refilling, and this is not the place to read it.
    /// `true` when there is more to read.
    fn take_ready<B>(
        &self,
        mut body: std::pin::Pin<&mut B>
    ) -> bool
    where
        B: http_body::Body<Data = Bytes>,
        B::Error: std::fmt::Display
    {
        use std::task::{Context, Poll, Waker};
        let mut context = Context::from_waker(Waker::noop());
        let mut state = self.shared.state.lock().unwrap();
        for _ in 0..2 {
            match body.as_mut().poll_frame(&mut context) {
                Poll::Ready(Some(Ok(frame))) => {
                    if let Ok(data) = frame.into_data()
                        && !data.is_empty()
                    {
                        state.queue.push_back(data);
                    }
                }
                Poll::Ready(Some(Err(_))) => {
                    state.failed = true;
                    return false;
                }
                Poll::Ready(None) => {
                    state.ended = true;
                    return false;
                }
                Poll::Pending => return true
            }
        }
        true
    }

    /// Read the rest, a part at a time, each after the application took the
    /// last.
    async fn pump<B>(
        self,
        mut body: std::pin::Pin<Box<B>>
    ) where
        B: http_body::Body<Data = Bytes>,
        B::Error: std::fmt::Display
    {
        use http_body_util::BodyExt;
        loop {
            match body.frame().await {
                Some(Ok(frame)) => {
                    let Ok(data) = frame.into_data() else { continue };
                    if !data.is_empty() && !self.deliver(data).await {
                        return;
                    }
                }
                Some(Err(error)) => {
                    tracing::debug!("request body ended early: {error}");
                    self.change(|state| state.failed = true);
                    return;
                }
                None => {
                    self.change(|state| state.ended = true);
                    return;
                }
            }
        }
    }

    /// Queue one part once the last is taken. `false` once the application is
    /// done with the request.
    async fn deliver(
        &self,
        part: Bytes
    ) -> bool {
        let mut part = Some(part);
        loop {
            let room = self.shared.room.notified();
            tokio::pin!(room);
            room.as_mut().enable();
            {
                let mut state = self.shared.state.lock().unwrap();
                if state.abandoned {
                    return false;
                }
                if state.queue.is_empty() {
                    state.queue.push_back(part.take().expect("delivered once"));
                    self.ring(state);
                    return true;
                }
            }
            room.await;
        }
    }

    /// The reply was dropped before its end.
    fn client_gone(&self) {
        self.change(|state| state.gone = true);
    }

    fn change(
        &self,
        apply: impl FnOnce(&mut BodyState)
    ) {
        let mut state = self.shared.state.lock().unwrap();
        apply(&mut state);
        self.ring(state);
    }

    fn ring(
        &self,
        mut state: std::sync::MutexGuard<'_, BodyState>
    ) {
        let ring = std::mem::take(&mut state.waiting);
        drop(state);
        if ring {
            let _ = self.commands.send(ApplicationCommand::Ready(self.id));
        }
    }
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
    fn take_exchanges(
        &self,
        py: Python<'_>
    ) -> PyResult<(Vec<Py<PyAny>>, Vec<u64>, bool)> {
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
        let mut ready = Vec::new();
        loop {
            match doorbell.queue.try_recv() {
                Ok(ApplicationCommand::Ready(id)) => ready.push(id),
                Ok(ApplicationCommand::Http { request, body, response }) => {
                    exchanges
                        .push(Py::new(py, HttpExchange::new(*request, body, response))?.into_any());
                }
                Ok(ApplicationCommand::WebSocket { request, ends }) => {
                    exchanges.push(Py::new(py, WebSocketExchange::new(*request, ends))?.into_any());
                }
                Ok(ApplicationCommand::Shutdown) | Err(mpsc::TryRecvError::Disconnected) => {
                    return Ok((exchanges, ready, true));
                }
                Err(mpsc::TryRecvError::Empty) => return Ok((exchanges, ready, closed))
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
    body: BodyFeed,
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
        body: BodyFeed,
        sender: oneshot::Sender<Result<HttpResponse>>
    ) -> Self {
        Self {
            request,
            body,
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
    fn kind(&self) -> &'static str {
        "http"
    }

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
    fn id(&self) -> u64 {
        self.body.id
    }

    /// The next ASGI receive event, or `None` to wait: the loop is rung when
    /// there is one.
    ///
    /// Parts as they came, `more_body` false on the last. Past the body,
    /// `http.disconnect` once the reply is complete or the client has left;
    /// a body cut short is a disconnect at once, which is what ASGI makes of
    /// a request that did not arrive whole.
    fn try_receive<'py>(
        &self,
        py: Python<'py>
    ) -> PyResult<Option<Bound<'py, PyDict>>> {
        let mut state = self.body.shared.state.lock().unwrap();
        let event = PyDict::new(py);
        if let Some(part) = state.queue.pop_front() {
            let last = state.queue.is_empty() && state.ended;
            state.delivered |= last;
            drop(state);
            self.body.shared.room.notify_one();
            event.set_item("type", "http.request")?;
            event.set_item("body", pyo3::types::PyBytes::new(py, &part))?;
            event.set_item("more_body", !last)?;
            return Ok(Some(event));
        }
        if state.ended && !state.delivered {
            state.delivered = true;
            event.set_item("type", "http.request")?;
            event.set_item("body", pyo3::types::PyBytes::new(py, b""))?;
            event.set_item("more_body", false)?;
            return Ok(Some(event));
        }
        if state.failed || state.gone || (state.delivered && state.replied) {
            event.set_item("type", "http.disconnect")?;
            return Ok(Some(event));
        }
        state.waiting = true;
        Ok(None)
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
        if !more_body {
            self.body.shared.state.lock().unwrap().replied = true;
        }
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
                body: ResponseStream { first, rest, request: Some(self.body.clone()) }
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
        // The application is done with the request: its body is not read any
        // further, and whatever feeds it stops.
        self.body.shared.state.lock().unwrap().abandoned = true;
        self.body.shared.room.notify_waiters();
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

#[pyclass]
struct WebSocketExchange {
    id: u64,
    request: HttpRequest,
    decision: Mutex<Option<oneshot::Sender<WebSocketDecision>>>,
    inbound: Arc<Inbound>,
    outbound: Mutex<Option<chunks::Sender<Message>>>
}

impl WebSocketExchange {
    fn new(
        request: HttpRequest,
        ends: WebSocketEnds
    ) -> Self {
        Self {
            id: ends.id,
            request,
            decision: Mutex::new(Some(ends.decision)),
            inbound: ends.inbound,
            outbound: Mutex::new(Some(ends.outbound))
        }
    }

    fn refuse_if_undecided(&self) {
        if let Some(decision) = self.decision.lock().unwrap().take() {
            let _ = decision.send(WebSocketDecision::Reject);
        }
    }

    fn abandon(&self) {
        self.inbound.state.lock().unwrap().abandoned = true;
        self.inbound.room.notify_waiters();
    }
}

#[pymethods]
impl WebSocketExchange {
    #[getter]
    fn kind(&self) -> &'static str {
        "websocket"
    }

    #[getter]
    fn id(&self) -> u64 {
        self.id
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
    fn client(&self) -> Option<(String, u16)> {
        self.request.client.map(|address| (address.ip().to_string(), address.port()))
    }

    #[getter]
    fn server(&self) -> Option<(String, u16)> {
        self.request.server.map(|address| (address.ip().to_string(), address.port()))
    }

    /// `websocket.accept`.
    fn accept(
        &self,
        subprotocol: Option<String>,
        headers: Vec<(Vec<u8>, Vec<u8>)>
    ) -> PyResult<()> {
        let Some(decision) = self.decision.lock().unwrap().take() else {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "the WebSocket was already accepted or closed"
            ));
        };
        let mut headers = headers;
        if let Some(subprotocol) = subprotocol {
            headers.push((b"sec-websocket-protocol".to_vec(), subprotocol.into_bytes()));
        }
        let _ = decision.send(WebSocketDecision::Accept { headers });
        Ok(())
    }

    /// The next `websocket.receive`, or `websocket.disconnect` once the client
    /// is gone — or `None` when nothing is there yet. The application then
    /// waits on a future of its own loop, and the doorbell resolves it when a
    /// message comes.
    fn try_receive(
        &self,
        py: Python<'_>
    ) -> PyResult<Option<Py<PyAny>>> {
        let message = {
            let mut state = self.inbound.state.lock().unwrap();
            match state.queue.pop_front() {
                Some(message) => Some(message),
                None if state.closed => None,
                None => {
                    state.waiting = true;
                    return Ok(None);
                }
            }
        };
        if message.is_some() {
            self.inbound.room.notify_one();
        }
        let event = PyDict::new(py);
        match message {
            Some(Message::Text(text)) => {
                event.set_item("type", "websocket.receive")?;
                event.set_item("text", text)?;
            }
            Some(Message::Binary(bytes)) => {
                event.set_item("type", "websocket.receive")?;
                event.set_item("bytes", pyo3::types::PyBytes::new(py, &bytes))?;
            }
            Some(Message::Close { code, reason }) => {
                event.set_item("type", "websocket.disconnect")?;
                event.set_item("code", code)?;
                event.set_item("reason", reason)?;
            }
            None => {
                event.set_item("type", "websocket.disconnect")?;
                event.set_item("code", 1006)?;
            }
        }
        Ok(Some(event.into_any().unbind()))
    }

    /// `websocket.send`. `None` when taken at once, an awaitable when the
    /// client is behind.
    fn send<'py>(
        &self,
        py: Python<'py>,
        text: Option<String>,
        bytes: Option<Vec<u8>>
    ) -> PyResult<Option<Bound<'py, PyAny>>> {
        let message = match (text, bytes) {
            (Some(text), _) => Message::Text(text),
            (None, Some(bytes)) => Message::Binary(bytes),
            (None, None) => return Ok(None)
        };
        let Some(outbound) = self.outbound.lock().unwrap().clone() else {
            return Ok(None);
        };
        match outbound.try_send(message) {
            Ok(()) | Err(chunks::error::TrySendError::Closed(_)) => Ok(None),
            Err(chunks::error::TrySendError::Full(message)) => {
                pyo3_async_runtimes::tokio::future_into_py(py, async move {
                    let _ = outbound.send(message).await;
                    Ok(())
                })
                .map(Some)
            }
        }
    }

    /// `websocket.close`: refuses the handshake if it was not accepted yet,
    /// closes the connection if it was.
    fn close<'py>(
        &self,
        py: Python<'py>,
        code: u16,
        reason: String
    ) -> PyResult<Option<Bound<'py, PyAny>>> {
        if let Some(decision) = self.decision.lock().unwrap().take() {
            let _ = decision.send(WebSocketDecision::Reject);
            return Ok(None);
        }
        let Some(outbound) = self.outbound.lock().unwrap().take() else { return Ok(None) };
        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let _ = outbound.send(Message::Close { code, reason }).await;
            Ok(())
        })
        .map(Some)
    }

    /// The application raised: refuse if undecided, and let the edge close
    /// the connection with 1011 by ending the outbound direction.
    fn fail(
        &self,
        message: String
    ) {
        tracing::debug!("🐍 WebSocket application failed: {message}");
        self.finish();
    }

    /// The application returned.
    fn finish(&self) {
        self.refuse_if_undecided();
        self.outbound.lock().unwrap().take();
        self.abandon();
    }
}

impl Drop for WebSocketExchange {
    fn drop(&mut self) {
        self.refuse_if_undecided();
        self.abandon();
    }
}

fn run_application(
    definition: PythonApplication,
    doorbell: Doorbell,
    startup: mpsc::SyncSender<std::result::Result<(), String>>
) -> Result<()> {
    Python::attach(|py| {
        configure_python(py, &definition)?;

        install_claviron_module(
            py,
            definition.invocations,
            definition.counters,
            #[cfg(feature = "broadcast")]
            definition.broadcast
        )?;

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
    router: Option<Arc<InvocationRouter>>,
    counters: Option<Arc<claviron::orbit::Counter>>,
    #[cfg(feature = "broadcast")] broadcast: Option<crate::broadcast::Publisher>
) -> Result<()> {
    #[cfg(feature = "broadcast")]
    let nothing = router.is_none() && counters.is_none() && broadcast.is_none();
    #[cfg(not(feature = "broadcast"))]
    let nothing = router.is_none() && counters.is_none();
    if nothing {
        return Ok(());
    }
    let module = PyModule::new(py, "claviron").map_err(|error| python_error(py, error))?;
    if let Some(router) = router {
        let bridge = Py::new(py, PythonInvocationBridge { router })
            .map_err(|error| python_error(py, error))?;
        let invoke = bridge.bind(py).getattr("invoke").map_err(|error| python_error(py, error))?;
        module.add("invoke", invoke).map_err(|error| python_error(py, error))?;
        module.add("_bridge", bridge).map_err(|error| python_error(py, error))?;
    }
    if let Some(table) = counters {
        let counters = Py::new(py, crate::counter::Counters::new(table))
            .map_err(|error| python_error(py, error))?;
        let counter =
            counters.bind(py).getattr("counter").map_err(|error| python_error(py, error))?;
        module.add("counter", counter).map_err(|error| python_error(py, error))?;
        module
            .add_class::<crate::counter::PythonCounter>()
            .map_err(|error| python_error(py, error))?;
    }

    #[cfg(feature = "broadcast")]
    if let Some(publisher) = broadcast {
        let broadcast = Py::new(py, crate::broadcast::PythonBroadcast::new(publisher))
            .map_err(|error| python_error(py, error))?;
        module.add("broadcast", broadcast).map_err(|error| python_error(py, error))?;
    }

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
