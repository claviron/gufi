//! The ingress with no socket.
//!
//! In production nothing here binds a port. The process joins the shared
//! fleet, says which application it serves, and waits on its lane; a Claviron
//! edge worker that was asked for that name writes an exchange into it. The
//! service that answers is the same one [`crate::http::DevIngress`] calls —
//! `configure` never learns which door the request came through.
//!
//! ## Nobody is told about anybody
//!
//! There is no registration call, no supervisor and no pool. This process
//! starts on its own — its own binary, its own virtual environment — and
//! introduces itself by taking a lane and writing its name in it. The edge
//! reads the lanes serving a name and picks one. Several processes may serve
//! the same name without knowing about each other, and an edge started after
//! them finds them already there.
//!
//! ## The reader thread
//!
//! A lane is read by a thread parked on its wake word, not by a task polling
//! it. The word moves when a writer commits, and the wait ends because of
//! that — no interval, no deadline, nothing to tune. The thread hands frames
//! to Tokio over a channel as deep as the lane itself, so the lane is what
//! fills up and refuses, rather than two queues disagreeing about how full
//! things are.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use claviron::http::ext::RealIpExt;
use claviron::http::trust::Forwarded;
use claviron::orbit::transport::{
    Endpoint, Exchange, ExchangeReply, FrameKind, Message, TRANSPORT_BUFFER_BYTES, TransportBodies,
    TransportSegment, frame_kind, parking_supported, read_message, write_message
};
use claviron::runner::Application;
use claviron::runtime::{
    AppState, FleetIncarnation, Provider, RunContext, Runnable, ServiceStartPolicy, registry
};
use http::{Request, Uri};
use http_body_util::BodyExt;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::application::PythonApplicationService;
use crate::asgi::{HttpRequest, WebSocketDecision, WebSocketSession};

/// What this process's lane says serves its name. A PHP application given the
/// same name is refused at its join rather than splitting this one's requests.
const ASGI_RUNTIME: &str = "asgi";

/// Frames the reader thread may hand over before it has to wait.
///
/// A handoff, not a queue. The loop below spawns each frame on receipt, so
/// this only has to cover the gap between the reader delivering one and the
/// loop coming back round — a depth of a few is already generous, and
/// sixteen is chosen to be uninteresting rather than tuned.
///
/// Deliberately small, and deliberately *not* the lane's depth, which it used
/// to borrow. They answer opposite questions. The lane is where work waits
/// where the edge can see it: an edge reads a target's depth and picks the
/// idlest, so a backlog buffered inside this process is a backlog the edge is
/// choosing blind against. Work should queue in the lane, in the open, not
/// here.
const DISPATCH_HANDOFF: usize = 16;

/// This process's place in its application's fleet, as a runnable Claviron
/// owns.
pub(crate) struct OrbitIngress {
    application: String
}

impl OrbitIngress {
    pub(crate) fn new(application: impl Into<String>) -> Self {
        Self { application: application.into() }
    }
}

impl Provider<AppState> for OrbitIngress {
    fn name(&self) -> &'static str {
        "OrbitIngress"
    }

    fn as_runnable(self: Arc<Self>) -> Option<Arc<dyn Runnable<AppState>>> {
        Some(self)
    }
}

#[async_trait]
impl Runnable<AppState> for OrbitIngress {
    /// A master serves no traffic and takes no lane. Its workers each take
    /// their own, so an edge sees as many targets as there are interpreters.
    fn start_policy(
        &self,
        state: &AppState
    ) -> ServiceStartPolicy {
        if state.is_master() {
            ServiceStartPolicy::Unavailable
        } else {
            ServiceStartPolicy::Automatic
        }
    }

    async fn run(
        self: Arc<Self>,
        state: AppState,
        context: RunContext
    ) -> registry::Result<()> {
        if !parking_supported() {
            return Err(registry::Error::msg(
                "this host has no shared-memory wait primitive, so a lane cannot be read without \
                 polling"
            ));
        }

        let application = state
            .registry_ref()
            .resolve::<Application>()
            .ok_or_else(|| registry::Error::msg("no application service is registered"))?;
        // A WebSocket goes to Python directly: the application's own routes
        // answer HTTP, and there is no upgrade on this side of the slot.
        let python = state.registry_ref().resolve::<PythonApplicationService>();
        // Except a Pusher client's, when this process runs the broadcast
        // service: that one is a broadcast session, not the application's.
        #[cfg(feature = "broadcast")]
        let broadcast: BroadcastRoute =
            state.registry_ref().resolve::<claviron::broadcast::BroadcastService>();
        #[cfg(not(feature = "broadcast"))]
        let broadcast = BroadcastRoute;
        // The master's count of this worker slot's lives. It stamps the lane
        // and the streams, so a death report that names the process before
        // this one cannot end this one's work.
        let incarnation = state
            .registry_ref()
            .resolve::<FleetIncarnation>()
            .map_or(FleetIncarnation::FIRST, |held| *held)
            .raw();

        let segment = Arc::new(
            TransportSegment::for_application(&self.application)
                .map_err(|error| registry::Error::msg(error.to_string()))?
        );
        let lane = segment
            .inbox()
            .join(&self.application, ASGI_RUNTIME, incarnation)
            .map_err(|error| registry::Error::msg(error.to_string()))?;
        let bodies = Arc::new(
            TransportBodies::for_application(&self.application, lane, incarnation)
                .map_err(|error| registry::Error::msg(error.to_string()))?
        );

        // Info, like the development listener's line and for the same reason:
        // this is what an operator looks for when asking whether the edge can
        // see this process.
        tracing::info!("🛰 gufi serving '{}' on lane {lane} of its fleet", self.application);

        let (frames, mut arrived) = mpsc::channel::<Vec<u8>>(DISPATCH_HANDOFF);
        let stop = Arc::new(AtomicBool::new(false));
        let reader = spawn_reader(Arc::clone(&segment), lane, Arc::clone(&stop), frames)?;

        let mut exchanges = JoinSet::new();
        loop {
            tokio::select! {
                biased;
                () = context.cancelled() => break,
                frame = arrived.recv() => match frame {
                    Some(frame) => {
                        exchanges.spawn(serve(
                            frame,
                            lane,
                            Arc::clone(&application),
                            python.clone(),
                            broadcast.clone(),
                            Arc::clone(&segment),
                            Arc::clone(&bodies)
                        ));
                    }
                    None => break
                },
                Some(finished) = exchanges.join_next(), if !exchanges.is_empty() => {
                    if let Err(error) = finished {
                        tracing::debug!("exchange task failed: {error}");
                    }
                }
            }
        }

        // Stop being a target first, so nothing new is written into a lane
        // this process is about to stop reading, and the edge's next choice
        // lands on a worker that is still here.
        let _ = segment.inbox().release_lane(lane);

        // Then end the wait. The thread is parked on the lane's word, so it
        // is woken rather than waited out.
        stop.store(true, Ordering::Release);
        let _ = segment.inbox().nudge(lane);
        let _ = tokio::task::spawn_blocking(move || reader.join()).await;

        // Exchanges already taken are finished, not dropped: the caller is
        // still waiting for an answer it can have.
        while let Some(finished) = exchanges.join_next().await {
            if let Err(error) = finished {
                tracing::debug!("exchange task failed: {error}");
            }
        }
        Ok(())
    }
}

/// The thread that owns the lane.
///
/// It parks on the lane's word and drains everything the wake revealed before
/// parking again. `expected` is read before the drain, so a frame committed
/// while it was draining means the next park returns at once instead of
/// starting.
fn spawn_reader(
    segment: Arc<TransportSegment>,
    lane: usize,
    stop: Arc<AtomicBool>,
    frames: mpsc::Sender<Vec<u8>>
) -> registry::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new()
        .name(format!("gufi-lane-{lane}"))
        .spawn(move || {
            let inbox = segment.inbox();
            let mut frame = Vec::new();
            while !stop.load(Ordering::Acquire) {
                let Ok((_, expected)) = inbox.wait_word(lane) else { return };
                loop {
                    match inbox.read(lane, &mut frame) {
                        Ok(true) => {
                            if frames.blocking_send(std::mem::take(&mut frame)).is_err() {
                                return;
                            }
                        }
                        Ok(false) => break,
                        Err(error) => {
                            tracing::debug!("lane {lane} could not be read: {error}");
                            return;
                        }
                    }
                }
                if stop.load(Ordering::Acquire) {
                    return;
                }
                if let Err(error) = inbox.park(lane, expected) {
                    tracing::debug!("lane {lane} wait failed: {error}");
                    return;
                }
            }
        })
        .map_err(|error| registry::Error::msg(format!("lane reader thread: {error}")))
}

/// The broadcast service a Pusher WebSocket over Orbit is served by, when this
/// process runs one. Nothing when the feature is off.
#[cfg(feature = "broadcast")]
type BroadcastRoute = Option<Arc<claviron::broadcast::BroadcastService>>;
#[cfg(not(feature = "broadcast"))]
#[derive(Clone)]
struct BroadcastRoute;

/// One exchange, from the frame that arrived to the reply that leaves.
async fn serve(
    frame: Vec<u8>,
    lane: usize,
    application: Arc<Application>,
    python: Option<Arc<PythonApplicationService>>,
    broadcast: BroadcastRoute,
    segment: Arc<TransportSegment>,
    bodies: Arc<TransportBodies>
) {
    match frame_kind(&frame) {
        Ok(FrameKind::Request) => {}
        Ok(FrameKind::Reply) => {
            // An application lane receives requests. A reply here is a frame
            // written to the wrong lane, which is a bug in the writer rather
            // than something to answer.
            tracing::debug!("lane {lane} received a reply, which only an edge's lane takes");
            return;
        }
        Err(error) => {
            tracing::debug!("lane {lane} received an unreadable frame: {error}");
            return;
        }
    }

    let exchange = match Exchange::decode(&frame) {
        Ok(exchange) => exchange,
        Err(error) => {
            tracing::debug!("lane {lane} received a malformed exchange: {error}");
            return;
        }
    };
    let origin = exchange.origin as usize;
    let correlation = exchange.correlation;
    // Measured, not budgeted. Nothing here decides anything from the clock —
    // it is read so an operator can see what a request cost, which is the one
    // number neither side can infer from the other's logs.
    let arrived = Instant::now();
    tracing::debug!(
        correlation,
        origin,
        "🛰 → {} {} on lane {lane}",
        exchange.method,
        exchange.path
    );

    // The slot the edge created carries both bodies: the request arrives on
    // one direction and the reply leaves on the other.
    let body = match exchange.body {
        Some(ticket) => match bodies.accept(ticket) {
            Ok(endpoint) => Some(endpoint),
            Err(error) => {
                tracing::debug!("exchange {correlation} has no readable body: {error}");
                return;
            }
        },
        None => None
    };

    if exchange.upgrade {
        serve_websocket(&exchange, origin, body, python, broadcast, &segment).await;
        return;
    }

    // The request body is handed on as it arrives rather than read to its end
    // first: the edge streams it into the slot, and the application reads it
    // from there a part at a time. A body the edge cut short ends in an error,
    // not an end, and the application reads a disconnect.
    let (mut reply_body, request_body) = match body {
        Some(endpoint) => {
            let (incoming, outgoing) = endpoint.split();
            let piece = declared_length(&exchange.headers)
                .map_or(TRANSPORT_BUFFER_BYTES, |length| length.clamp(1, TRANSPORT_BUFFER_BYTES));
            (Some(outgoing), crate::routing::RequestBody::new(SlotBody::new(incoming, piece)))
        }
        None => (None, crate::routing::RequestBody::empty())
    };

    let request = match build_request(&exchange, request_body) {
        Ok(request) => request,
        Err(reason) => {
            tracing::debug!("exchange {correlation} could not be rebuilt: {reason}");
            return;
        }
    };

    let (parts, response_body) = application.call(request).await.into_parts();
    // A reply that is whole says its exact size. Taken here, it is in hand
    // before the head goes; anything else streams after the head.
    let whole = http_body::Body::size_hint(&response_body)
        .exact()
        .is_some_and(|size| size <= TRANSPORT_BUFFER_BYTES as u64);
    let mut streaming = None;
    let bytes = if whole {
        match response_body.collect().await {
            Ok(body) => body.to_bytes(),
            Err(error) => {
                tracing::debug!(correlation, "🛰 ✗ reply body failed: {error}");
                return;
            }
        }
    } else {
        streaming = Some(response_body);
        Bytes::new()
    };
    let reply = ExchangeReply {
        correlation,
        status: parts.status.as_u16(),
        headers: parts
            .headers
            .iter()
            .map(|(name, value)| (name.as_str().as_bytes().to_vec(), value.as_bytes().to_vec()))
            .collect(),
        body: exchange.body
    };

    // The body first when it fits the stream's window: written whole and
    // ended before the head goes, it is waiting when the head wakes the edge,
    // which then never parks on an empty slot. A body larger than the window
    // would wait for a reader not yet told to read, so it follows the head.
    let buffered = whole;
    if buffered && let Some(outgoing) = reply_body.as_mut() {
        if !bytes.is_empty()
            && let Err(error) = outgoing.write_all(&bytes).await
        {
            tracing::debug!(correlation, "🛰 ✗ reply body was cut short: {error}");
            return;
        }
        if let Err(error) = outgoing.shutdown().await {
            tracing::debug!(correlation, "🛰 ✗ reply body did not close: {error}");
        }
    }
    let encoded = reply.encode();

    // The frame first and the body after it. The edge cannot start answering
    // its client without a status, and a body longer than the stream's window
    // would wait for a reader that has not been told to read yet.
    let status = reply.status;
    match encoded {
        Ok(frame) => {
            if let Err(error) = segment.inbox().write(origin, &frame) {
                // Nobody to tell: the caller's lane is full or its process is
                // gone, and either way the answer has nowhere to go.
                tracing::debug!(correlation, "🛰 ✗ reply was not delivered: {error}");
                return;
            }
        }
        Err(error) => {
            tracing::debug!(correlation, "🛰 ✗ reply does not fit a frame: {error}");
            return;
        }
    }
    // The head is away. Timed separately from the body because that is the
    // moment the edge can start answering its client, and on a streamed
    // response the two are nothing alike.
    let answered = arrived.elapsed();

    let mut sent = bytes.len();
    if buffered {
        // Already written, before the head.
    } else if let (Some(outgoing), Some(mut response_body)) = (reply_body.as_mut(), streaming) {
        // Streamed: each part to the slot as the application sends it, so a
        // server-sent event reaches the edge when it is produced.
        while let Some(frame) = response_body.frame().await {
            let part = match frame {
                Ok(frame) => match frame.into_data() {
                    Ok(part) => part,
                    Err(_) => continue
                },
                Err(error) => {
                    tracing::debug!(correlation, "🛰 ✗ reply body failed: {error}");
                    let _ = outgoing.reset();
                    return;
                }
            };
            sent += part.len();
            if let Err(error) = outgoing.write_all(&part).await {
                tracing::debug!(correlation, "🛰 ✗ reply body was cut short: {error}");
                return;
            }
        }
        if let Err(error) = outgoing.shutdown().await {
            tracing::debug!(correlation, "🛰 ✗ reply body did not close: {error}");
        }
    }

    tracing::debug!(
        correlation,
        "🛰 ← {status} in {:?} (head {answered:?}, {sent} B{})",
        arrived.elapsed(),
        if buffered { ", buffered" } else { "" }
    );
}

/// A WebSocket the edge asked for: ask the application, answer the handshake,
/// then carry messages both ways as records on the slot until either side
/// closes.
async fn serve_websocket(
    exchange: &Exchange,
    origin: usize,
    endpoint: Option<Endpoint>,
    python: Option<Arc<PythonApplicationService>>,
    broadcast: BroadcastRoute,
    segment: &TransportSegment
) {
    let correlation = exchange.correlation;
    let answer = |status: u16, headers: Vec<(Vec<u8>, Vec<u8>)>| {
        let reply = ExchangeReply { correlation, status, headers, body: exchange.body };
        match reply.encode() {
            Ok(frame) => {
                if let Err(error) = segment.inbox().write(origin, &frame) {
                    tracing::debug!(correlation, "🛰 ✗ WebSocket answer was not delivered: {error}");
                }
            }
            Err(error) => tracing::debug!(correlation, "🛰 ✗ WebSocket answer: {error}")
        }
    };

    // A Pusher client's WebSocket is a broadcast session, served here and not
    // by the application. The edge speaks the protocol; the slot carries its
    // messages, which is what the session reads and writes.
    #[cfg(feature = "broadcast")]
    if let Some(service) = broadcast.as_deref()
        && let Some(outcome) = crate::broadcast::accept(service, exchange)
    {
        let (session, Some(endpoint)) = (outcome, endpoint) else {
            answer(400, Vec::new());
            return;
        };
        let session = match session {
            Ok(session) => session,
            Err(status) => {
                answer(status, Vec::new());
                return;
            }
        };
        // The session is held before it is acknowledged, so a 101 never
        // announces one that could not start.
        answer(101, Vec::new());
        let _activity = claviron::runtime::activity::ACTIVITY.open_session();
        if let Err(error) =
            session.run(claviron::broadcast::stream::OrbitMessageStream::new(endpoint)).await
        {
            tracing::debug!(correlation, "🛰 broadcast session ended: {error}");
        }
        return;
    }
    #[cfg(not(feature = "broadcast"))]
    let BroadcastRoute = broadcast;

    let (Some(endpoint), Some(handle)) = (endpoint, python.and_then(|python| python.handle()))
    else {
        answer(503, Vec::new());
        return;
    };
    let request = HttpRequest {
        method: exchange.method.clone(),
        scheme: exchange.scheme.clone(),
        http_version: crate::routing::http_version(exchange.version.into()).to_owned(),
        path: exchange.path.clone(),
        raw_path: exchange.raw_path.clone(),
        query_string: exchange.query.clone(),
        headers: exchange.headers.clone(),
        client: exchange.client,
        server: exchange.server
    };
    let Ok(WebSocketSession { decision, inbound, mut outbound }) = handle.handle_websocket(request)
    else {
        answer(503, Vec::new());
        return;
    };
    let (mut incoming, mut outgoing) = endpoint.split();
    let headers = match decision.await {
        Ok(WebSocketDecision::Accept { headers }) => headers,
        // Refused, or the application ended first: ASGI's answer is 403.
        _ => {
            answer(403, Vec::new());
            let _ = outgoing.shutdown().await;
            return;
        }
    };
    answer(101, headers);
    tracing::debug!(correlation, "🛰 WebSocket accepted");

    // The client's messages to the application. Its end drops the sender, and
    // the application's next `receive()` reads the disconnect.
    let from_client = async move {
        while let Ok(Some(message)) = read_message(&mut incoming).await {
            let closing = matches!(message, Message::Close { .. });
            if !inbound.deliver(message).await || closing {
                break;
            }
        }
        inbound.end();
    };
    // The application's messages to the client, until it closes or returns.
    let to_client = async move {
        while let Some(message) = outbound.recv().await {
            let closing = matches!(message, Message::Close { .. });
            if write_message(&mut outgoing, &message).await.is_err() || closing {
                break;
            }
        }
        let _ = outgoing.shutdown().await;
    };
    tokio::join!(from_client, to_client);
    tracing::debug!(correlation, "🛰 WebSocket ended");
}

/// Rebuild the request the edge took apart.
///
/// `raw_path` rather than `path`: the transport carries both because ASGI
/// does, and building the URI from the raw one means the decoded one comes
/// back out of it unchanged instead of being decoded twice.
fn build_request(
    exchange: &Exchange,
    body: crate::routing::RequestBody
) -> Result<crate::routing::AppRequest, String> {
    let mut target = exchange.raw_path.clone();
    if !exchange.query.is_empty() {
        target.push(b'?');
        target.extend_from_slice(&exchange.query);
    }
    let uri = Uri::try_from(target.as_slice()).map_err(|error| error.to_string())?;

    let mut request = Request::builder()
        .method(exchange.method.as_bytes())
        .uri(uri)
        .version(exchange.version.into());
    for (name, value) in &exchange.headers {
        request = request.header(name.as_slice(), value.as_slice());
    }
    let mut request = request.body(body).map_err(|error| error.to_string())?;

    // The edge accepted the connection and reported it in the exchange: a
    // trusted hop's word, which is what `Forwarded` carries. There is no
    // connection of this process's own to describe.
    if let Some(client) = exchange.client {
        request.extensions_mut().insert(RealIpExt(client.ip()));
    }
    request.extensions_mut().insert(Forwarded {
        client: exchange.client,
        server: exchange.server,
        scheme: exchange.scheme.parse().ok(),
        host: None
    });
    Ok(request)
}

/// The length a request declared, which sizes the reads of its body.
fn declared_length(headers: &[(Vec<u8>, Vec<u8>)]) -> Option<usize> {
    headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case(b"content-length"))
        .and_then(|(_, value)| std::str::from_utf8(value).ok()?.trim().parse().ok())
}

/// A request body read out of the exchange's slot, a part as large as the
/// ring holds at once.
///
/// The piece is sized the way the edge sizes a reply's: as long as the body
/// says it is, up to one window, so a small body is one read and a large one
/// is not cut into the stream's default four kilobytes. The buffer is reused:
/// a part splits off what was read and the rest of the allocation takes the
/// next.
struct SlotBody<R> {
    half: R,
    buffer: bytes::BytesMut,
    piece: usize
}

impl<R> SlotBody<R> {
    fn new(
        half: R,
        piece: usize
    ) -> Self {
        Self { half, buffer: bytes::BytesMut::new(), piece }
    }
}

impl<R> http_body::Body for SlotBody<R>
where
    R: tokio::io::AsyncRead + Unpin
{
    type Data = Bytes;
    type Error = crate::routing::BoxError;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>
    ) -> std::task::Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        use std::task::{Poll, ready};
        let this = &mut *self;
        if this.buffer.capacity() < this.piece {
            this.buffer.reserve(this.piece);
        }
        let read =
            ready!(tokio_util_poll_read(&mut this.half, &mut this.buffer, this.piece, context));
        Poll::Ready(match read {
            Ok(0) => None,
            Ok(_) => Some(Ok(http_body::Frame::data(this.buffer.split().freeze()))),
            Err(error) => Some(Err(error.into()))
        })
    }
}

/// Read into the spare capacity of `buffer`, at most `limit` bytes.
fn tokio_util_poll_read<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut R,
    buffer: &mut bytes::BytesMut,
    limit: usize,
    context: &mut std::task::Context<'_>
) -> std::task::Poll<std::io::Result<usize>> {
    use bytes::BufMut;
    let spare = buffer.chunk_mut();
    let len = spare.len().min(limit);
    // SAFETY: `ReadBuf` treats the slice as uninitialised and records only
    // what the reader fills; `advance_mut` then exposes exactly that much.
    let slice = unsafe { std::slice::from_raw_parts_mut(spare.as_mut_ptr().cast(), len) };
    let mut read = tokio::io::ReadBuf::uninit(slice);
    std::task::ready!(std::pin::Pin::new(reader).poll_read(context, &mut read))?;
    let filled = read.filled().len();
    unsafe { buffer.advance_mut(filled) };
    std::task::Poll::Ready(Ok(filled))
}
