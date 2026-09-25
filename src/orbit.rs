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
use claviron::orbit::transport::{
    Exchange, ExchangeReply, FrameKind, TransportBodies, TransportSegment, frame_kind,
    parking_supported
};
use claviron::runtime::{
    AppState, FleetIncarnation, Provider, RunContext, Runnable, ServiceStartPolicy, registry
};
use http::{Request, Uri};
use http_body_util::{BodyExt, Full};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::routing::{Application, PeerAddrs, RequestScheme};

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
            .join(&self.application, incarnation)
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

/// One exchange, from the frame that arrived to the reply that leaves.
async fn serve(
    frame: Vec<u8>,
    lane: usize,
    application: Arc<Application>,
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

    let (mut reply_body, request_body) = match body {
        Some(endpoint) => {
            let (mut incoming, outgoing) = endpoint.split();
            let mut collected = Vec::new();
            if let Err(error) = incoming.read_to_end(&mut collected).await {
                tracing::debug!("exchange {correlation} body ended early: {error}");
                return;
            }
            (Some(outgoing), collected)
        }
        None => (None, Vec::new())
    };

    let request = match build_request(&exchange, request_body) {
        Ok(request) => request,
        Err(reason) => {
            tracing::debug!("exchange {correlation} could not be rebuilt: {reason}");
            return;
        }
    };

    let (parts, response_body) = application.call(request).await.into_parts();
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

    // The frame first and the body after it. The edge cannot start answering
    // its client without a status, and a body longer than the stream's window
    // would wait for a reader that has not been told to read yet.
    let status = reply.status;
    match reply.encode() {
        Ok(bytes) => {
            if let Err(error) = segment.inbox().write(origin, &bytes) {
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

    let mut sent = 0usize;
    if let Some(outgoing) = reply_body.as_mut() {
        let bytes =
            response_body.collect().await.map_or_else(|_| Bytes::new(), |body| body.to_bytes());
        sent = bytes.len();
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

    tracing::debug!(
        correlation,
        "🛰 ← {status} in {:?} (head {answered:?}, {sent} B)",
        arrived.elapsed()
    );
}

/// Rebuild the request the edge took apart.
///
/// `raw_path` rather than `path`: the transport carries both because ASGI
/// does, and building the URI from the raw one means the decoded one comes
/// back out of it unchanged instead of being decoded twice.
fn build_request(
    exchange: &Exchange,
    body: Vec<u8>
) -> Result<Request<Full<Bytes>>, String> {
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
    let mut request =
        request.body(Full::new(Bytes::from(body))).map_err(|error| error.to_string())?;

    request.extensions_mut().insert(RequestScheme(exchange.scheme.clone()));
    if let (Some(client), Some(server)) = (exchange.client, exchange.server) {
        request.extensions_mut().insert(PeerAddrs { client, server });
    }
    Ok(request)
}
