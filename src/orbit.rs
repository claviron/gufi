//! Reliable message sessions over Link; network protocols belong to the edge.
//!
//! The ingress itself — the lane, the reader, exchanges and their bodies — is
//! the runner shell's (`claviron::runner::OrbitIngress`), and so is a Pusher
//! client's WebSocket, which is a broadcast session. What is Python's is any
//! other: the ASGI application's. The edge speaks the protocol and whole
//! messages cross the slot.

use std::future::{Future, poll_fn};
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use claviron::orbit::transport::{
    Endpoint, Message, MessageReader, write_message,
};
use claviron::runner::{AppRequest, Handshake};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::asgi::{
    MessageTransport, PythonApplicationHandle, WebSocketDecision,
    WebSocketInbound, WebSocketSession,
};

/// Default Link text/binary bound, matching the Lua message adapter's 1 MiB.
pub(crate) const DEFAULT_MESSAGE_LIMIT: usize = 1024 * 1024;

/// What the lane says serves this application's name. A PHP application given
/// the same name is refused at its join rather than splitting this one's
/// requests.
pub(crate) const ASGI_RUNTIME: &str = "asgi";

/// Each H1/H2 WebSocket or reliable WT bidi stream owns one ASGI invocation.
pub(crate) async fn session(
    request: AppRequest,
    endpoint: Endpoint,
    handshake: Handshake,
    python: Arc<RwLock<Option<PythonApplicationHandle>>>,
    message_limit: usize,
) {
    let Some(transport) = transport(&request) else {
        reject(handshake, endpoint, 400).await;
        return;
    };
    let handle = python.read().expect("python handle lock").clone();
    let Some(handle) = handle else {
        reject(handshake, endpoint, 503).await;
        return;
    };
    let (parts, _) = request.into_parts();
    let shutdown = parts
        .extensions
        .get::<claviron::runner::orbit::SessionShutdown>()
        .cloned();
    let retiring = async {
        match shutdown {
            Some(shutdown) => shutdown.cancelled().await,
            None => std::future::pending().await,
        }
    };
    tokio::pin!(retiring);
    let scope = crate::routing::scope(&parts);
    let Ok(WebSocketSession { decision, inbound, mut outbound }) =
        handle.handle_message_session(scope, transport)
    else {
        reject(handshake, endpoint, 503).await;
        return;
    };
    let (mut incoming, mut outgoing) = endpoint.split();
    let _accepted = EndOnDrop(&inbound);
    let reset = incoming.reset_signal();
    let decision = tokio::select! {
        biased;
        _ = poll_fn(|cx| reset.poll_reset(cx)) => return,
        () = &mut retiring => {
            if handshake.try_answer(503, Vec::new()) { let _ = outgoing.shutdown().await; }
            else { outgoing.reset(); }
            return;
        },
        decision = decision => decision,
    };
    let headers = match decision {
        Ok(WebSocketDecision::Accept { headers }) => headers,
        // Refused, or the application ended first: ASGI's answer is 403.
        _ => {
            if handshake.try_answer(403, Vec::new()) {
                let _ = outgoing.shutdown().await;
            } else {
                outgoing.reset();
            }
            return;
        }
    };
    if !handshake.try_answer(101, headers) {
        outgoing.reset();
        return;
    }
    tracing::debug!(?transport, "🛰 ASGI message session accepted");
    relay(
        &mut incoming,
        &mut outgoing,
        &inbound,
        &mut outbound,
        message_limit,
        retiring,
    )
    .await;
    tracing::debug!(?transport, "🛰 ASGI message session ended");
}

async fn reject(
    handshake: Handshake,
    endpoint: Endpoint,
    status: u16,
) {
    let (_incoming, mut outgoing) = endpoint.split();
    if handshake.try_answer(status, Vec::new()) {
        let _ = outgoing.shutdown().await;
    } else {
        outgoing.reset();
    }
}

fn transport(request: &AppRequest) -> Option<MessageTransport> {
    match (request.version(), request.method()) {
        (http::Version::HTTP_11, &http::Method::GET)
        | (http::Version::HTTP_2, &http::Method::CONNECT) => {
            Some(MessageTransport::WebSocket)
        }
        (http::Version::HTTP_3, &http::Method::CONNECT) => {
            Some(MessageTransport::WebTransport)
        }
        _ => None,
    }
}

trait Delivery: Send + Sync {
    fn deliver(
        &self,
        message: Message,
    ) -> impl Future<Output = bool> + Send;
    fn end(&self);
}

impl Delivery for WebSocketInbound {
    async fn deliver(
        &self,
        message: Message,
    ) -> bool {
        self.deliver(message).await
    }
    fn end(&self) {
        self.end();
    }
}

struct EndOnDrop<'a, D: Delivery>(&'a D);
impl<D: Delivery> Drop for EndOnDrop<'_, D> {
    fn drop(&mut self) {
        self.0.end();
    }
}

async fn relay<R: AsyncRead + Unpin, W: AsyncWrite + Unpin, D: Delivery>(
    incoming: &mut R,
    outgoing: &mut W,
    inbound: &D,
    outbound: &mut mpsc::Receiver<Message>,
    limit: usize,
    shutdown: impl Future<Output = ()>,
) {
    let application_done = CancellationToken::new();
    let peer_failed = CancellationToken::new();
    let retiring = AtomicBool::new(false);
    tokio::pin!(shutdown);

    // The client's messages to the application. Its end drops the sender, and
    // the application's next `receive()` reads the disconnect.
    let from_client = async {
        let _end = EndOnDrop(inbound);
        // Legal control records fit even when an operator chooses a smaller
        // text/binary bound. The shared codec checks lengths before allocation.
        let mut records = MessageReader::with_limit(limit.max(125));
        loop {
            let result = tokio::select! {
                biased;
                () = application_done.cancelled() => break,
                () = &mut shutdown => {
                    retiring.store(true, Ordering::Release);
                    let close = Message::Close { code: 1001, reason: "server shutting down".into() };
                    // Notify the ASGI owner in order, retaining its current
                    // work and allowing it to finish output before returning.
                    tokio::select! {
                        () = application_done.cancelled() => {},
                        _ = inbound.deliver(close) => {},
                    }
                    break;
                },
                result = records.read(incoming) => result,
            };
            let message = match result {
                Ok(Some(message))
                    if valid_message(&message, limit, true).is_ok() =>
                {
                    message
                }
                // FIN affects only input: the application may still send its
                // remaining response before returning.
                Ok(None) => break,
                _ => {
                    peer_failed.cancel();
                    break;
                }
            };
            let closing = matches!(message, Message::Close { .. });
            let delivered = tokio::select! {
                biased;
                () = application_done.cancelled() => false,
                delivered = inbound.deliver(message) => delivered,
            };
            if !delivered || closing {
                break;
            }
        }
    };
    // The application's messages to the client, until it closes or returns.
    let to_client = async {
        let _finished = application_done.clone().drop_guard();
        let mut closed = false;
        loop {
            let message = tokio::select! {
                biased;
                () = peer_failed.cancelled() => return,
                message = outbound.recv() => message,
            };
            let Some(message) = message else { break };
            if let Err(error) = valid_message(&message, limit, false) {
                tracing::debug!(%error, "ASGI produced an invalid Link message");
                return;
            }
            let closing = matches!(message, Message::Close { .. });
            let written = tokio::select! {
                biased;
                () = peer_failed.cancelled() => return,
                result = write_message(outgoing, &message) => result,
            };
            if written.is_err() {
                return;
            }
            if closing {
                closed = true;
                break;
            }
        }
        if !closed && retiring.load(Ordering::Acquire) {
            let close = Message::Close {
                code: 1001,
                reason: "server shutting down".into(),
            };
            // All earlier application messages completed before this close.
            let result = tokio::select! {
                () = peer_failed.cancelled() => return,
                result = write_message(outgoing, &close) => result,
            };
            if result.is_err() {
                return;
            }
        }
        let _ = tokio::select! {
            () = peer_failed.cancelled() => Ok(()),
            result = outgoing.shutdown() => result,
        };
    };
    tokio::join!(from_client, to_client);
}

fn valid_message(
    message: &Message,
    limit: usize,
    observation: bool,
) -> io::Result<()> {
    let valid = match message {
        Message::Text(text) => text.len() <= limit,
        Message::Binary(bytes) => bytes.len() <= limit,
        Message::Close { code, reason } => {
            (observation && matches!(code, 1005 | 1006) && reason.is_empty())
                || (matches!(code, 1000..=1003 | 1007..=1014 | 3000..=4999)
                    && reason.len() <= 123)
        }
    };
    if valid {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid or oversized Link message",
        ))
    }
}

#[cfg(test)]
#[path = "../tests/orbit.rs"]
mod tests;
