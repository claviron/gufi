//! WebSockets over Orbit, which the edge speaks for this application.
//!
//! The ingress itself — the lane, the reader, exchanges and their bodies — is
//! the runner shell's (`claviron::runner::OrbitIngress`). What is Python's is
//! who answers a WebSocket: a Pusher client's is a broadcast session when this
//! process runs the broadcast service, and any other is the ASGI
//! application's. Either way the edge speaks the protocol and whole messages
//! cross the slot.

use std::sync::{Arc, OnceLock};

use claviron::orbit::transport::{Endpoint, Message, read_message, write_message};
use claviron::runner::{AppRequest, Handshake};
use tokio::io::AsyncWriteExt;

use crate::asgi::{PythonApplicationHandle, WebSocketDecision, WebSocketSession};

/// What the lane says serves this application's name. A PHP application given
/// the same name is refused at its join rather than splitting this one's
/// requests.
pub(crate) const ASGI_RUNTIME: &str = "asgi";

/// The broadcast service a Pusher WebSocket over Orbit is served by, when this
/// process runs one.
#[cfg(feature = "broadcast")]
pub(crate) type BroadcastRoute = Option<Arc<claviron::broadcast::BroadcastService>>;
#[cfg(not(feature = "broadcast"))]
#[derive(Clone)]
pub(crate) struct BroadcastRoute;

/// Answer a WebSocket the edge asked for, then carry messages both ways until
/// either side closes.
pub(crate) async fn session(
    request: AppRequest,
    endpoint: Endpoint,
    handshake: Handshake,
    python: Arc<OnceLock<PythonApplicationHandle>>,
    broadcast: BroadcastRoute
) {
    // A Pusher client's WebSocket is a broadcast session, served here and not
    // by the application.
    #[cfg(feature = "broadcast")]
    if let Some(service) = broadcast.as_deref()
        && let Some(session) = crate::broadcast::accept(service, request.uri().path())
    {
        let session = match session {
            Ok(session) => session,
            Err(status) => {
                handshake.answer(status, Vec::new());
                return;
            }
        };
        // The session is held before it is acknowledged, so a 101 never
        // announces one that could not start.
        handshake.answer(101, Vec::new());
        if let Err(error) =
            session.run(claviron::broadcast::stream::OrbitMessageStream::new(endpoint)).await
        {
            tracing::debug!("🛰 broadcast session ended: {error}");
        }
        return;
    }
    #[cfg(not(feature = "broadcast"))]
    let BroadcastRoute = broadcast;

    let Some(handle) = python.get().cloned() else {
        handshake.answer(503, Vec::new());
        return;
    };
    let (parts, _) = request.into_parts();
    let scope = crate::routing::scope(&parts);
    let Ok(WebSocketSession { decision, inbound, mut outbound }) = handle.handle_websocket(scope)
    else {
        handshake.answer(503, Vec::new());
        return;
    };
    let (mut incoming, mut outgoing) = endpoint.split();
    let headers = match decision.await {
        Ok(WebSocketDecision::Accept { headers }) => headers,
        // Refused, or the application ended first: ASGI's answer is 403.
        _ => {
            handshake.answer(403, Vec::new());
            let _ = outgoing.shutdown().await;
            return;
        }
    };
    handshake.answer(101, headers);
    tracing::debug!("🛰 WebSocket accepted");

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
    tracing::debug!("🛰 WebSocket ended");
}
