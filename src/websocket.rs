//! WebSocket on gufi's own listener.
//!
//! The protocol is the runner shell's (`claviron::runner::websocket`): it
//! answers the handshake, joins fragments, answers pings and hands whole
//! [`Message`]s over, the same ones a WebSocket over Orbit carries. What is
//! Python's is the decision — the ASGI application accepts or refuses — and
//! where the messages go.

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};

use bytes::Bytes;
pub use claviron::runner::websocket::Message;
use claviron::runner::websocket::{Deliver, accept, is_websocket};
use http::{Request, StatusCode};

use crate::asgi::{
    PythonApplicationHandle, WebSocketDecision, WebSocketInbound,
    WebSocketSession,
};
use crate::routing::{AppRequest, AppResponse, full_body};

impl Deliver for WebSocketInbound {
    fn deliver(
        &self,
        message: Message,
    ) -> impl Future<Output = bool> + Send {
        WebSocketInbound::deliver(self, message)
    }

    fn end(&self) {
        WebSocketInbound::end(self);
    }
}

/// The owner's answer to a request asking to take its connection over: a
/// WebSocket goes to the application, anything else is not one this serves.
#[derive(Clone)]
pub(crate) struct Upgrades {
    python: Arc<RwLock<Option<PythonApplicationHandle>>>,
}

impl Upgrades {
    pub(crate) fn new(
        python: Arc<RwLock<Option<PythonApplicationHandle>>>
    ) -> Self {
        Self { python }
    }
}

impl tower_service::Service<AppRequest> for Upgrades {
    type Response = AppResponse;
    type Error = Infallible;
    type Future =
        Pin<Box<dyn Future<Output = Result<AppResponse, Infallible>> + Send>>;

    fn poll_ready(
        &mut self,
        _: &mut Context<'_>,
    ) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(
        &mut self,
        request: AppRequest,
    ) -> Self::Future {
        let python = self.python.read().expect("python handle lock").clone();
        Box::pin(async move {
            if !is_websocket(&request) {
                return Ok(status(StatusCode::NOT_IMPLEMENTED));
            }
            let Some(python) = python else {
                return Ok(status(StatusCode::SERVICE_UNAVAILABLE));
            };
            let (parts, body) = request.into_parts();
            let mut scope = crate::routing::scope(&parts);
            // The handshake is HTTP/1.1 whatever the connection negotiated.
            scope.http_version = "1.1".to_owned();
            let Ok(WebSocketSession { decision, inbound, outbound }) =
                python.handle_websocket(scope)
            else {
                return Ok(status(StatusCode::SERVICE_UNAVAILABLE));
            };
            let headers = match decision.await {
                Ok(WebSocketDecision::Accept { headers }) => headers,
                // Refused, or the application ended first: ASGI's answer is 403.
                _ => return Ok(status(StatusCode::FORBIDDEN)),
            };
            Ok(accept(
                Request::from_parts(parts, body),
                headers,
                inbound,
                outbound,
            ))
        })
    }
}

fn status(status: StatusCode) -> AppResponse {
    let mut response = http::Response::new(full_body(Bytes::new()));
    *response.status_mut() = status;
    response
}
