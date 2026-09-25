//! WebSocket on gufi's own listener.
//!
//! Over Orbit the edge speaks the WebSocket protocol and gufi sees whole
//! messages; on its own listener gufi speaks it itself, with `fastwebsockets`.
//! Either way the application answers the handshake and exchanges the same
//! [`Message`]s, so neither a Python nor a Rust handler knows which door the
//! socket came through.

use bytes::Bytes;
pub use claviron::orbit::transport::Message;
pub use fastwebsockets;
use fastwebsockets::{FragmentCollector, Frame, OpCode, Payload, WebSocket};
use http::{Request, StatusCode};
use hyper::body::Incoming;
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;

use crate::asgi::{HttpRequest, PythonApplicationHandle, WebSocketDecision, WebSocketSession};
use crate::routing::{AppResponse, full_body};

/// Whether a request asks to become a WebSocket.
pub(crate) fn is_upgrade(request: &Request<Incoming>) -> bool {
    fastwebsockets::upgrade::is_upgrade_request(request)
}

/// Ask the application, answer the handshake, and serve the socket on a task
/// of its own once the connection is upgraded.
pub(crate) async fn serve(
    mut request: Request<Incoming>,
    scope: HttpRequest,
    python: &PythonApplicationHandle
) -> AppResponse {
    let Ok(WebSocketSession { decision, inbound, outbound }) = python.handle_websocket(scope)
    else {
        return status(StatusCode::SERVICE_UNAVAILABLE);
    };
    let headers = match decision.await {
        Ok(WebSocketDecision::Accept { headers }) => headers,
        // Refused, or the application ended first: ASGI's answer is 403.
        _ => return status(StatusCode::FORBIDDEN)
    };
    let (response, upgrade) = match fastwebsockets::upgrade::upgrade(&mut request) {
        Ok(upgrade) => upgrade,
        Err(error) => {
            tracing::debug!("🐍 WebSocket handshake refused: {error}");
            inbound.end();
            return status(StatusCode::BAD_REQUEST);
        }
    };
    let mut response = response.map(|_| full_body(Bytes::new()));
    for (name, value) in headers {
        if let (Ok(name), Ok(value)) =
            (http::HeaderName::from_bytes(&name), http::HeaderValue::from_bytes(&value))
        {
            response.headers_mut().append(name, value);
        }
    }
    tokio::spawn(async move {
        match upgrade.await {
            Ok(socket) => pump(socket, inbound, outbound).await,
            Err(error) => {
                tracing::debug!("🐍 WebSocket upgrade failed: {error}");
                inbound.end();
            }
        }
    });
    response
}

/// Speak the protocol with the client and pass whole messages to and from the
/// application: fragments joined, pings answered, closes echoed here.
async fn pump(
    mut socket: WebSocket<TokioIo<Upgraded>>,
    inbound: crate::asgi::WebSocketInbound,
    mut outbound: tokio::sync::mpsc::Receiver<Message>
) {
    socket.set_auto_pong(true);
    socket.set_auto_close(true);
    let mut socket = FragmentCollector::new(socket);
    loop {
        tokio::select! {
            frame = socket.read_frame() => {
                let message = match frame {
                    Ok(frame) => match frame.opcode {
                        OpCode::Text => match String::from_utf8(frame.payload.to_vec()) {
                            Ok(text) => Message::Text(text),
                            Err(_) => Message::Close { code: 1007, reason: String::new() }
                        },
                        OpCode::Binary => Message::Binary(frame.payload.to_vec()),
                        OpCode::Close => {
                            let (code, reason) = close_details(&frame.payload);
                            Message::Close { code, reason }
                        }
                        _ => continue
                    },
                    Err(_) => Message::Close { code: 1006, reason: String::new() }
                };
                let closing = matches!(message, Message::Close { .. });
                if !inbound.deliver(message).await || closing {
                    break;
                }
            }
            message = outbound.recv() => {
                let sent = match message {
                    Some(Message::Text(text)) => {
                        socket.write_frame(Frame::text(Payload::Owned(text.into_bytes()))).await
                    }
                    Some(Message::Binary(bytes)) => {
                        socket.write_frame(Frame::binary(Payload::Owned(bytes))).await
                    }
                    Some(Message::Close { code, reason }) => {
                        let _ = socket.write_frame(Frame::close(code, reason.as_bytes())).await;
                        break;
                    }
                    None => {
                        let _ = socket.write_frame(Frame::close(1011, b"")).await;
                        break;
                    }
                };
                if sent.is_err() {
                    break;
                }
            }
        }
    }
    inbound.end();
}

/// A close frame's code and reason; 1005 when it carries none.
fn close_details(payload: &[u8]) -> (u16, String) {
    if payload.len() < 2 {
        return (1005, String::new());
    }
    let code = u16::from_be_bytes([payload[0], payload[1]]);
    (code, String::from_utf8_lossy(&payload[2..]).into_owned())
}

fn status(status: StatusCode) -> AppResponse {
    let mut response = http::Response::new(full_body(Bytes::new()));
    *response.status_mut() = status;
    response
}
