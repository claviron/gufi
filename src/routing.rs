//! What an application registers in `configure`, and how Python sits behind it.
//!
//! An application composes its own routes — with axum, with warp, with a bare
//! `service_fn` — and registers the result. This crate holds no router: the
//! frameworks already have one, and each already knows how to say "not mine"
//! in its own vocabulary. Writing a third would only be a worse copy.
//!
//! What gets registered is a [`tower_service::Service`], which is deliberately
//! not a server. The development listener calls it with a request read off a
//! socket; an Orbit ingress calls the same service with a request read out of a
//! frame. `configure` is written once and never learns which one it is under.

use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};

use bytes::Bytes;
/// The request an ingress hands to the registered service, its body read as
/// it arrives — the runner shell's, shared by every runner.
pub use claviron::runner::{AppRequest, RequestBody};
#[cfg(feature = "axum")]
use claviron::runtime::{AppState, registry};
use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use percent_encoding::percent_decode_str;
use tower_service::Service;

use crate::asgi::{HttpRequest, PythonApplicationHandle};

/// What the registered service answers with.
///
/// The body streams: an application's reply reaches the client as it is
/// produced — a server-sent event as it is sent — and one that is whole says
/// its exact size, which is how an ingress knows it can hand it over at once.
pub type AppResponse = http::Response<ResponseBody>;

/// A reply body: whatever the application produced, boxed so every ingress
/// takes one type.
pub type ResponseBody = claviron::http::RouteBody;

/// The error a reply body can end with.
pub type BoxError = claviron::http::RouteBodyError;

/// A whole body, as a [`ResponseBody`].
pub fn full_body(bytes: impl Into<Bytes>) -> ResponseBody {
    Full::new(bytes.into()).map_err(|never| match never {}).boxed_unsync()
}

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// This application's routes in Rust, in front of its Python application.
///
/// ```ignore
/// // app.rs
/// pub fn configure(state: &AppState) -> registry::Result<()> {
///     gufi::register_router(state, routes::router)
/// }
///
/// // routes.rs
/// pub fn router(state: AppState) -> Router {
///     Router::new().route("/hello/{name}", get(hello)).with_state(state)
/// }
/// ```
///
/// The runner shell's `register_router`: **the Python application is the
/// fallback.** A path your router does not declare, and a declared path
/// reached with an undeclared method, both go to the interpreter, so the
/// application stays the authority on what exists and your routes only
/// intercept. The same routes serve under any Claviron runner.
#[cfg(feature = "axum")]
pub fn register_router<F>(
    state: &AppState,
    router: F,
) -> registry::Result<()>
where
    F: FnOnce(AppState) -> claviron::axum::Router,
{
    claviron::runner::register_router(state, router)
}

/// The Python application, as a service to put behind your own routes.
///
/// It is generic over the request body, so it fits a framework's fallback slot
/// without this crate knowing that framework. The handle inside is filled when
/// the interpreter boots, which happens before any runnable is spawned. It
/// follows the current application generation; a stopped application returns 503.
#[derive(Clone)]
pub struct PythonService {
    handle: Arc<RwLock<Option<PythonApplicationHandle>>>,
}

impl PythonService {
    /// The interpreter's handle as it will be once booted, for a stage that
    /// reaches Python outside the HTTP exchange — a WebSocket.
    pub(crate) fn handle_cell(
        &self
    ) -> Arc<RwLock<Option<PythonApplicationHandle>>> {
        Arc::clone(&self.handle)
    }

    pub(crate) fn new(
        handle: Arc<RwLock<Option<PythonApplicationHandle>>>
    ) -> Self {
        Self { handle }
    }

    /// Hand a request to the application and wait for its answer.
    ///
    /// The same thing the `Service` implementation does, as a plain method, so
    /// a closure can reach the interpreter without importing a trait.
    pub async fn handle<B>(
        &self,
        request: Request<B>,
    ) -> AppResponse
    where
        B: http_body::Body<Data = Bytes> + Send + 'static,
        B::Error: std::fmt::Display,
    {
        let handle = self.handle.read().expect("python handle lock").clone();
        dispatch(handle, request).await
    }
}

impl<B> Service<Request<B>> for PythonService
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: std::fmt::Display,
{
    type Response = AppResponse;
    type Error = Infallible;
    type Future = BoxFuture<Result<AppResponse, Infallible>>;

    fn poll_ready(
        &mut self,
        _: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(
        &mut self,
        request: Request<B>,
    ) -> Self::Future {
        let handle = self.handle.read().expect("python handle lock").clone();
        Box::pin(async move { Ok(dispatch(handle, request).await) })
    }
}

async fn dispatch<B>(
    handle: Option<PythonApplicationHandle>,
    request: Request<B>,
) -> AppResponse
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: std::fmt::Display,
{
    let Some(handle) = handle else {
        return text_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "Python application is stopped".to_owned(),
        );
    };
    let (parts, body) = request.into_parts();
    let body = body.map_err(|error| BoxError::from(error.to_string()));
    let scope = scope(&parts);

    match handle.handle_http(scope, body).await {
        Ok(response) => response_from_asgi(
            response.status,
            response.headers,
            response.body.map_err(BoxError::from).boxed_unsync(),
        )
        .unwrap_or_else(|message| {
            text_response(StatusCode::INTERNAL_SERVER_ERROR, message.to_owned())
        }),
        Err(crate::asgi::Error::ApplicationStopped) => text_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "Python application is stopped".to_owned(),
        ),
        Err(error) => {
            text_response(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
        }
    }
}

/// The ASGI scope of a request, HTTP or WebSocket alike. Where it came from is
/// read the way every owner reads it: a trusted hop's word first, the accepted
/// connection's otherwise.
pub(crate) fn scope(parts: &http::request::Parts) -> HttpRequest {
    let origin = claviron::http::trust::Origin::of(&parts.extensions);
    // A plain listener is `http`, which is what an unknown scheme reports.
    let scheme = origin
        .scheme
        .as_ref()
        .map(|scheme| scheme.as_str())
        .or(parts.uri.scheme_str())
        .unwrap_or("http")
        .to_owned();
    HttpRequest {
        method: parts.method.as_str().to_owned(),
        scheme,
        http_version: http_version(parts.version).to_owned(),
        path: percent_decode_str(parts.uri.path())
            .decode_utf8_lossy()
            .into_owned(),
        raw_path: parts.uri.path().as_bytes().to_vec(),
        query_string: parts.uri.query().unwrap_or_default().as_bytes().to_vec(),
        headers: parts
            .headers
            .iter()
            .map(|(name, value)| {
                (name.as_str().as_bytes().to_vec(), value.as_bytes().to_vec())
            })
            .collect(),
        client: origin.client,
        server: origin.server,
    }
}

fn response_from_asgi(
    status: u16,
    headers: Vec<(Vec<u8>, Vec<u8>)>,
    body: ResponseBody,
) -> Result<AppResponse, &'static str> {
    let status = StatusCode::from_u16(status)
        .map_err(|_| "invalid ASGI response status")?;
    let mut response = Response::new(body);
    *response.status_mut() = status;
    for (name, value) in headers {
        let (Ok(name), Ok(value)) = (
            http::HeaderName::from_bytes(&name),
            http::HeaderValue::from_bytes(&value),
        ) else {
            return Err("invalid ASGI response header");
        };
        response.headers_mut().append(name, value);
    }
    Ok(response)
}

pub(crate) fn text_response(
    status: StatusCode,
    message: String,
) -> AppResponse {
    let mut response = Response::new(full_body(message));
    *response.status_mut() = status;
    response
}

pub(crate) fn http_version(version: http::Version) -> &'static str {
    match version {
        http::Version::HTTP_09 => "0.9",
        http::Version::HTTP_10 => "1.0",
        http::Version::HTTP_11 => "1.1",
        http::Version::HTTP_2 => "2",
        http::Version::HTTP_3 => "3",
        _ => "1.1",
    }
}

#[cfg(test)]
#[path = "../tests/routing.rs"]
mod tests;
