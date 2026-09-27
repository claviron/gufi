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
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};

use bytes::Bytes;
use claviron::runtime::{AppState, Provider, registry};
use http::{Request, Response, StatusCode};
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Full};
use percent_encoding::percent_decode_str;
use tower_service::Service;

use crate::application::PythonApplicationService;
use crate::asgi::{HttpRequest, PythonApplicationHandle};

/// The request an ingress hands to the registered service.
///
/// The body streams: a socket's connection and the lane's slot both deliver it
/// as it arrives, so a service reads it the same way whichever door it came
/// through. A Rust route that wants it whole asks for it whole (axum's
/// `Bytes`); the interpreter hands it to ASGI's `receive()` a part at a time.
pub type AppRequest = Request<RequestBody>;

/// A request body as an ingress hands it over: a socket connection's, or the
/// lane's slot, read as it arrives.
///
/// Its own type rather than an alias for a boxed body: a boxed error inside a
/// request type trips the compiler's `Send` check for any service future that
/// captures one, and a name says what it is besides.
pub struct RequestBody(UnsyncBoxBody<Bytes, BoxError>);

impl RequestBody {
    /// Box any body, its error carried as text.
    pub fn new<B>(body: B) -> Self
    where
        B: http_body::Body<Data = Bytes> + Send + 'static,
        B::Error: Into<BoxError>
    {
        Self(body.map_err(Into::into).boxed_unsync())
    }

    /// A request with no body.
    pub fn empty() -> Self {
        Self::new(http_body_util::Empty::<Bytes>::new())
    }
}

impl http_body::Body for RequestBody {
    type Data = Bytes;
    type Error = BoxError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, BoxError>>> {
        Pin::new(&mut self.0).poll_frame(context)
    }

    fn is_end_stream(&self) -> bool {
        self.0.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.0.size_hint()
    }
}

/// What the registered service answers with.
///
/// The body streams: an application's reply reaches the client as it is
/// produced — a server-sent event as it is sent — and one that is whole says
/// its exact size, which is how an ingress knows it can hand it over at once.
pub type AppResponse = Response<ResponseBody>;

/// A reply body: whatever the application produced, boxed so every ingress
/// takes one type.
pub type ResponseBody = UnsyncBoxBody<Bytes, BoxError>;

/// The error a reply body can end with.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// A whole body, as a [`ResponseBody`].
pub fn full_body(bytes: impl Into<Bytes>) -> ResponseBody {
    Full::new(bytes.into()).map_err(|never| match never {}).boxed_unsync()
}

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// Addresses of the connection a request arrived on.
///
/// ASGI's `scope` names both and `http::Request` carries neither, so the
/// ingress that accepted the request attaches this and the Python bridge reads
/// it. Nothing is invented: an ingress that does not know an address does not
/// attach one, and the scope reports `None`.
#[derive(Clone, Copy, Debug)]
pub struct PeerAddrs {
    pub client: SocketAddr,
    pub server: SocketAddr
}

/// The scheme the request arrived under.
///
/// `http::Uri` carries one only in absolute form, and a request read off a
/// socket or out of a frame has a path. The ingress knows — a TLS listener
/// served it, or an edge put it in the exchange — so it says here rather than
/// letting the scope guess. An ingress that does not know attaches nothing and
/// the scope reports `http`, which is what a plain listener is.
#[derive(Clone, Debug)]
pub struct RequestScheme(pub String);

/// The service this application answers requests with, as the runner holds it.
///
/// Registration erases the concrete type so one ingress can drive whatever the
/// application composed.
pub struct Application {
    service: Box<dyn ErasedService>
}

impl Application {
    /// Hand a request to whatever this application registered.
    ///
    /// Every ingress arrives here — the local listener, the fleet lane, and
    /// whatever comes next — which is why the exchange is counted here rather
    /// than in each of them. Claviron's edge does the same at its own single
    /// fork, so the number means the same thing on both sides of the
    /// transport: work this process has taken and not yet answered.
    pub(crate) fn call(
        &self,
        request: AppRequest
    ) -> BoxFuture<AppResponse> {
        let exchange = claviron::runtime::activity::ACTIVITY.begin();
        let answering = self.service.call_erased(request);
        Box::pin(async move {
            let response = answering.await;
            drop(exchange);
            response
        })
    }
}

impl Provider<AppState> for Application {
    fn name(&self) -> &'static str {
        "Application"
    }
}

trait ErasedService: Send + Sync + 'static {
    fn call_erased(
        &self,
        request: AppRequest
    ) -> BoxFuture<AppResponse>;
}

struct Erased<S>(S);

impl<S, B> ErasedService for Erased<S>
where
    S: Service<AppRequest, Response = Response<B>, Error = Infallible> + Clone + Send + Sync,
    S: 'static,
    S::Future: Send + 'static,
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: std::fmt::Display
{
    fn call_erased(
        &self,
        request: AppRequest
    ) -> BoxFuture<AppResponse> {
        let mut service = self.0.clone();
        Box::pin(async move {
            let Ok(response) = service.call(request).await;
            response
                .map(|body| body.map_err(|error| BoxError::from(error.to_string())).boxed_unsync())
        })
    }
}

fn register_service<S, B>(
    state: &AppState,
    service: S
) -> registry::Result<()>
where
    S: Service<AppRequest, Response = Response<B>, Error = Infallible> + Clone + Send + Sync,
    S: 'static,
    S::Future: Send + 'static,
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: std::fmt::Display
{
    state.registry_ref().insert(Arc::new(Application { service: Box::new(Erased(service)) }))?;
    Ok(())
}

/// Make sure something is registered to answer with.
///
/// An application that registered no routes is served by the interpreter
/// alone — which is the whole of it until its first Rust route exists. Putting
/// that case in the registry here rather than branching at every request keeps
/// one path through the ingress.
pub(crate) fn ensure_application(state: &AppState) -> registry::Result<()> {
    if state.registry_ref().resolve::<Application>().is_none() {
        register_service(state, python_service(state))?;
    }
    Ok(())
}

pub(crate) fn python_service(state: &AppState) -> PythonService {
    state
        .registry_ref()
        .resolve::<PythonApplicationService>()
        .expect("the runner registers the Python application before calling configure")
        .service()
}

/// This application's routes in Rust, in front of its Python application.
///
/// ```ignore
/// // app.rs
/// pub fn configure(state: &AppState) -> registry::Result<()> {
///     gufi::register_routes(state, routes::router)
/// }
///
/// // routes.rs
/// pub fn router(state: AppState) -> Router {
///     Router::new().route("/hello/{name}", get(hello)).with_state(state)
/// }
/// ```
///
/// Handed the function that builds the router, not the router: this crate
/// calls it with the application's state, so the call reads as what it is
/// rather than carrying a clone.
///
/// The router is yours and this crate has no opinion about it. What it does
/// have is the rule the whole runner is: **the Python application is the
/// fallback.** A path your router does not declare, and a declared path
/// reached with an undeclared method, both go to the interpreter — so the
/// application stays the authority on what exists and what does not, and your
/// routes only intercept. That wiring happens here rather than in your router,
/// because it is what this runner is rather than something to decide per
/// application. An application that does want to decide uses
/// [`register_routes_with`].
#[cfg(feature = "axum")]
pub fn register_routes<F>(
    state: &AppState,
    router: F
) -> registry::Result<()>
where
    F: FnOnce(AppState) -> claviron::axum::Router
{
    let router = router(state.clone());
    #[cfg(feature = "bench")]
    let router = {
        tracing::info!(
            "🐍 gufi bench routes mounted: /_gufi/rust and /_gufi/stats (feature \"bench\")"
        );
        router.merge(crate::bench::routes())
    };

    let python = python_service(state);
    let unmatched = python.clone();
    let router = router.fallback_service(python).method_not_allowed_fallback(
        move |request: claviron::axum::extract::Request| {
            let python = unmatched.clone();
            async move { python.handle(request).await }
        }
    );
    register_service(state, router)
}

/// This application's service, placing the interpreter yourself.
///
/// [`register_routes`] applies the rule for you: the Python application is the
/// fallback. Use this when the application wants to decide instead — its own
/// 404 for a subtree, axum's, the interpreter somewhere other than the end.
/// The builder is handed the Python application and puts it where it wants:
///
/// ```ignore
/// gufi::register_routes_with(state, |python| my_router().fallback_service(python))
/// ```
pub fn register_routes_with<F, S, B>(
    state: &AppState,
    build: F
) -> registry::Result<()>
where
    F: FnOnce(PythonService) -> S,
    S: Service<AppRequest, Response = Response<B>, Error = Infallible> + Clone + Send + Sync,
    S: 'static,
    S::Future: Send + 'static,
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: std::fmt::Display
{
    let service = build(python_service(state));
    register_service(state, service)
}

/// The Python application, as a service to put behind your own routes.
///
/// It is generic over the request body, so it fits a framework's fallback slot
/// without this crate knowing that framework. The handle inside is filled when
/// the interpreter boots, which happens before any runnable is spawned; calling
/// it earlier is a bug and says so rather than inventing a response.
#[derive(Clone)]
pub struct PythonService {
    handle: Arc<OnceLock<PythonApplicationHandle>>
}

impl PythonService {
    pub(crate) fn new(handle: Arc<OnceLock<PythonApplicationHandle>>) -> Self {
        Self { handle }
    }

    /// Hand a request to the application and wait for its answer.
    ///
    /// The same thing the `Service` implementation does, as a plain method, so
    /// a closure can reach the interpreter without importing a trait.
    pub async fn handle<B>(
        &self,
        request: Request<B>
    ) -> AppResponse
    where
        B: http_body::Body<Data = Bytes> + Send + 'static,
        B::Error: std::fmt::Display
    {
        let handle = self
            .handle
            .get()
            .expect("the Python application boots before any ingress is bound")
            .clone();
        dispatch(handle, request).await
    }
}

impl<B> Service<Request<B>> for PythonService
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: std::fmt::Display
{
    type Response = AppResponse;
    type Error = Infallible;
    type Future = BoxFuture<Result<AppResponse, Infallible>>;

    fn poll_ready(
        &mut self,
        _: &mut Context<'_>
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(
        &mut self,
        request: Request<B>
    ) -> Self::Future {
        let handle = Arc::clone(&self.handle);
        Box::pin(async move {
            let handle = handle
                .get()
                .expect("the Python application boots before any ingress is bound")
                .clone();
            Ok(dispatch(handle, request).await)
        })
    }
}

async fn dispatch<B>(
    handle: PythonApplicationHandle,
    request: Request<B>
) -> AppResponse
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: std::fmt::Display
{
    let peers = request.extensions().get::<PeerAddrs>().copied();
    let (parts, body) = request.into_parts();
    let body = body.map_err(|error| BoxError::from(error.to_string()));

    let scheme = match parts.extensions.get::<RequestScheme>() {
        Some(scheme) => scheme.0.clone(),
        None => parts.uri.scheme_str().unwrap_or("http").to_owned()
    };
    let scope = HttpRequest {
        method: parts.method.as_str().to_owned(),
        scheme,
        http_version: http_version(parts.version).to_owned(),
        path: percent_decode_str(parts.uri.path()).decode_utf8_lossy().into_owned(),
        raw_path: parts.uri.path().as_bytes().to_vec(),
        query_string: parts.uri.query().unwrap_or_default().as_bytes().to_vec(),
        headers: parts
            .headers
            .iter()
            .map(|(name, value)| (name.as_str().as_bytes().to_vec(), value.as_bytes().to_vec()))
            .collect(),
        client: peers.map(|peers| peers.client),
        server: peers.map(|peers| peers.server)
    };

    match handle.handle_http(scope, body).await {
        Ok(response) => response_from_asgi(
            response.status,
            response.headers,
            response.body.map_err(BoxError::from).boxed_unsync()
        )
        .unwrap_or_else(|message| {
            text_response(StatusCode::INTERNAL_SERVER_ERROR, message.to_owned())
        }),
        Err(error) => text_response(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
    }
}

fn response_from_asgi(
    status: u16,
    headers: Vec<(Vec<u8>, Vec<u8>)>,
    body: ResponseBody
) -> Result<AppResponse, &'static str> {
    let status = StatusCode::from_u16(status).map_err(|_| "invalid ASGI response status")?;
    let mut response = Response::new(body);
    *response.status_mut() = status;
    for (name, value) in headers {
        let (Ok(name), Ok(value)) =
            (http::HeaderName::from_bytes(&name), http::HeaderValue::from_bytes(&value))
        else {
            return Err("invalid ASGI response header");
        };
        response.headers_mut().append(name, value);
    }
    Ok(response)
}

pub(crate) fn text_response(
    status: StatusCode,
    message: String
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
        _ => "1.1"
    }
}
