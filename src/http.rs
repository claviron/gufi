//! The development listener.
//!
//! It binds plain HTTP/1, reads one request, and hands it to the service the
//! application registered. It owns no TLS, no vhost routing, no static files
//! and no supervision, and its numbers are not performance evidence — it
//! exists so an application can be run and read while it is being written.
//!
//! It knows nothing about routing and nothing about ASGI. It turns a socket
//! into an [`AppRequest`] and writes back an [`AppResponse`]; everything
//! between those two is the application's, which is why the same service runs
//! unchanged under an ingress that has no socket at all.

use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use claviron::runtime::{AppState, Provider, RunContext, Runnable, ServiceStartPolicy, registry};
use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;

use crate::routing::{AppRequest, AppResponse, Application, PeerAddrs};

pub(crate) type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("dev HTTP listener failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("dev HTTP connection failed: {0}")]
    Hyper(#[from] hyper::Error),
    #[error("dev HTTP connection task failed: {0}")]
    Join(#[from] tokio::task::JoinError)
}

pub(crate) struct DevServer {
    listener: TcpListener,
    local_addr: SocketAddr
}

impl DevServer {
    pub(crate) async fn bind(address: SocketAddr) -> Result<Self> {
        let listener = TcpListener::bind(address).await?;
        let local_addr = listener.local_addr()?;
        Ok(Self { listener, local_addr })
    }

    pub(crate) fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub(crate) async fn serve<H, HF, S>(
        self,
        handler: H,
        shutdown: S
    ) -> Result<()>
    where
        H: Fn(AppRequest) -> HF + Clone + Send + Sync + 'static,
        HF: Future<Output = AppResponse> + Send + 'static,
        S: Future<Output = ()>
    {
        tokio::pin!(shutdown);
        let mut connections = JoinSet::new();

        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => break,
                accepted = self.listener.accept() => {
                    let (stream, peer) = accepted?;
                    let handler = handler.clone();
                    let server = self.local_addr;
                    connections.spawn(async move {
                        serve_connection(stream, peer, server, handler).await
                    });
                }
                completed = connections.join_next(), if !connections.is_empty() => {
                    if let Some(result) = completed {
                        result??;
                    }
                }
            }
        }

        connections.abort_all();
        while let Some(result) = connections.join_next().await {
            if !result.as_ref().is_err_and(tokio::task::JoinError::is_cancelled) {
                result??;
            }
        }
        Ok(())
    }
}

async fn serve_connection<H, HF>(
    stream: TcpStream,
    peer: SocketAddr,
    server: SocketAddr,
    handler: H
) -> Result<()>
where
    H: Fn(AppRequest) -> HF + Clone + Send + Sync + 'static,
    HF: Future<Output = AppResponse> + Send + 'static
{
    let service = service_fn(move |request| handle_request(request, peer, server, handler.clone()));
    http1::Builder::new().serve_connection(TokioIo::new(stream), service).await?;
    Ok(())
}

async fn handle_request<H, HF>(
    request: Request<Incoming>,
    peer: SocketAddr,
    server: SocketAddr,
    handler: H
) -> std::result::Result<AppResponse, Infallible>
where
    H: Fn(AppRequest) -> HF,
    HF: Future<Output = AppResponse>
{
    let (parts, body) = request.into_parts();
    let body = match body.collect().await {
        Ok(body) => body.to_bytes(),
        Err(error) => return Ok(error_response(StatusCode::BAD_REQUEST, error.to_string()))
    };

    // The addresses belong to the connection, not to the request, so they ride
    // in extensions. A stage that needs them — the ASGI scope does — reads them
    // there; one that does not never sees them.
    let mut request = Request::from_parts(parts, Full::new(body));
    request.extensions_mut().insert(PeerAddrs { client: peer, server });

    Ok(handler(request).await)
}

fn error_response(
    status: StatusCode,
    message: String
) -> AppResponse {
    let mut response = Response::new(Full::new(Bytes::from(message)));
    *response.status_mut() = status;
    response
}

/// The development listener, as a runnable Claviron owns.
///
/// It is spawned after every provider has booted, so the interpreter behind it
/// is already up when the first connection arrives. It stops when its
/// generation is cancelled: the accept loop ends, connections in flight finish,
/// and only then does the future return.
pub(crate) struct DevIngress {
    address: SocketAddr
}

impl DevIngress {
    pub(crate) fn new(address: SocketAddr) -> Self {
        Self { address }
    }
}

impl Provider<AppState> for DevIngress {
    fn name(&self) -> &'static str {
        "DevIngress"
    }

    fn as_runnable(self: Arc<Self>) -> Option<Arc<dyn Runnable<AppState>>> {
        Some(self)
    }
}

#[async_trait]
impl Runnable<AppState> for DevIngress {
    /// A master with workers does not serve traffic; its workers do, each on
    /// its own interpreter. Standalone is not a master and still runs.
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
        // Registration guarantees this: `configure` either registered a
        // service or the runner registered the interpreter as one.
        let application = state
            .registry_ref()
            .resolve::<Application>()
            .ok_or_else(|| registry::Error::msg("no application service is registered"))?;

        let server = DevServer::bind(self.address)
            .await
            .map_err(|error| registry::Error::msg(error.to_string()))?;
        // Info on purpose, unlike the rest of this crate: a development
        // ingress exists to be looked at, and its address is the thing being
        // looked for.
        tracing::info!("🐍 gufi development ingress listening on http://{}", server.local_addr());

        server
            .serve(
                move |request| {
                    let application = Arc::clone(&application);
                    async move { application.call(request).await }
                },
                context.cancelled()
            )
            .await
            .map_err(|error| registry::Error::msg(error.to_string()))
    }
}
