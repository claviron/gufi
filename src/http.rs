//! The listener.
//!
//! It binds a socket, reads a request and hands it to the service the
//! application registered. It owns no TLS policy, no vhost routing, no static
//! files and no supervision — **it has no policy, because it stands behind a
//! server that has one.** In development there is nothing in front of it and
//! that is the same server, bound to localhost.
//!
//! There is no vhost here and there will not be one. This is a runner's
//! server: one application, reached however the launch asked for it.
//!
//! It knows nothing about routing and nothing about ASGI. It turns a socket
//! into an [`AppRequest`] and writes back an [`AppResponse`]; everything
//! between those two is the application's, which is why the same service runs
//! unchanged under an ingress that has no socket at all.
//!
//! ## No HTTP/3
//!
//! QUIC earns its handshake over a lossy wide-area path. This hop is a
//! loopback or a Unix socket, where it would buy a UDP stack, a certificate
//! and a congestion controller in exchange for nothing.

use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use claviron::runtime::{AppState, Provider, RunContext, Runnable, ServiceStartPolicy, registry};
use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::{TcpListener, UnixListener};
use tokio::task::JoinSet;

use crate::routing::{AppRequest, AppResponse, Application, PeerAddrs, RequestScheme};

pub(crate) type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("listener failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("connection failed: {0}")]
    Hyper(#[from] hyper::Error),
    #[error("connection task failed: {0}")]
    Join(#[from] tokio::task::JoinError)
}

/// Where a listener listens.
///
/// Two transports, one server. A Unix socket has no addresses to report, so
/// requests arriving on one carry none — the scope says `None` rather than
/// inventing a peer.
#[derive(Clone, Debug)]
pub enum Bind {
    Tcp(SocketAddr),
    Uds(PathBuf)
}

impl Bind {
    fn describe(&self) -> String {
        match self {
            Self::Tcp(address) => format!("http://{address}"),
            Self::Uds(path) => format!("unix:{}", path.display())
        }
    }
}

/// Which HTTP versions a listener accepts.
///
/// Both by default. Neither is the same as both: a launch that names no
/// version wants whatever the client speaks.
#[derive(Clone, Copy, Debug)]
pub struct Protocols {
    pub h1: bool,
    pub h2: bool
}

impl Default for Protocols {
    fn default() -> Self {
        Self { h1: true, h2: true }
    }
}

impl Protocols {
    pub(crate) fn resolve(
        h1: bool,
        h2: bool
    ) -> Self {
        if h1 || h2 { Self { h1, h2 } } else { Self::default() }
    }
}

enum Listener {
    Tcp(TcpListener),
    Uds(UnixListener)
}

impl Listener {
    async fn bind(bind: &Bind) -> Result<Self> {
        Ok(match bind {
            Bind::Tcp(address) => Self::Tcp(TcpListener::bind(address).await?),
            Bind::Uds(path) => {
                // A socket file outlives the process that made it. The
                // instance lock is what says no second process of this name is
                // running, so a file still here belongs to one that is gone.
                if path.exists() {
                    std::fs::remove_file(path)?;
                }
                Self::Uds(UnixListener::bind(path)?)
            }
        })
    }
}

pub(crate) struct Server {
    listener: Listener,
    bind: Bind,
    protocols: Protocols
}

impl Server {
    pub(crate) async fn bind(
        bind: Bind,
        protocols: Protocols
    ) -> Result<Self> {
        let listener = Listener::bind(&bind).await?;
        let bind = match (&listener, bind) {
            // Port zero becomes a real port; report the one that was taken.
            (Listener::Tcp(listener), _) => Bind::Tcp(listener.local_addr()?),
            (_, other) => other
        };
        Ok(Self { listener, bind, protocols })
    }

    pub(crate) fn bound(&self) -> &Bind {
        &self.bind
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
        let local = match &self.bind {
            Bind::Tcp(address) => Some(*address),
            Bind::Uds(_) => None
        };

        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown => break,
                accepted = accept(&self.listener) => {
                    let (stream, peer) = accepted?;
                    let handler = handler.clone();
                    let protocols = self.protocols;
                    connections.spawn(async move {
                        match stream {
                            Accepted::Tcp(stream) => {
                                serve_connection(stream, peer, local, protocols, handler).await
                            }
                            Accepted::Uds(stream) => {
                                serve_connection(stream, None, None, protocols, handler).await
                            }
                        }
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
        if let Bind::Uds(path) = &self.bind {
            let _ = std::fs::remove_file(path);
        }
        Ok(())
    }
}

enum Accepted {
    Tcp(tokio::net::TcpStream),
    Uds(tokio::net::UnixStream)
}

async fn accept(listener: &Listener) -> Result<(Accepted, Option<SocketAddr>)> {
    Ok(match listener {
        Listener::Tcp(listener) => {
            let (stream, peer) = listener.accept().await?;
            (Accepted::Tcp(stream), Some(peer))
        }
        Listener::Uds(listener) => {
            let (stream, _) = listener.accept().await?;
            (Accepted::Uds(stream), None)
        }
    })
}

async fn serve_connection<I, H, HF>(
    stream: I,
    peer: Option<SocketAddr>,
    server: Option<SocketAddr>,
    protocols: Protocols,
    handler: H
) -> Result<()>
where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    H: Fn(AppRequest) -> HF + Clone + Send + Sync + 'static,
    HF: Future<Output = AppResponse> + Send + 'static
{
    let service = service_fn(move |request| handle_request(request, peer, server, handler.clone()));
    let io = TokioIo::new(stream);
    match (protocols.h1, protocols.h2) {
        (true, false) => {
            hyper::server::conn::http1::Builder::new().serve_connection(io, service).await?
        }
        (false, true) => {
            hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                .serve_connection(io, service)
                .await?
        }
        // Whatever the client speaks: HTTP/1 with an upgrade, or HTTP/2 with
        // prior knowledge.
        _ => {
            hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                .serve_connection(io, service)
                .await
                .map_err(|error| Error::Io(std::io::Error::other(error.to_string())))?
        }
    }
    Ok(())
}

async fn handle_request<H, HF>(
    request: Request<Incoming>,
    peer: Option<SocketAddr>,
    server: Option<SocketAddr>,
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
    // there; one that does not never sees them. A Unix socket has none, and
    // the scope then reports none.
    let mut request = Request::from_parts(parts, Full::new(body));
    if let (Some(client), Some(server)) = (peer, server) {
        request.extensions_mut().insert(PeerAddrs { client, server });
    }
    request.extensions_mut().insert(RequestScheme("http".to_owned()));

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

/// One listener, as a runnable Claviron owns.
///
/// It is spawned after every provider has booted, so the interpreter behind it
/// is already up when the first connection arrives. It stops when its
/// generation is cancelled: the accept loop ends, connections in flight finish,
/// and only then does the future return.
pub(crate) struct HttpIngress {
    bind: Bind,
    protocols: Protocols
}

impl HttpIngress {
    pub(crate) fn new(
        bind: Bind,
        protocols: Protocols
    ) -> Self {
        Self { bind, protocols }
    }
}

impl Provider<AppState> for HttpIngress {
    fn name(&self) -> &'static str {
        "HttpIngress"
    }

    fn as_runnable(self: Arc<Self>) -> Option<Arc<dyn Runnable<AppState>>> {
        Some(self)
    }
}

#[async_trait]
impl Runnable<AppState> for HttpIngress {
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

        let server = Server::bind(self.bind.clone(), self.protocols)
            .await
            .map_err(|error| registry::Error::msg(error.to_string()))?;
        // Info on purpose, unlike the rest of this crate: an address is the
        // thing an operator looks for.
        tracing::info!("🐍 gufi listening on {}", server.bound().describe());

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
