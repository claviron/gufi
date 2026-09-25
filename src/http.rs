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
use crate::trust::Trust;

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
    fn describe(
        &self,
        secure: bool
    ) -> String {
        match self {
            Self::Tcp(address) if secure => format!("https://{address}"),
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
    protocols: Protocols,
    trust: Arc<Trust>,
    #[cfg(feature = "tls")]
    acceptor: Option<tokio_rustls::TlsAcceptor>
}

impl Server {
    pub(crate) async fn bind(
        bind: Bind,
        protocols: Protocols,
        trust: Arc<Trust>
    ) -> Result<Self> {
        let listener = Listener::bind(&bind).await?;
        let bind = match (&listener, bind) {
            // Port zero becomes a real port; report the one that was taken.
            (Listener::Tcp(listener), _) => Bind::Tcp(listener.local_addr()?),
            (_, other) => other
        };
        Ok(Self {
            listener,
            bind,
            protocols,
            trust,
            #[cfg(feature = "tls")]
            acceptor: None
        })
    }

    pub(crate) fn bound(&self) -> &Bind {
        &self.bind
    }

    /// Wrap accepted TCP connections in TLS. A Unix socket ignores it: its
    /// boundary is the filesystem's already.
    #[cfg(feature = "tls")]
    pub(crate) fn with_tls(
        mut self,
        acceptor: Option<tokio_rustls::TlsAcceptor>
    ) -> Self {
        if matches!(self.bind, Bind::Tcp(_)) {
            self.acceptor = acceptor;
        }
        self
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
                    let trust = Arc::clone(&self.trust);
                    #[cfg(feature = "tls")]
                    let acceptor = self.acceptor.clone();
                    connections.spawn(async move {
                        match stream {
                            #[cfg(feature = "tls")]
                            Accepted::Tcp(stream) if acceptor.is_some() => {
                                let stream = acceptor
                                    .expect("acceptor")
                                    .accept(stream)
                                    .await
                                    .map_err(Error::Io)?;
                                serve_secure_connection(
                                    stream, peer, local, protocols, trust, handler
                                )
                                .await
                            }
                            Accepted::Tcp(stream) => {
                                serve_connection(stream, peer, local, protocols, trust, handler)
                                    .await
                            }
                            Accepted::Uds(stream) => {
                                serve_connection(stream, None, None, protocols, trust, handler)
                                    .await
                            }
                        }
                    });
                }
                completed = connections.join_next(), if !connections.is_empty() => {
                    // A connection's failure is a connection's failure. A
                    // refused handshake, a client that vanished mid-request, a
                    // protocol error — none of them is a reason for the door
                    // to close on everyone else.
                    if let Some(Ok(Err(error))) = completed {
                        tracing::debug!("connection ended: {error}");
                    }
                }
            }
        }

        connections.abort_all();
        while let Some(result) = connections.join_next().await {
            if let Ok(Err(error)) = result {
                tracing::debug!("connection ended during shutdown: {error}");
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

/// The same connection, with the scheme the handshake established.
#[cfg(feature = "tls")]
async fn serve_secure_connection<I, H, HF>(
    stream: I,
    peer: Option<SocketAddr>,
    server: Option<SocketAddr>,
    protocols: Protocols,
    trust: Arc<Trust>,
    handler: H
) -> Result<()>
where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    H: Fn(AppRequest) -> HF + Clone + Send + Sync + 'static,
    HF: Future<Output = AppResponse> + Send + 'static
{
    serve_with_scheme(stream, peer, server, protocols, trust, "https", handler).await
}

async fn serve_connection<I, H, HF>(
    stream: I,
    peer: Option<SocketAddr>,
    server: Option<SocketAddr>,
    protocols: Protocols,
    trust: Arc<Trust>,
    handler: H
) -> Result<()>
where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    H: Fn(AppRequest) -> HF + Clone + Send + Sync + 'static,
    HF: Future<Output = AppResponse> + Send + 'static
{
    serve_with_scheme(stream, peer, server, protocols, trust, "http", handler).await
}

async fn serve_with_scheme<I, H, HF>(
    stream: I,
    peer: Option<SocketAddr>,
    server: Option<SocketAddr>,
    protocols: Protocols,
    trust: Arc<Trust>,
    scheme: &'static str,
    handler: H
) -> Result<()>
where
    I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    H: Fn(AppRequest) -> HF + Clone + Send + Sync + 'static,
    HF: Future<Output = AppResponse> + Send + 'static
{
    let service = service_fn(move |request| {
        handle_request(request, peer, server, Arc::clone(&trust), scheme, handler.clone())
    });
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
        _ => hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
            .serve_connection(io, service)
            .await
            .map_err(|error| Error::Io(std::io::Error::other(error.to_string())))?
    }
    Ok(())
}

async fn handle_request<H, HF>(
    request: Request<Incoming>,
    peer: Option<SocketAddr>,
    server: Option<SocketAddr>,
    trust: Arc<Trust>,
    scheme: &'static str,
    handler: H
) -> std::result::Result<AppResponse, Infallible>
where
    H: Fn(AppRequest) -> HF,
    HF: Future<Output = AppResponse>
{
    // Framing two parsers could read differently, refused before anything
    // reads it as a request. Claviron's own check, not a second opinion.
    if let Err(error) = claviron::http::forwarding::reject_request_smuggling(request.headers()) {
        return Ok(error_response(StatusCode::BAD_REQUEST, error.to_string()));
    }

    // Who the client is, and under which scheme — the socket's answer unless
    // the peer is one whose word this launch takes.
    let resolved = trust.resolve(peer, request.headers());

    let (mut parts, body) = request.into_parts();
    // A trusted proxy reported the host the client asked for; an application
    // reads `Host`, and behind a proxy the socket's is the proxy's own. This
    // is what trusting the proxy means, and it is why `ALLOWED_HOSTS` works
    // behind one.
    if let Some(host) = resolved.host.as_deref()
        && let Ok(value) = http::HeaderValue::from_str(host)
    {
        parts.headers.insert(http::header::HOST, value);
    }
    let body = match body.collect().await {
        Ok(body) => body.to_bytes(),
        Err(error) => return Ok(error_response(StatusCode::BAD_REQUEST, error.to_string()))
    };

    // The addresses belong to the connection, not to the request, so they ride
    // in extensions. A stage that needs them — the ASGI scope does — reads them
    // there; one that does not never sees them. A Unix socket has none, and
    // the scope then reports none.
    let mut request = Request::from_parts(parts, Full::new(body));
    if let (Some(client), Some(server)) = (resolved.client, server) {
        request.extensions_mut().insert(PeerAddrs { client, server });
    }
    request
        .extensions_mut()
        .insert(RequestScheme(resolved.scheme.unwrap_or_else(|| scheme.to_owned())));

    Ok(handler(request).await)
}

fn error_response(
    status: StatusCode,
    message: String
) -> AppResponse {
    let mut response = Response::new(crate::routing::full_body(message));
    *response.status_mut() = status;
    response
}

/// One listener, as a runnable Claviron owns.
///
/// It is spawned after every provider has booted, so the interpreter behind it
/// is already up when the first connection arrives. It stops when its
/// generation is cancelled: the accept loop ends, connections in flight finish,
/// and only then does the future return.
/// Every socket this process answers on, as one runnable.
///
/// One provider rather than one per socket, because the registry resolves by
/// type: two of these would be the same type twice. It is also the honest
/// shape — the listeners share a lifetime, a cancellation and an application,
/// and differ only in where they accept.
pub(crate) struct HttpIngress {
    binds: Vec<Bind>,
    protocols: Protocols,
    #[cfg(feature = "tls")]
    tls: Option<crate::Tls>
}

impl HttpIngress {
    pub(crate) fn new(
        binds: Vec<Bind>,
        protocols: Protocols,
        #[cfg(feature = "tls")] tls: Option<crate::Tls>
    ) -> Self {
        Self {
            binds,
            protocols,
            #[cfg(feature = "tls")]
            tls
        }
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

        // Whatever is registered, which is the launch's list unless the
        // application put something better there first.
        let trust = state.registry_ref().resolve::<Trust>().unwrap_or_default();

        // Read the certificate before any door opens: a bad path or a key
        // that does not match its certificate is a boot failure, not a
        // handshake that fails once traffic arrives.
        #[cfg(feature = "tls")]
        let acceptor = match &self.tls {
            None => None,
            Some(tls) => Some(tls.acceptor().map_err(registry::Error::msg)?)
        };

        let mut listeners = JoinSet::new();
        for bind in &self.binds {
            let server = Server::bind(bind.clone(), self.protocols, Arc::clone(&trust))
                .await
                .map_err(|error| registry::Error::msg(error.to_string()))?;
            // Info on purpose, unlike the rest of this crate: an address is
            // the thing an operator looks for.
            #[cfg(feature = "tls")]
            let secure = acceptor.is_some() && matches!(bind, Bind::Tcp(_));
            #[cfg(not(feature = "tls"))]
            let secure = false;
            tracing::info!(
                trusted = if trust.is_empty() { "nobody" } else { "registered peers" },
                "🐍 gufi listening on {}",
                server.bound().describe(secure)
            );

            let application = Arc::clone(&application);
            let cancelled = context.clone();
            #[cfg(feature = "tls")]
            let acceptor = acceptor.clone();
            listeners.spawn(async move {
                #[cfg(feature = "tls")]
                let server = server.with_tls(acceptor);
                server
                    .serve(
                        move |request| {
                            let application = Arc::clone(&application);
                            async move { application.call(request).await }
                        },
                        cancelled.cancelled()
                    )
                    .await
            });
        }

        // Every listener is bound before any of them is waited on, so a port
        // already taken is a boot failure rather than a process that half
        // serves.
        while let Some(finished) = listeners.join_next().await {
            match finished {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Err(registry::Error::msg(error.to_string())),
                Err(error) => return Err(registry::Error::msg(error.to_string()))
            }
        }
        Ok(())
    }
}
