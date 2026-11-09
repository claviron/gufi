//! The ingress itself — the lane, the reader, exchanges and their bodies — and
//! its message sessions are the runner shell's and the ASGI engine's. What is
//! left here is the name a lane answers to.

/// Default Link text/binary bound, matching the Lua message adapter's 1 MiB.
#[cfg(feature = "websocket")]
pub(crate) const DEFAULT_MESSAGE_LIMIT: usize =
    claviron_asgi::websocket::DEFAULT_MESSAGE_LIMIT;

/// What the lane says serves this application's name. A PHP application given
/// the same name is refused at its join rather than splitting this one's
/// requests.
pub(crate) const ASGI_RUNTIME: &str = "asgi";
