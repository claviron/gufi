//! Runs a Python application on Claviron: application state, registration,
//! the local invocation bridge, ASGI lifespan, ingress and shutdown, wired
//! once so an application's own binary is a `main.rs` and a callback.
//!
//! This crate deliberately has no TLS, vhost, configuration, supervision, or
//! production listener policy. It accepts an already configured application
//! and does not discover its environment.

#[cfg(feature = "bench")]
pub mod bench;

mod application;
pub mod asgi;
#[cfg(feature = "broadcast")]
mod broadcast;
mod counter;
#[cfg(feature = "django")]
mod django;
mod http;
pub(crate) mod options;
mod orbit;
mod python;
pub mod routing;
#[cfg(feature = "tls")]
pub mod tls;
pub mod trust;
#[cfg(feature = "websocket")]
pub mod websocket;

/// The axum `register` builds against — Claviron's, re-exported so an
/// application reaches it through the crate it actually depends on and cannot
/// end up with a second one.
#[cfg(feature = "axum")]
pub use claviron::axum;
pub use http::{Bind, Protocols};
#[cfg(feature = "broadcast")]
pub use options::Broadcasting;
#[cfg(feature = "django")]
pub use options::Channels;
pub use options::{CONFIG_FILE, GufiOptions};
pub use python::{Error, Result, Runner};
#[cfg(feature = "axum")]
pub use routing::register_router;
pub use routing::{
    AppRequest, AppResponse, PeerAddrs, PythonService, RequestBody, RequestScheme,
    register_router_with
};
#[cfg(feature = "tls")]
pub use tls::Tls;
pub use trust::Trust;
