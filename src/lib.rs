//! Runs a Python application on Claviron: application state, registration,
//! the local invocation bridge, ASGI lifespan, ingress and shutdown, wired
//! once so an application's own binary is a `main.rs` and a callback.
//!
//! This crate deliberately has no TLS, vhost, configuration, supervision, or
//! production listener policy. It accepts an already configured application
//! and does not discover its environment.

mod application;
/// The Python engine, retaining the runner's existing module path.
pub use claviron_asgi::asgi;
mod framework;
mod management;
pub(crate) mod options;
mod orbit;
mod python;
pub mod routing;
#[cfg(feature = "watch")]
mod watch;
#[cfg(feature = "websocket")]
pub use claviron_asgi::websocket;

/// The Claviron facade used by this Gufi build, without a separate dependency.
pub use claviron;

/// The axum `register` builds against — Claviron's, re-exported so an
/// application reaches it through the crate it actually depends on and cannot
/// end up with a second one.
#[cfg(feature = "axum")]
pub use claviron::axum;
/// Which proxies' forwarding headers count; Claviron's, re-exported.
pub use claviron::http::trust;
pub use claviron::http::trust::Trust;
/// Application-owned routes built from the registered shared provider.
#[cfg(feature = "axum")]
pub use claviron::pipeline::routing::Routable;
#[cfg(feature = "tls")]
pub use claviron_runner::Tls;
#[cfg(feature = "files")]
pub use claviron_runner::files::FilesSource;
pub use claviron_runner::{EdgeShutdownInitiated, Owner, register_router_with};
#[cfg(feature = "broadcast")]
pub use options::Broadcasting;
#[cfg(feature = "django")]
pub use options::Channels;
pub use options::{CONFIG_FILE, GufiOptions};
pub use python::{Error, Result, Runner};
#[cfg(feature = "axum")]
pub use routing::register_router;
pub use routing::{AppRequest, AppResponse, PythonService, RequestBody};
