//! Runs a Python application on Claviron: application state, registration,
//! the local invocation bridge, ASGI lifespan, ingress and shutdown, wired
//! once so an application's own binary is a `main.rs` and a callback.
//!
//! This crate deliberately has no TLS, vhost, configuration, supervision, or
//! production listener policy. It accepts an already configured application
//! and does not discover its environment.

mod application;
pub mod asgi;
mod orbit;
mod http;
mod options;
mod python;
pub mod routing;

/// The axum `register` builds against — Claviron's, re-exported so an
/// application reaches it through the crate it actually depends on and cannot
/// end up with a second one.
#[cfg(feature = "axum")]
pub use claviron::axum;
pub use options::GufiOptions;
pub use http::{Bind, Protocols};
pub use python::{Error, Result, Runner};
#[cfg(feature = "axum")]
pub use routing::register;
pub use routing::{
    AppRequest, AppResponse, PeerAddrs, PythonService, RequestScheme, register_with
};
