//! Embeddable CPython and ASGI execution without a runner or listener.
//!
//! The host owns registration, routing and process lifecycle. A prepared
//! [`asgi::PythonApplication`] boots its application loop and awaits ASGI
//! lifespan startup. Its running owner drains accepted exchanges and awaits
//! lifespan shutdown before joining the loop thread.
//!
//! Python initialization is process-wide. Applications share that interpreter
//! and its selected environment; this crate does not provide isolated venvs
//! or subinterpreters in one process. The native Python module remains `gufi`.

pub mod asgi;
#[cfg(feature = "broadcast")]
mod broadcast;
#[cfg(feature = "django")]
pub mod config;
mod counter;
#[cfg(feature = "django")]
mod django;
#[cfg(feature = "log")]
mod log;
pub mod routing;
