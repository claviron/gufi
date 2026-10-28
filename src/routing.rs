//! Runner routing over the shared ASGI HTTP adapter.

#[cfg(feature = "axum")]
use claviron::runtime::{AppState, registry};

pub use claviron::runner::{AppRequest, RequestBody};
pub(crate) use gufi_asgi::routing::request_scope as scope;
pub use gufi_asgi::routing::{
    AppResponse, BoxError, PythonService, ResponseBody, full_body,
};

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
