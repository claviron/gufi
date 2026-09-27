//! Static files in front of the Python application, served by Claviron's
//! file service.
//!
//! Behind an edge the edge serves them and this is not needed. Without one —
//! gufi's own listener in development, or a deployment that runs no edge — it
//! is what `runserver`'s static handling or WhiteNoise would otherwise do, in
//! Rust and before the interpreter is reached. The mounts and their rules are
//! the runner shell's (`claviron::runner::files`); this places them in front
//! of the application's service.
//!
//! ```yaml
//! files:
//!   root: public
//!   statics:
//!     - { path: /static, dir: static }
//! ```

use std::sync::Arc;

use claviron::axum::Router;
use claviron::axum::extract::Request;
pub use claviron::runner::files::{Files, FilesSource};

use crate::routing::{ErasedService, RequestBody};

/// The application's service with the mounts in front of it.
pub(crate) fn wrap(
    files: Arc<Files>,
    service: Arc<dyn ErasedService>
) -> Router {
    claviron::runner::files::in_front(
        files,
        Router::new().fallback(move |request: Request| {
            let service = Arc::clone(&service);
            async move { service.call_erased(request.map(RequestBody::new)).await }
        })
    )
}
