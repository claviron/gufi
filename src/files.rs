//! Static files in front of the Python application, served by Claviron's
//! file service.
//!
//! Behind an edge the edge serves them and this is not needed. Without one —
//! gufi's own listener in development, or a deployment that runs no edge — it
//! is what `runserver`'s static handling or WhiteNoise would otherwise do, in
//! Rust and before the interpreter is reached. Only GET and HEAD look at the
//! mounts; every other method goes to the application whatever its path.
//!
//! ```yaml
//! files:
//!   root: public
//!   statics:
//!     - { path: /static, dir: static }
//! ```

use std::path::{Path, PathBuf};
use std::sync::Arc;

use claviron::axum::Router;
use claviron::axum::body::Body;
use claviron::axum::extract::{Request, State};
use claviron::axum::middleware::{Next, from_fn_with_state};
use claviron::files::FileSite;
use claviron::http::config::statics::StaticConfig;
use http::{Method, StatusCode};
use serde::Deserialize;

use crate::routing::{ErasedService, RequestBody};

/// The `files:` section, as written.
///
/// Carried to workers as it was written rather than as parsed: Claviron's
/// mount rules read durations as text and keep seconds, so they do not write
/// back to what they read. Each process parses it; the master first, when the
/// launch is resolved, so a bad block stops the launch there.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct FilesSource(serde_yaml::Value);

impl FilesSource {
    pub(crate) fn parse(&self) -> Result<Files, String> {
        let files: Files =
            serde_yaml::from_value(self.0.clone()).map_err(|error| format!("files: {error}"))?;
        files.validate()?;
        Ok(files)
    }
}

/// Explicit mounts for one application. `root` is relative to the
/// application's root, and each mount's `dir` to `root`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Files {
    root: PathBuf,
    statics: Vec<StaticConfig>
}

impl Files {
    fn validate(&self) -> Result<(), String> {
        if self.root.as_os_str().is_empty() {
            return Err("files.root must not be empty".to_owned());
        }
        for mount in &self.statics {
            if !mount.path.starts_with('/') || mount.path.contains(['?', '#']) {
                return Err(format!("files.statics: '{}' is not an absolute URL path", mount.path));
            }
            if mount.dir.is_empty() {
                return Err(format!("files.statics: '{}' has no dir", mount.path));
            }
        }
        Ok(())
    }

    /// Anchored at the application's root.
    pub(crate) fn under(
        mut self,
        application_root: &Path
    ) -> Self {
        if self.root.is_relative() {
            self.root = application_root.join(&self.root);
        }
        self
    }
}

impl FileSite for Files {
    fn root(&self) -> PathBuf {
        self.root.clone()
    }

    fn indexes(&self) -> &[String] {
        &[]
    }

    fn try_files(&self) -> bool {
        false
    }

    fn spa(&self) -> Option<&str> {
        None
    }

    fn statics(&self) -> Option<&[StaticConfig]> {
        Some(&self.statics)
    }

    fn has_static_index(&self) -> bool {
        false
    }
}

/// The application's service with the mounts in front of it.
pub(crate) fn wrap(
    files: Arc<Files>,
    service: Arc<dyn ErasedService>
) -> Router {
    let site: Arc<dyn FileSite> = files;
    Router::new()
        .fallback(move |request: Request| {
            let service = Arc::clone(&service);
            async move { service.call_erased(request.map(RequestBody::new)).await }
        })
        .layer(from_fn_with_state(site, serve))
}

async fn serve(
    site: State<Arc<dyn FileSite>>,
    request: Request,
    next: Next
) -> Result<claviron::axum::response::Response, StatusCode> {
    // The file service takes no method: a POST to an asset's path is the
    // application's, not a file read.
    let head = request.method() == Method::HEAD;
    if !head && request.method() != Method::GET {
        return Ok(next.run(request).await);
    }
    let mut response = claviron::files::axum_static_mount_handler(site, request, next).await?;
    if head {
        *response.body_mut() = Body::empty();
    }
    Ok(response)
}
