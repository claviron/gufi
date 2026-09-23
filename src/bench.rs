//! A baseline to measure your own routes against.
//!
//! Turn on the `bench` feature and two routes appear beside yours. Neither
//! touches Python, so the gap between them and your own handler is what the
//! interpreter costs on your machine, with your application, under your load.
//!
//! ```text
//! $ wrk -t4 -c64 -d10s http://127.0.0.1:8000/_gufi/rust      # the floor
//! $ wrk -t4 -c64 -d10s http://127.0.0.1:8000/your/route      # yours
//! ```
//!
//! The ratio is the argument for moving one hot route into `app.rs` — and
//! the routes you never move stay exactly where they are. That is the whole
//! of this runner's proposition, as a number rather than a claim.
//!
//! | route | what it does |
//! |---|---|
//! | `/_gufi/rust` | answers in Rust, allocating one small body |
//! | `/_gufi/stats` | what this process has in flight, finished, and open |
//!
//! Off by default, and it says so at boot when it is on: routes an
//! application did not write should never be a surprise in a log.

use claviron::axum::Router;
use claviron::axum::response::IntoResponse;
use claviron::axum::routing::get;
use claviron::runtime::activity::ACTIVITY;
use http::header::{CACHE_CONTROL, CONTENT_TYPE};

/// The routes, for an application that composes its own router.
pub fn routes() -> Router {
    Router::new().route("/_gufi/rust", get(rust)).route("/_gufi/stats", get(stats))
}

async fn rust() -> impl IntoResponse {
    ([(CONTENT_TYPE, "application/json"), (CACHE_CONTROL, "no-store")], r#"{"from":"rust"}"#)
}

async fn stats() -> impl IntoResponse {
    let body = format!(
        r#"{{"in_flight":{},"total":{},"sessions":{}}}"#,
        ACTIVITY.in_flight(),
        ACTIVITY.total(),
        ACTIVITY.sessions()
    );
    ([(CONTENT_TYPE, "application/json"), (CACHE_CONTROL, "no-store")], body)
}
