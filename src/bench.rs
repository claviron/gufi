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
//! # Holding the exchange open
//!
//! `/_gufi/rust` takes `min` and `max`, in **milliseconds**, and waits before
//! answering:
//!
//! ```text
//! /_gufi/rust                 # answer at once
//! /_gufi/rust?min=5           # wait five milliseconds
//! /_gufi/rust?min=5&max=20    # wait somewhere between five and twenty
//! /_gufi/rust?max=20          # wait somewhere between none and twenty
//! ```
//!
//! A benchmark where every request returns instantly measures how fast a
//! path can be walked. Real requests wait — on a database, on a disk, on
//! another service — and while they wait they *hold* something: a connection
//! in a pool, a slot in a table, a lane entry. Two transports that look alike
//! at full speed can differ completely in what they hold, and a delay is how
//! that difference is made to show.
//!
//! A range rather than one value, because identical waits arrive in lockstep
//! and measure a metronome rather than a load. The spread breaks them up.
//!
//! The duration is the caller's, not this process's: it is read from the
//! request and never invented here.
//!
//! Off by default, and it says so at boot when it is on: routes an
//! application did not write should never be a surprise in a log.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use claviron::axum::Router;
use claviron::axum::extract::Query;
use claviron::axum::response::IntoResponse;
use claviron::axum::routing::get;
use claviron::runtime::activity::ACTIVITY;
use http::header::{CACHE_CONTROL, CONTENT_TYPE};

/// How long the caller wants this route to take, in milliseconds.
#[derive(serde::Deserialize, Default)]
pub struct Wait {
    min: Option<u64>,
    max: Option<u64>
}

impl Wait {
    /// What to wait, or nothing when neither bound was given.
    fn duration(&self) -> Option<Duration> {
        let (low, high) = match (self.min, self.max) {
            (None, None) => return None,
            (Some(only), None) => (only, only),
            (None, Some(high)) => (0, high),
            (Some(low), Some(high)) if low > high => (high, low),
            (Some(low), Some(high)) => (low, high)
        };
        Some(Duration::from_millis(if low == high { low } else { low + spread(high - low + 1) }))
    }
}

/// A number below `bound`, cheaply and without a dependency.
///
/// xorshift over a counter seeded once per process. It is not random enough
/// to hide anything and does not need to be: its whole job is to stop a
/// thousand identical waits from ending at the same instant.
fn spread(bound: u64) -> u64 {
    static STATE: AtomicU64 = AtomicU64::new(0x2545_F491_4F6C_DD1D);
    let mut x = STATE.fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed);
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 27;
    x % bound
}

/// The routes, for an application that composes its own router.
pub fn routes() -> Router {
    Router::new().route("/_gufi/rust", get(rust)).route("/_gufi/stats", get(stats))
}

async fn rust(Query(wait): Query<Wait>) -> impl IntoResponse {
    if let Some(waited) = wait.duration() {
        tokio::time::sleep(waited).await;
    }
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
