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
//! # Carrying something
//!
//! `payload` asks for a reply carrying a message of that many **characters**:
//!
//! ```text
//! /_gufi/rust?payload=640      # {"from":"rust","message":"<640 chars>"}
//! /_gufi/rust?payload=65536    # the same with 64 KiB of message
//! ```
//!
//! Still JSON, because that is what an application returns. The body is the
//! message plus its wrapper, so a few bytes more than asked for.
//!
//! A transport that moves bytes should be measured moving bytes. A reply of
//! fifty-three says almost nothing about one: at that size an exchange is
//! all setup, and the thing a shared-memory transport is supposed to save —
//! the copy through a kernel buffer, the syscall per read and per write —
//! has no room to appear. It is also where a shared ring can be shown to
//! *lose*, if its window forces the writer to wait where a socket would not.
//!
//! The bytes are generated once for the process and sliced, never built per
//! request: a benchmark must not spend the CPU it is trying to measure. They
//! are printable and unpatterned, so nothing along the way compresses them
//! or optimises them away.
//!
//! # The other direction
//!
//! The route answers `POST` as well, reads whatever body arrives, and
//! reports its size in an `x-received` header. That is the only way to
//! exercise the request direction at all: every measurement until now has
//! been a `GET`, so the half of the transport that carries a request body
//! has never run under load.
//!
//! The header rather than the JSON so the reply stays the cached one — a
//! body formatted per request would put the benchmark's own work into the
//! number it reports.
//!
//! ```text
//! curl -X POST --data-binary @4k.bin /_gufi/rust?payload=1024 -D-
//! x-received: 4096
//! ```
//!
//! Off by default, and it says so at boot when it is on: routes an
//! application did not write should never be a surprise in a log.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use claviron::axum::Router;
use claviron::axum::body::Bytes;
use claviron::axum::extract::Query;
use claviron::axum::response::IntoResponse;
use claviron::axum::routing::get;
use claviron::runtime::activity::ACTIVITY;
use http::header::{CACHE_CONTROL, CONTENT_TYPE, HeaderName, HeaderValue};

/// What the caller wants this route to do: how long to take, and how much
/// to send back.
#[derive(serde::Deserialize, Default)]
pub struct Ask {
    min: Option<u64>,
    max: Option<u64>,
    payload: Option<usize>
}

impl Ask {
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

/// The longest message this route will send.
///
/// One mebibyte. A bound stated here is better than a request deciding how
/// much memory the process holds.
const PAYLOAD_MAX: usize = 1024 * 1024;

/// The whole reply for a message of `bytes` characters, built once per size.
///
/// Built once and kept, rather than formatted per request: a benchmark must
/// not spend the CPU it is trying to measure, and a `format!` of a megabyte
/// on every request would be most of what the run reported. A process sees
/// one or two sizes in its life, so the handful of strings this keeps are
/// kept for good on purpose.
fn body(bytes: usize) -> &'static str {
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};

    static ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    static SOURCE: OnceLock<Vec<u8>> = OnceLock::new();
    static BODIES: OnceLock<Mutex<HashMap<usize, &'static str>>> = OnceLock::new();

    let bytes = bytes.min(PAYLOAD_MAX);
    let mut bodies =
        BODIES.get_or_init(Default::default).lock().expect("the body cache is never poisoned");
    bodies.entry(bytes).or_insert_with(|| {
        // Printable and unpatterned, so nothing compresses it or optimises
        // it away. None of these characters needs escaping in JSON.
        let source = SOURCE.get_or_init(|| {
            (0..PAYLOAD_MAX).map(|_| ALPHABET[spread(ALPHABET.len() as u64) as usize]).collect()
        });
        let message = std::str::from_utf8(&source[..bytes]).expect("the alphabet is ASCII");
        Box::leak(format!(r#"{{"from":"rust","message":"{message}"}}"#).into_boxed_str())
    })
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
    Router::new().route("/_gufi/rust", get(rust).post(rust)).route("/_gufi/stats", get(stats))
}

async fn rust(
    Query(ask): Query<Ask>,
    received: Bytes
) -> claviron::axum::response::Response {
    // Read before waiting: the request body has to cross before there is
    // anything to answer, and a benchmark that slept first would time the
    // sleep instead of the crossing.
    let received = received.len();

    if let Some(waited) = ask.duration() {
        tokio::time::sleep(waited).await;
    }
    let json = match ask.payload {
        Some(bytes) => body(bytes),
        None => r#"{"from":"rust"}"#
    };
    // `HeaderValue::from` writes the number into its own inline buffer, so
    // reporting the size costs no allocation. Formatting it would have cost
    // one per request, which is the benchmark paying for its own instrument.
    (
        [
            (CONTENT_TYPE, HeaderValue::from_static("application/json")),
            (CACHE_CONTROL, HeaderValue::from_static("no-store")),
            (HeaderName::from_static("x-received"), HeaderValue::from(received as u64))
        ],
        json
    )
        .into_response()
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
