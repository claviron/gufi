//! Compatibility access to Claviron's shared native benchmark capability.
//!
//! Runner registration and all workloads are owned by claviron-bench.
pub use claviron::bench::{Ask, runner::BenchService};

/// Preserve the original Gufi paths alongside the common benchmark paths.
pub fn routes() -> crate::axum::Router {
    claviron::bench::routes().nest("/_gufi", claviron::bench::baseline_routes())
}
