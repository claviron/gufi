# gufi

Python runner for Claviron. You write the binary; your ASGI application runs
inside it.

## Write your own runner

`uvicorn main:app` is a program you hand your application to. It owns the
process, you are its guest, and every question about the edges was answered
before you arrived. Gufi is the other way round: `main.rs` is yours, the binary
is yours, and the runner is a part you assemble.

That matters because the process is where Rust and Python meet. Once it is
yours, moving a piece of work across that line is a local decision instead of a
rewrite:

- **A function in Rust, called from Python.** `configure` registers a typed
  handler and the application awaits it through `claviron.invoke`. It is a
  native extension in everything but the build — same process, same binary, no
  separate `.so`, no FFI to maintain.
- **A route in Rust, never reaching Python.** Declare it and it answers;
  everything you did not declare still reaches the interpreter, unchanged.
- **The application in Rust.** The end of the same road, reached one handler at
  a time rather than in one commit — and if it is never reached, nothing is
  owed.

Nothing here asks you to choose up front. You start with the Python application
you already have.

## Getting started

```toml
[dependencies]
claviron = { version = "0.2", default-features = false, features = ["server"] }
gufi     = "0.1"
tokio    = { version = "1", features = ["macros", "rt-multi-thread", "signal"] }
axum     = "0.8"   # or warp, or nothing — see "Your router stays yours"
```

Neither Claviron crate is released yet; today both are path dependencies.

```rust
// src/main.rs
mod app;

use gufi::Runner;

#[tokio::main]
async fn main() -> gufi::Result<()> {
    Runner::new("main:app")
        .application_name("checkout")
        .fleet_name("checkout")
        .run(app::configure)
        .await
}
```

```rust
// src/app.rs
pub fn configure(state: &AppState) -> registry::Result<()> {
    gufi::register(state, routes::router())
}
```

```
your-app/
├── Cargo.toml
├── main.py          # app = FastAPI()
├── pyproject.toml
├── .venv/
└── src/
    ├── main.rs
    ├── app.rs
    └── app/routes.rs
```

`cargo run`, and the application is served on `127.0.0.1:8000`. `Ctrl-C` stops
admission, drains what is in flight, runs ASGI lifespan shutdown, and returns.

## Your router stays yours

Gufi has no router. Bring axum, warp, tower-http or a bare `service_fn`:

```rust
use gufi::axum::{Router, routing::get};

pub fn router() -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/orders/{id}", get(order))
}
```

Nothing there mentions a fallback, and nothing should: **the Python
application is the fallback.** A path your routes do not declare, and a
declared path reached with an undeclared method, both reach the interpreter.
Your application stays the authority on what exists; the Rust routes only
intercept. `register` wires that for you.

If you want to place the interpreter yourself — your own 404 for a subtree, or
a framework whose fallthrough lives somewhere else — use `register_with`, which
hands you the service and takes only `tower-service`:

```rust
gufi::register_with(state, |python| warp_filter().or(python_arm(python)))
```

`gufi::axum` re-exports Claviron's axum, so your application and this crate
cannot end up with two of them.

## Calling Rust from Python

`configure` registers typed Rust handlers, and the application calls them
through a module the runtime installs into the interpreter:

```python
import claviron

response = await claviron.invoke("claviron.ping.v1", {"echo": "app"})
```

The call stays in the process — it reaches a handler in this same binary
rather than leaving over a socket.

Because the module is installed at runtime it is not in your virtual
environment, so an editor has nothing to resolve the import against. The
[`claviron`](https://pypi.org/project/claviron/) package on PyPI carries its
signatures and nothing else:

```sh
uv add --dev claviron
```

## Registering your own services

Your services go in the same registry Claviron boots its own from, and get the
same lifecycle — a `Provider` is booted in dependency order and resolved by
type; one that also declares `Runnable` gets a runtime-owned generation,
started after every boot has completed and ended by cancellation rather than by
being dropped.

A Rust route reaches them with Claviron's extractor:

```rust
use claviron::http::ext::Provide;

async fn hits(Provide(counter): Provide<CounterService>) -> impl IntoResponse {
    Json(json!({ "hits": counter.hit() }))
}
```

## Development and production are different programs

The HTTP listener here is for writing an application, not for serving one. It
collects one request and one response and owns no TLS, vhost routing, static
files or supervision — and its numbers are not performance evidence.

In production a Claviron edge terminates TLS, routes vhosts and serves static
files, and reaches this process by name over Orbit shared memory instead of a
socket. That is the shape every Python deployment already has — nginx in front
of gunicorn, Apache's `WSGIDaemonProcess`. What differs is the cost of the hop,
not the topology. See `ARCHITECTURE.md`, including why the interpreter is not
inside the edge instead.

## The name

*Kufi* — κουφή, "the deaf one" — is what Cyprus calls its blunt-nosed viper. It
is not deaf. It has no external ear opening and does not startle at sound, so
people concluded it could not hear; what it actually does is read the ground,
picking up vibration through the bones of its jaw.

A serpent, for the obvious reason. A quiet one, for a better one.

---

`harness/` is the ASGI application the crate is exercised against while it is
worked on. `ARCHITECTURE.md` has the boundaries: what the runner owns, what it
deliberately does not, and why it is its own project rather than a crate inside
Claviron.
