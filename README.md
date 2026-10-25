<p align="center">
  <img src="assets/gufi.svg" alt="gufi" width="144" height="144">
</p>

<h1 align="center">gufi</h1>

gufi runs Python ASGI applications inside a Rust binary you build and own.
Your Python framework handles the application; gufi connects it to Claviron's
runtime and manages ASGI lifespan, requests and shutdown.

## Write your own runner

Your `main.rs` assembles the host, registers application services and starts
your Python application. Rust handlers and Python code run in the same process.

That matters because the process is where Rust and Python meet. Once it is
yours, moving a piece of work across that line is a local decision instead of a
rewrite:

- **A function in Rust, called from Python.** `configure` registers a typed
  handler and the application awaits it through `gufi.invoke`. It is a
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
gufi     = "0.2.0-alpha.19"
tokio    = { version = "1", features = ["macros", "rt-multi-thread", "signal"] }
axum     = "0.8"   # or warp, or nothing — see "Your router stays yours"
```

APIs and configuration may change between alpha releases.

The examples use Claviron through Gufi's `gufi::claviron` re-export.

```rust
// src/main.rs
mod app;

use gufi::Runner;

#[tokio::main]
async fn main() -> gufi::Result<()> {
    Runner::new("main:app")
        .name("checkout")
        .with_interceptor(app::routes::router)
        .run(app::configure)
        .await
}
```

```rust
// src/app.rs
use gufi::claviron::{AppState, registry};

pub fn configure(state: &AppState) -> registry::Result<()> {
    // Register your application services and event listeners here.
    Ok(())
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

With the `cli` feature, `GufiOptions::from_args()` accepts `--config-path FILE`
(alias `--config`, shorthand `-c`) to select the launch YAML file.

## Your router stays yours

Gufi has no router. Bring axum, warp, tower-http or a bare `service_fn`:

```rust
use gufi::claviron::AppState;
use gufi::axum::{Router, routing::get};

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/orders/{id}", get(order))
        .with_state(state)
}
```

Nothing there mentions a fallback, and nothing should: **the Python
application is the fallback.** A path your routes do not declare, and a
declared path reached with an undeclared method, both reach the interpreter.
Your application stays the authority on what exists; the Rust routes only
intercept. `with_interceptor` wires that after `configure`; registered HTTP
`Routable` services stand between that router and Python. Without an explicit
interceptor, those routables still run. `register_router` remains available
for custom registration callbacks.

If you want to place the interpreter yourself — your own 404 for a subtree, or
a framework whose fallthrough lives somewhere else — use `register_router_with`, which
hands you the service and takes only `tower-service`:

```rust
gufi::register_router_with(state, |python| warp_filter().or(python_arm(python)))
```

`gufi::axum` re-exports Claviron's axum, so your application and this crate
cannot end up with two of them.

## Calling Rust from Python

`configure` registers typed Rust handlers, and the application calls them
through a module the runtime installs into the interpreter:

```python
import gufi

response = await gufi.invoke("claviron.ping.v1", {"echo": "app"})
```

The call stays in the process — it reaches a handler in this same binary
rather than leaving over a socket.

Gufi installs this native module at runtime; it is not a Python package in the
application's virtual environment. The local [stubs](stubs/gufi/__init__.pyi)
describe invocation, counters, broadcasting, logging and the optional
[Django Channels layer](stubs/gufi/channels.pyi). Add this repository's `stubs/`
directory to your editor's stub search path. They contain type declarations
only, not a Python implementation or a fallback runtime.

Capabilities are available when configured and enabled by the application's
Gufi build. Use `gufi.LogHandler` for logging and
`gufi.channels.OrbitChannelLayer` for Django Channels. No `claviron` Python
alias is provided; Rust `claviron::...` names and invocation operation
identifiers such as `claviron.ping.v1` are unchanged.

## Registering your own services

Your services go in the same registry Claviron boots its own from, and get the
same lifecycle — a `Provider` is booted in dependency order and resolved by
type; one that also declares `Runnable` gets a runtime-owned generation,
started after every boot has completed and ended by cancellation rather than by
being dropped.

A Rust route reaches them with Claviron's extractor:

```rust
use gufi::claviron::http::ext::Provide;

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

## Orbit Link management

The application introduction is Python / Gufi / ASGI. Declare framework tags
with `frameworks: [FastAPI]` in `gufi.yaml`, repeatable `--framework`, or
`Runner::frameworks(["FastAPI"])`. An explicit list is preserved, including `[]`
to suppress detection. Scalar `framework` and `Runner::framework` remain shorthands. Otherwise, at
launch preparation Gufi reads `pyproject.toml` under the application root:
`project.dependencies` and `tool.poetry.dependencies` can identify FastAPI,
Django, Starlette, Flask, Quart, Sanic, Litestar or Falcon. Multiple distinct
frameworks remain distinct tags; optional and development dependencies do not count.
Missing or unrecognized metadata leaves the framework unknown. Unreadable or
invalid TOML is logged without preventing launch. Detection does not import
Python, read `requirements.txt`, or change ASGI dispatch. This management-only
metadata is shared by local and linked observations.

`icon: fastapi`, `--icon fastapi`, or `Runner::icon("fastapi")` selects a symbolic
admin-client icon. The GUI maps known codes locally; absent/unknown codes retain
the Python default. This hint never changes dispatch or loads an external asset.

With `orbit: true`, the shared runner exposes the ASGI application to the
existing Link management API, including gRPC through an edge. The target is
runtime `asgi` and the configured application name; fleet and Link address
select the runner, not another Python environment.

Start/stop/restart drive ASGI lifespan. The runner and Link lane remain alive
when Python is stopped, and Python requests return 503. Accepted requests and
WebSockets drain under their existing ownership; no extra timeout is added.
Restart does not reload imported source or reinitialize CPython: source changes
still use rolling worker replacement. Link connection events and command
dispatch are shared infrastructure; Python execution stays Gufi-owned.

## WebSocket and WebTransport messages over Link

Gufi adapts H1/H2 WebSockets and HTTP/3 WebTransport reliable bidi streams to
ASGI's `websocket.connect`, `websocket.receive`, `websocket.send` and
`websocket.close` events. The edge owns the network handshake. Each WT bidi
stream gets its own ASGI invocation, so a stream waiting for more bytes does
not serialize the other streams or the application's writes.

The scope retains the original HTTP version and includes
`scope["extensions"]["gufi.transport"]` with `name` (`websocket` or
`webtransport`), `method`, `scheme` and `http_version`. This is Gufi's message
adaptation, not a standard ASGI WebTransport session API. WT clients exchange
Link Text/Binary/Close records on their bidi streams. WebSocket subprotocol
negotiation applies to WebSockets; datagrams and creating WT streams from
Python are outside this adapter.

The Link acknowledgment is internal `101`; the edge emits the appropriate
H1/H2/H3 response. An ASGI refusal remains HTTP 403. Application completion
ends the pending input read, while input FIN leaves accepted output free to
drain. Protocol IO failures wake both directions. No drain deadline is added.

On ingress retirement, Gufi delivers `websocket.disconnect` with code 1001 to
the ASGI owner. The owner can finish its output before returning; Gufi then
finishes the protocol close and joins the session. It does not cancel Python
tasks or change the configured indefinite-drain policy.

`link_message_limit: 1048576` in `gufi.yaml` declares the maximum text/binary
payload in bytes. The default is 1 MiB; zero is rejected. The CLI accepts
`--link-message-limit BYTES`, and the Rust builder accepts
`Runner::link_message_limit(NonZeroUsize)`. The limit is inherited by workers
and checked in both directions; the inbound codec rejects oversized lengths
before reserving payload storage. This setting affects Link sessions; the
optional local listener retains its own WebSocket transport policy.

## Python environment

gufi embeds CPython in the application process. Set `PYO3_PYTHON` to the
interpreter used to build the host; the application's virtual environment
supplies its Python dependencies. Interpreter initialization happens once,
independently of ASGI lifespan restarts.

## The name

*Kufi* — κουφή, "the deaf one" — is what Cyprus calls its blunt-nosed viper. It
is not deaf. It has no external ear opening and does not startle at sound, so
people concluded it could not hear; what it actually does is read the ground,
picking up vibration through the bones of its jaw.

A serpent, for the obvious reason. A quiet one, for a better one.

---

`ARCHITECTURE.md` has the boundaries: what the runner owns, what it
deliberately does not, and why it is its own project rather than a crate inside
Claviron.
