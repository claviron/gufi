# Gufi Architecture

`gufi` runs a Python application on Claviron, and only Python. Nothing selects
that: there is no feature to turn on and no second adapter waiting behind one.

## What it builds

A binary that is a Claviron process. Not a plugin, not an extension of a server
someone else runs — the same instance lock, the same registry, the same fleet
membership, the same drain. What differs from Claviron's own binary is only
which providers are in it.

That is the whole shape, and most of what follows is a consequence of it.

## Boundary

It is its own project rather than a crate inside Claviron, because it compiles
the application's binary and a library does not compile the things built on it.
It reaches Claviron the way any consumer does — through the root facade,
`claviron::app`, `claviron::runtime`, `claviron::Server` — and never through an
internal `claviron-*` crate. There is no `claviron::runner` path back the other
way: the facade does not carry the thing that builds against it.

Claviron knows nothing about Python. The interpreter, the ASGI engine and PyO3
all live here, which is why Claviron can be built and tested without a Python
interpreter present at all.

It owns no TLS, vhost routing, static files, configuration providers, process
supervision, or Orbit transport.

## The lifecycle is Claviron's

`Runner::run` holds no sequence. It builds a `claviron::Server`, registers
providers into it, and Claviron's standalone bootstrap does the rest: instance
lock, service manager, Orbit fleet, signal handlers, dependency-ordered boot,
validation, runnable generations, drain, finalize.

Three providers take part, and an application's own sit beside them with no
special status:

| provider | what it does |
|---|---|
| `PythonApplicationService` | `boot()` brings the interpreter up and drives ASGI lifespan startup; `finalize()` takes it down. Declares `ProviderOrder::after::<InvocationRouter>()`, because its boot resolves that router. |
| `Application` | holds the service the application registered. No lifecycle of its own — a registry entry, not a stage. |
| `DevIngress` | `Runnable`. Binds the development listener, serves until its generation is cancelled, then finishes what it accepted. |

The order that falls out is the order that matters: the interpreter is up
before any runnable is spawned, and it goes down after they have drained. A
request cannot reach an application that is not ready, and shutdown waits for
work rather than interrupting it.

## What an application registers

A `tower_service::Service`, which is deliberately not a server.

```rust
pub fn configure(state: &AppState) -> registry::Result<()> {
    gufi::register(state, routes::router())
}
```

The development listener calls that service with a request it read off a
socket. An Orbit ingress will call the same service with a request it read out
of a frame. `configure` is written once and never learns which one it is under.

### The router is not this crate's

An application brings axum, warp, tower-http or a bare `service_fn`. This crate
holds no router: the frameworks already have one, and each already knows how to
say "not mine" in its own vocabulary. Writing a third would only be a worse
copy.

Two entry points, because that vocabulary differs:

- **`register`** — with the `axum` feature, takes an `axum::Router` and applies
  the rule for you: the Python application becomes that router's
  `fallback_service` and its `method_not_allowed_fallback`. The application
  does not write it, because it is not a choice. **The Python application is
  the fallback**, always. What the Rust routes do not declare — an undeclared
  path, or a declared path reached with an undeclared method — reaches the
  interpreter, so the application stays the authority on what exists and the
  Rust routes only intercept.
- **`register_with`** — takes a builder handed the Python service, and the
  application places it itself. It needs only `tower-service`, so a warp filter
  or a bare service goes through with no axum in the graph at all.

The `axum` feature exists because naming `axum::Router` in a signature means
this crate and the application must resolve the same axum. `gufi::axum`
re-exports Claviron's, so they cannot end up with two.

## Names are addresses

Two of them, separate on purpose:

- **`application_name`** — what this process is called in its fleet. The
  instance lock is held under it, and a Claviron edge serving this application
  names it in its own configuration the way it names a PHP or JavaScript one.
  The edge resolves it over shared memory, so the two spellings have to match.
- **`fleet_name`** — the shared namespace, and therefore the shared memory this
  process maps. It defaults to the application name and never to Claviron's
  `clv`: shared memory outlives the process that made it, so defaulting into an
  edge's fleet would map segments belonging to a server this process knows
  nothing about.

Both default from the working directory, which is convenient and is also why a
starter kit states them: a directory rename would move an address.

## The ASGI engine

`asgi.rs` is the in-process Python boundary. It loads an ASGI callable, drives
its lifespan protocol, and translates HTTP exchanges into `scope` / `receive` /
`send` events.

**ASGI never crosses a process boundary.** The wire is HTTP — a socket today,
an Orbit frame next — and the translation happens here, on the side that holds
the interpreter. It is how nginx and uvicorn divide the same work: nginx speaks
HTTP and knows nothing of ASGI.

**Python ownership.** One running application owns one Python event-loop
thread. The thread imports the configured `module:attribute` target, invokes
the lifespan protocol, and dispatches submitted HTTP exchanges on that same
loop. `boot()` does not return until `lifespan.startup.complete` is observed.
Shutdown stops admission, waits for submitted exchanges, sends
`lifespan.shutdown`, and waits without an implicit deadline for the application
task to return.

**The module the application imports** is `claviron`, installed into the
interpreter at boot:

```python
import claviron
response = await claviron.invoke("claviron.ping.v1", {"echo": "app"})
```

It is named for what answers rather than for what compiled the process: the
operations behind it are Claviron's, it is installed whether or not anything
sits above the engine, and the same import keeps working when the application
runs somewhere else. The module never exposes the raw Rust registry.

**Current scope.** Lifespan and buffered request/response. No WebSocket, no
streaming. A production transport must carry ASGI's streaming and backpressure
rather than inherit this buffering.

## Development ingress

The private `http` module binds a plain HTTP/1 listener, collects one request
and one response, and hands it to the registered service. It is not a public
server API, not a production transport, and its numbers are not performance
evidence.

It knows nothing about routing and nothing about ASGI: it turns a socket into a
request and writes back a response. Everything between is the application's,
which is why the same service runs unchanged under an ingress that has no
socket at all.

Connection addresses ride in request extensions. ASGI's `scope` names a client
and a server, `http::Request` carries neither, so the ingress that accepted the
request attaches them and the bridge reads them there. Nothing is invented: an
ingress that does not know an address does not attach one, and the scope
reports `None`.

## Production

In production a Claviron edge terminates TLS, routes vhosts and serves static
files, and reaches this process by name over Orbit shared memory instead of a
socket.

This is the shape every Python deployment already has — nginx in front of
gunicorn, Apache's `WSGIDaemonProcess`, one process per application behind a
socket. What differs is the cost of the hop, not the topology.

### Why the interpreter is not in the edge instead

Two independent reasons, either of which is enough.

**One interpreter per process.** PyO3 does not support sub-interpreters ("PyO3
modules do not yet support subinterpreters", PyO3 #576), and CPython's GIL
means one thread runs Python at a time in one. A worker holding Python would
hold one `sys.path`, one `os.environ`, one `sys.modules` and one GIL for every
site on the server: two applications could not have two versions of a library,
and a C extension that segfaults would take TLS, vhosts, static files, PHP and
JavaScript down with it.

**Worker anonymity.** Claviron's workers share a listening socket with
`SO_REUSEPORT`, so the kernel chooses which one accepts. Putting a different
application in each worker is therefore not routable: a request for one site
lands on whichever worker the kernel picked. That anonymity is worth keeping —
it is why a dead worker costs capacity rather than availability, why rotation
is uniform, and why load moves between applications on its own.

So Python does not get an exception to worker anonymity. It moves out of the
worker, and the anonymity repeats a level down: two processes of the same
application are interchangeable, and the edge balances across them.

`mod_python` embedded CPython in Apache and was abandoned. `mod_wsgi` still
offers an embedded mode and its own maintainer recommends daemon mode —
separate processes — for these reasons. The servers that do embed Python
(uWSGI, Granian, Robyn) are Python servers: they are the process at the end of
the hop, not the edge in front of it.
