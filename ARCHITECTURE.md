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
internal `claviron-*` crate. What every language runner needs alike lives in
Claviron's language-neutral runner shell (`claviron::runner`), and moves there
piece by piece; this crate supplies the Python owner. Nothing points from
Claviron back to it.

Claviron knows nothing about Python. The interpreter, the ASGI engine and PyO3
all live here, which is why Claviron can be built and tested without a Python
interpreter present at all.

The supported interpreter is CPython 3.15.x with its version-specific ABI.
Build preparation checks both the implementation and version and records the
selected executable. It does not introduce a second interpreter selector in
Claviron or probe PATH per request. Owned deployment builds and macOS's current
Homebrew build use the same selection contract; older Python fallback and
multi-version stable-ABI builds are not part of this runner.

It owns no vhost routing, configuration providers, process supervision, or
Orbit transport. TLS on its listener is the runner shell's; which proxies to
believe is Claviron's HTTP layer's.

## Native HTTP benchmarks

With `bench`, the runner registers Claviron's shared
`claviron::bench::runner::BenchService` through the existing
host-free Routable registry. Its native baselines are available without an
application interceptor; they do not execute Python or open another listener.
`/_bench/rust` and `/_bench/stats` are the shared runner benchmark format.
The original `/_gufi/rust` and `/_gufi/stats` remain aliases. Application
interceptors take precedence over these contributions; custom raw services may
compose `bench::routes()` explicitly. Without this feature, no benchmark provider
or handlers are compiled.
The feature forwards to `claviron/bench`; this crate retains only registration
and compatibility URLs, not an independent native workload implementation.
The shared `/bench/plain`, `/bench/json` and streaming `/bench/echo` endpoints
are available through the same runner ingress too.

## The lifecycle is Claviron's

`Runner::run` holds no sequence. It builds a `claviron::Server`, registers
providers into it, and Claviron's standalone bootstrap does the rest: instance
lock, service manager, Orbit fleet, signal handlers, dependency-ordered boot,
validation, runnable generations, drain, finalize.

The launch's supervisor policy is registered through `Server::with_definitions`,
before core components bind their configuration sources. Registering it only
in the application callback is too late for supervisor configuration.

The runner sets `AppState::config_dir()` to the selected launch file's parent
directory (`.` for a bare filename). Without an explicit path, the file is
`gufi.yaml` under the application root. Application-owned configuration sources
use this state value, and workers inherit the same directory. The Python root
and virtual environment remain independent of the configuration directory.

Three providers take part, and an application's own sit beside them with no
special status:

| provider | what it does |
|---|---|
| `PythonApplicationService` | `boot()` prepares the native Python application and adds its controller to `ApplicationManager`, then awaits ASGI startup. `Deps` declares the manager and optional runtime capabilities. Masters stay interpreter-free. `finalize()` drains the current application generation. |
| `Application` | holds the service the application registered. No lifecycle of its own — a registry entry, not a stage. |
| `DevIngress` **or** `OrbitIngress` | `Runnable`, and exactly one of them: `RunMode` decides which is registered. One binds a listener, the other binds nothing and takes a lane in its application's fleet. Both serve until their generation is cancelled, then finish what they accepted. |

The order that falls out is the order that matters: the interpreter is up
before any runnable is spawned, and it goes down after they have drained. A
request cannot reach an application that is not ready, and shutdown waits for
work rather than interrupting it.

### Link application management

The common Application description reports Python runtime, Gufi runner and
ASGI request mode, independently of the `asgi` control identity or its ingress.
An optional `frameworks` list in the shared launch/CLI (or `Runner::frameworks`)
declares informational tags and takes precedence over detection, even when empty.
Scalar `framework` and `Runner::framework` remain one-label shorthands.
When absent, launch preparation detects known frameworks from runtime
dependencies in the application's `pyproject.toml`, retaining all distinct
recognized names. Missing or unrecognized metadata yields no tags;
unreadable or malformed metadata emits a warning rather than blocking launch.
The resolved list and optional `icon` client code are carried in the worker launch, not calculated on requests
or through Python imports. Execution stores this description once at
preparation. Fleet and Link snapshots retain the same per-placement data,
without changing ASGI request dispatch. WSGI remains
an unimplemented interface, not an alias for request-scoped PHP scripts.

Orbit launches use the runner shell's common Link command bridge and connection
observations (`LinkJoined`, `EdgeConnected`, `EdgeDisconnected`,
`EdgeShutdownInitiated`). Gufi does not implement another Link transport or
connectivity watcher. The same gRPC Link management surface addresses PHP and
Python applications by fleet, Link name and runtime-qualified application id.

Gufi's `PythonApplicationController` owns only Python execution: one prepared
ASGI definition, its current loop thread, lifecycle, generation and error.
Start awaits lifespan startup; stop removes admission, drains accepted HTTP
and WebSocket work and awaits lifespan shutdown; restart serializes stop/start.
Native boot and join run on a blocking executor, not a Tokio reactor. The
controller retains the event bus and shutdown signal, never the owning registry.
There is no synthetic PHP-style pool status for the ASGI loop.

The runner, Rust routes and Link lane stay alive while Python is stopped.
Python HTTP/WebSocket dispatch returns 503 until a started generation publishes
its handle. Lifecycle events are emitted after actual transitions, not when a
command is merely accepted. No drain deadline or forced socket closure is
invented: existing work, including open WebSockets, must finish.

Restart re-runs ASGI lifespan on the process's existing CPython interpreter;
it does not finalize CPython or evict imported modules. Source-code refresh
remains the native watcher's existing rolling worker restart. These operations
are deliberately different, just as PHP source renewal remains PHP-owned.

## What an application registers

A `tower_service::Service`, which is deliberately not a server.

```rust
pub fn configure(state: &AppState) -> registry::Result<()> {
    // Register application services and event listeners here.
    Ok(())
}
```

`Runner::with_interceptor(routes::rust_interceptor)` builds the Rust router
after these registrations. The shared runner puts HTTP `Routable` contributions
behind it, then Python as the path and method fallback. With no interceptor,
registered routables still intercept Python. The low-level `register_router`
entry point remains available for custom compositions.

The development listener calls that service with a request it read off a
socket. An Orbit ingress will call the same service with a request it read out
of a frame. `configure` is written once and never learns which one it is under.

### The router is not this crate's

An application brings axum, warp, tower-http or a bare `service_fn`. This crate
holds no router: the frameworks already have one, and each already knows how to
say "not mine" in its own vocabulary. Writing a third would only be a worse
copy.

Two entry points, because that vocabulary differs:

- **`register_router`** — with the `axum` feature, takes the function that builds the
  application's `axum::Router` (called with its state) and applies
  the rule for you: the Python application becomes that router's
  `fallback_service` and its `method_not_allowed_fallback`. The application
  does not write it, because it is not a choice. **The Python application is
  the fallback**, always. What the Rust routes do not declare — an undeclared
  path, or a declared path reached with an undeclared method — reaches the
  interpreter, so the application stays the authority on what exists and the
  Rust routes only intercept.
- **`register_router_with`** — takes a builder handed the Python service, and the
  application places it itself. It needs only `tower-service`, so a warp filter
  or a bare service goes through with no axum in the graph at all.

The `axum` feature exists because naming `axum::Router` in a signature means
this crate and the application must resolve the same axum. `gufi::axum`
re-exports Claviron's, so they cannot end up with two.

The same surface exposes `gufi::Routable`, Claviron HTTP's host-independent
route capability. `routes(self: Arc<Self>, &AppState)` receives the registered
shared provider instance, allowing handlers to capture it directly. Registration
still passes an Arc; no provider-data clone or per-request registry lookup is
required. The contribution remains `Option<Router<AppState>>`, merged by the
shared runner shell rather than by a Gufi-specific registry.

## One name, and it is an address

`application_name` is the only name there is. The instance lock is held under
it, and a Claviron edge serving this application names it in its own
configuration the way it names a PHP or JavaScript one — so the two spellings
have to match. It defaults to the working directory's name, which is
convenient and is also why a starter kit states it: a directory rename would
move an address.

The process-family fleet can be selected separately with `fleet_name` (or
`--fleet-name`). If omitted, a valid application name is used; names exceeding
the fleet primitive's length limit require an explicit fleet name. Two distinct
namespaces exist:

- This process's own, holding its workers and its metrics, named after the
  application unless explicitly selected. Never implicitly Claviron's `clv` — shared memory outlives the process
  that made it, and defaulting into an edge's fleet would map segments
  belonging to a server this process knows nothing about.
- The shared one, where the transport's tables live. It is the same on every
  process on the machine, so an edge worker and an application find each other
  because they opened the same segment rather than because either was told
  about the other.

Several processes may answer to one application name. They take a lane each
and know nothing about each other; the edge reads the lanes serving the name
and picks the idlest one. Nothing has to be told that another started.

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

Interpreter initialization is serialized once per process through PyConfig.
It names the declared 3.15 venv executable, or the build-selected executable
when no venv is declared, and leaves signals/argv/stdio owned by the host.
Ambient PYTHONHOME/PYTHONPATH and user site packages cannot select another
installation. Venv version metadata is checked before initialization; the live
version and prefix are verified before application import. An existing process
cannot switch virtual environments. A missing venv executable is an error.

CPython owns site initialization, including PEP 829 startup ordering and .start
hooks. Lifespan restart does not replay addsitedir or duplicate module paths.
The shipped driver is `src/asgi/driver.py`, not a Rust string literal; only its
diagnostic traceback import is lazy. Importing the ASGI target and starting
lifespan stay eager so readiness does not hide an application import failure.

The Python driver owns the lifespan application in a lifecycle TaskGroup and
accepted HTTP/WebSocket exchanges in a separate request TaskGroup. Each exchange
contains its application, scope-construction and failure-reporting errors;
they never escape as a sibling-cancelling request-group failure. CancelledError
remains cancellation, not an ordinary application error. Pending receive entries
are removed when their exchange ends, including failure/cancellation paths.

Startup/shutdown message races use short-lived TaskGroups owning only their
queue waiter. Python 3.15 TaskGroup.cancel ends a phase and joins its losing
waiter; it has no authority over requests or the lifespan application. Declining
lifespan remains supported, while explicit failed protocol messages still fail
startup/shutdown and join the owned application task before returning.

Normal shutdown never cancels the request group. Its context exits normally
and joins all accepted exchanges before lifespan shutdown. The doorbell reader
remains registered through that join so body/message/backpressure work can
finish; finally cleanup removes it on every exit. An explicit cancellation of
the driver propagates and joins its owned tasks. No timeout or forced drain is
introduced, and unrelated application-created tasks are not adopted into these
groups.

Shutdown orders the command queue against new HTTP/WebSocket submissions, so
a previously cloned handle cannot enqueue work after the stop marker. Body
and message wakeups stay active until accepted work drains; repeated shutdown
does not insert a second marker into that drain.

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

**Current scope.** Lifespan, streaming HTTP and WebSocket message sessions.
Gufi owns the ASGI driver and Python execution; there is no separate
`claviron-asgi` engine. The runner owns ingress and Link dispatch.

### Link message adaptation

The owner callback accepts H1 GET WebSockets, H2 CONNECT WebSockets and one
HTTP/3 CONNECT reliable WT bidi stream per invocation. `MessageTransport`
travels with the native exchange into the driver, where `gufi.transport`
identifies the original transport, method, scheme and HTTP version. WT uses
the ASGI WebSocket message vocabulary as an explicit Gufi extension, without
claiming a full ASGI WebTransport session/datagram API.

`orbit::relay` owns independent read and write futures and joins both. One
Python message is pending in each direction. `link_message_limit` travels in
the inherited launch and bounds records before allocation; application output
uses the same payload bound. Input FIN closes ASGI receive but allows output
to drain. Application completion wakes a pending read/delivery; malformed or
failed input wakes a parked writer. Cancellation uses existing stream/reset
signals and tokens, never elapsed-time polling or an invented drain deadline.

An admission observes the stream reset while waiting for Python's decision.
The shared runner's `Handshake::try_answer` reports publication failure so
Gufi can reset its endpoint, waking the edge instead of leaving admission
waiting on a reply that never entered the inbox.

The runner attaches its generation's read-only `SessionShutdown` signal to
each Link message request before handing it to the owner. Gufi observes it
during admission and relay. Retirement queues an ASGI disconnect (1001), then
waits for the owner to return and its accepted output to finish before sending
the final close. Python tasks are not force-cancelled. This signal reaches an
idle owner before ingress drain, rather than waiting for Python finalization
which itself follows that drain. Ordinary in-flight HTTP work keeps its
existing completion contract.

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

## Fleet ingress

`RunMode::Orbit` binds nothing. The process joins its application's fleet —
segments named after the application, which no other application maps —
takes a lane, writes its application name in it, and waits; a Claviron edge
asked for that name opens the same fleet and writes an exchange into the
lane. The service that answers is
the one `DevIngress` would have called, so `configure` never learns which door
a request came through.

**Nobody is told about anybody.** There is no registration call, no supervisor
of application processes and no pool. This process starts on its own, and
introducing itself *is* taking the lane. Several processes may serve one name
without knowing about each other; the edge reads the lanes serving it and
picks the idlest, which is a number it reads rather than one anybody reports.

**The lane is read by a parked thread.** It waits on the lane's wake word — a
futex, or `os_sync_wait_on_address` on macOS — and the wait ends because
someone wrote, not because an interval elapsed. Frames cross to Tokio over a
channel as deep as the lane itself, so there is one place that can be full and
one refusal to explain. Shutdown releases the lane first, so the edge stops
choosing this process, and then *wakes* the thread rather than waiting out a
timeout it does not have.

A host with no shared-memory wait primitive is refused at boot. The
alternative would be a poll, and a poll is this transport's whole point
undone.

## Production

In production a Claviron edge terminates TLS, routes vhosts and serves static
files, and reaches this process by name over Orbit shared memory instead of a
socket. For that edge this is a vhost like any other: a document root for
static files, and a name for everything else. It knows the name and nothing
about the virtual environment behind it.

This is the shape every Python deployment already has — nginx in front of
gunicorn, Apache's `WSGIDaemonProcess`, one process per application behind a
socket. What differs is the cost of the hop, not the topology.

### Why the interpreter is not in the edge instead

Two independent reasons, either of which is enough.

**One interpreter per process.** This runner owns one main interpreter, not
subinterpreters. A GIL-enabled build runs Python on one attached thread at a
time; a free-threaded build does not change process-wide environment/module
ownership. A worker holding Python would share `sys.path`, `os.environ` and
`sys.modules` across sites: two applications could not have independently
isolated versions of a library,
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
