# gufi-asgi

Embeddable CPython and ASGI execution for Rust hosts. This package owns PyO3,
the interpreter boundary, ASGI lifespan and streaming HTTP/message execution.
It does not start a Gufi runner, HTTP listener or process supervisor.

The `gufi` runner uses this engine and preserves its existing public API.
Hosts that own routing and lifecycle can depend on the engine directly:

```toml
[dependencies]
gufi-asgi = "0.2.0-alpha.24"
```

## Ownership

`asgi::PythonApplication` declares the `module:attribute` target, module paths
and optional virtual environment. `boot()` starts the application-loop thread
and returns its `RunningPythonApplication` owner only after lifespan startup
completes. Its cloneable handle accepts streaming HTTP and message sessions.

The host retains the running owner while requests are admitted. It stops its
ingress and drains accepted work before calling `shutdown()`, which waits for
accepted exchanges, ASGI lifespan shutdown and the loop thread. Drop uses the
same shutdown path. Boot and shutdown may block; async hosts must run these
ownership transitions on their blocking executor. No implicit grace deadline
or forced Python-task cancellation is introduced.

`routing::PythonService` is a Tower-compatible HTTP adapter. Its shared handle
cell follows the host-managed application generation; an empty cell returns
503. `routing::request_scope` preserves Claviron's accepted connection and
trusted-proxy context when adapting message handshakes.

Initialization owns one process-wide CPython 3.15 interpreter and its selected
environment. This package provides no independent virtual-environment or
subinterpreter isolation inside a process. Application stop/restart runs ASGI
lifespan without finalizing CPython or evicting imported modules.

## Native Python capabilities

The native Python namespace remains `gufi`, including invocation and counters,
plus optional broadcast, logging and Django Channels capabilities. The
`broadcast`, `log` and `django` features select their native adapters; the host
supplies the matching services. They do not install a Python compatibility
package or change the ASGI protocol.

## Building

Select CPython 3.15 with `PYO3_PYTHON` or PyO3 configuration. This package owns
ABI validation and publishes the shared-library directory to a direct host's
build script as `DEP_GUFI_ASGI_PYTHON_LIB_DIR`. The `gufi` runner forwards the
same directory under its existing `DEP_GUFI_PYTHON_LIB_DIR` key.

Existing engine tests live under `tests/`. The Python driver checks can also
run directly, without booting a Rust host:

```sh
python3.15 -B tests/python/test_asgi_driver.py
python3.15 -B tests/python/test_native_module.py
```
