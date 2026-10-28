# Python 3.15 integration

Scope: CPython 3.15.x only. No older-interpreter compatibility path is retained.
Python execution belongs to the workspace's `gufi-asgi` engine. The `gufi`
runner uses that engine and keeps its existing launch and link-metadata API.

## Interpreter ownership and builds

The application binary links the installation selected through the shared
`PYO3_PYTHON`/PyO3 configuration. Build preparation rejects other implementations,
other minor versions and multi-version abi3/abi3t compilation. PyO3 and its build
configuration use the matching 0.30.1 family, alongside pyo3-async-runtimes 0.30.0.
The build configuration must resolve with PyO3 rather than retain a second,
older configuration crate. The 0.30 release supports CPython 3.15 final; its
pyclass extraction and native API changes were reviewed against this bridge.
The owned deployment Python includes its matching standard library and native
modules. The current macOS development installation is Homebrew CPython 3.15.0;
Linux/FreeBSD use `scripts/python-install.sh` and their explicitly selected prefix.

Keep Python/PHP selection, PATH and Rust flags in one shared host build
configuration. Gufi does not rewrite that configuration or inject PATH entries.
Do not alternate between a base interpreter and per-project venv executables
when building the same dependency artifacts. A venv belongs to application
dependency loading at runtime. Recreate 3.14/older environments explicitly and
declare `requires-python = ">=3.15,<3.16"` in the application.

Initialization uses PyConfig, the declared venv executable or the selected build
executable. Missing executables/version metadata reject startup, not a fallback
to `Python::initialize()` with the Rust program's location. Ambient PYTHONHOME,
PYTHONPATH and user site packages do not choose another interpreter. Process
signals, argv and stdio remain host-owned. Initialization is serialized; an
externally initialized interpreter or another venv requires a new worker.

The runtime also checks the live minor version and venv prefix before importing
the ASGI application. This is not a byte-level proof that a deployment's copied
stdlib/native modules match libpython: package and deploy the owned installation
as a unit. Do not mix distribution libraries into that prefix.

## Release features and decisions

Primary source: [What's new in Python 3.15](https://docs.python.org/3.15/whatsnew/3.15.html).

- **PEP 829 startup files:** CPython initializes site directories and processes
  `.pth`/`.start` files with its own StartupState ordering. Gufi no longer calls
  addsitedir again at every lifespan start. An ASGI restart is not interpreter
  reinitialization or a request to repeat package startup hooks.
- **PEP 810 lazy imports:** the shipped `crates/asgi/src/asgi/driver.py` lazily imports
  traceback, used only for error formatting. The configured ASGI target and
  lifespan startup remain eager: import errors must still prevent readiness.
  No global import-mode override delays application failures into requests.
- **UTF-8:** the 3.15 default is used; no legacy encoding fallback is introduced.
  ASGI raw headers, paths and bodies retain their byte contracts.
- **frozendict/sentinel:** applications may use these builtins directly. ASGI
  scope/state remain ordinary mutable dictionaries; freezing them would change
  the application contract, not optimize it transparently.
- **PEP 788 finalization protection:** the new interpreter guards/views are
  relevant to a future explicit finalization owner. This runner still never
  calls Py_Finalize on lifespan stop/restart. PyO3 owns thread attachment; no
  parallel hand-written FFI attachment/guard implementation is added here.
- **Free-threading/abi3t:** no old-Python stable-ABI compatibility build is used.
  A 3.15 free-threaded interpreter is a separate build/ABI and third-party
  extension audit, not a flag that converts the installed GIL build. It has not
  been validated here; Python process/module isolation remains necessary.
- **JIT/tail-call interpreter:** these depend on the selected CPython build and
  its compiler. The inspected Homebrew build has its GIL enabled and no JIT.
  This change does not install LLVM, rebuild Python or enable experimental JIT.
  Release benchmark improvements are not Gufi performance measurements.

## ASGI task ownership

The lifecycle and request groups are separate. HTTP/WebSocket application
errors, malformed scope access and receive failures terminate only the affected
exchange. Even an error while reporting that failure is isolated through the
loop's exception handler, not raised into the request TaskGroup. Cancellation
still propagates after owned receive-waiter cleanup.

Python 3.15 TaskGroup.cancel is used only for the short-lived startup/shutdown
queue-waiter races. The main lifespan task remains outside those phase groups;
declining the lifespan protocol therefore leaves no orphan get() waiter and
does not cancel another owner. Explicit protocol failures join the application's
cleanup before returning the original single error to Rust.

Graceful shutdown exits the request group normally and awaits every accepted
exchange, including WebSockets. Doorbell readiness stays active throughout
drain. Only after this join is the reader removed and lifespan shutdown sent.
No shutdown timer, implicit cancellation or sibling fail-fast policy is added.
The driver joins only tasks it creates, not arbitrary framework background work.

## Owned CPython installer

The installer accepts stable 3.15.x only, preserves supplied compiler/linker
flags and explicitly keeps frame pointers enabled. CPython 3.15 removed the
implicit bundled-libmpdec fallback: Debian/FreeBSD select the packaged system
library, while EL8 explicitly selects the bundled library. This preserves the
existing dependency policy without silently losing the decimal extension.
The macOS development installation is not rebuilt by this ELF-oriented script.
Verification checks the installed minor version, essential extensions and the
loaded libpython identity. No interpreter installation or operating-system
dependency operation was run as part of this source change.

## Profiling across Python and Rust

Python 3.15 adds `profiling.sampling` (Tachyon), including async-aware snapshots
and attach-to-PID operation. Frame pointers are enabled in CPython by default;
keep them in the final Rust application and native libraries as well. Add
`-C force-frame-pointers=yes` to the one shared build flag configuration,
preserving existing flags. A library build.rs cannot enforce code generation
flags on the downstream binary or the whole dependency graph.

Do not attach a profiler to a running deployment without operator approval.
Sample frequency, duration and output are operator choices; this change adds
no profiler loop, polling cadence or automatic process attachment.

## Verification boundary

- Python 3.15 syntax parsing of shipped Python and existing fixtures.
- `python3.15 -B crates/asgi/tests/python/test_asgi_driver.py`: isolated HTTP scope/state,
  streaming and lazy traceback checks, concurrent HTTP/WebSocket failure
  isolation, post-stop receive readiness, graceful drain ordering, failed or
  absent lifespan cleanup and explicit cancellation cleanup; no Claviron boot.
  Twelve tests passed on macOS with CPython 3.15.2. They use local descriptor
  pairs and explicit event signals, not sleeps, a service or an HTTP listener.
- `python3.15 -B crates/asgi/tests/python/test_native_module.py`: two native
  module compatibility checks passed.
- Shell syntax check of the installer and static diff checks.
- `rust-check.sh`: the workspace/all-features Rust suite passed on macOS with
  CPython 3.15.2 and PyO3 0.30.1 (18 runner tests and 17 engine tests; one
  documentation example ignored).
  This includes version-metadata rejection, embedded lifespan, HTTP dispatch,
  streaming and shutdown-admission behavior. The Python driver tests above
  exercise sibling isolation and explicit cancellation separately.

Deployment-specific venv/lifespan restart acceptance remains to be performed
by the operator. No running service, PHP code, application venv or installed
interpreter changed.
