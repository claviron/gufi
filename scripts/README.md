# Python installation

`python-install.sh` is the canonical CPython installer for Gufi on Linux and
FreeBSD. It defaults to **CPython 3.15.2**, shared libpython, PGO, LTO and a
non-debug production build. Claviron does not maintain an installer copy.

```sh
bash scripts/python-install.sh dry-run
bash scripts/python-install.sh install
bash scripts/python-install.sh verify
```

Running without a command prints help. `PYTHON_VERSION` selects another stable
3.15 patch release; `PYTHON_INSTALL_DIR` defaults to `/opt/python-3.15`.
`PYTHON_SRC_DIR`, `MAKE_JOBS` and `SKIP_APT=1` select source storage, parallelism
and preinstalled dependencies. Installation and application restarts remain
separate operator actions.

The release appends `-O3 -g0` to supplied C flags, explicitly disables
`Py_DEBUG`, keeps frame pointers and enables computed gotos. ELF shared builds
use `-fno-semantic-interposition` and initial-exec TLS because Gufi links
libpython at process startup. The interpreter uses the same library through
its rpath. `STATIC=1` selects a static archive instead.

FreeBSD uses `gmake` for cleanup, compilation and installation, and includes
the ports header/library and pkg-config directories. Clang PGO selects the
compiler's own `llvm-profdata`; set `CC` and a matching `LLVM_PROFDATA` explicitly
when using a separate LLVM toolchain. The installed interpreter is checked for
the selected patch release, a non-debug PGO/LTO build, required standard-library
modules and the correct shared-library identity.

`PGO_EXCLUDE` controls the training module exclusions. Set it empty to use
the complete upstream PGO training selection. No benchmark performance claim
is implied by these build settings.

After installation, select that interpreter with `PYO3_PYTHON` and `UV_PYTHON`;
application environments remain under `.venv`. Do not set `PYTHONHOME` globally.
macOS uses an explicitly selected native CPython installation; this installer
targets ELF platforms only.
