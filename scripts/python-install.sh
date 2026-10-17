#!/usr/bin/env bash
set -euo pipefail

# The CPython 3.15 a gufi runner embeds on Linux and FreeBSD.
#
# A runner loads libpython as a shared library, and on ELF how that library
# was built decides how fast every line of Python runs. Debian's
# `libpython3.13.so` runs the same code about 1.5x slower than Debian's own
# `/usr/bin/python3.13`, which links libpython statically: measured on xd01,
# 2026-09-25, it held a FastAPI SSR page at 392 req/s against uvicorn's 563.
# Two things cost it, both visible in a profile of the runner:
#
#   semantic interposition — every call inside the library goes through the
#     GOT/PLT and nothing inlines across functions;
#   the TLS model — each read of the thread state goes through `tlsdesc`.
#
# No distribution's Python is trusted for this, however it was built: the
# runner is shipped as our own package, and it carries the interpreter this
# script makes. Debian, Ubuntu and AlmaLinux 8 (production) alike.
#
# This builds the library without either: PGO and LTO as a release CPython,
# `-fno-semantic-interposition`, and the initial-exec TLS model, which is
# sound because a runner links libpython at start-up rather than dlopen-ing
# it. The interpreter binary links the same library through its rpath, so a
# venv made from it is the environment the runner embeds.
#
# macOS needs none of this: Mach-O's two-level namespace has no interposition
# cost and thread-locals are reached the same way in a dylib as in a static
# binary. macOS development currently selects Homebrew CPython 3.15 explicitly;
# it does not use the ELF-specific build below.
#
# Usage:
#   scripts/python-install.sh              # fetch, build, install, verify
#   scripts/python-install.sh deps         # print the package dependencies
#   scripts/python-install.sh dry-run      # show the resolved paths
#   scripts/python-install.sh verify       # smoke-test the installed interpreter
#
#   PYTHON_VERSION=3.15.0 scripts/python-install.sh
#   STATIC=1 scripts/python-install.sh     # libpython.a instead, for a runner
#                                          # linked statically (prod option)
PYTHON_VERSION="${PYTHON_VERSION:-3.15.0}"
[[ "$PYTHON_VERSION" =~ ^3\.15\.[0-9]+$ ]] || {
  echo "ERROR: this installer requires a stable CPython 3.15.x release" >&2
  exit 1
}
PYTHON_MINOR="${PYTHON_VERSION%.*}"
SRC_ROOT="${SRC_ROOT:-$HOME/src}"
PYTHON_SRC_DIR="${PYTHON_SRC_DIR:-$SRC_ROOT/Python-$PYTHON_VERSION}"
PYTHON_DIR="${PYTHON_INSTALL_DIR:-/opt/python-$PYTHON_MINOR}"
INSTALL_WITH_SUDO=0
STATIC="${STATIC:-0}"
MAKE_JOBS="${MAKE_JOBS:-}"
SKIP_APT="${SKIP_APT:-0}"

# The PGO training run is CPython's test suite. On the VM guests a signal
# test in test_generators fails on timing and stops the build; the profile
# is no worse without it.
PGO_EXCLUDE="${PGO_EXCLUDE:-test_generators}"

DEBIAN_PACKAGES=(
  build-essential
  curl
  xz-utils
  pkg-config
  libssl-dev
  zlib1g-dev
  libbz2-dev
  libffi-dev
  liblzma-dev
  libsqlite3-dev
  uuid-dev
  libncurses-dev
  libreadline-dev
  libgdbm-dev
  libmpdec-dev
  libedit-dev
)

# AlmaLinux 8 is the production host. Its gcc 8 builds PGO and LTO; the
# decimal module explicitly uses CPython's bundled libmpdec, which EL8 does
# not package; 3.15 no longer supplies an implicit fallback. Readline replaces
# libedit here.
ALMALINUX_PACKAGES=(
  gcc
  gcc-c++
  make
  curl
  xz
  pkgconf-pkg-config
  openssl-devel
  zlib-devel
  bzip2-devel
  libffi-devel
  xz-devel
  sqlite-devel
  libuuid-devel
  ncurses-devel
  readline-devel
  gdbm-devel
)

FREEBSD_PACKAGES=(
  gmake
  pkgconf
  curl
  libffi
  sqlite3
  readline
  gdbm
  mpdecimal
)

log() { printf '%s\n' "INFO: $*"; }
die() { printf '%s\n' "ERROR: $*" >&2; exit 1; }

detect_os() {
  case "$(uname -s)" in
    Linux) echo linux ;;
    FreeBSD) echo freebsd ;;
    Darwin) die "this installer builds ELF Python; select the installed Homebrew CPython 3.15 with PYO3_PYTHON on macOS" ;;
    *) die "unsupported OS: $(uname -s)" ;;
  esac
}

detect_linux_family() {
  if [[ -r /etc/os-release ]]; then
    . /etc/os-release
    case " ${ID:-} ${ID_LIKE:-} " in
      *" debian "*|*" ubuntu "*) echo debian ;;
      *" rhel "*|*" fedora "*|*" centos "*|*" almalinux "*|*" rocky "*) echo rhel ;;
      *) echo unknown ;;
    esac
  else
    echo unknown
  fi
}

jobs_count() {
  if [[ -n "$MAKE_JOBS" ]]; then
    echo "$MAKE_JOBS"
  elif command -v nproc >/dev/null 2>&1; then
    nproc
  else
    sysctl -n hw.ncpu 2>/dev/null || echo 2
  fi
}

print_deps() {
  case "$(detect_os)" in
    linux)
      case "$(detect_linux_family)" in
        rhel) echo "dnf install -y ${ALMALINUX_PACKAGES[*]}" ;;
        *) echo "apt-get install -y ${DEBIAN_PACKAGES[*]}" ;;
      esac
      ;;
    freebsd) echo "pkg install -y ${FREEBSD_PACKAGES[*]}" ;;
  esac
}

install_deps() {
  [[ "$SKIP_APT" == "1" ]] && { log "SKIP_APT=1, skipping package install"; return 0; }
  case "$(detect_os)" in
    linux)
      case "$(detect_linux_family)" in
        debian)
          sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q "${DEBIAN_PACKAGES[@]}"
          ;;
        rhel)
          sudo dnf install -y "${ALMALINUX_PACKAGES[@]}"
          ;;
        *) log "unknown Linux family; install by hand: $(print_deps)" ;;
      esac
      ;;
    freebsd)
      sudo pkg install -y "${FREEBSD_PACKAGES[@]}"
      ;;
  esac
}

prepare_prefix() {
  if [[ -d "$PYTHON_DIR" ]]; then
    if [[ ! -w "$PYTHON_DIR" ]]; then
      log "install prefix requires sudo: $PYTHON_DIR"
      sudo -v
      INSTALL_WITH_SUDO=1
    fi
    return 0
  fi

  local parent
  parent="$(dirname "$PYTHON_DIR")"
  while [[ ! -e "$parent" ]]; do
    parent="$(dirname "$parent")"
  done
  [[ -d "$parent" ]] || die "prefix parent is not a directory: $parent"

  if [[ -w "$parent" ]]; then
    mkdir -p "$PYTHON_DIR"
  else
    log "creating root-owned install prefix with sudo: $PYTHON_DIR"
    sudo install -d -o root -g "$(id -gn root)" -m 0755 "$PYTHON_DIR"
    INSTALL_WITH_SUDO=1
  fi
}

prepare_source() {
  mkdir -p "$SRC_ROOT"
  if [[ ! -f "$PYTHON_SRC_DIR/configure" ]]; then
    local tarball="Python-$PYTHON_VERSION.tar.xz"
    log "fetching $tarball → $SRC_ROOT"
    curl -sSfL -o "$SRC_ROOT/$tarball" \
      "https://www.python.org/ftp/python/$PYTHON_VERSION/$tarball"
    tar -C "$SRC_ROOT" -xf "$SRC_ROOT/$tarball"
  fi
}

build_python() {
  cd "$PYTHON_SRC_DIR"
  [[ -f Makefile ]] && { make distclean >/dev/null 2>&1 || true; }

  local flags="-fno-semantic-interposition"
  local shared=(--enable-shared)
  local ldflags="-fno-semantic-interposition -Wl,-rpath,$PYTHON_DIR/lib"
  if [[ "$STATIC" == "1" ]]; then
    # -fPIC so the archive can go into a runner, which is a PIE.
    flags="$flags -fPIC"
    shared=()
    ldflags="-fno-semantic-interposition"
  else
    flags="$flags -ftls-model=initial-exec"
  fi

  local decimal=(--with-system-libmpdec)
  if [[ "$(detect_os)" == "linux" && "$(detect_linux_family)" == "rhel" ]]; then
    decimal=(--without-system-libmpdec)
  fi

  CFLAGS="${CFLAGS:-} $flags" LDFLAGS="${LDFLAGS:-} $ldflags" ./configure \
    --prefix="$PYTHON_DIR" \
    "${shared[@]}" \
    "${decimal[@]}" \
    --with-frame-pointers \
    --enable-optimizations \
    --with-lto \
    --with-computed-gotos \
    --without-ensurepip

  local make="make"
  [[ "$(detect_os)" == "freebsd" ]] && make="gmake"
  "$make" -j"$(jobs_count)" PROFILE_TASK="-m test --pgo -x $PGO_EXCLUDE"
  # Compilation stays unprivileged; only installation into /opt needs root.
  if [[ "$INSTALL_WITH_SUDO" == "1" ]]; then
    sudo "$make" install
  else
    "$make" install
  fi
}

verify_install() {
  local python="$PYTHON_DIR/bin/python$PYTHON_MINOR"
  [[ -x "$python" ]] || die "interpreter missing: $python"
  "$python" -c '
import sys, sysconfig
import ssl, sqlite3, ctypes, decimal, lzma, bz2, zlib, uuid, readline, _struct
from builtins import frozendict, sentinel
assert sys.implementation.name == "cpython" and sys.version_info[:2] == (3, 15), "CPython 3.15 required"
print("OK:", sys.version.split()[0],
      "shared" if sysconfig.get_config_var("Py_ENABLE_SHARED") else "static",
      sysconfig.get_config_var("LIBDIR"))
'
  if [[ "$STATIC" != "1" ]]; then
    # The interpreter must load this prefix's library, not the system's.
    local loaded_lib
    loaded_lib="$(ldd "$python" | awk -v lib="libpython$PYTHON_MINOR.so" \
      '($1 == lib || index($1, lib ".") == 1) && $2 == "=>" { print $3; exit }')"
    # A relocated installation may retain its original rpath through a symlink.
    # Compare file identity so that this still rejects a different libpython.
    [[ -n "$loaded_lib" && "$loaded_lib" -ef "$PYTHON_DIR/lib/libpython$PYTHON_MINOR.so" ]] ||
      die "$python does not load $PYTHON_DIR/lib/libpython$PYTHON_MINOR.so"
  fi
  print_usage
}

# What to set once it is installed. Printed, not written into a shell file:
# which rc file a host reads is the operator's business.
print_usage() {
  local python="$PYTHON_DIR/bin/python$PYTHON_MINOR"
  cat <<EOF

Installed: $python

To use it, add to your shell rc (~/.zshrc or ~/.bashrc):

  # Python — own build, never the distro's
  export PYTHON_DIR="$PYTHON_DIR"
  export PATH="\$PYTHON_DIR/bin:\$PATH"
  export UV_PYTHON="\$PYTHON_DIR/bin/python$PYTHON_MINOR"
  export PYO3_PYTHON="\$PYTHON_DIR/bin/python$PYTHON_MINOR"
EOF
  cat <<EOF

Do not set PYTHONHOME: it would hand the system's python3 this stdlib. gufi
finds the stdlib through the venv's pyvenv.cfg.

Then, in the application:

  # pyproject.toml: requires-python = ">=3.15,<3.16"
  uv sync                                   # .venv, made from UV_PYTHON
  cargo build --release

Keep interpreter selection in one shared build environment (for example Cargo
[env]), not per-command overrides or each application's venv. Preserve PATH
and compiler flags across all callers. For native profiling add
-C force-frame-pointers=yes to that shared Rust flag configuration once,
preserving existing flags. CPython retains its own frame pointers by default.

A runner built against another Python must be rebuilt: PyO3 links the
libpython of the interpreter it was built with.
EOF
}

dry_run() {
  echo "OS: $(detect_os)"
  [[ "$(detect_os)" == "linux" ]] && echo "Linux family: $(detect_linux_family)"
  echo "version: $PYTHON_VERSION"
  echo "source: $PYTHON_SRC_DIR"
  echo "prefix: $PYTHON_DIR"
  echo "library: $([[ "$STATIC" == "1" ]] && echo static || echo shared)"
  echo "make jobs: $(jobs_count)"
  echo "pgo excludes: $PGO_EXCLUDE"
  echo "dependencies: $(print_deps)"
}

main() {
  case "${1:-install}" in
    deps) print_deps; exit 0 ;;
    dry-run) dry_run; exit 0 ;;
    verify) verify_install; exit 0 ;;
    install) ;;
    *) die "usage: $0 [install|verify|dry-run|deps]" ;;
  esac

  detect_os >/dev/null
  log "building CPython $PYTHON_VERSION → $PYTHON_DIR"
  prepare_prefix
  install_deps
  prepare_source
  build_python
  verify_install
}

main "$@"
