#!/usr/bin/env bash
set -euo pipefail

# Production CPython for Gufi's Linux and FreeBSD application binaries.
# PGO and LTO optimize the interpreter. ELF shared builds also avoid semantic
# interposition and use initial-exec TLS because libpython is linked at startup.
# macOS uses an explicitly selected native CPython installation instead.
#
# Usage: scripts/python-install.sh [install|verify|dry-run|deps|help]
# STATIC=1 selects a static archive for an application linked statically.
PYTHON_VERSION="${PYTHON_VERSION:-3.15.2}"
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

# Override or set empty to use the complete upstream PGO training selection.
PGO_EXCLUDE="${PGO_EXCLUDE-test_generators}"

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

# On AlmaLinux 8, GCC supports PGO and LTO; the
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
  local make="make"
  [[ "$(detect_os)" == "freebsd" ]] && make="gmake"

  # Use the selected Clang's profiling tools, not an unrelated LLVM on PATH.
  # Profile formats are not interchangeable across LLVM releases.
  local compiler=() compiler_version profdata
  read -r -a compiler <<< "${CC:-cc}"
  compiler_version="$("${compiler[@]}" --version)"
  if [[ "$compiler_version" == *clang* && -z "${LLVM_PROFDATA:-}" ]]; then
    profdata="$("${compiler[@]}" -print-prog-name=llvm-profdata)"
    [[ "$profdata" == /* && -x "$profdata" ]] ||
      die "cannot locate this Clang's llvm-profdata; set LLVM_PROFDATA to its matching tool"
    export LLVM_PROFDATA="$profdata"
    log "PGO profile merger: $LLVM_PROFDATA"
  fi

  [[ -f Makefile ]] && "$make" distclean

  local flags="-fno-semantic-interposition -O3 -g0 -fno-omit-frame-pointer"
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

  local cppflags="${CPPFLAGS:-}"
  if [[ "$(detect_os)" == freebsd ]]; then
    cppflags="$cppflags -I/usr/local/include"
    ldflags="$ldflags -L/usr/local/lib"
    export PKG_CONFIG_PATH="/usr/local/libdata/pkgconfig:/usr/local/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
  fi

  local decimal=(--with-system-libmpdec)
  if [[ "$(detect_os)" == "linux" && "$(detect_linux_family)" == "rhel" ]]; then
    decimal=(--without-system-libmpdec)
  fi

  CFLAGS="${CFLAGS:-} $flags" CPPFLAGS="$cppflags" LDFLAGS="${LDFLAGS:-} $ldflags" ./configure \
    --prefix="$PYTHON_DIR" \
    "${shared[@]}" \
    "${decimal[@]}" \
    --with-frame-pointers \
    --without-pydebug \
    --enable-optimizations \
    --with-lto \
    --with-computed-gotos \
    --with-readline=readline \
    --without-ensurepip

  local profile_task='-m test --pgo'
  if [[ -n "$PGO_EXCLUDE" ]]; then profile_task+=" -x $PGO_EXCLUDE"; fi
  # CPython's LTO configuration adds -g after CFLAGS. These make variables
  # follow configure's flags, including in the recursive PGO build.
  local release_flags=(
    "CFLAGS_NODIST=${CFLAGS_NODIST:+$CFLAGS_NODIST }-O3 -g0"
    "LDFLAGS_NODIST=${LDFLAGS_NODIST:+$LDFLAGS_NODIST }-g0"
  )
  "$make" -j"$(jobs_count)" "${release_flags[@]}" PROFILE_TASK="$profile_task"
  # Compilation stays unprivileged; only installation into /opt needs root.
  if [[ "$INSTALL_WITH_SUDO" == "1" ]]; then
    sudo "$make" "${release_flags[@]}" install
  else
    "$make" "${release_flags[@]}" install
  fi
}

verify_install() {
  local python="$PYTHON_DIR/bin/python$PYTHON_MINOR"
  [[ -x "$python" ]] || die "interpreter missing: $python"
  EXPECTED_PYTHON_VERSION="$PYTHON_VERSION" "$python" -I -c '
import os, sys, sysconfig
import ssl, sqlite3, ctypes, decimal, lzma, bz2, zlib, uuid, readline, _struct
from builtins import frozendict, sentinel
assert sys.implementation.name == "cpython" and sys.version_info[:2] == (3, 15), "CPython 3.15 required"
assert sys.version.split()[0] == os.environ["EXPECTED_PYTHON_VERSION"], "Installed patch version does not match PYTHON_VERSION"
assert not sysconfig.get_config_var("Py_DEBUG"), "A non-debug interpreter is required"
args = sysconfig.get_config_var("CONFIG_ARGS") or ""
assert "--enable-optimizations" in args and "--with-lto" in args, "PGO and LTO are required"
print("OK:", sys.version.split()[0],
      "shared" if sysconfig.get_config_var("Py_ENABLE_SHARED") else "static",
      sysconfig.get_config_var("LIBDIR"))
'
  if [[ "$STATIC" != "1" ]]; then
    # The interpreter must load this prefix's library, not the system's.
    local loaded_lib
    loaded_lib="$(ldd "$python" | awk -v lib="libpython$PYTHON_MINOR.so" \
      '($1 == lib || index($1, lib ".") == 1) && $2 == "=>" && !found { print $3; found = 1 }')"
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
  echo "profile: production release (-O3 -g0, PGO, LTO)"
  echo "debug: disabled (--without-pydebug)"
  echo "make jobs: $(jobs_count)"
  echo "pgo excludes: $PGO_EXCLUDE"
  echo "dependencies: $(print_deps)"
}

main() {
  case "${1:-help}" in
    help|-h|--help)
      cat <<'HELP'
Usage: scripts/python-install.sh [install|verify|dry-run|deps|help]

Build production CPython 3.15.2 by default, with PGO, LTO and debug disabled.
PYTHON_VERSION selects another stable 3.15 patch release.
PYTHON_INSTALL_DIR and PYTHON_SRC_DIR select installation and source paths.
CC selects the compiler; Clang requires its matching LLVM_PROFDATA for PGO.
STATIC=1 builds a static library; shared is the default for Gufi applications.
MAKE_JOBS controls build parallelism; SKIP_APT=1 skips dependency installation.
PGO_EXCLUDE selects excluded training modules; an empty value excludes none.
HELP
      return 0 ;;
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

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then main "$@"; fi
