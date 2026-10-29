#!/usr/bin/env bash
set -euo pipefail
name="$(basename "$0")"
printf '%s\0' "$name" "${CFLAGS:-}" "${CPPFLAGS:-}" "${LDFLAGS:-}" "${LLVM_PROFDATA:-}" "$@" >> "$CHECK_LOG"
printf '\n' >> "$CHECK_LOG"
case "$name" in
  uname) printf '%s\n' "${FIXTURE_OS:-Linux}" ;;
  cc)
    case "$1" in
      --version) printf '%s\n' "${FIXTURE_COMPILER:-gcc fixture}" ;;
      -print-prog-name=llvm-profdata) printf '%s\n' "$FIXTURE_PROFDATA" ;;
    esac
    ;;
  configure) touch Makefile ;;
esac
