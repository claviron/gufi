#!/usr/bin/env bash
set -euo pipefail
source "$INSTALLER"
detect_linux_family() { printf '%s\n' "${FIXTURE_FAMILY:-debian}"; }
case "$1" in
  build) build_python ;;
  dry-run) dry_run ;;
esac
