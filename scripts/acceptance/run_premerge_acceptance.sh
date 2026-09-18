#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
build_dir=${BWB_ACCEPTANCE_BUILD_DIR:-/tmp/bwb-scheduler-acceptance-build}
mkdir -p -- "$build_dir"

docker run --rm \
  -u "$(id -u):$(id -g)" \
  -e HOME=/tmp -e GOCACHE=/tmp/gocache -e GOMODCACHE=/tmp/gomodcache \
  -v "$repo_root:/src:ro" -v "$build_dir:/build" -w /src \
  golang:1.24.4 \
  /usr/local/go/bin/go build -trimpath -o /build/bwbScheduler .

exec python3 "$repo_root/scripts/acceptance/run_premerge_acceptance.py" \
  --binary "$build_dir/bwbScheduler" "$@"

