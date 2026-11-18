#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../../.."

# A PR can carry several suite labels. Validate every selector before downloading
# datasets or compiling, and let the all-suite selector subsume the others.
IFS=',' read -r -a labels <<< "${1:-benchmarks-default}"
suites=()
for label in "${labels[@]}"; do
    case "$label" in
        benchmarks-all) suites=(benchmarks-all); break ;;
        benchmarks-*|bm-*) suites+=("$label") ;;
    esac
done
if [[ ${#suites[@]} -eq 0 ]]; then
    echo "No VecSim benchmark suite selected" >&2
    exit 1
fi
for suite in "${suites[@]}"; do
    bash tests/benchmarks/vecsim/benchmarks.sh "$suite" >/dev/null
done
for suite in "${suites[@]}"; do
    make vecsim-benchmark-data BM_FILTER="$suite"
    make vecsim-benchmarks BM_FILTER="$suite"
done
