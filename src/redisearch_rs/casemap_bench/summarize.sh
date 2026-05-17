#!/usr/bin/env bash
#
# Summarize `cargo bench -p casemap_bench` results.
#
# Usage:
#   summarize.sh                       # latest run (criterion's `new/` dir),
#                                      # prints libnu-vs-ICU ratio table
#   summarize.sh BASELINE              # same table, reads BASELINE instead of `new`
#                                      # (saved via `cargo bench -- --save-baseline BASELINE`)
#   summarize.sh BASELINE_A BASELINE_B # side-by-side comparison: per-corpus libnu/icu
#                                      # times for each baseline plus the libnu speedup
#                                      # of B over A. Use for libnu version A/B testing.
#
# Reads `mean.point_estimate` (nanoseconds) from criterion's per-benchmark
# estimates.json files. Run from anywhere — paths are resolved relative to
# this script.

set -euo pipefail

# Force `.` as decimal separator regardless of locale, so the printed
# ratios are stable across machines.
export LC_NUMERIC=C

script_dir=$(cd "$(dirname "$0")" && pwd)
crit_dir="$script_dir/../../../bin/redisearch_rs/criterion"

if [[ ! -d "$crit_dir" ]]; then
  echo "criterion output not found at $crit_dir" >&2
  echo "run \`cargo bench -p casemap_bench\` first" >&2
  exit 1
fi

corpora=(ascii_lower ascii_mixed latin_mixed greek_mixed pathological)

read_ns() {
  # $1: group (fold|lower), $2: backend (libnu|icu), $3: corpus, $4: baseline dir
  local file="$crit_dir/$1/$2/$3/$4/estimates.json"
  if [[ ! -f "$file" ]]; then
    echo "missing: $file" >&2
    exit 1
  fi
  jq '.mean.point_estimate' "$file"
}

case ${#@} in
  0) baseline_a="new"; baseline_b="" ;;
  1) baseline_a="$1"; baseline_b="" ;;
  2) baseline_a="$1"; baseline_b="$2" ;;
  *) echo "usage: $0 [BASELINE_A [BASELINE_B]]" >&2; exit 2 ;;
esac

if [[ -z "$baseline_b" ]]; then
  # Single-baseline mode: per-corpus libnu vs ICU ratio.
  for group in fold lower; do
    echo "== $group ($baseline_a) =="
    for corpus in "${corpora[@]}"; do
      libnu=$(read_ns "$group" libnu "$corpus" "$baseline_a")
      icu=$(read_ns   "$group" icu   "$corpus" "$baseline_a")
      ratio=$(echo "$icu / $libnu" | bc -l)
      printf '  %-13s libnu=%7.0fns  icu=%7.0fns  ratio=%.2fx\n' \
        "$corpus" "$libnu" "$icu" "$ratio"
    done
  done
else
  # Two-baseline mode: per-corpus side-by-side, plus libnu A→B speedup.
  for group in fold lower; do
    echo "== $group ($baseline_a vs $baseline_b) =="
    printf '  %-13s  %22s  %22s  %s\n' \
      "" "$baseline_a (libnu/icu)" "$baseline_b (libnu/icu)" "libnu Δ"
    for corpus in "${corpora[@]}"; do
      libnu_a=$(read_ns "$group" libnu "$corpus" "$baseline_a")
      icu_a=$(read_ns   "$group" icu   "$corpus" "$baseline_a")
      libnu_b=$(read_ns "$group" libnu "$corpus" "$baseline_b")
      icu_b=$(read_ns   "$group" icu   "$corpus" "$baseline_b")
      # Positive % = baseline_b is faster than baseline_a on libnu.
      delta=$(echo "($libnu_a - $libnu_b) / $libnu_a * 100" | bc -l)
      printf '  %-13s  %8.0fns / %8.0fns  %8.0fns / %8.0fns  %+6.1f%%\n' \
        "$corpus" "$libnu_a" "$icu_a" "$libnu_b" "$icu_b" "$delta"
    done
  done
fi
