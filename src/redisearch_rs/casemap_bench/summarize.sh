#!/usr/bin/env bash
#
# Summarize the most recent `cargo bench -p casemap_bench` run as a
# libnu-vs-ICU ratio table. Reads `mean.point_estimate` (nanoseconds)
# from criterion's per-benchmark estimates.json files.
#
# Run from anywhere — paths are resolved relative to this script.

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

for group in fold lower; do
  echo "== $group =="
  for corpus in "${corpora[@]}"; do
    libnu=$(jq '.mean.point_estimate' "$crit_dir/$group/libnu/$corpus/new/estimates.json")
    icu=$(jq '.mean.point_estimate' "$crit_dir/$group/icu/$corpus/new/estimates.json")
    ratio=$(echo "$icu / $libnu" | bc -l)
    printf '  %-13s libnu=%7.0fns  icu=%7.0fns  ratio=%.2fx\n' \
      "$corpus" "$libnu" "$icu" "$ratio"
  done
done
