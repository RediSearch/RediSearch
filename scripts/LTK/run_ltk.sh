#!/usr/bin/env bash
#
# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  run_ltk.sh [--monitor-trace PATH] [--s3-bucket BUCKET] [--s3-prefix PREFIX] \
    [--monitor-s3-key KEY] [--import-workers COUNT] [--port PORT] [--username USER] \
    [--search-timeout-ms MILLISECONDS] [--s3-expected-bucket-owner ACCOUNT_ID] \
    SHARD_IP [SCENARIO_OPTIONS...]

Downloads and imports the LTK baseline RDB shards from S3, creates idx:acs, then
runs the LTK scenario after downloading its monitor trace from S3. A local trace
can be supplied with --monitor-trace. AWS credentials use the standard
environment or instance-role chain. Redis authentication is read from
REDIS_PASSWORD or REDISCLI_AUTH. Arguments after SHARD_IP are passed only to
run_ltk_scenario.py.

Example:
  scripts/LTK/run_ltk.sh --port 7000 10.0.1.10 --replay-duration 600
EOF
}

port=7000
username=${REDIS_USERNAME:-}
monitor_trace=
s3_bucket=rl-automation
s3_prefix=rlauto/search-customer-scenarios/ltk/rdbs-without-index
monitor_s3_key=rlauto/search-customer-scenarios/ltk/monitor.txt
import_workers=4
search_timeout_ms=2000
s3_expected_bucket_owner=620187402834

while (($# > 0)); do
  case "$1" in
    --port)
      (($# >= 2)) || { echo "error: --port requires a value" >&2; exit 2; }
      port=$2
      shift 2
      ;;
    --username)
      (($# >= 2)) || { echo "error: --username requires a value" >&2; exit 2; }
      username=$2
      shift 2
      ;;
    --tls)
      echo "error: run_ltk.sh cannot import the local temporary Redis source over TLS" >&2
      exit 2
      ;;
    --monitor-trace)
      (($# >= 2)) || { echo "error: --monitor-trace requires a path" >&2; exit 2; }
      monitor_trace=$2
      shift 2
      ;;
    --s3-bucket)
      (($# >= 2)) || { echo "error: --s3-bucket requires a value" >&2; exit 2; }
      s3_bucket=$2
      shift 2
      ;;
    --s3-prefix)
      (($# >= 2)) || { echo "error: --s3-prefix requires a value" >&2; exit 2; }
      s3_prefix=$2
      shift 2
      ;;
    --monitor-s3-key)
      (($# >= 2)) || { echo "error: --monitor-s3-key requires a value" >&2; exit 2; }
      monitor_s3_key=$2
      shift 2
      ;;
    --import-workers)
      (($# >= 2)) || { echo "error: --import-workers requires a value" >&2; exit 2; }
      import_workers=$2
      shift 2
      ;;
    --search-timeout-ms)
      (($# >= 2)) || { echo "error: --search-timeout-ms requires a value" >&2; exit 2; }
      search_timeout_ms=$2
      shift 2
      ;;
    --s3-expected-bucket-owner)
      (($# >= 2)) || { echo "error: --s3-expected-bucket-owner requires a value" >&2; exit 2; }
      s3_expected_bucket_owner=$2
      shift 2
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      break
      ;;
  esac
done

(($# >= 1)) || { usage >&2; exit 2; }
[[ -z "$monitor_trace" || -f "$monitor_trace" ]] || {
  echo "error: monitor trace does not exist: $monitor_trace" >&2
  exit 2
}
shard_ip=$1
shift

scenario_args=()
while (($# > 0)); do
  case "$1" in
    --search-timeout-ms)
      (($# >= 2)) || { echo "error: --search-timeout-ms requires a value" >&2; exit 2; }
      search_timeout_ms=$2
      shift 2
      ;;
    --search-timeout-ms=*)
      search_timeout_ms=${1#*=}
      shift
      ;;
    --s3-expected-bucket-owner)
      (($# >= 2)) || { echo "error: --s3-expected-bucket-owner requires a value" >&2; exit 2; }
      s3_expected_bucket_owner=$2
      shift 2
      ;;
    --s3-expected-bucket-owner=*)
      s3_expected_bucket_owner=${1#*=}
      shift
      ;;
    *)
      scenario_args+=("$1")
      shift
      ;;
  esac
done

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
python=${PYTHON:-python3}
common_args=(--port "$port")
[[ -z "$username" ]] || common_args+=(--username "$username")
if [[ -n "$monitor_trace" ]]; then
  monitor_args=(--monitor-trace "$monitor_trace")
else
  monitor_args=(--s3-bucket "$s3_bucket" --s3-key "$monitor_s3_key")
fi
s3_owner_args=(--s3-expected-bucket-owner "$s3_expected_bucket_owner")

echo "Configuring the cluster and importing the LTK baseline RDBs..."
"$python" "$script_dir/create_ltk_db.py" "${common_args[@]}" \
  --s3-bucket "$s3_bucket" --s3-prefix "$s3_prefix" \
  "${s3_owner_args[@]}" --workers "$import_workers" \
  --search-timeout-ms "$search_timeout_ms" "$shard_ip"

echo "Creating the LTK index..."
"$python" "$script_dir/create_acs_index.py" "${common_args[@]}" "$shard_ip"

echo "Running the LTK scenario..."
"$python" "$script_dir/run_ltk_scenario.py" "${common_args[@]}" \
  "${monitor_args[@]}" "${s3_owner_args[@]}" --search-timeout-ms "$search_timeout_ms" \
  "$shard_ip" "${scenario_args[@]}"
