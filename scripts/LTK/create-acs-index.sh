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
Create the LTK idx:acs index on a Redis Cluster.

Usage:
  create-acs-index.sh [options] SHARD_IP

Options:
  --port PORT        Shard port (default: 7000)
  --redis-cli PATH   redis-cli command or path (default: redis-cli)
  -h, --help         Show this help

The target cluster must have the RedisJSON and RediSearch modules loaded. The
script creates a HASH index with prefix A# and verifies it with FT.INFO.
REDISCLI_AUTH can provide a password.

Example:
  scripts/LTK/create-acs-index.sh --port 7000 10.0.1.10
EOF
}

die() {
  echo "error: $*" >&2
  exit 1
}

is_ipv4() {
  local ip=$1 octet
  local -a octets

  IFS=. read -r -a octets <<<"$ip"
  [[ ${#octets[@]} -eq 4 ]] || return 1
  for octet in "${octets[@]}"; do
    [[ "$octet" =~ ^[0-9]{1,3}$ ]] || return 1
    ((10#$octet <= 255)) || return 1
  done
}

port=7000
redis_cli=redis-cli

while (($# > 0)); do
  case "$1" in
    --port)
      (($# >= 2)) || die "--port requires a value"
      port=$2
      shift 2
      ;;
    --redis-cli)
      (($# >= 2)) || die "--redis-cli requires a value"
      redis_cli=$2
      shift 2
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    --)
      shift
      break
      ;;
    -*)
      die "unknown option: $1"
      ;;
    *)
      break
      ;;
  esac
done

(($# == 1)) || {
  usage >&2
  exit 2
}

shard_ip=$1
is_ipv4 "$shard_ip" || die "SHARD_IP is not a valid IPv4 address: $shard_ip"
if [[ ! "$port" =~ ^[0-9]+$ ]] || ((port < 1 || port > 65535)); then
  die "--port must be between 1 and 65535"
fi

if [[ "$redis_cli" == */* ]]; then
  [[ -x "$redis_cli" ]] || die "redis-cli is not executable: $redis_cli"
else
  command -v "$redis_cli" >/dev/null || die "redis-cli is not installed: $redis_cli"
fi

redis() {
  "$redis_cli" -c -h "$shard_ip" -p "$port" --raw "$@"
}

echo "Checking Redis Cluster shard $shard_ip:$port..."
[[ "$(redis PING)" == PONG ]] || die "shard did not respond to PING"

cluster_info=$(redis CLUSTER INFO | tr -d '\r')
grep -qx 'cluster_state:ok' <<<"$cluster_info" || die "cluster_state is not ok"

module_list=$(redis MODULE LIST)
grep -Fqxi ReJSON <<<"$module_list" || die "RedisJSON is not loaded on the shard"
grep -Fqxi search <<<"$module_list" || die "RediSearch is not loaded on the shard"

ft_create_idx=(
  FT.CREATE idx:acs ON HASH PREFIX 1 'A#' SCHEMA
  profile_id TAG SEPARATOR ',' SORTABLE UNF
  post_id TAG SEPARATOR ',' SORTABLE UNF
  product_id TAG SEPARATOR ',' SORTABLE UNF
  type TAG SEPARATOR ',' SORTABLE UNF
  iscore NUMERIC SORTABLE UNF
  iscore2 NUMERIC SORTABLE UNF
  iscore3 NUMERIC SORTABLE UNF
  updated_at NUMERIC SORTABLE UNF
  profile_display_name TAG SEPARATOR ',' SORTABLE UNF
  profile_display_name AS profile_display_name_txt TEXT WEIGHT 5 WITHSUFFIXTRIE
  profile_display_name_text TEXT WEIGHT 5 WITHSUFFIXTRIE
  country_code TAG SEPARATOR ',' SORTABLE UNF
  country_region TAG SEPARATOR ',' SORTABLE UNF
  country_state TAG SEPARATOR ',' SORTABLE UNF
  region TAG SEPARATOR ',' SORTABLE UNF
  lvl0s TAG SEPARATOR '|' SORTABLE UNF
  lvl0s AS lvl0s_txt TEXT WEIGHT 1 WITHSUFFIXTRIE
  lvl1s TAG SEPARATOR '|' SORTABLE UNF
  lvl1s AS lvl1s_txt TEXT WEIGHT 1 WITHSUFFIXTRIE
  lvl2s TAG SEPARATOR '|' SORTABLE UNF
  lvl2s AS lvl2s_txt TEXT WEIGHT 1 WITHSUFFIXTRIE
  brands TAG SEPARATOR '|' SORTABLE UNF
  brands AS brands_txt TEXT WEIGHT 1 NOSTEM WITHSUFFIXTRIE
  retailer_names TAG SEPARATOR '|' SORTABLE UNF
  retailer_names AS retailer_names_txt TEXT WEIGHT 1 NOSTEM WITHSUFFIXTRIE
  searchable TAG SEPARATOR ',' SORTABLE UNF
  segment TAG SEPARATOR ',' SORTABLE UNF
  has_mentions_enabled TAG SEPARATOR ',' SORTABLE UNF
  full_name TEXT WEIGHT 10 NOSTEM WITHSUFFIXTRIE
  alternate_names TEXT WEIGHT 5 NOSTEM WITHSUFFIXTRIE
  bio TEXT WEIGHT 1 WITHSUFFIXTRIE
  created_at NUMERIC SORTABLE UNF
  media_type TAG SEPARATOR ',' SORTABLE UNF
  post_type TAG SEPARATOR ',' SORTABLE UNF
  styled TAG SEPARATOR ',' SORTABLE UNF
  storytelling TAG SEPARATOR ',' SORTABLE UNF
  post_tags TAG SEPARATOR '|' SORTABLE UNF
  post_tags AS post_tags_txt TEXT WEIGHT 1 WITHSUFFIXTRIE
  retailer_ids TAG SEPARATOR '|' SORTABLE UNF
  caption TEXT WEIGHT 1 WITHSUFFIXTRIE
  has_products TAG SEPARATOR ',' SORTABLE UNF
  product_ids TAG SEPARATOR '|' SORTABLE UNF NOINDEX
  name TEXT WEIGHT 3 WITHSUFFIXTRIE
  description TEXT WEIGHT 1 WITHSUFFIXTRIE
  retailer_id TAG SEPARATOR ',' SORTABLE UNF
  price NUMERIC SORTABLE UNF
  currency TAG SEPARATOR ',' SORTABLE UNF
  available TAG SEPARATOR ',' SORTABLE UNF
  rp_id TAG SEPARATOR ',' SORTABLE UNF
  posts_index_name TAG SEPARATOR ',' SORTABLE UNF
  product_reference_id TAG SEPARATOR ',' SORTABLE UNF
  text_vector16 VECTOR HNSW 10 TYPE FLOAT16 DIM 1024 DISTANCE_METRIC COSINE
    M 16 EF_CONSTRUCTION 200
  multimodal_vector16 VECTOR HNSW 10 TYPE FLOAT16 DIM 1536 DISTANCE_METRIC COSINE
    M 16 EF_CONSTRUCTION 200
)

echo "Creating idx:acs..."
create_result=$(redis "${ft_create_idx[@]}")
[[ "$create_result" == OK ]] || die "FT.CREATE failed: $create_result"

echo "Inspecting idx:acs..."
index_info=$(redis FT.INFO idx:acs)
grep -Fqx idx:acs <<<"$index_info" || die "FT.INFO did not return idx:acs"

echo "idx:acs is ready on $shard_ip:$port"
