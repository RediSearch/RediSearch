#!/usr/bin/env bash
#
# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

set -Eeuo pipefail

usage() {
  cat <<'EOF'
Start a replicated Redis OSS Cluster across three remote hosts.

Usage:
  start-redis-oss-cluster.sh [options] HOST1_IP HOST2_IP HOST3_IP REDIS_VERSION

Arguments:
  HOST1_IP..HOST3_IP  IPv4 addresses used for SSH and advertised by Redis.
  REDIS_VERSION       Exact Redis version expected on all hosts (for example: 8.2.1).

Options:
  --ssh-user USER         SSH user (default: current local user)
  --identity-file PATH    SSH private key
  --base-port PORT        First Redis port on each host (default: 7000)
  --remote-dir PATH       Remote state/log directory (default: unique path under /tmp)
  --redis-server PATH     Remote redis-server command or path (default: redis-server)
  --redis-cli PATH        Remote redis-cli command or path (default: redis-cli)
  --json-module PATH      Remote absolute path to rejson.so (required)
  --search-module PATH    Remote absolute path to redisearch.so (required)
  --shards COUNT          Number of primary shards (default: 20)
  --db-memory-gb GB       Logical primary capacity in GiB (default: 400)
  --startup-timeout SEC   Seconds to wait for startup and cluster health (default: 60)
  --keep-on-failure       Do not stop Redis processes if setup fails
  --dry-run               Validate arguments and print the allocation only
  -h, --help              Show this help

Example:
  scripts/start-redis-oss-cluster.sh --ssh-user ec2-user \
    --identity-file ~/.ssh/test.pem \
    --json-module /opt/redis/lib/rejson.so \
    --search-module /opt/redis/lib/redisearch.so \
    10.0.1.10 10.0.2.10 10.0.3.10 8.2.1

The three hosts must be able to reach each other at the supplied IPs. Their security
groups must allow the Redis ports and the corresponding cluster-bus ports (port +
10000) between the hosts. Keep these ports off the public internet. Logical database
capacity is divided evenly across primary shards; replicas have the same maxmemory,
so replication doubles the physical memory capacity required by Redis.
EOF
}

die() {
  echo "error: $*" >&2
  exit 1
}

is_uint() {
  [[ "$1" =~ ^[0-9]+$ ]]
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

# ssh joins remote arguments into a shell command, so values passed to the remote
# shell must be restricted even though they are stored in local Bash arrays.
is_safe_remote_value() {
  [[ "$1" =~ ^[a-zA-Z0-9_./:+-]+$ ]]
}

ssh_user=${USER:-}
identity_file=
base_port=7000
remote_dir=
redis_server=redis-server
redis_cli=redis-cli
json_module=
search_module=
shards=20
db_memory_gb=400
startup_timeout=60
keep_on_failure=0
dry_run=0

while (($# > 0)); do
  case "$1" in
    --ssh-user)
      (($# >= 2)) || die "--ssh-user requires a value"
      ssh_user=$2
      shift 2
      ;;
    --identity-file)
      (($# >= 2)) || die "--identity-file requires a value"
      identity_file=$2
      shift 2
      ;;
    --base-port)
      (($# >= 2)) || die "--base-port requires a value"
      base_port=$2
      shift 2
      ;;
    --remote-dir)
      (($# >= 2)) || die "--remote-dir requires a value"
      remote_dir=$2
      shift 2
      ;;
    --redis-server)
      (($# >= 2)) || die "--redis-server requires a value"
      redis_server=$2
      shift 2
      ;;
    --redis-cli)
      (($# >= 2)) || die "--redis-cli requires a value"
      redis_cli=$2
      shift 2
      ;;
    --json-module)
      (($# >= 2)) || die "--json-module requires a value"
      json_module=$2
      shift 2
      ;;
    --search-module)
      (($# >= 2)) || die "--search-module requires a value"
      search_module=$2
      shift 2
      ;;
    --shards)
      (($# >= 2)) || die "--shards requires a value"
      shards=$2
      shift 2
      ;;
    --db-memory-gb)
      (($# >= 2)) || die "--db-memory-gb requires a value"
      db_memory_gb=$2
      shift 2
      ;;
    --startup-timeout)
      (($# >= 2)) || die "--startup-timeout requires a value"
      startup_timeout=$2
      shift 2
      ;;
    --keep-on-failure)
      keep_on_failure=1
      shift
      ;;
    --dry-run)
      dry_run=1
      shift
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

(($# == 4)) || {
  usage >&2
  exit 2
}

host1=$1
host2=$2
host3=$3
redis_version=$4

is_ipv4 "$host1" || die "HOST1_IP is not a valid IPv4 address: $host1"
is_ipv4 "$host2" || die "HOST2_IP is not a valid IPv4 address: $host2"
is_ipv4 "$host3" || die "HOST3_IP is not a valid IPv4 address: $host3"
[[ "$host1" != "$host2" && "$host1" != "$host3" && "$host2" != "$host3" ]] ||
  die "the three host IPs must be different"
if ! is_uint "$shards" || ((shards < 3)); then
  die "--shards must be an integer of at least 3"
fi
if ! is_uint "$base_port" || ((base_port < 1 || base_port > 55535)); then
  die "--base-port must be between 1 and 55535"
fi
if ! is_uint "$startup_timeout" || ((startup_timeout < 1)); then
  die "--startup-timeout must be a positive integer"
fi
if ! is_uint "$db_memory_gb" || ((db_memory_gb < 1)); then
  die "--db-memory-gb must be a positive integer"
fi
((db_memory_gb % shards == 0)) ||
  die "--db-memory-gb must be evenly divisible by --shards"
[[ "$ssh_user" =~ ^[a-zA-Z0-9_.-]+$ ]] || die "invalid SSH user: $ssh_user"
is_safe_remote_value "$redis_server" || die "unsafe --redis-server value: $redis_server"
is_safe_remote_value "$redis_cli" || die "unsafe --redis-cli value: $redis_cli"
[[ -n "$json_module" ]] || die "--json-module is required"
[[ -n "$search_module" ]] || die "--search-module is required"
[[ "$json_module" == /* ]] || die "--json-module must be an absolute remote path"
[[ "$search_module" == /* ]] || die "--search-module must be an absolute remote path"
is_safe_remote_value "$json_module" || die "unsafe --json-module value: $json_module"
is_safe_remote_value "$search_module" || die "unsafe --search-module value: $search_module"
[[ "$redis_version" =~ ^[0-9]+\.[0-9]+(\.[0-9]+)?([.-][a-zA-Z0-9]+)*$ ]] ||
  die "REDIS_VERSION is invalid: $redis_version"

host1_primaries=$(((shards + 2) / 3))
host2_primaries=$(((shards + 1) / 3))
host3_primaries=$((shards / 3))
host1_nodes=$((host1_primaries + host3_primaries))
host2_nodes=$((host2_primaries + host1_primaries))
host3_nodes=$((host3_primaries + host2_primaries))
total_nodes=$((shards * 2))
memory_per_node_gb=$((db_memory_gb / shards))
physical_memory_gb=$((memory_per_node_gb * total_nodes))
max_nodes_per_host=$host1_nodes
((host2_nodes > max_nodes_per_host)) && max_nodes_per_host=$host2_nodes
((host3_nodes > max_nodes_per_host)) && max_nodes_per_host=$host3_nodes
highest_port=$((base_port + max_nodes_per_host - 1))
((highest_port + 10000 <= 65535)) ||
  die "shard and cluster-bus ports exceed 65535; choose a lower --base-port"

if [[ -z "$remote_dir" ]]; then
  remote_dir="/tmp/redis-oss-cluster-${base_port}-${shards}-$(date +%Y%m%d%H%M%S)-$$"
fi
[[ "$remote_dir" == /* ]] || die "--remote-dir must be an absolute path"
is_safe_remote_value "$remote_dir" || die "unsafe --remote-dir value: $remote_dir"

if [[ -n "$identity_file" && ! -f "$identity_file" ]]; then
  die "identity file does not exist: $identity_file"
fi

declare -a primary_endpoints=()
declare -a replica_endpoints=()
for ((i = 0; i < host1_primaries; i++)); do
  primary_endpoints+=("$host1:$((base_port + i))")
done
for ((i = 0; i < host2_primaries; i++)); do
  primary_endpoints+=("$host2:$((base_port + i))")
done
for ((i = 0; i < host3_primaries; i++)); do
  primary_endpoints+=("$host3:$((base_port + i))")
done
for ((i = 0; i < host1_primaries; i++)); do
  replica_endpoints+=("$host2:$((base_port + host2_primaries + i))")
done
for ((i = 0; i < host2_primaries; i++)); do
  replica_endpoints+=("$host3:$((base_port + host3_primaries + i))")
done
for ((i = 0; i < host3_primaries; i++)); do
  replica_endpoints+=("$host1:$((base_port + host1_primaries + i))")
done
declare -a endpoints=("${primary_endpoints[@]}" "${replica_endpoints[@]}")

echo "Redis Cluster allocation ($shards primary shards, $shards replicas, $total_nodes nodes):"
echo "  Logical database memory: ${db_memory_gb} GiB"
echo "  Maxmemory per primary/replica: ${memory_per_node_gb} GiB"
echo "  Physical Redis capacity including replicas: ${physical_memory_gb} GiB"
echo "  Redis version: $redis_version"
echo "  RedisJSON module: $json_module"
echo "  RediSearch module: $search_module"
echo "  $host1: $host1_nodes nodes, ports $base_port-$((base_port + host1_nodes - 1))"
echo "  $host2: $host2_nodes nodes, ports $base_port-$((base_port + host2_nodes - 1))"
echo "  $host3: $host3_nodes nodes, ports $base_port-$((base_port + host3_nodes - 1))"
echo "  Planned primaries: $host1_primaries on $host1, $host2_primaries on $host2," \
  "$host3_primaries on $host3"
echo "  Remote state: $remote_dir"
echo "  Cluster-bus ports: $((base_port + 10000))-$((highest_port + 10000))"
echo >&2
echo "WARNING: Restrict Redis and cluster-bus ports to trusted security groups;" >&2
echo "         Redis Cluster bus traffic is not authenticated or encrypted." >&2

if ((dry_run)); then
  echo "Primary endpoints:"
  printf '%s\n' "${primary_endpoints[@]}"
  echo "Replica endpoints:"
  printf '%s\n' "${replica_endpoints[@]}"
  exit 0
fi

command -v ssh >/dev/null || die "ssh is not installed locally"

declare -a ssh_options=(-o BatchMode=yes -o ConnectTimeout=10)
if [[ -n "$identity_file" ]]; then
  ssh_options+=(-i "$identity_file")
fi

host1_destination="$ssh_user@$host1"
host2_destination="$ssh_user@$host2"
host3_destination="$ssh_user@$host3"
declare -a started_destinations=()

cleanup() {
  local status=$?
  trap - ERR INT TERM

  if ((status != 0 && !keep_on_failure && ${#started_destinations[@]} > 0)); then
    echo "Setup failed; stopping Redis processes started by this run..." >&2
    for destination in "${started_destinations[@]}"; do
      ssh "${ssh_options[@]}" "$destination" bash -s -- "$remote_dir" <<'REMOTE_CLEANUP' || true
set -u
remote_dir=$1
for pidfile in "$remote_dir"/node-*/redis.pid; do
  [[ -f "$pidfile" ]] || continue
  pid=$(<"$pidfile")
  [[ "$pid" =~ ^[0-9]+$ ]] || continue
  command_name=$(ps -p "$pid" -o comm= 2>/dev/null || true)
  if [[ "$command_name" == *redis-server* ]]; then
    kill "$pid" 2>/dev/null || true
  fi
done
REMOTE_CLEANUP
    done
    echo "Remote logs and configuration were preserved in $remote_dir" >&2
  fi
  exit "$status"
}
trap cleanup ERR INT TERM

preflight_host() {
  local destination=$1 count=$2

  echo "Preflight: $destination"
  ssh "${ssh_options[@]}" "$destination" bash -s -- \
    "$base_port" "$count" "$remote_dir" "$redis_server" "$redis_cli" "$redis_version" \
    "$json_module" "$search_module" "$memory_per_node_gb" <<'REMOTE_PREFLIGHT'
set -euo pipefail
base_port=$1
count=$2
remote_dir=$3
redis_server=$4
redis_cli=$5
expected_redis_version=$6
json_module=$7
search_module=$8
memory_per_node_gb=$9

resolve_command() {
  if [[ "$1" == */* ]]; then
    [[ -x "$1" ]] || { echo "not executable: $1" >&2; exit 1; }
  else
    command -v "$1" >/dev/null || { echo "command not found: $1" >&2; exit 1; }
  fi
}

resolve_command "$redis_server"
resolve_command "$redis_cli"
[[ -r "$json_module" ]] || {
  echo "RedisJSON module is not readable: $json_module" >&2
  exit 1
}
[[ -r "$search_module" ]] || {
  echo "RediSearch module is not readable: $search_module" >&2
  exit 1
}

version_output=$("$redis_server" --version)
installed_redis_version=$(sed -n 's/.* v=\([^ ]*\).*/\1/p' <<<"$version_output")
[[ -n "$installed_redis_version" ]] || {
  echo "could not parse Redis version from: $version_output" >&2
  exit 1
}
[[ "$installed_redis_version" == "$expected_redis_version" ]] || {
  echo "Redis version mismatch: expected $expected_redis_version, found $installed_redis_version" >&2
  exit 1
}

if [[ -r /proc/meminfo ]]; then
  total_memory_kib=$(awk '$1 == "MemTotal:" { print $2 }' /proc/meminfo)
  required_memory_kib=$((count * memory_per_node_gb * 1024 * 1024))
  ((total_memory_kib >= required_memory_kib)) || {
    echo "insufficient RAM: $count nodes at ${memory_per_node_gb} GiB each require at least" \
      "$((required_memory_kib / 1024 / 1024)) GiB, host has $((total_memory_kib / 1024 / 1024)) GiB" >&2
    exit 1
  }
fi
[[ ! -e "$remote_dir" ]] || {
  echo "remote directory already exists: $remote_dir (choose another --remote-dir)" >&2
  exit 1
}

if command -v ss >/dev/null; then
  for ((i = 0; i < count; i++)); do
    port=$((base_port + i))
    if ss -ltnH "sport = :$port" | grep -q .; then
      echo "port $port is already in use" >&2
      exit 1
    fi
    bus_port=$((port + 10000))
    if ss -ltnH "sport = :$bus_port" | grep -q .; then
      echo "cluster-bus port $bus_port is already in use" >&2
      exit 1
    fi
  done
fi
REMOTE_PREFLIGHT
}

start_host() {
  local destination=$1 announce_ip=$2 count=$3

  echo "Starting $count shards on $destination"
  ssh "${ssh_options[@]}" "$destination" bash -s -- \
    "$announce_ip" "$base_port" "$count" "$remote_dir" "$redis_server" "$redis_cli" \
    "$startup_timeout" "$json_module" "$search_module" "$memory_per_node_gb" <<'REMOTE_START'
set -euo pipefail
announce_ip=$1
base_port=$2
count=$3
remote_dir=$4
redis_server=$5
redis_cli=$6
startup_timeout=$7
json_module=$8
search_module=$9
memory_per_node_gb=${10}

mkdir -p -m 700 "$remote_dir"
for ((i = 0; i < count; i++)); do
  port=$((base_port + i))
  node_dir="$remote_dir/node-$port"
  mkdir -m 700 "$node_dir"
  "$redis_server" \
    --port "$port" \
    --bind 0.0.0.0 \
    --protected-mode no \
    --daemonize yes \
    --dir "$node_dir" \
    --dbfilename dump.rdb \
    --appendonly no \
    --maxmemory "${memory_per_node_gb}gb" \
    --maxmemory-policy noeviction \
    --cluster-enabled yes \
    --cluster-config-file nodes.conf \
    --cluster-node-timeout 5000 \
    --cluster-announce-ip "$announce_ip" \
    --cluster-announce-port "$port" \
    --cluster-announce-bus-port "$((port + 10000))" \
    --loadmodule "$json_module" \
    --loadmodule "$search_module" \
    --pidfile "$node_dir/redis.pid" \
    --logfile "$node_dir/redis.log"
done

deadline=$((SECONDS + startup_timeout))
for ((i = 0; i < count; i++)); do
  port=$((base_port + i))
  pidfile="$remote_dir/node-$port/redis.pid"
  until [[ -f "$pidfile" ]] && "$redis_cli" -h 127.0.0.1 -p "$port" --raw PING 2>/dev/null |
    grep -qx PONG; do
    if ((SECONDS >= deadline)); then
      echo "shard on port $port did not become ready; log follows:" >&2
      tail -50 "$remote_dir/node-$port/redis.log" >&2 || true
      exit 1
    fi
    sleep 0.2
  done

  module_list=$("$redis_cli" -h 127.0.0.1 -p "$port" --raw MODULE LIST)
  if ! grep -Fqxi ReJSON <<<"$module_list"; then
    echo "shard on port $port did not load RedisJSON" >&2
    exit 1
  fi
  if ! grep -Fqxi search <<<"$module_list"; then
    echo "shard on port $port did not load RediSearch" >&2
    exit 1
  fi
  configured_maxmemory=$("$redis_cli" -h 127.0.0.1 -p "$port" --raw CONFIG GET maxmemory |
    tail -1)
  expected_maxmemory=$((memory_per_node_gb * 1024 * 1024 * 1024))
  if [[ "$configured_maxmemory" != "$expected_maxmemory" ]]; then
    echo "shard on port $port has maxmemory $configured_maxmemory, expected $expected_maxmemory" >&2
    exit 1
  fi
done
REMOTE_START
}

preflight_host "$host1_destination" "$host1_nodes"
preflight_host "$host2_destination" "$host2_nodes"
preflight_host "$host3_destination" "$host3_nodes"

started_destinations+=("$host1_destination")
start_host "$host1_destination" "$host1" "$host1_nodes"
started_destinations+=("$host2_destination")
start_host "$host2_destination" "$host2" "$host2_nodes"
started_destinations+=("$host3_destination")
start_host "$host3_destination" "$host3" "$host3_nodes"

echo "Creating cluster and assigning all 16384 slots..."
ssh "${ssh_options[@]}" "$host1_destination" bash -s -- \
  "$redis_cli" "${endpoints[@]}" <<'REMOTE_CREATE'
set -euo pipefail
redis_cli=$1
shift
"$redis_cli" --cluster create "$@" --cluster-replicas 1 --cluster-yes
REMOTE_CREATE

echo "Waiting for cluster health..."
ssh "${ssh_options[@]}" "$host1_destination" bash -s -- \
  "$redis_cli" "$shards" "$total_nodes" "$startup_timeout" "${endpoints[@]}" <<'REMOTE_VERIFY'
set -euo pipefail
redis_cli=$1
expected_shards=$2
expected_nodes=$3
startup_timeout=$4
shift 4
endpoints=("$@")
first_host=${endpoints[0]%:*}
first_port=${endpoints[0]##*:}
deadline=$((SECONDS + startup_timeout))

while ((SECONDS < deadline)); do
  all_ping=1
  for endpoint in "${endpoints[@]}"; do
    host=${endpoint%:*}
    port=${endpoint##*:}
    if ! "$redis_cli" -h "$host" -p "$port" --raw PING 2>/dev/null | grep -qx PONG; then
      all_ping=0
      break
    fi
  done

  info=$("$redis_cli" -h "$first_host" -p "$first_port" --raw CLUSTER INFO 2>/dev/null |
    tr -d '\r' || true)
  state=$(awk -F: '$1 == "cluster_state" { print $2 }' <<<"$info")
  assigned=$(awk -F: '$1 == "cluster_slots_assigned" { print $2 }' <<<"$info")
  slots_ok=$(awk -F: '$1 == "cluster_slots_ok" { print $2 }' <<<"$info")
  known_nodes=$(awk -F: '$1 == "cluster_known_nodes" { print $2 }' <<<"$info")

  if ((all_ping)) && [[ "$state" == ok && "$assigned" == 16384 && "$slots_ok" == 16384 &&
    "$known_nodes" == "$expected_nodes" ]]; then
    nodes=$("$redis_cli" -h "$first_host" -p "$first_port" --raw CLUSTER NODES | tr -d '\r')
    if awk -v expected="$expected_shards" '
      function node_ip(address, bus, parts) {
        split(address, bus, "@");
        split(bus[1], parts, ":");
        return parts[1];
      }
      {
        ips[$1] = node_ip($2);
        healthy = $3 !~ /(^|,)(fail|fail\?)(,|$)/ && $8 == "connected";
        if (healthy && $3 ~ /(^|,)master(,|$)/) {
          masters[$1] = 1;
          master_count++;
        } else if (healthy && $3 ~ /(^|,)(slave|replica)(,|$)/) {
          replica_master[$1] = $4;
          replica_count++;
        }
      }
      END {
        if (master_count != expected || replica_count != expected) exit 1;
        for (replica in replica_master) {
          master = replica_master[replica];
          if (!(master in masters) || ips[replica] == ips[master]) exit 1;
          replicas_per_master[master]++;
        }
        for (master in masters) {
          if (replicas_per_master[master] != 1) exit 1;
        }
      }' <<<"$nodes"; then
      exit 0
    fi
  fi
  sleep 1
done

echo "cluster did not become healthy within ${startup_timeout}s" >&2
"$redis_cli" -h "$first_host" -p "$first_port" --raw CLUSTER INFO >&2 || true
"$redis_cli" -h "$first_host" -p "$first_port" --raw CLUSTER NODES >&2 || true
exit 1
REMOTE_VERIFY

trap - ERR INT TERM

echo
echo "Cluster is healthy. All primary and replica node endpoints:"
printf '%s\n' "${endpoints[@]}"
