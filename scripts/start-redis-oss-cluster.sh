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
Start a replicated Redis OSS Cluster across one to ten remote hosts.

Usage:
  start-redis-oss-cluster.sh [options] HOST_IP... REDIS_VERSION

Arguments:
  HOST_IP              One to ten IPv4 addresses used for SSH.
  REDIS_VERSION        Exact Redis version expected on all hosts (for example: 8.2.1).

Options:
  --ssh-user USER         SSH user (default: current local user)
  --identity-file PATH    SSH private key
  --cluster-ip IP          Private IPv4 address advertised by Redis; repeat once per
                           host, in the same order (default: auto-detect on each host)
  --base-port PORT        First Redis port on each host (default: 7000)
  --remote-dir PATH       Remote state/log directory (default: unique path under /tmp)
  --redis-server PATH     Remote redis-server command or path (default: redis-server)
  --redis-cli PATH        Remote redis-cli command or path (default: redis-cli)
  --json-module PATH      Remote absolute path to rejson.so (required)
  --search-module PATH    Remote absolute path to redisearch.so (required)
  --search-module-args ARGS
                           Space-separated arguments passed to redisearch.so
  --install               Install the exact Redis 8+ distribution on all hosts
  --shards COUNT          Number of primary shards (default: 20)
  --db-memory-gb GB       Logical primary capacity in GiB (default: 400)
  --startup-timeout SEC   Seconds to wait for startup and cluster health (default: 60)
  --keep-on-failure       Do not stop Redis processes if setup fails
  --dry-run               Validate arguments and print the allocation only
  -h, --help              Show this help

Example:
  scripts/start-redis-oss-cluster.sh --ssh-user ec2-user \
    --identity-file ~/.ssh/test.pem \
    --search-module-args "WORKERS 12 TIMEOUT 2000 ON_TIMEOUT fail CURSOR_MAX_IDLE 60000 WORKERS_PRIORITY_BIAS_THRESHOLD 6 ENABLE_UNSTABLE_FEATURES TRUE BM25STD_TANH_FACTOR 200 DEFAULT_DIALECT 2 INDEX_CURSOR_LIMIT 256" \
    --install \
    10.0.1.10 10.0.2.10 10.0.3.10 10.0.4.10 8.2.1

Without --install, provide existing module paths:
  scripts/start-redis-oss-cluster.sh --ssh-user ec2-user \
    --json-module /opt/redis/lib/rejson.so \
    --search-module /opt/redis/lib/redisearch.so \
    10.0.1.10 10.0.2.10 10.0.3.10 8.2.1

The hosts must be able to reach each other at the cluster IPs. By default, the script
detects a private IPv4 address on each host; use --cluster-ip to override it. A dry run
cannot perform detection and previews the SSH addresses unless --cluster-ip is supplied.
Security groups must allow the Redis ports and corresponding cluster-bus ports (port +
10000) between the hosts. The script checks every required path before creating the
cluster. Keep these ports off the public internet. Logical database capacity is divided
evenly across primary shards; replicas have the same maxmemory, so replication doubles
the physical memory capacity required by Redis. With one host, replicas do not protect
against host failure. --install changes system packages and requires dedicated hosts
with root or passwordless sudo.
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

is_private_ipv4() {
  local ip=$1 second_octet

  is_ipv4 "$ip" || return 1
  case "$ip" in
    10.* | 192.168.*)
      return 0
      ;;
    172.*)
      second_octet=${ip#172.}
      second_octet=${second_octet%%.*}
      ((10#$second_octet >= 16 && 10#$second_octet <= 31))
      ;;
    *)
      return 1
      ;;
  esac
}

# ssh joins remote arguments into a shell command, so values passed to the remote
# shell must be restricted even though they are stored in local Bash arrays.
is_safe_remote_value() {
  [[ "$1" =~ ^[a-zA-Z0-9_./:+-]+$ ]]
}

ssh_user=${USER:-}
identity_file=
declare -a cluster_hosts=()
base_port=7000
remote_dir=
redis_server=redis-server
redis_cli=redis-cli
json_module=
search_module=
search_module_args_text=
install_redis=0
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
    --cluster-ip)
      (($# >= 2)) || die "--cluster-ip requires an IPv4 address"
      cluster_hosts+=("$2")
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
    --search-module-args)
      (($# >= 2)) || die "--search-module-args requires a value"
      search_module_args_text=$2
      shift 2
      ;;
    --install)
      install_redis=1
      shift
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

(($# >= 2 && $# <= 11)) || {
  usage >&2
  exit 2
}

redis_version=${!#}
host_count=$(($# - 1))
declare -a hosts=("${@:1:host_count}")

if ((${#cluster_hosts[@]} != 0 && ${#cluster_hosts[@]} != host_count)); then
  die "--cluster-ip must be omitted or repeated exactly once per host"
fi

for ((i = 0; i < host_count; i++)); do
  is_ipv4 "${hosts[i]}" || die "host IP $((i + 1)) is invalid: ${hosts[i]}"
  for ((j = 0; j < i; j++)); do
    [[ "${hosts[i]}" != "${hosts[j]}" ]] || die "host IPs must be unique: ${hosts[i]}"
  done
done
if ((${#cluster_hosts[@]} > 0)); then
  for ((i = 0; i < host_count; i++)); do
    is_ipv4 "${cluster_hosts[i]}" ||
      die "cluster IP $((i + 1)) is invalid: ${cluster_hosts[i]}"
    for ((j = 0; j < i; j++)); do
      [[ "${cluster_hosts[i]}" != "${cluster_hosts[j]}" ]] ||
        die "cluster IPs must be unique: ${cluster_hosts[i]}"
    done
  done
fi
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
logical_memory_mb=$((db_memory_gb * 1024))
((logical_memory_mb % shards == 0)) ||
  die "--db-memory-gb must be evenly divisible by --shards at MiB precision"
[[ "$ssh_user" =~ ^[a-zA-Z0-9_.-]+$ ]] || die "invalid SSH user: $ssh_user"
is_safe_remote_value "$redis_server" || die "unsafe --redis-server value: $redis_server"
is_safe_remote_value "$redis_cli" || die "unsafe --redis-cli value: $redis_cli"
[[ "$redis_version" =~ ^[0-9]+\.[0-9]+(\.[0-9]+)?([.-][a-zA-Z0-9]+)*$ ]] ||
  die "REDIS_VERSION is invalid: $redis_version"

if ((install_redis)); then
  redis_major=${redis_version%%.*}
  ((redis_major >= 8)) || die "--install supports Redis 8+, whose modules share the Redis version"
  install_root="/opt/redis/$redis_version"
  redis_server="$install_root/bin/redis-server"
  redis_cli="$install_root/bin/redis-cli"
  json_module="$install_root/lib/rejson.so"
  search_module="$install_root/lib/redisearch.so"
else
  [[ -n "$json_module" ]] || die "--json-module is required without --install"
  [[ -n "$search_module" ]] || die "--search-module is required without --install"
fi
[[ "$json_module" == /* ]] || die "--json-module must be an absolute remote path"
[[ "$search_module" == /* ]] || die "--search-module must be an absolute remote path"
is_safe_remote_value "$redis_server" || die "unsafe resolved redis-server path: $redis_server"
is_safe_remote_value "$redis_cli" || die "unsafe resolved redis-cli path: $redis_cli"
is_safe_remote_value "$json_module" || die "unsafe --json-module value: $json_module"
is_safe_remote_value "$search_module" || die "unsafe --search-module value: $search_module"

total_nodes=$((shards * 2))
memory_per_node_mb=$((logical_memory_mb / shards))
physical_memory_mb=$((memory_per_node_mb * total_nodes))
declare -a search_module_args=()
if [[ -n "$search_module_args_text" ]]; then
  read -r -a search_module_args <<<"$search_module_args_text"
  for module_arg in "${search_module_args[@]}"; do
    is_safe_remote_value "$module_arg" || die "unsafe --search-module-args value: $module_arg"
  done
fi

declare -a primary_counts=()
declare -a replica_counts=()
declare -a node_counts=()
declare -a replica_next_ports=()
for ((i = 0; i < host_count; i++)); do
  primary_counts[i]=0
  replica_counts[i]=0
done
for ((i = 0; i < shards; i++)); do
  host_index=$((i % host_count))
  primary_counts[host_index]=$((primary_counts[host_index] + 1))
done
replica_offset=$((host_count / 2))
((replica_offset > 0)) || replica_offset=1
for ((i = 0; i < host_count; i++)); do
  replica_host=$(((i + replica_offset) % host_count))
  replica_counts[replica_host]=$((replica_counts[replica_host] + primary_counts[i]))
done

max_nodes_per_host=0
for ((i = 0; i < host_count; i++)); do
  node_counts[i]=$((primary_counts[i] + replica_counts[i]))
  replica_next_ports[i]=$((base_port + primary_counts[i]))
  ((node_counts[i] <= max_nodes_per_host)) || max_nodes_per_host=${node_counts[i]}
done
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

declare -a ssh_options=(-o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new)
if [[ -n "$identity_file" ]]; then
  ssh_options+=(-i "$identity_file")
fi

declare -a destinations=()
for ((i = 0; i < host_count; i++)); do
  destinations+=("$ssh_user@${hosts[i]}")
done

if ((${#cluster_hosts[@]} == 0)); then
  if ((dry_run)); then
    cluster_hosts=("${hosts[@]}")
  else
    command -v ssh >/dev/null || die "ssh is not installed locally"
    echo "Detecting private cluster IPs..."
    for ((i = 0; i < host_count; i++)); do
      remote_addresses=$(ssh "${ssh_options[@]}" "${destinations[i]}" hostname -I)
      cluster_ip=
      for candidate in $remote_addresses; do
        if is_private_ipv4 "$candidate"; then
          cluster_ip=$candidate
          break
        fi
      done
      [[ -n "$cluster_ip" ]] ||
        die "could not detect a private IPv4 address on ${hosts[i]}; supply --cluster-ip"
      cluster_hosts+=("$cluster_ip")
      echo "  ${hosts[i]} -> $cluster_ip"
    done
  fi
fi

for ((i = 0; i < host_count; i++)); do
  for ((j = 0; j < i; j++)); do
    [[ "${cluster_hosts[i]}" != "${cluster_hosts[j]}" ]] ||
      die "cluster IPs must be unique: ${cluster_hosts[i]}"
  done
done

declare -a primary_endpoints=()
declare -a replica_endpoints=()
for ((host_index = 0; host_index < host_count; host_index++)); do
  for ((i = 0; i < primary_counts[host_index]; i++)); do
    primary_endpoints+=("${cluster_hosts[host_index]}:$((base_port + i))")
  done
done
for ((primary_host = 0; primary_host < host_count; primary_host++)); do
  replica_host=$(((primary_host + replica_offset) % host_count))
  for ((i = 0; i < primary_counts[primary_host]; i++)); do
    replica_endpoints+=("${cluster_hosts[replica_host]}:${replica_next_ports[replica_host]}")
    replica_next_ports[replica_host]=$((replica_next_ports[replica_host] + 1))
  done
done
declare -a endpoints=("${primary_endpoints[@]}" "${replica_endpoints[@]}")

echo "Redis Cluster allocation ($shards primary shards, $shards replicas, $total_nodes nodes):"
install_label=no
((install_redis == 0)) || install_label=yes
echo "  Install Redis: $install_label"
echo "  Logical database memory: ${db_memory_gb} GiB"
echo "  Maxmemory per primary/replica: ${memory_per_node_mb} MiB"
echo "  Physical Redis capacity including replicas: $((physical_memory_mb / 1024)) GiB"
echo "  Redis version: $redis_version"
echo "  RedisJSON module: $json_module"
echo "  RediSearch module: $search_module"
if ((${#search_module_args[@]})); then
  echo "  RediSearch module args: ${search_module_args[*]}"
else
  echo "  RediSearch module args: <none>"
fi
for ((i = 0; i < host_count; i++)); do
  if ((node_counts[i] == 0)); then
    port_range="none"
  else
    port_range="$base_port-$((base_port + node_counts[i] - 1))"
  fi
  echo "  ${hosts[i]} -> ${cluster_hosts[i]}: ${node_counts[i]} nodes" \
    "(${primary_counts[i]} primaries, ${replica_counts[i]} replicas), ports $port_range"
done
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

install_host() {
  local destination=$1

  echo "Installing Redis $redis_version on $destination"
  ssh "${ssh_options[@]}" "$destination" bash -s -- \
    "$redis_version" "$install_root" <<'REMOTE_INSTALL'
set -euo pipefail
redis_version=$1
install_root=$2
server_target="$install_root/bin/redis-server"
cli_target="$install_root/bin/redis-cli"
json_target="$install_root/lib/rejson.so"
search_target="$install_root/lib/redisearch.so"

installed_version() {
  "$1" --version 2>/dev/null | sed -n 's/.* v=\([^ ]*\).*/\1/p'
}

if [[ -x "$server_target" && -x "$cli_target" && -x "$json_target" &&
  -x "$search_target" && "$(installed_version "$server_target")" == "$redis_version" ]]; then
  echo "Redis $redis_version is already installed in $install_root"
  exit 0
fi

if ((EUID == 0)); then
  elevate=()
else
  command -v sudo >/dev/null || {
    echo "--install requires root or passwordless sudo" >&2
    exit 1
  }
  sudo -n true || {
    echo "--install requires passwordless sudo on remote hosts" >&2
    exit 1
  }
  elevate=(sudo -n)
fi

install_with_apt() {
  "${elevate[@]}" env DEBIAN_FRONTEND=noninteractive apt-get update
  "${elevate[@]}" env DEBIAN_FRONTEND=noninteractive apt-get install -y \
    ca-certificates curl gpg

  # shellcheck disable=SC1091
  source /etc/os-release
  codename=${VERSION_CODENAME:-}
  [[ -n "$codename" ]] || {
    echo "could not determine the Debian/Ubuntu codename" >&2
    exit 1
  }

  key_file=$(mktemp)
  curl -fsSL https://packages.redis.io/gpg -o "$key_file"
  "${elevate[@]}" gpg --batch --yes --dearmor \
    -o /usr/share/keyrings/redis-archive-keyring.gpg "$key_file"
  rm -f "$key_file"
  "${elevate[@]}" chmod 0644 /usr/share/keyrings/redis-archive-keyring.gpg
  printf '%s\n' \
    "deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg] https://packages.redis.io/deb $codename main" |
    "${elevate[@]}" tee /etc/apt/sources.list.d/redis.list >/dev/null
  "${elevate[@]}" env DEBIAN_FRONTEND=noninteractive apt-get update

  package_version=$(apt-cache madison redis | awk -v requested="$redis_version" '
    {
      candidate = $3;
      sub(/^[0-9]+:/, "", candidate);
      if (package_version == "" && index(candidate, requested "-") == 1) {
        package_version = $3;
      }
    }
    END { print package_version }')
  [[ -n "$package_version" ]] || {
    echo "Redis $redis_version is not available from packages.redis.io for $codename" >&2
    exit 1
  }
  "${elevate[@]}" env DEBIAN_FRONTEND=noninteractive apt-get install -y --allow-downgrades \
    "redis=$package_version" \
    "redis-server=$package_version" \
    "redis-sentinel=$package_version" \
    "redis-tools=$package_version"
}

install_with_rpm() {
  package_manager=$1
  "${elevate[@]}" "$package_manager" install -y ca-certificates curl
  # shellcheck disable=SC1091
  source /etc/os-release
  case "${ID:-}" in
    rocky | almalinux | rhel | centos)
      rpm_major=${VERSION_ID%%.*}
      [[ "$rpm_major" == 8 || "$rpm_major" == 9 ]] || {
        echo "unsupported RPM distribution version: ${ID:-} ${VERSION_ID:-}" >&2
        exit 1
      }
      ;;
    amzn)
      # Amazon Linux 2023 is RPM-compatible with the Rocky Linux 9 package.
      [[ "${VERSION_ID:-}" == 2023 ]] || {
        echo "--install supports Amazon Linux 2023, not ${VERSION_ID:-unknown}" >&2
        exit 1
      }
      rpm_major=9
      ;;
    *)
      echo "unsupported RPM distribution: ${ID:-unknown}" >&2
      exit 1
      ;;
  esac

  repo_file=$(mktemp)
  printf '%s\n' \
    '[Redis]' \
    'name=Redis' \
    "baseurl=https://packages.redis.io/rpm/rockylinux$rpm_major" \
    'enabled=1' \
    'gpgcheck=1' >"$repo_file"
  "${elevate[@]}" install -m 0644 "$repo_file" /etc/yum.repos.d/redis.repo
  rm -f "$repo_file"
  key_file=$(mktemp)
  curl -fsSL https://packages.redis.io/gpg -o "$key_file"
  "${elevate[@]}" rpm --import "$key_file"
  rm -f "$key_file"
  "${elevate[@]}" "$package_manager" makecache -y

  package_version=$("$package_manager" --showduplicates list redis 2>/dev/null |
    awk -v requested="$redis_version" '$1 ~ /^redis\./ {
      candidate = $2;
      sub(/^[0-9]+:/, "", candidate);
      if (index(candidate, requested "-") == 1) version = $2;
    } END { print version }')
  [[ -n "$package_version" ]] || {
    echo "Redis $redis_version is not available from the Redis RPM repository" >&2
    exit 1
  }
  "${elevate[@]}" "$package_manager" install -y "redis-$package_version"
}

if command -v apt-get >/dev/null; then
  install_with_apt
elif command -v dnf >/dev/null; then
  install_with_rpm dnf
elif command -v yum >/dev/null; then
  install_with_rpm yum
else
  echo "--install supports apt, dnf, or yum hosts" >&2
  exit 1
fi

if command -v systemctl >/dev/null; then
  "${elevate[@]}" systemctl stop redis-server.service redis.service 2>/dev/null || true
fi

server_source=$(command -v redis-server)
cli_source=$(command -v redis-cli)
json_source=$(find /usr/lib /usr/local/lib -type f -name rejson.so \
  ! -path "$install_root/*" -print -quit 2>/dev/null || true)
search_source=$(find /usr/lib /usr/local/lib -type f -name redisearch.so \
  ! -path "$install_root/*" -print -quit 2>/dev/null || true)
[[ -n "$json_source" && -n "$search_source" ]] || {
  echo "Redis $redis_version packages did not provide rejson.so and redisearch.so" >&2
  exit 1
}

"${elevate[@]}" install -d -m 0755 "$install_root/bin" "$install_root/lib"
"${elevate[@]}" install -m 0755 "$server_source" "$server_target"
"${elevate[@]}" install -m 0755 "$cli_source" "$cli_target"
"${elevate[@]}" install -m 0755 "$json_source" "$json_target"
"${elevate[@]}" install -m 0755 "$search_source" "$search_target"

[[ "$(installed_version "$server_target")" == "$redis_version" ]] || {
  echo "installed redis-server does not report version $redis_version" >&2
  exit 1
}
echo "Installed Redis $redis_version with Search and JSON in $install_root"
REMOTE_INSTALL
}

preflight_host() {
  local destination=$1 count=$2

  echo "Preflight: $destination"
  ssh "${ssh_options[@]}" "$destination" bash -s -- \
    "$base_port" "$count" "$remote_dir" "$redis_server" "$redis_cli" "$redis_version" \
    "$json_module" "$search_module" "$memory_per_node_mb" <<'REMOTE_PREFLIGHT'
set -euo pipefail
base_port=$1
count=$2
remote_dir=$3
redis_server=$4
redis_cli=$5
expected_redis_version=$6
json_module=$7
search_module=$8
memory_per_node_mb=$9

resolve_command() {
  if [[ "$1" == */* ]]; then
    [[ -x "$1" ]] || { echo "not executable: $1" >&2; exit 1; }
  else
    command -v "$1" >/dev/null || { echo "command not found: $1" >&2; exit 1; }
  fi
}

resolve_command "$redis_server"
resolve_command "$redis_cli"
resolve_command timeout
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
  required_memory_kib=$((count * memory_per_node_mb * 1024))
  ((total_memory_kib >= required_memory_kib)) || {
    echo "insufficient RAM: $count nodes at ${memory_per_node_mb} MiB each require at least" \
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
    "$startup_timeout" "$json_module" "$search_module" "$memory_per_node_mb" \
    "${search_module_args[@]}" <<'REMOTE_START'
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
memory_per_node_mb=${10}
shift 10
search_module_args=("$@")

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
    --save "" \
    --maxmemory "${memory_per_node_mb}mb" \
    --maxmemory-policy noeviction \
    --cluster-enabled yes \
    --cluster-config-file nodes.conf \
    --cluster-node-timeout 5000 \
    --cluster-announce-ip "$announce_ip" \
    --cluster-announce-port "$port" \
    --cluster-announce-bus-port "$((port + 10000))" \
    --loadmodule "$json_module" \
    --loadmodule "$search_module" "${search_module_args[@]}" \
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
  expected_maxmemory=$((memory_per_node_mb * 1024 * 1024))
  if [[ "$configured_maxmemory" != "$expected_maxmemory" ]]; then
    echo "shard on port $port has maxmemory $configured_maxmemory, expected $expected_maxmemory" >&2
    exit 1
  fi
done
REMOTE_START
}

check_cluster_ports() {
  local destination=$1

  echo "Checking Redis and cluster-bus ports from $destination"
  ssh "${ssh_options[@]}" "$destination" bash -s -- \
    "$redis_cli" "${endpoints[@]}" <<'REMOTE_CHECK_PORTS'
set -euo pipefail
redis_cli=$1
shift
endpoints=("$@")
declare -a check_pids=()
declare -a check_labels=()

for endpoint in "${endpoints[@]}"; do
  host=${endpoint%:*}
  port=${endpoint##*:}
  bus_port=$((port + 10000))

  (timeout 3 "$redis_cli" -h "$host" -p "$port" --raw PING 2>/dev/null |
    grep -qx PONG) &
  check_pids+=("$!")
  check_labels+=("Redis port $host:$port")

  timeout 3 bash -c ': </dev/tcp/$1/$2' _ "$host" "$bus_port" 2>/dev/null &
  check_pids+=("$!")
  check_labels+=("cluster-bus port $host:$bus_port")
done

failed=0
for ((i = 0; i < ${#check_pids[@]}; i++)); do
  if ! wait "${check_pids[i]}"; then
    echo "unreachable ${check_labels[i]}" >&2
    failed=1
  fi
done
((failed == 0))
REMOTE_CHECK_PORTS
}

if ((install_redis)); then
  for destination in "${destinations[@]}"; do
    install_host "$destination"
  done
fi

for ((i = 0; i < host_count; i++)); do
  preflight_host "${destinations[i]}" "${node_counts[i]}"
done

for ((i = 0; i < host_count; i++)); do
  ((node_counts[i] > 0)) || continue
  started_destinations+=("${destinations[i]}")
  start_host "${destinations[i]}" "${cluster_hosts[i]}" "${node_counts[i]}"
done

for destination in "${started_destinations[@]}"; do
  check_cluster_ports "$destination"
done

echo "Creating cluster and assigning all 16384 slots..."
ssh "${ssh_options[@]}" "${destinations[0]}" bash -s -- \
  "$redis_cli" "${endpoints[@]}" <<'REMOTE_CREATE'
set -euo pipefail
redis_cli=$1
shift
"$redis_cli" --cluster create "$@" --cluster-replicas 1 --cluster-yes
REMOTE_CREATE

echo "Waiting for cluster health..."
ssh "${ssh_options[@]}" "${destinations[0]}" bash -s -- \
  "$redis_cli" "$shards" "$total_nodes" "$host_count" "$startup_timeout" \
  "${endpoints[@]}" <<'REMOTE_VERIFY'
set -euo pipefail
redis_cli=$1
expected_shards=$2
expected_nodes=$3
expected_hosts=$4
startup_timeout=$5
shift 5
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
    if awk -v expected="$expected_shards" -v expected_hosts="$expected_hosts" '
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
          if (!(master in masters)) exit 1;
          if (expected_hosts > 1 && ips[replica] == ips[master]) exit 1;
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
