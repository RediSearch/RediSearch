#!/usr/bin/env bash
#
# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).
set -uo pipefail

# -P: the documented .claude/skills path is a symlink, and a logical ../.. would stop in .claude/.
REPO_ROOT="$(cd -P "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
# A password exported for some other deployment would make every call to these passwordless servers fail.
unset REDISCLI_AUTH
RSV_ROOT="${RSV_ROOT:-/tmp/rsv-$(id -u)}"
REDIS_SERVER="${REDIS_SERVER:-$(command -v redis-server || true)}"
REDIS_CLI="${REDIS_CLI:-$(command -v redis-cli || true)}"

die() { echo "rsv: $*" >&2; exit 1; }

usage() {
  cat <<'EOF'
usage: rsv.sh [--name N] <command> [opts]

  start [--module P] [--json] [--json-path P] [--modargs "..."]   standalone server, then doctor
  cluster-start [same opts] [--shards N]                         N-node OSS cluster, then doctor
  doctor                      read-only health check; non-zero exit on any FAIL
  cli <args...>               redis-cli --no-raw [-c] -p <first port> <args...>
  rec <artifact> <args...>    cli, and append a stamped record to the evidence file
  port | ports                first node's port | every node's port
  evidence-dir                print (and create) this instance's evidence directory
  stop                        tear down this instance; evidence is kept
  list                        live instances under $RSV_ROOT
EOF
}

ensure_root() {
  [[ -e "$RSV_ROOT" ]] || mkdir -m 700 "$RSV_ROOT" || die "cannot create $RSV_ROOT"
  [[ -d "$RSV_ROOT" && -O "$RSV_ROOT" && ! -L "$RSV_ROOT" && "$(stat -c '%a' "$RSV_ROOT")" == 700 ]] \
    || die "$RSV_ROOT must be a mode-700 directory owned by you; set RSV_ROOT to a dedicated path"
}

bin_root() { case "$(uname -m)" in x86_64) echo "$REPO_ROOT/bin/linux-x64" ;; *) echo "$REPO_ROOT/bin/linux-$(uname -m)" ;; esac; }

default_module() {
  local p
  for p in "$(bin_root)-debug/search-community/redisearch.so" \
           "$(bin_root)-release/search-community/redisearch.so"; do
    [[ -f "$p" ]] && { echo "$p"; return; }
  done
}
default_json() {
  local p; p="$(bin_root)-release/RedisJSON/master/rejson.so"
  [[ -f "$p" ]] && echo "$p"
}

port_free() { [[ -z "$(ss -ltnH "sport = :$1" 2>/dev/null)" ]]; }
pick_ports() {
  local n="$1" base i ok
  for _ in $(seq 1 50); do
    base=$(( 20000 + RANDOM % 20000 )); ok=1
    # Cluster bus uses port+10000, so check that too.
    for (( i=0; i<n; i++ )); do
      port_free $((base+i)) && port_free $((base+i+10000)) || { ok=0; break; }
    done
    (( ok )) && { seq "$base" $((base+n-1)); return; }
  done
  die "could not find $n free ports"
}

bounded_cli() { local t="$1"; shift; timeout "$t" "$REDIS_CLI" "$@"; }

inst_dir() { echo "$RSV_ROOT/inst/$NAME"; }
ports() { cat "$(inst_dir)/ports" 2>/dev/null; }
first_port() { ports | head -1; }
is_cluster() { [[ -f "$(inst_dir)/cluster" ]]; }

start_node() {
  local port="$1"; shift
  local d; d="$(inst_dir)/node-$port"; mkdir -p "$d"
  local args=(--port "$port" --bind 127.0.0.1 --dir "$d" --daemonize yes
              --pidfile "$d/redis.pid" --logfile "$d/redis.log"
              --save "" --appendonly no --enable-debug-command local)
  [[ -n "$JSON" ]] && args+=(--loadmodule "$JSON")
  args+=(--loadmodule "$MODULE" "${MODARGS[@]}")
  "$REDIS_SERVER" "${args[@]}" "$@" || die "redis-server failed to start on $port (see $d/redis.log)"
}

# Redis tags warning-level log lines with " # ".
redis_log_warnings() { grep -E ' # ' "$1" 2>/dev/null | grep -v 'overcommit'; }

wait_ready() {
  for _ in $(seq 1 60); do
    [[ "$(bounded_cli 2 -p "$1" PING 2>/dev/null)" == PONG ]] && return 0
    sleep 0.5
  done
  redis_log_warnings "$(inst_dir)/node-$1/redis.log" | tail -3 >&2
  die "port $1 never answered PING; log: $(inst_dir)/node-$1/redis.log (run 'stop' before retrying)"
}

parse_start_args() {
  MODULE="$(default_module)"; JSON=""; MODARGS=(); SHARDS=3
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --module) MODULE="$2"; shift 2 ;;
      --json) JSON="$(default_json)"; [[ -n "$JSON" ]] || die "--json: no rejson.so under $(bin_root)-release/RedisJSON/master"; shift ;;
      --json-path) JSON="$2"; [[ -f "$JSON" ]] || die "--json-path: $JSON is not a file"; shift 2 ;;
      --modargs) read -ra MODARGS <<<"$2"; shift 2 ;;
      --shards) SHARDS="$2"; [[ "$SHARDS" =~ ^[1-9][0-9]?$ ]] || die "--shards must be an integer from 1 to 99"; shift 2 ;;
      *) die "unknown option $1" ;;
    esac
  done
  [[ -n "$MODULE" && -f "$MODULE" ]] || die "module not found (build with ./build.sh DEBUG=1, or pass --module)"
  # redis-server chdirs into --dir before loading modules, so paths must be absolute.
  MODULE="$(realpath "$MODULE")"; [[ -n "$JSON" ]] && JSON="$(realpath "$JSON")"
  [[ -x "$REDIS_SERVER" && -x "$REDIS_CLI" ]] || die "redis-server/redis-cli not on PATH (or set REDIS_SERVER/REDIS_CLI)"
  [[ -d "$(inst_dir)" ]] && die "instance '$NAME' already exists at $(inst_dir); run 'stop' first or use --name"
  mkdir -p "$(inst_dir)" "$RSV_ROOT/evidence/$NAME"
  echo "$MODULE" > "$(inst_dir)/module"
  if [[ -n "$(ls -A "$RSV_ROOT/evidence/$NAME" 2>/dev/null)" ]]; then
    echo "rsv: note: $RSV_ROOT/evidence/$NAME already holds earlier records; new ones are appended with their own stamp"
  fi
}

cmd_start() {
  parse_start_args "$@"
  local p; p="$(pick_ports 1)"; echo "$p" > "$(inst_dir)/ports"
  start_node "$p"; wait_ready "$p"; record_identity "$p"
  echo "rsv: instance '$NAME' up on 127.0.0.1:$p (module $MODULE)"
  cmd_doctor
}

coord_passes_cluster_gate() { bounded_cli 5 -p "$1" FT.SEARCH rsv-probe-no-such-index '*' 2>&1 | grep -q 'Index not found'; }
cluster_ok() { bounded_cli 2 -p "$1" CLUSTER INFO 2>/dev/null | grep -q 'cluster_state:ok'; }
coord_shards() { bounded_cli 5 -p "$1" SEARCH.CLUSTERINFO 2>/dev/null | tr -d '\r' | sed -n '/^num_partitions$/{n;p;q}'; }

cmd_cluster_start() {
  parse_start_args "$@"
  (( SHARDS >= 3 )) || { rm -rf "$(inst_dir)"; die "--shards must be at least 3: redis-cli --cluster create needs three masters"; }
  touch "$(inst_dir)/cluster"
  local ps p addrs=(); mapfile -t ps < <(pick_ports "$SHARDS"); printf '%s\n' "${ps[@]}" > "$(inst_dir)/ports"
  for p in "${ps[@]}"; do
    start_node "$p" --cluster-enabled yes --cluster-config-file "$(inst_dir)/node-$p/nodes.conf"
  done
  for p in "${ps[@]}"; do wait_ready "$p"; record_identity "$p"; addrs+=("127.0.0.1:$p"); done
  bounded_cli 60 --cluster create "${addrs[@]}" --cluster-replicas 0 --cluster-yes >"$(inst_dir)/cluster-create.log" 2>&1 \
    || die "cluster create failed; see $(inst_dir)/cluster-create.log"
  for p in "${ps[@]}"; do
    for _ in $(seq 1 60); do cluster_ok "$p" && break; sleep 0.5; done
  done
  # Servers without the cluster-topology-change module event never push the topology to the
  # coordinator ("ERRCLUSTER Uninitialized cluster state"); refreshing by hand is harmless otherwise.
  for p in "${ps[@]}"; do bounded_cli 10 -p "$p" SEARCH.CLUSTERREFRESH >/dev/null 2>&1; done
  for p in "${ps[@]}"; do
    for _ in $(seq 1 40); do coord_passes_cluster_gate "$p" && break; sleep 0.25; done
  done
  echo "rsv: cluster '$NAME' up on ports ${ps[*]} (module $MODULE)"
  cmd_doctor
}

pid_on_port() { bounded_cli "${2:-5}" -p "$1" INFO server 2>/dev/null | tr -d '\r' | sed -n 's/^process_id://p'; }
# Kernel start time in clock ticks: unlike the pid, it is not reused. The comm field can hold
# spaces, so count fields after its closing parenthesis.
proc_starttime() { sed 's/.*) //' "/proc/$1/stat" 2>/dev/null | awk '{print $20}'; }
record_identity() { local d; d="$(inst_dir)/node-$1"; proc_starttime "$(cat "$d/redis.pid")" > "$d/starttime"; }

# The cmdline fallback covers a server too wedged to answer INFO.
is_our_server() {
  [[ "$(proc_starttime "$2")" == "$(cat "$(inst_dir)/node-$1/starttime" 2>/dev/null)" ]] || return 1
  [[ "$(pid_on_port "$1" 2)" == "$2" ]] \
    || { grep -qa redis-server "/proc/$2/cmdline" 2>/dev/null && grep -qa ":$1" "/proc/$2/cmdline" 2>/dev/null; }
}

proc_start_epoch() { date -d "$(ps -o lstart= -p "$1")" +%s 2>/dev/null; }

cmd_doctor() {
  local d; d="$(inst_dir)"
  [[ -d "$d" ]] || { echo "doctor: FAIL no instance '$NAME' (start one first)"; return 1; }
  local module rc=0 p pid n; module="$(cat "$d/module")"; n="$(ports | wc -l)"
  (( n > 0 )) || { echo "doctor: FAIL instance '$NAME' has no recorded ports (startup interrupted?); run stop"; return 1; }
  for p in $(ports); do
    pid="$(cat "$d/node-$p/redis.pid" 2>/dev/null)"
    if [[ -z "$pid" ]] || ! kill -0 "$pid" 2>/dev/null; then
      echo "doctor: FAIL :$p process not running (log: $d/node-$p/redis.log)"; rc=1; continue
    fi
    if [[ "$(pid_on_port "$p")" != "$pid" ]]; then
      echo "doctor: FAIL :$p is not answered by our pid $pid"; rc=1; continue
    fi
    local mods; mods="$(bounded_cli 5 -p "$p" MODULE LIST 2>/dev/null | tr '\n' ' ')"
    if ! grep -q "name search ver" <<<"$mods"; then
      echo "doctor: FAIL :$p search module not loaded"; rc=1; continue
    fi
    echo "doctor: ok   :$p pid $pid modules: $mods"
    if grep -q "name ReJSON ver" <<<"$mods" && ! grep -q "Acquired RedisJSON_V" "$d/node-$p/redis.log" 2>/dev/null; then
      echo "doctor: FAIL :$p ReJSON loaded but search did not acquire its API (rejson.so too old?) — ON JSON indexes will fail"; rc=1
    fi
    local started; started="$(proc_start_epoch "$pid")"
    if [[ -n "$started" && "$(stat -c '%Y' "$module")" -gt "$started" ]]; then
      echo "doctor: FAIL :$p $module was rebuilt after this server started — stop and start to test the new build"; rc=1
    fi
    if is_cluster; then
      cluster_ok "$p" || { echo "doctor: FAIL :$p cluster_state is not ok"; rc=1; }
      local seen
      if ! coord_passes_cluster_gate "$p"; then
        echo "doctor: FAIL :$p coordinator not ready (try: redis-cli -p $p SEARCH.CLUSTERREFRESH)"; rc=1
      elif seen="$(coord_shards "$p")"; [[ "$seen" != "$n" ]]; then
        echo "doctor: FAIL :$p coordinator sees ${seen:-no} shards, expected $n (try: redis-cli -p $p SEARCH.CLUSTERREFRESH)"; rc=1
      else
        echo "doctor: ok   :$p cluster_state:ok, coordinator sees $n shards"
      fi
    fi
  done
  local newer; newer="$(find "$REPO_ROOT/src" -path '*/target' -prune -o -type f \( -name '*.c' -o -name '*.h' -o -name '*.cpp' -o -name '*.rs' -o -name '*.rl' -o -name '*.y' \
    -o -name 'Cargo.toml' -o -name 'Cargo.lock' -o -name 'CMakeLists.txt' \) -newer "$module" -print 2>/dev/null | head -3)"
  [[ -n "$newer" ]] && echo "doctor: WARN sources newer than the module build (rebuild?): $(tr '\n' ' ' <<<"$newer")"
  echo "doctor: module file $module ($(stat -c '%y' "$module" | cut -d. -f1)); HEAD $(git -C "$REPO_ROOT" log -1 --format='%h %cd' --date=format:'%F %T')"
  return $rc
}

cmd_cli() {
  local p a; p="$(first_port)"; [[ -n "$p" ]] || die "no instance '$NAME'"
  for a in "$@"; do
    [[ "$a" == -* ]] || break
    [[ "$a" == -x ]] || die "cli: only -x may precede the command; '$a' could point redis-cli at another server"
  done
  # --no-raw keeps reply types visible ((error), (integer), (nil)) even when stdout is not a tty.
  local t="${RSV_CLI_TIMEOUT:-120}"
  if is_cluster; then timeout "$t" "$REDIS_CLI" --no-raw -c -p "$p" "$@"; else timeout "$t" "$REDIS_CLI" --no-raw -p "$p" "$@"; fi
}

cmd_rec() {
  local art="$1"; shift
  [[ "$art" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] || die "artifact name must match [A-Za-z0-9][A-Za-z0-9._-]*"
  local f module; f="$(cmd_evidence_dir)/$art.txt"; module="$(cat "$(inst_dir)/module" 2>/dev/null)"
  local out rc stdin_hex="" a has_stdin=0
  for a in "$@"; do [[ "$a" == -* ]] || break; [[ "$a" == -x ]] && has_stdin=1; done
  if (( has_stdin )); then
    local tmp; tmp="$(mktemp)"; cat > "$tmp"
    stdin_hex="$(od -An -tx1 -v "$tmp" | tr -d ' \n')"
    out="$(cmd_cli "$@" < "$tmp" 2>&1)"; rc=$?; rm -f "$tmp"
  else
    out="$(cmd_cli "$@" 2>&1)"; rc=$?
  fi
  # redis-cli exits 0 on an error reply, so flag it explicitly.
  local tag="exit $rc"; grep -q '^(error)' <<<"$out" && tag="$tag, ERROR REPLY"
  {
    printf '# %s instance=%s ports=%s module=%s (built %s) HEAD=%s\n' \
      "$(date '+%F %T')" "$NAME" "$(ports | tr '\n' ',' | sed 's/,$//')" "$module" \
      "$(stat -c '%y' "$module" 2>/dev/null | cut -d. -f1)" "$(git -C "$REPO_ROOT" rev-parse --short HEAD 2>/dev/null)"
    printf '$ redis-cli'; printf ' %q' "$@"; printf '\n'
    [[ -n "$stdin_hex" ]] && printf '[stdin hex] %s\n' "$stdin_hex"
    printf '%s\n[%s]\n\n' "$out" "$tag"
  } >> "$f" || die "rec: could not append evidence to $f"
  printf '%s\n' "$out"
  return $rc
}

cmd_evidence_dir() { mkdir -p "$RSV_ROOT/evidence/$NAME"; echo "$RSV_ROOT/evidence/$NAME"; }

cmd_stop() {
  local d p pid orphan=0; d="$(inst_dir)"
  [[ -d "$d" ]] || { echo "rsv: no instance '$NAME'"; return 0; }
  mkdir -p "$RSV_ROOT/evidence/$NAME"
  for p in $(ports); do
    pid="$(cat "$d/node-$p/redis.pid" 2>/dev/null)"
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      if is_our_server "$p" "$pid"; then
        bounded_cli 5 -p "$p" SHUTDOWN NOSAVE >/dev/null 2>&1 || kill "$pid" 2>/dev/null
        for _ in $(seq 1 20); do kill -0 "$pid" 2>/dev/null || break; sleep 0.25; done
        kill -0 "$pid" 2>/dev/null && kill -9 "$pid"
      else
        echo "rsv: WARN pid $pid is alive but cannot be confirmed as our server on :$p; not killed" >&2
        orphan=1
      fi
    fi
    [[ -f "$d/node-$p/redis.log" ]] && cp "$d/node-$p/redis.log" "$RSV_ROOT/evidence/$NAME/redis-$p.log"
  done
  if (( orphan )); then
    echo "rsv: kept $d so the unconfirmed server stays traceable; inspect it, then remove the dir" >&2
    return 1
  fi
  rm -rf "$d"
  echo "rsv: stopped '$NAME'; evidence kept in $RSV_ROOT/evidence/$NAME"
}

cmd_list() {
  local d
  for d in "$RSV_ROOT"/inst/*/; do [[ -d "$d" ]] && echo "$(basename "$d") ports: $(tr '\n' ' ' < "$d/ports")"; done
  return 0
}

NAME="${RSV_NAME:-default}"
[[ "${1:-}" == --name ]] && { NAME="${2:-}"; shift 2 || true; }
[[ "$NAME" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] || die "invalid instance name '$NAME' (use letters, digits, . _ -; must not start with . or -)"
ensure_root
cmd="${1:-}"; shift || true
case "$cmd" in
  start) cmd_start "$@" ;;
  cluster-start) cmd_cluster_start "$@" ;;
  doctor) cmd_doctor ;;
  cli) cmd_cli "$@" ;;
  rec) cmd_rec "$@" ;;
  port) first_port ;;
  ports) ports ;;
  evidence-dir) cmd_evidence_dir ;;
  stop) cmd_stop ;;
  list) cmd_list ;;
  *) usage; exit 2 ;;
esac
