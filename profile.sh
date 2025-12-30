#!/usr/bin/env bash
set -euo pipefail

# This script handles VTune profiling of the RediSearch module
# It can be called directly or sourced from build.sh

# Global variables for cleanup (set by run_profile)
_CLEANUP_REDIS_PID=""
_CLEANUP_REDIS_PID_FILE=""
_CLEANUP_TEMP_DIR=""
_CLEANUP_VTUNE_DIR=""

# Cleanup function for Redis and VTune
# Uses global variables to avoid issues with special characters in trap strings
cleanup_profile() {
  echo ""
  echo "[profile] Cleaning up..."
  # Stop VTune if result directory is set
  if [ -n "${_CLEANUP_VTUNE_DIR}" ]; then
    if vtune -r "${_CLEANUP_VTUNE_DIR}" -command stop &> /dev/null; then
      echo "[profile] VTune profiling stopped"
    fi
  fi
  # Shutdown Redis by PID (not by port to avoid shutting down unrelated Redis instances)
  if [ -n "${_CLEANUP_REDIS_PID}" ]; then
    # Check if the process is still running
    if kill -0 "${_CLEANUP_REDIS_PID}" 2>/dev/null; then
      echo "[profile] Shutting down Redis server (PID: ${_CLEANUP_REDIS_PID})..."
      kill -TERM "${_CLEANUP_REDIS_PID}" 2>/dev/null || true
      # Wait briefly for graceful shutdown
      sleep 1
      # Force kill if still running
      if kill -0 "${_CLEANUP_REDIS_PID}" 2>/dev/null; then
        kill -KILL "${_CLEANUP_REDIS_PID}" 2>/dev/null || true
      fi
    fi
  fi
  # Remove temp directory
  if [ -n "${_CLEANUP_TEMP_DIR}" ] && [ -d "${_CLEANUP_TEMP_DIR}" ]; then
    echo "[profile] Removing temporary directory..."
    rm -rf "${_CLEANUP_TEMP_DIR}"
  fi
  # Clear the trap to prevent it from persisting if this script was sourced
  trap - EXIT
}

run_profile() {
  local root_dir="${1}"
  local build_dir="${2}"
  shift 2

  echo "[profile] Profiling with VTune"

  # Check if vtune is available
  if ! command -v vtune &> /dev/null; then
    echo "ERROR: vtune command not found. Please install Intel VTune Profiler."
    echo "Visit: https://www.intel.com/content/www/us/en/developer/tools/oneapi/vtune-profiler.html"
    exit 1
  fi

  # Check if redis-server is available
  if ! command -v redis-server &> /dev/null; then
    echo "ERROR: redis-server not found. Please install Redis."
    exit 1
  fi

  # Check if redis-cli is available
  if ! command -v redis-cli &> /dev/null; then
    echo "ERROR: redis-cli not found. Please install Redis."
    exit 1
  fi

  # Parse arguments
  local result_dir="${root_dir}/vtune_results"
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --result-dir)
        if [[ $# -lt 2 ]]; then
          echo "ERROR: --result-dir requires a path argument"
          echo "Usage: ./profile.sh <root_dir> <build_dir> [--result-dir <path>]"
          exit 1
        fi
        result_dir="$2"
        shift 2
        ;;
      *)
        echo "Unknown option: $1"
        echo "Usage: ./profile.sh <root_dir> <build_dir> [--result-dir <path>]"
        exit 1
        ;;
    esac
  done

  # Create result directory
  mkdir -p "${result_dir}"

  # Create a temporary directory for Redis
  local temp_dir=$(mktemp -d)
  local redis_port=6379
  local redis_pid_file="${temp_dir}/redis.pid"
  local redis_log_file="${temp_dir}/redis.log"
  local redis_config_file="${temp_dir}/redis.conf"
  local bigredis_path="${temp_dir}/redis.big"

  # Set up cleanup trap immediately after temp directory creation
  # to ensure cleanup happens even if redis-server fails to start
  # Only use EXIT trap to avoid duplicate cleanup when signals cause script to exit
  # Use global variables to avoid issues with special characters in trap strings
  _CLEANUP_REDIS_PID=""  # Will be set after Redis starts
  _CLEANUP_REDIS_PID_FILE="${redis_pid_file}"
  _CLEANUP_TEMP_DIR="${temp_dir}"
  trap cleanup_profile EXIT

  # Create Redis config file
  cat > "${redis_config_file}" <<EOF
port ${redis_port}
daemonize yes
pidfile "${redis_pid_file}"
logfile "${redis_log_file}"
dir "${temp_dir}"
loadmodule "${build_dir}/redisearch.so"
bigredis-enabled yes
bigredis-path "${bigredis_path}"
loglevel debug
EOF

  echo "[profile] Starting Redis server on port ${redis_port}..."
  redis-server "${redis_config_file}"

  # Wait for Redis to start and PID file to be created
  local max_wait=10
  local waited=0
  while [ ! -f "${redis_pid_file}" ] && [ $waited -lt $max_wait ]; do
    sleep 1
    waited=$((waited + 1))
  done

  # Check if PID file was created
  if [ ! -f "${redis_pid_file}" ]; then
    echo "ERROR: Redis PID file not created. Check logs at ${redis_log_file}"
    cat "${redis_log_file}"
    exit 1
  fi

  # Get Redis PID
  local redis_pid=$(cat ${redis_pid_file})

  # Verify PID is not empty
  if [ -z "${redis_pid}" ]; then
    echo "ERROR: Redis PID is empty. Check logs at ${redis_log_file}"
    cat "${redis_log_file}"
    exit 1
  fi

  # Set the cleanup PID now that we have it
  _CLEANUP_REDIS_PID="${redis_pid}"

  # Check if Redis started successfully
  if ! redis-cli -p ${redis_port} ping &> /dev/null; then
    echo "ERROR: Redis failed to start. Check logs at ${redis_log_file}"
    cat "${redis_log_file}"
    exit 1
  fi

  echo "[profile] Redis server started successfully (PID: ${redis_pid})"

  # Send SEARCH.CLUSTERSET command
  echo "[profile] Configuring cluster with SEARCH.CLUSTERSET..."
  if ! redis-cli -p ${redis_port} SEARCH.CLUSTERSET \
    MYID 1 \
    RANGES 1 \
    SHARD 1 \
    SLOTRANGE 0 16383 \
    ADDR "password@127.0.0.1:${redis_port}" \
    MASTER > /dev/null; then
    echo "ERROR: Failed to execute SEARCH.CLUSTERSET command"
    exit 1
  fi

  echo "[profile] Cluster configured successfully"

  # Create unique result directory with timestamp
  local timestamp=$(date +%Y%m%d_%H%M%S)
  local vtune_result_dir="${result_dir}/vtune_${timestamp}"

  echo "[profile] Starting VTune profiling..."
  echo "[profile] Result directory: ${vtune_result_dir}"
  echo "[profile] Redis PID: ${redis_pid}"
  echo ""
  echo "Redis is running and ready for profiling."
  echo "You can now run your workload against Redis on port ${redis_port}"
  echo ""
  echo "Press Enter when you're ready to start VTune profiling..."
  read

  # Update global variable to include VTune cleanup BEFORE starting VTune
  # This prevents a race condition where VTune could be left running if interrupted
  _CLEANUP_VTUNE_DIR="${vtune_result_dir}"

  # Start VTune profiling
  vtune -collect hotspots -result-dir "${vtune_result_dir}" -target-pid=${redis_pid} &
  local vtune_pid=$!

  # Verify VTune started successfully
  sleep 2
  if ! kill -0 ${vtune_pid} 2>/dev/null; then
    echo "ERROR: VTune failed to start or exited immediately"
    echo "This can happen due to:"
    echo "  - Insufficient ptrace permissions (try: echo 0 | sudo tee /proc/sys/kernel/yama/ptrace_scope)"
    echo "  - Invalid PID"
    echo "  - Missing kernel modules or VTune components"
    echo "Check VTune logs for details"
    exit 1
  fi

  echo "[profile] VTune profiling started successfully (PID: ${vtune_pid})"
  echo ""
  echo "VTune is now collecting profiling data."
  echo "Run your workload against Redis on port ${redis_port}"
  echo ""
  echo "Press Enter when you're done to stop profiling..."
  read

  # Stop VTune
  echo "[profile] Stopping VTune profiling..."
  if vtune -r "${vtune_result_dir}" -command stop 2>/dev/null; then
    wait ${vtune_pid} 2>/dev/null || true
    echo "[profile] VTune profiling stopped"
  else
    echo "[profile] VTune profiling already stopped or failed to stop"
  fi

  echo ""
  echo "✓ Profiling complete!"
  echo "  Results saved to: ${vtune_result_dir}"
  echo ""
  echo "To view results in GUI:"
  echo "  vtune-gui ${vtune_result_dir}"
  echo ""
  echo "To generate text report:"
  echo "  vtune -report hotspots -result-dir ${vtune_result_dir}"
}

# If script is executed directly (not sourced), run the profile function
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  if [ $# -lt 2 ]; then
    echo "Usage: $0 <root_dir> <build_dir> [--result-dir <path>]"
    exit 1
  fi
  run_profile "$@"
fi

