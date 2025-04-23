#!/bin/bash

# Read command line arguments
if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <command> <mem-limit> "
  exit 1
fi

# parse command line arguments
command=$1
mem_limit=$2

# Check if the command is valid
if ! command -v $command &> /dev/null; then
  echo "Command '$command' not found"
  exit 1
fi

# Check if the memory limit is a valid number
if ! [[ $mem_limit =~ ^[0-9]+$ ]]; then
  echo "Memory limit '$mem_limit' is not a valid number"
  exit 1
fi

# Check if the memory limit is a positive number
if [ $mem_limit -le 0 ]; then
  echo "Memory limit '$mem_limit' must be a positive number"
  exit 1
fi

# Check if the memory limit is less than 14GB (16GB is max mem size of typical ubuntu github runners)
if [ $mem_limit -gt $((15*1024*1024)) ]; then
  echo "Memory limit '$mem_limit' kb is too high. Please use a value less than 15GB."
  exit 1
fi

# Limit memory usage
echo "ulimit -v $mem_limit"
if [ "$(uname)" == "Darwin" ]; then
  # On macOS, ulimit -v often doesn't work as expected
  echo "Warning: Memory limiting with ulimit -v may not work on macOS"
  # Try setting it anyway, but don't exit on failure
  ulimit -v $mem_limit 2>/dev/null || echo "Could not set memory limit"
else
  ulimit -v $mem_limit
fi
# Run the command
echo "Running command: $command"
$command &
BENCH_PID=$!

# Start memory monitoring with platform detection
(
echo "Memory monitoring started at $(date)"
if [ "$(uname)" == "Darwin" ]; then
  # macOS monitoring approach
  while ps -p $BENCH_PID > /dev/null 2>&1; do
    echo "$(date +%H:%M:%S) PID $BENCH_PID:"
    ps -o pid,rss,%mem,%cpu,command | grep -E 'cargo|bench' | grep -v grep;
    sleep 5
  done
else
  # Linux monitoring approach
  while [ -d /proc/$BENCH_PID ]; do
    echo "$(date +%H:%M:%S) PID $BENCH_PID:"
    ps -o pid,rss,%mem,%cpu,cmd | grep -E 'cargo|bench' | grep -v grep;
    sleep 5
  done
fi
echo "Memory monitoring ended at $(date)"
) >> memory_usage.log &
MONITOR_PID=$!

# Output process ids and cancellation instructions
echo "Benchmark running with PID: $BENCH_PID"
echo "Memory monitor running with PID: $MONITOR_PID"
echo "To cancel both processes: kill $BENCH_PID $MONITOR_PID"

# Wait for the benchmark to complete and capture its exit status
wait $BENCH_PID
COMMAND_EXIT_CODE=$?


if [ $? -ne 0 ]; then
  echo "Command '$command' failed"
  exit 1
fi

kill $MONITOR_PID 2>/dev/null

# Now check the command's actual exit code
if [ $COMMAND_EXIT_CODE -ne 0 ]; then
  echo "Command '$command' failed with exit code $COMMAND_EXIT_CODE"
  exit $COMMAND_EXIT_CODE
fi

echo "Last minute memory usage entries (pid,rss,%mem,%cpu,cmd):"
# 5 rows per entry --> 60 lines for one minute
tail -n 60 memory_usage.log
