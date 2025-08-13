#!/bin/bash

# Add a 'loop/' prefix to the result file name,
# Use 'results.txt' as default if no argument provided
RESULT_FILE=loop/${1:-results.txt}

# Calculate total iterations for progress tracking
total_iterations=$(seq 100000 300000 3000000 | wc -l)

echo "=========================================="
echo "🚀 Starting RediSearch Benchmark Loop"
echo "=========================================="
echo "📊 Document range: 100,000 to 3,000,000 (step: 300,000)"
echo "📁 Result file: $RESULT_FILE"
echo ""

current_iteration=0

# Loop from 100,000 to 3,000,000 with steps of 300,000
for num_docs in $(seq 100000 300000 3000000); do
  current_iteration=$((current_iteration + 1))

  echo "🔄 ITERATION $current_iteration/$total_iterations"
  echo "📈 Running benchmark with $num_docs documents"
  echo "⏰ Started at: $(date '+%Y-%m-%d %H:%M:%S')"
  echo ""

  # Clean up previous files
  echo "🧹 Cleaning up previous files..."
  rm -f dump.rdb
  rm -rf redisearch
  echo "   ✅ Removed dump.rdb and redisearch directory"

  # Start Redis server with module in background
  echo "🔧 Starting Redis server with RediSearch module..."
  redis-server --loadmodule bin/linux-x64-release/search-community/redisearch.so DEFAULT_DIALECT 2 ON_TIMEOUT FAIL TIMEOUT 1000000 &

  # Store the PID of Redis server
  REDIS_PID=$!
  echo "   ✅ Redis server started (PID: $REDIS_PID)"

  # Wait a moment for Redis to start up
  echo "⏳ Waiting 2 seconds for Redis to initialize..."
  sleep 2
  echo "   ✅ Redis should be ready"
  echo ""

  # Run the benchmark script
  echo "🏃 Running benchmark script..."
  echo "   Command: python3 tests/benchmarks/manual/bench_disk_vs_ram.py --num_docs $num_docs --result_file $RESULT_FILE"
  python3 tests/benchmarks/manual/bench_disk_vs_ram.py --num_docs $num_docs --result_file $RESULT_FILE
  benchmark_exit_code=$?

  if [ $benchmark_exit_code -eq 0 ]; then
    echo "   ✅ Benchmark completed successfully"
  else
    echo "   ❌ Benchmark failed with exit code: $benchmark_exit_code"
  fi
  echo ""

  # Kill Redis server after benchmark completes
  echo "🛑 Shutting down Redis server..."
  echo "   Killing Redis server (PID: $REDIS_PID)"
  pkill -9 redis-server

  # Wait a moment to ensure Redis is fully terminated
  echo "⏳ Waiting 2 seconds for Redis to fully terminate..."
  sleep 2
  echo "   ✅ Redis server terminated"
  echo ""

  echo "✅ COMPLETED iteration $current_iteration/$total_iterations with $num_docs documents"
  echo "⏰ Finished at: $(date '+%Y-%m-%d %H:%M:%S')"
  echo "=========================================="
  echo ""
done

echo "🎉 ALL BENCHMARKS COMPLETED SUCCESSFULLY!"
echo "📊 Total iterations completed: $total_iterations"
echo "⏰ Final completion time: $(date '+%Y-%m-%d %H:%M:%S')"
echo "📁 Results saved to: $RESULT_FILE"
echo "=========================================="