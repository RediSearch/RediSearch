#!/bin/bash

# Start monitoring in the background
(while true; do 
    echo "--- $(date) ---"
    # This works on both macOS and Linux
    if [[ "$(uname)" == "Darwin" ]]; then
        # macOS version - uses command instead of comm
        ps -ax -o pid,rss,%mem,%cpu,command | grep -E 'cargo|operations|criterion' | grep -v grep
    else
        # Linux version
        ps -ax -o pid,rss,%mem,%cpu,cmd | grep -E 'cargo|operations|criterion|redisearch_rs' | grep -v grep
    fi
    echo ""
    sleep 5
done) &
MONITOR_PID=$!

# Run the benchmark
cargo bench

# Stop the monitoring
kill $MONITOR_PID
