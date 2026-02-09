#!/bin/bash
# Download datasets for disk-based HNSW benchmarks
# Following VectorSimilarity's bm_files.sh pattern, but downloads datasets instead of
# pre-built indices since we don't have index serialization yet.
# Index will be built at benchmark runtime.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="$SCRIPT_DIR/data"

echo "Downloading datasets (10K, 100K, 1M vectors)..."
cat "$SCRIPT_DIR/data/resources/hnsw_disk_datasets.txt" | xargs -n 1 -P 0 wget --no-check-certificate -P "$DATA_DIR"

