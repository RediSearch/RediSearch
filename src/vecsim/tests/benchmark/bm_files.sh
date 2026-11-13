#!/usr/bin/env bash
set -euo pipefail

# Download each URL once and suppress noisy progress logs.
download_indices() {
    cat "$@" | sed '/^[[:space:]]*$/d' | sort -u | \
        xargs -r -n 1 -P 0 wget --no-check-certificate --no-verbose \
            --timeout=60 --tries=3 --waitretry=5 -P tests/benchmark/data
}

BM_TYPE=${1:-}
alg="hnsw"

if [ -z "$BM_TYPE"  ] || [ "$BM_TYPE" = "benchmarks-all" ]; then
    download_indices tests/benchmark/data/hnsw_indices/*.txt tests/benchmark/data/svs_indices/*.txt
    exit 0
elif [ "$BM_TYPE" = "benchmarks-default" ] \
|| [ "$BM_TYPE" = "bm-basics-fp32-single" ] \
|| [ "$BM_TYPE" = "bm-basics-fp32-multi" ] \
|| [ "$BM_TYPE" = "bm-batch-iter-fp32-single" ] \
|| [ "$BM_TYPE" = "bm-batch-iter-fp32-multi" ]
then
    file_name="basic_fp32"
elif [ "$BM_TYPE" = "bm-basics-fp64-single" ] \
|| [ "$BM_TYPE" = "bm-basics-fp64-multi" ] \
|| [ "$BM_TYPE" = "bm-batch-iter-fp64-single" ] \
|| [ "$BM_TYPE" = "bm-batch-iter-fp64-multi" ]
then
    file_name="basic_fp64"
elif [ "$BM_TYPE" = "bm-basics-bf16-single" ] \
|| [ "$BM_TYPE" = "bm-basics-bf16-multi" ] \
|| [ "$BM_TYPE" = "bm-batch-iter-bf16-single" ] \
|| [ "$BM_TYPE" = "bm-batch-iter-bf16-multi" ]
then
    file_name="basic_bf16"
elif [ "$BM_TYPE" = "bm-basics-fp16-single" ] \
|| [ "$BM_TYPE" = "bm-basics-fp16-multi" ] \
|| [ "$BM_TYPE" = "bm-batch-iter-fp16-single" ] \
|| [ "$BM_TYPE" = "bm-batch-iter-fp16-multi" ]
then
    file_name="basic_fp16"
elif [ "$BM_TYPE" = "bm-basics-int8-single" ] \
|| [ "$BM_TYPE" = "bm-basics-int8-multi" ] \
|| [ "$BM_TYPE" = "bm-batch-iter-int8-single" ] \
|| [ "$BM_TYPE" = "bm-batch-iter-int8-multi" ]
then
    file_name="basic_int8"
elif [ "$BM_TYPE" = "bm-basics-uint8-single" ] \
|| [ "$BM_TYPE" = "bm-basics-uint8-multi" ] \
|| [ "$BM_TYPE" = "bm-batch-iter-uint8-single" ] \
|| [ "$BM_TYPE" = "bm-batch-iter-uint8-multi" ] \
|| [ "$BM_TYPE" = "benchmarks-uint8" ]
then
    file_name="basic_uint8"
elif [ "$BM_TYPE" = "bm-sq8-compare-fp32" ]; then
    download_indices tests/benchmark/data/hnsw_indices/hnsw_indices_basic_fp32.txt \
        tests/benchmark/data/svs_indices/svs_indices_basic_fp32.txt \
        tests/benchmark/data/svs_indices/svs_indices_training.txt
    exit 0
elif [ "$BM_TYPE" = "bm-sq8-compare-fp16" ]; then
    download_indices tests/benchmark/data/hnsw_indices/hnsw_indices_basic_fp16.txt \
        tests/benchmark/data/svs_indices/svs_indices_training.txt
    exit 0
elif [ "$BM_TYPE" = "bm-updated-fp32-single" ]; then
    file_name="updated"
elif [ "$BM_TYPE" = "bm-sq8-train-fp32" ] \
|| [ "$BM_TYPE" = "bm-sq8-train-fp16" ] \
|| [ "$BM_TYPE" = "bm-svs-train-fp32" ] \
|| [ "$BM_TYPE" = "bm-svs-train-fp16" ]
then
    file_name="training"
    alg="svs"
elif [ "$BM_TYPE" = "bm-basics-svs-fp32-single" ]; then
    file_name="basic_fp32"
    alg="svs"
else
    echo "No files to download for BM_TYPE=$BM_TYPE"
    exit 0
fi

download_indices "tests/benchmark/data/${alg}_indices/${alg}_indices_$file_name.txt"
