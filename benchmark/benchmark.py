#!/usr/bin/env python3

import os
import re
import json
import time
import pandas as pd
import redis
import numpy as np
import matplotlib.pyplot as plt
from tqdm import tqdm
from collections import Counter
from convert import csv_to_redis_json

def load_data_into_redis(json_file_path, index_name, port=6379):
    """Load data into Redis and create a search index.

    Args:
        json_file_path: Path to the JSON file with Redis data
        index_name: Name of the search index to create

    Returns:
        redis_client: Redis client instance or None if connection failed
    """
    # Connect to Redis
    r = redis.Redis(host='localhost', port=port, decode_responses=True)

    # Check connection
    try:
        r.ping()
        print("Successfully connected to Redis")
    except redis.ConnectionError:
        print("Failed to connect to Redis. Make sure Redis with RediSearch is running.")
        return None

    # Clear existing data and index
    try:
        r.execute_command(f"FT.DROPINDEX {index_name}")
        print(f"Dropped existing index: {index_name}")
    except:
        pass

    # Delete all keys with prefix 'doc:'
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor, match="doc:*", count=1000)
        if keys:
            r.delete(*keys)
        if cursor == 0:
            break
    print("Cleared existing data")

    # Load data from JSON
    print(f"Loading data from {json_file_path}...")
    with open(json_file_path, 'r') as f:
        redis_data = json.load(f)

    # Create RediSearch index - updated to reflect that CID is used in doc name, not as a field
    index_cmd = f"""
    FT.CREATE {index_name} ON HASH PREFIX 1 doc:
    SCHEMA
        TID TEXT
        title TEXT
        length TEXT
        artist TEXT
        album TEXT
        year NUMERIC SORTABLE
    """
    r.execute_command(index_cmd)
    print(f"Created search index: {index_name}")

    # Insert data using Redis pipeline with 1000 HSET granularity for better performance
    with r.pipeline() as pipe:
        for i, (doc_key, mapping) in enumerate(tqdm(redis_data.items())):
            pipe.hset(doc_key, mapping=mapping)
            if (i + 1) % 10000 == 0:  # Execute pipeline every 1000 commands
                pipe.execute()
            if i == 50000000:
                pipe.execute()  # Execute any remaining commands
                break
        pipe.execute()  # Execute any remaining commands
    return r

def extract_profile_time(response):
    """
    Extract the 'Total profile time' from Redis FT.PROFILE response.
    Handle different response formats.
    """
    if response[1][0][0] =='Total profile time':
        # FT.PROFILE response format
        return float(response[1][0][1])

    # If all else fails
    print("Warning: Could not extract profile time from response")
    return 0.0

def benchmark_queries(redis_client, index_name, word_dict, num_runs=5):
    """Benchmark query performance and return execution time ratio.

    Args:
        redis_client: Redis client instance
        index_name: Name of the search index
        word_dict: Dictionary containing common words to use in queries
        num_runs: Number of benchmark runs to perform

    Returns:
        results: Dictionary with benchmark results
    """
    if redis_client is None:
        print("Redis client is not available. Skipping benchmark.")
        return None

    # Use both most common and median frequency words for benchmarking
    most_common_word = word_dict['most_common']
    median_word = word_dict['median']

    # Define queries with specific words
    query1_common = f"FT.PROFILE {index_name} AGGREGATE QUERY {most_common_word} LOAD 1 @year SORTBY 2 @year ASC"
    query2_common = f"FT.PROFILE {index_name} AGGREGATE QUERY {most_common_word} LOAD 1 @year"

    query1_median = f"FT.PROFILE {index_name} AGGREGATE QUERY {median_word} LOAD 1 @year SORTBY 2 @year ASC"
    query2_median = f"FT.PROFILE {index_name} AGGREGATE QUERY {median_word} LOAD 1 @year"

    # Add star query (match all documents)
    query1_star = f"FT.PROFILE {index_name} AGGREGATE QUERY * LOAD 1 @year SORTBY 2 @year ASC"
    query2_star = f"FT.PROFILE {index_name} AGGREGATE QUERY * LOAD 1 @year"

    results = {
        'common_word': {
            'word': most_common_word,
            'with_sort': [],
            'without_sort': []
        },
        'median_word': {
            'word': median_word,
            'with_sort': [],
            'without_sort': []
        },
        'star_query': {
            'word': '*',
            'with_sort': [],
            'without_sort': []
        }
    }

    print(f"Running benchmarks with {num_runs} iterations each...")
    print(f"Using most common word: '{most_common_word}', median word: '{median_word}', and star query '*'")

    for _ in range(num_runs):
        # Run queries with most common word
        response1 = redis_client.execute_command(query1_common)
        time1 = extract_profile_time(response1)
        results['common_word']['with_sort'].append(time1)

        response2 = redis_client.execute_command(query2_common)
        time2 = extract_profile_time(response2)
        results['common_word']['without_sort'].append(time2)

        # Run queries with median frequency word
        response3 = redis_client.execute_command(query1_median)
        time3 = extract_profile_time(response3)
        results['median_word']['with_sort'].append(time3)

        response4 = redis_client.execute_command(query2_median)
        time4 = extract_profile_time(response4)
        results['median_word']['without_sort'].append(time4)

        # Run star queries
        response5 = redis_client.execute_command(query1_star)
        time5 = extract_profile_time(response5)
        results['star_query']['with_sort'].append(time5)

        response6 = redis_client.execute_command(query2_star)
        time6 = extract_profile_time(response6)
        results['star_query']['without_sort'].append(time6)

    # Calculate statistics for all query types
    stat_types = ['common_word', 'median_word', 'star_query']
    final_results = {}

    for stat_type in stat_types:
        avg_with_sort = np.mean(results[stat_type]['with_sort'])
        avg_without_sort = np.mean(results[stat_type]['without_sort'])
        ratio = avg_with_sort / avg_without_sort
        percent_diff = (avg_with_sort - avg_without_sort) / avg_without_sort * 100

        print(f"\nResults for {stat_type.replace('_', ' ')} '{results[stat_type]['word']}':")
        print(f"  Average time with sorting: {avg_with_sort:.6f} ms")
        print(f"  Average time without sorting: {avg_without_sort:.6f} ms")
        print(f"  Ratio (with sort / without sort): {ratio:.2f}")
        print(f"  Percentage difference: {percent_diff:.2f}%")

        final_results[stat_type] = {
            'word': results[stat_type]['word'],
            'with_sort': avg_with_sort,
            'without_sort': avg_without_sort,
            'ratio': ratio,
            'percent_diff': percent_diff
        }

    return final_results

def visualize_results(benchmark_results, output_file="benchmark_results.png"):
    """Visualize benchmark results.

    Args:
        benchmark_results: Dictionary with benchmark results for each dataset
        output_file: Path to save the visualization
    """
    datasets = list(benchmark_results.keys())

    # Create subplots for each query type
    fig, axs = plt.subplots(3, 2, figsize=(18, 18))

    # Query types
    query_types = ['common_word', 'median_word', 'star_query']
    labels = [ds.split("/")[-1].replace(".csv.dapo", "").replace(".csv", "") for ds in datasets]

    for i, query_type in enumerate(query_types):
        # Extract data for this query type
        ratios = [benchmark_results[ds][query_type]['ratio'] for ds in datasets]
        diffs = [benchmark_results[ds][query_type]['percent_diff'] for ds in datasets]

        # Plot ratios
        bars1 = axs[i, 0].bar(labels, ratios, color=['royalblue', 'green', 'purple'][i])
        axs[i, 0].axhline(y=1, color='r', linestyle='-', alpha=0.3)
        query_name = query_type.replace('_', ' ').title()
        if query_type == 'star_query':
            query_name = 'Star Query (*)'
        axs[i, 0].set_title(f'Execution Time Ratio - {query_name}', fontsize=14)
        axs[i, 0].set_ylabel('Ratio (with sort / without sort)', fontsize=12)
        axs[i, 0].tick_params(axis='x', rotation=45)

        for bar in bars1:
            height = bar.get_height()
            axs[i, 0].text(bar.get_x() + bar.get_width()/2., height + 0.05,
                    f'{height:.2f}', ha='center', va='bottom')

        # Plot percentage differences
        bars2 = axs[i, 1].bar(labels, diffs, color=['darkorange', 'brown', 'teal'][i])
        axs[i, 1].set_title(f'Percentage Difference - {query_name}', fontsize=14)
        axs[i, 1].set_ylabel('Percentage (%)', fontsize=12)
        axs[i, 1].tick_params(axis='x', rotation=45)

        for bar in bars2:
            height = bar.get_height()
            axs[i, 1].text(bar.get_x() + bar.get_width()/2., height + 5,
                    f'{height:.2f}%', ha='center', va='bottom')

    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Visualization saved to {output_file}")
    plt.show()

def main():
    """Main function to run the complete benchmark."""
    # Use only the smallest dataset
    datasets = [
        "datasets/musicbrainz-200-A01.csv.dapo",
        "datasets/musicbrainz-2000-A01.csv.dapo",  # Commented out larger datasets
        "datasets/musicbrainz-20000-A01.csv"
    ]

    # Generate JSON data for each dataset
    word_dicts = {}
    json_files = {}

    for dataset in datasets:
        output_path = dataset.split("/")[-1].replace(".csv.dapo", ".json").replace(".csv", ".json")
        word_dict = csv_to_redis_json(dataset, output_path)
        word_dicts[dataset] = word_dict
        json_files[dataset] = output_path

    # Run benchmarks for each dataset
    benchmark_results = {}

    for dataset in datasets:
        print(f"\n{'='*50}")
        print(f"Processing dataset: {dataset}")
        print(f"{'='*50}")

        # Get the JSON file path
        json_file_path = json_files[dataset]

        # Create index name based on dataset size
        size = dataset.split('-')[1]
        index_name = f"idx_{size}"

        # Load data into Redis
        redis_client = load_data_into_redis(json_file_path, index_name)

        # Run benchmarks with both common and median frequency words
        results = benchmark_queries(redis_client, index_name, word_dicts[dataset])
        if results:
            benchmark_results[dataset] = results

    # Visualize results if we have any
    if benchmark_results:
        visualize_results(benchmark_results)

        # Print summary of findings
        print("\nSummary of Benchmark Results:")
        print("-" * 60)

        for dataset, results in benchmark_results.items():
            dataset_name = dataset.split("/")[-1].replace(".csv.dapo", "").replace(".csv", "")
            print(f"Dataset: {dataset_name}")

            for query_type in ['common_word', 'median_word', 'star_query']:
                query_name = query_type.replace('_', ' ').title()
                if query_type == 'star_query':
                    query_name = 'Star Query (*)'

                word = results[query_type]['word']
                print(f"  {query_name} '{word}':")
                print(f"    With sort: {results[query_type]['with_sort']:.6f} ms")
                print(f"    Without sort: {results[query_type]['without_sort']:.6f} ms")
                print(f"    Ratio: {results[query_type]['ratio']:.2f}")
                print(f"    Percentage difference: {results[query_type]['percent_diff']:.2f}%")

            print("-" * 60)
    else:
        print("No benchmark results to visualize. Please check if Redis with RediSearch is running.")

if __name__ == "__main__":
    main()