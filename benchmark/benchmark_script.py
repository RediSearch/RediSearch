#!/usr/bin/env python3
import os
import json
import time
import redis
import numpy as np
import timeit
from tqdm import tqdm
from benchmark import load_data_into_redis

def save_results_to_json(results_dict, filename):
    """Save benchmark results to a JSON file."""
    with open(filename, 'w') as f:
        json.dump(results_dict, f, indent=4)
    print(f"Results saved to {filename}")

def run_limit_benchmark():
    """Run benchmarks testing different limit sizes on aggregation queries."""
    # Dataset to process - use the largest available from your datasets
    dataset = 'musicbrainz-20000-A01.json'  # Update this to a larger dataset if available
    
    # Limit sizes to test (in ascending order)
    limit_sizes = [10000, 50000, 100000, 500000, 1000000]  # 100k, 500k, 1m, 5m
    
    # Number of repetitions for the benchmark
    num_experiments = 5
    
    # Data for results
    branch_results = {}
    master_results = {}
    
    # Define Redis configurations
    prefix = "/Users/itzik.vaknin/dev/"
    master_workdir = "RediSearchClean"  # Master build
    branch_workdir = "RediSearch"  # Branch build
    
    # Index name
    index_name = "idx_limit_test"

    # =================== Run benchmark with MASTER build ===================
    print("\n📊 Running benchmark with MASTER build...")
    
    # Kill any existing Redis server
    os.system("pkill -9 redis-server")
    time.sleep(1)
    
    # Remove dump file if it exists
    if os.path.exists(f"{os.getcwd()}/dump.rdb"):
        os.remove(f"{os.getcwd()}/dump.rdb")
    
    # Start Redis server with master build
    artifact = f"{prefix}{master_workdir}/bin/macos-arm64v8-debug/search-community/redisearch.so"
    os.system(f"redis-server --loadmodule {artifact} &")
    time.sleep(2)  # Give Redis some time to start up
    
    # Load data into Redis
    redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
    redis_client.execute_command("FLUSHALL")
    load_data_into_redis(dataset, index_name, 6379)
    
    # Test each limit size
    for limit in limit_sizes:
        checkpoint_file = f"master_limit_{limit}_checkpoint.json"
        if os.path.exists(checkpoint_file):
            print(f"Checkpoint file {checkpoint_file} already exists. Skipping this limit.")
            master_results[str(limit)] = json.load(open(checkpoint_file))[str(limit)]
            continue
        print(f"\nTesting MASTER with limit: {limit}")
        query = f"FT.AGGREGATE {index_name} * LIMIT 0 {limit} TIMEOUT 0"
        
        # Create a reusable function for timeit
        query_func = lambda: redis.Redis(host='localhost', port=6379, decode_responses=True).execute_command(query)
        
        # Run the query multiple times
        execution_times = []
        for _ in tqdm(range(num_experiments)):
            # Run once each time and measure with timeit
            query_time = timeit.timeit(query_func, number=1)
            execution_times.append(query_time)
        
        # Store results for this limit
        master_results[str(limit)] = {
            "execution_times": execution_times,
            "average_time": np.mean(execution_times),
            "min_time": np.min(execution_times),
            "max_time": np.max(execution_times)
        }
        
        # Save checkpoint after each limit test - this provides protection against crashes

        save_results_to_json(master_results, checkpoint_file)
    
    # Save master results
    master_results_file = "master_results_limit_test.json"
    save_results_to_json(master_results, master_results_file)

    #sanity:
    print(redis_client.execute_command(f"FT.PROFILE {index_name} AGGREGATE QUERY * TIMEOUT 0 LIMIT 0 100000")[-1])

    # =================== Run benchmark with BRANCH build ===================
    print("\n📊 Running benchmark with BRANCH build...")
    
    # Kill any existing Redis server
    os.system("pkill -9 redis-server")
    time.sleep(1)
    
    # Remove dump file if it exists
    if os.path.exists(f"{os.getcwd()}/dump.rdb"):
        os.remove(f"{os.getcwd()}/dump.rdb")
    
    # Start Redis server with branch build
    artifact = f"{prefix}{branch_workdir}/bin/macos-arm64v8-debug/search-community/redisearch.so"
    os.system(f"redis-server --loadmodule {artifact} &")
    time.sleep(2)  # Give Redis some time to start up
    
    # Load data into Redis
    redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
    redis_client.execute_command("FLUSHALL")
    load_data_into_redis(dataset, index_name, 6379)
    
    # Test each limit size
    for limit in limit_sizes:
        checkpoint_file = f"branch_limit_{limit}_checkpoint.json"
        if os.path.exists(checkpoint_file):
            print(f"Checkpoint file {checkpoint_file} already exists. Skipping this limit.")
            branch_results[str(limit)] = json.load(open(checkpoint_file))[str(limit)]
            continue
        print(f"\nTesting BRANCH with limit: {limit}")
        query = f"FT.AGGREGATE {index_name} * LIMIT 0 {limit} TIMEOUT 0"
        
        # Create a reusable function for timeit
        query_func = lambda: redis.Redis(host='localhost', port=6379, decode_responses=True).execute_command(query)
        
        # Run the query multiple times
        execution_times = []
        for _ in tqdm(range(num_experiments)):
            # Run once each time and measure with timeit
            query_time = timeit.timeit(query_func, number=1)
            execution_times.append(query_time)
        
        # Store results for this limit
        branch_results[str(limit)] = {
            "execution_times": execution_times,
            "average_time": np.mean(execution_times),
            "min_time": np.min(execution_times),
            "max_time": np.max(execution_times)
        }
        
        # Save checkpoint after each limit test - this provides protection against crashes
        
        save_results_to_json(branch_results, checkpoint_file)
    

    #sanity:
    print(redis_client.execute_command(f"FT.PROFILE {index_name} AGGREGATE QUERY * TIMEOUT 0 LIMIT 0 100000")[-1])

    # Save branch results
    branch_results_file = "branch_results_limit_test.json"
    save_results_to_json(branch_results, branch_results_file)
    
    # Kill Redis server after benchmarks are complete
    os.system("pkill -9 redis-server")
    
    # Create combined results with comparisons
    combined_results = {
        "dataset": dataset,
        "limits": {}
    }
    
    for limit in limit_sizes:
        limit_str = str(limit)
        master_avg = master_results[limit_str]["average_time"]
        branch_avg = branch_results[limit_str]["average_time"]
        
        combined_results["limits"][limit_str] = {
            "master": master_results[limit_str],
            "branch": branch_results[limit_str],
            "comparison": {
                "ratio_branch_to_master": branch_avg / master_avg if master_avg > 0 else float('inf'),
                "percentage_difference": ((branch_avg - master_avg) / master_avg) * 100 if master_avg > 0 else float('inf')
            }
        }
        
        # Save combined checkpoint file for each limit
        combined_checkpoint = f"combined_limit_{limit}_checkpoint.json"
        save_results_to_json(combined_results, combined_checkpoint)
    
    # Save combined results
    combined_results_file = "combined_limit_test_results.json"
    save_results_to_json(combined_results, combined_results_file)
    



    # Print summary
    print("\n=== Benchmark Summary ===")
    print(f"Dataset: {dataset}")
    print("\nAverage execution times (seconds):")
    print("-" * 70)
    print(f"{'Limit':<10} {'Master':<15} {'Branch':<15} {'Ratio (B/M)':<15} {'Diff %':<15}")
    print("-" * 70)
    
    for limit in limit_sizes:
        limit_str = str(limit)
        master_avg = combined_results["limits"][limit_str]["master"]["average_time"]
        branch_avg = combined_results["limits"][limit_str]["branch"]["average_time"]
        ratio = combined_results["limits"][limit_str]["comparison"]["ratio_branch_to_master"]
        pct_diff = combined_results["limits"][limit_str]["comparison"]["percentage_difference"]
        
        print(f"{limit:<10} {master_avg:<15.6f} {branch_avg:<15.6f} {ratio:<15.2f} {pct_diff:<15.2f}")
    
    return combined_results

if __name__ == "__main__":
    print("🚀 Starting Redis limit size benchmark script...")
    results = run_limit_benchmark()
    print("\n✅ Benchmark completed successfully!")