#!/usr/bin/env python3
"""
Redis Command Benchmarking Script for RRF Single Shard Benchmarks

This script provides utilities for benchmarking Redis commands by measuring
execution times and comparing performance between different commands.
"""

import redis
import time
import argparse
import sys
from typing import Optional, Tuple, Any


def connect_to_redis(host: str = "localhost", port: int = 6379) -> Optional[redis.Redis]:
    """
    Connect to a Redis server.

    Args:
        host (str): Redis server hostname (default: localhost)
        port (int): Redis server port (default: 6379)

    Returns:
        redis.Redis: Redis connection object, or None if connection fails
    """
    try:
        client = redis.Redis(host=host, port=port, decode_responses=True)
        # Test the connection
        client.ping()
        print(f"Successfully connected to Redis at {host}:{port}")
        return client
    except redis.ConnectionError as e:
        print(f"Failed to connect to Redis at {host}:{port}: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error connecting to Redis: {e}")
        return None


def time_command(client: redis.Redis, command: str, trials: int = 100) -> Tuple[float, Any]:
    """
    Execute a Redis command multiple times and measure average execution time.

    Args:
        client (redis.Redis): Redis client connection
        command (str): Redis command to execute (e.g., "SET key value" or "GET key")
        trials (int): Number of trials to run for averaging (default: 100)

    Returns:
        Tuple[float, Any]: Average execution time in seconds and last command result
    """
    # Parse the command string into parts
    cmd_parts = command.strip().split()
    if not cmd_parts:
        raise ValueError("Empty command provided")

    total_time = 0.0
    result = None
    successful_trials = 0

    for i in range(trials):
        start_time = time.perf_counter()
        try:
            # Execute the command using redis-py's execute_command method
            result = client.execute_command(*cmd_parts)
            end_time = time.perf_counter()
            total_time += (end_time - start_time)
            successful_trials += 1
        except Exception as e:
            end_time = time.perf_counter()
            total_time += (end_time - start_time)
            if i == 0:  # Only print error on first failure
                print(f"Error executing command '{command}': {e}")

    average_time = total_time / trials
    return average_time, result


def benchmark_commands(client: redis.Redis, command1: str, command2: str, trials: int = 100) -> None:
    """
    Benchmark two Redis commands and compare their performance.

    Args:
        client (redis.Redis): Redis client connection
        command1 (str): First Redis command to benchmark
        command2 (str): Second Redis command to benchmark
        trials (int): Number of trials to run for averaging (default: 100)
    """
    print(f"\nBenchmarking commands (averaging over {trials} trials):")
    print(f"Command 1: {command1}")
    print(f"Command 2: {command2}")
    print("-" * 50)

    # Time first command
    print(f"Running {trials} trials for Command 1...")
    time1, result1 = time_command(client, command1, trials)
    print(f"Command 1 average execution time: {time1:.6f} seconds")
    print(f"Command 1 result: {result1}")

    # Time second command
    print(f"Running {trials} trials for Command 2...")
    time2, result2 = time_command(client, command2, trials)
    print(f"Command 2 average execution time: {time2:.6f} seconds")
    print(f"Command 2 result: {result2}")

    # Calculate and display ratio
    if time2 > 0:
        ratio = time1 / time2
        print(f"\nPerformance ratio (Command 1 / Command 2): {ratio:.2f}")
        if ratio > 1:
            print(f"Command 2 is {ratio:.2f}x faster than Command 1")
        elif ratio < 1:
            print(f"Command 1 is {1/ratio:.2f}x faster than Command 2")
        else:
            print("Commands have similar performance")
    else:
        print("Cannot calculate ratio (Command 2 execution time is 0)")


def main():
    """Main function to handle command line arguments and run benchmarks."""
    parser = argparse.ArgumentParser(description="Benchmark Redis commands")
    parser.add_argument("--host", default="localhost", help="Redis server host (default: localhost)")
    parser.add_argument("--port", type=int, default=6379, help="Redis server port (default: 6379)")
    parser.add_argument("--cmd1", help="First Redis command to benchmark")
    parser.add_argument("--cmd2", help="Second Redis command to benchmark")
    parser.add_argument("--trials", type=int, default=100, help="Number of trials to run for averaging (default: 100)")

    args = parser.parse_args()

    # Connect to Redis
    client = connect_to_redis(args.host, args.port)
    if not client:
        sys.exit(1)

    # If commands are provided via command line, use them
    if args.cmd1 and args.cmd2:
        benchmark_commands(client, args.cmd1, args.cmd2, args.trials)
    else:
        # Interactive mode
        print(f"\nInteractive mode - enter Redis commands to benchmark (averaging over {args.trials} trials)")
        try:
            while True:
                print("\nEnter two Redis commands to compare (or 'quit' to exit):")
                cmd1 = input("Command 1: ").strip()
                if cmd1.lower() == 'quit':
                    break

                cmd2 = input("Command 2: ").strip()
                if cmd2.lower() == 'quit':
                    break

                if cmd1 and cmd2:
                    benchmark_commands(client, cmd1, cmd2, args.trials)
                else:
                    print("Please provide both commands")

        except KeyboardInterrupt:
            print("\nExiting...")
        except EOFError:
            print("\nExiting...")


if __name__ == "__main__":
    main()