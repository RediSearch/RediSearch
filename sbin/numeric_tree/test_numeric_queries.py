#!/usr/bin/env python3
"""
RediSearch Numeric Query Performance Tester

Tests numeric queries against RediSearch indexes with different insertion orders to evaluate
iterator performance across various tree structures (sequential, random, sparsed).

Features:
- Fair comparison: Same queries run on all indexes
- Infinity bounds: Support for -inf and +inf range queries
- Table output: Results organized by index for easy comparison
- Multiple query types: Single, union, and intersection queries

Usage:
    python test_numeric_queries.py --query-type all --iterations 100
    python test_numeric_queries.py --query-type single --infinity-ratio 0.5
    python test_numeric_queries.py --query-type intersection --range-size 50
"""

import argparse
import random
import redis
import statistics
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List


# Configuration constants
DEFAULT_FIELD_NAMES = ['price', 'score']
DEFAULT_VALUE_RANGE = (0, 1000)
QUERY_LIMIT = 10000
PROGRESS_INTERVAL = 10


@dataclass
class QueryResult:
    """Results from a single query execution"""
    query: str
    execution_time: float
    result_count: int
    total_docs: int
    index_name: str = ""


class NumericQueryTester:
    """Tests numeric queries against RediSearch indexes for performance evaluation"""

    def __init__(self, redis_host: str = 'localhost', redis_port: int = 6379, redis_db: int = 0):
        """Initialize Redis connection and validate connectivity"""
        try:
            self.redis_client = redis.Redis(
                host=redis_host,
                port=redis_port,
                db=redis_db,
                decode_responses=True
            )
            self.redis_client.ping()
            print(f"✓ Connected to Redis at {redis_host}:{redis_port}")
        except redis.ConnectionError:
            print(f"✗ Failed to connect to Redis at {redis_host}:{redis_port}")
            sys.exit(1)
    
    # Index discovery and metadata methods

    def get_index_info(self, index_name: str) -> Dict[str, Any]:
        """Get detailed information about a RediSearch index"""
        try:
            info = self.redis_client.execute_command('FT.INFO', index_name)
            # Parse the flat list response into a dictionary
            info_dict = {}
            for i in range(0, len(info), 2):
                if i + 1 < len(info):
                    info_dict[info[i]] = info[i + 1]
            return info_dict
        except redis.ResponseError:
            return {}

    def discover_indexes(self, prefix: str = "numeric_idx_") -> List[str]:
        """Discover available numeric indexes matching the prefix"""
        try:
            indexes = self.redis_client.execute_command('FT._LIST')
            numeric_indexes = [idx for idx in indexes if idx.startswith(prefix)]
            return sorted(numeric_indexes)
        except redis.ResponseError:
            return []
    
    def get_field_names(self, index_name: str) -> List[str]:
        """Extract numeric field names from index metadata"""
        info = self.get_index_info(index_name)
        field_names = []

        if 'attributes' in info and info['attributes']:
            attrs = info['attributes']
            for attr in attrs:
                if isinstance(attr, list) and len(attr) > 1 and 'NUMERIC' in attr:
                    # Redis index structure: ['identifier', 'field_name', 'attribute', 'field_name', 'type', 'NUMERIC', ...]
                    # The actual field name is at index 1
                    field_names.append(attr[1])

        # Fallback to standard field names if parsing fails
        return field_names if field_names else DEFAULT_FIELD_NAMES
    
    # Query execution methods

    def execute_query(self, index_name: str, query: str, limit: int = QUERY_LIMIT) -> QueryResult:
        """Execute a search query and measure performance metrics"""
        start_time = time.perf_counter()

        try:
            result = self.redis_client.execute_command(
                'FT.SEARCH', index_name, query, 'LIMIT', '0', str(limit)
            )
            execution_time = time.perf_counter() - start_time

            # Parse Redis response: [count, doc_id1, fields1, doc_id2, fields2, ...]
            result_count = result[0] if result else 0
            total_docs = len(result[1:]) // 2 if len(result) > 1 else 0

            return QueryResult(
                query=query,
                execution_time=execution_time,
                result_count=result_count,
                total_docs=total_docs,
                index_name=index_name
            )
        except redis.ResponseError as e:
            print(f"Query error on {index_name}: {e}")
            return QueryResult(query, 0.0, 0, 0, index_name)
    
    def generate_range_query(self, field_name: str, range_size: float,
                           use_infinity: bool = True, infinity_ratio: float = 0.3) -> str:
        """Generate a numeric range query with optional infinity bounds"""
        min_val, max_val = DEFAULT_VALUE_RANGE

        if use_infinity and random.random() < infinity_ratio:
            infinity_type = random.choice(['start_inf', 'end_inf', 'both_inf'])

            if infinity_type == 'start_inf':
                end_val = random.uniform(min_val, max_val)
                return f"@{field_name}:[-inf {end_val}]"
            elif infinity_type == 'end_inf':
                start_val = random.uniform(min_val, max_val)
                return f"@{field_name}:[{start_val} +inf]"
            else:  # both_inf - full range scan
                return f"@{field_name}:[-inf +inf]"
        else:
            # Regular bounded range query
            start_val = random.uniform(min_val, max_val - range_size)
            end_val = start_val + range_size
            return f"@{field_name}:[{start_val} {end_val}]"

    def _generate_multi_field_query(self, range_size: float, use_infinity: bool,
                                   infinity_ratio: float, operator: str) -> str:
        """Generate a query combining multiple fields with the specified operator"""
        query_parts = []
        for field_name in DEFAULT_FIELD_NAMES:
            range_query = self.generate_range_query(field_name, range_size, use_infinity, infinity_ratio)
            query_parts.append(range_query)

        separator = " | " if operator == "OR" else " "
        return separator.join(query_parts)

    # Test execution methods

    def _execute_queries_on_indexes(self, indexes: List[str], queries: List[str],
                                   query_type: str) -> List[QueryResult]:
        """Execute a list of queries on all indexes and return results"""
        results = []
        iterations = len(queries)

        for index_name in indexes:
            for i, query in enumerate(queries):
                result = self.execute_query(index_name, query)
                results.append(result)

                if (i + 1) % PROGRESS_INTERVAL == 0:
                    print(f"  {index_name}: {i + 1}/{iterations} {query_type} queries completed")

        return results

    def test_single_range_queries(self, indexes: List[str], iterations: int, range_size: float,
                                 use_infinity: bool = True, infinity_ratio: float = 0.3) -> List[QueryResult]:
        """Test single field range queries across all indexes with identical queries"""
        print(f"Testing single range queries (range size: {range_size}, infinity: {infinity_ratio:.1%})...")

        # Pre-generate queries to ensure fair comparison across indexes
        queries = []
        for i in range(iterations):
            field_name = DEFAULT_FIELD_NAMES[i % len(DEFAULT_FIELD_NAMES)]
            query = self.generate_range_query(field_name, range_size, use_infinity, infinity_ratio)
            queries.append(query)

        return self._execute_queries_on_indexes(indexes, queries, "single")
    
    def test_union_queries(self, indexes: List[str], iterations: int, range_size: float,
                          use_infinity: bool = True, infinity_ratio: float = 0.3) -> List[QueryResult]:
        """Test union queries (OR operations) across multiple fields"""
        if len(indexes) < 2:
            print("Need at least 2 indexes for union queries")
            return []

        print(f"Testing union queries across {len(indexes)} indexes...")

        # Pre-generate union queries for fair comparison
        queries = [
            self._generate_multi_field_query(range_size, use_infinity, infinity_ratio, "OR")
            for _ in range(iterations)
        ]

        return self._execute_queries_on_indexes(indexes, queries, "union")
    
    def test_intersection_queries(self, indexes: List[str], iterations: int, range_size: float,
                                 use_infinity: bool = True, infinity_ratio: float = 0.3) -> List[QueryResult]:
        """Test intersection queries (AND operations) across multiple fields"""
        if len(indexes) < 2:
            print("Need at least 2 indexes for intersection queries")
            return []

        print(f"Testing intersection queries across {len(indexes)} indexes...")

        # Pre-generate intersection queries for fair comparison
        queries = [
            self._generate_multi_field_query(range_size, use_infinity, infinity_ratio, "AND")
            for _ in range(iterations)
        ]

        return self._execute_queries_on_indexes(indexes, queries, "intersection")
    
    # Results processing and display methods

    def organize_results_by_index(self, results: List[QueryResult]) -> Dict[str, List[QueryResult]]:
        """Group query results by index name for comparison"""
        by_index = {}
        for result in results:
            index_name = result.index_name
            if index_name not in by_index:
                by_index[index_name] = []
            by_index[index_name].append(result)
        return by_index

    def _calculate_stats(self, execution_times: List[float]) -> Dict[str, float]:
        """Calculate statistical metrics for execution times"""
        if not execution_times:
            return {'mean': 0, 'median': 0, 'min': 0, 'max': 0, 'std_dev': 0}

        return {
            'mean': statistics.mean(execution_times),
            'median': statistics.median(execution_times),
            'min': min(execution_times),
            'max': max(execution_times),
            'std_dev': statistics.stdev(execution_times) if len(execution_times) > 1 else 0.0
        }

    def print_statistics_table(self, results: List[QueryResult], query_type: str):
        """Print comprehensive performance statistics in table format"""
        if not results:
            print(f"No results for {query_type} queries")
            return

        by_index = self.organize_results_by_index(results)

        print(f"\n{query_type.upper()} Query Performance by Index:")
        print("=" * 100)

        # Table header
        header = f"{'Index Name':<25} {'Queries':<8} {'Mean (ms)':<10} {'Median (ms)':<12} {'Min (ms)':<9} {'Max (ms)':<9} {'Std Dev':<8} {'Avg Results':<12}"
        print(header)
        print("-" * 100)

        # Per-index statistics
        for index_name in sorted(by_index.keys()):
            index_results = by_index[index_name]
            execution_times = [r.execution_time * 1000 for r in index_results]  # Convert to ms
            result_counts = [r.result_count for r in index_results]

            stats = self._calculate_stats(execution_times)
            avg_results = statistics.mean(result_counts) if result_counts else 0

            print(f"{index_name:<25} {len(index_results):<8} {stats['mean']:<10.2f} {stats['median']:<12.2f} "
                  f"{stats['min']:<9.2f} {stats['max']:<9.2f} {stats['std_dev']:<8.2f} {avg_results:<12.1f}")

        print("-" * 100)

        # Overall statistics
        all_times = [r.execution_time * 1000 for r in results]
        all_counts = [r.result_count for r in results]
        overall_stats = self._calculate_stats(all_times)
        overall_avg_results = statistics.mean(all_counts) if all_counts else 0

        print(f"{'OVERALL':<25} {len(results):<8} {overall_stats['mean']:<10.2f} {overall_stats['median']:<12.2f} "
              f"{overall_stats['min']:<9.2f} {overall_stats['max']:<9.2f} {overall_stats['std_dev']:<8.2f} {overall_avg_results:<12.1f}")

        # Example queries
        self._print_example_queries(by_index)

    def _print_example_queries(self, by_index: Dict[str, List[QueryResult]]):
        """Print example queries for each index"""
        print(f"\nExample queries per index:")
        for index_name in sorted(by_index.keys()):
            index_results = by_index[index_name]
            print(f"  {index_name}:")
            for result in index_results[:2]:  # Show first 2 queries per index
                print(f"    {result.query} -> {result.result_count} results in {result.execution_time*1000:.2f}ms")


def create_argument_parser() -> argparse.ArgumentParser:
    """Create and configure the command line argument parser"""
    parser = argparse.ArgumentParser(
        description="Test numeric queries against RediSearch indexes with different insertion orders",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Test all query types with default infinity bounds (30%)
  python test_numeric_queries.py --query-type all --iterations 100

  # Test with higher infinity ratio (60%)
  python test_numeric_queries.py --query-type single --use-infinity 0.6

  # Test intersection queries with custom range size and infinity bounds
  python test_numeric_queries.py --query-type intersection --range-size 50 --use-infinity 0.4

  # Test single field queries without infinity bounds
  python test_numeric_queries.py --query-type single --no-infinity

This script tests the 3 indexes created by generate_numeric_trees.py:
  - numeric_idx_sequential: Values inserted in ascending order (balanced tree)
  - numeric_idx_random: Values inserted in random order (average case)
  - numeric_idx_sparsed: Same value inserted multiple times (unbalanced tree)
        """
    )

    # Query configuration
    parser.add_argument('--query-type', '-q',
                       choices=['single', 'union', 'intersection', 'all'],
                       default='all',
                       help='Type of queries to test (default: all)')
    parser.add_argument('--iterations', '-n', type=int, default=100,
                       help='Number of iterations per query type (default: 100)')
    parser.add_argument('--range-size', '-r', type=float, default=100.0,
                       help='Size of numeric ranges in queries (default: 100.0)')

    # Infinity bounds configuration
    infinity_group = parser.add_mutually_exclusive_group()
    infinity_group.add_argument('--use-infinity', type=float, metavar='RATIO', nargs='?',
                               const=0.3, default=0.3,
                               help='Include infinity bounds in queries with optional ratio (0.0-1.0, default: 0.3)')
    infinity_group.add_argument('--no-infinity', action='store_const', const=0.0, dest='use_infinity',
                               help='Disable infinity bounds in queries')

    # Index selection
    parser.add_argument('--indexes', '-i', type=int, default=0,
                       help='Number of indexes to use (0 = auto-discover, default: 0)')

    # Redis connection
    parser.add_argument('--redis-host', default='localhost',
                       help='Redis host (default: localhost)')
    parser.add_argument('--redis-port', type=int, default=6379,
                       help='Redis port (default: 6379)')
    parser.add_argument('--redis-db', type=int, default=0,
                       help='Redis database number (default: 0)')

    return parser


def main():
    """Main execution function"""
    parser = create_argument_parser()
    args = parser.parse_args()

    # Parse infinity configuration
    if args.use_infinity == 0.0:
        use_infinity = False
        infinity_ratio = 0.0
    else:
        use_infinity = True
        infinity_ratio = args.use_infinity

        # Validate infinity ratio
        if infinity_ratio < 0.0 or infinity_ratio > 1.0:
            print("Error: Infinity ratio must be between 0.0 and 1.0")
            sys.exit(1)

    # Print configuration
    print(f"Testing numeric queries with {args.iterations} iterations per type")
    print(f"Range size: {args.range_size}, Query types: {args.query_type}")
    if use_infinity:
        print(f"Infinity bounds: enabled ({infinity_ratio:.1%} of queries)")
    else:
        print("Infinity bounds: disabled")
    print("-" * 60)

    # Initialize tester and discover indexes
    tester = NumericQueryTester(args.redis_host, args.redis_port, args.redis_db)
    available_indexes = tester.discover_indexes()

    if not available_indexes:
        print("No numeric indexes found. Run generate_numeric_trees.py first.")
        sys.exit(1)

    indexes_to_use = available_indexes[:args.indexes] if args.indexes > 0 else available_indexes

    print(f"Found {len(available_indexes)} indexes, using {len(indexes_to_use)}:")
    for idx in indexes_to_use:
        info = tester.get_index_info(idx)
        doc_count = info.get('num_docs', 'unknown')
        print(f"  {idx}: {doc_count} documents")
    print()

    # Execute tests
    all_results = {}
    test_params = (args.iterations, args.range_size, use_infinity, infinity_ratio)

    # Define test configurations
    test_configs = [
        ('single', tester.test_single_range_queries, True),
        ('union', tester.test_union_queries, len(indexes_to_use) >= 2),
        ('intersection', tester.test_intersection_queries, len(indexes_to_use) >= 2)
    ]

    for test_name, test_method, condition in test_configs:
        if args.query_type in [test_name, 'all'] and condition:
            results = test_method(indexes_to_use, *test_params)
            all_results[test_name] = results
            tester.print_statistics_table(results, test_name)

    # Print summary
    total_queries = sum(len(results) for results in all_results.values())
    total_time = sum(sum(r.execution_time for r in results) for results in all_results.values())

    print("\n" + "=" * 60)
    print(f"✓ Completed {total_queries} queries in {total_time:.2f} seconds")
    if total_queries > 0:
        print(f"✓ Average query time: {(total_time/total_queries)*1000:.2f}ms")


if __name__ == '__main__':
    main()
