# -*- coding: utf-8 -*-

"""
Memory Regression Test for RedisJSON

This test ensures that memory overhead doesn't increase across versions.
It defines acceptable memory budgets and fails if they are exceeded.

Purpose for CI/CD:
- Detect memory regressions early
- Enforce memory budgets per document size
- Track memory efficiency improvements
- Prevent accidental memory bloat

Usage in CI/CD:
    TEST=test_memory_regression.py bash tests.sh <module-path>
"""

import json
from RLTest import Env
from includes import *
from common import *

# =============================================================================
# MEMORY BUDGETS - Update these when making intentional memory optimizations
# =============================================================================

# Maximum acceptable memory overhead ratios (RedisJSON memory / String length)
# These are the "budget" - tests fail if actual usage exceeds these values
MEMORY_BUDGETS = {
    # Format: 'test_name': max_acceptable_ratio
    'tiny_doc': 7.5,      # Tiny docs have high overhead (measured: ~7.04x)
    'small_doc': 4.0,     # Small docs (measured: ~3.08x)
    'medium_doc': 3.5,    # Medium docs (measured: ~3.12x)
    'large_doc': 3.0,     # Large docs (measured: ~2.63x)
    'array_10': 3.0,      # Small arrays (measured: ~2.56x)
    'array_100': 2.5,     # Medium arrays (measured: ~2.20x)
    'array_1000': 2.5,    # Large arrays (measured: ~2.27x)
    'nested_5': 3.5,      # Nested objects (measured: ~3.19x)
    'nested_20': 3.5,     # Deep nesting (measured: ~2.99x)
    'homogeneous_u8_100': 2.0,    # Homogeneous u8 arrays (optimized)
    'homogeneous_u8_10000': 1.5,  # Large homogeneous u8 (better optimization)
    'homogeneous_float_100': 2.0,   # Homogeneous float arrays
    'homogeneous_float_10000': 1.5, # Large homogeneous float
    'homogeneous_int_100': 2.0,     # Homogeneous int arrays
    'homogeneous_int_10000': 1.5,   # Large homogeneous int
}

# Absolute memory limits in bytes for specific test cases
ABSOLUTE_LIMITS = {
    'tiny_doc': 200,      # Max 200 bytes for tiny doc (measured: 176 bytes)
    'small_doc': 400,     # Max 400 bytes for small doc
    'large_doc': 15000,   # Max 15KB for large doc
}

# Comparison ratio: JSON.SET vs regular SET (should be close to constant)
# This catches if we're regressing relative to simple string storage
MAX_REDISJSON_VS_STRING_RATIO = 3.5  # RedisJSON should not use >3.5x regular string

# =============================================================================
# TEST DOCUMENTS
# =============================================================================

def get_tiny_doc():
    """Minimal JSON document"""
    return {"id": 1, "name": "test"}

def get_small_doc():
    """Small realistic document"""
    return {
        "id": 123,
        "name": "John Doe",
        "email": "john@example.com",
        "active": True,
        "score": 95.5,
        "tags": ["user", "premium"]
    }

def get_medium_doc():
    """Medium complexity document"""
    return {
        "user": {
            "id": 123,
            "name": "John Doe",
            "email": "john@example.com",
            "profile": {
                "age": 30,
                "city": "San Francisco",
                "interests": ["coding", "music", "travel"],
                "settings": {
                    "theme": "dark",
                    "notifications": True
                }
            }
        },
        "posts": [
            {"id": 1, "title": "First Post", "likes": 10, "tags": ["intro"]},
            {"id": 2, "title": "Second Post", "likes": 25, "tags": ["update", "news"]}
        ],
        "metadata": {
            "created": "2024-01-01",
            "updated": "2024-12-31"
        }
    }

def get_large_doc():
    """Large realistic document"""
    return {
        "user": {
            "id": 12345,
            "username": "john_doe_2025",
            "email": "john.doe@example.com",
            "profile": {
                "firstName": "John",
                "lastName": "Doe",
                "age": 32,
                "verified": True,
                "bio": "Software engineer passionate about distributed systems and databases.",
                "location": {
                    "city": "San Francisco",
                    "state": "CA",
                    "coordinates": {"lat": 37.7749, "lng": -122.4194}
                }
            }
        },
        "posts": [
            {
                "id": i,
                "title": f"Post {i}",
                "content": "This is a sample post with some content that makes it realistic.",
                "tags": ["tag1", "tag2", "tag3"],
                "likes": i * 10,
                "comments": i * 2
            }
            for i in range(10)
        ],
        "followers": [{"id": i, "name": f"user_{i}"} for i in range(20)],
        "statistics": {
            "total_posts": 10,
            "total_likes": 550,
            "follower_count": 20
        }
    }

def get_array_doc(size):
    """Array-heavy document"""
    return {
        "data": [
            {
                "id": i,
                "value": f"item_{i}",
                "score": i * 1.5,
                "active": i % 2 == 0
            }
            for i in range(size)
        ],
        "metadata": {"count": size}
    }

def get_nested_doc(depth):
    """Deeply nested document"""
    def create_nested(current_depth):
        if current_depth >= depth:
            return {"value": "leaf", "id": current_depth}
        return {
            "level": current_depth,
            "data": f"level_{current_depth}_data",
            "nested": create_nested(current_depth + 1)
        }
    return create_nested(0)

def get_homogeneous_u8_array(size):
    """Homogeneous u8 array (0-255 values)"""
    return {
        "data": [i % 256 for i in range(size)],
        "type": "u8"
    }

def get_homogeneous_float_array(size):
    """Homogeneous float array"""
    return {
        "data": [float(i) * 1.5 for i in range(size)],
        "type": "float"
    }

def get_homogeneous_int_array(size):
    """Homogeneous int array"""
    return {
        "data": [i for i in range(size)],
        "type": "int"
    }

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def measure_memory(env, doc, test_name):
    """
    Measure memory usage for a document and compare against budgets.
    Returns dict with measurements and pass/fail status.
    """
    json_str = json.dumps(doc)
    string_bytes = len(json_str.encode('utf-8'))
    
    # Store with JSON.SET
    key_json = f"test:regression:json:{test_name}"
    env.cmd('JSON.SET', key_json, '.', json_str)
    
    # Store with regular SET
    key_string = f"test:regression:string:{test_name}"
    env.cmd('SET', key_string, json_str)
    
    # Measure memory
    memory_json = env.cmd('MEMORY', 'USAGE', key_json)
    memory_string = env.cmd('MEMORY', 'USAGE', key_string)
    
    # Calculate ratios
    json_overhead_ratio = memory_json / string_bytes
    json_vs_string_ratio = memory_json / memory_string
    
    # Check against budgets
    budget_ratio = MEMORY_BUDGETS.get(test_name)
    absolute_limit = ABSOLUTE_LIMITS.get(test_name)
    
    results = {
        'test_name': test_name,
        'string_bytes': string_bytes,
        'memory_json': memory_json,
        'memory_string': memory_string,
        'json_overhead_ratio': json_overhead_ratio,
        'json_vs_string_ratio': json_vs_string_ratio,
        'budget_ratio': budget_ratio,
        'absolute_limit': absolute_limit,
        'passes_budget': True,
        'passes_absolute': True,
        'passes_comparison': True,
        'failures': []
    }
    
    # Check budget ratio
    if budget_ratio and json_overhead_ratio > budget_ratio:
        results['passes_budget'] = False
        results['failures'].append(
            f"Overhead ratio {json_overhead_ratio:.2f}x exceeds budget {budget_ratio:.2f}x"
        )
    
    # Check absolute limit
    if absolute_limit and memory_json > absolute_limit:
        results['passes_absolute'] = False
        results['failures'].append(
            f"Memory {memory_json} bytes exceeds limit {absolute_limit} bytes"
        )
    
    # Check comparison ratio
    if json_vs_string_ratio > MAX_REDISJSON_VS_STRING_RATIO:
        results['passes_comparison'] = False
        results['failures'].append(
            f"JSON vs String ratio {json_vs_string_ratio:.2f}x exceeds max {MAX_REDISJSON_VS_STRING_RATIO:.2f}x"
        )
    
    # Cleanup
    env.cmd('DEL', key_json, key_string)
    
    return results

def print_results_table(results_list):
    """Print formatted results table"""
    print("\n" + "="*100)
    print("MEMORY REGRESSION TEST RESULTS")
    print("="*100)
    print(f"\n{'Test':<20} {'Bytes':<10} {'JSON Mem':<12} {'Ratio':<10} {'Budget':<10} {'Status':<10}")
    print("-"*100)
    
    for r in results_list:
        status = "✓ PASS" if (r['passes_budget'] and r['passes_absolute'] and r['passes_comparison']) else "✗ FAIL"
        budget = f"{r['budget_ratio']:.2f}x" if r['budget_ratio'] else "N/A"
        
        print(f"{r['test_name']:<20} {r['string_bytes']:<10,} {r['memory_json']:<12,} "
              f"{r['json_overhead_ratio']:<10.2f} {budget:<10} {status:<10}")
    
    print("="*100 + "\n")

def print_failure_details(results_list):
    """Print detailed failure information"""
    failures = [r for r in results_list if not (r['passes_budget'] and r['passes_absolute'] and r['passes_comparison'])]
    
    if not failures:
        return
    
    print("\n" + "="*100)
    print("FAILURE DETAILS")
    print("="*100)
    
    for r in failures:
        print(f"\n❌ {r['test_name']}:")
        print(f"   String size: {r['string_bytes']:,} bytes")
        print(f"   JSON memory: {r['memory_json']:,} bytes")
        print(f"   Overhead ratio: {r['json_overhead_ratio']:.2f}x")
        print(f"   JSON vs String: {r['json_vs_string_ratio']:.2f}x")
        
        for failure in r['failures']:
            print(f"   • {failure}")
    
    print("\n" + "="*100)
    print("ACTION REQUIRED:")
    print("  1. Investigate why memory usage increased")
    print("  2. If intentional, update MEMORY_BUDGETS in test_memory_regression.py")
    print("  3. Document the change in commit message")
    print("="*100 + "\n")

# =============================================================================
# REGRESSION TESTS
# =============================================================================

def test_memory_regression_all_sizes(env):
    """
    Comprehensive memory regression test for all document sizes.
    This is the main test that should run in CI/CD.
    """
    env.skipOnCluster()
    
    results = []
    
    # Test different document sizes
    results.append(measure_memory(env, get_tiny_doc(), 'tiny_doc'))
    results.append(measure_memory(env, get_small_doc(), 'small_doc'))
    results.append(measure_memory(env, get_medium_doc(), 'medium_doc'))
    results.append(measure_memory(env, get_large_doc(), 'large_doc'))
    
    # Test array-heavy documents
    results.append(measure_memory(env, get_array_doc(10), 'array_10'))
    results.append(measure_memory(env, get_array_doc(100), 'array_100'))
    results.append(measure_memory(env, get_array_doc(1000), 'array_1000'))
    
    # Test nested documents
    results.append(measure_memory(env, get_nested_doc(5), 'nested_5'))
    results.append(measure_memory(env, get_nested_doc(20), 'nested_20'))
    
    # Print results
    print_results_table(results)
    
    # Check for failures
    failures = [r for r in results if not (r['passes_budget'] and r['passes_absolute'] and r['passes_comparison'])]
    
    if failures:
        print_failure_details(results)
        env.assertTrue(False, message=f"Memory regression detected in {len(failures)} test(s)")
    else:
        print("✅ All memory regression tests passed!")
        print("   Memory usage is within acceptable budgets.\n")


def test_memory_regression_tiny_doc(env):
    """Test memory budget for tiny documents"""
    env.skipOnCluster()
    
    result = measure_memory(env, get_tiny_doc(), 'tiny_doc')
    
    env.assertTrue(result['passes_budget'], 
                   message=f"Tiny doc overhead {result['json_overhead_ratio']:.2f}x exceeds budget {result['budget_ratio']:.2f}x")
    env.assertTrue(result['passes_absolute'],
                   message=f"Tiny doc memory {result['memory_json']} exceeds limit {result['absolute_limit']}")


def test_memory_regression_small_doc(env):
    """Test memory budget for small documents"""
    env.skipOnCluster()
    
    result = measure_memory(env, get_small_doc(), 'small_doc')
    
    env.assertTrue(result['passes_budget'],
                   message=f"Small doc overhead {result['json_overhead_ratio']:.2f}x exceeds budget {result['budget_ratio']:.2f}x")
    env.assertTrue(result['passes_absolute'],
                   message=f"Small doc memory {result['memory_json']} exceeds limit {result['absolute_limit']}")


def test_memory_regression_large_doc(env):
    """Test memory budget for large documents"""
    env.skipOnCluster()
    
    result = measure_memory(env, get_large_doc(), 'large_doc')
    
    env.assertTrue(result['passes_budget'],
                   message=f"Large doc overhead {result['json_overhead_ratio']:.2f}x exceeds budget {result['budget_ratio']:.2f}x")
    env.assertTrue(result['passes_absolute'],
                   message=f"Large doc memory {result['memory_json']} exceeds limit {result['absolute_limit']}")


def test_memory_regression_arrays(env):
    """Test memory budget for array-heavy documents"""
    env.skipOnCluster()
    
    results = []
    results.append(measure_memory(env, get_array_doc(10), 'array_10'))
    results.append(measure_memory(env, get_array_doc(100), 'array_100'))
    results.append(measure_memory(env, get_array_doc(1000), 'array_1000'))
    
    for result in results:
        env.assertTrue(result['passes_budget'],
                       message=f"{result['test_name']} overhead {result['json_overhead_ratio']:.2f}x exceeds budget {result['budget_ratio']:.2f}x")


def test_memory_regression_nested(env):
    """Test memory budget for nested documents"""
    env.skipOnCluster()
    
    results = []
    results.append(measure_memory(env, get_nested_doc(5), 'nested_5'))
    results.append(measure_memory(env, get_nested_doc(20), 'nested_20'))
    
    for result in results:
        env.assertTrue(result['passes_budget'],
                       message=f"{result['test_name']} overhead {result['json_overhead_ratio']:.2f}x exceeds budget {result['budget_ratio']:.2f}x")


def test_memory_regression_homogeneous_arrays(env):
    """Test memory budget for homogeneous arrays (optimized storage)"""
    env.skipOnCluster()
    
    results = []
    # U8 arrays (0-255 values)
    results.append(measure_memory(env, get_homogeneous_u8_array(100), 'homogeneous_u8_100'))
    results.append(measure_memory(env, get_homogeneous_u8_array(10000), 'homogeneous_u8_10000'))
    
    # Float arrays
    results.append(measure_memory(env, get_homogeneous_float_array(100), 'homogeneous_float_100'))
    results.append(measure_memory(env, get_homogeneous_float_array(10000), 'homogeneous_float_10000'))
    
    # Int arrays
    results.append(measure_memory(env, get_homogeneous_int_array(100), 'homogeneous_int_100'))
    results.append(measure_memory(env, get_homogeneous_int_array(10000), 'homogeneous_int_10000'))
    
    for result in results:
        env.assertTrue(result['passes_budget'],
                       message=f"{result['test_name']} overhead {result['json_overhead_ratio']:.2f}x exceeds budget {result['budget_ratio']:.2f}x")


def test_memory_regression_vs_string(env):
    """
    Test that RedisJSON memory usage stays within acceptable ratio of regular strings.
    This catches regressions relative to simple string storage.
    """
    env.skipOnCluster()
    
    test_cases = [
        ('tiny', get_tiny_doc()),
        ('small', get_small_doc()),
        ('medium', get_medium_doc()),
        ('large', get_large_doc()),
    ]
    
    for name, doc in test_cases:
        result = measure_memory(env, doc, name)
        
        env.assertTrue(result['passes_comparison'],
                       message=f"{name} JSON vs String ratio {result['json_vs_string_ratio']:.2f}x exceeds max {MAX_REDISJSON_VS_STRING_RATIO:.2f}x")


# =============================================================================
# BENCHMARK TEST (Optional - for tracking trends)
# =============================================================================

def test_memory_benchmark_report(env):
    """
    Generate a memory benchmark report for tracking trends over time.
    This test always passes but outputs metrics for CI/CD to collect.
    
    CI/CD can parse this output and track metrics over time.
    """
    env.skipOnCluster()
    
    results = []
    
    # Measure all test cases
    results.append(measure_memory(env, get_tiny_doc(), 'tiny_doc'))
    results.append(measure_memory(env, get_small_doc(), 'small_doc'))
    results.append(measure_memory(env, get_medium_doc(), 'medium_doc'))
    results.append(measure_memory(env, get_large_doc(), 'large_doc'))
    results.append(measure_memory(env, get_array_doc(100), 'array_100'))
    results.append(measure_memory(env, get_nested_doc(10), 'nested_10'))
    
    # Output in machine-readable format for CI/CD
    print("\n=== MEMORY_BENCHMARK_START ===")
    for r in results:
        print(f"METRIC:{r['test_name']}:string_bytes:{r['string_bytes']}")
        print(f"METRIC:{r['test_name']}:memory_json:{r['memory_json']}")
        print(f"METRIC:{r['test_name']}:memory_string:{r['memory_string']}")
        print(f"METRIC:{r['test_name']}:overhead_ratio:{r['json_overhead_ratio']:.4f}")
        print(f"METRIC:{r['test_name']}:vs_string_ratio:{r['json_vs_string_ratio']:.4f}")
    print("=== MEMORY_BENCHMARK_END ===\n")
    
    # This test always passes - it's just for metrics collection
    env.assertTrue(True)
