# -*- coding: utf-8 -*-

"""
Nightly Memory Overhead Report for RedisJSON

This test generates a comprehensive memory overhead report designed for:
- Nightly CI/CD runs
- Long-term trend tracking
- Performance dashboards
- Release notes

The output is both human-readable and machine-parseable for automated collection.

Usage:
    TEST=test_memory_nightly_report.py bash tests.sh <module-path>
    
Output includes:
- Current overhead ratios (e.g., "2.4x")
- Historical comparison (if baseline provided)
- Detailed breakdown by document type
- Machine-readable metrics for dashboards
"""

import json
import os
import time
from datetime import datetime
from RLTest import Env
from includes import *
from common import *

# =============================================================================
# TEST DOCUMENTS (Same as regression tests for consistency)
# =============================================================================

def get_test_documents():
    """Return all test documents with metadata"""
    return {
        'tiny': {
            'doc': {"id": 1, "name": "test"},
            'category': 'basic',
            'description': 'Minimal JSON (2 fields)'
        },
        'small': {
            'doc': {
                "id": 123,
                "name": "John Doe",
                "email": "john@example.com",
                "active": True,
                "score": 95.5,
                "tags": ["user", "premium"]
            },
            'category': 'basic',
            'description': 'Small document (6 fields, 1 array)'
        },
        'medium': {
            'doc': {
                "user": {
                    "id": 123,
                    "name": "John Doe",
                    "email": "john@example.com",
                    "profile": {
                        "age": 30,
                        "city": "San Francisco",
                        "interests": ["coding", "music", "travel"],
                        "settings": {"theme": "dark", "notifications": True}
                    }
                },
                "posts": [
                    {"id": 1, "title": "First Post", "likes": 10, "tags": ["intro"]},
                    {"id": 2, "title": "Second Post", "likes": 25, "tags": ["update", "news"]}
                ],
                "metadata": {"created": "2024-01-01", "updated": "2024-12-31"}
            },
            'category': 'realistic',
            'description': 'Medium complexity (nested objects, arrays)'
        },
        'large': {
            'doc': {
                "user": {
                    "id": 12345,
                    "username": "john_doe_2025",
                    "email": "john.doe@example.com",
                    "profile": {
                        "firstName": "John",
                        "lastName": "Doe",
                        "age": 32,
                        "verified": True,
                        "bio": "Software engineer passionate about distributed systems.",
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
                        "content": "Sample post content that makes it realistic.",
                        "tags": ["tag1", "tag2", "tag3"],
                        "likes": i * 10,
                        "comments": i * 2
                    }
                    for i in range(10)
                ],
                "followers": [{"id": i, "name": f"user_{i}"} for i in range(20)],
                "statistics": {"total_posts": 10, "total_likes": 550, "follower_count": 20}
            },
            'category': 'realistic',
            'description': 'Large document (10 posts, 20 followers)'
        },
        'array_small': {
            'doc': {
                "data": [
                    {"id": i, "value": f"item_{i}", "score": i * 1.5, "active": i % 2 == 0}
                    for i in range(10)
                ],
                "metadata": {"count": 10}
            },
            'category': 'array-heavy',
            'description': 'Small array (10 items)'
        },
        'array_medium': {
            'doc': {
                "data": [
                    {"id": i, "value": f"item_{i}", "score": i * 1.5, "active": i % 2 == 0}
                    for i in range(100)
                ],
                "metadata": {"count": 100}
            },
            'category': 'array-heavy',
            'description': 'Medium array (100 items)'
        },
        'array_large': {
            'doc': {
                "data": [
                    {"id": i, "value": f"item_{i}", "score": i * 1.5, "active": i % 2 == 0}
                    for i in range(1000)
                ],
                "metadata": {"count": 1000}
            },
            'category': 'array-heavy',
            'description': 'Large array (1000 items)'
        },
        'nested_moderate': {
            'doc': _create_nested(5),
            'category': 'nested',
            'description': 'Moderately nested (5 levels)'
        },
        'nested_deep': {
            'doc': _create_nested(20),
            'category': 'nested',
            'description': 'Deeply nested (20 levels)'
        },
        'homogeneous_u8_small': {
            'doc': {
                "data": [i % 256 for i in range(100)],
                "type": "u8"
            },
            'category': 'homogeneous',
            'description': 'Small homogeneous u8 array (100 items, values 0-255)'
        },
        'homogeneous_u8_large': {
            'doc': {
                "data": [i % 256 for i in range(10000)],
                "type": "u8"
            },
            'category': 'homogeneous',
            'description': 'Large homogeneous u8 array (10K items, values 0-255)'
        },
        'homogeneous_float_small': {
            'doc': {
                "data": [float(i) * 1.5 for i in range(100)],
                "type": "float"
            },
            'category': 'homogeneous',
            'description': 'Small homogeneous float array (100 items)'
        },
        'homogeneous_float_large': {
            'doc': {
                "data": [float(i) * 1.5 for i in range(10000)],
                "type": "float"
            },
            'category': 'homogeneous',
            'description': 'Large homogeneous float array (10K items)'
        },
        'homogeneous_int_small': {
            'doc': {
                "data": [i for i in range(100)],
                "type": "int"
            },
            'category': 'homogeneous',
            'description': 'Small homogeneous int array (100 items)'
        },
        'homogeneous_int_large': {
            'doc': {
                "data": [i for i in range(10000)],
                "type": "int"
            },
            'category': 'homogeneous',
            'description': 'Large homogeneous int array (10K items)'
        }
    }

def _create_nested(depth, current=0):
    """Helper to create nested documents"""
    if current >= depth:
        return {"value": "leaf", "id": current}
    return {
        "level": current,
        "data": f"level_{current}_data",
        "nested": _create_nested(depth, current + 1)
    }

# =============================================================================
# MEASUREMENT & REPORTING
# =============================================================================

def measure_document(env, name, doc_info):
    """Measure memory usage for a document"""
    doc = doc_info['doc']
    json_str = json.dumps(doc)
    string_bytes = len(json_str.encode('utf-8'))
    
    # Store with JSON.SET
    key_json = f"nightly:json:{name}"
    env.cmd('JSON.SET', key_json, '.', json_str)
    
    # Store with regular SET
    key_string = f"nightly:string:{name}"
    env.cmd('SET', key_string, json_str)
    
    # Measure memory
    memory_json = env.cmd('MEMORY', 'USAGE', key_json)
    memory_string = env.cmd('MEMORY', 'USAGE', key_string)
    
    # Calculate metrics
    json_overhead_bytes = memory_json - string_bytes
    json_overhead_ratio = memory_json / string_bytes
    string_overhead_bytes = memory_string - string_bytes
    string_overhead_ratio = memory_string / string_bytes
    json_vs_string_ratio = memory_json / memory_string
    
    # Cleanup
    env.cmd('DEL', key_json, key_string)
    
    return {
        'name': name,
        'category': doc_info['category'],
        'description': doc_info['description'],
        'string_bytes': string_bytes,
        'memory_json': memory_json,
        'memory_string': memory_string,
        'json_overhead_bytes': json_overhead_bytes,
        'json_overhead_ratio': json_overhead_ratio,
        'string_overhead_bytes': string_overhead_bytes,
        'string_overhead_ratio': string_overhead_ratio,
        'json_vs_string_ratio': json_vs_string_ratio
    }

def print_summary_header():
    """Print report header with metadata"""
    print("\n" + "="*100)
    print("REDISJSON NIGHTLY MEMORY OVERHEAD REPORT")
    print("="*100)
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"Version: {os.getenv('REDISJSON_VERSION', 'unknown')}")
    print(f"Commit: {os.getenv('GIT_COMMIT', 'unknown')}")
    print(f"Branch: {os.getenv('GIT_BRANCH', 'unknown')}")
    print(f"Build: {os.getenv('BUILD_NUMBER', 'unknown')}")
    print("="*100 + "\n")

def print_key_metrics(results):
    """Print key headline metrics"""
    print("KEY METRICS (Headline Numbers)")
    print("-"*100)
    
    # Calculate weighted average based on realistic usage
    # Weight: small=20%, medium=30%, large=50%
    small = next(r for r in results if r['name'] == 'small')
    medium = next(r for r in results if r['name'] == 'medium')
    large = next(r for r in results if r['name'] == 'large')
    
    weighted_avg = (
        small['json_vs_string_ratio'] * 0.2 +
        medium['json_vs_string_ratio'] * 0.3 +
        large['json_vs_string_ratio'] * 0.5
    )
    
    print(f"\n📊 OVERALL OVERHEAD: {weighted_avg:.2f}x")
    print(f"   (RedisJSON uses {weighted_avg:.2f}x more memory than regular Redis strings)")
    print(f"\n📈 BY DOCUMENT SIZE:")
    print(f"   • Small documents:  {small['json_vs_string_ratio']:.2f}x overhead")
    print(f"   • Medium documents: {medium['json_vs_string_ratio']:.2f}x overhead")
    print(f"   • Large documents:  {large['json_vs_string_ratio']:.2f}x overhead")
    
    # Find best and worst cases
    best = min(results, key=lambda r: r['json_vs_string_ratio'])
    worst = max(results, key=lambda r: r['json_vs_string_ratio'])
    
    print(f"\n✅ BEST CASE:  {best['name']:<20} {best['json_vs_string_ratio']:.2f}x ({best['description']})")
    print(f"❌ WORST CASE: {worst['name']:<20} {worst['json_vs_string_ratio']:.2f}x ({worst['description']})")
    print("\n" + "-"*100 + "\n")

def print_detailed_table(results):
    """Print detailed results table"""
    print("DETAILED BREAKDOWN")
    print("-"*100)
    print(f"\n{'Document':<20} {'Size':<12} {'JSON Mem':<12} {'String Mem':<12} {'Overhead':<12} {'Category':<15}")
    print("-"*100)
    
    for r in results:
        print(f"{r['name']:<20} "
              f"{r['string_bytes']:>10,}B "
              f"{r['memory_json']:>10,}B "
              f"{r['memory_string']:>10,}B "
              f"{r['json_vs_string_ratio']:>10.2f}x "
              f"{r['category']:<15}")
    
    print("-"*100 + "\n")

def print_category_summary(results):
    """Print summary by category"""
    print("SUMMARY BY CATEGORY")
    print("-"*100)
    
    categories = {}
    for r in results:
        cat = r['category']
        if cat not in categories:
            categories[cat] = []
        categories[cat].append(r)
    
    for cat, items in sorted(categories.items()):
        avg_overhead = sum(r['json_vs_string_ratio'] for r in items) / len(items)
        print(f"\n{cat.upper()}:")
        print(f"  Average overhead: {avg_overhead:.2f}x")
        print(f"  Documents tested: {len(items)}")
        for r in items:
            print(f"    • {r['name']:<20} {r['json_vs_string_ratio']:.2f}x  ({r['description']})")
    
    print("\n" + "-"*100 + "\n")

def print_machine_readable_metrics(results):
    """Print metrics in machine-readable format for CI/CD collection"""
    print("MACHINE-READABLE METRICS (for automated collection)")
    print("-"*100)
    print("# Format: METRIC:<metric_name>:<value>")
    print("# These can be parsed and sent to monitoring systems")
    print()
    
    # Calculate key metrics
    small = next(r for r in results if r['name'] == 'small')
    medium = next(r for r in results if r['name'] == 'medium')
    large = next(r for r in results if r['name'] == 'large')
    
    weighted_avg = (
        small['json_vs_string_ratio'] * 0.2 +
        medium['json_vs_string_ratio'] * 0.3 +
        large['json_vs_string_ratio'] * 0.5
    )
    
    # Overall metrics
    print(f"METRIC:overall_overhead_ratio:{weighted_avg:.4f}")
    print(f"METRIC:small_doc_overhead:{small['json_vs_string_ratio']:.4f}")
    print(f"METRIC:medium_doc_overhead:{medium['json_vs_string_ratio']:.4f}")
    print(f"METRIC:large_doc_overhead:{large['json_vs_string_ratio']:.4f}")
    
    # Per-document metrics
    for r in results:
        prefix = r['name']
        print(f"METRIC:{prefix}_string_bytes:{r['string_bytes']}")
        print(f"METRIC:{prefix}_json_memory:{r['memory_json']}")
        print(f"METRIC:{prefix}_string_memory:{r['memory_string']}")
        print(f"METRIC:{prefix}_overhead_ratio:{r['json_vs_string_ratio']:.4f}")
    
    # Category averages
    categories = {}
    for r in results:
        cat = r['category']
        if cat not in categories:
            categories[cat] = []
        categories[cat].append(r)
    
    for cat, items in categories.items():
        avg = sum(r['json_vs_string_ratio'] for r in items) / len(items)
        print(f"METRIC:category_{cat}_avg_overhead:{avg:.4f}")
    
    print()
    print("-"*100 + "\n")

def print_json_output(results):
    """Print results in JSON format for programmatic consumption"""
    print("JSON OUTPUT (for programmatic consumption)")
    print("-"*100)
    
    output = {
        'timestamp': datetime.now().isoformat(),
        'version': os.getenv('REDISJSON_VERSION', 'unknown'),
        'commit': os.getenv('GIT_COMMIT', 'unknown'),
        'branch': os.getenv('GIT_BRANCH', 'unknown'),
        'build': os.getenv('BUILD_NUMBER', 'unknown'),
        'results': results,
        'summary': {
            'overall_overhead': round(
                sum(r['json_vs_string_ratio'] for r in results if r['name'] in ['small', 'medium', 'large']) / 3,
                2
            ),
            'best_case': min(results, key=lambda r: r['json_vs_string_ratio'])['name'],
            'worst_case': max(results, key=lambda r: r['json_vs_string_ratio'])['name']
        }
    }
    
    print(json.dumps(output, indent=2))
    print()
    print("-"*100 + "\n")

def print_trend_comparison(results, baseline_file=None):
    """Compare with baseline/previous results if available"""
    if not baseline_file or not os.path.exists(baseline_file):
        print("TREND COMPARISON")
        print("-"*100)
        print("No baseline file provided or found. Skipping trend comparison.")
        print("To enable trend comparison, set BASELINE_FILE environment variable.")
        print("-"*100 + "\n")
        return
    
    try:
        with open(baseline_file, 'r') as f:
            baseline = json.load(f)
        
        print("TREND COMPARISON (vs baseline)")
        print("-"*100)
        print(f"Baseline: {baseline.get('timestamp', 'unknown')} (commit: {baseline.get('commit', 'unknown')})")
        print()
        print(f"{'Document':<20} {'Current':<12} {'Baseline':<12} {'Change':<12} {'Status':<10}")
        print("-"*100)
        
        baseline_results = {r['name']: r for r in baseline.get('results', [])}
        
        for r in results:
            name = r['name']
            current = r['json_vs_string_ratio']
            
            if name in baseline_results:
                baseline_val = baseline_results[name]['json_vs_string_ratio']
                change = current - baseline_val
                change_pct = (change / baseline_val) * 100
                
                if abs(change_pct) < 1:
                    status = "✓ STABLE"
                elif change < 0:
                    status = "✓ BETTER"
                else:
                    status = "⚠ HIGHER"
                
                print(f"{name:<20} {current:>10.2f}x {baseline_val:>10.2f}x "
                      f"{change:>+9.2f}x {status:<10}")
            else:
                print(f"{name:<20} {current:>10.2f}x {'N/A':<12} {'N/A':<12} {'NEW':<10}")
        
        print("-"*100 + "\n")
        
    except Exception as e:
        print(f"Error loading baseline: {e}")
        print("-"*100 + "\n")

def save_results(results, output_file):
    """Save results to file for future comparison"""
    output = {
        'timestamp': datetime.now().isoformat(),
        'version': os.getenv('REDISJSON_VERSION', 'unknown'),
        'commit': os.getenv('GIT_COMMIT', 'unknown'),
        'branch': os.getenv('GIT_BRANCH', 'unknown'),
        'build': os.getenv('BUILD_NUMBER', 'unknown'),
        'results': results
    }
    
    with open(output_file, 'w') as f:
        json.dump(output, f, indent=2)
    
    print(f"Results saved to: {output_file}")

# =============================================================================
# TEST FUNCTION
# =============================================================================

def test_nightly_memory_report(env):
    """
    Generate comprehensive nightly memory overhead report.
    
    This test always passes - it's for reporting only.
    
    Environment Variables:
        BASELINE_FILE: Path to baseline JSON for comparison
        OUTPUT_FILE: Path to save results (default: memory_report.json)
        REPORT_FORMAT: Format (all|summary|metrics|json) default: all
    """
    env.skipOnCluster()
    
    # Get configuration from environment
    baseline_file = os.getenv('BASELINE_FILE')
    output_file = os.getenv('OUTPUT_FILE', 'memory_report.json')
    report_format = os.getenv('REPORT_FORMAT', 'all')
    
    # Measure all documents
    test_docs = get_test_documents()
    results = []
    
    for name, doc_info in test_docs.items():
        result = measure_document(env, name, doc_info)
        results.append(result)
    
    # Generate report based on format
    if report_format in ['all', 'summary']:
        print_summary_header()
        print_key_metrics(results)
        print_detailed_table(results)
        print_category_summary(results)
        print_trend_comparison(results, baseline_file)
    
    if report_format in ['all', 'metrics']:
        print_machine_readable_metrics(results)
    
    if report_format in ['all', 'json']:
        print_json_output(results)
    
    # Save results for future comparison
    save_results(results, output_file)
    
    print("="*100)
    print("REPORT COMPLETE")
    print("="*100)
    print(f"\n✅ Report generated successfully")
    print(f"📁 Results saved to: {output_file}")
    print(f"💡 Use this file as baseline: BASELINE_FILE={output_file}")
    print("\n")
    
    # Test always passes - it's just for reporting
    env.assertTrue(True)


def test_nightly_memory_report_summary_only(env):
    """
    Generate summary-only report (faster, for quick checks).
    Includes homogeneous arrays to track optimization.
    """
    env.skipOnCluster()
    
    # Measure key documents including homogeneous arrays
    all_docs = get_test_documents()
    key_docs = {
        'small': all_docs['small'],
        'medium': all_docs['medium'],
        'large': all_docs['large'],
        'homogeneous_u8_small': all_docs['homogeneous_u8_small'],
        'homogeneous_u8_large': all_docs['homogeneous_u8_large'],
        'homogeneous_float_large': all_docs['homogeneous_float_large'],
        'homogeneous_int_large': all_docs['homogeneous_int_large']
    }
    
    results = []
    for name, doc_info in key_docs.items():
        result = measure_document(env, name, doc_info)
        results.append(result)
    
    print_summary_header()
    print_key_metrics(results)
    print_machine_readable_metrics(results)
    
    env.assertTrue(True)
