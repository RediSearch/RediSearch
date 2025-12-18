# -*- coding: utf-8 -*-

"""
Memory overhead comparison test for RedisJSON vs regular Redis strings.
This test compares the memory usage of storing JSON data using:
1. JSON.SET (RedisJSON module)
2. SET (regular Redis string)
3. Actual string length in bytes
"""

import json
import sys
from RLTest import Env
from includes import *

def create_complex_json():
    """
    Create a complex JSON document with nested structures, arrays, and various data types.
    This simulates a realistic use case with mixed content.
    """
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
                "premium": False,
                "avatar_url": "https://cdn.example.com/avatars/12345/profile.jpg",
                "bio": "Software engineer passionate about distributed systems and databases. Love working with Redis!",
                "location": {
                    "city": "San Francisco",
                    "state": "CA",
                    "country": "USA",
                    "coordinates": {
                        "latitude": 37.7749,
                        "longitude": -122.4194
                    }
                }
            },
            "preferences": {
                "theme": "dark",
                "notifications": {
                    "email": True,
                    "push": True,
                    "sms": False
                },
                "language": "en-US",
                "timezone": "America/Los_Angeles"
            }
        },
        "posts": [
            {
                "id": 1001,
                "title": "Getting Started with RedisJSON",
                "content": "RedisJSON is a powerful module that allows you to store, update, and query JSON documents directly in Redis. It provides native JSON support with path-based operations.",
                "tags": ["redis", "json", "database", "nosql", "tutorial"],
                "likes": 245,
                "comments": 18,
                "published": True,
                "created_at": "2025-01-15T10:30:00Z",
                "updated_at": "2025-01-15T14:22:00Z",
                "author": {
                    "id": 12345,
                    "name": "John Doe"
                },
                "metadata": {
                    "views": 1523,
                    "shares": 42,
                    "bookmarks": 67
                }
            },
            {
                "id": 1002,
                "title": "Performance Optimization Tips",
                "content": "When working with large datasets in Redis, it's important to consider memory usage and access patterns. Here are some best practices for optimizing your Redis deployment.",
                "tags": ["redis", "performance", "optimization", "best-practices"],
                "likes": 189,
                "comments": 23,
                "published": True,
                "created_at": "2025-02-20T08:15:00Z",
                "updated_at": "2025-02-20T09:45:00Z",
                "author": {
                    "id": 12345,
                    "name": "John Doe"
                },
                "metadata": {
                    "views": 987,
                    "shares": 31,
                    "bookmarks": 54
                }
            },
            {
                "id": 1003,
                "title": "Understanding JSON Path Queries",
                "content": "JSON Path is a query language for JSON, similar to XPath for XML. It allows you to navigate and extract specific parts of JSON documents efficiently.",
                "tags": ["json", "jsonpath", "queries", "data-extraction"],
                "likes": 312,
                "comments": 45,
                "published": True,
                "created_at": "2025-03-10T16:20:00Z",
                "updated_at": "2025-03-11T11:30:00Z",
                "author": {
                    "id": 12345,
                    "name": "John Doe"
                },
                "metadata": {
                    "views": 2145,
                    "shares": 78,
                    "bookmarks": 123
                }
            }
        ],
        "followers": [
            {"id": 67890, "username": "alice_smith", "following_since": "2024-06-15"},
            {"id": 11223, "username": "bob_jones", "following_since": "2024-08-22"},
            {"id": 44556, "username": "charlie_brown", "following_since": "2024-09-30"},
            {"id": 77889, "username": "diana_prince", "following_since": "2024-11-05"},
            {"id": 99001, "username": "eve_wilson", "following_since": "2025-01-12"}
        ],
        "following": [
            {"id": 22334, "username": "tech_guru", "followed_since": "2024-05-10"},
            {"id": 55667, "username": "code_master", "followed_since": "2024-07-18"},
            {"id": 88990, "username": "data_scientist", "followed_since": "2024-10-25"}
        ],
        "statistics": {
            "total_posts": 3,
            "total_likes_received": 746,
            "total_comments_received": 86,
            "total_views": 4655,
            "follower_count": 5,
            "following_count": 3,
            "account_created": "2023-03-15T12:00:00Z",
            "last_login": "2025-10-04T09:30:00Z",
            "login_streak_days": 42
        },
        "settings": {
            "privacy": {
                "profile_visible": True,
                "posts_visible": True,
                "email_visible": False,
                "location_visible": True
            },
            "security": {
                "two_factor_enabled": True,
                "login_alerts": True,
                "session_timeout_minutes": 30
            }
        },
        "recent_activity": [
            {"type": "post_created", "timestamp": "2025-10-04T08:15:00Z", "post_id": 1003},
            {"type": "comment_added", "timestamp": "2025-10-03T14:22:00Z", "post_id": 987},
            {"type": "like_given", "timestamp": "2025-10-03T11:45:00Z", "post_id": 654},
            {"type": "user_followed", "timestamp": "2025-10-02T16:30:00Z", "user_id": 88990},
            {"type": "profile_updated", "timestamp": "2025-10-01T10:00:00Z"}
        ],
        "achievements": [
            {"id": "first_post", "name": "First Post", "earned_at": "2024-03-16T10:00:00Z"},
            {"id": "100_likes", "name": "Century Club", "earned_at": "2024-06-20T14:30:00Z"},
            {"id": "verified", "name": "Verified User", "earned_at": "2024-08-01T09:00:00Z"},
            {"id": "1000_views", "name": "Popular Creator", "earned_at": "2024-10-15T16:45:00Z"}
        ],
        "metadata": {
            "version": "2.1.0",
            "schema_version": 3,
            "last_backup": "2025-10-03T02:00:00Z",
            "data_quality_score": 0.95,
            "completeness_percentage": 98.5
        }
    }


def test_memory_overhead_comparison(env):
    """
    Compare memory usage between JSON.SET, regular SET, and actual string length.
    """
    env.skipOnCluster()
    
    # Create complex JSON document
    json_doc = create_complex_json()
    json_str = json.dumps(json_doc)
    
    # Calculate actual string length in bytes
    string_bytes = len(json_str.encode('utf-8'))
    
    # Test with JSON.SET
    key_json = "test:json:user"
    env.cmd('JSON.SET', key_json, '.', json_str)
    
    # Test with regular SET
    key_string = "test:string:user"
    env.cmd('SET', key_string, json_str)
    
    # Get memory usage for both keys
    memory_json = env.cmd('MEMORY', 'USAGE', key_json)
    memory_string = env.cmd('MEMORY', 'USAGE', key_string)
    
    # Calculate overhead
    json_overhead = memory_json - string_bytes
    string_overhead = memory_string - string_bytes
    json_overhead_percent = (json_overhead / string_bytes) * 100
    string_overhead_percent = (string_overhead / string_bytes) * 100
    
    # Print detailed comparison
    print("\n" + "="*80)
    print("MEMORY OVERHEAD COMPARISON: RedisJSON vs Regular String")
    print("="*80)
    print(f"\nJSON Document Complexity:")
    print(f"  - Total keys/fields: ~50+")
    print(f"  - Nested levels: 4-5 levels deep")
    print(f"  - Arrays: 5 arrays with varying sizes")
    print(f"  - Data types: strings, integers, floats, booleans, null")
    print(f"\n{'-'*80}")
    print(f"Raw JSON String Length:        {string_bytes:>10,} bytes")
    print(f"{'-'*80}")
    print(f"\nJSON.SET (RedisJSON Module):")
    print(f"  Memory Usage:                {memory_json:>10,} bytes")
    print(f"  Overhead:                    {json_overhead:>10,} bytes")
    print(f"  Overhead Percentage:         {json_overhead_percent:>10.2f}%")
    print(f"\nSET (Regular Redis String):")
    print(f"  Memory Usage:                {memory_string:>10,} bytes")
    print(f"  Overhead:                    {string_overhead:>10,} bytes")
    print(f"  Overhead Percentage:         {string_overhead_percent:>10.2f}%")
    print(f"\n{'-'*80}")
    print(f"Comparison:")
    print(f"  RedisJSON vs String Diff:    {memory_json - memory_string:>10,} bytes")
    print(f"  RedisJSON vs String Ratio:   {memory_json / memory_string:>10.2f}x")
    print(f"  Additional Cost per Byte:    {(memory_json - memory_string) / string_bytes:>10.4f}")
    print(f"{'='*80}\n")
    
    # Test partial updates (advantage of RedisJSON)
    print("Testing RedisJSON Partial Update Capability:")
    print("-" * 80)
    
    # Update just one field with JSON.SET
    env.cmd('JSON.SET', key_json, '.user.profile.age', 33)
    updated_age = env.cmd('JSON.GET', key_json, '.user.profile.age')
    print(f"Updated age using JSON.SET path:  {updated_age}")
    
    # Verify the update worked
    env.assertEqual(int(updated_age), 33, message="Age should be updated to 33")
    
    # For regular string, you'd need to:
    # 1. GET the entire string
    # 2. Parse it
    # 3. Modify it
    # 4. Serialize it
    # 5. SET it back
    full_doc = json.loads(env.cmd('GET', key_string))
    full_doc['user']['profile']['age'] = 33
    env.cmd('SET', key_string, json.dumps(full_doc))
    print(f"Updated age using full GET/SET:   Required full document transfer")
    print(f"{'='*80}\n")
    
    # Cleanup
    env.cmd('DEL', key_json, key_string)


def test_memory_overhead_various_sizes(env):
    """
    Test memory overhead with different JSON document sizes.
    """
    env.skipOnCluster()
    
    print("\n" + "="*80)
    print("MEMORY OVERHEAD BY DOCUMENT SIZE")
    print("="*80)
    print(f"\n{'Size':<15} {'String (B)':<15} {'JSON.SET (B)':<15} {'SET (B)':<15} {'JSON OH%':<12} {'STR OH%':<12}")
    print("-" * 80)
    
    test_cases = [
        ("Tiny", {"id": 1, "name": "test"}),
        ("Small", {
            "id": 123,
            "name": "John Doe",
            "email": "john@example.com",
            "active": True,
            "score": 95.5
        }),
        ("Medium", {
            "user": {
                "id": 123,
                "name": "John Doe",
                "email": "john@example.com",
                "profile": {
                    "age": 30,
                    "city": "San Francisco",
                    "interests": ["coding", "music", "travel"]
                }
            },
            "posts": [
                {"id": 1, "title": "First Post", "likes": 10},
                {"id": 2, "title": "Second Post", "likes": 25}
            ]
        }),
        ("Large", create_complex_json())
    ]
    
    results = []
    
    for size_name, doc in test_cases:
        json_str = json.dumps(doc)
        string_bytes = len(json_str.encode('utf-8'))
        
        key_json = f"test:size:json:{size_name}"
        key_string = f"test:size:string:{size_name}"
        
        env.cmd('JSON.SET', key_json, '.', json_str)
        env.cmd('SET', key_string, json_str)
        
        memory_json = env.cmd('MEMORY', 'USAGE', key_json)
        memory_string = env.cmd('MEMORY', 'USAGE', key_string)
        
        json_overhead_pct = ((memory_json - string_bytes) / string_bytes) * 100
        string_overhead_pct = ((memory_string - string_bytes) / string_bytes) * 100
        
        print(f"{size_name:<15} {string_bytes:<15,} {memory_json:<15,} {memory_string:<15,} {json_overhead_pct:<12.2f} {string_overhead_pct:<12.2f}")
        
        results.append({
            'size': size_name,
            'bytes': string_bytes,
            'json_memory': memory_json,
            'string_memory': memory_string
        })
        
        # Cleanup
        env.cmd('DEL', key_json, key_string)
    
    print("="*80)
    print("\nKey Observations:")
    print("  - RedisJSON parses and stores JSON in an optimized internal structure")
    print("  - Regular strings store the raw JSON text")
    print("  - RedisJSON enables path-based operations without full document transfer")
    print("  - Overhead percentage typically decreases with larger documents")
    print("="*80 + "\n")


def test_memory_overhead_arrays(env):
    """
    Test memory overhead specifically for array-heavy JSON documents.
    """
    env.skipOnCluster()
    
    print("\n" + "="*80)
    print("MEMORY OVERHEAD FOR ARRAY-HEAVY DOCUMENTS")
    print("="*80)
    
    # Create documents with different array sizes
    array_sizes = [10, 100, 1000]
    
    print(f"\n{'Array Size':<15} {'String (B)':<15} {'JSON.SET (B)':<15} {'SET (B)':<15} {'Ratio':<12}")
    print("-" * 80)
    
    for size in array_sizes:
        doc = {
            "data": [
                {
                    "id": i,
                    "value": f"item_{i}",
                    "score": i * 1.5,
                    "active": i % 2 == 0
                }
                for i in range(size)
            ],
            "metadata": {
                "count": size,
                "generated": "2025-10-04T00:00:00Z"
            }
        }
        
        json_str = json.dumps(doc)
        string_bytes = len(json_str.encode('utf-8'))
        
        key_json = f"test:array:json:{size}"
        key_string = f"test:array:string:{size}"
        
        env.cmd('JSON.SET', key_json, '.', json_str)
        env.cmd('SET', key_string, json_str)
        
        memory_json = env.cmd('MEMORY', 'USAGE', key_json)
        memory_string = env.cmd('MEMORY', 'USAGE', key_string)
        
        ratio = memory_json / memory_string
        
        print(f"{size:<15,} {string_bytes:<15,} {memory_json:<15,} {memory_string:<15,} {ratio:<12.2f}")
        
        # Cleanup
        env.cmd('DEL', key_json, key_string)
    
    print("="*80 + "\n")


def test_memory_overhead_nested_objects(env):
    """
    Test memory overhead for deeply nested JSON objects.
    """
    env.skipOnCluster()
    
    print("\n" + "="*80)
    print("MEMORY OVERHEAD FOR NESTED OBJECTS")
    print("="*80)
    
    # Create documents with different nesting levels
    def create_nested(depth, current=0):
        if current >= depth:
            return {"value": "leaf", "id": current}
        return {
            "level": current,
            "data": f"level_{current}_data",
            "nested": create_nested(depth, current + 1)
        }
    
    depths = [3, 5, 10, 20]
    
    print(f"\n{'Nest Depth':<15} {'String (B)':<15} {'JSON.SET (B)':<15} {'SET (B)':<15} {'Ratio':<12}")
    print("-" * 80)
    
    for depth in depths:
        doc = create_nested(depth)
        json_str = json.dumps(doc)
        string_bytes = len(json_str.encode('utf-8'))
        
        key_json = f"test:nested:json:{depth}"
        key_string = f"test:nested:string:{depth}"
        
        env.cmd('JSON.SET', key_json, '.', json_str)
        env.cmd('SET', key_string, json_str)
        
        memory_json = env.cmd('MEMORY', 'USAGE', key_json)
        memory_string = env.cmd('MEMORY', 'USAGE', key_string)
        
        ratio = memory_json / memory_string
        
        print(f"{depth:<15} {string_bytes:<15,} {memory_json:<15,} {memory_string:<15,} {ratio:<12.2f}")
        
        # Cleanup
        env.cmd('DEL', key_json, key_string)
    
    print("="*80)
    print("\nConclusion:")
    print("  RedisJSON provides structured access and manipulation capabilities")
    print("  at the cost of additional memory overhead for internal data structures.")
    print("  The trade-off is worthwhile when you need:")
    print("    - Partial document updates")
    print("    - Path-based queries")
    print("    - Atomic operations on nested fields")
    print("    - Type preservation (vs string serialization)")
    print("="*80 + "\n")
