import numpy as np
from RLTest import Env
from common import *


def setup_filter_test_index(env):
    """Setup basic index with test data for filter testing"""
    conn = env.getClusterConnectionIfNeeded()
    env.expect(
        'FT.CREATE filter_idx SCHEMA '
        'text TEXT '
        'vector VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2 '
        'tag TAG '
        'category TAG'
    ).ok

    # Create test documents with different categories for filtering
    conn.execute_command(
        'HSET', 'doc:1{hash_tag}',
        'text', 'yellow apples',
        'vector', np.array([0.0, 1.0]).astype(np.float32).tobytes(),
        'tag', '57-300',
        'category', 'fruit'
    )
    conn.execute_command(
        'HSET', 'doc:2{hash_tag}',
        'text', 'red apples',
        'vector', np.array([0.0, 2.0]).astype(np.float32).tobytes(),
        'tag', '57-300',
        'category', 'fruit'
    )
    conn.execute_command(
        'HSET', 'doc:3{hash_tag}',
        'text', 'green shoes',
        'vector', np.array([1.0, 1.0]).astype(np.float32).tobytes(),
        'tag', '57-300',
        'category', 'clothing'
    )
    conn.execute_command(
        'HSET', 'doc:4{hash_tag}',
        'text', 'red vegetables',
        'vector', np.array([2.0, 1.0]).astype(np.float32).tobytes(),
        'tag', '57-300',
        'category', 'vegetable'
    )


def test_hybrid_filter_behavior():
    """Test that FILTER without and with COMBINE behavior in hybrid queries"""
    env = Env()
    env.expect('CONFIG', 'SET', 'search-default-dialect', 2).ok()
    setup_filter_test_index(env)
    query_vector = np.array([0.0, 0.2]).astype(np.float32).tobytes()

    response = env.cmd(
        'FT.HYBRID', 'filter_idx',
        'SEARCH', '@text:(green)',
        'VSIM', '@vector', query_vector,
        'FILTER', '@category:{"fruit"}'
    )
    results, _ = get_results_from_hybrid_response(response)
    # This should return all with fruit from vector subquery (doc:1, doc:2) and all with green text (doc:3)
    env.assertEqual(set(results.keys()), {"doc:1{hash_tag}", "doc:2{hash_tag}", "doc:3{hash_tag}"})

    response = env.cmd(
        'FT.HYBRID', 'filter_idx',
        'SEARCH', '@text:(green)',
        'VSIM', '@vector', query_vector,
        'FILTER', '@category:{"fruit"}', "COMBINE", "RRF", "2", "CONSTANT", "30",
    )
    results, _ = get_results_from_hybrid_response(response)
    # This should filter as before, just an extra combine
    env.assertEqual(set(results.keys()), {"doc:1{hash_tag}", "doc:2{hash_tag}", "doc:3{hash_tag}"})

    response = env.cmd(
        'FT.HYBRID', 'filter_idx',
        'SEARCH', '@text:(green)',
        'VSIM', '@vector', query_vector,
        "COMBINE", "RRF", "2", "CONSTANT", "30", "LOAD", 2, "__key", "category", "FILTER", "@category==\"fruit\"",
    )
    results, _ = get_results_from_hybrid_response(response)
    # This should filter as post processing.
    env.assertEqual(set(results.keys()), {"doc:1{hash_tag}", "doc:2{hash_tag}"})

    response = env.cmd(
        'FT.HYBRID', 'filter_idx',
        'SEARCH', '@text:(green)',
        'VSIM', '@vector', query_vector,
        'FILTER', '@category:{"vegetable"}', "COMBINE", "RRF", "2", "CONSTANT", "30", "LOAD", 2, "__key", "category", "FILTER", "@category==\"fruit\"",
    )
    results, _ = get_results_from_hybrid_response(response)
    env.assertEqual(results, {})

    response = env.cmd(
        'FT.HYBRID', 'filter_idx',
        'SEARCH', '@text:(green)',
        'VSIM', '@vector', query_vector,
        'FILTER', '@category:{"vegetable"}', "LOAD", 2, "__key", "category", "FILTER", "@category==\"clothing\"",
    )
    results, _ = get_results_from_hybrid_response(response)
    # This should filter as before, just an extra combine
    env.assertEqual(set(results.keys()), {"doc:3{hash_tag}"})

    response = env.cmd(
        'FT.HYBRID', 'filter_idx',
        'SEARCH', '@text:(green)',
        'VSIM', '@vector', query_vector,
        'FILTER', '@category:{"vegetable"}',
    )
    results, _ = get_results_from_hybrid_response(response)
    env.assertEqual(set(results.keys()), {'doc:3{hash_tag}', 'doc:4{hash_tag}'})

    response = env.cmd(
        'FT.HYBRID', 'filter_idx',
        'SEARCH', '@text:(green)',
        'VSIM', '@vector', query_vector,
        'FILTER', '@category:{"vegetable"}', "FILTER", "@__key!=\"doc:3{hash_tag}\"",
    )
    results, _ = get_results_from_hybrid_response(response)
    env.assertEqual(set(results.keys()), {'doc:4{hash_tag}'})

    response = env.cmd(
        'FT.HYBRID', 'filter_idx',
        'SEARCH', '@text:(green)',
        'VSIM', '@vector', query_vector,
        'FILTER', '@category:{"vegetable"}', "FILTER", "@__key==\"doc:3{hash_tag}\"",
    )
    results, _ = get_results_from_hybrid_response(response)
    # This should filter as before, just an extra combine
    env.assertEqual(set(results.keys()), {"doc:3{hash_tag}"})
