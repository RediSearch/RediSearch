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
        'HSET', 'doc:1',
        'text', 'yellow apples',
        'vector', np.array([0.0, 1.0]).astype(np.float32).tobytes(),
        'tag', '57-300',
        'category', 'fruit'
    )
    conn.execute_command(
        'HSET', 'doc:2',
        'text', 'red apples',
        'vector', np.array([0.0, 2.0]).astype(np.float32).tobytes(),
        'tag', '57-300',
        'category', 'fruit'
    )
    conn.execute_command(
        'HSET', 'doc:3',
        'text', 'green shoes',
        'vector', np.array([1.0, 1.0]).astype(np.float32).tobytes(),
        'tag', '57-300',
        'category', 'clothing'
    )
    conn.execute_command(
        'HSET', 'doc:4',
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
    env.assertEqual(set(results.keys()), {"doc:1", "doc:2", "doc:3"})

    response = env.cmd(
        'FT.HYBRID', 'filter_idx',
        'SEARCH', '@text:(green)',
        'VSIM', '@vector', query_vector,
        'FILTER', '@category:{"fruit"}', "COMBINE", "RRF", "2", "CONSTANT", "30",
    )
    results, _ = get_results_from_hybrid_response(response)
    env.assertEqual(set(results.keys()), {"doc:1", "doc:2", "doc:3"})

    response = env.cmd(
        'FT.HYBRID', 'filter_idx',
        'SEARCH', '@text:(green)',
        'VSIM', '@vector', query_vector,
        "COMBINE", "RRF", "2", "CONSTANT", "30", "LOAD", 2, "@__key", "@category", "FILTER", "@category==\"fruit\"",
    )
    results, _ = get_results_from_hybrid_response(response)
    env.assertEqual(set(results.keys()), {"doc:1", "doc:2"})

    response = env.cmd(
        'FT.HYBRID', 'filter_idx',
        'SEARCH', '@text:(green)',
        'VSIM', '@vector', query_vector,
        'FILTER', '@category:{"vegetable"}', "COMBINE", "RRF", "2", "CONSTANT", "30", "LOAD", 2, "@__key", "@category", "FILTER", "@category==\"fruit\"",
    )
    results, _ = get_results_from_hybrid_response(response)
    env.assertEqual(results, {})

    response = env.cmd(
        'FT.HYBRID', 'filter_idx',
        'SEARCH', '@text:(green)',
        'VSIM', '@vector', query_vector,
        'FILTER', '@category:{"vegetable"}', "LOAD", 2, "@__key", "@category", "FILTER", "@category==\"clothing\"",
    )
    results, _ = get_results_from_hybrid_response(response)
    env.assertEqual(set(results.keys()), {"doc:3"})

    response = env.cmd(
        'FT.HYBRID', 'filter_idx',
        'SEARCH', '@text:(green)',
        'VSIM', '@vector', query_vector,
        'FILTER', '@category:{"vegetable"}',
    )
    results, _ = get_results_from_hybrid_response(response)
    env.assertEqual(set(results.keys()), {"doc:3", "doc:4"})

    response = env.cmd(
        'FT.HYBRID', 'filter_idx',
        'SEARCH', '@text:(green)',
        'VSIM', '@vector', query_vector,
        'FILTER', '@category:{"vegetable"}', "FILTER", "@__key!=\"doc:3\"",
    )
    results, _ = get_results_from_hybrid_response(response)
    env.assertEqual(set(results.keys()), {"doc:4"})

    response = env.cmd(
        'FT.HYBRID', 'filter_idx',
        'SEARCH', '@text:(green)',
        'VSIM', '@vector', query_vector,
        'FILTER', '@category:{"vegetable"}', "FILTER", "@__key==\"doc:3\"",
    )
    results, _ = get_results_from_hybrid_response(response)
    env.assertEqual(set(results.keys()), {"doc:3"})

def test_hybrid_policy_errors():
    """Test that errors are returned for invalid POLICY values"""
    env = Env()
    setup_filter_test_index(env)
    query_vector = np.array([0.0, 0.2]).astype(np.float32).tobytes()

    env.expect(
        'FT.HYBRID', 'filter_idx',
        'SEARCH', '@text:(green)',
        'VSIM', '@vector', query_vector,
        'FILTER', '3','@category:{"vegetable"}', "POLICY", "INVALID_POLICY").error().contains("POLICY: Invalid value for argument")

    env.expect(
        'FT.HYBRID', 'filter_idx',
        'SEARCH', '@text:(green)',
        'VSIM', '@vector', query_vector,
        'FILTER', '5', '@category:{"vegetable"}', "POLICY", "ADHOC", "BATCH_SIZE", "100").error().contains("Error parsing vector similarity parameters: 'batch size' is irrelevant for the selected policy")

    env.expect(
        'FT.HYBRID', 'filter_idx',
        'SEARCH', '@text:(green)',
        'VSIM', '@vector', query_vector,
        'FILTER', '3', '@category:{"vegetable"}', "POLICY", "ADHOC_BF").error().contains("POLICY: Invalid value for argument")

    env.expect(
        'FT.HYBRID', 'filter_idx',
        'SEARCH', '@text:(green)',
        'VSIM', '@vector', query_vector, "RANGE", "2", "RADIUS", "2",
        'FILTER', '3', '@category:{"vegetable"}', "POLICY", "ADHOC").error().contains("Error parsing vector similarity parameters: hybrid query attributes were sent for a non-hybrid query")

    env.expect(
        'FT.HYBRID', 'filter_idx',
        'SEARCH', '@text:(green)',
        'VSIM', '@vector', query_vector, "RANGE", "2", "RADIUS", "2",
        'FILTER', '3', '@category:{"vegetable"}', "BATCH_SIZE", "5").error().contains("Error parsing vector similarity parameters: hybrid query attributes were sent for a non-hybrid query")

    env.expect(
        'FT.HYBRID', 'filter_idx',
        'SEARCH', '@text:(green)',
        'VSIM', '@vector', query_vector, "RANGE", "2", "RADIUS", "2",
        'FILTER').error().contains("Missing argument count for FILTER")

    env.expect(
        'FT.HYBRID', 'filter_idx',
        'SEARCH', '@text:(green)',
        'VSIM', '@vector', query_vector, "RANGE", "2", "RADIUS", "2",
        'FILTER', '3', '@category:{"vegetable"}').error().contains("Not enough arguments in FILTER, specified 3 but provided only 1")
