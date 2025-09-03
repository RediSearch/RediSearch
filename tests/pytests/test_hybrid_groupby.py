from RLTest import Env
from includes import *
from common import *
import numpy as np
from collections import Counter

"""
FT.HYBRID GROUPBY FLOW TESTS:
===============================

VECTOR SPACE LAYOUT:
===================
Query vector at origin [0, 0]

doc:1, doc:2 and doc:3 are at distance 1 from the query vector [0,0]
doc:4, doc:5 and doc:6 are at distance 2 from the query vector [0,0]
doc:7, doc:8 and doc:9 are at distance 3 from the query vector [0,0]

This allows controlling result set sizes with RADIUS:
- RADIUS 1.5 → 3 results (distance 1)
- RADIUS 2.5 → 6 results (distances 1-2)
- RADIUS 3.5 → 9 results (distances 1-3)
- RADIUS 4.5 → 10 results (all distances)
"""

def setup_hybrid_groupby_index(env):
    """Setup index with documents at controlled distances for precise result control"""
    conn = env.getClusterConnectionIfNeeded()

    # Create index with text, category, and vector fields
    env.expect('FT.CREATE idx SCHEMA description TEXT category TAG embedding VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2').ok()

    # Place documents at specific L2 distances from query vector [0,0]
    test_docs = {
        # Distance 1 (3 docs)
        'doc:1': {'description': 'red sports car', 'category': 'automotive', 'embedding': np.array([1.0, 0.0]).astype(np.float32).tobytes()},
        'doc:2': {'description': 'red racing vehicle', 'category': 'automotive', 'embedding': np.array([0.0, 1.0]).astype(np.float32).tobytes()},
        'doc:3': {'description': 'red dress shirt', 'category': 'clothing', 'embedding': np.array([-1.0, 0.0]).astype(np.float32).tobytes()},

        # Distance 2 (3 docs)
        'doc:4': {'description': 'red winter jacket', 'category': 'clothing', 'embedding': np.array([2.0, 0.0]).astype(np.float32).tobytes()},
        'doc:5': {'description': 'red leather shoes', 'category': 'footwear', 'embedding': np.array([0.0, 2.0]).astype(np.float32).tobytes()},
        'doc:6': {'description': 'red running sneakers', 'category': 'footwear', 'embedding': np.array([-2.0, 0.0]).astype(np.float32).tobytes()},

        # Distance 3 (3 docs)
        'doc:7': {'description': 'red apple fruit', 'category': 'food', 'embedding': np.array([3.0, 0.0]).astype(np.float32).tobytes()},
        'doc:8': {'description': 'red cherry tomato', 'category': 'food', 'embedding': np.array([0.0, 3.0]).astype(np.float32).tobytes()},
        'doc:9': {'description': 'red exercise equipment', 'category': 'fitness', 'embedding': np.array([-3.0, 0.0]).astype(np.float32).tobytes()},
    }

    for doc_id, doc_data in test_docs.items():
        conn.execute_command('HSET', doc_id,
                           'description', doc_data['description'],
                           'category', doc_data['category'],
                           'embedding', doc_data['embedding'])

    return test_docs

def parse_hybrid_groupby_response(response) -> Dict[str, int]:
    res_results_index = recursive_index(response, 'results')
    res_results_index[-1] += 1
    results = access_nested_list(response, res_results_index)
    results = {
        item[1][0][1]: int(item[1][0][3]) for item in results
    }
    return results

def l2_from_bytes(a_bytes, b_bytes) -> float:
    a = np.frombuffer(a_bytes, dtype=np.float32)
    b = np.frombuffer(b_bytes, dtype=np.float32)
    return np.linalg.norm(a - b)

# TODO: remove once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_hybrid_groupby_small():
    """Test hybrid search with small result set (3 docs) + groupby"""
    env = Env()
    test_docs = setup_hybrid_groupby_index(env)

    # Search for text that doesn't appear in any document - no text search results
    search_query_with_no_results = "xyznomatch"

    # Query vector at origin [0,0], RADIUS 1.5 returns 3 docs at distance 1
    query_vector = np.array([0.0, 0.0]).astype(np.float32).tobytes()
    radius = 1
    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', search_query_with_no_results,
                       'VSIM', '@embedding', query_vector, 'RANGE', '2', 'RADIUS', str(radius), 'GROUPBY', '1', '@category', 'REDUCE', 'COUNT', '0', 'AS', 'count')

    results = parse_hybrid_groupby_response(response)
    # Distance 1 docs: doc:1, doc:2, doc:3 -> automotive, automotive, clothing
    expected_categories = Counter(doc['category'] for doc in test_docs.values() if l2_from_bytes(doc['embedding'], query_vector)**2 <= radius)
    env.assertEqual(Counter(results), expected_categories)

# TODO: remove once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_hybrid_groupby_medium():
    """Test hybrid search with medium result set (6 docs) + groupby"""
    env = Env()
    test_docs = setup_hybrid_groupby_index(env)

    # Search for text that doesn't appear in any document
    search_query_with_no_results = "xyznomatch"

    # Query vector at origin [0,0], RADIUS 2.5 returns 6 docs at distances 1-2
    query_vector = np.array([0.0, 0.0]).astype(np.float32).tobytes()
    radius = 4
    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', search_query_with_no_results,
                       'VSIM', '@embedding', query_vector, 'RANGE', '2', 'RADIUS', str(radius), 'GROUPBY', '1', '@category', 'REDUCE', 'COUNT', '0', 'AS', 'count')

    results = parse_hybrid_groupby_response(response)
    expected_categories = Counter(doc['category'] for doc in test_docs.values() if l2_from_bytes(doc['embedding'], query_vector)**2 <= radius)
    env.assertEqual(Counter(results), expected_categories)

# TODO: remove once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_hybrid_groupby_large():
    """Test hybrid search with large result set (9 docs) + groupby"""
    env = Env()
    test_docs = setup_hybrid_groupby_index(env)

    # Search for text that doesn't appear in any document
    search_query_with_no_results = "xyznomatch"

    # Query vector at origin [0,0], RADIUS 3.5 returns 9 docs at distances 1-3
    query_vector = np.array([0.0, 0.0]).astype(np.float32).tobytes()
    radius = 9
    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', search_query_with_no_results,
                       'VSIM', '@embedding', query_vector, 'RANGE', '2', 'RADIUS', str(radius), 'GROUPBY', '1', '@category', 'REDUCE', 'COUNT', '0', 'AS', 'count')

    results = parse_hybrid_groupby_response(response)
    # All 9 docs -> automotive(2), clothing(2), footwear(2), food(2), fitness(1)
    expected_categories = Counter(doc['category'] for doc in test_docs.values() if l2_from_bytes(doc['embedding'], query_vector)**2 <= radius)
    env.assertEqual(Counter(results), expected_categories)