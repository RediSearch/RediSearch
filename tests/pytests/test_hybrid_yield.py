from RLTest import Env
from includes import *
from common import *

SCORE_FIELD = "__score"

"""
VECTOR SPACE LAYOUT:
====================

The test data creates a 2D vector space with 4 documents positioned as follows:

    doc:3 ●────────────● doc:4
          │            │
          │            │
          │            │  ● Query
          │            │    Vector
    doc:1 ●────────────● doc:2
        Query
        Vector

    Coordinates:
    - doc:1: (0.0, 0.0) - "red shoes"
    - doc:2: (1.0, 0.0) - "red running shoes"
    - doc:3: (0.0, 1.0) - "running gear"
    - doc:4: (1.0, 1.0) - "blue shoes"
    - Query Vector: (0.0, 0.0)

"""

# Test data with deterministic vectors
test_data = {
    'doc:1': {
        'description': "red shoes",
        'embedding': np.array([0.0, 0.0]).astype(np.float32).tobytes()
    },
    'doc:2': {
        'description': "red running shoes",
        'embedding': np.array([1.0, 0.0]).astype(np.float32).tobytes()
    },
    'doc:3': {
        'description': "running gear",
        'embedding': np.array([0.0, 1.0]).astype(np.float32).tobytes()
    },
    'doc:4': {
        'description': "blue shoes",
        'embedding': np.array([1.0, 1.0]).astype(np.float32).tobytes()
    }
}

def setup_basic_index(env):
    """Setup basic index with test data"""
    conn = env.getClusterConnectionIfNeeded()
    env.expect('FT.CREATE idx SCHEMA description TEXT embedding VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2').ok

    # Load test data
    for doc_id, doc_data in test_data.items():
        conn.execute_command('HSET', doc_id, 'description', doc_data['description'], 'embedding', doc_data['embedding'])

def calculate_l2_distance_normalized(vec1_bytes, vec2_bytes):
    """Calculate L2 distance between two vector byte arrays and normalize"""
    def VectorNorm_L2(distance):
        return 1.0 / (1.0 + distance)

    vec1 = np.frombuffer(vec1_bytes, dtype=np.float32)
    vec2 = np.frombuffer(vec2_bytes, dtype=np.float32)
    return VectorNorm_L2(np.linalg.norm(vec1 - vec2)**2)

def calculate_l2_distance_raw(vec1_bytes, vec2_bytes):
    """Calculate raw L2 distance between two vector byte arrays"""
    vec1 = np.frombuffer(vec1_bytes, dtype=np.float32)
    vec2 = np.frombuffer(vec2_bytes, dtype=np.float32)
    return np.linalg.norm(vec1 - vec2)**2

# TODO: remove once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_hybrid_vsim_knn_yield_score_as():
    """Test VSIM KNN with YIELD_SCORE_AS parameter"""
    env = Env()
    setup_basic_index(env)
    query_vector = np.array([0.0, 0.0]).astype(np.float32).tobytes()

    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'shoes', 'VSIM', '@embedding', query_vector,
                        'KNN', '4', 'K', '10', 'YIELD_SCORE_AS', 'vector_score')
    results, _ = get_results_from_hybrid_response(response)

    # Validate the score field for all returned results
    env.assertGreater(len(results.keys()), 0)  # Should return docs with "shoes" in description

    for doc_key in results:
        doc_result = results[doc_key]
        env.assertTrue('vector_score' in doc_result)
        returned_distance = float(doc_result['vector_score'])
        expected_distance = calculate_l2_distance_normalized(query_vector, test_data[doc_key]['embedding'])
        env.assertAlmostEqual(returned_distance, expected_distance, delta=1e-6)

# TODO: remove once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_hybrid_vsim_range_yield_score_as():
    """Test VSIM RANGE with YIELD_SCORE_AS parameter"""
    env = Env()
    setup_basic_index(env)
    query_vector = np.array([0.0, 0.0]).astype(np.float32).tobytes()
    radius = 2

    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'shoes', 'VSIM', '@embedding', query_vector,
                        'RANGE', '4', 'RADIUS', str(radius), 'YIELD_SCORE_AS', 'vector_score')
    results, _ = get_results_from_hybrid_response(response)

    # Validate the vector_score field for all returned results
    env.assertGreater(len(results.keys()), 0)

    for doc_key in results:
        doc_result = results[doc_key]
        env.assertTrue('vector_score' in doc_result)
        returned_distance = float(doc_result['vector_score'])
        expected_distance = calculate_l2_distance_normalized(query_vector, test_data[doc_key]['embedding'])
        env.assertAlmostEqual(returned_distance, expected_distance, delta=1e-6)
        

# TODO: remove once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_hybrid_search_yield_score_as():
    """Test SEARCH with YIELD_SCORE_AS parameter"""
    env = Env()
    setup_basic_index(env)

    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', '*', 'YIELD_SCORE_AS', 'search_score',
                        'VSIM', '@embedding', np.array([0.0, 0.0]).astype(np.float32).tobytes())
    results, _ = get_results_from_hybrid_response(response)

    # Validate the search_score field for all returned results
    env.assertGreater(len(results.keys()), 0)

    for doc_key in results:
        doc_result = results[doc_key]
        env.assertTrue('search_score' in doc_result)
        # Search score should be a valid float
        search_score = float(doc_result['search_score'])
        env.assertGreater(search_score, 0)

# TODO: remove once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_hybrid_search_and_vsim_yield_parameters():
    """Test using SEARCH YIELD_SCORE_AS with VSIM YIELD_SCORE_AS together"""
    env = Env()
    setup_basic_index(env)
    query_vector = np.array([0.0, 0.0]).astype(np.float32).tobytes()

    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', '*', 'YIELD_SCORE_AS', 'search_score',
                        'VSIM', '@embedding', query_vector,
                        'KNN', '4', 'K', '10', 'YIELD_SCORE_AS', 'vector_distance')
    results, _ = get_results_from_hybrid_response(response)

    # Validate both search_score and vector_distance fields
    env.assertGreater(len(results.keys()), 0)

    for doc_key in results:
        doc_result = results[doc_key]
        # Should have either search_score or vector_distance (or both)
        has_search_score = 'search_score' in doc_result
        has_vector_distance = 'vector_distance' in doc_result
        env.assertTrue(has_search_score or has_vector_distance)

# TODO: remove once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_hybrid_vsim_both_yield_distance_and_score():
    """Test VSIM with both YIELD_SCORE_AS and YIELD_SCORE_AS together - should fail because YIELD_SCORE_AS is not supported in VSIM"""
    env = Env()
    setup_basic_index(env)
    query_vector = np.array([0.0, 0.0]).astype(np.float32).tobytes()

    # YIELD_SCORE_AS is not supported in VSIM clauses and should return an error
    env.expect('FT.HYBRID', 'idx', 'SEARCH', 'shoes', 'VSIM', '@embedding', query_vector,
               'KNN', '6', 'K', '10', 'YIELD_SCORE_AS', 'vector_distance', 'YIELD_SCORE_AS', 'vector_score').error()

# TODO: remove once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_hybrid_vsim_range_both_yield_distance_and_score():
    """Test VSIM RANGE with both YIELD_SCORE_AS and YIELD_SCORE_AS together - should fail because YIELD_SCORE_AS is not supported in VSIM"""
    env = Env()
    setup_basic_index(env)
    query_vector = np.array([0.0, 0.0]).astype(np.float32).tobytes()
    radius = 2

    # YIELD_SCORE_AS is not supported in VSIM clauses and should return an error
    env.expect('FT.HYBRID', 'idx', 'SEARCH', 'shoes', 'VSIM', '@embedding', query_vector,
               'RANGE', '6', 'RADIUS', str(radius), 'YIELD_SCORE_AS', 'vector_distance', 'YIELD_SCORE_AS', 'vector_score').error()
    

# TODO: remove once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_hybrid_yield_score_as_after_combine_error():
    """Test that YIELD_SCORE_AS after COMBINE keyword fails"""
    env = Env()
    setup_basic_index(env)
    query_vector = np.array([0.0, 0.0]).astype(np.float32).tobytes()

    # This should fail because YIELD_SCORE_AS appears after COMBINE
    env.expect('FT.HYBRID', 'idx', 'SEARCH', 'shoes', 'VSIM', '@embedding', query_vector,
               'KNN', '4', 'K', '10', 'COMBINE', 'RRF', '2', 'CONSTANT', '60',
               'YIELD_SCORE_AS', 'vector_distance').error()

# TODO: remove once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_hybrid_search_yield_score_as_after_combine():
    """Test that SEARCH YIELD_SCORE_AS after COMBINE keyword works"""
    env = Env()
    setup_basic_index(env)
    query_vector = np.array([0.0, 0.0]).astype(np.float32).tobytes()

    # YIELD_SCORE_AS after COMBINE should now work
    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'shoes', 'VSIM', '@embedding', query_vector,
                        'COMBINE', 'RRF', '4', 'CONSTANT', '60', 'YIELD_SCORE_AS', 'search_score')
    results, _ = get_results_from_hybrid_response(response)

    # Validate the search_score field
    env.assertGreater(len(results.keys()), 0)

    for doc_key in results:
        doc_result = results[doc_key]
        env.assertTrue('search_score' in doc_result)
        search_score = float(doc_result['search_score'])
        env.assertGreater(search_score, 0)

# TODO: remove once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_hybrid_multiple_yield_after_combine_error():
    """Test that multiple YIELD parameters after COMBINE keyword fail"""
    env = Env()
    setup_basic_index(env)
    query_vector = np.array([0.0, 0.0]).astype(np.float32).tobytes()

    # This should fail because both YIELD parameters appear after COMBINE
    env.expect('FT.HYBRID', 'idx', 'SEARCH', 'shoes', 'VSIM', '@embedding', query_vector,
               'KNN', '4', 'K', '10', 'COMBINE', 'LINEAR', '8', 'ALPHA', '0.5', 'BETA', '0.5',
               'YIELD_SCORE_AS', 'vector_distance', 'YIELD_SCORE_AS', 'vector_score').error()
