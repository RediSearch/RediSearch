from RLTest import Env
from includes import *
from common import *

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

DEFAULT_ALPHA = 0.3
DEFAULT_BETA = 0.7

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


def test_hybrid_linear_default_weights():
    env = Env()
    setup_basic_index(env)
    query_vector = np.array([0.0, 0.0]).astype(np.float32).tobytes()

    response = env.cmd(
        'FT.HYBRID', 'idx',
        'SEARCH', 'shoes',
            'YIELD_SCORE_AS', 's_score',
        'VSIM', '@embedding', query_vector,
            'KNN', '2', 'K', '10',
            'YIELD_SCORE_AS', 'v_score',
        'COMBINE', 'LINEAR', '2',
            'YIELD_SCORE_AS', 'fused_score')
    results, _ = get_results_from_hybrid_response(response)
    env.assertGreater(len(results.keys()), 0)
    for doc_key, doc_result in results.items():
        env.assertTrue('s_score' in doc_result or 'v_score' in doc_result)
        env.assertTrue('fused_score' in doc_result)

        search_score = float(doc_result.get('s_score', 0))
        vector_score = float(doc_result.get('v_score', 0))

        fused_score = float(doc_result['fused_score'])
        calculated_score = search_score * DEFAULT_ALPHA + vector_score * DEFAULT_BETA
        env.assertAlmostEqual(fused_score, calculated_score, delta=1e-6)


def test_hybrid_linear_partial_weights():
    env = Env()
    setup_basic_index(env)
    query_vector = np.array([0.0, 0.0]).astype(np.float32).tobytes()
    query_without_combine = ['FT.HYBRID', 'idx', 'SEARCH', 'shoes', 'VSIM', '@embedding', query_vector,
                        'KNN', '2', 'K', '10']

    env.expect(*query_without_combine ,'COMBINE', 'LINEAR', '2', 'ALPHA', '0.5').error().contains('Missing value for BETA')
    env.expect(*query_without_combine ,'COMBINE', 'LINEAR', '2', 'BETA', '0.5').error().contains('Missing value for ALPHA')
    env.expect(*query_without_combine ,'COMBINE', 'LINEAR', '4', 'WINDOW', '10', 'ALPHA', '0.5').error().contains('Missing value for BETA')
    env.expect(*query_without_combine ,'COMBINE', 'LINEAR', '4', 'WINDOW', '10', 'BETA', '0.5').error().contains('Missing value for ALPHA')

def test_hybrid_linear_explicit_weights():
    env = Env()
    setup_basic_index(env)
    query_vector = np.array([0.0, 0.0]).astype(np.float32).tobytes()
    alpha = 0.1
    beta = 0.9

    response = env.cmd(
        'FT.HYBRID', 'idx',
        'SEARCH', 'shoes',
            'YIELD_SCORE_AS', 's_score',
        'VSIM', '@embedding', query_vector,
            'KNN', '2', 'K', '10',
            'YIELD_SCORE_AS', 'v_score',
        'COMBINE', 'LINEAR', '6', 'ALPHA', alpha, 'BETA', beta,
            'YIELD_SCORE_AS', 'fused_score')
    results, _ = get_results_from_hybrid_response(response)
    env.assertGreater(len(results.keys()), 0)
    for doc_key, doc_result in results.items():
        env.assertTrue('s_score' in doc_result or 'v_score' in doc_result)
        env.assertTrue('fused_score' in doc_result)

        search_score = float(doc_result.get('s_score', 0))
        vector_score = float(doc_result.get('v_score', 0))

        fused_score = float(doc_result['fused_score'])
        calculated_score = search_score * alpha + vector_score * beta
        env.assertAlmostEqual(fused_score, calculated_score, delta=1e-6)
