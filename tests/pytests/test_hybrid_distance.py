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
    dim = 2
    conn = env.getClusterConnectionIfNeeded()
    env.expect('FT.CREATE idx SCHEMA description TEXT embedding VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2').ok

    # Load test data
    for doc_id, doc_data in test_data.items():
        conn.execute_command('HSET', doc_id, 'description', doc_data['description'], 'embedding', doc_data['embedding'])


"""
IMPORTANT: to save calculations, redis stores only the squared distance in the vector index,
therefore we square the radius and numpy l2 norm to get the squared distance
"""
def calculate_l2_distance_normalized(vec1_bytes, vec2_bytes):
    def VectorNorm_L2 (distance):
        return 1.0 / (1.0 + distance)

    """Calculate L2 distance between two vector byte arrays"""
    vec1 = np.frombuffer(vec1_bytes, dtype=np.float32)
    vec2 = np.frombuffer(vec2_bytes, dtype=np.float32)
    return VectorNorm_L2(np.linalg.norm(vec1 - vec2)**2)


# TODO: remove once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_hybrid_vector_knn_with_score():
    env = Env()
    setup_basic_index(env)
    query_vector = np.array([0.0, 0.0]).astype(np.float32).tobytes()

    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'green', 'VSIM', '@embedding', query_vector,
                        'KNN', '4', 'K', '10', 'YIELD_DISTANCE_AS', 'vector_score')
    results = get_results_from_hybrid_response(response)

    # Validate the vector_score field for all returned results
    env.assertEqual(len(results.keys()), len(test_data.keys()))

    for doc_key in results:
        doc_result = results[doc_key]
        returned_score = float(doc_result['vector_score'])
        expected_score = calculate_l2_distance_normalized(query_vector, test_data[doc_key]['embedding'])

        # Validate that the returned score matches the calculated L2 normalized distance
        env.assertAlmostEqual(returned_score, expected_score, delta=1e-6)

# TODO: remove once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_hybrid_vector_range_with_score():
    env = Env()
    setup_basic_index(env)
    query_vector = np.array([0.0, 0.0]).astype(np.float32).tobytes()
    radius = 2
    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'green', 'VSIM', '@embedding', query_vector,
                        'RANGE', '4', 'RADIUS', str(radius), 'YIELD_DISTANCE_AS', 'vector_score')
    results = get_results_from_hybrid_response(response)

    # Validate the vector_score field for all returned results
    env.assertEqual(len(results.keys()), len(test_data.keys()))

    for doc_key in results:
        doc_result = results[doc_key]
        returned_score = float(doc_result['vector_score'])
        expected_score = calculate_l2_distance_normalized(query_vector, test_data[doc_key]['embedding'])

        # Validate that the returned score matches the calculated L2 normalized distance
        env.assertAlmostEqual(returned_score, expected_score, delta=1e-6)
