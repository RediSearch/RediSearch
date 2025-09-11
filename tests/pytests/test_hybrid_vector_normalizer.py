from RLTest import Env
from includes import *
from common import *
import scipy.spatial

SCORE_FIELD = "__score"

"""
Test data with deterministic vectors for distance metric testing.
"""
epsilon = 1e-3
test_data = {
    'doc:1': {
        'description': "red shoes",
        'embedding': np.array([0.0 + epsilon, 0.0 + epsilon]).astype(np.float32).tobytes()
    },
    'doc:2': {
        'description': "red running shoes",
        'embedding': np.array([1.0 + epsilon, 0.0 + epsilon]).astype(np.float32).tobytes()
    },
    'doc:3': {
        'description': "running gear",
        'embedding': np.array([0.0 + epsilon, 1.0 + epsilon]).astype(np.float32).tobytes()
    },
    'doc:4': {
        'description': "blue shoes",
        'embedding': np.array([1.0 + epsilon, 1.0 + epsilon]).astype(np.float32).tobytes()
    }
}

def setup_index_with_metric(env, metric):
    """Setup index with specified distance metric and test data"""
    dim = 2
    conn = env.getClusterConnectionIfNeeded()
    env.expect(f'FT.CREATE idx SCHEMA description TEXT embedding VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC {metric}').ok()

    # Load test data
    for doc_id, doc_data in test_data.items():
        conn.execute_command('HSET', doc_id, 'description', doc_data['description'], 'embedding', doc_data['embedding'])

"""
Distance calculation functions for different metrics.
These match the distance calculations used by RediSearch internally.
"""
def calculate_l2_distance_normalized(vec1_bytes, vec2_bytes):
    """Calculate L2 (squared euclidean) distance between two vector byte arrays"""

    """
    IMPORTANT: to save calculations, redis stores only the squared distance in the vector index,
    therefore we square the radius and numpy l2 norm to get the squared distance
    """
    def VectorNorm_L2 (distance):
        return 1.0 / (1.0 + distance)

    vec1 = np.frombuffer(vec1_bytes, dtype=np.float32)
    vec2 = np.frombuffer(vec2_bytes, dtype=np.float32)
    return VectorNorm_L2(np.linalg.norm(vec1 - vec2)**2)

def calculate_cosine_distance_normalized(vec1_bytes, vec2_bytes):
    """Calculate cosine distance between two vector byte arrays"""
    def VectorNorm_Cosine(cosine_distance):
        return (1.0 + (1.0 - cosine_distance)) / 2.0;

    vec1 = np.frombuffer(vec1_bytes, dtype=np.float32)
    vec2 = np.frombuffer(vec2_bytes, dtype=np.float32)
    return VectorNorm_Cosine(scipy.spatial.distance.cosine(vec1, vec2))

def calculate_ip_distance_normalized(vec1_bytes, vec2_bytes):
    """Calculate inner product distance between two vector byte arrays"""
    def VectorNorm_IP(dot_product):
        return (1.0 + dot_product) / 2.0

    vec1 = np.frombuffer(vec1_bytes, dtype=np.float32)
    vec2 = np.frombuffer(vec2_bytes, dtype=np.float32)
    # IP distance is 1 - dot_product
    return VectorNorm_IP(1.0 - np.dot(vec1, vec2))

# Distance calculation function mapping
DISTANCE_CALCULATORS = {
    'L2': calculate_l2_distance_normalized,
    'COSINE': calculate_cosine_distance_normalized,
    'IP': calculate_ip_distance_normalized
}

# TODO: remove once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_hybrid_vector_normalizer():
    """Test that yield_distance_as value equals __score when using COMBINE LINEAR 0 1 for all distance metrics"""

    # Test all supported distance metrics
    for metric in ['L2', 'COSINE', 'IP']:
        env = Env()
        setup_index_with_metric(env, metric)
        query_vector = np.array([0.5, 0.5]).astype(np.float32).tobytes()

        for vector_query in [['KNN', '4', 'K', '10'], ['RANGE', '4', 'RADIUS', '1']]:
            response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'green', 'VSIM', '@embedding', query_vector,
                                *vector_query, 'YIELD_DISTANCE_AS', 'vector_distance')
            results = get_results_from_hybrid_response(response)

            for doc_key in results:
                doc_result = results[doc_key]
                yielded_distance = float(doc_result['vector_distance'])

                calculate_distance_normalized = DISTANCE_CALCULATORS[metric]
                expected_distance = calculate_distance_normalized(query_vector, test_data[doc_key]['embedding'])
                env.assertAlmostEqual(yielded_distance, expected_distance, delta=1e-6)

        # Clean up for next metric
        env.expect('FT.DROPINDEX', 'idx', "DD").ok()