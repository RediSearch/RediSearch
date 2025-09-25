from RLTest import Env
from includes import *
from common import *
import scipy.spatial
import numpy as np
from ml_dtypes import bfloat16

SCORE_FIELD = "__score"

"""
Test data with deterministic vectors for distance metric testing.
"""

def setup_index(env, metric, algorithm, data_type):
    """Setup index with specified distance metric and test data"""
    dim = 2
    conn = env.getClusterConnectionIfNeeded()
    env.expect(f'FT.CREATE idx SCHEMA description TEXT embedding VECTOR {algorithm} 6 TYPE {data_type} DIM 2 DISTANCE_METRIC {metric}').ok()

    # Load test data
    test_data = create_test_data(data_type)
    for doc_id, doc_data in test_data.items():
        conn.execute_command('HSET', doc_id, 'description', doc_data['description'], 'embedding', doc_data['embedding'])

    return test_data

"""
Distance calculation functions for different metrics.
These match the distance calculations used by RediSearch internally.
"""
def calculate_l2_distance_normalized(vec1_bytes, vec2_bytes, data_type):
    """Calculate L2 (squared euclidean) distance between two vector byte arrays"""

    """
    IMPORTANT: to save calculations, redis stores only the squared distance in the vector index,
    therefore we square the radius and numpy l2 norm to get the squared distance
    """
    def VectorNorm_L2 (distance):
        return 1.0 / (1.0 + distance)

    # Convert bytes back to numpy arrays
    vec1 = np.frombuffer(vec1_bytes, dtype=data_type.lower())
    vec2 = np.frombuffer(vec2_bytes, dtype=data_type.lower())

    return VectorNorm_L2(np.linalg.norm(vec1 - vec2)**2)

def calculate_cosine_distance_normalized(vec1_bytes, vec2_bytes, data_type):
    """Calculate cosine distance between two vector byte arrays"""
    def VectorNorm_Cosine(cosine_distance):
        return (1.0 + (1.0 - cosine_distance)) / 2.0

    # Convert bytes back to numpy arrays
    vec1 = np.frombuffer(vec1_bytes, dtype=data_type.lower())
    vec2 = np.frombuffer(vec2_bytes, dtype=data_type.lower())

    return VectorNorm_Cosine(scipy.spatial.distance.cosine(vec1, vec2))

def calculate_ip_distance_normalized(vec1_bytes, vec2_bytes, data_type):
    """Calculate inner product distance between two vector byte arrays"""
    def VectorNorm_IP(dot_product):
        return (1.0 + dot_product) / 2.0

    # Convert bytes back to numpy arrays
    vec1 = np.frombuffer(vec1_bytes, dtype=data_type.lower())
    vec2 = np.frombuffer(vec2_bytes, dtype=data_type.lower())

    # IP distance is 1 - dot_product
    return VectorNorm_IP(1.0 - np.dot(vec1, vec2))

def create_test_data(data_type):
    """Create test data with the specified data type"""
    epsilon = 1e-3
    return {
        'doc:1': {
            'description': "red shoes",
            'embedding': np.array([0.0 + epsilon, 0.0 + epsilon], dtype=data_type.lower()).tobytes()
        },
        'doc:2': {
            'description': "red running shoes",
            'embedding': np.array([10.0 + epsilon, 0.0 + epsilon], dtype=data_type.lower()).tobytes()
        },
        'doc:3': {
            'description': "running gear",
            'embedding': np.array([0.0 + epsilon, 10.0 + epsilon], dtype=data_type.lower()).tobytes()
        },
        'doc:4': {
            'description': "blue shoes",
            'embedding': np.array([10.0 + epsilon, 10.0 + epsilon], dtype=data_type.lower()).tobytes()
        }
    }

# score calculation function mapping
SCORE_CALCULATORS = {
    'L2': calculate_l2_distance_normalized,
    'COSINE': calculate_cosine_distance_normalized,
    'IP': calculate_ip_distance_normalized
}

EPSILONS = {'FLOAT32': 1E-6, 'FLOAT64': 1E-9, 'FLOAT16': 1E-2, 'BFLOAT16': 1E-2, 'INT8': 1E-2, 'UINT8': 1E-2}

# TODO: remove once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_hybrid_vector_normalizer():
    """Test that yield_score_as value equals the expected distance for all distance metrics"""

    # Test all supported vector types
    data_types = VECSIM_DATA_TYPES + ['INT8', 'UINT8']
    metrics = ['L2', 'COSINE', 'IP']
    algorithms = ['FLAT', 'HNSW'] # TODO: Add SVS support here after FT.HYBRID is merged to master

    # Test all supported distance metrics
    for metric in metrics:
        for algorithm in algorithms:
            for data_type in data_types:
                env = Env()
                test_data = setup_index(env, metric, algorithm, data_type)
                query_vector = np.array([0.5, 0.5], dtype=data_type.lower()).tobytes()

                for vector_query in [['KNN', '4', 'K', '10'], ['RANGE', '4', 'RADIUS', '10']]:
                    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'green', 'VSIM', '@embedding', query_vector,
                                        *vector_query, 'YIELD_SCORE_AS', 'vector_score')
                    results = get_results_from_hybrid_response(response)

                    for doc_key in results:
                        doc_result = results[doc_key]
                        yielded_score = float(doc_result['vector_score'])

                        calculate_score = SCORE_CALCULATORS[metric]
                        expected_score = calculate_score(query_vector, test_data[doc_key]['embedding'], data_type)
                        env.assertAlmostEqual(yielded_score, expected_score, delta=EPSILONS[data_type])

                # Clean up for next vector type
                env.expect('FT.DROPINDEX', 'idx', "DD").ok()
