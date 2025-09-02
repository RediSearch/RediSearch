import numpy as np
from RLTest import Env
from common import *
from utils.hybrid import *

TEXT_SCORERS = ['TFIDF', 'TFIDF.DOCNORM', 'BM25', 'BM25STD', 'BM25STD.TANH',
                'BM25STD.NORM', 'DISMAX', 'DOCSCORE']

def setup_basic_index(env):
    """Setup basic index with test data"""
    conn = env.getClusterConnectionIfNeeded()
    env.expect('FT.CREATE idx SCHEMA text TEXT type TAG vector VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2').ok

    # Load test data
    texts = [
        'hello world hello again',
        'hello another world',
        'hello amazing another world'
    ]

    for i in range(len(texts)):
        # Create 9 documents: 3 with only text, 3 with only vector, and 3 with both
        conn.execute_command(
            'HSET', f'text:{i+1}',
            # Add "text" to the text to make sure we get different scores
            'text', f'{texts[i]}',
            'type', 'text'
        )
        conn.execute_command(
            'HSET', f'vector:{i+1}',
            'type', 'vector',
            'vector', np.array([i+1, 0.0]).astype(np.float32).tobytes()
        )
        conn.execute_command(
            'HSET', f'both:{i+1}',
            # Add "both" to the text to make sure we get different scores
            'text', f'both: {texts[i]}',
            'type', 'both',
            # Add 0.1 to the vector value to make sure we get different scores
            'vector', np.array([i+1.1, 0.0]).astype(np.float32).tobytes()
        )

# TODO: remove once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_rrf_knn_scorers(env: Env):
    """Test all scorers with RRF combination"""
    setup_basic_index(env)
    query_vector = np.array([1.3, 0.0]).astype(np.float32).tobytes()

    for scorer in TEXT_SCORERS:
        scenario = {
            "hybrid_query": (
                f"SEARCH '@text:(hello|text)' SCORER {scorer} "
                "VSIM @vector $BLOB "
            ),
            "search_equivalent": "@text:(hello|text)",
            "search_suffix": f"SCORER {scorer}",
            "vector_equivalent": "*=>[KNN 10 @vector $BLOB AS vector_distance]"
        }
        run_test_scenario(env, 'idx', scenario, query_vector)

def _test_linear_knn_scorers(env: Env, alpha: float, beta: float):
    """Test all scorers with KNN and linear combination"""
    setup_basic_index(env)
    query_vector = np.array([1.3, 0.0]).astype(np.float32).tobytes()

    for scorer in TEXT_SCORERS:
        scenario = {
            "hybrid_query": (
                f"SEARCH '@text:(hello|text)' SCORER {scorer} "
                "VSIM @vector $BLOB "
                f"COMBINE LINEAR {alpha} {beta}"
            ),
            "search_equivalent": "@text:(hello|text)",
            "search_suffix": f"SCORER {scorer}",
            "vector_equivalent": "*=>[KNN 10 @vector $BLOB AS vector_distance]",
            "alpha": alpha,
            "beta": beta,
            "window": 20
        }
        run_linear_test_scenario(env, 'idx', scenario, query_vector)

# TODO: remove once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_linear_knn_scorers_alpha_1_beta_0(env: Env):
    """Test all scorers with KNN and linear combination: alpha=1, beta=0"""
    _test_linear_knn_scorers(env, 1.0, 0.0)

@skip(cluster=True)
def test_linear_knn_scorers_alpha_minus_1_beta_0(env: Env):
    """Test all scorers with KNN and linear combination: alpha=1, beta=0"""
    _test_linear_knn_scorers(env, -1.0, 0.0)

# TODO: remove once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_linear_knn_scorers_alpha_0_beta_1(env: Env):
    """Test all scorers with KNN and linear combination: alpha=0, beta=1"""
    _test_linear_knn_scorers(env, 0.0, 1.0)

# TODO: remove once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_linear_knn_scorers_alpha_0_beta_minus_1(env: Env):
    """Test all scorers with KNN and linear combination: alpha=0, beta=-1"""
    _test_linear_knn_scorers(env, 0.0, -1.0)

# TODO: remove once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_linear_knn_scorers_alpha_1_beta_1(env: Env):
    """Test all scorers with KNN and linear combination: alpha=1, beta=1"""
    _test_linear_knn_scorers(env, 1.0, 1.0)

# TODO: remove once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_linear_knn_scorers_alpha_minus_1_beta_minus_1(env: Env):
    """Test all scorers with KNN and linear combination: alpha=-1, beta=-1"""
    _test_linear_knn_scorers(env, -1.0, -1.0)

def _test_linear_range_scorers(env: Env, alpha: float, beta: float):
    """Test all scorers with RANGE and linear combination"""
    setup_basic_index(env)
    query_vector = np.array([1.3, 0.0]).astype(np.float32).tobytes()

    for scorer in TEXT_SCORERS:
        scenario = {
            "hybrid_query": (
                f"SEARCH '@text:(hello|text)' SCORER {scorer} "
                "VSIM @vector $BLOB RANGE 2 RADIUS 5 "
                f"COMBINE LINEAR {alpha} {beta}"
            ),
            "search_equivalent": "@text:(hello|text)",
            "search_suffix": f"SCORER {scorer}",
            "vector_equivalent": "@vector:[VECTOR_RANGE 5 $BLOB]=>{$YIELD_DISTANCE_AS: vector_distance}",
            "alpha": alpha,
            "beta": beta,
            "window": 20
        }
        run_linear_test_scenario(env, 'idx', scenario, query_vector)

def test_linear_range_scorers_alpha_1_beta_0(env: Env):
    """Test all scorers with RANGE and linear combination: alpha=1, beta=0"""
    _test_linear_range_scorers(env, 1.0, 0.0)

def test_linear_range_scorers_alpha_minus_1_beta_0(env: Env):
    """Test all scorers with RANGE and linear combination: alpha=-1, beta=0"""
    _test_linear_range_scorers(env, -1.0, 0.0)

def test_linear_range_scorers_alpha_0_beta_1(env: Env):
    """Test all scorers with RANGE and linear combination: alpha=0, beta=1"""
    _test_linear_range_scorers(env, 0.0, 1.0)

def test_linear_range_scorers_alpha_0_beta_minus_1(env: Env):
    """Test all scorers with RANGE and linear combination: alpha=0, beta=-1"""
    _test_linear_range_scorers(env, 0.0, -1.0)

def test_linear_range_scorers_alpha_1_beta_1(env: Env):
    """Test all scorers with RANGE and linear combination: alpha=1, beta=1"""
    _test_linear_range_scorers(env, 1.0, 1.0)

def test_linear_range_scorers_alpha_minus_1_beta_minus_1(env: Env):
    """Test all scorers with RANGE and linear combination: alpha=-1, beta=-1"""
    _test_linear_range_scorers(env, -1.0, -1.0)
