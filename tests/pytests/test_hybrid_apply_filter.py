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
          │            │
          │            │
    doc:1 ●────────────● doc:2

    Coordinates:
    - doc:1: (0.0, 0.0) - "red shoes"
    - doc:2: (1.0, 0.0) - "red running shoes"
    - doc:3: (0.0, 1.0) - "running gear"
    - doc:4: (1.0, 1.0) - "blue shoes"

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
    env.expect('FT.CREATE idx SCHEMA description TEXT embedding VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2').ok()

    # Load test data
    for doc_id, doc_data in test_data.items():
        conn.execute_command('HSET', doc_id, 'description', doc_data['description'], 'embedding', doc_data['embedding'])

# TODO: remove once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_hybrid_apply_filter_linear():
    env = Env()
    setup_basic_index(env)
    query_vector = np.array([0, 0]).astype(np.float32).tobytes()
    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'green', 'VSIM' ,'@embedding', query_vector,\
         'COMBINE', 'LINEAR', '4', 'ALPHA', '0.0', 'BETA', '1.0', 'APPLY', '2*@__score', 'AS', 'doubled_score', 'FILTER', '@doubled_score>1')
    results, count = get_results_from_hybrid_response(response)
    env.assertTrue(set(results.keys()) == {"doc:1"})
    env.assertEqual(count, 1)

# TODO: remove once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_hybrid_apply_filter_rrf():
    env = Env()
    setup_basic_index(env)
    query_vector = test_data['doc:4']['embedding']
    search_query = "blue | shoes"
    # RRF (Reciprocal Rank Fusion) calculation with default constant k=60:
    # threshold = 2 * (1/(k + rank_search) + 1/(k + rank_vector))
    # For doc:4: rank_search = 1 (highest relevance to search query "blue | shoes")
    # For doc:4: rank_vector = 1 (closest vector match to query_vector)
    threshold = 2*(1/61 + 1/61)
    epsilon = 0.001
    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', search_query, 'VSIM' ,'@embedding', query_vector,\
        'COMBINE', 'RRF', '4', 'CONSTANT', '60', 'WINDOW', '10',
         'APPLY', '2*@__score', 'AS', 'doubled_score', 'FILTER', f'@doubled_score>{threshold - epsilon}')
    results, count = get_results_from_hybrid_response(response)
    env.assertTrue(set(results.keys()) == {"doc:4"})
    env.assertEqual(count, 1)
