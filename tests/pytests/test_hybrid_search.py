from RLTest import Env
from includes import *
from common import *

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

# TODO: remove once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_hybrid_search_invalid_query_with_vector():
    """Test that hybrid search subquery fails when it contains vector query"""
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    setup_basic_index(env)
    env.assertEqual(b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e" ,np.array([1.2, 0.2]).astype(np.float32).tobytes())
    # This should fail because vector expressions are not allowed in hybrid search subquery
    env.expect('FT.HYBRID', 'idx', 'SEARCH', '@embedding:[VECTOR_RANGE 0.01 $BLOB]', 'VSIM' ,'@embedding', b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e",\
               'PARAMS', "2", "BLOB", b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e").error().contains('Vector queries are not allowed in FT.HYBRID SEARCH')

