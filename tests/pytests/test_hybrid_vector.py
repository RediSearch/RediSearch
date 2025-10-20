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

    Coordinates:
    - doc:1: (0.0, 0.0) - "red shoes"
    - doc:2: (1.0, 0.0) - "red running shoes"
    - doc:3: (0.0, 1.0) - "running gear"
    - doc:4: (1.0, 1.0) - "blue shoes"
    - Query Vector: (1.2, 0.2)

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

# TODO: remove skip once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_hybrid_vector_direct_blob_knn():
    env = Env()
    setup_basic_index(env)
    env.assertEqual(b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e" ,np.array([1.2, 0.2]).astype(np.float32).tobytes())
    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'green', 'VSIM' ,'@embedding', b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e",\
                        'KNN', '2', 'K', '1')
    results, count = get_results_from_hybrid_response(response)
    env.assertEqual(count, len(results.keys()))
    env.assertTrue(set(results.keys()) == {"doc:2"})

# TODO: remove skip once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_hybrid_vector_direct_blob_knn_with_filter():
    env = Env()
    setup_basic_index(env)
    env.assertEqual(b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e" ,np.array([1.2, 0.2]).astype(np.float32).tobytes())
    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'green', 'VSIM' ,'@embedding', b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e",\
                        'KNN', '2', 'K', '2', 'FILTER', '@description:blue')
    results, count = get_results_from_hybrid_response(response)
    env.assertEqual(count, len(results.keys()))
    env.assertTrue(set(results.keys()) == {"doc:4"})

# TODO: remove skip once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_hybrid_vector_direct_blob_range():
    env = Env()
    setup_basic_index(env)
    env.assertEqual(b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e" ,np.array([1.2, 0.2]).astype(np.float32).tobytes())
    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'green', 'VSIM' ,'@embedding', b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e",\
                        'RANGE', '2', 'RADIUS', '1')
    results, count = get_results_from_hybrid_response(response)
    env.assertEqual(count, len(results.keys()))
    env.assertTrue(set(results.keys()) == {"doc:2", "doc:4"})

def test_hybrid_vector_direct_blob_range_with_filter():
    env = Env()
    setup_basic_index(env)
    env.assertEqual(b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e" ,np.array([1.2, 0.2]).astype(np.float32).tobytes())
    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'green', 'VSIM' ,'@embedding', b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e",\
                        'RANGE', '2', 'RADIUS', '1', 'FILTER', '@description:blue')
    results, count = get_results_from_hybrid_response(response)
    env.assertTrue(set(results.keys()) == {"doc:4"})
    env.assertEqual(count, len(results.keys()))

# TODO: remove skip once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_hybrid_vector_invalid_filter_with_weight():
    """Test that hybrid vector filter fails when it contains weight attribute"""
    env = Env()
    setup_basic_index(env)
    env.assertEqual(b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e" ,np.array([1.2, 0.2]).astype(np.float32).tobytes())
    # This should fail because weight attribute is not allowed in hybrid vector filters
    env.expect('FT.HYBRID', 'idx', 'SEARCH', 'green', 'VSIM' ,'@embedding', b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e",\
                'KNN', '2', 'K', '2', 'FILTER', '@description:blue => {$weight: 2.0}').error().contains('Weight attributes are not allowed in FT.HYBRID VSIM FILTER')

# TODO: remove skip once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_hybrid_vector_invalid_filter_with_vector():
    """Test that hybrid vector filter fails when it contains vector operations"""
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    setup_basic_index(env)
    env.assertEqual(b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e" ,np.array([1.2, 0.2]).astype(np.float32).tobytes())
    # This should fail because vector operations are not allowed in hybrid vector filters
    env.expect('FT.HYBRID', 'idx', 'SEARCH', 'green', 'VSIM' ,'@embedding', b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e",\
                'FILTER', '@embedding:[VECTOR_RANGE 0.01 $BLOB]','PARAMS', "2", "BLOB", b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e").error().contains('Vector expressions are not allowed in FT.HYBRID VSIM FILTER')


