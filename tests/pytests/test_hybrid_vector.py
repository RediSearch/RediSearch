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

def setup_basic_index(env, sorted_ids=True):
    """Setup basic index with test data"""
    conn = env.getClusterConnectionIfNeeded()
    env.expect('FT.CREATE idx SCHEMA description TEXT embedding VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2').ok

    # Load test data
    if sorted_ids:
        for doc_id, doc_data in test_data.items():
            conn.execute_command(
                'HSET', doc_id, 'description', doc_data['description'],
                'embedding', doc_data['embedding'])
    else:
        for doc_id, doc_data in reversed(test_data.items()):
            conn.execute_command(
                'HSET', doc_id, 'description', doc_data['description'],
                'embedding', doc_data['embedding'])

def test_hybrid_vector_knn():
    env = Env()
    env.assertEqual(b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e" ,np.array([1.2, 0.2]).astype(np.float32).tobytes())
    for sorted_ids in [True, False]:
        setup_basic_index(env, sorted_ids)
        response = env.cmd(
            'FT.HYBRID', 'idx',
            'SEARCH', 'green',
            'VSIM' ,'@embedding', '$BLOB', 'KNN', '2', 'K', '1',
            'PARAMS', "2", "BLOB", b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e" )
        results, count = get_results_from_hybrid_response(response)
        env.assertEqual(count, len(results.keys()))
        env.assertTrue(set(results.keys()) == {"doc:2"})
        env.flush()

def test_hybrid_vector_knn_with_filter():
    env = Env()
    env.assertEqual(b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e" ,np.array([1.2, 0.2]).astype(np.float32).tobytes())
    for sorted_ids in [True, False]:
        setup_basic_index(env, sorted_ids)
        response = env.cmd(
            'FT.HYBRID', 'idx',
            'SEARCH', 'green',
            'VSIM' ,'@embedding','$BLOB', 'KNN', '2', 'K', '2',
                'FILTER', '@description:blue',
            'PARAMS', "2", "BLOB", b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e" )
        results, count = get_results_from_hybrid_response(response)
        env.assertEqual(count, len(results.keys()))
        env.assertTrue(set(results.keys()) == {"doc:4"})
        env.flush()

def test_hybrid_vector_range():
    env = Env()

    vector_and_expected_results = [
        ([1.2, 0.2], ["doc:2", "doc:4"]),
        ([0.1, 0.3], ["doc:1", "doc:3", "doc:2"])
    ]

    for sorted_ids in [True, False]:
        setup_basic_index(env, sorted_ids)
        for vector, expected_results in vector_and_expected_results:
            blob = np.array(vector).astype(np.float32).tobytes()
            response = env.cmd(
                'FT.HYBRID', 'idx',
                'SEARCH', 'green',
                'VSIM' ,'@embedding', '$BLOB',
                    'RANGE', '2', 'RADIUS', '1',
                'PARAMS', "2", "BLOB", blob)
            results, count = get_results_from_hybrid_response(response)
            env.assertEqual(count, len(results.keys()))
            # get the keys from the results
            keys = [r['__key'] for r in results.values()]
            env.assertEqual(keys, expected_results,
                            message=f"sorted_ids={sorted_ids}")
        env.flush()

def test_hybrid_vector_range_with_filter():
    env = Env()
    blob = np.array([1.2, 0.2]).astype(np.float32).tobytes()
    # Test with unsorted ids, to make sure we don't rely on sorted ids in the
    # test, which is hiding a bug in the implementation, where the order of the
    # ids were assumed to be the same as the order of the vector results.

    for sorted_ids in [True, False]:
        setup_basic_index(env, sorted_ids)

        # query 1: returns 1 result
        response = env.cmd(
            'FT.HYBRID', 'idx',
            'SEARCH', 'green',
            'VSIM' ,'@embedding', '$BLOB',
                'RANGE', '2', 'RADIUS', '1',
                'FILTER', '@description:blue',
            'PARAMS', "2", "BLOB", blob)
        results, count = get_results_from_hybrid_response(response)
        env.assertTrue(set(results.keys()) == {"doc:4"})
        env.assertEqual(count, len(results.keys()))

        # query 2: returns 2 results
        response = env.cmd(
            'FT.HYBRID', 'idx',
            'SEARCH', 'green',
            'VSIM' ,'@embedding', '$BLOB',
                'RANGE', '2', 'RADIUS', '1',
                'FILTER', '@description:running | @description:shoes',
            'PARAMS', "2", "BLOB", blob)
        results, count = get_results_from_hybrid_response(response)
        env.assertEqual(count, len(results.keys()))
        env.assertTrue(set(results.keys()) == {"doc:2", "doc:4"})

        env.flush()

def test_hybrid_vector_invalid_filter_with_weight():
    """Test that hybrid vector filter fails when it contains weight attribute"""
    env = Env()
    setup_basic_index(env)
    env.assertEqual(b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e" ,np.array([1.2, 0.2]).astype(np.float32).tobytes())
    # This should fail because weight attribute is not allowed in hybrid vector filters
    env.expect('FT.HYBRID', 'idx', 'SEARCH', 'green', 'VSIM' ,'@embedding', '$BLOB',\
                'KNN', '2', 'K', '2', 'FILTER', '@description:blue => {$weight: 2.0}', 'PARAMS', "2", "BLOB", b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e").error().contains('Weight attributes are not allowed in FT.HYBRID VSIM FILTER')

def test_hybrid_vector_invalid_filter_with_vector():
    """Test that hybrid vector filter fails when it contains vector operations"""
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    setup_basic_index(env)
    env.assertEqual(b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e" ,np.array([1.2, 0.2]).astype(np.float32).tobytes())
    # This should fail because vector operations are not allowed in hybrid vector filters
    env.expect('FT.HYBRID', 'idx', 'SEARCH', 'green', 'VSIM' ,'@embedding', '$BLOB',\
                'FILTER', '@embedding:[VECTOR_RANGE 0.01 $BLOB]','PARAMS', "2", "BLOB", b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e").error().contains('Vector expressions are not allowed in FT.HYBRID VSIM FILTER')


