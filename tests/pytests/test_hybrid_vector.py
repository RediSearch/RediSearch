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

def test_hybrid_vector_params_numeric_args():
    """Regression test for MOD-12915: numeric VSIM args must accept $param
    placeholders via PARAMS, not just literal values. Before the fix these were
    validated as numbers during parsing (e.g. "Invalid K value") before PARAMS
    substitution, so a $param was rejected.

    Approach A (surgical) covers K and RADIUS (deferred via ParsedVectorData) and
    EF_RUNTIME and EPSILON (deferred via the VectorQuery needResolve mechanism).
    BATCH_SIZE goes through parseFilterClause/ArgParser and is NOT addressed by
    this surgical pass — it is intentionally out of scope here (see the PR body).
    """
    env = Env()
    blob = np.array([1.2, 0.2]).astype(np.float32).tobytes()

    # --- FLAT index: K (KNN) and RADIUS (RANGE) ---
    setup_basic_index(env)

    # K via $param: with K=1 the KNN side keeps only the single nearest vector.
    response = env.cmd(
        'FT.HYBRID', 'idx',
        'SEARCH', 'green',
        'VSIM', '@embedding', '$BLOB', 'KNN', '2', 'K', '$k',
        'PARAMS', '4', 'BLOB', blob, 'k', '1')
    results, count = get_results_from_hybrid_response(response)
    env.assertEqual(count, len(results.keys()))
    env.assertEqual(set(results.keys()), {"doc:2"})

    # RADIUS via $param: command must parse and return a well-formed response
    # (the bug was a parse-time rejection of the $param value).
    response = env.cmd(
        'FT.HYBRID', 'idx',
        'SEARCH', 'green',
        'VSIM', '@embedding', '$BLOB', 'RANGE', '2', 'RADIUS', '$r',
        'PARAMS', '4', 'BLOB', blob, 'r', '1')
    results, count = get_results_from_hybrid_response(response)
    env.assertEqual(count, len(results.keys()))

    env.flush()

    # --- HNSW index: EF_RUNTIME (KNN) and EPSILON (RANGE) are HNSW-only args ---
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'description', 'TEXT',
               'embedding', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32',
               'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()
    conn = env.getClusterConnectionIfNeeded()
    for doc_id, doc_data in test_data.items():
        conn.execute_command('HSET', doc_id, 'description', doc_data['description'],
                              'embedding', doc_data['embedding'])

    # EF_RUNTIME via $param
    response = env.cmd(
        'FT.HYBRID', 'idx',
        'SEARCH', 'green',
        'VSIM', '@embedding', '$BLOB', 'KNN', '4', 'K', '1', 'EF_RUNTIME', '$ef',
        'PARAMS', '4', 'BLOB', blob, 'ef', '10')
    results, count = get_results_from_hybrid_response(response)
    env.assertEqual(count, len(results.keys()))

    # EPSILON via $param
    response = env.cmd(
        'FT.HYBRID', 'idx',
        'SEARCH', 'green',
        'VSIM', '@embedding', '$BLOB', 'RANGE', '4', 'RADIUS', '1', 'EPSILON', '$e',
        'PARAMS', '4', 'BLOB', blob, 'e', '0.5')
    results, count = get_results_from_hybrid_response(response)
    env.assertEqual(count, len(results.keys()))

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


