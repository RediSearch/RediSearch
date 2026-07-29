from RLTest import Env
from includes import *
from common import *

SCORE_FIELD = "__score"

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
        'description': "running gear and many different shoes",
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

def setup_basic_index_hnsw(env):
    """Setup basic index with hnsw vector and load test data"""
    conn = env.getClusterConnectionIfNeeded()
    env.expect(
        'FT.CREATE', 'idx_hnsw', 'SCHEMA', 'description', 'TEXT',
        'embedding_hnsw', 'VECTOR', 'HNSW', 6, 'TYPE', 'FLOAT32', 'DIM', 2,
        'DISTANCE_METRIC', 'COSINE').ok()

    # Load test data
    for doc_id, doc_data in test_data.items():
        conn.execute_command(
            'HSET', doc_id, 'description', doc_data['description'],
            'embedding_hnsw', doc_data['embedding'])


def test_hybrid_search_invalid_query_with_vector():
    """Test that hybrid search subquery fails when it contains vector query"""
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    setup_basic_index(env)
    env.assertEqual(b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e" ,np.array([1.2, 0.2]).astype(np.float32).tobytes())
    # This should fail because vector expressions are not allowed in hybrid search subquery
    env.expect('FT.HYBRID', 'idx', 'SEARCH', '@embedding:[VECTOR_RANGE 0.01 $BLOB]', 'VSIM' ,'@embedding', '$BLOB',\
               'PARAMS', "2", "BLOB", b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e").error().contains('Vector expressions are not allowed in FT.HYBRID SEARCH')

def test_hybrid_search_explicit_scorer():
    """Test that hybrid search subquery fails when it contains vector query"""
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    setup_basic_index(env)
    for scorer in ['TFIDF', 'TFIDF.DOCNORM', 'BM25', 'BM25STD', 'BM25STD.NORM', 'DISMAX', 'DOCSCORE', 'HAMMING']:
        env.assertEqual(b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e" ,np.array([1.2, 0.2]).astype(np.float32).tobytes())
        hybrid_response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'shoes', 'SCORER', scorer, 'VSIM' ,'@embedding', '$BLOB',
            'COMBINE', 'LINEAR', '4', 'ALPHA', '1.0', 'BETA', '0.0',
            'PARAMS', '2', 'BLOB',  b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e")
        results, count = get_results_from_hybrid_response(hybrid_response)
        env.assertEqual(count, len(results.keys()))
        results = {a: float(results[a][SCORE_FIELD]) for a in results}
        agg_response = env.cmd('FT.AGGREGATE', 'idx', 'shoes', 'ADDSCORES', 'SCORER', scorer, 'LOAD', 2, '__key', '__score')
        agg_results = {dict['__key']: float(dict[SCORE_FIELD]) for dict in (to_dict(a) for a in agg_response[1:])}
        env.assertEqual(results, agg_results)

def test_hybrid_knn_invalid_syntax():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    setup_basic_index(env)

    env.expect(
        'FT.HYBRID', 'idx', 'SEARCH', 'shoes',
        'VSIM' ,'@embedding', '$BLOB',
        'KNN', 4, 'K', 15
    ).error().contains('Expected arguments 4, but 2 were provided')

    env.expect(
        'FT.HYBRID', 'idx', 'SEARCH', 'shoes',
        'VSIM' ,'@embedding', '$BLOB',
        'KNN', 'K', 15,
        'PARAMS', '2', 'BLOB', b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e"
    ).error().contains('Invalid argument count: expected an unsigned integer')

def test_invalid_ef_runtime():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    setup_basic_index_hnsw(env)

    for invalid_value in ['text', '-1', '0', '1.5']:
        env.expect(
            'FT.HYBRID', 'idx_hnsw', 'SEARCH', 'shoes',
            'VSIM' ,'@embedding_hnsw', '$BLOB',
            'KNN', 4, 'K', 15, 'EF_RUNTIME', invalid_value,
            'PARAMS', '2', 'BLOB',  b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e"
        ).error().contains('Invalid EF_RUNTIME value')

def test_invalid_epsilon():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    setup_basic_index_hnsw(env)

    for invalid_value in ['text', '-1', '-0.1', '0']:
        env.expect(
            'FT.HYBRID', 'idx_hnsw', 'SEARCH', 'shoes',
            'VSIM' ,'@embedding_hnsw', '$BLOB',
            'RANGE', 4, 'RADIUS', 1.1, 'EPSILON', invalid_value,
            'PARAMS', '2', 'BLOB',  b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e"
        ).error().contains('Invalid EPSILON value')

def test_invalid_radius():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    setup_basic_index(env)

    for invalid_value in ['text', '-1', '-0.1']:
        env.expect(
            'FT.HYBRID', 'idx', 'SEARCH', 'shoes',
            'VSIM' ,'@embedding', '$BLOB',
            'RANGE', 2, 'RADIUS', invalid_value,
            'PARAMS', '2', 'BLOB',  b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e"
        ).error().contains('Invalid RADIUS value')

def test_hybrid_range_invalid_syntax():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    setup_basic_index(env)

    env.expect(
        'FT.HYBRID', 'idx', 'SEARCH', 'shoes',
        'VSIM' ,'@embedding', '$BLOB',
        'RANGE', 4, 'RADIUS', 1
    ).error().contains('Expected arguments 4, but 2 were provided')

    env.expect(
        'FT.HYBRID', 'idx', 'SEARCH', 'shoes',
        'VSIM' ,'@embedding', '$BLOB',
        'RANGE', 'RADIUS', 1
    ).error().contains('Invalid argument count: expected an unsigned integer')

    env.expect(
        'FT.HYBRID', 'idx', 'SEARCH', 'shoes',
        'VSIM' ,'@embedding', '$BLOB',
        'RANGE', -1, 'RADIUS', 1
    ).error().contains('Invalid argument count: expected an unsigned integer')

    env.expect(
        'FT.HYBRID', 'idx', 'SEARCH', 'shoes',
        'VSIM' ,'@embedding', '$BLOB',
        'RANGE', 2, 'EPSILON', 0.1
    ).error().contains('Missing required argument RADIUS')


def test_hybrid_zero_max_results():
    """
    FT.HYBRID must not hang or crash when MAXSEARCHRESULTS or MAXAGGREGATERESULTS is 0.
    """
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    setup_basic_index(env)

    blob = np.array([0.3, 0.3]).astype(np.float32).tobytes()
    hybrid_cmd = ('FT.HYBRID', 'idx', 'SEARCH', 'hello',
                'VSIM', '@embedding', '$BLOB',
                'PARAMS', '2', 'BLOB', blob)

    # Baseline: both legs contribute, so we get rows.
    baseline = to_dict(env.cmd(*hybrid_cmd))
    env.assertGreater(len(baseline['results']), 0, message='baseline returns rows')

    # MAXSEARCHRESULTS=0 only -> the single hybrid cap is 0 -> both legs yield
    # nothing -> empty result set.
    run_command_on_all_shards(env, config_cmd(), 'SET', 'MAXSEARCHRESULTS', '0')
    res = to_dict(env.cmd(*hybrid_cmd))
    env.assertEqual(res['results'], [], message='MAXSEARCHRESULTS=0 -> no rows')
    run_command_on_all_shards(env, config_cmd(), 'SET', 'MAXSEARCHRESULTS', '-1')

    # MAXAGGREGATERESULTS=0 only -> inert for FT.HYBRID -> results unchanged.
    run_command_on_all_shards(env, config_cmd(), 'SET', 'MAXAGGREGATERESULTS', '0')
    res = to_dict(env.cmd(*hybrid_cmd))
    env.assertEqual(len(res['results']), len(baseline['results']),
                    message='MAXAGGREGATERESULTS=0 alone -> results unchanged')
    run_command_on_all_shards(env, config_cmd(), 'SET', 'MAXAGGREGATERESULTS', '-1')

    # Both at 0 -> MAXSEARCHRESULTS=0 dominates -> empty result set, no hang.
    run_command_on_all_shards(env, config_cmd(), 'SET', 'MAXSEARCHRESULTS', '0')
    run_command_on_all_shards(env, config_cmd(), 'SET', 'MAXAGGREGATERESULTS', '0')
    res = to_dict(env.cmd(*hybrid_cmd))
    env.assertIn('total_results', res, message='both=0')
    # Both subqueries capped to 0 rows: no rows are serialized, mirroring how
    # FT.SEARCH / FT.AGGREGATE return an empty result set under MAX*RESULTS 0.
    env.assertEqual(res['results'], [], message='both=0 serializes no rows')

