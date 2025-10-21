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
        conn.execute_command('HSET', doc_id, 'description', doc_data['description'], 'embedding', doc_data['embedding'])


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
        hybrid_response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'shoes', 'SCORER', scorer, 'VSIM' ,'@embedding', \
            b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e",'COMBINE', 'LINEAR', '4', 'ALPHA', '1.0', 'BETA', '0.0')
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
        'VSIM' ,'@embedding', b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e",
        'KNN', 4, 'K', 15
    ).error().contains('Expected arguments 4, but 2 were provided')

    env.expect(
        'FT.HYBRID', 'idx', 'SEARCH', 'shoes',
        'VSIM' ,'@embedding', b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e",
        'KNN', 'K', 15
    ).error().contains('Invalid argument count: expected an unsigned integer')

def test_invalid_ef_runtime():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    setup_basic_index_hnsw(env)

    env.expect(
        'FT.HYBRID', 'idx_hnsw', 'SEARCH', 'shoes',
        'VSIM' ,'@embedding_hnsw', b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e",
        'KNN', 4, 'K', 15, 'EF_RUNTIME', 'what?'
    ).error().contains('Invalid EF_RUNTIME value')

def test_invalid_epsilon():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    setup_basic_index_hnsw(env)

    env.expect(
        'FT.HYBRID', 'idx_hnsw', 'SEARCH', 'shoes',
        'VSIM' ,'@embedding_hnsw', b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e",
        'RANGE', 4, 'RADIUS', 1.1, 'EPSILON', 'what?'
    ).error().contains('Invalid EPSILON value')

def test_hybrid_range_invalid_syntax():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    setup_basic_index(env)

    env.expect(
        'FT.HYBRID', 'idx', 'SEARCH', 'shoes',
        'VSIM' ,'@embedding', b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e",
        'RANGE', 4, 'RADIUS', 1
    ).error().contains('Expected arguments 4, but 2 were provided')

    env.expect(
        'FT.HYBRID', 'idx', 'SEARCH', 'shoes',
        'VSIM' ,'@embedding', b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e",
        'RANGE', 2, 'EPSILON', 0.1
    ).error().contains('Missing required argument RADIUS')


