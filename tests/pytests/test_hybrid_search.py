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
    for scorer in ['TFIDF','TFIDF.DOCNORM', 'BM25', 'BM25STD', 'BM25STD.NORM', 'DISMAX', 'DOCSCORE', 'HAMMING']:
        env.assertEqual(b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e" ,np.array([1.2, 0.2]).astype(np.float32).tobytes())
        hybrid_response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'shoes', 'SCORER', scorer, 'VSIM' ,'@embedding', \
            b"\x9a\x99\x99\x3f\xcd\xcc\x4c\x3e",'COMBINE', 'LINEAR', '4', 'ALPHA', '1.0', 'BETA', '0.0')
        results, count = get_results_from_hybrid_response(hybrid_response)
        env.assertEqual(count, len(results.keys()))
        results = {a: float(results[a][SCORE_FIELD]) for a in results}
        agg_response = env.cmd('FT.AGGREGATE', 'idx', 'shoes', 'ADDSCORES', 'SCORER', scorer, 'LOAD', 2, '__key', '__score')
        agg_results = {dict['__key']: float(dict[SCORE_FIELD]) for dict in (to_dict(a) for a in agg_response[1:])}
        env.assertEqual(results, agg_results)
