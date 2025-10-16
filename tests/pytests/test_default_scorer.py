from RLTest import Env
from common import *


@skip(cluster=True)
def test_default_scorer_behavior():
    """
    Test that the default scorer is applied correctly for FT.SEARCH, FT.AGGREGATE, and FT.HYBRID
    and that scores change between different scorer types.
    Also test that setting default to X and overriding with Y gives same result as setting Y as default.
    """
    env = Env(moduleArgs='DEFAULT_DIALECT 2')

    # Create index with text and vector fields for hybrid testing
    env.cmd('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA',
            'title', 'TEXT',
            'content', 'TEXT',
            'vector', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '4', 'DISTANCE_METRIC', 'L2')

    # Add test documents
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc1', 'title', 'hello world', 'content', 'this is a test document', 'vector', b'\x00\x00\x80\x3f\x00\x00\x00\x40\x00\x00\x40\x40\x00\x00\x80\x40')
    conn.execute_command('HSET', 'doc2', 'title', 'hello universe', 'content', 'another test document', 'vector', b'\x00\x00\x00\x40\x00\x00\x40\x40\x00\x00\x80\x40\x00\x00\xa0\x40')
    conn.execute_command('HSET', 'doc3', 'title', 'world peace', 'content', 'final test document', 'vector', b'\x00\x00\x40\x40\x00\x00\x80\x40\x00\x00\xa0\x40\x00\x00\xc0\x40')

    # Wait for indexing
    waitForIndex(env, 'idx')
    search_default_bm25std_without_config_set = env.cmd('FT.SEARCH', 'idx', 'hello', 'WITHSCORES', 'NOCONTENT') # Default scorer is BM25STD from start

    default_scorer = env.cmd('FT.CONFIG', 'GET', 'DEFAULT_SCORER')
    env.assertEqual(default_scorer, [['DEFAULT_SCORER', 'BM25STD']])

    search_default_bm25std = env.cmd('FT.SEARCH', 'idx', 'hello', 'WITHSCORES', 'NOCONTENT')
    search_explicit_bm25std = env.cmd('FT.SEARCH', 'idx', 'hello', 'SCORER', 'BM25STD', 'WITHSCORES', 'NOCONTENT')
    env.assertEqual(search_default_bm25std, search_explicit_bm25std)
    env.assertEqual(search_default_bm25std, search_default_bm25std_without_config_set)

    search_tfidf_in_query = env.cmd('FT.SEARCH', 'idx', 'hello', 'SCORER', 'TFIDF', 'WITHSCORES', 'NOCONTENT')
    env.assertNotEqual(search_default_bm25std[2], search_tfidf_in_query[2])  # First document score should differ

    agg_default_bm25std = env.cmd('FT.AGGREGATE', 'idx', 'hello', 'ADDSCORES', 'SORTBY', '2', '@__score', 'DESC')
    agg_explicit_bm25std = env.cmd('FT.AGGREGATE', 'idx', 'hello', 'SCORER', 'BM25STD', 'ADDSCORES', 'SORTBY', '2', '@__score', 'DESC')
    env.assertEqual(agg_default_bm25std, agg_explicit_bm25std)

    agg_tfidf_in_query = env.cmd('FT.AGGREGATE', 'idx', 'hello', 'SCORER', 'TFIDF', 'ADDSCORES', 'SORTBY', '2', '@__score', 'DESC')
    env.assertNotEqual(agg_default_bm25std[1][1], agg_tfidf_in_query[1][1])  # First document score should differ

    vector_blob = b'\x00\x00\x80\x3f\x00\x00\x00\x40\x00\x00\x40\x40\x00\x00\x80\x40'

    hybrid_default_bm25std = env.cmd('FT.HYBRID', 'idx',
                            'SEARCH', 'hello',
                            'VSIM', '@vector', vector_blob,
                            'COMBINE', 'LINEAR', '4', 'ALPHA', '0.7', 'BETA', '0.3')
    hybrid_default_bm25std_results, hybrid_default_bm25std_count = get_results_from_hybrid_response(hybrid_default_bm25std)

    hybrid_explicit_bm25std = env.cmd('FT.HYBRID', 'idx',
                                     'SEARCH', 'hello', 'SCORER', 'BM25STD',
                                     'VSIM', '@vector', vector_blob,
                                     'COMBINE', 'LINEAR', '4', 'ALPHA', '0.7', 'BETA', '0.3')
    hybrid_explicit_bm25std_results, hybrid_explicit_bm25std_count = get_results_from_hybrid_response(hybrid_explicit_bm25std)

    env.assertEqual(hybrid_default_bm25std_results, hybrid_explicit_bm25std_results)
    env.assertEqual(hybrid_default_bm25std_count, hybrid_explicit_bm25std_count)

    hybrid_tfidf_in_query = env.cmd('FT.HYBRID', 'idx',
                          'SEARCH', 'hello', 'SCORER', 'TFIDF',
                          'VSIM', '@vector', vector_blob,
                          'COMBINE', 'LINEAR', '4', 'ALPHA', '0.7', 'BETA', '0.3')
    hybrid_tfidf_in_query_results, _ = get_results_from_hybrid_response(hybrid_tfidf_in_query)

    first_doc_default = list(hybrid_default_bm25std_results.keys())[0]
    first_doc_tfidf = list(hybrid_tfidf_in_query_results.keys())[0]
    env.assertNotEqual(hybrid_default_bm25std_results[first_doc_default]['__score'],
                      hybrid_tfidf_in_query_results[first_doc_tfidf]['__score'])

    # Change default scorer to TFIDF
    env.cmd('FT.CONFIG', 'SET', 'DEFAULT_SCORER', 'TFIDF')
    new_default = env.cmd('FT.CONFIG', 'GET', 'DEFAULT_SCORER')
    env.assertEqual(new_default, [['DEFAULT_SCORER', 'TFIDF']])

    search_default_tfidf = env.cmd('FT.SEARCH', 'idx', 'hello', 'WITHSCORES', 'NOCONTENT')
    env.assertEqual(search_default_tfidf, search_tfidf_in_query)
    search_bm25std_in_query = env.cmd('FT.SEARCH', 'idx', 'hello', 'SCORER', 'BM25STD', 'WITHSCORES', 'NOCONTENT')
    env.assertEqual(search_default_bm25std, search_bm25std_in_query)

    agg_default_tfidf = env.cmd('FT.AGGREGATE', 'idx', 'hello', 'ADDSCORES', 'SORTBY', '2', '@__score', 'DESC')
    env.assertEqual(agg_default_tfidf, agg_tfidf_in_query)

    agg_bm25std_in_query = env.cmd('FT.AGGREGATE', 'idx', 'hello', 'SCORER', 'BM25STD', 'ADDSCORES', 'SORTBY', '2', '@__score', 'DESC')
    env.assertEqual(agg_default_bm25std, agg_bm25std_in_query)

    hybrid_default_tfidf = env.cmd('FT.HYBRID', 'idx',
                            'SEARCH', 'hello',
                            'VSIM', '@vector', vector_blob,
                            'COMBINE', 'LINEAR', '4', 'ALPHA', '0.7', 'BETA', '0.3')
    hybrid_default_tfidf_results, _ = get_results_from_hybrid_response(hybrid_default_tfidf)
    env.assertEqual(hybrid_default_tfidf_results, hybrid_tfidf_in_query_results)
    hybrid_bm25std_in_query = env.cmd('FT.HYBRID', 'idx',
                                     'SEARCH', 'hello', 'SCORER', 'BM25STD',
                                     'VSIM', '@vector', vector_blob,
                                     'COMBINE', 'LINEAR', '4', 'ALPHA', '0.7', 'BETA', '0.3')
    hybrid_bm25std_in_query_results, _ = get_results_from_hybrid_response(hybrid_bm25std_in_query)
    env.assertEqual(hybrid_default_bm25std_results, hybrid_bm25std_in_query_results)
