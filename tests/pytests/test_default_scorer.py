from RLTest import Env
from common import *
import os


@skip(cluster=True)
def test_default_scorer_behavior():
    """
    Test that the default scorer is applied correctly for FT.SEARCH, FT.AGGREGATE, and FT.HYBRID
    and that scores change between different scorer types.
    Also test that setting default to X and overriding with Y gives same result as setting Y as default.
    """
    env = Env(moduleArgs='DEFAULT_DIALECT 2 ENABLE_UNSTABLE_FEATURES true')

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

    default_scorer = env.cmd('CONFIG', 'GET', 'search-default-scorer')
    env.assertEqual(default_scorer, ['search-default-scorer', 'BM25STD'])

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

@skip(cluster=True)
def test_default_scorer_with_extension():
    """
    Test that the default scorer can be set to a custom scorer from an extension
    and that it's applied correctly for FT.SEARCH, FT.AGGREGATE, and FT.HYBRID.
    """
    # Skip if extension is not available
    if 'EXT_TEST_PATH' in os.environ:
        ext_path = os.environ['EXT_TEST_PATH']
    else:
        ext_path = 'tests/ctests/ext-example/libexample_extension.so'

    if not os.path.exists(ext_path):
        assert False, f"Extension not found at {ext_path}"

    env = Env(moduleArgs=f'EXTLOAD {ext_path} DEFAULT_DIALECT 2 ENABLE_UNSTABLE_FEATURES true')

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

    # Verify default scorer is initially BM25STD
    default_scorer = env.cmd('FT.CONFIG', 'GET', 'DEFAULT_SCORER')
    env.assertEqual(default_scorer, [['DEFAULT_SCORER', 'BM25STD']])

    # Test search with default BM25STD scorer
    search_default_bm25std = env.cmd('FT.SEARCH', 'idx', 'hello', 'WITHSCORES', 'NOCONTENT')
    search_explicit_bm25std = env.cmd('FT.SEARCH', 'idx', 'hello', 'SCORER', 'BM25STD', 'WITHSCORES', 'NOCONTENT')
    env.assertEqual(search_default_bm25std, search_explicit_bm25std)

    # Test search with explicit extension scorer
    search_explicit_example = env.cmd('FT.SEARCH', 'idx', 'hello', 'SCORER', 'example_scorer', 'WITHSCORES', 'NOCONTENT')
    # Extension scorer returns 3.141 for all documents
    env.assertEqual(float(search_explicit_example[2]), 3.141)  # First document score
    env.assertEqual(float(search_explicit_example[4]), 3.141)  # Second document score

    # Change default scorer to the extension scorer
    env.cmd('FT.CONFIG', 'SET', 'DEFAULT_SCORER', 'example_scorer')
    new_default = env.cmd('FT.CONFIG', 'GET', 'DEFAULT_SCORER')
    env.assertEqual(new_default, [['DEFAULT_SCORER', 'example_scorer']])

    # Test that default scorer now uses the extension scorer
    search_default_example = env.cmd('FT.SEARCH', 'idx', 'hello', 'WITHSCORES', 'NOCONTENT')
    env.assertEqual(search_default_example, search_explicit_example)
    env.assertEqual(float(search_default_example[2]), 3.141)  # First document score
    env.assertEqual(float(search_default_example[4]), 3.141)  # Second document score

    # Test that explicit BM25STD still works when default is extension scorer
    search_explicit_bm25std_after = env.cmd('FT.SEARCH', 'idx', 'hello', 'SCORER', 'BM25STD', 'WITHSCORES', 'NOCONTENT')
    env.assertEqual(search_explicit_bm25std_after, search_default_bm25std)

    # Test FT.AGGREGATE with extension scorer as default
    agg_default_example = env.cmd('FT.AGGREGATE', 'idx', 'hello', 'ADDSCORES', 'SORTBY', '2', '@__score', 'DESC')
    agg_explicit_example = env.cmd('FT.AGGREGATE', 'idx', 'hello', 'SCORER', 'example_scorer', 'ADDSCORES', 'SORTBY', '2', '@__score', 'DESC')
    env.assertEqual(agg_default_example, agg_explicit_example)
    # All documents should have score 3.141
    env.assertEqual(float(agg_default_example[1][1]), 3.141)  # First document score
    env.assertEqual(float(agg_default_example[2][1]), 3.141)  # Second document score

@skip(cluster=True, asan=True)
def test_default_scorer_startup_validation():
    if 'EXT_TEST_PATH' in os.environ:
        ext_path = os.environ['EXT_TEST_PATH']
    else:
        ext_path = 'tests/ctests/ext-example/libexample_extension.so'
    try:
        env = Env(moduleArgs=f'EXTLOAD {ext_path} DEFAULT_DIALECT 2 ENABLE_UNSTABLE_FEATURES true DEFAULT_SCORER example_scorer2')
        assert not env.isUp()
    except Exception as e:
        # It sometimes captures the error of it not being up (PID dead and sometimes not). We cannot have a false positive that env.isUp but we still pass the test
        assert not isinstance(e, AssertionError)

    try:
        env = Env(moduleArgs=f'ENABLE_UNSTABLE_FEATURES true DEFAULT_SCORER example_scorer')
        assert not env.isUp()
    except Exception as e:
        # It sometimes captures the error of it not being up (PID dead and sometimes not). We cannot have a false positive that env.isUp but we still pass the test
        assert not isinstance(e, AssertionError)

    env = Env(moduleArgs=f'EXTLOAD {ext_path} DEFAULT_DIALECT 2 ENABLE_UNSTABLE_FEATURES true DEFAULT_SCORER example_scorer')
    assert env.isUp()

    env = Env(moduleArgs=f'EXTLOAD {ext_path} DEFAULT_DIALECT 2 ENABLE_UNSTABLE_FEATURES true DEFAULT_SCORER TFIDF')
    assert env.isUp()

    try:
        env = Env(moduleArgs=f'EXTLOAD {ext_path} DEFAULT_DIALECT 2 ENABLE_UNSTABLE_FEATURES false DEFAULT_SCORER example_scorer')
        assert not env.isUp()
    except Exception as e:
        # It sometimes captures the error of it not being up (PID dead and sometimes not). We cannot have a false positive that env.isUp but we still pass the test
        assert not isinstance(e, AssertionError)
    try:
        env = Env(moduleArgs=f'EXTLOAD {ext_path} DEFAULT_DIALECT 2 ENABLE_UNSTABLE_FEATURES false DEFAULT_SCORER TFIDF')
        assert not env.isUp()
    except:
        assert not isinstance(e, AssertionError)

    # These do not work because the ENABLE_UNSTABLE_FEATURES is after the DEFAULT_SCORER (not sure it can be bypassed)
    try:
        env = Env(moduleArgs=f'EXTLOAD {ext_path} DEFAULT_DIALECT 2 DEFAULT_SCORER example_scorer ENABLE_UNSTABLE_FEATURES true')
        assert not env.isUp()
    except:
        assert not isinstance(e, AssertionError)

    try:
        env = Env(moduleArgs=f'EXTLOAD {ext_path} DEFAULT_DIALECT 2 DEFAULT_SCORER TFIDF ENABLE_UNSTABLE_FEATURES true')
        assert not env.isUp()
    except:
        assert not isinstance(e, AssertionError)
