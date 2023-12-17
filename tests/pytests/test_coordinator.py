from common import *
from redis import ResponseError

def testInfo(env):
    SkipOnNonCluster(env)
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'SORTABLE', 'v', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2').ok()
    for i in range (100):
        conn.execute_command('HSET', i, 't', 'Hello world!', 'v', 'abcdefgh')

    idx_info = index_info(env, 'idx')
    env.assertLess(float(idx_info['inverted_sz_mb']), 1)
    env.assertLess(float(idx_info['offset_vectors_sz_mb']), 1)
    env.assertLess(float(idx_info['doc_table_size_mb']), 1)
    env.assertLess(float(idx_info['sortable_values_size_mb']), 1)
    env.assertLess(float(idx_info['key_table_size_mb']), 1)
    env.assertLess(float(idx_info['vector_index_sz_mb']), 1)
    env.assertGreater(float(idx_info['inverted_sz_mb']), 0)
    env.assertGreater(float(idx_info['offset_vectors_sz_mb']), 0)
    env.assertGreater(float(idx_info['doc_table_size_mb']), 0)
    env.assertGreater(float(idx_info['sortable_values_size_mb']), 0)
    env.assertGreater(float(idx_info['key_table_size_mb']), 0)
    env.assertGreater(float(idx_info['vector_index_sz_mb']), 0)

def test_required_fields(env):
    # Testing coordinator<-> shard `_REQUIRED_FIELDS` protocol
    env.skipOnCluster()
    env.expect('ft.create', 'idx', 'schema', 't', 'text').ok()
    env.cmd('HSET', '0', 't', 'hello')
    env.expect('ft.search', 'idx', 'hello', '_REQUIRED_FIELDS').error()
    env.expect('ft.search', 'idx', 'hello', '_REQUIRED_FIELDS', '2', 't').error()
    env.expect('ft.search', 'idx', 'hello', '_REQUIRED_FIELDS', '1', 't').equal([1, '0', '$hello', ['t', 'hello']])
    env.expect('ft.search', 'idx', 'hello', 'nocontent', 'SORTBY', 't', '_REQUIRED_FIELDS', '1', 't').equal([1, '0', '$hello'])
    # Field is not in Rlookup, will not load
    env.expect('ft.search', 'idx', 'hello', 'nocontent', '_REQUIRED_FIELDS', '1', 't').equal([1, '0', None])


def check_info_commandstats(env, cmd):
    res = env.cmd('INFO', 'COMMANDSTATS')
    env.assertGreater(res['cmdstat_' + cmd]['usec'], res['cmdstat__' + cmd]['usec'])

def testCommandStatsOnRedis(env):
    # This test checks the total time spent on the Coordinator is greater then
    # on a single shard
    SkipOnNonCluster(env)
    if not server_version_at_least(env, "6.2.0"):
        env.skip()

    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'SORTABLE').ok()
    # _FT.CREATE is not called. No option to test

    for i in range(100):
        conn.execute_command('HSET', i, 't', 'Hello world!')

    env.expect('FT.SEARCH', 'idx', 'hello', 'LIMIT', 0, 0).equal([100])
    check_info_commandstats(env, 'FT.SEARCH')

    env.expect('FT.AGGREGATE', 'idx', 'hello', 'LIMIT', 0, 0).equal([env.shardsCount])
    check_info_commandstats(env, 'FT.AGGREGATE')

    conn.execute_command('FT.INFO', 'idx')
    check_info_commandstats(env, 'FT.INFO')

def test_curly_brackets(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'SORTABLE').ok()

    conn.execute_command('HSET', 'foo{bar}', 't', 'Hello world!')
    env.expect('ft.search', 'idx', 'hello').equal([1, 'foo{bar}', ['t', 'Hello world!']])
    env.expect('ft.aggregate', 'idx', 'hello', 'LOAD', 1, '__key').equal([1, ['__key', 'foo{bar}']])

def test_MOD_3540(env):
    # check server does not crash when MAX argument for SORTBY is greater than 10
    SkipOnNonCluster(env)
    conn = getConnectionByEnv(env)

    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT')
    for i in range(100):
        conn.execute_command('HSET', i, 't', i)

    env.expect('FT.SEARCH', 'idx', '*', 'SORTBY', 't', 'DESC', 'MAX', '20')

def test_error_propagation_from_shards(env):
    """Tests that errors from the shards are propagated properly to the
    coordinator, for both `FT.SEARCH` and `FT.AGGREGATE` commands.
    We check the following errors:
    1. Non-existing index.
    2. Bad query.

    * Timeouts are handled and tested separately.
    """

    SkipOnNonCluster(env)

    # indexing an index that doesn't exist (today revealed only in the shards)
    if env.protocol == 3:
        err = env.cmd('FT.AGGREGATE', 'idx', '*')['error']
    else:
        err = env.cmd('FT.AGGREGATE', 'idx', '*')[1]

    env.assertEqual(type(err[0]), ResponseError)
    env.assertContains('idx: no such index', str(err[0]))
    # The same for `FT.SEARCH`.
    env.expect('FT.SEARCH', 'idx', '*').error().contains('idx: no such index')

    # Bad query
    # create the index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    if env.protocol == 3:
        err = env.cmd('FT.AGGREGATE', 'idx', '**')['error']
    else:
        err = env.cmd('FT.AGGREGATE', 'idx', '**')[1]

    env.assertEqual(type(err[0]), ResponseError)
    env.assertContains('Syntax error', str(err[0]))
    # The same for `FT.SEARCH`.
    env.expect('FT.SEARCH', 'idx', '**').error().contains('Syntax error')

    # Other stuff that are being checked only on the shards (FYI):
    #   1. The language requested in the command.
    #   2. The scorer requested in the command.
    #   3. Parameters evaluation

def test_timeout():
    """Tests that timeouts are handled properly by the coordinator.
    We check that the coordinator returns a timeout error when the timeout is
    reached in the shards or in the coordinator itself.
    """

    env = Env(moduleArgs='DEFAULT_DIALECT 2 ON_TIMEOUT FAIL TIMEOUT 1')
    SkipOnNonCluster(env)
    conn = getConnectionByEnv(env)

    # Create the index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT').ok()

    # Populate the database with many documents (more docs --> less flakiness)
    n_docs = 25000 * env.shardsCount
    for i in range(n_docs):
        conn.execute_command('HSET', i ,'t1', str(i))

    # No client cursor
    res = env.execute_command('FT.AGGREGATE', 'idx', '*',
                'LOAD', '2', '@t1', '@__key',
                'APPLY', '@t1 ^ @t1', 'AS', 't1exp',
                'groupby', '2', '@t1', '@t1exp',
                'REDUCE', 'tolist', '1', '@__key', 'AS', 'keys', 'timeout', '1')
    # TODO: Add this once the response will be fixed to be and error instead of a string
    # env.assertEquals(type(res[0]), ResponseError)
    env.assertContains('Timeout limit was reached', str(res[0]))

    # Client cursor mid execution
    # If the cursor id is 0, this means there was a timeout throughout execution
    # caught by the coordinator
    res, cursor = conn.execute_command('FT.AGGREGATE', 'idx', '*', 'LOAD', '*',
                                       'WITHCURSOR', 'COUNT', n_docs, 'timeout', '1')
    env.assertEqual(cursor, 0)

    # FT.SEARCH
    res = conn.execute_command('FT.SEARCH', 'idx', '*', 'LIMIT', '0', n_docs,
                               'timeout', '1')
    # TODO: Add this when MOD-5965 is merged
    # env.assertEqual(type(res[0]), ResponseError)
    env.assertContains('Timeout limit was reached', str(res[0]))
