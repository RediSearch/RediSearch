from common import *
from redis import ResponseError
from time import sleep

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

@skip(cluster=True)
def test_required_fields(env):
    # Testing coordinator<-> shard `_REQUIRED_FIELDS` protocol
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
    env.expect('FT.AGGREGATE', 'idx', '*').error().contains('idx: no such index')
    env.expect('FT.SEARCH', 'idx', '*').error().contains('idx: no such index')

    # Bad query
    # create the index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    env.expect('FT.AGGREGATE', 'idx', '**').error().contains('Syntax error')
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
    env.expect(
        'FT.AGGREGATE', 'idx', '*', 'LOAD', '2', '@t1', '@__key', 'APPLY',
        '@t1 ^ @t1', 'AS', 't1exp', 'groupby', '2', '@t1', '@t1exp', 'REDUCE',
        'tolist', '1', '@__key', 'AS', 'keys', 'timeout', '1'
    ).error().contains('Timeout limit was reached')

    # Client cursor mid execution
    env.expect(
        'FT.AGGREGATE', 'idx', '*', 'LOAD', '*', 'WITHCURSOR', 'COUNT', n_docs,
        'timeout', '1'
    ).error().contains('Timeout limit was reached')

    # FT.SEARCH
    env.expect(
        'FT.SEARCH', 'idx', '*', 'LIMIT', '0', n_docs, 'timeout', '1'
    ).error().contains('Timeout limit was reached')

@skip(cluster=False, min_shards=2)
def test_mod_6287(env):
    """Tests that the coordinator does not crash on aggregations with cursors,
    when some of the shards return an error while the others don't. Specifically,
    such a scenario depicted in PR #4324 results in a crash since the `depleted`
    and `pending` flags/counter were not aligned."""

    conn = getConnectionByEnv(env)
    con2 = env.getConnection(2)

    # Create an index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC').ok()

    # Populate the database with enough documents to make sure that each shard
    # will get at least 2 `_FT.CURSOR READ` commands from the coordinator, and
    # still have more docs to return.
    # Each such command pulls 1000 docs from each shard, so 2500 should work.
    n_docs = 2500 * env.shardsCount
    for i in range(n_docs):
        conn.execute_command('HSET', i, 'n', i)

    # Dispatch an aggregate with cursor command to the coordinator
    res, cid = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', '*', 'WITHCURSOR')
    received = len(res)-1

    # Delete a shard cursor (of shard 2 in this case) so that once the coordinator
    # sends a `CURSOR READ` command to that shard, it will return an error.
    # Now (after PR 6287), the command for the errored shard will be set as
    # `depleted`, such that the `depleted` shards will be aligned with the
    # `pending` counter.
    con2.execute_command('_FT.DEBUG', 'DELETE_LOCAL_CURSORS')

    # Dispatch an `FT.CURSOR READ` command that will request for more results from the shards
    # This results in the crash solved by #6287
    res, cid = env.cmd('FT.CURSOR', 'READ', 'idx', cid, 'COUNT', n_docs - received)
    env.assertEqual(cid, 0)

    # Send another command to make sure that the coordinator is healthy
    res = env.cmd('FT.AGGREGATE', 'idx', '*', 'LIMIT', '0', str(n_docs))
    env.assertEqual(len(res)-1, n_docs)
