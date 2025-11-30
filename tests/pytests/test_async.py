from common import *

def testCreateIndex(env):
    conn = getConnectionByEnv(env)
    N = 1000
    for i in range(N):
        res = conn.execute_command('hset', 'foo:%d' % i, 'name', 'john doe')
        env.assertEqual(res, 1)

    env.expect('ft.create', 'idx', 'ON', 'HASH', 'ASYNC', 'schema', 'name', 'text').ok()
    waitForIndex(env, 'idx')
    res = env.cmd('ft.search', 'idx', 'doe', 'nocontent')
    env.assertEqual(N, res[0])

def testAlterIndex(env):
    conn = getConnectionByEnv(env)
    N = 10000
    for i in range(N):
        res = conn.execute_command('hset', 'foo:%d' % i, 'name', 'john doe', 'age', str(10 + i))
        env.assertEqual(res, 2)

    env.expect('ft.create', 'idx', 'ON', 'HASH', 'ASYNC', 'schema', 'name', 'text').ok()
    env.cmd('ft.alter', 'idx', 'schema', 'add', 'age', 'numeric')
    # note the two background scans
    waitForIndex(env, 'idx')
    res = env.cmd('ft.search', 'idx', '@age: [10 inf]', 'nocontent')
    env.assertEqual(N, res[0])

def testDeleteIndex(env):
    conn = getConnectionByEnv(env)
    r = env
    N = 100
    for i in range(N):
        res = conn.execute_command('hset', 'foo:%d' % i, 'name', 'john doe')
        env.assertEqual(res, 1)

    r.expect('ft.create', 'idx', 'ON', 'HASH', 'ASYNC', 'schema', 'name', 'text').ok()
    r.expect('ft.drop', 'idx').ok()
    r.expect('ft.info', 'idx').equal('Unknown index name')
    # time.sleep(1)


def test_yield_while_bg_indexing_mod4745(env):
    conn = getConnectionByEnv(env)
    # Create an index in which each shard has > 1000 docs.
    n = 1010 * env.shardsCount
    for i in range(n):
        res = conn.execute_command('hset', f'doc:{i}', 'name', f'hello world')
        env.assertEqual(res, 1)

    # Baseline - zero yields before index has created.
    env.assertEqual(run_command_on_all_shards(env, debug_cmd(), 'YIELDS_COUNTER', 'BG_INDEX'),
                    [0]*env.shardsCount)
    env.expect('ft.create', 'idx', 'schema', 'name', 'text').ok()
    allShards_waitForIndexFinishScan(env)
    # Validate that we yielded at least once (we should after every 100 bg indexing iterations).
    # The background scan in Redis may scan keys more than once (see RM_Scan() docs), so we assert that each shard
    # yields *at least* once for each 100 documents.
    for shard in env.getOSSMasterNodesConnectionList():
        env.assertGreaterEqual(shard.execute_command(debug_cmd(), 'YIELDS_COUNTER', 'BG_INDEX'),
                               int((n/env.shardsCount) // 100))
    # The yield mechanism was introduced is to make sure cluster will not mark itself as fail since the server is not
    # responsive and fail to send cluster PING on time before we reach cluster-node-timeout. Every time we yield, we
    # give the main thread a chance to reply to PINGs.

@skip(noWorkers=True)
def test_eval_node_errors_async():
    env = Env(moduleArgs='DEFAULT_DIALECT 2 WORKERS 1 ON_TIMEOUT FAIL')
    conn = getConnectionByEnv(env)
    dim = 1000

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'foo', 'TEXT', 'bar', 'TEXT', 'WITHSUFFIXTRIE', 'g', 'GEO', 'num', 'NUMERIC',
               'v', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', dim, 'DISTANCE_METRIC', 'L2').ok()
    waitForIndex(env, 'idx')

    n_docs = 10000
    for i in range(n_docs):
        env.assertEqual(conn.execute_command('HSET', f'key{i}', 'foo', 'hello',
                                             'v', create_np_array_typed([i/1000]*dim).tobytes()), 2)

    # Test various scenarios where evaluating the AST should raise an error,
    # and validate that it was caught from the BG thread
    env.expect('FT.SEARCH', 'idx', '@g:[29.69465 34.95126 200 100]', 'NOCONTENT').error()\
        .contains("Invalid GeoFilter unit")
    env.expect('ft.search', 'idx', '@foo:*ell*', 'NOCONTENT').error() \
        .contains('Contains query on fields without WITHSUFFIXTRIE support')
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b]', 'PARAMS', '2', 'b', 'abcdefg').error()\
        .contains('Error parsing vector similarity query: query vector blob size (7) does not match'
                  f' index\'s expected size ({dim*4}).')
    env.expect('FT.SEARCH', 'idx', '@v:[VECTOR_RANGE 10000000 $vec_param]', 'NOCONTENT', 'LIMIT', 0, n_docs,
               'PARAMS', 2, 'vec_param', create_np_array_typed([0]*dim).tobytes(),
               'TIMEOUT', 1).error().equal('Timeout limit was reached')

    # This error is caught during building the implicit pipeline (also should occur in BG thread)
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b]=>{$yield_distance_as:v}', 'timeout', 0, 'PARAMS', '2', 'b',
               create_np_array_typed([0]*dim).tobytes()).error()\
        .contains(f'Property `v` already exists in schema')
