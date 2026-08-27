# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

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

    r.expect('ft.info', 'idx').contains('SEARCH_INDEX_NOT_FOUND Index not found')
    # time.sleep(1)

def test_eval_node_errors_async():
    env = Env(moduleArgs='DEFAULT_DIALECT 2 WORKERS 1 ON_TIMEOUT FAIL')
    conn = getConnectionByEnv(env)
    dim = 1000

    env.assertEqual(conn.execute_command('HSET', 'key', 'foo', 'hello',
                                         'v', create_np_array_typed([0] * dim).tobytes()), 2)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'foo', 'TEXT', 'bar', 'TEXT', 'WITHSUFFIXTRIE', 'g', 'GEO', 'num', 'NUMERIC',
               'v', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', dim, 'DISTANCE_METRIC', 'L2').ok()
    waitForIndex(env, 'idx')

    # Test various scenarios where evaluating the AST should raise an error,
    # and validate that it was caught from the BG thread
    env.expect('FT.SEARCH', 'idx', '@g:[29.69465 34.95126 200 100]', 'NOCONTENT').error()\
        .contains("Invalid GeoFilter unit")
    env.expect('ft.search', 'idx', '@foo:*ell*', 'NOCONTENT').error() \
        .contains('Contains query on fields without WITHSUFFIXTRIE support')
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b]', 'PARAMS', '2', 'b', 'abcdefg').error()\
        .contains('Error parsing vector similarity query: query vector blob size (7) does not match'
                  f' index\'s expected size ({dim*4}).')
    # This error is caught during building the implicit pipeline (also should occur in BG thread)
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b]=>{$yield_distance_as:v}', 'timeout', 0, 'PARAMS', '2', 'b',
               create_np_array_typed([0]*dim).tobytes()).error()\
        .contains(f'Property `v` already exists in schema')
