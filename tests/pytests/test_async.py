import unittest
import random
import time
import numpy as np
from RLTest import Env

from includes import *
from common import getConnectionByEnv, waitForIndex, create_np_array_typed

def testCreateIndex(env):
    conn = getConnectionByEnv(env)
    r = env
    N = 1000
    for i in range(N):
        res = conn.execute_command('hset', 'foo:%d' % i, 'name', 'john doe')
        env.assertEqual(res, 1)

    r.expect('ft.create', 'idx', 'ON', 'HASH', 'ASYNC', 'schema', 'name', 'text').ok()
    waitForIndex(r, 'idx')
    res = r.execute_command('ft.search', 'idx', 'doe', 'nocontent')
    env.assertEqual(N, res[0])

def testAlterIndex(env):
    conn = getConnectionByEnv(env)
    r = env
    N = 10000
    for i in range(N):
        res = conn.execute_command('hset', 'foo:%d' % i, 'name', 'john doe', 'age', str(10 + i))
        env.assertEqual(res, 2)

    r.expect('ft.create', 'idx', 'ON', 'HASH', 'ASYNC', 'schema', 'name', 'text').ok()
    env.cmd('ft.alter', 'idx', 'schema', 'add', 'age', 'numeric')
    # note the two background scans
    waitForIndex(r, 'idx')
    res = r.execute_command('ft.search', 'idx', '@age: [10 inf]', 'nocontent')
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


def test_mod4745(env):
    conn = getConnectionByEnv(env)
    r = env
    # Create an index with large dim so that a single indexing operation will take a long time
    N = 1000 * env.shardsCount
    dim = 30000
    for i in range(N):
        res = conn.execute_command('hset', 'foo:%d' % i, 'name', f'some string with information to index in the '
                                                                 f'background later on for id {i}',
                                   'v', create_np_array_typed(np.random.random((1, dim))).tobytes())
        env.assertEqual(res, 2)

    r.expect('ft.create', 'idx', 'schema', 'name', 'text', 'v', 'VECTOR', 'HNSW', '6', 'distance_metric', 'l2', 'DIM',
             dim, 'type', 'float32').ok()
    # Make sure we are getting here without having cluster mark itself as fail since the server is not responsive and
    # fail to send cluster PING on time before we reach cluster-node-timeout.
    waitForIndex(r, 'idx')


def test_eval_node_errors_async():
    if not MT_BUILD:
        raise unittest.SkipTest("Skipping since worker threads are not enabled")
    env = Env(moduleArgs='DEFAULT_DIALECT 2 WORKER_THREADS 1 MT_MODE MT_MODE_FULL ON_TIMEOUT FAIL')
    conn = getConnectionByEnv(env)
    dim = 1000

    async_err_prefix = "The following error was caught upon running the query asynchronously: "
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'foo', 'TEXT', 'bar', 'TEXT', 'WITHSUFFIXTRIE', 'g', 'GEO', 'num', 'NUMERIC',
               'v', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', dim, 'DISTANCE_METRIC', 'L2').ok()
    waitForIndex(env, 'idx')

    n_docs = 10000
    for i in range(n_docs):
        env.assertEqual(conn.execute_command('HSET', f'key{i}', 'foo', 'hello',
                                             'v', create_np_array_typed([i/1000]*dim).tobytes()), 2)

    # Test various scenarios where evaluating the AST should raise an error,
    # and validate that it was caught from the BG thread
    env.expect('FT.SEARCH', 'idx', '@g:[29.69465 34.95126 200 100]', 'NOCONTENT').raiseError()\
        .contains(f"{async_err_prefix}Invalid GeoFilter unit")
    env.expect('ft.search', 'idx', '@foo:*ell*', 'NOCONTENT').error() \
        .contains(f'{async_err_prefix}Contains query on fields without WITHSUFFIXTRIE support')
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b]', 'PARAMS', '2', 'b', 'abcdefg').error()\
        .contains(f'{async_err_prefix}Error parsing vector similarity query: query vector blob size (7) does not match'
                  f' index\'s expected size ({dim*4}).')
    env.expect('FT.SEARCH', 'idx', '@v:[VECTOR_RANGE 10000000 $vec_param]', 'NOCONTENT', 'LIMIT', 0, n_docs,
               'PARAMS', 2, 'vec_param', create_np_array_typed([0]*dim).tobytes(),
               'TIMEOUT', 1).error().equal(f'{async_err_prefix}Timeout limit was reached')

    # This error is caught during building the implicit pipeline (also should occur in BG thread)
    env.expect('FT.SEARCH', 'idx', '*=>[KNN 2 @v $b]=>{$yield_distance_as:v}', 'timeout', 0, 'PARAMS', '2', 'b',
               create_np_array_typed([0]*dim).tobytes()).error()\
        .contains(f'{async_err_prefix}Property `v` already exists in schema')
