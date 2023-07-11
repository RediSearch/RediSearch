import unittest
import random
import time
import numpy as np

from includes import *
from common import *

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
    r.expect('ft.info', 'idx').equal('Unknown Index name')
    # time.sleep(1)

@skip(macos=True)
def test_mod4745(env):
    conn = getConnectionByEnv(env)
    r = env
    # Create an index with large dim so that a single indexing operation will take a long time
    N = 1000 * env.shardsCount
    dim = 30000
    for i in range(N):
        res = conn.execute_command('hset', f'foo:{i}', 'name', f'some string with information to index in the '
                                   f'background later on for id {i}',
                                   'v', create_np_array_typed(np.random.random((1, dim))).tobytes())
        env.assertEqual(res, 2)

    r.expect('ft.create', 'idx', 'schema', 'name', 'text', 'v', 'VECTOR', 'HNSW', '6', 'distance_metric', 'l2', 'DIM',
             dim, 'type', 'float32').ok()
    # Make sure that redis server is responsive while we index in the background (responding is less than 1s)
    for _ in range(5):
        start = time.time()
        conn.execute_command('PING')
        env.assertLess(time.time()-start, 1)
    # Make sure we are getting here without having cluster mark itself as fail since the server is not responsive and
    # fail to send cluster PING on time before we reach cluster-node-timeout.
    waitForIndex(r, 'idx')
