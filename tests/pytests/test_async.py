import unittest
import random
import time

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
    r.expect('ft.info', 'idx').equal('Unknown Index name')
    # time.sleep(1)


def test_mod4745(env):
    conn = getConnectionByEnv(env)
    if env.isCluster():
        # Decrease the timeout so we will be able to reproduce the issue in which the cluster nodes fail to send pings
        # while docs are being indexed in the background.
        for con in env.getOSSMasterNodesConnectionList():
            con.execute_command("config", "set", "cluster-node-timeout", "500")
    r = env
    N = 50000
    for i in range(N):
        res = conn.execute_command('hset', 'foo:%d' % i, 'name', f'some string with information to index in the '
                                                                 f'background later on for id {i}',
                                   'v', create_np_array_typed([i]*100).tobytes())
        env.assertEqual(res, 2)

    r.expect('ft.create', 'idx', 'schema', 'name', 'text', 'v', 'VECTOR', 'HNSW', '6', 'distance_metric', 'l2', 'DIM', 100,
             'type', 'float32').ok()
    # Here we are getting a failure in cluster (with COV) due to a starvation of the main thread, which makes the
    # cluster mark itself as fail
    waitForIndex(r, 'idx')
