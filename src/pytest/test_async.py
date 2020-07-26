import unittest
import random
import time

from includes import *
from common import waitForIndex

def getConnectionByEnv(env):
    conn = None
    if env.env == 'oss-cluster':
        conn = env.envRunner.getClusterConnection()
    else:
        conn = env.getConnection()
    return conn

def testCreateIndex(env):
    r = env
    N = 1000
    for i in range(N):
        r.expect('hset', 'foo:%d' % i, 'name', 'john doe').equal(1L)

    r.expect('ft.create', 'idx', 'ON', 'HASH', 'ASYNC', 'schema', 'name', 'text').ok()
    waitForIndex(r, 'idx')
    res = r.execute_command('ft.search', 'idx', 'doe', 'nocontent')
    env.assertEqual(N, res[0])

def testAlterIndex(env):
    if env.is_cluster():
        raise unittest.SkipTest()

    r = env
    N = 10000
    for i in range(N):
        r.expect('hset', 'foo:%d' % i, 'name', 'john doe', 'age', str(10 + i)).equal(2L)

    r.expect('ft.create', 'idx', 'ON', 'HASH', 'ASYNC', 'schema', 'name', 'text').ok()
    env.cmd('ft.alter', 'idx', 'schema', 'add', 'age', 'numeric')
    # note the two background scans
    waitForIndex(r, 'idx')
    res = r.execute_command('ft.search', 'idx', '@age: [10 inf]', 'nocontent')
    env.assertEqual(N, res[0])

def testDeleteIndex(env):
    r = env
    N = 100
    for i in range(N):
        r.expect('hset', 'foo:%d' % i, 'name', 'john doe').equal(1L)

    r.expect('ft.create', 'idx', 'ON', 'HASH', 'ASYNC', 'schema', 'name', 'text').ok()
    r.expect('ft.drop', 'idx').ok()
    r.expect('ft.info', 'idx').equal('Unknown Index name')
    # time.sleep(1)
