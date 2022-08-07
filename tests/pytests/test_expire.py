import time
import unittest
from common import *
from RLTest import Env

def testExpireIndex(env):
    # temporary indexes
    if env.isCluster():
        raise unittest.SkipTest()
    env.cmd('ft.create', 'idx', 'TEMPORARY', '4', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT', 'SORTABLE')
    ttl = env.cmd('ft.debug', 'TTL', 'idx')
    env.assertTrue(ttl > 2)

    while ttl > 2:
        ttl = env.cmd('ft.debug', 'TTL', 'idx')
        time.sleep(1)
    env.cmd('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'this is a simple test')
    ttl = env.cmd('ft.debug', 'TTL', 'idx')
    env.assertTrue(ttl > 2)

    while ttl > 2:
        ttl = env.cmd('ft.debug', 'TTL', 'idx')
        time.sleep(1)
    env.cmd('ft.search', 'idx', 'simple')
    ttl = env.cmd('ft.debug', 'TTL', 'idx')
    env.assertTrue(ttl > 2)

    while ttl > 2:
        ttl = env.cmd('ft.debug', 'TTL', 'idx')
        time.sleep(1)
    env.cmd('ft.aggregate', 'idx', 'simple', 'LOAD', '1', '@test')
    ttl = env.cmd('ft.debug', 'TTL', 'idx')
    env.assertTrue(ttl > 2)

    try:
        while True:
            ttl = env.cmd('ft.debug', 'TTL', 'idx')
            time.sleep(1)
    except Exception as e:
        env.assertEqual(str(e), 'Unknown index name')

def testExpireDocs(env):
    env.skipOnCluster()
    conn = getConnectionByEnv(env)
    conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')

    conn.execute_command('FT.CREATE idx SCHEMA t TEXT')
    conn.execute_command('HSET', 'doc1', 't', 'foo')
    conn.execute_command('HSET', 'doc2', 't', 'bar')
    
    # both docs exist
    env.expect('FT.SEARCH', 'idx', '*').equal([2, 'doc1', ['t', 'foo'], 'doc2', ['t', 'bar']])

    conn.execute_command('PEXPIRE', 'doc1', 1)
    time.sleep(0.01)

    # both docs exist but doc1 fail to load field since they were expired passively
    env.expect('FT.SEARCH', 'idx', '*').equal([2, 'doc1', None, 'doc2', ['t', 'bar']])

    # only 1 doc is left
    env.expect('FT.SEARCH', 'idx', '*').equal([1, 'doc2', ['t', 'bar']])
