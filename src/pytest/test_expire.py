import time
import unittest


def testExpire(env):
    if env.isCluster():
        raise unittest.SkipTest()
    env.cmd('ft.create', 'idx', 'TEMPORARY', '4', 'SCHEMA', 'test', 'TEXT', 'SORTABLE')
    ttl = env.cmd('ft.debug', 'ttl', 'idx')
    env.assertTrue(ttl > 2)

    while ttl > 2:
        ttl = env.cmd('ft.debug', 'ttl', 'idx')
        time.sleep(1)

    env.cmd('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'this is a simple test')
    ttl = env.cmd('ft.debug', 'ttl', 'idx')
    env.assertTrue(ttl > 2)

    while ttl > 2:
        ttl = env.cmd('ft.debug', 'ttl', 'idx')
        time.sleep(1)
    env.cmd('ft.search', 'idx', 'simple')
    ttl = env.cmd('ft.debug', 'ttl', 'idx')
    env.assertTrue(ttl > 2)

    while ttl > 2:
        ttl = env.cmd('ft.debug', 'ttl', 'idx')
        time.sleep(1)
    env.cmd('ft.aggregate', 'idx', 'simple', 'LOAD', '1', '@test')
    ttl = env.cmd('ft.debug', 'ttl', 'idx')
    env.assertTrue(ttl > 2)

    # while ttl != -2:
    #     ttl = int(env.cmd('ft.debug', 'ttl', 'idx'))
    #     time.sleep(1)
    # env.expect('keys', '*').equal([])
    while env.cmd('ft.info', '*') != []:
        time.sleep(0.5)
