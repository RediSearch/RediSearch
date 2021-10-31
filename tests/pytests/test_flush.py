import time
import unittest


def testFlushDb(env):
    if env.isCluster():
        raise unittest.SkipTest()

    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'SCHEMA', 'f', 'TEXT', 'SORTABLE', 'test', 'TEXT', 'SORTABLE')
    env.cmd('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'f', 'field', 'test', 'test1')
    env.cmd('ft.add', 'idx', 'doc2', '1.0', 'FIELDS', 'f', 'field', 'test', 'test2')

    rv = env.cmd('ft.search', 'idx', '*', 'LIMIT', 0, 12345678)
    env.assertEqual(2, len(rv))

    env.cmd('flushdb 1')

    rv = env.cmd('ft.search', 'idx', '*', 'LIMIT', 0, 12345678)
    env.assertEqual(2, len(rv))

    env.cmd('flushdb 0')

    rv = env.cmd('ft.search', 'idx', '*', 'LIMIT', 0, 12345678)
    env.assertEqual(0, len(rv))

def testFlushAll(env):
    if env.isCluster():
        raise unittest.SkipTest()

    env.cmd('ft.create', 'idx', 'ON', 'HASH', 'SCHEMA', 'f', 'TEXT', 'SORTABLE', 'test', 'TEXT', 'SORTABLE')
    env.cmd('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'f', 'field', 'test', 'test1')
    env.cmd('ft.add', 'idx', 'doc2', '1.0', 'FIELDS', 'f', 'field', 'test', 'test2')

    rv = env.cmd('ft.search', 'idx', '*', 'LIMIT', 0, 12345678)
    env.assertEqual(2, len(rv))

    env.cmd('flushall')

    rv = env.cmd('ft.search', 'idx', '*', 'LIMIT', 0, 12345678)
    env.assertEqual(0, len(rv))
