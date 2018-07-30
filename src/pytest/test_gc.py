<<<<<<< 079de0c0fe6e8d101f6d0f4a308f2179b0ba498a
import time
import unittest

=======
from base_case import BaseSearchTestCase
>>>>>>> review fixes

def testBasicGC(env):
    if env.isCluster():
        raise unittest.SkipTest()
    env.assertOk(env.cmd('ft.create', 'idx', 'schema', 'title', 'text', 'id', 'numeric', 't', 'tag'))
    env.assertOk(env.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields',
                         'title', 'hello world',
                         'id', '5',
                         't', 'tag1'))

    env.assertOk(env.cmd('ft.add', 'idx', 'doc2', 1.0, 'fields',
                         'title', 'hello world 1',
                         'id', '7',
                         't', 'tag2'))

    env.assertEqual(env.cmd('ft.debug', 'DUMP_INVIDX', 'idx', 'world'), [1, 2])
    env.assertEqual(env.cmd('ft.debug', 'DUMP_NUMIDX', 'idx', 'id'), [[1, 2], [2], [1]])
    env.assertEqual(env.cmd('ft.debug', 'DUMP_TAGIDX', 'idx', 't'), [['tag1', [1]], ['tag2', [2]]])

    env.assertEqual(env.cmd('ft.del', 'idx', 'doc2'), 1)

    for i in range(100):
        # gc is random so we need to do it long enough times for it to work
        env.cmd('ft.debug', 'GC_FORCEINVOKE', 'idx')

    # check that the gc collected the deleted docs
    env.assertEqual(env.cmd('ft.debug', 'DUMP_INVIDX', 'idx', 'world'), [1])
    env.assertEqual(env.cmd('ft.debug', 'DUMP_NUMIDX', 'idx', 'id'), [[1], [], [1]])
    env.assertEqual(env.cmd('ft.debug', 'DUMP_TAGIDX', 'idx', 't'), [['tag1', [1]], ['tag2', []]])


def testNumerciGCIntensive(env):
    if env.isCluster():
        raise unittest.SkipTest()
    NumberOfDocs = 1000
    env.assertOk(env.cmd('ft.create', 'idx', 'schema', 'id', 'numeric'))

    for i in range(NumberOfDocs):
        env.assertOk(env.cmd('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields', 'id', str(i)))

    for i in range(0, NumberOfDocs, 2):
        env.assertEqual(env.cmd('ft.del', 'idx', 'doc%d' % i), 1)

    for i in range(100):
        env.cmd('ft.debug', 'GC_FORCEINVOKE', 'idx')

    res = env.cmd('ft.debug', 'DUMP_NUMIDX', 'idx', 'id')
    for r1 in res:
        for r2 in r1:
            env.assertEqual(r2 % 2, 0)


def testTagGC(env):
    if env.isCluster():
        raise unittest.SkipTest()
    NumberOfDocs = 10
    env.assertOk(env.cmd('ft.create', 'idx', 'schema', 't', 'tag'))

    for i in range(NumberOfDocs):
        env.assertOk(env.cmd('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields', 't', str(i)))

    for i in range(0, NumberOfDocs, 2):
        env.assertEqual(env.cmd('ft.del', 'idx', 'doc%d' % i), 1)

    for i in range(100):
        # gc is random so we need to do it long enough times for it to work
        env.cmd('ft.debug', 'GC_FORCEINVOKE', 'idx')

    res = env.cmd('ft.debug', 'DUMP_TAGIDX', 'idx', 't')
    for r1 in res:
        for r2 in r1[1]:
            env.assertEqual(r2 % 2, 0)
