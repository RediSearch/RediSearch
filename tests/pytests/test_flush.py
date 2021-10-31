import time
import unittest


def testFlushDb(env):
    if env.isCluster():
        raise unittest.SkipTest()

    env.cmd('SELECT 0')
    env.cmd('SET foo bar')

    rv = env.cmd('GET foo')
    env.assertEqual('bar', rv)

    env.cmd('SELECT 1')
    env.cmd('FLUSHDB')
    env.cmd('SELECT 0')

    rv = env.cmd('GET foo')
    env.assertEqual('bar', rv)

    env.cmd('FLUSHDB')

    rv = env.cmd('GET foo')
    env.assertEqual(None, rv)

def testFlushAll(env):
    if env.isCluster():
        raise unittest.SkipTest()

    env.cmd('SET foo bar')

    rv = env.cmd('GET foo')
    env.assertEqual('bar', rv)

    env.cmd('FLUSHALL')

    rv = env.cmd('GET foo')
    env.assertEqual(None, rv)
