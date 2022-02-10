# -*- coding: utf-8 -*-

import unittest
from includes import *
from common import *
from time import sleep
from RLTest import Env
import math

def testEmptyNumericLeakIncrease(env):
    # test numeric field which updates with increasing value
    env.skipOnCluster()
    env.expect('ft.config', 'set', 'FORK_GC_CLEAN_NUMERIC_EMPTY_NODES').ok()

    conn = getConnectionByEnv(env)
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC')

    repeat = 3
    docs = 10000

    for i in range(repeat):
        for j in range(docs):
            x = j + i * docs
            conn.execute_command('HSET', 'doc{}'.format(j), 'n', format(x))
        res = env.cmd('FT.SEARCH', 'idx', '@n:[-inf +inf]', 'NOCONTENT')
        env.assertEqual(res[0], docs)

    num_summery_before = env.cmd('FT.DEBUG', 'NUMIDX_SUMMARY', 'idx', 'n')
    forceInvokeGC(env, 'idx')
    num_summery_after = env.cmd('FT.DEBUG', 'NUMIDX_SUMMARY', 'idx', 'n')
    env.assertGreater(num_summery_before[1], num_summery_after[1])

    res = env.cmd('FT.SEARCH', 'idx', '@n:[-inf +inf]', 'NOCONTENT')
    env.assertEqual(res[0], docs)

def testEmptyNumericLeakCenter(env):
    # keep documents 0 to 99 and rewrite docs 100 to 199
    # the value increases and reach `repeat * docs`
    # check that no empty node are left
    env.skipOnCluster()
    env.expect('ft.config', 'set', 'FORK_GC_CLEAN_NUMERIC_EMPTY_NODES').ok()

    conn = getConnectionByEnv(env)
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC')

    repeat = 5
    docs = 10000

    for i in range(100):
        conn.execute_command('HSET', 'doc{}'.format(i), 'n', format(i))

    for i in range(repeat):
        for j in range(docs):
            x = j + i * docs
            conn.execute_command('HSET', 'doc{}'.format(j % 100 + 100), 'n', format(x))
        res = env.cmd('FT.SEARCH', 'idx', '@n:[-inf + inf]', 'NOCONTENT')
        env.assertEqual(res[0], docs / 100 + 100)

    num_summery_before = env.cmd('FT.DEBUG', 'NUMIDX_SUMMARY', 'idx', 'n')
    forceInvokeGC(env, 'idx')
    num_summery_after = env.cmd('FT.DEBUG', 'NUMIDX_SUMMARY', 'idx', 'n')
    env.assertGreater(num_summery_before[1], num_summery_after[1])

    res = env.cmd('FT.SEARCH', 'idx', '@n:[-inf + inf]', 'NOCONTENT')
    env.assertEqual(res[0], docs / 100 + 100)
