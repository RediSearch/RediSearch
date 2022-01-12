# -*- coding: utf-8 -*-

import unittest
from includes import *
from common import *
from time import sleep
from RLTest import Env
import math

def testCompression(env):
	accuracy = 0.000001
	repeat = int(math.sqrt(1 / accuracy))

	conn = getConnectionByEnv(env)
	pl = conn.pipeline()
	env.cmd('ft.create', 'idx', 'SCHEMA', 'n', 'numeric')
	for i in range(repeat):
		value = accuracy * i
		pl.execute_command('hset', i, 'n', str(value))
		if (i % 999) is 0:
			pl.execute()
	pl.execute()

	for i in range(repeat):
		value = accuracy * i
		env.expect('ft.search', 'idx', ('@n:[%s %s]' % (value, value))).equal([1L, str(i), ['n', str(value)]])
  
def testSanity(env):
	env.skipOnCluster()
	skipOnExistingEnv(env)
	repeat = 100000
	conn = getConnectionByEnv(env)
	env.cmd('ft.create', 'idx', 'SCHEMA', 'n', 'numeric')
	for i in range(repeat):
		conn.execute_command('hset', i, 'n', i % 100)
	env.expect('ft.search', 'idx', ('@n:[0 %d]' % (repeat)), 'limit', 0 ,0).equal([repeat])
	env.expect('FT.DEBUG', 'numidx_summary', 'idx', 'n') \
				.equal(['numRanges', 15L, 'numEntries', 100000L, 'lastDocId', 100000L, 'revisionId', 14L])

def testCompressionConfig(env):
	env.skipOnCluster()
	conn = getConnectionByEnv(env)
	env.cmd('ft.create', 'idx', 'SCHEMA', 'n', 'numeric')

	# w/o compression. exact number match.
	env.expect('ft.config', 'set', '_NUMERIC_COMPRESS', 'false').equal('OK')
	for i in range(100):
	  env.execute_command('hset', i, 'n', str(1 + i / 100.0))
	for i in range(100):
		num = str(1 + i / 100.0)
		env.expect('ft.search', 'idx', '@n:[%s %s]' % (num, num)).equal([1L, str(i), ['n', num]])

	# with compression. no exact number match.
	env.expect('ft.config', 'set', '_NUMERIC_COMPRESS', 'true').equal('OK')
	for i in range(100):
	  env.execute_command('hset', i, 'n', str(1 + i / 100.0))
	
	# delete keys where compression does not change value
	env.execute_command('del', '0')
	env.execute_command('del', '25')
	env.execute_command('del', '50')
	env.execute_command('del', '75')

	for i in range(100):
		num = str(1 + i / 100.0)
		env.expect('ft.search', 'idx', '@n:[%s %s]' % (num, num)).equal([0L])

def testRangeParentsConfig(env):
	env.skipOnCluster()
	elements = 1000
	conn = getConnectionByEnv(env)

	concurrent = env.cmd('ft.config', 'get', 'CONCURRENT_WRITE_MODE')
	if str(concurrent[0][1]) == 'true':
		env.skip()

	result = [['numRanges', 4L], ['numRanges', 6L]]
	for test in range(2):
		# check number of ranges
		env.cmd('ft.create', 'idx0', 'SCHEMA', 'n', 'numeric')
		for i in range(elements):
			env.execute_command('hset', i, 'n', i)
		actual_res = env.cmd('FT.DEBUG', 'numidx_summary', 'idx0', 'n')
		env.assertEqual(actual_res[0:2], result[test])

		# reset with old ranges parents param
		env.cmd('ft.drop', 'idx0')
		env.expect('ft.config', 'set', '_NUMERIC_RANGES_PARENTS', '2').equal('OK')

	# reset back
	env.expect('ft.config', 'set', '_NUMERIC_RANGES_PARENTS', '0').equal('OK')

def testEmptyNumericLeakIncrease(env):
    # test numeric field which updates with increasing value
    env.skipOnCluster()

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
