# -*- coding: utf-8 -*-

import unittest
from includes import *
from common import *
from time import sleep
from RLTest import Env
import math


def testNumEntries(env):
	env.skipOnCluster()

	env.expect('ft.config', 'set', 'FORK_GC_CLEAN_THRESHOLD', 0).equal('OK')

	env.expect('FT.CREATE', 'idx', 'SCHEMA', 'num', 'numeric').ok()

	loops = 15
	hashes_number = 10_000
	expected_depth = 17
 
	for i in range(loops):
		# In each loop re-index 0, 1,...,`hashes_number`-1 entries with increasing values
		for i in range(hashes_number):
			env.cmd('hset', f'{i}', 'num', f'{i}')

		# explicitly run gc to update spec stats and the inverted index number of entries.
		forceInvokeGC(env, 'idx')

		# num records should be equal to the number of indexed hashes.
		info = index_info(env, 'idx')
		env.assertEqual(int(info['num_records']), hashes_number)

		# the tree depth was experimentally calculated, and should remain constant since we are using the same values.
		numeric_tree_depth = dump_numeric_index_tree_root(env, 'idx', 'num')[
                    'maxDepth']
		env.assertGreaterEqual(expected_depth, numeric_tree_depth)

def testCompression(env):
	accuracy = 0.000001
	repeat = int(math.sqrt(1 / accuracy))

	conn = getConnectionByEnv(env)
	pl = conn.pipeline()
	env.cmd('ft.create', 'idx', 'SCHEMA', 'n', 'numeric')
	for i in range(repeat):
		value = accuracy * i
		pl.execute_command('hset', i, 'n', str(value))
		if (i % 999) == 0:
			pl.execute()
	pl.execute()

	for i in range(repeat):
		value = accuracy * i
		env.expect('ft.search', 'idx', ('@n:[%s %s]' % (value, value))).equal([1, str(i), ['n', str(value)]])

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
				.equal(['numRanges', 15, 'numEntries', 100000, 'lastDocId', 100000, 'revisionId', 14])

def testCompressionConfig(env):
	env.skipOnCluster()
	env.cmd('ft.create', 'idx', 'SCHEMA', 'n', 'numeric')

	# w/o compression. exact number match.
	env.expect('ft.config', 'set', '_NUMERIC_COMPRESS', 'false').equal('OK')
	for i in range(100):
	  	env.execute_command('hset', i, 'n', str(1 + i / 100.0))
	for i in range(100):
		num = str(1 + i / 100.0)
		env.expect('ft.search', 'idx', '@n:[%s %s]' % (num, num)).equal([1, str(i), ['n', num]])

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
		env.expect('ft.search', 'idx', '@n:[%s %s]' % (num, num)).equal([0])

def testRangeParentsConfig(env):
	env.skipOnCluster()
	elements = 1000

	concurrent = env.cmd('ft.config', 'get', 'CONCURRENT_WRITE_MODE')
	if str(concurrent[0][1]) == 'true':
		env.skip()

	result = [['numRanges', 4], ['numRanges', 6]]
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

    num_summery_before = to_dict(env.cmd('FT.DEBUG', 'NUMIDX_SUMMARY', 'idx', 'n'))
    forceInvokeGC(env, 'idx')
    num_summery_after = to_dict(env.cmd('FT.DEBUG', 'NUMIDX_SUMMARY', 'idx', 'n'))
    env.assertGreater(num_summery_before['numRanges'], num_summery_after['numRanges'])

    # test for PR#3018. check `numEntries` is updated after GC
    env.assertGreater(num_summery_before['numEntries'], num_summery_after['numEntries'])

    res = env.cmd('FT.SEARCH', 'idx', '@n:[-inf +inf]', 'NOCONTENT')
    env.assertEqual(res[0], docs)

def testEmptyNumericLeakCenter(env):
    # keep documents 0 to 99 and rewrite docs 100 to 199
    # the value increases and reach `repeat * docs`
    # check that no empty node are left

    env.skipOnCluster()

	# Make sure GC is not triggerred sporadically
    env.expect('FT.CONFIG', 'SET', 'FORK_GC_CLEAN_THRESHOLD', 0).equal('OK')
    
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

def testCardinalityCrash(env):
    # this test reproduces crash where cardinality array was cleared on the GC
    env.skipOnCluster()
    conn = getConnectionByEnv(env)
    count = 100

    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC')

    for i in range(count):
        conn.execute_command('HSET', 'doc{}'.format(i), 'n', format(i))

    for i in range(count):
        conn.execute_command('DEL', 'doc{}'.format(i))
    forceInvokeGC(env, 'idx')

    for i in range(count):
        conn.execute_command('HSET', 'doc{}'.format(i), 'n', format(i))
