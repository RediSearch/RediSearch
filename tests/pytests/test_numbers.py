# -*- coding: utf-8 -*-

import unittest
from includes import *
from common import *
from time import sleep
from RLTest import Env
import math


# MOD-5815 TEST
@skip(cluster=True)
def testUniqueSum(env):

    # coordinator doesn't support FT.CONFIG FORK_GC_CLEAN_THRESHOLD

    hashes_number = 100

    values = [("int", str(3)), ("negative double", str(-0.4)), ("positive double",str(4.67))]

    env.expect('ft.config', 'set', 'FORK_GC_CLEAN_THRESHOLD', 0).equal('OK')

    for (title, value) in values:
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 'num', 'numeric').ok()

        # index documents with the same value
        for i in range(hashes_number):
            env.cmd('hset', f'doc:{i}', 'num', value)

        numeric_index_tree = dump_numeric_index_tree_root(env, 'idx', 'num')
        env.assertEqual((to_dict(numeric_index_tree['range']))['unique_sum'], value, message=f"{title} before gc")

        # delete one entry to trigger the gc
        env.cmd('hdel', 'doc:1', 'num')

        forceInvokeGC(env, 'idx')

        # Since we index the same value we expect the unique_sum to be equal to that value.
        # Before the fix, the gc casted the value from double to uint64, and then read this binary
        # representation back into unique_sum, which is double, without casting it back to double.
        # for example, for value = 1.0, the unique sum became ~1.482E-323
        numeric_index_tree = dump_numeric_index_tree_root(env, 'idx', 'num')
        env.assertEqual((to_dict(numeric_index_tree['range']))['unique_sum'], value, message=f"{title} after gc")

        env.cmd('flushall')

'''
This test checks the split decision efficiency and correctness of the tree.
PR#3892 fixes MOD-5815: the gc corrupted the uniquesum and cardinality values.

TLDR: In this test scenario, we made sure the gc runs before a node is splitted.
The gc corrupted the uniquesum and thr cardinality values (for small ints they were close to ~0).
As a result, the split value was determined only by the value of the document that triggered the split.
Eventually we ended up with moving *all* the entries to one leaf.

This test relies on the following values:
1. range->splitCard initial value = 16,
2. In each split the new nodes range->splitCard = 1 + range->splitCard * NR_EXPONENT,
where range is the splitted node, and NR_EXPONENT = 4.
range->splitCard = 16, 65, 261 ...
3. split condition: card * NR_CARD_CHECK >= n->range->splitCard,
where NR_CARD_CHECK = 10.
4. cardinality check interval (for each range) = NR_CARD_CHECK

First we index 19 docs with the same value (100)
Now the card = 1. We will check the cardinality in the next insertion.
We delete one document to cause the gc to re-write the index and its attributes,
including the uniquesum and the cardinality values, which were corrupted before the fix.

Insert a doc with a **different value** (10), to increase the cardinality and trigger a split.
(now card = 2,  card * NR_CARD_CHECK =20 > range->splitCard = 16)
split value = unique_sum / card,
where unique_sum is the sum of the cardinality values(100 + 10 = 110).
The split value should be = (100 + 10) / 2 = 55
All the 19 first docs go right, the new doc goes left.
'''
@skip(cluster=True)
def testSplit(env):

    env.expect('ft.config', 'set', 'FORK_GC_CLEAN_THRESHOLD', 0).equal('OK')

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'num', 'numeric').ok()

    value1 = '100'
    NR_CARD_CHECK = 10

    curr_docId = 0

    # Add documents until we get to one before the cardinality check
    for i in range(1, NR_CARD_CHECK * 2):
        env.cmd('hset', f'doc:{i}', 'num', value1)
        curr_docId = i

    numeric_index_tree = dump_numeric_index_tree_root(env, 'idx', 'num')
    env.assertEqual((to_dict(numeric_index_tree['range']))['unique_sum'], value1)
    env.assertEqual((to_dict(numeric_index_tree['range']))['card'], 1)
    env.assertEqual((to_dict(numeric_index_tree['range']))['cardCheck'], 1)

    # delete something to make sure the gc re writes the index
    env.cmd('hdel', f'doc:1', 'num')
#    forceInvokeGC(env, 'idx')
    forceInvokeGC(env, 'idx')

    # Add 10 more document with a different value to increase cardinality to 2, and trigger a split.
    value2 = '10'

    for i in range(NR_CARD_CHECK):
        env.cmd('hset', f'doc:{curr_docId + 1}', 'num', value2)
        curr_docId = i

    numeric_index_tree = dump_numeric_index_tree_root(env, 'idx', 'num')
    env.assertEqual(numeric_index_tree['value'], '55') # before fix = 10/2 = 5

    def checkRange(range, expected_val, expected_numEntries, msg):

        # before the fix: right = 110 , left = 0
        env.assertEqual(range['unique_sum'], expected_val, message = msg + ' unique_sum')
        # before the fix: right = 2, left = 0
        env.assertEqual(range['card'], 1, message = msg + ' card')
        # before the fix: right = 10, left = DBL_MAX
        env.assertEqual(range['minVal'], expected_val, message = msg + ' minVal')
        # before the fix: right = 100, left = DBL_MIN
        env.assertEqual(range['maxVal'], expected_val, message = msg + 'maxVal')

        inverted_index = to_dict(range['entries'])
        # before the fix: right = 20, left = 0
        env.assertEqual(inverted_index['numEntries'], expected_numEntries, message = msg+ ' numEntries')

    # Value1 = 100 goes right
    root_right = to_dict(to_dict(numeric_index_tree['right'])['range'])
    checkRange(root_right, value1, 18, "right leaf")

    # Value2 = 10 goes left
    root_left = to_dict(to_dict(numeric_index_tree['left'])['range'])
    checkRange(root_left, value2, 10, "left leaf")

@skip(cluster=True)
def testOverrides(env):

    env.expect('ft.config', 'set', 'FORK_GC_CLEAN_THRESHOLD', 0).equal('OK')

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'num', 'numeric').ok()

    loops = 10
    hashes_number = 10_000

    # Build the tree to get its structure statistics
    for i in range(hashes_number):
        env.cmd('hset', f'{i}', 'num', f'{i}')

    info = index_info(env, 'idx')
    expected_inverted_sz_mb = round(float(info['inverted_sz_mb']), 4)
    expected_root_max_depth = numeric_tree_summary(env, 'idx', 'num')['RootMaxDepth']

    for i in range(loops):

        # In each loop re-index 0, 1,...,`hashes_number`-1 entries with increasing values
        for j in range(hashes_number):
            env.cmd('hset', f'{j}', 'num', f'{j}')

        # explicitly run gc to update spec stats and the inverted index number of entries.
        forceInvokeGC(env, 'idx')

        # num records should be equal to the number of indexed hashes.
        info = index_info(env, 'idx')
        env.assertEqual(hashes_number, int(info['num_records']), message = "expected ft.info:num_records")

        # size in bytes shouldn't grow
        env.assertEqual(expected_inverted_sz_mb, round(float(info['inverted_sz_mb']), 4), message = "expected ft.info:inverted_sz_mb")

        # the tree depth was experimentally calculated, and should remain constant since we are using the same values.
        numeric_tree = numeric_tree_summary(env, 'idx', 'num')
        env.assertEqual(hashes_number, numeric_tree['numEntries'], message = "expected numEntries")
        env.assertEqual(0, numeric_tree['emptyLeaves'], message = "expected emptyLeaves")
        env.assertEqual(expected_root_max_depth, numeric_tree['RootMaxDepth'], message = "expected RootMaxDepth")

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

@skip(cluster=True)
def testSanity(env):
	skipOnExistingEnv(env)
	repeat = 100000
	conn = getConnectionByEnv(env)
	env.cmd('ft.create', 'idx', 'SCHEMA', 'n', 'numeric')
	for i in range(repeat):
		conn.execute_command('hset', i, 'n', i % 100)
	env.expect('ft.search', 'idx', ('@n:[0 %d]' % (repeat)), 'limit', 0 ,0).equal([repeat])
	env.expect('FT.DEBUG', 'numidx_summary', 'idx', 'n') \
				.equal(['numRanges', 15, 'numEntries', 100000, 'lastDocId', 100000, 'revisionId', 14, 'emptyLeaves', 0, 'RootMaxDepth', 5])

@skip(cluster=True)
def testCompressionConfig(env):
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

@skip(cluster=True)
def testRangeParentsConfig(env):
	elements = 1000

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

@skip(cluster=True)
def testEmptyNumericLeakIncrease(env):
    # test numeric field which updates with increasing value

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
    env.assertEqual(docs, num_summery_after['numEntries'])

    res = env.cmd('FT.SEARCH', 'idx', '@n:[-inf +inf]', 'NOCONTENT')
    env.assertEqual(res[0], docs)

@skip(cluster=True)
def testEmptyNumericLeakCenter(env):
    # keep documents 0 to 99 and rewrite docs 100 to 199
    # the value increases and reach `repeat * docs`
    # check that no empty node are left

	# Make sure GC is not triggerred sporadically (only manually)
    env.expect('FT.CONFIG', 'SET', 'FORK_GC_RUN_INTERVAL', 3600).equal('OK')
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

@skip(cluster=True)
def testCardinalityCrash(env):
    # this test reproduces crash where cardinality array was cleared on the GC
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
