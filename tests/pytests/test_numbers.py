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

    env.expect(config_cmd(), 'set', 'FORK_GC_CLEAN_THRESHOLD', 0).equal('OK')

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

    env.expect(config_cmd(), 'set', 'FORK_GC_CLEAN_THRESHOLD', 0).equal('OK')

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

    env.expect(config_cmd(), 'set', 'FORK_GC_CLEAN_THRESHOLD', 0).equal('OK')

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

        # size shouldn't vary more than 5% from the expected size.
        delta_size = abs(expected_inverted_sz_mb - round(float(info['inverted_sz_mb']), 4))/expected_inverted_sz_mb
        env.assertGreater(0.05, delta_size)

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
	env.expect(debug_cmd(), 'numidx_summary', 'idx', 'n') \
				.equal(['numRanges', 15, 'numEntries', 100000, 'lastDocId', 100000, 'revisionId', 14, 'emptyLeaves', 0, 'RootMaxDepth', 5])

@skip(cluster=True)
def testCompressionConfig(env):
	env.cmd('ft.create', 'idx', 'SCHEMA', 'n', 'numeric')

	# w/o compression. exact number match.
	env.expect(config_cmd(), 'set', '_NUMERIC_COMPRESS', 'false').equal('OK')
	for i in range(100):
	  	env.cmd('hset', i, 'n', str(1 + i / 100.0))
	for i in range(100):
		num = str(1 + i / 100.0)
		env.expect('ft.search', 'idx', '@n:[%s %s]' % (num, num)).equal([1, str(i), ['n', num]])

	# with compression. no exact number match.
	env.expect(config_cmd(), 'set', '_NUMERIC_COMPRESS', 'true').equal('OK')
	for i in range(100):
	  env.cmd('hset', i, 'n', str(1 + i / 100.0))

	# delete keys where compression does not change value
	env.cmd('del', '0')
	env.cmd('del', '25')
	env.cmd('del', '50')
	env.cmd('del', '75')

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
			env.cmd('hset', i, 'n', i)
		actual_res = env.cmd(debug_cmd(), 'numidx_summary', 'idx0', 'n')
		env.assertEqual(actual_res[0:2], result[test])

		# reset with old ranges parents param
		env.cmd('ft.drop', 'idx0')
		env.expect(config_cmd(), 'set', '_NUMERIC_RANGES_PARENTS', '2').equal('OK')

	# reset back
	env.expect(config_cmd(), 'set', '_NUMERIC_RANGES_PARENTS', '0').equal('OK')

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

    num_summery_before = to_dict(env.cmd(debug_cmd(), 'NUMIDX_SUMMARY', 'idx', 'n'))
    forceInvokeGC(env, 'idx')
    num_summery_after = to_dict(env.cmd(debug_cmd(), 'NUMIDX_SUMMARY', 'idx', 'n'))
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

	# Make sure GC is not triggered sporadically (only manually)
    env.expect(config_cmd(), 'SET', 'FORK_GC_RUN_INTERVAL', 3600).equal('OK')
    env.expect(config_cmd(), 'SET', 'FORK_GC_CLEAN_THRESHOLD', 0).equal('OK')

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

    num_summery_before = env.cmd(debug_cmd(), 'NUMIDX_SUMMARY', 'idx', 'n')
    forceInvokeGC(env, 'idx')
    num_summery_after = env.cmd(debug_cmd(), 'NUMIDX_SUMMARY', 'idx', 'n')
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

''' The following test aims to reproduce a bug:
During garbage collection the unique sum potentially became NaN.
The bug affects the determination of split values for nodes in the tree structure,
causing premature termination of searched due to NaN comparisons.
The bug was fixed in PR#3892.
More details in:
https://redislabs.atlassian.net/wiki/spaces/DX/pages/4054876404/BUG+numeric+index+suddenly+return+0+or+partial+results
'''

@skip(cluster=True) # coordinator doesn't suppory ft.config
def testNegativeValues(env):

    env.expect(config_cmd(), 'set', 'FORK_GC_CLEAN_THRESHOLD', 0).equal('OK')

    env.expect('FT.CREATE', 'idx', 'PREFIX', 1, 'doc:', 'SCHEMA', 'num', 'numeric').ok()

    # add 2 docs with the same negative value
    doc_id = 2
    for i in range(doc_id):
        val = -1
        env.cmd('hset', f'doc:{i}', 'num', val)

    # delete one a doc to trigger the gc.
    env.cmd('hdel', f'doc:1', 'num')
    forceInvokeGC(env, 'idx')
    doc_id -= 1

    # the unique_sum should be equal to the first value, as it is now the only value.
    numeric_index_tree = dump_numeric_index_tree_root(env, 'idx', 'num')
    range_data = to_dict(numeric_index_tree['range'])
    unique_sum = range_data['unique_sum']
    env.assertEqual(unique_sum, '-1')

    # add docs to trigger a split and calculate the expected unique_sum
    expected_unique_sum = int(unique_sum)
    split = False
    while split == False:
        val = - 1 - doc_id
        numeric_index_tree = dump_numeric_index_tree_root(env, 'idx', 'num')
        range_data = to_dict(numeric_index_tree['range'])
        #  The next hset will be added to the cardinality values and increase the cardinalities counter
        if range_data['cardCheck'] == 1:
            expected_unique_sum += val
            # when the cardinalities counter equals 2, the first tree split will be triggered.
            if range_data['card'] == 1:
                split = True
        env.cmd('hset', f'doc:{doc_id}', 'num', val)
        doc_id += 1


    # Before the bug fix, the split value of the root at this point was nan or some other unexpected value.
    # now we expect it to be expected_unique_sum / 2 (relying on the assumption that the split occurred when there were 2 cardinality values.)
    numeric_index_tree = dump_numeric_index_tree_root(env, 'idx', 'num')
    env.assertEqual(float(numeric_index_tree['value']), expected_unique_sum/2)

    # Query the index. if the split value of the root is nan, the query won't return any results.
    res = env.cmd('FT.SEARCH', 'idx', '@num:[-inf +inf]', 'NOCONTENT')
    env.assertEqual(res[0], doc_id)

def testNumberFormat(env):
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'numeric').ok()
    env.assertEqual(conn.execute_command('HSET', 'doc01', 'n', '1.0'), 1)
    env.assertEqual(conn.execute_command('HSET', 'doc02', 'n', '1'), 1)
    env.assertEqual(conn.execute_command('HSET', 'doc03', 'n', '1.0e0'), 1)
    env.assertEqual(conn.execute_command('HSET', 'doc04', 'n', '10e+2'), 1)
    env.assertEqual(conn.execute_command('HSET', 'doc05', 'n', '1.5e+2'), 1)
    env.assertEqual(conn.execute_command('HSET', 'doc06', 'n', '10e-2'), 1)
    env.assertEqual(conn.execute_command('HSET', 'doc07', 'n', '1.5e-2'), 1)
    env.assertEqual(conn.execute_command('HSET', 'doc08', 'n', 'INF'), 1)
    env.assertEqual(conn.execute_command('HSET', 'doc09', 'n', '1e6'), 1)
    env.assertEqual(conn.execute_command('HSET', 'doc10', 'n', 'iNf'), 1)
    env.assertEqual(conn.execute_command('HSET', 'doc11', 'n', '+INF'), 1)
    env.assertEqual(conn.execute_command('HSET', 'doc12', 'n', '+inf'), 1)
    env.assertEqual(conn.execute_command('HSET', 'doc13', 'n', '+iNf'), 1)
    env.assertEqual(conn.execute_command('HSET', 'doc14', 'n', '-INF'), 1)
    env.assertEqual(conn.execute_command('HSET', 'doc15', 'n', '-inf'), 1)
    env.assertEqual(conn.execute_command('HSET', 'doc16', 'n', '-iNf'), 1)
    env.assertEqual(conn.execute_command('HSET', 'doc17', 'n', '-1'), 1)

    # Test unsigned numbers
    expected = [3, 'doc01', 'doc02', 'doc03']
    res = env.cmd('FT.SEARCH', 'idx', '@n:[1 1]', 'NOCONTENT', 'WITHCOUNT')
    env.assertEqual(res, expected)
    res = env.cmd('FT.SEARCH', 'idx', '@n:[1e0 1]', 'NOCONTENT', 'WITHCOUNT')
    env.assertEqual(res, expected)
    # Breaking change, should be solved in major version
    # res = env.cmd('FT.SEARCH', 'idx', '@n:[.1e1 .1e+1]', 'NOCONTENT', 'WITHCOUNT')
    # env.assertEqual(res, expected)

    # Test signed numbers
    res = env.cmd('FT.SEARCH', 'idx', '@n:[+1e0 +1]', 'NOCONTENT', 'WITHCOUNT')
    env.assertEqual(res, expected)

    res = env.cmd('FT.SEARCH', 'idx', '@n:[-1e0 -1]', 'NOCONTENT')
    env.assertEqual(res, [1, 'doc17'])

    # Test +inf
    res1 = env.cmd('FT.SEARCH', 'idx', '@n:[1e6 inf]', 'NOCONTENT', 'WITHCOUNT')
    expected = [6, 'doc08', 'doc09', 'doc10', 'doc11', 'doc12', 'doc13']
    env.assertEqual(res1, expected)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n:[1e6 INF]', 'NOCONTENT', 'WITHCOUNT')
    env.assertEqual(res2, expected)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n:[1e6 +inf]', 'NOCONTENT', 'WITHCOUNT')
    env.assertEqual(res2, expected)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n:[1e6 +INF]', 'NOCONTENT', 'WITHCOUNT')
    env.assertEqual(res2, expected)

    # Test -inf
    res1 = env.cmd('FT.SEARCH', 'idx', '@n:[-inf 0]', 'NOCONTENT', 'WITHCOUNT')
    expected = [4, 'doc14', 'doc15', 'doc16', 'doc17']
    env.assertEqual(res1, expected)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n:[-INF 0]', 'NOCONTENT', 'WITHCOUNT')
    env.assertEqual(res2, expected)

    # Test float numbers
    res1 = env.cmd('FT.SEARCH', 'idx', '@n:[-0.1 0.1]', 'NOCONTENT', 'WITHCOUNT')
    expected = [2, 'doc06', 'doc07']
    env.assertEqual(res1, expected)
    # Breaking change, should be solved in major version
    # res2 = env.cmd('FT.SEARCH', 'idx', '@n:[-.1 +.1]', 'NOCONTENT', 'WITHCOUNT')
    # env.assertEqual(res2, expected)
    # res2 = env.cmd('FT.SEARCH', 'idx', '@n:[-  .1 +  .1]', 'NOCONTENT', 'WITHCOUNT')
    # env.assertEqual(res2, expected)

def testNumericOperators():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC').ok()
    waitForIndex(env, 'idx')

    env.assertEqual(conn.execute_command('HSET', 'key1', 'n', '11'), 1)
    env.assertEqual(conn.execute_command('HSET', 'key2', 'n', '12'), 1)
    env.assertEqual(conn.execute_command('HSET', 'key3', 'n', '13'), 1)
    env.assertEqual(conn.execute_command('HSET', 'key4', 'n', '14'), 1)
    env.assertEqual(conn.execute_command('HSET', 'key5', 'n', '15'), 1)
    env.assertEqual(conn.execute_command('HSET', 'key6', 'n', '-10'), 1)
    env.assertEqual(conn.execute_command('HSET', 'key7', 'n', '-11'), 1)
    env.assertEqual(conn.execute_command('HSET', 'key8', 'n', '3.14'), 1)
    env.assertEqual(conn.execute_command('HSET', 'key9', 'n', '-3.14'), 1)
    env.assertEqual(conn.execute_command('HSET', 'key10', 'n', '+inf'), 1)
    env.assertEqual(conn.execute_command('HSET', 'key11', 'n', 'inf'), 1)
    env.assertEqual(conn.execute_command('HSET', 'key12', 'n', '-inf'), 1)

    # Test >= and <=
    res1 = env.cmd('FT.SEARCH', 'idx', '@n>=12 @n<=14', 'NOCONTENT', 'WITHCOUNT')
    env.assertEqual(res1, [3, 'key2', 'key3', 'key4'])
    res2 = env.cmd('FT.SEARCH', 'idx', '@n>= +12 @n<=+14', 'NOCONTENT',
                   'WITHCOUNT')
    env.assertEqual(res2, res1)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n>=$min @n<=$max', 'NOCONTENT',
                   'WITHCOUNT', 'PARAMS', '4', 'min', '12', 'max', '14')
    env.assertEqual(res2, res1)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n>=+$min @n<= +$max', 'NOCONTENT',
                   'WITHCOUNT', 'PARAMS', '4', 'min', '12', 'max', '14')
    env.assertEqual(res2, res1)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n>=-$min @n<= -$max', 'NOCONTENT',
                   'WITHCOUNT', 'PARAMS', '4', 'min', '-12', 'max', '-14')
    env.assertEqual(res2, res1)

    res1 = env.cmd('FT.SEARCH', 'idx', '@n>=3.14 @n<=3.14', 'NOCONTENT')
    env.assertEqual(res1, [1, 'key8'])

    # Test > and <=
    res1 = env.cmd('FT.SEARCH', 'idx', '@n>12 @n<=14', 'NOCONTENT', 'WITHCOUNT')
    env.assertEqual(res1, [2, 'key3', 'key4'])
    res2 = env.cmd('FT.SEARCH', 'idx', '@n>$min @n<=$max', 'NOCONTENT',
                   'WITHCOUNT', 'PARAMS', '4', 'min', '12', 'max', '14')
    env.assertEqual(res2, res1)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n>+$min @n<=+$max', 'NOCONTENT',
                   'WITHCOUNT', 'PARAMS', '4', 'min', '12', 'max', '14')
    env.assertEqual(res2, res1)

    res1 = env.cmd('FT.SEARCH', 'idx', '@n>0 @n<=3.14', 'NOCONTENT')
    env.assertEqual(res1, [1, 'key8'])

    res1 = env.cmd('FT.SEARCH', 'idx', '@n>3.14 @n<=3.14', 'NOCONTENT')
    env.assertEqual(res1, [0])

    # Test >= and <
    res1 = env.cmd('FT.SEARCH', 'idx', '@n>=12 @n<14', 'NOCONTENT', 'WITHCOUNT')
    env.assertEqual(res1, [2, 'key2', 'key3'])
    res2 = env.cmd('FT.SEARCH', 'idx', '@n>=+12 @n< +14', 'NOCONTENT',
                   'WITHCOUNT')
    env.assertEqual(res2, res1)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n>=$min @n<$max', 'NOCONTENT',
                   'WITHCOUNT', 'PARAMS', '4', 'min', '12', 'max', '14')
    env.assertEqual(res2, res1)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n>=+$min @n< +$max', 'NOCONTENT',
                   'WITHCOUNT', 'PARAMS', '4', 'min', '12', 'max', '14')
    env.assertEqual(res2, res1)
    res1 = env.cmd('FT.SEARCH', 'idx', '@n>=3.14 @n<11.5', 'NOCONTENT',
                   'WITHCOUNT')
    env.assertEqual(res1, [2, 'key1', 'key8'])

    # Test > and <
    res1 = env.cmd('FT.SEARCH', 'idx', '@n>12 @n<14', 'NOCONTENT')
    env.assertEqual(res1, [1, 'key3'])
    res2 = env.cmd('FT.SEARCH', 'idx', '@n>$min @n<$max', 'NOCONTENT',
                   'PARAMS', '4', 'min', '12', 'max', '14')
    env.assertEqual(res2, res1)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n> $min @n<+$max', 'NOCONTENT',
                   'PARAMS', '4', 'min', '12', 'max', '14')
    env.assertEqual(res2, res1)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n<+$max   @n> $min', 'NOCONTENT',
                   'PARAMS', '4', 'min', '12', 'max', '14')
    env.assertEqual(res2, res1)

    res1 = env.cmd('FT.SEARCH', 'idx', '@n>3.14 @n<11.5', 'NOCONTENT')
    env.assertEqual(res1, [1, 'key1'])

    # Test >
    res1 = env.cmd('FT.SEARCH', 'idx', '@n>12', 'NOCONTENT',
                   'SORTBY', 'n', 'ASC')
    env.assertEqual(res1, [5, 'key3', 'key4', 'key5', 'key10', 'key11'])
    res2 = env.cmd('FT.SEARCH', 'idx', '@n>$min', 'NOCONTENT',
                   'PARAMS', '2', 'min', '12', 'SORTBY', 'n', 'ASC')
    env.assertEqual(res2, res1)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n>+$min', 'NOCONTENT',
                   'PARAMS', '2', 'min', '12', 'SORTBY', 'n', 'ASC')
    env.assertEqual(res2, res1)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n> +$min', 'NOCONTENT',
                   'PARAMS', '2', 'min', '12', 'SORTBY', 'n', 'ASC')
    env.assertEqual(res2, res1)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n> +$min   @n> +11', 'NOCONTENT',
                   'PARAMS', '2', 'min', '12', 'SORTBY', 'n', 'ASC')
    env.assertEqual(res2, res1)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n> -$min', 'NOCONTENT',
                   'PARAMS', '2', 'min', '-12', 'SORTBY', 'n', 'ASC')
    env.assertEqual(res2, res1)

    # Test > +inf
    res1 = env.cmd('FT.SEARCH', 'idx', '@n>inf', 'NOCONTENT')
    env.assertEqual(res1, [0])
    res2 = env.cmd('FT.SEARCH', 'idx', '@n>+$p', 'NOCONTENT',
                   'PARAMS', 2, 'p', '+inf')
    env.assertEqual(res2, res1)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n>+$p', 'NOCONTENT',
                   'PARAMS', 2, 'p', 'inf')
    env.assertEqual(res2, res1)

    # Test > -inf
    res1 = env.cmd('FT.SEARCH', 'idx', '@n>-inf', 'NOCONTENT', 'LIMIT', 0, 20,
                   'SORTBY', 'n', 'ASC')
    env.assertEqual(res1, [11, 'key7', 'key6', 'key9', 'key8', 'key1', 'key2',
                           'key3', 'key4', 'key5', 'key10', 'key11'])
    res2 = env.cmd('FT.SEARCH', 'idx', '@n>$p', 'NOCONTENT', 'PARAMS', 2,
                   'p', '-inf', 'LIMIT', 0, 20, 'SORTBY', 'n', 'ASC')
    env.assertEqual(res2, res1)

    # Test >= +inf
    res1 = env.cmd('FT.SEARCH', 'idx', '@n>=inf', 'NOCONTENT', 'WITHCOUNT')
    env.assertEqual(res1, [2, 'key10', 'key11'])
    res2 = env.cmd('FT.SEARCH', 'idx', '@n>=+$p', 'NOCONTENT', 'WITHCOUNT',
                   'PARAMS', 2, 'p', '+inf')
    env.assertEqual(res2, res1)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n>=+$p', 'NOCONTENT', 'WITHCOUNT',
                   'PARAMS', 2, 'p', 'inf')
    env.assertEqual(res2, res1)

    # Test >= -inf
    res1 = env.cmd('FT.SEARCH', 'idx', '@n>=-inf', 'NOCONTENT', 'LIMIT', 0, 20,
                   'SORTBY', 'n', 'ASC')
    env.assertEqual(res1, [12, 'key12', 'key7', 'key6', 'key9', 'key8', 'key1',
                           'key2', 'key3', 'key4', 'key5', 'key10', 'key11'])
    res2 = env.cmd('FT.SEARCH', 'idx', '@n>=$p', 'NOCONTENT',
                   'PARAMS', 2, 'p', '-inf', 'LIMIT', 0, 20,
                   'SORTBY', 'n', 'ASC')
    env.assertEqual(res2, res1)

    # Test <
    res1 = env.cmd('FT.SEARCH', 'idx', '@n<15', 'NOCONTENT',
                   'SORTBY', 'n', 'ASC')
    env.assertEqual(res1, [9, 'key12', 'key7', 'key6', 'key9', 'key8', 'key1',
                           'key2', 'key3', 'key4'])
    res2 = env.cmd('FT.SEARCH', 'idx', '@n<$max', 'NOCONTENT',
                   'PARAMS', '2', 'max', '15', 'SORTBY', 'n', 'ASC')
    env.assertEqual(res2, res1)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n<+$max', 'NOCONTENT',
                   'PARAMS', '2', 'max', '15', 'SORTBY', 'n', 'ASC')
    env.assertEqual(res2, res1)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n< +$max', 'NOCONTENT',
                   'PARAMS', '2', 'max', '15', 'SORTBY', 'n', 'ASC')
    env.assertEqual(res2, res1)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n< +$max  @n < 20', 'NOCONTENT',
                   'PARAMS', '2', 'max', '15', 'SORTBY', 'n', 'ASC')
    env.assertEqual(res2, res1)

    # Test < +inf
    res1 = env.cmd('FT.SEARCH', 'idx', '@n<inf', 'NOCONTENT',
                   'SORTBY', 'n', 'ASC')
    env.assertEqual(res1, [10, 'key12', 'key7', 'key6', 'key9', 'key8', 'key1',
                           'key2', 'key3', 'key4', 'key5'])
    res2 = env.cmd('FT.SEARCH', 'idx', '@n<+$p', 'NOCONTENT',
                   'PARAMS', 2, 'p', '+inf', 'SORTBY', 'n', 'ASC')
    env.assertEqual(res2, res1)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n<+$p', 'NOCONTENT',
                   'PARAMS', 2, 'p', 'inf', 'SORTBY', 'n', 'ASC')
    env.assertEqual(res2, res1)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n<-$p', 'NOCONTENT',
                   'PARAMS', 2, 'p', '-inf', 'SORTBY', 'n', 'ASC')
    env.assertEqual(res2, res1)

    # Test < -inf
    res1 = env.cmd('FT.SEARCH', 'idx', '@n<-inf', 'NOCONTENT')
    env.assertEqual(res1, [0])
    res2 = env.cmd('FT.SEARCH', 'idx', '@n<$p', 'NOCONTENT',
                   'PARAMS', 2, 'p', '-inf')
    env.assertEqual(res2, res1)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n<-$p', 'NOCONTENT',
                   'PARAMS', 2, 'p', 'inf')
    env.assertEqual(res2, res1)

    # Test <= +inf
    res1 = env.cmd('FT.SEARCH', 'idx', '@n<=inf', 'NOCONTENT', 'LIMIT', 0, 12,
                   'SORTBY', 'n', 'ASC')
    env.assertEqual(res1, [12, 'key12', 'key7', 'key6', 'key9', 'key8', 'key1',
                           'key2', 'key3', 'key4', 'key5', 'key10', 'key11'])
    res2 = env.cmd('FT.SEARCH', 'idx', '@n<=$p', 'NOCONTENT',
                   'PARAMS', 2, 'p', 'inf', 'LIMIT', 0, 12,
                   'SORTBY', 'n', 'ASC')
    env.assertEqual(res2, res1)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n<=+$p', 'NOCONTENT',
                   'PARAMS', 2, 'p', 'inf', 'LIMIT', 0, 12,
                   'SORTBY', 'n', 'ASC')
    env.assertEqual(res2, res1)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n<=+$p', 'NOCONTENT',
                   'PARAMS', 2, 'p', '+inf', 'LIMIT', 0, 12,
                   'SORTBY', 'n', 'ASC')
    env.assertEqual(res2, res1)

    # Test <= -inf
    res1 = env.cmd('FT.SEARCH', 'idx', '@n<=-inf', 'NOCONTENT')
    env.assertEqual(res1, [1, 'key12'])
    res2 = env.cmd('FT.SEARCH', 'idx', '@n<=$p', 'NOCONTENT',
                   'PARAMS', 2, 'p', '-inf')
    env.assertEqual(res2, res1)

    # Test ==
    res1 = env.cmd('FT.SEARCH', 'idx', '@n==15', 'NOCONTENT')
    env.assertEqual(res1, [1, 'key5'])
    res2 = env.cmd('FT.SEARCH', 'idx', '@n==$n', 'NOCONTENT', 'PARAMS', 2,
                   'n', '15')
    env.assertEqual(res2, res1)
    res1 = env.cmd('FT.SEARCH', 'idx', '@n==+$n', 'NOCONTENT', 'PARAMS', 2,
                   'n', '15')
    env.assertEqual(res2, res1)
    res1 = env.cmd('FT.SEARCH', 'idx', '@n==+$n   @n==15', 'NOCONTENT',
                   'PARAMS', 2, 'n', '15')
    env.assertEqual(res2, res1)

    res1 = env.cmd('FT.SEARCH', 'idx', '@n==-15', 'NOCONTENT')
    env.assertEqual(res1, [0])

    res1 = env.cmd('FT.SEARCH', 'idx', '@n==-11', 'NOCONTENT')
    env.assertEqual(res1, [1, 'key7'])

    res1 = env.cmd('FT.SEARCH', 'idx', '@n==-11 | @n==-10', 'NOCONTENT',
                   'WITHCOUNT')
    env.assertEqual(res1, [2, 'key6', 'key7'])

    # Test == double number
    res1 = env.cmd('FT.SEARCH', 'idx', '@n==3.14', 'NOCONTENT')
    env.assertEqual(res1, [1, 'key8'])
    res2 = env.cmd('FT.SEARCH', 'idx', '@n:[3.14 3.14]', 'NOCONTENT')
    env.assertEqual(res2, res1)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n==+$n', 'NOCONTENT', 'PARAMS', 2,
                   'n', '3.14')
    env.assertEqual(res2, res1)


    res1 = env.cmd('FT.SEARCH', 'idx', '@n==-3.14', 'NOCONTENT')
    env.assertEqual(res1, [1, 'key9'])
    res2 = env.cmd('FT.SEARCH', 'idx', '@n:[-3.14 -3.14]', 'NOCONTENT')
    env.assertEqual(res2, res1)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n==+$n', 'NOCONTENT', 'PARAMS', 2,
                   'n', '-3.14')
    env.assertEqual(res2, res1)

    # Test == +inf
    res1 = env.cmd('FT.SEARCH', 'idx', '@n==inf', 'NOCONTENT', 'WITHCOUNT')
    env.assertEqual(res1, [2, 'key10', 'key11'])
    res2 = env.cmd('FT.SEARCH', 'idx', '@n:[inf inf]', 'NOCONTENT', 'WITHCOUNT')
    env.assertEqual(res2, res1)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n==+inf', 'NOCONTENT', 'WITHCOUNT')
    env.assertEqual(res2, res1)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n==$n', 'NOCONTENT', 'WITHCOUNT',
                   'PARAMS', 2, 'n', 'inf')
    env.assertEqual(res2, res1)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n==$n', 'NOCONTENT', 'WITHCOUNT',
                   'PARAMS', 2, 'n', '+inf')
    env.assertEqual(res2, res1)

    # Test == -inf
    res1 = env.cmd('FT.SEARCH', 'idx', '@n==-inf', 'NOCONTENT')
    env.assertEqual(res1, [1, 'key12'])
    res2 = env.cmd('FT.SEARCH', 'idx', '@n:[-inf -inf]', 'NOCONTENT')
    env.assertEqual(res2, res1)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n==$n', 'NOCONTENT', 'PARAMS', 2,
                   'n', '-inf')
    env.assertEqual(res2, res1)

    # Test !=
    res1 = env.cmd('FT.SEARCH', 'idx', '@n!=12 @n!= -10', 'NOCONTENT',
                   'SORTBY', 'n', 'ASC')
    env.assertEqual(res1, [10, 'key12', 'key7', 'key9', 'key8', 'key1', 'key3',
                           'key4', 'key5', 'key10', 'key11'])
    res2 = env.cmd('FT.SEARCH', 'idx', '@n!=+12 @n!= -10', 'NOCONTENT',
                   'SORTBY', 'n', 'ASC')
    env.assertEqual(res2, res1)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n!= $n @n!=$m', 'NOCONTENT',
                   'PARAMS', 4, 'n', '12', 'm', '-10', 'SORTBY', 'n', 'ASC')
    env.assertEqual(res2, res1)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n!= +$n @n!=+$m', 'NOCONTENT',
                   'PARAMS', 4, 'n', '+12', 'm', '-10', 'SORTBY', 'n', 'ASC')
    env.assertEqual(res2, res1)

    # Test != double number
    res1 = env.cmd('FT.SEARCH', 'idx', '@n!=3.14', 'NOCONTENT', 'LIMIT', 0, 20,
                   'SORTBY', 'n', 'ASC')
    env.assertEqual(res1, [11, 'key12', 'key7', 'key6', 'key9', 'key1', 'key2',
                           'key3', 'key4', 'key5', 'key10', 'key11'])
    res2 = env.cmd('FT.SEARCH', 'idx', '@n:[-inf (3.14] | @n:[(3.14 +inf]',
                   'NOCONTENT', 'WITHCOUNT', 'LIMIT', 0, 20,
                   'SORTBY', 'n', 'ASC')
    env.assertEqual(res2, res1)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n!=+$n', 'NOCONTENT',
                   'PARAMS', 2, 'n', '3.14', 'LIMIT', 0, 20,
                   'SORTBY', 'n', 'ASC')
    env.assertEqual(res2, res1)

    res1 = env.cmd('FT.SEARCH', 'idx', '@n!=-3.14', 'NOCONTENT',
                   'LIMIT', 0, 20, 'SORTBY', 'n', 'ASC')
    env.assertEqual(res1, [11, 'key12', 'key7', 'key6', 'key8', 'key1', 'key2',
                           'key3', 'key4', 'key5', 'key10', 'key11'])
    res2 = env.cmd('FT.SEARCH', 'idx', '@n:[-inf (-3.14] | @n:[(-3.14 +inf]',
                   'NOCONTENT', 'WITHCOUNT', 'LIMIT', 0, 20, 'SORTBY', 'n', 'ASC')
    env.assertEqual(res2, res1)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n!=+$n', 'NOCONTENT',
                   'PARAMS', 2, 'n', '-3.14', 'LIMIT', 0, 20,
                   'SORTBY', 'n', 'ASC')
    env.assertEqual(res2, res1)

    # Test != +inf
    res1 = env.cmd('FT.SEARCH', 'idx', '@n!=inf', 'NOCONTENT',
                   'SORTBY', 'n', 'ASC')
    env.assertEqual(res1, [10, 'key12', 'key7', 'key6', 'key9', 'key8', 'key1',
                           'key2', 'key3', 'key4', 'key5'])
    res2 = env.cmd('FT.SEARCH', 'idx', '@n!=+inf', 'NOCONTENT',
                   'SORTBY', 'n', 'ASC')
    env.assertEqual(res2, res1)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n!=$n', 'NOCONTENT', 'PARAMS', 2,
                   'n', 'inf', 'SORTBY', 'n', 'ASC')
    env.assertEqual(res2, res1)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n!=$n', 'NOCONTENT', 'PARAMS', 2,
                   'n', '+inf', 'SORTBY', 'n', 'ASC')
    env.assertEqual(res2, res1)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n!=+$n', 'NOCONTENT', 'PARAMS', 2,
                     'n', 'inf', 'SORTBY', 'n', 'ASC')
    env.assertEqual(res2, res1)

    # Test != -inf
    res1 = env.cmd('FT.SEARCH', 'idx', '@n!=-inf', 'NOCONTENT', 'LIMIT', 0, 20,
                   'SORTBY', 'n', 'ASC')
    env.assertEqual(res1, [11, 'key7', 'key6', 'key9', 'key8', 'key1', 'key2',
                           'key3', 'key4', 'key5', 'key10', 'key11'])
    res2 = env.cmd('FT.SEARCH', 'idx', '@n!=$n', 'NOCONTENT',
                   'PARAMS', 2, 'n', '-inf', 'LIMIT', 0, 20,
                   'SORTBY', 'n', 'ASC')
    env.assertEqual(res2, res1)
    res2 = env.cmd('FT.SEARCH', 'idx', '@n!=+$n', 'NOCONTENT',
                   'PARAMS', 2, 'n', '-inf', 'LIMIT', 0, 20,
                   'SORTBY', 'n', 'ASC')
    env.assertEqual(res2, res1)

    # Test range and operator in the same query
    res1 = env.cmd('FT.SEARCH', 'idx', '@n==3.14 | @n:[10 13]', 'NOCONTENT',
                   'WITHCOUNT')
    env.assertEqual(res1, [4, 'key1', 'key2', 'key3', 'key8'])
    res2 = env.cmd('FT.SEARCH', 'idx', '@n:[3.14 3.14] | @n>=10 @n<=13',
                   'NOCONTENT', 'WITHCOUNT')
    env.assertEqual(res2, res1)

    res1 = env.cmd('FT.SEARCH', 'idx', '@n==11 | @n:[10 13]', 'NOCONTENT',
                   'WITHCOUNT')
    env.assertEqual(res1, [3, 'key1', 'key2', 'key3'])
    res2 = env.cmd('FT.SEARCH', 'idx', '@n:[11 11] | @n>=10 @n<=13',
                   'NOCONTENT', 'WITHCOUNT')
    env.assertEqual(res2, res1)

    # Test contradiction query
    res1 = env.cmd('FT.SEARCH', 'idx', '@n==3.14 @n!=3.14', 'NOCONTENT')
    env.assertEqual(res1, [0])

    res1 = env.cmd('FT.SEARCH', 'idx', '@n==12 @n==11', 'NOCONTENT')
    env.assertEqual(res1, [0])

    res1 = env.cmd('FT.SEARCH', 'idx', '@n<10 @n>12', 'NOCONTENT')
    env.assertEqual(res1, [0])

    res1 = env.cmd('FT.SEARCH', 'idx', '@n==10 @n:[12 15]', 'NOCONTENT')
    env.assertEqual(res1, [0])

    # It is valid to have spaces between the operator and the value
    for operator in ['==', '!=', '>', '>=', '<', '<=']:
        res1 = env.cmd('FT.SEARCH', 'idx', '@n' + operator + '0')
        res2 = env.cmd('FT.SEARCH', 'idx', '@n' + operator + ' 0')
        env.assertEqual(res1, res2)
        res2 = env.cmd('FT.SEARCH', 'idx', '@n ' + operator + '0')
        env.assertEqual(res1, res2)
        res2 = env.cmd('FT.SEARCH', 'idx', '@n ' + operator + ' 0')
        env.assertEqual(res1, res2)
        res2 = env.cmd('FT.SEARCH', 'idx', '@n ' + operator + ' $p',
                       'PARAMS', 2, 'p', '0')
        env.assertEqual(res1, res2)

    # Invalid syntax
    for operator in ['==', '!=', '>', '>=', '<', '<=']:
        env.expect('FT.SEARCH', 'idx', '@n' + operator + '(105').error()
        env.expect('FT.SEARCH', 'idx', '@n' + operator + '+(105').error()
        env.expect('FT.SEARCH', 'idx', '@n' + operator + '-(105').error()
        env.expect('FT.SEARCH', 'idx', '@n' + operator + '(-105').error()
        env.expect('FT.SEARCH', 'idx', '@n' + operator + '(inf').error()
        env.expect('FT.SEARCH', 'idx', '@n' + operator + '(-inf').error()
        env.expect('FT.SEARCH', 'idx', '@n' + operator + '(+inf').error()
        env.expect('FT.SEARCH', 'idx', '@n' + operator + '-(inf').error()
        env.expect('FT.SEARCH', 'idx', '@n' + operator + '+(inf').error()
        env.expect('FT.SEARCH', 'idx', '@n' + operator + '($param',
                'PARAMS', 2, 'param', 100).error()
        env.expect('FT.SEARCH', 'idx', '@n' + operator + '+($param',
                'PARAMS', 2, 'param', 100).error()
        env.expect('FT.SEARCH', 'idx', '@n' + operator + '-($param',
                'PARAMS', 2, 'param', 100).error()
        env.expect('FT.SEARCH', 'idx', '@n' + operator + '(+$param',
                'PARAMS', 2, 'param', 100).error()
        env.expect('FT.SEARCH', 'idx', '@n' + operator + '(-$param',
                'PARAMS', 2, 'param', 100).error()
        env.expect('FT.SEARCH', 'idx', '@n' + operator + 'w').error()\
            .contains('Syntax error')
        env.expect('FT.SEARCH', 'idx', '@n' + operator + '$p', 'PARAMS', 2,
                   'p', 'w').error().contains('Invalid numeric value')

    # If the operator has two characters, it can't have spaces between them
    env.expect('FT.SEARCH', 'idx', "@n! =1").error().contains('Syntax error')
    env.expect('FT.SEARCH', 'idx', "@n> =1").error().contains('Syntax error')
    env.expect('FT.SEARCH', 'idx', "@n< =1").error().contains('Syntax error')
    env.expect('FT.SEARCH', 'idx', "@n= =1").error().contains('Syntax error')

    # Invalid operators
    env.expect('FT.SEARCH', 'idx', "@n===1").error().contains('Syntax error')
    env.expect('FT.SEARCH', 'idx', "@n!!=1").error().contains('Syntax error')
    env.expect('FT.SEARCH', 'idx', "@n!>=1").error().contains('Syntax error')
    env.expect('FT.SEARCH', 'idx', "@n>>1").error().contains('Syntax error')
    env.expect('FT.SEARCH', 'idx', "@n<<1").error().contains('Syntax error')
    env.expect('FT.SEARCH', 'idx', "@n*-").error().contains('Syntax error')

def testNumericOperatorsModifierWithEscapes():
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    alias_and_modifier = {
         'm ': '@m\ ',
         "m ": "@m\\ ",
         " m": "@\\ m",
         "m\\": "@m\\\\",
         " m ": "@\\ m\\ ",
         "m@1": "@m\\@1",
         'm ': '@m\     ',
    }

    for alias, escaped_mod in alias_and_modifier.items():
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'AS', alias, 'NUMERIC').ok()
        env.assertEqual(conn.execute_command('HSET', 'key1', 'n', '10'), 1)
        env.assertEqual(conn.execute_command('HSET', 'key2', 'n', '20'), 1)

        res = env.cmd('FT.SEARCH', 'idx', escaped_mod + '  > 15', 'NOCONTENT')
        env.assertEqual(res, [1, 'key2'])

        res = env.cmd('FT.SEARCH', 'idx', escaped_mod + ' >= 15', 'NOCONTENT')
        env.assertEqual(res, [1, 'key2'])

        res = env.cmd('FT.SEARCH', 'idx', escaped_mod + '< 15', 'NOCONTENT')
        env.assertEqual(res, [1, 'key1'])

        res = env.cmd('FT.SEARCH', 'idx', escaped_mod + '  <= 15', 'NOCONTENT')
        env.assertEqual(res, [1, 'key1'])

        res = env.cmd('FT.SEARCH', 'idx', escaped_mod + '== 10', 'NOCONTENT')
        env.assertEqual(res, [1, 'key1'])

        res = env.cmd('FT.SEARCH', 'idx', escaped_mod + '!= 10', 'NOCONTENT')
        env.assertEqual(res, [1, 'key2'])

        env.flush()
