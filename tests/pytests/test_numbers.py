# -*- coding: utf-8 -*-

from includes import *
from common import *
from RLTest import Env
import math



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
        env.expect('ft.search', 'idx', (f'@n:[{value} {value}]')).equal([1, str(i), ['n', str(value)]])

@skip(cluster=True)
def testSanity(env):
    skipOnExistingEnv(env)
    repeat = 100000
    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'idx', 'SCHEMA', 'n', 'numeric')
    for i in range(repeat):
        conn.execute_command('hset', i, 'n', i % 100)
    env.expect('ft.search', 'idx', ('@n:[0 %d]' % (repeat)), 'limit', 0 ,0).equal([repeat])
    env.expect(debug_cmd(), 'numidx_summary', 'idx', 'n').equal([
        'numRanges', 13, 'numLeaves', 13, 'numEntries', 100000, 'lastDocId', 100000, 'revisionId', 12, 'emptyLeaves', 0, 'RootMaxDepth', 4, 'MemoryUsage', ANY])

@skip(cluster=True)
def testCompressionConfig(env):
    env.cmd('ft.create', 'idx', 'SCHEMA', 'n', 'numeric')

    # w/o compression. exact number match.
    env.expect(config_cmd(), 'set', '_NUMERIC_COMPRESS', 'false').equal('OK')
    for i in range(100):
          env.cmd('hset', i, 'n', str(1 + i / 100.0))
    for i in range(100):
        num = str(1 + i / 100.0)
        env.expect('ft.search', 'idx', f'@n:[{num} {num}]').equal([1, str(i), ['n', num]])

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
        env.expect('ft.search', 'idx', f'@n:[{num} {num}]').equal([0])

@skip(cluster=True)
def testRangeParentsConfig(env):
    elements = 1000

    result = [['numRanges', 5], ['numRanges', 7]]
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
            conn.execute_command('HSET', f'doc{j}', 'n', format(x))
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
        conn.execute_command('HSET', f'doc{i}', 'n', format(i))

    for i in range(repeat):
        for j in range(docs):
            x = j + i * docs
            conn.execute_command('HSET', f'doc{j % 100 + 100}', 'n', format(x))
        res = env.cmd('FT.SEARCH', 'idx', '@n:[-inf + inf]', 'NOCONTENT')
        env.assertEqual(res[0], docs / 100 + 100)

    num_summery_before = env.cmd(debug_cmd(), 'NUMIDX_SUMMARY', 'idx', 'n')
    forceInvokeGC(env, 'idx')
    num_summery_after = env.cmd(debug_cmd(), 'NUMIDX_SUMMARY', 'idx', 'n')
    env.assertGreater(num_summery_before[1], num_summery_after[1])

    res = env.cmd('FT.SEARCH', 'idx', '@n:[-inf + inf]', 'NOCONTENT')
    env.assertEqual(res[0], docs / 100 + 100)


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
    res = env.cmd('FT.SEARCH', 'idx', '@n:[.1e1 .1e+1]', 'NOCONTENT', 'WITHCOUNT')
    env.assertEqual(res, expected)

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
    # Leading zero are optional
    res2 = env.cmd('FT.SEARCH', 'idx', '@n:[-.1 +.1]', 'NOCONTENT', 'WITHCOUNT')
    env.assertEqual(res2, expected)
    res2 = env.cmd('FT.AGGREGATE', 'idx', '@n:[-.1 +.1]', 'LIMIT', 0, 0)
    env.assertEqual(res2, [2])
    res2 = env.cmd('FT.SEARCH', 'idx', '@n:[-  .1 +  .1]', 'NOCONTENT', 'WITHCOUNT')
    env.assertEqual(res2, expected)
    res2 = env.cmd('FT.AGGREGATE', 'idx', '@n:[-  .1 +  .1]', 'LIMIT', 0, 0)
    env.assertEqual(res2, [2])

    # Test float numbers with exponent
    res1 = env.cmd('FT.SEARCH', 'idx', '@n:[1.5e2 1.5e2]', 'NOCONTENT', 'WITHCOUNT')
    env.assertEqual(res1, [1, 'doc05'])
    res1 = env.cmd('FT.AGGREGATE', 'idx', '@n:[1.5e2 1.5e2]', 'LIMIT', 0, 0)
    env.assertEqual(res1, [1])
    res1 = env.cmd('FT.SEARCH', 'idx', '@n:[1.5e+2 1500e-1]', 'NOCONTENT', 'WITHCOUNT')
    env.assertEqual(res1, [1, 'doc05'])
    res = env.cmd('FT.AGGREGATE', 'idx', '@n:[1.5e+2 1500e-1]', 'LIMIT', 0, 0)
    env.assertEqual(res, [1])


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

@skip(cluster=True)
def testNumericTree(env:Env):
    idx = 'idx'
    field = 'n'

    cur_id = 0
    def add_val(val):
        nonlocal cur_id
        env.cmd('HSET', 'doc%d' % cur_id, 'n', val)
        cur_id += 1

    def equal_structure(tree1, tree2):
        def equal_subtree_structure(st1, st2):
            st1 = to_dict(st1)
            st2 = to_dict(st2)
            if 'left' not in st1 or 'right' not in st1:
                return 'left' not in st2 or 'right' not in st2
            return equal_subtree_structure(st1['left'], st2['left']) and equal_subtree_structure(st1['right'], st2['right'])
        return equal_subtree_structure(to_dict(tree1)['root'], to_dict(tree2)['root'])

    def cause_split(val):
        # Add close-but-not-equal values to the tree, until the tree is split
        start_tree = env.cmd(debug_cmd(), 'DUMP_NUMIDXTREE', idx, field, 'MINIMAL')
        epsilon = 0.
        while equal_structure(start_tree, env.cmd(debug_cmd(), 'DUMP_NUMIDXTREE', idx, field, 'MINIMAL')):
            add_val(val + epsilon)
            epsilon += 0.0001

    # Recursively validate the maxDepth field in the tree
    def validate_tree(tree):
        maxDepthRange = int(env.cmd(config_cmd(), 'GET', '_NUMERIC_RANGES_PARENTS')[0][1])
        def validate_subtree(subtree):
            subtree = to_dict(subtree)
            if 'left' not in subtree or 'right' not in subtree:
                env.assertContains('range', subtree)
                return 0 # a leaf
            left_max_depth = validate_subtree(subtree['left'])
            right_max_depth = validate_subtree(subtree['right'])
            expected_max_depth = max(left_max_depth, right_max_depth) + 1
            env.assertEqual(subtree['maxDepth'], expected_max_depth)
            # Some balancing rotation might cause a node with no range to get a maxDepth below the maxDepthRange,
            # so we can't validate all nodes to be below the maxDepthRange have a range.
            # We validate that if the maxDepth is above the maxDepthRange, there is no range
            env.assertTrue(subtree['maxDepth'] <= maxDepthRange or 'range' not in subtree)
            return expected_max_depth
        validate_subtree(to_dict(tree)['root'])

    env.expect('FT.CREATE', idx, 'SCHEMA', field, 'NUMERIC').ok()
    # Split to the right 5 times
    for i in range(5):
        cause_split(i)

    validate_tree(env.cmd(debug_cmd(), 'DUMP_NUMIDXTREE', idx, field, 'MINIMAL'))

    env.flush()
    env.expect(config_cmd(), 'SET', '_NUMERIC_RANGES_PARENTS', '2').ok()
    env.expect('FT.CREATE', idx, 'SCHEMA', field, 'NUMERIC').ok()

    # Split to the right and then to the left
    cause_split(0)
    cause_split(2) # to the right
    cause_split(1) # to the left (of 2, right of 0)

    validate_tree(env.cmd(debug_cmd(), 'DUMP_NUMIDXTREE', idx, field, 'MINIMAL'))
