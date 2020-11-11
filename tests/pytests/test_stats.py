
import unittest
from random import random, seed 
from includes import *
from common import getConnectionByEnv, waitForIndex, sortedResults, toSortedFlatList
from time import sleep, time
from RLTest import Env

##########################################################################

def ft_info_to_dict(env, idx):
    res = env.execute_command('ft.info', idx)
    return {res[i]: res[i + 1] for i in range(0, len(res), 2)}

def ft_debug_to_dict(env, idx, n):
    res = env.execute_command('ft.debug', 'NUMIDX_SUMMARY', idx, n)
    return {res[i]: res[i + 1] for i in range(0, len(res), 2)}

def check_empty(env, idx):
    d = ft_info_to_dict(env, idx)
    env.assertEqual(float(d['inverted_sz_mb']), 0)
    env.assertEqual(float(d['num_records']), 0)

def check_not_empty(env, idx):
    d = ft_info_to_dict(env, idx)
    env.assertGreater(float(d['inverted_sz_mb']), 0)
    env.assertGreater(float(d['num_records']), 0)

##########################################################################

def runTestWithSeed(env, s=None):
    conn = getConnectionByEnv(env)

    env.expect('FLUSHALL')
    if s == None:
        s = time()
    env.debugPrint('seed: %s' % str(s), force=True)    
    seed(s)

    idx = 'idx'
    count = 10000
    cleaning_loops = 5
    loop_count = count / cleaning_loops

    ### test increasing integers
    env.expect('ft.config set FORK_GC_CLEAN_THRESHOLD 0').ok()
    
    env.expect('FT.CREATE idx SCHEMA n NUMERIC').ok()
    check_empty(env, idx)

    for i in range(count):
        conn.execute_command('HSET', 'doc%d' % i, 'n', i)

    env.expect('FT.SEARCH idx * LIMIT 0 0').equal([count])
    for i in range(count):
        env.expect('FT.SEARCH', 'idx', '@n:[%d,%d]' % (i, i))#.equal([1L, 'doc%d' % i, ['n', str(i)]])
    #check_not_empty(env, idx)

    for i in range(cleaning_loops):
        check_not_empty(env, idx)
        for ii in range(loop_count):
            conn.execute_command('DEL', 'doc%d' % int(loop_count * i + ii))
        for jj in range(10):
            env.expect('FT.DEBUG', 'GC_FORCEINVOKE', 'idx')

    for i in range(count):
        env.expect('FT.SEARCH', 'idx', '@n:[%d,%d]' % (i, i))#.equal([0L])
    check_empty(env, idx)

    ### test random integers
    env.expect('FLUSHALL')
    env.expect('FT.CREATE idx SCHEMA n NUMERIC').ok()
    for i in range(count):
        temp = int(random() * count / 10)
        conn.execute_command('HSET', 'doc%d' % i, 'n', temp)

    env.expect('FT.SEARCH idx * LIMIT 0 0').equal([count])
    check_not_empty(env, idx)

    for i in range(cleaning_loops):
        check_not_empty(env, idx)
        for ii in range(loop_count):
            conn.execute_command('DEL', 'doc%d' % int(loop_count * i + ii))
        env.expect('FT.DEBUG', 'GC_FORCEINVOKE', 'idx')
    check_empty(env, idx)

    for i in range(count):
        env.expect('FT.SEARCH', 'idx', '@n:[%d,%d]' % (i, i))#.equal([0L])
    check_empty(env, idx)

    ## test random floats
    env.expect('FLUSHALL')
    env.expect('FT.CREATE idx SCHEMA n NUMERIC').ok()
    for i in range(count):
        conn.execute_command('HSET', 'doc%d' % i, 'n', int(random()))

    env.expect('FT.SEARCH idx * LIMIT 0 0').equal([count])
    check_not_empty(env, idx)

    for i in range(cleaning_loops):
        check_not_empty(env, idx)
        for ii in range(loop_count):
            conn.execute_command('DEL', 'doc%d' % int(loop_count * i + ii))
        for jj in range(10):
            env.expect('FT.DEBUG', 'GC_FORCEINVOKE', 'idx')
    check_empty(env, idx)

def testRandom(env):
    env.skipOnCluster()

    if env.cmd('FT.CONFIG', 'GET', 'GC_POLICY')[0][1] != 'fork':
        env.skip()

    runTestWithSeed(env, 2)

    runTestWithSeed(env)

def testMemoryAfterDrop(env):
    env.skipOnCluster()

    if env.cmd('FT.CONFIG', 'GET', 'GC_POLICY')[0][1] != 'fork':
        env.skip()

    idx_count = 100
    doc_count = 50
    divide_by = 1000000   # ensure limits of geo are not exceeded 
    pl = env.getConnection().pipeline()

    env.execute_command('FLUSHALL')
    env.execute_command('ft.config', 'set', 'FORK_GC_CLEAN_THRESHOLD', 0)

    for i in range(idx_count):
        env.expect('FT.CREATE', 'idx%d' % i, 'PREFIX', 1, '%ddoc' % i, 'SCHEMA', 't', 'TEXT', 'n', 'NUMERIC', 'tg', 'TAG', 'g', 'GEO').ok()

    for i in range(idx_count):
        geo = '1.23456,' + str(float(i) / divide_by)
        for j in range(doc_count):
            pl.execute_command('HSET', '%ddoc%d' % (i, j), 't', '%dhello%d' % (i, j), 'tg', '%dworld%d' % (i, j), 'n', i, 'g', geo)
        pl.execute()
        d = ft_info_to_dict(env, 'idx%d' % i)
        env.assertEqual(d['num_docs'], str(doc_count))

    for i in range(idx_count):
        for j in range(doc_count):
            pl.execute_command('DEL', '%ddoc%d' % (i, j))
        pl.execute()
        d = ft_info_to_dict(env, 'idx%d' % i)
        env.assertEqual(d['num_docs'], '0')
        for _ in range(10):
            env.cmd('ft.debug', 'GC_FORCEINVOKE', 'idx%d' % i)

    for i in range(idx_count):
        check_empty(env, 'idx%d' % i)

def testIssue1497(env):
    env.skipOnCluster()

    if env.cmd('FT.CONFIG', 'GET', 'GC_POLICY')[0][1] != 'fork':
        env.skip()

    count = 110
    divide_by = 1000000   # ensure limits of geo are not exceeded 
    number_of_fields = 4  # one of every type

    env.execute_command('FLUSHALL')
    env.execute_command('ft.config', 'set', 'FORK_GC_CLEAN_THRESHOLD', 0)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'n', 'NUMERIC', 'tg', 'TAG', 'g', 'GEO').ok()

    res = env.execute_command('ft.info', 'idx')
    d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
    env.assertEqual(d['inverted_sz_mb'], '0')
    env.assertEqual(d['num_records'], '0')
    for i in range(count):
        geo = '1.23456,' + str(float(i) / divide_by)
        env.expect('HSET', 'doc%d' % i, 't', 'hello%d' % i, 'tg', 'world%d' % i, 'n', i * 1.01, 'g', geo)
    res = env.cmd('FT.SEARCH idx *')
    check_not_empty(env, 'idx')
    env.assertEqual(res[0], count)

    res = env.execute_command('ft.info', 'idx')
    d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
    env.assertGreater(d['inverted_sz_mb'], '0')
    env.assertGreaterEqual(int(d['num_records']), count * number_of_fields)
    for i in range(count):
        env.expect('DEL', 'doc%d' % i)

    for _ in range(50):
        env.cmd('ft.debug', 'GC_FORCEINVOKE', 'idx')

    res = env.execute_command('ft.info', 'idx')
    d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
    env.assertEqual(d['inverted_sz_mb'], '0')
    env.assertEqual(d['num_records'], '0')
    check_empty(env, 'idx')
