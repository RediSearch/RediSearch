
import unittest
from random import random, seed
from includes import *
from common import *
from time import sleep, time
from RLTest import Env

##########################################################################

def ft_debug_to_dict(env, idx, n):
    res = env.cmd('ft.debug', 'NUMIDX_SUMMARY', idx, n)
    return {res[i]: res[i + 1] for i in range(0, len(res), 2)}

def check_empty(env, idx, memory_consumption):
    d = index_info(env, idx)
    env.assertEqual(float(d['num_records']), 0)
    env.assertEqual(memory_consumption, float(d['inverted_sz_mb']))

def check_not_empty(env, idx):
    d = index_info(env, idx)
    env.assertGreater(float(d['inverted_sz_mb']), 0)
    env.assertGreater(float(d['num_records']), 0)

##########################################################################

def runTestWithSeed(env, s=None):
    conn = getConnectionByEnv(env)

    expected_empty_inverted_sz_mb = 102 / (1024 * 1024)
    env.expect('FLUSHALL')
    if s == None:
        s = int(time())
    env.debugPrint('seed: %s' % str(s), force=TEST_DEBUG)
    seed(s)

    idx = 'idx'
    count = 10000
    cleaning_loops = 5
    loop_count = int(count / cleaning_loops)

    ### test increasing integers
    env.expect('ft.config set FORK_GC_CLEAN_THRESHOLD 0').ok()

    env.expect('FT.CREATE idx SCHEMA n NUMERIC').ok()
    check_empty(env, idx, 0)

    for i in range(count):
        conn.execute_command('HSET', 'doc%d' % i, 'n', i)

    env.expect('FT.SEARCH idx * LIMIT 0 0').equal([count])
    for i in range(count):
        env.expect('FT.SEARCH', 'idx', '@n:[%d,%d]' % (i, i))#.equal([1, 'doc%d' % i, ['n', str(i)]])
    check_not_empty(env, idx)

    for i in range(cleaning_loops):
        check_not_empty(env, idx)
        for ii in range(loop_count):
            conn.execute_command('DEL', 'doc%d' % int(loop_count * i + ii))
        forceInvokeGC(env, 'idx')

    for i in range(count):
        env.expect('FT.SEARCH', 'idx', '@n:[%d,%d]' % (i, i))#.equal([0])
    check_empty(env, idx, expected_empty_inverted_sz_mb)

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
        forceInvokeGC(env, 'idx')
    check_empty(env, idx, expected_empty_inverted_sz_mb)

    for i in range(count):
        env.expect('FT.SEARCH', 'idx', '@n:[%d,%d]' % (i, i))#.equal([0])
    check_empty(env, idx, expected_empty_inverted_sz_mb)

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
        forceInvokeGC(env, 'idx')
    check_empty(env, idx, expected_empty_inverted_sz_mb)

@skip(cluster=True)
def testRandom(env):

    if env.cmd('FT.CONFIG', 'GET', 'GC_POLICY')[0][1] != 'fork':
        env.skip()

    runTestWithSeed(env, 2)

    runTestWithSeed(env)

@skip(cluster=True)
def testMemoryAfterDrop(env):

    if env.cmd('FT.CONFIG', 'GET', 'GC_POLICY')[0][1] != 'fork':
        env.skip()

    idx_count = 100
    doc_count = 50
    divide_by = 1000000   # ensure limits of geo are not exceeded
    pl = env.getConnection().pipeline()

    env.cmd('FLUSHALL')
    env.cmd('ft.config', 'set', 'FORK_GC_CLEAN_THRESHOLD', 0)

    for i in range(idx_count):
        env.expect('FT.CREATE', 'idx%d' % i, 'PREFIX', 1, '%ddoc' % i, 'SCHEMA', 't', 'TEXT', 'n', 'NUMERIC', 'tg', 'TAG', 'g', 'GEO').ok()

    for i in range(idx_count):
        geo = '1.23456,' + str(float(i) / divide_by)
        for j in range(doc_count):
            pl.execute_command('HSET', '%ddoc%d' % (i, j), 't', '%dhello%d' % (i, j), 'tg', '%dworld%d' % (i, j), 'n', i, 'g', geo)
        pl.execute()
        d = index_info(env, 'idx%d' % i)
        env.assertEqual(d['num_docs'], doc_count)

    for i in range(idx_count):
        for j in range(doc_count):
            pl.execute_command('DEL', '%ddoc%d' % (i, j))
        pl.execute()
        d = index_info(env, 'idx%d' % i)
        env.assertEqual(d['num_docs'], 0)
        for _ in range(10):
            forceInvokeGC(env, 'idx%d' % i)

    # The memory occupied by a empty TEXT and TAG inverted index is 
    # 54 bytes * doc_count, becase FGC_applyInvertedIndex() is calling 
    # InvertedIndex_AddBlock() for each delete doc.
    # The memory occupied by a empty NUMERIC and GEO inverted index is 102 bytes.
    expected_inverted_sz_mb = (54 * 2 * doc_count) / (1024 * 1024) \
                    + (102 * 2) / (1024 * 1024)
    for i in range(idx_count):
        check_empty(env, 'idx%d' % i, expected_inverted_sz_mb)

@skip(cluster=True)
def testIssue1497(env):

    if env.cmd('FT.CONFIG', 'GET', 'GC_POLICY')[0][1] != 'fork':
        env.skip()

    count = 110
    divide_by = 1000000   # ensure limits of geo are not exceeded
    number_of_fields = 4  # one of every type

    env.cmd('FLUSHALL')
    waitForRdbSaveToFinish(env)
    env.cmd('ft.config', 'set', 'FORK_GC_CLEAN_THRESHOLD', 0)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'n', 'NUMERIC', 'tg', 'TAG', 'g', 'GEO').ok()

    res = env.cmd('ft.info', 'idx')
    d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
    env.assertEqual(d['inverted_sz_mb'], '0')
    env.assertEqual(d['num_records'], 0)
    for i in range(count):
        geo = '1.23456,' + str(float(i) / divide_by)
        env.expect('HSET', 'doc%d' % i, 't', 'hello%d' % i, 'tg', 'world%d' % i, 'n', i * 1.01, 'g', geo)
    res = env.cmd('FT.SEARCH idx *')
    check_not_empty(env, 'idx')
    env.assertEqual(res[0], count)

    res = env.cmd('ft.info', 'idx')
    d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
    env.assertGreater(d['inverted_sz_mb'], '0')
    env.assertGreaterEqual(int(d['num_records']), count * number_of_fields)
    for i in range(count):
        env.expect('DEL', 'doc%d' % i)

    forceInvokeGC(env, 'idx')

    # The memory occupied by a empty TEXT and TAG inverted index is 
    # 54 bytes * doc_count, becase FGC_applyInvertedIndex() is calling 
    # InvertedIndex_AddBlock() for each delete doc.
    # The memory occupied by a empty NUMERIC and GEO inverted index is 102 bytes.
    expected_inverted_sz_mb = (54 * 2 * count) / (1024 * 1024) \
                    + (102 * 2) / (1024 * 1024)
    check_empty(env, 'idx', expected_inverted_sz_mb)

@skip(cluster=True)
def testMemoryAfterDrop_numeric(env):

    if env.cmd('FT.CONFIG', 'GET', 'GC_POLICY')[0][1] != 'fork':
        env.skip()

    idx_count = 100
    doc_count = 120
    divide_by = 1000000   # ensure limits of geo are not exceeded
    pl = env.getConnection().pipeline()

    env.execute_command('FLUSHALL')
    env.execute_command('ft.config', 'set', 'FORK_GC_CLEAN_THRESHOLD', 0)

    for i in range(idx_count):
        env.expect('FT.CREATE', 'idx%d' % i, 'PREFIX', 1, '%ddoc' % i, 'SCHEMA', 'n', 'NUMERIC').ok()

    for i in range(idx_count):
        geo = '1.23456,' + str(float(i) / divide_by)
        for j in range(doc_count):
            pl.execute_command('HSET', '%ddoc%d' % (i, j), 'n', i)
        pl.execute()
        d = index_info(env, 'idx%d' % i)
        env.assertEqual(d['num_docs'], doc_count)

    for i in range(idx_count):
        for j in range(doc_count):
            pl.execute_command('DEL', '%ddoc%d' % (i, j))
        pl.execute()
        d = index_info(env, 'idx%d' % i)
        env.assertEqual(d['num_docs'], 0)
        forceInvokeGC(env, 'idx%d' % i)

    # The memory occupied by a empty NUMERIC inverted index is 102 bytes,
    # which is the sum of the following (See NewInvertedIndex()):
    # sizeof_InvertedIndex(index->flags)   48
    # sizeof(IndexBlock)                   48
    # INDEX_BLOCK_INITIAL_CAP               6
    expected_inverted_sz_mb = 102 / (1024 * 1024)
    for i in range(idx_count):
        check_empty(env, 'idx%d' % i, expected_inverted_sz_mb)

@skip(cluster=True)
def testMemoryAfterDrop_geo(env):

    if env.cmd('FT.CONFIG', 'GET', 'GC_POLICY')[0][1] != 'fork':
        env.skip()

    idx_count = 100
    doc_count = 50
    divide_by = 1000000   # ensure limits of geo are not exceeded
    pl = env.getConnection().pipeline()

    env.execute_command('FLUSHALL')
    env.execute_command('ft.config', 'set', 'FORK_GC_CLEAN_THRESHOLD', 0)

    for i in range(idx_count):
        env.expect('FT.CREATE', 'idx%d' % i, 'PREFIX', 1, '%ddoc' % i, 'SCHEMA', 'g', 'GEO').ok()

    for i in range(idx_count):
        geo = '1.23456,' + str(float(i) / divide_by)
        for j in range(doc_count):
            pl.execute_command('HSET', '%ddoc%d' % (i, j), 'g', geo)
        pl.execute()
        d = index_info(env, 'idx%d' % i)
        env.assertEqual(d['num_docs'], doc_count)

    for i in range(idx_count):
        for j in range(doc_count):
            pl.execute_command('DEL', '%ddoc%d' % (i, j))
        pl.execute()
        d = index_info(env, 'idx%d' % i)
        env.assertEqual(d['num_docs'], 0)
        forceInvokeGC(env, 'idx%d' % i)

    # The memory occupied by a empty NUMERIC inverted index is 102 bytes,
    # which is the sum of the following (See NewInvertedIndex()):
    # sizeof_InvertedIndex(index->flags)   48
    # sizeof(IndexBlock)                   48
    # INDEX_BLOCK_INITIAL_CAP               6
    expected_inverted_sz_mb = 102 / (1024 * 1024)
    for i in range(idx_count):
        check_empty(env, 'idx%d' % i, expected_inverted_sz_mb)

@skip(cluster=True)
def testMemoryAfterDrop_text(env):

    if env.cmd('FT.CONFIG', 'GET', 'GC_POLICY')[0][1] != 'fork':
        env.skip()

    idx_count = 1
    doc_count = 100
    pl = env.getConnection().pipeline()

    env.execute_command('FLUSHALL')
    env.execute_command('ft.config', 'set', 'FORK_GC_CLEAN_THRESHOLD', 0)

    for i in range(idx_count):
        env.expect('FT.CREATE', 'idx%d' % i, 'PREFIX', 1, '%ddoc' % i, 'SCHEMA', 't', 'TEXT').ok()

    for i in range(idx_count):
        for j in range(doc_count):
            pl.execute_command('HSET', '%ddoc%d' % (i, j), 't', '%dhello%d' % (i, j))
        pl.execute()
        d = index_info(env, 'idx%d' % i)
        env.assertEqual(d['num_docs'], doc_count)

    for i in range(idx_count):
        for j in range(doc_count):
            pl.execute_command('DEL', '%ddoc%d' % (i, j))
        pl.execute()
        d = index_info(env, 'idx%d' % i)
        env.assertEqual(d['num_docs'], 0)
        forceInvokeGC(env, 'idx%d' % i)

    # The memory occupied by a empty TEXT inverted index is 54 bytes * doc_count,
    # becase FGC_applyInvertedIndex() is calling InvertedIndex_AddBlock() for 
    # each delete doc. 
    # The size of a new block is 54 bytes, which is the sum of:
    # sizeof(IndexBlock)                   48
    # INDEX_BLOCK_INITIAL_CAP               6
    expected_inverted_sz_mb = (54 * doc_count) / (1024 * 1024)
    for i in range(idx_count):
        check_empty(env, 'idx%d' % i, expected_inverted_sz_mb)

@skip(cluster=True)
def testMemoryAfterDrop_tag(env):

    if env.cmd('FT.CONFIG', 'GET', 'GC_POLICY')[0][1] != 'fork':
        env.skip()

    idx_count = 1
    doc_count = 100
    pl = env.getConnection().pipeline()

    env.execute_command('FLUSHALL')
    env.execute_command('ft.config', 'set', 'FORK_GC_CLEAN_THRESHOLD', 0)

    for i in range(idx_count):
        env.expect('FT.CREATE', 'idx%d' % i, 'PREFIX', 1, '%ddoc' % i, 'SCHEMA', 'tg', 'TAG').ok()

    for i in range(idx_count):
        for j in range(doc_count):
            pl.execute_command('HSET', '%ddoc%d' % (i, j), 'tg', '%dworld%d' % (i, j))
        pl.execute()
        d = index_info(env, 'idx%d' % i)
        env.assertEqual(d['num_docs'], doc_count)

    for i in range(idx_count):
        for j in range(doc_count):
            pl.execute_command('DEL', '%ddoc%d' % (i, j))
        pl.execute()
        d = index_info(env, 'idx%d' % i)
        env.assertEqual(d['num_docs'], 0)
        forceInvokeGC(env, 'idx%d' % i)

    # The memory occupied by a empty TAG inverted index is 54 bytes * doc_count,
    # becase FGC_applyInvertedIndex() is calling InvertedIndex_AddBlock() for 
    # each delete doc. 
    # The size of a new block is 54 bytes, which is the sum of:
    # sizeof(IndexBlock)                   48
    # INDEX_BLOCK_INITIAL_CAP               6
    expected_inverted_sz_mb = (54 * doc_count) / (1024 * 1024)
    for i in range(idx_count):
        check_empty(env, 'idx%d' % i, expected_inverted_sz_mb)

def testDocTableInfo(env):
    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'txt', 'TEXT', 'SORTABLE')

    d = index_info(env)
    env.assertEqual(int(d['num_docs']), 0)
    env.assertEqual(int(d['doc_table_size_mb']), 0)
    env.assertEqual(int(d['sortable_values_size_mb']), 0)

    conn.execute_command('HSET', 'a', 'txt', 'hello')
    conn.execute_command('HSET', 'b', 'txt', 'world')

    # check
    d = index_info(env)
    env.assertEqual(int(d['num_docs']), 2)
    doctable_size1 = float(d['doc_table_size_mb'])
    env.assertGreater(doctable_size1, 0)
    sortable_size1 = float(d['sortable_values_size_mb'])
    env.assertGreater(sortable_size1, 0)

    # check size after an update with larger text
    conn.execute_command('HSET', 'a', 'txt', 'hello world')
    d = index_info(env)
    env.assertEqual(int(d['num_docs']), 2)
    doctable_size2 = float(d['doc_table_size_mb'])
    env.assertEqual(doctable_size1, doctable_size2)
    sortable_size2 = float(d['sortable_values_size_mb'])
    env.assertLess(sortable_size1, sortable_size2)

    # check size after an update with identical text
    conn.execute_command('HSET', 'b', 'txt', 'world')
    d = index_info(env)
    env.assertEqual(int(d['num_docs']), 2)
    doctable_size3 = float(d['doc_table_size_mb'])
    env.assertEqual(doctable_size2, doctable_size3)
    sortable_size3 = float(d['sortable_values_size_mb'])
    env.assertEqual(sortable_size2, sortable_size3)

    # check 0 after deletion
    conn.execute_command('DEL', 'a')
    conn.execute_command('DEL', 'b')
    d = index_info(env)
    env.assertEqual(int(d['num_docs']), 0)
    env.assertEqual(int(d['doc_table_size_mb']), 0)
    env.assertEqual(int(d['sortable_values_size_mb']), 0)

@skip(cluster=True)
def testInfoIndexingTime(env):
    conn = getConnectionByEnv(env)

    # Add indexing time with HSET
    env.cmd('FT.CREATE', 'idx1', 'SCHEMA', 'txt', 'TEXT', 'SORTABLE')

    d = index_info(env, 'idx1')
    env.assertEqual(int(d['total_indexing_time']), 0)

    num_docs = 10000
    for i in range(num_docs):
        conn.execute_command('HSET', f'a{i}', 'txt', f'hello world {i}')

    d = index_info(env, 'idx1')
    env.assertGreater(float(d['total_indexing_time']), 0)

    # Add indexing time with scanning of existing docs
    env.cmd('FT.CREATE', 'idx2', 'SCHEMA', 'txt', 'TEXT', 'SORTABLE')
    waitForIndex(env, 'idx2')

    d = index_info(env, 'idx2')
    env.assertGreater(float(d['total_indexing_time']), 0)
