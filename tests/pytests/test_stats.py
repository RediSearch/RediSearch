
import unittest
from random import random, seed
from includes import *
from common import *
from time import sleep, time
from RLTest import Env

##########################################################################

def check_index_info(env, idx, exp_num_records, exp_inv_idx_size):
    d = index_info(env, idx)
    env.assertEqual(float(d['num_records']), exp_num_records)

    if(exp_inv_idx_size != None):
        env.assertEqual(float(d['inverted_sz_mb']), exp_inv_idx_size)

##########################################################################

def runTestWithSeed(env, s=None):
    conn = getConnectionByEnv(env)

    env.expect('FLUSHALL')
    if s == None:
        s = int(time())
    env.debugPrint('seed: %s' % str(s), force=TEST_DEBUG)
    seed(s)

    idx = 'idx'
    count = 100
    num_values = 4
    cleaning_loops = 4
    loop_count = int(count / cleaning_loops)

    ### test increasing integers
    env.expect('ft.config set FORK_GC_CLEAN_THRESHOLD 0').ok()

    env.expect('FT.CREATE idx SCHEMA n NUMERIC').ok()
    check_index_info(env, idx, 0, 0)

    value_offset = 4096
    # Each value written to the buffer will occupy 4 bytes:
    # 1 byte for the header
    # 1 byte for the delta
    # 2 bytes for the actual number (4096-4099)

    for i in range(count):
        # write only 4 different values to get a range tree with a root node 
        # with a left child and a right child. Each child has an inverted index.
        conn.execute_command('HSET', 'doc%d' % i, 'n', (i % num_values) + value_offset)
    
    # Expected inverted index size total: 606 bytes
    # 2 * (buffer size + inverted index structure size)
    # 2 * (207 + 96) = 606

    # 207 is the buffer size after writing 4 bytes 50 times.
    # The buffer grows according to Buffer_Grow() in buffer.c
    # 96 is the size of the inverted index structure without counting the
    # buffer capacity.
    expected_inv_idx_size = 606 / (1024 * 1024)
    check_index_info(env, idx, count, expected_inv_idx_size)

    env.expect('FT.SEARCH idx * LIMIT 0 0').equal([count])
    for i in range(count):
         x = (i % num_values) + value_offset
         env.expect('FT.SEARCH', 'idx', '@n:[%d,%d]' % (x, x), 'LIMIT', 0, 0).\
            equal([count / num_values])

    for i in range(cleaning_loops):
        exp_num_records = count - (loop_count * i)
        check_index_info(env, idx, exp_num_records, None)
        for ii in range(loop_count):
            conn.execute_command('DEL', 'doc%d' % int(loop_count * i + ii))
        forceInvokeGC(env, 'idx')

    for i in range(count):
        env.expect('FT.SEARCH', 'idx', '@n:[%d,%d]' % (i, i)).equal([0])
    check_index_info(env, idx, 0, 0)

    ### test random integers
    env.expect('FLUSHALL')
    env.expect('FT.CREATE idx SCHEMA n NUMERIC').ok()

    for i in range(count):
        temp = int(random() * count / 10)
        conn.execute_command('HSET', 'doc%d' % i, 'n', temp)

    # Test only the number of records, because the memory size depends on 
    # the random values.
    check_index_info(env, idx, count, None)

    env.expect('FT.SEARCH idx * LIMIT 0 0').equal([count])

    for i in range(cleaning_loops):
        exp_num_records = count - loop_count * i
        check_index_info(env, idx, exp_num_records, None)
        for ii in range(loop_count):
            conn.execute_command('DEL', 'doc%d' % int(loop_count * i + ii))
        forceInvokeGC(env, 'idx')
    check_index_info(env, idx, 0, 0)

    for i in range(count):
        env.expect('FT.SEARCH', 'idx', '@n:[%d,%d]' % (i, i)).equal([0])
    check_index_info(env, idx, 0, 0)

    ## test random floats
    env.expect('FLUSHALL')
    env.expect('FT.CREATE idx SCHEMA n NUMERIC').ok()
    check_index_info(env, idx, 0, 0)

    # Each value written to the buffer will occupy 10 bytes:
    # 1 byte for the header
    # 1 byte for the delta
    # 8 bytes for the actual number (NUM_ENCODING_COMMON_TYPE_FLOAT)
    for i in range(count):
        temp = (random() * count / 10)
        conn.execute_command('HSET', 'doc%d' % i, 'n', temp)
        exp_num_records = i + 1

    # Check only the number of records, because the memory size depends on
    # the random values.
    check_index_info(env, idx, count, None)

    env.expect('FT.SEARCH idx * LIMIT 0 0').equal([count])

    for i in range(cleaning_loops):
        exp_num_records = count - loop_count * i
        check_index_info(env, idx, exp_num_records, None)
        for ii in range(loop_count):
            conn.execute_command('DEL', 'doc%d' % int(loop_count * i + ii))
        forceInvokeGC(env, 'idx')
    check_index_info(env, idx, 0, 0)

@skip(cluster=True, gc_no_fork=True)
def testRandom(env):

    runTestWithSeed(env, 2)

    runTestWithSeed(env)

@skip(cluster=True, gc_no_fork=True)
def testMemoryAfterDrop(env):

    idx_count = 100
    doc_count = 50
    divide_by = 1_000_000   # ensure limits of geo are not exceeded
    pl = env.getConnection().pipeline()

    env.cmd('FLUSHALL')
    env.cmd('ft.config', 'set', 'FORK_GC_CLEAN_THRESHOLD', 0)

    for i in range(idx_count):
        env.expect('FT.CREATE', 'idx%d' % i, 'PREFIX', 1, '%ddoc' % i, 'SCHEMA', 'n', 'NUMERIC').ok()

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
        forceInvokeGC(env, 'idx%d' % i)

    for i in range(idx_count):
        check_index_info(env, 'idx%d' % i, 0, 0)

@skip(cluster=True, gc_no_fork=True)
def testIssue1497(env):

    count = 110
    divide_by = 1_000_000 # ensure limits of geo are not exceeded
    number_of_fields = 4  # one of every type

    env.cmd('FLUSHALL')
    waitForRdbSaveToFinish(env)
    env.cmd('ft.config', 'set', 'FORK_GC_CLEAN_THRESHOLD', 0)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'n', 'NUMERIC', 'tg', 'TAG', 'g', 'GEO').ok()

    res = env.cmd('ft.info', 'idx')
    check_index_info(env, 'idx', 0, 0)
    for i in range(count):
        geo = '1.23456,' + str(float(i) / divide_by)
        env.expect('HSET', 'doc%d' % i, 't', 'hello%d' % i, 'tg', 'world%d' % i, 'n', i * 1.01, 'g', geo)

    res = env.cmd('FT.SEARCH idx *')
    env.assertEqual(res[0], count)
    exp_num_records = count * number_of_fields
    check_index_info(env, 'idx', exp_num_records, None)

    for i in range(count):
        env.expect('DEL', 'doc%d' % i)

    forceInvokeGC(env, 'idx')

    check_index_info(env, 'idx', 0, 0)

@skip(cluster=True, gc_no_fork=True)
def testMemoryAfterDrop_numeric(env):

    idx_count = 100
    doc_count = 120
    pl = env.getConnection().pipeline()

    env.execute_command('FLUSHALL')
    env.execute_command('ft.config', 'set', 'FORK_GC_CLEAN_THRESHOLD', 0)

    for i in range(idx_count):
        env.expect('FT.CREATE', 'idx%d' % i, 'PREFIX', 1, '%ddoc' % i, 'SCHEMA', 'n', 'NUMERIC').ok()

    for i in range(idx_count):
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

    for i in range(idx_count):
        check_index_info(env, 'idx%d' % i, 0, 0)

@skip(cluster=True, gc_no_fork=True)
def testMemoryAfterDrop_geo(env):

    idx_count = 100
    doc_count = 50
    divide_by = 1_000_000   # ensure limits of geo are not exceeded
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

    for i in range(idx_count):
        check_index_info(env, 'idx%d' % i, 0, 0)

@skip(cluster=True, gc_no_fork=True)
def testMemoryAfterDrop_text(env):

    idx_count = 10
    doc_count = 150
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

    for i in range(idx_count):
        check_index_info(env, 'idx%d' % i, 0, 0)

@skip(cluster=True, gc_no_fork=True)
def testMemoryAfterDrop_tag(env):

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

    for i in range(idx_count):
        check_index_info(env, 'idx%d' % i, 0, 0)

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
