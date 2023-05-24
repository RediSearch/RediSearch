# -*- coding: utf-8 -*-
import time
import unittest
from cmath import inf
from email import message
from includes import *
from common import *
from RLTest import Env


def testEmptyBuffer():
    if not POWER_TO_THE_WORKERS:
        raise unittest.SkipTest("Skipping since worker threads are not enabled")
    env = Env(moduleArgs='WORKER_THREADS 1 ALWAYS_USE_THREADS TRUE')
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC')

    env.expect('ft.search', 'idx', '*', 'sortby', 'n').equal([0])


def CreateAndSearchSortBy(docs_count):
    env = Env(moduleArgs='WORKER_THREADS 1 ALWAYS_USE_THREADS TRUE')
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC')
    conn = getConnectionByEnv(env)

    for n in range (1, docs_count + 1):
        doc_name = f'doc{n}'
        conn.execute_command('HSET', doc_name, 'n', n) 
    output = conn.execute_command('FT.SEARCH', 'idx', '*', 'sortby', 'n')
    
    # The first element in the results array is the number of docs.
    env.assertEqual(output[0], docs_count)
    
    # The results are sorted according to n
    result_len = 2
    n = 1
    for i in range(1, len(output) - result_len, result_len):
        result = output[i: i + result_len]
        # docs id starts from 1
        # each result should contain the doc name, the field name and its value 
        expected = [f'doc{n}', ['n', f'{n}']]
        env.assertEqual(result, expected)
        n += 1


def testSimpleBuffer():
    if not POWER_TO_THE_WORKERS:
        raise unittest.SkipTest("Skipping since worker threads are not enabled")
    CreateAndSearchSortBy(docs_count = 10)


# In this test we have more than BlockSize docs to buffer, we want to make sure there are no leaks
# caused by the buffer memory management.
def testMultipleBlocksBuffer():
    if not POWER_TO_THE_WORKERS:
        raise unittest.SkipTest("Skipping since worker threads are not enabled")
    CreateAndSearchSortBy(docs_count = 2500)
    
''' 
Test pipeline:
Sorter with no access to redis:
case 1: no loader (sortby SORTABLE NOCONTENT/sortby a (SORTABLE UNF) return a, no sortby NOCONTENT)
expected pipeline: root(<-scorer)<-sorter  
case 2: with loader (sortby SORTABLE (loadall)/ sortby SORTABLE a return b, no sortby (loadall) )
expected pipeline: root<-(<-scorer)sorter<-buffer-locker<-loader<-unlocker
case 3: with pager, no loader 
expected pipeline: root<-sorter<-pager
case 4: with pager, with loader
expected pipeline: root<-sorter<-pager<-buffer-locker<-loader<-unlocker

Sorter with access to redis:
case 1: no loader (sortby NOTSORTABLE NOCONTENT/sortby a (NOTSORTABLE) return a)
expected pipeline: root<-buffer-locker<-sorter<-unlocker
case 2: with loader (sortby NOTSORTABLE (loadall)/ sortby NOTSORTABLE a return b)
expected pipeline: root<-buffer-locker<-sorter<-loader<-unlocker
case 3: with pager, no loader
expected pipeline: root<-buffer-locker<-sorter<-pager<-unlocker
case 4: with pager, with loader
expected pipeline: root<-buffer-locker<-sorter<-pager<-loader<-unlocker

Additional rp:
case 1: highlighter (last to access redis is not endProc)
expected pipeline: root<-sorter<-buffer-locker<-loader<-unlocker<-highlighter
case 2: metric
expected pipeline: root<-metric<-sorter<-buffer-locker<-loader<-unlocker 
'''

def get_pipeline(profile_res):
    for entry in profile_res[1]:
        if (entry[0] == 'Result processors profile'):
            return entry


def test_pipeline():
    if not POWER_TO_THE_WORKERS:
        raise unittest.SkipTest("Skipping since worker threads are not enabled")
    env = Env(moduleArgs='WORKER_THREADS 1 ALWAYS_USE_THREADS TRUE')
    env.skipOnCluster()
    env.cmd('FT.CONFIG', 'SET', '_PRINT_PROFILE_CLOCK', 'false')
    
    docs_count = 3
    # if we use LIMIT
    reply_length = 1
    
    root = ['Type', 'Index', 'Counter', docs_count]
    scorer = ['Type', 'Scorer', 'Counter', docs_count]
    def sorter(paged= False):
        return ['Type', 'Sorter', 'Counter', docs_count if not paged else reply_length]
    def buffer_locker(paged= False):
        return ['Type', 'Buffer and Locker', 'Counter', docs_count if not paged else reply_length]
    def unlocker(paged= False):
        return ['Type', 'Unlocker', 'Counter', docs_count if not paged else reply_length]
    def loader(paged= False):
        return ['Type', 'Loader', 'Counter', docs_count if not paged else reply_length]
    pager = ['Type', 'Pager/Limiter', 'Counter', reply_length]
    highlighter = ['Type', 'Highlighter', 'Counter', docs_count]
    
    sortable_UNF_field_name = 'n_sortable'
    nonsortable_field_name = 'n'
    notindexed_field_name = 'x'
    
    
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', nonsortable_field_name, 'NUMERIC', sortable_UNF_field_name, 'NUMERIC', 'SORTABLE', 'UNF')
    conn = getConnectionByEnv(env)
    
    env.assertEqual(conn.execute_command('HSET', 'doc1', nonsortable_field_name, '102', sortable_UNF_field_name, '202'), 2)
    env.assertEqual(conn.execute_command('HSET', 'doc2', nonsortable_field_name, '101', sortable_UNF_field_name, '201'), 2)
    env.assertEqual(conn.execute_command('HSET', 'doc3', nonsortable_field_name, '100', sortable_UNF_field_name, '200'), 2)
    
    ft_profile_cmd = ['FT.PROFILE', 'idx', 'SEARCH', 'LIMITED', 'QUERY']
    search_UNF_sortable = '@n_sortable:[200 202]'
    sortby_UNF_sortable = ['SORTBY', sortable_UNF_field_name]
    sortby_not_sortable = ['SORTBY', nonsortable_field_name]
    
    ############################### Sorter WITH NO access to redis ###############################
    
    ''' case 1: no loader (sortby SORTABLE NOCONTENT/sortby a (SORTABLE UNF) return a, no sortby NOCONTENT)
        expected pipeline: root(<-scorer)<-sorter  '''
    expected_pipeline = ['Result processors profile', root, sorter()]
    #sortby SORTABLE NOCONTENT
    res = conn.execute_command(*ft_profile_cmd, search_UNF_sortable, *sortby_UNF_sortable, 'NOCONTENT')
    env.assertEqual(get_pipeline(res), expected_pipeline)
    
    #sortby a (SORTABLE UNF) return a
    res = conn.execute_command(*ft_profile_cmd, search_UNF_sortable, *sortby_UNF_sortable, 'RETURN', 1, sortable_UNF_field_name)
    env.assertEqual(get_pipeline(res), expected_pipeline)
    
    #no sortby NOCONTENT
    expected_pipeline = ['Result processors profile', root, scorer, sorter()]
    res = conn.execute_command(*ft_profile_cmd, search_UNF_sortable, 'NOCONTENT')
    env.assertEqual(get_pipeline(res), expected_pipeline)
    
    ''' case 2: with loader (sortby SORTABLE (loadall)/ sortby SORTABLE a return b, no sortby (loadall) )
        expected pipeline: root<-(<-scorer)sorter<-buffer-locker<-loader<-unlocker  '''
    expected_pipeline = ['Result processors profile', root, sorter(), buffer_locker(), loader(), unlocker()]
    #sortby SORTABLE (loadall)
    res = conn.execute_command(*ft_profile_cmd, search_UNF_sortable, *sortby_UNF_sortable)
    env.assertEqual(get_pipeline(res), expected_pipeline)
    
    #sortby a (SORTABLE UNF) return b
    res = conn.execute_command(*ft_profile_cmd, search_UNF_sortable, *sortby_UNF_sortable, 'RETURN', 1, notindexed_field_name)
    env.assertEqual(get_pipeline(res), expected_pipeline)
    
    #no sortby (loadall)
    expected_pipeline = ['Result processors profile', root, scorer, sorter(), buffer_locker(), loader(), unlocker()]
    res = conn.execute_command(*ft_profile_cmd, search_UNF_sortable)
    env.assertEqual(get_pipeline(res), expected_pipeline)
    
    '''case 3: with pager, no loader (sortby a (SORTABLE UNF) LIMIT 1 1 NOCONTENT)
        expected pipeline: root<-sorter<-pager '''
    paged = True
    expected_pipeline = ['Result processors profile', root, sorter(paged), pager]
    res = conn.execute_command(*ft_profile_cmd, search_UNF_sortable, *sortby_UNF_sortable, 'LIMIT', 1, 1, 'NOCONTENT')
    env.assertEqual(get_pipeline(res), expected_pipeline)
 
    '''case 4: with pager, with loader (sortby a (SORTABLE UNF) LIMIT 1 1 (loadall))
        expected pipeline: root<-sorter<-pager<-buffer-locker<-loader<-unlocker'''   
    expected_pipeline = ['Result processors profile', root, sorter(paged), pager, buffer_locker(paged), loader(paged), unlocker(paged)]
    res = conn.execute_command(*ft_profile_cmd, search_UNF_sortable, *sortby_UNF_sortable, 'LIMIT', 1, 1)
    env.assertEqual(get_pipeline(res), expected_pipeline)
    
    ############################### Sorter WITH access to redis: ############################### 
    
    ''' case 1: no loader
        expected pipeline: root<-buffer-locker<-sorter<-unlocker '''
    expected_pipeline = ['Result processors profile', root, buffer_locker(), sorter(), unlocker()]
    #sortby NOT SORTABLE NOCONTENT
    res = conn.execute_command(*ft_profile_cmd, search_UNF_sortable, *sortby_not_sortable, 'NOCONTENT')
    env.assertEqual(get_pipeline(res), expected_pipeline)
    
    #sortby a (NOTSORTABLE) return a
    res = conn.execute_command(*ft_profile_cmd, search_UNF_sortable, *sortby_not_sortable, 'RETURN', 1, nonsortable_field_name)
    env.assertEqual(get_pipeline(res), expected_pipeline)

    ''' case 2: with loader
        expected pipeline: root<-buffer-locker<-sorter<-loader<-unlocker'''
    expected_pipeline = ['Result processors profile', root, buffer_locker(), sorter(), loader(), unlocker()]
    #sortby NOT SORTABLE (loadall)
    res = conn.execute_command(*ft_profile_cmd, search_UNF_sortable, *sortby_not_sortable)
    env.assertEqual(get_pipeline(res), expected_pipeline)
    
    # sortby NOTSORTABLE a return b
    res = conn.execute_command(*ft_profile_cmd, search_UNF_sortable, *sortby_not_sortable, 'RETURN', 1, notindexed_field_name)
    env.assertEqual(get_pipeline(res), expected_pipeline)
 
    '''case 3: with pager, no loader 
        expected pipeline: root<-buffer-locker<-sorter<-pager<-unlocker'''
    expected_pipeline = ['Result processors profile', root, buffer_locker(), sorter(paged), pager, unlocker(paged)]
    #(sortby NOTSORTABLE LIMIT 1 1 NOCONTENT)
    res = conn.execute_command(*ft_profile_cmd, search_UNF_sortable, *sortby_not_sortable, 'LIMIT', 1, 1, 'NOCONTENT')
    env.assertEqual(get_pipeline(res), expected_pipeline)
    
    '''case 4: with pager, with loader 
        expected pipeline: root<-buffer-locker<-sorter<-pager<-loader<-unlocker '''
    expected_pipeline = ['Result processors profile', root, buffer_locker(), sorter(paged), pager, loader(paged), unlocker(paged)]
    # (sortby NOTSORTABLE LIMIT 1 1 (loadall))
    res = conn.execute_command(*ft_profile_cmd, search_UNF_sortable, *sortby_not_sortable, 'LIMIT', 1, 1)
    env.assertEqual(get_pipeline(res), expected_pipeline)
    
    ############################### Additional rp ############################### 
    
    ''' case 1: highlighter (last to access redis is not endProc)
        expected pipeline: root<-sorter<-buffer-locker<-loader<-unlocker<-highlighter '''
    expected_pipeline = ['Result processors profile', root, sorter(), buffer_locker(), loader(), unlocker(), highlighter]
    res = conn.execute_command(*ft_profile_cmd, '*', *sortby_UNF_sortable, 'HIGHLIGHT')
    env.assertEqual(get_pipeline(res), expected_pipeline)
    
    
    '''case 2: metric
    expected pipeline: root<-metric<-sorter<-buffer-locker<-loader<-unlocker '''
    env.cmd('flushall')
    
    env.cmd('FT.CONFIG', 'SET', 'DEFAULT_DIALECT', '2')

    env.expect('FT.CREATE idx SCHEMA v VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2 t TEXT').ok()
    conn.execute_command('hset', '1', 'v', 'bababaca', 't', "hello")
    conn.execute_command('hset', '2', 'v', 'babababa', 't', "hello")
    conn.execute_command('hset', '3', 'v', 'aabbaabb', 't', "hello")

    metric = ['Type', 'Metrics Applier', 'Counter', docs_count]
    
    res = conn.execute_command(*ft_profile_cmd, '*=>[KNN 3 @v $vec]',
                               'SORTBY', '__v_score', 'PARAMS', '2', 'vec', 'aaaaaaaa')   
        
    expected_pipeline = ['Result processors profile', root, metric, sorter(), buffer_locker(), loader(), unlocker()]
    #sortby NOT SORTABLE NOCONTENT
    env.assertEqual(get_pipeline(res), expected_pipeline)


def test_invalid_config():
    if not POWER_TO_THE_WORKERS:
        raise unittest.SkipTest("Skipping since worker threads are not enabled")
    # Invalid 0 worker threads with ALWAYS_USE_THREADS set to true configuration.
    try:
        env = Env(enableDebugCommand=True, moduleArgs='WORKER_THREADS 0 ALWAYS_USE_THREADS TRUE')
        prefix = '_' if env.isCluster() else ''
        env.cmd(f"{prefix}ft.config", "get", "WORKER_THREADS")
        env.assertFalse(True)   # We shouldn't get here
    except Exception:
        # Create dummy env to collect exit gracefully.
        env = Env()
        pass


def test_reload_index_while_indexing():
    if not POWER_TO_THE_WORKERS or CODE_COVERAGE:
        raise unittest.SkipTest("Skipping since worker threads are not enabled")

    env = Env(enableDebugCommand=True, moduleArgs='WORKER_THREADS 2 ALWAYS_USE_THREADS TRUE DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    n_shards = env.shardsCount
    n_vectors = 50000 * n_shards
    dim = 64
    # Load random vectors into redis.
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'vector', 'VECTOR', 'HNSW', '8', 'TYPE', 'FLOAT32', 'M', '64',
               'DIM', dim, 'DISTANCE_METRIC', 'L2').ok()
    load_vectors_to_redis(env, n_vectors, 0, dim)
    for i in env.reloadingIterator():
        debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')
        # At first, we expect to see background indexing, but after RDB load, we expect that all vectors
        # are indexed before RDB loading ends
        env.assertEqual(debug_info['BACKGROUND_INDEXING'], 1 if i == 1 else 0)


def test_delete_index_while_indexing():
    if not POWER_TO_THE_WORKERS or CODE_COVERAGE:
        raise unittest.SkipTest("Skipping since worker threads are not enabled")

    env = Env(enableDebugCommand=True, moduleArgs='WORKER_THREADS 2 ALWAYS_USE_THREADS TRUE DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    n_shards = env.shardsCount
    n_vectors = 50000 * n_shards
    dim = 64
    # Load random vectors into redis.
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'vector', 'VECTOR', 'HNSW', '8', 'TYPE', 'FLOAT32', 'M', '64',
               'DIM', dim, 'DISTANCE_METRIC', 'L2').ok()
    load_vectors_to_redis(env, n_vectors, 0, dim)
    # Delete index while vectors are being indexed (to validate proper cleanup of background jobs in sanitizer).
    debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')
    env.assertEqual(debug_info['BACKGROUND_INDEXING'], 1)
    conn.execute_command('FT.DROPINDEX', 'idx', 'DD')


def test_burst_threads_sanity():
    if not POWER_TO_THE_WORKERS:
        raise unittest.SkipTest("Skipping since worker threads are not enabled")

    env = Env(enableDebugCommand=True, moduleArgs='WORKER_THREADS 8 ALWAYS_USE_THREADS FALSE DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    n_shards = env.shardsCount
    n_vectors = 1000 * n_shards
    dim = 10
    for algo in VECSIM_ALGOS:
        for data_type in VECSIM_DATA_TYPES:
            # Load random vectors into redis, save the first one to use as query vector later on.
            env.expect('FT.CREATE', 'idx', 'SCHEMA', 'vector', 'VECTOR', algo, '6', 'TYPE', data_type,
                       'DIM', dim, 'DISTANCE_METRIC', 'L2').ok()
            query_vec = load_vectors_to_redis(env, n_vectors, 0, dim, data_type)

            res_before = conn.execute_command('FT.SEARCH', 'idx', '*=>[KNN $K @vector $vec_param]', 'SORTBY',
                                              '__vector_score', 'RETURN', 1, '__vector_score', 'LIMIT', 0, 10,
                                              'PARAMS', 4, 'K', n_vectors, 'vec_param', query_vec.tobytes())
            # Expect that the first result's would be around zero, since the query vector itself exists in the
            # index (id 0)
            env.assertAlmostEqual(float(res_before[2][1]), 0, 1e-5)

            for _ in env.retry_with_rdb_reload():
                debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')
                env.assertEqual(debug_info['ALGORITHM'], 'TIERED' if algo == 'HNSW' else algo)
                if algo == 'HNSW':
                    env.assertEqual(debug_info['BACKGROUND_INDEXING'], 0)
                assertInfoField(env, 'idx', 'num_docs', str(n_vectors))

                # Run the same KNN query and see that we are getting the same results after the reload
                res = conn.execute_command('FT.SEARCH', 'idx', '*=>[KNN $K @vector $vec_param]', 'SORTBY',
                                           '__vector_score', 'RETURN', 1, '__vector_score', 'LIMIT', 0, 10,
                                           'PARAMS', 4, 'K', n_vectors, 'vec_param', query_vec.tobytes())
                env.assertEqual(res, res_before)

            conn.flushall()


def test_workers_priority_queue():
    if not POWER_TO_THE_WORKERS:
        raise unittest.SkipTest("Skipping since worker threads are not enabled")
    env = Env(enableDebugCommand=True, moduleArgs='WORKER_THREADS 2 TIERED_HNSW_BUFFER_LIMIT 10000'
                                                  ' ALWAYS_USE_THREADS TRUE DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    n_shards = env.shardsCount
    n_vectors = 50000 * n_shards
    dim = 64

    # Load random vectors into redis, save the last one to use as query vector later on.
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'vector', 'VECTOR', 'HNSW', '8', 'TYPE', 'FLOAT32',
               'M', '64', 'DIM', dim, 'DISTANCE_METRIC', 'L2').ok()
    query_vec = load_vectors_to_redis(env, n_vectors, n_vectors-1, dim)
    assertInfoField(env, 'idx', 'num_docs', str(n_vectors))

    # Expect that some vectors are still being indexed in the background after we are done loading.
    debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')
    env.assertEqual(debug_info['BACKGROUND_INDEXING'], 1)
    vectors_left_to_index = to_dict(debug_info['FRONTEND_INDEX'])['INDEX_SIZE']
    # Validate that buffer limit config was set properly (so that more vectors than the
    # default limit are waiting in the buffer).
    env.assertGreater(vectors_left_to_index, 1024)

    # Run queries during indexing
    while debug_info['BACKGROUND_INDEXING'] == 1:
        start = time.time()
        res = conn.execute_command('FT.SEARCH', 'idx', '*=>[KNN $K @vector $vec_param EF_RUNTIME 10000]',
                                   'SORTBY', '__vector_score', 'RETURN', 1, '__vector_score', 'LIMIT', 0, 10,
                                   'PARAMS', 4, 'K', 10, 'vec_param', query_vec.tobytes())
        query_time = time.time() - start
        # Expect that the first result's would be around zero, since the query vector itself exists in the
        # index (last id)
        env.assertAlmostEqual(float(res[2][1]), 0, 1e-5)
        # Validate that queries get priority and are executed before indexing finishes.
        if not SANITIZER and not CODE_COVERAGE:
            env.assertLess(query_time, 1)

        # We expect that the number of vectors left to index will decrease from one iteration to another.
        debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')
        vectors_left_to_index_new = to_dict(debug_info['FRONTEND_INDEX'])['INDEX_SIZE']
        env.assertLessEqual(vectors_left_to_index_new, vectors_left_to_index)
        vectors_left_to_index = vectors_left_to_index_new


def test_async_updates_sanity():
    if not POWER_TO_THE_WORKERS:
        raise unittest.SkipTest("Skipping since worker threads are not enabled")
    env = Env(enableDebugCommand=True, moduleArgs='WORKER_THREADS 2 ALWAYS_USE_THREADS TRUE DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    n_shards = env.shardsCount
    n_vectors = 5000 * n_shards
    dim = 32
    block_size = 1024

    for data_type in VECSIM_DATA_TYPES:
        # Load random vectors into redis
        load_vectors_to_redis(env, n_vectors, 0, dim, data_type)
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 'vector', 'VECTOR', 'HNSW', '8', 'TYPE', data_type,
                   'DIM', dim, 'DISTANCE_METRIC', 'L2', 'M', '64').ok()
        waitForIndex(env, 'idx')
        assertInfoField(env, 'idx', 'num_docs', str(n_vectors))

        # Wait until al vectors are indexed into HNSW.
        debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')
        while debug_info['BACKGROUND_INDEXING'] == 1:
            time.sleep(1)
            debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')

        # Overwrite vectors - trigger background delete and ingest jobs.
        query_vec = load_vectors_to_redis(env, n_vectors, 0, dim, data_type)
        assertInfoField(env, 'idx', 'num_docs', str(n_vectors))
        debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')
        marked_deleted_vectors = to_dict(debug_info['BACKEND_INDEX'])['NUMBER_OF_MARKED_DELETED']
        env.assertGreater(marked_deleted_vectors, block_size/n_shards)

        # We dispose marked deleted vectors whenever we have at least <block_size> vectors that are ready
        # (that is, no other node in HNSW is pointing to the deleted node)
        while marked_deleted_vectors > block_size/n_shards:
            start = time.time()
            res = conn.execute_command('FT.SEARCH', 'idx', '*=>[KNN $K @vector $vec_param EF_RUNTIME 5000]',
                                       'SORTBY', '__vector_score', 'RETURN', 1, '__vector_score',
                                       'LIMIT', 0, 10, 'PARAMS', 4, 'K', 10, 'vec_param', query_vec.tobytes())
            query_time = time.time() - start
            # Validate that queries get priority and are executed before indexing/deletion is finished.
            if not SANITIZER and not CODE_COVERAGE:
                env.assertLess(query_time, 1)

            # Expect that the first result's would be around zero, since the query vector itself exists in the
            # index (id 0)
            env.assertAlmostEqual(float(res[2][1]), 0, 1e-5)

            # Overwrite another vector to trigger swap jobs. Use a random vector (except for label 0
            # which is the query vector that we expect to get), to ensure that we eventually remove a
            # vector from the main shard (of which we get info when we call ft.debug) in cluster mode.
            conn.execute_command("HSET", np.random.randint(1, n_vectors), 'vector',
                                 create_np_array_typed(np.random.rand(dim), data_type).tobytes())
            debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')
            marked_deleted_vectors_new = to_dict(debug_info['BACKEND_INDEX'])['NUMBER_OF_MARKED_DELETED']

            # After overwriting 1, there may be another one zombie.
            env.assertLessEqual(marked_deleted_vectors_new, marked_deleted_vectors + 1)
            marked_deleted_vectors = marked_deleted_vectors_new

        conn.flushall()
