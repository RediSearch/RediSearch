# -*- coding: utf-8 -*-
import time
import unittest
from cmath import inf
from email import message
from includes import *
from common import *
from RLTest import Env


def initEnv(moduleArgs: str = 'WORKER_THREADS 1 MT_MODE MT_MODE_FULL'):
    if(moduleArgs == ''):
        raise SkipTest('moduleArgs cannot be empty')
    if not MT_BUILD:
        raise SkipTest('MT_BUILD is not set')
    env = Env(enableDebugCommand=True, moduleArgs=moduleArgs)
    return env

def testEmptyBuffer():
    env = initEnv()
    env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC')

    env.expect('ft.search', 'idx', '*', 'sortby', 'n').equal([0])


def CreateAndSearchSortBy(docs_count):
    env = initEnv()
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
    CreateAndSearchSortBy(docs_count = 10)

# In this test we have more than BlockSize docs to buffer, we want to make sure there are no leaks
# caused by the safe-loader memory management.
def testMultipleBlocksBuffer():
    CreateAndSearchSortBy(docs_count = 2500)

def test_invalid_MT_MODE_FULL_config():
    try:
        env = initEnv(moduleArgs='WORKER_THREADS 0 MT_MODE MT_MODE_FULL')
        prefix = '_' if env.isCluster() else ''
        env.cmd(f"{prefix}ft.config", "get", "WORKER_THREADS")
        env.assertFalse(True)   # We shouldn't get here
    except Exception:
        # Create dummy env to collect exit gracefully.
        env = Env()
        pass

def test_invalid_MT_MODE_ONLY_ON_OPERATIONS_config():
    # Invalid 0 worker threads with MT_MODE_ONLY_ON_OPERATIONS.
    try:
        env = initEnv(moduleArgs='WORKER_THREADS 0 MT_MODE MT_MODE_ONLY_ON_OPERATIONS')
        prefix = '_' if env.isCluster() else ''
        env.cmd(f"{prefix}ft.config", "get", "WORKER_THREADS")
        env.assertFalse(True)   # We shouldn't get here
    except Exception:
        # Create dummy env to collect exit gracefully.
        env = Env()
        pass


def test_reload_index_while_indexing():
    if CODE_COVERAGE:
        raise unittest.SkipTest()

    env = initEnv(moduleArgs='WORKER_THREADS 2 MT_MODE MT_MODE_FULL DEFAULT_DIALECT 2')
    n_shards = env.shardsCount
    n_vectors = 10000 * n_shards if not SANITIZER and not CODE_COVERAGE else 500 * n_shards
    dim = 64
    # Load random vectors into redis.
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'vector', 'VECTOR', 'HNSW', '8', 'TYPE', 'FLOAT32', 'M', '64',
               'DIM', dim, 'DISTANCE_METRIC', 'L2').ok()

    # Run DEBUG RELOAD twice to see that the thread pool is running as expected
    # (even after threads are terminated once).
    for it in range(2):
        # At first iteration insert vectors 0,1,...,n_vectors-1, and the second insert ids
        # n_vectors, n_vector+1,...,2*n_vectors-1.
        load_vectors_to_redis(env, n_vectors, 0, dim, ids_offset=it*n_vectors)
        waitForRdbSaveToFinish(env)
        for i in env.reloadingIterator():
            # TODO: this is causing a crush occasionally in Cursors_RenderStats - need to fix this.
            # assertInfoField(env, 'idx', 'num_docs', str(n_vectors*(it+1)))
            debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')
            if not env.isCluster():
                env.assertEqual(debug_info['INDEX_LABEL_COUNT'], n_vectors*(it+1))
            # At first, we expect to see background indexing, but after RDB load, we expect that all vectors
            # are indexed before RDB loading ends
            # TODO: try making this not-flaky
            if i == 2:
                env.assertEqual(debug_info['BACKGROUND_INDEXING'], 0)
            # env.assertEqual(debug_info['BACKGROUND_INDEXING'], 1 if i == 1 else 0)


def test_delete_index_while_indexing():
    if CODE_COVERAGE:
        raise unittest.SkipTest("Skipping since worker threads are not enabled")

    env = initEnv(moduleArgs='WORKER_THREADS 2 MT_MODE MT_MODE_FULL DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    n_shards = env.shardsCount
    n_vectors = 10000 * n_shards if not SANITIZER and not CODE_COVERAGE else 500 * n_shards
    dim = 64
    # Load random vectors into redis.
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'vector', 'VECTOR', 'HNSW', '8', 'TYPE', 'FLOAT32', 'M', '64',
               'DIM', dim, 'DISTANCE_METRIC', 'L2').ok()
    load_vectors_to_redis(env, n_vectors, 0, dim)
    # Delete index while vectors are being indexed (to validate proper cleanup of background jobs in sanitizer).
    debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')
    # TODO: try making this not-flaky
    # env.assertEqual(debug_info['BACKGROUND_INDEXING'], 1)
    conn.execute_command('FT.DROPINDEX', 'idx')


def test_burst_threads_sanity():
    env = initEnv(moduleArgs='WORKER_THREADS 8 MT_MODE MT_MODE_ONLY_ON_OPERATIONS DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    n_shards = env.shardsCount
    n_vectors = 5000 * n_shards if not SANITIZER and not CODE_COVERAGE else 500 * n_shards
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
            waitForRdbSaveToFinish(env)
            for i in env.retry_with_rdb_reload():
                debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')
                env.assertEqual(debug_info['ALGORITHM'], 'TIERED' if algo == 'HNSW' else algo)
                if algo == 'HNSW':
                    env.assertEqual(debug_info['BACKGROUND_INDEXING'], 0,
                                    message=f"{'before loading' if i==1 else 'after loading'}")
                # TODO: this is causing a crush occasionally in Cursors_RenderStats - need to fix this.
                # assertInfoField(env, 'idx', 'num_docs', str(n_vectors))
                if not env.isCluster():
                    env.assertEqual(debug_info['INDEX_LABEL_COUNT'], n_vectors)
                # Run the same KNN query and see that we are getting the same results after the reload
                res = conn.execute_command('FT.SEARCH', 'idx', '*=>[KNN $K @vector $vec_param]', 'SORTBY',
                                           '__vector_score', 'RETURN', 1, '__vector_score', 'LIMIT', 0, 10,
                                           'PARAMS', 4, 'K', n_vectors, 'vec_param', query_vec.tobytes())
                env.assertEqual(res, res_before)

            conn.flushall()


def test_workers_priority_queue():
    env = initEnv(moduleArgs='WORKER_THREADS 2 TIERED_HNSW_BUFFER_LIMIT 10000'
                                                  ' MT_MODE MT_MODE_FULL DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    n_shards = env.shardsCount
    n_vectors = 10000 * n_shards if not SANITIZER and not CODE_COVERAGE else 500 * n_shards
    dim = 64

    # Load random vectors into redis, save the last one to use as query vector later on.
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'vector', 'VECTOR', 'HNSW', '8', 'TYPE', 'FLOAT32',
               'M', '64', 'DIM', dim, 'DISTANCE_METRIC', 'L2').ok()
    query_vec = load_vectors_to_redis(env, n_vectors, n_vectors-1, dim)
    assertInfoField(env, 'idx', 'num_docs', str(n_vectors))

    # Expect that some vectors are still being indexed in the background after we are done loading.
    debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')
    vectors_left_to_index = to_dict(debug_info['FRONTEND_INDEX'])['INDEX_SIZE']
    # TODO: try making this not-flaky
    # Validate that buffer limit config was set properly (so that more vectors than the
    # default limit are waiting in the buffer).
    # env.assertEqual(debug_info['BACKGROUND_INDEXING'], 1)
    # env.assertGreater(vectors_left_to_index, 1024/n_shards)

    # Run queries during indexing
    while debug_info['BACKGROUND_INDEXING'] == 1:
        start = time.time()
        res = conn.execute_command('FT.SEARCH', 'idx', f'*=>[KNN $K @vector $vec_param EF_RUNTIME {n_vectors}]',
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
    env = initEnv(moduleArgs='WORKER_THREADS 2 MT_MODE MT_MODE_FULL DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    n_shards = env.shardsCount
    n_vectors = 10000 * n_shards if not SANITIZER and not CODE_COVERAGE else 500 * n_shards
    dim = 16
    block_size = 1024

    # Load random vectors into redis
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'vector', 'VECTOR', 'HNSW', '8', 'TYPE', 'FLOAT32',
               'DIM', dim, 'DISTANCE_METRIC', 'L2', 'M', '64').ok()
    load_vectors_to_redis(env, n_vectors, 0, dim)
    waitForRdbSaveToFinish(env)
    assertInfoField(env, 'idx', 'num_docs', str(n_vectors))

    # Wait until al vectors are indexed into HNSW.
    debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')
    while debug_info['BACKGROUND_INDEXING'] == 1:
        time.sleep(1)
        debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')

    # Overwrite vectors - trigger background delete and ingest jobs.
    query_vec = load_vectors_to_redis(env, n_vectors, 0, dim)
    assertInfoField(env, 'idx', 'num_docs', str(n_vectors))
    debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')
    marked_deleted_vectors = to_dict(debug_info['BACKEND_INDEX'])['NUMBER_OF_MARKED_DELETED']
    # TODO: try making this not-flaky
    # env.assertGreater(marked_deleted_vectors, block_size/n_shards)

    # We dispose marked deleted vectors whenever we have at least <block_size> vectors that are ready
    # (that is, no other node in HNSW is pointing to the deleted node)
    while marked_deleted_vectors > block_size/n_shards:
        start = time.time()
        res = conn.execute_command('FT.SEARCH', 'idx', f'*=>[KNN $K @vector $vec_param EF_RUNTIME {n_vectors}]',
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
                             create_np_array_typed(np.random.rand(dim)).tobytes())
        debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')
        marked_deleted_vectors_new = to_dict(debug_info['BACKEND_INDEX'])['NUMBER_OF_MARKED_DELETED']

        # After overwriting 1, there may be another one zombie.
        env.assertLessEqual(marked_deleted_vectors_new, marked_deleted_vectors + 1)
        marked_deleted_vectors = marked_deleted_vectors_new
