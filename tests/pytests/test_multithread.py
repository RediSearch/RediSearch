# -*- coding: utf-8 -*-
import time
import unittest
from cmath import inf
from email import message
import redis.exceptions
from includes import *
from common import *
from RLTest import Env

debug_cmd = "_ft.debug" if COORD else "ft.debug"

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


def getWorkersThpoolStats(env):
    return to_dict(env.cmd("ft.debug worker_threads stats"))

def testSimpleBuffer():
    CreateAndSearchSortBy(docs_count = 10)

# In this test we have more than BlockSize docs to buffer, we want to make sure there are no leaks
# caused by the safe-loader memory management.
def testMultipleBlocksBuffer():
    CreateAndSearchSortBy(docs_count = 2500)

# Skipping for cluster as we only test for loading the module to the shard.
# We test it only for redis versions > 7.2.3 due to a bug in redis - onFLush callback is called even though module loading
# failed, causing access to an invalid memory that was freed (module context) - see
# https://github.com/redis/redis/issues/12808.
@skip(cluster=True, noWorkers=True)
def test_invalid_mt_config_combinations(env):
    if server_version_less_than(env, "7.2.4"):
        env.skip()
    module_path =  env.envRunner.modulePath[0]  # extract search module path from RLTest default env
    for mode in ['MT_MODE_FULL', 'MT_MODE_ONLY_ON_OPERATIONS']:
        env = Env(module=[])  # create a new env without any module
        env.assertEqual(env.cmd('MODULE', 'LIST'), [], message=mode)
        try:
            env.cmd('MODULE', 'LOAD', module_path, 'WORKER_THREADS', '0', 'MT_MODE', mode)
            env.assertFalse(True, message=mode)   # we shouldn't get here
        except Exception as e:
            # Expect to see a failure in loading the module due to the invalid configuration combination.
            env.assertEqual(type(e), redis.exceptions.ModuleError, message=mode)
            env.assertContains("Error loading the extension.", str(e))


@skip(cluster=True)
def test_worker_threads_sanity():
    env = initEnv(moduleArgs='WORKER_THREADS 2 MT_MODE MT_MODE_FULL DEFAULT_DIALECT 2')
    n_shards = env.shardsCount
    n_vectors = 100 * n_shards
    dim = 4
    # Load random vectors into redis.
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'vector', 'VECTOR', 'HNSW', '8', 'TYPE', 'FLOAT32', 'M', '64',
               'DIM', dim, 'DISTANCE_METRIC', 'L2').ok()
    # Run DEBUG RELOAD twice to see that the thread pool is running as expected
    # (even after threads are terminated once).
    for it in range(2):
        # At first iteration insert vectors 0,1,...,n_vectors-1, and the second insert ids
        # n_vectors, n_vector+1,...,2*n_vectors-1.
        env.expect('FT.DEBUG', 'WORKER_THREADS', 'PAUSE').ok()
        load_vectors_to_redis(env, n_vectors, 0, dim, ids_offset=it*n_vectors)
        env.assertEqual(getWorkersThpoolStats(env)['totalPendingJobs'], n_vectors, message=f"iteration {it+1}")
        env.assertEqual(getWorkersThpoolStats(env)['totalJobsDone'], 0 if it==0 else 2*n_vectors,
                        message=f"iteration {it+1}")
        waitForRdbSaveToFinish(env)
        # i=1 before reload, and i=2 after.
        for i in env.reloadingIterator():
            # Before reload, we expect that every vector that were loaded into redis will increase pending jobs in 1,
            # and after reload, we expect to have no pending jobs (as we are waiting for jobs to be done upon loading).
            env.assertEqual(getWorkersThpoolStats(env)['totalPendingJobs'], n_vectors if i==1 else 0,
                            message=f"iteration {it+1} {'before' if i==1 else 'after'} loading")

            # At first iteration before reload we expect no jobs to be executed (thread pool paused).
            # At second iteration before reload we expect 2*n_vectors jobs to be executed, before and after reload of
            # the first iteration.
            # At first iteration after reload we expect 2*n_vectors jobs to be executed as well (thread pool paused).
            # At second iteration after reload we expect another n_vectors jobs to be executed (the second batch of
            # vectors), after we resumed the workers, and then another 2*n_vector jobs upon loading. Overall 3*n_vectors
            # jobs were added.
            env.assertEqual(getWorkersThpoolStats(env)['totalJobsDone'],
                            (0 if it==0 else 2*n_vectors) if i==1 else (2*n_vectors if it==0 else 5*n_vectors),
                            message=f"iteration {it+1} {'before' if i==1 else 'after'} loading")
            assertInfoField(env, 'idx', 'num_docs', str(n_vectors*(it+1)))
            # Resume the workers thread pool, let the background indexing start (in the first iteration it is paused)
            if i==1:
                env.expect('FT.DEBUG', 'WORKER_THREADS', 'RESUME').ok()
            # At first, we expect to see background indexing, but after RDB load, we expect that all vectors
            # are indexed before RDB loading ends
            debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')
            if i==2:
                env.assertEqual(debug_info['BACKGROUND_INDEXING'], 0, message=f"iteration {it+1} after reloading")
            env.expect('FT.DEBUG', 'WORKER_THREADS', 'drain').ok()


def test_delete_index_while_indexing():
    env = initEnv(moduleArgs='WORKER_THREADS 2 MT_MODE MT_MODE_FULL DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    n_shards = env.shardsCount
    n_vectors = 100 * n_shards
    dim = 4
    # Load random vectors into redis.
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'vector', 'VECTOR', 'HNSW', '8', 'TYPE', 'FLOAT32', 'M', '64',
               'DIM', dim, 'DISTANCE_METRIC', 'L2').ok()
    env.expect(debug_cmd, 'WORKER_THREADS', 'PAUSE').ok()
    load_vectors_to_redis(env, n_vectors, 0, dim)
    assertInfoField(env, 'idx', 'num_docs', str(n_vectors))
    env.expect(debug_cmd, 'WORKER_THREADS', 'RESUME').ok()

    # Delete index while vectors are being indexed (to validate proper cleanup of background jobs in sanitizer).
    conn.execute_command('FT.DROPINDEX', 'idx')


def test_burst_threads_sanity():
    env = initEnv(moduleArgs='WORKER_THREADS 2 MT_MODE MT_MODE_ONLY_ON_OPERATIONS DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    n_shards = env.shardsCount
    n_vectors = 100 * n_shards
    dim = 4
    k = 10
    expected_total_jobs = 0
    for algo in VECSIM_ALGOS:
        additional_params = ['EF_CONSTRUCTION', n_vectors, 'EF_RUNTIME', n_vectors] if algo=='HNSW' else []
        for data_type in VECSIM_DATA_TYPES:
            # Load random vectors into redis, save the first one to use as query vector later on. We set EF_C and
            # EF_R to n_vectors to ensure that all vectors would be reachable in HNSW and avoid flakiness in search.
            env.expect('FT.CREATE', 'idx', 'SCHEMA', 'vector', 'VECTOR', algo, str(6+len(additional_params)),
                       'TYPE', data_type, 'DIM', dim, 'DISTANCE_METRIC', 'L2', *additional_params).ok()
            query_vec = load_vectors_to_redis(env, n_vectors, 0, dim, data_type)

            res_before = conn.execute_command('FT.SEARCH', 'idx', '*=>[KNN $K @vector $vec_param]', 'SORTBY',
                                              '__vector_score', 'RETURN', 1, '__vector_score', 'LIMIT', 0, 10,
                                              'PARAMS', 4, 'K', k, 'vec_param', query_vec.tobytes())
            # Expect that the first result's would be around zero, since the query vector itself exists in the
            # index (id 0)
            env.assertAlmostEqual(float(res_before[2][1]), 0, 1e-5)
            waitForRdbSaveToFinish(env)
            for i in env.reloadingIterator():
                debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')
                env.assertEqual(debug_info['ALGORITHM'], 'TIERED' if algo == 'HNSW' else algo)
                if algo == 'HNSW':
                    env.assertEqual(debug_info['BACKGROUND_INDEXING'], 0,
                                    message=f"{'before loading' if i==1 else 'after loading'}")
                    if i==2:  # after reloading in HNSW, we expect to run insert job for each vector
                        expected_total_jobs += n_vectors
                assertInfoField(env, 'idx', 'num_docs', str(n_vectors))
                if not env.isCluster():
                    env.assertEqual(debug_info['INDEX_LABEL_COUNT'], n_vectors)
                    env.assertEqual(getWorkersThpoolStats(env)['totalPendingJobs'], 0)
                    if algo == 'HNSW':
                        # Expect that 0 jobs was done before reloading, and another n_vector insert jobs during the reloading.
                        env.assertEqual(getWorkersThpoolStats(env)['totalJobsDone'], expected_total_jobs)
                # Run the same KNN query and see that we are getting the same results after the reload
                res = conn.execute_command('FT.SEARCH', 'idx', '*=>[KNN $K @vector $vec_param]', 'SORTBY',
                                           '__vector_score', 'RETURN', 1, '__vector_score', 'LIMIT', 0, 10,
                                           'PARAMS', 4, 'K', k, 'vec_param', query_vec.tobytes())
                env.assertEqual(res, res_before)

            conn.flushall()


def test_workers_priority_queue():
    env = initEnv(moduleArgs='WORKER_THREADS 2 MT_MODE MT_MODE_FULL DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    n_shards = env.shardsCount
    n_vectors = 200 * n_shards
    dim = 4

    # Load random vectors into redis, save the last one to use as query vector later on.
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'vector', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', dim,
               'DISTANCE_METRIC', 'L2').ok()
    env.expect(debug_cmd, 'WORKER_THREADS', 'PAUSE').ok()
    query_vec = load_vectors_to_redis(env, n_vectors, n_vectors-1, dim)
    assertInfoField(env, 'idx', 'num_docs', str(n_vectors))

    # Expect that some vectors are still being indexed in the background after we are done loading.
    debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')
    vectors_left_to_index = to_dict(debug_info['FRONTEND_INDEX'])['INDEX_SIZE']
    env.assertEqual(vectors_left_to_index, n_vectors)

    # Run queries during indexing
    iteration_count = 0
    while debug_info['BACKGROUND_INDEXING'] == 1:
        env.expect(debug_cmd, 'WORKER_THREADS', 'RESUME').ok()
        iteration_count+=1
        res = conn.execute_command('FT.SEARCH', 'idx', f'*=>[KNN $K @vector $vec_param EF_RUNTIME {n_vectors}]',
                                   'SORTBY', '__vector_score', 'RETURN', 1, '__vector_score', 'LIMIT', 0, 10,
                                   'PARAMS', 4, 'K', 10, 'vec_param', query_vec.tobytes())
        # Expect that the first result's would be around zero, since the query vector itself exists in the
        # index (last id)
        env.assertAlmostEqual(float(res[2][1]), 0, 1e-5)
        env.expect(debug_cmd, 'WORKER_THREADS', 'PAUSE').ok()

        # We expect that the number of vectors left to index will decrease from one iteration to another.
        debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')
        vectors_left_to_index_new = to_dict(debug_info['FRONTEND_INDEX'])['INDEX_SIZE']
        env.assertLessEqual(vectors_left_to_index_new, vectors_left_to_index)
        vectors_left_to_index = vectors_left_to_index_new
        # Number of jobs done should be the number of vector indexed plus number of queries that ran.
        env.assertEqual(getWorkersThpoolStats(env)['totalJobsDone'], n_vectors-vectors_left_to_index + iteration_count)
    env.expect(debug_cmd, 'WORKER_THREADS', 'RESUME').ok()
    #todo: DEADLOCK IN CLUSTER WHY?



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
