# -*- coding: utf-8 -*-
import time
import unittest
from cmath import inf
from email import message
import redis.exceptions
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
    n_vectors = 100
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
            assertInfoField(env, 'idx', 'num_docs', n_vectors*(it+1))
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
    n_shards = env.shardsCount
    n_vectors = 100 * n_shards
    dim = 4
    # Load random vectors into redis.
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'vector', 'VECTOR', 'HNSW', '8', 'TYPE', 'FLOAT32', 'M', '64',
               'DIM', dim, 'DISTANCE_METRIC', 'L2').ok()
    env.expect(debug_cmd(), 'WORKER_THREADS', 'PAUSE').ok()
    load_vectors_to_redis(env, n_vectors, 0, dim)
    assertInfoField(env, 'idx', 'num_docs', n_vectors)
    n_local_vector = get_vecsim_debug_dict(env, 'idx', 'vector')['INDEX_LABEL_COUNT']

    # Delete index while vectors are being indexed (to validate proper cleanup of background jobs in sanitizer).
    # We expect that jobs will continue running, but the weak ref will not be promoted, and we discard them.
    env.cmd('FT.DROPINDEX', 'idx')
    stats = getWorkersThpoolStats(env)
    env.assertEqual(n_local_vector, stats['totalPendingJobs'], message=stats)
    env.expect(debug_cmd(), 'WORKER_THREADS', 'RESUME').ok()
    env.expect(debug_cmd(), 'WORKER_THREADS', 'DRAIN').ok()
    stats = getWorkersThpoolStats(env)
    env.assertEqual(n_local_vector, stats['totalJobsDone'], message=stats)


def test_burst_threads_sanity():
    env = initEnv(moduleArgs='WORKER_THREADS 2 MT_MODE MT_MODE_ONLY_ON_OPERATIONS DEFAULT_DIALECT 2')
    n_shards = env.shardsCount
    n_vectors = 100 * n_shards
    dim = 4
    k = 10
    expected_total_jobs = 0
    for algo in VECSIM_ALGOS:
        additional_params = ['EF_CONSTRUCTION', n_vectors, 'EF_RUNTIME', n_vectors] if algo == 'HNSW' else []
        for data_type in VECSIM_DATA_TYPES:
            # Load random vectors into redis, save the first one to use as query vector later on. We set EF_C and
            # EF_R to n_vectors to ensure that all vectors would be reachable in HNSW and avoid flakiness in search.
            env.expect('FT.CREATE', 'idx', 'SCHEMA', 'vector', 'VECTOR', algo, str(6+len(additional_params)),
                       'TYPE', data_type, 'DIM', dim, 'DISTANCE_METRIC', 'L2', *additional_params).ok()
            query_vec = load_vectors_to_redis(env, n_vectors, 0, dim, data_type)
            n_local_vectors = get_vecsim_debug_dict(env, 'idx', 'vector')['INDEX_LABEL_COUNT']

            res_before = env.cmd('FT.SEARCH', 'idx', '*=>[KNN $K @vector $vec_param]', 'SORTBY',
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
                        expected_total_jobs += n_local_vectors
                assertInfoField(env, 'idx', 'num_docs', n_vectors)
                env.assertEqual(debug_info['INDEX_LABEL_COUNT'], n_local_vectors)
                env.assertEqual(getWorkersThpoolStats(env)['totalPendingJobs'], 0)
                if algo == 'HNSW':
                    # Expect that 0 jobs was done before reloading, and another n_vector insert jobs during the reloading.
                    env.assertEqual(getWorkersThpoolStats(env)['totalJobsDone'], expected_total_jobs)
                # Run the same KNN query and see that we are getting the same results after the reload
                res = env.cmd('FT.SEARCH', 'idx', '*=>[KNN $K @vector $vec_param]', 'SORTBY',
                                           '__vector_score', 'RETURN', 1, '__vector_score', 'LIMIT', 0, 10,
                                           'PARAMS', 4, 'K', k, 'vec_param', query_vec.tobytes())
                env.assertEqual(res, res_before)
            env.flush()


def test_workers_priority_queue():
    env = initEnv(moduleArgs='WORKER_THREADS 2 MT_MODE MT_MODE_FULL DEFAULT_DIALECT 2')
    n_shards = env.shardsCount
    n_vectors = 200 * n_shards
    dim = 4

    # Load random vectors into redis, save the last one to use as query vector later on.
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'vector', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', dim,
               'DISTANCE_METRIC', 'L2').ok()
    env.expect(debug_cmd(), 'WORKER_THREADS', 'PAUSE').ok()
    query_vec = load_vectors_to_redis(env, n_vectors, n_vectors-1, dim)
    assertInfoField(env, 'idx', 'num_docs', n_vectors)

    # Expect that some vectors are still being indexed in the background after we are done loading.
    debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')
    local_n_vectors = to_dict(debug_info['FRONTEND_INDEX'])['INDEX_SIZE']
    vectors_left_to_index = local_n_vectors

    # Run queries during indexing
    iteration_count = 0
    while debug_info['BACKGROUND_INDEXING'] == 1:
        env.expect(debug_cmd(), 'WORKER_THREADS', 'RESUME').ok()
        iteration_count+=1
        res = env.cmd('FT.SEARCH', 'idx', f'*=>[KNN $K @vector $vec_param EF_RUNTIME {n_vectors}]',
                                   'SORTBY', '__vector_score', 'RETURN', 1, '__vector_score', 'LIMIT', 0, 10,
                                   'PARAMS', 4, 'K', 10, 'vec_param', query_vec.tobytes())
        # Expect that the first result's would be around zero, since the query vector itself exists in the
        # index (last id)
        env.assertAlmostEqual(float(res[2][1]), 0, 1e-5)
        env.expect(debug_cmd(), 'WORKER_THREADS', 'PAUSE').ok()
        # We expect that the number of vectors left to index will decrease from one iteration to another.
        debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')
        vectors_left_to_index_new = to_dict(debug_info['FRONTEND_INDEX'])['INDEX_SIZE']
        env.assertLessEqual(vectors_left_to_index_new, vectors_left_to_index)
        vectors_left_to_index = vectors_left_to_index_new
        # Number of jobs done should be the number of vector indexed plus number of queries that ran.
        env.assertEqual(getWorkersThpoolStats(env)['totalJobsDone'],
                        local_n_vectors-vectors_left_to_index + iteration_count)
    env.expect(debug_cmd(), 'WORKER_THREADS', 'RESUME').ok()


def test_buffer_limit():
    buffer_limit = 100
    env = initEnv(moduleArgs=f'WORKER_THREADS 2 MT_MODE MT_MODE_FULL DEFAULT_DIALECT 2'
                             f' TIERED_HNSW_BUFFER_LIMIT {buffer_limit}')
    n_shards = env.shardsCount
    dim = 4
    n_vectors = 2 * n_shards * buffer_limit

    # Load random vectors into redis
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'vector', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32',
               'DIM', dim, 'DISTANCE_METRIC', 'L2').ok()
    env.expect(debug_cmd(), 'WORKER_THREADS', 'PAUSE').ok()
    load_vectors_to_redis(env, n_vectors, 0, dim)
    assertInfoField(env, 'idx', 'num_docs', n_vectors)

    # Verify that the frontend flat index is full up to the buffer limit, and the rest of the vectors were indexed
    # directly into HNSW backend index.
    debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')
    n_local_vectors = debug_info['INDEX_LABEL_COUNT']
    env.assertEqual(to_dict(debug_info['FRONTEND_INDEX'])['INDEX_SIZE'], buffer_limit)
    env.assertEqual(to_dict(debug_info['BACKEND_INDEX'])['INDEX_SIZE'], n_local_vectors-buffer_limit)

    env.expect(debug_cmd(), 'WORKER_THREADS', 'RESUME').ok()
    env.expect(debug_cmd(), 'WORKER_THREADS', 'DRAIN').ok()

    # After running all insert jobs, all vectors should move to the backend index.
    debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')
    env.assertEqual(to_dict(debug_info['FRONTEND_INDEX'])['INDEX_SIZE'], 0)
    env.assertEqual(to_dict(debug_info['BACKEND_INDEX'])['INDEX_SIZE'], n_local_vectors)


def test_async_updates_sanity():
    env = initEnv(moduleArgs='WORKER_THREADS 2 MT_MODE MT_MODE_FULL DEFAULT_DIALECT 2 TIERED_HNSW_BUFFER_LIMIT 10000')
    env.expect(config_cmd(), 'set', 'FORK_GC_CLEAN_THRESHOLD', 0).ok()
    n_shards = env.shardsCount
    dim = 4
    block_size = 1024
    n_vectors = 2 * n_shards * block_size

    # Load random vectors into redis
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'vector', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32',
               'DIM', dim, 'DISTANCE_METRIC', 'L2').ok()
    query_before_update = load_vectors_to_redis(env, n_vectors, n_vectors-1, dim)
    n_local_vectors_before_update = get_vecsim_debug_dict(env, 'idx', 'vector')['INDEX_LABEL_COUNT']
    assertInfoField(env, 'idx', 'num_docs', n_vectors)
    res = env.cmd('FT.SEARCH', 'idx', f'*=>[KNN $K @vector $vec_param EF_RUNTIME {n_vectors}]',
                                       'SORTBY', '__vector_score', 'RETURN', 1, '__vector_score',
                                       'LIMIT', 0, 10, 'PARAMS', 4, 'K', 10, 'vec_param', query_before_update.tobytes())

    # Expect that the first result's would be around zero, since the query vector itself exists in the
    # index (id n_vectors-1)
    env.assertAlmostEqual(float(res[2][1]), 0, 1e-5)

    # Wait until all vectors are indexed into HNSW.
    conns = env.getOSSMasterNodesConnectionList()
    total_jobs_done = 0
    for con in conns:
        env.expect(debug_cmd(), 'WORKER_THREADS', 'DRAIN').ok()
        env.assertEqual(getWorkersThpoolStatsFromShard(con)['totalPendingJobs'], 0)
        total_jobs_done += getWorkersThpoolStatsFromShard(con)['totalJobsDone']

    env.assertEqual(total_jobs_done, n_vectors + n_shards)  # job per vector + one job for the query.
    debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')
    env.assertEqual(to_dict(debug_info['BACKEND_INDEX'])['INDEX_SIZE'], n_local_vectors_before_update)

    # Overwrite vectors. All vectors were ingested into the background index, so now we collect new vectors
    # into the frontend index and prepare repair and ingest jobs. The overwritten vector were not removed from
    # the backend index yet.
    env.expect(debug_cmd(), 'WORKER_THREADS', 'PAUSE').ok()
    query_vec = load_vectors_to_redis(env, n_vectors, 0, dim, ids_offset=0, seed=11) # new seed to generate new vectors
    assertInfoField(env, 'idx', 'num_docs', n_vectors)
    debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')
    local_marked_deleted_vectors = to_dict(debug_info['BACKEND_INDEX'])['NUMBER_OF_MARKED_DELETED']
    env.assertEqual(local_marked_deleted_vectors, n_local_vectors_before_update)

    # Get the updated numer of local vectors after the update, and validate that all of them are in the frontend
    # index (hadn't been ingested already).
    n_local_vectors = get_vecsim_debug_dict(env, 'idx', 'vector')['INDEX_LABEL_COUNT']
    env.assertEqual(to_dict(debug_info['FRONTEND_INDEX'])['INDEX_SIZE'], n_local_vectors)
    env.assertEqual(to_dict(debug_info['BACKEND_INDEX'])['INDEX_SIZE'], n_local_vectors_before_update)

    # We dispose marked deleted vectors whenever we have at least <block_size> vectors that are ready
    # (that is, no other node in HNSW is pointing to the deleted node).
    while local_marked_deleted_vectors > block_size:
        env.expect(debug_cmd(), 'WORKER_THREADS', 'RESUME').ok()
        res = env.cmd('FT.SEARCH', 'idx', f'*=>[KNN $K @vector $vec_param EF_RUNTIME {n_local_vectors}]',
                                   'SORTBY', '__vector_score', 'RETURN', 1, '__vector_score',
                                   'LIMIT', 0, 10, 'PARAMS', 4, 'K', 10, 'vec_param', query_vec.tobytes())
        # Expect that the first result's would be around zero, since the query vector itself exists in the
        # index (id 0)
        env.assertAlmostEqual(float(res[2][1]), 0, 1e-5)
        # Also validate that we don't find documents that are marked as deleted - the query vector was overwritten.
        res = env.cmd('FT.SEARCH', 'idx', f'*=>[KNN $K @vector $vec_param EF_RUNTIME {n_vectors}]',
                                           'SORTBY', '__vector_score', 'RETURN', 1, '__vector_score',
                                           'LIMIT', 0, 10, 'PARAMS', 4, 'K', 10, 'vec_param',
                      query_before_update.tobytes())
        env.assertGreater(float(res[2][1]), float(0))

        # Invoke GC, so we clean zombies for which all their repair jobs are done.
        forceInvokeGC(env)

        # Number of zombies should decrease from one iteration to another.
        env.expect(debug_cmd(), 'WORKER_THREADS', 'PAUSE').ok()
        debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')
        local_marked_deleted_vectors_new = to_dict(debug_info['BACKEND_INDEX'])['NUMBER_OF_MARKED_DELETED']
        env.assertLessEqual(local_marked_deleted_vectors_new, local_marked_deleted_vectors)
        local_marked_deleted_vectors = local_marked_deleted_vectors_new

    # Eventually, all updated vectors should be in the backend index, and all zombies should be removed.
    env.expect(debug_cmd(), 'WORKER_THREADS', 'RESUME').ok()
    env.expect(debug_cmd(), 'WORKER_THREADS', 'DRAIN').ok()
    forceInvokeGC(env)
    debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')
    env.assertEqual(to_dict(debug_info['BACKEND_INDEX'])['INDEX_SIZE'], n_local_vectors)
    env.assertEqual(to_dict(debug_info['FRONTEND_INDEX'])['INDEX_SIZE'], 0)
    env.assertEqual(to_dict(debug_info['BACKEND_INDEX'])['NUMBER_OF_MARKED_DELETED'], 0)

@skip(cluster=True)
def test_multiple_loaders():
    env = initEnv()
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC').ok()
    n_docs = 10
    limit = 5

    conn = getConnectionByEnv(env)
    for n in range(n_docs):
        conn.execute_command('HSET', n, 'n', n)

    cmd = ['FT.AGGREGATE', 'idx', '*']
    cmd += ['LOAD', '*'] * limit # Add multiple loaders
    cmd += ['LIMIT', '0', limit]

    env.expect(*cmd).noError().apply(lambda x: x[1:]).equal([['n', '0'], ['n', '1'], ['n', '2'], ['n', '3'], ['n', '4']])
