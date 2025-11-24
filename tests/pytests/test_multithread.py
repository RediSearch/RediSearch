# -*- coding: utf-8 -*-
from common import *

def initEnv(moduleArgs: str = 'WORKERS 1'):
    assert(moduleArgs != '')
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


@skip(cluster=True)
def test_worker_threads_sanity():
    env = initEnv(moduleArgs='WORKERS 2 DEFAULT_DIALECT 2')
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
        env.expect(debug_cmd(), 'WORKERS', 'PAUSE').ok()
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
                env.expect(debug_cmd(), 'WORKERS', 'RESUME').ok()
            # At first, we expect to see background indexing, but after RDB load, we expect that all vectors
            # are indexed before RDB loading ends
            debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')
            if i==2:
                env.assertEqual(debug_info['BACKGROUND_INDEXING'], 0, message=f"iteration {it+1} after reloading")
            env.expect(debug_cmd(), 'WORKERS', 'drain').ok()


def test_delete_index_while_indexing():
    env = initEnv(moduleArgs='WORKERS 2 DEFAULT_DIALECT 2')
    n_shards = env.shardsCount
    n_vectors = 100 * n_shards
    dim = 4
    data_type = 'FLOAT16'
    # Load random vectors into redis.
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'vector', 'VECTOR', 'HNSW', '8', 'TYPE', data_type, 'M', '64',
               'DIM', dim, 'DISTANCE_METRIC', 'L2').ok()
    env.expect(debug_cmd(), 'WORKERS', 'PAUSE').ok()
    load_vectors_to_redis(env, n_vectors, 0, dim, data_type)
    assertInfoField(env, 'idx', 'num_docs', n_vectors)
    n_local_vector = get_vecsim_debug_dict(env, 'idx', 'vector')['INDEX_LABEL_COUNT']

    # Delete index while vectors are being indexed (to validate proper cleanup of background jobs in sanitizer).
    # We expect that jobs will continue running, but the weak ref will not be promoted, and we discard them.
    env.cmd('FT.DROPINDEX', 'idx')
    stats = getWorkersThpoolStats(env)
    env.assertEqual(n_local_vector, stats['totalPendingJobs'], message=stats)
    env.expect(debug_cmd(), 'WORKERS', 'RESUME').ok()
    env.expect(debug_cmd(), 'WORKERS', 'DRAIN').ok()
    stats = getWorkersThpoolStats(env)
    env.assertEqual(n_local_vector, stats['totalJobsDone'], message=stats)


def do_burst_threads_sanity(algo, data_type, test_name):
    env = initEnv(moduleArgs='MIN_OPERATION_WORKERS 2 DEFAULT_DIALECT 2')
    # Sanity check that the test parameters match the test name
    env.assertEqual([algo, data_type], test_name.split('_')[-2:])
    n_vectors = 100 * env.shardsCount
    dim = 4
    k = 10
    expected_total_jobs = 0

    additional_params = {'HNSW': ['EF_CONSTRUCTION', n_vectors, 'EF_RUNTIME', n_vectors],
    'FLAT': [], 'SVS-VAMANA': ['CONSTRUCTION_WINDOW_SIZE', n_vectors, 'SEARCH_WINDOW_SIZE', n_vectors]}
    # Load random vectors into redis, save the first one to use as query vector later on. We set EF_C and
    # EF_R to n_vectors to ensure that all vectors would be reachable in HNSW and avoid flakiness in search.
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'vector', 'VECTOR', algo, str(6+len(additional_params[algo])),
                'TYPE', data_type, 'DIM', dim, 'DISTANCE_METRIC', 'L2', *additional_params[algo]).ok()
    query_vec = load_vectors_to_redis(env, n_vectors, 0, dim, data_type)
    n_local_vectors = get_vecsim_debug_dict(env, 'idx', 'vector')['INDEX_LABEL_COUNT']

    res_before = env.cmd('FT.SEARCH', 'idx', '*=>[KNN $K @vector $vec_param]', 'SORTBY',
                                        '__vector_score', 'RETURN', 1, '__vector_score', 'LIMIT', 0, k,
                                        'PARAMS', 4, 'K', k, 'vec_param', query_vec.tobytes())
    # Expect that the first result's would be around zero, since the query vector itself exists in the
    # index (id 0)
    env.assertAlmostEqual(float(res_before[2][1]), 0, 1e-5)
    waitForRdbSaveToFinish(env)
    for i in env.reloadingIterator():
        debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')
        env.assertEqual(debug_info['ALGORITHM'], 'TIERED' if algo == 'HNSW' or algo == 'SVS-VAMANA' else algo)
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
                                    '__vector_score', 'RETURN', 1, '__vector_score', 'LIMIT', 0, k,
                                    'PARAMS', 4, 'K', k, 'vec_param', query_vec.tobytes())
        env.assertEqual(res, res_before)

# Generate test functions for each combination of algorithm and data type
func_gen = lambda al, dt, tn: lambda: do_burst_threads_sanity(al, dt, tn)
for algo in VECSIM_ALGOS:
    for data_type in VECSIM_DATA_TYPES:
        if algo == 'SVS-VAMANA' and data_type not in ('FLOAT32', 'FLOAT16'):
            continue
        test_name = f"test_burst_threads_sanity_{algo}_{data_type}"
        globals()[test_name] = func_gen(algo, data_type, test_name)

def test_workers_priority_queue():
    env = initEnv(moduleArgs='WORKERS 2 DEFAULT_DIALECT 2')
    n_shards = env.shardsCount
    n_vectors = 200 * n_shards
    dim = 4
    data_type = 'BFLOAT16'

    # Load random vectors into redis, save the last one to use as query vector later on.
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'vector', 'VECTOR', 'HNSW', '6', 'TYPE', data_type, 'DIM', dim,
               'DISTANCE_METRIC', 'L2').ok()
    env.expect(debug_cmd(), 'WORKERS', 'PAUSE').ok()
    query_vec = load_vectors_to_redis(env, n_vectors, n_vectors-1, dim, data_type)
    assertInfoField(env, 'idx', 'num_docs', n_vectors)

    # Expect that some vectors are still being indexed in the background after we are done loading.
    debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')
    local_n_vectors = to_dict(debug_info['FRONTEND_INDEX'])['INDEX_SIZE']
    vectors_left_to_index = local_n_vectors

    # Run queries during indexing
    iteration_count = 0
    while debug_info['BACKGROUND_INDEXING'] == 1:
        env.expect(debug_cmd(), 'WORKERS', 'RESUME').ok()
        iteration_count+=1
        res = env.cmd('FT.SEARCH', 'idx', f'*=>[KNN $K @vector $vec_param EF_RUNTIME {n_vectors}]',
                                   'SORTBY', '__vector_score', 'RETURN', 1, '__vector_score', 'LIMIT', 0, 10,
                                   'PARAMS', 4, 'K', 10, 'vec_param', query_vec.tobytes())
        # Expect that the first result's would be around zero, since the query vector itself exists in the
        # index (last id)
        env.assertAlmostEqual(float(res[2][1]), 0, 1e-5)
        env.expect(debug_cmd(), 'WORKERS', 'PAUSE').ok()
        # We expect that the number of vectors left to index will decrease from one iteration to another.
        debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')
        vectors_left_to_index_new = to_dict(debug_info['FRONTEND_INDEX'])['INDEX_SIZE']
        env.assertLessEqual(vectors_left_to_index_new, vectors_left_to_index)
        vectors_left_to_index = vectors_left_to_index_new
        # Number of jobs done should be the number of vector indexed plus number of queries that ran.
        env.assertEqual(getWorkersThpoolStats(env)['totalJobsDone'],
                        local_n_vectors-vectors_left_to_index + iteration_count)
    env.expect(debug_cmd(), 'WORKERS', 'RESUME').ok()


def test_buffer_limit():
    buffer_limit = 100
    env = initEnv(moduleArgs=f'WORKERS 2 DEFAULT_DIALECT 2 TIERED_HNSW_BUFFER_LIMIT {buffer_limit}')
    n_shards = env.shardsCount
    dim = 4
    n_vectors = 2 * n_shards * buffer_limit

    # Load random vectors into redis
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'vector', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32',
               'DIM', dim, 'DISTANCE_METRIC', 'L2').ok()
    env.expect(debug_cmd(), 'WORKERS', 'PAUSE').ok()
    load_vectors_to_redis(env, n_vectors, 0, dim)
    assertInfoField(env, 'idx', 'num_docs', n_vectors)

    # Verify that the frontend flat index is full up to the buffer limit, and the rest of the vectors were indexed
    # directly into HNSW backend index.
    debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')
    n_local_vectors = debug_info['INDEX_LABEL_COUNT']
    env.assertEqual(to_dict(debug_info['FRONTEND_INDEX'])['INDEX_SIZE'], buffer_limit)
    env.assertEqual(to_dict(debug_info['BACKEND_INDEX'])['INDEX_SIZE'], n_local_vectors-buffer_limit)

    env.expect(debug_cmd(), 'WORKERS', 'RESUME').ok()
    env.expect(debug_cmd(), 'WORKERS', 'DRAIN').ok()

    # After running all insert jobs, all vectors should move to the backend index.
    debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')
    env.assertEqual(to_dict(debug_info['FRONTEND_INDEX'])['INDEX_SIZE'], 0)
    env.assertEqual(to_dict(debug_info['BACKEND_INDEX'])['INDEX_SIZE'], n_local_vectors)

@skip(asan=True, msan=True)
def test_async_updates_sanity():
    env = initEnv(moduleArgs='WORKERS 2 DEFAULT_DIALECT 2 TIERED_HNSW_BUFFER_LIMIT 10000')
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
    env.assertEqual(run_command_on_all_shards(env, *[debug_cmd(), 'WORKERS', 'DRAIN']), ['OK']*n_shards)
    stats = run_command_on_all_shards(env, *[debug_cmd(), 'WORKERS', 'STATS'])
    env.assertEqual(sum([to_dict(shard_stat)['totalPendingJobs'] for shard_stat in stats]), 0)  # 0 in each shard
    total_jobs_done = sum([to_dict(shard_stat)['totalJobsDone'] for shard_stat in stats])

    env.assertEqual(total_jobs_done, n_vectors + n_shards)  # job per vector + one job for the query.
    debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')
    env.assertEqual(to_dict(debug_info['BACKEND_INDEX'])['INDEX_SIZE'], n_local_vectors_before_update)

    # Overwrite vectors. All vectors were ingested into the background index, so now we collect new vectors
    # into the frontend index and prepare repair and ingest jobs. The overwritten vector were not removed from
    # the backend index yet.
    env.assertEqual(run_command_on_all_shards(env, *[debug_cmd(), 'WORKERS', 'PAUSE']), ['OK']*n_shards)
    query_vec = load_vectors_to_redis(env, n_vectors, 0, dim, ids_offset=0, seed=11) # new seed to generate new vectors
    assertInfoField(env, 'idx', 'num_docs', n_vectors)
    debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')
    local_marked_deleted_vectors = to_dict(debug_info['BACKEND_INDEX'])['NUMBER_OF_MARKED_DELETED']
    env.assertEqual(local_marked_deleted_vectors, n_local_vectors_before_update)

    # Get the updated number of local vectors after the update, and validate that all of them are in the frontend
    # index (hadn't been ingested already).
    n_local_vectors = get_vecsim_debug_dict(env, 'idx', 'vector')['INDEX_LABEL_COUNT']
    env.assertEqual(to_dict(debug_info['FRONTEND_INDEX'])['INDEX_SIZE'], n_local_vectors)
    env.assertEqual(to_dict(debug_info['BACKEND_INDEX'])['INDEX_SIZE'], n_local_vectors_before_update)

    # We dispose marked deleted vectors whenever we have at least <block_size> vectors that are ready
    # (that is, no other node in HNSW is pointing to the deleted node).
    while local_marked_deleted_vectors > block_size:
        env.assertEqual(run_command_on_all_shards(env, *[debug_cmd(), 'WORKERS', 'RESUME']), ['OK']*n_shards)
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

        # Invoke GC, so we clean zombies for which all their repair jobs are done. We run in background
        # so in case child process is not receiving cpu time, we do not hang the gc thread in the parent process.
        forceBGInvokeGC(env)

        # Number of zombies should decrease from one iteration to another.
        env.assertEqual(run_command_on_all_shards(env, *[debug_cmd(), 'WORKERS', 'PAUSE']), ['OK']*n_shards)
        debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')

        local_marked_deleted_vectors_new = to_dict(debug_info['BACKEND_INDEX'])['NUMBER_OF_MARKED_DELETED']
        env.assertLessEqual(local_marked_deleted_vectors_new, local_marked_deleted_vectors)
        local_marked_deleted_vectors = local_marked_deleted_vectors_new

    # Eventually, all updated vectors should be in the backend index, and all zombies should be removed.
    env.assertEqual(run_command_on_all_shards(env, *[debug_cmd(), 'WORKERS', 'RESUME']), ['OK']*n_shards)
    env.assertEqual(run_command_on_all_shards(env, *[debug_cmd(), 'WORKERS', 'DRAIN']), ['OK']*n_shards)

    forceInvokeGC(env, timeout=0)
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

@skip(cluster=True)
def test_switch_loader_modes():
    # Create an environment with workers (0)
    env = initEnv('WORKERS 1')
    n_docs = 10
    cursor_count = 2
    # Having two loaders to test when the loader is last and when it is not
    query = ('FT.AGGREGATE', 'idx', '*', 'LOAD', '*', 'LOAD', '*', 'WITHCURSOR', 'COUNT', cursor_count)
    read_from_cursor = lambda cursor: env.expect('FT.CURSOR', 'READ', 'idx', cursor).noError().res[1]

    # Add some documents and create an index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC').ok()
    for n in range(n_docs):
        env.cmd('HSET', n, 'n', n)

    # Create a cursor while using the full mode
    _, cursor1 = env.cmd(*query)

    # Turn off the multithread mode (1)
    env.expect(config_cmd(), 'SET', 'WORKERS', '0').ok()

    # Create a cursor while using the off mode
    _, cursor2 = env.cmd(*query)
    # Read from the first cursor
    cursor1 = read_from_cursor(cursor1)

    # Turn on the multithread mode (2)
    env.expect(config_cmd(), 'SET', 'WORKERS', '1').ok()

    # Read from the cursors
    cursor1 = read_from_cursor(cursor1)
    cursor2 = read_from_cursor(cursor2)

    # Turn off the multithread mode (3)
    env.expect(config_cmd(), 'SET', 'WORKERS', '0').ok()

    # Read from the cursors
    cursor1 = read_from_cursor(cursor1)
    cursor2 = read_from_cursor(cursor2)

    # Turn on the multithread mode last time (4)
    env.expect(config_cmd(), 'SET', 'WORKERS', '1').ok()

    # Read from the second cursor
    cursor2 = read_from_cursor(cursor2)

    # Delete the cursors.
    # The first cursor should be in the off mode and the second cursor should be in the full mode
    # We expect no errors or leaks
    env.expect('FT.CURSOR', 'DEL', 'idx', cursor1).noError().ok()
    env.expect('FT.CURSOR', 'DEL', 'idx', cursor2).noError().ok()

    # Send a new query with an implicit loader
    _, cursor3 = env.cmd('FT.AGGREGATE', 'idx', '*', 'GROUPBY', '1', '@n',
                         'WITHCURSOR', 'COUNT', cursor_count)

    env.expect(config_cmd(), 'SET', 'WORKERS', '0').ok()

    cursor3 = read_from_cursor(cursor3)

    env.expect(config_cmd(), 'SET', 'WORKERS', '1').ok()

    cursor3 = read_from_cursor(cursor3)

    env.expect('FT.CURSOR', 'DEL', 'idx', cursor3).noError().ok()

@skip(cluster=False)
def test_change_num_connections():
    env = initEnv('SEARCH_IO_THREADS 20')
    # Validate the default values
    env.expect(config_cmd(), 'GET', 'WORKERS').equal([['WORKERS', '0']])
    env.expect(config_cmd(), 'GET', 'CONN_PER_SHARD').equal([['CONN_PER_SHARD', '0']])

    # The logic of the number of connections is as follows:
    # - If `CONN_PER_SHARD` is not 0, the number of connections is `CONN_PER_SHARD`
    # - If `CONN_PER_SHARD` is 0, the number of connections is `WORKERS` + 1
    # - Each SEARCH_IO_THREAD has this number of connections: CEIL_DIV(connPerShard, coordinatorIOThreads)

    # Helper that will return the expected output structure.
    # In this test we don't care about the actual values, so we use the ANY matcher.
    # Example of the expected output (for 3 shards and 2 connections):
    # ['127.0.0.1:6379', ['Connected', 'Connected'],
    #  '127.0.0.1:6381', ['Connected', 'Connecting'],
    #  '127.0.0.1:6383', ['Connected', 'Connected']]
    num_io_threads = 20

    # This function implements the same logic as CEIL_DIV(connPerShard, coordinatorIOThreads)
    # from src/coord/config.c line 85: size_t conn_pool_size = CEIL_DIV(connPerShard, realConfig->coordinatorIOThreads);
    def compute_total_number_of_connections(num_connections):
        import math
        return num_io_threads * max(1, math.ceil(num_connections // num_io_threads))

    def expected(conns):
        return [
            ANY,          # The shard id (host:port)
            [ANY] * conns # The connections states
        ] * env.shardsCount

    # By default, the number of connections is 1 (WORKERS=0, so connPerShard=0+1=1, conn_pool_size=ceil(1/20)=1)
    env.expect(debug_cmd(), 'SHARD_CONNECTION_STATES').equal(expected(compute_total_number_of_connections(1)))

    # Increase the number of worker threads to 6
    env.expect(config_cmd(), 'SET', 'WORKERS', '6').ok()
    # The number of connections should be ceil(7/20) = 1
    env.expect(debug_cmd(), 'SHARD_CONNECTION_STATES').equal(expected(compute_total_number_of_connections(7)))

    # Set the number of connections to 4
    env.expect(config_cmd(), 'SET', 'CONN_PER_SHARD', '4').ok()
    # The number of connections should be ceil(4/20) = 1
    env.expect(debug_cmd(), 'SHARD_CONNECTION_STATES').equal(expected(compute_total_number_of_connections(4)))

    # Decrease the number of worker threads to 5
    env.expect(config_cmd(), 'SET', 'WORKERS', '5').ok()
    # The number of connections should remain ceil(4/20) = 1 (CONN_PER_SHARD takes precedence)
    env.expect(debug_cmd(), 'SHARD_CONNECTION_STATES').equal(expected(compute_total_number_of_connections(4)))

    # Set the number of connections to 0
    env.expect(config_cmd(), 'SET', 'CONN_PER_SHARD', '0').ok()
    # The number of connections should be ceil(6/20) = 1 (5 worker threads + 1)
    env.expect(debug_cmd(), 'SHARD_CONNECTION_STATES').equal(expected(compute_total_number_of_connections(6)))

    # Set back the number of worker threads to 0
    env.expect(config_cmd(), 'SET', 'WORKERS', '0').ok()
    # The number of connections should be ceil(1/20) = 1 again
    env.expect(debug_cmd(), 'SHARD_CONNECTION_STATES').equal(expected(compute_total_number_of_connections(1)))

    # Set back Connection per shard to 40
    env.expect(config_cmd(), 'SET', 'CONN_PER_SHARD', '40').ok()
    # The number of connections should be ceil(40/20) = 2
    env.expect(debug_cmd(), 'SHARD_CONNECTION_STATES').equal(expected(compute_total_number_of_connections(40)))

    # Set Connection per shard to 100
    env.expect(config_cmd(), 'SET', 'CONN_PER_SHARD', '100').ok()
    # The number of connections should be ceil(100/20) = 5
    env.expect(debug_cmd(), 'SHARD_CONNECTION_STATES').equal(expected(compute_total_number_of_connections(100)))

def test_change_workers_number():
    def send_query():
        env.expect('ft.search', 'idx', '*').equal([0])

    def check_threads(expected_num_threads_alive, expected_n_threads):
        env.assertEqual(getWorkersThpoolStats(env)['numThreadsAlive'], expected_num_threads_alive, depth=1, message='numThreadsAlive should match num_threads_alive')
        env.assertEqual(getWorkersThpoolNumThreads(env), expected_n_threads, depth=1, message='n_threads should match WORKERS')

    # On start up the threadpool is not initialized. We can change the value of requested threads
    # without actually creating the threads.
    env = initEnv(moduleArgs='WORKERS 1')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'text').ok()
    check_threads(expected_num_threads_alive=0, expected_n_threads=1)

    # Before starting the test, set the number of connections per shard to 2 to avoid flakiness
    # due to connections being rapidly opened/closed when changing the number of workers.
    env.expect(config_cmd(), 'SET', 'CONN_PER_SHARD', '2').ok()

    # Increase number of threads
    env.expect(config_cmd(), 'SET', 'WORKERS', '2').ok()
    # After the first increase, since no queries arrived yet
    check_threads(expected_num_threads_alive=0, expected_n_threads=2)
    # Decrease number of threads
    env.expect(config_cmd(), 'SET', 'WORKERS', '1').ok()
    check_threads(expected_num_threads_alive=0, expected_n_threads=1)
    # If I send many queries, we know one of the threads will take the ADMIN job and terminate
    num_query_threads = 100
    query_threads = []

    for i in range(num_query_threads):
        t = threading.Thread(target=send_query, name=f'QueryThread-{i}')
        t.start()
        query_threads.append(t)

    for t in query_threads:
        t.join()

    check_threads(expected_num_threads_alive=1, expected_n_threads=1)
    # Set it to 0
    env.expect(config_cmd(), 'SET', 'WORKERS', '0').ok()
    time.sleep(1)
    check_threads(expected_num_threads_alive=0, expected_n_threads=0)

    # Query should not be executed by the threadpool

    env.expect('ft.search', 'idx', '*').equal([0])
    check_threads(expected_num_threads_alive=0, expected_n_threads=0)
    env.assertEqual(getWorkersThpoolStats(env)['totalJobsDone'], num_query_threads)

    # Enable threadpool
    env.expect(config_cmd(), 'SET', 'WORKERS', '1').ok()
    # Since additioning workers after initialization is not lazy anymore, this would indeed create the thread
    check_threads(expected_num_threads_alive=0, expected_n_threads=1)
    env.expect('ft.search', 'idx', '*').equal([0])
    # Keep initialized
    check_threads(expected_num_threads_alive=1, expected_n_threads=1)
    # wait for the job to finish
    env.expect(debug_cmd(), 'WORKERS', 'DRAIN').ok()

    # Query should be executed by the threadpool
    env.assertEqual(getWorkersThpoolStats(env)['totalJobsDone'], num_query_threads + 1)

    # Add threads to a running pool
    env.expect(config_cmd(), 'SET', 'WORKERS', '2').ok()
    check_threads(expected_num_threads_alive=2, expected_n_threads=2)
    # Remove threads from a running pool
    env.expect(config_cmd(), 'SET', 'WORKERS', '1').ok()
    time.sleep(1)
    check_threads(expected_num_threads_alive=1, expected_n_threads=1)

    # Terminate all threads
    env.expect(config_cmd(), 'SET', 'WORKERS', '0').ok()
    time.sleep(1)
    env.assertEqual(getWorkersThpoolNumThreads(env), 0)

    # Query should not be executed by the threadpool
    env.expect('ft.search', 'idx', '*').equal([0])
    env.assertEqual(getWorkersThpoolStats(env)['totalJobsDone'], num_query_threads + 1)

def check_threads(env, expected_num_threads_alive, expected_n_threads):
    env.assertEqual(getWorkersThpoolStats(env)['numThreadsAlive'], expected_num_threads_alive, depth=1, message='numThreadsAlive should match num_threads_alive')
    env.assertEqual(getWorkersThpoolNumThreads(env), expected_n_threads, depth=1, message='n_threads should match WORKERS')

def test_workers_reduction_sequence():
    """
    Test gradual reduction of workers to see if the issue is specific to large deltas.
    This test reduces workers gradually: 8 -> 4 -> 2 -> 1 -> 0
    """
    env = Env(moduleArgs='WORKERS 8', enableDebugCommand=True)

    # Create simple index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'text', 'TEXT').ok()

    # Add some documents
    conn = getConnectionByEnv(env)
    for i in range(100):
        conn.execute_command('HSET', f'doc{i}', 'text', f'document {i} with searchable content')

    waitForIndex(env, 'idx')

    # Test gradual reduction
    worker_sequence = [8, 4, 2, 1, 0]
    result = env.cmd('FT.SEARCH', 'idx', 'searchable', 'LIMIT', '0', '5')
    # I can check the thread pool state after the thpool is initialized by the first query
    check_threads(env, 8, 8)

    for workers in worker_sequence:
        env.debugPrint(f"Testing with WORKERS={workers}", force=True)

        if workers < 8:  # Skip first iteration (already at 8)
            env.expect(config_cmd(), 'SET', 'WORKERS', str(workers)).ok()

        # Verify config
        current = env.cmd(config_cmd(), 'GET', 'WORKERS')
        env.assertEqual(current, [['WORKERS', str(workers)]])

        # Verify responsiveness
        ping_result = env.cmd('PING')
        env.assertTrue(ping_result in ['PONG', True])

        # Run a query
        result = env.cmd('FT.SEARCH', 'idx', 'searchable', 'LIMIT', '0', '5')
        env.assertTrue(result[0] > 0, message="Search should work with WORKERS={}".format(workers))
        # Small delay between changes
        time.sleep(0.5)

    time.sleep(5)
    check_threads(env, 0, 0)


def test_workers_zero_to_nonzero():
    """
    Test that increasing workers from 0 to a higher value also works correctly.
    This tests the reverse direction to ensure the connection pool expansion works.
    """
    # Start with WORKERS=0
    env = Env(moduleArgs='WORKERS 0', enableDebugCommand=True)

    check_threads(env, 0, 0)
    # Create index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'text', 'TEXT').ok()

    # Add documents
    conn = getConnectionByEnv(env)
    for i in range(100):
        conn.execute_command('HSET', f'doc{i}', 'text', f'document {i}')

    waitForIndex(env, 'idx')

    # Verify initial state
    env.assertEqual(env.cmd(config_cmd(), 'GET', 'WORKERS'), [['WORKERS', '0']])

    # Query should work with WORKERS=0 (on main thread)
    result = env.cmd('FT.SEARCH', 'idx', '*', 'LIMIT', '0', '5')
    env.assertTrue(result[0] > 0)

    # Increase workers to 8
    env.expect(config_cmd(), 'SET', 'WORKERS', '8').ok()
    env.assertEqual(env.cmd(config_cmd(), 'GET', 'WORKERS'), [['WORKERS', '8']])
    # Lazy initialization of threads
    check_threads(env, 0, 8)
    result = env.cmd('FT.SEARCH', 'idx', '*', 'LIMIT', '0', '5')
    check_threads(env, 8, 8)

    # Verify still responsive
    ping_result = env.cmd('PING')
    env.assertTrue(ping_result in ['PONG', True])

    # Query should still work
    result = env.cmd('FT.SEARCH', 'idx', '*', 'LIMIT', '0', '5')
    env.assertTrue(result[0] > 0)

def test_workers_increase_from_nonzero():
    """
    Test that increasing workers from 0 to a higher value also works correctly.
    This tests the reverse direction to ensure the connection pool expansion works.
    """
    # Start with WORKERS=0
    env = Env(moduleArgs='WORKERS 2', enableDebugCommand=True)

    # Create index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'text', 'TEXT').ok()

    # Add documents
    conn = getConnectionByEnv(env)
    for i in range(100):
        conn.execute_command('HSET', f'doc{i}', 'text', f'document {i}')

    waitForIndex(env, 'idx')

    # Verify initial state
    env.assertEqual(env.cmd(config_cmd(), 'GET', 'WORKERS'), [['WORKERS', '2']])

    # Query should work with WORKERS=0 (on main thread)
    result = env.cmd('FT.SEARCH', 'idx', '*', 'LIMIT', '0', '5')
    env.assertTrue(result[0] > 0)
    # I can check the thread pool state after the thpool is initialized by the first query
    check_threads(env, 2, 2)

    # Increase workers to 8
    env.expect(config_cmd(), 'SET', 'WORKERS', '8').ok()
    env.assertEqual(env.cmd(config_cmd(), 'GET', 'WORKERS'), [['WORKERS', '8']])
    check_threads(env, 8, 8)

    # Verify still responsive
    ping_result = env.cmd('PING')
    env.assertTrue(ping_result in ['PONG', True])

    # Query should still work
    result = env.cmd('FT.SEARCH', 'idx', '*', 'LIMIT', '0', '5')
    env.assertTrue(result[0] > 0)

def testNameLoader(env: Env):
    def get_RP_name(profile_res):
        shard0 = to_dict(profile_res[1])['Shards'][0]
        last_rp = to_dict(shard0)['Result processors profile'][-1]
        return to_dict(last_rp)['Type']

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'sortable', 'TEXT', 'SORTABLE', 'UNF', 'not-sortable', 'TEXT').ok()
    with env.getClusterConnectionIfNeeded() as con:
        for i in range(10):
            con.execute_command('HSET', f'doc:{i}', 'sortable', f'S{i}', 'not-sortable', f'NS{i}')

    normal_search = env.cmd('FT.SEARCH', 'idx', '*', 'SORTBY', 'sortable', 'RETURN', 1, '__key')
    normal_aggregate = env.cmd('FT.AGGREGATE', 'idx', '*', 'SORTBY', '1', '@sortable', 'LOAD', 3, '@__key', 'AS', 'doc_id')

    # enable unstable features so we have the special loader
    verify_command_OK_on_all_shards(env, config_cmd(), 'SET', 'ENABLE_UNSTABLE_FEATURES', 'true')

    # Run the search and aggregate commands again, expecting the same results
    env.expect('FT.SEARCH', 'idx', '*', 'SORTBY', 'sortable', 'RETURN', 1, '__key').equal(normal_search)
    env.expect('FT.AGGREGATE', 'idx', '*', 'SORTBY', '1', '@sortable', 'LOAD', 3, '@__key', 'AS', 'doc_id').equal(normal_aggregate)

    # Check that the right loader is used
    res = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '*', 'RETURN', 1, '__key')
    env.assertEqual(get_RP_name(res), 'Key Name Loader')
    res = env.cmd('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*', 'LOAD', 3, '@__key', 'AS', 'doc_id')
    env.assertEqual(get_RP_name(res), 'Key Name Loader')

    # Check that the right loader is used in the aggregate command when loading multiple fields
    res = env.cmd('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*', 'LOAD', 2, '@sortable', '@__key')
    env.assertEqual(get_RP_name(res), 'Key Name Loader', message="Expected to be optimized when loading only sortables and __key")
    res = env.cmd('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*', 'LOAD', 2, '@__key', '@sortable')
    env.assertEqual(get_RP_name(res), 'Key Name Loader', message="Expected to be optimized when loading only sortables and __key")
    res = env.cmd('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*', 'LOAD', 2, '@not-sortable', '@__key')
    env.assertEqual(get_RP_name(res), 'Loader', message="Expected not to be optimized")
    res = env.cmd('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*', 'LOAD', 2, '@__key', '@not-sortable')
    env.assertEqual(get_RP_name(res), 'Loader', message="Expected not to be optimized")
    res = env.cmd('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*', 'LOAD', 1, '@not-sortable')
    env.assertEqual(get_RP_name(res), 'Loader', message="Expected not to be optimized")

    env.expect('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@__key', 'SORTBY', '1', '@__key').equal(
        [10] + [['__key', f'doc:{i}'] for i in range(10)])
    env.expect('FT.AGGREGATE', 'idx', '*', 'LOAD', 3, '@__key', 'AS', 'key',
                                           'GROUPBY', '1', '@key', 'REDUCE', 'COUNT', '0', 'AS', 'count',
                                           'SORTBY', '1', '@key').equal(
        [10] + [['key', f'doc:{i}', 'count', '1'] for i in range(10)])

    # Check that the right loader is used in the aggregate command when loading multiple fields with BG query
    verify_command_OK_on_all_shards(env, config_cmd(), 'SET', 'WORKERS', '1')
    res = env.cmd('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*', 'LOAD', 2, '@sortable', '@__key')
    env.assertEqual(get_RP_name(res), 'Key Name Loader', message="Expected to be optimized when loading only sortables and __key")
    res = env.cmd('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*', 'LOAD', 2, '@__key', '@sortable')
    env.assertEqual(get_RP_name(res), 'Key Name Loader', message="Expected to be optimized when loading only sortables and __key")
    res = env.cmd('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*', 'LOAD', 2, '@not-sortable', '@__key')
    env.assertEqual(get_RP_name(res), 'Threadsafe-Loader', message="Expected not to be optimized")
    res = env.cmd('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*', 'LOAD', 2, '@__key', '@not-sortable')
    env.assertEqual(get_RP_name(res), 'Threadsafe-Loader', message="Expected not to be optimized")
    res = env.cmd('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*', 'LOAD', 1, '@not-sortable')
    env.assertEqual(get_RP_name(res), 'Threadsafe-Loader', message="Expected not to be optimized")

def _test_ft_search_with_io_threads(io_threads):
    """Helper function to test queries with specific IO thread count"""
    # Create environment with specific IO thread count
    env = initEnv(moduleArgs=f'SEARCH_IO_THREADS {io_threads}')

    # Create index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'txt', 'TEXT', 'num', 'NUMERIC', 'SORTABLE').ok()

    # Add test documents
    conn = getConnectionByEnv(env)
    doc_count = 100
    for i in range(doc_count):
        conn.execute_command('HSET', f'doc:{i}',
                            'txt', f'hello world document {i}',
                            'num', i)

    # Run different query types and verify results

    # 1. Simple search
    res = env.cmd('FT.SEARCH', 'idx', 'hello', 'NOCONTENT')
    env.assertEqual(res[0], doc_count, message=f"Simple search with {io_threads} IO threads")

    # 2. Numeric range query
    res = env.cmd('FT.SEARCH', 'idx', '@num:[10 50]', 'NOCONTENT')
    env.assertEqual(res[0], 41, message=f"Numeric range query with {io_threads} IO threads")

    # 3. Combined query with sorting
    res = env.cmd('FT.SEARCH', 'idx', 'world @num:[20 40]', 'SORTBY', 'num', 'DESC', 'NOCONTENT')
    env.assertEqual(res[0], 21, message=f"Combined query with {io_threads} IO threads")
    # Check sort order (first result should be doc:40)
    env.assertEqual(res[1], 'doc:40', message=f"Sort order with {io_threads} IO threads")

    # 4. Aggregate query
    res = env.cmd('FT.AGGREGATE', 'idx', '*',
                 'GROUPBY', '1', '@num',
                 'REDUCE', 'count', '0', 'AS', 'count',
                 'FILTER', '@count > 0')
    env.assertEqual(len(res), doc_count + 1, message=f"Aggregate query with {io_threads} IO threads")

    # Clean up for next iteration
    env.cmd('FLUSHALL')
    env.flush()
    env.stop()


@skip(cluster=False)
def test_ft_search_with_coord_1_io_thread():
    _test_ft_search_with_io_threads(1)

@skip(cluster=False)
def test_ft_search_with_coord_5_io_threads():
    _test_ft_search_with_io_threads(5)

@skip(cluster=False)
def test_ft_search_with_coord_10_io_threads():
    _test_ft_search_with_io_threads(10)


def _test_ft_aggregate_with_io_threads(io_threads):
    """Helper function to test aggregate queries with specific IO thread count"""
    # Create environment with specific IO thread count
    env = initEnv(moduleArgs=f'SEARCH_IO_THREADS {io_threads}')

    # Create index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'txt', 'TEXT', 'num', 'NUMERIC', 'SORTABLE', 'tag', 'TAG').ok()

    # Add test documents
    conn = getConnectionByEnv(env)
    doc_count = 100
    for i in range(doc_count):
        tag_value = f"tag{i % 10}" # Create 10 different tag values
        conn.execute_command('HSET', f'doc:{i}',
                            'txt', f'hello world document {i}',
                            'num', i,
                            'tag', tag_value)

    # Run different aggregate queries and verify results

    # 1. Simple aggregate
    res = env.cmd('FT.AGGREGATE', 'idx', '*')
    # Check exact structure - 100 empty arrays after the counter
    env.assertEqual(len(res), 101, message=f"Simple aggregate with {io_threads} IO threads")
    env.assertEqual(res[1:], [[] for _ in range(100)], message=f"Simple aggregate structure with {io_threads} IO threads")

    # 2. Aggregate with LOAD
    res = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@num', 'LIMIT', 0, 10)
    # Check exact number of results and structure
    env.assertEqual(len(res), 11, message=f"Aggregate with LOAD with {io_threads} IO threads")
    # Verify each result has the correct format ['num', value]
    for i in range(1, len(res)):
        env.assertEqual(len(res[i]), 2, message=f"Result format with {io_threads} IO threads")
        env.assertEqual(res[i][0], 'num', message=f"Field name with {io_threads} IO threads")

    # 3. Aggregate with GROUPBY
    res = env.cmd('FT.AGGREGATE', 'idx', '*',
                 'GROUPBY', 1, '@tag',
                 'REDUCE', 'COUNT', 0, 'AS', 'count')
    # Check exact number of groups and their structure
    env.assertEqual(len(res), 11, message=f"Aggregate with GROUPBY with {io_threads} IO threads")
    for i in range(1, len(res)):
        env.assertEqual(len(res[i]), 4, message=f"Group format with {io_threads} IO threads")
        env.assertEqual(res[i][0], 'tag', message=f"Group field name with {io_threads} IO threads")
        env.assertEqual(res[i][2], 'count', message=f"Count field name with {io_threads} IO threads")
        env.assertEqual(res[i][3], '10', message=f"Group count with {io_threads} IO threads")

    # 4. Aggregate with SORTBY
    res = env.cmd('FT.AGGREGATE', 'idx', '*',
                 'SORTBY', 2, '@num', 'DESC',
                 'LIMIT', 0, 5)
    # Check exact number of results and descending order
    env.assertEqual(len(res), 6, message=f"Aggregate with SORTBY with {io_threads} IO threads")
    # Verify descending order of results
    expected_values = ['99', '98', '97', '96', '95']
    for i in range(5):
        env.assertEqual(res[i+1][1], expected_values[i],
                       message=f"Sort order at position {i} with {io_threads} IO threads")

    # 5. Aggregate with APPLY
    res = env.cmd('FT.AGGREGATE', 'idx', '*',
                 'LOAD', 1, '@num',
                 'APPLY', '@num * 2', 'AS', 'doubled',
                 'LIMIT', 0, 5)
    # Check exact structure and calculated values
    env.assertEqual(len(res), 6, message=f"Aggregate with APPLY with {io_threads} IO threads")
    for i in range(1, len(res)):
        env.assertEqual(len(res[i]), 4, message=f"Result format with {io_threads} IO threads")
        env.assertEqual(res[i][0], 'num', message=f"First field name with {io_threads} IO threads")
        env.assertEqual(res[i][2], 'doubled', message=f"Second field name with {io_threads} IO threads")
        # Verify doubled value is correct
        num_val = int(res[i][1])
        doubled_val = int(res[i][3])
        env.assertEqual(doubled_val, num_val * 2,
                       message=f"APPLY calculation for {num_val} with {io_threads} IO threads")

    # 6. Aggregate with FILTER
    res = env.cmd('FT.AGGREGATE', 'idx', '*',
                 'LOAD', 1, '@num',
                 'FILTER', '@num > 90')
    # Check exact number of results (9 documents with num > 90)
    env.assertEqual(len(res), 10, message=f"Aggregate with FILTER with {io_threads} IO threads")
    # Verify all values are > 90
    for i in range(1, len(res)):
        env.assertGreater(int(res[i][1]), 90,
                         message=f"Filter condition with {io_threads} IO threads")

    # 7. Complex aggregate with multiple operations
    res = env.cmd('FT.AGGREGATE', 'idx', '*',
                 'GROUPBY', 1, '@tag',
                 'REDUCE', 'AVG', 1, '@num', 'AS', 'avg_num',
                 'REDUCE', 'COUNT', 0, 'AS', 'count',
                 'SORTBY', 2, '@avg_num', 'DESC')
    # Check exact number of groups and descending order of avg_num
    env.assertEqual(len(res), 11, message=f"Complex aggregate with {io_threads} IO threads")
    # Verify descending order of avg_num
    for i in range(1, len(res)-1):
        current_avg = float(res[i][5])  # Fixed: value is at index 5, not 4
        next_avg = float(res[i+1][5])   # Fixed: value is at index 5, not 4
        env.assertGreaterEqual(current_avg, next_avg,
                              message=f"Sort order of avg_num with {io_threads} IO threads")

    # Clean up for next iteration
    env.cmd('FLUSHALL')
    env.flush()
    env.stop()

@skip(cluster=False)
def test_ft_aggregate_with_coord_1_io_thread():
    _test_ft_aggregate_with_io_threads(1)

@skip(cluster=False)
def test_ft_aggregate_with_coord_5_io_threads():
    _test_ft_aggregate_with_io_threads(5)

@skip(cluster=False)
def test_ft_aggregate_with_coord_10_io_threads():
    _test_ft_aggregate_with_io_threads(10)

@skip(cluster=False)
def test_ft_aggregate_with_coord_20_io_threads():
    _test_ft_aggregate_with_io_threads(20)
