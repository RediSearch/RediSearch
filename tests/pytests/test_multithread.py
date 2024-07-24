# -*- coding: utf-8 -*-
from common import *

def initEnv(moduleArgs: str = 'WORKERS 1'):
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

    additional_params = ['EF_CONSTRUCTION', n_vectors, 'EF_RUNTIME', n_vectors] if algo == 'HNSW' else []
    # Load random vectors into redis, save the first one to use as query vector later on. We set EF_C and
    # EF_R to n_vectors to ensure that all vectors would be reachable in HNSW and avoid flakiness in search.
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'vector', 'VECTOR', algo, str(6+len(additional_params)),
                'TYPE', data_type, 'DIM', dim, 'DISTANCE_METRIC', 'L2', *additional_params).ok()
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
                                    '__vector_score', 'RETURN', 1, '__vector_score', 'LIMIT', 0, k,
                                    'PARAMS', 4, 'K', k, 'vec_param', query_vec.tobytes())
        env.assertEqual(res, res_before)

# Generate test functions for each combination of algorithm and data type
func_gen = lambda al, dt, tn: lambda: do_burst_threads_sanity(al, dt, tn)
for algo in VECSIM_ALGOS:
    for data_type in VECSIM_DATA_TYPES:
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

    # Get the updated numer of local vectors after the update, and validate that all of them are in the frontend
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

        # Invoke GC, so we clean zombies for which all their repair jobs are done.
        forceInvokeGC(env)

        # Number of zombies should decrease from one iteration to another.
        env.assertEqual(run_command_on_all_shards(env, *[debug_cmd(), 'WORKERS', 'PAUSE']), ['OK']*n_shards)
        debug_info = get_vecsim_debug_dict(env, 'idx', 'vector')

        local_marked_deleted_vectors_new = to_dict(debug_info['BACKEND_INDEX'])['NUMBER_OF_MARKED_DELETED']
        env.assertLessEqual(local_marked_deleted_vectors_new, local_marked_deleted_vectors)
        local_marked_deleted_vectors = local_marked_deleted_vectors_new

    # Eventually, all updated vectors should be in the backend index, and all zombies should be removed.
    env.assertEqual(run_command_on_all_shards(env, *[debug_cmd(), 'WORKERS', 'RESUME']), ['OK']*n_shards)
    env.assertEqual(run_command_on_all_shards(env, *[debug_cmd(), 'WORKERS', 'DRAIN']), ['OK']*n_shards)

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

@skip(cluster=False, noWorkers=True)
def test_change_num_connections(env: Env):

    # Validate the default values
    env.expect(config_cmd(), 'GET', 'WORKERS').equal([['WORKERS', '0']])
    env.expect(config_cmd(), 'GET', 'CONN_PER_SHARD').equal([['CONN_PER_SHARD', '0']])

    # The logic of the number of connections is as follows:
    # - If `CONN_PER_SHARD` is not 0, the number of connections is `CONN_PER_SHARD`
    # - If `CONN_PER_SHARD` is 0, the number of connections is `WORKERS` + 1

    # Helper that will return the expected output structure.
    # In this test we don't care about the actual values, so we use the ANY matcher.
    # Example of the expected output (for 3 shards and 2 connections):
    # ['127.0.0.1:6379', ['Connected', 'Connected'],
    #  '127.0.0.1:6381', ['Connected', 'Connecting'],
    #  '127.0.0.1:6383', ['Connected', 'Connected']]
    def expected(conns):
        return [
            ANY,          # The shard id (host:port)
            [ANY] * conns # The connections states
        ] * env.shardsCount

    # By default, the number of connections is 1
    env.expect(debug_cmd(), 'SHARD_CONNECTION_STATES').equal(expected(1))

    # Increase the number of worker threads to 6
    env.expect(config_cmd(), 'SET', 'WORKERS', '6').ok()
    # The number of connections should be 7
    env.expect(debug_cmd(), 'SHARD_CONNECTION_STATES').equal(expected(7))

    # Set the number of connections to 4
    env.expect(config_cmd(), 'SET', 'CONN_PER_SHARD', '4').ok()
    # The number of connections should be 4
    env.expect(debug_cmd(), 'SHARD_CONNECTION_STATES').equal(expected(4))

    # Decrease the number of worker threads to 5
    env.expect(config_cmd(), 'SET', 'WORKERS', '5').ok()
    # The number of connections should remain 4
    env.expect(debug_cmd(), 'SHARD_CONNECTION_STATES').equal(expected(4))

    # Set the number of connections to 0
    env.expect(config_cmd(), 'SET', 'CONN_PER_SHARD', '0').ok()
    # The number of connections should be 6 (5 worker threads + 1)
    env.expect(debug_cmd(), 'SHARD_CONNECTION_STATES').equal(expected(6))

    # Set back the number of worker threads to 0
    env.expect(config_cmd(), 'SET', 'WORKERS', '0').ok()
    # The number of connections should be 1 again
    env.expect(debug_cmd(), 'SHARD_CONNECTION_STATES').equal(expected(1))

def test_change_workers_number():

    def check_threads(expected_num_threads_alive, expected_n_threads):
        env.assertEqual(getWorkersThpoolStats(env)['numThreadsAlive'], expected_num_threads_alive)
        env.assertEqual(getWorkersThpoolNumThreads(env), expected_n_threads)
    # On start up the threadpool is not initialized. We can change the value of requested threads
    # without actually creating the threads.
    env = initEnv(moduleArgs='WORKERS 1')
    check_threads(expected_num_threads_alive=0, expected_n_threads=1)
    # Increase number of threads
    env.expect(config_cmd(), 'SET', 'WORKERS', '2').ok()
    check_threads(expected_num_threads_alive=0, expected_n_threads=2)
    # Decrease number of threads
    env.expect(config_cmd(), 'SET', 'WORKERS', '1').ok()
    check_threads(expected_num_threads_alive=0, expected_n_threads=1)
    # Set it to 0
    env.expect(config_cmd(), 'SET', 'WORKERS', '0').ok()
    check_threads(expected_num_threads_alive=0, expected_n_threads=0)

    # Query should not be executed by the threadpool
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'text').ok()
    env.expect('ft.search', 'idx', '*').equal([0])
    check_threads(expected_num_threads_alive=0, expected_n_threads=0)
    env.assertEqual(getWorkersThpoolStats(env)['totalJobsDone'], 0)

    # Enable threadpool
    env.expect(config_cmd(), 'SET', 'WORKERS', '1').ok()
    check_threads(expected_num_threads_alive=0, expected_n_threads=1)

    # Trigger thpool initialization.
    env.expect('ft.search', 'idx', '*').equal([0])
    check_threads(expected_num_threads_alive=1, expected_n_threads=1)
    # wait for the job to finish
    env.expect(debug_cmd(), 'WORKERS', 'DRAIN').ok()

    # Query should be executed by the threadpool
    env.assertEqual(getWorkersThpoolStats(env)['totalJobsDone'], 1)

    # Add threads to a running pool
    env.expect(config_cmd(), 'SET', 'WORKERS', '2').ok()
    check_threads(expected_num_threads_alive=2, expected_n_threads=2)
    # Remove threads from a running pool
    env.expect(config_cmd(), 'SET', 'WORKERS', '1').ok()
    check_threads(expected_num_threads_alive=1, expected_n_threads=1)

    # Terminate all threads
    env.expect(config_cmd(), 'SET', 'WORKERS', '0').ok()
    env.assertEqual(getWorkersThpoolNumThreads(env), 0)

    # Query should not be executed by the threadpool
    env.expect('ft.search', 'idx', '*').equal([0])
    env.assertEqual(getWorkersThpoolStats(env)['totalJobsDone'], 1)
