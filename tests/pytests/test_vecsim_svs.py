from RLTest import Env
import distro
from includes import *
import threading
import random

from vecsim_utils import *
from common import (
    getConnectionByEnv,
    skip,
    assertInfoField,
    index_info,
    to_dict,
    get_redis_memory_in_mb,
    config_cmd,
    runDebugQueryCommandPauseBeforeRPAfterN,
    getIsRPPaused,
    setPauseRPResume,
    forceInvokeGC,
    waitForIndex,
    SkipTest,
    call_and_store,
    getWorkersThpoolStats
)

VECSIM_SVS_DATA_TYPES = ['FLOAT32', 'FLOAT16']
SVS_COMPRESSION_TYPES = ['NO_COMPRESSION', 'LVQ8', 'LVQ4', 'LVQ4x4', 'LVQ4x8', 'LeanVec4x8', 'LeanVec8x8']

    # Simple platform-agnostic check for Intel CPU.
def is_intel_opt_supported():
    import platform
    def is_linux_and_intel_cpu():
        if platform.system() != 'Linux' or platform.machine() != 'x86_64':
            return False
        # Check CPU vendor in /proc/cpuinfo on Linux
        try:
            with open('/proc/cpuinfo', 'r') as f:
                cpuinfo = f.read().lower()
                if 'vendor_id' in cpuinfo and 'genuineintel' in cpuinfo:
                    return True
        except (IOError, FileNotFoundError):
            return False

    is_alpine = distro.name().lower() == 'alpine linux'

    return is_linux_and_intel_cpu() and not is_alpine

def is_intel_opt_enabled():
    return is_intel_opt_supported() and BUILD_INTEL_SVS_OPT

'''
This test reproduce the crash described in MOD-10771 and MOD-12011,
where SVS crashes during topk search if CONSTRUCTION_WINDOW_SIZE given in creation is small.
'''
def test_small_window_size():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    dim = 2
    # The vectors will be moved from the flat buffer to svs after 1024 * 10 vectors.
    svs_transfer_th = 1024 * 10
    keep_count = 10
    num_vectors = svs_transfer_th
    field_name = 'v_SVS_VAMANA'
    for data_type in VECSIM_SVS_DATA_TYPES:
        query_vec = create_random_np_array_typed(dim, data_type)
        for compression in [[], ["COMPRESSION", "LVQ8"]]:
            params = ['TYPE', data_type, 'DIM', dim, 'DISTANCE_METRIC', 'L2', "CONSTRUCTION_WINDOW_SIZE", 10, *compression]
            conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', field_name, 'VECTOR', 'SVS-VAMANA', len(params), *params)

            # Add enough vector to trigger transfer to svs
            vectors = []
            for i in range(num_vectors):
                vector = create_random_np_array_typed(dim, data_type)
                vectors.append(vector)
                conn.execute_command('HSET', f'doc_{i}', field_name, vector.tobytes())

            # Create unique filename for this iteration
            compression_str = "no_compression" if not compression else "_".join(compression)
            filename = f"vectors_{data_type}_{compression_str}.txt"
            with open(filename, 'w') as f:
                f.write(f"Data Type: {data_type}, Compression: {compression}, Dim: {dim}, Count: {num_vectors}\n")
                f.write(str([vector.tolist() for vector in vectors]))
            # try:
            #     conn.execute_command('FT.SEARCH', 'idx', f'*=>[KNN {keep_count} @{field_name} $vec_param]', 'PARAMS', 2, 'vec_param', query_vec.tobytes(), 'RETURN', 1, f'__{field_name}_score')
            # except Exception as e:
            #     env.assertTrue(False, message=f"compression: {compression} data_type: {data_type}. Search failed with exception: {e}")
            # delete most
            for i in range(num_vectors - keep_count):
                conn.execute_command('DEL', f'doc_{i}')

            # run topk for remaining
            # Before fixing MOD-10771, search crashed
            try:
                conn.execute_command('FT.SEARCH', 'idx', f'*=>[KNN {keep_count} @{field_name} $vec_param]', 'PARAMS', 2, 'vec_param', query_vec.tobytes(), 'RETURN', 1, f'__{field_name}_score')
            except Exception as e:
                env.assertTrue(False, message=f"compression: {compression} data_type: {data_type}. Search failed with exception: {e}")
                return
            conn.execute_command('FLUSHALL')

def test_rdb_load_trained_svs_vamana():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')

    training_threshold = DEFAULT_BLOCK_SIZE
    num_docs = int(training_threshold * 1.1 * env.shardsCount) # To ensure all shards' svs index is initialized.
    extend_params = ['COMPRESSION', 'LVQ8', 'TRAINING_THRESHOLD', training_threshold]
    dim = 2
    index_name=DEFAULT_INDEX_NAME
    field_name=DEFAULT_FIELD_NAME
    data_type = random.choice(VECSIM_SVS_DATA_TYPES)
    env.debugPrint(f"data_type for test {env.testName}: {data_type}", force=True)

    create_vector_index(env, dim, datatype=data_type, alg='SVS-VAMANA', additional_vec_params=extend_params)

    frontend_index_info = get_tiered_frontend_debug_info(env, index_name, field_name)
    env.assertEqual(frontend_index_info['INDEX_SIZE'], 0)

    # Insert vectors (not triggering training yet)
    populate_with_vectors(env, num_docs=training_threshold - 1, dim=dim, datatype=data_type)

    # Expect all vectors to be in the flat buffer
    for i, con in enumerate(env.getOSSMasterNodesConnectionList()):
        shard_keys = con.execute_command('DBSIZE')
        env.assertEqual(get_tiered_frontend_debug_info(con, index_name, field_name)['INDEX_SIZE'], shard_keys, message=f"datatype: {data_type}, shard_id: {i}")
        env.assertEqual(get_tiered_backend_debug_info(con, index_name, field_name)['INDEX_SIZE'], 0, message=f"datatype: {data_type}, shard_id: {i}")

    # Insert more vectors to trigger training
    populate_with_vectors(env, num_docs=num_docs - (training_threshold - 1), dim=dim, datatype=data_type, initial_doc_id=training_threshold)

    for i, con in enumerate(env.getOSSMasterNodesConnectionList()):
        shard_keys = con.execute_command('DBSIZE')
        # We are in writeInPlace mode, so once the index is trained, all vectors are transferred to the backend index in place.
        env.assertEqual(get_tiered_frontend_debug_info(con, index_name, field_name)['INDEX_SIZE'], 0, message=f"shard_id: {i}, datatype: {data_type}, shard_keys: {shard_keys}, after adding {num_docs} vectors")
        env.assertEqual(get_tiered_backend_debug_info(con, index_name, field_name)['INDEX_SIZE'], shard_keys, message=f"shard_id: {i}, datatype: {data_type}, after adding {num_docs} vectors")
        env.assertEqual(get_tiered_debug_info(con, index_name, field_name)['INDEX_SIZE'], shard_keys, message=f"shard_id: {i}, datatype: {data_type}, after adding {num_docs} vectors")

    # reload rdb
    for _ in env.reloadingIterator():
        # rdb load occurs in a multi threaded environment, where the vectors are transferred to svs in batches of update_threshold vectors.
        wait_for_background_indexing(env, index_name, field_name, message=f"datatype: {data_type} after rdb load")
        for i, con in enumerate(env.getOSSMasterNodesConnectionList()):
            shard_keys = con.execute_command('DBSIZE')
            # We passed the training threshold, so we always have less than training_threshold ( = update_threshold)
            # vectors in the frontend index.
            env.assertLessEqual(get_tiered_frontend_debug_info(con, index_name, field_name)['INDEX_SIZE'], training_threshold, message=f"shard_id: {i}, datatype: {data_type}, shard_keys: {shard_keys}, after rdb reload of {num_docs} vectors")
            # We passed the training threshold, so we always have at least training_threshold vectors in the backend index.
            env.assertGreaterEqual(get_tiered_backend_debug_info(con, index_name, field_name)['INDEX_SIZE'], training_threshold, message=f"shard_id: {i}, datatype: {data_type}, after rdb reload of {num_docs} vectors")
            env.assertEqual(get_tiered_debug_info(con, index_name, field_name)['INDEX_SIZE'], shard_keys, message=f"shard_id: {i}, datatype: {data_type}, after rdb reload of {num_docs} vectors")

@skip(cluster=True)
def test_svs_vamana_info():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    dim = 2
    data_type = random.choice(VECSIM_SVS_DATA_TYPES)

    # Create SVS VAMANA index with all compression flavors (except for global SQ8).
    compression_types = SVS_COMPRESSION_TYPES if is_intel_opt_enabled() and EXTENDED_PYTESTS else ['NO_COMPRESSION', 'LVQ8', 'LeanVec4x8']
    for compression_type in compression_types:
        cmd_params = ['TYPE', data_type,
                    'DIM', dim, 'DISTANCE_METRIC', 'L2']
        if compression_type != 'NO_COMPRESSION':
            cmd_params.extend(['COMPRESSION', compression_type])
            if compression_type == 'LeanVec4x8':
                cmd_params.extend(['REDUCE', dim // 2])
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', len(cmd_params), *cmd_params).ok()

        # Validate that ft.info returns the default params for SVS VAMANA, along with compression
        # compression in runtime is LVQ8 if we are running on intel optimizations are enabled and GlobalSQ otherwise.
        compression_runtime = compression_type if is_intel_opt_enabled() or compression_type == 'NO_COMPRESSION' else 'GlobalSQ8'
        expected_info = [['identifier', 'v', 'attribute', 'v', 'type', 'VECTOR', 'algorithm', 'SVS-VAMANA',
                          'data_type', data_type, 'dim', dim, 'distance_metric', 'L2', 'graph_max_degree', 32,
                          'construction_window_size', 200, 'compression', compression_runtime]]
        if compression_type != 'NO_COMPRESSION':
            expected_info[0].extend(['training_threshold', 10240])
        if compression_runtime == 'LeanVec4x8' or compression_runtime == 'LeanVec8x8':
            expected_info[0].extend(['reduced_dim', dim // 2])
        assertInfoField(env, 'idx', 'attributes',
                        expected_info)
        env.expect('FT.DROPINDEX', 'idx').ok()

def test_vamana_debug_info_vs_info():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')

    extend_params = [None,
                     # non default params
                     ['COMPRESSION', 'LVQ8', 'GRAPH_MAX_DEGREE', 1, 'CONSTRUCTION_WINDOW_SIZE', 1, 'TRAINING_THRESHOLD', DEFAULT_BLOCK_SIZE * 2],
                     ['COMPRESSION', 'LVQ8'], ['COMPRESSION', 'LeanVec4x8']]
    dim = 2
    index_name = DEFAULT_INDEX_NAME
    field_name = DEFAULT_FIELD_NAME

    def compare_debug_info_to_ft_info(index_debug_info: dict, vec_field_info: dict, extend_params, message):
        backend_debug_info = to_dict(index_debug_info['BACKEND_INDEX'])
        env.assertEqual(backend_debug_info['DIMENSION'], vec_field_info['dim'], message=message)
        env.assertEqual(backend_debug_info['TYPE'], vec_field_info['data_type'], message=message)
        env.assertEqual(backend_debug_info['METRIC'], vec_field_info['distance_metric'], message=message)
        env.assertEqual(backend_debug_info['GRAPH_MAX_DEGREE'], vec_field_info['graph_max_degree'], message=message)
        env.assertEqual(backend_debug_info['CONSTRUCTION_WINDOW_SIZE'], vec_field_info['construction_window_size'], message=message)
        env.assertEqual(index_debug_info['TIERED_SVS_UPDATE_THRESHOLD'], DEFAULT_BLOCK_SIZE, message=message)

        if extend_params:
            env.assertEqual(index_debug_info['TIERED_SVS_TRAINING_THRESHOLD'], vec_field_info['training_threshold'], message=message)
        else:
            # for non-compressed index, first batch TH equals to subsequent batches TH.
            env.assertEqual(index_debug_info['TIERED_SVS_TRAINING_THRESHOLD'], DEFAULT_BLOCK_SIZE, message=message)
    for data_type in VECSIM_SVS_DATA_TYPES:
        for params in extend_params:
            create_vector_index(env, dim, index_name=index_name, field_name=field_name, datatype=data_type, alg='SVS-VAMANA',
                                additional_vec_params=params, message=f"datatype: {data_type}, params: {params}")
            debug_info = get_tiered_debug_info(env, index_name, field_name)
            vec_field_info = to_dict(index_info(env, index_name)['attributes'][0])

            compare_debug_info_to_ft_info(debug_info, vec_field_info,
                                          params, message=f"datatype: {data_type}, params: {params}")
            conn = getConnectionByEnv(env)
            conn.execute_command('FLUSHALL')

@skip(cluster=True)
def test_memory_info():
    env = Env(moduleArgs='DEFAULT_DIALECT 2 WORKERS 4 _FREE_RESOURCE_ON_THREAD FALSE')
    dimension = 2
    data_type = random.choice(VECSIM_SVS_DATA_TYPES)
    env.debugPrint(f"data_type for test {env.testName}: {data_type}", force=True)
    index_key = DEFAULT_INDEX_NAME
    vector_field = DEFAULT_FIELD_NAME
    training_threshold = DEFAULT_BLOCK_SIZE
    compression_params = [None, ['COMPRESSION', 'LVQ8', 'TRAINING_THRESHOLD', training_threshold]]

    cur_redisearch_memory = 0
    cur_redis_memory = get_redis_memory_in_mb(env)

    def verify_mem(message):
        nonlocal cur_redisearch_memory, cur_redis_memory
        vecsim_memory = get_vecsim_memory(env, index_key=index_key, field_name=vector_field)
        redisearch_memory = get_redisearch_vector_index_memory(env, index_key=index_key)
        redis_memory = get_redis_memory_in_mb(env)
        env.assertEqual(redisearch_memory, vecsim_memory, message=message)
        env.assertLessEqual(redisearch_memory, redis_memory, message=message)
        env.assertGreater(redis_memory, cur_redis_memory, message=message)
        env.assertGreater(vecsim_memory, cur_redisearch_memory, message=message)
        cur_redisearch_memory = vecsim_memory
        cur_redis_memory = redis_memory

    for extended_params in compression_params:
        cur_redisearch_memory = 0
        cur_redis_memory = get_redis_memory_in_mb(env)

        message_prefix = f"datatype: {data_type}, compression: {extended_params}"

        create_vector_index(env, dimension, alg='SVS-VAMANA', additional_vec_params=extended_params, message=message_prefix)

        # Add vectors, not triggering transfer to the backend index
        populate_with_vectors(env, training_threshold - 1, dimension)
        index_size = get_vecsim_index_size(env, index_key, vector_field)
        env.assertEqual(index_size, training_threshold - 1, message=message_prefix)
        env.assertEqual(get_tiered_backend_debug_info(env, index_key, vector_field)['INDEX_SIZE'], 0, message=f"{message_prefix}, total index size: {index_size}")
        verify_mem(f"{message_prefix}. after adding less than training threshold vectors")

        # Add vector to trigger svs initialization
        added_vectors = 5
        populate_with_vectors(env, num_docs=added_vectors, dim=dimension, initial_doc_id=training_threshold)
        wait_for_background_indexing(env, index_key, vector_field,
                                        f"{message_prefix}. after adding {added_vectors + training_threshold - 1} vectors, to trigger transition to the backend index")
        index_size = get_vecsim_index_size(env, index_key, vector_field)
        env.assertEqual(index_size, training_threshold - 1 + added_vectors, message=message_prefix)
        # we have at least training_threshold in the backend index
        env.assertGreaterEqual(get_tiered_backend_debug_info(env, index_key, vector_field)['INDEX_SIZE'], training_threshold, message=f"{message_prefix}, total index size: {index_size}")
        verify_mem(f"{message_prefix}. after adding more than training threshold vectors")

        env.execute_command('FLUSHALL')


func_gen = lambda tn, comp, dt, dist, wr: lambda: queries_sanity(tn, comp, dt, dist, wr)
for workers in [0, 4]:
    name_suffix = "_async" if workers else ""
    # Create SVS VAMANA index with all compression flavors
    # for non intel machines, we only test NO_COMPRESSION and any compression type (will result in GlobalSQ8)
    compression_types = SVS_COMPRESSION_TYPES if is_intel_opt_enabled() and EXTENDED_PYTESTS else ['NO_COMPRESSION', 'LVQ8']
    for compression_type in compression_types:
        for data_type in VECSIM_SVS_DATA_TYPES:
            metrics = VECSIM_DISTANCE_METRICS if EXTENDED_PYTESTS else ['IP']
            for metric in metrics:
                test_name = f"test_queries_sanity_{compression_type}_{data_type}_{metric}" + name_suffix
                globals()[test_name] = func_gen(test_name, compression_type, data_type, metric, workers)

'''
This test validates SVS-VAMANA tiered indexing across all datatype, metric, and compression combinations.
For each datatype/metric combination, it creates one index per compression type, adds vectors just below
the training threshold, then adds one more vector to trigger backend training. It verifies that all
vectors are transferred to the backend index. It then performs a KNN search using the last added vector
and verifies that vector is returned as the top result.
Distance verification is skipped since some compression types would require larger training thresholds
and vector dimension to get an exact match, making the test prohibitively slow.
'''
def queries_sanity(test_name, compression_type, data_type, metric, workers):
    env = Env(moduleArgs=f'DEFAULT_DIALECT 2 WORKERS {workers}')
    # Sanity check that the test parameters match the test name
    env.debugPrint(f"test name: {test_name}, compression_type: {compression_type}, data_type: {data_type}, metric: {metric}", force=True)
    dim = 28
    training_threshold = DEFAULT_BLOCK_SIZE
    num_docs = int(training_threshold * 1.1 * env.shardsCount) # To ensure all shards' svs index is initialized.
    score_title = f'__{DEFAULT_FIELD_NAME}_score'
    conn = getConnectionByEnv(env)


    env.debugPrint(f"Extended tests: {EXTENDED_PYTESTS}", force=True)
    index_name = f"idx"
    index_params = ['CONSTRUCTION_WINDOW_SIZE', num_docs, 'SEARCH_WINDOW_SIZE', num_docs]
    if compression_type != 'NO_COMPRESSION':
        index_params.extend(['COMPRESSION', compression_type, 'TRAINING_THRESHOLD', training_threshold])

    create_vector_index(env, dim, index_name=index_name, datatype=data_type, metric=metric, alg='SVS-VAMANA',
                        additional_vec_params=index_params, message=f"datatype: {data_type}, metric: {metric}, compression: {compression_type}")

    # add vectors with the same field name so they will be indexed in all indexes
    normalize = metric == 'IP'
    query = populate_with_vectors(env, dim=dim, num_docs=num_docs, datatype=data_type, normalize=normalize, ret_vec_offset=0)

    message = f"datatype: {data_type}, metric: {metric}, index: {index_name}"
    wait_for_background_indexing(env, index_name, DEFAULT_FIELD_NAME, message=message)
    env.assertEqual(index_info(env, index_name)['num_docs'], num_docs, message=message)
    for i, con in enumerate(env.getOSSMasterNodesConnectionList()):
        env.assertGreaterEqual(get_tiered_backend_debug_info(con, index_name, DEFAULT_FIELD_NAME)['INDEX_SIZE'], training_threshold, message=f"shard id: {i} " + message)

    message = f"datatype: {data_type}, metric: {metric}, index: {index_name}"
    knn_res = env.execute_command('FT.SEARCH', index_name, f'*=>[KNN 10 @{DEFAULT_FIELD_NAME} $vec_param]', 'PARAMS', 2, 'vec_param', query.tobytes(), 'sortby', score_title, 'RETURN', 1, score_title)
    cmd_range = f'@{DEFAULT_FIELD_NAME}:[VECTOR_RANGE 10 $b]=>{{$yield_distance_as:{score_title}}}'
    range_res = conn.execute_command('FT.SEARCH', index_name, cmd_range, 'PARAMS', 2, 'b', query.tobytes(), 'sortby', score_title, 'RETURN', 1, score_title)
    env.assertEqual(knn_res[1], f'doc{1}', message=str(knn_res) + " " + message)
    env.assertEqual(range_res[1], f'doc{1}', message=message)

def empty_index(env):
    env.execute_command(config_cmd(), 'SET', 'FORK_GC_RUN_INTERVAL', '1000000')
    dim = 2
    data_type = random.choice(VECSIM_SVS_DATA_TYPES)
    env.debugPrint(f"data_type for test {env.testName}: {data_type}", force=True)
    num_docs = int(DEFAULT_BLOCK_SIZE * 1.1 * env.shardsCount) # To ensure all shards' svs index is initialized.
    training_threshold = DEFAULT_BLOCK_SIZE
    compression_params = [None, ['COMPRESSION', 'LVQ8', 'TRAINING_THRESHOLD', training_threshold]]
    score_title = f'__{DEFAULT_FIELD_NAME}_score'
    k = 10
    query = create_random_np_array_typed(dim, data_type)
    query_cmd = ['FT.SEARCH', DEFAULT_INDEX_NAME, f'*=>[KNN {k} @v $vec_param]', 'PARAMS', 2, 'vec_param', query.tobytes(), 'RETURN', 1, score_title]

    conn = getConnectionByEnv(env)
    for compression_params in compression_params:
        message_prefix = f"compression_params: {compression_params}"
        create_vector_index(env, dim, index_name=DEFAULT_INDEX_NAME, datatype=data_type, alg='SVS-VAMANA', additional_vec_params=compression_params)

        # Scenario 1: Query uninitialized index
        res = env.execute_command(*query_cmd)
        env.assertEqual(res, [0], message=f"{message_prefix}")

        # Scenario 2: adding less than training threshold vectors (index is created, but no vectors in svs yet)
        populate_with_vectors(env, dim=dim, num_docs=training_threshold - 1, datatype=data_type)
        tiered_backend_debug_info = [get_tiered_backend_debug_info(con, DEFAULT_INDEX_NAME, DEFAULT_FIELD_NAME) for con in env.getOSSMasterNodesConnectionList()]
        for debug_info in tiered_backend_debug_info:
            env.assertEqual(debug_info['INDEX_SIZE'], 0, message=f"{message_prefix}")
        res = env.execute_command(*query_cmd)
        env.assertEqual(res[0], k, message=f"{message_prefix}")

        # Scenario 3: Querying svs index after it was initialized and emptied
        populate_with_vectors(env, dim=dim, num_docs=num_docs - (training_threshold - 1), datatype=data_type, initial_doc_id=training_threshold)
        expected_index_size = num_docs
        wait_for_background_indexing(env, DEFAULT_INDEX_NAME, DEFAULT_FIELD_NAME, message=message_prefix)
        tiered_backend_debug_info = [get_tiered_backend_debug_info(con, DEFAULT_INDEX_NAME, DEFAULT_FIELD_NAME) for con in env.getOSSMasterNodesConnectionList()]
        tiered_debug_info = [get_tiered_debug_info(con, DEFAULT_INDEX_NAME, DEFAULT_FIELD_NAME) for con in env.getOSSMasterNodesConnectionList()]
        for debug_info in tiered_debug_info:
            env.assertGreaterEqual(debug_info['INDEX_SIZE'], training_threshold, message=f"{message_prefix}")
        env.assertEqual(index_info(env, DEFAULT_INDEX_NAME)['num_docs'], num_docs, message=f"{message_prefix}")

        for i in range(expected_index_size):
            conn.execute_command('DEL', f'{DEFAULT_DOC_NAME_PREFIX}{i+1}')
        env.assertEqual(index_info(env, DEFAULT_INDEX_NAME)['num_docs'], 0, message=f"{message_prefix}")
        tiered_debug_info = [get_tiered_debug_info(con, DEFAULT_INDEX_NAME, DEFAULT_FIELD_NAME) for con in env.getOSSMasterNodesConnectionList()]
        for debug_info in tiered_debug_info:
            env.assertGreaterEqual(debug_info['INDEX_SIZE'], 0, message=f"{message_prefix}")

        res = env.execute_command(*query_cmd)
        env.assertEqual(res, [0], message=f"{message_prefix}")

        conn.execute_command('FLUSHALL')

def test_empty_index():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    empty_index(env)

def test_empty_index_async():
    env = Env(moduleArgs='DEFAULT_DIALECT 2 WORKERS 4')
    empty_index(env)

def change_threads(initial_workers, final_workers):
    if CLUSTER:
        raise SkipTest()
    env = Env(moduleArgs=f'DEFAULT_DIALECT 2 WORKERS {initial_workers}')
    message_prefix = f"initial_workers: {initial_workers}, final_workers: {final_workers}"
    env.assertEqual(int(env.execute_command(config_cmd(), 'GET', 'WORKERS')[0][1]), initial_workers, message=message_prefix)
    training_threshold = DEFAULT_BLOCK_SIZE
    update_threshold = training_threshold
    dim = 2
    prev_last_reserved_num_threads = 0
    def verify_num_threads(expected_num_threads, expected_reserved_num_threads, message):
        nonlocal prev_last_reserved_num_threads
        # zero workers is also considered as 1 thread
        expected_num_threads = 1 if expected_num_threads == 0 else expected_num_threads
        for i, con in enumerate(env.getOSSMasterNodesConnectionList()):
            expected_reserved_num_threads = 1 if expected_reserved_num_threads == 0 else expected_reserved_num_threads
            tiered_debug_info = get_tiered_backend_debug_info(con, DEFAULT_INDEX_NAME, DEFAULT_FIELD_NAME)
            num_threads = tiered_debug_info['NUM_THREADS']
            last_reserved_num_threads = tiered_debug_info['LAST_RESERVED_NUM_THREADS']
            env.assertEqual(num_threads, expected_num_threads, message=message)
            # 0 < last_reserved_num_threads <= expected_reserved_num_threads
            env.assertGreater(last_reserved_num_threads, 0, message=message)
            env.assertLessEqual(last_reserved_num_threads, expected_reserved_num_threads, message=message)
            prev_last_reserved_num_threads = last_reserved_num_threads

    set_up_database_with_vectors(env, dim, num_docs=training_threshold, alg='SVS-VAMANA')
    wait_for_background_indexing(env, DEFAULT_INDEX_NAME, DEFAULT_FIELD_NAME, message=f"{message_prefix}, add more vectors to trigger training")
    verify_num_threads(initial_workers, initial_workers, message=f"{message_prefix}, after training trigger")

    env.execute_command(config_cmd(), 'SET', 'WORKERS', final_workers)
    env.assertEqual(int(env.execute_command(config_cmd(), 'GET', 'WORKERS')[0][1]), final_workers, message=message_prefix)

    # TODO: new num_threads should be `final_workers` once VecSim gets notified of RediSearch thread pool changes
    # last_reserved_num_threads should remain the same as we didn't do any operation
    verify_num_threads(initial_workers, prev_last_reserved_num_threads, message=f"{message_prefix}, after changing workers to {final_workers}")

    # Add more vectors to trigger background indexing
    populate_with_vectors(env, dim=dim, num_docs=update_threshold, datatype='FLOAT32', initial_doc_id=training_threshold + 1)
    wait_for_background_indexing(env, DEFAULT_INDEX_NAME, DEFAULT_FIELD_NAME, message=f"{message_prefix}, add more vectors to trigger update")
    # Since VecSim doesn't get notified when RediSearch worker count changes, when RediSearch worker count changes
    # svs index continues to request the original number of threads during operations
    #
    # The actual thread reservation will be limited by the current RediSearch worker count
    # - If workers decreased: requested > actual_workers
    # - If workers increased: requested < actual_workers
    #
    # TODO: Expected behavior after VecSim gets worker change notifications:
    # - num_threads should reflect the current RediSearch worker count
    # - last_reserved_num_threads should match the actual threads used in operations
    expected_last_reserved_num_threads = min(final_workers, initial_workers)
    verify_num_threads(initial_workers, expected_last_reserved_num_threads,
                    message=f"{message_prefix}, after changing workers to {final_workers} and triggering another update job")

def test_change_threads_turn_on():
    change_threads(0, 4)
def test_change_threads_decrease():
    change_threads(5, 3)
def test_change_threads_turn_off():
    change_threads(4, 0)
def test_change_threads_increase():
    change_threads(3, 5)

@skip(cluster=True)
def test_drop_index_memory():
    env = Env(moduleArgs='DEFAULT_DIALECT 2 _FREE_RESOURCE_ON_THREAD FALSE')
    num_docs = DEFAULT_BLOCK_SIZE # enough to trigger svs initialization
    dim = 2

    # add vectors and measure memory
    populate_with_vectors(env, dim=dim, num_docs=num_docs, datatype='FLOAT32')
    no_index_proc_memory = get_redis_memory_in_mb(env)

    # create index and measure memory. Expect it to increase by at least the size in bytes of vectors
    create_vector_index(env, dim, alg='SVS-vamana')
    waitForIndex(env, DEFAULT_INDEX_NAME)
    env.assertEqual(get_tiered_debug_info(env, DEFAULT_INDEX_NAME, DEFAULT_FIELD_NAME)['INDEX_SIZE'], num_docs)
    env.assertGreater(get_tiered_backend_debug_info(env, DEFAULT_INDEX_NAME, DEFAULT_FIELD_NAME)['INDEX_SIZE'], 0)

    proc_memory = get_redis_memory_in_mb(env)
    vectors_mem_mb = (num_docs * dim * 4) / (1024 * 1024)
    env.assertGreater(proc_memory, no_index_proc_memory + vectors_mem_mb,
                    message=f"no_index_proc_memory: {no_index_proc_memory}, proc_memory: {proc_memory}, vectors_mem_mb: {vectors_mem_mb}")

    # drop index and measure memory. Expect it to decrease by at least the size in bytes of vectors
    env.expect('FT.DROPINDEX', DEFAULT_INDEX_NAME).ok()
    memory_after_drop = get_redis_memory_in_mb(env)
    env.assertGreaterEqual(proc_memory - memory_after_drop, vectors_mem_mb, message=f"proc_memory: {proc_memory}, memory_after_drop: {memory_after_drop}, vectors_mem_mb: {vectors_mem_mb}")

    # No operations on a dropped index are allowed
    env.expect('FT.INFO', DEFAULT_INDEX_NAME).error().contains(f"no such index")
    query = create_random_np_array_typed(dim, 'FLOAT32')
    env.expect('FT.SEARCH', 'idx', f'*=>[KNN 1 @{DEFAULT_FIELD_NAME} $vec_param]', 'PARAMS', 2, 'vec_param',
               query.tobytes(), 'NOCONTENT').error().contains(f"No such index")

@skip(cluster=True)
def test_drop_index_during_query():
    env = Env(moduleArgs='DEFAULT_DIALECT 2 WORKERS 1')
    dim = 2
    data_type = 'FLOAT32'
    training_threshold = DEFAULT_BLOCK_SIZE
    k = 10
    set_up_database_with_vectors(env, dim, num_docs=training_threshold, index_name=DEFAULT_INDEX_NAME, datatype=data_type, alg='SVS-VAMANA')
    wait_for_background_indexing(env, DEFAULT_INDEX_NAME, DEFAULT_FIELD_NAME)
    env.assertEqual(index_info(env, DEFAULT_INDEX_NAME)['num_docs'], training_threshold)

    query = create_random_np_array_typed(dim, 'FLOAT32')
    query_cmd = ['FT.SEARCH', DEFAULT_INDEX_NAME, f'*=>[KNN {k} @{DEFAULT_FIELD_NAME} $vec_param]', 'PARAMS', 2, 'vec_param', query.tobytes(), 'NOCONTENT']

    query_result = []
    # Build threads
    t_query = threading.Thread(
        target=call_and_store,
        args=(runDebugQueryCommandPauseBeforeRPAfterN,
              (env, query_cmd, 'Metrics Applier', 2),
              query_result),
        daemon=True
    )

    # Start the query and the pause-check in parallel
    t_query.start()

    while getIsRPPaused(env) != 1:
        time.sleep(0.1)

    # drop the index while query is running
    env.expect('FT.DROPINDEX', DEFAULT_INDEX_NAME).ok()

    env.expect('FT.INFO', DEFAULT_INDEX_NAME).error().contains(f"no such index")
    env.expect(*query_cmd).error().contains(f"No such index")
    # Resume the query
    setPauseRPResume(env)
    t_query.join()
    env.assertEqual(query_result[0][0], k)

    env.expect('FT.INFO', DEFAULT_INDEX_NAME).error().contains(f"no such index")
    env.expect(*query_cmd).error().contains(f"No such index")

@skip(cluster=True)
def test_gc():
    num_workers = 2
    env = Env(moduleArgs=f'DEFAULT_DIALECT 2 FORK_GC_RUN_INTERVAL 1000000 FORK_GC_CLEAN_THRESHOLD 0 WORKERS {num_workers}'
                         f' _FREE_RESOURCE_ON_THREAD FALSE')
    dim = 28
    data_type = 'FLOAT32'
    training_threshold = DEFAULT_BLOCK_SIZE
    index_size = DEFAULT_BLOCK_SIZE
    compression_types = ['NO_COMPRESSION', 'LVQ8']
    if is_intel_opt_enabled():
        compression_types.append('LeanVec4x8')

    for compression_type in compression_types:
        compression_params = None
        if compression_type != 'NO_COMPRESSION':
            compression_params = ['COMPRESSION', compression_type, 'TRAINING_THRESHOLD', training_threshold]
        message_prefix = f"compression_params: {compression_params}"
        set_up_database_with_vectors(env, dim, num_docs=index_size, index_name=DEFAULT_INDEX_NAME, datatype=data_type, alg='SVS-VAMANA', additional_vec_params=compression_params)
        wait_for_background_indexing(env, DEFAULT_INDEX_NAME, DEFAULT_FIELD_NAME, message=message_prefix)

        tiered_backend_debug_info = get_tiered_backend_debug_info(env, DEFAULT_INDEX_NAME, DEFAULT_FIELD_NAME)

        env.assertGreaterEqual(tiered_backend_debug_info['INDEX_SIZE'], DEFAULT_BLOCK_SIZE, message=f"{message_prefix}")

        memory_before_deletion = get_vecsim_memory(env, DEFAULT_INDEX_NAME, DEFAULT_FIELD_NAME)
        size_before = tiered_backend_debug_info['INDEX_SIZE']
        label_count_before = tiered_backend_debug_info['INDEX_LABEL_COUNT']

        # Phase 1: Delete some vectors
        vecs_to_delete = 1000
        for i in range (vecs_to_delete):
            env.execute_command('DEL', f'{DEFAULT_DOC_NAME_PREFIX}{i + 1}')

        # Verify that the number of marked deleted vectors is as expected
        tiered_backend_debug_info = get_tiered_backend_debug_info(env, DEFAULT_INDEX_NAME, DEFAULT_FIELD_NAME)
        env.assertEqual(tiered_backend_debug_info['NUMBER_OF_MARKED_DELETED'],
                        vecs_to_delete,
                        message=f"{message_prefix}: size_before: {size_before}")

        # Memory should remain unchanged
        after_del_memory = get_vecsim_memory(env, DEFAULT_INDEX_NAME, DEFAULT_FIELD_NAME)
        env.assertEqual(after_del_memory, memory_before_deletion, message=f"{message_prefix}")

        # Index size should reflect the number vectors + marked deleted
        size_after = tiered_backend_debug_info['INDEX_SIZE']
        env.assertEqual(size_after, size_before, message=f"{message_prefix}")

        # Labels count should reflect the number of valid vectors
        label_count_after = tiered_backend_debug_info['INDEX_LABEL_COUNT']
        env.assertEqual(label_count_after, label_count_before - vecs_to_delete, message=f"{message_prefix}: labels_count_before: {label_count_before}, vecs_to_delete: {vecs_to_delete}")

        # Phase 2: Force garbage collection to reclaim memory
        # Explicit GC should reduce memory usage after marked deletions
        env.expect(debug_cmd(), 'WORKERS', 'PAUSE').ok()
        forceInvokeGC(env, DEFAULT_INDEX_NAME)
        cur_workers_stats = getWorkersThpoolStats(env)

        # GC background jobs for SVS should be pending in low priority queue.
        env.assertEqual(cur_workers_stats['lowPriorityPendingJobs'], num_workers, message=f"{message_prefix}")
        env.expect(debug_cmd(), 'WORKERS', 'RESUME').ok()
        env.expect(debug_cmd(), 'WORKERS', 'DRAIN').ok()

        tiered_backend_debug_info = get_tiered_backend_debug_info(env, DEFAULT_INDEX_NAME, DEFAULT_FIELD_NAME)

        # Verify that the number of marked deleted vectors is as expected
        env.assertEqual(tiered_backend_debug_info['NUMBER_OF_MARKED_DELETED'], 0, message=f"{message_prefix}")

        # Memory should decrease
        after_gc_memory = get_vecsim_memory(env, DEFAULT_INDEX_NAME, DEFAULT_FIELD_NAME)
        env.assertLess(after_gc_memory, after_del_memory, message=f"{message_prefix}")

        # Index size should be updated
        size_after = tiered_backend_debug_info['INDEX_SIZE']
        env.assertEqual(size_after, size_before - vecs_to_delete, message=f"{message_prefix}: size_before: {size_before}, vecs_to_delete: {vecs_to_delete}")

        # Labels count should reflect the number of valid vectors
        label_count_after = tiered_backend_debug_info['INDEX_LABEL_COUNT']
        env.assertEqual(label_count_after, label_count_before - vecs_to_delete, message=f"{message_prefix}")

        env.execute_command('FLUSHALL')
