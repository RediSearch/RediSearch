from RLTest import Env
import distro
# from common import *
from includes import *

from vecsim_utils import (
    DEFAULT_BLOCK_SIZE,
    DEFAULT_INDEX_NAME,
    DEFAULT_FIELD_NAME,

    create_vector_index,
    get_tiered_frontend_debug_info,
    get_tiered_backend_debug_info,
    wait_for_background_indexing,
    get_tiered_debug_info,
    populate_with_vectors,
    get_vecsim_index_size,
    get_vecsim_memory,
    get_redisearch_vector_index_memory
    )
from common import (
    create_random_np_array_typed,
    getConnectionByEnv,
    skip,
    assertInfoField,
    index_info,
    to_dict,
    get_redis_memory_in_mb,

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
This test reproduce the crash described in MOD-10771,
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
    for data_type in VECSIM_SVS_DATA_TYPES:
        for compression in [[], ["COMPRESSION", "LVQ8"]]:
            params = ['TYPE', data_type, 'DIM', dim, 'DISTANCE_METRIC', 'L2', "CONSTRUCTION_WINDOW_SIZE", 10, *compression]
            conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'v_SVS_VAMANA', 'VECTOR', 'SVS-VAMANA', len(params), *params)

            # Add enough vector to trigger transfer to svs
            for i in range(num_vectors):
                vector = create_random_np_array_typed(dim, data_type)
                conn.execute_command('HSET', f'doc_{i}', 'v_SVS_VAMANA', vector.tobytes())

            # delete most
            for i in range(num_vectors - keep_count):
                conn.execute_command('DEL', f'doc_{i}')

            # run topk for remaining
            query_vec = create_random_np_array_typed(dim, data_type)
            # Before fixing MOD-10771, search crashed
            try:
                res = conn.execute_command('FT.SEARCH', 'idx', f'*=>[KNN {keep_count} @v_SVS_VAMANA $vec_param]', 'PARAMS', 2, 'vec_param', query_vec.tobytes(), 'RETURN', 1, '__v_score')
                env.assertGreater(res[0], 0)
            except Exception as e:
                env.assertTrue(False, message=f"compression: {compression} data_type: {data_type}. Search failed with exception: {e}")
            conn.execute_command('FLUSHALL')

@skip(cluster=True)
# TODO: CLUSTER????
def test_rdb_load_trained_svs_vamana():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    training_threshold = DEFAULT_BLOCK_SIZE * 3
    extend_params = ['COMPRESSION', 'LVQ8', 'TRAINING_THRESHOLD', training_threshold]
    dim = 4
    index_name=DEFAULT_INDEX_NAME
    field_name=DEFAULT_FIELD_NAME

    for data_type in VECSIM_SVS_DATA_TYPES:
        create_vector_index(env, dim, datatype=data_type, alg='SVS-VAMANA', additional_vec_params=extend_params)

        frontend_index_info = get_tiered_frontend_debug_info(env, index_name, field_name)
        env.assertEqual(frontend_index_info['INDEX_SIZE'], 0)

        # Insert vectors (not triggering training yet)
        populate_with_vectors(env, num_docs=training_threshold - 1, dim=dim, datatype=data_type)

        env.assertEqual(get_tiered_frontend_debug_info(env, index_name, field_name)['INDEX_SIZE'], training_threshold - 1)
        env.assertEqual(get_tiered_backend_debug_info(env, index_name, field_name)['INDEX_SIZE'], 0)

        # Insert more vectors to trigger training
        populate_with_vectors(env, num_docs=1, dim=dim, datatype=data_type, initial_doc_id=training_threshold)

        def verify_trained(message):
            wait_for_background_indexing(env, index_name, field_name, message)

            env.assertEqual(get_tiered_frontend_debug_info(env, index_name, field_name)['INDEX_SIZE'], 0, message=message)
            env.assertEqual(get_tiered_backend_debug_info(env, index_name, field_name)['INDEX_SIZE'], training_threshold, message=message)

        verify_trained(f"datatype: {data_type} before rdb load")

        # reload rdb
        for _ in env.reloadingIterator():
            verify_trained(f"datatype: {data_type} after rdb load")

        conn.execute_command('FLUSHALL')

@skip(cluster=True)
def test_svs_vamana_info():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    dim = 16
    data_type = 'FLOAT32'

    # Create SVS VAMANA index with all compression flavors (except for global SQ8).
    for compression_type in SVS_COMPRESSION_TYPES:
        cmd_params = ['TYPE', data_type,
                    'DIM', dim, 'DISTANCE_METRIC', 'L2']
        if compression_type != 'NO_COMPRESSION':
            cmd_params.extend(['COMPRESSION', compression_type])
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', len(cmd_params), *cmd_params).ok()

        # Validate that ft.info returns the default params for SVS VAMANA, along with compression
        # compression in runtime is LVQ8 if we are running on intel optimizations are enabled and GlobalSQ otherwise.
        compression_runtime = compression_type if is_intel_opt_enabled() or compression_type == 'NO_COMPRESSION' else 'GlobalSQ8'
        expected_info = [['identifier', 'v', 'attribute', 'v', 'type', 'VECTOR', 'algorithm', 'SVS-VAMANA',
                          'data_type', 'FLOAT32', 'dim', 16, 'distance_metric', 'L2', 'graph_max_degree', 32,
                          'construction_window_size', 200, 'compression', compression_runtime]]
        if compression_type != 'NO_COMPRESSION':
            expected_info[0].extend(['training_threshold', 10240])
        if compression_runtime == 'LeanVec4x8' or compression_runtime == 'LeanVec8x8':
            expected_info[0].extend(['reduced_dim', dim // 2])
        assertInfoField(env, 'idx', 'attributes',
                        expected_info)
        env.expect('FT.DROPINDEX', 'idx').ok()

# TODO: Elaborate for doord
def test_vamana_debug_info_vs_info():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')

    extend_params = [None,
                     # non default params
                     ['COMPRESSION', 'LVQ8', 'GRAPH_MAX_DEGREE', 1, 'CONSTRUCTION_WINDOW_SIZE', 1, 'TRAINING_THRESHOLD', DEFAULT_BLOCK_SIZE * 2],
                     ['COMPRESSION', 'LVQ8'], ['COMPRESSION', 'LeanVec4x8']]
    dim = 4
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
    dimension = 10
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

    for data_type in VECSIM_SVS_DATA_TYPES:
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
