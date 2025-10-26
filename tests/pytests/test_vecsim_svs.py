from RLTest import Env
import distro
from common import *
from includes import *

from vecsim_utils import (
    DEFAULT_BLOCK_SIZE,
    DEFAULT_INDEX_NAME,
    DEFAULT_FIELD_NAME,

    create_vector_index,
    get_tiered_frontend_debug_info,
    get_tiered_backend_debug_info,
    wait_for_background_indexing
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

# Global counter for unique vector generation
_vector_seed_counter = 0

def get_unique_vector(dim, data_type='FLOAT32'):
    """Generate a unique random vector by incrementing seed counter"""
    global _vector_seed_counter
    _vector_seed_counter += 1
    return create_random_np_array_typed(dim, data_type, seed=_vector_seed_counter)

'''
This test reproduce the crash described in MOD-10771,
wherer SVS crashes during topk search if CONSTRUCTION_WINDOW_SIZE given in creation is small.
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
                vector = get_unique_vector(dim, data_type)
                conn.execute_command('HSET', f'doc_{i}', 'v_SVS_VAMANA', vector.tobytes())


            # delete most
            for i in range(num_vectors - keep_count):
                conn.execute_command('DEL', f'doc_{i}')

            # run topk for remaining
            query_vec = get_unique_vector(dim, data_type)
            # Before fixing MOD-10771, search crashed
            conn.execute_command('FT.SEARCH', 'idx', f'*=>[KNN {keep_count} @v_SVS_VAMANA $vec_param]', 'PARAMS', 2, 'vec_param', query_vec.tobytes(), 'RETURN', 1, '__v_score')

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
        # populate_with_vectors(env, num_docs=training_threshold - 1, dim=dim, datatype=data_type)
        for i in range(training_threshold - 1):
            vector = get_unique_vector(dim, data_type)
            conn.execute_command('HSET', f'doc_{1 + i}', DEFAULT_FIELD_NAME, vector.tobytes())

        env.assertEqual(get_tiered_frontend_debug_info(env, index_name, field_name)['INDEX_SIZE'], training_threshold - 1)
        env.assertEqual(get_tiered_backend_debug_info(env, index_name, field_name)['INDEX_SIZE'], 0)

        # Insert more vectors to trigger training
        # populate_with_vectors(env, num_docs=1, dim=dim, datatype=data_type, initial_doc_id=training_threshold)
        vector = get_unique_vector(dim, data_type)
        conn.execute_command('HSET', f'doc_{training_threshold + 1}', DEFAULT_FIELD_NAME, vector.tobytes())

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
