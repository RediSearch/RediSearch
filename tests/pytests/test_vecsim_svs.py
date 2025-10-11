from RLTest import Env
import time

from vecsim_utils import (
    create_vector_index,
    get_unique_vector,
    DEFAULT_BLOCK_SIZE,
    populated_with_vectors,
    get_tiered_frontnend_debug_info,
    get_tiered_debug_info,
    get_tiered_backnend_debug_info
)
from common import (
    getConnectionByEnv,
    skip,
    TimeLimit,
)
VECSIM_SVS_DATA_TYPES = ['FLOAT32', 'FLOAT16']

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
def test_rdb_load_trained_svs_vamana():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    training_threshold = DEFAULT_BLOCK_SIZE * 3
    extend_params = ['COMPRESSION', 'LVQ8', 'TRAINING_THRESHOLD', training_threshold]
    dim = 4
    index_name='idx'
    field_name='vec'

    for data_type in VECSIM_SVS_DATA_TYPES:
        create_vector_index(env, dim, index_name=index_name, field_name=field_name, datatype=data_type, alg='SVS-VAMANA', addtional_vec_params=extend_params)

        frontend_index_info = get_tiered_frontnend_debug_info(env, index_name, field_name)
        env.assertEqual(frontend_index_info['INDEX_SIZE'], 0)

        # Insert vectors (not triggering training yet)
        populated_with_vectors(env, num_docs=training_threshold - 1, dim=dim, datatype=data_type, field_name=field_name)

        env.assertEqual(get_tiered_frontnend_debug_info(env, index_name, field_name)['INDEX_SIZE'], training_threshold - 1)
        env.assertEqual(get_tiered_backnend_debug_info(env, index_name, field_name)['INDEX_SIZE'], 0)

        # Insert more vectors to trigger training
        populated_with_vectors(env, num_docs=1, dim=dim, datatype=data_type, field_name=field_name, initial_doc_id=training_threshold)

        def verify_trained(message):
            with TimeLimit(30, message):
                is_trained = False
                while not is_trained:
                    # 'BACKGROUND_INDEXING' == 0 means training is done
                    is_trained = get_tiered_debug_info(env, index_name, field_name)['BACKGROUND_INDEXING'] == 0
                    time.sleep(0.1)


            env.assertEqual(get_tiered_frontnend_debug_info(env, index_name, field_name)['INDEX_SIZE'], 0, message=message)
            env.assertEqual(get_tiered_backnend_debug_info(env, index_name, field_name)['INDEX_SIZE'], training_threshold, message=message)

        verify_trained(f"datatype: {data_type} before rdb load")

        # reload rdb
        for _ in env.reloadingIterator():
            verify_trained(f"datatype: {data_type} after rdb load")

        conn.execute_command('FLUSHALL')
