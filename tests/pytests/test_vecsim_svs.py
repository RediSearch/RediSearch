from RLTest import Env
import time

from vecsim_utils import (
    create_vector_index,
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
    run_command_on_all_shards
)

# SVS-VAMANA only supports FLOAT32 and FLOAT16 data types
VECSIM_SVS_DATA_TYPES = ['FLOAT32', 'FLOAT16']

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
