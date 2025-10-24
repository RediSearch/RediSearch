from RLTest import Env
from common import *
from includes import *
VECSIM_SVS_DATA_TYPES = ['FLOAT32', 'FLOAT16']

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
                print(vector)
                conn.execute_command('HSET', f'doc_{i}', 'v_SVS_VAMANA', vector.tobytes())


            # delete most
            for i in range(num_vectors - keep_count):
                conn.execute_command('DEL', f'doc_{i}')

            # run topk for remaining
            query_vec = get_unique_vector(dim, data_type)
            # Before fixing MOD-10771, search crashed
            conn.execute_command('FT.SEARCH', 'idx', f'*=>[KNN {keep_count} @v_SVS_VAMANA $vec_param]', 'PARAMS', 2, 'vec_param', query_vec.tobytes(), 'RETURN', 1, '__v_score')

            conn.execute_command('FLUSHALL')

def test_2():
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
