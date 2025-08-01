from RLTest import Env
from common import create_random_np_array_typed, getConnectionByEnv, to_dict, debug_cmd
import numpy as np

DEFAULT_BLOCK_SIZE = 1024

# Global counter for unique vector generation
_vector_seed_counter = 0

def get_unique_vector(dim, data_type='FLOAT32'):
    """Generate a unique random vector by incrementing seed counter"""
    global _vector_seed_counter
    _vector_seed_counter += 1
    return create_random_np_array_typed(dim, data_type, seed=_vector_seed_counter)

# @param additional_schema_args - additional arguments to pass to FT.CREATE beyond TYPE, DIM, DISTANCE_METRIC
def create_vector_index(env: Env, dim, index_name='idx', field_name='v', datatype='FLOAT32', alg='FLAT', addtional_vec_params=None, additional_schema_args=None):
    if additional_schema_args is None:
        additional_schema_args = []
    params = ['TYPE', datatype, 'DIM', dim, 'DISTANCE_METRIC', 'L2']
    if addtional_vec_params is not None:
        params.extend(addtional_vec_params)
    env.expect('FT.CREATE', index_name, 'SCHEMA',
               field_name, 'VECTOR', alg, len(params), *params,
               *additional_schema_args).ok()

def populated_with_vectors(env, num_docs, dim, datatype='FLOAT32', field_name='v', initial_doc_id=1):
    conn = getConnectionByEnv(env)
    p = conn.pipeline(transaction=False)
    for i in range(initial_doc_id, initial_doc_id + num_docs):
        vector = get_unique_vector(dim, datatype)
        p.execute_command('HSET', f'doc{i}', field_name, vector.tobytes())
    p.execute()

def set_up_database_with_vectors(env: Env, dim, num_docs, index_name='idx', datatype='FLOAT32', alg='FLAT', addtional_vec_params=None, additional_schema_args=None):
    create_vector_index(env, dim=dim,
                        index_name=index_name,
                        datatype=datatype,
                        alg=alg,
                        addtional_vec_params=addtional_vec_params,
                        additional_schema_args=additional_schema_args)

    populated_with_vectors(env, num_docs, dim, datatype)

def get_tiered_debug_info(env, index_name, field_name) -> dict:
    return to_dict(env.cmd(debug_cmd(), "VECSIM_INFO", index_name, field_name))

def get_tiered_frontnend_debug_info(env, index_name, field_name) -> dict:
    tiered_index_info = get_tiered_debug_info(env, index_name, field_name)
    return to_dict(tiered_index_info['FRONTEND_INDEX'])

def get_tiered_backnend_debug_info(env, index_name, field_name) -> dict:
    tiered_index_info = get_tiered_debug_info(env, index_name, field_name)
    return to_dict(tiered_index_info['BACKEND_INDEX'])
