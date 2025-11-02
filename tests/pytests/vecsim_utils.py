from RLTest import Env
from includes import *
from common import (
    getConnectionByEnv,
    to_dict,
    debug_cmd,
    index_info,
    TimeLimit,
    create_random_np_array_typed,
)
import numpy as np
import time
from redis import exceptions as redis_exceptions

VECSIM_DISTANCE_METRICS = ['COSINE', 'L2', 'IP']

DEFAULT_BLOCK_SIZE = 1024
DEFAULT_INDEX_NAME = 'idx'
DEFAULT_FIELD_NAME = 'v'
DEFAULT_DOC_NAME_PREFIX = 'doc'

# @param additional_schema_args - additional arguments to pass to FT.CREATE beyond TYPE, DIM, DISTANCE_METRIC
def create_vector_index(env: Env, dim, index_name=DEFAULT_INDEX_NAME, field_name=DEFAULT_FIELD_NAME, datatype='FLOAT32', metric='L2',
                        alg='FLAT', additional_vec_params=None,
                        additional_schema_args=None, message='', depth=0):
    if additional_schema_args is None:
        additional_schema_args = []
    params = ['TYPE', datatype, 'DIM', dim, 'DISTANCE_METRIC', metric]
    if additional_vec_params is not None:
        params.extend(additional_vec_params)
    try:
        env.execute_command('FT.CREATE', index_name, 'SCHEMA',
                field_name, 'VECTOR', alg, len(params), *params,
                *additional_schema_args)
    except redis_exceptions.ResponseError as e:
        env.assertTrue(False, message=f"Failed to create index: '{index_name}', metric: {metric}, datatype: {datatype}, alg: {alg}, {message} with error: {e}", depth=depth+1)

# Will populate the database with hashes doc_name_prefix<doc_id> containing a single vector field
# @param ret_vec_offset - return the i-th vector that is indexed.
def populate_with_vectors(env, num_docs, dim, datatype='FLOAT32', field_name=DEFAULT_FIELD_NAME, initial_doc_id=1, doc_name_prefix=DEFAULT_DOC_NAME_PREFIX, normalize=False, ret_vec_offset=0):
    conn = getConnectionByEnv(env)
    p = conn.pipeline(transaction=False)
    ret = None
    for i in range(num_docs):
        vector = create_random_np_array_typed(dim, datatype, normalize=normalize)
        if i == ret_vec_offset:
            ret = vector
        p.execute_command('HSET', f'{doc_name_prefix}{initial_doc_id + i}', field_name, vector.tobytes())
    p.execute()

    return ret

def set_up_database_with_vectors(env: Env, dim, num_docs, index_name=DEFAULT_INDEX_NAME, field_name=DEFAULT_FIELD_NAME, datatype='FLOAT32', metric='L2', alg='FLAT', additional_vec_params=None, additional_schema_args=None):
    create_vector_index(env, dim=dim, metric=metric,
                        index_name=index_name,
                        field_name=field_name,
                        datatype=datatype,
                        alg=alg,
                        additional_vec_params=additional_vec_params,
                        additional_schema_args=additional_schema_args)

    populate_with_vectors(env, num_docs, dim, datatype, field_name)

def get_tiered_debug_info(env, index_name, field_name) -> dict:
    return to_dict(env.execute_command(debug_cmd(), "VECSIM_INFO", index_name, field_name))

def get_tiered_frontend_debug_info(env, index_name, field_name) -> dict:
    tiered_index_info = get_tiered_debug_info(env, index_name, field_name)
    return to_dict(tiered_index_info['FRONTEND_INDEX'])

def get_tiered_backend_debug_info(env, index_name, field_name) -> dict:
    tiered_index_info = get_tiered_debug_info(env, index_name, field_name)
    return to_dict(tiered_index_info['BACKEND_INDEX'])

def get_vecsim_memory(env, index_key, field_name):
    return float(to_dict(env.cmd(debug_cmd(), "VECSIM_INFO", index_key, field_name))["MEMORY"])/0x100000

def get_vecsim_index_size(env, index_key, field_name):
    return int(to_dict(env.cmd(debug_cmd(), "VECSIM_INFO", index_key, field_name))["INDEX_SIZE"])

def get_redisearch_vector_index_memory(env, index_key):
    return float(index_info(env, index_key)["vector_index_sz_mb"])

def wait_for_background_indexing(env, index_name, field_name, message=''):
    index_sizes = [0] * env.shardsCount
    flat_index_sizes = [0] * env.shardsCount
    backend_index_sizes = [0] * env.shardsCount
    iter = 0
    is_trained = [False] * env.shardsCount
    index_state = f"iter: {iter}, index_sizes: {index_sizes}, flat_index_sizes: {flat_index_sizes}, backend_index_sizes: {backend_index_sizes}, is_trained: {is_trained}"

    try:
        with TimeLimit(60):
            while not all(is_trained):
                # 'BACKGROUND_INDEXING' == 0 means training is done
                for i, con in enumerate(env.getOSSMasterNodesConnectionList()):
                    tiered_info = get_tiered_debug_info(con, index_name, field_name)
                    is_trained[i] = tiered_info['BACKGROUND_INDEXING'] == 0
                    index_sizes[i] = tiered_info['INDEX_SIZE']
                    flat_index_sizes[i] = to_dict(tiered_info['FRONTEND_INDEX'])['INDEX_SIZE']
                    backend_index_sizes[i] = to_dict(tiered_info['BACKEND_INDEX'])['INDEX_SIZE']

                time.sleep(0.1)
                iter += 1
                index_state = f"iter: {iter}, index_sizes: {index_sizes}, flat_index_sizes: {flat_index_sizes}, backend_index_sizes: {backend_index_sizes}, is_trained: {is_trained}"
        for id, con in enumerate(env.getOSSMasterNodesConnectionList()):
            index_size = get_tiered_debug_info(con, index_name, field_name)['INDEX_SIZE']
            env.assertGreater(get_tiered_backend_debug_info(con, index_name, field_name)['INDEX_SIZE'], 0, message=f"wait_for_background_indexing: shard: {id}, index size: {index_size}" + message)
            if OS == 'macos':
                env.debugPrint(f"wait_for_background_indexing: {index_state}, {message})", force=True)
    except Exception as e:
        message = f"wait_for_background_indexing: {index_state}, {message})"
        raise Exception(f'Timeout: {message}')
