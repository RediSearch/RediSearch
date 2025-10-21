from RLTest import Env

from common import (
    getConnectionByEnv,
    to_dict,
    debug_cmd,
    index_info,
    TimeLimit,
    create_random_np_array_typed,
)
import numpy as np
import sys
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
        env.assertTrue(False, message=f"Failed to create index: '{index_name}' {message} with error: {e}", depth=depth+1)

# Will populate the database with hashes doc_name_prefix<doc_id> containing a single vector field
def populate_with_vectors(env, num_docs, dim, datatype='FLOAT32', field_name=DEFAULT_FIELD_NAME, initial_doc_id=1, doc_name_prefix=DEFAULT_DOC_NAME_PREFIX):
    conn = getConnectionByEnv(env)
    p = conn.pipeline(transaction=False)
    for i in range(num_docs):
        vector = create_random_np_array_typed(dim, datatype)
        p.execute_command('HSET', f'{doc_name_prefix}{initial_doc_id + i}', field_name, vector.tobytes())
    p.execute()

def set_up_database_with_vectors(env: Env, dim, num_docs, index_name=DEFAULT_INDEX_NAME, field_name=DEFAULT_FIELD_NAME, datatype='FLOAT32', metric='L2', alg='FLAT', additional_vec_params=None, additional_schema_args=None):
    create_vector_index(env, dim=dim, metric=metric,
                        index_name=index_name,
                        field_name=field_name,
                        datatype=datatype,
                        alg=alg,
                        additional_vec_params=additional_vec_params,
                        additional_schema_args=additional_schema_args)

    populate_with_vectors(env, num_docs, dim, datatype)

def get_tiered_debug_info(env, index_name, field_name) -> dict:
    return to_dict(env.cmd(debug_cmd(), "VECSIM_INFO", index_name, field_name))

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
    with TimeLimit(30, message):
        is_trained = False
        while not is_trained:
            # 'BACKGROUND_INDEXING' == 0 means training is done
            is_trained = get_tiered_debug_info(env, index_name, field_name)['BACKGROUND_INDEXING'] == 0
            time.sleep(0.1)

        env.assertGreater(get_tiered_backend_debug_info(env, index_name, field_name)['INDEX_SIZE'], 0, message=message)
