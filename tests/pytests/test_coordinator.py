from common import *
from RLTest import Env

def testInfo(env):
    SkipOnNonCluster(env)
    conn = getConnectionByEnv(env)
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'SORTABLE', 'v', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2')
    for i in range (100):
        conn.execute_command('HSET', i, 't', 'Hello world!', 'v', 'abcdefgh')
    
    idx_info = index_info(env, 'idx')
    env.assertLess(float(idx_info['inverted_sz_mb']), 1)
    env.assertLess(float(idx_info['offset_vectors_sz_mb']), 1)
    env.assertLess(float(idx_info['doc_table_size_mb']), 1)
    env.assertLess(float(idx_info['sortable_values_size_mb']), 1)
    env.assertLess(float(idx_info['key_table_size_mb']), 1)
    env.assertLess(float(idx_info['vector_index_sz_mb']), 1)
    env.assertGreater(float(idx_info['inverted_sz_mb']), 0)
    env.assertGreater(float(idx_info['offset_vectors_sz_mb']), 0)
    env.assertGreater(float(idx_info['doc_table_size_mb']), 0)
    env.assertGreater(float(idx_info['sortable_values_size_mb']), 0)
    env.assertGreater(float(idx_info['key_table_size_mb']), 0)
    env.assertGreater(float(idx_info['vector_index_sz_mb']), 0)


    