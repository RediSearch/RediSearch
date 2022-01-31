from common import *
from RLTest import Env

def testInfo(env):
    skipOnOss(env)
    conn = getConnectionByEnv(env)
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'SORTABLE')
    for i in range (100):
        conn.execute_command('HSET', i, 't', 'Hello world!')
    
    idx_info = index_info(env, 'idx')
    env.assertLess(float(idx_info['inverted_sz_mb']), 1)
    env.assertLess(float(idx_info['offset_vectors_sz_mb']), 1)
    env.assertLess(float(idx_info['doc_table_size_mb']), 1)
    env.assertLess(float(idx_info['sortable_values_size_mb']), 1)
    env.assertLess(float(idx_info['key_table_size_mb']), 1)


    