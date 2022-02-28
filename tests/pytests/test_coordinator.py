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

def test_required_fields(env):
    # Testing coordinator<-> shard `_REQUIRED_FIELDS` protocol
    env.skipOnCluster()
    env.assertOk(env.execute_command('ft.create', 'idx', 'schema', 't', 'text'))
    env.execute_command('HSET', '0', 't', 'hello')
    env.expect('ft.search', 'idx', 'hello', '_REQUIRED_FIELDS').error()
    env.expect('ft.search', 'idx', 'hello', '_REQUIRED_FIELDS', '2', 't').error()
    env.expect('ft.search', 'idx', 'hello', '_REQUIRED_FIELDS', '1', 't').equal([1L, '0', '$hello', ['t', 'hello']])
    env.expect('ft.search', 'idx', 'hello', 'nocontent', 'SORTBY', 't', '_REQUIRED_FIELDS', '1', 't').equal([1L, '0', '$hello'])
    # Field is not in Rlookup, will not load
    env.expect('ft.search', 'idx', 'hello', 'nocontent', '_REQUIRED_FIELDS', '1', 't').equal([1L, '0', None])
    