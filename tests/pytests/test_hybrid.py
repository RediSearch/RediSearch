from RLTest import Env
from includes import *
from common import *
import numpy as np


def setup_test_index(env, index_name, dim=64, n_vectors=100):
    """Helper function to create and populate a test index"""
    # Set dialect to 2 for consistency
    env.expect('FT.CONFIG', 'SET', 'DEFAULT_DIALECT', '2').ok()

    # Create index
    env.expect(f'FT.CREATE {index_name} SCHEMA v VECTOR HNSW 6 DIM {dim} DISTANCE_METRIC L2 TYPE FLOAT32'
               f' t TEXT').equal('OK')

    # Populate with data
    conn = getConnectionByEnv(env)
    for i in range(n_vectors):
        vector = create_np_array_typed(np.random.rand(dim))
        conn.execute_command('HSET', f'doc{i}', 'v', vector.tobytes(), 't', f'text{i}')

    waitForIndex(env, index_name)
    return create_np_array_typed(np.random.rand(dim))


def test_hybrid_timeout_functionality(env):
    """Test FT.HYBRID timeout parameter functionality"""
    index_name = 'idx_timeout_func'
    query_vec = setup_test_index(env, index_name, dim=32, n_vectors=100)

    # Set timeout policy to RETURN for easier testing
    env.expect(config_cmd(), 'set', 'ON_TIMEOUT', 'RETURN').ok()

    # Test that TIMEOUT parameter is accepted
    res = env.cmd('FT.HYBRID', index_name, 'SEARCH', 'text0', 'VSIM', '@v', query_vec.tobytes(), 'KNN', '2', 'K', '5', 'TIMEOUT', 1000)
    env.assertTrue(isinstance(res, list))
    res_dict = dict(zip(res[::2], res[1::2]))
    env.assertTrue('total_results' in res_dict)

    # Test with 0 timeout (no timeout)
    res = env.cmd('FT.HYBRID', index_name, 'SEARCH', 'text0', 'VSIM', '@v', query_vec.tobytes(), 'KNN', '2', 'K', '5', 'TIMEOUT', 0)
    env.assertTrue(isinstance(res, list))
    res_dict = dict(zip(res[::2], res[1::2]))
    env.assertTrue('total_results' in res_dict)


def test_hybrid_timeout_policies(env):
    """Test FT.HYBRID timeout behavior with different timeout policies"""
    index_name = 'idx_timeout_policies'
    query_vec = setup_test_index(env, index_name, dim=64, n_vectors=500)

    # Test RETURN policy
    env.expect(config_cmd(), 'set', 'ON_TIMEOUT', 'RETURN').ok()
    res = env.cmd('FT.HYBRID', index_name, 'SEARCH', 'text0', 'VSIM', '@v', query_vec.tobytes(), 'KNN', '2', 'K', '5', 'TIMEOUT', 1000)
    env.assertTrue(isinstance(res, list))
    res_dict = dict(zip(res[::2], res[1::2]))
    env.assertTrue('total_results' in res_dict)

    # Test FAIL policy
    env.expect(config_cmd(), 'set', 'ON_TIMEOUT', 'FAIL').ok()
    res = env.cmd('FT.HYBRID', index_name, 'SEARCH', 'text0', 'VSIM', '@v', query_vec.tobytes(), 'KNN', '2', 'K', '5', 'TIMEOUT', 1000)
    env.assertTrue(isinstance(res, list))
    res_dict = dict(zip(res[::2], res[1::2]))
    env.assertTrue('total_results' in res_dict)

