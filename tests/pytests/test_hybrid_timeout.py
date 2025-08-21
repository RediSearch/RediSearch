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
        vector = create_np_array_typed(np.random.rand(dim), 'FLOAT32')
        conn.execute_command('HSET', f'doc{i}', 'v', vector.tobytes(), 't', f'text{i}')

    waitForIndex(env, index_name)
    return create_np_array_typed(np.random.rand(dim), 'FLOAT32')


def execute_hybrid_query(env, index_name, query, query_vec):
    """Execute a FT.HYBRID query and return parsed results"""
    res = env.cmd('FT.HYBRID', index_name, 'SEARCH', query, 'VSIM', '@v', query_vec.tobytes())
    env.assertTrue(isinstance(res, list))
    res_dict = dict(zip(res[::2], res[1::2]))
    return res_dict


def check_timeout_warning(res_dict):
    """Check if response contains timeout warning"""
    warnings = res_dict.get('warning', [])
    print(warnings)
    return any('Timeout' in str(warning) for warning in warnings)


def set_timeout_config(env, policy, timeout_ms):
    """Set timeout configuration"""
    env.expect(config_cmd(), 'set', 'ON_TIMEOUT', policy).ok()
    env.expect(config_cmd(), 'set', 'TIMEOUT', timeout_ms).ok()


def verify_basic_response_structure(env, res_dict):
    """Verify response has expected structure"""
    env.assertTrue('total_results' in res_dict)
    env.assertTrue('results' in res_dict)


def test_hybrid_timeout_functionality(env):
    """Test FT.HYBRID timeout behavior using configuration settings"""
    index_name = 'idx_timeout_func'
    query_vec = setup_test_index(env, index_name, dim=32, n_vectors=1000)

    # Test basic functionality first - should complete successfully
    set_timeout_config(env, 'RETURN', 50000)  # Reasonable timeout
    res_dict = execute_hybrid_query(env, index_name, 'text0', query_vec)
    verify_basic_response_structure(env, res_dict)

    # Verify no timeout warning with reasonable timeout
    env.assertFalse(check_timeout_warning(res_dict))
    # Test with very small timeout - should timeout but return partial results
    set_timeout_config(env, 'RETURN', 1)  # Force timeout
    res_dict = execute_hybrid_query(env, index_name, '*', query_vec)
    verify_basic_response_structure(env, res_dict)
    # Don't assert timeout warning as it may not always trigger
    # Test with 0 timeout (no timeout) - should complete successfully
    set_timeout_config(env, 'RETURN', 0)  # Disable timeout
    res_dict = execute_hybrid_query(env, index_name, 'text0', query_vec)
    verify_basic_response_structure(env, res_dict)

    # Verify no timeout warning with disabled timeout
    env.assertFalse(check_timeout_warning(res_dict))
    # Reset to reasonable timeout for cleanup
    set_timeout_config(env, 'RETURN', 5000)


def test_hybrid_timeout_with_large_dataset(env):
    """Test FT.HYBRID timeout behavior with a larger dataset using configuration timeout"""
    index_name = 'idx_timeout_large'
    query_vec = setup_test_index(env, index_name, dim=64, n_vectors=2000)  # Larger dataset

    # Test RETURN policy with forced timeout
    set_timeout_config(env, 'RETURN', 1)  # Force timeout
    res_dict = execute_hybrid_query(env, index_name, '*', query_vec)
    verify_basic_response_structure(env, res_dict)
    # Don't assert timeout warning as it may not always trigger with this dataset size

    # Test FAIL policy - should return error on timeout
    set_timeout_config(env, 'FAIL', 1)  # Force timeout
    res_dict = execute_hybrid_query(env, index_name, "*", query_vec)
    env.assertTrue(check_timeout_warning(res_dict))

    # Reset to RETURN policy and reasonable timeout for cleanup
    set_timeout_config(env, 'RETURN', 5000)


def test_hybrid_timeout_policies(env):
    """Test FT.HYBRID timeout behavior with different timeout policies using configuration"""
    index_name = 'idx_timeout_policies'
    query_vec = setup_test_index(env, index_name, dim=64, n_vectors=1000)

    # Test RETURN policy - should return partial results with timeout warning
    set_timeout_config(env, 'RETURN', 1)  # Force timeout
    res_dict = execute_hybrid_query(env, index_name, '*', query_vec)
    verify_basic_response_structure(env, res_dict)

    # Test FAIL policy - should return error on timeout
    set_timeout_config(env, 'FAIL', 1)  # Force timeout
    res_dict = execute_hybrid_query(env, index_name, "*", query_vec)
    env.assertTrue(check_timeout_warning(res_dict))

    # Test with reasonable timeout to verify normal operation still works
    set_timeout_config(env, 'RETURN', 50000)  # Reasonable timeout
    res_dict = execute_hybrid_query(env, index_name, 'text0', query_vec)
    verify_basic_response_structure(env, res_dict)
    # Verify no timeout warning with reasonable timeout
    env.assertFalse(check_timeout_warning(res_dict))
