from RLTest import Env
from includes import *
from common import *

# Test data with deterministic vectors
test_data = {
    'doc:1': {
        'description': "red shoes",
        'embedding': np.array([0.0, 0.0]).astype(np.float32).tobytes()
    },
    'doc:2': {
        'description': "red running shoes",
        'embedding': np.array([1.0, 0.0]).astype(np.float32).tobytes()
    },
    'doc:3': {
        'description': "running gear",
        'embedding': np.array([0.0, 1.0]).astype(np.float32).tobytes()
    },
    'doc:4': {
        'description': "blue shoes",
        'embedding': np.array([1.0, 1.0]).astype(np.float32).tobytes()
    }
}

# Query vector for testing
query_vector = np.array([1.2, 0.2]).astype(np.float32).tobytes()

def get_warnings(response):
    """Extract warnings from hybrid search response"""
    warnings_index = recursive_index(response, 'warnings')
    warnings_index[-1] += 1
    return access_nested_list(response, warnings_index)

def setup_basic_index(env):
    """Setup basic index with test data for debug timeout testing"""
    dim = 2
    conn = env.getClusterConnectionIfNeeded()
    env.expect('FT.CREATE idx SCHEMA description TEXT embedding VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2').ok

    # Load test data
    for doc_id, doc_data in test_data.items():
        conn.execute_command('HSET', doc_id, 'description', doc_data['description'], 'embedding', doc_data['embedding'])

def test_hybrid_debug_with_no_index_error():
    """Test error when index does not exist"""
    env = Env(enableDebugCommand=True)
    env.expect(
        '_FT.DEBUG', 'FT.HYBRID', 'nonexistent_idx',
        'SEARCH', '*',
        'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector,
        'TIMEOUT_AFTER_N_SEARCH', '1', 'DEBUG_PARAMS_COUNT', '2').error()\
        .contains('SEARCH_INDEX_NOT_FOUND Index not found: nonexistent_idx')

# Debug timeout tests using TIMEOUT_AFTER_N_* parameters
#TODO: remove skip once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_debug_timeout_fail_search():
    """Test FAIL policy with search timeout using debug parameters"""
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT FAIL')
    setup_basic_index(env)
    env.expect('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector, 'TIMEOUT_AFTER_N_SEARCH', '1', 'DEBUG_PARAMS_COUNT', '2').error().contains('SEARCH_TIMEOUT Timeout limit was reached')

def test_debug_timeout_fail_vsim():
    """Test FAIL policy with vector similarity timeout using debug parameters"""
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT FAIL')
    setup_basic_index(env)
    env.expect('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector, 'TIMEOUT_AFTER_N_VSIM', '1', 'DEBUG_PARAMS_COUNT', '2').error().contains('SEARCH_TIMEOUT Timeout limit was reached')

#TODO: remove skip once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_debug_timeout_fail_both():
    """Test FAIL policy with both components timeout using debug parameters"""
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT FAIL')
    setup_basic_index(env)
    env.expect('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector, 'TIMEOUT_AFTER_N_SEARCH', '1','TIMEOUT_AFTER_N_VSIM', '2', 'DEBUG_PARAMS_COUNT', '4').error().contains('SEARCH_TIMEOUT Timeout limit was reached')

#TODO: remove skip once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_debug_timeout_fail_tail():
    """Test FAIL policy with tail timeout using debug parameters"""
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT FAIL')
    setup_basic_index(env)
    env.expect('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector, 'TIMEOUT_AFTER_N_TAIL', '1', 'DEBUG_PARAMS_COUNT', '2').error().contains('SEARCH_TIMEOUT Timeout limit was reached')

#TODO: remove skip once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_debug_timeout_return_tail():
    """Test RETURN policy with tail timeout using debug parameters"""
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT RETURN')
    setup_basic_index(env)
    response = env.cmd('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector,
                       'TIMEOUT_AFTER_N_TAIL', '1', 'DEBUG_PARAMS_COUNT', '2')
    env.assertEqual(['Timeout limit was reached (POST PROCESSING)'], get_warnings(response))


#TODO: remove skip once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_debug_timeout_return_search():
    """Test RETURN policy with search timeout using debug parameters"""
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT RETURN')
    setup_basic_index(env)
    response = env.cmd('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector,
                       'TIMEOUT_AFTER_N_SEARCH', '1', 'DEBUG_PARAMS_COUNT', '2')
    env.assertEqual(['Timeout limit was reached (SEARCH)'], get_warnings(response))

#TODO: remove skip once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_debug_timeout_return_vsim():
    """Test RETURN policy with vector similarity timeout using debug parameters"""
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT RETURN')
    setup_basic_index(env)
    response = env.cmd('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector,
                       'TIMEOUT_AFTER_N_VSIM', '1', 'DEBUG_PARAMS_COUNT', '2')
    env.assertEqual(['Timeout limit was reached (VSIM)'], get_warnings(response))

#TODO: remove skip once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_debug_timeout_return_both():
    """Test RETURN policy with both components timeout using debug parameters"""
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT RETURN')
    setup_basic_index(env)
    response = env.cmd('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector,
                       'TIMEOUT_AFTER_N_SEARCH', '1','TIMEOUT_AFTER_N_VSIM', '1', 'DEBUG_PARAMS_COUNT', '4')
    warnings = get_warnings(response)
    env.assertTrue('Timeout limit was reached (SEARCH)' in get_warnings(response))
    env.assertTrue('Timeout limit was reached (VSIM)' in get_warnings(response))
    # TODO: add test for tail timeout once MOD-11004 is merged

#TODO: remove skip once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_debug_timeout_return_with_results():
    """Test RETURN policy returns partial results when components timeout"""
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT RETURN')
    setup_basic_index(env)
    # VSIM returns doc:2 and doc:4 (without timeout), SEARCH returns doc:3 (without timeout)
    response = env.cmd('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'gear', 'VSIM', \
                       '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector, \
                       'TIMEOUT_AFTER_N_SEARCH', '1', 'TIMEOUT_AFTER_N_VSIM', '1', 'DEBUG_PARAMS_COUNT', '4')
    results, count = get_results_from_hybrid_response(response)
    env.assertEqual(count, len(results.keys()))
    env.assertTrue('doc:3' in results.keys())
    # Expect exactly one document from VSIM since the timeout occurred after processing one result - should be either doc:2 or doc:4
    env.assertTrue(('doc:2' in results.keys()) ^ ('doc:4' in results.keys()))

# Warning and error tests
def test_maxprefixexpansions_warning_search_only():
    """Test max prefix expansions warning when only SEARCH component is affected"""
    env = Env(enableDebugCommand=True)
    setup_basic_index(env)
    conn = env.getClusterConnectionIfNeeded()
    # Use hash tags to ensure documents land on the same shard in cluster mode
    # This ensures the shard has multiple terms starting with "run" to trigger
    # the warning
    conn.execute_command('HSET', '{tag}:run1', 'description', 'running')
    conn.execute_command('HSET', '{tag}:run2', 'description', 'runo')
    run_command_on_all_shards(env, f'{config_cmd()} SET MAXPREFIXEXPANSIONS 1')

    # Only SEARCH returns results, VSIM returns empty
    response = env.cmd(
        'FT.HYBRID', 'idx',
        'SEARCH', 'run*',
        'VSIM', '@embedding', '$BLOB', 'RANGE', '2', 'RADIUS', '0.01',
        'PARAMS', '2', 'BLOB', query_vector)
    env.assertTrue('Max prefix expansions limit was reached (SEARCH)' in get_warnings(response))

def test_maxprefixexpansions_warning_vsim_only():
    """Test max prefix expansions warning when only VSIM component is affected"""
    env = Env(enableDebugCommand=True)
    setup_basic_index(env)
    conn = env.getClusterConnectionIfNeeded()
    # Use hash tags to ensure documents land on the same shard in cluster mode
    # This ensures the shard has multiple terms starting with "run" to trigger
    # the warning
    conn.execute_command('HSET', '{tag}:run1', 'description', 'running')
    conn.execute_command('HSET', '{tag}:run2', 'description', 'runo')
    run_command_on_all_shards(env, f'{config_cmd()} SET MAXPREFIXEXPANSIONS 1')

    # Only VSIM returns results, SEARCH returns empty
    response = env.cmd(
        'FT.HYBRID', 'idx',
        'SEARCH', 'green',
        'VSIM', '@embedding', '$BLOB', 'FILTER', '@description:run*',
        'PARAMS', '2', 'BLOB', query_vector)
    env.assertTrue('Max prefix expansions limit was reached (VSIM)' in get_warnings(response))

def test_maxprefixexpansions_warning_both_components():
    """Test max prefix expansions warning when both SEARCH and VSIM components are affected"""
    env = Env(enableDebugCommand=True)
    setup_basic_index(env)
    conn = env.getClusterConnectionIfNeeded()
    # Use hash tags to ensure documents land on the same shard in cluster mode
    # This ensures the shard has multiple terms starting with "run" to trigger
    # the warning
    conn.execute_command('HSET', '{tag}:run1', 'description', 'running')
    conn.execute_command('HSET', '{tag}:run2', 'description', 'runo')
    run_command_on_all_shards(env, f'{config_cmd()} SET MAXPREFIXEXPANSIONS 1')

    # Both SEARCH and VSIM return results
    response = env.cmd(
        'FT.HYBRID', 'idx',
        'SEARCH', 'run*',
        'VSIM', '@embedding', '$BLOB', 'FILTER', '@description:run*',
        'PARAMS', '2', 'BLOB', query_vector)
    warning = get_warnings(response)
    env.assertTrue('Max prefix expansions limit was reached (SEARCH)' in warning)
    env.assertTrue('Max prefix expansions limit was reached (VSIM)' in warning)

@skip(cluster=True)
def test_tail_property_not_loaded_error_standalone():
    """Test error when tail pipeline references property not loaded (standalone mode)"""
    env = Env()
    setup_basic_index(env)
    # In standalone, this is a fatal error (PROP_NOT_FOUND)
    env.expect('FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM', \
              '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', \
              query_vector, 'LOAD', '1', '@__key', 'APPLY', '2*@__score',\
              'AS', 'doubled_score').error().contains('__score')

@skip(cluster=False)
def test_tail_property_not_loaded_warning_coordinator():
    """Test warning when tail pipeline references property not loaded (coordinator mode)

    Related: test_tail_property_not_loaded_error_standalone
    In coordinator mode, tail pipeline errors become warnings (protocol limitation).
    The error code also differs: VALUE_NOT_FOUND (coord) vs PROP_NOT_FOUND (standalone).
    """
    env = Env()
    setup_basic_index(env)
    # In coordinator, this returns partial results with a warning (POST PROCESSING)
    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM', \
                      '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', \
                      query_vector, 'LOAD', '1', '@__key', 'APPLY', '2*@__score',\
                      'AS', 'doubled_score')
    # Extract warnings from RESP2 (list) or RESP3 (dict)
    if isinstance(response, dict):
        warnings = response.get('warnings', [])
    elif isinstance(response, list) and 'warnings' in response:
        idx = response.index('warnings')
        warnings = response[idx + 1] if idx + 1 < len(response) else []
    else:
        warnings = []
    env.assertTrue(any('__score' in w for w in warnings),
                   message=f"Expected warning about __score, got: {warnings}")
