from RLTest import Env
from includes import *
from common import *
from test_info_modules import info_modules_to_dict
import psutil

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

# Debug timeout tests using TIMEOUT_AFTER_N_* parameters
#TODO: remove skip once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_debug_timeout_fail_search():
    """Test FAIL policy with search timeout using debug parameters"""
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT FAIL')
    setup_basic_index(env)
    env.expect('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector, 'TIMEOUT_AFTER_N_SEARCH', '1', 'DEBUG_PARAMS_COUNT', '2').error().contains('Timeout limit was reached')

def test_debug_timeout_fail_vsim():
    """Test FAIL policy with vector similarity timeout using debug parameters"""
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT FAIL')
    setup_basic_index(env)
    env.expect('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector, 'TIMEOUT_AFTER_N_VSIM', '1', 'DEBUG_PARAMS_COUNT', '2').error().contains('Timeout limit was reached')

#TODO: remove skip once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_debug_timeout_fail_both():
    """Test FAIL policy with both components timeout using debug parameters"""
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT FAIL')
    setup_basic_index(env)
    env.expect('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector, 'TIMEOUT_AFTER_N_SEARCH', '1','TIMEOUT_AFTER_N_VSIM', '2', 'DEBUG_PARAMS_COUNT', '4').error().contains('Timeout limit was reached')

#TODO: remove skip once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_debug_timeout_fail_tail():
    """Test FAIL policy with tail timeout using debug parameters"""
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT FAIL')
    setup_basic_index(env)
    env.expect('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector, 'TIMEOUT_AFTER_N_TAIL', '1', 'DEBUG_PARAMS_COUNT', '2').error().contains('Timeout limit was reached')

#TODO: remove skip once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_debug_timeout_return_tail():
    """Test FAIL policy with tail timeout using debug parameters"""
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
#TODO: remove skip once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_maxprefixexpansions_warning_search_only():
    """Test max prefix expansions warning when only SEARCH component is affected"""
    env = Env(enableDebugCommand=True)
    setup_basic_index(env)
    conn = env.getClusterConnectionIfNeeded()
    conn.execute_command('HSET', 'doc:5', 'description', 'runo')
    conn.execute_command(config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', '1')
    # Only SEARCH returns results, VSIM returns empty
    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'run*', 'VSIM', \
                       '@embedding', '$BLOB', 'RANGE', '2', 'RADIUS', '0.01', 'PARAMS', '2', 'BLOB', query_vector)
    env.assertTrue('Max prefix expansions limit was reached (SEARCH)' in get_warnings(response))

#TODO: remove skip once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_maxprefixexpansions_warning_vsim_only():
    """Test max prefix expansions warning when only VSIM component is affected"""
    env = Env(enableDebugCommand=True)
    setup_basic_index(env)
    conn = env.getClusterConnectionIfNeeded()
    conn.execute_command('HSET', 'doc:5', 'description', 'runo')
    conn.execute_command(config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', '1')

    # Only VSIM returns results, SEARCH returns empty
    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'green', 'VSIM', \
                       '@embedding', '$BLOB', 'FILTER', '@description:run*', 'PARAMS', '2', 'BLOB', query_vector)
    env.assertTrue('Max prefix expansions limit was reached (VSIM)' in get_warnings(response))

#TODO: remove skip once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_maxprefixexpansions_warning_both_components():
    """Test max prefix expansions warning when both SEARCH and VSIM components are affected"""
    env = Env(enableDebugCommand=True)
    setup_basic_index(env)
    conn = env.getClusterConnectionIfNeeded()
    conn.execute_command('HSET', 'doc:5', 'description', 'runo')
    conn.execute_command(config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', '1')
    coord_section = 'search_coordinator_warnings_and_errors'
    metric = 'search_coord_total_query_warnings_max_prefix_expansions'
    before_info = info_modules_to_dict(env)
    base_warning_count = int(before_info[coord_section][metric])

    # Both SEARCH and VSIM return results
    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'run*', 'VSIM', \
                       '@embedding', '$BLOB', 'FILTER', '@description:run*', 'PARAMS', '2', 'BLOB', query_vector)
    warning = get_warnings(response)
    env.assertTrue('Max prefix expansions limit was reached (SEARCH)' in warning)
    env.assertTrue('Max prefix expansions limit was reached (VSIM)' in warning)
    after_info = info_modules_to_dict(env)
    env.assertEqual(after_info[coord_section][metric], str(base_warning_count + 1),
                    message="Coordinator max-prefix warning should be +1 per query")

#TODO: remove skip once FT.HYBRID for cluster is implemented
@skip(cluster=True)
def test_tail_property_not_loaded_error():
    """Test error when tail pipeline references property not loaded nor in pipeline"""
    env = Env()
    setup_basic_index(env)
    response = env.expect('FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM', \
                          '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', \
                          query_vector, 'LOAD', '1', '@__key', 'APPLY', '2*@__score',\
                          'AS', 'doubled_score').error().contains('Property `__score` not loaded nor in pipeline')


@skip(cluster=False)
def test_timeout_setup_phase_hybrid():
    """FT.HYBRID cursor-setup timeout with one shard suspended, both policies.

    Suspending a non-coordinator shard parks the coordinator in
    ProcessHybridCursorMappings' cursor-mapping wait on a connected-but-silent shard.
    The per-query TIMEOUT bounds that wait, so instead of hanging the command resolves
    at the deadline per policy:
      - FAIL   -> a timeout error.
      - RETURN -> an empty result set carrying the timeout warning (no cursor mappings
                  were established, so there is nothing to return).
    """
    # WORKERS 1 dispatches the query to a BG thread so the cursor-setup wait is
    # reachable; cluster mode gives us a non-coordinator shard to suspend.
    env = Env(moduleArgs='WORKERS 1', protocol=3)
    for i in range(1, env.shardsCount + 1):
        verify_shard_init(env.getConnection(i))
    conn = getConnectionByEnv(env)

    env.expect(
        'FT.CREATE', 'hybrid_idx', 'PREFIX', '1', 'hybrid_doc', 'SCHEMA',
        'name', 'TEXT',
        'embedding', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2'
    ).ok()
    for i in range(100):
        vec = np.array([float(i), float(i)], dtype=np.float32).tobytes()
        conn.execute_command('HSET', f'hybrid_doc{i}', 'name', f'hello{i}', 'embedding', vec)
    query_vec = np.array([0.0, 0.0], dtype=np.float32).tobytes()
    env.expect(
        'FT.HYBRID', 'hybrid_idx', 'SEARCH', '*', 'VSIM', '@embedding', '$BLOB',
        'PARAMS', '2', 'BLOB', query_vec
    ).noError()

    # A finite per-query TIMEOUT arms the coordinator's cursor-setup deadline.
    query_args = [
        'FT.HYBRID', 'hybrid_idx',
        'SEARCH', '*',
        'VSIM', '@embedding', '$BLOB',
        'PARAMS', '2', 'BLOB', query_vec,
        'TIMEOUT', '200',
    ]

    prev_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
    _, _, paused_pid, _ = split_shards_pick_one_paused(env)
    shard_to_pause_p = psutil.Process(paused_pid)

    shard_to_pause_p.suspend()
    try:
        wait_for_condition(
            lambda: (shard_to_pause_p.status() == psutil.STATUS_STOPPED,
                     {'status': shard_to_pause_p.status()}),
            'Timeout while waiting for shard to pause'
        )

        # FAIL policy: the bounded setup wait surfaces a timeout error.
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail')
        env.expect(*query_args).error().contains('Timeout')

        # RETURN policy: empty result set + timeout warning instead of an error.
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return')
        res = env.cmd(*query_args)
        env.assertEqual(res['total_results'], 0, message=f"expected 0 results, got {res}")
        assert_timeout_warning(env, res, message="RETURN-policy setup-phase timeout")
    finally:
        # Resume so the shard drains the queued _FT.HYBRID / CURSOR DEL and frees
        # its cursors before later tests run.
        shard_to_pause_p.resume()
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy)
