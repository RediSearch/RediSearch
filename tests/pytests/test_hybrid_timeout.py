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


# ---------------------------------------------------------------------------
# Parameter validation tests for FT.HYBRID debug commands
# ---------------------------------------------------------------------------

def _base_hybrid_debug_cmd(idx='idx'):
    """Build the common prefix for a hybrid debug command."""
    return ['_FT.DEBUG', 'FT.HYBRID', idx, 'SEARCH', '*',
            'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector]

def test_hybrid_debug_wrong_arity():
    """Test wrong arity for both the distributed and shard-level debug wrappers."""
    env = Env(enableDebugCommand=True)
    setup_basic_index(env)
    # Too few arguments (need at least 9 for _FT.DEBUG FT.HYBRID)
    env.expect('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM', '@embedding') \
        .error().contains('wrong number of arguments')

def test_hybrid_debug_missing_debug_params_count():
    """Test error when DEBUG_PARAMS_COUNT is not provided."""
    env = Env(enableDebugCommand=True)
    setup_basic_index(env)
    # Valid hybrid command but no DEBUG_PARAMS_COUNT at the end
    env.expect(*_base_hybrid_debug_cmd(),
               'TIMEOUT_AFTER_N_SEARCH', '1') \
        .error().contains('DEBUG_PARAMS_COUNT')

def test_hybrid_debug_invalid_debug_params_count():
    """Test error when DEBUG_PARAMS_COUNT has an invalid value."""
    env = Env(enableDebugCommand=True)
    setup_basic_index(env)
    for invalid_count in ['meow', '-1', '0.5']:
        env.expect(*_base_hybrid_debug_cmd(),
                   'TIMEOUT_AFTER_N_SEARCH', '1',
                   'DEBUG_PARAMS_COUNT', invalid_count) \
            .error().contains('Invalid DEBUG_PARAMS_COUNT count')

def test_hybrid_debug_params_count_exceeds_argc():
    """Test error when DEBUG_PARAMS_COUNT is larger than the number of available arguments."""
    env = Env(enableDebugCommand=True)
    setup_basic_index(env)
    env.expect(*_base_hybrid_debug_cmd(),
               'TIMEOUT_AFTER_N_SEARCH', '1',
               'DEBUG_PARAMS_COUNT', '9999') \
        .error().contains('DEBUG_PARAMS_COUNT exceeds')

def test_hybrid_debug_unrecognized_argument():
    """Test error when an unrecognized debug argument is provided."""
    env = Env(enableDebugCommand=True)
    setup_basic_index(env)
    env.expect(*_base_hybrid_debug_cmd(),
               'TIMEOUT_AFTER_N_MEOW', '1',
               'DEBUG_PARAMS_COUNT', '2') \
        .error().contains('Unrecognized argument')

@skip(cluster=True)
def test_hybrid_debug_no_component_timeout_sa():
    """Test error when no component timeout parameter is specified (SA).

    In SA mode, HybridRequest_Debug_New short-circuits on debug_params_count==0
    before parseHybridDebugParams can validate. The reply is an error but
    without the specific "At least one component timeout parameter" message.
    """
    env = Env(enableDebugCommand=True)
    setup_basic_index(env)
    env.expect(*_base_hybrid_debug_cmd(), 'DEBUG_PARAMS_COUNT', '0').error()

@skip(cluster=False)
def test_hybrid_debug_no_component_timeout_cluster():
    """Test error when no component timeout parameter is specified (cluster).

    In cluster mode, RSShardedHybridCommand_Debug calls parseHybridDebugParams
    which validates that at least one component timeout is present.
    """
    env = Env(enableDebugCommand=True)
    setup_basic_index(env)
    env.expect(*_base_hybrid_debug_cmd(),
               'DEBUG_PARAMS_COUNT', '0') \
        .error().contains('At least one component timeout parameter')

def test_hybrid_debug_invalid_timeout_values():
    """Test error when timeout count values are invalid."""
    env = Env(enableDebugCommand=True)
    setup_basic_index(env)
    for param in ['TIMEOUT_AFTER_N_SEARCH', 'TIMEOUT_AFTER_N_VSIM', 'TIMEOUT_AFTER_N_TAIL']:
        for bad_val in ['meow', '-1', '0.5']:
            env.expect(*_base_hybrid_debug_cmd(),
                       param, bad_val,
                       'DEBUG_PARAMS_COUNT', '2') \
                .error().contains(f'Invalid {param} count')

def test_hybrid_debug_missing_timeout_value():
    """Test error when timeout parameter is provided without a value."""
    env = Env(enableDebugCommand=True)
    setup_basic_index(env)
    # TIMEOUT_AFTER_N_SEARCH without its numeric argument;
    # DEBUG_PARAMS_COUNT 1 means only 1 token is parsed as debug args.
    env.expect(*_base_hybrid_debug_cmd(),
               'TIMEOUT_AFTER_N_SEARCH',
               'DEBUG_PARAMS_COUNT', '1') \
        .error().contains('TIMEOUT_AFTER_N_SEARCH')

# Debug timeout tests using TIMEOUT_AFTER_N_* parameters
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

def test_debug_timeout_fail_both():
    """Test FAIL policy with both components timeout using debug parameters"""
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT FAIL')
    setup_basic_index(env)
    env.expect('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector, 'TIMEOUT_AFTER_N_SEARCH', '1','TIMEOUT_AFTER_N_VSIM', '2', 'DEBUG_PARAMS_COUNT', '4').error().contains('SEARCH_TIMEOUT Timeout limit was reached')

# Tail pipeline runs on the coordinator; debug timeout params aren't applied there in cluster.
@skip(cluster=True)
def test_debug_timeout_fail_tail():
    """Test FAIL policy with tail timeout using debug parameters"""
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT FAIL')
    setup_basic_index(env)
    env.expect('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector, 'TIMEOUT_AFTER_N_TAIL', '1', 'DEBUG_PARAMS_COUNT', '2').error().contains('SEARCH_TIMEOUT Timeout limit was reached')

# Tail pipeline runs on the coordinator; debug timeout params aren't applied there in cluster.
@skip(cluster=True)
def test_debug_timeout_return_tail():
    """Test FAIL policy with tail timeout using debug parameters"""
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT RETURN')
    setup_basic_index(env)
    response = env.cmd('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector,
                       'TIMEOUT_AFTER_N_TAIL', '1', 'DEBUG_PARAMS_COUNT', '2')
    env.assertEqual(['Timeout limit was reached (POST PROCESSING)'], get_warnings(response))


def test_debug_timeout_return_search():
    """Test RETURN policy with search timeout using debug parameters"""
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT RETURN')
    setup_basic_index(env)
    response = env.cmd('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector,
                       'TIMEOUT_AFTER_N_SEARCH', '1', 'DEBUG_PARAMS_COUNT', '2')
    env.assertEqual(['Timeout limit was reached (SEARCH)'], get_warnings(response))

def test_debug_timeout_return_vsim():
    """Test RETURN policy with vector similarity timeout using debug parameters"""
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT RETURN')
    setup_basic_index(env)
    response = env.cmd('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector,
                       'TIMEOUT_AFTER_N_VSIM', '1', 'DEBUG_PARAMS_COUNT', '2')
    env.assertEqual(['Timeout limit was reached (VSIM)'], get_warnings(response))

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

# Partial result assertions depend on data distribution across shards.
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

# Helper to add enough documents with distinct "run*" terms to guarantee
# max prefix expansion triggers on at least one shard in cluster mode.
def add_run_prefix_docs(conn, count=20):
    vec = np.array([0.5, 0.5]).astype(np.float32).tobytes()
    for i in range(count):
        conn.execute_command('HSET', f'run_doc:{i}', 'description', f'run{i}word', 'embedding', vec)

# ---------------------------------------------------------------------------
# Sanity comparison: debug results vs regular results
# ---------------------------------------------------------------------------

@skip(cluster=True)
def test_debug_sanity_no_truncation():
    """Verify a debug query with high timeouts returns the same results as a regular query.

    Analogous to the Sanity() method in test_debug_commands.py for FT.SEARCH/FT.AGGREGATE.
    """
    env = Env(enableDebugCommand=True)
    setup_basic_index(env)

    regular_res = env.cmd('FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM',
                          '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector)
    regular_results, regular_count = get_results_from_hybrid_response(regular_res)

    # Timeouts high enough that no component actually times out (4 docs in dataset).
    debug_res = env.cmd('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM',
                        '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector,
                        'TIMEOUT_AFTER_N_SEARCH', '100', 'TIMEOUT_AFTER_N_VSIM', '100',
                        'DEBUG_PARAMS_COUNT', '4')
    debug_results, debug_count = get_results_from_hybrid_response(debug_res)

    env.assertEqual(regular_count, debug_count,
                    message=f"Expected same count: regular={regular_count}, debug={debug_count}")
    env.assertEqual(set(regular_results.keys()), set(debug_results.keys()),
                    message="Debug query should return the same documents as regular query")

@skip(cluster=True)
def test_debug_sanity_truncated_subset():
    """Verify a debug query with truncation returns a subset of regular query results."""
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT RETURN')
    setup_basic_index(env)

    regular_res = env.cmd('FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM',
                          '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector)
    regular_results, _ = get_results_from_hybrid_response(regular_res)

    # Both components limited to 1 result each; the union is at most 2 of the 4 docs.
    debug_res = env.cmd('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM',
                        '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector,
                        'TIMEOUT_AFTER_N_SEARCH', '1', 'TIMEOUT_AFTER_N_VSIM', '1',
                        'DEBUG_PARAMS_COUNT', '4')
    debug_results, debug_count = get_results_from_hybrid_response(debug_res)

    env.assertGreater(len(regular_results), len(debug_results),
                      message="Debug query with truncation should return fewer documents")
    env.assertTrue(set(debug_results.keys()).issubset(set(regular_results.keys())),
                   message="Debug results should be a subset of regular results")
    warnings = get_warnings(debug_res)
    env.assertGreater(len(warnings), 0, message="Expected at least one timeout warning")

# ---------------------------------------------------------------------------
# Boundary: TIMEOUT_AFTER_N_* 0 means "no timeout for that component"
# ---------------------------------------------------------------------------

@skip(cluster=True)
def test_debug_timeout_zero_means_no_timeout():
    """TIMEOUT_AFTER_N_* 0 means "no timeout for this component" — it runs normally.

    This differs from non-hybrid TIMEOUT_AFTER_N where 0 means "timeout immediately."
    """
    env = Env(enableDebugCommand=True)
    setup_basic_index(env)

    regular_res = env.cmd('FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM',
                          '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector)
    regular_results, regular_count = get_results_from_hybrid_response(regular_res)

    debug_res = env.cmd('_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM',
                        '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector,
                        'TIMEOUT_AFTER_N_SEARCH', '0', 'TIMEOUT_AFTER_N_VSIM', '0',
                        'DEBUG_PARAMS_COUNT', '4')
    debug_results, debug_count = get_results_from_hybrid_response(debug_res)

    env.assertEqual(regular_count, debug_count,
                    message="Timeout 0 should not truncate results")
    env.assertEqual(set(regular_results.keys()), set(debug_results.keys()),
                    message="Timeout 0 should return the same documents as regular query")

# Warning and error tests
def test_maxprefixexpansions_warning_search_only():
    """Test max prefix expansions warning when only SEARCH component is affected"""
    env = Env(enableDebugCommand=True)
    setup_basic_index(env)
    conn = env.getClusterConnectionIfNeeded()
    add_run_prefix_docs(conn)
    run_command_on_all_shards(env, config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', '1')

    # Only SEARCH returns results, VSIM returns empty
    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'run*', 'VSIM', \
                       '@embedding', '$BLOB', 'RANGE', '2', 'RADIUS', '0.01', 'PARAMS', '2', 'BLOB', query_vector)
    warnings = get_warnings(response)
    env.assertTrue('Max prefix expansions limit was reached (SEARCH)' in warnings)
    # Ensure the expansion warning is not in VSIM as well.
    env.assertFalse('Max prefix expansions limit was reached (VSIM)' in warnings)

def test_maxprefixexpansions_warning_vsim_only():
    """Test max prefix expansions warning when only VSIM component is affected"""
    env = Env(enableDebugCommand=True)
    setup_basic_index(env)
    conn = env.getClusterConnectionIfNeeded()
    add_run_prefix_docs(conn)
    run_command_on_all_shards(env, config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', '1')

    # Only VSIM returns results, SEARCH returns empty
    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'green', 'VSIM', \
                       '@embedding', '$BLOB', 'FILTER', '@description:run*', 'PARAMS', '2', 'BLOB', query_vector)
    warnings = get_warnings(response)
    env.assertTrue('Max prefix expansions limit was reached (VSIM)' in warnings)
    # Ensure the expansion warning is not in SEARCH as well.
    env.assertFalse('Max prefix expansions limit was reached (SEARCH)' in warnings)

def test_maxprefixexpansions_warning_both_components():
    """Test max prefix expansions warning when both SEARCH and VSIM components are affected"""
    env = Env(enableDebugCommand=True)
    setup_basic_index(env)
    conn = env.getClusterConnectionIfNeeded()
    add_run_prefix_docs(conn)
    run_command_on_all_shards(env, config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', '1')

    # Both SEARCH and VSIM return results
    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'run*', 'VSIM', \
                       '@embedding', '$BLOB', 'FILTER', '@description:run*', 'PARAMS', '2', 'BLOB', query_vector)
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
def test_debug_profile_hybrid_uses_normal_callback():
    """Test FT.DEBUG FT.PROFILE is handled correctly."""
    env = Env(enableDebugCommand=True)
    setup_basic_index(env)
    res = env.cmd(
        '_FT.DEBUG', 'FT.PROFILE', 'idx', 'HYBRID', 'QUERY',
        'SEARCH', '*',
        'VSIM', '@embedding', '$BLOB',
        'PARAMS', '2', 'BLOB', query_vector)
    # Basic sanity: we got results and profile info without an error.
    env.assertIsNotNone(res)
    env.assertGreater(len(res), 0)

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

def test_debug_timeout_return_strict_rejected():
    """Test that _FT.DEBUG FT.HYBRID rejects ON_TIMEOUT RETURN-STRICT policy."""
    env = Env(enableDebugCommand=True, moduleArgs='ON_TIMEOUT RETURN-STRICT')
    setup_basic_index(env)
    env.expect(
        '_FT.DEBUG', 'FT.HYBRID', 'idx', 'SEARCH', 'running',
        'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector,
        'TIMEOUT_AFTER_N_SEARCH', '1', 'DEBUG_PARAMS_COUNT', '2'
    ).error().contains('not supported with ON_TIMEOUT RETURN-STRICT')
