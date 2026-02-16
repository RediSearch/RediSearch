from RLTest import Env
from includes import *
from common import *
import threading
import time
import redis

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

    # Both SEARCH and VSIM return results
    response = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'run*', 'VSIM', \
                       '@embedding', '$BLOB', 'FILTER', '@description:run*', 'PARAMS', '2', 'BLOB', query_vector)
    warning = get_warnings(response)
    env.assertTrue('Max prefix expansions limit was reached (SEARCH)' in warning)
    env.assertTrue('Max prefix expansions limit was reached (VSIM)' in warning)

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


#------------------------------------------------------------------------------
# Barrier Timeout Tests (cluster mode)
# These tests verify that the coordinator properly handles timeout during
# the cursor mapping phase (ProcessHybridCursorMappings) when waiting for
# shard responses.
#------------------------------------------------------------------------------

def run_hybrid_query_with_delayed_shard(env, cmd, query_result, sleep_duration):
    """Execute FT.HYBRID query where shard 1 is delayed (the one with 0 docs)"""
    # Use env.getConnection() to get coordinator connection, not cluster client
    conn = env.getConnection()
    try:
        # Start DEBUG SLEEP on shard 1 (connection index 2) in a background thread
        # This will block the entire Redis main thread on that shard
        def block_shard():
            # Connection index 2 corresponds to shard 1 (the one with 0 docs)
            shard_conn = env.getConnection(2)
            # This blocks the entire Redis instance
            shard_conn.execute_command('DEBUG', 'SLEEP', sleep_duration)

        sleep_thread = threading.Thread(target=block_shard, daemon=True)
        sleep_thread.start()

        # Wait a bit to ensure DEBUG SLEEP has started
        time.sleep(0.5)

        # Now send the query from coordinator
        # Shards 0 and 2 will respond quickly, but the coordinator must wait
        # for shard 1 during the cursor mapping phase
        start_time = time.time()
        result = conn.execute_command(*cmd)
        elapsed = time.time() - start_time

        query_result.append((result, elapsed))
    except Exception as e:
        query_result.append(e)


def _test_hybrid_barrier_waits_for_delayed_shard(protocol):
    """
    Test that ProcessHybridCursorMappings waits for all shards before returning
    results.

    This test uses DEBUG SLEEP on a specific shard to simulate delayed responses.
    Data is distributed across shards so fast shards start sending data while
    one shard is delayed. We verify that the coordinator waits for all shards
    and returns results after the delay.

    Shard 0: 100 docs (responds fast)
    Shard 2: 200 docs (responds fast)
    Shard 1: 0 docs - delayed with DEBUG SLEEP (via env.getConnection(2))
    """
    env = Env(moduleArgs='DEFAULT_DIALECT 2', protocol=protocol, shardsCount=3)
    conn = getConnectionByEnv(env)

    # Create index with vector field
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH',
               'SCHEMA', 'title', 'TEXT',
               'embedding', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32',
               'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()

    # Add docs to shard 0 (small shard)
    num_docs0 = 100
    for i in range(num_docs0):
        vec = np.array([float(i) / 2, 0.0]).astype(np.float32).tobytes()
        conn.execute_command('HSET', f'{{shard-0}}:doc:{i}',
                             'title', f'doc{i}', 'embedding', vec)

    # Add docs to shard 2 (large shard)
    num_docs2 = 200
    for i in range(num_docs2):
        vec = np.array([0.0, float(i) / 2]).astype(np.float32).tobytes()
        conn.execute_command('HSET', f'{{shard-2}}:doc:{i}',
                             'title', f'doc{i}', 'embedding', vec)

    query_vec = np.array([0.5, 0.5]).astype(np.float32).tobytes()

    # Now test with delayed shard using DEBUG SLEEP
    # We delay shard 1 (connection index 2) which has 0 docs
    # This tests that ProcessHybridCursorMappings waits for ALL shards
    sleep_duration = 3  # seconds

    # --------------------------------------------------------------------------
    # Case 1: No timeout - should wait for delayed shard and succeed
    # --------------------------------------------------------------------------
    cmd = ['FT.HYBRID', 'idx',
           'SEARCH', 'unexistent_term',
           'VSIM', '@embedding', '$BLOB', 'KNN', '2', 'K', '10',
           'PARAMS', '2', 'BLOB', query_vec]
    query_result = []

    t_query = threading.Thread(
        target=run_hybrid_query_with_delayed_shard,
        args=(env, cmd, query_result, sleep_duration),
        daemon=True
    )
    t_query.start()
    # Wait for query to complete (should take ~sleep_duration seconds)
    t_query.join(timeout=sleep_duration + 5)

    # Verify query completed
    env.assertEqual(len(query_result), 1,
                    message="Query should have completed")
    if isinstance(query_result[0], Exception):
        env.assertTrue(False, message=f"Query failed with exception: {query_result[0]}")

    # Verify we got 10 results (K=10, and SEARCH has no results)
    result, elapsed = query_result[0]
    total_results = result[1] if protocol == 2 else result['total_results']
    env.assertEqual(total_results, 10)

    env.assertGreaterEqual(
        elapsed, sleep_duration - 1,
        message=f"Query should take ~{sleep_duration} seconds, took {elapsed}")

    # --------------------------------------------------------------------------
    # Case 2: Timeout - ON_TIMEOUT FAIL
    # --------------------------------------------------------------------------
    config_cmd = ['CONFIG', 'SET', 'search-on-timeout', 'FAIL']
    verify_command_OK_on_all_shards(env, *config_cmd)

    cmd = ['FT.HYBRID', 'idx',
           'SEARCH', 'unexistent_term',
           'VSIM', '@embedding', '$BLOB', 'KNN', '2', 'K', '10',
           'PARAMS', '2', 'BLOB', query_vec, 'TIMEOUT', '1']
    query_result = []

    t_query = threading.Thread(
        target=run_hybrid_query_with_delayed_shard,
        args=(env, cmd, query_result, sleep_duration),
        daemon=True
    )
    t_query.start()
    # Wait for query to complete (should take ~sleep_duration seconds)
    t_query.join(timeout=sleep_duration + 5)

    # Verify query completed with timeout error
    env.assertEqual(len(query_result), 1,
                    message="Query should have completed")
    env.assertTrue(isinstance(query_result[0], redis.exceptions.ResponseError),
                   message=f"Expected ResponseError, got {type(query_result[0])}")
    env.assertContains('Timeout limit was reached', str(query_result[0]))

    # --------------------------------------------------------------------------
    # Case 3: Timeout - ON_TIMEOUT RETURN
    # --------------------------------------------------------------------------
    # Note: Even with RETURN policy, cursor mapping timeout returns an error
    # because the cursor mapping phase must complete successfully for the query
    # to proceed.
    # There are no partial results to return at this phase.
    config_cmd = ['CONFIG', 'SET', 'search-on-timeout', 'RETURN']
    verify_command_OK_on_all_shards(env, *config_cmd)

    cmd = ['FT.HYBRID', 'idx',
           'SEARCH', 'unexistent_term',
           'VSIM', '@embedding', '$BLOB', 'KNN', '2', 'K', '10',
           'PARAMS', '2', 'BLOB', query_vec, 'TIMEOUT', '1']
    query_result = []

    t_query = threading.Thread(
        target=run_hybrid_query_with_delayed_shard,
        args=(env, cmd, query_result, sleep_duration),
        daemon=True
    )
    t_query.start()
    t_query.join(timeout=sleep_duration + 5)

    # Verify query completed with timeout error (even with RETURN policy)
    # Cursor mapping timeout is a critical failure - no partial results possible
    env.assertEqual(len(query_result), 1,
                    message="Query should have completed")
    env.assertTrue(isinstance(query_result[0], redis.exceptions.ResponseError),
                   message=f"Expected ResponseError, got {type(query_result[0])}")
    env.assertContains('Timeout limit was reached', str(query_result[0]))


@skip(cluster=False)
def test_hybrid_barrier_waits_for_delayed_shard_resp2():
    _test_hybrid_barrier_waits_for_delayed_shard(2)


@skip(cluster=False)
def test_hybrid_barrier_waits_for_delayed_shard_resp3():
    _test_hybrid_barrier_waits_for_delayed_shard(3)
