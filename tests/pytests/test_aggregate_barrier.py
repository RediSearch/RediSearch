"""
Tests for ShardResponseBarrier functionality in FT.AGGREGATE with WITHCOUNT.

These tests verify that the coordinator properly waits for all shards' first responses
before returning results when WITHCOUNT is specified, ensuring accurate total_results.

Test Categories:
1. Delayed shard responses - verify coordinator waits for all shards
2. Concurrent queries - verify thread safety of atomic operations
3. Error handling - verify behavior when shards return errors
4. Timeout handling - verify barrier respects query timeout
"""

from common import *
import threading
import time
import redis


def setup_index_with_data(env, num_docs, index_name='idx'):
    """Create an index and populate with documents distributed across shards."""
    env.expect('FT.CREATE', index_name, 'ON', 'HASH',
               'SCHEMA', 'title', 'TEXT', 'SORTABLE',
               'num', 'NUMERIC', 'SORTABLE').ok()

    conn = getConnectionByEnv(env)
    for i in range(num_docs):
        conn.execute_command(
            'HSET', f'doc:{index_name}:{i}', 'title', f'hello world {i}', 'num', i)


def _get_total_results(res):
    """Extract total_results from query response (handles both RESP2 and RESP3)."""
    if isinstance(res, dict):
        return res.get('total_results', 0)
    else:
        return res[0] if res else 0

def _get_results(res):
    # Extract the results from the query response
    if isinstance(res, dict):
        return res['results']
    else:
        return res[1:]

#------------------------------------------------------------------------------
# Delayed Shard Response Tests
#------------------------------------------------------------------------------

def call_and_store(func, args, results_list):
    """Helper to call a function and store the result in a list."""
    try:
        result = func(*args)
        results_list.append(result)
    except Exception as e:
        results_list.append(e)


def _run_query_store_result(conn, cmd, result_list):
    """Execute a query and store result or exception in result_list."""
    try:
        result = conn.execute_command(*cmd)
        result_list.append(result)
    except Exception as e:
        result_list.append(e)


def _test_barrier_waits_for_delayed_shard(protocol):
    """
    Test that the barrier waits for all shards before returning results.

    This test uses PAUSE_BEFORE_RP_N to pause query execution on shards,
    then selectively resumes them to simulate one shard being slower.
    We verify that the coordinator waits for all shards and returns
    accurate total_results.
    """
    # WORKERS must be set to use PAUSE_BEFORE_RP_N
    env = Env(moduleArgs='DEFAULT_DIALECT 2 WORKERS 1', protocol=protocol)
    num_docs = 3000 * env.shardsCount
    setup_index_with_data(env, num_docs)

    conn = getConnectionByEnv(env)

    # First verify baseline without pausing
    baseline_res = conn.execute_command('FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', '0', num_docs//2)
    baseline_total = _get_total_results(baseline_res)
    env.assertEqual(baseline_total, num_docs,
                    message=f"Baseline should return {num_docs}, got {baseline_total}")

    # Now test with delayed shard using PAUSE_BEFORE_RP_N
    query_result = []
    query_args = ['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', '0', num_docs//2]

    # Start query in background thread - it will pause on all shards at Index RP
    t_query = threading.Thread(
        target=call_and_store,
        args=(runDebugQueryCommandPauseBeforeRPAfterN,
              (env, query_args, 'Index', 0, ['INTERNAL_ONLY']),
              query_result),
        daemon=True
    )
    t_query.start()

    # Wait for all shards to be paused
    max_wait = 5  # seconds
    start = time.time()
    while time.time() - start < max_wait:
        paused_states = allShards_getIsRPPaused(env)
        if all(state == 1 for state in paused_states):
            break
        time.sleep(0.05)

    # Verify all shards are paused
    paused_states = allShards_getIsRPPaused(env)
    env.assertTrue(all(state == 1 for state in paused_states),
                   message=f"Expected all shards to be paused, got: {paused_states}")

    # Resume all shards except shard 1 (simulating shard 1 being slow)
    # start_shard=2 means resume shards 2, 3, ... (skipping shard 1)
    if env.shardsCount > 1:
        allShards_setPauseRPResume(env, start_shard=2)

    # Small delay to let fast shards respond
    time.sleep(0.1)

    # Query should still be running (waiting for shard 1)
    env.assertTrue(len(query_result) == 0,
                   message="Query should still be waiting for slow shard")

    # Now resume shard 1 (the "slow" shard)
    env.getConnection(1).execute_command(debug_cmd(), 'QUERY_CONTROLLER', 'SET_PAUSE_RP_RESUME')

    # Wait for query to complete
    t_query.join(timeout=5)

    # Verify query completed and returned correct total
    env.assertEqual(len(query_result), 1,
                    message="Query should have completed")

    if isinstance(query_result[0], Exception):
        env.assertTrue(False, message=f"Query failed with: {query_result[0]}")
    else:
        total = _get_total_results(query_result[0])
        env.assertEqual(total, num_docs,
                        message=f"WITHCOUNT after delayed shard should return {num_docs}, got {total}")


@skip(cluster=False)
def test_barrier_waits_for_delayed_shard_resp2():
    _test_barrier_waits_for_delayed_shard(2)


@skip(cluster=False)
def test_barrier_waits_for_delayed_shard_resp3():
    _test_barrier_waits_for_delayed_shard(3)


def _test_barrier_waits_for_delayed_unbalanced_shard(protocol):
    """
    Test that the barrier waits for all shards before returning results.

    Uses sync points to deterministically block one shard's query execution.
    Data is distributed across shards so fast shards start sending data while
    one shard is blocked. We verify that the coordinator waits for all shards
    and returns accurate total_results.

    Shard 0: 5000 docs (responds fast)
    Shard 2: 10000 docs (responds fast)
    Shard 1: 0 docs - blocked at sync point (via env.getConnection(2))
    """
    # WORKERS 1 is required so shard queries run on a worker thread,
    # leaving the main thread free to handle SYNC_POINT commands.
    env = Env(moduleArgs='DEFAULT_DIALECT 2 WORKERS 1', protocol=protocol, shardsCount=3)
    skipIfNoEnableAssert(env)
    conn = getConnectionByEnv(env)

    # Create index
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'title', 'TEXT').ok()

    # Add docs to shard 0 (small shard)
    num_docs0 = 5000
    for i in range(num_docs0):
        conn.execute_command('HSET', f'{{shard-0}}:doc:{i}', 'title', f'doc{i}')

    # Add docs to shard 2 (large shard)
    num_docs2 = 10000
    for i in range(num_docs2):
        conn.execute_command('HSET', f'{{shard-2}}:doc:{i}', 'title', f'doc{i}')

    num_docs = num_docs0 + num_docs2

    # Shard 1 (connection index 2) has 0 docs.
    # We block it at a sync point to test that the coordinator waits for it.
    shard_conn = env.getConnection(2)
    sync_point = 'BeforeFirstRead'

    try:
        # ----------------------------------------------------------------------
        # Case 1: No timeout - verify barrier waits for all shards
        # ----------------------------------------------------------------------
        shard_conn.execute_command(debug_cmd(), 'SYNC_POINT', 'CLEAR')
        shard_conn.execute_command(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point)

        cmd = ['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', 0, 3]
        query_result = []
        t_query = threading.Thread(
            target=_run_query_store_result,
            args=(conn, cmd, query_result),
            daemon=True
        )
        t_query.start()

        # Wait deterministically for shard 1 to reach the sync point
        wait_for_condition(
            lambda: (shard_conn.execute_command(
                debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sync_point) == 1, {}),
            'Timeout waiting for shard to reach sync point')

        # The query should still be in progress (coordinator barrier waiting for shard 1)
        env.assertEqual(len(query_result), 0,
                        message="Query should still be waiting for blocked shard")

        # Release shard 1
        shard_conn.execute_command(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point)

        t_query.join(timeout=10)

        expected = 3
        env.assertEqual(len(query_result), 1,
                        message="Query should have completed")
        env.assertFalse(isinstance(query_result[0], Exception),
                        message=f"Query failed with: {query_result[0]}")
        result = query_result[0]
        total = _get_total_results(result)
        env.assertEqual(
            total, num_docs,
            message=f"expected total_results:{num_docs}, got {total}")
        env.assertEqual(
            len(_get_results(result)), expected,
            message=f"Expected {expected} results, got {len(_get_results(result))}")

        # ----------------------------------------------------------------------
        # Case 2: Timeout - ON_TIMEOUT FAIL
        # ----------------------------------------------------------------------
        shard_conn.execute_command(debug_cmd(), 'SYNC_POINT', 'CLEAR')
        shard_conn.execute_command(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point)

        verify_command_OK_on_all_shards(env, 'CONFIG', 'SET', 'search-on-timeout', 'FAIL')
        cmd = ['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', 0, 2, 'TIMEOUT', 500]
        query_result = []
        t_query = threading.Thread(
            target=_run_query_store_result,
            args=(conn, cmd, query_result),
            daemon=True
        )
        t_query.start()
        # The coordinator should time out while shard 1 is blocked at the sync point
        t_query.join(timeout=10)

        # Verify query completed with a timeout error.
        # The error may come from either the barrier timeout (specific message) or
        # the blocked client timeout (generic message) -- both are valid FAIL behaviors.
        env.assertEqual(len(query_result), 1,
                        message="Query should have completed")
        env.assertTrue(isinstance(query_result[0], redis.exceptions.ResponseError))
        env.assertContains('Timeout', str(query_result[0]))

        # Release shard 1's worker thread and wait for it to finish
        shard_conn.execute_command(debug_cmd(), 'SYNC_POINT', 'CLEAR')
        shard_conn.execute_command(debug_cmd(), 'WORKERS', 'drain')

        # ----------------------------------------------------------------------
        # Case 3: Timeout - ON_TIMEOUT RETURN
        # ----------------------------------------------------------------------
        shard_conn.execute_command(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point)

        verify_command_OK_on_all_shards(env, 'CONFIG', 'SET', 'search-on-timeout', 'RETURN')
        cmd = ['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', 0, 2, 'TIMEOUT', 1]
        query_result = []
        t_query = threading.Thread(
            target=_run_query_store_result,
            args=(conn, cmd, query_result),
            daemon=True
        )
        t_query.start()
        # The coordinator should time out while shard 1 is blocked at the sync point
        t_query.join(timeout=10)

        # Verify the barrier timed out: total_results must be 0.
        # Since RETURN policy has no blocked client timeout (unlike FAIL),
        # the barrier is the sole timeout mechanism. Shards 0 and 2 (with docs)
        # are NOT blocked and respond quickly, so total_results == 0 proves
        # the barrier timed out before accumulating any shard totals.
        expected = 0
        env.assertEqual(len(query_result), 1,
                        message="Query should have completed")
        env.assertFalse(isinstance(query_result[0], Exception),
                        message=f"Query failed with: {query_result[0]}")
        result = query_result[0]
        total = _get_total_results(result)
        env.assertEqual(
            total, 0,
            message=f"expected total_results:0, got {total}")
        env.assertEqual(
            len(_get_results(result)), expected,
            message=f"Expected {expected} results, got {len(_get_results(result))}")
        # Verify we got a timeout warning in the response.
        # RETURN policy has no blocked client timeout, so the barrier is the sole
        # timeout mechanism and the warning is always the standard message.
        if isinstance(result, dict):
            env.assertEqual(result.get('warning', []),
                            ['Timeout limit was reached'])
    finally:
        shard_conn.execute_command(debug_cmd(), 'SYNC_POINT', 'CLEAR')
        shard_conn.execute_command(debug_cmd(), 'WORKERS', 'drain')


@skip(cluster=False)
def test_barrier_waits_for_delayed_unbalanced_shard_resp2():
    _test_barrier_waits_for_delayed_unbalanced_shard(2)


@skip(cluster=False)
def test_barrier_waits_for_delayed_unbalanced_shard_resp3():
    _test_barrier_waits_for_delayed_unbalanced_shard(3)


def _test_barrier_all_shards_delayed_then_resume(protocol):
    """
    Test barrier with all shards paused, then resumed together.

    This verifies the barrier correctly accumulates results when all
    shards respond at roughly the same time.
    """
    env = Env(moduleArgs='DEFAULT_DIALECT 2 WORKERS 1', protocol=protocol)
    num_docs = 100 * env.shardsCount
    setup_index_with_data(env, num_docs)

    query_result = []
    query_args = ['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', '0', '0']

    # Start query - will pause on all shards
    t_query = threading.Thread(
        target=call_and_store,
        args=(runDebugQueryCommandPauseBeforeRPAfterN,
              (env, query_args, 'Index', 0, ['INTERNAL_ONLY']),
              query_result),
        daemon=True
    )
    t_query.start()

    # Wait for all shards to be paused
    max_wait = 5
    start = time.time()
    while time.time() - start < max_wait:
        paused_states = allShards_getIsRPPaused(env)
        if all(state == 1 for state in paused_states):
            break
        time.sleep(0.05)

    # Resume all shards at once
    allShards_setPauseRPResume(env, start_shard=1)

    # Wait for query to complete
    t_query.join(timeout=5)

    # Verify correct result
    env.assertEqual(len(query_result), 1, message="Query should have completed")

    if isinstance(query_result[0], Exception):
        env.assertTrue(False, message=f"Query failed with: {query_result[0]}")
    else:
        total = _get_total_results(query_result[0])
        env.assertEqual(total, num_docs,
                        message=f"WITHCOUNT should return {num_docs}, got {total}")


@skip(cluster=False)
def test_barrier_all_shards_delayed_then_resume_resp2():
    _test_barrier_all_shards_delayed_then_resume(2)


@skip(cluster=False)
def test_barrier_all_shards_delayed_then_resume_resp3():
    _test_barrier_all_shards_delayed_then_resume(3)


#------------------------------------------------------------------------------
# Concurrent Query Tests
#------------------------------------------------------------------------------

def _test_barrier_concurrent_queries(protocol):
    """
    Test thread safety: multiple concurrent WITHCOUNT queries.

    This tests the atomic operations in ShardResponseBarrier when
    multiple queries are running simultaneously.
    """
    env = Env(moduleArgs='DEFAULT_DIALECT 2', protocol=protocol)
    num_docs = 100 * env.shardsCount
    setup_index_with_data(env, num_docs)

    results = []
    errors = []
    num_threads = 5

    def run_query():
        try:
            conn = getConnectionByEnv(env)
            res = conn.execute_command(
                'FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', '0', '0')
            total = _get_total_results(res)
            results.append(total)
        except Exception as e:
            errors.append(str(e))

    # Start multiple concurrent queries
    threads = []
    for _ in range(num_threads):
        t = threading.Thread(target=run_query)
        threads.append(t)
        t.start()

    # Wait for all threads
    for t in threads:
        t.join(timeout=10)

    # Verify no errors
    env.assertEqual(len(errors), 0,
                    message=f"Concurrent queries had errors: {errors}")

    # Verify all queries returned correct total
    for i, total in enumerate(results):
        env.assertEqual(total, num_docs,
                        message=f"Concurrent query {i+1}: expected {num_docs}, got {total}")


@skip(cluster=False)
def test_barrier_concurrent_queries_resp2():
    _test_barrier_concurrent_queries(2)


@skip(cluster=False)
def test_barrier_concurrent_queries_resp3():
    _test_barrier_concurrent_queries(3)


#------------------------------------------------------------------------------
# Error Handling Tests
#------------------------------------------------------------------------------

def _test_barrier_handles_empty_results(protocol):
    """
    Test barrier handles queries that return zero results.
    """
    env = Env(moduleArgs='DEFAULT_DIALECT 2', protocol=protocol)
    num_docs = 100 * env.shardsCount
    setup_index_with_data(env, num_docs)

    conn = getConnectionByEnv(env)

    # Query that matches nothing
    res = conn.execute_command('FT.AGGREGATE', 'idx', 'nonexistent_term_xyz',
                               'WITHCOUNT', 'LIMIT', '0', '0')
    total = _get_total_results(res)

    env.assertEqual(total, 0,
                    message=f"Query with no matches should return 0, got {total}")


@skip(cluster=False)
def test_barrier_handles_empty_results_resp2():
    _test_barrier_handles_empty_results(2)


@skip(cluster=False)
def test_barrier_handles_empty_results_resp3():
    _test_barrier_handles_empty_results(3)


def _test_barrier_handles_single_shard_results(protocol):
    """
    Test barrier works correctly when only one shard has matching docs.
    """
    env = Env(moduleArgs='DEFAULT_DIALECT 2', protocol=protocol)

    # Create index
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH',
               'SCHEMA', 'title', 'TEXT', 'num', 'NUMERIC').ok()

    conn = getConnectionByEnv(env)

    # Add docs with unique identifier to target specific distribution
    # Use a single key pattern that will hash to one shard
    n_docs = 1200
    for i in range(n_docs):
        conn.execute_command('HSET', f'{{singleshard}}:doc:{i}', 'title', 'unique_xyz', 'num', i)

    # Query for the unique term
    res = conn.execute_command('FT.AGGREGATE', 'idx', 'unique_xyz',
                               'WITHCOUNT', 'LIMIT', '0', '0')
    total = _get_total_results(res)

    # Should get exactly n_docs results
    env.assertEqual(total, n_docs,
                    message=f"Expected {n_docs} docs, got {total}")


@skip(cluster=False)
def test_barrier_handles_single_shard_results_resp2():
    _test_barrier_handles_single_shard_results(2)


@skip(cluster=False)
def test_barrier_handles_single_shard_results_resp3():
    _test_barrier_handles_single_shard_results(3)


def _test_barrier_handles_error_in_shard(protocol):
    """
    Test barrier behavior when a shard returns an error.
    """
    env = Env(moduleArgs='DEFAULT_DIALECT 2', protocol=protocol)

    # Create index
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH',
               'SCHEMA', 't', 'TEXT').ok()
    conn = getConnectionByEnv(env)

    conn.execute_command('HSET', 'doc1', 't', 'foo', 'n', 1)
    conn.execute_command('HSET', 'doc1', 't', 'bar', 'n', 2)
    conn.execute_command('HSET', 'doc1', 't', 'baz', 'n', 'z')

    env.expect(
        'FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 2, '@t', '@n',
        'APPLY', '1 / @n', 'AS', 'reciprocal')\
            .error().contains("SEARCH_NUMERIC_VALUE_INVALID")

    env.expect(
        'FT.PROFILE', 'idx', 'AGGREGATE',
        'QUERY', '*', 'WITHCOUNT', 'LOAD', 2, '@t', '@n',
        'APPLY', '1 / @n', 'AS', 'reciprocal')\
            .error().contains("SEARCH_NUMERIC_VALUE_INVALID")


def test_barrier_handles_error_in_shard_resp2():
    _test_barrier_handles_error_in_shard(2)


def test_barrier_handles_error_in_shard_resp3():
    _test_barrier_handles_error_in_shard(3)


#------------------------------------------------------------------------------
# Simulated Shard Timeout Tests (using TIMEOUT_AFTER_N)
#------------------------------------------------------------------------------

def _test_barrier_shard_timeout_with_return_policy(protocol):
    """
    Test barrier behavior when a shard times out with ON_TIMEOUT RETURN policy.

    This test uses TIMEOUT_AFTER_N with INTERNAL_ONLY to simulate a timeout
    on the shards. With RETURN policy, should return partial results.
    """
    env = Env(moduleArgs='DEFAULT_DIALECT 2 ON_TIMEOUT RETURN', protocol=protocol)
    num_docs = 2400 * env.shardsCount
    setup_index_with_data(env, num_docs)

    # Use TIMEOUT_AFTER_N with INTERNAL_ONLY to simulate shard timeout
    query_args = ['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', '0', num_docs]

    # Timeout after 50 results on each shard - with RETURN policy should return partial
    res = runDebugQueryCommandTimeoutAfterN(env, query_args, 50, internal_only=True)

    # With RETURN policy, we should get some results (possibly partial)
    total = _get_total_results(res)

    # Total should be positive but likely less than num_docs due to timeout
    env.assertGreater(total, 0,
                    message=f"With RETURN policy, should get non-negative total, got {total}")

    # Verify we got a timeout warning in the response
    if isinstance(res, dict) and 'warning' in res:
        warnings = res.get('warning', [])
        has_timeout_warning = any('Timeout' in str(w) for w in warnings)
        env.assertTrue(has_timeout_warning,
                       message=f"Expected timeout warning, got: {warnings}")


@skip(cluster=False)
def test_barrier_shard_timeout_with_return_policy_resp2():
    _test_barrier_shard_timeout_with_return_policy(2)


@skip(cluster=False)
def test_barrier_shard_timeout_with_return_policy_resp3():
    _test_barrier_shard_timeout_with_return_policy(3)
