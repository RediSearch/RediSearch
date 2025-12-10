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


def run_query_with_delayed_shard(env, cmd, query_result, sleep_duration):
    """Execute query where shard 1 is delayed (the one with 0 docs)"""
    conn = getConnectionByEnv(env)
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
        # Shards 0 and 2 will respond quickly and start sending data
        # but the coordinator must wait for shard 1 before reporting total
        start_time = time.time()
        result = conn.execute_command(*cmd)
        elapsed = time.time() - start_time

        query_result.append((result, elapsed))
    except Exception as e:
        query_result.append(e)


def _test_barrier_waits_for_delayed_unbalanced_shard(protocol):
    """
    Test that the barrier waits for all shards before returning results.

    This test uses DEBUG SLEEP on a specific shard to simulate delayed responses.
    Data is distributed across shards so fast shards start sending data while
    one shard is delayed. We verify that the coordinator waits for all shards
    and returns accurate total_results.

    Shard 0: 5000 docs (responds fast)
    Shard 2: 10000 docs (responds fast)
    Shard 1: 0 docs - delayed with DEBUG SLEEP (via env.getConnection(2))
    """
    env = Env(moduleArgs='DEFAULT_DIALECT 2', protocol=protocol, shardsCount=3)
    enable_unstable_features(env)
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

    # Now test with delayed shard using DEBUG SLEEP
    # We delay shard 1 (connection index 2) which has 0 docs
    # This tests that the coordinator waits for ALL shards even while
    # receiving lots of data from the fast shards (0 and 2)
    sleep_duration = 3  # seconds

    # --------------------------------------------------------------------------
    # Case 1: No timeout
    # --------------------------------------------------------------------------
    cmd = ['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', 0, 3]
    query_result = []
    # Run the query
    t_query = threading.Thread(
        target=run_query_with_delayed_shard,
        args=(env, cmd, query_result, sleep_duration),
        daemon=True
    )
    t_query.start()
    # Wait for query to complete (should take ~sleep_duration seconds)
    t_query.join(timeout=sleep_duration + 5)

    expected = 3
    # Verify query completed and returned correct total
    env.assertEqual(len(query_result), 1,
                    message="Query should have completed")
    result, elapsed = query_result[0]
    total = _get_total_results(result)
    env.assertEqual(
        total, num_docs,
        message=f"expected total_results:{num_docs}, got {total}")
    env.assertEqual(
        len(_get_results(result)), expected,
        message=f"Expected {expected} results, got {len(_get_results(result))}")
    env.assertGreaterEqual(
        elapsed, sleep_duration - 1,
        message=f"Query should take ~{sleep_duration} seconds, took {elapsed}")

    # --------------------------------------------------------------------------
    # Case 2: Timeout - ON_TIMEOUT FAIL
    # --------------------------------------------------------------------------
    config_commad = [config_cmd(), 'SET', 'ON_TIMEOUT', 'FAIL']
    query_result = []
    verify_command_OK_on_all_shards(env, *config_commad)
    cmd = ['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', 0, 2, 'TIMEOUT', 1]
    # Run the query
    t_query = threading.Thread(
        target=run_query_with_delayed_shard,
        args=(env, cmd, query_result, sleep_duration),
        daemon=True
    )
    t_query.start()
    # Wait for query to complete (should take ~sleep_duration seconds)
    t_query.join(timeout=sleep_duration + 5)

    # Verify query completed with error
    env.assertEqual(len(query_result), 1,
                    message="Query should have completed")
    env.assertTrue(isinstance(query_result[0], redis.exceptions.ResponseError))
    err_msg = "ShardResponseBarrier: Timeout while waiting for first responses from all shards"
    env.assertContains(str(query_result[0]), err_msg)

    # --------------------------------------------------------------------------
    # Case 3: Timeout - ON_TIMEOUT RETURN
    # --------------------------------------------------------------------------
    config_commad = [config_cmd(), 'SET', 'ON_TIMEOUT', 'RETURN']
    query_result = []
    verify_command_OK_on_all_shards(env, *config_commad)
    cmd = ['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', 0, 2, 'TIMEOUT', 1]
    # Run the query
    t_query = threading.Thread(
        target=run_query_with_delayed_shard,
        args=(env, cmd, query_result, sleep_duration),
        daemon=True
    )
    t_query.start()
    # Wait for query to complete (should take ~sleep_duration seconds)
    t_query.join(timeout=sleep_duration + 5)

    expected = 0
    # Verify query completed and returned 0 results
    env.assertEqual(len(query_result), 1,
                    message="Query should have completed")
    result, elapsed = query_result[0]
    total = _get_total_results(result)
    env.assertEqual(
        total, 0,
        message=f"expected total_results:0, got {total}")
    env.assertEqual(
        len(_get_results(result)), expected,
        message=f"Expected {expected} results, got {len(_get_results(result))}")
    # Verify we got a timeout warning in the response
    if isinstance(result, dict):
        env.assertEqual(result.get('warning', []),
                        ['Timeout limit was reached'])


#@skip(cluster=False)
#def test_barrier_waits_for_delayed_unbalanced_shard_resp2():
#    _test_barrier_waits_for_delayed_unbalanced_shard(2)


@skip(cluster=False)
def test_barrier_waits_for_delayed_unbalanced_shard_resp3():
    _test_barrier_waits_for_delayed_unbalanced_shard(3)



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
    enable_unstable_features(env)
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
    enable_unstable_features(env)
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
    enable_unstable_features(env)

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
    enable_unstable_features(env)

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
            .error().contains("Could not convert value to a number")

    env.expect(
        'FT.PROFILE', 'idx', 'AGGREGATE',
        'QUERY', '*', 'WITHCOUNT', 'LOAD', 2, '@t', '@n',
        'APPLY', '1 / @n', 'AS', 'reciprocal')\
            .error().contains("Could not convert value to a number")


def test_barrier_handles_error_in_shard_resp2():
    _test_barrier_handles_error_in_shard(2)


def test_barrier_handles_error_in_shard_resp3():
    _test_barrier_handles_error_in_shard(3)


#------------------------------------------------------------------------------
# Simulated Shard Timeout Tests (using TIMEOUT_AFTER_N)
#------------------------------------------------------------------------------

def _test_barrier_shard_timeout_with_fail_policy(protocol):
    """
    Test barrier behavior when a shard times out with ON_TIMEOUT FAIL policy.

    This test uses TIMEOUT_AFTER_N with INTERNAL_ONLY to simulate a timeout
    on the shards while the coordinator waits for responses.
    With FAIL policy, the query should return a timeout error.
    """
    env = Env(moduleArgs='DEFAULT_DIALECT 2 ON_TIMEOUT FAIL', protocol=protocol)
    enable_unstable_features(env)
    num_docs = 100 * env.shardsCount
    setup_index_with_data(env, num_docs)

    # Use TIMEOUT_AFTER_N with INTERNAL_ONLY to simulate shard timeout
    # This causes shards to timeout after processing N results
    query_args = ['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LIMIT', '0', num_docs]

    # Timeout after 10 results on each shard - with FAIL policy should error
    try:
        runDebugQueryCommandTimeoutAfterN(env, query_args, 10, internal_only=True)
        env.assertTrue(False, message="Expected timeout error, got valid result")
    except Exception as e:
        # Timeout error is expected with FAIL policy
        env.assertContains(str(e), 'Timeout limit was reached')


@skip(cluster=False)
def test_barrier_shard_timeout_with_fail_policy_resp2():
    _test_barrier_shard_timeout_with_fail_policy(2)


@skip(cluster=False)
def test_barrier_shard_timeout_with_fail_policy_resp3():
    _test_barrier_shard_timeout_with_fail_policy(3)


def _test_barrier_shard_timeout_with_return_policy(protocol):
    """
    Test barrier behavior when a shard times out with ON_TIMEOUT RETURN policy.

    This test uses TIMEOUT_AFTER_N with INTERNAL_ONLY to simulate a timeout
    on the shards. With RETURN policy, should return partial results.
    """
    env = Env(moduleArgs='DEFAULT_DIALECT 2 ON_TIMEOUT RETURN', protocol=protocol)
    enable_unstable_features(env)
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
