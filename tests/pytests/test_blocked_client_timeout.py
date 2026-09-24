from common import *
import threading
import psutil
from redis import ConnectionPool, Redis
from redis.backoff import NoBackoff
from redis.exceptions import ConnectionError, ResponseError
from redis.retry import Retry

TIMEOUT_ERROR = "Timeout limit was reached"
TIMEOUT_WARNING = TIMEOUT_ERROR
ON_TIMEOUT_CONFIG = 'search-on-timeout'


def run_cmd_expect_timeout(env, query_args):
    env.expect(*query_args).error().contains(TIMEOUT_ERROR)

def pid_cmd(conn):
    """Get the process ID of a Redis connection."""
    return conn.execute_command('info', 'server')['process_id']


def get_all_shards_pid(env):
    """Get PIDs from all environment shards (excluding the coordinator)."""
    for shardId in range(1, env.shardsCount + 1):
        conn = env.getConnection(shardId)
        yield pid_cmd(conn)

def parse_client_list(client_list_output):
    """Parse the output of CLIENT LIST command into a list of dictionaries.

    Args:
        client_list_output: String output from CLIENT LIST command.

    Returns:
        List of dicts, where each dict represents a client with key-value pairs.
    """
    clients = []
    for line in client_list_output.strip().split('\n'):
        if not line:
            continue
        client = {}
        for pair in line.split(' '):
            if '=' in pair:
                key, value = pair.split('=', 1)
                client[key] = value
        clients.append(client)
    return clients

def is_client_blocked(env, client_id):
    """Check if a client is blocked based on its flags.

    A client is blocked when it has the 'b' flag set, which indicates
    the client is waiting in a blocking operation.

    Args:
        env: The test environment.
        client_id: The client ID to check.

    Returns:
        True if the client is blocked, False otherwise.
    """
    conn = getConnectionByEnv(env)
    output = conn.execute_command('CLIENT', 'LIST', 'ID', client_id)
    clients = parse_client_list(output)
    if not clients:
        return False
    return 'b' in clients[0].get('flags', '')


def wait_for_client_unblocked(env, client_id, timeout=30):
    """Wait for a client to become unblocked."""
    def check_fn():
        blocked = is_client_blocked(env, client_id)
        return not blocked, {'client_id': client_id, 'blocked': blocked}
    wait_for_condition(check_fn, f'Timeout waiting for client {client_id} to be unblocked', timeout)

def get_query_client(conn, query, msg='Client for query not found'):
    """Wait until a client hason a query and return its client id."""
    output = conn.execute_command('CLIENT', 'LIST')
    clients = parse_client_list(output)
    for client in clients:
        if client['cmd'] == query and 'b' in client['flags']:
            return client['id']
    return None

def wait_for_blocked_query_client(env, query, msg='Client for query not found', timeout=30):
    """Wait for a client to become blocked on a query."""
    with TimeLimit(timeout, msg):
        while True:
            client_id = get_query_client(env, query, msg)
            if client_id:
                return client_id
            time.sleep(0.1)

class TestCoordinatorTimeout:
    """Tests for the blocked client timeout mechanism for the coordinator."""

    def __init__(self):
        # Skip if not cluster
        skipTest(cluster=False)

        # Workers are necessary to ensure the query is dispatched before timeout
        self.env = Env(moduleArgs='WORKERS 1', protocol=3)
        self.n_docs = 100

        # Init all shards
        for i in range(1, self.env.shardsCount + 1):
            verify_shard_init(self.env.getConnection(i))

        conn = getConnectionByEnv(self.env)

        # Create an index with prefix filter
        self.env.expect('FT.CREATE', 'idx', 'PREFIX', '1', 'doc', 'SCHEMA', 'name', 'TEXT').ok()

        # Create an index with vector field for FT.HYBRID tests (different prefix)
        self.env.expect(
            'FT.CREATE', 'hybrid_idx', 'PREFIX', '1', 'hybrid_doc', 'SCHEMA',
            'name', 'TEXT',
            'embedding', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2'
        ).ok()

        # Insert documents for regular index
        for i in range(self.n_docs):
            conn.execute_command('HSET', f'doc{i}', 'name', f'hello{i}')

        # Insert documents with vectors for hybrid index
        for i in range(self.n_docs):
            vec = np.array([float(i), float(i)], dtype=np.float32).tobytes()
            conn.execute_command('HSET', f'hybrid_doc{i}', 'name', f'hello{i}', 'embedding', vec)

        # Warmup query
        self.env.expect('FT.SEARCH', 'idx', '*').noError()

        # Warmup hybrid query
        query_vec = np.array([0.0, 0.0], dtype=np.float32).tobytes()

        self.env.expect(
            'FT.HYBRID', 'hybrid_idx',
            'SEARCH', '*',
            'VSIM', '@embedding', '$BLOB',
            'PARAMS', '2', 'BLOB', query_vec
        ).noError()
        self.hybrid_query_vec = query_vec

    def _test_fail_timeout_impl(self, query_args):
        env = self.env

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()

        initial_jobs_done = getWorkersThpoolStats(env)['totalJobsDone']

        coord_pid = pid_cmd(env.con)
        shards_pid = list(get_all_shards_pid(env))
        shards_pid.remove(coord_pid)

        shard_to_pause_pid = shards_pid[0]
        shard_to_pause_p = psutil.Process(shard_to_pause_pid)

        shard_to_pause_p.suspend()
        wait_for_condition(
            lambda: (shard_to_pause_p.status() == psutil.STATUS_STOPPED, {'status': shard_to_pause_p.status()}),
            'Timeout while waiting for shard to pause'
        )

        t_query = threading.Thread(
            target=run_cmd_expect_timeout,
            args=(env, query_args),
            daemon=True
        )
        t_query.start()

        blocked_client_id = wait_for_blocked_query_client(env, query_args[0], f'Client for query {query_args[0]} not found')

        wait_for_condition(
            lambda: (getWorkersThpoolStats(env)['numThreadsAlive'] > 0, {'numThreadsAlive': getWorkersThpoolStats(env)['numThreadsAlive']}),
            'Timeout while waiting for worker to be created'
        )

        wait_for_condition(
            lambda: (getWorkersThpoolStats(env)['totalJobsDone'] > initial_jobs_done, {'totalJobsDone': getWorkersThpoolStats(env)['totalJobsDone']}),
            'Timeout while waiting for worker to finish job'
        )

        env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)

        wait_for_client_unblocked(env, blocked_client_id)

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

        shard_to_pause_p.resume()
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy).ok()

    def test_fail_timeout_search(self):
        self._test_fail_timeout_impl(['FT.SEARCH', 'idx', '*'])

    def test_fail_timeout_profile_search(self):
        self._test_fail_timeout_impl(['FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '*'])

    def test_fail_timeout_profile_hybrid(self):
        self._test_fail_timeout_impl([
            'FT.PROFILE', 'hybrid_idx', 'HYBRID', 'QUERY',
            'SEARCH', '*',
            'VSIM', '@embedding', '$BLOB',
            'PARAMS', '2', 'BLOB', self.hybrid_query_vec
        ])

    def test_fail_timeout_hybrid(self):
        self._test_fail_timeout_impl([
            'FT.HYBRID', 'hybrid_idx',
            'SEARCH', '*',
            'VSIM', '@embedding', '$BLOB',
            'PARAMS', '2', 'BLOB', self.hybrid_query_vec
        ])

    def _test_fail_timeout_before_coord_pickup_impl(self, query_args):
        """Test timeout occurring before coordinator picks up the query job."""
        env = self.env

        # Extract command name for waiting on blocked client
        cmd_name = query_args[0]

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()

        # Pause coordinator thread pool to prevent pickup
        env.expect(debug_cmd(), 'COORD_THREADS', 'PAUSE').ok()
        wait_for_condition(
            lambda: (env.cmd(debug_cmd(), 'COORD_THREADS', 'is_paused') == 1, {}),
            'Timeout while waiting for coordinator threads to pause', timeout=30)

        t_query = threading.Thread(
            target=run_cmd_expect_timeout,
            args=(env, query_args),
            daemon=True
        )
        t_query.start()

        blocked_client_id = wait_for_blocked_query_client(env, cmd_name)

        # Unblock the client to simulate timeout
        env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)

        wait_for_client_unblocked(env, blocked_client_id)

        # Resume coordinator threads and restore config
        env.expect(debug_cmd(), 'COORD_THREADS', 'RESUME').ok()
        wait_for_condition(
            lambda: (env.cmd(debug_cmd(), 'COORD_THREADS', 'is_paused') == 0, {}),
            'Timeout while waiting for coordinator threads to resume', timeout=30)

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy).ok()

    def test_fail_timeout_before_coord_pickup_search(self):
        """Test timeout occurring before coordinator picks up an FT.SEARCH query."""
        self._test_fail_timeout_before_coord_pickup_impl(['FT.SEARCH', 'idx', '*'])

    def test_fail_timeout_before_coord_pickup_hybrid(self):
        """Test timeout occurring before coordinator picks up an FT.HYBRID query."""
        self._test_fail_timeout_before_coord_pickup_impl([
            'FT.HYBRID', 'hybrid_idx',
            'SEARCH', '*',
            'VSIM', '@embedding', '$BLOB',
            'PARAMS', '2', 'BLOB', self.hybrid_query_vec
        ])

    def test_fail_timeout_after_fanout_search(self):
        """Test timeout occurring after the fanout (after query is dispatched to shards - best effort)."""
        env = self.env

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()

        # Get initial jobs done count from all shards
        initial_jobs_done = [stats['totalJobsDone'] for stats in getWorkersThpoolStatsFromAllShards(env)]

        # Pause worker thread pool on all shards first
        verify_command_OK_on_all_shards(env, debug_cmd(), 'WORKERS', 'PAUSE')

        coord_initial_jobs_done = getCoordThpoolStats(env)['totalJobsDone']

        t_query = threading.Thread(
            target=run_cmd_expect_timeout,
            args=(env, ['FT.SEARCH', 'idx', '*']),
            daemon=True
        )
        t_query.start()

        blocked_client_id = wait_for_blocked_query_client(env, 'FT.SEARCH')

        # Verify coordinator fanned out to all shards (jobs done should increase on coordinator by 1)
        wait_for_condition(
            lambda: (getCoordThpoolStats(env)['totalJobsDone'] == coord_initial_jobs_done + 1, {'totalJobsDone': getCoordThpoolStats(env)['totalJobsDone']}),
            'Timeout while waiting for coordinator to dispatch query'
        )

        # Pause coordinator thread pool
        env.expect(debug_cmd(), 'COORD_THREADS', 'PAUSE').ok()
        wait_for_condition(
            lambda: (env.cmd(debug_cmd(), 'COORD_THREADS', 'is_paused') == 1, {}),
            'Timeout while waiting for coordinator threads to pause', timeout=30)

        # Resume worker thread pool on all shards
        verify_command_OK_on_all_shards(env, debug_cmd(), 'WORKERS', 'RESUME')

        # Wait for coordinator to dispatch the query (jobs done should increase on shards)
        def check_jobs_done():
            current_jobs_done = [stats['totalJobsDone'] for stats in getWorkersThpoolStatsFromAllShards(env)]
            done = all(current > initial for current, initial in zip(current_jobs_done, initial_jobs_done))
            return done, {'current_jobs_done': current_jobs_done, 'initial_jobs_done': initial_jobs_done}
        wait_for_condition(check_jobs_done, 'Timeout while waiting for shards to process query', timeout=30)

        # Unblock the client to simulate timeout
        env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)

        wait_for_client_unblocked(env, blocked_client_id)

        # Resume coordinator threads
        env.expect(debug_cmd(), 'COORD_THREADS', 'RESUME').ok()
        wait_for_condition(
            lambda: (env.cmd(debug_cmd(), 'COORD_THREADS', 'is_paused') == 0, {}),
            'Timeout while waiting for coordinator threads to resume', timeout=30)

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy).ok()

    def test_no_timeout(self):
        """
        Test that the fail policy doesn't affect the regular flow when there is no
        timeout (i.e., FT.SEARCH completes normally and gets all expected replies
        from shards).
        """
        env = self.env

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]

        # Test with 'fail' policy
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()
        result = env.cmd('FT.SEARCH', 'idx', '*')
        env.assertEqual(result['total_results'], self.n_docs,
                        message=f"Expected {self.n_docs} total results with 'fail' policy")
        env.assertEqual(result.get('warning', []), [],
                        message="Expected no warning with 'fail' policy")

        # Test FT.PROFILE with 'fail' policy
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()
        result = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '*')
        env.assertContains('Results', result, message="Expected 'Results' key in FT.PROFILE output")
        profile_results = result['Results']
        env.assertEqual(profile_results['total_results'], self.n_docs,
                        message=f"Expected {self.n_docs} total results with 'fail' policy (FT.PROFILE)")
        env.assertEqual(profile_results.get('warning', []), [],
                        message="Expected no warning with 'fail' policy (FT.PROFILE)")


        # Test FT.HYBRID with 'fail' policy
        # Use K=10000, WINDOW=10000, LIMIT=10000 (100^2) to ensure all docs are returned.
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()
        result = env.cmd(
            'FT.HYBRID', 'hybrid_idx',
            'SEARCH', '*',
            'VSIM', '@embedding', '$BLOB',
            'KNN', '2', 'K', '10000',
            'COMBINE', 'RRF', '2', 'WINDOW', '10000',
            'PARAMS', '2', 'BLOB', self.hybrid_query_vec,
            'LIMIT', '0', '10000'
        )
        env.assertEqual(result['total_results'], self.n_docs,
                        message=f"Expected {self.n_docs} total results with 'fail' policy (FT.HYBRID)")
        env.assertEqual(result.get('warning', []), [],
                        message="Expected no warning with 'fail' policy (FT.HYBRID)")

        # Restore previous policy
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy).ok()

    def test_no_timeout_cursor(self):
        """
        Test that FAIL policy doesn't break cursor reads when there is no timeout.
        This verifies that useReplyCallback is properly cleared for cursor reads,
        since cursor reads use BlockCursorClient which has no reply_callback.
        """
        env = self.env

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()

        # Run FT.AGGREGATE with cursor, small chunk size to force multiple reads
        chunk_size = 10
        res, cursor_id = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@name',
                                  'WITHCURSOR', 'COUNT', str(chunk_size))

        # First chunk should have results
        env.assertGreater(len(res), 0, message="Expected results in first chunk")
        env.assertNotEqual(cursor_id, 0, message="Expected non-zero cursor ID for pagination")

        # Read all remaining chunks
        total_results = res['total_results']
        while cursor_id != 0:
            res, cursor_id = env.cmd('FT.CURSOR', 'READ', 'idx', cursor_id)
            total_results += res['total_results']

        env.assertEqual(total_results, self.n_docs,
                        message=f"Expected {self.n_docs} total results across all cursor reads")

        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy).ok()

    def test_shard_timeout_fail(self):
        """Test shard timeout with FAIL policy."""
        env = self.env
        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]

        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail')


        for query_type in ['FT.SEARCH', 'FT.AGGREGATE']:

            # Pause workers on coordinator
            env.expect(debug_cmd(), 'WORKERS', 'pause').ok()

            t_query = threading.Thread(
                target=run_cmd_expect_timeout,
                args=(env, [query_type, 'idx', '*']),
                daemon=True
            )
            t_query.start()

            blocked_client_id = wait_for_blocked_query_client(env, f'_{query_type}', f'Client for query _{query_type} not found')

            # Unblock the client to simulate timeout
            env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)

            wait_for_client_unblocked(env, blocked_client_id)

            t_query.join(timeout=10)
            env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

            # Resume worker threads on all shards
            env.expect(debug_cmd(), 'WORKERS', 'resume').ok()
            env.expect(debug_cmd(), 'WORKERS', 'drain').ok()

        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy)

    def _test_fail_timeout_before_coord_store_impl(self, query_args):
        """Test timeout occurring before coordinator stores results (reply_callback path).

        This tests the FAIL timeout policy when timeout occurs just before the
        background thread stores results for the reply_callback to serialize.
        """
        env = self.env

        # Skip if ENABLE_ASSERT is not enabled
        skipIfNoEnableAssert(env)

        cmd_name = query_args[0]

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()

        # Enable pause before store results
        setPauseBeforeStoreResults(env, True)

        t_query = threading.Thread(
            target=run_cmd_expect_timeout,
            args=(env, query_args),
            daemon=True
        )
        t_query.start()

        blocked_client_id = wait_for_blocked_query_client(env, cmd_name)

        # Wait for the query to be paused before storing results
        wait_for_condition(
            lambda: (getIsStoreResultsPaused(env) == 1, {'paused': getIsStoreResultsPaused(env)}),
            'Timeout while waiting for query to pause before store results'
        )

        # Unblock the client to simulate timeout
        env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)

        wait_for_client_unblocked(env, blocked_client_id)

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

        # Cleanup
        resetStoreResultsDebug(env)
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy).ok()

    def _test_fail_timeout_after_coord_store_impl(self, query_args):
        """Test timeout occurring after coordinator stores results but before reply_callback.

        This tests the FAIL timeout policy when timeout occurs just after the
        background thread stores results, but before the reply_callback is triggered.
        """
        env = self.env

        # Skip if ENABLE_ASSERT is not enabled
        skipIfNoEnableAssert(env)

        cmd_name = query_args[0]

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()

        # Enable pause after store results
        setPauseAfterStoreResults(env, True)

        t_query = threading.Thread(
            target=run_cmd_expect_timeout,
            args=(env, query_args),
            daemon=True
        )
        t_query.start()

        blocked_client_id = wait_for_blocked_query_client(env, cmd_name)

        # Wait for the query to be paused after storing results
        wait_for_condition(
            lambda: (getIsStoreResultsPaused(env) == 1, {'paused': getIsStoreResultsPaused(env)}),
            'Timeout while waiting for query to pause after store results'
        )

        # Unblock the client to simulate timeout
        env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)

        wait_for_client_unblocked(env, blocked_client_id)

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

        # Cleanup
        resetStoreResultsDebug(env)
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy).ok()

    def test_fail_timeout_before_coord_store_hybrid(self):
        """Test timeout occurring before coordinator stores results for FT.HYBRID."""
        self._test_fail_timeout_before_coord_store_impl([
            'FT.HYBRID', 'hybrid_idx',
            'SEARCH', '*',
            'VSIM', '@embedding', '$BLOB',
            'PARAMS', '2', 'BLOB', self.hybrid_query_vec
        ])

    def test_fail_timeout_after_coord_store_hybrid(self):
        """Test timeout occurring after coordinator stores results for FT.HYBRID."""
        self._test_fail_timeout_after_coord_store_impl([
            'FT.HYBRID', 'hybrid_idx',
            'SEARCH', '*',
            'VSIM', '@embedding', '$BLOB',
            'PARAMS', '2', 'BLOB', self.hybrid_query_vec
        ])


class TestCoordinatorReducePause:
    """Tests for timeout during coordinator reduction using the PAUSE_BEFORE_REDUCE mechanism.

    These tests require ENABLE_ASSERT to be enabled in the build.
    """

    def __init__(self):
        # Skip if not cluster
        skipTest(cluster=False)

        # Workers are necessary to ensure the query is dispatched before timeout
        self.env = Env(moduleArgs='WORKERS 1', protocol=3)

        # Skip if ENABLE_ASSERT is not enabled
        skipIfNoEnableAssert(self.env)

        self.n_docs = 100

        # Init all shards
        for i in range(self.env.shardsCount):
            verify_shard_init(self.env.getConnection(i))

        conn = getConnectionByEnv(self.env)

        # Create an index
        self.env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()

        # Insert documents
        for i in range(self.n_docs):
            conn.execute_command('HSET', f'doc{i}', 'name', f'hello{i}')

    def _cleanup_pause_state(self):
        """Clean up the pause state after each test."""
        resetCoordReduceDebug(self.env)

    def test_timeout_fail_during_reduce_before_first(self):
        """Test timeout occurring during reduction before the first result is reduced."""
        env = self.env

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail')

        # Set pause before first result (N=1 means pause before 1st result)
        setPauseBeforeReduce(env, 1)

        t_query = threading.Thread(
            target=run_cmd_expect_timeout,
            args=(env, ['FT.SEARCH', 'idx', '*']),
            daemon=True
        )
        t_query.start()

        # Wait for coordinator to be paused during reduce
        wait_for_condition(
            lambda: (getIsCoordReducePaused(env) == 1, {'paused': getIsCoordReducePaused(env)}),
            'Timeout while waiting for coordinator to pause during reduce'
        )

        blocked_client_id = wait_for_blocked_query_client(env, 'FT.SEARCH')

        # Trigger timeout - the pause loop in the reducer will detect the timeout
        # and auto-break to avoid deadlock with timeout callback
        env.cmd('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT')

        wait_for_client_unblocked(env, blocked_client_id)

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy)
        self._cleanup_pause_state()

    def test_timeout_fail_during_reduce_after_last(self):
        """Test timeout occurring after the last result is reduced (N=-1).

        Note: For N=-1, the pause happens AFTER all results are reduced but BEFORE
        the reply is sent to the client. The client is still blocked at this point.
        """
        env = self.env

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail')

        # Set pause after last result
        setPauseBeforeReduce(env, PAUSE_AFTER_LAST_RESULT)

        t_query = threading.Thread(
            target=run_cmd_expect_timeout,
            args=(env, ['FT.SEARCH', 'idx', '*']),
            daemon=True
        )
        t_query.start()

        # First wait for client to be blocked (query is being processed)
        blocked_client_id = wait_for_blocked_query_client(env, 'FT.SEARCH')

        # Then wait for coordinator to be paused (after all results are reduced)
        wait_for_condition(
            lambda: (getIsCoordReducePaused(env) == 1, {'paused': getIsCoordReducePaused(env)}),
            'Timeout while waiting for coordinator to pause during reduce'
        )

        # Trigger timeout - the pause loop in the reducer will detect the timeout
        # and auto-break to avoid deadlock with timeout callback
        env.cmd('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT')

        wait_for_client_unblocked(env, blocked_client_id)

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy)
        self._cleanup_pause_state()


class TestShardTimeout:
    """Tests for the blocked client timeout mechanism for shards."""
    def __init__(self):
        # Skip if cluster
        skipTest(cluster=True)

        self.env = Env(protocol=3, moduleArgs='WORKERS 1 TIMEOUT 0')
        self.n_docs = 100

        conn = getConnectionByEnv(self.env)

        # Create an index
        self.env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()

        # Insert documents
        for i in range(self.n_docs):
            conn.execute_command('HSET', f'doc{i}', 'name', f'hello{i}')

    def test_shard_timeout_fail(self):
        """Test shard timeout with FAIL policy."""
        env = self.env

        # Set timeout policy to FAIL
        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()

        # Pause worker thread

        for query_type in ['FT.SEARCH', 'FT.AGGREGATE']:

            env.expect(debug_cmd(), 'WORKERS', 'pause').ok()

            # Run a query that will be blocked
            t_query = threading.Thread(
                target=run_cmd_expect_timeout,
                args=(env, [query_type, 'idx', '*']),
                daemon=True
            )
            t_query.start()

            # Some cases cause the query client to change, so we check the client id explicitly
            blocked_client_id = wait_for_blocked_query_client(env, query_type, f'Client for query {query_type} not found')

            # Unblock the client to simulate timeout
            env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)

            wait_for_client_unblocked(env, blocked_client_id)

            t_query.join(timeout=10)
            env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

            # Resume worker thread
            env.expect(debug_cmd(), 'WORKERS', 'resume').ok()
            env.expect(debug_cmd(), 'WORKERS', 'drain').ok()

        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy).ok()

    def test_shard_timeout_fail_in_pipeline(self):
        """Test shard timeout with FAIL policy when query is paused inside the pipeline.

        This test uses PAUSE_BEFORE_RP_N to pause the query inside the pipeline,
        then triggers a timeout via CLIENT UNBLOCK to verify the blocked client
        timeout mechanism works correctly when the query is mid-execution.
        """
        env = self.env

        # Set timeout policy to FAIL
        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()


        # Run a query that will be blocked
        # Using PAUSE_BEFORE_RP_N to pause inside the pipeline
        for query_type in ['FT.SEARCH', 'FT.AGGREGATE']:

            query_args = [query_type, 'idx', '*']
            debug_args = ['PAUSE_BEFORE_RP_N', 'Index', 0]
            if query_type == 'FT.AGGREGATE':
                debug_args.append('INTERNAL_ONLY')
            if query_type == 'FT.SEARCH':
                # NOCONTENT is required to not use SAFE-LOADER
                query_args.append('NOCONTENT')
            t_query = threading.Thread(
                target=run_cmd_expect_timeout,
                args=(env, [debug_cmd()] + parseDebugQueryCommandArgs(query_args, debug_args)),
                daemon=True
            )
            t_query.start()

            # Some cases cause the query client to change, so we check the client id explicitly
            blocked_client_id = wait_for_blocked_query_client(env, f'{debug_cmd()}|{query_type}', f'Client for query {debug_cmd()}|{query_type} not found')

            # Wait for the query to be paused inside the pipeline
            wait_for_condition(
                lambda: (getIsRPPaused(env) == 1, {'paused': getIsRPPaused(env)}),
                'Timeout while waiting for query to pause in pipeline'
            )


            # Unblock the client to simulate timeout
            env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)

            wait_for_client_unblocked(env, blocked_client_id)

            t_query.join(timeout=10)
            env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

            # Resume the paused RP to clean up (the query already timed out, but we need to resume)
            setPauseRPResume(env)
            # Wait for RP to resume
            wait_for_condition(
                lambda: (getIsRPPaused(env) == 0, {'paused': getIsRPPaused(env)}),
                'Timeout while waiting for query to resume in pipeline'
            )
            env.expect(debug_cmd(), 'WORKERS', 'drain').ok()

        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy).ok()

    def _test_fail_timeout_reply_boundary_impl(self, query_args, before):
        """Test timeout occurring before/after the worker encodes a FAIL reply in standalone."""
        env = self.env

        # Skip if ENABLE_ASSERT is not enabled
        skipIfNoEnableAssert(env)

        cmd_name = query_args[0]
        point = 'BeforeBackgroundReplyEncode' if before else 'AfterBackgroundReplyEncode'

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()

        # Pause the worker at the encoding boundary
        env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', point).ok()

        t_query = threading.Thread(
            target=run_cmd_expect_timeout,
            args=(env, query_args),
            daemon=True
        )
        try:
            t_query.start()

            blocked_client_id = wait_for_blocked_query_client(env, cmd_name)

            # Wait for the worker to reach the encoding boundary
            wait_for_condition(
                lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', point) == 1, {}),
                f'Timeout while waiting for {cmd_name} to pause at {point}'
            )

            # Unblock the client to simulate timeout
            env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)

            wait_for_client_unblocked(env, blocked_client_id)

            t_query.join(timeout=10)
            env.assertFalse(t_query.is_alive(), message="Query thread should have finished")
        finally:
            # Cleanup
            env.expect(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point).ok()
            t_query.join(timeout=10)
            env.expect(debug_cmd(), 'WORKERS', 'drain').ok()
            env.expect(debug_cmd(), 'SYNC_POINT', 'CLEAR').ok()
            env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy).ok()

    def test_fail_timeout_before_encode_search(self):
        """Test timeout occurring before encoding results for FT.SEARCH in standalone."""
        self._test_fail_timeout_reply_boundary_impl(['FT.SEARCH', 'idx', '*'], True)

    def test_fail_timeout_before_encode_aggregate(self):
        """Test timeout occurring before encoding results for FT.AGGREGATE in standalone."""
        self._test_fail_timeout_reply_boundary_impl(['FT.AGGREGATE', 'idx', '*'], True)

    def test_fail_timeout_after_encode_search(self):
        """Test timeout occurring after encoding results for FT.SEARCH in standalone."""
        self._test_fail_timeout_reply_boundary_impl(['FT.SEARCH', 'idx', '*'], False)

    def test_fail_timeout_after_encode_aggregate(self):
        """Test timeout occurring after encoding results for FT.AGGREGATE in standalone."""
        self._test_fail_timeout_reply_boundary_impl(['FT.AGGREGATE', 'idx', '*'], False)

    def test_no_timeout_cursor(self):
        """
        Test that FAIL policy doesn't break cursor reads when there is no timeout.
        This verifies that useReplyCallback is properly cleared for cursor reads,
        since cursor reads use BlockCursorClient which has no reply_callback.
        """
        env = self.env

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()

        # Run FT.AGGREGATE with cursor, small chunk size to force multiple reads
        chunk_size = 10
        res, cursor_id = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@name',
                                  'WITHCURSOR', 'COUNT', str(chunk_size))

        # First chunk should have results
        env.assertGreater(len(res), 0, message="Expected results in first chunk")
        env.assertNotEqual(cursor_id, 0, message="Expected non-zero cursor ID for pagination")

        # Read all remaining chunks
        total_results = res['total_results']
        while cursor_id != 0:
            res, cursor_id = env.cmd('FT.CURSOR', 'READ', 'idx', cursor_id)
            total_results += res['total_results']

        env.assertEqual(total_results, self.n_docs,
                        message=f"Expected {self.n_docs} total results across all cursor reads")

        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy).ok()

    def test_cursor_read_after_initial_timeout(self):
        """
        Test FT.AGGREGATE WITHCURSOR when the initial request times out,
        then attempting to read from the cursor after timeout.

        This verifies that after a timeout on the initial cursor request
        in standalone mode, proper cleanup occurs and subsequent cursor
        reads handle the state correctly.
        """
        env = self.env

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()

        # Pause worker thread pool
        env.expect(debug_cmd(), 'WORKERS', 'pause').ok()

        # Run FT.AGGREGATE with cursor in a thread
        t_query = threading.Thread(
            target=run_cmd_expect_timeout,
            args=(env, ['FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@name',
                        'WITHCURSOR', 'COUNT', '10']),
            daemon=True
        )
        t_query.start()

        blocked_client_id = wait_for_blocked_query_client(env, 'FT.AGGREGATE')

        # Unblock the client to simulate timeout
        env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)

        wait_for_client_unblocked(env, blocked_client_id)

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

        # Resume worker threads and drain
        env.expect(debug_cmd(), 'WORKERS', 'resume').ok()
        env.expect(debug_cmd(), 'WORKERS', 'drain').ok()

        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy).ok()


# Background FAIL serialization: dispatch, reply parity, errors, and timeout cleanup.
# FT.CURSOR READ already encodes on workers without a blocked-client deadline on
# this branch, so READ is covered for flag reset and cursor cleanup only.

def _background_fail_cursor_dispatch_switches(protocol):
    env = Env(protocol=protocol, moduleArgs='WORKERS 1 TIMEOUT 0')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE').ok()
    for n in range(8):
        env.cmd('HSET', f'doc:{n}', 'n', n)
    waitForIndex(env, 'idx')

    def values(chunk):
        if protocol == 3:
            return [int(row['extra_attributes']['n']) for row in chunk['results']]
        return [int(to_dict(row)['n']) for row in chunk[1:]]

    # The global policy changes after creation. Each cursor must retain its
    # captured policy while moving between background and inline dispatches.
    cases = [('FAIL', 'RETURN', 1), ('FAIL', 'RETURN', 0), ('RETURN', 'FAIL', 1)]
    for policy, next_policy, initial_workers in cases:
        env.expect('FT.CONFIG', 'SET', 'WORKERS', initial_workers).ok()
        env.expect('FT.CONFIG', 'SET', 'ON_TIMEOUT', policy).ok()
        chunk, cursor = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@n',
                                'SORTBY', 2, '@n', 'ASC',
                                'WITHCURSOR', 'COUNT', 2)
        env.assertEqual(values(chunk), [0, 1])
        env.assertNotEqual(cursor, 0)
        env.expect('FT.CONFIG', 'SET', 'ON_TIMEOUT', next_policy).ok()
        if initial_workers:
            dispatch_workers = [0, 1, 0, 1]
        else:
            dispatch_workers = [1, 0, 1, 0]
        for workers, expected in zip(dispatch_workers, [[2, 3], [4, 5], [6, 7], []]):
            env.expect('FT.CONFIG', 'SET', 'WORKERS', workers).ok()
            chunk, cursor = env.cmd('FT.CURSOR', 'READ', 'idx', cursor, 'COUNT', 2)
            label = (f'RESP{protocol}, captured={policy}, current={next_policy}, '
                     f'initial_workers={initial_workers}, workers={workers}')
            env.assertEqual(values(chunk), expected, message=label)
            if expected:
                env.assertNotEqual(cursor, 0, message=label)
            else:
                env.assertEqual(cursor, 0, message=label)
            env.assertTrue(env.cmd('PING'))


@skip(cluster=True)
def test_background_fail_cursor_dispatch_resp2():
    _background_fail_cursor_dispatch_switches(2)


@skip(cluster=True)
def test_background_fail_cursor_dispatch_resp3():
    _background_fail_cursor_dispatch_switches(3)


def _compare_background_fail_replies(protocol):
    env = Env(protocol=protocol, moduleArgs='WORKERS 1 ON_TIMEOUT FAIL DEFAULT_DIALECT 2')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'text', 'TEXT', 'n', 'NUMERIC', 'SORTABLE').ok()
    for n in range(12):
        env.cmd('HSET', f'doc:{n:02}', 'text', 'hello redis', 'n', n,
                'binary', 'prefix\x00suffix\r\n', 'wide', 'w' * 4096)
    waitForIndex(env, 'idx')
    queries = [
        ['FT.SEARCH', 'idx', '*', 'SORTBY', 'n', 'ASC', 'WITHSORTKEYS', 'LIMIT', 0, 12],
        ['FT.SEARCH', 'idx', 'hello', 'SCORER', 'TFIDF', 'WITHSCORES', 'EXPLAINSCORE',
         'SORTBY', 'n', 'ASC', 'LIMIT', 0, 12],
        ['FT.SEARCH', 'idx', '*', 'NOCONTENT', 'LIMIT', 0, 0],
        ['FT.SEARCH', 'idx', 'missingword', 'LIMIT', 0, 12],
        ['FT.AGGREGATE', 'idx', '*', 'LOAD', 3, '@n', '@binary', '@wide',
         'SORTBY', 2, '@n', 'ASC'],
        ['FT.AGGREGATE', 'idx', '*', 'GROUPBY', 0, 'REDUCE', 'COUNT', 0, 'AS', 'count'],
        ['FT.AGGREGATE', 'idx', 'missingword'],
    ]
    for timeout in (0, 10000):
        for query in queries:
            command = query + ['TIMEOUT', timeout]
            env.expect(config_cmd(), 'SET', 'WORKERS', 0).ok()
            expected = env.cmd(*command)
            env.expect(config_cmd(), 'SET', 'WORKERS', 1).ok()
            env.assertEqual(env.cmd(*command), expected, message=str(command))
            env.assertTrue(env.cmd('PING'))

    # PROFILE uses the same rows and reply framing while timing and processor
    # names intentionally reflect the different execution context.
    for kind, args in [('SEARCH', ['SORTBY', 'n', 'ASC', 'LIMIT', 0, 12]),
                       ('AGGREGATE', ['LOAD', 1, '@n', 'SORTBY', 2, '@n', 'ASC'])]:
        command = ['FT.PROFILE', 'idx', kind, 'QUERY', '*', 'TIMEOUT', 0] + args
        env.expect(config_cmd(), 'SET', 'WORKERS', 0).ok()
        expected = env.cmd(*command)
        env.expect(config_cmd(), 'SET', 'WORKERS', 1).ok()
        actual = env.cmd(*command)
        if protocol == 3:
            env.assertEqual(actual['Results'], expected['Results'])
            env.assertTrue(bool(actual['Profile']))
        else:
            env.assertEqual(actual[0], expected[0])
            env.assertTrue(bool(actual[1]))
        env.assertTrue(env.cmd('PING'))


@skip(cluster=True)
def test_background_fail_reply_parity_resp2():
    _compare_background_fail_replies(2)


@skip(cluster=True)
def test_background_fail_reply_parity_resp3():
    _compare_background_fail_replies(3)


@skip(cluster=True)
def test_background_fail_cursor_protocol_switch():
    env = Env(protocol=2, moduleArgs='WORKERS 1 TIMEOUT 0 ON_TIMEOUT FAIL')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE').ok()
    for n in range(6):
        env.cmd('HSET', f'doc:{n}', 'n', n)
    waitForIndex(env, 'idx')
    _, cursor = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@n',
                        'SORTBY', 2, '@n', 'ASC', 'WITHCURSOR', 'COUNT', 1)
    base = env.getConnection().connection_pool
    for expected, protocol in enumerate((3, 2, 3, 2, 3), start=1):
        pool = ConnectionPool(connection_class=base.connection_class,
                              **dict(base.connection_kwargs, protocol=protocol))
        client = Redis(connection_pool=pool)
        try:
            chunk, cursor = client.execute_command('FT.CURSOR', 'READ', 'idx', cursor, 'COUNT', 1)
            if protocol == 3:
                value = chunk['results'][0]['extra_attributes']['n']
            else:
                value = chunk[1][1]
            env.assertEqual(int(value), expected)
            env.assertTrue(client.ping())
        finally:
            client.close()
            pool.disconnect()
    chunk, cursor = env.cmd('FT.CURSOR', 'READ', 'idx', cursor, 'COUNT', 1)
    env.assertEqual(chunk, [0])
    env.assertEqual(cursor, 0)


def _assert_background_fail_late_error(env, command):
    # A late expression error must replace the complete chunk, including rows
    # already collected successfully. An array containing an error is not enough.
    env.expect(*command).error().contains('SEARCH_NUMERIC_VALUE_INVALID')
    env.assertTrue(env.cmd('PING'))
    info = env.cmd('FT.INFO', 'idx')
    if isinstance(info, list):
        info = to_dict(info)
    stats = info['cursor_stats']
    if isinstance(stats, list):
        stats = to_dict(stats)
    env.assertEqual(stats['index_total'], 0)


def _test_background_fail_late_expression_errors(protocol):
    env = Env(protocol=protocol,
              moduleArgs='WORKERS 1 ON_TIMEOUT FAIL DEFAULT_DIALECT 2')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'position', 'NUMERIC', 'SORTABLE').ok()
    for position, value in enumerate(('1', '2', 'not-a-number')):
        env.cmd('HSET', f'doc:{position}', 'position', position, 'value', value)
    waitForIndex(env, 'idx')

    # Sorting before APPLY/FILTER makes the successful rows precede the invalid
    # value regardless of iterator ordering. The value is deliberately unindexed
    # so indexing accepts the document and expression evaluation detects it.
    query = ['FT.AGGREGATE', 'idx', '*', 'TIMEOUT', 0,
             'LOAD', 1, '@value', 'SORTBY', 2, '@position', 'ASC']
    expressions = [
        ['APPLY', '@value + 1', 'AS', 'incremented'],
        ['FILTER', '(@value + 1) > 0'],
    ]

    # ON_OOM RETURN/IGNORE must not weaken ON_TIMEOUT FAIL's collection path or
    # turn an expression failure into partial success. No actual OOM is needed.
    for oom_policy in ('FAIL', 'RETURN', 'IGNORE'):
        env.expect(config_cmd(), 'SET', 'ON_OOM', oom_policy).ok()
        for expression in expressions:
            command = query + expression
            _assert_background_fail_late_error(env, command)
            _assert_background_fail_late_error(env, command + ['WITHCURSOR', 'COUNT', 3])

            # Commit one successful chunk, then fail after one valid row in READ.
            rows, cursor = env.cmd(*(command + ['WITHCURSOR', 'COUNT', 1]))
            env.assertNotEqual(cursor, 0)
            if protocol == 3:
                env.assertEqual(len(rows['results']), 1)
                first = rows['results'][0]['extra_attributes']
            else:
                env.assertEqual(len(rows), 2)
                first = to_dict(rows[1])
            env.assertEqual(str(first['value']), '1')
            _assert_background_fail_late_error(env, ['FT.CURSOR', 'READ', 'idx', cursor, 'COUNT', 2])
            env.expect('FT.CURSOR', 'READ', 'idx', cursor).error().contains('Cursor not found')


@skip(cluster=True)
def test_background_fail_late_errors_resp2():
    _test_background_fail_late_expression_errors(2)


@skip(cluster=True)
def test_background_fail_late_errors_resp3():
    _test_background_fail_late_expression_errors(3)


def _background_fail_cursor_total(env, idx='idx'):
    # Do not silently accept missing stats: cleanup is part of the assertion.
    return int(to_dict(to_dict(env.cmd('FT.INFO', idx))['cursor_stats'])['global_total'])


def _wait_for_background_fail_workers(env):
    def idle():
        stats = to_dict(env.cmd(debug_cmd(), 'WORKERS', 'STATS'))
        return (stats['numJobsInProgress'] == 0 and stats['totalPendingJobs'] == 0, stats)
    wait_for_condition(idle, 'Cancelled encoding worker did not finish', timeout=5)


def _exercise_background_fail_timeout(stage):
    point = f'{stage}BackgroundReplyEncode'
    for protocol in (2, 3):
        env = Env(protocol=protocol, moduleArgs='WORKERS 1 TIMEOUT 0 ON_TIMEOUT FAIL NOGC')
        skipIfNoEnableAssert(env)
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT', 'SORTABLE').ok()
        for i in range(8):
            env.cmd('HSET', f'doc:{i}', 'name', f'hello{i}')

        # FT.CURSOR READ has no blocked-client deadline on this branch.
        for kind in ('search', 'aggregate', 'cursor_initial'):
            # The real blocked-client deadline expires while the hook remains armed.
            timeout = 1000
            aggregate = ['FT.AGGREGATE', 'idx', '*', 'TIMEOUT', timeout,
                         'LOAD', '1', '@name', 'LIMIT', 0, 8]
            baseline = _background_fail_cursor_total(env)
            if kind == 'search':
                command = ['FT.SEARCH', 'idx', '*', 'TIMEOUT', timeout]
            elif kind == 'aggregate':
                command = aggregate
            else:
                command = [*aggregate, 'WITHCURSOR', 'COUNT', 2]

            original = env.getConnection().connection_pool
            kwargs = dict(original.connection_kwargs, retry=Retry(NoBackoff(), 0),
                          socket_timeout=5)
            pool = ConnectionPool(connection_class=original.connection_class, **kwargs)
            client = Redis(connection_pool=pool, single_connection_client=True)
            client_id = client.client_id()
            results, errors = [], []

            def query():
                try:
                    results.append(client.execute_command(*command))
                except Exception as error:
                    errors.append(error)

            thread = threading.Thread(target=query, daemon=True)
            env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', point).ok()
            try:
                thread.start()
                wait_for_condition(
                    lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', point) == 1,
                             {'results': results, 'errors': errors}),
                    f'{kind} never reached {point}', timeout=5)
                thread.join(timeout=5)
                env.assertFalse(thread.is_alive(), message=f'{kind}: cancellation waited for worker')
                env.assertEqual(results, [])
                env.assertEqual(len(errors), 1, message=errors)
                env.assertTrue(isinstance(errors[0], ResponseError), message=errors)
                env.assertContains('Timeout limit was reached', str(errors[0]))
                # Same connection must receive its PONG, never buffered success
                # bytes, both before and after worker exit.
                env.assertTrue(client.ping())
                env.assertEqual(client.client_id(), client_id)
                env.assertTrue(env.cmd('PING'))
                env.assertEqual(env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', point), 1)
                env.expect(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point).ok()
                _wait_for_background_fail_workers(env)
                wait_for_condition(
                    lambda: (_background_fail_cursor_total(env) == baseline, {'total': _background_fail_cursor_total(env)}),
                    f'{kind}: abandoned reply leaked a cursor', timeout=5)
                env.assertTrue(client.ping())
                env.assertEqual(client.client_id(), client_id)
                # A subsequent worker query also detects stale per-dispatch
                # state and gives Redis a turn to process completion callbacks.
                result = env.cmd('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'TIMEOUT', 0)
                env.assertEqual(result[0] if protocol == 2 else result['total_results'], 8)
                env.assertTrue(client.ping())
            finally:
                # Never require an index writer to release a hook: these hooks
                # deliberately keep the worker's spec read lock held.
                env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point)
                thread.join(timeout=5)
                client.close()
                pool.disconnect()
                _wait_for_background_fail_workers(env)
                env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')


@skip(cluster=True)
def test_timeout_before_background_reply():
    _exercise_background_fail_timeout('Before')


@skip(cluster=True)
def test_timeout_during_background_reply():
    _exercise_background_fail_timeout('During')


@skip(cluster=True)
def test_timeout_after_background_reply():
    _exercise_background_fail_timeout('After')


@skip(cluster=True)
def test_background_reply_output_limit_cleanup():
    for protocol in (2, 3):
        env = Env(protocol=protocol, moduleArgs='WORKERS 1 TIMEOUT 0 ON_TIMEOUT FAIL NOGC')
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC').ok()
        for i in range(128):
            env.cmd('HSET', f'doc:{i}', 'n', i, 'payload', 'x' * 16384)
        env.expect('CONFIG', 'SET', 'client-output-buffer-limit', 'normal 65536 0 0').ok()
        commands = [
            ['FT.SEARCH', 'idx', '*', 'TIMEOUT', 0, 'LIMIT', 0, 128],
            ['FT.AGGREGATE', 'idx', '*', 'TIMEOUT', 0, 'LOAD', 1, '@payload'],
        ]
        for command in commands:
            original = env.getConnection().connection_pool
            pool = ConnectionPool(connection_class=original.connection_class,
                                  **dict(original.connection_kwargs,
                                         retry=Retry(NoBackoff(), 0), socket_timeout=5))
            client = Redis(connection_pool=pool, single_connection_client=True)
            try:
                client_id = client.client_id()
                try:
                    client.execute_command(*command)
                    env.assertTrue(False, message='Oversized reply should close the client')
                except ConnectionError:
                    pass
                _wait_for_background_fail_workers(env)
                env.assertFalse(env.cmd('CLIENT', 'LIST', 'ID', client_id))
                env.assertTrue(env.cmd('PING'))
            finally:
                client.close()
                pool.disconnect()
        env.expect('CONFIG', 'SET', 'client-output-buffer-limit', 'normal 0 0 0').ok()


def _exercise_background_fail_queued_cleanup(drop_index):
    for protocol in (2, 3):
        env = Env(protocol=protocol, moduleArgs='WORKERS 1 TIMEOUT 0 ON_TIMEOUT FAIL NOGC')
        # Keep an index for observing global cursor cleanup after idx is dropped.
        env.expect('FT.CREATE', 'observer', 'PREFIX', 1, 'observer:',
                   'SCHEMA', 'name', 'TEXT').ok()

        # FT.CURSOR READ has no blocked-client deadline on this branch, so it
        # is exercised only for the dropped-index cleanup.
        kinds = ('search', 'cursor_initial', 'cursor_read') if drop_index else ('search', 'cursor_initial')
        for kind in kinds:
            env.expect('FT.CREATE', 'idx', 'PREFIX', 1, 'doc:',
                       'SCHEMA', 'name', 'TEXT', 'SORTABLE').ok()
            for i in range(4):
                env.cmd('HSET', f'doc:{i}', 'name', f'hello{i}')
            waitForIndex(env, 'idx')
            baseline = _background_fail_cursor_total(env, 'observer')
            timeout = 0 if drop_index else 1000
            aggregate = ['FT.AGGREGATE', 'idx', '*', 'TIMEOUT', timeout,
                         'WITHCURSOR', 'COUNT', 1]
            cursor_id = None
            if kind == 'search':
                command = ['FT.SEARCH', 'idx', '*', 'TIMEOUT', timeout]
            elif kind == 'cursor_initial':
                command = aggregate
            else:
                _, cursor_id = env.cmd(*aggregate)
                env.assertNotEqual(cursor_id, 0)
                command = ['FT.CURSOR', 'READ', 'idx', cursor_id]

            original = env.getConnection().connection_pool
            kwargs = dict(original.connection_kwargs, retry=Retry(NoBackoff(), 0),
                          socket_timeout=5)
            pool = ConnectionPool(connection_class=original.connection_class, **kwargs)
            client = Redis(connection_pool=pool, single_connection_client=True)
            client_id = client.client_id()
            results, errors = [], []

            def query():
                try:
                    results.append(client.execute_command(*command))
                except Exception as error:
                    errors.append(error)

            thread = threading.Thread(target=query, daemon=True)
            env.expect(debug_cmd(), 'WORKERS', 'PAUSE').ok()
            paused = True
            try:
                thread.start()
                wait_for_condition(
                    lambda: (to_dict(env.cmd(debug_cmd(), 'WORKERS', 'STATS'))['totalPendingJobs'] > 0,
                             {'results': results, 'errors': errors}),
                    f'{kind} did not enter the worker queue', timeout=5)
                if drop_index:
                    env.expect('FT.DROPINDEX', 'idx').ok()
                else:
                    # The timeout must reply while the prepared job remains queued.
                    thread.join(timeout=5)
                    env.assertFalse(thread.is_alive())
                    env.assertEqual(len(errors), 1, message=errors)
                    env.assertContains('Timeout limit was reached', str(errors[0]))
                    env.assertTrue(client.ping())
                env.expect(debug_cmd(), 'WORKERS', 'RESUME').ok()
                paused = False
                thread.join(timeout=5)
                env.assertFalse(thread.is_alive())
                env.assertEqual(results, [])
                env.assertEqual(len(errors), 1, message=errors)
                env.assertTrue(isinstance(errors[0], ResponseError), message=errors)
                if drop_index:
                    env.assertContains('dropped', str(errors[0]))

                def cleaned_up():
                    stats = to_dict(env.cmd(debug_cmd(), 'WORKERS', 'STATS'))
                    return (stats['numJobsInProgress'] == 0 and stats['totalPendingJobs'] == 0
                            and _background_fail_cursor_total(env, 'observer') == baseline, stats)

                wait_for_condition(cleaned_up, f'{kind}: queued job leaked a cursor', timeout=5)
                env.assertTrue(client.ping())
                env.assertEqual(client.client_id(), client_id)
                if cursor_id is not None:
                    env.expect('FT.CURSOR', 'READ', 'observer', cursor_id).error().contains('Cursor not found')
            finally:
                if paused:
                    env.cmd(debug_cmd(), 'WORKERS', 'RESUME')
                thread.join(timeout=5)
                client.close()
                pool.disconnect()
            if not drop_index:
                env.expect('FT.DROPINDEX', 'idx').ok()


@skip(cluster=True)
def test_background_fail_timeout_while_queued():
    _exercise_background_fail_queued_cleanup(False)


@skip(cluster=True)
def test_background_fail_index_dropped_while_queued():
    _exercise_background_fail_queued_cleanup(True)
