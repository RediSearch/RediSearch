from common import *
from test_info_modules import (
    info_modules_to_dict,
    WARN_ERR_SECTION, COORD_WARN_ERR_SECTION,
    TIMEOUT_ERROR_SHARD_METRIC, TIMEOUT_WARNING_SHARD_METRIC,
    TIMEOUT_ERROR_COORD_METRIC, TIMEOUT_WARNING_COORD_METRIC,
    _verify_metrics_not_changed,
)
import threading
import psutil

TIMEOUT_ERROR = "Timeout limit was reached"
TIMEOUT_WARNING = TIMEOUT_ERROR
ON_TIMEOUT_CONFIG = 'search-on-timeout'


def run_cmd_expect_timeout(env, query_args):
    env.expect(*query_args).error().contains(TIMEOUT_ERROR)


def debug_print_hybrid_clients(env, label=""):
    """Debug helper: Print clients with HYBRID commands from coordinator and all shards.

    Filters and prints only clients whose last command contains 'HYBRID' (FT.HYBRID or _FT.HYBRID).
    """
    prefix = f"[{label}] " if label else ""

    # Check coordinator
    try:
        conn = getConnectionByEnv(env)
        output = conn.execute_command('CLIENT', 'LIST')
        clients = parse_client_list(output)
        hybrid_clients = [c for c in clients if 'HYBRID' in c.get('cmd', '').upper()]
        if hybrid_clients:
            env.debugPrint(f"{prefix}Coordinator HYBRID clients:", force=True)
            for c in hybrid_clients:
                env.debugPrint(f"  id={c.get('id')} cmd={c.get('cmd')} flags={c.get('flags')}", force=True)
        else:
            env.debugPrint(f"{prefix}Coordinator: No HYBRID clients found", force=True)
    except Exception as e:
        env.debugPrint(f"{prefix}Coordinator CLIENT LIST error: {e}", force=True)

    # Check all shards
    for shardId in range(1, env.shardsCount + 1):
        try:
            shard_conn = env.getConnection(shardId)
            output = shard_conn.execute_command('CLIENT', 'LIST')
            clients = parse_client_list(output)
            hybrid_clients = [c for c in clients if 'HYBRID' in c.get('cmd', '').upper()]
            if hybrid_clients:
                env.debugPrint(f"{prefix}Shard {shardId} HYBRID clients:", force=True)
                for c in hybrid_clients:
                    env.debugPrint(f"  id={c.get('id')} cmd={c.get('cmd')} flags={c.get('flags')}", force=True)
        except Exception as e:
            env.debugPrint(f"{prefix}Shard {shardId} CLIENT LIST error: {e}", force=True)

def pid_cmd(conn):
    """Get the process ID of a Redis connection."""
    return conn.execute_command('info', 'server')['process_id']


def get_all_shards_pid(env):
    """Get PIDs from all environment shards (excluding the coordinator)."""
    for shardId in range(1, env.shardsCount + 1):
        conn = env.getConnection(shardId)
        yield pid_cmd(conn)

def get_shard_counts(env):
    """Get the number of documents in each shard using KEYS doc*."""
    shard_counts = []
    for i in range(1, env.shardsCount + 1):
        keys = env.getConnection(i).execute_command('KEYS', 'doc*')
        shard_counts.append(len(keys))
    return shard_counts


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


def wait_for_client_blocked(env, client_id, timeout=30):
    """Wait for a client to become blocked."""
    def check_fn():
        blocked = is_client_blocked(env, client_id)
        return blocked, {'client_id': client_id, 'blocked': blocked}
    client_list = env.execute_command('CLIENT', 'LIST')
    wait_for_condition(check_fn, f'Timeout waiting for client {client_id} to be blocked , list = {client_list}', timeout)


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


def wait_for_blocked_query_client_contains(env, cmd_substr,
                                           msg='Client for blocked command not found',
                                           timeout=30):
    """Wait for a blocked client whose command contains the given substring."""
    cmd_substr = cmd_substr.upper()
    with TimeLimit(timeout, msg):
        while True:
            output = env.execute_command('CLIENT', 'LIST')
            clients = parse_client_list(output)
            for client in clients:
                if cmd_substr in client.get('cmd', '').upper() and 'b' in client.get('flags', ''):
                    return client['id']
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

    def tearDown(self):
        """Teardown: Print debug info about any remaining HYBRID clients."""
        debug_print_hybrid_clients(self.env, "TestCoordinatorTimeout teardown")

    def _test_fail_timeout_impl(self, query_args):
        env = self.env

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()

        # Capture baseline metrics
        before_info = info_modules_to_dict(env)
        base_err_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])

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

        # Verify coord timeout error metric incremented by 1
        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC],
                        str(base_err_coord + 1),
                        message=f"Coordinator timeout error should be +1 after {query_args[0]}")
        _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_ERROR_COORD_METRIC])

        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy).ok()

    def test_fail_timeout_search(self):
        self._test_fail_timeout_impl(['FT.SEARCH', 'idx', '*'])

    def test_fail_timeout_aggregate(self):
        self._test_fail_timeout_impl(['FT.AGGREGATE', 'idx', '*'])

    def test_fail_timeout_profile_search(self):
        self._test_fail_timeout_impl(['FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '*'])

    def test_fail_timeout_profile_aggregate(self):
        self._test_fail_timeout_impl(['FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*'])

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

        # Capture baseline metrics
        before_info = info_modules_to_dict(env)
        base_err_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])

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

        # Verify coord timeout error metric incremented by 1
        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC],
                        str(base_err_coord + 1),
                        message=f"Coordinator timeout error should be +1 after {cmd_name}")
        _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_ERROR_COORD_METRIC])

        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy).ok()

    def test_fail_timeout_before_coord_pickup_search(self):
        """Test timeout occurring before coordinator picks up an FT.SEARCH query."""
        self._test_fail_timeout_before_coord_pickup_impl(['FT.SEARCH', 'idx', '*'])

    def test_fail_timeout_before_coord_pickup_aggregate(self):
        """Test timeout occurring before coordinator picks up an FT.AGGREGATE query."""
        self._test_fail_timeout_before_coord_pickup_impl(['FT.AGGREGATE', 'idx', '*'])

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

        # Capture baseline metrics
        before_info = info_modules_to_dict(env)
        base_err_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])

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

        # Verify coord timeout error metric incremented by 1
        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC],
                        str(base_err_coord + 1),
                        message="Coordinator timeout error should be +1 after FT.SEARCH fanout")
        _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_ERROR_COORD_METRIC])

        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy).ok()

    def test_partial_results_no_replies_timeout(self):
        """
        Test the partial results timeout mechanism when no replies are received.

        This test:
        1. Sets timeout policy to 'return-strict' (partial results)
        2. Pauses coordinator threads before fanout
        3. Runs FT.SEARCH from the coordinator
        4. Manually unblocks the client with timeout using CLIENT UNBLOCK
        5. Verifies 0 results and timeout warning
        """
        env = self.env

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict')

        # Capture baseline metrics
        before_info = info_modules_to_dict(env)
        base_warn_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])

        # Pause coordinator thread pool to prevent fanout
        env.expect(debug_cmd(), 'COORD_THREADS', 'PAUSE').ok()

        wait_for_condition(
            lambda: (env.cmd(debug_cmd(), 'COORD_THREADS', 'IS_PAUSED') == 1, {'is_paused': env.cmd(debug_cmd(), 'COORD_THREADS', 'IS_PAUSED')}),
            'Timeout while waiting for coordinator to pause'
        )

        query_result = []

        t_query = threading.Thread(
            target=call_and_store,
            args=(env.cmd, ['FT.SEARCH', 'idx', '*'], query_result),
            daemon=True
        )
        t_query.start()

        blocked_client_id = wait_for_blocked_query_client(env, 'FT.SEARCH')

        # Unblock the client to simulate timeout
        env.cmd('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT')

        wait_for_client_unblocked(env, blocked_client_id)

        # Resume coordinator threads
        env.expect(debug_cmd(), 'COORD_THREADS', 'RESUME').ok()

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

        # Verify 0 results and timeout warning
        env.assertEqual(len(query_result), 1, message="Expected 1 result from query thread")
        result = query_result[0]
        env.assertEqual(result['total_results'], 0, message="Expected 0 results")
        env.assertEqual(result['warning'], [TIMEOUT_WARNING])

        # Verify coord timeout warning metric incremented by 1
        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC],
                        str(base_warn_coord + 1),
                        message="Coordinator timeout warning should be +1 after FT.SEARCH with RETURN_STRICT")
        _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_WARNING_COORD_METRIC])

        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy)

    def test_no_timeout(self):
        """
        Test that using result-strict or fail policies doesn't affect the regular flow
        when there is no timeout (i.e., FT.SEARCH completes normally and gets all expected
        replies from shards).
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

        # Test with 'return-strict' policy
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict').ok()
        result = env.cmd('FT.SEARCH', 'idx', '*')
        env.assertEqual(result['total_results'], self.n_docs,
                        message=f"Expected {self.n_docs} total results with 'return-strict' policy")
        env.assertEqual(result.get('warning', []), [],
                        message="Expected no warning with 'return-strict' policy")

        # Test FT.PROFILE with 'fail' policy
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()
        result = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '*')
        env.assertContains('Results', result, message="Expected 'Results' key in FT.PROFILE output")
        profile_results = result['Results']
        env.assertEqual(profile_results['total_results'], self.n_docs,
                        message=f"Expected {self.n_docs} total results with 'fail' policy (FT.PROFILE)")
        env.assertEqual(profile_results.get('warning', []), [],
                        message="Expected no warning with 'fail' policy (FT.PROFILE)")

        # Test FT.PROFILE with 'return-strict' policy
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict').ok()
        result = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '*')
        env.assertContains('Results', result, message="Expected 'Results' key in FT.PROFILE output")
        profile_results = result['Results']
        env.assertEqual(profile_results['total_results'], self.n_docs,
                        message=f"Expected {self.n_docs} total results with 'return-strict' policy (FT.PROFILE)")
        env.assertEqual(profile_results.get('warning', []), [],
                        message="Expected no warning with 'return-strict' policy (FT.PROFILE)")

        # Test FT.AGGREGATE with 'fail' policy
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()
        result = env.cmd('FT.AGGREGATE', 'idx', '*')
        env.assertEqual(result['total_results'], self.n_docs,
                        message=f"Expected {self.n_docs} total results with 'fail' policy (FT.AGGREGATE)")
        env.assertEqual(result.get('warning', []), [],
                        message="Expected no warning with 'fail' policy (FT.AGGREGATE)")

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
        This verifies cursor pagination still works even when cursor reads use the
        blocked-client timeout path.
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

    def test_cursor_read_timeout_fail(self):
        """Test FT.CURSOR READ timeout via the blocked-client timeout mechanism."""
        env = self.env

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()

        before_info = info_modules_to_dict(env)
        base_err_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])

        res, cursor_id = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@name',
                                 'WITHCURSOR', 'COUNT', '10')
        env.assertGreater(len(res), 0, message="Expected results in first chunk")
        env.assertNotEqual(cursor_id, 0, message="Expected non-zero cursor ID for pagination")

        initial_jobs_done = getWorkersThpoolStats(env)['totalJobsDone']
        env.expect(debug_cmd(), 'WORKERS', 'pause').ok()

        t_query = threading.Thread(
            target=run_cmd_expect_timeout,
            args=(env, ['FT.CURSOR', 'READ', 'idx', cursor_id]),
            daemon=True
        )
        t_query.start()

        blocked_client_id = wait_for_blocked_query_client_contains(
            env, 'FT.CURSOR',
            'Client for FT.CURSOR READ not found'
        )

        env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)
        wait_for_client_unblocked(env, blocked_client_id)

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Cursor read thread should have finished")

        env.expect(debug_cmd(), 'WORKERS', 'resume').ok()
        wait_for_condition(
            lambda: (getWorkersThpoolStats(env)['totalJobsDone'] > initial_jobs_done,
                     {'totalJobsDone': getWorkersThpoolStats(env)['totalJobsDone']}),
            'Timeout while waiting for worker to finish cursor read job'
        )
        env.expect(debug_cmd(), 'WORKERS', 'drain').ok()

        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC],
                        str(base_err_coord + 1),
                        message="Coordinator timeout error should be +1 after FT.CURSOR READ timeout")
        _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_ERROR_COORD_METRIC])

        env.expect('FT.CURSOR', 'READ', 'idx', cursor_id).error().contains('Cursor not found')
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy).ok()

    def test_shard_timeout_fail(self):
        """Test shard timeout with FAIL policy."""
        env = self.env
        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]

        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail')

        # Capture baseline shard and coordinator metrics
        before_info = info_modules_to_dict(env)
        base_err_shard = int(before_info[WARN_ERR_SECTION][TIMEOUT_ERROR_SHARD_METRIC])
        base_err_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])

        for i, query_type in enumerate(['FT.SEARCH', 'FT.AGGREGATE', 'FT.HYBRID']):

            initial_jobs_done = getWorkersThpoolStats(env)['totalJobsDone']

            # Pause workers on coordinator
            env.expect(debug_cmd(), 'WORKERS', 'pause').ok()

            query_args = [query_type, 'idx', '*']

            if query_type == 'FT.HYBRID':
                query_args = [query_type, 'hybrid_idx', 'SEARCH', '*', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', self.hybrid_query_vec]

            t_query = threading.Thread(
                target=run_cmd_expect_timeout,
                args=(env, query_args),
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
            if query_type != 'FT.HYBRID':
                env.expect(debug_cmd(), 'WORKERS', 'drain').ok()
            else:
                # In hybrid, we can't drain because of depleters.
                # Wait for totalJobsDone to increase.
                wait_for_condition(
                    lambda: (getWorkersThpoolStats(env)['totalJobsDone'] > initial_jobs_done, {'totalJobsDone': getWorkersThpoolStats(env)['totalJobsDone']}),
                    'Timeout while waiting for worker to finish job'
                )

            # Verify shard and coord timeout error metrics incremented
            info_dict = info_modules_to_dict(env)
            env.assertEqual(info_dict[WARN_ERR_SECTION][TIMEOUT_ERROR_SHARD_METRIC],
                            str(base_err_shard + i + 1),
                            message=f"Shard timeout error should be +{i+1} after {query_type}")
            env.assertEqual(info_dict[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC],
                            str(base_err_coord + i + 1),
                            message=f"Coordinator timeout error should be +{i+1} after {query_type}")

        # Verify no other metrics changed
        _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_ERROR_SHARD_METRIC, TIMEOUT_ERROR_COORD_METRIC])

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

        # Capture baseline metrics
        before_info = info_modules_to_dict(env)
        base_err_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])

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

        # Verify coord timeout error metric incremented by 1
        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC],
                        str(base_err_coord + 1),
                        message=f"Coordinator timeout error should be +1 after {cmd_name} before coord store")
        _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_ERROR_COORD_METRIC])

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

        # Capture baseline metrics
        before_info = info_modules_to_dict(env)
        base_err_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])

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

        # Verify coord timeout error metric incremented by 1
        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC],
                        str(base_err_coord + 1),
                        message=f"Coordinator timeout error should be +1 after {cmd_name} after coord store")
        _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_ERROR_COORD_METRIC])

        # Cleanup
        resetStoreResultsDebug(env)
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy).ok()

    def test_fail_timeout_before_coord_store_aggregate(self):
        """Test timeout occurring before coordinator stores results for FT.AGGREGATE."""
        self._test_fail_timeout_before_coord_store_impl(['FT.AGGREGATE', 'idx', '*'])

    def test_fail_timeout_after_coord_store_aggregate(self):
        """Test timeout occurring after coordinator stores results for FT.AGGREGATE."""
        self._test_fail_timeout_after_coord_store_impl(['FT.AGGREGATE', 'idx', '*'])

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

    def _test_fail_timeout_shard_store_cursors_impl(self, before):
        """Test timeout occurring before/after shard stores cursors for internal FT.HYBRID.

        This tests the FAIL timeout policy when timeout occurs before or after
        the shard stores the cursors list for the internal _FT.HYBRID command.
        """
        env = self.env

        # Skip if ENABLE_ASSERT is not enabled
        skipIfNoEnableAssert(env)

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()

        # Enable pause before/after hybrid cursor storage on ALL shards
        if before:
            setPauseBeforeHybridStoreCursors(env, True)
        else:
            setPauseAfterHybridStoreCursors(env, True)

        query_args = [
            'FT.HYBRID', 'hybrid_idx',
            'SEARCH', '*',
            'VSIM', '@embedding', '$BLOB',
            'PARAMS', '2', 'BLOB', self.hybrid_query_vec
        ]

        t_query = threading.Thread(
            target=run_cmd_expect_timeout,
            args=(env, query_args),
            daemon=True
        )
        t_query.start()

        blocked_client_id = wait_for_blocked_query_client(env, f'_FT.HYBRID', f'Client for query _FT.HYBRID not found')

        # Wait for shard to be paused during store cursors
        wait_for_condition(
            lambda: (getIsHybridStoreCursorsPaused(env) == 1, {'paused': getIsHybridStoreCursorsPaused(env)}),
            'Timeout while waiting for shard to pause during store cursors'
        )

        # Unblock the client to simulate timeout
        env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)

        wait_for_client_unblocked(env, blocked_client_id)

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

        # Cleanup - reset hybrid store cursors debug
        resetHybridStoreCursorsDebug(env)
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy).ok()

    def test_fail_timeout_before_shard_store_cursors_hybrid(self):
        """Test timeout occurring before shard stores cursors for internal FT.HYBRID."""
        self._test_fail_timeout_shard_store_cursors_impl(before=True)

    def test_fail_timeout_after_shard_store_cursors_hybrid(self):
        """Test timeout occurring after shard stores cursors for internal FT.HYBRID."""
        self._test_fail_timeout_shard_store_cursors_impl(before=False)


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

        # Capture baseline metrics
        before_info = info_modules_to_dict(env)
        base_err_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])

        # Set pause before first result (N=1 means pause before 1st result)
        setPauseBeforeReduce(env, 1)

        blocked_client_id = env.cmd('CLIENT', 'ID')

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

        wait_for_client_blocked(env, blocked_client_id)

        # Trigger timeout - the pause loop in the reducer will detect the timeout
        # and auto-break to avoid deadlock with timeout callback
        env.cmd('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT')

        wait_for_client_unblocked(env, blocked_client_id)

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

        # Verify coord timeout error metric incremented by 1
        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC],
                        str(base_err_coord + 1),
                        message="Coordinator timeout error should be +1 after fail during reduce before first")
        _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_ERROR_COORD_METRIC])

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

        # Capture baseline metrics
        before_info = info_modules_to_dict(env)
        base_err_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])

        # Set pause after last result
        setPauseBeforeReduce(env, PAUSE_AFTER_LAST_RESULT)

        blocked_client_id = env.cmd('CLIENT', 'ID')

        t_query = threading.Thread(
            target=run_cmd_expect_timeout,
            args=(env, ['FT.SEARCH', 'idx', '*']),
            daemon=True
        )
        t_query.start()

        # First wait for client to be blocked (query is being processed)
        wait_for_client_blocked(env, blocked_client_id)

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

        # Verify coord timeout error metric incremented by 1
        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC],
                        str(base_err_coord + 1),
                        message="Coordinator timeout error should be +1 after fail during reduce after last")
        _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_ERROR_COORD_METRIC])

        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy)
        self._cleanup_pause_state()

    def test_timeout_return_strict_before_first_reduce(self):
        """Test return-strict timeout policy when timeout occurs before first result is reduced.

        Uses pause mechanism (N=1) to pause before the 1st result. When timeout is triggered,
        the timeout callback waits for the reducer to finish. With the early
        exit behavior on timeout, we get only the results from the first shard
        that responded, not all 100 results.
        """
        env = self.env

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict').ok()

        # Capture baseline metrics
        before_info = info_modules_to_dict(env)
        base_warn_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])

        setPauseBeforeReduce(env, 1)

        blocked_client_id = env.cmd('CLIENT', 'ID')

        query_result = []

        t_query = threading.Thread(
            target=call_and_store,
            args=(env.cmd, ['FT.SEARCH', 'idx', '*', 'LIMIT', '0', '10'], query_result),
            daemon=True
        )
        t_query.start()

        wait_for_condition(
            lambda: (getIsCoordReducePaused(env) == 1, {'paused': getIsCoordReducePaused(env)}),
            'Timeout while waiting for coordinator to pause during reduce'
        )

        wait_for_client_blocked(env, blocked_client_id)

        env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)

        wait_for_client_unblocked(env, blocked_client_id)

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

        env.assertEqual(len(query_result), 1, message="Expected 1 result from query thread")
        result = query_result[0]

        shard_counts = get_shard_counts(env)

        env.assertContains(result['total_results'], shard_counts,
                           message=f"Expected total results to exactly match one of the shards' document counts {shard_counts}")
        env.assertEqual(result['warning'], [TIMEOUT_WARNING], message="Expected timeout warning")

        # Verify coord timeout warning metric incremented by 1
        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC],
                        str(base_warn_coord + 1),
                        message="Coordinator timeout warning should be +1 after return-strict before first reduce")
        _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_WARNING_COORD_METRIC])

        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy).ok()
        self._cleanup_pause_state()

    def test_timeout_return_strict_before_reducer_ctx_init(self):
        """Test return-strict timeout after reducer claims ownership but before req->rctx init."""
        env = self.env

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict').ok()

        # Pause right after the background reducer claims reducing so timeout
        # will wait for it, then force the reducer to take the timed-out early exit.
        setPauseBeforeReduce(env, PAUSE_BEFORE_REDUCER_INIT)

        blocked_client_id = env.cmd('CLIENT', 'ID')

        query_result = []

        t_query = threading.Thread(
            target=call_and_store,
            args=(env.cmd, ['FT.SEARCH', 'idx', '*', 'LIMIT', '0', '10'], query_result),
            daemon=True
        )
        t_query.start()

        wait_for_condition(
            lambda: (getIsCoordReducePaused(env) == 1, {'paused': getIsCoordReducePaused(env)}),
            'Timeout while waiting for coordinator to pause before reducer ctx init'
        )

        wait_for_client_blocked(env, blocked_client_id)

        env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)

        wait_for_client_unblocked(env, blocked_client_id)

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

        env.assertEqual(len(query_result), 1, message="Expected 1 result from query thread")
        result = query_result[0]
        env.assertEqual(result['total_results'], 100, message="Expected 100 total results from all shards")
        env.assertEqual(result['warning'], [TIMEOUT_WARNING], message="Expected timeout warning")

        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy).ok()
        self._cleanup_pause_state()

    def test_timeout_return_strict_mid_reduce(self):
        """Test return-strict timeout policy when timeout occurs mid-reduction.

        Uses pause mechanism (N=2) to pause before the 2nd result. When timeout is triggered,
        the timeout callback waits for the reducer to finish. With the early exit behavior,
        we get only the results from the first shard that responded.
        """
        env = self.env

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict').ok()

        # Capture baseline metrics
        before_info = info_modules_to_dict(env)
        base_warn_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])

        pause_before_n = 2
        setPauseBeforeReduce(env, pause_before_n)

        blocked_client_id = env.cmd('CLIENT', 'ID')

        query_result = []

        t_query = threading.Thread(
            target=call_and_store,
            args=(env.cmd, ['FT.SEARCH', 'idx', '*', 'LIMIT', '0', '10'], query_result),
            daemon=True
        )
        t_query.start()

        wait_for_condition(
            lambda: (getIsCoordReducePaused(env) == 1, {'paused': getIsCoordReducePaused(env)}),
            'Timeout while waiting for coordinator to pause during reduce'
        )

        reduce_count = getCoordReduceCount(env)
        env.assertEqual(reduce_count, pause_before_n - 1,
                        message=f"Expected {pause_before_n - 1} results reduced before pause")

        wait_for_client_blocked(env, blocked_client_id)

        env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)

        wait_for_client_unblocked(env, blocked_client_id)

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

        env.assertEqual(len(query_result), 1, message="Expected 1 result from query thread")
        result = query_result[0]

        shard_counts = get_shard_counts(env)
        env.assertContains(result['total_results'], shard_counts,
                           message=f"Expected total results to exactly match one of the shards' document counts {shard_counts}")
        env.assertEqual(result['warning'], [TIMEOUT_WARNING], message="Expected timeout warning")

        # Verify coord timeout warning metric incremented by 1
        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC],
                        str(base_warn_coord + 1),
                        message="Coordinator timeout warning should be +1 after return-strict mid reduce")
        _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_WARNING_COORD_METRIC])

        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy).ok()
        self._cleanup_pause_state()

    def test_timeout_return_strict_after_last_reduce(self):
        """Test return-strict timeout policy when timeout occurs after all results are reduced.

        Uses pause mechanism (N=-1) to pause after the last result. When timeout is triggered,
        the timeout callback waits for the reducer to finish, so we get all results.
        """
        env = self.env

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict').ok()

        # Capture baseline metrics
        before_info = info_modules_to_dict(env)
        base_warn_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])

        setPauseBeforeReduce(env, PAUSE_AFTER_LAST_RESULT)

        blocked_client_id = env.cmd('CLIENT', 'ID')

        query_result = []

        t_query = threading.Thread(
            target=call_and_store,
            args=(env.cmd, ['FT.SEARCH', 'idx', '*', 'LIMIT', '0', '10'], query_result),
            daemon=True
        )
        t_query.start()

        wait_for_client_blocked(env, blocked_client_id)

        wait_for_condition(
            lambda: (getIsCoordReducePaused(env) == 1, {'paused': getIsCoordReducePaused(env)}),
            'Timeout while waiting for coordinator to pause during reduce'
        )

        env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)

        wait_for_client_unblocked(env, blocked_client_id)

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

        env.assertEqual(len(query_result), 1, message="Expected 1 result from query thread")
        result = query_result[0]

        shard_counts = get_shard_counts(env)
        env.assertEqual(result['total_results'], sum(shard_counts),
                        message=f"Expected total results to match all shards combined ({sum(shard_counts)})")
        env.assertEqual(result['warning'], [TIMEOUT_WARNING], message="Expected timeout warning")

        # Verify coord timeout warning metric incremented by 1
        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC],
                        str(base_warn_coord + 1),
                        message="Coordinator timeout warning should be +1 after return-strict after last reduce")
        _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_WARNING_COORD_METRIC])

        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy).ok()
        self._cleanup_pause_state()

    def test_timeout_return_strict_with_profile(self):
        """Test return-strict timeout policy with FT.PROFILE command.

        Uses pause mechanism (N=2) to pause before the 2nd result. When timeout is triggered,
        the timeout callback waits for the reducer to finish. With the early exit behavior,
        we get only the results from the first shard that responded.
        """
        env = self.env

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict').ok()

        # Capture baseline metrics
        before_info = info_modules_to_dict(env)
        base_warn_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])

        setPauseBeforeReduce(env, 2)

        blocked_client_id = env.cmd('CLIENT', 'ID')

        query_result = []

        t_query = threading.Thread(
            target=call_and_store,
            args=(env.cmd, ['FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '*', 'LIMIT', '0', '10'], query_result),
            daemon=True
        )
        t_query.start()

        wait_for_condition(
            lambda: (getIsCoordReducePaused(env) == 1, {'paused': getIsCoordReducePaused(env)}),
            'Timeout while waiting for coordinator to pause during reduce'
        )

        wait_for_client_blocked(env, blocked_client_id)

        env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)

        wait_for_client_unblocked(env, blocked_client_id)

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

        env.assertEqual(len(query_result), 1, message="Expected 1 result from query thread")
        result = query_result[0]

        # FT.PROFILE returns: {'Results': {...}, 'Profile': {...}}
        env.assertContains('Results', result, message="Expected 'Results' key in FT.PROFILE output")
        profile_results = result['Results']

        shard_counts = get_shard_counts(env)
        env.assertContains(profile_results['total_results'], shard_counts,
                           message=f"Expected total results to exactly match one of the shards' document counts {shard_counts}")
        env.assertContains('warning', profile_results, message="Expected warning in Results")
        env.assertEqual(profile_results['warning'], [TIMEOUT_WARNING], message="Expected timeout warning")

        # Verify coord timeout warning metric incremented by 1
        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC],
                        str(base_warn_coord + 1),
                        message="Coordinator timeout warning should be +1 after return-strict with profile")
        _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_WARNING_COORD_METRIC])

        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy).ok()
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

        # Warmup hybrid query and store vector for tests
        self.hybrid_query_vec = np.array([0.0, 0.0], dtype=np.float32).tobytes()
        self.env.expect(
            'FT.HYBRID', 'hybrid_idx',
            'SEARCH', '*',
            'VSIM', '@embedding', '$BLOB',
            'PARAMS', '2', 'BLOB', self.hybrid_query_vec
        ).noError()

    def test_shard_timeout_fail(self):
        """Test shard timeout with FAIL policy."""
        env = self.env

        # Set timeout policy to FAIL
        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()

        # Capture baseline metrics (standalone uses coord metrics)
        before_info = info_modules_to_dict(env)
        base_err_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])

        for i, query_type in enumerate(['FT.SEARCH', 'FT.AGGREGATE', 'FT.HYBRID']):

            initial_jobs_done = getWorkersThpoolStats(env)['totalJobsDone']

            # Pause worker thread
            env.expect(debug_cmd(), 'WORKERS', 'pause').ok()

            query_args = [query_type, 'idx', '*']

            if query_type == 'FT.HYBRID':
                query_args = [query_type, 'hybrid_idx', 'SEARCH', '*', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', self.hybrid_query_vec]

            # Run a query that will be blocked
            t_query = threading.Thread(
                target=run_cmd_expect_timeout,
                args=(env, query_args),
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
            if query_type != 'FT.HYBRID':
                env.expect(debug_cmd(), 'WORKERS', 'drain').ok()
            else:
                # In hybrid, we can't drain because of depleters.
                # Wait for totalJobsDone to increase.
                wait_for_condition(
                    lambda: (getWorkersThpoolStats(env)['totalJobsDone'] > initial_jobs_done, {'totalJobsDone': getWorkersThpoolStats(env)['totalJobsDone']}),
                    'Timeout while waiting for worker to finish job'
                )

            # Verify coord timeout error metric incremented (standalone uses coord metrics)
            info_dict = info_modules_to_dict(env)
            env.assertEqual(info_dict[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC],
                            str(base_err_coord + i + 1),
                            message=f"Coordinator timeout error should be +{i+1} after {query_type}")

        # Verify no other metrics changed
        _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_ERROR_COORD_METRIC])

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

        # Capture baseline metrics (standalone uses coord metrics)
        before_info = info_modules_to_dict(env)
        base_err_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])

        # Run a query that will be blocked
        # Using PAUSE_BEFORE_RP_N to pause inside the pipeline
        for i, query_type in enumerate(['FT.SEARCH', 'FT.AGGREGATE']):

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

            # Verify coord timeout error metric incremented (standalone uses coord metrics)
            info_dict = info_modules_to_dict(env)
            env.assertEqual(info_dict[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC],
                            str(base_err_coord + i + 1),
                            message=f"Coordinator timeout error should be +{i+1} after {query_type} in pipeline")

        # Verify no other metrics changed
        _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_ERROR_COORD_METRIC])

        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy).ok()

    def _test_fail_timeout_before_store_impl(self, query_args):
        """Test timeout occurring before storing results (reply_callback path) in standalone."""
        env = self.env

        # Skip if ENABLE_ASSERT is not enabled
        skipIfNoEnableAssert(env)

        cmd_name = query_args[0]

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()

        # Capture baseline metrics (standalone uses coord metrics)
        before_info = info_modules_to_dict(env)
        base_err_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])

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

        # Verify coord timeout error metric incremented by 1 (standalone uses coord metrics)
        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC],
                        str(base_err_coord + 1),
                        message=f"Coordinator timeout error should be +1 after {cmd_name} before store")
        _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_ERROR_COORD_METRIC])

        # Cleanup
        resetStoreResultsDebug(env)
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy).ok()

    def _test_fail_timeout_after_store_impl(self, query_args):
        """Test timeout occurring after storing results but before reply_callback in standalone."""
        env = self.env

        # Skip if ENABLE_ASSERT is not enabled
        skipIfNoEnableAssert(env)

        cmd_name = query_args[0]

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()

        # Capture baseline metrics (standalone uses coord metrics)
        before_info = info_modules_to_dict(env)
        base_err_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])

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

        # Verify coord timeout error metric incremented by 1 (standalone uses coord metrics)
        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC],
                        str(base_err_coord + 1),
                        message=f"Coordinator timeout error should be +1 after {cmd_name} after store")
        _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_ERROR_COORD_METRIC])

        # Cleanup
        resetStoreResultsDebug(env)
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy).ok()

    def test_fail_timeout_before_store_search(self):
        """Test timeout occurring before storing results for FT.SEARCH in standalone."""
        self._test_fail_timeout_before_store_impl(['FT.SEARCH', 'idx', '*'])

    def test_fail_timeout_before_store_aggregate(self):
        """Test timeout occurring before storing results for FT.AGGREGATE in standalone."""
        self._test_fail_timeout_before_store_impl(['FT.AGGREGATE', 'idx', '*'])

    def test_fail_timeout_after_store_search(self):
        """Test timeout occurring after storing results for FT.SEARCH in standalone."""
        self._test_fail_timeout_after_store_impl(['FT.SEARCH', 'idx', '*'])

    def test_fail_timeout_after_store_aggregate(self):
        """Test timeout occurring after storing results for FT.AGGREGATE in standalone."""
        self._test_fail_timeout_after_store_impl(['FT.AGGREGATE', 'idx', '*'])

    def test_fail_timeout_before_store_hybrid(self):
        """Test timeout occurring before storing results for FT.HYBRID in standalone."""
        self._test_fail_timeout_before_store_impl([
            'FT.HYBRID', 'hybrid_idx',
            'SEARCH', '*',
            'VSIM', '@embedding', '$BLOB',
            'PARAMS', '2', 'BLOB', self.hybrid_query_vec
        ])

    def test_fail_timeout_after_store_hybrid(self):
        """Test timeout occurring after storing results for FT.HYBRID in standalone."""
        self._test_fail_timeout_after_store_impl([
            'FT.HYBRID', 'hybrid_idx',
            'SEARCH', '*',
            'VSIM', '@embedding', '$BLOB',
            'PARAMS', '2', 'BLOB', self.hybrid_query_vec
        ])

    def test_no_timeout_cursor(self):
        """
        Test that FAIL policy doesn't break cursor reads when there is no timeout.
        This verifies cursor pagination still works even when cursor reads use the
        blocked-client timeout path.
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

    def test_cursor_read_timeout_fail(self):
        """Test FT.CURSOR READ timeout via the blocked-client timeout mechanism."""
        env = self.env

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()

        before_info = info_modules_to_dict(env)
        base_err_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])

        res, cursor_id = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@name',
                                 'WITHCURSOR', 'COUNT', '10')
        env.assertGreater(len(res), 0, message="Expected results in first chunk")
        env.assertNotEqual(cursor_id, 0, message="Expected non-zero cursor ID for pagination")

        initial_jobs_done = getWorkersThpoolStats(env)['totalJobsDone']
        env.expect(debug_cmd(), 'WORKERS', 'pause').ok()

        t_query = threading.Thread(
            target=run_cmd_expect_timeout,
            args=(env, ['FT.CURSOR', 'READ', 'idx', cursor_id]),
            daemon=True
        )
        t_query.start()

        blocked_client_id = wait_for_blocked_query_client_contains(
            env, 'FT.CURSOR',
            'Client for FT.CURSOR READ not found'
        )

        env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)
        wait_for_client_unblocked(env, blocked_client_id)

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Cursor read thread should have finished")

        env.expect(debug_cmd(), 'WORKERS', 'resume').ok()
        wait_for_condition(
            lambda: (getWorkersThpoolStats(env)['totalJobsDone'] > initial_jobs_done,
                     {'totalJobsDone': getWorkersThpoolStats(env)['totalJobsDone']}),
            'Timeout while waiting for worker to finish cursor read job'
        )
        env.expect(debug_cmd(), 'WORKERS', 'drain').ok()

        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC],
                        str(base_err_coord + 1),
                        message="Coordinator timeout error should be +1 after FT.CURSOR READ timeout")
        _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_ERROR_COORD_METRIC])

        env.expect('FT.CURSOR', 'READ', 'idx', cursor_id).error().contains('Cursor not found')
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

        # Capture baseline metrics (standalone uses coord metrics)
        before_info = info_modules_to_dict(env)
        base_err_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])

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

        # Verify coord timeout error metric incremented by 1 (standalone uses coord metrics)
        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC],
                        str(base_err_coord + 1),
                        message="Coordinator timeout error should be +1 after cursor initial timeout")
        _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_ERROR_COORD_METRIC])

        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy).ok()

