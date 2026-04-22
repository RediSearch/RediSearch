from common import *
from test_hybrid_internal import get_shard_slot_ranges
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

def _coord_cursor_total(env, idx='idx'):
    """Return the coordinator's global cursor count from FT.INFO.

    Returns 0 if the cursor_stats section is absent (e.g. all cursors gone
    and the index dropped it), matching the behavior of getCursorStats in
    test_cursors.py.
    """
    info = env.cmd('FT.INFO', idx)
    try:
        stats = to_dict(to_dict(info)['cursor_stats'])
        return int(stats.get('global_total', 0))
    except Exception:
        return 0

def _wait_for_cursor_cleanup(env, baseline_total, context, idx='idx', timeout=30):
    """Wait for the coord cursor count to drop below `baseline_total`.

    Tests in this file share a class-level `env`, so polling for an absolute
    zero count is unreliable (a previous test may have left cursors behind).
    Instead, callers capture the coord cursor count immediately after creating
    their cursor and wait for at least one cursor to be reclaimed.
    """
    wait_for_condition(
        lambda: (_coord_cursor_total(env, idx) < baseline_total,
                 {'total': _coord_cursor_total(env, idx), 'baseline': baseline_total}),
        f'coord cursor was not cleaned up after {context}',
        timeout=timeout,
    )

def assert_timeout_warning(env, res, message=''):
    warnings = res.get('warning', res.get('warnings', []))
    env.assertTrue(warnings, message=message + " expected timeout warning")
    env.assertContains('Timeout', warnings[0], message=message + " expected timeout warning")

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

    def _test_remaining_timeout_exhausted_before_shard_execution_impl(self, internal_cmd_args,
                                                                      verify_return_result):
        """
        Test that a query whose entire timeout budget is consumed by coordinator dispatch
        time is handled correctly for the 'fail' and 'return-strict' ON_TIMEOUT policies.

        Instead of going through the coordinator (which has its own blocked-client timer
        that masks the shard-level behavior), this test talks directly to the shard
        using internal commands (_FT.SEARCH, _FT.AGGREGATE, _FT.HYBRID) with a
        _COORD_DISPATCH_TIME that exceeds the TIMEOUT budget.

        Args:
            internal_cmd_args: Base args for the internal command (e.g. ['_FT.SEARCH', 'idx', '*']).
                Must NOT include TIMEOUT, _SLOTS_INFO, or _COORD_DISPATCH_TIME — these are added
                automatically.
            verify_return_result: Callable(env, cmd_args) to verify response under 'return-strict' policy.
        """
        env = self.env
        # A 50ms TIMEOUT with 100ms dispatch time → budget is exhausted before execution.
        timeout_ms = '50'
        dispatch_time_ns = '100000000'  # 100ms in nanoseconds (> 50ms timeout)

        # env.cmd uses env.con which connects to shard 1; get its slot range.
        _, slots_data = get_shard_slot_ranges(env)[0]
        env.cmd('DEBUG', 'MARK-INTERNAL-CLIENT')

        full_args = list(internal_cmd_args) + [
            'TIMEOUT', timeout_ms,
            '_SLOTS_INFO', slots_data,
            '_COORD_DISPATCH_TIME', dispatch_time_ns,
        ]

        for on_timeout_policy in ['return-strict', 'fail']:
            env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, on_timeout_policy)
            try:
                if on_timeout_policy == 'fail':
                    env.expect(*full_args).error().contains(TIMEOUT_ERROR)
                else:
                    verify_return_result(env, full_args)
            finally:
                env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return')

    def test_remaining_timeout_exhausted_before_shard_execution_search(self):
        def verify_return(env, args):
            res = env.cmd(*args)
            env.assertEqual(res['total_results'], 0,
                            message=f"Expected 0 search results under return-strict, got: {res}")
            assert_timeout_warning(env, res, message="_FT.SEARCH return-strict")
        self._test_remaining_timeout_exhausted_before_shard_execution_impl(
            ['_FT.SEARCH', 'idx', '*'],
            verify_return_result=verify_return,
        )

    def test_remaining_timeout_exhausted_before_shard_execution_aggregate(self):
        def verify_return(env, args):
            res = env.cmd(*args)
            env.assertEqual(len(res['results']), 0,
                            message=f"Expected 0 aggregate results under return-strict, got: {res}")
            assert_timeout_warning(env, res, message="_FT.AGGREGATE return-strict")
        self._test_remaining_timeout_exhausted_before_shard_execution_impl(
            ['_FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@name'],
            verify_return_result=verify_return,
        )

    def test_remaining_timeout_exhausted_before_shard_execution_hybrid(self):
        def verify_return(env, args):
            res = env.cmd(*args)
            assert_timeout_warning(env, res, message=f"_FT.HYBRID return-strict, got: {res}")
        self._test_remaining_timeout_exhausted_before_shard_execution_impl(
            [
                '_FT.HYBRID', 'hybrid_idx',
                'SEARCH', '*',
                'VSIM', '@embedding', '$BLOB',
                'PARAMS', '2', 'BLOB', self.hybrid_query_vec,
            ],
            verify_return_result=verify_return,
        )

    def _test_remaining_timeout_exhausted_before_shard_execution_profile_impl(self, internal_cmd_args):
        """
        Test that FT.PROFILE commands with pre-execution timeout produce consistent
        reply structures across SEARCH, AGGREGATE, and HYBRID.

        When profiling is active, timeout errors are suppressed (never returned as errors)
        regardless of the ON_TIMEOUT policy. Instead, empty results with profile wrapping
        should be returned.

        Args:
            internal_cmd_args: Base args for the internal profile command
                (e.g. ['_FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '*']).
                Must NOT include TIMEOUT, _SLOTS_INFO, or _COORD_DISPATCH_TIME.
        """
        env = self.env
        timeout_ms = '50'
        dispatch_time_ns = '100000000'  # 100ms in nanoseconds (> 50ms timeout)

        _, slots_data = get_shard_slot_ranges(env)[0]
        env.cmd('DEBUG', 'MARK-INTERNAL-CLIENT')

        full_args = list(internal_cmd_args) + [
            'TIMEOUT', timeout_ms,
            '_SLOTS_INFO', slots_data,
            '_COORD_DISPATCH_TIME', dispatch_time_ns,
        ]

        # Profile suppresses timeout errors for all policies, so both 'fail' and
        # 'return-strict' should return empty results with profile structure (not an error).
        for on_timeout_policy in ['fail', 'return-strict']:
            env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, on_timeout_policy)
            try:
                result = env.expect(*full_args).noError().res

                # Verify profile wrapping: response should have 'Results' key
                env.assertContains('Results', result,
                    message=f"Expected 'Results' key in profile output with {on_timeout_policy} policy, got: {result}")

                profile_results = result['Results']

                # Verify timeout warning in results
                warnings = profile_results.get('warning', profile_results.get('warnings', []))
                env.assertTrue(warnings,
                    message=f"Expected timeout warning with {on_timeout_policy} policy, got: {profile_results}")
                env.assertContains('Timeout', warnings[0],
                    message=f"Expected timeout in warning with {on_timeout_policy} policy, got: {warnings}")
            finally:
                env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return')

    def test_remaining_timeout_exhausted_before_shard_execution_profile_search(self):
        self._test_remaining_timeout_exhausted_before_shard_execution_profile_impl(
            ['_FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '*'],
        )

    def test_remaining_timeout_exhausted_before_shard_execution_profile_aggregate(self):
        self._test_remaining_timeout_exhausted_before_shard_execution_profile_impl(
            ['_FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*', 'LOAD', '1', '@name'],
        )

    def test_remaining_timeout_exhausted_before_shard_execution_profile_hybrid(self):
        self._test_remaining_timeout_exhausted_before_shard_execution_profile_impl(
            [
                '_FT.PROFILE', 'hybrid_idx', 'HYBRID', 'QUERY',
                'SEARCH', '*',
                'VSIM', '@embedding', '$BLOB',
                'PARAMS', '2', 'BLOB', self.hybrid_query_vec,
            ],
        )


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

    def _create_fail_cursor(self, chunk_size=10):
        """Create an FT.AGGREGATE WITHCURSOR under FAIL policy and return (cursor_id, first_chunk)."""
        res, cursor_id = self.env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@name',
                                      'WITHCURSOR', 'COUNT', str(chunk_size))
        self.env.assertNotEqual(cursor_id, 0, message="Expected non-zero cursor ID")
        return cursor_id, res

    def _drain_cursor(self, cursor_id):
        """Drain remaining chunks from a cursor, tolerating 'Cursor not found'."""
        while cursor_id != 0:
            try:
                _, cursor_id = self.env.cmd('FT.CURSOR', 'READ', 'idx', cursor_id)
            except Exception:
                break

    def _drain_cursor_counting(self, cursor_id, total_results):
        """Drain a cursor accumulating total_results from each chunk. Returns the final total."""
        while cursor_id != 0:
            res, cursor_id = self.env.cmd('FT.CURSOR', 'READ', 'idx', cursor_id)
            total_results += res['total_results']
        return total_results

    def _setup_fail_cursor_state(self, chunk_size=10):
        """Common FAIL-timeout cursor test setup.

        Switches all shards to FAIL, creates an FT.AGGREGATE WITHCURSOR, and
        captures the coord cursor-count baseline plus a pre-timeout snapshot of
        the coord error metric. Returns
        ``(prev_policy, cursor_id, baseline_cursor_total, before_info, base_err_coord)``.
        """
        env = self.env
        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail')
        cursor_id, _ = self._create_fail_cursor(chunk_size=chunk_size)
        baseline_cursor_total = _coord_cursor_total(env)
        before_info = info_modules_to_dict(env)
        base_err_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])
        return prev_on_timeout_policy, cursor_id, baseline_cursor_total, before_info, base_err_coord

    def _start_blocked_cursor_read(self, cursor_id):
        """Start FT.CURSOR READ in a thread and wait for its client to be blocked.

        Returns ``(thread, blocked_client_id)``. Caller is responsible for
        firing the timeout (see ``_fire_client_timeout``) and joining the
        thread (see ``_join_cursor_read_thread``).
        """
        env = self.env
        t_query = threading.Thread(
            target=run_cmd_expect_timeout,
            args=(env, ['FT.CURSOR', 'READ', 'idx', str(cursor_id)]),
            daemon=True
        )
        t_query.start()
        blocked_client_id = wait_for_blocked_query_client(env, 'FT.CURSOR|READ',
                                                          'Client for FT.CURSOR|READ not found')
        return t_query, blocked_client_id

    def _fire_client_timeout(self, blocked_client_id):
        """Fire CLIENT UNBLOCK ... TIMEOUT and wait for the client to be unblocked."""
        env = self.env
        env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)
        wait_for_client_unblocked(env, blocked_client_id)

    def _fire_blocked_cursor_read_timeout(self, cursor_id):
        """Start FT.CURSOR READ in a thread, fire CLIENT UNBLOCK ... TIMEOUT, return the thread.

        Caller is responsible for any sync-point / pause arming before the call
        so the worker is pinned when we fire the timeout.
        """
        t_query, blocked_client_id = self._start_blocked_cursor_read(cursor_id)
        self._fire_client_timeout(blocked_client_id)
        return t_query

    def _join_cursor_read_thread(self, t_query):
        env = self.env
        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Cursor read thread should have finished")

    def _assert_cursor_freed_and_metric_bumped(self, cursor_id, baseline_cursor_total,
                                               before_info, base_err_coord, context):
        """Post-FAIL-timeout assertions: cursor gone, coord error +1, other metrics unchanged."""
        env = self.env
        _wait_for_cursor_cleanup(env, baseline_cursor_total, context)
        env.expect('FT.CURSOR', 'READ', 'idx', str(cursor_id)).error().contains('Cursor not found')
        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC],
                        str(base_err_coord + 1),
                        message=f"Coordinator timeout error should be +1 after {context}")
        _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_ERROR_COORD_METRIC])

    def _arm_cursor_read_sync_point(self, sync_point):
        """Arm `sync_point` and return a context callable to wait-until-pinned / signal-to-release."""
        env = self.env
        env.expect(debug_cmd(), 'SYNC_POINT', 'CLEAR').ok()
        env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point).ok()

    def _wait_worker_pinned_at_sync_point(self, sync_point):
        env = self.env
        wait_for_condition(
            lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sync_point) == 1, {}),
            f'worker never reached {sync_point}'
        )

    def test_fail_timeout_cursor_read(self):
        """FAIL timeout fired on the coord+FAIL worker path mid-pipeline.

        Uses the `BeforeCursorReadSendChunk` sync point to pin the worker
        between `CoordRequestCtx_SetRequest` and `sendChunk`, then fires the
        blocked-client deadline via `CLIENT UNBLOCK ... TIMEOUT`. Deterministic,
        no reliance on shard suspension or buffer draining.
        """
        env = self.env
        sync_point = 'BeforeCursorReadSendChunk'

        prev_policy, cursor_id, baseline, before_info, base_err_coord = self._setup_fail_cursor_state()

        self._arm_cursor_read_sync_point(sync_point)
        try:
            t_query, blocked_client_id = self._start_blocked_cursor_read(cursor_id)
            self._wait_worker_pinned_at_sync_point(sync_point)
            self._fire_client_timeout(blocked_client_id)
        finally:
            env.expect(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point).ok()

        self._join_cursor_read_thread(t_query)

        # The timeout callback replies -TIMEOUT on the main thread while the
        # worker is still completing its post-sync-point wind-down (UnblockClient
        # → free_privdata → Cursor_Free). The helper waits for cursor reclaim so
        # the next FT.CURSOR READ deterministically observes "Cursor not found"
        # instead of racing with in-flight cleanup.
        self._assert_cursor_freed_and_metric_bumped(cursor_id, baseline, before_info,
                                                    base_err_coord, 'FAIL cursor-read timeout')

        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy)

    def test_fail_timeout_before_coord_pickup_cursor_read(self):
        """Test FAIL timeout before coordinator threadpool picks up an FT.CURSOR READ."""
        env = self.env

        prev_policy, cursor_id, baseline, before_info, base_err_coord = self._setup_fail_cursor_state()

        # Pause coordinator thread pool to prevent pickup of the FT.CURSOR READ job
        env.expect(debug_cmd(), 'COORD_THREADS', 'PAUSE').ok()
        wait_for_condition(
            lambda: (env.cmd(debug_cmd(), 'COORD_THREADS', 'is_paused') == 1, {}),
            'Timeout while waiting for coordinator threads to pause', timeout=30)

        try:
            t_query = self._fire_blocked_cursor_read_timeout(cursor_id)
            self._join_cursor_read_thread(t_query)
        finally:
            env.expect(debug_cmd(), 'COORD_THREADS', 'RESUME').ok()
            wait_for_condition(
                lambda: (env.cmd(debug_cmd(), 'COORD_THREADS', 'is_paused') == 0, {}),
                'Timeout while waiting for coordinator threads to resume', timeout=30)

        # After RESUME, the worker dequeues the already-timed-out job and frees
        # the cursor on the timeout-early-exit branch. The helper waits for
        # cursor reclaim so the next FT.CURSOR READ deterministically observes
        # "Cursor not found" instead of racing with the worker.
        self._assert_cursor_freed_and_metric_bumped(cursor_id, baseline, before_info,
                                                    base_err_coord,
                                                    'FAIL pre-pickup cursor-read timeout')

        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy)

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

    def _test_fail_timeout_coord_store_cursor_read_impl(self, before):
        """Test FAIL timeout on FT.CURSOR READ paused before/after AREQ_StoreResults.

        Uses setPauseBefore/AfterStoreResults to freeze the worker at the exact
        point around AREQ_StoreResults on the coord side, then triggers timeout
        via CLIENT UNBLOCK.
        """
        env = self.env
        skipIfNoEnableAssert(env)

        prev_policy, cursor_id, baseline, before_info, base_err_coord = self._setup_fail_cursor_state()

        if before:
            setPauseBeforeStoreResults(env, True)
        else:
            setPauseAfterStoreResults(env, True)

        try:
            t_query, blocked_client_id = self._start_blocked_cursor_read(cursor_id)
            wait_for_condition(
                lambda: (getIsStoreResultsPaused(env) == 1, {'paused': getIsStoreResultsPaused(env)}),
                'Timeout while waiting for FT.CURSOR READ to pause around store results'
            )
            self._fire_client_timeout(blocked_client_id)
            self._join_cursor_read_thread(t_query)
        finally:
            resetStoreResultsDebug(env)

        # The helper waits for the worker to complete its post-timeout wind-down
        # (UnblockClient → free_privdata → Cursor_Free) before asserting the
        # cursor is gone.
        self._assert_cursor_freed_and_metric_bumped(cursor_id, baseline, before_info,
                                                    base_err_coord,
                                                    'FAIL coord-store cursor-read timeout')

        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy)

    def test_fail_timeout_before_coord_store_cursor_read(self):
        """Test FAIL timeout on FT.CURSOR READ just before coord AREQ_StoreResults."""
        self._test_fail_timeout_coord_store_cursor_read_impl(before=True)

    def test_fail_timeout_after_coord_store_cursor_read(self):
        """Test FAIL timeout on FT.CURSOR READ just after coord AREQ_StoreResults."""
        self._test_fail_timeout_coord_store_cursor_read_impl(before=False)

    def test_sticky_policy_fail_aggregate_config_return_cursor_read(self):
        """Cursor created under FAIL keeps FAIL semantics after CONFIG SET to RETURN."""
        env = self.env
        sync_point = 'BeforeCursorReadSendChunk'

        prev_policy, cursor_id, baseline, _, _ = self._setup_fail_cursor_state()

        # Flip global policy to RETURN after cursor creation (all shards, so the
        # test proves FAIL-stickiness against a fully-propagated RETURN global).
        # The baselines captured before the flip are discarded; re-snapshot now
        # so the metric-delta assertion below is against the post-flip state.
        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return')

        before_info = info_modules_to_dict(env)
        base_err_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])

        # Pin the worker mid-pipeline via the sync point and fire the
        # blocked-client deadline; proves the cursor takes the FAIL
        # (blocked-client) path despite the RETURN global.
        self._arm_cursor_read_sync_point(sync_point)
        try:
            t_query, blocked_client_id = self._start_blocked_cursor_read(cursor_id)
            self._wait_worker_pinned_at_sync_point(sync_point)
            self._fire_client_timeout(blocked_client_id)
        finally:
            env.expect(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point).ok()

        self._join_cursor_read_thread(t_query)

        # FAIL semantics held: cursor was freed by the timeout, error metric bumped.
        self._assert_cursor_freed_and_metric_bumped(cursor_id, baseline, before_info,
                                                    base_err_coord,
                                                    'sticky FAIL cursor-read timeout under RETURN global')

        # Global must remain as most recently set (RETURN), untouched by the sticky snapshot
        env.assertEqual(env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG], 'return',
                        message="Global timeout policy should remain 'return' after sticky-policy test")

        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy)

    def test_sticky_policy_return_aggregate_config_fail_cursor_read(self):
        """Cursor created under RETURN keeps RETURN semantics after CONFIG SET to FAIL.

        Under RETURN, the coord does not block the client for the cursor read; it
        executes inline and returns partial results on timeout. After flipping
        the global to FAIL, a subsequent FT.CURSOR READ must still take the
        RETURN path: no blocked-client, no coord timeout metric bump, reply is
        well-formed and the cursor can be drained to completion.
        """
        env = self.env

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return')

        chunk_size = 10
        res, cursor_id = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@name',
                                 'WITHCURSOR', 'COUNT', str(chunk_size))
        env.assertNotEqual(cursor_id, 0, message="Expected non-zero cursor ID")

        # Flip global policy to FAIL after cursor creation (all shards).
        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail')

        before_info = info_modules_to_dict(env)
        base_err_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])

        # Drain the cursor through normal reads; with RETURN sticky, no timeout
        # metrics should bump and no -TIMEOUT error should be raised.
        total_results = self._drain_cursor_counting(cursor_id, res['total_results'])

        env.assertEqual(total_results, self.n_docs,
                        message=f"Expected {self.n_docs} total results across all cursor reads under sticky RETURN")

        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC],
                        str(base_err_coord),
                        message="Coord timeout error must not bump: cursor's sticky RETURN policy must win")
        _verify_metrics_not_changed(env, env, before_info, [])

        env.assertEqual(env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG], 'fail',
                        message="Global timeout policy should remain 'fail' after sticky-policy test")

        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy)

    def test_sticky_policy_between_cursor_reads(self):
        """Multiple CONFIG SET flips between FT.CURSOR READ calls must not change
        the cursor's effective policy captured at creation time.

        Creates the cursor under FAIL, performs a happy FT.CURSOR READ, flips to
        RETURN, performs a second happy read, flips back to FAIL, performs a
        third happy read. All reads must succeed without timeout metric bumps.
        Then forces a mid-pipeline timeout on a 4th read under the RETURN global
        to prove FAIL is still sticky: coord arms the blocked-client timer and
        returns -TIMEOUT on CLIENT UNBLOCK.
        """
        env = self.env

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail')

        chunk_size = 10
        res, cursor_id = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@name',
                                 'WITHCURSOR', 'COUNT', str(chunk_size))
        env.assertNotEqual(cursor_id, 0, message="Expected non-zero cursor ID")
        total_results = res['total_results']

        before_info = info_modules_to_dict(env)
        base_err_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])

        # Read #1 under FAIL
        res, cursor_id = env.cmd('FT.CURSOR', 'READ', 'idx', cursor_id)
        total_results += res['total_results']
        env.assertNotEqual(cursor_id, 0, message="Expected cursor still live after read #1")

        # Flip to RETURN (all shards); read #2 must still be FAIL-sticky.
        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return')
        res, cursor_id = env.cmd('FT.CURSOR', 'READ', 'idx', cursor_id)
        total_results += res['total_results']

        # Flip back to FAIL (all shards); read #3 still consistent (happy path).
        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail')
        total_results = self._drain_cursor_counting(cursor_id, total_results)

        env.assertEqual(total_results, self.n_docs,
                        message=f"Expected {self.n_docs} total results across reads with sticky FAIL")

        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC],
                        str(base_err_coord),
                        message="Happy-path sticky reads must not bump coord timeout metric")
        _verify_metrics_not_changed(env, env, before_info, [])

        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy)

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


    def _test_remaining_timeout_exhausted_before_shard_execution_debug_impl(self, query_cmd, verify_return_result):
        """
        Test that FT.DEBUG commands with pre-execution timeout (via _COORD_DISPATCH_TIME)
        correctly handle timeout in the debug command path (DEBUG_execCommandCommon).

        EXEC_DEBUG does NOT include EXEC_WITH_PROFILE, so:
        - 'fail' policy → timeout error
        - 'return-strict' policy → empty results with timeout warning
        """
        env = self.env
        timeout_ms = '50'
        dispatch_time_ns = '100000000'  # 100ms in nanoseconds (> 50ms timeout)

        _, slots_data = get_shard_slot_ranges(env)[0]
        env.cmd('DEBUG', 'MARK-INTERNAL-CLIENT')

        # Build the debug command: query args + timeout/slots/dispatch + debug params.
        # We need at least 1 debug param for AREQ_Debug_New to succeed.
        # Use TIMEOUT_AFTER_N 100 as a dummy (never reached since we time out before execution).
        debug_params = ['TIMEOUT_AFTER_N', '100']
        base_query_args = list(query_cmd) + [
            'TIMEOUT', timeout_ms,
            '_SLOTS_INFO', slots_data,
            '_COORD_DISPATCH_TIME', dispatch_time_ns,
        ]
        full_args = [debug_cmd()] + parseDebugQueryCommandArgs(base_query_args, debug_params)

        for on_timeout_policy in ['return-strict', 'fail']:
            env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, on_timeout_policy)
            try:
                if on_timeout_policy == 'fail':
                    env.expect(*full_args).error().contains(TIMEOUT_ERROR)
                else:
                    verify_return_result(env, full_args)
            finally:
                env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return')

    def test_remaining_timeout_exhausted_before_shard_execution_debug_search(self):
        def verify_return(env, args):
            res = env.cmd(*args)
            env.assertEqual(res['total_results'], 0,
                            message=f"Expected 0 search results under return-strict, got: {res}")
            assert_timeout_warning(env, res, message="_FT.DEBUG _FT.SEARCH return-strict")
        self._test_remaining_timeout_exhausted_before_shard_execution_debug_impl(
            ['_FT.SEARCH', 'idx', '*'],
            verify_return_result=verify_return,
        )

    def test_remaining_timeout_exhausted_before_shard_execution_debug_aggregate(self):
        def verify_return(env, args):
            res = env.cmd(*args)
            env.assertEqual(len(res['results']), 0,
                            message=f"Expected 0 aggregate results under return-strict, got: {res}")
            assert_timeout_warning(env, res, message="_FT.DEBUG _FT.AGGREGATE return-strict")
        self._test_remaining_timeout_exhausted_before_shard_execution_debug_impl(
            ['_FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@name'],
            verify_return_result=verify_return,
        )



class TestShardTimeoutResp2:
    """Tests for shard timeout behavior with RESP2 protocol.

    Covers the RESP2 branch in sendChunk_ReplyOnly_EmptyResults, where timeout warnings
    are tracked in ProfileWarnings and global stats but not emitted in the reply
    (consistent with RESP2 not having a warnings array).
    """
    def __init__(self):
        skipTest(cluster=True)

        self.env = Env(protocol=2, moduleArgs='WORKERS 1 TIMEOUT 0')

        conn = getConnectionByEnv(self.env)
        self.env.expect('FT.CREATE', 'idx', 'PREFIX', '1', 'doc', 'SCHEMA', 'name', 'TEXT').ok()
        for i in range(10):
            conn.execute_command('HSET', f'doc{i}', 'name', f'hello{i}')

    def test_remaining_timeout_exhausted_before_shard_execution_resp2(self):
        """Test RESP2 pre-execution timeout with return-strict and fail policies."""
        env = self.env
        timeout_ms = '50'
        dispatch_time_ns = '100000000'  # 100ms > 50ms timeout

        _, slots_data = get_shard_slot_ranges(env)[0]
        env.cmd('DEBUG', 'MARK-INTERNAL-CLIENT')

        for cmd_type, query_args in [
            ('search', ['_FT.SEARCH', 'idx', '*']),
            ('aggregate', ['_FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@name']),
        ]:
            full_args = list(query_args) + [
                'TIMEOUT', timeout_ms,
                '_SLOTS_INFO', slots_data,
                '_COORD_DISPATCH_TIME', dispatch_time_ns,
            ]

            for on_timeout_policy in ['return-strict', 'fail']:
                env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, on_timeout_policy)
                try:
                    if on_timeout_policy == 'fail':
                        env.expect(*full_args).error().contains(TIMEOUT_ERROR)
                    else:
                        # RESP2 returns a list where first element is total_results (0)
                        res = env.cmd(*full_args)
                        env.assertEqual(res[0], 0,
                                        message=f"Expected 0 total results in RESP2 {cmd_type}, got: {res}")
                finally:
                    env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return')
