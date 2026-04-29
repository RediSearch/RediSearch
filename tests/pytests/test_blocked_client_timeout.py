from common import *
from test_hybrid_internal import get_shard_slot_ranges
from test_info_modules import (
    info_modules_to_dict,
    wait_for_info_metric,
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
    """Return the coordinator's global cursor count, or 0 if cursor_stats is absent."""
    info = env.cmd('FT.INFO', idx)
    try:
        stats = to_dict(to_dict(info)['cursor_stats'])
        return int(stats.get('global_total', 0))
    except Exception:
        return 0

def _wait_for_cursor_cleanup(env, baseline_total, context, idx='idx', timeout=30):
    """Wait for the coord cursor count to drop below `baseline_total`.

    Tests share a class-level `env`; polling against an absolute baseline
    captured after cursor creation avoids races with cursors from prior tests.
    """
    wait_for_condition(
        lambda: (_coord_cursor_total(env, idx) < baseline_total,
                 {'total': _coord_cursor_total(env, idx), 'baseline': baseline_total}),
        f'coord cursor was not cleaned up after {context}',
        timeout=timeout,
    )

def _setup_fail_cursor_state(env, chunk_size=10):
    """Switch shards to FAIL, create a WITHCURSOR aggregate, and return
    ``(prev_policy, cursor_id, baseline_cursor_total, before_info, base_err_coord)``."""
    prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
    run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail')
    before_info = info_modules_to_dict(env)
    base_err_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])
    _, cursor_id = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@name',
                           'WITHCURSOR', 'COUNT', str(chunk_size))
    env.assertNotEqual(cursor_id, 0, message="Expected non-zero cursor ID")
    baseline_cursor_total = _coord_cursor_total(env)
    return prev_on_timeout_policy, cursor_id, baseline_cursor_total, before_info, base_err_coord

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


def _non_coord_shard_conns(env):
    """Return shard connections whose process id differs from the coordinator's."""
    coord_pid = pid_cmd(env.con)
    conns = []
    for shardId in range(1, env.shardsCount + 1):
        conn = env.getConnection(shardId)
        if pid_cmd(conn) != coord_pid:
            conns.append(conn)
    return conns


def _wait_pinned_shard_with_blocked_cmd(shard_conn, sync_point, cmd_name, timeout=30):
    """Wait for `shard_conn` to be paused at `sync_point` while a client is
    blocked running `cmd_name`. Returns the blocked client id."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if shard_conn.execute_command(debug_cmd(), 'SYNC_POINT',
                                      'IS_WAITING', sync_point) == 1:
            cid = get_query_client(shard_conn, cmd_name)
            if cid:
                return cid
        time.sleep(0.1)
    raise TimeoutError(
        f'Shard not pinned at {sync_point} with a blocked {cmd_name} client within {timeout}s')


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
        since cursor reads use BlockCursorClientWithTimeout which has no reply_callback.
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

    def _start_blocked_cursor_read(self, cursor_id):
        """Start FT.CURSOR READ in a thread and return ``(thread, blocked_client_id)``
        once the client is blocked. Caller fires the timeout and joins the thread."""
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
        """FAIL timeout fired mid-pipeline on the coord+FAIL worker path.

        Pins the worker at `BeforeCursorReadSendChunk`, then fires
        CLIENT UNBLOCK ... TIMEOUT to trigger the blocked-client deadline.
        """
        env = self.env
        skipIfNoEnableAssert(env)
        sync_point = 'BeforeCursorReadSendChunk'

        prev_policy, cursor_id, baseline, before_info, base_err_coord = _setup_fail_cursor_state(env)

        self._arm_cursor_read_sync_point(sync_point)
        try:
            t_query, blocked_client_id = self._start_blocked_cursor_read(cursor_id)
            self._wait_worker_pinned_at_sync_point(sync_point)
            env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)
            wait_for_client_unblocked(env, blocked_client_id)
        finally:
            env.expect(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point).ok()

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Cursor read thread should have finished")

        # Wait for cursor reclaim so the next FT.CURSOR READ deterministically
        # sees "Cursor not found" instead of racing with the worker's wind-down.
        self._assert_cursor_freed_and_metric_bumped(cursor_id, baseline, before_info,
                                                    base_err_coord, 'FAIL cursor-read timeout')

        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy)

    def test_fail_timeout_before_coord_pickup_cursor_read(self):
        """Test FAIL timeout before coordinator threadpool picks up an FT.CURSOR READ."""
        env = self.env

        prev_policy, cursor_id, baseline, before_info, base_err_coord = _setup_fail_cursor_state(env)

        # Pause coordinator thread pool to prevent pickup of the FT.CURSOR READ job
        env.expect(debug_cmd(), 'COORD_THREADS', 'PAUSE').ok()
        wait_for_condition(
            lambda: (env.cmd(debug_cmd(), 'COORD_THREADS', 'is_paused') == 1, {}),
            'Timeout while waiting for coordinator threads to pause', timeout=30)

        try:
            t_query, blocked_client_id = self._start_blocked_cursor_read(cursor_id)
            env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)
            wait_for_client_unblocked(env, blocked_client_id)
            t_query.join(timeout=10)
            env.assertFalse(t_query.is_alive(), message="Cursor read thread should have finished")
        finally:
            env.expect(debug_cmd(), 'COORD_THREADS', 'RESUME').ok()
            wait_for_condition(
                lambda: (env.cmd(debug_cmd(), 'COORD_THREADS', 'is_paused') == 0, {}),
                'Timeout while waiting for coordinator threads to resume', timeout=30)

        # After RESUME, the worker dequeues the already-timed-out job and frees
        # the cursor on the timeout-early-exit branch. Wait for cursor reclaim.
        self._assert_cursor_freed_and_metric_bumped(cursor_id, baseline, before_info,
                                                    base_err_coord,
                                                    'FAIL pre-pickup cursor-read timeout')

        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy)

    def test_fail_timeout_internal_cursor_read(self):
        """FAIL timeout fired on a non-coord shard's _FT.CURSOR READ BC timer.

        Pin a non-coord shard's internal ``_FT.CURSOR READ`` at
        ``BeforeCursorReadSendChunk`` and fire ``CLIENT UNBLOCK ... TIMEOUT``
        to invoke ``CursorReadTimeoutFailCallback``. Verify the user sees
        ``-TIMEOUT``, the coord error metric bumps, and the coord cursor is
        reclaimed via ``AREQ_CleanUpStoredCursor``.
        """
        env = self.env
        skipIfNoEnableAssert(env)
        sync_point = 'BeforeCursorReadSendChunk'

        non_coord_shards = _non_coord_shard_conns(env)
        env.assertGreater(len(non_coord_shards), 0,
                          message="Test requires at least one shard process distinct "
                                  "from the coordinator to exercise the internal "
                                  "_FT.CURSOR READ path")
        # One pinned shard stalls the whole coord: MR_ManuallyTriggerNextIfNeeded
        # won't dispatch a new round while any prior command is still in flight.
        target_shard = non_coord_shards[0]

        # Shrink cursor read size on every shard so each _FT.CURSOR READ returns
        # 1 doc; otherwise the coord-self shard could satisfy the request alone
        # and the target shard would never be dispatched to.
        all_shards = [env.getConnection(i) for i in range(1, env.shardsCount + 1)]
        prev_sizes = [
            c.execute_command(debug_cmd(), 'QUERY_CONTROLLER', 'SET_CURSOR_READ_SIZE', 1)
            for c in all_shards
        ]
        prev_policy = None
        try:
            prev_policy, cursor_id, baseline, before_info, base_err_coord = \
                _setup_fail_cursor_state(env)

            # Per-shard baseline: only the timed-out shard should bump
            # TIMEOUT_ERROR_SHARD_METRIC via CursorReadTimeoutFailCallback;
            # all other shards stay flat.
            base_err_shards = [
                int(info_modules_to_dict(c)[WARN_ERR_SECTION][TIMEOUT_ERROR_SHARD_METRIC])
                for c in all_shards
            ]
            target_pid = pid_cmd(target_shard)

            target_shard.execute_command(debug_cmd(), 'SYNC_POINT', 'CLEAR')
            target_shard.execute_command(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point)

            t_query = threading.Thread(
                target=run_cmd_expect_timeout,
                args=(env, ['FT.CURSOR', 'READ', 'idx', str(cursor_id)]),
                daemon=True,
            )
            t_query.start()
            try:
                blocked_client_id = _wait_pinned_shard_with_blocked_cmd(
                    target_shard, sync_point, '_FT.CURSOR|READ')
                # Fire the BC timeout on the pinned shard's internal cursor-read client.
                env.assertEqual(
                    target_shard.execute_command('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT'),
                    1,
                    message="CLIENT UNBLOCK on shard's _FT.CURSOR|READ should report 1")
            finally:
                target_shard.execute_command(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point)

            t_query.join(timeout=10)
            env.assertFalse(t_query.is_alive(),
                            message="Cursor read thread should have finished")

            self._assert_cursor_freed_and_metric_bumped(
                cursor_id, baseline, before_info, base_err_coord,
                'FAIL internal _FT.CURSOR READ timeout')

            # Verify the shard-side timeout metric: +1 on the target shard
            # only (CursorReadTimeoutFailCallback runs on its main thread),
            # unchanged everywhere else.
            for c, base in zip(all_shards, base_err_shards):
                expected = base + (1 if pid_cmd(c) == target_pid else 0)
                wait_for_info_metric(
                    c, [WARN_ERR_SECTION, TIMEOUT_ERROR_SHARD_METRIC],
                    str(expected),
                    msg=f"Shard pid={pid_cmd(c)} TIMEOUT_ERROR_SHARD_METRIC "
                        f"expected {expected} (base={base}, target_pid={target_pid})")
        finally:
            if prev_policy is not None:
                run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy)
            for c, prev in zip(all_shards, prev_sizes):
                c.execute_command(debug_cmd(), 'QUERY_CONTROLLER',
                                  'SET_CURSOR_READ_SIZE', prev)

    def test_fail_timeout_queued_internal_cursor_read(self):
        """FAIL timeout on a non-coord shard's _FT.CURSOR READ while queued.

        Times out the shard's ``_FT.CURSOR|READ`` blocked client while its
        cursor-read job is still queued in the worker pool, so the worker
        takes the early-exit branch and frees the cursor without running
        the pipeline.
        """
        env = self.env
        skipIfNoEnableAssert(env)

        non_coord_shards = _non_coord_shard_conns(env)
        env.assertGreater(len(non_coord_shards), 0,
                          message="Test requires at least one shard process distinct "
                                  "from the coordinator to exercise the internal "
                                  "_FT.CURSOR READ path")
        # One pinned shard stalls the whole coord: MR_ManuallyTriggerNextIfNeeded
        # won't dispatch a new round while any prior command is still in flight.
        target_shard = non_coord_shards[0]

        # Shrink cursor read size on every shard so each _FT.CURSOR READ returns
        # 1 doc; otherwise the coord-self shard could satisfy the request alone
        # and the target shard would never be dispatched to.
        all_shards = [env.getConnection(i) for i in range(1, env.shardsCount + 1)]
        prev_sizes = [
            c.execute_command(debug_cmd(), 'QUERY_CONTROLLER', 'SET_CURSOR_READ_SIZE', 1)
            for c in all_shards
        ]
        prev_policy = None
        try:
            prev_policy, cursor_id, baseline, before_info, base_err_coord = \
                _setup_fail_cursor_state(env)

            # Per-shard baseline: only the timed-out shard should bump
            # TIMEOUT_ERROR_SHARD_METRIC via CursorReadTimeoutFailCallback;
            # all other shards stay flat.
            base_err_shards = [
                int(info_modules_to_dict(c)[WARN_ERR_SECTION][TIMEOUT_ERROR_SHARD_METRIC])
                for c in all_shards
            ]
            target_pid = pid_cmd(target_shard)

            # Pause WORKERS on the target shard so its cursorRead_ctx queues
            # without running. The shard's main thread still processes the
            # incoming _FT.CURSOR|READ and blocks the BC.
            target_shard.execute_command(debug_cmd(), 'WORKERS', 'pause')
            try:
                t_query = threading.Thread(
                    target=run_cmd_expect_timeout,
                    args=(env, ['FT.CURSOR', 'READ', 'idx', str(cursor_id)]),
                    daemon=True,
                )
                t_query.start()
                blocked_client_id = wait_for_blocked_query_client(
                    target_shard, '_FT.CURSOR|READ',
                    f'Client for _FT.CURSOR|READ not found on shard pid={target_pid}')
                # Fire the BC timeout on the pinned shard's internal cursor-read client.
                env.assertEqual(
                    target_shard.execute_command('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT'),
                    1,
                    message="CLIENT UNBLOCK on shard's _FT.CURSOR|READ should report 1")
            finally:
                target_shard.execute_command(debug_cmd(), 'WORKERS', 'resume')
                target_shard.execute_command(debug_cmd(), 'WORKERS', 'drain')

            t_query.join(timeout=10)
            env.assertFalse(t_query.is_alive(),
                            message="Cursor read thread should have finished")

            self._assert_cursor_freed_and_metric_bumped(
                cursor_id, baseline, before_info, base_err_coord,
                'FAIL queued internal _FT.CURSOR READ timeout')

            # Verify the shard-side timeout metric: +1 on the target shard
            # only (CursorReadTimeoutFailCallback runs on its main thread),
            # unchanged everywhere else.
            for c, base in zip(all_shards, base_err_shards):
                expected = base + (1 if pid_cmd(c) == target_pid else 0)
                wait_for_info_metric(
                    c, [WARN_ERR_SECTION, TIMEOUT_ERROR_SHARD_METRIC],
                    str(expected),
                    msg=f"Shard pid={pid_cmd(c)} TIMEOUT_ERROR_SHARD_METRIC "
                        f"expected {expected} (base={base}, target_pid={target_pid})")
        finally:
            if prev_policy is not None:
                run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy)
            for c, prev in zip(all_shards, prev_sizes):
                c.execute_command(debug_cmd(), 'QUERY_CONTROLLER',
                                  'SET_CURSOR_READ_SIZE', prev)

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
        """FAIL timeout on FT.CURSOR READ paused before/after coord AREQ_StoreResults."""
        env = self.env
        skipIfNoEnableAssert(env)

        prev_policy, cursor_id, baseline, before_info, base_err_coord = _setup_fail_cursor_state(env)

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
            env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)
            wait_for_client_unblocked(env, blocked_client_id)
            t_query.join(timeout=10)
            env.assertFalse(t_query.is_alive(), message="Cursor read thread should have finished")
        finally:
            resetStoreResultsDebug(env)

        # Wait for the worker's post-timeout wind-down before asserting.
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
        skipIfNoEnableAssert(env)
        sync_point = 'BeforeCursorReadSendChunk'

        prev_policy, cursor_id, baseline, _, _ = _setup_fail_cursor_state(env)

        # Flip the global to RETURN on all shards after cursor creation, then
        # re-snapshot metrics so the post-flip delta is measured.
        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return')

        before_info = info_modules_to_dict(env)
        base_err_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])

        # Pin the worker mid-pipeline and fire the blocked-client deadline;
        # the cursor must still take the FAIL path despite the RETURN global.
        self._arm_cursor_read_sync_point(sync_point)
        try:
            t_query, blocked_client_id = self._start_blocked_cursor_read(cursor_id)
            self._wait_worker_pinned_at_sync_point(sync_point)
            env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)
            wait_for_client_unblocked(env, blocked_client_id)
        finally:
            env.expect(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point).ok()

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Cursor read thread should have finished")

        # FAIL semantics held: cursor was freed by the timeout, error metric bumped.
        self._assert_cursor_freed_and_metric_bumped(
            cursor_id, baseline, before_info, base_err_coord,
            'FAIL cursor-read timeout after global config flipped to RETURN')

        # Global must remain as most recently set (RETURN), untouched by the sticky snapshot
        env.assertEqual(env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG], 'return',
                        message="Global timeout policy should remain 'return' after sticky-policy test")

        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy)

    def test_sticky_policy_return_aggregate_config_fail_cursor_read(self):
        """Cursor created under RETURN keeps RETURN semantics after CONFIG SET to FAIL. """
        env = self.env
        chunk_size = 10
        # Sized so the simulator fires on the second pipeline call:
        #   FT.AGGREGATE returns chunk_size results (remaining = 5)
        #   FT.CURSOR READ #1 returns 5 then triggers timeout.
        timeout_after_n = chunk_size + 5

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return')

        res, cursor_id = runDebugQueryCommandTimeoutAfterN(
            env, ['FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@name',
                  'WITHCURSOR', 'COUNT', str(chunk_size)],
            timeout_res_count=timeout_after_n)
        env.assertNotEqual(cursor_id, 0, message="Expected non-zero cursor ID")
        env.assertEqual(res.get('warning', []), [],
                        message="FT.AGGREGATE first batch must not warn before timeout simulator fires")

        # Flip global policy to FAIL after cursor creation (all shards).
        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail')

        before_info = info_modules_to_dict(env)
        base_warn_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])
        base_err_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])

        # First FT.CURSOR READ hits the in-pipeline timeout simulator: sticky
        # RETURN must produce a partial reply with a timeout warning, not an error.
        res, cursor_id_after = env.cmd('FT.CURSOR', 'READ', 'idx', cursor_id)
        VerifyTimeoutWarningResp3(env, res,
                                  message="sticky RETURN cursor-read must produce a timeout warning")
        env.assertNotEqual(cursor_id_after, 0,
                           message="Sticky RETURN must keep the cursor live after a timeout")

        # Coord warning metric bumps (RETURN), error metric does not (no FAIL).
        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC],
                        str(base_warn_coord + 1),
                        message="Coord timeout warning should be +1 after sticky RETURN timeout")
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC],
                        str(base_err_coord),
                        message="Coord timeout error must not bump: cursor's sticky RETURN policy must win")

        # Free the still-live cursor so it doesn't leak past the test.
        env.expect('FT.CURSOR', 'DEL', 'idx', cursor_id_after).ok()

        env.assertEqual(env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG], 'fail',
                        message="Global timeout policy should remain 'fail' after sticky-policy test")

        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy)

    def test_sticky_policy_fail_between_cursor_reads(self):
        """Cursor created under FAIL stays FAIL even if global flips to RETURN
        between FT.CURSOR READ calls. """
        env = self.env
        skipIfNoEnableAssert(env)
        sync_point = 'BeforeCursorReadSendChunk'

        prev_policy, cursor_id, baseline, _, _ = _setup_fail_cursor_state(env)

        # Happy FT.CURSOR READ under FAIL before flipping the global.
        # Cursor must not be depleted yet so the next read can hit the timeout.
        _, cursor_id = env.cmd('FT.CURSOR', 'READ', 'idx', cursor_id)
        env.assertNotEqual(cursor_id, 0,
                           message="Cursor was depleted by the first read; "
                                   "the cursor must still have pages so the next read can hit the forced timeout")

        # Flip global to RETURN; the next read must still take the FAIL path.
        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return')

        before_info = info_modules_to_dict(env)
        base_err_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])

        self._arm_cursor_read_sync_point(sync_point)
        try:
            t_query, blocked_client_id = self._start_blocked_cursor_read(cursor_id)
            self._wait_worker_pinned_at_sync_point(sync_point)
            env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)
            wait_for_client_unblocked(env, blocked_client_id)
        finally:
            env.expect(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point).ok()

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Cursor read thread should have finished")

        # FAIL semantics held: -TIMEOUT error, cursor freed, coord error metric +1.
        self._assert_cursor_freed_and_metric_bumped(
            cursor_id, baseline, before_info, base_err_coord,
            'sticky FAIL cursor-read timeout between reads under RETURN global')

        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy)

    def test_sticky_policy_return_between_cursor_reads(self):
        """Cursor created under RETURN stays RETURN even if global flips to FAIL
        between FT.CURSOR READ calls. """
        env = self.env
        chunk_size = 10
        # Sized so the simulator fires on the third pipeline call:
        #   FT.AGGREGATE returns chunk_size results (remaining = chunk_size + 5)
        #   FT.CURSOR READ #1 returns chunk_size (remaining = 5)
        #   FT.CURSOR READ #2 returns 5 then triggers timeout.
        timeout_after_n = chunk_size * 2 + 5

        prev_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return')

        res, cursor_id = runDebugQueryCommandTimeoutAfterN(
            env, ['FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@name',
                  'WITHCURSOR', 'COUNT', str(chunk_size)],
            timeout_res_count=timeout_after_n)
        env.assertNotEqual(cursor_id, 0, message="Expected non-zero cursor ID")
        env.assertEqual(res.get('warning', []), [],
                        message="FT.AGGREGATE first batch must not warn before timeout simulator fires")

        # Happy FT.CURSOR READ under RETURN before flipping the global.
        res, cursor_id = env.cmd('FT.CURSOR', 'READ', 'idx', cursor_id)
        env.assertNotEqual(cursor_id, 0,
                           message="Cursor was depleted by the first read; "
                                   "the cursor must still have pages so the next read can hit the forced timeout")
        env.assertEqual(res.get('warning', []), [],
                        message="Happy RETURN cursor read must not warn")

        # Flip global to FAIL; the next read must still take the RETURN path.
        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail')

        before_info = info_modules_to_dict(env)
        base_warn_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])
        base_err_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])

        # FT.CURSOR READ that hits the in-pipeline timeout simulator: sticky
        # RETURN must produce a partial reply with a timeout warning, not an error.
        res, cursor_id_after = env.cmd('FT.CURSOR', 'READ', 'idx', cursor_id)
        VerifyTimeoutWarningResp3(env, res,
                                  message="sticky RETURN cursor-read must produce a timeout warning")
        env.assertNotEqual(cursor_id_after, 0,
                           message="Sticky RETURN must keep the cursor live after a timeout")

        # Coord warning metric bumps (RETURN), error metric does not (no FAIL).
        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC],
                        str(base_warn_coord + 1),
                        message="Coord timeout warning should be +1 after sticky RETURN timeout")
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC],
                        str(base_err_coord),
                        message="Coord timeout error must not bump under sticky RETURN")

        # Free the still-live cursor so it doesn't leak past the test.
        env.expect('FT.CURSOR', 'DEL', 'idx', cursor_id_after).ok()

        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy)

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

    def test_return_strict_timeout_at_claim_sync_point_aggregate(self):
        """RETURN_STRICT timeout while BG is parked before AREQ_TryClaimAggregateResults.

        Uses the BeforeAggregateResultsClaim sync point to deterministically race the
        main-thread timeout callback against the BG worker's TryClaim. BG is held
        before the claim so the main-thread callback always wins TryClaim and replies
        empty + timeout warning. After unblocking the client, the sync point is
        signalled so BG observes the lost claim and exits startPipeline cleanly.
        """
        env = self.env
        skipIfNoEnableAssert(env)

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict')

        before_info = info_modules_to_dict(env)
        base_warn_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])

        sync_point = 'BeforeAggregateResultsClaim'
        env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
        env.cmd(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point)

        query_result = []
        t_query = threading.Thread(
            target=call_and_store,
            args=(env.cmd, ['FT.AGGREGATE', 'idx', '*'], query_result),
            daemon=True
        )
        t_query.start()

        # Wait for BG to park at the sync point (before TryClaim).
        wait_for_condition(
            lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sync_point) == 1, {}),
            f'Timeout waiting for {sync_point} sync point'
        )

        # Fire the blocked-client timeout on the main thread while BG is parked.
        # Main-thread callback wins TryClaim (BG hasn't reached it yet) and
        # replies empty + TIMEOUT warning directly.
        blocked_client_id = wait_for_blocked_query_client(env, 'FT.AGGREGATE')
        env.cmd('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT')
        wait_for_client_unblocked(env, blocked_client_id)

        # Release BG so it can observe the lost claim and return from startPipeline.
        env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point)

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

        env.assertEqual(len(query_result), 1, message="Expected 1 result from query thread")
        result = query_result[0]
        env.assertEqual(result['total_results'], 0, message="Expected 0 results")
        env.assertEqual(result.get('results', []), [],
                        message=f"Expected no rows, got {result.get('results')}")
        env.assertEqual(result.get('warning', []), [TIMEOUT_WARNING])

        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC],
                        str(base_warn_coord + 1),
                        message="Coordinator timeout warning should be +1")
        _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_WARNING_COORD_METRIC])

        env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy)

    def test_return_strict_timeout_at_rpnet_start_sync_point_aggregate(self):
        """RETURN_STRICT timeout while BG is parked just before the rpnetNext_Start
        iterator dispatch.

        Uses the BeforeRPNetStart sync point to deterministically race the
        main-thread timeout callback against a BG worker that has already won
        TryClaim but not yet dispatched to the shards. Because BG owns the
        claim, the main-thread callback loses TryClaim and falls through to
        AREQ_WaitForAggregateResultsComplete. BG breaks out of the sync point's
        interruptible wait as soon as the callback flips the timedOut flag,
        observes AREQ_TimedOut in rpnetNext_Start, returns RS_RESULT_TIMEDOUT
        without ever dispatching the iterator, and signals completion so the
        callback can reply with empty results + TIMEOUT warning.
        """
        env = self.env
        skipIfNoEnableAssert(env)

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict')

        before_info = info_modules_to_dict(env)
        base_warn_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])

        sync_point = 'BeforeRPNetStart'
        env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
        env.cmd(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point)

        query_result = []
        t_query = threading.Thread(
            target=call_and_store,
            args=(env.cmd, ['FT.AGGREGATE', 'idx', '*'], query_result),
            daemon=True
        )
        t_query.start()

        # BG has already won TryClaim by the time it parks here.
        wait_for_condition(
            lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sync_point) == 1, {}),
            f'Timeout waiting for {sync_point} sync point'
        )

        # Fire the blocked-client timeout on the main thread. The callback loses
        # TryClaim (BG owns it) and blocks in AREQ_WaitForAggregateResultsComplete.
        # BG's SyncPoint_WaitTimeoutInterruptible breaks out on the timedOut flag,
        # returns RS_RESULT_TIMEDOUT without dispatching, and signals completion
        # so the callback wakes and replies.
        blocked_client_id = wait_for_blocked_query_client(env, 'FT.AGGREGATE')
        env.cmd('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT')
        wait_for_client_unblocked(env, blocked_client_id)

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

        env.assertEqual(len(query_result), 1, message="Expected 1 result from query thread")
        result = query_result[0]
        env.assertEqual(result['total_results'], 0, message="Expected 0 results")
        env.assertEqual(result.get('results', []), [],
                        message=f"Expected no rows, got {result.get('results')}")
        env.assertEqual(result.get('warning', []), [TIMEOUT_WARNING])

        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC],
                        str(base_warn_coord + 1),
                        message="Coordinator timeout warning should be +1")
        _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_WARNING_COORD_METRIC])

        env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy)

    def test_return_strict_timeout_one_shard_paused_aggregate(self):
        """RETURN_STRICT timeout while one shard process is suspended.

        Suspends a single non-coordinator shard via SIGSTOP so its replies never
        arrive. Uses the RpnetReplyAdmitted sync point to park BG after each
        responsive shard's reply is admitted into the pipeline, so the test can
        count exactly how many replies have been drained before firing the
        blocked-client timeout. This makes the partial-count assertion exact.
        """
        env = self.env
        skipIfNoEnableAssert(env)

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict')

        before_info = info_modules_to_dict(env)
        base_warn_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])

        coord_pid = pid_cmd(env.con)
        paused_pid = next(pid for pid in get_all_shards_pid(env) if pid != coord_pid)
        responsive_shard_conns = [
            env.getConnection(shardId)
            for shardId in range(1, env.shardsCount + 1)
            if pid_cmd(env.getConnection(shardId)) != paused_pid
        ]

        # Docs on responsive shards. The paused shard's docs never reach BG,
        # so this is the exact count BG will emit (one _FT.AGGREGATE reply per
        # responsive shard, each carrying that shard's docs as rows).
        expected_partial = sum(len(c.execute_command('KEYS', 'doc*'))
                               for c in responsive_shard_conns)
        expected_replies = len(responsive_shard_conns)

        shard_to_pause_p = psutil.Process(paused_pid)
        shard_to_pause_p.suspend()
        wait_for_condition(
            lambda: (shard_to_pause_p.status() == psutil.STATUS_STOPPED, {'status': shard_to_pause_p.status()}),
            'Timeout while waiting for shard to pause'
        )

        sync_point = 'RpnetReplyAdmitted'
        env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
        env.cmd(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point)

        query_result = []
        t_query = threading.Thread(
            target=call_and_store,
            args=(env.cmd, ['FT.AGGREGATE', 'idx', '*'], query_result),
            daemon=True
        )
        t_query.start()

        blocked_client_id = wait_for_blocked_query_client(env, 'FT.AGGREGATE')

        # Count sync-point hits: BG parks once per responsive shard reply admitted
        # into the pipeline. After `expected_replies` signals, every responsive
        # shard's rows are in the downstream result set. The paused shard never
        # triggers a hit since its reply never arrives.
        hits = 0
        while hits < expected_replies:
            wait_for_condition(
                lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sync_point) == 1,
                         {'hits': hits, 'expected': expected_replies}),
                f'Timeout waiting for BG to park at {sync_point} (hits={hits}/{expected_replies})'
            )
            hits += 1
            env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point)
            if hits < expected_replies:
                env.cmd(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point)

        # All responsive replies have been admitted. BG is now either emitting
        # rows from the last reply or blocked on the next channel pop waiting
        # for the (never-arriving) paused shard. Fire the blocked-client timeout;
        # the abort flag wakes the pop and BG returns TIMEDOUT with the already-
        # accumulated rows intact.
        env.cmd('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT')
        wait_for_client_unblocked(env, blocked_client_id)

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

        env.assertEqual(len(query_result), 1, message="Expected 1 result from query thread")
        result = query_result[0]
        env.assertEqual(result['total_results'], expected_partial,
                        message=f"Expected {expected_partial} docs from responsive shards, "
                                f"got {result['total_results']}")
        env.assertEqual(len(result.get('results', [])), expected_partial,
                        message=f"Expected {expected_partial} rows from responsive shards in reply, "
                                f"got {len(result.get('results', []))}")
        env.assertEqual(result.get('warning', []), [TIMEOUT_WARNING])

        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC],
                        str(base_warn_coord + 1),
                        message="Coordinator timeout warning should be +1")
        _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_WARNING_COORD_METRIC])

        env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
        shard_to_pause_p.resume()
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy)

    def test_return_strict_timeout_withcount_one_shard_paused_aggregate(self):
        """RETURN_STRICT timeout on FT.AGGREGATE WITHCOUNT with one shard suspended.

        Covers two related coordinator-side code paths that both fire on this
        scenario:

        1. ShardResponseBarrier (rpnet.c): WITHCOUNT installs a barrier that
           makes RPNet's first getNextReply block until every shard has sent
           its first reply so that total_results reflects the pre-LIMIT count
           across the full cluster. With one shard paused, the barrier never
           completes: shardResponseBarrier_HandleTimeout fires before any row
           is serialized and shardResponseBarrier_UpdateTotalResults is
           skipped, so RPNet returns TIMEDOUT with no buffered rows.

        2. RPDepleter RETURN_STRICT discard (result_processor.c): WITHCOUNT
           without SORTBY/GROUPBY adds an RPDepleter between RPNet and
           RPPager (see IsNeededDepleter in aggregate_request.c). When its
           upstream returns TIMEDOUT, RPDepleter_Next_Accumulate must drop
           any buffered rows and propagate TIMEDOUT in O(1) under
           RETURN_STRICT - returning a partial count would silently
           understate the result set. In this scenario the depleter's buffer
           is empty (the barrier blocked all rows), but the discard branch
           still executes and is asserted by the empty-result expectation.

        The reply must carry 0 rows, total_results=0, and a TIMEOUT warning
        regardless of which side (main-thread callback or BG) wins TryClaim.
        This is the distinguishing behavior from the non-WITHCOUNT
        one-shard-paused test, which can return partial rows from the
        responsive shards.
        """
        env = self.env

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict')

        before_info = info_modules_to_dict(env)
        base_warn_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])

        coord_pid = pid_cmd(env.con)
        paused_pid = next(pid for pid in get_all_shards_pid(env) if pid != coord_pid)
        responsive_shard_conns = [
            env.getConnection(shardId)
            for shardId in range(1, env.shardsCount + 1)
            if pid_cmd(env.getConnection(shardId)) != paused_pid
        ]

        shard_to_pause_p = psutil.Process(paused_pid)
        shard_to_pause_p.suspend()
        wait_for_condition(
            lambda: (shard_to_pause_p.status() == psutil.STATUS_STOPPED, {'status': shard_to_pause_p.status()}),
            'Timeout while waiting for shard to pause'
        )

        base_jobs_done = [getWorkersThpoolStatsFromShard(c)['totalJobsDone']
                          for c in responsive_shard_conns]

        query_result = []
        t_query = threading.Thread(
            target=call_and_store,
            args=(env.cmd, ['FT.AGGREGATE', 'idx', '*', 'WITHCOUNT'], query_result),
            daemon=True
        )
        t_query.start()

        blocked_client_id = wait_for_blocked_query_client(env, 'FT.AGGREGATE')

        # Wait for every responsive shard to complete its _FT.AGGREGATE job so
        # the barrier has received n-1 replies (and is stuck waiting for the
        # paused shard's reply that will never arrive) before the timeout fires.
        wait_for_condition(
            lambda: (
                all(getWorkersThpoolStatsFromShard(c)['totalJobsDone'] >= base + 1
                    for c, base in zip(responsive_shard_conns, base_jobs_done)),
                {'totalJobsDone': [getWorkersThpoolStatsFromShard(c)['totalJobsDone']
                                   for c in responsive_shard_conns],
                 'base': base_jobs_done}
            ),
            'Timeout waiting for responsive shards to complete their aggregate jobs'
        )

        env.cmd('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT')
        wait_for_client_unblocked(env, blocked_client_id)

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

        env.assertEqual(len(query_result), 1, message="Expected 1 result from query thread")
        result = query_result[0]
        # WITHCOUNT + incomplete barrier: total_results stays at its default 0
        # because shardResponseBarrier_UpdateTotalResults is not called when the
        # barrier times out. No rows are serialized either.
        env.assertEqual(result['total_results'], 0,
                        message=f"Expected 0 total_results with incomplete WITHCOUNT barrier, got {result['total_results']}")
        env.assertEqual(result.get('results', []), [],
                        message=f"Expected no rows, got {result.get('results')}")
        env.assertEqual(result.get('warning', []), [TIMEOUT_WARNING])

        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC],
                        str(base_warn_coord + 1),
                        message="Coordinator timeout warning should be +1")
        _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_WARNING_COORD_METRIC])

        shard_to_pause_p.resume()
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy)

    def test_return_strict_timeout_all_shards_paused_aggregate(self):
        """RETURN_STRICT timeout while every shard's workers are paused.

        Pauses the worker thread pool on every shard (including the coordinator's
        local shard) so no shard can execute the dispatched _FT.AGGREGATE and no
        replies arrive at the coordinator. The coordinator's dispatch thread still
        runs (WORKERS and COORD_THREADS are separate pools), so BG reaches
        MRIterator_Next and blocks on the channel. Firing the blocked-client
        timeout wakes BG via the WakeAbort broadcast, BG stores zero partial
        results and signals main, main replies with 0 results + warning.

        Uses the AfterIteratorStart sync point to park the IO thread after the
        fan-out loop, guaranteeing every shard has been handed an _FT.AGGREGATE
        command before the timeout fires.
        """
        env = self.env
        skipIfNoEnableAssert(env)

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict')

        before_info = info_modules_to_dict(env)
        base_warn_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])

        verify_command_OK_on_all_shards(env, debug_cmd(), 'WORKERS', 'pause')

        sync_point = 'AfterIteratorStart'
        env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
        env.cmd(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point)

        query_result = []
        t_query = threading.Thread(
            target=call_and_store,
            args=(env.cmd, ['FT.AGGREGATE', 'idx', '*'], query_result),
            daemon=True
        )
        t_query.start()

        # Wait for the IO thread to park after dispatching _FT.AGGREGATE to
        # every shard. Once it is parked we know the fan-out has happened.
        wait_for_condition(
            lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sync_point) == 1, {}),
            f'Timeout waiting for {sync_point} sync point'
        )

        blocked_client_id = wait_for_blocked_query_client(env, 'FT.AGGREGATE')
        env.cmd('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT')
        wait_for_client_unblocked(env, blocked_client_id)

        # Release the IO thread so iterStartCb can complete and the cluster
        # runtime can drain normally once workers are resumed below.
        env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point)

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

        env.assertEqual(len(query_result), 1, message="Expected 1 result from query thread")
        result = query_result[0]
        env.assertEqual(result['total_results'], 0, message="Expected 0 results")
        env.assertEqual(result.get('results', []), [],
                        message=f"Expected no rows, got {result.get('results')}")
        env.assertEqual(result.get('warning', []), [TIMEOUT_WARNING])

        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC],
                        str(base_warn_coord + 1),
                        message="Coordinator timeout warning should be +1")
        _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_WARNING_COORD_METRIC])

        env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
        verify_command_OK_on_all_shards(env, debug_cmd(), 'WORKERS', 'resume')
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy)

    def test_return_strict_timeout_channel_drain_aggregate(self):
        """RETURN_STRICT timeout while shard replies are queued in the channel.

        Parks BG at RpnetReplyAdmitted after the first reply is admitted, lets
        every shard finish so the remaining replies pile up in the channel,
        then fires the blocked-client timeout. BG breaks out of the
        interruptible wait via the timedOut flag and drains the queued items
        (PopWithTimeout returns queued items regardless of the abort flag),
        then completes the pipeline naturally because MRIterator_GetPending
        is already 0. The full row count must be present in the reply.
        """
        env = self.env
        skipIfNoEnableAssert(env)

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict')

        shard_conns = [env.getConnection(shardId)
                       for shardId in range(1, env.shardsCount + 1)]
        base_jobs_done = [getWorkersThpoolStatsFromShard(c)['totalJobsDone']
                          for c in shard_conns]

        sync_point = 'RpnetReplyAdmitted'
        env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
        env.cmd(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point)

        query_result = []
        t_query = threading.Thread(
            target=call_and_store,
            args=(env.cmd, ['FT.AGGREGATE', 'idx', '*', 'LIMIT', '0', str(self.n_docs)], query_result),
            daemon=True
        )
        t_query.start()

        blocked_client_id = wait_for_blocked_query_client(env, 'FT.AGGREGATE')

        # Wait for BG to park after admitting the first shard reply.
        wait_for_condition(
            lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sync_point) == 1, {}),
            f'Timeout waiting for BG to park at {sync_point}'
        )

        # Wait for every shard to complete its _FT.AGGREGATE job so the
        # remaining replies are pushed into the channel by the IO threads.
        wait_for_condition(
            lambda: (
                all(getWorkersThpoolStatsFromShard(c)['totalJobsDone'] >= base + 1
                    for c, base in zip(shard_conns, base_jobs_done)),
                {'totalJobsDone': [getWorkersThpoolStatsFromShard(c)['totalJobsDone']
                                   for c in shard_conns],
                 'base': base_jobs_done}
            ),
            'Timeout waiting for all shards to complete their aggregate jobs'
        )

        # Fire the blocked-client timeout while BG is still parked at the sync
        # point. BG breaks out via SyncPoint_WaitTimeoutInterruptible (timedOut
        # flag), then drains all queued channel items.
        env.cmd('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT')
        wait_for_client_unblocked(env, blocked_client_id)

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

        env.assertEqual(len(query_result), 1, message="Expected 1 result from query thread")
        result = query_result[0]
        env.assertEqual(result['total_results'], self.n_docs,
                        message=f"Expected {self.n_docs} docs after channel drain, "
                                f"got {result['total_results']}")
        env.assertEqual(len(result.get('results', [])), self.n_docs,
                        message=f"Expected {self.n_docs} drained rows in reply, "
                                f"got {len(result.get('results', []))}")

        env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy)

    def test_return_strict_timeout_sortby_one_shard_paused_aggregate(self):
        """RETURN_STRICT timeout on FT.AGGREGATE SORTBY with one shard suspended.

        SORTBY adds an RPSorter to the pipeline, so it does not yield partial
        results (canYieldPartialResults=false) and drainPartialResultsAfterTimeout
        is a no-op. RPSorter buffers rows from upstream and yields them only
        after EOF. Under RETURN_STRICT, when upstream returns TIMEDOUT the
        sorter must discard its buffered rows and propagate TIMEDOUT in O(1)
        to avoid an inconsistent partial sort. The reply must therefore carry
        0 rows plus a TIMEOUT warning, even though responsive shards delivered
        rows that the sorter had already buffered.
        """
        env = self.env

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict')

        before_info = info_modules_to_dict(env)
        base_warn_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])

        coord_pid = pid_cmd(env.con)
        paused_pid = next(pid for pid in get_all_shards_pid(env) if pid != coord_pid)
        responsive_shard_conns = [
            env.getConnection(shardId)
            for shardId in range(1, env.shardsCount + 1)
            if pid_cmd(env.getConnection(shardId)) != paused_pid
        ]

        shard_to_pause_p = psutil.Process(paused_pid)
        shard_to_pause_p.suspend()
        wait_for_condition(
            lambda: (shard_to_pause_p.status() == psutil.STATUS_STOPPED, {'status': shard_to_pause_p.status()}),
            'Timeout while waiting for shard to pause'
        )

        base_jobs_done = [getWorkersThpoolStatsFromShard(c)['totalJobsDone']
                          for c in responsive_shard_conns]

        query_result = []
        t_query = threading.Thread(
            target=call_and_store,
            args=(env.cmd, ['FT.AGGREGATE', 'idx', '*', 'SORTBY', '1', '@name'], query_result),
            daemon=True
        )
        t_query.start()

        blocked_client_id = wait_for_blocked_query_client(env, 'FT.AGGREGATE')

        # Wait for every responsive shard to complete its _FT.AGGREGATE job so
        # the sorter has buffered their rows before the timeout fires.
        wait_for_condition(
            lambda: (
                all(getWorkersThpoolStatsFromShard(c)['totalJobsDone'] >= base + 1
                    for c, base in zip(responsive_shard_conns, base_jobs_done)),
                {'totalJobsDone': [getWorkersThpoolStatsFromShard(c)['totalJobsDone']
                                   for c in responsive_shard_conns],
                 'base': base_jobs_done}
            ),
            'Timeout waiting for responsive shards to complete their aggregate jobs'
        )

        env.cmd('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT')
        wait_for_client_unblocked(env, blocked_client_id)

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

        env.assertEqual(len(query_result), 1, message="Expected 1 result from query thread")
        result = query_result[0]
        # SORTBY makes the pipeline not yield partial results; under
        # RETURN_STRICT the sorter must discard its buffer on upstream TIMEDOUT,
        # and the timeout reply path replaces the partial response with a fully
        # empty result set so `total_results` and `results` stay consistent.
        env.assertEqual(result.get('results', []), [],
                        message=f"Expected no rows, got {result.get('results')}")
        env.assertEqual(result['total_results'], 0,
                        message=f"Expected total_results=0 for SORTBY pipeline "
                                f"under RETURN_STRICT, got {result['total_results']}")
        env.assertEqual(result.get('warning', []), [TIMEOUT_WARNING])

        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC],
                        str(base_warn_coord + 1),
                        message="Coordinator timeout warning should be +1")
        _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_WARNING_COORD_METRIC])

        shard_to_pause_p.resume()
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy)

    def test_return_strict_timeout_after_store_aggregate(self):
        """RETURN_STRICT timeout race after the BG pipeline has stored results.

        Verifies the post-pipeline race: BG runs the pipeline to completion
        and stores its results via AREQ_StoreResults, then parks in
        debugPauseStoreResults' "after store" loop before calling
        AREQ_SignalAggregateResultsComplete. The blocked-client timeout
        callback fires on the main thread:
          - sets timedOut on AREQ
          - loses TryClaim (BG owns it)
          - blocks in AREQ_WaitForAggregateResultsComplete

        debugPauseStoreResults' loop polls AREQ_TimedOut and breaks out, so BG
        proceeds to AREQ_SignalAggregateResultsComplete. The main-thread
        callback wakes, sees hasStoredResults=true, drains the (already
        empty) channel via drainPartialResultsAfterTimeout, and replies with
        the full set of stored rows.

        Because the pipeline finished before the timeout had any chance to
        abort it, the reply carries the complete result set with no TIMEOUT
        warning and no coordinator timeout metric increment - the timeout
        callback was effectively a no-op race that we just need to handle
        gracefully (no deadlock, no double-reply, no leak).
        """
        env = self.env
        skipIfNoEnableAssert(env)

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict')

        before_info = info_modules_to_dict(env)

        setPauseAfterStoreResults(env, True)

        query_result = []
        t_query = threading.Thread(
            target=call_and_store,
            args=(env.cmd, ['FT.AGGREGATE', 'idx', '*', 'LIMIT', '0', str(self.n_docs)], query_result),
            daemon=True
        )
        t_query.start()

        blocked_client_id = wait_for_blocked_query_client(env, 'FT.AGGREGATE')

        # Wait for BG to park in the "pause after store" loop. At this point
        # AREQ_StoreResults has populated storedReplyState.results but
        # AREQ_SignalAggregateResultsComplete has not been called yet.
        wait_for_condition(
            lambda: (getIsStoreResultsPaused(env) == 1, {'paused': getIsStoreResultsPaused(env)}),
            'Timeout while waiting for query to pause after store results'
        )

        # Fire the timeout. Callback sets timedOut, loses TryClaim, and blocks
        # on AREQ_WaitForAggregateResultsComplete. BG's pause loop observes
        # AREQ_TimedOut and breaks, then signals completion. Callback wakes
        # and replies with the stored results.
        env.cmd('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT')
        wait_for_client_unblocked(env, blocked_client_id)

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

        env.assertEqual(len(query_result), 1, message="Expected 1 result from query thread")
        result = query_result[0]
        # The pipeline finished before the timeout could abort it: all shards
        # responded and BG stored a complete result set. The reply carries
        # the full row count, no TIMEOUT warning, and no metric increment.
        env.assertEqual(result['total_results'], self.n_docs,
                        message=f"Expected {self.n_docs} stored results, got {result['total_results']}")
        env.assertEqual(len(result.get('results', [])), self.n_docs,
                        message=f"Expected {self.n_docs} rows, got {len(result.get('results', []))}")
        env.assertEqual(result.get('warning', []), [],
                        message=f"Expected no warning (pipeline completed before timeout took effect), "
                                f"got {result.get('warning', [])}")

        # Coordinator timeout warning metric must not increment because the
        # timeout callback found stored results and replied with them.
        _verify_metrics_not_changed(env, env, before_info, [])

        resetStoreResultsDebug(env)
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy)


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

        blocked_client_id = wait_for_blocked_query_client(env, 'FT.SEARCH')

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

        blocked_client_id = wait_for_blocked_query_client(env, 'FT.SEARCH')

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

        blocked_client_id = wait_for_blocked_query_client(env, 'FT.SEARCH')

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

        query_result = []

        t_query = threading.Thread(
            target=call_and_store,
            args=(env.cmd, ['FT.SEARCH', 'idx', '*', 'LIMIT', '0', '10'], query_result),
            daemon=True
        )
        t_query.start()

        blocked_client_id = wait_for_blocked_query_client(env, 'FT.SEARCH')

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

        blocked_client_id = wait_for_blocked_query_client(env, 'FT.PROFILE')

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

    def _test_fail_timeout_before_store_impl(self, query_args, cmd_name=None):
        """Test timeout occurring before storing results (reply_callback path) in standalone."""
        env = self.env

        # Skip if ENABLE_ASSERT is not enabled
        skipIfNoEnableAssert(env)

        cmd_name = cmd_name or query_args[0]

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

    def _test_fail_timeout_after_store_impl(self, query_args, cmd_name=None):
        """Test timeout occurring after storing results but before reply_callback in standalone."""
        env = self.env

        # Skip if ENABLE_ASSERT is not enabled
        skipIfNoEnableAssert(env)

        cmd_name = cmd_name or query_args[0]

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
        since cursor reads use BlockCursorClientWithTimeout which has no reply_callback.
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

    def test_fail_timeout_shard_cursor_read(self):
        """FAIL timeout fired mid-pipeline on the shard FAIL+workers cursor-read path.

        Pins the worker at `BeforeCursorReadSendChunk`, then fires
        CLIENT UNBLOCK ... TIMEOUT to trigger the blocked-client deadline.
        """
        env = self.env
        skipIfNoEnableAssert(env)
        sync_point = 'BeforeCursorReadSendChunk'

        prev_policy, cursor_id, baseline, before_info, base_err_coord = _setup_fail_cursor_state(env)

        env.expect(debug_cmd(), 'SYNC_POINT', 'CLEAR').ok()
        env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point).ok()
        try:
            t_query = threading.Thread(
                target=run_cmd_expect_timeout,
                args=(env, ['FT.CURSOR', 'READ', 'idx', str(cursor_id)]),
                daemon=True
            )
            t_query.start()
            blocked_client_id = wait_for_blocked_query_client(env, 'FT.CURSOR|READ',
                                                              'Client for FT.CURSOR|READ not found')
            wait_for_condition(
                lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sync_point) == 1, {}),
                f'worker never reached {sync_point}'
            )
            env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)
            wait_for_client_unblocked(env, blocked_client_id)
        finally:
            env.expect(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point).ok()

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Cursor read thread should have finished")

        _wait_for_cursor_cleanup(env, baseline, 'shard FAIL cursor-read timeout')
        env.expect('FT.CURSOR', 'READ', 'idx', str(cursor_id)).error().contains('Cursor not found')
        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC],
                        str(base_err_coord + 1),
                        message="Coordinator timeout error should be +1 after shard FAIL cursor-read timeout")
        _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_ERROR_COORD_METRIC])

        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy).ok()

    def test_fail_timeout_shard_cursor_read_before_store(self):
        """FAIL timeout on FT.CURSOR READ paused before AREQ_StoreResults in standalone."""
        env = self.env
        prev_policy, cursor_id, baseline, _, _ = _setup_fail_cursor_state(env)
        try:
            self._test_fail_timeout_before_store_impl(
                ['FT.CURSOR', 'READ', 'idx', str(cursor_id)], cmd_name='FT.CURSOR|READ')
            _wait_for_cursor_cleanup(env, baseline,
                                     'shard FAIL cursor-read timeout before store')
            env.expect('FT.CURSOR', 'READ', 'idx', str(cursor_id)).error().contains('Cursor not found')
        finally:
            env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy).ok()

    def test_fail_timeout_shard_cursor_read_after_store(self):
        """FAIL timeout on FT.CURSOR READ paused after AREQ_StoreResults in standalone."""
        env = self.env
        prev_policy, cursor_id, baseline, _, _ = _setup_fail_cursor_state(env)
        try:
            self._test_fail_timeout_after_store_impl(
                ['FT.CURSOR', 'READ', 'idx', str(cursor_id)], cmd_name='FT.CURSOR|READ')
            _wait_for_cursor_cleanup(env, baseline,
                                     'shard FAIL cursor-read timeout after store')
            env.expect('FT.CURSOR', 'READ', 'idx', str(cursor_id)).error().contains('Cursor not found')
        finally:
            env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy).ok()

    def test_fail_timeout_queued_shard_cursor_read(self):
        """FAIL timeout on FT.CURSOR READ while queued in workersThreadPool (standalone).

        Times out the ``FT.CURSOR READ`` blocked client while its cursor-read
        job is still queued in the worker pool, so the worker takes the
        early-exit branch and frees the cursor without running the pipeline.
        """
        env = self.env
        prev_policy, cursor_id, baseline, before_info, base_err_coord = \
            _setup_fail_cursor_state(env)

        env.expect(debug_cmd(), 'WORKERS', 'pause').ok()
        try:
            t_query = threading.Thread(
                target=run_cmd_expect_timeout,
                args=(env, ['FT.CURSOR', 'READ', 'idx', str(cursor_id)]),
                daemon=True,
            )
            t_query.start()
            blocked_client_id = wait_for_blocked_query_client(
                env, 'FT.CURSOR|READ', 'Client for FT.CURSOR|READ not found')
            env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)
            wait_for_client_unblocked(env, blocked_client_id)
            t_query.join(timeout=10)
            env.assertFalse(t_query.is_alive(),
                            message="Cursor read thread should have finished")
        finally:
            env.expect(debug_cmd(), 'WORKERS', 'resume').ok()
            env.expect(debug_cmd(), 'WORKERS', 'drain').ok()

        # After drain, the queued cursorRead_ctx ran, observed AREQ_TimedOut(req)
        # and freed the cursor on the early-exit branch (aggregate_exec.c:1922-1924).
        _wait_for_cursor_cleanup(env, baseline,
                                 'FAIL queued cursor-read timeout (standalone)')
        env.expect('FT.CURSOR', 'READ', 'idx', str(cursor_id)).error().contains('Cursor not found')
        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC],
                        str(base_err_coord + 1),
                        message="Coordinator timeout error should be +1 after queued cursor-read timeout")
        _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_ERROR_COORD_METRIC])

        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy).ok()

    def test_fail_dropped_index_during_queued_cursor_read(self):
        """FAIL cursor-read replies the stored error when the index is dropped while queued.

        Drops the index while the ``cursorRead_ctx`` job is queued in the
        worker pool, so the worker takes the dropped-spec branch in
        ``cursorRead`` and stores the error on ``storedReplyState.err``.
        ``CursorReadReplyCallback`` then has no stored results and falls
        into the ``QueryError_HasError`` branch, replying with the stored
        error.
        """
        env = self.env

        # Use a dedicated index so we don't break the class-level shared 'idx'.
        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()
        try:
            env.expect('FT.CREATE', 'drop_idx', 'PREFIX', '1', 'drop_doc',
                       'SCHEMA', 'name', 'TEXT').ok()
            conn = getConnectionByEnv(env)
            for i in range(20):
                conn.execute_command('HSET', f'drop_doc{i}', 'name', f'hello{i}')
            waitForIndex(env, 'drop_idx')

            _, cursor_id = env.cmd('FT.AGGREGATE', 'drop_idx', '*',
                                   'LOAD', '1', '@name',
                                   'WITHCURSOR', 'COUNT', '1')
            env.assertNotEqual(cursor_id, 0,
                               message="Expected non-zero cursor ID")

            env.expect(debug_cmd(), 'WORKERS', 'pause').ok()
            try:
                expected_err = 'The index was dropped while the cursor was idle'
                t_query = threading.Thread(
                    target=lambda: env.expect(
                        'FT.CURSOR', 'READ', 'drop_idx', str(cursor_id)
                    ).error().contains(expected_err),
                    daemon=True,
                )
                t_query.start()
                wait_for_blocked_query_client(
                    env, 'FT.CURSOR|READ', 'Client for FT.CURSOR|READ not found')
                # Drop the index while the cursor-read job sits in the worker queue.
                env.expect('FT.DROPINDEX', 'drop_idx').ok()
            finally:
                env.expect(debug_cmd(), 'WORKERS', 'resume').ok()
                env.expect(debug_cmd(), 'WORKERS', 'drain').ok()

            t_query.join(timeout=10)
            env.assertFalse(t_query.is_alive(),
                            message="Cursor read thread should have finished")
        finally:
            env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy).ok()

    def test_sticky_policy_fail_aggregate_config_return_shard_cursor_read(self):
        """Cursor created under FAIL keeps FAIL semantics after CONFIG SET to RETURN (standalone)."""
        env = self.env
        skipIfNoEnableAssert(env)
        sync_point = 'BeforeCursorReadSendChunk'

        prev_policy, cursor_id, baseline, _, _ = _setup_fail_cursor_state(env)

        # Flip the global to RETURN after cursor creation; cursor must stay FAIL.
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return').ok()

        # Re-snapshot metrics so the post-flip delta is measured.
        before_info = info_modules_to_dict(env)
        base_err_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])

        env.expect(debug_cmd(), 'SYNC_POINT', 'CLEAR').ok()
        env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point).ok()
        try:
            t_query = threading.Thread(
                target=run_cmd_expect_timeout,
                args=(env, ['FT.CURSOR', 'READ', 'idx', str(cursor_id)]),
                daemon=True
            )
            t_query.start()
            blocked_client_id = wait_for_blocked_query_client(env, 'FT.CURSOR|READ',
                                                              'Client for FT.CURSOR|READ not found')
            wait_for_condition(
                lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sync_point) == 1, {}),
                f'worker never reached {sync_point}'
            )
            env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)
            wait_for_client_unblocked(env, blocked_client_id)
        finally:
            env.expect(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point).ok()

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Cursor read thread should have finished")

        # FAIL semantics held: cursor freed by the timeout, error metric bumped.
        _wait_for_cursor_cleanup(env, baseline,
                                 'sticky FAIL shard cursor-read timeout under RETURN global')
        env.expect('FT.CURSOR', 'READ', 'idx', str(cursor_id)).error().contains('Cursor not found')
        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC],
                        str(base_err_coord + 1),
                        message="Coordinator timeout error should be +1 after sticky FAIL cursor-read timeout")
        _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_ERROR_COORD_METRIC])

        env.assertEqual(env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG], 'return',
                        message="Global timeout policy should remain 'return' after sticky-policy test")
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy).ok()

    def test_sticky_policy_return_aggregate_config_fail_shard_cursor_read(self):
        """Cursor created under RETURN keeps RETURN semantics after CONFIG SET to FAIL (standalone)."""
        env = self.env
        chunk_size = 10
        # Sized so the simulator fires on the first FT.CURSOR READ:
        #   FT.AGGREGATE returns chunk_size results (remaining = 5)
        #   FT.CURSOR READ #1 returns 5 then triggers timeout.
        timeout_after_n = chunk_size + 5

        prev_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return').ok()

        res, cursor_id = runDebugQueryCommandTimeoutAfterN(
            env, ['FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@name',
                  'WITHCURSOR', 'COUNT', str(chunk_size)],
            timeout_res_count=timeout_after_n)
        env.assertNotEqual(cursor_id, 0, message="Expected non-zero cursor ID")
        env.assertEqual(res.get('warning', []), [],
                        message="FT.AGGREGATE first batch must not warn before timeout simulator fires")

        # Flip global to FAIL after cursor creation; cursor must stay RETURN.
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()

        before_info = info_modules_to_dict(env)
        base_warn_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])
        base_err_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])

        # First FT.CURSOR READ hits the in-pipeline timeout simulator: sticky
        # RETURN must produce a partial reply with a timeout warning, not an error.
        res, cursor_id_after = env.cmd('FT.CURSOR', 'READ', 'idx', cursor_id)
        VerifyTimeoutWarningResp3(env, res,
                                  message="sticky RETURN cursor-read must produce a timeout warning")
        env.assertNotEqual(cursor_id_after, 0,
                           message="Sticky RETURN must keep the cursor live after a timeout")

        # Coord warning metric bumps (RETURN), error metric does not (no FAIL).
        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC],
                        str(base_warn_coord + 1),
                        message="Coord timeout warning should be +1 after sticky RETURN timeout")
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC],
                        str(base_err_coord),
                        message="Coord timeout error must not bump: cursor's sticky RETURN policy must win")

        # Free the still-live cursor so it doesn't leak past the test.
        env.expect('FT.CURSOR', 'DEL', 'idx', cursor_id_after).ok()

        env.assertEqual(env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG], 'fail',
                        message="Global timeout policy should remain 'fail' after sticky-policy test")
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy).ok()

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
