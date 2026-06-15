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

def _setup_return_strict_cursor_state(env, chunk_size=10, agg_steps=None):
    """Switch shards to RETURN_STRICT, create a WITHCURSOR aggregate, and return
    ``(prev_policy, cursor_id, baseline_cursor_total, before_info, base_warn_coord)``.

    `agg_steps` is an optional list of pipeline tokens inserted between the query
    string and ``WITHCURSOR`` (e.g. ``['SORTBY', '1', '@name']``).
    """
    # CONFIG GET returns a dict under RESP3 and a flat [key, value] list under RESP2.
    prev_cfg = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)
    prev_on_timeout_policy = (prev_cfg[ON_TIMEOUT_CONFIG] if isinstance(prev_cfg, dict)
                              else prev_cfg[1])
    run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict')
    before_info = info_modules_to_dict(env)
    base_warn_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])
    cmd = ['FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@name']
    if agg_steps:
        cmd += list(agg_steps)
    cmd += ['WITHCURSOR', 'COUNT', str(chunk_size)]
    res, cursor_id = env.cmd(*cmd)
    env.assertNotEqual(cursor_id, 0, message="Expected non-zero cursor ID")
    baseline_cursor_total = _coord_cursor_total(env)
    return prev_on_timeout_policy, cursor_id, baseline_cursor_total, before_info, base_warn_coord, res

def _get_coord_req_ctx_free_count(env):
    """Read the coordinator CoordRequestCtx_Free invocation counter (debug builds)."""
    return int(env.cmd(debug_cmd(), 'QUERY_CONTROLLER', 'GET_COORD_REQ_CTX_FREE_COUNT'))

def _assert_return_strict_cursor_timeout_reply(env, res_pair, expected_cid,
                                               expected_results,
                                               message_prefix=""):
    """Assert RESP3 cursor-shaped timeout reply: ``({...}, cid)`` with a TIMEOUT
    warning and a preserved cursor id.
    """

    res, cid = res_pair
    env.assertEqual(cid, expected_cid,
                    message=f"{message_prefix}: cursor id should be preserved on timeout")
    env.assertEqual(len(res['results']), expected_results,
                        message=f"{message_prefix}: got {res}")
    env.assertEqual(res.get('warning', []), [TIMEOUT_WARNING],
                    message=f"{message_prefix}: expected [TIMEOUT_WARNING], got {res.get('warning')}")

def _assert_cursor_read_happy_path(env, cursor_id, chunk_size=10, message_prefix=""):
    """Assert happy-path FT.CURSOR READ: non-empty results and no warnings."""
    prev_info = info_modules_to_dict(env)
    read_res, read_cid = env.cmd('FT.CURSOR', 'READ', 'idx', str(cursor_id))
    env.assertEqual(read_cid, cursor_id,
                        message=f"{message_prefix}: Cursor should still be open after one follow-up read")
    rows = read_res.get('results', [])
    env.assertEqual(len(rows), chunk_size,
                    message=f"{message_prefix}: Expected exactly {chunk_size} rows on follow-up read, "
                            f"got {rows}")
    env.assertEqual(read_res.get('warning', []), [],
                    message=f"{message_prefix}: Expected no warnings on follow-up read, got {read_res.get('warning')}")
    _verify_metrics_not_changed(env, env, prev_info, [TIMEOUT_WARNING_COORD_METRIC])


def _start_collecting_cursor_read(env, cursor_id, out_list, blocked_msg='Client for FT.CURSOR|READ not found'):
    """Run ``FT.CURSOR READ`` in a thread, collecting the reply tuple in `out_list`,
    and return ``(thread, blocked_client_id)`` once the BC is parked."""
    t_query = threading.Thread(
        target=call_and_store,
        args=(env.cmd, ['FT.CURSOR', 'READ', 'idx', str(cursor_id)], out_list),
        daemon=True
    )
    t_query.start()
    blocked_client_id = wait_for_blocked_query_client(env, 'FT.CURSOR|READ', blocked_msg)
    return t_query, blocked_client_id

def _drain_cursor(env, cursor_id, idx='idx'):
    """Drain a paused cursor to completion, returning the total number of rows seen.

    Handles both the RESP3 dict-shaped reply (``res['results']``) and the RESP2
    array-shaped reply where ``res[0]`` is total_results and the row entries
    follow.
    """
    total_rows = 0
    cid = cursor_id
    while cid != 0:
        res, cid = env.cmd('FT.CURSOR', 'READ', idx, cid)
        if isinstance(res, dict):
            total_rows += len(res.get('results', []))
        else:
            # RESP2: [total_results, row0, row1, ...]
            total_rows += max(0, len(res) - 1)
    return total_rows

def _reply_row_count(res):
    if isinstance(res, dict):
        return len(res.get('results', []))
    # RESP2 aggregate cursor reply: [total_results, row0, row1, ...]
    return max(0, len(res) - 1)

def _assert_aggregate_cursor_total_rows(env, first_res, cursor_id, expected_rows, context):
    total_rows = _reply_row_count(first_res) + _drain_cursor(env, cursor_id)
    env.assertEqual(total_rows, expected_rows,
                    message=f"{context}: expected {expected_rows} rows across "
                            f"FT.AGGREGATE + FT.CURSOR READ replies, got {total_rows}")
    if cursor_id:
        env.expect('FT.CURSOR', 'READ', 'idx', str(cursor_id)).error().contains('Cursor not found')

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

def is_client_blocked(target, client_id):
    """Check if a client is blocked based on its flags.

    A client is blocked when it has the 'b' flag set, which indicates
    the client is waiting in a blocking operation.

    `target` may be an Env (uses ``getConnectionByEnv``) or a raw connection
    (used directly), so per-shard tests can poll a specific shard's process.
    """
    conn = getConnectionByEnv(target) if hasattr(target, 'getConnection') else target
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


def _split_shards_pick_one_paused(env):
    """Pick one non-coordinator shard to designate as paused and split the rest.

    Returns ``(all_shard_conns, paused_conn, paused_pid, responsive_conns)``.
    Asserts that at least one non-coordinator shard exists.
    """
    all_shard_conns = [env.getConnection(i) for i in range(1, env.shardsCount + 1)]
    non_coord_conns = _non_coord_shard_conns(env)
    env.assertGreater(len(non_coord_conns), 0,
                      message="Test requires at least one non-coordinator shard")
    paused_conn = non_coord_conns[0]
    paused_pid = pid_cmd(paused_conn)
    responsive_conns = [c for c in all_shard_conns if pid_cmd(c) != paused_pid]
    return all_shard_conns, paused_conn, paused_pid, responsive_conns


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


def _wait_shard_paused_after_aggregate_result(shard_conn, cmd_name, timeout=30):
    """Wait for `shard_conn` to be parked in the AggregateResults spin loop
    (i.e. mid-pipeline, with N rows already appended) while a client is
    blocked running `cmd_name`. Returns the blocked client id."""
    with TimeLimit(timeout, f'Shard not paused at AggregateResults with a blocked {cmd_name} client'):
        while True:
            if getIsAggregateResultsPaused(shard_conn) == 1:
                cid = get_query_client(shard_conn, cmd_name)
                if cid:
                    return cid
            time.sleep(0.1)


def _internal_hybrid_cursor_map(result):
    if isinstance(result, dict):
        result = dict(result)
        result.pop('warnings', None)
        return result

    if 'warnings' in result:
        result = result[:result.index('warnings')]
    return to_dict(result)


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

    def test_aggregate_cursor_reply_count_cluster(self):
        """Cluster FT.AGGREGATE WITHCURSOR drains exactly all aggregate rows."""
        env = self.env
        chunk_size = 7

        prev_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict').ok()
        try:
            first_res, cursor_id = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@name',
                                           'WITHCURSOR', 'COUNT', str(chunk_size))
            env.assertNotEqual(cursor_id, 0, message="Expected non-zero cursor ID")
            env.assertEqual(first_res.get('warning', []), [],
                            message=f"Happy aggregate cursor reply should not warn: {first_res}")

            _assert_aggregate_cursor_total_rows(
                env, first_res, cursor_id, self.n_docs,
                'cluster happy FT.AGGREGATE WITHCURSOR')
        finally:
            env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy).ok()

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

    def test_return_strict_internal_hybrid_vsim_cursor_read_timeout_uses_cached_policy(self):
        """_FT.HYBRID-created VSIM cursors keep RETURN_STRICT for _FT.CURSOR READ.

        The shard cursor is created while ON_TIMEOUT is return-strict, then the
        live shard config is changed back to return before reading. The
        subsequent _FT.CURSOR READ must still use the cursor's cached policy,
        arm the RETURN_STRICT blocked-client timeout callback, and reply with a
        cursor-shaped timeout warning instead of a hard timeout error.
        """
        env = self.env
        skipIfNoEnableAssert(env)

        non_coord_shards = _non_coord_shard_conns(env)
        env.assertGreater(len(non_coord_shards), 0,
                          message="Test requires a non-coordinator shard")
        target_shard = non_coord_shards[0]
        target_pid = pid_cmd(target_shard)
        target_shard_id = next(
            shard_id for shard_id in range(1, env.shardsCount + 1)
            if pid_cmd(env.getConnection(shard_id)) == target_pid)
        slots_by_shard = dict(get_shard_slot_ranges(env))
        slots_data = slots_by_shard[target_shard_id]

        prev_policy = target_shard.execute_command('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        before_info = info_modules_to_dict(target_shard)
        base_err_shard = int(before_info[WARN_ERR_SECTION][TIMEOUT_ERROR_SHARD_METRIC])
        base_warn_shard = int(before_info[WARN_ERR_SECTION][TIMEOUT_WARNING_SHARD_METRIC])

        try:
            target_shard.execute_command('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict')
            target_shard.execute_command('DEBUG', 'MARK-INTERNAL-CLIENT')
            cursor_reply = target_shard.execute_command(
                '_FT.HYBRID', 'hybrid_idx',
                'SEARCH', '*',
                'VSIM', '@embedding', '$BLOB',
                'WITHCURSOR', 'COUNT', '1',
                '_SLOTS_INFO', slots_data,
                'PARAMS', '2', 'BLOB', self.hybrid_query_vec,
                '_COORD_DISPATCH_TIME', '1000000')
            cursors = _internal_hybrid_cursor_map(cursor_reply)
            env.assertIn('VSIM', cursors)
            env.assertIn('SEARCH', cursors)
            vsim_cursor = cursors['VSIM']
            search_cursor = cursors['SEARCH']
            env.assertNotEqual(vsim_cursor, 0, message="VSIM cursor should be active")

            # Prove _FT.CURSOR READ uses the cursor snapshot rather than the
            # current config value.
            target_shard.execute_command('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return')
            target_shard.execute_command(debug_cmd(), 'WORKERS', 'pause')

            read_result = []
            def read_vsim_cursor():
                try:
                    read_result.append(target_shard.execute_command(
                        '_FT.CURSOR', 'READ', 'hybrid_idx', str(vsim_cursor)))
                except redis_exceptions.ResponseError as e:
                    read_result.append(str(e))

            try:
                t_query = threading.Thread(
                    target=read_vsim_cursor,
                    daemon=True)
                t_query.start()
                blocked_client_id = wait_for_blocked_query_client(
                    target_shard, '_FT.CURSOR|READ',
                    f'Client for _FT.CURSOR|READ not found on shard pid={pid_cmd(target_shard)}')
                env.assertEqual(
                    target_shard.execute_command('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT'),
                    1,
                    message="CLIENT UNBLOCK on shard's _FT.CURSOR|READ should report 1")
                wait_for_client_unblocked(target_shard, blocked_client_id)
                t_query.join(timeout=10)
                env.assertFalse(t_query.is_alive(),
                                message="VSIM cursor read thread should have finished")
                env.assertEqual(len(read_result), 1, message="Expected one cursor read result")
                env.assertFalse(isinstance(read_result[0], str),
                                message=f"RETURN_STRICT cursor read must not hard-error: {read_result[0]}")
                _assert_return_strict_cursor_timeout_reply(
                    env, read_result[0], vsim_cursor, expected_results=0,
                    message_prefix='VSIM _FT.CURSOR READ RETURN_STRICT timeout')

            finally:
                target_shard.execute_command(debug_cmd(), 'WORKERS', 'resume')
                target_shard.execute_command(debug_cmd(), 'WORKERS', 'drain')

            wait_for_info_metric(
                target_shard, [WARN_ERR_SECTION, TIMEOUT_WARNING_SHARD_METRIC],
                str(base_warn_shard + 1),
                msg="VSIM _FT.CURSOR READ RETURN_STRICT timeout warning should bump shard metric")

            after_info = info_modules_to_dict(target_shard)
            env.assertEqual(after_info[WARN_ERR_SECTION][TIMEOUT_ERROR_SHARD_METRIC],
                            str(base_err_shard),
                            message="RETURN_STRICT cursor read timeout must not bump shard error metric")
            target_shard.execute_command('_FT.CURSOR', 'DEL', 'hybrid_idx', str(vsim_cursor))
            if search_cursor:
                target_shard.execute_command('_FT.CURSOR', 'DEL', 'hybrid_idx', str(search_cursor))
        finally:
            try:
                target_shard.execute_command(debug_cmd(), 'WORKERS', 'resume')
                target_shard.execute_command(debug_cmd(), 'WORKERS', 'drain')
            except Exception:
                pass
            target_shard.execute_command('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy)

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
        setPauseBeforeStoreResults(env, True, internal=False)

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
        setPauseAfterStoreResults(env, True, internal=False)

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
            setPauseBeforeStoreResults(env, True, internal=False)
        else:
            setPauseAfterStoreResults(env, True, internal=False)

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
        """RETURN_STRICT timeout with one shard's reply gated off forever.

        Pauses the search worker pool on every shard so no `_FT.AGGREGATE`
        job can run. Resumes the responsive shards one at a time, parking
        BG at `RpnetReplyAdmitted` between admissions so each reply is
        fully integrated before the next one is in flight. The chosen
        `paused` shard's workers are never resumed, so its reply never
        arrives; firing the blocked-client timeout drains BG with the
        already-accumulated rows from every other shard.

        Determinism comes from never having more than one reply in flight:
        between SIGNAL and the next resume, BG is guaranteed to be parked
        in `MRIterator_PopWithTimeout` with an empty channel.
        """
        env = self.env
        skipIfNoEnableAssert(env)

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict')

        before_info = info_modules_to_dict(env)
        base_warn_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])

        all_shard_conns, paused_conn, paused_pid, responsive_conns = \
            _split_shards_pick_one_paused(env)

        # Docs on responsive shards. The paused shard's docs never reach BG,
        # so this is the exact count BG will emit (one _FT.AGGREGATE reply per
        # responsive shard, each carrying that shard's docs as rows).
        expected_partial = sum(len(c.execute_command('KEYS', 'doc*'))
                               for c in responsive_conns)

        # Pause workers on every shard so no `_FT.AGGREGATE` job can run.
        verify_command_OK_on_all_shards(env, debug_cmd(), 'WORKERS', 'pause')
        workers_paused = True

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

        try:
            for c in responsive_conns:
                base_jobs = getWorkersThpoolStatsFromShard(c)['totalJobsDone']
                c.execute_command(debug_cmd(), 'WORKERS', 'resume')

                # Wait for the shard to run its `_FT.AGGREGATE` job (so the
                # reply has been sent), then for BG to park at the sync
                # point (so the reply has been admitted on the coord).
                wait_for_condition(
                    lambda c=c, base_jobs=base_jobs: (
                        getWorkersThpoolStatsFromShard(c)['totalJobsDone'] > base_jobs,
                        {'totalJobsDone': getWorkersThpoolStatsFromShard(c)['totalJobsDone'],
                         'base': base_jobs}),
                    'Timeout waiting for shard to process its _FT.AGGREGATE job'
                )
                wait_for_condition(
                    lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sync_point) == 1, {}),
                    f'Timeout waiting for BG to park at {sync_point} after resuming a shard'
                )

                # Release BG, then wait for it to fully exit the sync-point
                # spin loop before re-arming. Once IS_WAITING is 0, BG is
                # back in MRIterator_PopWithTimeout with an empty channel
                # (the next responsive shard's workers are still paused),
                # so the next ARM cannot race with an in-flight reply.
                env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point)
                wait_for_condition(
                    lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sync_point) == 0, {}),
                    f'Timeout waiting for BG to exit {sync_point}'
                )
                env.cmd(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point)

            # All responsive replies are in. BG is blocked in pop waiting
            # for the (never-arriving) paused shard. Fire the blocked-client
            # timeout; the abort flag wakes the pop and BG returns TIMEDOUT
            # with the accumulated rows intact.
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

        finally:
            # Best-effort cleanup: disarm the sync point and resume any shard
            # whose workers are still paused. On the happy path only
            # `paused_conn` is still paused; on a mid-loop failure several
            # shards may need resuming. WORKERS resume returns ERR if a
            # shard is already running, so swallow per-shard errors.
            try:
                env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
            except Exception:
                pass
            for c in all_shard_conns:
                try:
                    c.execute_command(debug_cmd(), 'WORKERS', 'resume')
                except Exception:
                    pass
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

        _, _, paused_pid, responsive_shard_conns = \
            _split_shards_pick_one_paused(env)

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

    def test_return_strict_timeout_all_shards_paused_aggregate_with_cursor_count(self):
        """Cluster FT.AGGREGATE WITHCURSOR timeout with no shard replies returns no rows."""
        env = self.env
        skipIfNoEnableAssert(env)

        prev_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict')

        before_info = info_modules_to_dict(env)
        base_warn_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])

        verify_command_OK_on_all_shards(env, debug_cmd(), 'WORKERS', 'pause')
        workers_paused = True

        sync_point = 'AfterIteratorStart'
        env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
        env.cmd(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point)

        query_result = []
        t_query = threading.Thread(
            target=call_and_store,
            args=(env.cmd,
                  ['FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@name',
                   'WITHCURSOR', 'COUNT', '5'],
                  query_result),
            daemon=True
        )
        t_query.start()

        try:
            wait_for_condition(
                lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sync_point) == 1, {}),
                f'Timeout waiting for {sync_point} sync point'
            )

            blocked_client_id = wait_for_blocked_query_client(env, 'FT.AGGREGATE')
            env.cmd('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT')
            wait_for_client_unblocked(env, blocked_client_id)

            env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point)

            t_query.join(timeout=10)
            env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

            env.assertEqual(len(query_result), 1, message="Expected 1 result from query thread")
            res, cursor_id = query_result[0]
            env.assertEqual(_reply_row_count(res), 0,
                            message=f"Expected no rows in timed-out first page, got {res}")
            env.assertNotEqual(cursor_id, 0,
                               message=f"Cursor should be preserved for delayed shard replies: {query_result[0]}")
            env.assertEqual(res.get('warning', []), [TIMEOUT_WARNING])

            verify_command_OK_on_all_shards(env, debug_cmd(), 'WORKERS', 'resume')
            workers_paused = False
            _assert_aggregate_cursor_total_rows(
                env, res, cursor_id, self.n_docs,
                'cluster all-shards-paused FT.AGGREGATE WITHCURSOR timeout')

            after_info = info_modules_to_dict(env)
            env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC],
                            str(base_warn_coord + 1),
                            message="Coordinator timeout warning should be +1")
            _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_WARNING_COORD_METRIC])
        finally:
            try:
                env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
            except Exception:
                pass
            if workers_paused:
                verify_command_OK_on_all_shards(env, debug_cmd(), 'WORKERS', 'resume')
            env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy)

    def test_return_strict_timeout_channel_drain_aggregate(self):
        """RETURN_STRICT timeout while shard replies are queued in the channel.

        Parks BG at RpnetReplyAdmitted after the first reply is admitted, waits
        until every shard reply has been admitted into the coordinator's
        channel (FT.DEBUG BG_PENDING_REPLIES == 0), then fires the blocked-client
        timeout. BG breaks out of the interruptible wait via the timedOut flag
        and drains the queued items (PopWithTimeout returns queued items
        regardless of the abort flag), then completes the pipeline naturally
        because MRIterator_GetPending is already 0. The full row count must be
        present in the reply.
        """
        env = self.env
        skipIfNoEnableAssert(env)

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict')

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

        # Wait until every shard's reply has been admitted into the coordinator
        # channel. `BG_PENDING_REPLIES` returns the iterator's `pending` counter
        # (number of shards that have not yet sent EOF). The IO callback
        # decrements `pending` only after it has called MRChannel_Push on the
        # reply, so reaching 0 guarantees all replies are physically queued.
        wait_for_condition(
            lambda: (env.cmd(debug_cmd(), 'BG_PENDING_REPLIES') == 0,
                     {'pending': env.cmd(debug_cmd(), 'BG_PENDING_REPLIES')}),
            'Timeout waiting for all shard replies to be admitted into the coordinator channel'
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

    def _drive_one_shard_paused_aggregate_return_strict(self, agg_steps, assert_reply):
        """Shared driver for one-shard-paused RETURN_STRICT FT.AGGREGATE timeout tests.

        Configures ``return-strict``, pauses every shard's worker pool,
        starts ``FT.AGGREGATE idx * <agg_steps>`` on a thread, then
        resumes responsive shards one at a time while parking BG at the
        ``RpnetWaitingForReply`` sync point between iterations so each
        reply is fully drained into the sorter before the next one is in
        flight. Once every responsive reply has been drained, fires
        ``CLIENT UNBLOCK ... TIMEOUT`` and joins the query thread.

        ``assert_reply(result, responsive_count)`` is invoked inside the
        try block after the reply has been parsed; it must assert the
        test-specific reply-shape expectations (``total_results``,
        ``results``). ``responsive_count`` is the number of docs on
        responsive shards. The shared assertions (single reply,
        ``TIMEOUT`` warning, coord warning counter ``+1``, other metrics
        unchanged) and full cleanup (sync-point clear, WORKERS resume on
        every shard, restore previous on-timeout policy) are performed
        by this driver.
        """
        env = self.env
        skipIfNoEnableAssert(env)

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict')

        before_info = info_modules_to_dict(env)
        base_warn_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])

        all_shard_conns, paused_conn, paused_pid, responsive_conns = \
            _split_shards_pick_one_paused(env)

        # Docs on responsive shards. The paused shard's docs never reach
        # BG, so this is the exact count BG sees as admitted.
        responsive_count = sum(len(c.execute_command('KEYS', 'doc*'))
                               for c in responsive_conns)

        # Pause workers on every shard so no `_FT.AGGREGATE` job can run.
        verify_command_OK_on_all_shards(env, debug_cmd(), 'WORKERS', 'pause')

        # `RpnetWaitingForReply` parks BG in `getNextReply` immediately
        # before the channel pop, i.e. AFTER any previously admitted reply
        # has been fully drained downstream. That gives an exact post-drain
        # checkpoint, unlike `RpnetReplyAdmitted` which parks BG mid-batch
        # before its rows are yielded.
        sync_point = 'RpnetWaitingForReply'
        env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
        env.cmd(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point)

        query_result = []
        t_query = threading.Thread(
            target=call_and_store,
            args=(env.cmd,
                  ['FT.AGGREGATE', 'idx', '*'] + list(agg_steps),
                  query_result),
            daemon=True
        )
        t_query.start()

        blocked_client_id = wait_for_blocked_query_client(env, 'FT.AGGREGATE')

        # BG enters `getNextReply` for the very first time and parks at the
        # sync point with no previous batch to drain.
        wait_for_condition(
            lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sync_point) == 1, {}),
            f'Timeout waiting for BG to park at {sync_point} initially'
        )

        try:
            for c in responsive_conns:
                base_jobs = getWorkersThpoolStatsFromShard(c)['totalJobsDone']

                # Release BG into the channel pop, then re-arm BEFORE
                # resuming the worker so BG re-parks after fully draining
                # this shard's reply (rather than racing past the disarmed
                # sync point into the next pop).
                env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point)
                wait_for_condition(
                    lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sync_point) == 0, {}),
                    f'Timeout waiting for BG to exit {sync_point}'
                )
                env.cmd(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point)

                # Resume the worker. BG: pops the reply, drains every row
                # into the sorter via repeated `rpnetNext` calls, re-enters
                # `getNextReply`, parks at the sync point.
                c.execute_command(debug_cmd(), 'WORKERS', 'resume')
                wait_for_condition(
                    lambda c=c, base_jobs=base_jobs: (
                        getWorkersThpoolStatsFromShard(c)['totalJobsDone'] > base_jobs,
                        {'totalJobsDone': getWorkersThpoolStatsFromShard(c)['totalJobsDone'],
                         'base': base_jobs}),
                    'Timeout waiting for shard to process its _FT.AGGREGATE job'
                )
                wait_for_condition(
                    lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sync_point) == 1, {}),
                    f'Timeout waiting for BG to re-park at {sync_point} after draining'
                )

            # All responsive replies are fully drained into the heap and BG
            # is parked at the sync point. Fire the timeout: BG breaks out
            # of the spin loop via `areq_timed_out` and the channel pop
            # returns NULL because `syncCtx.timedOut` was set first.
            env.cmd('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT')
            wait_for_client_unblocked(env, blocked_client_id)

            t_query.join(timeout=10)
            env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

            env.assertEqual(len(query_result), 1, message="Expected 1 result from query thread")
            result = query_result[0]

            assert_reply(result, responsive_count)

            # WITHCURSOR replies are shaped ``(dict, cid)``; bare aggregate
            # replies are the dict itself. Unwrap so the shared warning check
            # works for both shapes.
            reply_dict = result[0] if isinstance(result, (list, tuple)) else result
            env.assertEqual(reply_dict.get('warning', []), [TIMEOUT_WARNING])

            after_info = info_modules_to_dict(env)
            env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC],
                            str(base_warn_coord + 1),
                            message="Coordinator timeout warning should be +1")
            _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_WARNING_COORD_METRIC])

        finally:
            # Best-effort cleanup: disarm the sync point and resume any
            # shard whose workers are still paused. WORKERS resume
            # returns ERR if a shard is already running, so swallow
            # per-shard errors.
            try:
                env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
            except Exception:
                pass
            for c in all_shard_conns:
                try:
                    c.execute_command(debug_cmd(), 'WORKERS', 'resume')
                except Exception:
                    pass
            env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy)

    def _run_return_strict_timeout_sortby_one_shard_paused_aggregate(self, agg_steps, sort_field):
        """RETURN_STRICT one-shard-paused helper for FT.AGGREGATE shapes ending in RPSorter.

        Runs an FT.AGGREGATE whose coordinator pipeline ends in RPSorter
        (optionally with an RPPager_Limiter directly above it; any number
        of intermediate RPs are allowed between RPSorter and RPNet) with
        one non-coordinator shard's reply gated off forever, and asserts
        that the sorter's buffered prefix is harvested as the partial
        reply.

        ``agg_steps`` is the argument list following the query expression
        (must include a SORTBY clause and a ``LIMIT 0 self.n_docs`` sizing
        the sorter heap so that all responsive-shard rows fit).
        ``sort_field`` is the attribute whose values are asserted to be
        sorted in the reply.
        """
        env = self.env

        def assert_reply(result, responsive_count):
            env.assertEqual(len(result.get('results', [])), responsive_count,
                            message=f"Expected {responsive_count} sorted rows from responsive "
                                    f"shards in reply, got {len(result.get('results', []))}")
            values = [row['extra_attributes'][sort_field] for row in result['results']]
            env.assertEqual(values, sorted(values),
                            message=f"Rows must be sorted by @{sort_field}, got {values}")

        self._drive_one_shard_paused_aggregate_return_strict(agg_steps, assert_reply)

    def test_return_strict_timeout_sortby_one_shard_paused_aggregate(self):
        """RETURN_STRICT timeout on FT.AGGREGATE SORTBY with one shard suspended.

        Coordinator pipeline shape: Pager -> RPSorter -> RPNet. The end is
        an RPPager_Limiter sitting directly above RPSorter, so
        pipelineCanYieldPartialResults peels the pager and accepts shape
        (sorter directly above the network root). On TIMEDOUT, RPSorter
        freezes its heap and switches to yield mode; the BG thread's
        AggregateResults pops the buffered prefix and stores it for the
        main-thread reply.

        Uses the RpnetWaitingForReply sync point to park BG after each
        responsive shard's reply is fully drained into the pipeline (and
        thus merged into the sorter's heap), so the partial-row count is
        exact.
        """
        self._run_return_strict_timeout_sortby_one_shard_paused_aggregate(
            agg_steps=['SORTBY', '1', '@name', 'LIMIT', '0', str(self.n_docs)],
            sort_field='name')

    def test_return_strict_timeout_apply_sortby_one_shard_paused_aggregate(self):
        """RETURN_STRICT timeout on FT.AGGREGATE APPLY ... SORTBY with one shard suspended.

        Exercises the user-visible APPLY ... SORTBY shape. AGGPLN_Distribute
        moves APPLY (and the auto-injected LOAD) onto the shards, so the
        coordinator pipeline ends up identical to the bare SORTBY case:
        RPNet -> RPSorter -> RPPager_Limiter. The classifier accepts
        (peels the pager, sees RPSorter directly above RPNet), and the
        sorter's buffered prefix is harvested.

        The point of the test is to confirm that a query whose user-facing
        shape introduces an upstream projector still yields partial
        results: @uname is materialized by the projector on the shards
        before the sorted rows reach the coord, so the harvested partial
        reply must still expose @uname for every row.
        """
        self._run_return_strict_timeout_sortby_one_shard_paused_aggregate(
            agg_steps=['LOAD', '1', '@name',
                       'APPLY', 'upper(@name)', 'AS', 'uname',
                       'SORTBY', '1', '@uname',
                       'LIMIT', '0', str(self.n_docs)],
            sort_field='uname')

    def test_return_strict_timeout_sortby_with_cursor_one_shard_paused_aggregate(self):
        """RETURN_STRICT timeout on the initial FT.AGGREGATE SORTBY ... WITHCURSOR
        with one shard's reply gated off; subsequent FT.CURSOR READs drain
        the buffered snapshot.

        Coord pipeline: RPNet -> RPSorter -> RPPager_Limiter. RPSorter is in
        Accum mode while admitting shard replies; the timeout fires before
        the paused shard's reply can land, so RPNet returns TIMEDOUT, the
        sorter latches into Yield with only the responsive shards' rows in
        its heap, and ``drainPartialResultsAfterTimeout`` pops up to
        ``chunk_size`` of them into the timed-out reply. Because the heap
        still has rows, ``finishSendChunk`` does not set ``QEXEC_S_ITERDONE``
        and ``AREQ_ReplyWithStoredResults`` parks the cursor instead of
        freeing it.

        Follow-up cursor reads run RPSorter in Yield (heap pop only — never
        re-enters RPNet, per the Yield-latching invariant) so they drain
        the rest of the buffered snapshot. The total rows recovered across
        the timed-out reply and all drain reads must equal exactly the
        number of docs on the responsive shards (the size of the latched
        heap), and must be strictly less than ``self.n_docs`` because the
        paused shard's docs never made it into the heap.
        """
        env = self.env
        chunk_size = 5

        def assert_reply(reply, responsive_count):
            env.assertLess(responsive_count, self.n_docs,
                           message="responsive shards must hold fewer than n_docs "
                                   "(paused shard contributes none)")
            env.assertGreater(responsive_count, chunk_size,
                              message="responsive_count must exceed chunk_size so the "
                                      "cursor stays paused after the timed-out reply")

            res, cursor_id = reply
            env.assertNotEqual(cursor_id, 0,
                               message=f"cursor must be preserved when the latched heap "
                                       f"still has rows after the harvest; reply={reply!r}")

            timeout_reply_rows = len(res.get('results', []))
            env.assertEqual(timeout_reply_rows, chunk_size,
                            message=f"reply={reply!r}")

            sort_values = [row['extra_attributes']['name'] for row in res['results']]
            env.assertEqual(sort_values, sorted(sort_values),
                            message=f"harvest rows must be sorted by @name; reply={reply!r}")

            drain_rows = _drain_cursor(env, cursor_id)
            total = timeout_reply_rows + drain_rows
            env.assertEqual(total, responsive_count,
                            message=f"timeout_reply_rows={timeout_reply_rows} "
                                    f"drain_rows={drain_rows} reply={reply!r}")

            env.expect('FT.CURSOR', 'READ', 'idx', str(cursor_id)).error().contains('Cursor not found')

        self._drive_one_shard_paused_aggregate_return_strict(
            agg_steps=['SORTBY', '1', '@name',
                       'LIMIT', '0', str(self.n_docs),
                       'WITHCURSOR', 'COUNT', str(chunk_size)],
            assert_reply=assert_reply)

    def _run_return_strict_timeout_no_partial_rows_one_shard_paused_aggregate(
            self, agg_steps):
        """RETURN_STRICT one-shard-paused helper for FT.AGGREGATE shapes that must NOT yield partial rows.

        Mirrors ``_run_return_strict_timeout_sortby_one_shard_paused_aggregate``
        (same pause/resume/sync-point determinism), but asserts that the
        reply contains no rows and a single coord-side TIMEOUT warning.

        ``agg_steps`` is the argument list following the query expression.
        """
        env = self.env

        def assert_reply(result, responsive_count):
            env.assertEqual(result.get('results', []), [],
                            message=f"Expected no rows, got {result.get('results')}")

        self._drive_one_shard_paused_aggregate_return_strict(agg_steps, assert_reply)

    def test_return_strict_timeout_sortby_then_filter_one_shard_paused_aggregate(self):
        """RETURN_STRICT timeout on FT.AGGREGATE SORTBY ... FILTER with one shard's reply gated off.

        Negative counterpart to test_return_strict_timeout_apply_sortby_*:
        the coordinator pipeline here is RPNet -> RPSorter -> RPPager ->
        RPFilter, i.e. RPSorter sits in the middle and an RPFilter is the
        end RP. (FILTER is the only step that AGGPLN_Distribute leaves
        local once a SORTBY has set hadArrange=true; APPLY/LOAD are
        always pushed onto the shards.)

        pipelineCanYieldPartialResults sees RPFilter as the end (not
        RPPager_Limiter, so the pager-peeling branch is not taken) and
        falls through to the final RPSorter check, which fails. The
        coordinator must therefore take the discard path: no rows and a
        TIMEOUT warning, even though the sorter's heap was populated
        from the responsive shards.
        """
        self._run_return_strict_timeout_no_partial_rows_one_shard_paused_aggregate(
            agg_steps=['SORTBY', '1', '@name',
                       'FILTER', '1==1',
                       'LIMIT', '0', str(self.n_docs)])

    def test_return_strict_timeout_groupby_sortby_one_shard_paused_aggregate(self):
        """RETURN_STRICT timeout on FT.AGGREGATE GROUPBY ... SORTBY with one shard's reply gated off.

        Exercises shape 3 of pipelineCanYieldPartialResults: a non-trivial
        RP sits between RPNet and RPSorter on the coordinator. GROUPBY is
        the only step that breaks AGGPLN_Distribute's loop and forces
        all subsequent steps to remain local, so the coord pipeline is
        RPNet -> RPGrouper -> RPSorter -> RPPager_Limiter. The
        classifier peels the pager and accepts (RPSorter at the
        peeled-tail end, an arbitrary intermediate RP between RPSorter
        and RPNet is allowed).

        RPGrouper is fully buffering: ``Grouper_rpAccum`` accumulates
        upstream rows until it observes EOF before transitioning into
        its yield state. The BG pipeline call gets RS_RESULT_TIMEDOUT
        from RPNet first, so the grouper aborts immediately (no flush
        into the sorter) and the sorter heap stays empty. The
        post-timeout drain then finds nothing to pop, yet the harvest
        path still runs end-to-end without crashing -- which is the
        safety property the classifier promises for shape 3.
        """
        self._run_return_strict_timeout_no_partial_rows_one_shard_paused_aggregate(
            agg_steps=['GROUPBY', '1', '@name',
                       'SORTBY', '1', '@name',
                       'LIMIT', '0', str(self.n_docs)])

    def test_return_strict_timeout_sortby_all_shards_paused_aggregate(self):
        """RETURN_STRICT timeout on FT.AGGREGATE SORTBY with every shard paused.

        Pauses the worker thread pool on every shard so no _FT.AGGREGATE reply
        ever reaches the coordinator. BG enters the sorter accumulation phase
        and blocks on the channel waiting for the first reply. Firing the
        blocked-client timeout wakes BG; RPNet returns TIMEDOUT, the sorter
        switches to yield mode with an empty heap and immediately returns
        EOF. The reply carries 0 rows + TIMEOUT warning.

        Mirrors test_return_strict_timeout_all_shards_paused_aggregate but
        exercises the sorter's empty-heap path on TIMEDOUT.
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
            args=(env.cmd,
                  ['FT.AGGREGATE', 'idx', '*', 'SORTBY', '1', '@name',
                   'LIMIT', '0', str(self.n_docs)],
                  query_result),
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
        # Sorter heap was empty when RPNet returned TIMEDOUT -> yield phase
        # pops nothing -> reply carries 0 rows + TIMEOUT warning.
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

    def test_return_strict_timeout_sortby_after_store_aggregate(self):
        """RETURN_STRICT timeout race after the BG SORTBY pipeline stored results.

        SORTBY variant of test_return_strict_timeout_after_store_aggregate.
        BG runs the full pipeline (sorter accumulates all shard rows, drains
        the heap, downstream loaders/pager produce final rows) and stores
        them via AREQ_StoreResults, then parks in debugPauseStoreResults'
        "after store" loop. The blocked-client timeout fires, but the
        pipeline has already completed, so the stored set carries the full,
        sorted result and the reply omits the TIMEOUT warning.
        """
        env = self.env
        skipIfNoEnableAssert(env)

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict')

        before_info = info_modules_to_dict(env)

        setPauseAfterStoreResults(env, True, internal=False)

        query_result = []
        t_query = threading.Thread(
            target=call_and_store,
            args=(env.cmd,
                  ['FT.AGGREGATE', 'idx', '*', 'SORTBY', '1', '@name',
                   'LIMIT', '0', str(self.n_docs)],
                  query_result),
            daemon=True
        )
        t_query.start()

        blocked_client_id = wait_for_blocked_query_client(env, 'FT.AGGREGATE')

        # Wait for BG to park in the "pause after store" loop. At this point
        # the sorter has fully drained and storedReplyState.results carries
        # the complete, sorted result set.
        wait_for_condition(
            lambda: (getIsStoreResultsPaused(env) == 1, {'paused': getIsStoreResultsPaused(env)}),
            'Timeout while waiting for query to pause after store results'
        )

        # Fire the timeout. Callback sets timedOut, loses TryClaim, blocks on
        # AREQ_WaitForAggregateResultsComplete. BG's pause loop observes
        # AREQ_TimedOut and breaks, then signals completion. Callback wakes
        # and replies with the stored rows.
        env.cmd('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT')
        wait_for_client_unblocked(env, blocked_client_id)

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

        env.assertEqual(len(query_result), 1, message="Expected 1 result from query thread")
        result = query_result[0]
        # Pipeline finished before timeout had any chance to abort it: full
        # row count, sorted, no warning, no metric increment.
        env.assertEqual(result['total_results'], self.n_docs,
                        message=f"Expected {self.n_docs} stored results, got {result['total_results']}")
        env.assertEqual(len(result.get('results', [])), self.n_docs,
                        message=f"Expected {self.n_docs} rows, got {len(result.get('results', []))}")
        names = [row['extra_attributes']['name'] for row in result['results']]
        env.assertEqual(names, sorted(names),
                        message=f"Rows must be sorted by @name, got {names}")
        env.assertEqual(result.get('warning', []), [],
                        message=f"Expected no warning (pipeline completed before timeout took effect), "
                                f"got {result.get('warning', [])}")

        _verify_metrics_not_changed(env, env, before_info, [])

        resetStoreResultsDebug(env)
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

        setPauseAfterStoreResults(env, True, internal=False)

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

    # ----- RETURN_STRICT FT.HYBRID timeout tests -----

    def test_return_strict_timeout_before_coord_pickup_hybrid(self):
        """RETURN_STRICT timeout while an FT.HYBRID job is still queued in COORD_THREADS.

        Mirrors test_fail_timeout_before_coord_pickup_hybrid but with the
        return-strict policy: pauses the coordinator thread pool so the
        dispatched FT.HYBRID is never picked up, fires the blocked-client
        timeout, and asserts the callback replies empty + TIMEOUT warning
        (no error). Because the BG worker never ran, the claim/signal
        handshake is irrelevant: the timeout callback observes hreq == NULL
        (or wins TryClaim trivially) and uses common_hybrid_query_reply_empty.
        """
        env = self.env

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict')

        before_info = info_modules_to_dict(env)
        base_warn_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])

        env.expect(debug_cmd(), 'COORD_THREADS', 'PAUSE').ok()
        wait_for_condition(
            lambda: (env.cmd(debug_cmd(), 'COORD_THREADS', 'is_paused') == 1, {}),
            'Timeout while waiting for coordinator threads to pause', timeout=30)

        query_args = [
            'FT.HYBRID', 'hybrid_idx',
            'SEARCH', '*',
            'VSIM', '@embedding', '$BLOB',
            'PARAMS', '2', 'BLOB', self.hybrid_query_vec
        ]
        query_result = []
        t_query = threading.Thread(
            target=call_and_store,
            args=(env.cmd, query_args, query_result),
            daemon=True
        )
        t_query.start()

        blocked_client_id = wait_for_blocked_query_client(env, 'FT.HYBRID')

        env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)
        wait_for_client_unblocked(env, blocked_client_id)

        env.expect(debug_cmd(), 'COORD_THREADS', 'RESUME').ok()
        wait_for_condition(
            lambda: (env.cmd(debug_cmd(), 'COORD_THREADS', 'is_paused') == 0, {}),
            'Timeout while waiting for coordinator threads to resume', timeout=30)

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

        env.assertEqual(len(query_result), 1, message="Expected 1 result from query thread")
        result = query_result[0]
        env.assertEqual(result['total_results'], 0, message=f"Expected 0 results, got: {result}")
        env.assertEqual(result.get('results', []), [],
                        message=f"Expected no rows, got {result.get('results')}")
        assert_timeout_warning(env, result, message=f"FT.HYBRID return-strict before coord pickup, got: {result}")

        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC],
                        str(base_warn_coord + 1),
                        message="Coordinator timeout warning should be +1")
        _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_WARNING_COORD_METRIC])

        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy)

    def test_return_strict_timeout_at_claim_sync_point_hybrid(self):
        """RETURN_STRICT timeout while BG is parked before HybridRequest_TryClaimAggregateResults.

        Mirrors test_return_strict_timeout_at_claim_sync_point_aggregate for
        FT.HYBRID. Uses the BeforeHybridResultsClaim sync point to
        deterministically race the main-thread timeout callback against the
        BG worker's TryClaim. BG is held before the claim so the main-thread
        callback always wins TryClaim and replies empty + timeout warning.
        After unblocking the client, the sync point is signalled so BG
        observes the lost claim and exits startPipelineHybrid cleanly.
        """
        env = self.env
        skipIfNoEnableAssert(env)

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict')

        before_info = info_modules_to_dict(env)
        base_warn_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])

        sync_point = 'BeforeHybridResultsClaim'
        env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
        env.cmd(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point)

        query_args = [
            'FT.HYBRID', 'hybrid_idx',
            'SEARCH', '*',
            'VSIM', '@embedding', '$BLOB',
            'PARAMS', '2', 'BLOB', self.hybrid_query_vec
        ]
        query_result = []
        t_query = threading.Thread(
            target=call_and_store,
            args=(env.cmd, query_args, query_result),
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
        blocked_client_id = wait_for_blocked_query_client(env, 'FT.HYBRID')
        env.cmd('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT')
        wait_for_client_unblocked(env, blocked_client_id)

        # Release BG so it can observe the lost claim and return from startPipelineHybrid.
        env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point)

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

        env.assertEqual(len(query_result), 1, message="Expected 1 result from query thread")
        result = query_result[0]
        env.assertEqual(result['total_results'], 0, message=f"Expected 0 results, got: {result}")
        env.assertEqual(result.get('results', []), [],
                        message=f"Expected no rows, got {result.get('results')}")
        assert_timeout_warning(env, result, message=f"FT.HYBRID return-strict at claim sync point, got: {result}")

        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC],
                        str(base_warn_coord + 1),
                        message="Coordinator timeout warning should be +1")
        _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_WARNING_COORD_METRIC])

        env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy)

    def test_return_strict_timeout_after_store_hybrid(self):
        """RETURN_STRICT timeout race after the BG hybrid pipeline has stored results.

        Mirrors test_return_strict_timeout_after_store_aggregate for FT.HYBRID.
        BG runs the pipeline to completion and stores its results via
        HREQ_StoreResults, then parks in debugPauseStoreResultsHybrid's "after
        store" loop before calling HybridRequest_SignalAggregateResultsComplete.
        The blocked-client timeout callback fires on the main thread:
          - sets timedOut on the HybridRequest
          - loses TryClaim (BG owns it)
          - blocks in HybridRequest_WaitForAggregateResultsComplete

        debugPauseStoreResultsHybrid's loop polls HybridRequest_TimedOut and
        breaks out, so BG proceeds to HybridRequest_SignalAggregateResultsComplete.
        The main-thread callback wakes and replies from the stored state with
        the full set of rows.

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

        setPauseAfterStoreResults(env, True, internal=False)

        # K=10000, WINDOW=10000, LIMIT=10000 (mirrors the FT.HYBRID full-set
        # query used elsewhere in this file) so the BG pipeline produces the
        # complete n_docs result set instead of the default KNN K=10 per shard.
        query_args = [
            'FT.HYBRID', 'hybrid_idx',
            'SEARCH', '*',
            'VSIM', '@embedding', '$BLOB',
            'KNN', '2', 'K', '10000',
            'COMBINE', 'RRF', '2', 'WINDOW', '10000',
            'PARAMS', '2', 'BLOB', self.hybrid_query_vec,
            'LIMIT', '0', '10000'
        ]
        query_result = []
        t_query = threading.Thread(
            target=call_and_store,
            args=(env.cmd, query_args, query_result),
            daemon=True
        )
        t_query.start()

        blocked_client_id = wait_for_blocked_query_client(env, 'FT.HYBRID')

        # Wait for BG to park in the "pause after store" loop. At this point
        # HREQ_StoreResults has populated storedReplyState but
        # HybridRequest_SignalAggregateResultsComplete has not been called yet.
        wait_for_condition(
            lambda: (getIsStoreResultsPaused(env) == 1, {'paused': getIsStoreResultsPaused(env)}),
            'Timeout while waiting for hybrid query to pause after store results'
        )

        # Fire the timeout. Callback sets timedOut, loses TryClaim, and blocks
        # on HybridRequest_WaitForAggregateResultsComplete. BG's pause loop
        # observes HybridRequest_TimedOut and breaks, then signals completion.
        # Callback wakes and replies with the stored results.
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
        env.assertEqual(result.get('warnings', []), [],
                        message=f"Expected no warnings (pipeline completed before timeout took effect), "
                                f"got {result.get('warnings', [])}")

        # Coordinator timeout warning metric must not increment because the
        # timeout callback found stored results and replied with them.
        _verify_metrics_not_changed(env, env, before_info, [])

        resetStoreResultsDebug(env)
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy)

    def test_return_strict_timeout_tail_scheduled_late_hybrid(self):
        """RETURN_STRICT timeout fired after the depleters are scheduled but
        before the tail merger, so the tail is scheduled into an already-replied
        request.

        Arms the ``AfterScheduleDepleters`` sync point so the coordinator
        dispatch thread (``RSExecDistHybrid``) parks after ``scheduleDepleters``
        but before ``scheduleHybridTail``. With the tail not yet scheduled, the
        main-thread timeout callback wins ``HybridRequest_TryClaimAggregateResults``
        and replies via ``sendChunk_ReplyOnly_HybridEmptyResults`` with an empty
        result set and a single TIMEOUT warning. Signalling the sync point then
        lets the dispatcher schedule the tail into the already-replied,
        timed-out request: the tail must lose the claim, drain its depleters,
        and tear ``hreq`` down cleanly without crashing or sending a second
        reply.
        """
        env = self.env
        skipIfNoEnableAssert(env)

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict')

        before_info = info_modules_to_dict(env)
        base_warn_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])

        sync_point = 'AfterScheduleDepleters'
        env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
        env.cmd(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point)

        query_args = [
            'FT.HYBRID', 'hybrid_idx',
            'SEARCH', '*',
            'VSIM', '@embedding', '$BLOB',
            'PARAMS', '2', 'BLOB', self.hybrid_query_vec,
        ]
        query_result = []
        t_query = threading.Thread(
            target=call_and_store,
            args=(env.cmd, query_args, query_result),
            daemon=True
        )
        t_query.start()

        # Fire the timeout. The main-thread callback wins TryClaim (no tail
        # yet) and replies with an empty result set + one TIMEOUT warning.
        blocked_client_id = wait_for_blocked_query_client(env, 'FT.HYBRID')

        try:
            # Wait for the coordinator dispatch thread to park between scheduling
            # the depleters and scheduling the tail merger. At this point hreq
            # exists and is set in the CoordRequestCtx, but no tail can contend
            # for the aggregate-results claim yet.
            wait_for_condition(
                lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sync_point) == 1, {}),
                f'Timeout waiting for dispatcher to park at {sync_point}'
            )

            env.cmd('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT')
            wait_for_client_unblocked(env, blocked_client_id)

            # Release the dispatcher so it schedules the tail into the
            # already-replied request. The tail must lose the claim, drain the
            # depleters, and free hreq without a second reply.
            env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point)

            t_query.join(timeout=10)
            env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

            env.assertEqual(len(query_result), 1, message="Expected exactly 1 reply from query thread")
            result = query_result[0]

            # Win-claim reply path: 0 rows + a single TIMEOUT warning.
            env.assertEqual(result['total_results'], 0,
                            message=f"Expected 0 results, got: {result}")
            env.assertEqual(result.get('results', []), [],
                            message=f"Expected no rows, got {result.get('results')}")
            warnings = result.get('warnings', [])
            env.assertEqual(len(warnings), 1,
                            message=f"Expected a single TIMEOUT warning, got: {warnings}")
            env.assertContains('Timeout', warnings[0],
                               message=f"Expected TIMEOUT warning, got: {warnings}")

            # sendChunk_ReplyOnly_HybridEmptyResults bumps the coord
            # timeout-warning metric exactly once on the win-claim path.
            after_info = info_modules_to_dict(env)
            env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC],
                            str(base_warn_coord + 1),
                            message="Coordinator timeout warning should be +1 (win-claim empty reply)")
            _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_WARNING_COORD_METRIC])
        finally:
            try:
                env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
            except Exception:
                pass
            env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy)

    def _drive_all_shards_paused_hybrid_return_strict(self, agg_steps_suffix, assert_reply):
        """Shared driver for all-shards-paused RETURN_STRICT FT.HYBRID timeout tests.

        Arms ``BeforeCursorReadSendChunk`` on every shard so the initial
        ``_FT.HYBRID`` job runs to completion (cursors stored, ids sent back
        to the coord) but every shard's cursor-read worker then parks at the
        sync point before sending its first chunk. Once every shard is
        parked, the coord BG is guaranteed to be sleeping in
        ``MRChannel_PopWithTimeout`` waiting for cursor-read replies that
        never arrive.

        Fires ``CLIENT UNBLOCK ... TIMEOUT`` to flip ``syncCtx.timedOut`` on
        every subquery AREQ and broadcast on each registered abort channel;
        BG observes the abort, returns ``RS_RESULT_TIMEDOUT`` from each
        depleter, the merger switches to Yield with an empty dict, the tail
        pipeline returns ``TIMEDOUT``, and the main-thread callback replies
        with whatever the tail already placed in ``storedReplyState.results``
        via ``serializeStoredResults_hybrid``. The coordinator hybrid
        pipeline is not drainable, so no partial rows are harvested from the
        tail processors after the deadline.

        ``agg_steps_suffix`` is appended after the standard
        ``SEARCH * VSIM @embedding $BLOB PARAMS 2 BLOB <vec>`` prefix so
        callers can vary the tail pipeline shape (trivial / sorter / grouper)
        and confirm every shape times out to the same empty reply.
        ``assert_reply(result)`` is invoked after the reply has been parsed;
        callers assert their tail-shape-specific reply expectations.

        Shared assertions (single reply, two per-subquery TIMEOUT warnings,
        coord warning counter ``+2``, other metrics unchanged) and full
        cleanup (signal sync point on every shard, restore previous
        on-timeout policy) are performed by this driver.
        """
        env = self.env
        skipIfNoEnableAssert(env)

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict')

        before_info = info_modules_to_dict(env)
        base_warn_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])

        sync_point = 'BeforeCursorReadSendChunk'
        all_shard_conns = [env.getConnection(i) for i in range(1, env.shardsCount + 1)]
        for c in all_shard_conns:
            c.execute_command(debug_cmd(), 'SYNC_POINT', 'CLEAR')
            c.execute_command(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point)

        query_args = [
            'FT.HYBRID', 'hybrid_idx',
            'SEARCH', '*',
            'VSIM', '@embedding', '$BLOB',
            'PARAMS', '2', 'BLOB', self.hybrid_query_vec,
        ] + list(agg_steps_suffix)
        query_result = []
        t_query = threading.Thread(
            target=call_and_store,
            args=(env.cmd, query_args, query_result),
            daemon=True
        )
        t_query.start()

        # Wait until every shard's cursor-read worker is parked at the sync
        # point. Once all are parked, the coord BG is guaranteed to be
        # sleeping in MRChannel_PopWithTimeout waiting for cursor-read replies
        # that will never arrive.
        for c in all_shard_conns:
            wait_for_condition(
                lambda c=c: (c.execute_command(debug_cmd(), 'SYNC_POINT',
                                               'IS_WAITING', sync_point) == 1, {}),
                f'Timeout waiting for shard to park at {sync_point}'
            )

        blocked_client_id = wait_for_blocked_query_client(env, 'FT.HYBRID')
        try:
            env.cmd('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT')
            wait_for_client_unblocked(env, blocked_client_id)

            # Release the parked workers so they complete sendChunk and clean
            # up their cursors. The coord cursors were freed by the timeout
            # reply path, so the eventual cursor-read replies are dropped.
            for c in all_shard_conns:
                c.execute_command(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point)

            t_query.join(timeout=10)
            env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

            env.assertEqual(len(query_result), 1, message="Expected 1 result from query thread")
            result = query_result[0]

            assert_reply(result)

            # Both subqueries (SEARCH and VSIM) were woken by WakeAbortChannel
            # broadcast, each returning RS_RESULT_TIMEDOUT, so the reply
            # carries one timeout warning per subquery (suffixed (SEARCH) /
            # (VSIM)).
            warnings = result.get('warnings', [])
            env.assertEqual(len(warnings), 2,
                            message=f"Expected one TIMEOUT warning per subquery, got: {warnings}")
            env.assertContains('Timeout', warnings[0],
                               message=f"Expected SEARCH TIMEOUT warning, got: {warnings}")
            env.assertContains('Timeout', warnings[1],
                               message=f"Expected VSIM TIMEOUT warning, got: {warnings}")

            # finishSendChunkReply_hybrid -> replyWarningsWithSuffixes bumps
            # the coord timeout-warning metric once per subquery that returned
            # RS_RESULT_TIMEDOUT, so the metric grows by the number of
            # subqueries.
            after_info = info_modules_to_dict(env)
            env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC],
                            str(base_warn_coord + 2),
                            message="Coordinator timeout warning should be +2 (one per subquery)")
            _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_WARNING_COORD_METRIC])
        finally:
            for c in all_shard_conns:
                try:
                    c.execute_command(debug_cmd(), 'SYNC_POINT', 'CLEAR')
                except Exception:
                    pass
            env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy)

    def test_return_strict_timeout_all_shards_paused_hybrid(self):
        """RETURN_STRICT timeout on a trivial FT.HYBRID tail pipeline with
        every shard's cursor-read worker parked before sendChunk.

        Trivial tail = no SORTBY / GROUPBY: the coord pipeline is
        ``RPHybridMerger -> ... -> RPPager_Limiter`` with no RPSorter
        between them. The coordinator hybrid pipeline is not drainable, so
        on timeout the reply carries only whatever BG already moved into
        ``hreq->storedReplyState.results`` before the deadline fired -- the
        merger's accumulation dict is dropped on the abort path and is
        never harvested.

        With every shard parked at ``BeforeCursorReadSendChunk`` BG never
        receives any cursor-read reply, so ``storedReplyState.results`` is
        empty when the timeout fires and the reply carries 0 rows + two
        per-subquery TIMEOUT warnings. This is the channel-wake analogue
        of ``test_return_strict_timeout_all_shards_paused_aggregate``
        (which bumps the warning counter +1; hybrid bumps +2, one per
        subquery, see MOD-15973).
        """
        def assert_reply(result):
            env = self.env
            env.assertEqual(result['total_results'], 0,
                            message=f"Expected 0 results, got: {result}")
            env.assertEqual(result.get('results', []), [],
                            message=f"Expected no rows, got {result.get('results')}")

        self._drive_all_shards_paused_hybrid_return_strict(
            agg_steps_suffix=[], assert_reply=assert_reply)

    def test_return_strict_timeout_sortby_all_shards_paused_hybrid(self):
        """RETURN_STRICT timeout on an FT.HYBRID SORTBY tail pipeline with
        every shard's cursor-read worker parked before sendChunk.

        SORTBY pushes an ``RPSorter`` onto the tail pipeline above the
        merger: ``RPHybridMerger -> ... -> RPSorter -> RPPager_Limiter``.
        The coordinator hybrid pipeline is not drainable, so the sorter
        heap is never popped from the main thread after the deadline; the
        reply carries only what BG stored into ``storedReplyState.results``
        before timing out.

        With every shard parked at ``BeforeCursorReadSendChunk`` BG never
        admits any reply, so the stored results are empty and the reply
        carries 0 rows + two per-subquery TIMEOUT warnings -- identical to
        the trivial-tail case, confirming that a sorter-terminated tail
        gets no special drain treatment.
        """
        def assert_reply(result):
            env = self.env
            env.assertEqual(result['total_results'], 0,
                            message=f"Expected 0 results, got: {result}")
            env.assertEqual(result.get('results', []), [],
                            message=f"Expected no rows, got {result.get('results')}")

        self._drive_all_shards_paused_hybrid_return_strict(
            agg_steps_suffix=['LOAD', '1', '@name',
                              'SORTBY', '1', '@name',
                              'LIMIT', '0', str(self.n_docs)],
            assert_reply=assert_reply)

    def test_return_strict_timeout_groupby_all_shards_paused_hybrid(self):
        """RETURN_STRICT timeout on an FT.HYBRID GROUPBY tail pipeline with
        every shard's cursor-read worker parked before sendChunk.

        GROUPBY breaks ``HybridPlan_Distribute``'s loop and forces every
        subsequent step to remain on the coordinator: the tail pipeline is
        ``RPHybridMerger -> ... -> RPGrouper`` with no RPSorter. The
        coordinator hybrid pipeline is not drainable, and the grouper is
        fully buffering (flushes only on EOF), so on timeout nothing is
        harvested -- the reply carries only what BG stored into
        ``storedReplyState.results`` before timing out.

        With every shard parked at ``BeforeCursorReadSendChunk`` BG never
        admits any reply, the grouper buffer stays empty, and the reply
        carries 0 rows + two per-subquery TIMEOUT warnings -- identical to
        the trivial- and sorter-tail cases.
        """
        def assert_reply(result):
            env = self.env
            env.assertEqual(result['total_results'], 0,
                            message=f"Expected 0 results, got: {result}")
            env.assertEqual(result.get('results', []), [],
                            message=f"Expected no rows, got {result.get('results')}")

        self._drive_all_shards_paused_hybrid_return_strict(
            agg_steps_suffix=['LOAD', '1', '@name',
                              'GROUPBY', '1', '@name',
                              'REDUCE', 'COUNT', '0', 'AS', 'cnt'],
            assert_reply=assert_reply)

    def _drive_one_shard_paused_hybrid_return_strict(self, agg_steps_suffix, assert_reply):
        """Shared driver for one-shard-paused RETURN_STRICT FT.HYBRID timeout tests.

        Hybrid's two-phase protocol (``_FT.HYBRID`` -> cursor mappings ->
        ``_FT.CURSOR READ``) requires the paused shard to complete Phase 1
        before being suspended -- otherwise the coordinator's cursor
        mapping wait blocks forever and Phase 2 never starts. The
        coord-side ``BeforeRPNetStart`` sync point in
        ``rpnetNext_StartWithMappings`` (mirroring the aggregate
        ``rpnetNext_Start`` site) fires after every shard has delivered
        its Phase 1 cursor mapping and just before BG dispatches the
        Phase 2 cursor reads, giving us a deterministic window to
        ``SIGSTOP`` the chosen shard.

        Protocol:
          1. ARM ``BeforeRPNetStart`` on the coord.
          2. Issue FT.HYBRID. Phase 1 runs on every shard (none are
             suspended yet) and BG parks at ``BeforeRPNetStart`` once
             every cursor mapping has been admitted -- i.e. just before
             subquery 0's Phase 2 cursor-read dispatch.
          3. ``SIGSTOP`` the chosen shard's redis process; its already-
             created subquery cursors stay alive but the kernel queues
             any incoming Phase 2 command without delivering it.
          4. SIGNAL ``BeforeRPNetStart``. BG dispatches subquery 0's
             Phase 2 to every shard; responsive shards reply, the
             paused shard's command sits in its receive queue. BG
             accumulates the responsive replies into the merger dict.
          5. Poll ``BG_PENDING_REPLIES`` until it equals 1 -- only the
             paused shard's final reply is outstanding. This guarantees
             the merger dict has absorbed every responsive shard's rows
             before we trip the deadline.
          6. ``CLIENT UNBLOCK ... TIMEOUT``: ``HybridRequest_SetTimedOut``
             flips ``syncCtx.timedOut`` on both subquery AREQs and
             ``WakeAbortChannel`` broadcasts on every registered abort
             channel. BG's pending subquery-0 pop returns NULL with the
             abort flag set; ``rpnetNext`` returns TIMEDOUT. The merger
             advances to subquery 1 whose ``rpnetNext_StartWithMappings``
             passes through ``BeforeRPNetStart`` (now disarmed and
             ``areq_timed_out`` true), dispatches subquery 1's Phase 2,
             then inline ``rpnetNext`` short-circuits on
             ``AREQ_TimedOut`` and also returns TIMEDOUT. The merger
             exits Accum, switches to Yield (RETURN_STRICT skips the
             FAIL-only early-return), and yields its accumulated
             entries through the tail pipeline.
          7. ``SIGCONT`` the shard so its queued cursor reads complete
             and the shard's cursors are released before subsequent
             tests run.

        ``agg_steps_suffix`` is appended after the standard
        ``SEARCH * VSIM @embedding $BLOB KNN ... COMBINE RRF ...``
        prefix. ``assert_reply(result, expected_rows)`` is invoked
        after the reply has been parsed; callers assert their
        tail-shape-specific reply expectations. ``expected_rows`` is
        the number of subquery-0 rows the merger accumulated across
        responsive shards, which the driver counts deterministically.

        Shared assertions (single reply, two per-subquery TIMEOUT
        warnings, coord warning counter ``+2``, other metrics
        unchanged) and best-effort cleanup (SIGCONT the shard, clear
        coord sync, restore previous on-timeout policy) are performed
        by this driver.
        """
        env = self.env
        skipIfNoEnableAssert(env)

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict')

        before_info = info_modules_to_dict(env)
        base_warn_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])

        _, _, paused_pid, responsive_conns = _split_shards_pick_one_paused(env)

        # Each responsive shard contributes one subquery-0 (SEARCH *)
        # reply to the merger's accumulation dict. SEARCH * matches every
        # local doc and one cursor chunk per shard exhausts the cursor at
        # this dataset size. The merger keys its dict by doc id (shards
        # have disjoint key spaces), so accumulated subquery-0 rows =
        # sum of `hybrid_doc*` keys on responsive shards.
        expected_rows = sum(len(c.execute_command('KEYS', 'hybrid_doc*'))
                            for c in responsive_conns)

        # Coord-side gate: parks BG at the top of
        # rpnetNext_StartWithMappings, just before Phase 2 cursor-read
        # dispatch. Fires once per subquery's first Next call; after we
        # SIGNAL it for subquery 0 it is disarmed, so subquery 1's
        # Start passes through immediately (areq_timed_out will be
        # true by then so dispatch is a no-op as well).
        sync_point = 'BeforeRPNetStart'
        env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
        env.cmd(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point)

        # K=10000, WINDOW=10000 so the merger never caps subquery 0's
        # accumulation at the default window=20 (which would let the
        # merger complete subquery 0 from a single responsive shard's
        # reply alone, never blocking on the paused shard and never
        # reaching the timeout path).
        query_args = [
            'FT.HYBRID', 'hybrid_idx',
            'SEARCH', '*',
            'VSIM', '@embedding', '$BLOB',
            'KNN', '2', 'K', '10000',
            'COMBINE', 'RRF', '2', 'WINDOW', '10000',
            'PARAMS', '2', 'BLOB', self.hybrid_query_vec,
        ] + list(agg_steps_suffix)

        query_result = []
        t_query = threading.Thread(
            target=call_and_store,
            args=(env.cmd, query_args, query_result),
            daemon=True
        )
        t_query.start()

        blocked_client_id = wait_for_blocked_query_client(env, 'FT.HYBRID')

        shard_resumed = False
        shard_to_pause_p = psutil.Process(paused_pid)
        try:
            # BG has received every shard's Phase 1 cursor mapping and
            # is now parked at BeforeRPNetStart, about to dispatch
            # subquery 0's Phase 2 cursor reads.
            wait_for_condition(
                lambda: (env.cmd(debug_cmd(), 'SYNC_POINT',
                                 'IS_WAITING', sync_point) == 1, {}),
                f'Timeout waiting for BG to park at {sync_point}'
            )

            # Suspend the chosen shard now: its Phase 1 reply has
            # already arrived, but the upcoming `_FT.CURSOR READ` will
            # sit in its receive queue and never be processed.
            shard_to_pause_p.suspend()
            wait_for_condition(
                lambda: (shard_to_pause_p.status() == psutil.STATUS_STOPPED,
                         {'status': shard_to_pause_p.status()}),
                'Timeout waiting for shard to suspend'
            )

            # Release BG. It dispatches subquery 0's Phase 2 to every
            # shard; responsive shards reply normally, the suspended
            # shard's command is queued in its TCP buffer. BG
            # accumulates the responsive replies into the merger dict
            # for subquery 0, then blocks in the channel pop waiting
            # for the paused shard's reply.
            env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point)

            # Wait until only the paused shard's reply remains
            # outstanding (every responsive reply has been admitted
            # and drained into the merger dict).
            wait_for_condition(
                lambda: (env.cmd(debug_cmd(), 'BG_PENDING_REPLIES') == 1, {}),
                'Timeout waiting for responsive shards to admit subquery-0 replies'
            )

            # Trip the deadline. The merger sees TIMEDOUT from both
            # subquery RPNets, switches to Yield, and pushes its
            # accumulated entries through the tail pipeline.
            env.cmd('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT')
            wait_for_client_unblocked(env, blocked_client_id)

            # Resume the paused shard so its queued cursor reads can
            # complete and free their cursors before the test exits.
            shard_to_pause_p.resume()
            shard_resumed = True

            t_query.join(timeout=10)
            env.assertFalse(t_query.is_alive(), message="Query thread should have finished")
            env.assertEqual(len(query_result), 1, message="Expected 1 result from query thread")
            result = query_result[0]

            assert_reply(result, expected_rows)

            # Both subqueries (SEARCH and VSIM) saw their RPNet return
            # TIMEDOUT, so the reply carries one timeout warning per
            # subquery (suffixed with (SEARCH) / (VSIM)).
            warnings = result.get('warnings', [])
            env.assertEqual(len(warnings), 2,
                            message=f"Expected one TIMEOUT warning per subquery, got: {warnings}")
            env.assertContains('Timeout', warnings[0],
                               message=f"Expected SEARCH TIMEOUT warning, got: {warnings}")
            env.assertContains('Timeout', warnings[1],
                               message=f"Expected VSIM TIMEOUT warning, got: {warnings}")

            after_info = info_modules_to_dict(env)
            env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC],
                            str(base_warn_coord + 2),
                            message="Coordinator timeout warning should be +2 (one per subquery)")
            _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_WARNING_COORD_METRIC])

        finally:
            # Best-effort: resume the shard if we suspended it but
            # didn't get to the happy-path resume, then clear coord
            # sync state.
            if not shard_resumed:
                try:
                    shard_to_pause_p.resume()
                except Exception:
                    pass
            try:
                env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
            except Exception:
                pass
            env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy)

    def test_return_strict_timeout_sortby_one_shard_paused_hybrid(self):
        """RETURN_STRICT timeout on FT.HYBRID SORTBY with one shard suspended.

        Coord tail-pipeline shape:
        ``RPHybridMerger -> ... -> RPSorter -> RPPager_Limiter``. The
        coordinator hybrid pipeline is not drainable, so the sorter heap
        is never popped from the main thread after the deadline.

        This is the strongest no-drain check in the suite: with one shard
        paused before Phase 2 dispatch, the merger's Accum loop has already
        absorbed the responsive shards' subquery-0 rows into its
        accumulation dict, yet that dict is dropped on the abort path and
        is never moved into ``storedReplyState.results``. When the deadline
        trips, BG's pending pop returns TIMEDOUT and BG unwinds without
        pushing the accumulated entries downstream, so the reply carries no
        rows even though partial data existed at abort time.

        Asserts that the reply carries an empty ``results`` array together
        with one TIMEOUT warning per subquery.
        """
        env = self.env

        def assert_reply(result, expected_rows):
            rows = result.get('results', [])
            env.assertEqual(len(rows), 0,
                            message=f"Expected empty results (hybrid pipeline "
                                    f"is not drainable; accumulated merger "
                                    f"entries are dropped on timeout), "
                                    f"got: {result}")

        self._drive_one_shard_paused_hybrid_return_strict(
            agg_steps_suffix=['LOAD', '1', '@name',
                              'SORTBY', '1', '@name',
                              'LIMIT', '0', str(self.n_docs)],
            assert_reply=assert_reply)

    # ----- RETURN_STRICT FT.CURSOR READ timeout tests -----

    def _assert_warn_metric_bumped(self, before_info, base_warn_coord, context):
        env = self.env
        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC],
                        str(base_warn_coord + 1),
                        message=f"Coordinator timeout warning metric should be +1 after {context}")
        _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_WARNING_COORD_METRIC])

    def _drain_paused_cursor_after_timeout(self, cursor_id, baseline_cursor_total, context):
        """After a RETURN_STRICT timeout reply, drain the cursor to EOF and assert it
        was reclaimed cleanly. Returns total rows seen across drain reads."""
        env = self.env
        rows = _drain_cursor(env, cursor_id)
        _wait_for_cursor_cleanup(env, baseline_cursor_total, context)
        env.expect('FT.CURSOR', 'READ', 'idx', str(cursor_id)).error().contains('Cursor not found')
        return rows

    def test_return_strict_timeout_before_coord_pickup_cursor_read(self):
        """Scenario 1: RETURN_STRICT timeout fires before the coord threadpool
        dequeues the FT.CURSOR READ job.

        The coord threadpool is paused so the worker never enters
        ``coordCursorReadReturnStrict`` before the deadline. The timer callback
        observes ``req == NULL`` (BG hasn't called ``SetRequest``), and replies
        directly via ``coord_cursor_read_empty_reply_timeout`` with the cursor
        shape ``({empty results, TIMEOUT warning}, cid)``. The cursor stays
        paused and is reusable on the next FT.CURSOR READ.
        """
        env = self.env
        skipIfNoEnableAssert(env)

        prev_policy, cursor_id, baseline, before_info, base_warn, _ = \
            _setup_return_strict_cursor_state(env)

        env.expect(debug_cmd(), 'COORD_THREADS', 'PAUSE').ok()
        wait_for_condition(
            lambda: (env.cmd(debug_cmd(), 'COORD_THREADS', 'is_paused') == 1, {}),
            'Timeout while waiting for coordinator threads to pause', timeout=30)

        result = []
        try:
            t_query, blocked_client_id = _start_collecting_cursor_read(env, cursor_id, result)
            env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)
            wait_for_client_unblocked(env, blocked_client_id)
        finally:
            env.expect(debug_cmd(), 'COORD_THREADS', 'RESUME').ok()
            wait_for_condition(
                lambda: (env.cmd(debug_cmd(), 'COORD_THREADS', 'is_paused') == 0, {}),
                'Timeout while waiting for coordinator threads to resume', timeout=30)

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Cursor read thread should have finished")
        _assert_return_strict_cursor_timeout_reply(
            env, result[0], cursor_id, expected_results=0,
            message_prefix='RETURN_STRICT pre-pickup cursor-read timeout')
        self._assert_warn_metric_bumped(before_info, base_warn,
                                        'RETURN_STRICT pre-pickup cursor-read timeout')

        # Follow-up read after timeout: cursor must be reusable and return
        # exactly one chunk. BG never ran on the timed-out read, so the cursor's
        # remaining capacity is unchanged from right after the initial AGGREGATE.
        _assert_cursor_read_happy_path(
            env, cursor_id, message_prefix='RETURN_STRICT pre-pickup cursor-read followup')

        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy)

    def test_return_strict_timeout_after_set_request_cursor_read(self):
        """Scenario 2: RETURN_STRICT timeout fires after the BG worker has taken
        the cursor and published the AREQ via ``SetRequest`` but before the
        pipeline has started.

        The BG worker is pinned at ``BeforeCursorReadSendChunk`` (in ``runCursor``
        after ``Cursors_TakeForExecution`` + ``CoordRequestCtx_SetRequest``,
        before ``sendChunk``). The timer callback observes ``req != NULL``,
        sets the ``TimedOut`` atomic, wakes the abort channel and waits on the
        aggregate-results condition. BG early bails on the
        ``TimedOut`` leg before entring the pipeline, stores empty results with ``rc=RS_RESULT_TIMEDOUT``
        and signals completion. The timer wakes, drains the (possibly empty)
        channel and replies cursor-shaped + TIMEOUT warning + cid preserved.
        """
        env = self.env
        skipIfNoEnableAssert(env)

        prev_policy, cursor_id, baseline, before_info, base_warn, _ = \
            _setup_return_strict_cursor_state(env)

        sync_point = 'BeforeCursorReadSendChunk'
        self._arm_cursor_read_sync_point(sync_point)

        result = []
        try:
            t_query, blocked_client_id = _start_collecting_cursor_read(env, cursor_id, result)
            self._wait_worker_pinned_at_sync_point(sync_point)
            env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)
            wait_for_client_unblocked(env, blocked_client_id)
        finally:
            # Best-effort signal in case BG didn't auto-release through areq_timed_out.
            env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point)

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Cursor read thread should have finished")
        chunk_size = 10  # matches _setup_return_strict_cursor_state default
        _assert_return_strict_cursor_timeout_reply(
            env, result[0], cursor_id, expected_results=chunk_size,
            message_prefix='RETURN_STRICT pre-pipeline cursor-read timeout')
        self._assert_warn_metric_bumped(before_info, base_warn,
                                        'RETURN_STRICT pre-pipeline cursor-read timeout')

        # Follow-up read after timeout: cursor must be reusable and return
        # exactly one chunk.
        _assert_cursor_read_happy_path(
            env, cursor_id, message_prefix='RETURN_STRICT pre-pipeline cursor-read followup')

        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy)

    def test_return_strict_timeout_in_pipeline_cursor_read_observed(self):
        """Scenario 3 (observed via the ``rpnetNext`` short-circuit):
        RETURN_STRICT timeout fires while BG is parked at ``BeforeRPNetNext``
        - i.e. at the very top of ``rpnetNext`` before timeout check and return RS_RESULT_TIMEDOUT;
       BG signals completion with ``rc=TIMEDOUT``. The timer wakes, drains the channel and
        replies cursor-shaped + TIMEOUT warning + preserved cid; the cursor
        stays paused so a follow-up drain must succeed.
        """
        env = self.env
        skipIfNoEnableAssert(env)

        prev_policy, cursor_id, baseline, before_info, base_warn, _ = \
            _setup_return_strict_cursor_state(env)

        sync_point = 'BeforeRPNetNext'
        self._arm_cursor_read_sync_point(sync_point)

        result = []
        try:
            t_query, blocked_client_id = _start_collecting_cursor_read(env, cursor_id, result)
            self._wait_worker_pinned_at_sync_point(sync_point)
            env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)
            wait_for_client_unblocked(env, blocked_client_id)
        finally:
            env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point)

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Cursor read thread should have finished")
        chunk_size = 10  # matches _setup_return_strict_cursor_state default
        _assert_return_strict_cursor_timeout_reply(
            env, result[0], cursor_id, expected_results=chunk_size,
            message_prefix='RETURN_STRICT observed in-pipeline cursor-read timeout')
        self._assert_warn_metric_bumped(
            before_info, base_warn,
            'RETURN_STRICT observed in-pipeline cursor-read timeout')

        # Follow-up read after timeout: cursor must be reusable and return
        # exactly one chunk.
        _assert_cursor_read_happy_path(
            env, cursor_id, message_prefix='RETURN_STRICT pre-pipeline cursor-read followup')
        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy)

    def test_return_strict_timeout_in_pipeline_sortby_drain_cursor_read(self):
        """Scenario 2 + drain with SORTBY: after a RETURN_STRICT timeout on a
        SORTBY cursor read where BG was parked before ``sendChunk``, the
        timer's drain pulls remaining rows out of the coordinator-side sorter
        heap (shape 3 in ``pipelineCanYieldPartialResults``) and the cursor
        closes after the buffered snapshot is exhausted.

        SORTBY's per-cursor-read pipeline is ``rpsortNext_Yield`` — a heap pop
        that never re-enters RPNet — so a ``BeforeRPNetNext`` pin would never
        fire here. ``BeforeCursorReadSendChunk`` reliably parks BG just before
        ``sendChunk`` whether the next call is going to enter ``rpnet`` or not.
        The total recovered across all reads cannot exceed the buffered
        snapshot, which is bounded by ``n_docs``.
        """
        env = self.env
        skipIfNoEnableAssert(env)

        chunk_size = 5
        # LIMIT 0 n_docs sizes the sorter heap so all n_docs rows are buffered
        # by the initial FT.AGGREGATE; without an explicit LIMIT, AGGREGATE
        # caps the heap at DEFAULT_LIMIT (10), and the snapshot-pop drain
        # would terminate after only 10 total rows.
        prev_policy, cursor_id, baseline, before_info, base_warn, first_res = \
            _setup_return_strict_cursor_state(
                env, chunk_size=chunk_size,
                agg_steps=['SORTBY', '1', '@name',
                           'LIMIT', '0', str(self.n_docs)])
        first_chunk_rows = len(first_res.get('results', []))

        sync_point = 'BeforeCursorReadSendChunk'
        self._arm_cursor_read_sync_point(sync_point)

        result = []
        try:
            t_query, blocked_client_id = _start_collecting_cursor_read(env, cursor_id, result)
            self._wait_worker_pinned_at_sync_point(sync_point)
            env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)
            wait_for_client_unblocked(env, blocked_client_id)
        finally:
            env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point)

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Cursor read thread should have finished")
        _assert_return_strict_cursor_timeout_reply(
            env, result[0], cursor_id, expected_results=chunk_size,
            message_prefix='RETURN_STRICT in-pipeline sortby drain')
        self._assert_warn_metric_bumped(before_info, base_warn,
                                        'RETURN_STRICT in-pipeline sortby drain')

        timeout_reply_rows = len(result[0][0].get('results', []))
        drain_rows = self._drain_paused_cursor_after_timeout(
            cursor_id, baseline, 'RETURN_STRICT in-pipeline sortby drain')
        total = first_chunk_rows + timeout_reply_rows + drain_rows
        # The initial FT.AGGREGATE completed before the timeout, so RPSorter
        # had already accumulated all n_docs rows into its heap and latched
        # into Yield mode. Subsequent cursor reads (including the timed-out
        # one + drain) only pop from that full heap, so the total recovered
        # across all reads must equal n_docs exactly.
        env.assertEqual(total, self.n_docs,
                        message=f"Expected exactly {self.n_docs} rows across reads "
                                f"after sortby drain timeout, got first_chunk={first_chunk_rows} "
                                f"timeout_reply={timeout_reply_rows} drain={drain_rows} total={total}")

        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy)

    def test_return_strict_cursor_read_no_stale_free_after_followup(self):
        """Cross-read non-tear-down: after a RETURN_STRICT cursor-read timeout,
        the BC's ``free_privdata`` (``CoordRequestCtx_Free``) eventually fires
        for the timed-out read; verify it does NOT destroy the still-paused
        cursor that subsequent reads on the same cid depend on.

        Without the RETURN_STRICT guard in ``CoordRequestCtx_Free``, the
        delayed free for read N would call ``AREQ_CleanUpStoredCursor`` and
        wipe the cursor that's parked for read N+1, leading to "Cursor not
        found" or use-after-free. With the guard, follow-up reads succeed.

        Polled with the ``GET_COORD_REQ_CTX_FREE_COUNT`` debug counter to
        deterministically witness the free callback running before issuing
        the follow-up, without blocking the main-thread dispatcher.
        """
        env = self.env
        skipIfNoEnableAssert(env)

        prev_policy, cursor_id, baseline, before_info, base_warn, _ = \
            _setup_return_strict_cursor_state(env)

        free_count_before = _get_coord_req_ctx_free_count(env)

        # Trigger the simplest timeout scenario (pre-pickup) for read 1.
        env.expect(debug_cmd(), 'COORD_THREADS', 'PAUSE').ok()
        wait_for_condition(
            lambda: (env.cmd(debug_cmd(), 'COORD_THREADS', 'is_paused') == 1, {}),
            'Timeout while waiting for coordinator threads to pause', timeout=30)

        result = []
        try:
            t_query, blocked_client_id = _start_collecting_cursor_read(env, cursor_id, result)
            env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)
            wait_for_client_unblocked(env, blocked_client_id)
        finally:
            env.expect(debug_cmd(), 'COORD_THREADS', 'RESUME').ok()
            wait_for_condition(
                lambda: (env.cmd(debug_cmd(), 'COORD_THREADS', 'is_paused') == 0, {}),
                'Timeout while waiting for coordinator threads to resume', timeout=30)

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Read 1 thread should have finished")
        _assert_return_strict_cursor_timeout_reply(
            env, result[0], cursor_id, expected_results=0,
            message_prefix='RETURN_STRICT no-stale-free read 1 timeout')

        # Wait until CoordRequestCtx_Free has fired for read 1's BC privdata.
        # The counter is bumped by the BC tear-down on the main thread after
        # the worker job completes; polling avoids a deadlock that a sync-point
        # in CoordRequestCtx_Free would cause.
        wait_for_condition(
            lambda: (_get_coord_req_ctx_free_count(env) > free_count_before,
                     {'before': free_count_before,
                      'now': _get_coord_req_ctx_free_count(env)}),
            'Timeout waiting for CoordRequestCtx_Free counter to bump after read 1',
            timeout=10,
        )

        # Read 2 must succeed on the same cursor: rows expected, no error.
        res2, cid2 = env.cmd('FT.CURSOR', 'READ', 'idx', cursor_id)
        env.assertGreater(len(res2.get('results', [])), 0,
                          message=f"Expected non-empty results from read 2, got {res2}")
        # Drain any remaining; cursor must close cleanly.
        if cid2 != 0:
            self._drain_paused_cursor_after_timeout(
                cid2, baseline, 'RETURN_STRICT no-stale-free drain')
        else:
            _wait_for_cursor_cleanup(env, baseline, 'RETURN_STRICT no-stale-free drain')
            env.expect('FT.CURSOR', 'READ', 'idx', str(cursor_id)).error().contains('Cursor not found')

        self._assert_warn_metric_bumped(before_info, base_warn,
                                        'RETURN_STRICT no-stale-free cursor-read sequence')

        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy)

    def test_return_strict_cursor_read_deleted_before_pickup(self):
        """RETURN_STRICT cursor read after the coord-side cursor is purged
        while the BG worker is still queued (no timeout).

        The cursor exists when ``CursorCommand`` validates ``cid`` on the main
        thread and arms the BC; the coord threadpool is paused so the BG
        worker is queued. ``FT.DEBUG DELETE_LOCAL_COORD_CURSORS`` is fanned
        out per-shard to empty ``g_CursorsListCoord`` synchronously on each
        shard's main thread (it does not use ``DIST_THREADPOOL``), purging
        the still-idle coord cursor (odd cid -> ``g_CursorsListCoord``).
        When the coord threadpool resumes, ``coordCursorReadReturnStrict``
        enters under ``LockSetRequest``, observes ``!TimedOut``, then
        ``Cursors_TakeForExecution`` returns ``NULL`` and the worker bails
        through ``CoordRequestCtx_ReplyOrStoreError`` with
        ``"Cursor not found, id: <cid>"``. The reply callback flushes that
        stored error on unblock, so the client sees the standard cursor-gone
        error rather than a cursor-shaped reply.
        """
        env = self.env

        prev_policy, cursor_id, baseline, before_info, base_warn, _ = \
            _setup_return_strict_cursor_state(env)

        env.expect(debug_cmd(), 'COORD_THREADS', 'PAUSE').ok()
        wait_for_condition(
            lambda: (env.cmd(debug_cmd(), 'COORD_THREADS', 'is_paused') == 1, {}),
            'Timeout while waiting for coordinator threads to pause', timeout=30)

        expected_err = f'Cursor not found, id: {cursor_id}'
        try:
            # Use `env.expect(...).error().contains(...)` directly in the
            # thread (rather than `_start_collecting_cursor_read` /
            # `call_and_store`) so the error reply is asserted in-thread:
            # `call_and_store` does not catch exceptions, and the BG worker's
            # "Cursor not found" reply surfaces as a `ResponseError` raised
            # by `env.cmd`, which would leave the result list empty.
            t_query = threading.Thread(
                target=lambda: env.expect(
                    'FT.CURSOR', 'READ', 'idx', str(cursor_id)
                ).error().contains(expected_err),
                daemon=True,
            )
            t_query.start()
            wait_for_blocked_query_client(
                env, 'FT.CURSOR|READ', 'Client for FT.CURSOR|READ not found')
            # Cursor is still idle on the coord (BG hasn't taken it). Purge
            # it via the wholesale debug command rather than `FT.CURSOR DEL
            # idx <cid>`: DEL routes through the same paused DIST_THREADPOOL
            # via `ConcurrentSearch_HandleRedisCommandEx` in `CursorCommand`,
            # so the DEL would itself block forever waiting for the pool.
            # `DELETE_LOCAL_COORD_CURSORS` calls `CursorList_Empty` on
            # `g_CursorsListCoord` synchronously on the Redis main thread,
            # bypassing the pool. Fanned out per-shard since the coord-side
            # cursor lives on whichever shard handled the AGGREGATE.
            run_command_on_all_shards(env, debug_cmd(), 'DELETE_LOCAL_COORD_CURSORS')
        finally:
            env.expect(debug_cmd(), 'COORD_THREADS', 'RESUME').ok()
            wait_for_condition(
                lambda: (env.cmd(debug_cmd(), 'COORD_THREADS', 'is_paused') == 0, {}),
                'Timeout while waiting for coordinator threads to resume', timeout=30)

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Cursor read thread should have finished")

        # No timeout fired: coord-side timeout warning metric must be unchanged.
        _verify_metrics_not_changed(env, env, before_info, [])
        # Cursor is gone: a follow-up read confirms the main-thread validation
        # also reports the now-missing cid.
        env.expect('FT.CURSOR', 'READ', 'idx', str(cursor_id)).error().contains('Cursor not found')
        _wait_for_cursor_cleanup(
            env, baseline, 'RETURN_STRICT cursor-deleted-before-pickup')

        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy)

    def test_return_strict_cursor_read_deleted_then_timeout_before_pickup(self):
        """RETURN_STRICT cursor read where the cursor is purged *and* the BC
        timeout fires before the BG worker dequeues. The timer does NOT
        detect that the cursor is gone — by design.

        Sequence:
          1. Coord threadpool is paused; BG worker is queued.
          2. ``FT.DEBUG DELETE_LOCAL_COORD_CURSORS`` is fanned out per-shard
             and synchronously empties ``g_CursorsListCoord``, freeing the
             still-idle coord cursor without touching ``DIST_THREADPOOL``.
          3. ``CLIENT UNBLOCK ... TIMEOUT`` fires the BC timeout callback.
             The timer holds ``LockSetRequest``, observes ``req == NULL``
             (BG never called ``SetRequest``) and replies via
             ``coord_cursor_read_empty_reply_timeout`` with the *original*
             cid — which now points at a freed cursor. The cid is taken
             verbatim from ``argv[3]`` and is not re-validated against the
             cursor table; this is the documented contract.
          4. Coord threadpool is resumed. The BG worker runs, takes
             ``LockSetRequest``, sees ``TimedOut`` and returns without
             touching the (already-gone) cursor or the reply.

        The client therefore sees a normal RETURN_STRICT timeout reply
        ``({empty results, TIMEOUT warning}, original_cid)``, and a
        follow-up ``FT.CURSOR READ`` on that cid surfaces the staleness
        as ``"Cursor not found"`` from the main-thread validation in
        ``CursorCommand``.
        """
        env = self.env
        skipIfNoEnableAssert(env)

        prev_policy, cursor_id, baseline, before_info, base_warn, _ = \
            _setup_return_strict_cursor_state(env)

        env.expect(debug_cmd(), 'COORD_THREADS', 'PAUSE').ok()
        wait_for_condition(
            lambda: (env.cmd(debug_cmd(), 'COORD_THREADS', 'is_paused') == 1, {}),
            'Timeout while waiting for coordinator threads to pause', timeout=30)

        result = []
        try:
            t_query, blocked_client_id = _start_collecting_cursor_read(env, cursor_id, result)
            # Purge the cursor while it is still idle on the coord and the BG
            # worker is queued. Wholesale `DELETE_LOCAL_COORD_CURSORS` rather
            # than `FT.CURSOR DEL idx <cid>` because DEL also routes through
            # the paused DIST_THREADPOOL (`ConcurrentSearch_HandleRedisCommandEx`
            # in `CursorCommand`) and would block forever; the debug command
            # runs synchronously on the main thread.
            run_command_on_all_shards(env, debug_cmd(), 'DELETE_LOCAL_COORD_CURSORS')
            # Fire the BC timeout *before* resuming coord threads so the
            # timer (main thread) wins the race against the BG worker.
            env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)
            wait_for_client_unblocked(env, blocked_client_id)
        finally:
            env.expect(debug_cmd(), 'COORD_THREADS', 'RESUME').ok()
            wait_for_condition(
                lambda: (env.cmd(debug_cmd(), 'COORD_THREADS', 'is_paused') == 0, {}),
                'Timeout while waiting for coordinator threads to resume', timeout=30)

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Cursor read thread should have finished")
        # Timer replied with the original cid even though the cursor was
        # already freed by the concurrent DEL — this is the documented
        # behavior of ``DistCursorReadTimeoutReturnStrictClient``.
        _assert_return_strict_cursor_timeout_reply(
            env, result[0], cursor_id, expected_results=0,
            message_prefix='RETURN_STRICT cursor-deleted+timeout pre-pickup')
        self._assert_warn_metric_bumped(
            before_info, base_warn,
            'RETURN_STRICT cursor-deleted+timeout pre-pickup')

        # Follow-up read on the (stale) cid: main-thread validation in
        # CursorCommand surfaces the staleness as a plain "Cursor not found".
        env.expect('FT.CURSOR', 'READ', 'idx', str(cursor_id)).error().contains('Cursor not found')
        _wait_for_cursor_cleanup(
            env, baseline, 'RETURN_STRICT cursor-deleted+timeout pre-pickup')

        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy)

    def test_return_strict_cursor_read_drain_happy_path(self):
        """Sanity: under RETURN_STRICT with no induced timeout, an FT.AGGREGATE
        WITHCURSOR can be drained to completion exactly like under any other
        policy and recovers all ``n_docs`` rows."""
        env = self.env

        prev_policy, cursor_id, baseline, _, _, first_res = \
            _setup_return_strict_cursor_state(env)

        first_chunk_rows = len(first_res.get('results', []))
        drained_rows = _drain_cursor(env, cursor_id)
        env.assertEqual(first_chunk_rows + drained_rows, self.n_docs,
                        message=f"Expected {self.n_docs} rows in happy-path drain, "
                                f"got first_chunk={first_chunk_rows} drained={drained_rows}")
        _wait_for_cursor_cleanup(env, baseline, 'RETURN_STRICT happy-path drain')
        env.expect('FT.CURSOR', 'READ', 'idx', str(cursor_id)).error().contains('Cursor not found')

        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy)

    # ------------------------------------------------------------------
    # Shard-level RETURN_STRICT timeout: a single shard's blocked-client
    # timer (CLIENT UNBLOCK ... TIMEOUT) fires while the user query
    # carries TIMEOUT 0, so the coord deadline never arms.
    # ------------------------------------------------------------------
    def _cleanup_strict_pause_state(self, all_shard_conns, prev_on_timeout_policy):
        """Clear AggregateResults pause state on every shard and restore the
        global on-timeout policy."""
        for c in all_shard_conns:
            resetAggregateResultsDebug(c)
        try:
            run_command_on_all_shards(self.env, 'CONFIG', 'SET',
                                      ON_TIMEOUT_CONFIG, prev_on_timeout_policy)
        except Exception:
            pass

    def _assert_unsorted_partial_reply(self, env, result, expected_rows,
                                       pause_after_n, other_docs):
        # Temporary until MOD-15971 defines deterministic internal cursor
        # draining after RETURN_STRICT shard timeouts.
        env.assertGreaterEqual(len(result.get('results', [])), expected_rows,
                               message="rows in reply")

    def _run_one_shard_timesout(self, *, coord_cmd, shard_cmd, query_args,
                                assert_reply, coord_cmd_prefix=None,
                                expected_warning=None):
        """Drive a one-shard-timesout query under RETURN_STRICT.

        Parks one non-coord shard at ``AggregateResults`` result
        #pause_after_n, then unblocks it via ``CLIENT UNBLOCK ... TIMEOUT``.
        The query carries ``TIMEOUT 0`` so only the manual unblock can fire.

        ``coord_cmd`` / ``shard_cmd`` are the user-facing and internal
        command names (e.g. ``FT.AGGREGATE`` / ``_FT.AGGREGATE``).
        ``coord_cmd_prefix`` overrides the default ``[coord_cmd, 'idx', '*']``
        for shapes like ``FT.PROFILE idx AGGREGATE QUERY *``.
        ``expected_warning`` defaults to ``[TIMEOUT_WARNING]``; pass ``[]``
        when the warning is nested inside a wrapper (e.g. ``FT.PROFILE``).

        ``assert_reply(env, result, expected_rows, pause_after_n, other_docs)``
        validates the coord reply; this helper only asserts the warning and
        the coord warning metric.
        """
        env = self.env

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        run_command_on_all_shards(env, 'CONFIG', 'SET',
                                  ON_TIMEOUT_CONFIG, 'return-strict')

        before_info = info_modules_to_dict(env)
        base_warn_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])

        all_shard_conns, target_conn, _target_pid, other_conns = \
            _split_shards_pick_one_paused(env)

        # Target contributes pause_after_n; other shards contribute their
        # full per-shard share.
        pause_after_n = 2
        other_docs = sum(len(c.execute_command('KEYS', 'doc*')) for c in other_conns)
        expected_rows = pause_after_n + other_docs

        try:
            setPauseAfterAggregateResult(target_conn, pause_after_n)

            prefix = (list(coord_cmd_prefix)
                      if coord_cmd_prefix is not None
                      else [coord_cmd, 'idx', '*'])
            full_cmd = prefix + list(query_args) + ['TIMEOUT', '0']
            query_result = []
            t_query = threading.Thread(
                target=call_and_store,
                args=(env.cmd, full_cmd, query_result),
                daemon=True
            )
            t_query.start()

            blocked_client_id = _wait_shard_paused_after_aggregate_result(
                target_conn, shard_cmd)

            target_conn.execute_command('CLIENT', 'UNBLOCK',
                                        blocked_client_id, 'TIMEOUT')

            # Wait for the timeout callback to actually run (it sets
            # AREQ_TimedOut, which the pause loop polls) before disarming
            # the pause counter, else BG could resume on rc=EOF.
            wait_for_client_unblocked(target_conn, blocked_client_id)

            resetAggregateResultsDebug(target_conn)

            t_query.join(timeout=10)
            env.assertFalse(t_query.is_alive(), message="query thread alive")

            env.assertEqual(len(query_result), 1, message="query result count")
            result = query_result[0]

            assert_reply(env, result, expected_rows, pause_after_n, other_docs)

            warn_expected = ([TIMEOUT_WARNING]
                             if expected_warning is None
                             else expected_warning)
            env.assertEqual(result.get('warning', []), warn_expected,
                            message="reply warning")

            after_info = info_modules_to_dict(env)
            env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC],
                            str(base_warn_coord + 1),
                            message="coord timeout-warning metric +1")
            _verify_metrics_not_changed(env, env, before_info,
                                        [TIMEOUT_WARNING_COORD_METRIC])
        finally:
            self._cleanup_strict_pause_state(all_shard_conns, prev_on_timeout_policy)

    def test_return_strict_one_shard_timesout_flat_aggregate(self):
        """Flat aggregate, one shard times out mid-pipeline.

        Temporarily expect at least ``pause_after_n + other_docs`` rows: the
        timed-out shard ships its buffered prefix, and the current internal
        cursor loop may drain additional rows before completion (MOD-15971).
        """
        skipIfNoEnableAssert(self.env)

        def assert_flat_reply(env, result, expected_rows, pause_after_n, other_docs):
            # Temporary until MOD-15971 defines deterministic internal cursor
            # draining after RETURN_STRICT shard timeouts.
            env.assertGreaterEqual(len(result.get('results', [])), expected_rows,
                                   message="rows in reply")

        self._run_one_shard_timesout(
            coord_cmd='FT.AGGREGATE', shard_cmd='_FT.AGGREGATE',
            query_args=['LOAD', '1', '@name',
                        'LIMIT', '0', str(self.n_docs)],
            assert_reply=assert_flat_reply)

    def test_return_strict_one_shard_timesout_sortby_aggregate(self):
        """SORTBY one-shard timeout.

        Expect ``self.n_docs`` rows globally sorted: the shard-side
        RPSorter is a buffering stage, so its heap already holds the full
        per-shard set when the pause fires and the strict drain pops all
        of it.
        """
        skipIfNoEnableAssert(self.env)

        def assert_sorted_reply(env, result, expected_rows, pause_after_n, other_docs):
            full_rows = self.n_docs
            env.assertEqual(len(result.get('results', [])), full_rows,
                            message="rows in reply")
            values = [row['extra_attributes']['name'] for row in result['results']]
            env.assertEqual(values, sorted(values),
                            message="global sort order")

        self._run_one_shard_timesout(
            coord_cmd='FT.AGGREGATE', shard_cmd='_FT.AGGREGATE',
            query_args=['LOAD', '1', '@name',
                        'SORTBY', '1', '@name',
                        'LIMIT', '0', str(self.n_docs)],
            assert_reply=assert_sorted_reply)

    def test_return_strict_one_shard_timesout_groupby_aggregate(self):
        """GROUPBY one-shard timeout.

        Temporarily expect at least ``pause_after_n + other_docs`` groups: doc
        names are unique, so re-grouping at the coord neither merges nor drops
        any group key, while the current internal cursor loop may drain more
        rows (MOD-15971).
        """
        skipIfNoEnableAssert(self.env)

        def assert_groupby_reply(env, result, expected_rows, pause_after_n, other_docs):
            # Temporary until MOD-15971 defines deterministic internal cursor
            # draining after RETURN_STRICT shard timeouts.
            env.assertGreaterEqual(len(result.get('results', [])), expected_rows,
                                   message="rows in reply")

        self._run_one_shard_timesout(
            coord_cmd='FT.AGGREGATE', shard_cmd='_FT.AGGREGATE',
            query_args=['LOAD', '1', '@name',
                        'GROUPBY', '1', '@name',
                        'LIMIT', '0', str(self.n_docs)],
            assert_reply=assert_groupby_reply)

    def test_return_strict_one_shard_timesout_search(self):
        """FT.SEARCH (with content) one-shard timeout.

        Expect ``pause_after_n + other_docs`` rows. The shard pipeline
        ends in RPLoader (after RPPager), which is rejected by
        ``pipelineCanYieldPartialResults``, so only the rows already
        buffered by the time the timeout fires are shipped.
        """
        skipIfNoEnableAssert(self.env)

        def assert_search_partial_reply(env, result, expected_rows,
                                        pause_after_n, other_docs):
            env.assertEqual(len(result.get('results', [])), expected_rows,
                            message="rows in reply")

        self._run_one_shard_timesout(
            coord_cmd='FT.SEARCH', shard_cmd='_FT.SEARCH',
            query_args=['LIMIT', '0', str(self.n_docs)],
            assert_reply=assert_search_partial_reply)

    def test_return_strict_one_shard_timesout_search_nocontent(self):
        """FT.SEARCH NOCONTENT one-shard timeout.

        Expect ``self.n_docs`` rows. NOCONTENT skips RPLoader, so the
        shard pipeline ends in ``RPPager -> RPSorter`` -- the shape
        ``pipelineCanYieldPartialResults`` accepts -- and the strict
        drain pops the full per-shard heap.
        """
        skipIfNoEnableAssert(self.env)

        def assert_search_full_reply(env, result, expected_rows,
                                     pause_after_n, other_docs):
            env.assertEqual(result['total_results'], self.n_docs,
                            message="merged total_results")
            env.assertEqual(len(result.get('results', [])), self.n_docs,
                            message="rows in reply")

        self._run_one_shard_timesout(
            coord_cmd='FT.SEARCH', shard_cmd='_FT.SEARCH',
            query_args=['NOCONTENT', 'LIMIT', '0', str(self.n_docs)],
            assert_reply=assert_search_full_reply)

    def test_return_strict_one_shard_timesout_profile_aggregate(self):
        """FT.PROFILE AGGREGATE wrapper around the flat one-shard case.

        Same inner ``Results`` shape as the flat aggregate test, plus
        the ``Profile`` envelope must survive the timeout fast path.
        Profiling moves the warning under ``result['Results']``, so the
        outer dict has no top-level ``warning`` key.
        """
        skipIfNoEnableAssert(self.env)

        def assert_profile_partial_reply(env, result, expected_rows,
                                         pause_after_n, other_docs):
            env.assertContains('Results', result, message="Results key")
            env.assertContains('Profile', result, message="Profile key")
            inner = result['Results']
            self._assert_unsorted_partial_reply(env, inner, expected_rows,
                                                pause_after_n, other_docs)
            env.assertEqual(inner.get('warning', []), [TIMEOUT_WARNING],
                            message="inner Results warning")

        self._run_one_shard_timesout(
            coord_cmd='FT.PROFILE', shard_cmd='_FT.PROFILE',
            coord_cmd_prefix=['FT.PROFILE', 'idx', 'AGGREGATE',
                              'QUERY', '*'],
            query_args=['LOAD', '1', '@name',
                        'LIMIT', '0', str(self.n_docs)],
            assert_reply=assert_profile_partial_reply,
            expected_warning=[])

    # ------------------------------------------------------------------
    # Every shard times out: same RETURN_STRICT plumbing, but every
    # shard is parked and individually unblocked. With no coord deadline
    # armed, RPNet drains every shard's buffered prefix.
    # ------------------------------------------------------------------
    def _run_all_shards_timesout(self, *, coord_cmd, shard_cmd, query_args,
                                 assert_reply, pause_after_n=2,
                                 pause_per_shard=None):
        """Drive an all-shards-timeout query under RETURN_STRICT.

        Pauses every shard at ``AggregateResults`` result
        #pause_after_n (or per-shard via ``pause_per_shard``), then
        unblocks each parked client. ``assert_reply(env, result, pauses,
        shards_count)`` validates the coord reply; this helper only
        asserts the warning, the coord metric, and the per-shard metrics.
        """
        env = self.env

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        run_command_on_all_shards(env, 'CONFIG', 'SET',
                                  ON_TIMEOUT_CONFIG, 'return-strict')

        shard_conns = [env.getConnection(i)
                       for i in range(1, env.shardsCount + 1)]

        if pause_per_shard is None:
            pauses = [pause_after_n] * len(shard_conns)
        else:
            env.assertEqual(len(pause_per_shard), len(shard_conns),
                            message="pause_per_shard length must match shardsCount")
            pauses = list(pause_per_shard)

        before_coord_info = info_modules_to_dict(env)
        base_warn_coord = int(
            before_coord_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])
        before_shard_warns = [
            int(info_modules_to_dict(c)[WARN_ERR_SECTION][TIMEOUT_WARNING_SHARD_METRIC])
            for c in shard_conns
        ]

        try:
            for c, p in zip(shard_conns, pauses):
                setPauseAfterAggregateResult(c, p)

            full_cmd = ([coord_cmd, 'idx', '*']
                        + list(query_args)
                        + ['TIMEOUT', '0'])
            query_result = []
            t_query = threading.Thread(
                target=call_and_store,
                args=(env.cmd, full_cmd, query_result),
                daemon=True
            )
            t_query.start()

            blocked_ids = [
                (c, _wait_shard_paused_after_aggregate_result(c, shard_cmd))
                for c in shard_conns
            ]

            for c, cid in blocked_ids:
                c.execute_command('CLIENT', 'UNBLOCK', cid, 'TIMEOUT')

            # Wait for each shard's pause loop to self-release on
            # AREQ_TimedOut before disarming the counter, else BG could
            # resume on rc=EOF and bypass the TIMEDOUT path.
            for c, cid in blocked_ids:
                wait_for_client_unblocked(c, cid)
                resetAggregateResultsDebug(c)

            t_query.join(timeout=10)
            env.assertFalse(t_query.is_alive(), message="query thread alive")
            env.assertEqual(len(query_result), 1, message="query result count")
            result = query_result[0]

            assert_reply(env, result, pauses, env.shardsCount)

            env.assertEqual(result.get('warning', []), [TIMEOUT_WARNING],
                            message="reply warning")

            after_coord_info = info_modules_to_dict(env)
            env.assertEqual(after_coord_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC],
                            str(base_warn_coord + 1),
                            message="coord timeout-warning metric +1")
            # The coord's local shard also replied with TIMEDOUT, so its
            # shard metric increments on the same connection; verify
            # per-shard counts explicitly below instead.
            _verify_metrics_not_changed(env, env, before_coord_info,
                                        [TIMEOUT_WARNING_COORD_METRIC,
                                         TIMEOUT_WARNING_SHARD_METRIC])

            for i, c in enumerate(shard_conns):
                after = info_modules_to_dict(c)
                env.assertEqual(after[WARN_ERR_SECTION][TIMEOUT_WARNING_SHARD_METRIC],
                                str(before_shard_warns[i] + 1),
                                message=f"shard {i + 1} timeout-warning metric +1")
        finally:
            self._cleanup_strict_pause_state(shard_conns, prev_on_timeout_policy)

    def test_return_strict_all_shards_timesout_flat_aggregate(self):
        """Flat aggregate, every shard times out.

        Temporarily expect at least ``sum(pauses)`` rows: every shard's
        admitted rows survive, and the current internal cursor loop may drain
        more rows (MOD-15971).
        """
        skipIfNoEnableAssert(self.env)

        def assert_flat_reply(env, result, pauses, shards_count):
            expected = sum(pauses)
            # Temporary until MOD-15971 defines deterministic internal cursor
            # draining after RETURN_STRICT shard timeouts.
            env.assertGreaterEqual(len(result.get('results', [])), expected,
                                   message="rows in reply")

        self._run_all_shards_timesout(
            coord_cmd='FT.AGGREGATE', shard_cmd='_FT.AGGREGATE',
            query_args=['LOAD', '1', '@name',
                        'LIMIT', '0', str(self.n_docs)],
            assert_reply=assert_flat_reply)

    def test_return_strict_all_shards_timesout_withcount_aggregate(self):
        """WITHCOUNT all-shards-timeout (barrier + RPDepleter).

        Temporarily expect at least ``sum(pauses)`` rows: every shard's
        admitted rows survive, and the current internal cursor loop may drain
        more rows (MOD-15971).
        """
        skipIfNoEnableAssert(self.env)

        def assert_withcount_reply(env, result, pauses, shards_count):
            expected = sum(pauses)
            # Temporary until MOD-15971 defines deterministic internal cursor
            # draining after RETURN_STRICT shard timeouts.
            env.assertGreaterEqual(len(result.get('results', [])), expected,
                                   message="rows in reply")

        # WITHCOUNT must precede pipeline steps (LOAD/GROUPBY/...).
        self._run_all_shards_timesout(
            coord_cmd='FT.AGGREGATE', shard_cmd='_FT.AGGREGATE',
            query_args=['WITHCOUNT', 'LOAD', '1', '@name'],
            assert_reply=assert_withcount_reply)

    def test_return_strict_all_shards_timesout_sortby_aggregate(self):
        """SORTBY all-shards-timeout.

        Expect ``self.n_docs`` rows globally sorted: every shard's
        RPSorter heap holds its full per-shard set, and the coord's
        RPSorter merges them all into one globally-sorted stream.
        """
        skipIfNoEnableAssert(self.env)

        def assert_sortby_all_timesout_reply(env, result, pauses, shards_count):
            n_rows = len(result.get('results', []))
            env.assertEqual(n_rows, self.n_docs, message="merged row count")
            values = [row['extra_attributes']['name']
                      for row in result['results']]
            env.assertEqual(values, sorted(values),
                            message="global sort order")

        self._run_all_shards_timesout(
            coord_cmd='FT.AGGREGATE', shard_cmd='_FT.AGGREGATE',
            query_args=['LOAD', '1', '@name',
                        'SORTBY', '1', '@name',
                        'LIMIT', '0', str(self.n_docs)],
            assert_reply=assert_sortby_all_timesout_reply)

    def test_return_strict_all_shards_timesout_partial_each_aggregate(self):
        """All-shards-timeout with distinct per-shard pause counts.

        Temporarily expect at least ``sum(pauses)`` rows. Distinct pause values
        reject regressions where admitted rows are lost, while the current
        internal cursor loop may drain additional rows (MOD-15971).
        """
        skipIfNoEnableAssert(self.env)

        # One distinct pause count per shard; pad/truncate to shardsCount.
        base_pauses = [1, 3, 5]
        n_shards = self.env.shardsCount
        if n_shards <= len(base_pauses):
            pauses = base_pauses[:n_shards]
        else:
            pauses = base_pauses + [2] * (n_shards - len(base_pauses))

        def assert_partial_each_reply(env, result, pauses, shards_count):
            expected = sum(pauses)
            # Temporary until MOD-15971 defines deterministic internal cursor
            # draining after RETURN_STRICT shard timeouts.
            env.assertGreaterEqual(len(result.get('results', [])), expected,
                                   message="rows in reply")

        self._run_all_shards_timesout(
            coord_cmd='FT.AGGREGATE', shard_cmd='_FT.AGGREGATE',
            query_args=['LOAD', '1', '@name',
                        'LIMIT', '0', str(self.n_docs)],
            assert_reply=assert_partial_each_reply,
            pause_per_shard=pauses)


class TestCoordinatorTimeoutReturnStrictResp2:
    """RESP2 counterpart of the one-shard-timesout flat aggregate test.

    RESP2 has no warnings slot in the shard reply, so the per-shard
    TIMEDOUT warning is not propagated to the coord and the coord
    warning metric stays unchanged. The merged reply is still well
    formed: a flat list whose head is ``total_results`` and whose tail
    is the merged shard rows.

    Temporarily expect at least ``pause_after_n + other_docs`` trailing rows:
    only admitted rows are required while internal cursor draining remains
    non-deterministic (MOD-15971).
    """

    def __init__(self):
        skipTest(cluster=False)
        self.env = Env(moduleArgs='WORKERS 1', protocol=2)
        self.n_docs = 100

        for i in range(1, self.env.shardsCount + 1):
            verify_shard_init(self.env.getConnection(i))

        conn = getConnectionByEnv(self.env)
        self.env.expect('FT.CREATE', 'idx', 'PREFIX', '1', 'doc',
                        'SCHEMA', 'name', 'TEXT').ok()
        for i in range(self.n_docs):
            conn.execute_command('HSET', f'doc{i}', 'name', f'hello{i}')

    def test_return_strict_one_shard_timesout_flat_aggregate_resp2(self):
        """RESP2 variant of test_return_strict_one_shard_timesout_flat_aggregate."""
        env = self.env
        skipIfNoEnableAssert(env)

        # CONFIG GET in RESP2 returns a flat [key, value] list, not a map.
        prev_on_timeout_policy = env.cmd('CONFIG', 'GET',
                                         ON_TIMEOUT_CONFIG)[1]
        run_command_on_all_shards(env, 'CONFIG', 'SET',
                                  ON_TIMEOUT_CONFIG, 'return-strict')

        before_info = info_modules_to_dict(env)
        base_warn_coord = int(
            before_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])

        all_shard_conns, target_conn, _target_pid, other_conns = \
            _split_shards_pick_one_paused(env)

        pause_after_n = 2
        other_docs = sum(len(c.execute_command('KEYS', 'doc*'))
                         for c in other_conns)
        expected_rows = pause_after_n + other_docs

        try:
            setPauseAfterAggregateResult(target_conn, pause_after_n)

            full_cmd = ['FT.AGGREGATE', 'idx', '*',
                        'LOAD', '1', '@name',
                        'LIMIT', '0', str(self.n_docs),
                        'TIMEOUT', '0']
            query_result = []
            t_query = threading.Thread(
                target=call_and_store,
                args=(env.cmd, full_cmd, query_result),
                daemon=True
            )
            t_query.start()

            blocked_client_id = _wait_shard_paused_after_aggregate_result(
                target_conn, '_FT.AGGREGATE')
            target_conn.execute_command('CLIENT', 'UNBLOCK',
                                        blocked_client_id, 'TIMEOUT')
            wait_for_client_unblocked(target_conn, blocked_client_id)
            resetAggregateResultsDebug(target_conn)

            t_query.join(timeout=10)
            env.assertFalse(t_query.is_alive(), message="query thread alive")
            env.assertEqual(len(query_result), 1, message="query result count")
            result = query_result[0]

            # RESP2 FT.AGGREGATE: [total_results, doc1_fields, ...].
            env.assertTrue(isinstance(result, list),
                           message=f"RESP2 reply type ({type(result).__name__})")
            row_count = len(result) - 1
            # Temporary until MOD-15971 defines deterministic internal cursor
            # draining after RETURN_STRICT shard timeouts.
            env.assertGreaterEqual(row_count, expected_rows,
                                   message="trailing row count")

            # RESP2 has no warnings slot, so the coord warning metric
            # must NOT increment despite the shard timing out.
            after_info = info_modules_to_dict(env)
            env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC],
                            str(base_warn_coord),
                            message="coord warning metric unchanged (RESP2)")
            _verify_metrics_not_changed(env, env, before_info,
                                        [TIMEOUT_WARNING_COORD_METRIC])
        finally:
            for c in all_shard_conns:
                resetAggregateResultsDebug(c)
            try:
                run_command_on_all_shards(env, 'CONFIG', 'SET',
                                          ON_TIMEOUT_CONFIG,
                                          prev_on_timeout_policy)
            except Exception:
                pass

class TestCoordinatorTimeoutCursorReadResp2:
    """RESP2 coverage for RETURN_STRICT FT.CURSOR READ timeout reply shape.

    Mirrors ``test_return_strict_timeout_in_pipeline_cursor_read_observed`` on
    a RESP2 connection: the cursor-read timeout reply is an outer two-element
    array ``[results_array, cid]`` where ``results_array[0]`` is the
    total_results long, followed by the drained per-doc payloads. RESP2 has
    no per-reply warnings array — timeout is tracked only in the global
    warning metric.
    """

    def __init__(self):
        skipTest(cluster=False)
        self.env = Env(moduleArgs='WORKERS 1', protocol=2)
        self.n_docs = 100
        for i in range(1, self.env.shardsCount + 1):
            verify_shard_init(self.env.getConnection(i))
        conn = getConnectionByEnv(self.env)
        self.env.expect('FT.CREATE', 'idx', 'PREFIX', '1', 'doc',
                        'SCHEMA', 'name', 'TEXT').ok()
        for i in range(self.n_docs):
            conn.execute_command('HSET', f'doc{i}', 'name', f'hello{i}')
        self.env.expect('FT.SEARCH', 'idx', '*').noError()

    def test_return_strict_cursor_read_resp2_timeout(self):
        """RESP2 mirror of the *observed* in-pipeline cursor-read timeout
        via the ``rpnetNext`` short-circuit. BG parks at ``BeforeRPNetNext``;
        timer fires; on resume the short-circuit returns ``RS_RESULT_TIMEDOUT``
        before any rows are emitted, so the RESP2 reply carries
        ``total_results == 0`` with a preserved cursor id.
        """
        env = self.env
        skipIfNoEnableAssert(env)

        prev_policy, cursor_id, baseline, _, base_warn, _ = \
            _setup_return_strict_cursor_state(env)

        sync_point = 'BeforeRPNetNext'
        env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
        env.cmd(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point)

        replies = []
        try:
            t_query = threading.Thread(
                target=call_and_store,
                args=(env.cmd, ['FT.CURSOR', 'READ', 'idx', str(cursor_id)], replies),
                daemon=True
            )
            t_query.start()
            blocked_client_id = wait_for_blocked_query_client(
                env, 'FT.CURSOR|READ', 'Client for FT.CURSOR|READ not found')
            wait_for_condition(
                lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sync_point) == 1, {}),
                f'worker never reached {sync_point}'
            )
            env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)
            wait_for_client_unblocked(env, blocked_client_id)
        finally:
            env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point)

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Cursor read thread should have finished")
        env.assertEqual(len(replies), 1, message="Expected one reply collected")
        reply = replies[0]

        # RESP2 cursor reply: [results_array, cid];
        env.assertEqual(len(reply), 2,
                        message=f"reply={reply!r}")
        env.debugPrint(f"{replies[0]}", force=True)
        results_array, cid = reply[0], reply[1]
        env.assertEqual(cid, cursor_id,
                        message=f"reply={reply!r}")
        # BG parked at BeforeRPNetNext returns TIMEDOUT before emitting rows
        # itself, but ``drainPartialResultsAfterTimeout`` then pops up to
        # ``chunk_size`` rows out of the channel/sorter buffer into the
        # timed-out reply. RESP2 envelope is [total_results, *doc_payloads]
        # so the total length is 1 + chunk_size.
        chunk_size = 10  # matches _setup_return_strict_cursor_state default
        env.assertEqual(len(results_array), 1 + chunk_size,
                        message=f"reply={reply!r}")

        # Coord-side warning metric still bumps under RESP2.
        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC],
                        str(base_warn + 1),
                        message="Coordinator timeout warning metric should be +1 (RESP2)")

        # Cursor must remain reusable on RESP2 too.
        _drain_cursor(env, cursor_id)
        _wait_for_cursor_cleanup(env, baseline, 'RETURN_STRICT RESP2 cursor-read drain')
        env.expect('FT.CURSOR', 'READ', 'idx', str(cursor_id)).error().contains('Cursor not found')
        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy)


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

    def test_shard_timeout_qi_not_iterator(self):
        """MOD-15397: the Rust NOT iterator's per-iterator timeout callback
        receives the blocked-client timeout signal under FAIL + WORKERS.

        Arms ``BeforeQITimeoutCheck``, runs ``FT.SEARCH idx -hello1`` so the
        Rust ``NotIterator``'s ``check_timeout`` parks at the sync point, then
        fires ``CLIENT UNBLOCK ... TIMEOUT`` to flip the AREQ timed-out flag.
        The sync-point predicate (``AREQ_TimedOut``) releases the wait, the
        iterator returns ``Timeout``, and the query reports the timeout error.
        """
        env = self.env
        skipIfNoEnableAssert(env)
        sync_point = 'BeforeQITimeoutCheck'

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()

        before_info = info_modules_to_dict(env)
        base_err_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])

        env.expect(debug_cmd(), 'SYNC_POINT', 'CLEAR').ok()
        env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point).ok()

        try:
            query_args = ['FT.SEARCH', 'idx', '-hello1']
            t_query = threading.Thread(
                target=run_cmd_expect_timeout,
                args=(env, query_args),
                daemon=True
            )
            t_query.start()

            blocked_client_id = wait_for_blocked_query_client(env, 'FT.SEARCH')

            # Worker must reach the QI timeout check (proves the callback was
            # installed and the iterator's check_timeout was called).
            wait_for_condition(
                lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sync_point) == 1, {}),
                f'Worker never reached {sync_point}'
            )

            # Fire blocked-client timeout: main-thread callback sets
            # AREQ.timedOut; the sync-point predicate releases the wait and
            # the iterator's check_timeout returns Timeout.
            env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)
            wait_for_client_unblocked(env, blocked_client_id)
        finally:
            # Disarm the sync point. The predicate may have already released
            # the worker, but the point itself stays armed until signalled
            # or cleared, which would block subsequent tests.
            env.expect(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point).ok()

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

        # Standalone uses coord metrics for shard-side timeouts.
        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC],
                        str(base_err_coord + 1),
                        message="Coord timeout error should be +1 after QI sync-point timeout")

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
        setPauseBeforeStoreResults(env, True, internal=False)

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
        setPauseAfterStoreResults(env, True, internal=False)

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

    def test_aggregate_cursor_reply_count_standalone(self):
        """Standalone FT.AGGREGATE WITHCURSOR drains exactly all aggregate rows."""
        env = self.env
        chunk_size = 7

        prev_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict').ok()
        try:
            first_res, cursor_id = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@name',
                                           'WITHCURSOR', 'COUNT', str(chunk_size))
            env.assertNotEqual(cursor_id, 0, message="Expected non-zero cursor ID")
            env.assertEqual(first_res.get('warning', []), [],
                            message=f"Happy aggregate cursor reply should not warn: {first_res}")

            _assert_aggregate_cursor_total_rows(
                env, first_res, cursor_id, self.n_docs,
                'standalone happy FT.AGGREGATE WITHCURSOR')
        finally:
            env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy).ok()

    def test_return_strict_timeout_aggregate_cursor_reply_count_standalone(self):
        """Standalone timed-out FT.AGGREGATE WITHCURSOR can drain the remaining rows."""
        env = self.env
        skipIfNoEnableAssert(env)
        chunk_size = 10

        prev_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict').ok()
        resetAggregateResultsDebug(env)
        setPauseAfterAggregateResult(env, 1)

        query_result = []
        try:
            t_query = threading.Thread(
                target=call_and_store,
                args=(env.cmd, ['FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@name',
                                'WITHCURSOR', 'COUNT', str(chunk_size)], query_result),
                daemon=True,
            )
            t_query.start()

            blocked_client_id = _wait_shard_paused_after_aggregate_result(env, 'FT.AGGREGATE')
            env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)
            wait_for_client_unblocked(env, blocked_client_id)

            resetAggregateResultsDebug(env)

            t_query.join(timeout=10)
            env.assertFalse(t_query.is_alive(), message="Query thread should have finished")
            env.assertEqual(len(query_result), 1, message="Expected one aggregate result")

            first_res, cursor_id = query_result[0]
            env.assertNotEqual(cursor_id, 0, message="Expected non-zero cursor ID after partial timeout")
            VerifyTimeoutWarningResp3(env, first_res,
                                      message="initial standalone aggregate cursor reply must warn")

            _assert_aggregate_cursor_total_rows(
                env, first_res, cursor_id, self.n_docs,
                'standalone timed-out FT.AGGREGATE WITHCURSOR')
        finally:
            resetAggregateResultsDebug(env)
            env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy).ok()

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
        # and freed the cursor on the early-exit branch in cursorRead_ctx
        # (src/aggregate/aggregate_exec.c).
        _wait_for_cursor_cleanup(env, baseline,
                                 'FAIL queued cursor-read timeout (standalone)')
        env.expect('FT.CURSOR', 'READ', 'idx', str(cursor_id)).error().contains('Cursor not found')
        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC],
                        str(base_err_coord + 1),
                        message="Coordinator timeout error should be +1 after queued cursor-read timeout")
        _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_ERROR_COORD_METRIC])

        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy).ok()

    def test_return_strict_timeout_shard_cursor_read_does_not_hang(self):
        """Standalone RETURN_STRICT cursor-read timeout does not hang."""
        env = self.env
        skipIfNoEnableAssert(env)
        sync_point = 'BeforeCursorReadSendChunk'

        prev_policy, cursor_id, baseline, before_info, _, _ = \
            _setup_return_strict_cursor_state(env)
        base_err_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])
        base_warn_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])
        base_warn_shard = int(before_info[WARN_ERR_SECTION][TIMEOUT_WARNING_SHARD_METRIC])

        try:
            env.expect(debug_cmd(), 'SYNC_POINT', 'CLEAR').ok()
            env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point).ok()

            result = []
            try:
                t_query = threading.Thread(
                    target=call_and_store,
                    args=(env.cmd, ['FT.CURSOR', 'READ', 'idx', str(cursor_id)], result),
                    daemon=True,
                )
                t_query.start()
                blocked_client_id = wait_for_blocked_query_client(
                    env, 'FT.CURSOR|READ', 'Client for FT.CURSOR|READ not found')
                wait_for_condition(
                    lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sync_point) == 1, {}),
                    f'worker never reached {sync_point}'
                )
                env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)
                wait_for_client_unblocked(env, blocked_client_id)
            finally:
                env.expect(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point).ok()
                env.expect(debug_cmd(), 'SYNC_POINT', 'CLEAR').ok()

            t_query.join(timeout=10)
            env.assertFalse(t_query.is_alive(), message="Cursor read thread should have finished")
            env.assertEqual(len(result), 1, message="Expected one cursor read result")
            _assert_return_strict_cursor_timeout_reply(
                env, result[0], cursor_id, expected_results=0,
                message_prefix='standalone RETURN_STRICT cursor-read timeout')

            after_info = info_modules_to_dict(env)
            env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC],
                            str(base_err_coord),
                            message="RETURN_STRICT cursor-read timeout must not bump coord error metric")
            env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC],
                            str(base_warn_coord + 1),
                            message="Coord timeout warning should be +1 after standalone cursor-read timeout")
            env.assertEqual(after_info[WARN_ERR_SECTION][TIMEOUT_WARNING_SHARD_METRIC],
                            str(base_warn_shard),
                            message="Standalone cursor-read timeout must not bump shard warning metric")

            env.expect('FT.CURSOR', 'DEL', 'idx', str(cursor_id)).ok()
        finally:
            env.expect(debug_cmd(), 'SYNC_POINT', 'CLEAR').ok()
            env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy).ok()

    def test_return_strict_timeout_shard_cursor_read_after_aggregate_claim(self):
        """Standalone RETURN_STRICT cursor-read timeout after BG owns results.

        The cursor-read worker is parked inside AggregateResults after appending
        one row. The timeout callback then loses AREQ_TryClaimAggregateResults,
        waits for the worker to store the partial reply, and returns the normal
        cursor-shaped timeout warning.
        """
        env = self.env
        skipIfNoEnableAssert(env)

        prev_policy, cursor_id, _, before_info, _, _ = \
            _setup_return_strict_cursor_state(env)
        base_err_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])
        base_warn_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])

        resetAggregateResultsDebug(env)
        setPauseAfterAggregateResult(env, 1)

        result = []
        try:
            t_query = threading.Thread(
                target=call_and_store,
                args=(env.cmd, ['FT.CURSOR', 'READ', 'idx', str(cursor_id)], result),
                daemon=True,
            )
            t_query.start()

            blocked_client_id = _wait_shard_paused_after_aggregate_result(env, 'FT.CURSOR|READ')
            env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)
            wait_for_client_unblocked(env, blocked_client_id)

            resetAggregateResultsDebug(env)

            t_query.join(timeout=10)
            env.assertFalse(t_query.is_alive(), message="Cursor read thread should have finished")
            env.assertEqual(len(result), 1, message="Expected one cursor read result")
            _assert_return_strict_cursor_timeout_reply(
                env, result[0], cursor_id, expected_results=1,
                message_prefix='standalone RETURN_STRICT cursor-read timeout after claim')

            after_info = info_modules_to_dict(env)
            env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC],
                            str(base_err_coord),
                            message="RETURN_STRICT cursor-read timeout must not bump coord error metric")
            env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC],
                            str(base_warn_coord + 1),
                            message="Coord timeout warning should be +1 after cursor-read timeout")

            env.expect('FT.CURSOR', 'DEL', 'idx', str(cursor_id)).ok()
        finally:
            resetAggregateResultsDebug(env)
            env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy).ok()

    def test_return_strict_cursor_read_no_deadlock_at_safe_loader_gil(self):
        """RETURN_STRICT cursor-read must not deadlock when the BG safe loader
        needs the GIL while the timeout callback waits for completion.

        The cursor was created with ``LOAD``, so the BG cursor-read pipeline runs
        the background-safe loader (RPSafeLoader). The worker wins
        AREQ_TryClaimAggregateResults, buffers the chunk, then parks at
        BeforeSafeLoaderGILLock - right before RedisModule_ThreadSafeContextLock.
        The main-thread timeout callback then loses the claim and blocks in
        AREQ_WaitForAggregateResultsComplete while holding the GIL. The worker is
        released by the timedOut predicate and tries to take the GIL: if the
        callback never releases it, the worker can neither load nor signal
        completion, so the callback waits forever and the server hangs.

        Guards against that deadlock: the cursor-read thread must finish and the
        callback must return a single reply.
        """
        env = self.env
        skipIfNoEnableAssert(env)
        sync_point = 'BeforeSafeLoaderGILLock'

        prev_policy, cursor_id, _, before_info, base_warn_coord, _ = \
            _setup_return_strict_cursor_state(env)
        base_err_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])

        try:
            env.expect(debug_cmd(), 'SYNC_POINT', 'CLEAR').ok()
            env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point).ok()

            result = []
            try:
                t_query = threading.Thread(
                    target=call_and_store,
                    args=(env.cmd, ['FT.CURSOR', 'READ', 'idx', str(cursor_id)], result),
                    daemon=True,
                )
                t_query.start()
                blocked_client_id = wait_for_blocked_query_client(
                    env, 'FT.CURSOR|READ', 'Client for FT.CURSOR|READ not found')
                # Worker won the claim, buffered the chunk, and parked before the GIL.
                wait_for_condition(
                    lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sync_point) == 1, {}),
                    f'worker never reached {sync_point}'
                )
                # Fire the deadline. The callback loses the claim and waits for
                # the worker; the worker is released by the timedOut predicate
                # and must still be able to take the GIL and signal completion.
                env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)
                wait_for_client_unblocked(env, blocked_client_id)
            finally:
                # WaitUntil auto-releases when timedOut flips, but disarm + clear
                # so the next test starts from a clean slate.
                env.expect(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point).ok()
                env.expect(debug_cmd(), 'SYNC_POINT', 'CLEAR').ok()

            t_query.join(timeout=10)
            env.assertFalse(t_query.is_alive(),
                            message="Cursor read thread hung - safe-loader GIL deadlock")
            env.assertEqual(len(result), 1, message="Expected one cursor read result")

            after_info = info_modules_to_dict(env)
            env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC],
                            str(base_err_coord),
                            message="RETURN_STRICT cursor-read timeout must not bump coord error metric")
            env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC],
                            str(base_warn_coord + 1),
                            message="Coord timeout warning should be +1 after cursor-read timeout")

            env.expect('FT.CURSOR', 'DEL', 'idx', str(cursor_id)).ok()
        finally:
            env.expect(debug_cmd(), 'SYNC_POINT', 'CLEAR').ok()
            env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy).ok()

    def _run_return_strict_no_deadlock_at_safe_loader_gil(self, query_args, cmd_name):
        """RETURN_STRICT FT.SEARCH / FT.AGGREGATE must not deadlock when the BG
        safe loader needs the GIL while the timeout callback runs.

        The query loads document fields, so the BG pipeline runs the
        background-safe loader (RPSafeLoader). The worker wins
        AREQ_TryClaimAggregateResults, buffers the chunk, then parks at
        BeforeSafeLoaderGILLock - right before RedisModule_ThreadSafeContextLock.
        The main-thread QueryTimeoutReturnStrictCallback then loses the claim.
        It must not block in AREQ_WaitForAggregateResultsComplete while holding
        the GIL. The safe-loader GIL handshake makes that safe: either the worker
        already marked itself at the GIL gate and the callback preempts it
        (AREQ_TimeoutPreemptSafeLoaderGIL) and replies empty without waiting, or
        the worker is released by the timedOut predicate and bails at
        AREQ_SafeLoaderEnterGIL without taking the GIL. If the callback blocked
        while the worker needed the GIL, neither could progress and the server
        would hang (MOD-8477).

        Guards against that deadlock: the query thread must finish and the
        callback must return a single empty + TIMEOUT-warning reply.
        """
        env = self.env
        skipIfNoEnableAssert(env)
        sync_point = 'BeforeSafeLoaderGILLock'

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict')

        before_info = info_modules_to_dict(env)
        base_warn_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])
        base_err_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])

        try:
            env.expect(debug_cmd(), 'SYNC_POINT', 'CLEAR').ok()
            env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point).ok()

            query_result = []
            try:
                t_query = threading.Thread(
                    target=call_and_store,
                    args=(env.cmd, list(query_args), query_result),
                    daemon=True,
                )
                t_query.start()
                blocked_client_id = wait_for_blocked_query_client(env, cmd_name)
                # Worker won the claim, buffered the chunk, and parked before the GIL.
                wait_for_condition(
                    lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sync_point) == 1, {}),
                    f'worker never reached {sync_point}'
                )
                # Fire the deadline. The callback loses the claim but must not
                # wait: it replies empty and the worker, released by the timedOut
                # predicate, takes the GIL and discards its buffer.
                env.cmd('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT')
                wait_for_client_unblocked(env, blocked_client_id)
            finally:
                # WaitUntil auto-releases when timedOut flips, but disarm + clear
                # so the next test starts from a clean slate.
                env.expect(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point).ok()
                env.expect(debug_cmd(), 'SYNC_POINT', 'CLEAR').ok()

            t_query.join(timeout=10)
            env.assertFalse(t_query.is_alive(),
                            message=f"{cmd_name} thread hung - safe-loader GIL deadlock")
            env.assertEqual(len(query_result), 1, message="Expected 1 result from query thread")
            result = query_result[0]
            # RETURN_STRICT contract: no rows are returned and a TIMEOUT warning is
            # set. total_results is intentionally not asserted - the pipeline may
            # have already produced (counted) some or all matches into the safe
            # loader's buffer before parking at the GIL gate, so its value is not
            # guaranteed.
            env.assertEqual(result.get('results', []), [],
                            message=f"Expected no rows, got {result.get('results')}")
            env.assertEqual(result.get('warning', []), [TIMEOUT_WARNING],
                            message=f"Expected TIMEOUT warning, got {result.get('warning', [])}")

            after_info = info_modules_to_dict(env)
            env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC],
                            str(base_err_coord),
                            message=f"RETURN_STRICT {cmd_name} timeout must not bump coord error metric")
            env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC],
                            str(base_warn_coord + 1),
                            message=f"Coord timeout warning should be +1 after {cmd_name}")
            _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_WARNING_COORD_METRIC])
        finally:
            env.expect(debug_cmd(), 'SYNC_POINT', 'CLEAR').ok()
            env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy)

    def test_return_strict_no_deadlock_at_safe_loader_gil_search(self):
        """RETURN_STRICT FT.SEARCH must not deadlock at the safe-loader GIL."""
        self._run_return_strict_no_deadlock_at_safe_loader_gil(
            ['FT.SEARCH', 'idx', '*'], 'FT.SEARCH')

    def test_return_strict_no_deadlock_at_safe_loader_gil_aggregate(self):
        """RETURN_STRICT FT.AGGREGATE with LOAD must not deadlock at the safe-loader GIL."""
        self._run_return_strict_no_deadlock_at_safe_loader_gil(
            ['FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@name'], 'FT.AGGREGATE')

    def test_return_strict_cursor_read_preempt_no_use_after_free(self):
        """RETURN_STRICT FT.CURSOR READ must not free the cursor under the worker.

        The cursor was created with LOAD, so the BG cursor-read pipeline runs the
        safe loader. The worker wins the claim, marks itself at the GIL gate
        (safeLoaderHoldingGIL == true), and parks at AfterSafeLoaderGILHandshake -
        after the handshake but before taking the Redis lock. The timeout callback
        loses the claim, observes holding == true, and takes the preempt branch:
        it replies with an exhausted cursor (id 0) and returns immediately. The
        blocked-client free callback (ShardCursorBlockClient_FreeAREQ) then drains
        req->storedReplyState.cursor - freeing the cursor (stashed before
        sendChunk) and dropping its AREQ reference - while the worker is still
        parked. Releasing the worker makes it resume sendChunk and keep using the
        freed cursor/AREQ: a use-after-free (caught under SAN, otherwise a crash).

        Detects the bug: releasing the worker after the cursor was freed must not
        corrupt memory; the cursor-read thread finishes and the server survives.
        """
        env = self.env
        skipIfNoEnableAssert(env)
        sync_point = 'AfterSafeLoaderGILHandshake'

        prev_policy, cursor_id, _, _, _, _ = _setup_return_strict_cursor_state(env)

        try:
            env.expect(debug_cmd(), 'SYNC_POINT', 'CLEAR').ok()
            env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point).ok()

            result = []
            try:
                t_query = threading.Thread(
                    target=call_and_store,
                    args=(env.cmd, ['FT.CURSOR', 'READ', 'idx', str(cursor_id)], result),
                    daemon=True,
                )
                t_query.start()
                blocked_client_id = wait_for_blocked_query_client(
                    env, 'FT.CURSOR|READ', 'Client for FT.CURSOR|READ not found')
                # Worker won the claim, set safeLoaderHoldingGIL, and parked after
                # the handshake (before taking the GIL).
                wait_for_condition(
                    lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sync_point) == 1, {}),
                    f'worker never reached {sync_point}'
                )
                # Fire the deadline: the callback preempts (holding == true), replies
                # cursor id 0, and the free callback frees the cursor while the
                # worker is still parked.
                env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)
                wait_for_client_unblocked(env, blocked_client_id)
            finally:
                # Release the parked worker: it resumes sendChunk and (pre-fix)
                # touches the freed cursor/AREQ.
                env.expect(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point).ok()
                env.expect(debug_cmd(), 'SYNC_POINT', 'CLEAR').ok()

            t_query.join(timeout=10)
            env.assertFalse(t_query.is_alive(),
                            message="Cursor read thread hung after preempt")
            env.assertEqual(len(result), 1, message="Expected one cursor read result")
            # Server must still be responsive (no UAF crash).
            env.expect('PING').true()
        finally:
            env.expect(debug_cmd(), 'SYNC_POINT', 'CLEAR').ok()
            env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy).ok()

    def test_return_strict_initial_withcursor_preempt_no_cursor_leak(self):
        """Initial RETURN_STRICT FT.AGGREGATE WITHCURSOR must not orphan its cursor.

        The initial WITHCURSOR query reserves a cursor and stashes it in
        storedReplyState.cursor before sendChunk. The worker wins the claim, marks
        itself at the GIL gate, and parks at AfterSafeLoaderGILHandshake. The
        timeout callback loses the claim, observes holding == true, and preempts:
        it sends an empty cursor-id-0 reply and returns, skipping the normal
        AREQ_ReplyWithStoredResults path that would pause/free the reserved cursor.
        The query blocked-client free callback only decrefs the AREQ, so the cursor
        stays in the global lookup with pos == -1: unreachable by FT.CURSOR READ
        and not idle-GC eligible, i.e. leaked on every such timeout.

        Detects the bug: after the worker finishes, the global cursor count must
        return to the pre-query baseline (no orphaned cursor).
        """
        env = self.env
        skipIfNoEnableAssert(env)
        sync_point = 'AfterSafeLoaderGILHandshake'

        prev_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict').ok()
        baseline_total = _coord_cursor_total(env)

        try:
            env.expect(debug_cmd(), 'SYNC_POINT', 'CLEAR').ok()
            env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point).ok()

            result = []
            try:
                t_query = threading.Thread(
                    target=call_and_store,
                    args=(env.cmd, ['FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@name',
                                    'WITHCURSOR', 'COUNT', '10'], result),
                    daemon=True,
                )
                t_query.start()
                blocked_client_id = wait_for_blocked_query_client(env, 'FT.AGGREGATE')
                # Worker reserved the cursor, set safeLoaderHoldingGIL, and parked.
                wait_for_condition(
                    lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sync_point) == 1, {}),
                    f'worker never reached {sync_point}'
                )
                # Fire the deadline: the callback preempts with an empty cursor-id-0
                # reply, skipping the cursor pause/free path.
                env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)
                wait_for_client_unblocked(env, blocked_client_id)
            finally:
                env.expect(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point).ok()
                env.expect(debug_cmd(), 'SYNC_POINT', 'CLEAR').ok()

            t_query.join(timeout=10)
            env.assertFalse(t_query.is_alive(), message="Aggregate thread hung after preempt")
            env.assertEqual(len(result), 1, message="Expected one aggregate result")
            _, reply_cursor_id = result[0]
            env.assertEqual(reply_cursor_id, 0,
                            message="Preempted WITHCURSOR reply must advertise an exhausted cursor")
            # The reserved cursor must not be orphaned: the global count returns to
            # baseline once the worker finishes (pre-fix it stays at baseline + 1).
            _wait_for_cursor_cleanup(env, baseline_total + 1,
                                     'initial WITHCURSOR preempt timeout')
        finally:
            env.expect(debug_cmd(), 'SYNC_POINT', 'CLEAR').ok()
            env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy).ok()

    def test_return_strict_stale_holding_flag_after_unlock_keeps_results(self):
        """A timeout in the unlock->clear-flag window must not drop loaded results.

        The worker wins the claim, takes the GIL gate, loads under the Redis lock,
        and unlocks - but parks at BeforeSafeLoaderExitGIL before clearing
        safeLoaderHoldingGIL. With the GIL already released, the worker no longer
        needs it, yet the handshake flag is still set. The timeout callback loses
        the claim, observes the stale holding == true, and takes the preempt
        branch: it returns an empty reply instead of waiting for the already-loaded
        partial results, losing data the RETURN_STRICT contract promises.

        The park is interruptible via the timedOut predicate so the post-fix path
        (flag cleared before unlock) does not deadlock; pre-fix the synchronous
        callback observes the stale flag before the worker's poll clears it.

        Detects the bug: the reply must carry the loaded rows (pre-fix it is empty).
        """
        env = self.env
        skipIfNoEnableAssert(env)
        sync_point = 'BeforeSafeLoaderExitGIL'

        prev_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict').ok()

        try:
            env.expect(debug_cmd(), 'SYNC_POINT', 'CLEAR').ok()
            env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point).ok()

            result = []
            try:
                t_query = threading.Thread(
                    target=call_and_store,
                    args=(env.cmd, ['FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@name',
                                    'LIMIT', '0', str(self.n_docs)], result),
                    daemon=True,
                )
                t_query.start()
                blocked_client_id = wait_for_blocked_query_client(env, 'FT.AGGREGATE')
                # Worker loaded under the lock, unlocked, and parked before clearing
                # the handshake flag (holding still true, GIL no longer held).
                wait_for_condition(
                    lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sync_point) == 1, {}),
                    f'worker never reached {sync_point}'
                )
                # Fire the deadline: pre-fix the callback observes the stale flag and
                # preempts, dropping the loaded rows.
                env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)
                wait_for_client_unblocked(env, blocked_client_id)
            finally:
                env.expect(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point).ok()
                env.expect(debug_cmd(), 'SYNC_POINT', 'CLEAR').ok()

            t_query.join(timeout=10)
            env.assertFalse(t_query.is_alive(), message="Aggregate thread hung after preempt")
            env.assertEqual(len(result), 1, message="Expected one aggregate result")
            reply = result[0]
            env.assertGreater(len(reply.get('results', [])), 0,
                              message="Loaded rows must survive a timeout in the "
                                      "unlock->clear-flag window, got an empty reply")
        finally:
            env.expect(debug_cmd(), 'SYNC_POINT', 'CLEAR').ok()
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

    def _run_return_strict_timeout_basic(self, query_args, cmd_name,
                                         arm, wait_for_park, teardown):
        """Driver for the three RETURN_STRICT shard scenarios that all
        produce the same observable reply: empty results + TIMEOUT warning,
        coord warning metric +1, no other metric movement.

        ``arm(env)`` is called before the query thread is launched: install
        any pause (sync-point ARM, WORKERS pause, ...) that BG must hit on
        its way to the target point.

        ``wait_for_park(env)`` is called after the query thread is launched
        and the client is registered as blocked: block until BG has actually
        reached the pause installed by ``arm``. Pass ``None`` for pauses
        that take effect implicitly (e.g. WORKERS pause).

        ``teardown(env, blocked_client_id)`` runs after CLIENT UNBLOCK and
        wait_for_client_unblocked: release any pause installed by ``arm`` so
        BG can finish cleanly.
        """
        env = self.env
        skipIfNoEnableAssert(env)

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict')

        before_info = info_modules_to_dict(env)
        base_warn_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])

        arm(env)

        query_result = []
        t_query = threading.Thread(
            target=call_and_store,
            args=(env.cmd, list(query_args), query_result),
            daemon=True,
        )
        t_query.start()

        blocked_client_id = wait_for_blocked_query_client(env, cmd_name)

        if wait_for_park is not None:
            wait_for_park(env)

        # Fire the blocked-client timeout. Depending on where BG is parked,
        # the callback either wins TryClaim (replies empty directly) or
        # loses TryClaim and waits for BG to finish + drain.
        env.cmd('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT')
        wait_for_client_unblocked(env, blocked_client_id)

        teardown(env, blocked_client_id)

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

        env.assertEqual(len(query_result), 1, message="Expected 1 result from query thread")
        result = query_result[0]
        env.assertEqual(result['total_results'], 0,
                        message=f"Expected 0 total_results, got {result['total_results']}")
        env.assertEqual(result.get('results', []), [],
                        message=f"Expected no rows, got {result.get('results')}")
        env.assertEqual(result.get('warning', []), [TIMEOUT_WARNING],
                        message=f"Expected TIMEOUT warning, got {result.get('warning', [])}")

        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC],
                        str(base_warn_coord + 1),
                        message=f"Coordinator timeout warning should be +1 after {cmd_name}")
        _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_WARNING_COORD_METRIC])

        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy)

    # --- Scenario 1: timeout fires before the worker thread picks up the job ---
    # WORKERS pause holds the job in the threadpool queue, so BG never reaches
    # AREQ_TryClaimAggregateResults. Main-thread callback wins TryClaim and
    # replies empty + warning; BG runs to completion after WORKERS resume,
    # observes the lost claim in startPipeline, and exits cleanly.

    def _run_return_strict_timeout_before_worker_pickup(self, query_args, cmd_name):
        def arm(env):
            env.expect(debug_cmd(), 'WORKERS', 'pause').ok()

        def teardown(env, blocked_client_id):
            env.expect(debug_cmd(), 'WORKERS', 'resume').ok()
            env.expect(debug_cmd(), 'WORKERS', 'drain').ok()

        self._run_return_strict_timeout_basic(query_args, cmd_name,
                                              arm, None, teardown)

    def test_return_strict_timeout_before_worker_pickup_search(self):
        """RETURN_STRICT shard timeout while the worker thread is paused.

        Pre-claim race: BG never enters startPipeline, the main-thread
        callback wins AREQ_TryClaimAggregateResults and emits an empty
        FT.SEARCH reply with the TIMEOUT warning.
        """
        self._run_return_strict_timeout_before_worker_pickup(
            ['FT.SEARCH', 'idx', '*'], 'FT.SEARCH')

    def test_return_strict_timeout_before_worker_pickup_aggregate(self):
        """RETURN_STRICT shard timeout while the worker thread is paused (FT.AGGREGATE).

        Same pre-claim race as the FT.SEARCH variant; covered separately
        because FT.AGGREGATE wires a different sorter / pager pipeline on
        top of RPIndex even for the bare ``*`` case.
        """
        self._run_return_strict_timeout_before_worker_pickup(
            ['FT.AGGREGATE', 'idx', '*'], 'FT.AGGREGATE')

    # --- Scenario 2: timeout fires before AREQ_TryClaimAggregateResults ---
    # BG is parked at the BeforeAggregateResultsClaim sync point (inside
    # startPipeline, before the TryClaim race). Main-thread callback wins
    # TryClaim and replies empty + warning. After CLIENT UNBLOCK we signal
    # the sync point so BG observes the lost claim and exits startPipeline.

    def _run_return_strict_timeout_at_claim(self, query_args, cmd_name):
        sync_point = 'BeforeAggregateResultsClaim'

        def arm(env):
            env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
            env.cmd(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point)

        def wait_for_park(env):
            wait_for_condition(
                lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sync_point) == 1, {}),
                f'Timeout waiting for {sync_point} sync point',
            )

        def teardown(env, blocked_client_id):
            env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point)
            env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')

        self._run_return_strict_timeout_basic(query_args, cmd_name,
                                              arm, wait_for_park, teardown)

    def test_return_strict_timeout_at_claim_sync_point_search(self):
        """RETURN_STRICT shard timeout at BeforeAggregateResultsClaim (FT.SEARCH).

        BG parks at the sync point just before AREQ_TryClaimAggregateResults;
        the main-thread callback always wins the claim and replies empty +
        TIMEOUT warning. BG observes the lost claim after the sync point is
        signalled and exits startPipeline cleanly.
        """
        self._run_return_strict_timeout_at_claim(
            ['FT.SEARCH', 'idx', '*'], 'FT.SEARCH')

    def test_return_strict_timeout_at_claim_sync_point_aggregate(self):
        """RETURN_STRICT shard timeout at BeforeAggregateResultsClaim (FT.AGGREGATE)."""
        self._run_return_strict_timeout_at_claim(
            ['FT.AGGREGATE', 'idx', '*'], 'FT.AGGREGATE')

    # --- Scenario 3: timeout fires after BG won TryClaim, before RPIndex's first read ---
    # BG is parked at the BeforeFirstRead sync point (interruptible via
    # areq_timed_out). The main-thread callback loses TryClaim and waits for
    # completion; BG breaks out of the WaitUntil as soon as the timedOut
    # flag is flipped, returns RS_RESULT_TIMEDOUT from rpQueryItNext without
    # producing any rows, signals completion, and the callback drains the
    # (empty) buffer and replies empty + warning.

    def _run_return_strict_timeout_at_first_read(self, query_args, cmd_name):
        sync_point = 'BeforeFirstRead'

        def arm(env):
            env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
            env.cmd(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point)

        def wait_for_park(env):
            wait_for_condition(
                lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sync_point) == 1, {}),
                f'Timeout waiting for {sync_point} sync point',
            )

        def teardown(env, blocked_client_id):
            # WaitUntil already auto-released when AREQ_TimedOut flipped, but
            # disarm + clear so the next test starts from a clean slate.
            env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')

        self._run_return_strict_timeout_basic(query_args, cmd_name,
                                              arm, wait_for_park, teardown)

    def test_return_strict_timeout_at_first_read_sync_point_search(self):
        """RETURN_STRICT shard timeout at BeforeFirstRead (FT.SEARCH).

        BG has already won AREQ_TryClaimAggregateResults and entered
        rpQueryItNext, but parks at BeforeFirstRead before the first
        iterator read. Main-thread callback loses the claim and waits;
        BG breaks out of the interruptible wait when timedOut flips,
        returns TIMEDOUT from RPIndex without producing rows, and the
        callback replies empty + TIMEOUT warning.
        """
        self._run_return_strict_timeout_at_first_read(
            ['FT.SEARCH', 'idx', '*'], 'FT.SEARCH')

    def test_return_strict_timeout_at_first_read_sync_point_aggregate(self):
        """RETURN_STRICT shard timeout at BeforeFirstRead (FT.AGGREGATE)."""
        self._run_return_strict_timeout_at_first_read(
            ['FT.AGGREGATE', 'idx', '*'], 'FT.AGGREGATE')

    # --- Scenario 4: timeout fires mid-iteration on a trivial pipeline ---
    # Uses AggregateResultsDebugCtx (FT.DEBUG QUERY_CONTROLLER
    # SET_PAUSE_AFTER_AGGREGATE_RESULT) to park the worker after exactly N
    # rows have been appended to `state.results`. The driver then fires
    # CLIENT UNBLOCK ... TIMEOUT (flips AREQ_TimedOut, callback runs and
    # loses TryClaim) and resumes the loop. The next rp->Next short-circuits
    # to RS_RESULT_TIMEDOUT in rpQueryItNext (the AREQ_TimedOut check at the
    # top of the while(1) loop), so AggregateResults exits with N buffered
    # rows. The callback (loser of TryClaim) waits for completion, skips the
    # post-timeout drain (canYieldPartialResults == false for the trivial
    # RPIndex -> RPPager shape), and replies with N harvested rows + the
    # TIMEOUT warning.
    #
    # Trivial shape (RPIndex -> RPPager) is reachable only via FT.AGGREGATE
    # without SORTBY/GROUPBY/APPLY/FILTER (FT.SEARCH always inserts an
    # implicit RPSorter, so it lands on shape (3) instead).

    def _run_return_strict_timeout_after_aggregate_result(
            self, query_args, cmd_name, pause_after_n, expected_rows):
        env = self.env
        skipIfNoEnableAssert(env)

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict')

        before_info = info_modules_to_dict(env)
        base_warn_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])

        resetAggregateResultsDebug(env)
        setPauseAfterAggregateResult(env, pause_after_n)

        query_result = []
        t_query = threading.Thread(
            target=call_and_store,
            args=(env.cmd, list(query_args), query_result),
            daemon=True,
        )
        t_query.start()

        blocked_client_id = wait_for_blocked_query_client(env, cmd_name)

        # Wait for the worker to park after the Nth appended row.
        wait_for_condition(
            lambda: (getIsAggregateResultsPaused(env) == 1,
                     {'count': getAggregateResultsCount(env)}),
            f'Timeout waiting for AggregateResults to pause after {pause_after_n} rows',
        )

        # Fire the blocked-client timeout. The callback flips AREQ_TimedOut,
        # loses TryClaim (BG already won), and blocks on
        # AREQ_WaitForAggregateResultsComplete. The pause loop in
        # debugCheckAndPauseAfterAggregateResult self-releases when it
        # observes AREQ_TimedOut, so BG resumes, exits AggregateResults with
        # N buffered rows, and signals completion -- unblocking the callback
        # which then replies with the partial results.
        env.cmd('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT')

        wait_for_client_unblocked(env, blocked_client_id)

        resetAggregateResultsDebug(env)

        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

        env.assertEqual(len(query_result), 1, message="Expected 1 result from query thread")
        result = query_result[0]
        env.assertEqual(len(result.get('results', [])), expected_rows,
                        message=f"Expected {expected_rows} harvested row(s), got {result.get('results')}")
        env.assertEqual(result.get('warning', []), [TIMEOUT_WARNING],
                        message=f"Expected TIMEOUT warning, got {result.get('warning', [])}")

        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC],
                        str(base_warn_coord + 1),
                        message="Coordinator timeout warning should be +1 after mid-iter harvest")
        _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_WARNING_COORD_METRIC])

        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy)

    def test_return_strict_timeout_after_first_result_aggregate(self):
        """Mid-iteration timeout right after the first appended row.

        LIMIT well above 1 so the pager doesn't EOF before the pause point.
        Verifies that rows BG accumulated in state.results before the
        timeout fired are emitted via the buffered-results path
        (independent of the canYieldPartialResults drain classifier).
        """
        self._run_return_strict_timeout_after_aggregate_result(
            ['FT.AGGREGATE', 'idx', '*', 'LIMIT', '0', '50'], 'FT.AGGREGATE',
            pause_after_n=1, expected_rows=1)

    def test_return_strict_timeout_after_last_result_aggregate(self):
        """Mid-iteration timeout right before the last row would be read.

        Pauses after LIMIT-1 buffered rows, then fires the timeout. The
        next rp->Next sees AREQ_TimedOut and returns RS_RESULT_TIMEDOUT
        instead of producing the LIMIT-th row, so the reply contains
        exactly LIMIT-1 rows. Boundary case: confirms the loop exits via
        the timeout path (not via the pager's EOF) when the timeout fires
        on the would-be-final iteration.
        """
        limit = 5
        self._run_return_strict_timeout_after_aggregate_result(
            ['FT.AGGREGATE', 'idx', '*', 'LIMIT', '0', str(limit)], 'FT.AGGREGATE',
            pause_after_n=limit - 1, expected_rows=limit - 1)

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

class TestNoDeadlockQueryWithConcurrentWriter:
    """MOD-15364: BG query holds the spec read lock; a concurrent writer
    parks on the spec write lock and blocks the main thread. With the fix,
    the BG worker releases the read lock on the BG thread (inside
    `AREQ_Execute`, before `AREQ_DecrRef`) prior to `RedisModule_UnblockClient`,
    so the writer can acquire the write lock and the main thread later runs
    the unblock callback. Without the fix, the unblock callback never runs
    (main thread is parked on wrlock), the read lock is never released, and
    the server deadlocks.

    Reproduction sequence (per test):
      1. Pause the BG worker at `BeforeAggregateResultsClaim` (mid-pipeline,
         while holding the spec read lock). Both FT.SEARCH and FT.AGGREGATE
         go through `startPipeline` and hit this sync point.
      2. Issue HSET on a separate connection. It parks the main thread on
         `pthread_rwlock_wrlock` and bumps the global `PendingSpecWriters`
         counter (in debug_commands.c).
      3. The sync point's stop predicate (`PendingSpecWriters_Get() > 0`)
         sees the bump and lets the BG worker resume on its own. We can't
         use a `SIGNAL` here because the main thread is blocked.
      4. BG worker finishes the pipeline; `AREQ_Execute` releases the read
         lock (the fix) before dropping the worker's ref, then the callback
         calls `RedisModule_UnblockClient`.
      5. Main thread acquires the wrlock, completes HSET, then processes
         the unblock callback.
    """

    SYNC_POINT = 'BeforeAggregateResultsClaim'

    def __init__(self):
        skipTest(cluster=True)

        self.env = Env(protocol=3, moduleArgs='WORKERS 1 TIMEOUT 0')
        skipIfNoEnableAssert(self.env)

        conn = getConnectionByEnv(self.env)
        self.env.expect('FT.CREATE', 'idx', 'PREFIX', '1', 'doc', 'SCHEMA',
                        'name', 'TEXT').ok()
        for i in range(10):
            conn.execute_command('HSET', f'doc{i}', 'name', f'hello{i}')

        self.env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()

    def _run(self, query_args):
        env = self.env
        env.expect(debug_cmd(), 'SYNC_POINT', 'CLEAR').ok()
        env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', self.SYNC_POINT).ok()
        try:
            query_conn = env.getConnection()
            writer_conn = env.getConnection()

            query_result = []
            t_query = threading.Thread(
                target=call_and_store,
                args=(query_conn.execute_command, list(query_args), query_result),
                daemon=True,
            )
            t_query.start()

            wait_for_condition(
                lambda: (env.cmd(debug_cmd(), 'SYNC_POINT',
                                 'IS_WAITING', self.SYNC_POINT) == 1, {}),
                f'BG worker never reached {self.SYNC_POINT}',
            )

            writer_result = []
            t_writer = threading.Thread(
                target=call_and_store,
                args=(writer_conn.execute_command,
                      ['HSET', 'doc:mod15364', 'name', 'concurrent-write'],
                      writer_result),
                daemon=True,
            )
            t_writer.start()

            t_writer.join(timeout=15)
            env.assertFalse(t_writer.is_alive(),
                            message='Writer (HSET) hung - main thread is blocked '
                                    'on the spec write lock; BG worker did not '
                                    'release the spec read lock before unblocking '
                                    'the client (MOD-15364)')

            t_query.join(timeout=15)
            env.assertFalse(t_query.is_alive(),
                            message=f'{query_args[0]} thread hung - blocked-client '
                                    'unblock callback never ran (MOD-15364)')
        finally:
            env.expect(debug_cmd(), 'SYNC_POINT', 'SIGNAL', self.SYNC_POINT).ok()
            env.expect(debug_cmd(), 'SYNC_POINT', 'CLEAR').ok()

    def test_aggregate(self):
        self._run(['FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@name',
                   'LIMIT', '0', '1'])

    def test_search(self):
        self._run(['FT.SEARCH', 'idx', '*', 'LIMIT', '0', '1'])
