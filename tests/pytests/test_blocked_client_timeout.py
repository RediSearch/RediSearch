# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from common import *
from contextlib import contextmanager
from redis import ConnectionPool, Redis
from redis.backoff import NoBackoff
from redis.exceptions import ResponseError
from redis.retry import Retry
from test_hybrid_internal import get_shard_slot_ranges
from test_info_modules import (
    info_modules_to_dict,
    wait_for_info_metric,
    WARN_ERR_SECTION, COORD_WARN_ERR_SECTION,
    TIMEOUT_ERROR_SHARD_METRIC, TIMEOUT_WARNING_SHARD_METRIC,
    TIMEOUT_WARNING_SHARD_QUEUE_METRIC,
    TIMEOUT_WARNING_SHARD_PIPELINE_METRIC, TIMEOUT_WARNING_SHARD_REPLY_METRIC,
    TIMEOUT_ERROR_COORD_METRIC, TIMEOUT_WARNING_COORD_METRIC,
    TIMEOUT_ERROR_COORD_QUEUE_METRIC, TIMEOUT_ERROR_COORD_PIPELINE_METRIC,
    TIMEOUT_ERROR_COORD_REPLY_METRIC,
    TIMEOUT_WARNING_COORD_QUEUE_METRIC, TIMEOUT_WARNING_COORD_PIPELINE_METRIC,
    TIMEOUT_WARNING_COORD_REPLY_METRIC,
    _verify_metrics_not_changed,
)
import threading
import psutil

TIMEOUT_ERROR = "Timeout limit was reached"
TIMEOUT_WARNING = TIMEOUT_ERROR


def run_cmd_expect_timeout(env, query_args):
    env.expect(*query_args).error().contains(TIMEOUT_ERROR)


def run_cmd_expect_disconnect(env, query_args, unexpected, client=None):
    client = client or env.getConnection()
    connection = client.connection_pool.get_connection()
    try:
        if query_args[0].startswith('_FT.'):
            connection.send_command('DEBUG', 'MARK-INTERNAL-CLIENT')
            connection.read_response()
        connection.send_command(*query_args)
        connection.read_response()
        unexpected.append(AssertionError('query returned after its client was killed'))
    except redis_exceptions.ConnectionError:
        pass
    except Exception as exc:
        unexpected.append(exc)
    finally:
        client.connection_pool.release(connection)


def _assert_disconnect_no_timeout_error(env, command, point, conn=None):
    """Check counters after cancellation has propagated through worker cleanup."""
    conn = conn or env.getConnection()
    before_info = info_modules_to_dict(conn)
    freed = int(conn.execute_command(debug_cmd(), 'QUERY_CONTROLLER',
                                     'GET_BLOCKED_REQUEST_ONFREE_COUNT'))
    unexpected = []
    thread = threading.Thread(target=run_cmd_expect_disconnect,
                              args=(env, command, unexpected, conn), daemon=True)
    # HYBRID's initial shard mapping has a separate publication hook.
    mapping = point in ('BEFORE', 'AFTER')
    if mapping:
        arm = [debug_cmd(), 'QUERY_CONTROLLER', f'SET_PAUSE_{point}_HYBRID_STORE_CURSORS']
        waiting = [debug_cmd(), 'QUERY_CONTROLLER', 'GET_IS_HYBRID_STORE_CURSORS_PAUSED']
        conn.execute_command(*arm, 'true')
    else:
        waiting = [debug_cmd(), 'SYNC_POINT', 'IS_WAITING', point]
        conn.execute_command(debug_cmd(), 'SYNC_POINT', 'ARM', point)
    command_name = command[0] + ('|READ' if command[0].endswith('.CURSOR') else '')
    try:
        thread.start()
        wait_for_condition(lambda: (conn.execute_command(*waiting) == 1, {}),
                           f'{command_name} did not pause at {point}')
        client_id = get_query_client(conn, command_name)
        env.assertTrue(client_id, message=f'No blocked {command_name} client')
        env.assertEqual(conn.execute_command('CLIENT', 'KILL', 'ID', client_id), 1)
        thread.join(timeout=10)
        env.assertFalse(thread.is_alive(), message=f'{command_name} did not disconnect')
        env.assertEqual(unexpected, [], message=unexpected)
        wait_for_condition(
            lambda: (int(conn.execute_command(debug_cmd(), 'QUERY_CONTROLLER',
                                             'GET_BLOCKED_REQUEST_ONFREE_COUNT')) > freed, {}),
            f'{command_name} did not release its blocked-client cycle')
        # PROFILE may encode a discarded timeout warning; no timeout callback ran.
        _verify_metrics_not_changed(env, conn, before_info,
                                    [TIMEOUT_WARNING_COORD_METRIC, TIMEOUT_WARNING_SHARD_METRIC])
    finally:
        if mapping:
            conn.execute_command(*arm, 'false')
        else:
            conn.execute_command(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point)
        thread.join(timeout=10)


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


def _get_blocked_request_onfree_count(env):
    """Read the coordinator QueryRequest_OnFree invocation counter (debug builds)."""
    return int(env.cmd(debug_cmd(), 'QUERY_CONTROLLER', 'GET_BLOCKED_REQUEST_ONFREE_COUNT'))


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


def _setup_hybrid_index(env):
    """Create a small hybrid index with a few docs on `env` and return a query vector."""
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
    return np.array([0.0, 0.0], dtype=np.float32).tobytes()


# Skipped under ASan pending MOD-16907.
@skip(cluster=False, asan=True)
def test_hybrid_cursors_race_with_flushall():
    """Regression for MOD-16878: publishing a shard's _FT.HYBRID cursors must not
    race with concurrent cursor cleanup (FLUSHALL / DROPINDEX / ...).

    Pause the shard just after it stores its cursors, FLUSHALL it, then resume and
    assert the shard did not crash.
    """
    env = Env(moduleArgs='WORKERS 1', protocol=3)
    skipIfNoEnableAssert(env)  # QUERY_CONTROLLER pause hooks are ENABLE_ASSERT-only
    query_vec = _setup_hybrid_index(env)

    target_shard = non_coord_shard_conns(env)[0]
    # Keep the query alive and long-running (RETURN policy + large TIMEOUT) so it
    # stays paused while we trigger the race.
    run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return')
    query = ['FT.HYBRID', 'hybrid_idx', 'SEARCH', '*',
             'VSIM', '@embedding', '$BLOB',
             'PARAMS', '2', 'BLOB', query_vec, 'TIMEOUT', '60000']

    def run_hybrid_ignore_errors():
        # The query may error after the flush; we only assert the shard stays up.
        try:
            env.cmd(*query)
        except Exception:
            pass

    target_shard.execute_command(debug_cmd(), 'QUERY_CONTROLLER',
                                 'SET_PAUSE_AFTER_HYBRID_STORE_CURSORS', 'true')
    t = threading.Thread(target=run_hybrid_ignore_errors, daemon=True)
    t.start()

    wait_for_condition(
        lambda: (target_shard.execute_command(
            debug_cmd(), 'QUERY_CONTROLLER',
            'GET_IS_HYBRID_STORE_CURSORS_PAUSED') == 1, {}),
        'shard did not pause after storing _FT.HYBRID cursors')

    # Flush the shard while its cursors are still in flight, so the teardown
    # runs concurrently with the cursor reply.
    target_shard.execute_command('FLUSHALL')

    # Resume the worker; it must finish replying without crashing the shard.
    target_shard.execute_command(debug_cmd(), 'QUERY_CONTROLLER',
                                 'SET_PAUSE_AFTER_HYBRID_STORE_CURSORS', 'false')
    t.join(timeout=10)
    env.assertFalse(t.is_alive(), message="FT.HYBRID thread should have finished")

    # The flush delete-marked the sub-cursors mid-cycle; the cycle-end park
    # on the main thread frees them. Nothing may leak.
    # (INFO hides the cursors section with zero indexes — recreate one.)
    env.expect('FT.CREATE', 'observe_idx', 'SCHEMA', 't', 'TEXT').ok()
    def shard_cursor_total():
        info = target_shard.execute_command('INFO', 'MODULES')
        return info['search_global_total_user'] + info['search_global_total_internal']
    wait_for_condition(lambda: (shard_cursor_total() == 0, {}),
                       'delete-marked cursors were not freed at the cycle-end park')

    # The shard survived the race.
    env.assertEqual(target_shard.execute_command('PING'), True)


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

    def _assert_disconnect_wakes_abort_channels(self, query_args, command_name):
        """Disconnect while every shard cursor read is parked and require cycle teardown."""
        env = self.env
        skipIfNoEnableAssert(env)

        prev_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()

        sync_point = 'BeforeCursorReadSendChunk'
        shard_connections = [env.getConnection(i) for i in range(1, env.shardsCount + 1)]
        for connection in shard_connections:
            connection.execute_command(debug_cmd(), 'SYNC_POINT', 'CLEAR')
            connection.execute_command(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point)

        before_info = info_modules_to_dict(env)
        free_count_before = _get_blocked_request_onfree_count(env)
        unexpected = []
        t_query = threading.Thread(
            target=run_cmd_expect_disconnect,
            args=(env, [*query_args, 'TIMEOUT', 0], unexpected),
            daemon=True,
        )
        try:
            t_query.start()
            for connection in shard_connections:
                wait_for_condition(
                    lambda connection=connection: (
                        connection.execute_command(
                            debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sync_point) == 1,
                        {},
                    ),
                    f'Timeout waiting for shard to park at {sync_point}',
                )

            blocked_client_id = wait_for_blocked_query_client(env, command_name)
            env.expect('CLIENT', 'KILL', 'ID', blocked_client_id).equal(1)
            t_query.join(timeout=10)
            env.assertFalse(t_query.is_alive(),
                            message=f'Disconnected {command_name} client should finish')
            env.assertEqual(unexpected, [], message=f'Unexpected query outcome: {unexpected}')

            # The shard workers remain parked, so only the disconnect wakeup can
            # let the coordinator readers finish and release the request cycle.
            wait_for_condition(
                lambda: (
                    _get_blocked_request_onfree_count(env) > free_count_before,
                    {
                        'before': free_count_before,
                        'now': _get_blocked_request_onfree_count(env),
                    },
                ),
                f'Disconnect did not release the blocked {command_name} request',
                timeout=10,
            )
            _verify_metrics_not_changed(env, env, before_info, [])
            env.assertTrue(env.isUp())
        finally:
            for connection in shard_connections:
                try:
                    connection.execute_command(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point)
                    connection.execute_command(debug_cmd(), 'SYNC_POINT', 'CLEAR')
                except Exception:
                    pass
            env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy).ok()

    def test_disconnect_wakes_aggregate_abort_channel(self):
        """A disconnected coordinator aggregate wakes its parked MR reader."""
        self._assert_disconnect_wakes_abort_channels(
            ['FT.AGGREGATE', 'idx', '*', 'LIMIT', '0', str(self.n_docs)],
            'FT.AGGREGATE',
        )

    def test_disconnect_wakes_hybrid_abort_channels(self):
        """A disconnected coordinator hybrid wakes both subqueries and its tail."""
        self._assert_disconnect_wakes_abort_channels(
            [
                'FT.HYBRID', 'hybrid_idx',
                'SEARCH', '*',
                'VSIM', '@embedding', '$BLOB',
                'PARAMS', '2', 'BLOB', self.hybrid_query_vec,
            ],
            'FT.HYBRID',
        )

    def test_disconnect_cursor_read(self):
        """Coordinator cursor cancellation must not increment timeout errors."""
        env = self.env
        skipIfNoEnableAssert(env)
        with _preserve_config(env, ON_TIMEOUT_CONFIG):
            env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()
            _, cursor = env.cmd('FT.AGGREGATE', 'idx', '*', 'TIMEOUT', 0,
                                'WITHCURSOR', 'COUNT', 1)
            env.assertNotEqual(cursor, 0)
            _assert_disconnect_no_timeout_error(
                env, ['FT.CURSOR', 'READ', 'idx', cursor], 'BeforeCursorReadSendChunk')
            env.expect('FT.CURSOR', 'READ', 'idx', cursor).error().contains('Cursor not found')

    def test_internal_disconnect_no_timeout_error(self):
        """Cover shard queries, MT HYBRID mappings, and both kinds of cursor read."""
        env = self.env
        skipIfNoEnableAssert(env)
        conn = non_coord_shard_conns(env)[0]
        prev_policy = to_dict(conn.execute_command('CONFIG', 'GET', ON_TIMEOUT_CONFIG))[ON_TIMEOUT_CONFIG]
        conn.execute_command('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail')
        try:
            for kind in ('SEARCH', 'AGGREGATE'):
                _assert_disconnect_no_timeout_error(
                    env, [f'_FT.{kind}', 'idx', '*', 'TIMEOUT', 0],
                    'BeforeAggregateResultsClaim', conn)

            aggregate = ['_FT.AGGREGATE', 'idx', '*', 'TIMEOUT', 0, 'WITHCURSOR', 'COUNT', 1]
            _assert_disconnect_no_timeout_error(env, aggregate, 'BeforeAggregateResultsClaim', conn)
            conn.execute_command('DEBUG', 'MARK-INTERNAL-CLIENT')
            _, cursor = conn.execute_command(*aggregate)
            env.assertNotEqual(cursor, 0)
            _assert_disconnect_no_timeout_error(
                env, ['_FT.CURSOR', 'READ', 'idx', cursor], 'BeforeCursorReadSendChunk', conn)

            hybrid = ['_FT.HYBRID', 'hybrid_idx', 'SEARCH', '*',
                      'VSIM', '@embedding', '$BLOB', 'PARAMS', 2, 'BLOB', self.hybrid_query_vec,
                      'TIMEOUT', 0, 'WITHCURSOR', 'COUNT', 1, '_COORD_DISPATCH_TIME', 0]
            for point in ('BEFORE', 'AFTER'):
                _assert_disconnect_no_timeout_error(env, hybrid, point, conn)
            conn.execute_command('DEBUG', 'MARK-INTERNAL-CLIENT')
            cursors = _internal_hybrid_cursor_map(conn.execute_command(*hybrid))
            for cursor in cursors.values():
                env.assertNotEqual(cursor, 0)
                _assert_disconnect_no_timeout_error(
                    env, ['_FT.CURSOR', 'READ', 'hybrid_idx', cursor],
                    'BeforeCursorReadSendChunk', conn)
        finally:
            conn.execute_command('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy)

    def _test_fail_timeout_impl(self, query_args, allow_timeout_warning=False):
        env = self.env

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()

        # Capture baseline metrics
        before_info = info_modules_to_dict(env)
        base_err_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])
        # The coordinator times out while fanning out to shards -> PIPELINE stage.
        base_err_pipeline = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_PIPELINE_METRIC])

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
        # FT.SEARCH (MR fan-out) and FT.AGGREGATE (coord AREQ in RPNet) are in their
        # pipeline while waiting for shards -> PIPELINE. FT.HYBRID/FT.PROFILE block in
        # the subquery depleters before the tail pipeline, so their coord fan-out
        # stage is approximate; only the sum invariant is checked for them.
        if query_args[0] in ('FT.SEARCH', 'FT.AGGREGATE'):
            env.assertEqual(int(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_PIPELINE_METRIC]),
                            base_err_pipeline + 1,
                            message=f"Coordinator fan-out timeout should bump the PIPELINE stage after {query_args[0]}")
        changed_metrics = [TIMEOUT_ERROR_COORD_METRIC]
        if allow_timeout_warning:
            warning_delta = (
                int(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])
                - int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC]))
            env.assertIn(warning_delta, (0, 1), message=after_info)
            changed_metrics.append(TIMEOUT_WARNING_COORD_METRIC)
        _verify_metrics_not_changed(env, env, before_info, changed_metrics)

        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy).ok()

    def test_fail_timeout_search(self):
        self._test_fail_timeout_impl(['FT.SEARCH', 'idx', '*'])

    def test_fail_timeout_aggregate(self):
        self._test_fail_timeout_impl(['FT.AGGREGATE', 'idx', '*'])

    def test_fail_timeout_profile_search(self):
        self._test_fail_timeout_impl(['FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '*'])

    def test_fail_timeout_profile_aggregate(self):
        self._test_fail_timeout_impl(['FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*'],
                                     allow_timeout_warning=True)

    def test_fail_timeout_wakes_profile_reply_wait(self):
        """FAIL releases the worker even while a shard's final profile is missing."""
        env = self.env
        skipIfNoEnableAssert(env)
        prev_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()
        coord_pid = pid_cmd(env.con)
        shard = psutil.Process(next(pid for pid in get_all_shards_pid(env) if pid != coord_pid))
        encode_point = 'DuringCoordBackgroundReplyEncode'
        reply_point = 'RpnetWaitingForReply'
        results, errors = [], []

        def query():
            try:
                results.append(env.cmd('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*',
                                       'LIMIT', 0, 1, 'TIMEOUT', 10000))
            except Exception as error:
                errors.append(error)

        thread = threading.Thread(target=query, daemon=True)
        shard.suspend()
        try:
            env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', encode_point).ok()
            thread.start()
            wait_for_condition(
                lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', encode_point),
                         {'results': results, 'errors': errors}),
                'PROFILE did not reach reply encoding', timeout=5)
            wait_for_condition(
                lambda: (env.cmd(debug_cmd(), 'BG_PENDING_REPLIES') == 1, {}),
                'Responsive shards did not finish', timeout=5)
            jobs_done = getCoordThpoolStats(env)['totalJobsDone']
            client_id = wait_for_blocked_query_client(env, 'FT.PROFILE')

            # The only result has been encoded. The next RPNet read therefore
            # belongs to printAggProfile, which still needs the paused shard.
            env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', reply_point).ok()
            env.expect(debug_cmd(), 'SYNC_POINT', 'SIGNAL', encode_point).ok()
            wait_for_condition(
                lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', reply_point), {}),
                'PROFILE did not start collecting remaining replies', timeout=5)
            env.expect(debug_cmd(), 'SYNC_POINT', 'SIGNAL', reply_point).ok()
            wait_for_condition(
                lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', reply_point) == 0, {}),
                'PROFILE did not leave the reply sync point', timeout=5)
            # Let the worker consume queued replies and enter the channel wait.
            # Cancelling at the sync point only tests the flag check before sleeping.
            time.sleep(0.1)
            env.expect('CLIENT', 'UNBLOCK', client_id, 'TIMEOUT').equal(1)
            thread.join(timeout=5)
            env.assertFalse(thread.is_alive())
            env.assertEqual(results, [])
            env.assertEqual(len(errors), 1, message=errors)
            if errors:
                env.assertTrue(isinstance(errors[0], ResponseError), message=errors)
                env.assertContains(TIMEOUT_ERROR, str(errors[0]))
            wait_for_condition(
                lambda: (getCoordThpoolStats(env)['totalJobsDone'] > jobs_done, {}),
                'FAIL timeout left the worker waiting for the paused shard', timeout=5)
        finally:
            shard.resume()
            env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', encode_point)
            env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', reply_point)
            thread.join(timeout=5)
            env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
            env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy)

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
        # The coordinator never picks up the job (thread pool paused below), so the
        # request is still queued -> QUEUE stage. AGGREGATE/HYBRID use the
        # CoordRequestCtx marker and attribute exactly; FT.SEARCH uses the MR path
        # (no marker) so only the sum invariant is checked for it.
        base_err_queue = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_QUEUE_METRIC])

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
        if cmd_name in ('FT.AGGREGATE', 'FT.HYBRID'):
            env.assertEqual(int(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_QUEUE_METRIC]),
                            base_err_queue + 1,
                            message=f"Timeout before coord pickup should bump the QUEUE stage after {cmd_name}")
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

    def _test_remaining_timeout_exhausted_before_shard_execution_impl(self, internal_cmd_args):
        """
        Test that a query whose entire timeout budget is consumed by coordinator dispatch
        time is handled correctly for the 'fail' and 'return' ON_TIMEOUT policies.

        Instead of going through the coordinator (which has its own blocked-client timer
        that masks the shard-level behavior), this test talks directly to the shard
        using internal commands (_FT.SEARCH, _FT.AGGREGATE, _FT.HYBRID) with a
        _COORD_DISPATCH_TIME that exceeds the TIMEOUT budget.

        Args:
            internal_cmd_args: Base args for the internal command (e.g. ['_FT.SEARCH', 'idx', '*']).
                Must NOT include TIMEOUT, _SLOTS_INFO, or _COORD_DISPATCH_TIME — these are added
                automatically.
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

        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail')
        try:
            env.expect(*full_args).error().contains(TIMEOUT_ERROR)
        finally:
            env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return')

    def test_remaining_timeout_exhausted_before_shard_execution_search(self):
        self._test_remaining_timeout_exhausted_before_shard_execution_impl(
            ['_FT.SEARCH', 'idx', '*'],
        )

    def test_remaining_timeout_exhausted_before_shard_execution_aggregate(self):
        self._test_remaining_timeout_exhausted_before_shard_execution_impl(
            ['_FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@name'],
        )

    def test_remaining_timeout_exhausted_before_shard_execution_hybrid(self):
        self._test_remaining_timeout_exhausted_before_shard_execution_impl(
            [
                '_FT.HYBRID', 'hybrid_idx',
                'SEARCH', '*',
                'VSIM', '@embedding', '$BLOB',
                'PARAMS', '2', 'BLOB', self.hybrid_query_vec,
            ],
        )

    def _test_remaining_timeout_exhausted_before_shard_execution_profile_impl(self, internal_cmd_args, hybrid=False):
        """
        Test that FT.PROFILE commands with pre-execution timeout produce consistent
        reply structures across SEARCH, AGGREGATE, and HYBRID.

        When profiling is active, timeout errors are suppressed (never returned as errors)
        regardless of the ON_TIMEOUT policy. SEARCH and AGGREGATE return empty results
        with profile wrapping; HYBRID's internal reply is its bare cursor mapping — the
        same shape as the non-bailing path, never profile-wrapped, since the coordinator's
        mapping parser is its only consumer and mapping-stage profile data has none.

        Args:
            internal_cmd_args: Base args for the internal profile command
                (e.g. ['_FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '*']).
                Must NOT include TIMEOUT, _SLOTS_INFO, or _COORD_DISPATCH_TIME.
            hybrid: expect the bare cursor-mapping reply instead of profile wrapping.
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

        # Profile suppresses the timeout error and preserves the profile structure.
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail')
        try:
            result = env.expect(*full_args).noError().res
            if hybrid:
                # Bare cursor mapping; a bailing shard publishes no cursors.
                env.assertEqual(result.get('SEARCH'), 0,
                    message=f"Expected no SEARCH cursor with FAIL policy, got: {result}")
                env.assertEqual(result.get('VSIM'), 0,
                    message=f"Expected no VSIM cursor with FAIL policy, got: {result}")
                profile_results = result
            else:
                # Verify profile wrapping: response should have 'Results' key
                env.assertContains('Results', result,
                    message=f"Expected 'Results' key in profile output with FAIL policy, got: {result}")
                profile_results = result['Results']
            # Verify timeout warning in results
            warnings = profile_results.get('warning', profile_results.get('warnings', []))
            env.assertTrue(warnings,
                message=f"Expected timeout warning with FAIL policy, got: {profile_results}")
            env.assertContains('Timeout', warnings[0],
                message=f"Expected timeout in warning with FAIL policy, got: {warnings}")
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
            hybrid=True,
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


    def test_no_timeout(self):
        """
        Test that using return or fail policies doesn't affect the regular flow
        when there is no timeout (i.e., FT.SEARCH completes normally and gets all expected
        replies from shards).
        """
        env = self.env

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        # No timeout occurs in this test: no error/warning metric may change, in
        # particular not the per-stage timeout breakdown (checked at the end).
        before_info = info_modules_to_dict(env)

        # Test with 'fail' policy
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()
        result = env.cmd('FT.SEARCH', 'idx', '*')
        env.assertEqual(result['total_results'], self.n_docs,
                        message=f"Expected {self.n_docs} total results with 'fail' policy")
        env.assertEqual(result.get('warning', []), [],
                        message="Expected no warning with 'fail' policy")

        # Test with 'return' policy
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return').ok()
        result = env.cmd('FT.SEARCH', 'idx', '*')
        env.assertEqual(result['total_results'], self.n_docs,
                        message=f"Expected {self.n_docs} total results with 'return' policy")
        env.assertEqual(result.get('warning', []), [],
                        message="Expected no warning with 'return' policy")

        # Test FT.PROFILE with 'fail' policy
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()
        result = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '*')
        env.assertContains('Results', result, message="Expected 'Results' key in FT.PROFILE output")
        profile_results = result['Results']
        env.assertEqual(profile_results['total_results'], self.n_docs,
                        message=f"Expected {self.n_docs} total results with 'fail' policy (FT.PROFILE)")
        env.assertEqual(profile_results.get('warning', []), [],
                        message="Expected no warning with 'fail' policy (FT.PROFILE)")

        # Test FT.PROFILE with 'return' policy
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return').ok()
        result = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '*')
        env.assertContains('Results', result, message="Expected 'Results' key in FT.PROFILE output")
        profile_results = result['Results']
        env.assertEqual(profile_results['total_results'], self.n_docs,
                        message=f"Expected {self.n_docs} total results with 'return' policy (FT.PROFILE)")
        env.assertEqual(profile_results.get('warning', []), [],
                        message="Expected no warning with 'return' policy (FT.PROFILE)")

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

        # None of the queries timed out, so no error/warning metric changed --
        # including the per-stage timeout breakdown.
        _verify_metrics_not_changed(env, env, before_info, [])

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
        # No timeout occurs in this test: no error/warning metric may change, in
        # particular not the per-stage timeout breakdown (checked at the end).
        before_info = info_modules_to_dict(env)
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

        # No cursor read timed out, so no error/warning metric changed -- including
        # the per-stage timeout breakdown.
        _verify_metrics_not_changed(env, env, before_info, [])

        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy).ok()

    def test_aggregate_cursor_reply_count_cluster(self):
        """Cluster FT.AGGREGATE WITHCURSOR drains exactly all aggregate rows."""
        env = self.env
        chunk_size = 7

        prev_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return').ok()
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
        reclaimed at cycle end (a cycle that records no park frees by default).
        """
        env = self.env
        skipIfNoEnableAssert(env)
        sync_point = 'BeforeCursorReadSendChunk'

        non_coord_shards = non_coord_shard_conns(env)
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

            # Only the target shard counts the blocked-client timeout error.
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

            # The worker observes the timeout and skips counting the discarded error.
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

        non_coord_shards = non_coord_shard_conns(env)
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
        """Time out at the coordinator's reply-production boundary."""
        env = self.env

        # Skip if ENABLE_ASSERT is not enabled
        skipIfNoEnableAssert(env)

        cmd_name = query_args[0]
        point = 'BeforeCoordBackgroundReplyEncode'

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()

        # Capture baseline metrics
        before_info = info_modules_to_dict(env)
        base_err_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])
        # The pipeline finished before this reply boundary.
        base_err_reply = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_REPLY_METRIC])

        env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', point).ok()

        jobs_done = getCoordThpoolStats(env)['totalJobsDone']
        t_query = threading.Thread(
            target=run_cmd_expect_timeout,
            args=(env, query_args),
            daemon=True
        )
        try:
            t_query.start()

            blocked_client_id = wait_for_blocked_query_client(env, cmd_name)

            wait_for_condition(
                lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', point) == 1, {}),
                'Timeout while waiting for query to pause before reply production'
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
                            message=f"Coordinator timeout error should be +1 after {cmd_name} before reply production")
            env.assertEqual(int(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_REPLY_METRIC]),
                            base_err_reply + 1,
                            message=f"Coordinator timeout before reply production should bump the REPLY stage after {cmd_name}")
            _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_ERROR_COORD_METRIC])

        finally:
            env.expect(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point).ok()
            _wait_for_coord_background_workers(env, jobs_done)
            t_query.join(timeout=10)
            env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy).ok()

    def _test_fail_timeout_after_coord_store_impl(self, query_args):
        """Time out at the coordinator's reply-production boundary."""
        env = self.env

        # Skip if ENABLE_ASSERT is not enabled
        skipIfNoEnableAssert(env)

        cmd_name = query_args[0]
        point = 'AfterCoordBackgroundReplyEncode'

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()

        # Capture baseline metrics
        before_info = info_modules_to_dict(env)
        base_err_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])
        # Still in the REPLY phase (the phase advanced to REPLY before the store).
        base_err_reply = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_REPLY_METRIC])

        env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', point).ok()

        jobs_done = getCoordThpoolStats(env)['totalJobsDone']
        t_query = threading.Thread(
            target=run_cmd_expect_timeout,
            args=(env, query_args),
            daemon=True
        )
        try:
            t_query.start()

            blocked_client_id = wait_for_blocked_query_client(env, cmd_name)

            wait_for_condition(
                lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', point) == 1, {}),
                'Timeout while waiting for query to pause after reply production'
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
                            message=f"Coordinator timeout error should be +1 after {cmd_name} after reply production")
            env.assertEqual(int(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_REPLY_METRIC]),
                            base_err_reply + 1,
                            message=f"Coordinator timeout after reply production should bump the REPLY stage after {cmd_name}")
            _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_ERROR_COORD_METRIC])

        finally:
            env.expect(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point).ok()
            _wait_for_coord_background_workers(env, jobs_done)
            t_query.join(timeout=10)
            env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy).ok()

    def test_fail_timeout_before_coord_encode_aggregate(self):
        """Timeout before coordinator background encoding of FT.AGGREGATE."""
        self._test_fail_timeout_before_coord_store_impl(['FT.AGGREGATE', 'idx', '*'])

    def test_fail_timeout_after_coord_encode_aggregate(self):
        """Timeout after coordinator background encoding of FT.AGGREGATE."""
        self._test_fail_timeout_after_coord_store_impl(['FT.AGGREGATE', 'idx', '*'])

    def test_fail_timeout_before_coord_encode_hybrid(self):
        """Test timeout occurring before coordinator stores results for FT.HYBRID."""
        self._test_fail_timeout_before_coord_store_impl([
            'FT.HYBRID', 'hybrid_idx',
            'SEARCH', '*',
            'VSIM', '@embedding', '$BLOB',
            'PARAMS', '2', 'BLOB', self.hybrid_query_vec
        ])

    def test_fail_timeout_after_coord_encode_hybrid(self):
        """Test timeout occurring after coordinator stores results for FT.HYBRID."""
        self._test_fail_timeout_after_coord_store_impl([
            'FT.HYBRID', 'hybrid_idx',
            'SEARCH', '*',
            'VSIM', '@embedding', '$BLOB',
            'PARAMS', '2', 'BLOB', self.hybrid_query_vec
        ])

    def _test_fail_timeout_coord_encode_cursor_read_impl(self, before):
        """FAIL timeout on FT.CURSOR READ paused before/after coordinator encoding."""
        env = self.env
        skipIfNoEnableAssert(env)

        prev_policy, cursor_id, baseline, before_info, base_err_coord = _setup_fail_cursor_state(env)

        sync_point = ('BeforeCoordBackgroundReplyEncode' if before
                      else 'AfterCoordBackgroundReplyEncode')
        self._arm_cursor_read_sync_point(sync_point)

        try:
            t_query, blocked_client_id = self._start_blocked_cursor_read(cursor_id)
            self._wait_worker_pinned_at_sync_point(sync_point)
            env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)
            wait_for_client_unblocked(env, blocked_client_id)
            t_query.join(timeout=10)
            env.assertFalse(t_query.is_alive(), message="Cursor read thread should have finished")
        finally:
            env.expect(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point).ok()
            run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy)


        # Wait for the worker's post-timeout wind-down before asserting.
        self._assert_cursor_freed_and_metric_bumped(cursor_id, baseline, before_info,
                                                    base_err_coord,
                                                    'FAIL coord-encode cursor-read timeout')


    def test_fail_timeout_before_coord_encode_cursor_read(self):
        """Test FAIL timeout on FT.CURSOR READ just before coordinator encoding."""
        self._test_fail_timeout_coord_encode_cursor_read_impl(before=True)

    def test_fail_timeout_after_coord_encode_cursor_read(self):
        """Test FAIL timeout on FT.CURSOR READ just after coordinator encoding."""
        self._test_fail_timeout_coord_encode_cursor_read_impl(before=False)

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

        # Cluster-wide node-local cursor totals (INFO MODULES per shard). The
        # timed-out coordinator must fan out `_FT.CURSOR DEL` for the published
        # sub-cursors it abandoned, so the counts return to this baseline
        # without waiting for the idle sweep (MOD-17913).
        def cluster_cursor_total():
            return sum(info['search_global_total_user'] +
                       info['search_global_total_internal']
                       for info in run_command_on_all_shards(env, 'INFO', 'MODULES'))
        baseline_cursor_total = cluster_cursor_total()

        # Enable pause before/after hybrid cursor storage on ALL shards
        if before:
            setPauseBeforeHybridStoreCursors(env, True)
        else:
            setPauseAfterHybridStoreCursors(env, True)

        query_args = [
            'FT.HYBRID', 'hybrid_idx',
            'SEARCH', '*',
            'VSIM', '@embedding', '$BLOB',
            'PARAMS', '2', 'BLOB', self.hybrid_query_vec, 'TIMEOUT', 0
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

        # The resumed shards publish their sub-cursor mappings after the
        # coordinator already failed the query; the coordinator must delete
        # them rather than leave them parked until the idle sweep.
        wait_for_condition(
            lambda: (cluster_cursor_total() <= baseline_cursor_total,
                     {'total': cluster_cursor_total(),
                      'baseline': baseline_cursor_total}),
            'FAIL-timed-out hybrid query leaked its published shard sub-cursors')

    def test_fail_timeout_before_shard_store_cursors_hybrid(self):
        """Test timeout occurring before shard stores cursors for internal FT.HYBRID."""
        self._test_fail_timeout_shard_store_cursors_impl(before=True)

    def test_fail_timeout_after_shard_store_cursors_hybrid(self):
        """Test timeout occurring after shard stores cursors for internal FT.HYBRID."""
        self._test_fail_timeout_shard_store_cursors_impl(before=False)


    def test_timeout_before_hybrid_read_arming(self):
        """A request may time out and be torn down before its reads are armed.

        Parks the coordinator IO thread at BeforeHybridArmReads — a shard's
        cursor mapping is in hand but its reads are not yet armed — then fires
        a FAIL timeout. The request replies and is torn down while the
        fan-out is still outstanding; once released, the arming callback must
        observe the timed-out read iterators, delete every published shard
        cursor instead of reading it, and drive all iterators to completion
        (IO pending-request count returns to zero, shard cursor counts return
        to baseline without waiting for the idle sweep).
        """
        env = self.env
        skipIfNoEnableAssert(env)

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail')

        def cluster_cursor_total():
            return sum(info['search_global_total_user'] +
                       info['search_global_total_internal']
                       for info in run_command_on_all_shards(env, 'INFO', 'MODULES'))
        baseline_cursor_total = cluster_cursor_total()

        sync_point = 'BeforeHybridArmReads'
        env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
        env.cmd(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point)

        query_args = [
            'FT.HYBRID', 'hybrid_idx',
            'SEARCH', '*',
            'VSIM', '@embedding', '$BLOB',
            'KNN', '2', 'K', '10',
            'COMBINE', 'RRF', '2', 'WINDOW', '10',
            'PARAMS', '2', 'BLOB', self.hybrid_query_vec,
        ]
        t_query = threading.Thread(
            target=run_cmd_expect_timeout,
            args=(env, query_args),
            daemon=True
        )
        t_query.start()

        try:
            blocked_client_id = wait_for_blocked_query_client(env, 'FT.HYBRID')
            wait_for_condition(
                lambda: (env.cmd(debug_cmd(), 'SYNC_POINT',
                                 'IS_WAITING', sync_point) == 1, {}),
                f'Timeout waiting for IO thread to park at {sync_point}'
            )

            env.cmd('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT')
            wait_for_client_unblocked(env, blocked_client_id)

            t_query.join(timeout=10)
            env.assertFalse(t_query.is_alive(), message="Query thread should have finished")

            # The request replied and was torn down, but the arming fan-out is
            # still parked: its IO requests are outstanding.
            pending = env.cmd(debug_cmd(), 'IO_RUNTIME_PENDING_REQUESTS')
            env.assertGreaterEqual(pending, 1)
            env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point)

            def request_completed():
                pending = env.cmd(debug_cmd(), 'IO_RUNTIME_PENDING_REQUESTS')
                return pending == 0, {'pending': pending}

            wait_for_condition(
                request_completed,
                'Timeout waiting for abandoned arming fan-out completion',
                timeout=10
            )

            # The late-armed placeholders were dispatched as DELs: nothing may
            # stay parked shard-side.
            wait_for_condition(
                lambda: (cluster_cursor_total() <= baseline_cursor_total,
                         {'total': cluster_cursor_total(),
                          'baseline': baseline_cursor_total}),
                'abandoned arming fan-out leaked its published shard cursors')
            env.expect('PING').equal(True)
        finally:
            try:
                env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
            except Exception:
                pass
            env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy)


    # ------------------------------------------------------------------
    # timer (CLIENT UNBLOCK ... TIMEOUT) fires while the user query
    # carries TIMEOUT 0, so the coord deadline never arms.
    # ------------------------------------------------------------------




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

    def test_disconnect_during_reduce(self):
        """A coordinator FT.SEARCH treats disconnect as cancellation."""
        env = self.env
        prev_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()
        before_info = info_modules_to_dict(env)
        freed = _get_blocked_request_onfree_count(env)
        unexpected = []
        setPauseBeforeReduce(env, 1)
        t_query = threading.Thread(
            target=run_cmd_expect_disconnect,
            args=(env, ['FT.SEARCH', 'idx', '*', 'TIMEOUT', 0], unexpected),
            daemon=True,
        )
        try:
            t_query.start()
            blocked_client_id = wait_for_blocked_query_client(env, 'FT.SEARCH')
            wait_for_condition(
                lambda: (getIsCoordReducePaused(env) == 1,
                         {'paused': getIsCoordReducePaused(env)}),
                'Timeout waiting for coordinator reducer to pause',
            )
            env.expect('CLIENT', 'KILL', 'ID', blocked_client_id).equal(1)
            t_query.join(timeout=10)
            env.assertFalse(t_query.is_alive(),
                            message='Disconnected coordinator query should finish')
            env.assertEqual(unexpected, [], message=f'Unexpected query outcome: {unexpected}')
            self._cleanup_pause_state()
            wait_for_condition(lambda: (_get_blocked_request_onfree_count(env) > freed, {}),
                               'Disconnected SEARCH did not release its request')
            _verify_metrics_not_changed(env, env, before_info, [])
            env.assertTrue(env.isUp())
        finally:
            self._cleanup_pause_state()
            env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy).ok()

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


class TestWorkerTransitions:
    """FAIL remains usable when shard execution changes between inline and workers."""

    def __init__(self):
        self.env = Env(
            protocol=3,
            moduleArgs='WORKERS 0 ON_TIMEOUT FAIL TIMEOUT 10000',
            enableDebugCommand=True,
        )
        conn = getConnectionByEnv(self.env)
        self.env.expect(
            'FT.CREATE', 'idx', 'PREFIX', '1', 'doc:',
            'SCHEMA', 'name', 'TEXT'
        ).ok()
        self.env.expect(
            'FT.CREATE', 'hybrid_idx', 'PREFIX', '1', 'hybrid_doc:',
            'SCHEMA',
            'name', 'TEXT',
            'embedding', 'VECTOR', 'FLAT', '6',
            'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2'
        ).ok()
        for i in range(12):
            conn.execute_command('HSET', f'doc:{i}', 'name', f'hello{i}')
            vector = np.array([float(i), float(i)], dtype=np.float32).tobytes()
            conn.execute_command(
                'HSET', f'hybrid_doc:{i}', 'name', f'hello{i}', 'embedding', vector
            )
        self.query_vector = np.array([0.0, 0.0], dtype=np.float32).tobytes()

    def _set_workers(self, workers):
        set_workers(self.env, workers)

    def _pause_workers(self):
        verify_command_OK_on_all_shards(self.env, debug_cmd(), 'WORKERS', 'pause')

    def _resume_and_drain_workers(self):
        # Resume every shard before draining any of them: a cluster cursor job
        # on one shard may be waiting for work queued on another shard.
        verify_command_OK_on_all_shards(self.env, debug_cmd(), 'WORKERS', 'resume')
        verify_command_OK_on_all_shards(self.env, debug_cmd(), 'WORKERS', 'drain')


    def _create_cursor(self):
        res, cursor_id = self.env.cmd(
            'FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@name',
            'WITHCURSOR', 'COUNT', '2'
        )
        self.env.assertEqual(res.get('warning', []), [], message=res)
        self.env.assertNotEqual(cursor_id, 0, message=res)
        return cursor_id

    def _read_and_delete_cursor(self, cursor_id):
        res, next_cursor_id = self.env.cmd(
            'FT.CURSOR', 'READ', 'idx', cursor_id, 'COUNT', '2'
        )
        self.env.assertEqual(res.get('warning', []), [], message=res)
        self.env.assertGreater(len(res.get('results', [])), 0, message=res)
        if next_cursor_id:
            self.env.expect('FT.CURSOR', 'DEL', 'idx', next_cursor_id).ok()


    def test_cursor_restores_timeout_after_workers_restart(self):
        """A foreground cap must not replace the timeout cached for later worker reads."""
        skipTest(cluster=True)
        self._set_workers(1)
        cursor_id = self._create_cursor()
        self.env.expect(
            'CONFIG', 'SET', 'search-_max-foreground-timeout-limit', '1000'
        ).ok()

        try:
            self._set_workers(0)
            _, cursor_id = self.env.cmd(
                'FT.CURSOR', 'READ', 'idx', cursor_id, 'COUNT', '2'
            )
            self.env.assertNotEqual(cursor_id, 0)

            self._set_workers(1)
            res, next_cursor_id = self.env.cmd(
                'FT.CURSOR', 'READ', 'idx', cursor_id, 'COUNT', '2'
            )
            self.env.assertGreater(len(res.get('results', [])), 0, message=res)
            if next_cursor_id:
                self.env.expect('FT.CURSOR', 'DEL', 'idx', next_cursor_id).ok()
        finally:
            self.env.expect(
                'CONFIG', 'SET', 'search-_max-foreground-timeout-limit', '0'
            ).ok()

    def test_fail_cursor_switches_from_clock_to_blocked_client(self):
        """FAIL cursor reads can move inline and then rearm a blocked-client timeout."""
        skipTest(cluster=True)
        previous_policy = self.env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        run_command_on_all_shards(self.env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail')
        cursor_id = 0
        workers_paused = False
        try:
            self._set_workers(1)
            cursor_id = self._create_cursor()

            self._set_workers(0)
            inline_res, cursor_id = self.env.cmd(
                'FT.CURSOR', 'READ', 'idx', cursor_id, 'COUNT', '2')
            self.env.assertEqual(inline_res.get('warning', []), [], message=inline_res)
            self.env.assertNotEqual(cursor_id, 0, message=inline_res)

            self._set_workers(1)
            before_info = info_modules_to_dict(self.env)
            self._pause_workers()
            workers_paused = True
            t_query = threading.Thread(
                target=run_cmd_expect_timeout,
                args=(self.env, ['FT.CURSOR', 'READ', 'idx', str(cursor_id), 'COUNT', '2']),
                daemon=True,
            )
            t_query.start()
            blocked_client_id = wait_for_blocked_query_client(
                self.env, 'FT.CURSOR|READ', 'Client for FT.CURSOR|READ not found')
            self.env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)
            wait_for_client_unblocked(self.env, blocked_client_id)
            t_query.join(timeout=10)
            self.env.assertFalse(t_query.is_alive(), message="Cursor read thread should finish")

            self._resume_and_drain_workers()
            workers_paused = False
            self.env.expect('FT.CURSOR', 'READ', 'idx', str(cursor_id)).error().contains(
                'Cursor not found')
            after_info = info_modules_to_dict(self.env)
            self.env.assertEqual(
                int(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_QUEUE_METRIC]),
                int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_QUEUE_METRIC]) + 1,
                message='FAIL timeout after clock-to-blocked transition should be attributed to QUEUE')
            cursor_id = 0
        finally:
            if workers_paused:
                self._resume_and_drain_workers()
            if cursor_id:
                try:
                    self.env.cmd('FT.CURSOR', 'DEL', 'idx', cursor_id)
                except Exception:
                    pass
            run_command_on_all_shards(
                self.env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, previous_policy)


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

    def test_disconnect_marks_blocked_client_timeout(self):
        """Disconnect cancels standalone queries without counting a timeout error."""
        env = self.env
        skipIfNoEnableAssert(env)
        with _preserve_config(env, ON_TIMEOUT_CONFIG):
            env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()
            for kind in ('SEARCH', 'AGGREGATE', 'HYBRID'):
                query = (self._standalone_hybrid_query(['TIMEOUT', 0]) if kind == 'HYBRID'
                         else [f'FT.{kind}', 'idx', '*', 'TIMEOUT', 0])
                point = ('BeforeHybridResultsClaim' if kind == 'HYBRID'
                         else 'BeforeAggregateResultsClaim')
                _assert_disconnect_no_timeout_error(env, query, point)
                profile = ['FT.PROFILE', query[1], kind, 'QUERY', *query[2:]]
                _assert_disconnect_no_timeout_error(env, profile, point)

            aggregate = ['FT.AGGREGATE', 'idx', '*', 'TIMEOUT', 0, 'WITHCURSOR', 'COUNT', 1]
            _assert_disconnect_no_timeout_error(env, aggregate, 'BeforeAggregateResultsClaim')
            _, cursor = env.cmd(*aggregate)
            env.assertNotEqual(cursor, 0)
            _assert_disconnect_no_timeout_error(
                env, ['FT.CURSOR', 'READ', 'idx', cursor], 'BeforeCursorReadSendChunk')
            env.expect('FT.CURSOR', 'READ', 'idx', cursor).error().contains('Cursor not found')

    def test_shard_timeout_fail(self):
        """Test shard timeout with FAIL policy."""
        env = self.env

        # Set timeout policy to FAIL
        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()

        # Capture baseline metrics (standalone uses coord metrics)
        before_info = info_modules_to_dict(env)
        base_err_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])
        # The worker pool is paused below, so the query times out while still
        # queued (before its pipeline runs) -> the timeout is attributed to the
        # QUEUE stage for every query type, including FT.HYBRID.
        base_err_queue = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_QUEUE_METRIC])

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
            # A queued timeout is attributed to the QUEUE stage for every query type.
            env.assertEqual(
                int(info_dict[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_QUEUE_METRIC]),
                base_err_queue + i + 1,
                message=f"{query_type} queued timeout should bump the QUEUE-stage counter")

        # Verify no other metrics changed, and the aggregate equals the per-stage sum.
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
        # The query is parked inside the result-processor pipeline, so the timeout
        # is attributed to the PIPELINE stage.
        base_err_pipeline = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_PIPELINE_METRIC])

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
                            message=f"Coordinator timeout error should increment once after {query_type}")
            # The timeout fired mid-pipeline, so it lands in the PIPELINE stage.
            env.assertEqual(
                int(info_dict[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_PIPELINE_METRIC]),
                base_err_pipeline + i + 1,
                message=f"{query_type} in-pipeline timeout should bump the PIPELINE-stage counter")

        # Both the stage and aggregate counters count the timeout callback once.
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

        # Observe accounting after the worker has finished discarding its reply.
        env.expect(debug_cmd(), 'WORKERS', 'DRAIN').ok()
        after_info = info_modules_to_dict(env)
        env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC],
                        str(base_err_coord + 1),
                        message="Coord timeout error should be +1 after QI sync-point timeout")

        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy).ok()

    def _test_fail_timeout_reply_boundary_impl(self, query_args, before, cmd_name=None):
        """Pause FAIL around worker encoding, including HYBRID."""
        env = self.env
        skipIfNoEnableAssert(env)
        cmd_name = cmd_name or query_args[0]
        point = 'BeforeBackgroundReplyEncode' if before else 'AfterBackgroundReplyEncode'
        prev_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()
        before_info = info_modules_to_dict(env)
        base_err_coord = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])
        base_err_reply = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_REPLY_METRIC])

        env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', point).ok()

        t_query = threading.Thread(target=run_cmd_expect_timeout, args=(env, query_args), daemon=True)
        try:
            t_query.start()
            blocked_client_id = wait_for_blocked_query_client(env, cmd_name)
            wait_for_condition(
                lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', point) == 1, {}),
                f'Timeout waiting for {cmd_name} reply boundary')
            env.expect('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT').equal(1)
            wait_for_client_unblocked(env, blocked_client_id)
            t_query.join(timeout=10)
            env.assertFalse(t_query.is_alive(), message='Query thread should have finished')

            after_info = info_modules_to_dict(env)
            env.assertEqual(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC],
                            str(base_err_coord + 1),
                            message=f'Expected one timeout error for {cmd_name}')
            env.assertEqual(
                int(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_REPLY_METRIC]),
                base_err_reply + 1, message=f'{cmd_name}: timeout must be attributed to REPLY')
            _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_ERROR_COORD_METRIC])
        finally:
            env.expect(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point).ok()
            t_query.join(timeout=10)
            env.expect(debug_cmd(), 'WORKERS', 'DRAIN').ok()
            env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy).ok()

    def _test_fail_timeout_before_store_impl(self, query_args, cmd_name=None):
        self._test_fail_timeout_reply_boundary_impl(query_args, True, cmd_name)

    def _test_fail_timeout_after_store_impl(self, query_args, cmd_name=None):
        self._test_fail_timeout_reply_boundary_impl(query_args, False, cmd_name)

    def test_fail_timeout_before_encode_search(self):
        """Test timeout occurring before encoding results for FT.SEARCH in standalone."""
        self._test_fail_timeout_before_store_impl(['FT.SEARCH', 'idx', '*'])

    def test_fail_timeout_before_encode_aggregate(self):
        """Test timeout occurring before encoding results for FT.AGGREGATE in standalone."""
        self._test_fail_timeout_before_store_impl(['FT.AGGREGATE', 'idx', '*'])

    def test_fail_timeout_after_encode_search(self):
        """Test timeout occurring after encoding results for FT.SEARCH in standalone."""
        self._test_fail_timeout_after_store_impl(['FT.SEARCH', 'idx', '*'])

    def test_fail_timeout_after_encode_aggregate(self):
        """Test timeout occurring after encoding results for FT.AGGREGATE in standalone."""
        self._test_fail_timeout_after_store_impl(['FT.AGGREGATE', 'idx', '*'])

    def test_fail_timeout_before_encode_hybrid(self):
        """Test timeout occurring before encoding results for FT.HYBRID in standalone."""
        self._test_fail_timeout_before_store_impl([
            'FT.HYBRID', 'hybrid_idx',
            'SEARCH', '*',
            'VSIM', '@embedding', '$BLOB',
            'PARAMS', '2', 'BLOB', self.hybrid_query_vec
        ])

    def test_fail_timeout_after_encode_hybrid(self):
        """Test timeout occurring after encoding results for FT.HYBRID in standalone."""
        self._test_fail_timeout_after_store_impl([
            'FT.HYBRID', 'hybrid_idx',
            'SEARCH', '*',
            'VSIM', '@embedding', '$BLOB',
            'PARAMS', '2', 'BLOB', self.hybrid_query_vec
        ])

    def _standalone_hybrid_query(self, extra_args=None):
        args = [
            'FT.HYBRID', 'hybrid_idx',
            'SEARCH', '*',
            'VSIM', '@embedding', '$BLOB',
            'PARAMS', '2', 'BLOB', self.hybrid_query_vec,
        ]
        if extra_args:
            args += list(extra_args)
        return args

    def _standalone_hybrid_full_result_query(self, extra_args=None):
        args = [
            'FT.HYBRID', 'hybrid_idx',
            'SEARCH', '*',
            'VSIM', '@embedding', '$BLOB',
            'KNN', '2', 'K', '10000',
            'COMBINE', 'RRF', '2', 'WINDOW', '10000',
            'PARAMS', '2', 'BLOB', self.hybrid_query_vec,
            'LIMIT', '0', '10000',
        ]
        if extra_args:
            args += list(extra_args)
        return args


    def test_no_timeout_cursor(self):
        """
        Test that FAIL policy doesn't break cursor reads when there is no timeout.
        This verifies that useReplyCallback is properly cleared for cursor reads,
        since cursor reads use BlockCursorClientWithTimeout which has no reply_callback.
        """
        env = self.env

        prev_on_timeout_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        # No timeout occurs in this test: no error/warning metric may change, in
        # particular not the per-stage timeout breakdown (checked at the end).
        before_info = info_modules_to_dict(env)
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

        # No cursor read timed out, so no error/warning metric changed -- including
        # the per-stage timeout breakdown.
        _verify_metrics_not_changed(env, env, before_info, [])

        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_on_timeout_policy).ok()

    def test_aggregate_cursor_reply_count_standalone(self):
        """Standalone FT.AGGREGATE WITHCURSOR drains exactly all aggregate rows."""
        env = self.env
        chunk_size = 7

        prev_policy = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)[ON_TIMEOUT_CONFIG]
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return').ok()
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
                        message="Coordinator timeout error should increment once after shard FAIL cursor-read timeout")
        _verify_metrics_not_changed(env, env, before_info, [TIMEOUT_ERROR_COORD_METRIC])

        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy).ok()

    def test_fail_timeout_shard_cursor_read_before_encode(self):
        """FAIL timeout on FT.CURSOR READ paused before background encoding."""
        env = self.env
        prev_policy, cursor_id, baseline, _, _ = _setup_fail_cursor_state(env)
        try:
            self._test_fail_timeout_before_store_impl(
                ['FT.CURSOR', 'READ', 'idx', str(cursor_id)], cmd_name='FT.CURSOR|READ')
            _wait_for_cursor_cleanup(env, baseline,
                                     'shard FAIL cursor-read timeout before encoding')
            env.expect('FT.CURSOR', 'READ', 'idx', str(cursor_id)).error().contains('Cursor not found')
        finally:
            env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, prev_policy).ok()

    def test_fail_timeout_shard_cursor_read_after_encode(self):
        """FAIL timeout on FT.CURSOR READ paused after background encoding."""
        env = self.env
        prev_policy, cursor_id, baseline, _, _ = _setup_fail_cursor_state(env)
        try:
            self._test_fail_timeout_after_store_impl(
                ['FT.CURSOR', 'READ', 'idx', str(cursor_id)], cmd_name='FT.CURSOR|READ')
            _wait_for_cursor_cleanup(env, baseline,
                                     'shard FAIL cursor-read timeout after encoding')
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

    def _test_remaining_timeout_exhausted_before_shard_execution_debug_impl(self, query_cmd):
        """
        Test that FT.DEBUG commands with pre-execution timeout (via _COORD_DISPATCH_TIME)
        correctly handle timeout in the debug command path (DEBUG_execCommandCommon).

        EXEC_DEBUG does NOT include EXEC_WITH_PROFILE, so:
        - 'fail' policy → timeout error
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

        env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail')
        try:
            env.expect(*full_args).error().contains(TIMEOUT_ERROR)
        finally:
            env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return')

    def test_remaining_timeout_exhausted_before_shard_execution_debug_search(self):
        self._test_remaining_timeout_exhausted_before_shard_execution_debug_impl(
            ['_FT.SEARCH', 'idx', '*'],
        )

    def test_remaining_timeout_exhausted_before_shard_execution_debug_aggregate(self):
        self._test_remaining_timeout_exhausted_before_shard_execution_debug_impl(
            ['_FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@name'],
        )


    # --- Scenario 1: timeout fires before the worker thread picks up the job ---
    # WORKERS pause holds the job in the threadpool queue, so BG never reaches
    # AREQ_TryClaimAggregateResults. Main-thread callback wins TryClaim and
    # replies empty + warning; BG runs to completion after WORKERS resume,
    # observes the lost claim in startPipeline, and exits cleanly.


    # --- Scenario 2: timeout fires before AREQ_TryClaimAggregateResults ---
    # BG is parked at the BeforeAggregateResultsClaim sync point (inside
    # startPipeline, before the TryClaim race). Main-thread callback wins
    # TryClaim and replies empty + warning. After CLIENT UNBLOCK we signal
    # the sync point so BG observes the lost claim and exits startPipeline.


    # --- Scenario 3: timeout fires after BG won TryClaim, before RPIndex's first read ---
    # BG is parked at the BeforeFirstRead sync point (interruptible via
    # areq_timed_out). The main-thread callback loses TryClaim and waits for
    # completion; BG breaks out of the WaitUntil as soon as the timedOut
    # flag is flipped, returns RS_RESULT_TIMEDOUT from rpQueryItNext without
    # producing any rows, signals completion, and the callback drains the
    # (empty) buffer and replies empty + warning.


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




class TestShardTimeoutResp2:
    """Tests for shard timeout behavior with RESP2 protocol.

    Covers the RESP2 branch in sendChunk_ReplyOnly_EmptyResults, where timeout warnings
    are tracked in ProfileWarnings and global stats but not emitted in the reply
    (consistent with RESP2 not having a warnings array).
    """
    def __init__(self):
        skipTest(cluster=True)

        self.env = Env(protocol=2, moduleArgs='WORKERS 1 TIMEOUT 0')
        self.n_docs = 100

        conn = getConnectionByEnv(self.env)
        self.env.expect('FT.CREATE', 'idx', 'PREFIX', '1', 'doc', 'SCHEMA', 'name', 'TEXT').ok()
        for i in range(self.n_docs):
            conn.execute_command('HSET', f'doc{i}', 'name', f'hello{i}')

    def test_remaining_timeout_exhausted_before_shard_execution_resp2(self):
        """Test RESP2 pre-execution timeout with the FAIL policy."""
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

            env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail')
            try:
                env.expect(*full_args).error().contains(TIMEOUT_ERROR)
            finally:
                env.cmd('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return')



class TestNoDeadlockQueryWithConcurrentWriter:
    """MOD-15364: BG query holds the spec read lock; a concurrent writer
    parks on the spec write lock and blocks the main thread. With the fix,
    the BG worker releases the read lock on the BG thread (inside
    `AREQ_Execute`) prior to `RedisModule_UnblockClient`,
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


# QueryRequest teardown and buffered-reply delivery must survive the spec being
# freed after serialization but before the worker publishes completion.
@skip(cluster=True)
def test_buffered_reply_after_index_dropped_mid_cycle():
    """A worker-buffered reply survives index deletion before unblocking."""
    env = Env(moduleArgs='WORKERS 1 TIMEOUT 0 _FREE_RESOURCE_ON_THREAD FALSE')
    skipIfNoEnableAssert(env)
    env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'text', 'TEXT').ok()
    n_docs = 5
    for i in range(n_docs):
        env.cmd('HSET', f'doc{i}', 'text', 'hello world')
    waitForIndex(env, 'idx')

    point = 'BeforeBackgroundReplyUnblock'
    env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', point).ok()
    try:
        result = {}
        def run_query():
            try:
                result['reply'] = env.getConnection().execute_command(
                    'FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@text')
            except Exception as e:
                result['error'] = e

        t = threading.Thread(target=run_query, daemon=True)
        t.start()

        wait_for_blocked_query_client(env, 'FT.AGGREGATE')
        wait_for_condition(
            lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', point) == 1,
                     {'paused': env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', point)}),
            'Timeout while waiting for the worker to pause before publishing the reply')

        # The worker released its execution reference; DROPINDEX may free the
        # spec before the buffered reply is delivered and QueryRequest is freed.
        env.expect('FT.DROPINDEX', 'idx').ok()
    finally:
        env.expect(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point).ok()

    t.join(timeout=10)
    env.assertFalse(t.is_alive(), message='query thread should have finished')
    env.assertNotIn('error', result,
                    message=f"query failed: {result.get('error')}")
    # [total, row...] — every row buffered before the drop is delivered.
    env.assertEqual(len(result['reply']) - 1, n_docs)
    for row in result['reply'][1:]:
        env.assertEqual(row, ['text', 'hello world'])


@skip(cluster=True)
def test_buffered_profile_reply_after_index_dropped_mid_cycle():
    """Buffered PROFILE results and request teardown survive index deletion."""
    env = Env(
        protocol=3,
        moduleArgs='WORKERS 1 TIMEOUT 0 _FREE_RESOURCE_ON_THREAD FALSE')
    skipIfNoEnableAssert(env)
    env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'text', 'TEXT').ok()
    n_docs = 5
    for i in range(n_docs):
        env.cmd('HSET', f'doc{i}', 'text', 'hello world')
    waitForIndex(env, 'idx')

    point = 'BeforeBackgroundReplyUnblock'
    env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', point).ok()
    try:
        result = {}

        def run_query():
            try:
                result['reply'] = env.getConnection().execute_command(
                    'FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', 'hello',
                    'LOAD', '1', '@text')
            except Exception as e:
                result['error'] = e

        t = threading.Thread(target=run_query, daemon=True)
        t.start()

        wait_for_blocked_query_client(env, 'FT.PROFILE')
        wait_for_condition(
            lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', point) == 1,
                     {'paused': env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', point)}),
            'Timeout while waiting for the worker to pause before publishing the profile reply')

        env.expect('FT.DROPINDEX', 'idx').ok()
    finally:
        env.expect(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point).ok()

    t.join(timeout=10)
    env.assertFalse(t.is_alive(), message='profile query thread should have finished')
    env.assertNotIn('error', result,
                    message=f"profile query failed: {result.get('error')}")

    reply = result['reply']
    env.assertContains('Results', reply, message=reply)
    env.assertContains('Profile', reply, message=reply)
    env.assertEqual(reply['Results']['total_results'], n_docs, message=reply)
    env.assertEqual(len(reply['Results']['results']), n_docs, message=reply)


@skip(cluster=True)
def test_hybrid_reply_after_index_dropped_mid_cycle():
    """Buffered HYBRID reply survives dropping the index after worker encoding."""
    env = Env(
        protocol=3,
        moduleArgs='WORKERS 1 TIMEOUT 0 _FREE_RESOURCE_ON_THREAD FALSE')
    skipIfNoEnableAssert(env)  # QUERY_CONTROLLER pause hooks are ENABLE_ASSERT-only
    env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail').ok()

    query_vec = _setup_hybrid_index(env)
    n_docs = 100
    query = [
        'FT.HYBRID', 'hybrid_idx',
        'SEARCH', '*',
        'VSIM', '@embedding', '$BLOB',
        'KNN', '2', 'K', '10000',
        'COMBINE', 'RRF', '2', 'WINDOW', '10000',
        'PARAMS', '2', 'BLOB', query_vec,
        'LIMIT', '0', '10000',
    ]

    point = 'BeforeBackgroundReplyUnblock'
    env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', point).ok()
    try:
        result = {}

        def run_query():
            try:
                result['reply'] = env.getConnection().execute_command(*query)
            except Exception as e:
                result['error'] = e

        t = threading.Thread(target=run_query, daemon=True)
        t.start()

        wait_for_blocked_query_client(env, 'FT.HYBRID')
        wait_for_condition(
            lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', point), {}),
            'Timeout while waiting for the hybrid worker to pause before unblocking')

        env.expect('FT.DROPINDEX', 'hybrid_idx').ok()
    finally:
        env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point)

    t.join(timeout=10)
    env.assertFalse(t.is_alive(), message='hybrid query thread should have finished')
    env.assertNotIn('error', result,
                    message=f"hybrid query failed: {result.get('error')}")

    reply = result['reply']
    env.assertEqual(reply['total_results'], n_docs, message=reply)
    env.assertEqual(len(reply['results']), n_docs, message=reply)
    env.assertEqual(reply.get('warnings', []), [], message=reply)


@contextmanager
def _preserve_config(env, *names):
    values = {name: to_dict(env.cmd('CONFIG', 'GET', name))[name] for name in names}
    try:
        yield
    finally:
        for name, value in values.items():
            env.expect('CONFIG', 'SET', name, value).ok()


def _background_fail_cursor_dispatch_switches(protocol):
    env = Env(protocol=protocol, moduleArgs='WORKERS 1 TIMEOUT 0')
    with _preserve_config(env, 'search-workers', 'search-on-timeout',
                         'search-_max-foreground-timeout-limit'):
        # Disable the foreground cap while switching between inline and worker reads.
        env.expect('CONFIG', 'SET', 'search-_max-foreground-timeout-limit', 0).ok()
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE').ok()
        for n in range(8):
            env.cmd('HSET', f'doc:{n}', 'n', n)

        def values(chunk):
            if protocol == 3:
                return [int(row['extra_attributes']['n']) for row in chunk['results']]
            return [int(to_dict(row)['n']) for row in chunk[1:]]

        # The global policy changes after creation. Each cursor must retain its
        # captured policy while moving between background and inline dispatches.
        cases = [('FAIL', 'RETURN', 1), ('FAIL', 'RETURN', 0),
                 ('RETURN', 'FAIL', 1)]
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
    """RESP2 cursors retain their policy across inline and worker reads."""
    _background_fail_cursor_dispatch_switches(2)


@skip(cluster=True)
def test_background_fail_cursor_dispatch_resp3():
    """RESP3 cursors retain their policy across inline and worker reads."""
    _background_fail_cursor_dispatch_switches(3)


def _background_hybrid_query(query_vec, timeout=0):
    return ['FT.HYBRID', 'hybrid_idx', 'SEARCH', '*',
            'VSIM', '@embedding', '$BLOB', 'KNN', 2, 'K', 100,
            'COMBINE', 'RRF', 2, 'WINDOW', 100,
            'LOAD', 1, '@name', 'SORTBY', 2, '@name', 'ASC',
            'PARAMS', 2, 'BLOB', query_vec, 'TIMEOUT', timeout, 'LIMIT', 0, 100]


def _hybrid_reply_without_timing(env, reply, profile=False):
    if isinstance(reply, dict):
        reply = dict(reply)
        if profile:
            env.assertTrue(bool(reply.pop('Profile')), message=reply)
        env.assertGreaterEqual(float(reply.pop('execution_time')), 0, message=reply)
    else:
        if profile:
            env.assertTrue(bool(reply[-1]), message=reply)
            reply = reply[:-1]
        reply = to_dict(reply)
        env.assertGreaterEqual(float(reply.pop('execution_time')), 0, message=reply)
    return reply


def _compare_background_fail_replies(protocol):
    env = Env(protocol=protocol, moduleArgs='WORKERS 1 ON_TIMEOUT FAIL DEFAULT_DIALECT 2')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'text', 'TEXT', 'n', 'NUMERIC', 'SORTABLE').ok()
    for n in range(12):
        env.cmd('HSET', f'doc:{n:02}', 'text', 'hello redis', 'n', n,
                'binary', 'prefix\x00suffix\r\n', 'wide', 'w' * 4096)
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
    with _preserve_config(env, 'search-workers'):
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
                env.assertEqual(actual['Results'], expected['Results'], message=actual)
                env.assertTrue(bool(actual['Profile']), message=actual)
            else:
                env.assertEqual(actual[0], expected[0], message=actual)
                env.assertTrue(bool(actual[1]), message=actual)
            env.assertTrue(env.cmd('PING'))

    query_vec = _setup_hybrid_index(env)
    with _preserve_config(env, 'search-workers'):
        for profile in (False, True):
            command = _background_hybrid_query(query_vec)
            if profile:
                command = ['FT.PROFILE', command[1], 'HYBRID', 'QUERY', *command[2:]]
            env.expect(config_cmd(), 'SET', 'WORKERS', 0).ok()
            expected = _hybrid_reply_without_timing(env, env.cmd(*command), profile)
            env.expect(config_cmd(), 'SET', 'WORKERS', 1).ok()
            actual = _hybrid_reply_without_timing(env, env.cmd(*command), profile)
            env.assertEqual(actual, expected)
            env.assertEqual(actual['total_results'], 100, message=actual)
            env.assertEqual(actual['warnings'], [], message=actual)
            env.expect(*command, 'APPLY', '@name + 1', 'AS', 'invalid').error().contains(
                'SEARCH_NUMERIC_VALUE_INVALID')


@skip(cluster=True)
def test_background_fail_reply_parity_resp2():
    """Worker FAIL replies match inline RESP2 results, including PROFILE."""
    _compare_background_fail_replies(2)


@skip(cluster=True)
def test_background_fail_reply_parity_resp3():
    """Worker FAIL replies match inline RESP3 results, including PROFILE."""
    _compare_background_fail_replies(3)


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
    env.assertEqual(stats['index_total'], 0, message=stats)


def _test_background_fail_late_expression_errors(protocol):
    env = Env(protocol=protocol,
              moduleArgs='WORKERS 1 ON_TIMEOUT FAIL DEFAULT_DIALECT 2')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'position', 'NUMERIC', 'SORTABLE').ok()
    for position, value in enumerate(('1', '2', 'not-a-number')):
        env.cmd('HSET', f'doc:{position}', 'position', position, 'value', value)

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
    with _preserve_config(env, 'search-on-oom'):
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
                    env.assertEqual(len(rows['results']), 1, message=rows)
                    first = rows['results'][0]['extra_attributes']
                else:
                    env.assertEqual(len(rows), 2, message=rows)
                    first = to_dict(rows[1])
                env.assertEqual(str(first['value']), '1', message=rows)
                _assert_background_fail_late_error(env, ['FT.CURSOR', 'READ', 'idx', cursor, 'COUNT', 2])
                env.expect('FT.CURSOR', 'READ', 'idx', cursor).error().contains('Cursor not found')


@skip(cluster=True)
def test_background_fail_late_errors_resp2():
    """Late expression errors replace complete RESP2 chunks and free cursors."""
    _test_background_fail_late_expression_errors(2)


@skip(cluster=True)
def test_background_fail_late_errors_resp3():
    """Late expression errors replace complete RESP3 chunks and free cursors."""
    _test_background_fail_late_expression_errors(3)


def _background_fail_cursor_total(env, idx='idx'):
    # Do not silently accept missing stats: cleanup is part of the assertion.
    return int(to_dict(to_dict(env.cmd('FT.INFO', idx))['cursor_stats'])['global_total'])


def _wait_for_background_fail_workers(env):
    def idle():
        stats = to_dict(env.cmd(debug_cmd(), 'WORKERS', 'STATS'))
        return (stats['numJobsInProgress'] == 0 and stats['totalPendingJobs'] == 0, stats)
    wait_for_condition(idle, 'Timed-out encoding worker did not finish', timeout=5)


def _exercise_background_fail_queued_cleanup(drop_index):
    for protocol in (2, 3):
        env = Env(protocol=protocol, moduleArgs='WORKERS 1 TIMEOUT 0 ON_TIMEOUT FAIL NOGC')
        skipIfNoEnableAssert(env)
        # Keep an index for observing global cursor cleanup after idx is dropped.
        env.expect('FT.CREATE', 'observer', 'PREFIX', 1, 'observer:',
                   'SCHEMA', 'name', 'TEXT').ok()

        for kind in ('search', 'cursor_initial', 'cursor_read', 'hybrid'):
            env.expect('FT.CREATE', 'idx', 'PREFIX', 1, 'doc:',
                       'SCHEMA', 'name', 'TEXT', 'SORTABLE', 'embedding', 'VECTOR', 'FLAT', 6,
                       'TYPE', 'FLOAT32', 'DIM', 2, 'DISTANCE_METRIC', 'L2').ok()
            for i in range(4):
                env.cmd('HSET', f'doc:{i}', 'name', f'hello{i}', 'embedding',
                        np.array([float(i), float(i)], dtype=np.float32).tobytes())
            baseline = _background_fail_cursor_total(env, 'observer')
            timeout = 0
            aggregate = ['FT.AGGREGATE', 'idx', '*', 'TIMEOUT', timeout,
                         'WITHCURSOR', 'COUNT', 1]
            cursor_id = None
            if kind == 'search':
                command = ['FT.SEARCH', 'idx', '*', 'TIMEOUT', timeout]
            elif kind == 'hybrid':
                command = _background_hybrid_query(np.zeros(2, dtype=np.float32).tobytes())
                command[1] = 'idx'
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

            freed = _get_blocked_request_onfree_count(env)
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
                    env.expect('FT.DROPINDEX', 'idx', 'DD').ok()
                else:
                    env.expect('CLIENT', 'UNBLOCK', client_id, 'TIMEOUT').equal(1)
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
                            and _background_fail_cursor_total(env, 'observer') == baseline
                            and _get_blocked_request_onfree_count(env) == freed + 1, stats)

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
                env.expect('FT.DROPINDEX', 'idx', 'DD').ok()


@skip(cluster=True)
def test_background_fail_timeout_while_queued():
    """Timeout replies while work is queued; later pickup releases the request and cursor."""
    _exercise_background_fail_queued_cleanup(False)


@skip(cluster=True)
def test_background_fail_index_dropped_while_queued():
    """Queued queries and cursor reads report index deletion and release their resources."""
    _exercise_background_fail_queued_cleanup(True)


def _exercise_background_fail_timeout(point, real_timeout=False,
                                     kinds=('search', 'aggregate', 'profile_search',
                                            'profile_aggregate', 'cursor_initial', 'cursor_read',
                                            'hybrid', 'profile_hybrid')):
    for protocol in (2, 3):
        env = Env(protocol=protocol, moduleArgs='WORKERS 1 TIMEOUT 0 ON_TIMEOUT FAIL NOGC')
        skipIfNoEnableAssert(env)
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT', 'SORTABLE').ok()
        for i in range(8):
            env.cmd('HSET', f'doc:{i}', 'name', f'hello{i}')

        query_vec = _setup_hybrid_index(env)
        for kind in kinds:
            baseline = _background_fail_cursor_total(env)
            # The timer test holds the worker at a known point until Redis expires it.
            timeout = 5000 if real_timeout else 0
            aggregate = ['FT.AGGREGATE', 'idx', '*', 'TIMEOUT', timeout, 'LOAD', 1, '@name']
            cursor_id = None
            if kind in ('search', 'profile_search'):
                command = ['FT.SEARCH', 'idx', '*', 'TIMEOUT', timeout]
            elif kind in ('hybrid', 'profile_hybrid'):
                command = _background_hybrid_query(query_vec, timeout)
            elif kind in ('aggregate', 'profile_aggregate'):
                command = aggregate
            elif kind == 'cursor_initial':
                command = [*aggregate, 'WITHCURSOR', 'COUNT', 2]
            else:
                _, cursor_id = env.cmd(*aggregate, 'WITHCURSOR', 'COUNT', 2)
                env.assertNotEqual(cursor_id, 0)
                command = ['FT.CURSOR', 'READ', 'idx', cursor_id, 'COUNT', 2]
            if kind.startswith('profile_'):
                command = ['FT.PROFILE', command[1], command[0][3:], 'QUERY', *command[2:]]

            original = env.getConnection().connection_pool
            pool = ConnectionPool(connection_class=original.connection_class,
                                  **dict(original.connection_kwargs, retry=Retry(NoBackoff(), 0),
                                         socket_timeout=15))
            client = Redis(connection_pool=pool, single_connection_client=True)
            client_id = client.client_id()
            results, errors = [], []

            def query():
                try:
                    results.append(client.execute_command(*command))
                except Exception as error:
                    errors.append(error)

            free_count = _get_blocked_request_onfree_count(env)
            query_count = env.cmd('INFO', 'MODULES')['search_total_query_commands']
            thread = threading.Thread(target=query, daemon=True)
            env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', point).ok()
            try:
                thread.start()
                wait_for_condition(
                    lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', point),
                             {'results': results, 'errors': errors}),
                    f'{kind} did not reach {point}', timeout=5)
                if not real_timeout:
                    env.expect('CLIENT', 'UNBLOCK', client_id, 'TIMEOUT').equal(1)
                thread.join(timeout=10)
                env.assertFalse(thread.is_alive(), message=f'{kind}: timeout waited for worker')
                env.assertEqual(results, [])
                env.assertEqual(len(errors), 1, message=errors)
                env.assertTrue(isinstance(errors[0], ResponseError), message=errors)
                env.assertContains('Timeout limit was reached', str(errors[0]))
                env.assertTrue(client.ping())
                env.assertEqual(client.client_id(), client_id)
                # MT replied, but the blocked-client cycle still owns the active worker.
                env.expect(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', point).equal(1)
                env.assertEqual(_get_blocked_request_onfree_count(env), free_count)
                if point in ('AfterBackgroundReplyEncode', 'BeforeBackgroundReplyUnblock'):
                    env.assertEqual(env.cmd('INFO', 'MODULES')['search_total_query_commands'],
                                    query_count + 1)
                env.expect(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point).ok()
                _wait_for_background_fail_workers(env)
                wait_for_condition(
                    lambda: (_get_blocked_request_onfree_count(env) == free_count + 1 and
                             _background_fail_cursor_total(env) == baseline, {}),
                    f'{kind}: request or cursor did not finish cleanup', timeout=5)
                if point in ('AfterBackgroundReplyEncode', 'BeforeBackgroundReplyUnblock'):
                    env.assertEqual(env.cmd('INFO', 'MODULES')['search_total_query_commands'],
                                    query_count + 1)
                if cursor_id is not None:
                    env.expect('FT.CURSOR', 'READ', 'idx', cursor_id).error().contains('Cursor not found')
                env.assertTrue(client.ping())
                env.assertEqual(client.client_id(), client_id)
            finally:
                env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point)
                thread.join(timeout=10)
                _wait_for_background_fail_workers(env)
                client.close()
                pool.disconnect()
                env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')


@skip(cluster=True)
def test_timeout_before_background_reply():
    """MT timeout wins before any chunk bytes are encoded."""
    _exercise_background_fail_timeout('BeforeBackgroundReplyEncode')


@skip(cluster=True)
def test_timeout_during_background_reply():
    """A partially encoded SEARCH/AGGREGATE/PROFILE/cursor reply is discarded."""
    _exercise_background_fail_timeout('DuringBackgroundReplyEncode')


@skip(cluster=True)
def test_timeout_after_background_reply():
    """Completed reply bytes remain private until the worker unblocks."""
    _exercise_background_fail_timeout('AfterBackgroundReplyEncode')


@skip(cluster=True)
def test_timeout_after_cursor_pause_decision():
    """OnFree must override a worker's PAUSE decision when its reply was discarded."""
    _exercise_background_fail_timeout('BeforeBackgroundReplyUnblock',
                                     kinds=('cursor_initial', 'cursor_read', 'hybrid', 'profile_hybrid'))


@skip(cluster=True)
def test_background_reply_real_deadline():
    """Initial and reused cursor cycles retain the Redis timer through encoding."""
    _exercise_background_fail_timeout('DuringBackgroundReplyEncode', real_timeout=True,
                                     kinds=('cursor_initial', 'cursor_read', 'hybrid', 'profile_hybrid'))


@skip(cluster=False)
def test_internal_background_fail_serialization(env):
    """Cluster shard commands serialize on their workers before coordinator completion."""
    skipIfNoEnableAssert(env)
    verify_shard_init(env)
    shards = [env.getConnection(i) for i in range(1, env.shardsCount + 1)]
    target = non_coord_shard_conns(env)[0]
    policies = [c.execute_command('CONFIG', 'GET', 'search-on-timeout') for c in shards]
    workers = [c.execute_command('CONFIG', 'GET', 'search-workers') for c in shards]
    point = 'AfterBackgroundReplyEncode'
    query_vec = _setup_hybrid_index(env)
    env.expect('FT.CREATE', 'idx', 'PREFIX', 1, '{doc}:',
               'SCHEMA', 'name', 'TEXT').ok()
    getConnectionByEnv(env).execute_command('HSET', '{doc}:1', 'name', 'hello')
    try:
        for c in shards:
            c.execute_command('CONFIG', 'SET', 'search-on-timeout', 'fail')
            c.execute_command('CONFIG', 'SET', 'search-workers', 1)
        for command in (['FT.SEARCH', 'idx', '*', 'NOCONTENT', 'TIMEOUT', 0],
                        ['FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@name', 'TIMEOUT', 0],
                        _background_hybrid_query(query_vec)):
            expected = env.cmd(*command)
            results, errors = [], []

            def query():
                try:
                    results.append(env.getConnection().execute_command(*command))
                except Exception as error:
                    errors.append(error)

            read_point = 'BeforeCursorReadSendChunk' if command[0] == 'FT.HYBRID' else None
            target.execute_command(debug_cmd(), 'SYNC_POINT', 'ARM', read_point or point)
            thread = threading.Thread(target=query, daemon=True)
            try:
                thread.start()
                if read_point:
                    wait_for_condition(
                        lambda: (target.execute_command(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', read_point), {}),
                        'HYBRID did not dispatch its internal cursor read', timeout=5)
                    target.execute_command(debug_cmd(), 'SYNC_POINT', 'ARM', point)
                    target.execute_command(debug_cmd(), 'SYNC_POINT', 'SIGNAL', read_point)
                wait_for_condition(
                    lambda: (target.execute_command(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', point), {}),
                    'Internal shard command did not serialize on its worker', timeout=5)
                env.assertTrue(thread.is_alive())
                target.execute_command(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point)
                thread.join(timeout=10)
                env.assertFalse(thread.is_alive())
                env.assertEqual(errors, [])
                if command[0] == 'FT.HYBRID':
                    env.assertEqual(_hybrid_reply_without_timing(env, results[0]),
                                    _hybrid_reply_without_timing(env, expected))
                else:
                    env.assertEqual(results, [expected])
            finally:
                if read_point:
                    target.execute_command(debug_cmd(), 'SYNC_POINT', 'SIGNAL', read_point)
                target.execute_command(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point)
                thread.join(timeout=10)
                target.execute_command(debug_cmd(), 'SYNC_POINT', 'CLEAR')
        # Pipeline construction fails on the worker before a cursor mapping exists.
        _, slots_data = get_shard_slot_ranges(env)[0]
        env.cmd('DEBUG', 'MARK-INTERNAL-CLIENT')
        env.expect('_FT.HYBRID', 'hybrid_idx', 'SEARCH', '*',
                   'VSIM', '@embedding', '$BLOB', 'PARAMS', 2, 'BLOB', b'x',
                   'TIMEOUT', 0, '_SLOTS_INFO', slots_data,
                   '_COORD_DISPATCH_TIME', 0).error().contains('query vector blob size (1)')
    finally:
        for c, policy, worker in zip(shards, policies, workers):
            c.execute_command('CONFIG', 'SET', 'search-on-timeout', to_dict(policy)['search-on-timeout'])
            c.execute_command('CONFIG', 'SET', 'search-workers', to_dict(worker)['search-workers'])


def _new_coord_background_fail_env(protocol):
    # FAIL workers and explicit protocols cover both encoders and deferred WITHCOUNT.
    env = Env(protocol=protocol,
              moduleArgs='WORKERS 1 TIMEOUT 0 ON_TIMEOUT FAIL DEFAULT_DIALECT 2 NOGC')
    for shard in range(1, env.shardsCount + 1):
        verify_shard_init(env.getConnection(shard))
    env.expect('FT.CREATE', 'idx', 'PREFIX', 1, '{doc}:', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    for n in range(8):
        conn.execute_command('HSET', f'{{doc}}:{n}', 'n', n,
                             'value', 'invalid' if n == 3 else str(n),
                             'payload', 'prefix\x00suffix\r\n' + 'w' * 4096)
    return env


def _coord_background_aggregate(timeout=0, withcount=False):
    return ['FT.AGGREGATE', 'idx', '*', *(['WITHCOUNT'] if withcount else []),
            'TIMEOUT', timeout, 'LOAD', 3, '@n', '@value', '@payload',
            'SORTBY', 2, '@n', 'ASC']


def _wait_for_coord_background_workers(env, jobs_done):
    def idle():
        stats = getCoordThpoolStats(env)
        return (stats['totalJobsDone'] > jobs_done and stats['totalPendingJobs'] == 0, stats)
    wait_for_condition(idle, 'Coordinator encoding worker did not finish', timeout=5)


def _exercise_coord_background_timeout(protocol, real_timeout=False):
    env = _new_coord_background_fail_env(protocol)
    skipIfNoEnableAssert(env)
    query_vec = _setup_hybrid_index(env)
    points = (['DuringCoordBackgroundReplyEncode'] if real_timeout else
              ['BeforeCoordBackgroundReplyEncode', 'DuringCoordBackgroundReplyEncode',
               'AfterCoordBackgroundReplyEncode', 'BeforeCoordBackgroundReplyUnblock'])
    for point in points:
        for kind in ('aggregate', 'profile', 'withcount', 'cursor_initial', 'cursor_read',
                     'withcount_cursor_initial', 'withcount_cursor_read', 'hybrid', 'profile_hybrid'):
            # The deadline case pins encoding until the real Redis timer expires.
            timeout = 5000 if real_timeout else 0
            command = _coord_background_aggregate(timeout, withcount=kind.startswith('withcount'))
            baseline = _background_fail_cursor_total(env)
            cursor_id = None
            if kind in ('hybrid', 'profile_hybrid'):
                command = _background_hybrid_query(query_vec, timeout)
                if kind == 'profile_hybrid':
                    command = ['FT.PROFILE', command[1], 'HYBRID', 'QUERY', *command[2:]]
            elif kind == 'profile':
                command = ['FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', *command[2:]]
            elif kind.endswith('cursor_initial'):
                command += ['WITHCURSOR', 'COUNT', 2]
            elif kind.endswith('cursor_read'):
                _, cursor_id = env.cmd(*command, 'WITHCURSOR', 'COUNT', 2)
                env.assertNotEqual(cursor_id, 0)
                command = ['FT.CURSOR', 'READ', 'idx', cursor_id, 'COUNT', 2]
            before_info = info_modules_to_dict(env)
            base_errors = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])
            base_reply_errors = int(before_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_REPLY_METRIC])
            original = env.getConnection().connection_pool
            pool = ConnectionPool(connection_class=original.connection_class,
                                  **dict(original.connection_kwargs,
                                         retry=Retry(NoBackoff(), 0), socket_timeout=15))
            client = Redis(connection_pool=pool, single_connection_client=True)
            client_id = client.client_id()
            results, errors = [], []

            def query():
                try:
                    results.append(client.execute_command(*command))
                except Exception as error:
                    errors.append(error)

            jobs_done = getCoordThpoolStats(env)['totalJobsDone']
            thread = threading.Thread(target=query, daemon=True)
            env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', point).ok()
            try:
                thread.start()
                wait_for_condition(
                    lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', point),
                             {'results': results, 'errors': errors}),
                    f'{kind} did not reach {point}', timeout=5)
                jobs_done = getCoordThpoolStats(env)['totalJobsDone']
                freed = _get_blocked_request_onfree_count(env)
                if not real_timeout:
                    env.expect('CLIENT', 'UNBLOCK', client_id, 'TIMEOUT').equal(1)
                thread.join(timeout=10)
                env.assertFalse(thread.is_alive(), message=f'{kind}: timeout waited for encoding')
                env.assertEqual(results, [])
                env.assertEqual(len(errors), 1, message=errors)
                env.assertTrue(isinstance(errors[0], ResponseError), message=errors)
                env.assertContains(TIMEOUT_ERROR, str(errors[0]))
                env.assertTrue(client.ping())
                env.assertEqual(client.client_id(), client_id)
                env.expect(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', point).equal(1)
                env.expect(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point).ok()
                _wait_for_coord_background_workers(env, jobs_done)
                wait_for_condition(
                    lambda: (_get_blocked_request_onfree_count(env) > freed and
                             _background_fail_cursor_total(env) == baseline, {}),
                    f'{kind}: request or cursor was not freed', timeout=5)
                after_info = info_modules_to_dict(env)
                env.assertEqual(int(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC]),
                                base_errors + 1)
                env.assertEqual(int(after_info[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_REPLY_METRIC]),
                                base_reply_errors + 1)
                if cursor_id is not None:
                    env.expect('FT.CURSOR', 'READ', 'idx', cursor_id).error().contains('Cursor not found')
                env.assertTrue(client.ping())
                env.assertEqual(client.client_id(), client_id)
            finally:
                env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point)
                thread.join(timeout=10)
                _wait_for_coord_background_workers(env, jobs_done)
                client.close()
                pool.disconnect()
                env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')


@skip(cluster=False)
def test_coord_background_fail_timeout_resp2():
    """Discard RESP2 replies at encoding boundaries and after cursor disposition is recorded."""
    _exercise_coord_background_timeout(2)


@skip(cluster=False)
def test_coord_background_fail_timeout_resp3():
    """Discard RESP3 replies at encoding boundaries and after cursor disposition is recorded."""
    _exercise_coord_background_timeout(3)


@skip(cluster=False)
def test_coord_background_fail_deadline_resp2():
    """Keep the real blocked-client deadline active during RESP2 coordinator encoding."""
    _exercise_coord_background_timeout(2, real_timeout=True)


@skip(cluster=False)
def test_coord_background_fail_deadline_resp3():
    """Keep the real blocked-client deadline active during RESP3 coordinator encoding."""
    _exercise_coord_background_timeout(3, real_timeout=True)


def _compare_coord_background_fail_replies(protocol):
    env = _new_coord_background_fail_env(protocol)
    with _preserve_config(env, 'search-on-timeout'):
        for timeout in (0, 10000):
            commands = [
                _coord_background_aggregate(timeout),
                _coord_background_aggregate(timeout, withcount=True),
                ['FT.AGGREGATE', 'idx', '*', 'TIMEOUT', timeout,
                 'LOAD', 1, '@n', 'GROUPBY', 0, 'REDUCE', 'COUNT', 0, 'AS', 'count'],
                ['FT.AGGREGATE', 'idx', '@n:[100 200]', 'TIMEOUT', timeout],
                ['FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY',
                 *_coord_background_aggregate(timeout)[2:]],
            ]
            for command in commands:
                env.expect(config_cmd(), 'SET', 'ON_TIMEOUT', 'RETURN').ok()
                expected = env.cmd(*command)
                env.expect(config_cmd(), 'SET', 'ON_TIMEOUT', 'FAIL').ok()
                actual = env.cmd(*command)
                if command[0] == 'FT.PROFILE':
                    profile = actual['Profile'] if protocol == 3 else actual[1]
                    env.assertTrue(bool(profile), message=actual)
                    expected = expected['Results'] if protocol == 3 else expected[0]
                    actual = actual['Results'] if protocol == 3 else actual[0]
                env.assertEqual(actual, expected, message=str(command))
                env.assertTrue(env.cmd('PING'))

            for withcount in (False, True):
                baseline = _background_fail_cursor_total(env)
                chunk, cursor = env.cmd(*_coord_background_aggregate(timeout, withcount),
                                       'WITHCURSOR', 'COUNT', 2)
                values = []
                while True:
                    rows = ([row['extra_attributes'] for row in chunk['results']] if protocol == 3
                            else [to_dict(row) for row in chunk[1:]])
                    values.extend(int(row['n']) for row in rows)
                    if not cursor:
                        break
                    chunk, cursor = env.cmd('FT.CURSOR', 'READ', 'idx', cursor, 'COUNT', 2)
                env.assertEqual(values, list(range(8)))
                env.assertEqual(_background_fail_cursor_total(env), baseline)

        # GROUPBY keeps expression evaluation after coordinator reduction. A late
        # invalid value must replace earlier successful rows in the same chunk.
        query = ['FT.AGGREGATE', 'idx', '*', 'TIMEOUT', 0, 'LOAD', 1, '@value',
                 'GROUPBY', 1, '@n', 'REDUCE', 'FIRST_VALUE', 1, '@value', 'AS', 'value',
                 'SORTBY', 2, '@n', 'ASC']
        for expression in (['APPLY', '@value + 1', 'AS', 'incremented'],
                           ['FILTER', '(@value + 1) > 0']):
            command = query + expression
            _assert_background_fail_late_error(env, command)
            _assert_background_fail_late_error(env, command + ['WITHCURSOR', 'COUNT', 8])
            _, cursor = env.cmd(*command, 'WITHCURSOR', 'COUNT', 2)
            env.assertNotEqual(cursor, 0)
            _assert_background_fail_late_error(env, ['FT.CURSOR', 'READ', 'idx', cursor, 'COUNT', 2])
            env.expect('FT.CURSOR', 'READ', 'idx', cursor).error().contains('Cursor not found')

        env.expect('FT.AGGREGATE', 'idx', '*', 'UNKNOWN_OPTION').error().contains('Unknown argument')
        env.expect('FT.AGGREGATE', 'idx', '@n:[').error().contains('Syntax error')
        _, cursor = env.cmd(*_coord_background_aggregate(), 'WITHCURSOR', 'COUNT', 2)
        env.expect('FT.CURSOR', 'READ', 'idx', cursor, 'COUNT', 'invalid').error().contains('Bad value for COUNT')
        env.expect('FT.CURSOR', 'DEL', 'idx', cursor).ok()
        env.assertTrue(env.cmd('PING'))

    query_vec = _setup_hybrid_index(env)
    with _preserve_config(env, 'search-on-timeout'):
        for profile in (False, True):
            command = _background_hybrid_query(query_vec)
            if profile:
                command = ['FT.PROFILE', command[1], 'HYBRID', 'QUERY', *command[2:]]
            env.expect(config_cmd(), 'SET', 'ON_TIMEOUT', 'RETURN').ok()
            expected = _hybrid_reply_without_timing(env, env.cmd(*command), profile)
            env.expect(config_cmd(), 'SET', 'ON_TIMEOUT', 'FAIL').ok()
            actual = _hybrid_reply_without_timing(env, env.cmd(*command), profile)
            env.assertEqual(actual, expected)
            env.assertEqual(actual['total_results'], 100, message=actual)
            env.assertEqual(actual['warnings'], [], message=actual)
            env.expect(*command, 'APPLY', '@name + 1', 'AS', 'invalid').error().contains(
                'SEARCH_NUMERIC_VALUE_INVALID')

        # Parsing now replies directly from the coordinator worker too.
        env.expect(*_background_hybrid_query(query_vec), 'UNKNOWN_OPTION').error().contains(
            'Unknown')


@skip(cluster=False)
def test_coord_background_fail_reply_parity_resp2():
    """Preserve RESP2 counts, cursor chunks, profiles, and atomic late errors."""
    _compare_coord_background_fail_replies(2)


@skip(cluster=False)
def test_coord_background_fail_reply_parity_resp3():
    """Preserve RESP3 counts, cursor chunks, profiles, and atomic late errors."""
    _compare_coord_background_fail_replies(3)


def _coord_profile_timeout_before_fanout(protocol):
    env = _new_coord_background_fail_env(protocol)
    skipIfNoEnableAssert(env)
    point = 'BeforeRPNetStart'
    command = ['FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*', 'TIMEOUT', 0]
    baseline = _background_fail_cursor_total(env)
    freed = _get_blocked_request_onfree_count(env)
    original = env.getConnection().connection_pool
    pool = ConnectionPool(connection_class=original.connection_class,
                          **dict(original.connection_kwargs,
                                 retry=Retry(NoBackoff(), 0), socket_timeout=10))
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
            'PROFILE did not pause before RPNet iterator creation', timeout=5)
        # The hook releases on timeout. The worker must finish profiling with
        # no MR iterator, even though Redis will discard the encoded reply.
        env.expect('CLIENT', 'UNBLOCK', client_id, 'TIMEOUT').equal(1)
        thread.join(timeout=5)
        env.assertFalse(thread.is_alive())
        env.assertEqual(results, [])
        env.assertEqual(len(errors), 1, message=errors)
        env.assertTrue(isinstance(errors[0], ResponseError), message=errors)
        env.assertContains(TIMEOUT_ERROR, str(errors[0]))
        wait_for_condition(
            lambda: (_get_blocked_request_onfree_count(env) == freed + 1 and
                     _background_fail_cursor_total(env) == baseline, {}),
            'PROFILE timeout before fanout did not release its request', timeout=5)
        env.assertTrue(client.ping())
        env.assertEqual(client.client_id(), client_id)
    finally:
        env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point)
        thread.join(timeout=10)
        client.close()
        pool.disconnect()
        env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')


@skip(cluster=False)
def test_coord_profile_timeout_before_fanout_resp2():
    """RESP2 PROFILE survives timeout before its RPNet iterator exists."""
    _coord_profile_timeout_before_fanout(2)


@skip(cluster=False)
def test_coord_profile_timeout_before_fanout_resp3():
    """RESP3 PROFILE survives timeout before its RPNet iterator exists."""
    _coord_profile_timeout_before_fanout(3)
