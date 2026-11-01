# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""The cluster coordinator's query memory checkpoints, driven by `_FT.DEBUG MEMORY_PRESSURE`.
Pressure is per process and the coordinator's process also hosts shard 1, so each test places the
pressure where only the coordinator checkpoint under test can see it: a forced ratio set after the
query passed the entry guard (parked at a sync point), or a calibrated `AT_CALL`."""

import itertools
import threading

from common import *
from redis import ResponseError

OOM_QUERY_ERROR = "Not enough memory available to execute the query"
COORD_OOM_WARNING = "Coordinator failed to execute the query due to insufficient memory"
SHARD_OOM_WARNING = "One or more shards failed to execute the query due to insufficient memory"
N_DOCS = 30
# A timeout far longer than the test, so the FAIL and RETURN-STRICT timers never fire.
SEARCH = ('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'LIMIT', 0, N_DOCS, 'TIMEOUT', 100000)
TIMEOUT_POLICIES = ('RETURN', 'FAIL', 'RETURN-STRICT')
OOM_POLICIES = ('return', 'fail', 'ignore')


def create_index(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    for i in range(N_DOCS):
        conn.execute_command('HSET', f'doc{i}', 'n', i)


def set_policies(env, timeout_policy, oom_policy):
    verify_command_OK_on_all_shards(env, config_cmd(), 'SET', 'ON_TIMEOUT', timeout_policy)
    allShards_change_oom_policy(env, oom_policy)


def reset_policies(env):
    set_policies(env, 'RETURN', 'return')


def run_in_thread(conn, query):
    """Starts `query` on `conn` in a thread; join it and read result[0]."""
    result = []

    def run():
        try:
            result.append(conn.execute_command(*query))
        except Exception as e:
            result.append(e)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread, result


def sync_point(conn, *args):
    return conn.execute_command(debug_cmd(), 'SYNC_POINT', *args)


def wait_for_sync_point(conn, name):
    wait_for_condition(lambda: (sync_point(conn, 'IS_WAITING', name) == 1, {}),
                       f'Timeout waiting for sync point {name}')


def coord_oom_counters(conn):
    info = conn.execute_command('INFO', 'MODULES')
    if not isinstance(info, dict):
        info = {line.split(':', 1)[0]: line.split(':', 1)[1]
                for line in info.splitlines() if ':' in line}
    return (int(info['search_coord_total_query_errors_oom']),
            int(info['search_coord_total_query_warnings_oom']))


def search_ids(env, res):
    if env.protocol == 3:
        return [r['id'] for r in res['results']]
    return res[1:]


def expect_clean_end(env, conn):
    """The query replied once and released its blocked client: the connection answers the next
    command with that command's reply, and the coordinator holds no blocked client."""
    env.assertEqual(conn.execute_command('PING'), True)
    env.assertEqual(conn.execute_command('INFO', 'CLIENTS')['blocked_clients'], 0)


def stopped_at_first_reply(env, coord, query):
    """Runs `query` on the coordinator with memory forced over the limit after the entry guard and
    before the fan-out, so its first shard reply stops it."""
    sync_point(coord, 'CLEAR')
    sync_point(coord, 'ARM', 'BeforeCoordFanout')
    try:
        thread, result = run_in_thread(coord, query)
        wait_for_sync_point(coord, 'BeforeCoordFanout')
        coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RATIO', 1)
        sync_point(coord, 'SIGNAL', 'BeforeCoordFanout')
        thread.join(timeout=30)
        env.assertFalse(thread.is_alive())
    finally:
        coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RESET')
        sync_point(coord, 'CLEAR')
    return result[0]


def _test_search_fanout_stop(env):
    skipIfNoEnableAssert(env)
    create_index(env)
    coord = env.getConnection(1)
    full = search_ids(env, coord.execute_command(*SEARCH))
    env.assertEqual(len(full), N_DOCS)
    try:
        for timeout_policy in TIMEOUT_POLICIES:
            for oom_policy in OOM_POLICIES:
                set_policies(env, timeout_policy, oom_policy)
                case = (timeout_policy, oom_policy)
                errors, warnings = coord_oom_counters(coord)
                res = stopped_at_first_reply(env, coord, SEARCH)
                if oom_policy == 'fail':
                    env.assertTrue(isinstance(res, ResponseError), message=(case, res))
                    env.assertContains(OOM_QUERY_ERROR, str(res), message=case)
                    env.assertEqual(coord_oom_counters(coord), (errors + 1, warnings), message=case)
                elif oom_policy == 'return':
                    # Every reply landed after the stop and was discarded.
                    env.assertEqual(search_ids(env, res), [], message=(case, res))
                    if env.protocol == 3:
                        env.assertEqual(res['warning'], [COORD_OOM_WARNING], message=(case, res))
                    env.assertEqual(coord_oom_counters(coord), (errors, warnings + 1), message=case)
                else:
                    env.assertEqual(sorted(search_ids(env, res)), sorted(full), message=(case, res))
                expect_clean_end(env, coord)
    finally:
        reset_policies(env)


@skip(cluster=False)
def test_search_fanout_stop_resp3():
    """A shard reply that finds memory over the limit stops the coordinator FT.SEARCH under every
    timeout policy: it and every later reply are discarded, and when the last one lands the
    reducer replies what it kept with the coordinator OOM warning, or the OOM error under ON_OOM
    FAIL, once, releasing the blocked client. ON_OOM IGNORE reads nothing."""
    _test_search_fanout_stop(Env(protocol=3))


@skip(cluster=False)
def test_search_fanout_stop_resp2():
    """As the RESP3 case; RESP2 has no warning channel, so ON_OOM RETURN replies the empty result
    only."""
    _test_search_fanout_stop(Env(protocol=2))


@skip(cluster=False)
def test_search_fanout_stop_keeps_earlier_replies():
    """A stop at the second shard reply keeps the first: the coordinator merges that shard's
    documents only and warns (ON_OOM RETURN), or fails (ON_OOM FAIL). The other shards are parked
    until the first reply has been read, which fixes the reply order; the coordinator process's
    own shard pipeline is calibrated out of the `AT_CALL` count."""
    # Workers run the shard queries, so a shard parked at a sync point still answers SYNC_POINT.
    env = Env(protocol=3, moduleArgs='WORKERS 1')
    skipIfNoEnableAssert(env)
    create_index(env)
    coord = env.getConnection(1)
    others = [env.getConnection(i) for i in range(2, env.shardsCount + 1)]
    local_docs = coord.execute_command('KEYS', 'doc*')

    def checkpoint_calls():
        return coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'STATUS')[5]

    # Calls up to and including the first shard reply's read: all fan-out calls (shard 1's pipeline,
    # the same count on every run of this query and data) but the other shard replies' reads.
    first_reply_read = coordinator_calls_after_fanout(env, coord, SEARCH, N_DOCS) - 1 - len(others)

    try:
        for oom_policy in ('return', 'fail'):
            allShards_change_oom_policy(env, oom_policy)
            for shard in others:
                sync_point(shard, 'CLEAR')
                sync_point(shard, 'ARM', 'BeforeFirstRead')
            coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', first_reply_read + 1)
            try:
                thread, result = run_in_thread(coord, SEARCH)
                for shard in others:
                    wait_for_sync_point(shard, 'BeforeFirstRead')
                wait_for_condition(lambda: (checkpoint_calls() == first_reply_read, {}),
                                   'Timeout waiting for the first shard reply')
                for shard in others:
                    sync_point(shard, 'SIGNAL', 'BeforeFirstRead')
                thread.join(timeout=30)
                env.assertFalse(thread.is_alive())
            finally:
                coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RESET')
                for shard in others:
                    sync_point(shard, 'CLEAR')
            res = result[0]
            if oom_policy == 'fail':
                env.assertTrue(isinstance(res, ResponseError), message=res)
                env.assertContains(OOM_QUERY_ERROR, str(res))
            else:
                # Shard 1's reply, the only one kept.
                env.assertEqual(sorted(search_ids(env, res)), sorted(local_docs), message=res)
                env.assertEqual(res['warning'], [COORD_OOM_WARNING], message=res)
            expect_clean_end(env, coord)
    finally:
        allShards_change_oom_policy(env, 'return')


def _test_entry_guard_counts_the_oom_warning_once(env):
    create_index(env)
    coord = env.getConnection(1)
    errors, warnings = coord_oom_counters(coord)
    coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RATIO', 1)
    try:
        res = coord.execute_command(*SEARCH)
    finally:
        coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RESET')
    env.assertEqual(search_ids(env, res), [], message=res)
    if env.protocol == 3:
        env.assertEqual(res['warning'], [COORD_OOM_WARNING], message=res)
    env.assertEqual(coord_oom_counters(coord), (errors, warnings + 1))


@skip(cluster=False)
def test_entry_guard_counts_the_oom_warning_once_resp3():
    """The coordinator FT.SEARCH entry guard under ON_OOM RETURN replies an empty result with the
    coordinator OOM warning, and counts it once."""
    _test_entry_guard_counts_the_oom_warning_once(Env(protocol=3))


@skip(cluster=False)
def test_entry_guard_counts_the_oom_warning_once_resp2():
    """As the RESP3 case, without the warning in the reply."""
    _test_entry_guard_counts_the_oom_warning_once(Env(protocol=2))


def _test_search_fanout_stop_after_shard_oom_warnings(env):
    if env.shardsCount < 3:
        env.skip()
    create_index(env)
    coord = env.getConnection(1)
    remote_shards = [env.getConnection(i) for i in range(2, env.shardsCount + 1)]
    local_docs = coord.execute_command('KEYS', 'doc*')

    def arm_remote_shards():
        # Their first checkpoint stops them: an empty reply with the shard OOM warning.
        for conn in remote_shards:
            conn.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', 1)

    def reset_all():
        run_command_on_all_shards(env, debug_cmd(), 'MEMORY_PRESSURE', 'RESET')

    # The fan-out's last call is the last shard reply's read.
    last_reply_read = coordinator_calls_after_fanout(env, coord, SEARCH, N_DOCS) - 1

    arm_remote_shards()
    coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', last_reply_read)
    errors, warnings = coord_oom_counters(coord)
    try:
        res = coord.execute_command(*SEARCH)
    finally:
        reset_all()
    # The last reply is discarded; of the two kept, at least one is a remote shard's warning reply.
    env.assertContains(sorted(search_ids(env, res)), [[], sorted(local_docs)], message=res)
    if env.protocol == 3:
        env.assertEqual(res['warning'], [SHARD_OOM_WARNING, COORD_OOM_WARNING], message=res)
    env.assertEqual(coord_oom_counters(coord), (errors, warnings + 2 if env.protocol == 3 else warnings + 1))
    expect_clean_end(env, coord)


@skip(cluster=False)
def test_search_fanout_stop_after_shard_oom_warnings_resp3():
    """A coordinator stop after shard replies that carry the shard OOM warning replies both
    warnings, the shard's first, and counts each once."""
    _test_search_fanout_stop_after_shard_oom_warnings(Env(protocol=3))


@skip(cluster=False)
def test_search_fanout_stop_after_shard_oom_warnings_resp2():
    """As the RESP3 case; RESP2 replies no warning but counts the coordinator stop."""
    _test_search_fanout_stop_after_shard_oom_warnings(Env(protocol=2))
def reducer_reads(env, oom_policy):
    """Memory reads of the coordinator FT.SEARCH reducer: one after each merged shard reply but the
    last, which only ON_OOM FAIL reads (its last chance to fail before replying)."""
    return env.shardsCount - (oom_policy != 'fail')


def reply_reads(oom_policy, rows):
    """Checkpoint calls of the reply: one before the rows and one per row, or under ON_OOM FAIL
    the single read of the reply callback."""
    return 1 if oom_policy == 'fail' else rows + 1


def coordinator_calls_after_fanout(env, coord, query, rows, oom_policy='return'):
    """The `AT_CALL` position of the first reducer read: past every checkpoint call of the
    coordinator process during the fan-out (shard 1's pipeline, the same count on every run of this
    query and data, and one read per shard reply), all of which happen before the reducer starts.
    One full run under the current policies counts them."""
    coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', 1 << 40)
    try:
        coord.execute_command(*query)
        calls = coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'STATUS')[5]
    finally:
        coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RESET')
    return calls - reducer_reads(env, oom_policy) - reply_reads(oom_policy, rows) + 1


def run_at_call(coord, n, query):
    coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', n)
    try:
        return coord.execute_command(*query)
    except ResponseError as e:
        return e
    finally:
        coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RESET')


def _test_search_reducer_and_reply_stop(env):
    create_index(env)
    coord = env.getConnection(1)
    full = search_ids(env, coord.execute_command(*SEARCH))
    env.assertEqual(len(full), N_DOCS)
    shard_docs = [sorted(env.getConnection(i).execute_command('KEYS', 'doc*'))
                  for i in range(1, env.shardsCount + 1)]
    k = 5
    try:
        for timeout_policy in TIMEOUT_POLICIES:
            for oom_policy in OOM_POLICIES:
                set_policies(env, timeout_policy, oom_policy)
                case = (timeout_policy, oom_policy)
                first_reducer_read = coordinator_calls_after_fanout(env, coord, SEARCH, N_DOCS,
                                                                    oom_policy)

                # The reducer stops after merging the first shard reply: that shard's documents.
                errors, warnings = coord_oom_counters(coord)
                res = run_at_call(coord, first_reducer_read, SEARCH)
                if oom_policy == 'fail':
                    env.assertTrue(isinstance(res, ResponseError), message=(case, res))
                    env.assertContains(OOM_QUERY_ERROR, str(res), message=case)
                    env.assertEqual(coord_oom_counters(coord), (errors + 1, warnings), message=case)
                elif oom_policy == 'return':
                    env.assertContains(sorted(search_ids(env, res)), shard_docs,
                                       message=(case, res))
                    if env.protocol == 3:
                        env.assertEqual(res['warning'], [COORD_OOM_WARNING], message=(case, res))
                    env.assertEqual(coord_oom_counters(coord), (errors, warnings + 1), message=case)
                else:
                    env.assertEqual(len(search_ids(env, res)), N_DOCS, message=(case, res))
                expect_clean_end(env, coord)

                if oom_policy == 'fail':
                    # The read after the last merged reply fails the complete result before the
                    # reply starts; the reply loop never truncates under FAIL.
                    res = run_at_call(coord, first_reducer_read + env.shardsCount - 1, SEARCH)
                    env.assertTrue(isinstance(res, ResponseError), message=(case, res))
                    env.assertContains(OOM_QUERY_ERROR, str(res), message=case)
                    expect_clean_end(env, coord)
                    continue

                # The reply loop stops after k rows (past the read before the rows).
                errors, warnings = coord_oom_counters(coord)
                res = run_at_call(coord, first_reducer_read + reducer_reads(env, oom_policy) + 1 + k,
                                  SEARCH)
                ids = search_ids(env, res)
                env.assertEqual(len(ids), N_DOCS if oom_policy == 'ignore' else k,
                                message=(case, res))
                env.assertTrue(set(ids) <= set(full), message=(case, res))
                stopped = oom_policy == 'return'
                if env.protocol == 3:
                    env.assertEqual(res['total_results'], N_DOCS, message=(case, res))
                    env.assertEqual(res['warning'], [COORD_OOM_WARNING] if stopped else [],
                                    message=(case, res))
                else:
                    env.assertEqual(res[0], N_DOCS, message=(case, res))
                env.assertEqual(coord_oom_counters(coord), (errors, warnings + stopped),
                                message=case)
                expect_clean_end(env, coord)
    finally:
        reset_policies(env)


@skip(cluster=False)
def test_search_reducer_and_reply_stop_resp3():
    """Memory read once per shard reply while the coordinator merges FT.SEARCH replies: a stop
    replies what the merge holds with the coordinator OOM warning, or the OOM error under ON_OOM
    FAIL, which also reads after the last reply. Memory read while replying (every row under test):
    under ON_OOM RETURN a stop ends the results early with the warning. Under every timeout
    policy."""
    _test_search_reducer_and_reply_stop(Env(protocol=3))


@skip(cluster=False)
def test_search_reducer_and_reply_stop_resp2():
    """As the RESP3 case; RESP2 has no warning channel."""
    _test_search_reducer_and_reply_stop(Env(protocol=2))


def _test_profile_search_fanout_stop(env):
    skipIfNoEnableAssert(env)
    create_index(env)
    coord = env.getConnection(1)
    query = ('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '*', 'NOCONTENT', 'LIMIT', 0, N_DOCS,
             'TIMEOUT', 100000)
    try:
        for oom_policy in ('return', 'fail'):
            allShards_change_oom_policy(env, oom_policy)
            res = stopped_at_first_reply(env, coord, query)
            if oom_policy == 'fail':
                env.assertTrue(isinstance(res, ResponseError), message=res)
                env.assertContains(OOM_QUERY_ERROR, str(res))
            elif env.protocol == 3:
                env.assertEqual(res['Results']['results'], [], message=res)
                env.assertEqual(res['Results']['warning'], [COORD_OOM_WARNING], message=res)
                env.assertEqual(res['Profile']['Shards'], [], message=res)
            else:
                env.assertEqual(res[0], [0], message=res)
            expect_clean_end(env, coord)
    finally:
        allShards_change_oom_policy(env, 'return')


@skip(cluster=False)
def test_profile_search_fanout_stop_resp3():
    """FT.PROFILE SEARCH shares the FT.SEARCH fan-out: a stop at the first shard reply discards
    every reply, so the profile replies no results and no shard profile, with the coordinator
    OOM warning (ON_OOM RETURN) or the OOM error (ON_OOM FAIL)."""
    _test_profile_search_fanout_stop(Env(protocol=3))


@skip(cluster=False)
def test_profile_search_fanout_stop_resp2():
    """As the RESP3 case; RESP2 has no warning channel."""
    _test_profile_search_fanout_stop(Env(protocol=2))


def _test_search_short_reply_reads_memory(env):
    skipIfNoEnableAssert(env)
    create_index(env)
    coord = env.getConnection(1)
    # A page past the last result has no row at all.
    empty_page = ('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'LIMIT', N_DOCS, 10, 'TIMEOUT', 100000)
    try:
        for oom_policy, query in itertools.product(('return', 'fail'), (SEARCH, empty_page)):
            case = (oom_policy, query)
            allShards_change_oom_policy(env, oom_policy)
            errors, warnings = coord_oom_counters(coord)
            # Pressure appears after the reducer's last read and before the reply, whose rows are
            # fewer than the read interval.
            setPauseBeforeReduce(env, PAUSE_AFTER_LAST_RESULT)
            try:
                thread, result = run_in_thread(coord, query)
                wait_for_condition(lambda: (getIsCoordReducePaused(env) == 1, {}),
                                   'Timeout waiting for the reducer to pause')
                coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RATIO', 1)
                setCoordReduceResume(env)
                thread.join(timeout=30)
                env.assertFalse(thread.is_alive())
            finally:
                coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RESET')
                resetCoordReduceDebug(env)
            res = result[0]
            if oom_policy == 'fail':
                # FAIL cannot fail once a reply byte is out, so the reply callback reads first.
                env.assertTrue(isinstance(res, ResponseError), message=(case, res))
                env.assertContains(OOM_QUERY_ERROR, str(res), message=case)
                env.assertEqual(coord_oom_counters(coord), (errors + 1, warnings), message=case)
            else:
                env.assertEqual(search_ids(env, res), [], message=(case, res))
                if env.protocol == 3:
                    env.assertEqual(res['warning'], [COORD_OOM_WARNING], message=(case, res))
                env.assertEqual(coord_oom_counters(coord), (errors, warnings + 1), message=case)
            expect_clean_end(env, coord)
    finally:
        allShards_change_oom_policy(env, 'return')


def _test_search_merge_reads_memory_within_a_reply(env):
    skipIfNoEnableAssert(env)
    create_index(env)
    coord = env.getConnection(1)
    interval = 3
    try:
        for oom_policy in ('return', 'fail'):
            allShards_change_oom_policy(env, oom_policy)
            errors, warnings = coord_oom_counters(coord)
            # Pressure appears once the merge has started on the first shard reply.
            setPauseBeforeReduce(env, 1)
            coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'READ_INTERVAL', interval)
            try:
                thread, result = run_in_thread(coord, SEARCH)
                wait_for_condition(lambda: (getIsCoordReducePaused(env) == 1, {}),
                                   'Timeout waiting for the reducer to pause')
                coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RATIO', 1)
                setCoordReduceResume(env)
                thread.join(timeout=30)
                env.assertFalse(thread.is_alive())
            finally:
                coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RESET')
                resetCoordReduceDebug(env)
            res = result[0]
            if oom_policy == 'fail':
                env.assertTrue(isinstance(res, ResponseError), message=res)
                env.assertContains(OOM_QUERY_ERROR, str(res))
                env.assertEqual(coord_oom_counters(coord), (errors + 1, warnings))
            else:
                # The read on the interval-th row stops the merge before that row.
                env.assertEqual(len(search_ids(env, res)), interval - 1, message=res)
                if env.protocol == 3:
                    env.assertEqual(res['warning'], [COORD_OOM_WARNING], message=res)
                env.assertEqual(coord_oom_counters(coord), (errors, warnings + 1))
            expect_clean_end(env, coord)
    finally:
        allShards_change_oom_policy(env, 'return')


@skip(cluster=False)
def test_search_merge_reads_memory_within_a_reply_resp3():
    """The coordinator FT.SEARCH merge reads memory every `QUERY_MEMORY_RATIO_READ_INTERVAL` rows
    within a shard reply, not only after it: pressure during the first reply stops the merge
    there, with the rows merged so far and the coordinator OOM warning (ON_OOM RETURN) or the OOM
    error (ON_OOM FAIL)."""
    _test_search_merge_reads_memory_within_a_reply(Env(protocol=3))


@skip(cluster=False)
def test_search_merge_reads_memory_within_a_reply_resp2():
    """As the RESP3 case; RESP2 has no warning channel."""
    _test_search_merge_reads_memory_within_a_reply(Env(protocol=2))


def merged_rows(res, protocol):
    if protocol == 3:
        return res['Results']['results'] if 'Results' in res else res['results']
    return (res[0] if isinstance(res[0], list) else res)[1:]


def _test_search_merge_stop_shapes(env):
    """The merge's row checks under the reply shapes with their own merge code: SORTBY (sort keys,
    bounded heap), KNN (a K-bounded heap flushed after the merge) and FT.PROFILE. Pressure appears
    once the reducer starts, so the read on the interval-th merged row stops it."""
    skipIfNoEnableAssert(env)
    create_index(env)
    env.expect('FT.CREATE', 'vidx', 'PREFIX', 1, 'v:', 'SCHEMA', 'v', 'VECTOR', 'FLAT', 6, 'TYPE',
               'FLOAT32', 'DIM', 2, 'DISTANCE_METRIC', 'L2').ok()
    conn = getConnectionByEnv(env)
    for i in range(N_DOCS):
        conn.execute_command('HSET', f'v:{i}', 'v', np.array([i, i], dtype=np.float32).tobytes())
    blob = np.array([0, 0], dtype=np.float32).tobytes()
    coord = env.getConnection(1)
    interval = 3
    queries = {
        'sortby': ('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'SORTBY', 'n', 'ASC', 'LIMIT', 0, N_DOCS,
                   'TIMEOUT', 100000),
        'knn': ('FT.SEARCH', 'vidx', f'*=>[KNN {N_DOCS} @v $b]', 'PARAMS', 2, 'b', blob,
                'NOCONTENT', 'LIMIT', 0, N_DOCS, 'DIALECT', 2, 'TIMEOUT', 100000),
        'profile': ('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '*', 'NOCONTENT', 'LIMIT', 0, N_DOCS,
                    'TIMEOUT', 100000),
    }
    try:
        for oom_policy in ('return', 'fail'):
            allShards_change_oom_policy(env, oom_policy)
            for name, query in queries.items():
                case = (name, oom_policy)
                errors, warnings = coord_oom_counters(coord)
                sync_point(coord, 'CLEAR')
                sync_point(coord, 'ARM', 'BeforeCoordReducerClaim')
                coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'READ_INTERVAL', interval)
                try:
                    thread, result = run_in_thread(coord, query)
                    wait_for_sync_point(coord, 'BeforeCoordReducerClaim')
                    coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RATIO', 1)
                    sync_point(coord, 'SIGNAL', 'BeforeCoordReducerClaim')
                    thread.join(timeout=30)
                    env.assertFalse(thread.is_alive(), message=case)
                finally:
                    coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RESET')
                    sync_point(coord, 'CLEAR')
                res = result[0]
                if oom_policy == 'fail':
                    env.assertTrue(isinstance(res, ResponseError), message=(case, res))
                    env.assertContains(OOM_QUERY_ERROR, str(res), message=case)
                    env.assertEqual(coord_oom_counters(coord), (errors + 1, warnings), message=case)
                    expect_clean_end(env, coord)
                    continue
                rows = merged_rows(res, env.protocol)
                env.assertEqual(len(rows), interval - 1, message=(case, res))
                if name == 'sortby' and env.protocol == 3:
                    ns = [int(r['id'][3:]) for r in rows]
                    env.assertEqual(ns, sorted(ns), message=(case, res))
                if env.protocol == 3:
                    body = res['Results'] if name == 'profile' else res
                    env.assertEqual(body['warning'], [COORD_OOM_WARNING], message=(case, res))
                env.assertEqual(coord_oom_counters(coord), (errors, warnings + 1), message=case)
                expect_clean_end(env, coord)
    finally:
        allShards_change_oom_policy(env, 'return')


@skip(cluster=False)
def test_search_merge_stop_shapes_resp3():
    """See `_test_search_merge_stop_shapes`; ON_OOM RETURN replies the merged rows (SORTBY in
    order) with the coordinator OOM warning, ON_OOM FAIL the OOM error."""
    _test_search_merge_stop_shapes(Env(protocol=3))


@skip(cluster=False)
def test_search_merge_stop_shapes_resp2():
    """As the RESP3 case; RESP2 has no warning channel."""
    _test_search_merge_stop_shapes(Env(protocol=2))


@skip(cluster=False)
def test_search_resp3_warning_follows_results():
    """The coordinator FT.SEARCH RESP3 map carries `warning` after `results`, in a complete reply
    and in one an OOM stop cut short (the client library keeps the wire order of map keys)."""
    env = Env(protocol=3)
    skipIfNoEnableAssert(env)
    create_index(env)
    coord = env.getConnection(1)
    complete = coord.execute_command(*SEARCH)
    truncated = stopped_at_first_reply(env, coord, SEARCH)
    for res in (complete, truncated):
        keys = list(res.keys())
        env.assertLess(keys.index('results'), keys.index('warning'), message=keys)
    env.assertEqual(truncated['warning'], [COORD_OOM_WARNING], message=truncated)


@skip(cluster=False)
def test_search_short_reply_reads_memory_resp3():
    """The coordinator FT.SEARCH reply reads memory before its rows, so pressure that appears
    after the merge stops a reply shorter than the read interval, or one with no row: the results
    end early with the coordinator OOM warning (ON_OOM RETURN), or the query fails with the OOM
    error (ON_OOM FAIL, read by the reply callback)."""
    _test_search_short_reply_reads_memory(Env(protocol=3))


@skip(cluster=False)
def test_search_short_reply_reads_memory_resp2():
    """As the RESP3 case; RESP2 has no warning channel but counts the stop."""
    _test_search_short_reply_reads_memory(Env(protocol=2))


@skip(cluster=False)
def test_search_fanout_stop_then_timeout():
    """A fan-out stopped for memory still waits for its last shard reply; a blocked-client timeout
    that fires meanwhile owns the reply, and its warning is the timeout's, as on a shard: FT.SEARCH
    replies once, with the timeout error (ON_TIMEOUT FAIL) or the timeout's partial result and
    warning (RETURN-STRICT), and releases its blocked client. The remote shards are held before their first read so the timer
    fires first."""
    # Workers run the shard queries, so a parked shard still answers SYNC_POINT.
    env = Env(protocol=3, moduleArgs='WORKERS 1')
    skipIfNoEnableAssert(env)
    create_index(env)
    coord = env.getConnection(1)
    remote_shards = [env.getConnection(i) for i in range(2, env.shardsCount + 1)]
    query = ('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'LIMIT', 0, N_DOCS, 'TIMEOUT', 300)
    try:
        for timeout_policy, oom_policy in itertools.product(('FAIL', 'RETURN-STRICT'),
                                                            ('return', 'fail')):
            case = (timeout_policy, oom_policy)
            set_policies(env, timeout_policy, oom_policy)
            # The held shards run without a timer of their own, which would release them.
            for shard in remote_shards:
                shard.execute_command(config_cmd(), 'SET', 'ON_TIMEOUT', 'RETURN')
            for shard in remote_shards:
                sync_point(shard, 'CLEAR')
                sync_point(shard, 'ARM', 'BeforeFirstRead')
            sync_point(coord, 'CLEAR')
            sync_point(coord, 'ARM', 'BeforeCoordFanout')
            try:
                thread, result = run_in_thread(coord, query)
                wait_for_sync_point(coord, 'BeforeCoordFanout')
                coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RATIO', 1)
                sync_point(coord, 'SIGNAL', 'BeforeCoordFanout')
                for shard in remote_shards:
                    wait_for_sync_point(shard, 'BeforeFirstRead')
                # The timer fires while the remote shards are held.
                thread.join(timeout=30)
                env.assertFalse(thread.is_alive(), message=case)
            finally:
                coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RESET')
                sync_point(coord, 'CLEAR')
                for shard in remote_shards:
                    sync_point(shard, 'SIGNAL', 'BeforeFirstRead')
                    sync_point(shard, 'CLEAR')
            res = result[0]
            if timeout_policy == 'RETURN-STRICT' and oom_policy == 'fail':
                # Shard 1 shares the forced ratio and fails its own query; the timeout's reply is
                # that shard error.
                env.assertTrue(isinstance(res, ResponseError), message=(case, res))
                env.assertContains(OOM_QUERY_ERROR, str(res), message=(case, res))
            elif timeout_policy == 'FAIL':
                env.assertTrue(isinstance(res, ResponseError), message=(case, res))
                env.assertContains('Timeout limit was reached', str(res), message=(case, res))
            else:
                # Every reply was discarded by the stop.
                env.assertFalse(isinstance(res, Exception), message=(case, res))
                env.assertEqual(search_ids(env, res), [], message=(case, res))
                env.assertEqual(res['warning'], ['Timeout limit was reached'], message=(case, res))
            wait_for_condition(
                lambda: (coord.execute_command('INFO', 'CLIENTS')['blocked_clients'] == 0, {}),
                'Timeout waiting for the blocked client to be released')
            expect_clean_end(env, coord)
    finally:
        reset_policies(env)


AGGREGATE = ('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@n', 'TIMEOUT', 100000)


def aggregate_rows(env, res):
    return res['results'] if env.protocol == 3 else res[1:]


def stopped_at_first_read(env, coord, query):
    """Runs `query` on the coordinator with memory forced over the limit after the entry guard,
    before its pipeline fetches the first shard reply, so its memory read before that fetch stops
    it. Shard 1 shares the forced ratio, so the outcome alone does not prove where the stop came
    from; the FT.SEARCH tests above place the pressure precisely."""
    sync_point(coord, 'CLEAR')
    sync_point(coord, 'ARM', 'BeforeRPNetNext')
    try:
        thread, result = run_in_thread(coord, query)
        wait_for_sync_point(coord, 'BeforeRPNetNext')
        coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RATIO', 1)
        sync_point(coord, 'SIGNAL', 'BeforeRPNetNext')
        thread.join(timeout=30)
        env.assertFalse(thread.is_alive())
    finally:
        coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RESET')
        sync_point(coord, 'CLEAR')
    return result[0] if result else None


def _test_aggregate_stream_stop(env):
    skipIfNoEnableAssert(env)
    create_index(env)
    coord = env.getConnection(1)
    try:
        for query in (AGGREGATE, AGGREGATE + ('WITHCURSOR', 'COUNT', 5)):
            for timeout_policy in TIMEOUT_POLICIES:
                for oom_policy in OOM_POLICIES:
                    set_policies(env, timeout_policy, oom_policy)
                    case = ('WITHCURSOR' in query, timeout_policy, oom_policy)
                    errors, warnings = coord_oom_counters(coord)
                    res = stopped_at_first_read(env, coord, query)
                    if 'WITHCURSOR' in query and not isinstance(res, ResponseError):
                        # A stopped read ends the cursor.
                        res, cursor = res
                        if oom_policy == 'return':
                            env.assertEqual(cursor, 0, message=case)
                    if oom_policy == 'fail':
                        env.assertTrue(isinstance(res, ResponseError), message=(case, res))
                        env.assertContains(OOM_QUERY_ERROR, str(res), message=case)
                        env.assertEqual(coord_oom_counters(coord), (errors + 1, warnings),
                                        message=case)
                    elif oom_policy == 'return':
                        # Replies already queued when the stream stops are still drained into the
                        # reply, so with this little data it can be complete.
                        env.assertLessEqual(len(aggregate_rows(env, res)), N_DOCS,
                                            message=(case, res))
                        env.assertEqual(coord_oom_counters(coord), (errors, warnings + 1),
                                        message=case)
                        if env.protocol == 3:
                            env.assertContains(COORD_OOM_WARNING, res['warning'],
                                               message=(case, res))
                    else:
                        env.assertEqual(len(aggregate_rows(env, res)),
                                        5 if 'WITHCURSOR' in query else N_DOCS,
                                        message=(case, res))
                    expect_clean_end(env, coord)
    finally:
        reset_policies(env)


@skip(cluster=False)
def test_aggregate_stream_stop_resp3():
    """The coordinator FT.AGGREGATE reads memory once per shard reply it takes from the channel (and
    the IO thread once per reply before resending): a stop ends the stream and the reply is the rows
    taken or already queued with the coordinator OOM warning, or the OOM error under ON_OOM FAIL.
    With and without WITHCURSOR, under every timeout policy. WITHCOUNT is not covered here: its
    deferred pipeline never parked at the sync point used to place the pressure."""
    _test_aggregate_stream_stop(Env(protocol=3))


@skip(cluster=False)
def test_aggregate_stream_stop_resp2():
    """As the RESP3 case; RESP2 has no warning channel."""
    _test_aggregate_stream_stop(Env(protocol=2))


@skip(cluster=False)
def test_aggregate_profile_is_not_stopped():
    """FT.PROFILE reads the shard stream to its end to collect the shards' profiles, so the
    coordinator stream checkpoints leave it out: under pressure it still replies in full."""
    env = Env(protocol=3)
    skipIfNoEnableAssert(env)
    create_index(env)
    coord = env.getConnection(1)
    res = stopped_at_first_read(env, coord, ('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*',
                                             'LOAD', 1, '@n'))
    env.assertFalse(isinstance(res, Exception), message=res)
    env.assertEqual(len(res['Results']['results']), N_DOCS, message=res)
    expect_clean_end(env, coord)


def cursor_count(env, index):
    return to_dict(to_dict(env.cmd('FT.INFO', index))['cursor_stats'])['index_total']


@skip(cluster=False)
def test_aggregate_stream_io_thread_stops_resending():
    """Without a cursor the IO thread resends every shard's cursor read as soon as a reply lands.
    Pressure seen by the IO thread while the coordinator pipeline has not read yet stops the
    resends: the shards' cursors are deleted before the pipeline reads, and the reply misses the
    chunks never read, with the coordinator OOM warning."""
    env = Env(protocol=3)
    skipIfNoEnableAssert(env)
    # More than one shard cursor chunk on every shard.
    n_docs = 2000 * env.shardsCount
    env.expect('FT.CREATE', 'big', 'PREFIX', 1, 'big:', 'SCHEMA', 'n', 'NUMERIC').ok()
    conn = getConnectionByEnv(env)
    pipe = conn.pipeline(transaction=False)
    for i in range(n_docs):
        pipe.execute_command('HSET', f'big:{i}', 'n', i)
    pipe.execute()
    coord = env.getConnection(1)
    query = ('FT.AGGREGATE', 'big', '*', 'LOAD', 1, '@n', 'TIMEOUT', 100000)
    errors, warnings = coord_oom_counters(coord)
    sync_point(coord, 'CLEAR')
    sync_point(coord, 'ARM', 'BeforeRPNetNext')
    sync_point(coord, 'ARM', 'BeforeCursorResendCheckpoint')
    try:
        thread, result = run_in_thread(coord, query)
        wait_for_sync_point(coord, 'BeforeRPNetNext')
        wait_for_sync_point(coord, 'BeforeCursorResendCheckpoint')
        coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RATIO', 1)
        sync_point(coord, 'SIGNAL', 'BeforeCursorResendCheckpoint')
        # Resending would instead read every chunk into the channel before the cursors end.
        wait_for_condition(lambda: (cursor_count(env, 'big') == 0, {}),
                           'Timeout waiting for the shard cursors to end')
        sync_point(coord, 'SIGNAL', 'BeforeRPNetNext')
        thread.join(timeout=30)
        env.assertFalse(thread.is_alive())
    finally:
        coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RESET')
        sync_point(coord, 'CLEAR')
    res = result[0]
    env.assertFalse(isinstance(res, Exception), message=res)
    env.assertLess(len(res['results']), n_docs)
    env.assertContains(COORD_OOM_WARNING, res['warning'], message=res['warning'])
    env.assertEqual(coord_oom_counters(coord)[0], errors)
    env.assertGreater(coord_oom_counters(coord)[1], warnings)
    expect_clean_end(env, coord)


def _test_aggregate_withcount_stopped_before_fanout(env):
    create_index(env)
    coord = env.getConnection(1)
    query = ('FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 1, '@n', 'TIMEOUT', 100000)
    try:
        for oom_policy in OOM_POLICIES:
            allShards_change_oom_policy(env, oom_policy)
            errors, warnings = coord_oom_counters(coord)
            # Under ON_OOM RETURN the entry guard requests the stop and the query goes on: the
            # WITHCOUNT first-reply barrier and the stream see the requested stop.
            coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RATIO', 1)
            try:
                res = coord.execute_command(*query)
            except ResponseError as e:
                res = e
            finally:
                coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RESET')
            if oom_policy == 'fail':
                env.assertTrue(isinstance(res, ResponseError), message=res)
                env.assertContains(OOM_QUERY_ERROR, str(res))
                env.assertEqual(coord_oom_counters(coord), (errors + 1, warnings))
            elif oom_policy == 'return':
                env.assertLessEqual(len(aggregate_rows(env, res)), N_DOCS, message=res)
                if env.protocol == 3:
                    env.assertContains(COORD_OOM_WARNING, res['warning'], message=res)
                env.assertEqual(coord_oom_counters(coord), (errors, warnings + 1))
            else:
                env.assertFalse(isinstance(res, ResponseError), message=str(res))
                env.assertEqual(len(aggregate_rows(env, res)), N_DOCS, message=res)
            env.assertEqual(cursor_count(env, 'idx'), 0, message=oom_policy)
            expect_clean_end(env, coord)
    finally:
        allShards_change_oom_policy(env, 'return')


@skip(cluster=False)
def test_aggregate_withcount_stopped_before_fanout_resp3():
    """A stop requested before the fan-out (the ON_OOM RETURN entry guard) ends a coordinator
    FT.AGGREGATE WITHCOUNT once with the ON_OOM outcome and leaves no shard cursor."""
    _test_aggregate_withcount_stopped_before_fanout(Env(protocol=3))


@skip(cluster=False)
def test_aggregate_withcount_stopped_before_fanout_resp2():
    """As the RESP3 case; RESP2 has no warning channel."""
    _test_aggregate_withcount_stopped_before_fanout(Env(protocol=2))


@skip(cluster=False)
def test_aggregate_withcount_io_thread_stop_dispatches():
    """A memory stop found by the IO thread while the WITHCOUNT first-reply barrier still waits
    for a shard dispatches the coordinator pipeline at once: the query replies with the
    coordinator OOM warning without waiting for that shard's first reply."""
    # Workers run the shard queries, so a shard parked at a sync point still answers SYNC_POINT.
    env = Env(protocol=3, moduleArgs='WORKERS 1')
    skipIfNoEnableAssert(env)
    if env.shardsCount < 3:
        env.skip()
    # More than one shard cursor chunk on every shard, so first replies are resent.
    n_docs = 2000 * env.shardsCount
    env.expect('FT.CREATE', 'big', 'PREFIX', 1, 'big:', 'SCHEMA', 'n', 'NUMERIC').ok()
    conn = getConnectionByEnv(env)
    pipe = conn.pipeline(transaction=False)
    for i in range(n_docs):
        pipe.execute_command('HSET', f'big:{i}', 'n', i)
    pipe.execute()
    coord = env.getConnection(1)
    parked = env.getConnection(2)
    query = ('FT.AGGREGATE', 'big', '*', 'WITHCOUNT', 'LOAD', 1, '@n', 'TIMEOUT', 100000)
    sync_point(parked, 'CLEAR')
    sync_point(parked, 'ARM', 'BeforeFirstRead')
    sync_point(coord, 'CLEAR')
    sync_point(coord, 'ARM', 'BeforeCursorResendCheckpoint')
    try:
        thread, result = run_in_thread(coord, query)
        wait_for_sync_point(parked, 'BeforeFirstRead')
        wait_for_sync_point(coord, 'BeforeCursorResendCheckpoint')
        coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RATIO', 1)
        sync_point(coord, 'SIGNAL', 'BeforeCursorResendCheckpoint')
        thread.join(timeout=30)
        replied_while_parked = not thread.is_alive()
    finally:
        coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RESET')
        sync_point(coord, 'CLEAR')
        sync_point(parked, 'SIGNAL', 'BeforeFirstRead')
        sync_point(parked, 'CLEAR')
    thread.join(timeout=30)
    env.assertTrue(replied_while_parked)
    res = result[0]
    env.assertFalse(isinstance(res, Exception), message=res)
    env.assertLess(len(res['results']), n_docs)
    env.assertContains(COORD_OOM_WARNING, res['warning'], message=res['warning'])
    wait_for_condition(lambda: (cursor_count(env, 'big') == 0, {}),
                       'Timeout waiting for the shard cursors to end')
    expect_clean_end(env, coord)


@skip(cluster=False)
def test_tagvals_merge_stop():
    """The coordinator FT.TAGVALS merge runs the memory checkpoint per value it adds. FT.TAGVALS has
    no warning channel, so a stop replies the OOM error, under ON_OOM RETURN and FAIL alike; ON_OOM
    IGNORE merges every value. Nothing else in the coordinator process
    checks memory for FT.TAGVALS, so `AT_CALL` counts only the merge."""
    env = Env(protocol=3)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TAG').ok()
    conn = getConnectionByEnv(env)
    n_values = 50
    for i in range(n_values):
        conn.execute_command('HSET', f'doc{i}', 't', f'v{i}')
    coord = env.getConnection(1)
    try:
        for oom_policy in OOM_POLICIES:
            allShards_change_oom_policy(env, oom_policy)
            coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', 10)
            try:
                res = coord.execute_command('FT.TAGVALS', 'idx', 't')
            except ResponseError as e:
                res = e
            finally:
                coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RESET')
            if oom_policy == 'ignore':
                env.assertEqual(set(res), {f'v{i}' for i in range(n_values)},
                                message=(oom_policy, res))
            else:
                env.assertTrue(isinstance(res, ResponseError), message=(oom_policy, res))
                env.assertContains(OOM_QUERY_ERROR, str(res), message=oom_policy)
            expect_clean_end(env, coord)
    finally:
        allShards_change_oom_policy(env, 'return')



@skip(cluster=False)
def test_tagvals_merge_reads_memory_every_interval():
    """Without `AT_CALL`, the FT.TAGVALS merge reads memory on its `QUERY_MEMORY_RATIO_READ_INTERVAL`-th
    value: with more values than that and the ratio over the limit, it replies the OOM error."""
    env = Env(protocol=3)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TAG').ok()
    conn = getConnectionByEnv(env)
    n_values = 300
    for i in range(n_values):
        conn.execute_command('HSET', f'doc{i}', 't', f'v{i}')
    coord = env.getConnection(1)
    env.assertGreater(n_values, coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'STATUS')[3])
    try:
        for oom_policy in OOM_POLICIES:
            allShards_change_oom_policy(env, oom_policy)
            coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RATIO', 1)
            try:
                res = coord.execute_command('FT.TAGVALS', 'idx', 't')
            except ResponseError as e:
                res = e
            finally:
                coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RESET')
            if oom_policy == 'ignore':
                env.assertEqual(len(res), n_values, message=oom_policy)
            else:
                env.assertTrue(isinstance(res, ResponseError), message=(oom_policy, res))
                env.assertContains(OOM_QUERY_ERROR, str(res), message=oom_policy)
            expect_clean_end(env, coord)
    finally:
        allShards_change_oom_policy(env, 'return')


def _test_aggregate_withcount_reader_stop(env):
    create_index(env)
    coord = env.getConnection(1)
    query = ('FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 1, '@n', 'TIMEOUT', 100000)
    try:
        for oom_policy in ('return', 'fail'):
            allShards_change_oom_policy(env, oom_policy)
            # The WITHCOUNT pipeline starts once every shard replied, so its memory reads (one
            # per fetch: a reply per shard, then the end of the stream) are the coordinator
            # process's last checkpoint calls. Every shard answers in one chunk, so the IO thread
            # never reads.
            coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', 1 << 40)
            try:
                coord.execute_command(*query)
                calls = coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'STATUS')[5]
            finally:
                coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RESET')
            first_fetch_read = calls - env.shardsCount
            errors, warnings = coord_oom_counters(coord)
            coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', first_fetch_read)
            try:
                res = coord.execute_command(*query)
            except ResponseError as e:
                res = e
            finally:
                coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RESET')
            if oom_policy == 'fail':
                env.assertTrue(isinstance(res, ResponseError), message=res)
                env.assertContains(OOM_QUERY_ERROR, str(res))
                env.assertEqual(coord_oom_counters(coord), (errors + 1, warnings))
            else:
                # The stop before the first fetch leaves only the replies already queued.
                env.assertLessEqual(len(aggregate_rows(env, res)), N_DOCS, message=res)
                if env.protocol == 3:
                    env.assertContains(COORD_OOM_WARNING, res['warning'], message=res)
                env.assertEqual(coord_oom_counters(coord), (errors, warnings + 1))
            env.assertEqual(cursor_count(env, 'idx'), 0, message=oom_policy)
            expect_clean_end(env, coord)
    finally:
        allShards_change_oom_policy(env, 'return')


@skip(cluster=False)
def test_aggregate_withcount_reader_stop_resp3():
    """The WITHCOUNT pipeline reads memory before each fetch of a shard reply: a stop there replies
    the coordinator OOM warning (ON_OOM RETURN) or the OOM error (ON_OOM FAIL), counts it, and
    leaves no shard cursor."""
    _test_aggregate_withcount_reader_stop(Env(protocol=3))


@skip(cluster=False)
def test_aggregate_withcount_reader_stop_resp2():
    """As the RESP3 case; RESP2 has no warning channel."""
    _test_aggregate_withcount_reader_stop(Env(protocol=2))


@skip(cluster=False)
def test_aggregate_withcount_follow_up_read_stop_dispatches():
    """As `test_aggregate_withcount_io_thread_stop_dispatches`, with the memory stop found on a
    shard's follow-up cursor read rather than on its first reply: the WITHCOUNT pipeline is still
    dispatched at once, while the other shards' first replies are held."""
    env = Env(protocol=3, moduleArgs='WORKERS 1')
    skipIfNoEnableAssert(env)
    # Several cursor chunks on the coordinator's own shard, the only one replying.
    n_docs = 5000 * env.shardsCount
    env.expect('FT.CREATE', 'big', 'PREFIX', 1, 'big:', 'SCHEMA', 'n', 'NUMERIC').ok()
    conn = getConnectionByEnv(env)
    pipe = conn.pipeline(transaction=False)
    for i in range(n_docs):
        pipe.execute_command('HSET', f'big:{i}', 'n', i)
    pipe.execute()
    coord = env.getConnection(1)
    parked = [env.getConnection(i) for i in range(2, env.shardsCount + 1)]
    query = ('FT.AGGREGATE', 'big', '*', 'WITHCOUNT', 'LOAD', 1, '@n', 'TIMEOUT', 100000)
    point = 'BeforeFollowUpResendCheckpoint'
    for shard in parked:
        sync_point(shard, 'CLEAR')
        sync_point(shard, 'ARM', 'BeforeFirstRead')
    sync_point(coord, 'CLEAR')
    sync_point(coord, 'ARM', point)
    try:
        thread, result = run_in_thread(coord, query)
        for shard in parked:
            wait_for_sync_point(shard, 'BeforeFirstRead')
        # A follow-up read of shard 1, the only shard replying, after its first reply.
        wait_for_sync_point(coord, point)
        coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RATIO', 1)
        sync_point(coord, 'SIGNAL', point)
        thread.join(timeout=30)
        replied_while_parked = not thread.is_alive()
    finally:
        coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RESET')
        sync_point(coord, 'CLEAR')
        for shard in parked:
            sync_point(shard, 'SIGNAL', 'BeforeFirstRead')
            sync_point(shard, 'CLEAR')
    thread.join(timeout=30)
    env.assertTrue(replied_while_parked)
    res = result[0]
    env.assertFalse(isinstance(res, Exception), message=res)
    env.assertLess(len(res['results']), n_docs)
    env.assertContains(COORD_OOM_WARNING, res['warning'], message=res['warning'])
    wait_for_condition(lambda: (cursor_count(env, 'big') == 0, {}),
                       'Timeout waiting for the shard cursors to end')
    expect_clean_end(env, coord)


@skip(cluster=False)
def test_aggregate_withcount_stopped_before_fanout_does_not_wait_for_shards():
    """A request already stopped by the ON_OOM RETURN entry guard replies at once, without waiting
    for any shard's first reply: every other shard is parked and the query still ends with the
    coordinator OOM warning."""
    env = Env(protocol=3, moduleArgs='WORKERS 1')
    skipIfNoEnableAssert(env)
    create_index(env)
    coord = env.getConnection(1)
    parked = [env.getConnection(i) for i in range(2, env.shardsCount + 1)]
    query = ('FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 1, '@n', 'TIMEOUT', 100000)
    for shard in parked:
        sync_point(shard, 'CLEAR')
        sync_point(shard, 'ARM', 'BeforeFirstRead')
    try:
        # Raised before the query, so the entry guard (not a later checkpoint) stops it.
        coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RATIO', 1)
        thread, result = run_in_thread(coord, query)
        thread.join(timeout=30)
        replied_while_parked = not thread.is_alive()
    finally:
        coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RESET')
        for shard in parked:
            sync_point(shard, 'SIGNAL', 'BeforeFirstRead')
            sync_point(shard, 'CLEAR')
    thread.join(timeout=30)
    env.assertTrue(replied_while_parked)
    res = result[0]
    env.assertFalse(isinstance(res, Exception), message=res)
    env.assertContains(COORD_OOM_WARNING, res['warning'], message=res)
    wait_for_condition(lambda: (cursor_count(env, 'idx') == 0, {}),
                       'Timeout waiting for the shard cursors to end')
    expect_clean_end(env, coord)


def _test_aggregate_withcount_zero_row_pager_io_thread_stop(env, oom_policy):
    skipIfNoEnableAssert(env)
    if env.shardsCount < 3:
        env.skip()
    # More than one shard cursor chunk on every shard, so first replies are resent.
    n_docs = 2000 * env.shardsCount
    env.expect('FT.CREATE', 'big', 'PREFIX', 1, 'big:', 'SCHEMA', 'n', 'NUMERIC').ok()
    conn = getConnectionByEnv(env)
    pipe = conn.pipeline(transaction=False)
    for i in range(n_docs):
        pipe.execute_command('HSET', f'big:{i}', 'n', i)
    pipe.execute()
    coord = env.getConnection(1)
    parked = env.getConnection(2)
    # The pager reads no row, so only the worker's publication of the IO thread's stop can fail it.
    query = ('FT.AGGREGATE', 'big', '*', 'WITHCOUNT', 'LIMIT', 0, 0, 'TIMEOUT', 100000)
    allShards_change_oom_policy(env, oom_policy)
    errors, warnings = coord_oom_counters(coord)
    sync_point(parked, 'CLEAR')
    sync_point(parked, 'ARM', 'BeforeFirstRead')
    sync_point(coord, 'CLEAR')
    sync_point(coord, 'ARM', 'BeforeCursorResendCheckpoint')
    try:
        thread, result = run_in_thread(coord, query)
        wait_for_sync_point(parked, 'BeforeFirstRead')
        wait_for_sync_point(coord, 'BeforeCursorResendCheckpoint')
        coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RATIO', 1)
        sync_point(coord, 'SIGNAL', 'BeforeCursorResendCheckpoint')
        thread.join(timeout=30)
        replied_while_parked = not thread.is_alive()
    finally:
        coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RESET')
        sync_point(coord, 'CLEAR')
        sync_point(parked, 'SIGNAL', 'BeforeFirstRead')
        sync_point(parked, 'CLEAR')
        allShards_change_oom_policy(env, 'return')
    thread.join(timeout=30)
    env.assertTrue(replied_while_parked)
    res = result[0]
    if oom_policy == 'fail':
        env.assertTrue(isinstance(res, ResponseError), message=res)
        env.assertContains(OOM_QUERY_ERROR, str(res))
        env.assertEqual(coord_oom_counters(coord), (errors + 1, warnings))
    else:
        env.assertFalse(isinstance(res, Exception), message=res)
        env.assertContains(COORD_OOM_WARNING, res['warning'], message=res)
        env.assertEqual(coord_oom_counters(coord), (errors, warnings + 1))
    wait_for_condition(lambda: (cursor_count(env, 'big') == 0, {}),
                       'Timeout waiting for the shard cursors to end')
    expect_clean_end(env, coord)


@skip(cluster=False)
def test_aggregate_withcount_zero_row_pager_io_thread_stop_fail():
    """An IO-thread memory stop on `WITHCOUNT LIMIT 0 0`, whose pager never reads the iterator,
    still fails the query with the OOM error under ON_OOM FAIL."""
    _test_aggregate_withcount_zero_row_pager_io_thread_stop(
        Env(protocol=3, moduleArgs='WORKERS 1'), 'fail')


@skip(cluster=False)
def test_aggregate_withcount_zero_row_pager_io_thread_stop_return():
    """As the FAIL case; under ON_OOM RETURN the reply carries the coordinator OOM warning."""
    _test_aggregate_withcount_zero_row_pager_io_thread_stop(
        Env(protocol=3, moduleArgs='WORKERS 1'), 'return')
