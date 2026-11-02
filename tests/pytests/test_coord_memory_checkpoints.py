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

    # Every checkpoint call of the coordinator process for one query: shard 1's pipeline (the same
    # count on every run of this query and data), then one memory read per shard reply. Count up to
    # and including the first reply's read.
    coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', 1 << 40)
    coord.execute_command(*SEARCH)
    calls = checkpoint_calls()
    coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RESET')
    first_reply_read = calls - len(others)

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
    skipIfNoEnableAssert(env)
    if env.shardsCount < 3:
        env.skip()
    create_index(env)
    coord = env.getConnection(1)
    remote_shards = [env.getConnection(i) for i in range(2, env.shardsCount + 1)]
    local_docs = coord.execute_command('KEYS', 'doc*')

    # The held shard replies normally: stopped at a build checkpoint it would never reach the
    # sync point that holds it.
    held = remote_shards[-1]

    def arm_remote_shards():
        # The other remotes' first checkpoint stops them: an empty reply with the shard OOM warning.
        for conn in remote_shards[:-1]:
            conn.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', 1)

    def reset_all():
        run_command_on_all_shards(env, debug_cmd(), 'MEMORY_PRESSURE', 'RESET')

    # Every checkpoint call of the coordinator process for one query (shard 1's pipeline, the same
    # count on every run of this query and data, then one read per shard reply); the last is the
    # last shard reply's read.
    arm_remote_shards()
    coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', 1 << 40)
    try:
        coord.execute_command(*SEARCH)
        last_reply_read = coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'STATUS')[5]
    finally:
        reset_all()

    # The last remote shard is held before its first read, so its reply is the last to land and
    # the one the stop discards; the local shard's reply and the other remote's are kept.
    sync_point(held, 'CLEAR')
    sync_point(held, 'ARM', 'BeforeFirstRead')
    arm_remote_shards()
    coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', last_reply_read)
    errors, warnings = coord_oom_counters(coord)
    try:
        thread, result = run_in_thread(coord, SEARCH)
        wait_for_sync_point(held, 'BeforeFirstRead')
        wait_for_condition(
            lambda: (coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'STATUS')[5]
                     == last_reply_read - 1, {}),
            'Timeout waiting for the kept shard replies')
        sync_point(held, 'SIGNAL', 'BeforeFirstRead')
        thread.join(timeout=30)
        env.assertFalse(thread.is_alive())
    finally:
        reset_all()
        sync_point(held, 'SIGNAL', 'BeforeFirstRead')
        sync_point(held, 'CLEAR')
    res = result[0]
    # The kept replies are the local shard's documents and a remote shard's warning reply.
    env.assertEqual(sorted(search_ids(env, res)), sorted(local_docs), message=res)
    if env.protocol == 3:
        env.assertEqual(res['warning'], [SHARD_OOM_WARNING, COORD_OOM_WARNING], message=res)
    env.assertEqual(coord_oom_counters(coord), (errors, warnings + 2 if env.protocol == 3 else warnings + 1))
    expect_clean_end(env, coord)


@skip(cluster=False)
def test_search_fanout_stop_after_shard_oom_warnings_resp3():
    """A coordinator stop after shard replies that carry the shard OOM warning replies both
    warnings, the shard's first, and counts each once."""
    _test_search_fanout_stop_after_shard_oom_warnings(
        Env(protocol=3, moduleArgs='WORKERS 1'))


@skip(cluster=False)
def test_search_fanout_stop_after_shard_oom_warnings_resp2():
    """As the RESP3 case; RESP2 replies no warning but counts the coordinator stop."""
    _test_search_fanout_stop_after_shard_oom_warnings(
        Env(protocol=2, moduleArgs='WORKERS 1'))


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


@skip(cluster=False)
def test_timeout_owned_reduction_after_shard_warning_and_oom_stop():
    """A shard warning is retained, the coordinator then stops for memory, and the RETURN-STRICT
    timer claims reduction before the background reducer: the timer owns the reply, so its
    warning is the timeout's, not the retained shard warning."""
    env = Env(protocol=3)
    skipIfNoEnableAssert(env)
    if env.shardsCount < 3:
        env.skip()
    create_index(env)
    coord = env.getConnection(1)
    remote_shards = [env.getConnection(i) for i in range(2, env.shardsCount + 1)]
    timed_search = SEARCH[:-1] + (300,)

    def arm_remote_shards():
        for conn in remote_shards:
            conn.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', 1)

    def reset_all():
        run_command_on_all_shards(env, debug_cmd(), 'MEMORY_PRESSURE', 'RESET')

    try:
        set_policies(env, 'RETURN-STRICT', 'return')
        # Calibrate the coordinator's checkpoint count; the last one is the last reply's read.
        arm_remote_shards()
        coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', 1 << 40)
        try:
            coord.execute_command(*SEARCH)
            last_reply_read = coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'STATUS')[5]
        finally:
            reset_all()

        arm_remote_shards()
        coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', last_reply_read)
        # Hold the background reducer so the timer claims reduction first.
        sync_point(coord, 'CLEAR')
        sync_point(coord, 'ARM', 'BeforeCoordReducerClaim')
        try:
            thread, result = run_in_thread(coord, timed_search)
            thread.join(timeout=30)
            env.assertFalse(thread.is_alive())
        finally:
            reset_all()
            sync_point(coord, 'SIGNAL', 'BeforeCoordReducerClaim')
            sync_point(coord, 'CLEAR')
        res = result[0]
        env.assertFalse(isinstance(res, Exception), message=res)
        env.assertEqual(res['warning'], ['Timeout limit was reached'], message=res)
        wait_for_condition(
            lambda: (coord.execute_command('INFO', 'CLIENTS')['blocked_clients'] == 0, {}),
            'Timeout waiting for the blocked client to be released')
        expect_clean_end(env, coord)
    finally:
        reset_policies(env)


@skip(cluster=False)
def test_timeout_after_reducer_claim_beats_the_fanout_oom_stop():
    """The fan-out recorded an OOM stop under ON_OOM FAIL, the background reducer claims reduction
    and is held there, and the RETURN-STRICT timer fires meanwhile: the timeout callback owns the
    reply, so it replies the timeout's partial result and warning, not the OOM error the reducer
    would map from the stop. The reducer self-releases, since the callback blocks the main thread
    that would signal it."""
    env = Env(protocol=3)
    skipIfNoEnableAssert(env)
    create_index(env)
    coord = env.getConnection(1)
    timed_search = SEARCH[:-1] + (300,)
    try:
        set_policies(env, 'RETURN-STRICT', 'fail')
        # Calibrate the coordinator's checkpoint count; the last one is the last reply's read.
        coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', 1 << 40)
        try:
            coord.execute_command(*SEARCH)
            last_reply_read = coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'STATUS')[5]
        finally:
            coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RESET')

        coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', last_reply_read)
        sync_point(coord, 'CLEAR')
        sync_point(coord, 'ARM', 'CoordSearchReducerClaimed', 1500)
        try:
            thread, result = run_in_thread(coord, timed_search)
            thread.join(timeout=30)
            env.assertFalse(thread.is_alive())
        finally:
            coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RESET')
            sync_point(coord, 'CLEAR')
        res = result[0]
        env.assertFalse(isinstance(res, Exception), message=res)
        env.assertEqual(res['warning'], ['Timeout limit was reached'], message=res)
        wait_for_condition(
            lambda: (coord.execute_command('INFO', 'CLIENTS')['blocked_clients'] == 0, {}),
            'Timeout waiting for the blocked client to be released')
        expect_clean_end(env, coord)
    finally:
        reset_policies(env)
