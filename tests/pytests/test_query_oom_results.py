# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Query memory checkpoints in the result buffering and reply paths, driven by
`_FT.DEBUG MEMORY_PRESSURE AT_CALL`, which makes the n-th checkpoint call report pressure."""

from common import *
from test_blocked_client_timeout import (
    _wait_shard_paused_after_aggregate_result, wait_for_client_unblocked,
)
from test_memory_pressure_hook import BUILD_CHECKPOINTS
from test_info_modules import (
    COORD_WARN_ERR_SECTION, OOM_ERROR_COORD_METRIC, OOM_WARNING_COORD_METRIC,
    info_modules_to_dict,
)
import threading

OOM_QUERY_ERROR = "Not enough memory available to execute the query"
COORD_OOM_WARNING = "Coordinator failed to execute the query due to insufficient memory"
SHARD_OOM_WARNING = "One or more shards failed to execute the query due to insufficient memory"
TIMEOUT_WARNING = "Timeout limit was reached"
N_DOCS = 10
# Larger than any test's checkpoint call count: arms the call counter without firing.
NEVER = 1_000_000
# FT.SEARCH and FT.AGGREGATE call this many build checkpoints before their pipeline runs.
B = BUILD_CHECKPOINTS


def _memory_pressure(env, *args):
    return env.cmd(debug_cmd(), 'MEMORY_PRESSURE', *args)


def _checkpoint_calls(env, query):
    """Checkpoint calls `query` makes when nothing fires (STATUS counts them while armed)."""
    _memory_pressure(env, 'AT_CALL', NEVER)
    res = env.cmd(*query)
    calls = _memory_pressure(env, 'STATUS')[5]
    _memory_pressure(env, 'RESET')
    if 'WITHCURSOR' in query and res[1] != 0:
        env.cmd('FT.CURSOR', 'DEL', 'idx', res[1])
    return calls


def _replay_call(env, query, replayed_rows, n):
    """The checkpoint call of the n-th replayed row (1-based): the replay of a buffered
    result is the query's last `replayed_rows` checkpoint calls."""
    return _checkpoint_calls(env, query) - replayed_rows + n


def _oom_counters(env):
    section = info_modules_to_dict(env)[COORD_WARN_ERR_SECTION]
    return int(section[OOM_ERROR_COORD_METRIC]), int(section[OOM_WARNING_COORD_METRIC])


def _index_with_docs(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT', 'n', 'NUMERIC', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    for i in range(N_DOCS):
        conn.execute_command('HSET', f'doc{i}', 'name', f'hello{i}', 'n', i)


def _set_policies(env, on_timeout, on_oom, workers):
    env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, on_timeout).ok()
    change_oom_policy(env, on_oom)
    env.expect(config_cmd(), 'SET', 'WORKERS', workers).ok()


def _reset(env):
    _memory_pressure(env, 'RESET')
    _set_policies(env, 'return', 'return', 0)


AGGREGATE = ['FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@name']


@skip(cluster=True)
@env_spec(protocol=3)
def test_buffered_results_stop_fails_under_on_oom_fail(env):
    """ON_OOM FAIL buffers every row before replying, so a stop while buffering replies the OOM
    error instead of the rows, with or without workers and under every ON_TIMEOUT policy."""
    try:
        _index_with_docs(env)
        for on_timeout in ['return', 'fail', 'return-strict']:
            for workers in [0, 2]:
                _set_policies(env, on_timeout, 'fail', workers)
                errors, warnings = _oom_counters(env)
                _memory_pressure(env, 'AT_CALL', B + 3)
                env.expect(*AGGREGATE).error().contains(OOM_QUERY_ERROR)
                env.assertEqual(_oom_counters(env), (errors + 1, warnings),
                                message=(on_timeout, workers))
    finally:
        _reset(env)


@skip(cluster=True)
@env_spec(protocol=3)
def test_buffered_results_stop_returns_the_prefix(env):
    """Under a buffering ON_TIMEOUT policy and ON_OOM RETURN, a stop while buffering replies the
    rows buffered so far with the OOM warning instead of a timeout. RETURN-STRICT only buffers
    with workers; without them it streams like RETURN."""
    try:
        _index_with_docs(env)
        for on_timeout, workers in [('fail', 0), ('fail', 2), ('return-strict', 2)]:
            _set_policies(env, on_timeout, 'return', workers)
            errors, warnings = _oom_counters(env)
            _memory_pressure(env, 'AT_CALL', B + 3)
            res = env.cmd(*AGGREGATE)
            env.assertEqual([row['extra_attributes']['name'] for row in res['results']],
                            ['hello0', 'hello1', 'hello2'], message=(on_timeout, workers, res))
            env.assertEqual(res['warning'], [COORD_OOM_WARNING], message=(on_timeout, workers))
            env.assertEqual(_oom_counters(env), (errors, warnings + 1),
                            message=(on_timeout, workers))
    finally:
        _reset(env)


@skip(cluster=True)
@env_spec(protocol=3)
def test_stored_results_replay_stop_truncates(env):
    """Replaying buffered rows checks memory per row: a stop there truncates the reply and warns,
    even under ON_OOM FAIL, whose error can only be decided while buffering."""
    try:
        _index_with_docs(env)
        search = ['FT.SEARCH', 'idx', '*', 'NOCONTENT', 'LIMIT', 0, 2 * N_DOCS]
        for on_oom in ['return', 'fail']:
            for workers in [0, 2]:
                _set_policies(env, 'fail', on_oom, workers)
                for query in [AGGREGATE, search]:
                    _memory_pressure(env, 'AT_CALL', _replay_call(env, query, N_DOCS, 3))
                    res = env.cmd(*query)
                    env.assertEqual(len(res['results']), 2, message=(on_oom, workers, query, res))
                    env.assertEqual(res['warning'], [COORD_OOM_WARNING], message=(on_oom, workers))
    finally:
        _reset(env)


@skip(cluster=True)
@env_spec(protocol=2)
def test_stored_results_replay_stop_truncates_resp2(env):
    """RESP2 has no warning channel: a replay stopped for OOM is a silent partial result."""
    try:
        _index_with_docs(env)
        _set_policies(env, 'fail', 'return', 0)
        _memory_pressure(env, 'AT_CALL', B + 3)
        env.assertEqual(len(env.cmd(*AGGREGATE)) - 1, 3)
        _memory_pressure(env, 'AT_CALL', _replay_call(env, AGGREGATE, N_DOCS, 3))
        env.assertEqual(len(env.cmd(*AGGREGATE)) - 1, 2)
    finally:
        _reset(env)


def _profile_rows(env, res):
    """The result rows of an FT.PROFILE reply, in the shape of the bare command's reply."""
    return res['Results']['results'] if env.protocol == 3 else res[0][1:]


def _bare_rows(env, res):
    return res['results'] if env.protocol == 3 else res[1:]


def _check_profile_replay_stop(env):
    try:
        _index_with_docs(env)
        search = ['FT.SEARCH', 'idx', '*', 'NOCONTENT', 'LIMIT', 0, 2 * N_DOCS]
        for on_oom in ['return', 'fail']:
            for workers in [0, 2]:
                _set_policies(env, 'fail', on_oom, workers)
                for bare in [AGGREGATE, search]:
                    kind = bare[0].split('.')[1]
                    profile = ['FT.PROFILE', 'idx', kind, 'QUERY', *bare[2:]]
                    message = (on_oom, workers, kind)

                    _memory_pressure(env, 'AT_CALL', _replay_call(env, bare, N_DOCS, 3))
                    expected = _bare_rows(env, env.cmd(*bare))
                    env.assertEqual(len(expected), 2, message=message)

                    _memory_pressure(env, 'AT_CALL', _replay_call(env, profile, N_DOCS, 3))
                    res = env.cmd(*profile)
                    _memory_pressure(env, 'RESET')
                    env.assertEqual(_profile_rows(env, res), expected, message=(message, res))

                    shards = get_shards_profile(env, res)
                    env.assertEqual(len(shards), 1, message=(message, res))
                    shard = to_dict(shards[0])
                    env.assertContains('Result processors profile', shard)
                    env.assertContains('Warning', shard)
                    env.assertContains(SHARD_OOM_WARNING, shard['Warning'], message=(message, res))
                    if kind == 'SEARCH':
                        env.assertContains('Iterators profile', shard)
                    if env.protocol == 3:
                        env.assertEqual(set(res.keys()), {'Results', 'Profile'}, message=(message, res))
                        env.assertEqual(res['Results']['warning'], [COORD_OOM_WARNING],
                                        message=(message, res))
                    else:
                        env.assertEqual(len(res), 2, message=(message, res))
    finally:
        _reset(env)


@skip(cluster=True)
@env_spec(protocol=3)
def test_profile_stored_results_replay_stop_truncates(env):
    """FT.PROFILE serializes through its own wrapper and warning list: a replay stopped for OOM
    keeps the bare command's partial rows, reports the OOM warning, and leaves the profile
    section complete, under both ON_OOM policies."""
    _check_profile_replay_stop(env)


@skip(cluster=True)
@env_spec(protocol=2)
def test_profile_stored_results_replay_stop_truncates_resp2(env):
    """RESP2 FT.PROFILE has no warning channel in the results, but the profile section still
    carries the OOM warning next to the same partial rows as the bare command."""
    _check_profile_replay_stop(env)


@skip(cluster=True)
@env_spec(protocol=3)
def test_profile_buffered_results_stop_fails_under_on_oom_fail(env):
    """Under ON_OOM FAIL a stop while buffering fails the profiled query with the OOM error."""
    try:
        _index_with_docs(env)
        profile = ['FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', *AGGREGATE[2:]]
        for workers in [0, 2]:
            _set_policies(env, 'fail', 'fail', workers)
            # The replay is the last N_DOCS calls and buffering checks every row but the one that
            # fills the limit, so the third buffering call is this far from the end.
            _memory_pressure(env, 'AT_CALL', _checkpoint_calls(env, profile) - (2 * N_DOCS - 1) + 3)
            env.expect(*profile).error().contains(OOM_QUERY_ERROR)
    finally:
        _reset(env)


@skip(cluster=True)
@env_spec(protocol=3)
def test_stored_results_replay_stop_ends_the_cursor(env):
    """A cursor chunk cut short while replaying ends the cursor: resuming it would skip the rows
    the replay left out. A full chunk otherwise keeps the cursor open (no buffering checkpoint
    on the row that fills it)."""
    try:
        _index_with_docs(env)
        for workers in [0, 2]:
            _set_policies(env, 'fail', 'return', workers)
            query = [*AGGREGATE, 'WITHCURSOR', 'COUNT', 4]
            _memory_pressure(env, 'AT_CALL', NEVER)
            res, cursor = env.cmd(*query)
            calls = _memory_pressure(env, 'STATUS')[5]
            _memory_pressure(env, 'RESET')
            env.assertNotEqual(cursor, 0, message=workers)
            env.cmd('FT.CURSOR', 'DEL', 'idx', cursor)

            _memory_pressure(env, 'AT_CALL', calls - 4 + 3)
            res, cursor = env.cmd(*query)
            env.assertEqual(len(res['results']), 2, message=(workers, res))
            env.assertEqual(res['warning'], [COORD_OOM_WARNING], message=workers)
            env.assertEqual(cursor, 0, message=workers)
    finally:
        _reset(env)


@skip(cluster=True)
@env_spec(protocol=3)
def test_pressure_after_the_row_that_fills_the_limit_is_no_oom_error(env):
    """Buffering does not check memory on the row that fills the limit: the check would end a
    full cursor chunk, or fail it under ON_OOM FAIL, for pressure that arrives after every
    requested row is held. The call where that check would be is the first replay call, so
    pressure there stops the replay, which is a truncated reply and never an OOM error."""
    try:
        _index_with_docs(env)
        count = 4
        query = [*AGGREGATE, 'WITHCURSOR', 'COUNT', count]
        for on_oom in ['return', 'fail']:
            for workers in [0, 2]:
                _set_policies(env, 'fail', on_oom, workers)
                errors, warnings = _oom_counters(env)
                # The safe loader adds per-row checkpoints under workers, so calibrate the call.
                _memory_pressure(env, 'AT_CALL', _replay_call(env, query, count, 1))
                res, cursor = env.cmd(*query)
                message = (on_oom, workers, res)
                env.assertEqual(res['results'], [], message=message)
                env.assertEqual(res['warning'], [COORD_OOM_WARNING], message=message)
                env.assertEqual(cursor, 0, message=message)
                env.assertEqual(_oom_counters(env)[0], errors, message=message)
    finally:
        _reset(env)


@skip(cluster=True)
@env_spec(protocol=3)
def test_stored_results_replay_stop_ends_a_timed_out_cursor(env):
    """A buffered cursor chunk that timed out under ON_TIMEOUT RETURN is replied and normally
    keeps its cursor open; cut short while replaying, it ends the cursor too. ON_OOM FAIL makes
    the chunk buffer; TIMEOUT_AFTER_N times it out after 4 rows, which the replay's last 4
    checkpoint calls serialize."""
    try:
        _index_with_docs(env)
        _set_policies(env, 'return', 'fail', 0)
        query = [debug_cmd(), *AGGREGATE, 'WITHCURSOR', 'COUNT', 8, 'TIMEOUT_AFTER_N', 4,
                 'DEBUG_PARAMS_COUNT', 2]
        _memory_pressure(env, 'AT_CALL', NEVER)
        res, cursor = env.cmd(*query)
        calls = _memory_pressure(env, 'STATUS')[5]
        _memory_pressure(env, 'RESET')
        env.assertEqual(len(res['results']), 4, message=res)
        env.assertNotEqual(cursor, 0)
        env.cmd('FT.CURSOR', 'DEL', 'idx', cursor)

        _memory_pressure(env, 'AT_CALL', calls - 4 + 2)
        res, cursor = env.cmd(*query)
        env.assertEqual(len(res['results']), 1, message=res)
        env.assertContains(COORD_OOM_WARNING, res['warning'])
        env.assertEqual(cursor, 0)
    finally:
        _reset(env)


@skip(cluster=True)
@env_spec(protocol=3, moduleArgs='WORKERS 1 TIMEOUT 0')
def test_return_strict_drain_stop_truncates(env):
    """After a RETURN-STRICT timeout the main thread drains the sorter heap into the reply; the
    drain checks memory per row, although the request is already stopped by the timeout, and
    a stop there keeps what was drained and warns of both."""
    t_query = None
    try:
        skipIfNoEnableAssert(env)
        _index_with_docs(env)
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict').ok()
        query = ['FT.AGGREGATE', 'idx', '*', 'SORTBY', 2, '@n', 'ASC', 'LIMIT', 0, N_DOCS]
        resetAggregateResultsDebug(env)
        setPauseAfterAggregateResult(env, 1)
        result = []
        t_query = threading.Thread(target=call_and_store, args=(env.cmd, query, result), daemon=True)
        t_query.start()
        blocked_client_id = _wait_shard_paused_after_aggregate_result(env, 'FT.AGGREGATE')
        # Only the drain counts from here: the worker sees the timeout before its next checkpoint,
        # and a call on an already stopped request is not counted.
        _memory_pressure(env, 'AT_CALL', 3)
        env.cmd('CLIENT', 'UNBLOCK', blocked_client_id, 'TIMEOUT')
        wait_for_client_unblocked(env, blocked_client_id)
        resetAggregateResultsDebug(env)
        t_query.join(timeout=10)
        env.assertFalse(t_query.is_alive())
        env.assertEqual(len(result), 1)

        res = result[0]
        # One row buffered before the timeout, two drained before the third drain call stopped it.
        env.assertEqual([row['extra_attributes']['n'] for row in res['results']], ['0', '1', '2'],
                        message=res)
        env.assertEqual(sorted(res['warning']), sorted([COORD_OOM_WARNING, TIMEOUT_WARNING]))
    finally:
        # A parked worker would leak the pause into later tests.
        if t_query is not None:
            resetAggregateResultsDebug(env)
            t_query.join(timeout=10)
        _reset(env)


def _hybrid_index(env):
    env.expect('FT.CREATE', 'hidx', 'PREFIX', 1, 'h:', 'SCHEMA', 'name', 'TEXT',
               'v', 'VECTOR', 'FLAT', 6, 'TYPE', 'FLOAT32', 'DIM', 2, 'DISTANCE_METRIC', 'L2').ok()
    conn = getConnectionByEnv(env)
    for i in range(N_DOCS):
        conn.execute_command('HSET', f'h:{i}', 'name', 'hello',
                             'v', np.array([float(i), float(i)], dtype=np.float32).tobytes())


HYBRID = ['FT.HYBRID', 'hidx', 'SEARCH', 'hello', 'VSIM', '@v', '$BLOB',
          'PARAMS', 2, 'BLOB', np.array([0.0, 0.0], dtype=np.float32).tobytes()]


@skip(cluster=True)
@env_spec(protocol=3)
def test_hybrid_stored_results_replay_stop_truncates(env):
    """FT.HYBRID replays its buffered tail rows with the same per-row memory check."""
    try:
        _hybrid_index(env)
        for workers in [0, 2]:
            _set_policies(env, 'fail', 'return', workers)
            rows = len(env.cmd(*HYBRID)['results'])
            env.assertGreater(rows, 2)
            # The replay makes one call per row, after the tail's buffering calls; its second call
            # stops it.
            buffering_calls = _checkpoint_calls(env, HYBRID) - rows
            env.assertGreater(buffering_calls, 0, message=workers)
            _memory_pressure(env, 'AT_CALL', buffering_calls + 2)
            res = env.cmd(*HYBRID)
            env.assertEqual(len(res['results']), 1, message=(workers, res))
            env.assertEqual(res['warnings'], [COORD_OOM_WARNING], message=workers)
    finally:
        _reset(env)


@skip(cluster=True)
@env_spec(protocol=3)
def test_hybrid_buffered_results_stop_replies_by_on_oom_policy(env):
    """FT.HYBRID maps a stop while buffering its tail like the other commands: ON_OOM FAIL
    replies the OOM error; under ON_OOM RETURN the tail keeps collecting what its merger
    already holds and replies it with the OOM warning."""
    try:
        _hybrid_index(env)
        for workers in [0, 2]:
            _set_policies(env, 'return', 'fail', workers)
            errors, warnings = _oom_counters(env)
            # Under ON_OOM FAIL each subquery's build is a checkpoint call: call 4 is the tail's
            # second buffered row.
            _memory_pressure(env, 'AT_CALL', 4)
            env.expect(*HYBRID).error().contains(OOM_QUERY_ERROR)
            env.assertEqual(_oom_counters(env), (errors + 1, warnings), message=workers)

            _set_policies(env, 'fail', 'return', workers)
            rows = len(env.cmd(*HYBRID)['results'])
            _memory_pressure(env, 'AT_CALL', 2)
            res = env.cmd(*HYBRID)
            env.assertEqual(len(res['results']), rows, message=(workers, res))
            env.assertEqual(res['warnings'], [COORD_OOM_WARNING], message=workers)
    finally:
        _reset(env)


@skip(cluster=False)
@env_spec(protocol=3)
def test_buffered_results_stop_through_the_coordinator(env):
    """A shard stopped while buffering (ON_TIMEOUT FAIL buffers) ends the cluster query per
    ON_OOM: the OOM error under FAIL, the rows kept with the coordinator OOM warning under
    RETURN. Shard 2 runs no coordinator pipeline, so its checkpoint calls are its own: after
    its build checkpoints, call B + 1 is its first buffered row."""
    _index_with_docs(env)
    shard = env.getConnection(2)

    def run():
        shard.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', B + 1)
        try:
            return env.cmd(*AGGREGATE)
        finally:
            shard.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RESET')

    run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'fail')
    try:
        allShards_change_oom_policy(env, 'fail')
        with env.assertResponseError(contained=OOM_QUERY_ERROR):
            run()

        allShards_change_oom_policy(env, 'return')
        res = run()
        env.assertEqual(res['warning'], [COORD_OOM_WARNING], message=res)
    finally:
        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return')
        allShards_change_oom_policy(env, 'return')
