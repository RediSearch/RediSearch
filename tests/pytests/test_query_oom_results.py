# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Query memory checkpoints in the result buffering, reply and dump paths, driven by
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
        rows = len(env.cmd(*HYBRID)['results'])

        def tail_buffering_call(n):
            # The tail buffers one checkpoint call per row and the replay follows with one per row:
            # those are the query's last 2 * rows calls.
            return _checkpoint_calls(env, HYBRID) - 2 * rows + n

        for workers in [0, 2]:
            _set_policies(env, 'return', 'fail', workers)
            errors, warnings = _oom_counters(env)
            _memory_pressure(env, 'AT_CALL', tail_buffering_call(2))
            env.expect(*HYBRID).error().contains(OOM_QUERY_ERROR)
            env.assertEqual(_oom_counters(env), (errors + 1, warnings), message=workers)

            _set_policies(env, 'fail', 'return', workers)
            _memory_pressure(env, 'AT_CALL', tail_buffering_call(2))
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


@skip(cluster=True)
@env_spec(protocol=3)
def test_reply_loop_stop_truncates(env):
    """A query that streams its rows into the reply checks memory before each row after the
    first. A stop there ends the stream at the index, with the OOM warning: a plain FT.AGGREGATE
    without workers keeps the rows already replied (the reply loop's 3rd call, B + 3, cuts after
    3 rows; RETURN-STRICT streams too without workers). Where stages hold rows (the safe loader
    with workers, FT.SEARCH's sorter) every stop position replies either everything or what was
    kept with the warning, as for any ON_OOM RETURN stop."""
    try:
        _index_with_docs(env)
        search = ['FT.SEARCH', 'idx', '*']
        for on_timeout in ['return', 'return-strict']:
            _set_policies(env, on_timeout, 'return', 0)
            errors, warnings = _oom_counters(env)
            _memory_pressure(env, 'AT_CALL', B + 3)
            res = env.cmd(*AGGREGATE)
            env.assertEqual(len(res['results']), 3, message=(on_timeout, res))
            env.assertEqual(res['warning'], [COORD_OOM_WARNING], message=on_timeout)
            env.assertEqual(_oom_counters(env), (errors, warnings + 1), message=on_timeout)

        for query, workers in [(AGGREGATE, 2), (search, 0), (search, 2)]:
            _set_policies(env, 'return', 'return', workers)
            warned = 0
            for at_call in range(B + 1, _checkpoint_calls(env, query) + 1):
                _memory_pressure(env, 'AT_CALL', at_call)
                res = env.cmd(*query)
                rows = len(res['results'])
                env.assertTrue(res['warning'] == [COORD_OOM_WARNING] or
                               (rows == N_DOCS and res['warning'] == []),
                               message=(query, workers, at_call, res))
                warned += res['warning'] == [COORD_OOM_WARNING]
            env.assertGreater(warned, 0, message=(query, workers))

        change_oom_policy(env, 'ignore')
        _memory_pressure(env, 'AT_CALL', B + 3)
        res = env.cmd(*AGGREGATE)
        env.assertEqual((len(res['results']), res['warning']), (N_DOCS, []), message=res)
    finally:
        _reset(env)


@skip(cluster=True)
@env_spec(protocol=2)
def test_reply_loop_stop_truncates_resp2(env):
    """RESP2 has no warning channel: a reply loop stopped for OOM is a silent partial result."""
    try:
        _index_with_docs(env)
        _memory_pressure(env, 'AT_CALL', B + 3)
        env.assertEqual(len(env.cmd(*AGGREGATE)) - 1, 3)
    finally:
        _reset(env)


@skip(cluster=True)
@env_spec(protocol=3)
def test_reply_loop_stop_ends_the_cursor(env):
    """A cursor chunk stopped in the reply loop ends the cursor instead of resuming later."""
    try:
        _index_with_docs(env)
        query = [*AGGREGATE, 'WITHCURSOR', 'COUNT', 4]
        # The chunk's reply loop makes 3 calls, the last ones of the query.
        _memory_pressure(env, 'AT_CALL', _checkpoint_calls(env, query) - 3 + 2)
        res, cursor = env.cmd(*query)
        env.assertEqual(len(res['results']), 2, message=res)
        env.assertEqual(res['warning'], [COORD_OOM_WARNING])
        env.assertEqual(cursor, 0)
    finally:
        _reset(env)


H_RRF = ['FT.HYBRID', 'hidx', 'SEARCH', '@name:hello', 'VSIM', '@v', '$BLOB', 'COMBINE', 'RRF', 2,
         'WINDOW', 20, 'LOAD', 1, '@name', 'LIMIT', 0, 20, 'PARAMS', 2, 'BLOB',
         np.array([0.0, 0.0], dtype=np.float32).tobytes()]
H_RANGE = ['FT.HYBRID', 'hidx', 'SEARCH', '@name:hello', 'VSIM', '@v', '$BLOB', 'RANGE', 2,
           'RADIUS', 1000, 'COMBINE', 'RRF', 2, 'WINDOW', 20, 'LIMIT', 0, 20, 'PARAMS', 2, 'BLOB',
           np.array([0.0, 0.0], dtype=np.float32).tobytes()]
# The to_hybrid_fire shape (large K, WINDOW and LIMIT, loading); LOAD * would return the raw
# vector bytes, which the test client cannot decode.
H_KNN_LARGE = ['FT.HYBRID', 'hidx', 'SEARCH', 'hello', 'VSIM', '@v', '$BLOB', 'KNN', 2, 'K', 1000,
               'COMBINE', 'RRF', 2, 'WINDOW', 1000, 'LOAD', 1, '@name', 'LIMIT', 0, 1000,
               'PARAMS', 2, 'BLOB', np.array([0.0, 0.0], dtype=np.float32).tobytes()]
# No tail sorter: the reply pulls straight from the merger, which holds every merged row.
H_NOSORT = ['FT.HYBRID', 'hidx', 'SEARCH', '@name:hello', 'VSIM', '@v', '$BLOB', 'NOSORT',
            'LIMIT', 0, 20, 'PARAMS', 2, 'BLOB', np.array([0.0, 0.0], dtype=np.float32).tobytes()]
HYBRID_SHAPES = {'h_rrf': H_RRF, 'h_range': H_RANGE, 'h_knn_large': H_KNN_LARGE,
                 'h_nosort': H_NOSORT}


def _hybrid_reply(env, res):
    """(rows, warnings) of an FT.HYBRID reply in either protocol."""
    if isinstance(res, dict):
        return res['results'], res['warnings']
    return res[res.index('results') + 1], res[res.index('warnings') + 1]


def _sweep_hybrid(env, query, check):
    """Arms pressure at every checkpoint call the query makes (build, subquery and tail
    pipelines, reply) and checks each reply. `check(res, at_call, calls)`."""
    try:
        calls = _checkpoint_calls(env, query)
        env.assertGreater(calls, 0, message=query)
        for at_call in range(1, calls + 1):
            _memory_pressure(env, 'AT_CALL', at_call)
            try:
                res = env.cmd(*query)
            except redis_exceptions.ResponseError as e:
                res = e
            check(res, at_call, calls)
    finally:
        _memory_pressure(env, 'RESET')


def _hybrid_stop_shapes(env):
    """FT.HYBRID maps a stop for OOM anywhere in the query, at a subquery's build, in a subquery
    pipeline, in the tail or in the reply, per ON_OOM: under RETURN the rows kept with the OOM
    warning and no timeout warning; under FAIL the OOM error, except in the replay of the
    buffered result, which can only truncate and warn; IGNORE runs to the end."""
    try:
        _hybrid_index(env)
        for name, query in HYBRID_SHAPES.items():
            full = len(_hybrid_reply(env, env.cmd(*query))[0])
            env.assertGreater(full, 0, message=name)
            for workers in [0, 2]:
                _set_policies(env, 'return', 'return', workers)

                # A stop that lands after a stage read everything truncates nothing and warns of
                # nothing; any cut carries the OOM warning and never a timeout one.
                def returns_with_warning(res, at_call, calls):
                    env.assertFalse(isinstance(res, Exception), message=(name, workers, at_call, res))
                    rows, warnings = _hybrid_reply(env, res)
                    env.assertTrue(warnings == [COORD_OOM_WARNING] or
                                   (len(rows) == full and warnings == []),
                                   message=(name, workers, at_call, res))
                _sweep_hybrid(env, query, returns_with_warning)

                _set_policies(env, 'return', 'fail', workers)

                def fails_before_the_replay(res, at_call, calls):
                    if at_call <= calls - full:
                        env.assertTrue(isinstance(res, redis_exceptions.ResponseError) and
                                       OOM_QUERY_ERROR in str(res),
                                       message=(name, workers, at_call, calls, res))
                    else:
                        rows, warnings = _hybrid_reply(env, res)
                        env.assertEqual((len(rows), warnings),
                                        (at_call - (calls - full) - 1, [COORD_OOM_WARNING]),
                                        message=(name, workers, at_call, calls, res))
                _sweep_hybrid(env, query, fails_before_the_replay)

                _set_policies(env, 'return', 'ignore', workers)
                _memory_pressure(env, 'AT_CALL', 1)
                rows, warnings = _hybrid_reply(env, env.cmd(*query))
                env.assertEqual((len(rows), warnings), (full, []), message=(name, workers))
    finally:
        _reset(env)


@skip(cluster=True)
@env_spec(protocol=3)
def test_hybrid_stop_replies_by_on_oom_policy(env):
    _hybrid_stop_shapes(env)


@skip(cluster=True)
@env_spec(protocol=2)
def test_hybrid_stop_replies_by_on_oom_policy_resp2(env):
    _hybrid_stop_shapes(env)


@skip(cluster=True)
@env_spec(protocol=3)
def test_hybrid_build_stop_reads_no_row(env):
    """A SEARCH subquery stopped at its build checkpoint under ON_OOM RETURN reads no row, with
    or without workers: the reply holds the vector subquery's rows only, with the OOM warning."""
    try:
        env.expect('FT.CREATE', 'hidx', 'PREFIX', 1, 'h:', 'SCHEMA', 'name', 'TEXT',
                   'v', 'VECTOR', 'FLAT', 6, 'TYPE', 'FLOAT32', 'DIM', 2, 'DISTANCE_METRIC', 'L2').ok()
        conn = getConnectionByEnv(env)
        # The first doc the SEARCH subquery reads is the farthest from the vector query.
        for i in range(N_DOCS):
            far = float(N_DOCS - 1 - i)
            conn.execute_command('HSET', f'h:{i}', 'name', 'hello',
                                 'v', np.array([far, far], dtype=np.float32).tobytes())
        query = ['FT.HYBRID', 'hidx', 'SEARCH', 'hello', 'VSIM', '@v', '$BLOB', 'KNN', 2, 'K', 2,
                 'PARAMS', 2, 'BLOB', np.array([0.0, 0.0], dtype=np.float32).tobytes()]
        nearest = {f'h:{N_DOCS - 1}', f'h:{N_DOCS - 2}'}
        for workers in [0, 2]:
            _set_policies(env, 'return', 'return', workers)
            # Call 1 is the SEARCH subquery's build checkpoint.
            _memory_pressure(env, 'AT_CALL', 1)
            res = env.cmd(*query)
            _memory_pressure(env, 'RESET')
            env.assertEqual({r['__key'] for r in res['results']}, nearest, message=(workers, res))
            env.assertEqual(res['warnings'], [COORD_OOM_WARNING], message=(workers, res))
    finally:
        _reset(env)


def _hybrid_index_on_a_line(env, name_of):
    """N_DOCS docs on one vector axis, the i-th named `name_of(i)`."""
    env.expect('FT.CREATE', 'hidx', 'PREFIX', 1, 'h:', 'SCHEMA', 'name', 'TEXT',
               'v', 'VECTOR', 'FLAT', 6, 'TYPE', 'FLOAT32', 'DIM', 2, 'DISTANCE_METRIC', 'L2').ok()
    conn = getConnectionByEnv(env)
    for i in range(N_DOCS):
        conn.execute_command('HSET', f'h:{i}', 'name', name_of(i),
                             'v', np.array([float(i), 0.0], dtype=np.float32).tobytes())


def _expect_every_hybrid_cut_warns(env, query, keys):
    """Pressure at every checkpoint call of `query` in turn, with and without workers: a reply
    whose `keys` differ from the complete one carries the OOM warning, and some stop cuts."""
    try:
        for workers in [0, 2]:
            _set_policies(env, 'return', 'return', workers)
            complete = keys(env.cmd(*query))
            cuts = 0
            for at_call in range(1, _checkpoint_calls(env, query) + 1):
                _memory_pressure(env, 'AT_CALL', at_call)
                res = env.cmd(*query)
                _memory_pressure(env, 'RESET')
                if keys(res) != complete:
                    cuts += 1
                    env.assertEqual(res['warnings'], [COORD_OOM_WARNING],
                                    message=(workers, at_call, res))
            env.assertGreater(cuts, 0, message=workers)
    finally:
        _reset(env)


@skip(cluster=True)
@env_spec(protocol=3)
def test_hybrid_subquery_stop_filling_the_window_warns(env):
    """A subquery sorter stopped while filling holds at most WINDOW rows, so the merger can take
    them all without reading the subquery's stop."""
    _hybrid_index_on_a_line(env, lambda i: ' '.join(['hello'] * (i + 1)))
    query = ['FT.HYBRID', 'hidx', 'SEARCH', 'hello', 'VSIM', '@v', '$BLOB',
             'COMBINE', 'RRF', 2, 'WINDOW', 4, 'LIMIT', 0, N_DOCS,
             'PARAMS', 2, 'BLOB', np.array([0.0, 0.0], dtype=np.float32).tobytes()]
    _expect_every_hybrid_cut_warns(env, query,
                                   lambda res: sorted(r['__key'] for r in res['results']))


@skip(cluster=True)
@env_spec(protocol=3)
def test_hybrid_safe_depleter_stop_past_the_window_warns(env):
    """A safe depleter (WORKERS) stopped under ON_OOM RETURN after buffering more than WINDOW
    rows keeps them, and the merger takes only WINDOW without reading the stop: the reply must
    still carry the OOM warning. With two depleter threads which checkpoint a late call lands on
    varies per run, and a stop there may cut nothing, so every call is swept against the
    invariant: a cut reply carries the OOM warning, an uncut one is the complete reply."""
    window = 4
    _hybrid_index_on_a_line(env, lambda i: ' '.join(['hello'] * (i + 1)))
    query = ['FT.HYBRID', 'hidx', 'SEARCH', 'hello', 'VSIM', '@v', '$BLOB',
             'COMBINE', 'RRF', 2, 'WINDOW', window, 'LIMIT', 0, N_DOCS,
             'PARAMS', 2, 'BLOB', np.array([0.0, 0.0], dtype=np.float32).tobytes()]
    try:
        _set_policies(env, 'return', 'return', 2)
        complete = sorted(r['__key'] for r in env.cmd(*query)['results'])
        env.assertGreater(len(complete), window)
        warned = 0
        for at_call in range(1, _checkpoint_calls(env, query) + 1):
            _memory_pressure(env, 'AT_CALL', at_call)
            res = env.cmd(*query)
            _memory_pressure(env, 'RESET')
            if res['warnings'] == [COORD_OOM_WARNING]:
                warned += 1
            else:
                env.assertEqual(res['warnings'], [], message=(at_call, res))
                env.assertEqual(sorted(r['__key'] for r in res['results']), complete,
                                message=(at_call, res))
        env.assertGreater(warned, 0)
    finally:
        _reset(env)

@skip(cluster=True)
@env_spec(protocol=3)
def test_hybrid_stop_past_the_offset_warns(env):
    """A tail sorter stopped while filling holds OFFSET + LIMIT rows at most, so the pager can end
    the reply before the stop is read."""
    _hybrid_index_on_a_line(env, lambda i: f'hello{i}')
    query = ['FT.HYBRID', 'hidx', 'SEARCH', 'hello*', 'VSIM', '@v', '$BLOB', 'LIMIT', 2, 2,
             'PARAMS', 2, 'BLOB', np.array([0.0, 0.0], dtype=np.float32).tobytes()]
    _expect_every_hybrid_cut_warns(env, query, lambda res: [r['__key'] for r in res['results']])


@skip(cluster=True)
@env_spec(protocol=3)
def test_hybrid_tail_stop_fails_over_a_soft_tail_error(env):
    """A tail APPLY on a field no doc has records a soft error (it only warns); under ON_OOM FAIL
    a stop at any checkpoint still fails the query with the OOM error, never a reply with rows."""
    try:
        _hybrid_index(env)
        query = ['FT.HYBRID', 'hidx', 'SEARCH', 'hello', 'VSIM', '@v', '$BLOB',
                 'LOAD', 1, '@opt', 'APPLY', '@opt', 'AS', 'x',
                 'PARAMS', 2, 'BLOB', np.array([0.0, 0.0], dtype=np.float32).tobytes()]
        for workers in [0, 2]:
            _set_policies(env, 'return', 'fail', workers)
            complete = env.cmd(*query)
            failures = 0

            def fails_or_is_complete(res, at_call, calls):
                nonlocal failures
                if isinstance(res, redis_exceptions.ResponseError):
                    env.assertContains(OOM_QUERY_ERROR, str(res), message=(workers, at_call, res))
                    failures += 1
                else:
                    env.assertEqual(res, complete, message=(workers, at_call, res))
            _sweep_hybrid(env, query, fails_or_is_complete)
            env.assertGreater(failures, 0, message=workers)
    finally:
        _reset(env)


@skip(cluster=True)
@env_spec(protocol=3)
def test_hybrid_reply_loop_stop_truncates(env):
    """FT.HYBRID streams its tail rows into the reply with the same per-row check; a stop there
    ends the stream at the tail's merger, so some stop positions keep a strict prefix of the
    rows with the OOM warning, and none replies a cut without it."""
    try:
        _hybrid_index(env)
        for workers in [0, 2]:
            _set_policies(env, 'return', 'return', workers)
            full = len(env.cmd(*HYBRID)['results'])
            cuts = 0
            for at_call in range(1, _checkpoint_calls(env, HYBRID) + 1):
                _memory_pressure(env, 'AT_CALL', at_call)
                res = env.cmd(*HYBRID)
                rows = len(res['results'])
                env.assertTrue(res['warnings'] == [COORD_OOM_WARNING] or
                               (rows == full and res['warnings'] == []),
                               message=(workers, at_call, res))
                cuts += 0 < rows < full
            env.assertGreater(cuts, 0, message=workers)
    finally:
        _reset(env)


@skip(cluster=False)
@env_spec(protocol=3)
def test_reply_loop_stop_on_a_shard_reaches_the_coordinator(env):
    """A shard stopped in its reply loop ends its rows with the shard OOM warning, which the
    coordinator replies as its own. Shard 2 runs no coordinator pipeline, so its calls are its
    own; its reply loop's are its last."""
    _index_with_docs(env)
    shard = env.getConnection(2)
    shard.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', NEVER)
    env.cmd(*AGGREGATE)
    calls = shard.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'STATUS')[5]
    env.assertGreater(calls, B, message=calls)
    shard.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', calls)
    try:
        res = env.cmd(*AGGREGATE)
    finally:
        shard.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RESET')
    env.assertEqual(res['warning'], [COORD_OOM_WARNING], message=res)


@skip(cluster=False)
@env_spec(protocol=3)
def test_hybrid_shard_stop_replies_by_on_oom_policy(env):
    """A shard's FT.HYBRID subqueries run the memory checkpoints too; stopped for OOM anywhere
    (build, depletion, cursor reads), the shard reports it in the shard wording, never as a
    timeout, and the coordinator replies per ON_OOM: under RETURN the rows kept with its OOM
    warning, under FAIL the OOM error (a replay stop only warns). Shard 2 runs no coordinator pipeline, so its calls are its
    own; every one of them is tried, under both ON_TIMEOUT RETURN and FAIL, with and without
    workers (background safe depleters, whose stop must outlive the cursor reads that follow)."""
    _hybrid_index(env)
    full = len(env.cmd(*HYBRID)['results'])
    shard = env.getConnection(2)
    try:
        for workers in [0, 2]:
            run_command_on_all_shards(env, config_cmd(), 'SET', 'WORKERS', workers)
            _hybrid_shard_stop_sweep(env, shard, full, workers)
    finally:
        run_command_on_all_shards(env, config_cmd(), 'SET', 'WORKERS', 0)


def _hybrid_shard_stop_sweep(env, shard, full, workers):
    def shard_calls():
        shard.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', NEVER)
        env.cmd(*HYBRID)
        calls = shard.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'STATUS')[5]
        shard.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RESET')
        return calls

    def run_at(at_call):
        shard.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', at_call)
        try:
            return env.cmd(*HYBRID)
        except redis_exceptions.ResponseError as e:
            return e
        finally:
            shard.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RESET')

    try:
        for on_timeout in ['return', 'fail']:
            run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, on_timeout)
            allShards_change_oom_policy(env, 'return')
            calls = shard_calls()
            env.assertGreater(calls, 0)
            for at_call in range(1, calls + 1):
                res = run_at(at_call)
                env.assertFalse(isinstance(res, Exception),
                                message=(workers, on_timeout, at_call, res))
                env.assertTrue(res['warnings'] == [COORD_OOM_WARNING] or
                               (len(res['results']) == full and res['warnings'] == []),
                               message=(workers, on_timeout, at_call, res))

            # Under FAIL a stop in the replay of a shard's buffered cursor read can only warn, as
            # standalone; everywhere else it is the OOM error.
            allShards_change_oom_policy(env, 'fail')
            # FAIL under ON_TIMEOUT RETURN buffers and replays, so it has checkpoints RETURN lacks.
            calls = shard_calls()
            errors = 0
            for at_call in range(1, calls + 1):
                res = run_at(at_call)
                if isinstance(res, Exception):
                    env.assertContains(OOM_QUERY_ERROR, str(res),
                                       message=(workers, on_timeout, at_call))
                    errors += 1
                else:
                    env.assertTrue(res['warnings'] == [COORD_OOM_WARNING] or
                                   (len(res['results']) == full and res['warnings'] == []),
                                   message=(workers, on_timeout, at_call, res))
            env.assertGreater(errors, 0, message=(workers, on_timeout))
    finally:
        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return')
        allShards_change_oom_policy(env, 'return')


@skip(cluster=False)
@env_spec(protocol=3)
def test_geoshape_collection_stop_on_a_shard_reaches_the_coordinator(env):
    """A shard whose GEOSHAPE collection stops for OOM answers like any shard stopped at build,
    and the coordinator replies per ON_OOM: the OOM error under FAIL, the other shards' rows
    with its OOM warning under RETURN. Shard 2 runs no coordinator pipeline; its first build
    checkpoint precedes the iterators, so its call 2 is the collection's first id."""
    env.expect('FT.CREATE', 'gidx', 'SCHEMA', 'geom', 'GEOSHAPE', 'FLAT').ok()
    conn = getConnectionByEnv(env)
    for i in range(N_DOCS):
        conn.execute_command('HSET', f'g{i}', 'geom', 'POLYGON((0 0, 0 20, 20 20, 20 0, 0 0))')
    query = ['FT.AGGREGATE', 'gidx', '@geom:[within $shape]', 'PARAMS', 2, 'shape',
             'POLYGON((-1 -1, -1 21, 21 21, 21 -1, -1 -1))', 'LOAD', 1, '@__key', 'DIALECT', 3]
    shard = env.getConnection(2)
    # Docs are spread by slot, so the survivors are every doc not stored on the stopped shard.
    stopped_keys = {k.decode() if isinstance(k, bytes) else k
                    for k in shard.execute_command('KEYS', 'g*')}
    expected_keys = {f'g{i}' for i in range(N_DOCS)} - stopped_keys

    def run():
        shard.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', 2)
        try:
            return env.cmd(*query)
        finally:
            shard.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RESET')

    try:
        allShards_change_oom_policy(env, 'fail')
        with env.assertResponseError(contained=OOM_QUERY_ERROR):
            run()
        allShards_change_oom_policy(env, 'return')
        res = run()
        env.assertEqual(res['warning'], [COORD_OOM_WARNING], message=res)
        # Counting rows alone would pass an implementation that dropped every shard's rows.
        env.assertEqual({row['extra_attributes']['__key'] for row in res['results']},
                        expected_keys, message=res)
    finally:
        allShards_change_oom_policy(env, 'return')


@skip(cluster=True)
@env_spec(protocol=3)
def test_hybrid_subquery_stop_follows_on_oom_under_timeout_fail(env):
    """A subquery stopped for OOM follows ON_OOM, not ON_TIMEOUT, in its depleter and in the
    merger, with and without workers. Both subqueries match every document here, so under
    ON_TIMEOUT FAIL with ON_OOM RETURN a stop at any of the first calls (subquery build and
    depletion, which come before the tail's) still replies every document, through the other
    subquery, with the OOM warning. Under ON_OOM FAIL those calls reply the OOM error."""
    _hybrid_index(env)
    full = len(env.cmd(*HYBRID)['results'])
    try:
        for workers in [0, 2]:
            _set_policies(env, 'fail', 'return', workers)
            calls = _checkpoint_calls(env, HYBRID)
            subquery_calls = 0
            for at_call in range(1, calls + 1):
                _memory_pressure(env, 'AT_CALL', at_call)
                res = env.cmd(*HYBRID)
                _memory_pressure(env, 'RESET')
                # The merger's own consume calls follow the subqueries': they keep only the rows
                # read so far.
                if res['warnings'] == [] or len(res['results']) < full:
                    break
                env.assertEqual(len(res['results']), full, message=(workers, at_call, res))
                env.assertEqual(res['warnings'], [COORD_OOM_WARNING], message=(workers, at_call))
                subquery_calls += 1
            env.assertGreater(subquery_calls, 2, message=workers)

            _set_policies(env, 'fail', 'fail', workers)
            for at_call in range(1, subquery_calls + 1):
                _memory_pressure(env, 'AT_CALL', at_call)
                try:
                    env.expect(*HYBRID).error().contains(OOM_QUERY_ERROR)
                finally:
                    _memory_pressure(env, 'RESET')
    finally:
        _reset(env)


@skip(cluster=True)
@env_spec(protocol=3)
def test_hybrid_merger_consume_stop_replies_by_on_oom_policy(env):
    """The merger checks memory per row it takes from its upstreams, which already ended their
    own rows. Its stop follows ON_OOM, never ON_TIMEOUT, with and without workers: under RETURN
    the rows taken so far are the reply with the OOM warning (a stop at the first row of the
    last upstream loses the rest of its window), under FAIL the reply is the OOM error. The
    last upstream's consume calls are the WINDOW calls before the tail's buffering and replay
    calls (one each per merged row); the first upstream's may interleave with a subquery's."""
    window = 4
    query = ['FT.HYBRID', 'hidx', 'SEARCH', 'hello', 'VSIM', '@v', '$BLOB',
             'COMBINE', 'RRF', 2, 'WINDOW', window, 'LIMIT', 0, N_DOCS,
             'PARAMS', 2, 'BLOB', np.array([0.0, 0.0], dtype=np.float32).tobytes()]
    _hybrid_index_on_a_line(env, lambda i: ' '.join(['hello'] * (i + 1)))
    try:
        for workers in [0, 2]:
            _set_policies(env, 'return', 'return', workers)
            full = len(env.cmd(*query)['results'])
            env.assertGreater(full, window, message=workers)
            last_upstream_call = _checkpoint_calls(env, query) - 2 * full - window

            for on_timeout, on_oom in [('return', 'return'), ('fail', 'return'),
                                       ('return', 'fail'), ('fail', 'fail')]:
                _set_policies(env, on_timeout, on_oom, workers)
                for n in range(1, window + 1):
                    _memory_pressure(env, 'AT_CALL', last_upstream_call + n)
                    errors, warnings = _oom_counters(env)
                    message = (workers, on_timeout, on_oom, n)
                    if on_oom == 'fail':
                        env.expect(*query).error().contains(OOM_QUERY_ERROR)
                        env.assertEqual(_oom_counters(env), (errors + 1, warnings),
                                        message=message)
                    else:
                        res = env.cmd(*query)
                        env.assertEqual(res['warnings'], [COORD_OOM_WARNING], message=message)
                        env.assertLessEqual(len(res['results']), full, message=message)
                        if n == 1:
                            env.assertLess(len(res['results']), full, message=message)
                    _memory_pressure(env, 'RESET')
    finally:
        _reset(env)


@skip(cluster=True)
@env_spec(protocol=3)
def test_geoshape_collection_stop_replies_by_on_oom_policy(env):
    """A GEOSHAPE query collects every matching id while its iterator is built; a stop for OOM
    there abandons the collection, and the query ends like any stop at build: ON_OOM FAIL
    replies the OOM error, ON_OOM RETURN an empty result with the OOM warning, for each
    predicate, with or without workers. ON_OOM IGNORE lets it finish. The first build
    checkpoint comes before the iterators, so call 1 + N_DOCS is the collection's last id: only
    the collection's checkpoint can empty the reply there (later calls are the pipeline's and
    the reply's, past the first rows)."""
    try:
        env.expect('FT.CREATE', 'gidx', 'SCHEMA', 'geom', 'GEOSHAPE', 'FLAT').ok()
        conn = getConnectionByEnv(env)
        for i in range(N_DOCS):
            conn.execute_command('HSET', f'g{i}', 'geom', 'POLYGON((0 0, 0 20, 20 20, 20 0, 0 0))')
        queries = {
            'within': 'POLYGON((-1 -1, -1 21, 21 21, 21 -1, -1 -1))',
            'contains': 'POINT(5 5)',
            'intersects': 'POINT(5 5)',
            'disjoint': 'POLYGON((100 100, 100 110, 110 110, 110 100, 100 100))',
        }
        for predicate, shape in queries.items():
            query = ['FT.SEARCH', 'gidx', f'@geom:[{predicate} $shape]', 'PARAMS', 2, 'shape', shape,
                     'NOCONTENT', 'DIALECT', 3]
            env.assertEqual(env.cmd(*query)['total_results'], N_DOCS, message=predicate)
            for workers in [0, 2]:
                _set_policies(env, 'return', 'fail', workers)
                errors, warnings = _oom_counters(env)
                _memory_pressure(env, 'AT_CALL', 1 + N_DOCS)
                env.expect(*query).error().equal(f'SEARCH_OOM {OOM_QUERY_ERROR}')
                env.assertEqual(_oom_counters(env), (errors + 1, warnings),
                                message=(predicate, workers))

                _set_policies(env, 'return', 'return', workers)
                _memory_pressure(env, 'AT_CALL', 1 + N_DOCS)
                res = env.cmd(*query)
                env.assertEqual((res['results'], res['warning']), ([], [COORD_OOM_WARNING]),
                                message=(predicate, workers, res))

            _set_policies(env, 'return', 'ignore', 0)
            _memory_pressure(env, 'AT_CALL', 1 + N_DOCS)
            env.assertEqual(env.cmd(*query)['total_results'], N_DOCS, message=predicate)
    finally:
        _reset(env)


def _open_cursors(env, index='idx'):
    """Client and coordinator-to-shard cursors on `index`. On the coordinator FT.INFO sums the
    shards, so hybrid shard subquery cursors (counted as `index_total` on each shard) are included."""
    info = env.cmd('FT.INFO', index)
    stats = info['cursor_stats'] if isinstance(info, dict) else to_dict(to_dict(info)['cursor_stats'])
    return int(stats['index_total']) + int(stats.get('index_total_internal', 0))


def _expect_no_open_cursors(env, message, index='idx'):
    # Shard cursors are freed asynchronously after the coordinator's cursor ends.
    with TimeLimit(10, f'cursor left open: {message}'):
        while _open_cursors(env, index):
            time.sleep(0.05)


def _is_oom_error(v):
    return isinstance(v, redis_exceptions.ResponseError) and OOM_QUERY_ERROR in str(v)


def _split_dump(reply):
    """A dump stopped for memory ends with the OOM error in place of its next value: returns the
    values before it and whether that last element is the OOM error. A RESP3 set may come back as
    a Python set, which has no order: there the OOM error only has to be one of its elements.
    A RESP3 map is flattened to key, value order. A stop between FT.SYNDUMP group IDs puts the
    error at the end of the last term's group array: it counts as stopped, and that array is
    returned without the error."""
    if isinstance(reply, dict):
        reply = [x for kv in reply.items() for x in kv]
    if isinstance(reply, (set, frozenset)):
        errors = [v for v in reply if isinstance(v, redis_exceptions.ResponseError)]
        stopped = len(errors) == 1 and OOM_QUERY_ERROR in str(errors[0])
        if len(errors) > int(stopped):
            raise AssertionError(f'unexpected error elements: {reply}')
        return [v for v in reply if not isinstance(v, redis_exceptions.ResponseError)], stopped
    reply = list(reply)
    last = reply[-1] if reply else None
    stopped = isinstance(last, redis_exceptions.ResponseError) and OOM_QUERY_ERROR in str(last)
    values = reply[:-1] if stopped else reply
    if isinstance(last, list) and last and _is_oom_error(last[-1]):
        stopped = True
        values = reply[:-1] + [last[:-1]]
        if any(isinstance(v, redis_exceptions.ResponseError) for v in last[:-1]):
            raise AssertionError(f'error element before the end: {reply}')
    if any(isinstance(v, redis_exceptions.ResponseError) for v in values):
        raise AssertionError(f'error element before the end: {reply}')
    return values, stopped


@skip(cluster=True)
def test_dumps_stop_with_the_oom_error(env):
    """FT.TAGVALS, FT.DICTDUMP and FT.SYNDUMP stream every value into a reply that has already
    started when memory runs out, so a stop replies the OOM error in place of the next value.
    ON_OOM IGNORE dumps everything."""
    try:
        env.expect('FT.CREATE', 'tidx', 'SCHEMA', 't', 'TAG').ok()
        conn = getConnectionByEnv(env)
        for i in range(N_DOCS):
            conn.execute_command('HSET', f't{i}', 't', f'tag{i}')
            conn.execute_command('FT.DICTADD', 'dict', f'term{i}')
            conn.execute_command('FT.SYNUPDATE', 'tidx', f'group{i}', f'syn{i}')
        dumps = [('FT.TAGVALS', 'tidx', 't'), ('FT.DICTDUMP', 'dict'), ('FT.SYNDUMP', 'tidx')]
        for dump in dumps:
            env.assertEqual(len(env.cmd(*dump)), N_DOCS * (2 if dump[0] == 'FT.SYNDUMP' else 1))

        # RESP2 flattens the FT.SYNDUMP map: term, groups, term, groups, term, error.
        expected_values = {'FT.TAGVALS': 2, 'FT.DICTDUMP': 2, 'FT.SYNDUMP': 5}
        for on_oom in ['return', 'fail']:
            change_oom_policy(env, on_oom)
            for dump in dumps:
                _memory_pressure(env, 'AT_CALL', 3)
                res = env.cmd(*dump)
                values, stopped = _split_dump(res)
                env.assertEqual((len(values), stopped), (expected_values[dump[0]], True),
                                message=(on_oom, res))

        change_oom_policy(env, 'ignore')
        for dump in dumps:
            _memory_pressure(env, 'AT_CALL', 3)
            env.assertFalse(_split_dump(env.cmd(*dump))[1], message=dump)
    finally:
        _reset(env)


@skip(cluster=True)
@env_spec(protocol=3)
def test_dumps_stop_with_the_oom_error_resp3(env):
    """RESP3 carries the stopped dump's OOM error inside its set or map the same way."""
    try:
        env.expect('FT.CREATE', 'tidx', 'SCHEMA', 't', 'TAG').ok()
        conn = getConnectionByEnv(env)
        for i in range(N_DOCS):
            conn.execute_command('HSET', f't{i}', 't', f'tag{i}')
            conn.execute_command('FT.DICTADD', 'dict', f'term{i}')
            conn.execute_command('FT.SYNUPDATE', 'tidx', f'group{i}', f'syn{i}')
        _memory_pressure(env, 'AT_CALL', 3)
        res = env.cmd('FT.TAGVALS', 'tidx', 't')
        values, stopped = _split_dump(res)
        env.assertEqual((len(values), stopped), (2, True), message=res)
        _memory_pressure(env, 'AT_CALL', 3)
        res = env.cmd('FT.DICTDUMP', 'dict')
        values, stopped = _split_dump(res)
        env.assertEqual((len(values), stopped), (2, True), message=res)
        env.assertTrue(all(v.startswith('term') for v in values), message=res)
        _memory_pressure(env, 'AT_CALL', 3)
        res = env.cmd('FT.SYNDUMP', 'tidx')
        env.assertEqual(len(res), 3, message=res)
        env.assertEqual(_split_dump(res.values())[1], True, message=res)
    finally:
        _reset(env)


@skip(cluster=True)
def test_syndump_counts_group_ids_toward_the_memory_read(env):
    """A term can belong to thousands of groups, and FT.SYNDUMP checkpoints once per group ID: the
    read interval counts the group IDs replied, so pressure is read within the dump even when it
    has fewer terms than the interval."""
    env.expect('FT.CREATE', 'tidx', 'SCHEMA', 't', 'TAG').ok()
    terms, groups = 3, 60
    for g in range(groups):
        env.cmd('FT.SYNUPDATE', 'tidx', f'group{g}', *[f'term{i}' for i in range(terms)])
    env.assertEqual(len(env.cmd('FT.SYNDUMP', 'tidx')), 2 * terms)
    try:
        _memory_pressure(env, 'READ_INTERVAL', 100)
        _memory_pressure(env, 'RATIO', 2)
        values, stopped = _split_dump(env.cmd('FT.SYNDUMP', 'tidx'))
        env.assertTrue(stopped, message=values)
        env.assertLess(len(values), 2 * terms - 1, message=values)
    finally:
        _reset(env)


def _syndump_checks_pressure_per_group_id(env):
    """A term in more groups than the read interval gets a checkpoint per group ID, not one per
    term: pressure that lands on a later ID ends the dump with the OOM error in place of that
    term's groups, and the reply stays a well-formed map in both protocols."""
    def flat(res):
        # RESP3 decodes the map to a dict: flatten it to RESP2's term, groups, term, groups order.
        if isinstance(res, dict):
            return [x for kv in res.items() for x in kv]
        return res

    env.expect('FT.CREATE', 'tidx', 'SCHEMA', 't', 'TAG').ok()
    groups = 300
    for g in range(groups):
        env.cmd('FT.SYNUPDATE', 'tidx', f'group{g}', 'term0', 'term1')
    env.assertEqual(len(flat(env.cmd('FT.SYNDUMP', 'tidx'))), 4)
    try:
        # Calls 1..300 belong to the first term, 301..600 to the second.
        _memory_pressure(env, 'AT_CALL', groups + 100)
        res = flat(env.cmd('FT.SYNDUMP', 'tidx'))
        env.assertEqual(len(res), 4, message=res)
        env.assertEqual(len(res[1]), groups, message=res)
        env.assertEqual(len(res[3]), 100, message=res)
        env.assertTrue(isinstance(res[3][-1], redis_exceptions.ResponseError)
                       and OOM_QUERY_ERROR in str(res[3][-1]), message=res)
        # A stop inside the first term's IDs leaves only that term, its array holding the IDs
        # written before the stop and closed by the error.
        _memory_pressure(env, 'AT_CALL', 100)
        res = flat(env.cmd('FT.SYNDUMP', 'tidx'))
        env.assertEqual(len(res), 2, message=res)
        env.assertEqual(len(res[1]), 100, message=res)
        env.assertTrue(isinstance(res[1][-1], redis_exceptions.ResponseError)
                       and OOM_QUERY_ERROR in str(res[1][-1]), message=res)
        env.assertFalse(any(isinstance(v, redis_exceptions.ResponseError)
                            for v in res[1][:-1]), message=res)
    finally:
        _reset(env)


@skip(cluster=True)
def test_syndump_checks_pressure_per_group_id(env):
    _syndump_checks_pressure_per_group_id(env)


@skip(cluster=True)
@env_spec(protocol=3)
def test_syndump_checks_pressure_per_group_id_resp3(env):
    _syndump_checks_pressure_per_group_id(env)


@skip(cluster=False)
def test_tagvals_shard_stop_through_the_coordinator(env):
    """The coordinator merges the shards' FT.TAGVALS values; a shard's OOM error element is not
    a value: the merged set ends with it, as a single shard's dump does, or it is the reply when
    no shard got to a value."""
    env.expect('FT.CREATE', 'tidx', 'SCHEMA', 't', 'TAG').ok()
    conn = getConnectionByEnv(env)
    for i in range(N_DOCS):
        conn.execute_command('HSET', f't{i}', 't', f'tag{i}')
    try:
        _memory_pressure(env, 'AT_CALL', 1)
        res = env.cmd('FT.TAGVALS', 'tidx', 't')
        values, stopped = _split_dump(res)
        env.assertTrue(stopped, message=res)
        env.assertLess(len(values), N_DOCS, message=res)
        env.assertTrue(all(v.startswith('tag') for v in values), message=res)

        # Every shard stopping before its first value is an error, not an empty field.
        run_command_on_all_shards(env, debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', 1)
        env.expect('FT.TAGVALS', 'tidx', 't').error().contains(OOM_QUERY_ERROR)
    finally:
        run_command_on_all_shards(env, debug_cmd(), 'MEMORY_PRESSURE', 'RESET')


@skip(cluster=False)
@env_spec(protocol=3)
def test_tagvals_shard_stop_through_the_coordinator_resp3(env):
    """Under RESP3 the shards' FT.TAGVALS sets reach the reducer as sets and the merged reply is a
    set: a shard's OOM error is one member of it, or the whole reply when no shard got to a value."""
    env.expect('FT.CREATE', 'tidx', 'SCHEMA', 't', 'TAG').ok()
    conn = getConnectionByEnv(env)
    for i in range(N_DOCS):
        conn.execute_command('HSET', f't{i}', 't', f'tag{i}')
    try:
        _memory_pressure(env, 'AT_CALL', 1)
        res = env.cmd('FT.TAGVALS', 'tidx', 't')
        values, stopped = _split_dump(res)
        env.assertTrue(stopped, message=res)
        env.assertLess(len(values), N_DOCS, message=res)
        env.assertTrue(all(v.startswith('tag') for v in values), message=res)

        run_command_on_all_shards(env, debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', 1)
        env.expect('FT.TAGVALS', 'tidx', 't').error().contains(OOM_QUERY_ERROR)
    finally:
        run_command_on_all_shards(env, debug_cmd(), 'MEMORY_PRESSURE', 'RESET')


@skip(cluster=False)
@env_spec(protocol=3)
def test_hybrid_coordinator_stop_replies_by_on_oom_policy(env):
    """The coordinator process runs the FT.HYBRID tail and reply loop plus its own shard's
    subqueries, all on one checkpoint counter that no scope splits. Sweeping every position of
    that counter reaches every coordinator checkpoint, whatever the thread interleaving, and
    each stop must reply per ON_OOM: under RETURN the complete reply or a strict prefix of it
    with the OOM warning, under FAIL the OOM error or such a prefix (a replay stop only warns),
    never a timeout, a foreign row or a duplicate."""
    _hybrid_index(env)
    coord = env.getConnection(1)

    def arm(n):
        coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', n)

    def disarm():
        coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RESET')

    # Keys only: a stopped subquery changes the fused scores of the rows that remain.
    def row_ids(res):
        return [row['__key'] for row in res['results']]

    full = row_ids(env.cmd(*HYBRID))
    env.assertGreater(len(full), 0)

    def run_at(at_call):
        arm(at_call)
        try:
            return env.cmd(*HYBRID)
        except redis_exceptions.ResponseError as e:
            return e
        finally:
            disarm()

    def check_reply(res, message):
        ids = row_ids(res)
        env.assertEqual(len(ids), len(set(ids)), message=message)
        env.assertTrue(set(ids) <= set(full), message=message)
        env.assertTrue(res['warnings'] == [COORD_OOM_WARNING] or
                       (ids == full and res['warnings'] == []), message=message)
        return res['warnings'] == [COORD_OOM_WARNING]

    try:
        for on_timeout in ['return', 'fail']:
            run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, on_timeout)
            allShards_change_oom_policy(env, 'return')
            arm(NEVER)
            env.cmd(*HYBRID)
            calls = coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'STATUS')[5]
            disarm()
            env.assertGreater(calls, 0, message=on_timeout)

            warned = 0
            for at_call in range(1, calls + 1):
                res = run_at(at_call)
                env.assertFalse(isinstance(res, Exception), message=(on_timeout, at_call, res))
                warned += check_reply(res, (on_timeout, at_call, res))
            env.assertGreater(warned, 0, message=on_timeout)

            allShards_change_oom_policy(env, 'fail')
            # FAIL under ON_TIMEOUT RETURN buffers and replays, so it has checkpoints RETURN lacks.
            arm(NEVER)
            env.cmd(*HYBRID)
            calls = coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'STATUS')[5]
            disarm()
            errors = 0
            for at_call in range(1, calls + 1):
                res = run_at(at_call)
                if isinstance(res, Exception):
                    env.assertContains(OOM_QUERY_ERROR, str(res), message=(on_timeout, at_call))
                    errors += 1
                else:
                    check_reply(res, (on_timeout, at_call, res))
            env.assertGreater(errors, 0, message=on_timeout)
    finally:
        disarm()
        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return')
        allShards_change_oom_policy(env, 'return')


@skip(cluster=False)
@env_spec(protocol=3)
def test_hybrid_coordinator_merger_stop_replies_by_on_oom_policy(env):
    """The coordinator's merger takes rows from its RPNet upstreams, one checkpoint per row.
    No scope isolates it from shard 1's own pipeline on the shared counter, so the sweep runs
    every call of a WINDOW query, which includes the merger's. Each stop must reply per ON_OOM
    (RETURN: the full reply or fewer rows with the OOM warning, FAIL: the OOM error or such a
    reply), and every query must finish: a stop that left an upstream undrained would
    hang the reply or leave a shard cursor open. A stop inside shard 1's own pipeline makes the
    coordinator re-rank the remaining replies, and with tied text scores the WINDOW top-k can
    pick rows the full reply lacks, so rows are only checked to be indexed keys, unique and at
    most one window per subquery."""
    window = 4
    query = ['FT.HYBRID', 'hidx', 'SEARCH', 'hello', 'VSIM', '@v', '$BLOB',
             'COMBINE', 'RRF', 2, 'WINDOW', window, 'LIMIT', 0, N_DOCS,
             'PARAMS', 2, 'BLOB', np.array([0.0, 0.0], dtype=np.float32).tobytes()]
    _hybrid_index(env)
    coord = env.getConnection(1)

    def arm(n):
        coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', n)

    def disarm():
        coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RESET')

    def row_ids(res):
        return [row['__key'] for row in res['results']]

    def run_at(at_call):
        arm(at_call)
        try:
            return env.cmd(*query)
        except redis_exceptions.ResponseError as e:
            return e
        finally:
            disarm()

    try:
        allShards_change_oom_policy(env, 'return')
        full = row_ids(env.cmd(*query))
        env.assertGreater(len(full), 0)

        for on_oom in ['return', 'fail']:
            allShards_change_oom_policy(env, on_oom)
            arm(NEVER)
            env.cmd(*query)
            calls = coord.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'STATUS')[5]
            disarm()
            env.assertGreater(calls, 0, message=on_oom)

            stops = 0
            for at_call in range(1, calls + 1):
                with TimeLimit(30, f'hybrid hung: {on_oom} {at_call}'):
                    res = run_at(at_call)
                message = (on_oom, at_call, res)
                if isinstance(res, Exception):
                    env.assertEqual(on_oom, 'fail', message=message)
                    env.assertContains(OOM_QUERY_ERROR, str(res), message=message)
                    stops += 1
                    continue
                ids = row_ids(res)
                env.assertEqual(len(ids), len(set(ids)), message=message)
                env.assertTrue(set(ids) <= {f'h:{i}' for i in range(N_DOCS)}, message=message)
                env.assertLessEqual(len(ids), 2 * window, message=message)
                env.assertTrue(res['warnings'] == [COORD_OOM_WARNING] or
                               (ids == full and res['warnings'] == []), message=message)
                stops += res['warnings'] == [COORD_OOM_WARNING]
            env.assertGreater(stops, 0, message=on_oom)
            _expect_no_open_cursors(env, f'coordinator merger stop {on_oom}', index='hidx')
    finally:
        disarm()
        allShards_change_oom_policy(env, 'return')
