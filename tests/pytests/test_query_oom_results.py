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
    env.cmd(*query)
    calls = _memory_pressure(env, 'STATUS')[5]
    _memory_pressure(env, 'RESET')
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
    _index_with_docs(env)
    for on_timeout in ['return', 'fail', 'return-strict']:
        for workers in [0, 2]:
            _set_policies(env, on_timeout, 'fail', workers)
            errors, warnings = _oom_counters(env)
            _memory_pressure(env, 'AT_CALL', B + 3)
            env.expect(*AGGREGATE).error().contains(OOM_QUERY_ERROR)
            env.assertEqual(_oom_counters(env), (errors + 1, warnings),
                            message=(on_timeout, workers))
    _reset(env)


@skip(cluster=True)
@env_spec(protocol=3)
def test_buffered_results_stop_returns_the_prefix(env):
    """Under a buffering ON_TIMEOUT policy and ON_OOM RETURN, a stop while buffering replies the
    rows buffered so far with the OOM warning instead of a timeout. RETURN-STRICT only buffers
    with workers; without them it streams like RETURN."""
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
    _reset(env)


@skip(cluster=True)
@env_spec(protocol=3)
def test_stored_results_replay_stop_truncates(env):
    """Replaying buffered rows checks memory per row: a stop there truncates the reply and warns,
    even under ON_OOM FAIL, whose error can only be decided while buffering."""
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
    _reset(env)


@skip(cluster=True)
@env_spec(protocol=2)
def test_stored_results_replay_stop_truncates_resp2(env):
    """RESP2 has no warning channel: a replay stopped for OOM is a silent partial result."""
    _index_with_docs(env)
    _set_policies(env, 'fail', 'return', 0)
    _memory_pressure(env, 'AT_CALL', B + 3)
    env.assertEqual(len(env.cmd(*AGGREGATE)) - 1, 3)
    _memory_pressure(env, 'AT_CALL', _replay_call(env, AGGREGATE, N_DOCS, 3))
    env.assertEqual(len(env.cmd(*AGGREGATE)) - 1, 2)
    _reset(env)


@skip(cluster=True)
@env_spec(protocol=3)
def test_stored_results_replay_stop_ends_the_cursor(env):
    """A cursor chunk cut short while replaying ends the cursor: resuming it would skip the rows
    the replay left out. A full chunk otherwise keeps the cursor open (no buffering checkpoint
    on the row that fills it)."""
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
    _reset(env)


@skip(cluster=True)
@env_spec(protocol=3)
def test_stored_results_replay_stop_ends_a_timed_out_cursor(env):
    """A buffered cursor chunk that timed out under ON_TIMEOUT RETURN is replied and normally
    keeps its cursor open; cut short while replaying, it ends the cursor too. ON_OOM FAIL makes
    the chunk buffer; TIMEOUT_AFTER_N times it out after 4 rows, which the replay's last 4
    checkpoint calls serialize."""
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
    _reset(env)


@skip(cluster=True)
@env_spec(protocol=3, moduleArgs='WORKERS 1 TIMEOUT 0')
def test_return_strict_drain_stop_truncates(env):
    """After a RETURN-STRICT timeout the main thread drains the sorter heap into the reply; the
    drain checks memory per row, although the request is already stopped by the timeout, and
    a stop there keeps what was drained and warns of both."""
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
    _reset(env)


@skip(cluster=True)
@env_spec(protocol=3)
def test_hybrid_buffered_results_stop_replies_by_on_oom_policy(env):
    """FT.HYBRID maps a stop while buffering its tail like the other commands: ON_OOM FAIL
    replies the OOM error; under ON_OOM RETURN the tail keeps collecting what its merger
    already holds and replies it with the OOM warning."""
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
    _reset(env)


@skip(cluster=True)
@env_spec(protocol=2)
def test_reply_loop_stop_truncates_resp2(env):
    """RESP2 has no warning channel: a reply loop stopped for OOM is a silent partial result."""
    _index_with_docs(env)
    _memory_pressure(env, 'AT_CALL', B + 3)
    env.assertEqual(len(env.cmd(*AGGREGATE)) - 1, 3)
    _reset(env)


@skip(cluster=True)
@env_spec(protocol=3)
def test_reply_loop_stop_ends_the_cursor(env):
    """A cursor chunk stopped in the reply loop ends the cursor instead of resuming later."""
    _index_with_docs(env)
    query = [*AGGREGATE, 'WITHCURSOR', 'COUNT', 4]
    # The chunk's reply loop makes 3 calls, the last ones of the query.
    _memory_pressure(env, 'AT_CALL', _checkpoint_calls(env, query) - 3 + 2)
    res, cursor = env.cmd(*query)
    env.assertEqual(len(res['results']), 2, message=res)
    env.assertEqual(res['warning'], [COORD_OOM_WARNING])
    env.assertEqual(cursor, 0)
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
    calls = _checkpoint_calls(env, query)
    env.assertGreater(calls, 0, message=query)
    for at_call in range(1, calls + 1):
        _memory_pressure(env, 'AT_CALL', at_call)
        try:
            res = env.cmd(*query)
        except redis_exceptions.ResponseError as e:
            res = e
        check(res, at_call, calls)
    _memory_pressure(env, 'RESET')


def _hybrid_stop_shapes(env):
    """FT.HYBRID maps a stop for OOM anywhere in the query, at a subquery's build, in a subquery
    pipeline, in the tail or in the reply, per ON_OOM: under RETURN the rows kept with the OOM
    warning and no timeout warning; under FAIL the OOM error, except in the replay of the
    buffered result, which can only truncate and warn; IGNORE runs to the end."""
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
    _reset(env)


@skip(cluster=True)
@env_spec(protocol=3)
def test_hybrid_stop_past_the_offset_warns(env):
    """A tail sorter stopped while filling holds OFFSET + LIMIT rows at most, so the pager can end
    the reply before the stop is read. Wherever the stop lands, a reply that differs from the
    complete one carries the OOM warning."""
    env.expect('FT.CREATE', 'hidx', 'PREFIX', 1, 'h:', 'SCHEMA', 'name', 'TEXT',
               'v', 'VECTOR', 'FLAT', 6, 'TYPE', 'FLOAT32', 'DIM', 2, 'DISTANCE_METRIC', 'L2').ok()
    conn = getConnectionByEnv(env)
    for i in range(N_DOCS):
        conn.execute_command('HSET', f'h:{i}', 'name', f'hello{i}',
                             'v', np.array([float(i), 0.0], dtype=np.float32).tobytes())
    query = ['FT.HYBRID', 'hidx', 'SEARCH', 'hello*', 'VSIM', '@v', '$BLOB', 'LIMIT', 2, 2,
             'PARAMS', 2, 'BLOB', np.array([0.0, 0.0], dtype=np.float32).tobytes()]

    def keys(res):
        return [r['__key'] for r in res['results']]

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
    _reset(env)


@skip(cluster=True)
@env_spec(protocol=3)
def test_hybrid_reply_loop_stop_truncates(env):
    """FT.HYBRID streams its tail rows into the reply with the same per-row check; a stop there
    ends the stream at the tail's merger, so some stop positions keep a strict prefix of the
    rows with the OOM warning, and none replies a cut without it."""
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
    own; every one of them is tried, under both ON_TIMEOUT RETURN and FAIL."""
    _hybrid_index(env)
    full = len(env.cmd(*HYBRID)['results'])
    shard = env.getConnection(2)

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
                env.assertFalse(isinstance(res, Exception), message=(on_timeout, at_call, res))
                env.assertTrue(res['warnings'] == [COORD_OOM_WARNING] or
                               (len(res['results']) == full and res['warnings'] == []),
                               message=(on_timeout, at_call, res))

            # Under FAIL a stop in the replay of a shard's buffered cursor read can only warn, as
            # standalone; everywhere else it is the OOM error.
            allShards_change_oom_policy(env, 'fail')
            errors = 0
            for at_call in range(1, calls + 1):
                res = run_at(at_call)
                if isinstance(res, Exception):
                    env.assertContains(OOM_QUERY_ERROR, str(res), message=(on_timeout, at_call))
                    errors += 1
                else:
                    env.assertTrue(res['warnings'] == [COORD_OOM_WARNING] or
                                   (len(res['results']) == full and res['warnings'] == []),
                                   message=(on_timeout, at_call, res))
            env.assertGreater(errors, 0, message=on_timeout)
    finally:
        run_command_on_all_shards(env, 'CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return')
        allShards_change_oom_policy(env, 'return')
