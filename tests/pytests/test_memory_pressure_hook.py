# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from common import *
from test_info_modules import (
    COORD_WARN_ERR_SECTION, OOM_ERROR_COORD_METRIC, OOM_WARNING_COORD_METRIC,
    info_modules_to_dict,
)

OOM_QUERY_ERROR = "Not enough memory available to execute the query"
COORD_OOM_WARNING = "Coordinator failed to execute the query due to insufficient memory"
DEFAULT_STATUS = ['ratio', -1, 'at_call', 0, 'checkpoint_calls', 0,
                  'read_interval', 256]


def memory_pressure(env, *args):
    return env.cmd(debug_cmd(), 'MEMORY_PRESSURE', *args)


def status(env):
    """The hook's settings, without the server's real ratio (raw_ratio), which depends on
    the Redis build."""
    res = memory_pressure(env, 'STATUS')
    env.assertEqual(res[-2], 'raw_ratio')
    # RESP2 replies the ratio as a bulk string.
    res[1] = float(res[1])
    return res[:-2]


@skip(cluster=True)
def test_memory_pressure_status_and_reset(env):
    """The hook's settings are visible through STATUS and RESET restores the defaults."""
    env.assertEqual(status(env), DEFAULT_STATUS)

    env.assertEqual(memory_pressure(env, 'RATIO', '1.5'), 'OK')
    env.assertEqual(memory_pressure(env, 'AT_CALL', '3'), 'OK')
    env.assertEqual(memory_pressure(env, 'READ_INTERVAL', '1'), 'OK')
    env.assertEqual(status(env), ['ratio', 1.5, 'at_call', 3, 'checkpoint_calls', 0,
                                  'read_interval', 1])

    # A negative ratio and a zero interval each restore their default alone.
    env.assertEqual(memory_pressure(env, 'RATIO', '-0'), 'OK')
    env.assertEqual(memory_pressure(env, 'READ_INTERVAL', '0'), 'OK')
    env.assertEqual(status(env), ['ratio', -1, 'at_call', 3, 'checkpoint_calls', 0,
                                  'read_interval', 256])

    env.assertEqual(memory_pressure(env, 'RESET'), 'OK')
    env.assertEqual(status(env), DEFAULT_STATUS)


@skip(cluster=True)
def test_memory_pressure_rejects_bad_arguments(env):
    """Malformed MEMORY_PRESSURE calls fail without changing the hook."""
    env.expect(debug_cmd(), 'MEMORY_PRESSURE').error().contains('wrong number of arguments')
    env.expect(debug_cmd(), 'MEMORY_PRESSURE', 'RATIO').error().contains(
        'wrong number of arguments')
    env.expect(debug_cmd(), 'MEMORY_PRESSURE', 'RESET', 'extra').error().contains(
        'wrong number of arguments')
    env.expect(debug_cmd(), 'MEMORY_PRESSURE', 'RATIO', 'high').error().contains('Invalid ratio')
    env.expect(debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', '-1').error().contains('Invalid count')
    env.expect(debug_cmd(), 'MEMORY_PRESSURE', 'READ_INTERVAL', '4294967296').error().contains(
        'Invalid count')
    env.expect(debug_cmd(), 'MEMORY_PRESSURE', 'BOGUS', '1').error().contains(
        "Invalid command for 'MEMORY_PRESSURE'")
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC').ok()
    env.expect(debug_cmd(), 'FT.AGGREGATE', 'idx', '*', 'MEMORY_CHECKPOINT', 'TIMEOUT_AFTER_N', 1,
               'DEBUG_PARAMS_COUNT', 3).error().contains('MEMORY_CHECKPOINT is not supported')
    env.assertEqual(status(env), DEFAULT_STATUS)


def _forced_ratio_drives_entry_guard(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc', 'name', 'hello')
    found = [1, 'doc', ['name', 'hello']]
    search = ['FT.SEARCH', 'idx', '*']
    aggregate = ['FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@name']

    try:
        allShards_change_oom_policy(env, 'fail')
        memory_pressure(env, 'RATIO', '0.99')
        env.assertEqual(env.cmd(*search), found)

        memory_pressure(env, 'RATIO', '1')
        env.expect(*search).error().contains(OOM_QUERY_ERROR)
        env.expect(*aggregate).error().contains(OOM_QUERY_ERROR)

        allShards_change_oom_policy(env, 'return')
        env.assertEqual(env.cmd(*search), [0])
        env.assertEqual(env.cmd(*aggregate), [0])

        allShards_change_oom_policy(env, 'ignore')
        env.assertEqual(env.cmd(*search), found)

        allShards_change_oom_policy(env, 'fail')
        memory_pressure(env, 'RESET')
        env.assertEqual(env.cmd(*search), found)
    finally:
        memory_pressure(env, 'RESET')
        allShards_change_oom_policy(env, 'return')


@skip(cluster=True)
def test_forced_ratio_drives_entry_guard(env):
    """A forced ratio replaces the memory read of the query entry guard on a server whose real
    memory is unlimited, under every ON_OOM policy."""
    _forced_ratio_drives_entry_guard(env)


@skip(cluster=False)
def test_forced_ratio_drives_coordinator_entry_guard(env):
    """In a cluster the hook is per process: forcing the coordinator's ratio trips its entry
    guard before the query fans out."""
    _forced_ratio_drives_entry_guard(env)


@skip(cluster=False)
def test_search_checkpoint_runs_on_shards(env):
    """FT.SEARCH has no coordinator pipeline, so with or without INTERNAL_ONLY its checkpoint
    runs on every shard: each shard makes more calls with it than without it. Every process
    also makes its build and other checkpoint calls, so only the difference counts."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC').ok()
    conn = getConnectionByEnv(env)
    for i in range(100):
        conn.execute_command('HSET', f'doc{i}', 'n', i)
    shards = [env.getConnection(i) for i in range(1, env.shardsCount + 1)]

    def calls(*debug_args):
        run_command_on_all_shards(env, debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', 1000)
        query = ('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'LIMIT', 0, 100)
        if debug_args:
            query = (debug_cmd(), *query, *debug_args, 'DEBUG_PARAMS_COUNT', len(debug_args))
        res = env.cmd(*query)
        env.assertEqual(len(res) - 1, 100, message=debug_args)
        return [shard.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'STATUS')[5]
                for shard in shards]

    try:
        without = calls()
        for debug_args in (['MEMORY_CHECKPOINT'], ['MEMORY_CHECKPOINT', 'INTERNAL_ONLY']):
            with_checkpoint = calls(*debug_args)
            for shard, (w, wo) in enumerate(zip(with_checkpoint, without), 1):
                env.assertGreater(w, wo, message=(debug_args, shard))
    finally:
        run_command_on_all_shards(env, debug_cmd(), 'MEMORY_PRESSURE', 'RESET')


@skip(cluster=False)
def test_aggregate_checkpoint_routing(env):
    """In a multi-shard FT.AGGREGATE the checkpoint runs in the coordinator pipeline, or with
    INTERNAL_ONLY in the shard pipelines only; the per-process call counters show where. Every
    process also makes its build and other checkpoint calls, so only the difference between the
    two placements counts."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC').ok()
    conn = getConnectionByEnv(env)
    for i in range(100):
        conn.execute_command('HSET', f'doc{i}', 'n', i)
    coordinator, other_shard = env.getConnection(1), env.getConnection(2)

    def calls(*debug_args):
        run_command_on_all_shards(env, debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', 1000)
        res = _debug_aggregate(env, *debug_args)
        env.assertEqual(len(res) - 1, 100, message=debug_args)
        return [con.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'STATUS')[5]
                for con in (coordinator, other_shard)]

    try:
        coord_pipeline = calls('MEMORY_CHECKPOINT')
        shard_pipelines = calls('MEMORY_CHECKPOINT', 'INTERNAL_ONLY')
        # The coordinator's process also hosts a shard, whose pipeline counts there too.
        env.assertGreater(coord_pipeline[0], shard_pipelines[0])
        env.assertLess(coord_pipeline[1], shard_pipelines[1])
    finally:
        run_command_on_all_shards(env, debug_cmd(), 'MEMORY_PRESSURE', 'RESET')


@skip(cluster=True)
def test_internal_only_checkpoint_runs_in_standalone(env):
    """Standalone has a single pipeline, so INTERNAL_ONLY still installs the checkpoint there."""
    _index_with_docs(env)
    try:
        memory_pressure(env, 'AT_CALL', BUILD_CHECKPOINTS + 4)
        res = _debug_aggregate(env, 'MEMORY_CHECKPOINT', 'INTERNAL_ONLY')
        env.assertEqual(len(res) - 1, 3, message=res)
    finally:
        memory_pressure(env, 'RESET')


@skip(cluster=True)
def test_raw_ratio_reports_the_memory_api(env):
    """STATUS shows whether this Redis exports RedisModule_GetUsedMemoryRatioEstimate, which the
    checkpoints read: null without it, the real ratio, unaffected by RATIO, with it."""
    memory_pressure(env, 'RATIO', '5')
    raw_ratio = memory_pressure(env, 'STATUS')[-1]
    memory_pressure(env, 'RESET')
    if raw_ratio is None:
        return
    max_memory = env.cmd('CONFIG', 'GET', 'maxmemory')[1]
    if int(max_memory) == 0:
        env.assertEqual(float(raw_ratio), 0.0)
    else:
        env.assertLess(float(raw_ratio), 5.0)
    env.expect('CONFIG', 'SET', 'maxmemory', '100gb').ok()
    try:
        raw_ratio = float(memory_pressure(env, 'STATUS')[-1])
        env.assertGreater(raw_ratio, 0.0)
        env.assertLess(raw_ratio, 1.0)
    finally:
        env.expect('CONFIG', 'SET', 'maxmemory', max_memory).ok()


N_DOCS = 10
# The query's build stages are checkpoints too, read before the debug checkpoint's first call.
BUILD_CHECKPOINTS = 3


def _index_with_docs(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    for i in range(N_DOCS):
        conn.execute_command('HSET', f'doc{i}', 'n', i)


def _debug_aggregate(env, *debug_args):
    return env.cmd(debug_cmd(), 'FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@n',
                   *debug_args, 'DEBUG_PARAMS_COUNT', len(debug_args))


@skip(cluster=True)
def test_pressure_at_call_stops_a_query_at_a_debug_checkpoint(env):
    """MEMORY_CHECKPOINT stands in for a real checkpoint: with AT_CALL n past the build stages
    the query stops at its n-th read, after n-1 rows, and ON_OOM IGNORE keeps it running."""
    _index_with_docs(env)

    memory_pressure(env, 'AT_CALL', BUILD_CHECKPOINTS + 4)
    res = _debug_aggregate(env, 'MEMORY_CHECKPOINT')
    env.assertEqual(len(res) - 1, 3, message=res)
    env.assertEqual(status(env)[5], BUILD_CHECKPOINTS + 4)

    change_oom_policy(env, 'ignore')
    memory_pressure(env, 'AT_CALL', BUILD_CHECKPOINTS + 4)
    res = _debug_aggregate(env, 'MEMORY_CHECKPOINT')
    env.assertEqual(len(res) - 1, N_DOCS, message=res)
    change_oom_policy(env, 'return')

    memory_pressure(env, 'RESET')
    res = _debug_aggregate(env, 'MEMORY_CHECKPOINT')
    env.assertEqual(len(res) - 1, N_DOCS, message=res)



def _coord_oom_counters(env):
    section = info_modules_to_dict(env)[COORD_WARN_ERR_SECTION]
    return int(section[OOM_ERROR_COORD_METRIC]), int(section[OOM_WARNING_COORD_METRIC])


@skip(cluster=True)
@env_spec(protocol=3)
def test_oom_stop_replies_by_on_oom_policy(env):
    """A query stopped for OOM ends like the entry guard's OOM, not like a timeout: ON_OOM
    RETURN keeps the rows produced so far and warns, ON_OOM FAIL replies the OOM error, and
    each bumps the matching INFO counter."""
    _index_with_docs(env)

    try:
        errors, warnings = _coord_oom_counters(env)
        memory_pressure(env, 'AT_CALL', BUILD_CHECKPOINTS + 4)
        res = _debug_aggregate(env, 'MEMORY_CHECKPOINT')
        env.assertEqual(len(res['results']), 3, message=res)
        env.assertEqual(res['warning'], [COORD_OOM_WARNING])
        env.assertEqual(_coord_oom_counters(env), (errors, warnings + 1))

        change_oom_policy(env, 'fail')
        memory_pressure(env, 'AT_CALL', BUILD_CHECKPOINTS + 4)
        env.expect(debug_cmd(), 'FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@n',
                   'MEMORY_CHECKPOINT', 'DEBUG_PARAMS_COUNT', 1).error().contains(OOM_QUERY_ERROR)
        env.assertEqual(_coord_oom_counters(env), (errors + 1, warnings + 1))
        change_oom_policy(env, 'return')

        memory_pressure(env, 'AT_CALL', BUILD_CHECKPOINTS + 4)
        res = env.cmd(debug_cmd(), 'FT.SEARCH', 'idx', '*', 'NOCONTENT', 'SORTBY', 'n',
                      'MEMORY_CHECKPOINT', 'DEBUG_PARAMS_COUNT', 1)
        env.assertEqual([r['id'] for r in res['results']], ['doc0', 'doc1', 'doc2'], message=res)
        env.assertEqual(res['warning'], [COORD_OOM_WARNING])
    finally:
        memory_pressure(env, 'RESET')
        change_oom_policy(env, 'return')


@skip(cluster=True)
def test_oom_stop_ends_the_cursor(env):
    """A cursor read stopped for OOM returns its rows and ends the cursor rather than leaving it
    open as a timeout under ON_TIMEOUT RETURN would."""
    _index_with_docs(env)
    try:
        memory_pressure(env, 'AT_CALL', BUILD_CHECKPOINTS + 4)
        res, cursor = env.cmd(debug_cmd(), 'FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@n',
                              'WITHCURSOR', 'COUNT', 2, 'MEMORY_CHECKPOINT',
                              'DEBUG_PARAMS_COUNT', 1)
        env.assertEqual(len(res) - 1, 2, message=res)
        env.assertNotEqual(cursor, 0)
        res, cursor = env.cmd('FT.CURSOR', 'READ', 'idx', cursor)
        env.assertEqual(len(res) - 1, 1, message=res)
        env.assertEqual(cursor, 0)
    finally:
        memory_pressure(env, 'RESET')


@skip(cluster=False)
@env_spec(protocol=3)
def test_oom_stop_on_shards_reaches_the_coordinator(env):
    """Shards stopped for OOM report it in the shard wording, which is what the coordinator
    recognizes, and the coordinator turns that into its own ON_OOM outcome, worded as for the
    entry guard. AT_CALL rather than RATIO keeps the coordinator's entry guard out of it."""
    _index_with_docs(env)
    query = [debug_cmd(), 'FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@n',
             'MEMORY_CHECKPOINT', 'INTERNAL_ONLY', 'DEBUG_PARAMS_COUNT', 2]

    try:
        run_command_on_all_shards(env, debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', 1)
        res = env.cmd(*query)
        env.assertEqual(res['results'], [], message=res)
        env.assertEqual(res['warning'], [COORD_OOM_WARNING])

        allShards_change_oom_policy(env, 'fail')
        run_command_on_all_shards(env, debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', 1)
        env.expect(*query).error().contains(OOM_QUERY_ERROR)
        allShards_change_oom_policy(env, 'return')

        run_command_on_all_shards(env, debug_cmd(), 'MEMORY_PRESSURE', 'RESET')
        res = env.cmd(*query)
        env.assertEqual(len(res['results']), N_DOCS, message=res)
    finally:
        run_command_on_all_shards(env, debug_cmd(), 'MEMORY_PRESSURE', 'RESET')
        allShards_change_oom_policy(env, 'return')
