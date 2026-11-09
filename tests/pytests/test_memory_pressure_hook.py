# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from common import *

OOM_QUERY_ERROR = "Not enough memory available to execute the query"
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
    env.expect(debug_cmd(), 'MEMORY_PRESSURE', 'RATIO', '-1').error().contains('Invalid ratio')
    env.expect(debug_cmd(), 'MEMORY_PRESSURE', 'RATIO', '-0').error().contains('Invalid ratio')
    env.expect(debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', '0').error().contains('Invalid count')
    env.expect(debug_cmd(), 'MEMORY_PRESSURE', 'READ_INTERVAL', '0').error().contains(
        'Invalid count')
    env.expect(debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', '-1').error().contains('Invalid count')
    env.expect(debug_cmd(), 'MEMORY_PRESSURE', 'READ_INTERVAL', '4294967296').error().contains(
        'Invalid count')
    env.expect(debug_cmd(), 'MEMORY_PRESSURE', 'BOGUS', '1').error().contains(
        "Invalid command for 'MEMORY_PRESSURE'")
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


@skip(cluster=True)
@env_spec(protocol=3, moduleArgs='WORKERS 0')
def test_force_timeout_hook(env):
    """FORCE_TIMEOUT AT_CALL makes the n-th query that arms a clock deadline time out, through
    the policy's own reply: an error under ON_TIMEOUT FAIL, a warning under RETURN. It fires
    once, RESET disarms it, and bad arguments leave it unchanged."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC').ok()
    conn = getConnectionByEnv(env)
    for i in range(10):
        conn.execute_command('HSET', f'doc{i}', 'n', i)
    query = ('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@n', 'TIMEOUT', 0)

    def force_timeout(*args):
        return env.cmd(debug_cmd(), 'FORCE_TIMEOUT', *args)

    def status():
        return force_timeout('STATUS')

    def expect_complete():
        res = env.cmd(*query)
        env.assertEqual(len(res['results']), 10)
        env.assertEqual(res['warning'], [])

    try:
        env.assertEqual(status(), ['at_call', 0, 'cycles', 0])
        expect_complete()

        env.expect(debug_cmd(), 'FORCE_TIMEOUT').error().contains('wrong number of arguments')
        env.expect(debug_cmd(), 'FORCE_TIMEOUT', 'AT_CALL').error().contains(
            'wrong number of arguments')
        env.expect(debug_cmd(), 'FORCE_TIMEOUT', 'RESET', 'x').error().contains(
            'wrong number of arguments')
        env.expect(debug_cmd(), 'FORCE_TIMEOUT', 'AT_CALL', '-1').error().contains(
            'Invalid count')
        env.expect(debug_cmd(), 'FORCE_TIMEOUT', 'BOGUS', '1').error().contains(
            "Invalid command for 'FORCE_TIMEOUT'")

        env.expect(config_cmd(), 'SET', 'ON_TIMEOUT', 'FAIL').ok()
        env.assertEqual(force_timeout('AT_CALL', 2), 'OK')
        expect_complete()
        env.expect(*query).error().contains('Timeout limit was reached')
        env.assertEqual(status(), ['at_call', 2, 'cycles', 2])
        expect_complete()

        env.assertEqual(force_timeout('AT_CALL', 1), 'OK')
        env.assertEqual(force_timeout('RESET'), 'OK')
        env.assertEqual(status(), ['at_call', 0, 'cycles', 0])
        expect_complete()

        env.expect(config_cmd(), 'SET', 'ON_TIMEOUT', 'RETURN').ok()
        env.assertEqual(force_timeout('AT_CALL', 1), 'OK')
        res = env.cmd(*query)
        env.assertContains('Timeout limit was reached', res['warning'])
        env.assertEqual(env.cmd('PING'), True)
    finally:
        force_timeout('RESET')
        env.cmd(config_cmd(), 'SET', 'ON_TIMEOUT', 'RETURN')
