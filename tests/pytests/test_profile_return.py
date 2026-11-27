# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from common import *
import threading


def _results(env, reply, hybrid=False):
    """Remove diagnostics whose clocks vary between otherwise identical executions."""
    if hybrid:
        result = dict(reply) if env.protocol == 3 else to_dict(reply[:-1])
        result.pop('Profile', None)
        result.pop('execution_time', None)
        return result
    return reply['Results'] if env.protocol == 3 else reply[0]


def _assert_profile(env, reply, hybrid=False, timed_out=False):
    """Validate the public envelope and that shard diagnostics survived completion."""
    if hybrid:
        profile = reply['Profile'] if env.protocol == 3 else reply[-1]
    else:
        profile = reply['Profile'] if env.protocol == 3 else reply[1]
    profile = profile if env.protocol == 3 else to_dict(profile)
    env.assertEqual(len(profile['Shards']), env.shardsCount if env.isCluster() else 1,
                    message=reply)
    if timed_out:
        env.assertContains('Timeout limit was reached', str(reply), message=reply)


def _profile_return_semantics(protocol, workers):
    """Compare FAIL profiles with RETURN through shard and coordinator timeout hooks."""
    env = Env(protocol=protocol, enableDebugCommand=True,
              moduleArgs=f'WORKERS {workers} TIMEOUT 0 ON_TIMEOUT FAIL ON_OOM RETURN')
    run_command_on_all_shards(env, config_cmd(), 'SET', '_PRINT_PROFILE_CLOCK', 'false')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE',
               'embedding', 'VECTOR', 'FLAT', 6, 'TYPE', 'FLOAT32', 'DIM', 2,
               'DISTANCE_METRIC', 'L2').ok()
    conn = getConnectionByEnv(env)
    for n in range(8):
        conn.execute_command('HSET', f'{{profile-return}}:{n}', 'n', n,
                             'embedding', np.array([float(n), 0], dtype=np.float32).tobytes())

    for kind, tail in (
            ('SEARCH', ['SORTBY', 'n', 'ASC', 'NOCONTENT', 'LIMIT', 0, 8]),
            ('AGGREGATE', ['LOAD', 1, '@n', 'SORTBY', 2, '@n', 'ASC'])):
        for limited in ([], ['LIMITED']):
            command = ['FT.PROFILE', 'idx', kind, *limited, 'QUERY', '*', *tail, 'TIMEOUT', 0]
            # Internal timeouts exercise shard cursor continuation; coordinator
            # timeouts exercise the local pipeline and final profile collection.
            for internal_only in ((False, True) if env.isCluster() else (False,)):
                replies = []
                for policy in ('RETURN', 'FAIL'):
                    run_command_on_all_shards(env, config_cmd(), 'SET', 'ON_TIMEOUT', policy)
                    reply = runDebugQueryCommandTimeoutAfterN(
                        env, command, 2, internal_only=internal_only)
                    _assert_profile(env, reply, timed_out=True)
                    replies.append(_results(env, reply))
                env.assertEqual(replies[1], replies[0], message=str(command))
                if not env.isCluster():
                    rows = replies[1]['results'] if protocol == 3 else replies[1][1:]
                    env.assertEqual(len(rows), 2, message=replies)

            # A successful profile retains the normal result envelope too.
            replies = []
            for policy in ('RETURN', 'FAIL'):
                run_command_on_all_shards(env, config_cmd(), 'SET', 'ON_TIMEOUT', policy)
                reply = env.cmd(*command)
                _assert_profile(env, reply)
                replies.append(_results(env, reply))
            env.assertEqual(replies[1], replies[0], message=str(command))

    command = ['FT.PROFILE', 'idx', 'HYBRID', 'QUERY', 'SEARCH', '*',
               'VSIM', '@embedding', '$BLOB', 'KNN', 2, 'K', 8,
               'COMBINE', 'RRF', 2, 'WINDOW', 8,
               'PARAMS', 2, 'BLOB', np.array([0, 0], dtype=np.float32).tobytes(),
               'SORTBY', 2, '@n', 'ASC', 'LOAD', 1, '@n', 'TIMEOUT', 0]
    for component in ('SEARCH', 'VSIM', 'TAIL'):
        replies = []
        for policy in ('RETURN', 'FAIL'):
            run_command_on_all_shards(env, config_cmd(), 'SET', 'ON_TIMEOUT', policy)
            reply = runDebugQueryCommand(env, command, [f'TIMEOUT_AFTER_N_{component}', 2])
            _assert_profile(env, reply, hybrid=True, timed_out=True)
            replies.append(_results(env, reply, hybrid=True))
        env.assertEqual(replies[1], replies[0], message=component)

    for shard in env.getOSSMasterNodesConnectionList():
        env.assertEqual(shard.execute_command(config_cmd(), 'GET', 'ON_TIMEOUT'),
                        [['ON_TIMEOUT', 'fail']])
    # The override is request-local: an ordinary FAIL query still rejects the
    # cooperative debug hook in a worker/coordinator execution.
    if workers or env.isCluster():
        env.expect(debug_cmd(), 'FT.AGGREGATE', 'idx', '*', 'TIMEOUT_AFTER_N', 2,
                   'DEBUG_PARAMS_COUNT', 2).error().contains('ON_TIMEOUT')
    else:
        env.expect(debug_cmd(), 'FT.AGGREGATE', 'idx', '*', 'TIMEOUT_AFTER_N', 2,
                   'DEBUG_PARAMS_COUNT', 2).error().contains('Timeout limit was reached')


def test_profile_return_resp2():
    _profile_return_semantics(2, 2)


def test_profile_return_resp3():
    _profile_return_semantics(3, 2)


@skip(cluster=True)
def test_profile_return_inline_resp2():
    _profile_return_semantics(2, 0)


@skip(cluster=True)
def test_profile_return_inline_resp3():
    _profile_return_semantics(3, 0)


def _profile_without_blocked_timeout(protocol):
    """A queued profile cannot be forcibly timed out through a FAIL callback."""
    env = Env(protocol=protocol, moduleArgs='WORKERS 2 TIMEOUT 0 ON_TIMEOUT FAIL',
              enableDebugCommand=True)
    skipIfNoEnableAssert(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'embedding',
               'VECTOR', 'FLAT', 6, 'TYPE', 'FLOAT32', 'DIM', 2,
               'DISTANCE_METRIC', 'L2').ok()
    vector = np.array([0, 0], dtype=np.float32).tobytes()
    getConnectionByEnv(env).execute_command('HSET', '{profile-return}:0',
                                          'n', 0, 'embedding', vector)
    pool = 'COORD_THREADS' if env.isCluster() else 'WORKERS'
    for kind, args in (
            ('SEARCH', ['*']),
            ('AGGREGATE', ['*']),
            ('HYBRID', ['SEARCH', '*', 'VSIM', '@embedding', '$BLOB',
                        'PARAMS', 2, 'BLOB', vector])):
        replies, errors = [], []

        def query():
            try:
                replies.append(env.cmd('FT.PROFILE', 'idx', kind, 'QUERY',
                                       *args, 'TIMEOUT', 100000))
            except Exception as error:
                errors.append(error)

        thread = threading.Thread(target=query, daemon=True)
        env.expect(debug_cmd(), pool, 'PAUSE').ok()
        try:
            thread.start()
            client_id = wait_for_blocked_query_client(env, 'FT.PROFILE')
            env.expect('CLIENT', 'UNBLOCK', client_id, 'TIMEOUT').equal(0)
            env.assertEqual(replies, [])
            env.assertEqual(errors, [])
        finally:
            env.cmd(debug_cmd(), pool, 'RESUME')
            thread.join(timeout=10)
        env.assertFalse(thread.is_alive(), message=kind)
        env.assertEqual(errors, [], message=kind)
        env.assertEqual(len(replies), 1, message=kind)
        if replies:
            _assert_profile(env, replies[0], hybrid=kind == 'HYBRID')


def test_profile_without_blocked_timeout_resp2():
    _profile_without_blocked_timeout(2)


def test_profile_without_blocked_timeout_resp3():
    _profile_without_blocked_timeout(3)
