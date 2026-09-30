# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

import numpy as np

from common import getConnectionByEnv, skip, to_dict


def assert_usage_counts(env, query_count, admin_count):
    info = to_dict(env.cmd('FT.INFO', 'idx'))
    env.assertEqual(int(info['number_of_uses']), query_count, message=info)
    env.assertEqual(int(info['number_of_admin_ops']), admin_count, message=info)


def test_index_usage_command_classification(env):
    """Classify each public query once while keeping INFO and EXPLAIN administrative."""
    env.expect(
        'FT.CREATE', 'idx', 'SCHEMA',
        't', 'TEXT',
        'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2',
        'DISTANCE_METRIC', 'L2',
    ).ok()
    conn = getConnectionByEnv(env)
    vector = np.array([0.0, 0.0], dtype=np.float32).tobytes()
    for i in range(8):
        conn.execute_command('HSET', f'{{usage}}:{i}', 't', 'hello world', 'v', vector)

    # FT.INFO increments the administrative counter before rendering it.
    assert_usage_counts(env, query_count=0, admin_count=1)

    env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT').noError()
    assert_usage_counts(env, query_count=1, admin_count=2)

    env.expect('FT.AGGREGATE', 'idx', '*', 'LIMIT', '0', '1').noError()
    assert_usage_counts(env, query_count=2, admin_count=3)

    env.expect(
        'FT.HYBRID', 'idx',
        'SEARCH', 'hello',
        'VSIM', '@v', '$BLOB', 'KNN', '2', 'K', '10',
        'COMBINE', 'LINEAR', '0',
        'PARAMS', '2', 'BLOB', vector,
    ).noError()
    assert_usage_counts(env, query_count=3, admin_count=4)

    env.expect('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '*', 'NOCONTENT').noError()
    assert_usage_counts(env, query_count=4, admin_count=5)

    env.expect('FT.SPELLCHECK', 'idx', 'helo').noError()
    assert_usage_counts(env, query_count=5, admin_count=6)

    _, cursor_id = env.cmd('FT.AGGREGATE', 'idx', '*', 'WITHCURSOR', 'COUNT', '1')
    env.assertGreater(cursor_id, 0)
    assert_usage_counts(env, query_count=6, admin_count=7)

    env.expect('FT.CURSOR', 'READ', 'idx', cursor_id, 'COUNT', '1').noError()
    assert_usage_counts(env, query_count=7, admin_count=8)

    # Monitoring adds only administrative operations, never query usage.
    assert_usage_counts(env, query_count=7, admin_count=9)

    env.expect('FT.EXPLAIN', 'idx', '*').noError()
    assert_usage_counts(env, query_count=7, admin_count=11)

    env.expect('FT.EXPLAINCLI', 'idx', '*').noError()
    assert_usage_counts(env, query_count=7, admin_count=13)


@skip(cluster=True)
def test_legacy_and_index_management_commands_are_administrative(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'tag', 'TAG').ok()
    env.cmd('HSET', 'doc', 't', 'hello', 'tag', 'value')

    def assert_admin_command(*command, command_increments=1):
        before = to_dict(env.cmd('FT.INFO', 'idx'))
        env.expect(*command).noError()
        after = to_dict(env.cmd('FT.INFO', 'idx'))

        env.assertEqual(after['number_of_uses'], before['number_of_uses'], message=command)
        env.assertEqual(
            int(after['number_of_admin_ops']),
            int(before['number_of_admin_ops']) + command_increments + 1,
            message=command,
        )

    assert_admin_command('FT.GET', 'idx', 'doc')
    assert_admin_command('FT.MGET', 'idx', 'doc')
    assert_admin_command('FT.TAGVALS', 'idx', 'tag')
    assert_admin_command('FT.ADD', 'idx', 'legacy-doc', '1', 'FIELDS', 't', 'hello')
    assert_admin_command('FT.DEL', 'idx', 'missing-doc')
    assert_admin_command('FT.ALTER', 'idx', 'SKIPINITIALSCAN', 'SCHEMA', 'ADD', 'n', 'NUMERIC')
    assert_admin_command('FT.SYNUPDATE', 'idx', 'group', 'SKIPINITIALSCAN', 'term')
    assert_admin_command('FT.SYNDUMP', 'idx')
    assert_admin_command('FT.ALIASADD', 'alias', 'idx')
    assert_admin_command('FT.ALIASLIST', 'idx')

    # ALIASUPDATE acquires both the old and new target, preserving its two existing increments.
    assert_admin_command('FT.ALIASUPDATE', 'alias', 'idx', command_increments=2)
    assert_admin_command('FT.ALIASDEL', 'alias')
