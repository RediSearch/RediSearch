# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from contextlib import contextmanager

from common import *


@contextmanager
def row_block_format(env, enabled):
    conn = env.getConnection()
    config = 'search-internal-row-block-format'
    previous = conn.execute_command('CONFIG', 'GET', config)[1]
    try:
        env.assertEqual(conn.execute_command('CONFIG', 'SET', config, enabled), 'OK')
        yield
    finally:
        conn.execute_command('CONFIG', 'SET', config, previous)


@skip(cluster=False)
def test_row_block_cursor_values(env):
    """Decode strings, numbers and missing fields across shard and client cursors."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE',
               'text', 'TEXT', 'optional', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    # Exceed the shard's default chunk size; client COUNT alone does not do so.
    count = 1005
    with conn.pipeline(transaction=False) as pipe:
        for i in range(count):
            fields = ['n', i, 'text', f'value\x00{i}']
            if i % 2:
                fields += ['optional', 'present']
            pipe.execute_command('HSET', f'{{rowblock}}:{i}', *fields)
        pipe.execute()

    expected = [dict(n=str(i), text=f'value\x00{i}',
                     **({'optional': 'present'} if i % 2 else {})) for i in range(count)]
    for enabled in ('no', 'yes'):
        with row_block_format(env, enabled):
            reply, cursor = env.cmd(
                'FT.AGGREGATE', 'idx', '*', 'LOAD', 3, '@n', '@text', '@optional',
                'SORTBY', 2, '@n', 'ASC', 'LIMIT', 0, count, 'WITHCURSOR', 'COUNT', 127)
            rows = reply[1:]
            env.assertNotEqual(cursor, 0, message=reply)
            while cursor:
                reply, cursor = env.cmd('FT.CURSOR', 'READ', 'idx', cursor, 'COUNT', 127)
                rows.extend(reply[1:])
            env.assertEqual([dict(zip(row[::2], row[1::2])) for row in rows], expected)
            profile = env.cmd(
                'FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*',
                'LOAD', 3, '@n', '@text', '@optional', 'LIMIT', 0, count)
            coordinator = to_dict(to_dict(profile[1])['Coordinator'])
            network = to_dict(coordinator['Result processors profile'][0])
            # Binary rows include a null slot for a missing field; RESP rows omit it.
            # This distinguishes actual binary decoding from silently using RESP.
            fields = 3 * count if enabled == 'yes' else 2 * count + count // 2
            env.assertEqual(network['Fields converted'], fields, message=network)
            env.assertGreaterEqual(network['Shard replies'], 2, message=network)


@skip(cluster=False)
def test_row_block_reducer_arrays(env):
    """Shard TOLIST arrays and numeric partial sums survive binary transport."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'category', 'TAG', 'amount', 'NUMERIC').ok()
    conn = getConnectionByEnv(env)
    for i in range(12):
        conn.execute_command('HSET', f'{{rowblock}}:{i}', 'category', str(i % 3),
                             'amount', i, 'label', f'label{i}')
    expected = [dict(category=str(group), total=str(sum(range(group, 12, 3))),
                     labels=sorted(f'label{i}' for i in range(group, 12, 3)))
                for group in range(3)]
    for enabled in ('no', 'yes'):
        with row_block_format(env, enabled):
            reply = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@label',
                            'GROUPBY', 1, '@category',
                            'REDUCE', 'SUM', 1, '@amount', 'AS', 'total',
                            'REDUCE', 'TOLIST', 1, '@label', 'AS', 'labels',
                            'SORTBY', 2, '@category', 'ASC')
            rows = [dict(zip(row[::2], row[1::2])) for row in reply[1:]]
            for row in rows:
                row['labels'].sort()
            env.assertEqual(rows, expected)


@skip(cluster=False)
def test_row_block_dynamic_schema_fallback(env):
    """A changing LOAD * schema replays earlier encoded rows without data loss."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'common', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    for i in range(6):
        conn.execute_command('HSET', f'{{rowblock}}:{i}', 'common', 'x', f'field{i}', i)
    expected = sorted(sorted([('common', 'x'), (f'field{i}', str(i))]) for i in range(6))
    for enabled in ('no', 'yes'):
        with row_block_format(env, enabled):
            reply = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', '*')
            rows = sorted(sorted(zip(row[::2], row[1::2])) for row in reply[1:])
            env.assertEqual(rows, expected)
