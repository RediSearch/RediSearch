# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

import struct

from common import debug_cmd, getConnectionByEnv, skip


def _profile_value(node, name):
    if isinstance(node, list):
        for i, value in enumerate(node[:-1]):
            if value == name:
                return node[i + 1]
        for value in node:
            found = _profile_value(value, name)
            if found is not None:
                return found
    return None


@skip(cluster=True)
def test_navix_exact_filter_and_batch_refill(env):
    """The explicit POC uses summaries in HNSW and keeps exact verification in the batch consumer."""
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA',
               't', 'TAG', 'n', 'NUMERIC', 'body', 'TEXT',
               'v', 'VECTOR', 'HNSW', 10, 'TYPE', 'FLOAT32', 'DIM', 1,
               'DISTANCE_METRIC', 'L2', 'M', 16, 'EF_CONSTRUCTION', 100).ok()
    conn = getConnectionByEnv(env)
    for i in range(40):
        conn.execute_command('HSET', f'd:{i}', 't', 'A' if i % 2 == 0 else 'B',
                             'n', i, 'body', 'keep' if i >= 30 else 'drop',
                             'v', struct.pack('<f', i))
    env.cmd(debug_cmd(), 'WORKERS', 'DRAIN')
    query_blob = struct.pack('<f', 0)

    def args(predicate, policy='NAVIX'):
        return [f'({predicate})=>[KNN 2 @v $q HYBRID_POLICY {policy} BATCH_SIZE 1 EF_RUNTIME 40 AS dist]',
                'SORTBY', 'dist', 'NOCONTENT', 'DIALECT', 2, 'PARAMS', 2, 'q', query_blob]

    policies = ['NAVIX', 'NAVIX_BLIND', 'NAVIX_DEDUP', 'NAVIX_BUDGET', 'NAVIX_BRIDGE']
    previous = env.cmd('CONFIG', 'GET', 'search-enable-unstable-features')[1]
    try:
        env.cmd('CONFIG', 'SET', 'search-enable-unstable-features', 'no')
        for policy in policies:
            for predicate in ['@t:{A}', '@t:{missing}']:
                env.expect('FT.SEARCH', 'idx', *args(predicate, policy)).error().contains('requires CONFIG SET')
        env.cmd('CONFIG', 'SET', 'search-enable-unstable-features', 'yes')
        cases = [('@t:{A}', [2, 'd:0', 'd:2']),
                 ('@n:[13 27]', [2, 'd:13', 'd:14']),
                 ('@n:[(13 (27]', [2, 'd:14', 'd:15']),
                 ('@t:{A} @n:[13 27]', [2, 'd:14', 'd:16']),
                 ('@n:[0 +inf] @body:keep', [2, 'd:30', 'd:31']),
                 ('@t:{missing}|@body:keep', [2, 'd:30', 'd:31'])]
        for predicate, expected in cases:
            for policy in policies + ['BATCHES']:
                result = env.cmd('FT.SEARCH', 'idx', *args(predicate, policy))
                env.assertEqual(result, expected, message=f'{predicate}, {policy}: {result}')
        oversized = '|'.join([f'@t:{{missing{i}}}' for i in range(260)] + ['@t:{A}'])
        env.assertEqual(env.cmd('FT.SEARCH', 'idx', *args(oversized)), [2, 'd:0', 'd:2'])
        default_args = args('~@body:missing')
        default_args[0] = default_args[0].replace(' BATCH_SIZE 1', '')
        default_profile = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', *default_args)
        env.assertEqual(default_profile[0], [2, 'd:0', 'd:1'])
        env.assertEqual(_profile_value(default_profile[1], 'Vector search mode'), 'NAVIX_QC')
        env.assertEqual(_profile_value(default_profile[1], 'Largest batch size'), 2)
        env.expect('FT.SEARCH', 'idx', *args('*')).error().contains('requires filtered')
        profiled = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', *args('@n:[0 +inf] @body:keep'))
        env.assertEqual(profiled[0], [2, 'd:30', 'd:31'])
        env.assertEqual(_profile_value(profiled[1], 'Vector search mode'), 'NAVIX_QC')
        env.assertGreater(_profile_value(profiled[1], 'NaviX expanded'), 0, message=profiled)
        env.assertGreater(_profile_value(profiled[1], 'Batches number'), 2, message=profiled)
        for policy in policies[2:]:
            result = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY',
                             *args('@n:[39 39]', policy))
            if policy == 'NAVIX_BRIDGE':
                env.assertEqual(result[0], [1, 'd:39'])
            elif policy == 'NAVIX_DEDUP':
                baseline = env.cmd('FT.SEARCH', 'idx', *args('@n:[39 39]', 'NAVIX'))
                env.assertEqual(result[0], baseline)
            else:
                env.assertTrue(result[0] in ([0], [1, 'd:39']), message=result)
            env.assertEqual(_profile_value(result[1], 'Vector search mode'), 'NAVIX_QC')
            if policy == 'NAVIX_BRIDGE':
                env.assertEqual(_profile_value(result[1], 'NaviX second hop visits'), 0)
                env.assertGreater(_profile_value(result[1], 'NaviX bridge expansions'), 0, message=result)
            env.assertEqual(env.cmd('FT.SEARCH', 'idx', *args('@n:[100 101]', policy)), [0])
    finally:
        env.cmd('CONFIG', 'SET', 'search-enable-unstable-features', previous)
