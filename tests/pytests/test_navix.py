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

    policies = ['NAVIX', 'NAVIX_BLIND', 'NAVIX_DEDUP', 'NAVIX_BUDGET', 'NAVIX_BRIDGE', 'NAVIX_AUTO']
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
            if policy in ('NAVIX_BRIDGE', 'NAVIX_AUTO'):
                env.assertEqual(result[0], [1, 'd:39'])
            elif policy == 'NAVIX_DEDUP':
                baseline = env.cmd('FT.SEARCH', 'idx', *args('@n:[39 39]', 'NAVIX'))
                env.assertEqual(result[0], baseline)
            else:
                env.assertTrue(result[0] in ([0], [1, 'd:39']), message=result)
            env.assertEqual(_profile_value(result[1], 'Vector search mode'), 'NAVIX_QC')
            if policy in ('NAVIX_BRIDGE', 'NAVIX_AUTO'):
                env.assertEqual(_profile_value(result[1], 'NaviX second hop visits'), 0)
                env.assertGreater(_profile_value(result[1], 'NaviX bridge expansions'), 0, message=result)
            env.assertEqual(env.cmd('FT.SEARCH', 'idx', *args('@n:[100 101]', policy)), [0])
    finally:
        env.cmd('CONFIG', 'SET', 'search-enable-unstable-features', previous)


@skip(cluster=True)
def test_navix_auto_sparse_filter_uses_exact_scan(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA',
               't', 'TAG', 'v', 'VECTOR', 'HNSW', 6,
               'TYPE', 'FLOAT32', 'DIM', 1, 'DISTANCE_METRIC', 'L2').ok()
    conn = getConnectionByEnv(env)
    for i in range(100):
        conn.execute_command('HSET', f'd:{i}', 't', 'rare' if i == 99 else 'common',
                             'v', struct.pack('<f', i))
    env.cmd(debug_cmd(), 'WORKERS', 'DRAIN')
    previous = env.cmd('CONFIG', 'GET', 'search-enable-unstable-features')[1]
    try:
        env.cmd('CONFIG', 'SET', 'search-enable-unstable-features', 'yes')
        query = '(@t:{rare})=>[KNN 2 @v $q HYBRID_POLICY NAVIX_AUTO EF_RUNTIME 40 AS dist]'
        result = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', query,
                         'SORTBY', 'dist', 'NOCONTENT', 'DIALECT', 2,
                         'PARAMS', 2, 'q', struct.pack('<f', 0))
        env.assertEqual(result[0], [1, 'd:99'])
        env.assertEqual(_profile_value(result[1], 'Vector search mode'), 'HYBRID_ADHOC_BF')
        env.assertEqual(_profile_value(result[1], 'NaviX bridge expansions'), None)
        info = env.cmd(debug_cmd(), 'VECSIM_INFO', 'idx', 'v')
        env.assertEqual(_profile_value(info, 'LAST_SEARCH_MODE'), 'HYBRID_ADHOC_BF')
    finally:
        env.cmd('CONFIG', 'SET', 'search-enable-unstable-features', previous)


@skip(cluster=True)
def test_navix_auto_search_effort_and_cutoff(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA',
               't', 'TAG', 'v', 'VECTOR', 'HNSW', 8,
               'TYPE', 'FLOAT32', 'DIM', 1, 'DISTANCE_METRIC', 'L2',
               'EF_RUNTIME', 10).ok()
    conn = getConnectionByEnv(env)
    for i in range(1000):
        tags = ['even' if i % 2 == 0 else 'odd']
        if i < 20:
            tags.append('cutoff')
        if i < 21:
            tags.append('above')
        conn.execute_command('HSET', f'd:{i}', 't', ','.join(tags),
                             'v', struct.pack('<f', i))
    env.cmd(debug_cmd(), 'WORKERS', 'DRAIN')
    previous = env.cmd('CONFIG', 'GET', 'search-enable-unstable-features')[1]
    try:
        env.cmd('CONFIG', 'SET', 'search-enable-unstable-features', 'yes')

        def profile(policy, predicate='@t:{even}', ef=None, k=5):
            effort = '' if ef is None else f' EF_RUNTIME {ef}'
            query = f'({predicate})=>[KNN {k} @v $q HYBRID_POLICY {policy} BATCH_SIZE 1{effort} AS dist]'
            return env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', query,
                           'SORTBY', 'dist', 'LIMIT', 0, k, 'NOCONTENT', 'DIALECT', 2,
                           'PARAMS', 2, 'q', struct.pack('<f', 0))

        for requested, effective, k in [(400, 200, 5), (40, 64, 5),
                                         (40, 100, 100), (None, None, 5)]:
            actual = profile('NAVIX_AUTO', ef=requested, k=k)
            control = profile('NAVIX_BRIDGE', ef=effective, k=k)
            env.assertEqual(actual[0], control[0])
            for counter in ['NaviX expanded', 'NaviX distances', 'NaviX filter checks']:
                env.assertEqual(_profile_value(actual[1], counter),
                                _profile_value(control[1], counter), message=(requested, k, counter))
        actual = profile('NAVIX_AUTO', ef=400)
        unscaled = profile('NAVIX_BRIDGE', ef=400)
        env.assertLess(_profile_value(actual[1], 'NaviX expanded'),
                       _profile_value(unscaled[1], 'NaviX expanded'))
        for tag, expected_mode in [('cutoff', 'HYBRID_ADHOC_BF'), ('above', 'NAVIX_QC')]:
            result = profile('NAVIX_AUTO', predicate=f'@t:{{{tag}}}', ef=400)
            env.assertEqual(result[0], [5, 'd:0', 'd:1', 'd:2', 'd:3', 'd:4'])
            env.assertEqual(_profile_value(result[1], 'Vector search mode'), expected_mode)
    finally:
        env.cmd('CONFIG', 'SET', 'search-enable-unstable-features', previous)


@skip(cluster=True)
def test_navix_auto_retains_scan_preference(env):
    """The second option retains exact scanning where the whole-index heuristic prefers it."""
    previous = env.cmd('CONFIG', 'GET', 'search-enable-unstable-features')[1]
    conn = getConnectionByEnv(env)
    try:
        for count, expected_mode in [(100, 'HYBRID_ADHOC_BF'), (6000, 'NAVIX_QC')]:
            index, prefix = f'idx:{count}', f'd{count}:'
            env.expect('FT.CREATE', index, 'ON', 'HASH', 'PREFIX', 1, prefix, 'SCHEMA',
                       't', 'TAG', 'body', 'TEXT', 'v', 'VECTOR', 'HNSW', 8,
                       'TYPE', 'FLOAT32', 'DIM', 1, 'DISTANCE_METRIC', 'L2',
                       'EF_CONSTRUCTION', 100).ok()
            for i in range(count):
                conn.execute_command('HSET', f'{prefix}{i}',
                                     't', 'even' if i % 2 == 0 else 'odd',
                                     'v', struct.pack('<f', i))
            env.cmd(debug_cmd(), 'WORKERS', 'DRAIN')
            query = '(@t:{even})=>[KNN 2 @v $q HYBRID_POLICY NAVIX_AUTO2 EF_RUNTIME 100 AS dist]'
            args = ['SORTBY', 'dist', 'NOCONTENT', 'DIALECT', 2,
                    'PARAMS', 2, 'q', struct.pack('<f', 0)]
            env.cmd('CONFIG', 'SET', 'search-enable-unstable-features', 'no')
            for predicate in ['even', 'missing']:
                env.expect('FT.SEARCH', index, query.replace('{even}', '{' + predicate + '}'),
                           *args).error().contains('requires CONFIG SET')
            env.cmd('CONFIG', 'SET', 'search-enable-unstable-features', 'yes')
            result = env.cmd('FT.PROFILE', index, 'SEARCH', 'QUERY', query, *args)
            env.assertEqual(result[0], [2, f'{prefix}0', f'{prefix}2'])
            env.assertEqual(_profile_value(result[1], 'Vector search mode'), expected_mode)
            control = env.cmd('FT.PROFILE', index, 'SEARCH', 'QUERY',
                              query.replace('NAVIX_AUTO2', 'NAVIX_AUTO'), *args)
            env.assertEqual(control[0], result[0])
            env.assertEqual(_profile_value(control[1], 'Vector search mode'), 'NAVIX_QC')
            if expected_mode == 'HYBRID_ADHOC_BF':
                env.assertEqual(_profile_value(result[1], 'NaviX bridge expansions'), None)
            else:
                env.assertGreater(_profile_value(result[1], 'NaviX bridge expansions'), 0,
                                  message=result)
            for predicate in ['-@t:{missing}', '~@body:missing']:
                wildcard_query = query.replace('@t:{even}', predicate)
                wildcard_result = env.cmd('FT.PROFILE', index, 'SEARCH', 'QUERY',
                                          wildcard_query, *args)
                env.assertEqual(wildcard_result[0], [2, f'{prefix}0', f'{prefix}1'])
                env.assertEqual(_profile_value(wildcard_result[1], 'Vector search mode'),
                                expected_mode)
            env.expect('FT.SEARCH', index, query.replace('(@t:{even})', '*'),
                       *args).error().contains('requires filtered')
    finally:
        env.cmd('CONFIG', 'SET', 'search-enable-unstable-features', previous)
