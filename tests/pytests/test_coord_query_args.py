# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

import struct

from common import debug_cmd, env_spec, getConnectionByEnv, skip


def _check_coord_query_args(env):
    """Read the held query at each search/profile offset, including debug commands."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE',
               'v', 'VECTOR', 'FLAT', 6, 'TYPE', 'FLOAT32', 'DIM', 2,
               'DISTANCE_METRIC', 'L2').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', '{doc}:1', 'n', 1, 'v', struct.pack('ff', 1, 0))
    conn.execute_command('HSET', '{doc}:2', 'n', 2, 'v', struct.pack('ff', 2, 0))

    commands = (
        ['FT.SEARCH', 'idx'],
        ['FT.PROFILE', 'idx', 'SEARCH', 'QUERY'],
        ['FT.PROFILE', 'idx', 'SEARCH', 'LIMITED', 'QUERY'],
    )
    knn = '*=>[KNN 1 @v $vec]'
    queries = (
        ('*', [2, '{doc}:1', '{doc}:2']),
        ('', [0]),
        ('\x00=>[KNN 1 @v $vec]', [0]),
        (knn, [1, '{doc}:1']),
        ('*=>[knn 1 @v $vec]', [1, '{doc}:1']),
        # The parser must ignore malformed bytes after NUL even when KNN is detected.
        (knn + '\x00 invalid [', [1, '{doc}:1']),
        ('*\x00=>[KNN 1 @v $vec]', [2, '{doc}:1', '{doc}:2']),
        # Large arguments can reuse Redis's input buffer and need trimming before dispatch.
        (' ' * 65536 + knn + ' ' * 65536, [1, '{doc}:1']),
        # Rewriting the shard K must only change the transport's command copy.
        ('*=>[KNN $k @v $vec]=>{$SHARD_K_RATIO: $ratio}', [2, '{doc}:1', '{doc}:2']),
    )
    config = env.cmd('CONFIG', 'GET', 'search-on-timeout')
    policy = config['search-on-timeout'] if isinstance(config, dict) else config[1]
    try:
        # Coordinator debug queries require RETURN even though these queries cannot time out.
        env.expect('CONFIG', 'SET', 'search-on-timeout', 'return').ok()
        for debug in (False, True):
            for command in commands:
                for query, expected in queries:
                    args = [*command, query, 'PARAMS', 6, 'vec', struct.pack('ff', 0, 0),
                            'k', 100, 'ratio', 0.5,
                            'NOCONTENT', 'SORTBY', 'n', 'ASC', 'TIMEOUT', 0, 'DIALECT', 2]
                    if debug:
                        args = [debug_cmd(), *args, 'TIMEOUT_AFTER_N', 1000,
                                'DEBUG_PARAMS_COUNT', 2]
                    pipe = env.getConnection().pipeline(transaction=False)
                    pipe.execute_command(*args)
                    pipe.ping()
                    result, pong = pipe.execute()
                    env.assertTrue(pong)
                    if command[0] == 'FT.PROFILE':
                        result = result['Results'] if isinstance(result, dict) else result[0]
                    if isinstance(result, dict):
                        env.assertEqual(result['total_results'], expected[0], message=result)
                        env.assertEqual([row['id'] for row in result['results']], expected[1:],
                                        message=result)
                    else:
                        env.assertEqual(result, expected, message=(command, debug, query))
    finally:
        env.expect('CONFIG', 'SET', 'search-on-timeout', policy).ok()


@skip(cluster=False, min_shards=2)
@env_spec(protocol=2)
def test_coord_query_args_preserve_nul_boundary(env):
    """Held arguments preserve RESP2 results through parsing and background dispatch."""
    _check_coord_query_args(env)


@skip(cluster=False, min_shards=2)
@env_spec(protocol=3)
def test_coord_query_args_resp3(env):
    """Held arguments preserve RESP3 results, including profile and debug wrappers."""
    _check_coord_query_args(env)


@skip(cluster=False, min_shards=2)
def test_coord_query_args_missing_query(env):
    """Reject missing queries before constructing the parser's argument slice."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    config = env.cmd('CONFIG', 'GET', 'search-on-timeout')
    policy = config['search-on-timeout'] if isinstance(config, dict) else config[1]
    try:
        env.expect('CONFIG', 'SET', 'search-on-timeout', 'return').ok()
        env.expect('FT.PROFILE', 'idx', 'SEARCH', 'LIMITED', 'QUERY').error().contains(
            'No query string provided')
        for command in (
            ['FT.SEARCH', 'idx'],
            ['FT.PROFILE', 'idx', 'SEARCH', 'QUERY'],
            ['FT.PROFILE', 'idx', 'SEARCH', 'LIMITED', 'QUERY'],
        ):
            env.expect(debug_cmd(), *command, 'TIMEOUT_AFTER_N', 1000,
                       'DEBUG_PARAMS_COUNT', 2).error().contains('No query string provided')
        env.assertTrue(env.cmd('PING'))
    finally:
        env.expect('CONFIG', 'SET', 'search-on-timeout', policy).ok()
