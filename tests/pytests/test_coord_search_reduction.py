# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from RLTest import Env
from common import getConnectionByEnv, skip


def _check_ordering(env, resp3):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'n', 'NUMERIC', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    # Different tags exercise reduction across shards, including ties and missing keys.
    keys = [f'doc:{{{i}}}:{i:02}' for i in range(12)]
    for i, key in enumerate(keys):
        fields = ['t', 'same'] + (['n', i // 3] if i < 10 else [])
        conn.execute_command('HSET', key, *fields)

    def expected(order, offset=0, limit=20):
        page = order[offset:offset + limit]
        if not resp3:
            return [len(keys), *page]
        return {'attributes': [], 'warning': [], 'total_results': len(keys),
                'format': 'STRING', 'results': [{'id': key, 'values': []} for key in page]}

    for direction in ('ASC', 'DESC'):
        present = sorted(keys[:10], key=lambda key: (keys.index(key) // 3, key),
                         reverse=direction == 'DESC')
        missing = sorted(keys[10:], reverse=direction == 'DESC')
        order = present + missing
        for offset, limit in ((0, 20), (2, 5), (11, 5), (20, 5), (0, 0)):
            args = ['idx', '*', 'SORTBY', 'n', direction, 'NOCONTENT', 'LIMIT', offset, limit]
            result = env.cmd('FT.SEARCH', *args)
            env.assertEqual(result, expected(order, offset, limit))
            profiled = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', *args[1:])
            env.assertEqual(profiled['Results'] if resp3 else profiled[0], result)

    env.assertEqual(env.cmd('FT.SEARCH', 'idx', 'same', 'SCORER', 'DOCSCORE',
                            'NOCONTENT', 'LIMIT', 2, 5), expected(sorted(keys), 2, 5))


@skip(cluster=False)
def test_reply_array_ordering_resp2(env):
    """Preserve coordinator tie ordering, missing-key placement, counts and pagination."""
    _check_ordering(env, False)


@skip(cluster=False)
def test_reply_array_ordering_resp3():
    """Exercise the map decoder and profile envelope with the same ordering contract."""
    env = Env(protocol=3)
    _check_ordering(env, True)


def _check_knn(env, resp3):
    import struct

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT', 6,
               'TYPE', 'FLOAT32', 'DIM', 1, 'DISTANCE_METRIC', 'L2',
               'label', 'TAG', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    keys = [f'doc:{{{i}}}:{i:02}' for i in range(12)]
    for i, key in enumerate(keys):
        conn.execute_command('HSET', key, 'v', struct.pack('f', i + 1),
                             'label', f'{12 - i:02}')
    query = '*=>[KNN 3 @v $vec AS distance]'
    for sorting, order in (([], keys[:3]),
                           (['SORTBY', 'distance', 'ASC'], keys[:3]),
                           (['SORTBY', 'label', 'ASC'], keys[2::-1]),
                           (['SORTBY', 'label', 'DESC'], keys[:3])):
        for offset, limit in ((0, 10), (1, 1), (3, 10)):
            args = [query, 'PARAMS', 2, 'vec', struct.pack('f', 0),
                    'SCORER', 'DOCSCORE', *sorting, 'NOCONTENT',
                    'LIMIT', offset, limit, 'DIALECT', 2]
            page = order[offset:offset + limit]
            expected = ({'attributes': [], 'warning': [], 'total_results': 3,
                         'format': 'STRING',
                         'results': [{'id': key, 'values': []} for key in page]}
                        if resp3 else [3, *page])
            env.assertEqual(env.cmd('FT.SEARCH', 'idx', *args), expected)
            profiled = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', *args)
            env.assertEqual(profiled['Results'] if resp3 else profiled[0], expected)


@skip(cluster=False)
def test_reply_array_knn_resp2(env):
    _check_knn(env, False)


@skip(cluster=False)
def test_reply_array_knn_resp3():
    _check_knn(Env(protocol=3), True)
