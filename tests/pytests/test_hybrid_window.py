# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

import struct
from unittest.mock import ANY

from common import getConnectionByEnv, get_results_from_hybrid_response, to_dict


def test_large_window_preserves_results(env):
    """Huge WINDOW values keep the same results without allocating for absent documents."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'text', 'TEXT',
               'vector', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2',
               'DISTANCE_METRIC', 'L2').ok()
    blob = struct.pack('ff', 1.0, 2.0)
    getConnectionByEnv(env).execute_command('HSET', '{doc}:1', 'text', 'hello', 'vector', blob)

    for method in ('LINEAR', 'RRF'):
        query = ['FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM', '@vector', '$BLOB',
                 'KNN', '2', 'K', '1', 'COMBINE', method, '2', 'WINDOW']
        params = ['PARAMS', '2', 'BLOB', blob]
        expected = env.cmd(*query, '10', *params)
        results, _ = get_results_from_hybrid_response(expected)
        env.assertEqual(list(results), ['{doc}:1'])
        expected = to_dict(expected)
        expected['execution_time'] = ANY
        env.assertEqual(to_dict(env.cmd(*query, '2147483647', *params)), expected)
