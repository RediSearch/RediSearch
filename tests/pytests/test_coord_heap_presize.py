# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

import numpy as np
from common import *

# Above the coordinator reducer heaps' initial capacity, so the requests below make them grow.
NUM_DOCS = 3000
K = 2000


def _create_index(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE',
               'v', 'VECTOR', 'FLAT', 6, 'TYPE', 'FLOAT32', 'DIM', 2,
               'DISTANCE_METRIC', 'L2').ok()
    conn = getConnectionByEnv(env)
    for i in range(NUM_DOCS):
        # Distance to the origin grows with i; n is a permutation of i, so the two orders differ.
        conn.execute_command('HSET', f'doc:{i}', 'n', (i * 7919) % NUM_DOCS,
                             'v', np.array([i, 0], dtype=np.float32).tobytes())


def _ids(res):
    return [int(doc_id.split(':')[1]) for doc_id in res[1::2]]


def _n_values(res):
    return [int(fields[fields.index('n') + 1]) for fields in res[2::2]]


@skip(cluster=False)
def test_search_limit_grows_reducer_heap(env):
    """FT.SEARCH merges every shard's top offset+limit in one heap; a large LIMIT must still
    return the full order and the right page."""
    _create_index(env)
    max_results = int(env.cmd(config_cmd(), 'GET', 'MAXSEARCHRESULTS')[0][1])

    res = env.cmd('FT.SEARCH', 'idx', '*', 'SORTBY', 'n', 'ASC', 'LIMIT', 0, max_results,
                  'RETURN', 1, 'n')
    env.assertEqual(res[0], NUM_DOCS)
    env.assertEqual(_n_values(res), list(range(NUM_DOCS)))

    res = env.cmd('FT.SEARCH', 'idx', '*', 'SORTBY', 'n', 'DESC', 'LIMIT', 1500, 10,
                  'RETURN', 1, 'n')
    env.assertEqual(_n_values(res), list(range(NUM_DOCS - 1501, NUM_DOCS - 1511, -1)))


@skip(cluster=False)
def test_knn_large_k_grows_reducer_heaps(env):
    """KNN keeps the K nearest in one heap and, when sorting by another field, re-sorts them in the
    offset+limit heap; with K above the initial capacity both must still hold exactly K results."""
    _create_index(env)
    query = f'*=>[KNN {K} @v $blob]'
    blob = np.array([0, 0], dtype=np.float32).tobytes()

    res = env.cmd('FT.SEARCH', 'idx', query, 'PARAMS', 2, 'blob', blob,
                  'LIMIT', 0, K, 'NOCONTENT', 'DIALECT', 2)
    env.assertEqual(res[0], K)
    # Only membership: the cluster reply order of a NOCONTENT KNN query is not by distance.
    env.assertEqual(sorted(int(doc_id.split(':')[1]) for doc_id in res[1:]), list(range(K)))

    res = env.cmd('FT.SEARCH', 'idx', query, 'PARAMS', 2, 'blob', blob,
                  'SORTBY', 'n', 'ASC', 'LIMIT', 0, K, 'RETURN', 1, 'n', 'DIALECT', 2)
    env.assertEqual(res[0], K)
    env.assertEqual(len(set(_ids(res))), K)
    n_values = _n_values(res)
    env.assertEqual(n_values, sorted(n_values))
    # Not asserted: that these are the K nearest. With SORTBY on another field the coordinator ranks
    # the KNN heap by the sort key instead of the distance, which predates the heap growth.
