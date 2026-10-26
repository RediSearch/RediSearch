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
               'label', 'TAG', 'SORTABLE', 'price', 'NUMERIC', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    keys = [f'doc:{{{i}}}:{i:02}' for i in range(12)]
    for i, key in enumerate(keys):
        conn.execute_command('HSET', key, 'v', struct.pack('f', i + 1),
                             'label', f'{12 - i:02}', 'price', 12 - i)
    query = '*=>[KNN 3 @v $vec AS distance]'
    for sorting, order in (([], keys[:3]),
                           (['SORTBY', 'distance', 'ASC'], keys[:3]),
                           (['SORTBY', 'distance', 'DESC'], keys[2::-1]),
                           (['SORTBY', 'price', 'ASC'], keys[2::-1]),
                           (['SORTBY', 'price', 'DESC'], keys[:3]),
                           (['SORTBY', 'label', 'ASC'], keys[2::-1]),
                           (['SORTBY', 'label', 'DESC'], keys[:3])):
        for offset, limit in ((0, 10), (1, 1), (3, 10), (0, 0)):
            args = [query, 'PARAMS', 2, 'vec', struct.pack('f', 0),
                    'SCORER', 'DOCSCORE', *sorting, 'NOCONTENT',
                    'LIMIT', offset, limit, 'DIALECT', 2]
            page = order[offset:offset + limit]
            # Preserve the existing distinction between metadata-only distance sorting
            # and preselection paths, which count returned shard candidates.
            total = 3 if limit or sorting[:2] == ['SORTBY', 'distance'] else 0
            expected = ({'attributes': [], 'warning': [], 'total_results': total,
                         'format': 'STRING',
                         'results': [{'id': key, 'values': []} for key in page]}
                        if resp3 else [total, *page])
            env.assertEqual(env.cmd('FT.SEARCH', 'idx', *args), expected)
            profiled = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', *args)
            env.assertEqual(profiled['Results'] if resp3 else profiled[0], expected)
            # A cached vector distance must not replace the public DOCSCORE value.
            scored = ({**expected, 'results': [dict(row, score=1.0)
                                             for row in expected['results']]}
                      if resp3 else [total, *[value for key in page for value in (key, '1')]])
            env.assertEqual(env.cmd('FT.SEARCH', 'idx', *args, 'WITHSCORES'), scored)


    # An equal-distance candidate on another shard must lose to the smaller document key.
    conn.execute_command('HSET', keys[3], 'v', struct.pack('f', 3))
    tied = env.cmd('FT.SEARCH', 'idx', query, 'PARAMS', 2, 'vec', struct.pack('f', 0),
                   'SCORER', 'DOCSCORE', 'NOCONTENT', 'LIMIT', 0, 3, 'DIALECT', 2)
    expected = ({'attributes': [], 'warning': [], 'total_results': 3, 'format': 'STRING',
                 'results': [{'id': key, 'values': []} for key in keys[:3]]}
                if resp3 else [3, *keys[:3]])
    env.assertEqual(tied, expected)


@skip(cluster=False)
def test_reply_array_knn_resp2(env):
    _check_knn(env, False)


@skip(cluster=False)
def test_reply_array_knn_resp3():
    _check_knn(Env(protocol=3), True)


def _check_buffered_knn_timeout(resp3):
    import struct
    import threading
    from common import (skipIfNoEnableAssert, setPauseBeforeReduce,
                        getIsCoordReducePaused, getCoordReduceCount,
                        resetCoordReduceDebug, wait_for_condition,
                        ON_TIMEOUT_CONFIG)
    from test_blocked_client_timeout import (
        _get_blocked_request_onfree_count, TIMEOUT_WARNING,
        wait_for_blocked_query_client, wait_for_client_unblocked)

    # Workers ensure reduction runs in the background while the main thread times out.
    env = Env(moduleArgs='WORKERS 1', protocol=3 if resp3 else 2)
    skipIfNoEnableAssert(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT', 6,
               'TYPE', 'FLOAT32', 'DIM', 1, 'DISTANCE_METRIC', 'L2',
               'label', 'TAG', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    # One nonempty shard makes the accepted candidate set independent of reply arrival order.
    keys = [f'doc:{{knn}}:{i:02}' for i in range(12)]
    for i, key in enumerate(keys):
        conn.execute_command('HSET', key, 'v', struct.pack('f', i + 1),
                             'label', f'{12 - i:02}')
    config = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)
    previous_policy = config[ON_TIMEOUT_CONFIG] if resp3 else config[1]
    env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict').ok()
    try:
        for offset, limit in ((0, 10), (1, 1)):
            outcome = []
            args = ['FT.SEARCH', 'idx', '*=>[KNN 3 @v $vec AS distance]',
                    'PARAMS', 2, 'vec', struct.pack('f', 0), 'SORTBY', 'label', 'ASC',
                    'NOCONTENT', 'LIMIT', offset, limit, 'TIMEOUT', 0, 'DIALECT', 2]

            def query():
                try:
                    with env.getConnection().client() as client:
                        reply = client.execute_command(*args)
                        # A second unsolicited reply would be consumed as the PING response.
                        outcome.append((reply, client.ping()))
                except Exception as error:
                    outcome.append(error)

            resetCoordReduceDebug(env)
            setPauseBeforeReduce(env, 2)
            worker = threading.Thread(target=query, daemon=True)
            try:
                worker.start()
                wait_for_condition(
                    lambda: (getIsCoordReducePaused(env) == 1, {}),
                    'KNN source did not pause with a candidate buffered')
                env.assertEqual(getCoordReduceCount(env), 1)
                free_before = _get_blocked_request_onfree_count(env)
                client_id = wait_for_blocked_query_client(env, 'FT.SEARCH')
                env.expect('CLIENT', 'UNBLOCK', client_id, 'TIMEOUT').equal(1)
                wait_for_client_unblocked(env, client_id)
                worker.join(timeout=10)
                env.assertFalse(worker.is_alive())
                page = keys[2::-1][offset:offset + limit]
                expected = ({'attributes': [], 'warning': [TIMEOUT_WARNING],
                             'total_results': 3, 'format': 'STRING',
                             'results': [{'id': key, 'values': []} for key in page]}
                            if resp3 else [3, *page])
                env.assertEqual(outcome, [(expected, True)])
                wait_for_condition(
                    lambda: (_get_blocked_request_onfree_count(env) > free_before, {}),
                    'Timed-out KNN request was not freed')
                env.assertEqual(_get_blocked_request_onfree_count(env), free_before + 1)
            finally:
                resetCoordReduceDebug(env)
                worker.join(timeout=10)
    finally:
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, previous_policy).ok()


@skip(cluster=False)
def test_buffered_knn_timeout_resp2():
    """Strict timeout preserves buffered nearest-K candidates and applies pagination once."""
    _check_buffered_knn_timeout(False)


@skip(cluster=False)
def test_buffered_knn_timeout_resp3():
    """The buffered KNN handoff retains RESP3 warnings and exactly-once cleanup."""
    _check_buffered_knn_timeout(True)
