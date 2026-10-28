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

    def expected(order, offset=0, limit=20, with_sortkeys=False):
        page = order[offset:offset + limit]

        def sortkey(key):
            i = keys.index(key)
            return f'#{i // 3}' if i < 10 else None

        if not resp3:
            values = [v for key in page for v in (key, sortkey(key))] if with_sortkeys else page
            return [len(keys), *values]
        rows = [{'id': key, 'values': []} for key in page]
        if with_sortkeys:
            rows = [dict(row, sortkey=sortkey(row['id'])) for row in rows]
        return {'attributes': [], 'warning': [], 'total_results': len(keys),
                'format': 'STRING', 'results': rows}

    for direction in ('ASC', 'DESC'):
        present = sorted(keys[:10], key=lambda key: (keys.index(key) // 3, key),
                         reverse=direction == 'DESC')
        missing = sorted(keys[10:], reverse=direction == 'DESC')
        order = present + missing
        for offset, limit in ((0, 20), (2, 5), (11, 5), (20, 5), (0, 0)):
            args = ['idx', '*', 'SORTBY', 'n', direction, 'NOCONTENT', 'LIMIT', offset, limit]
            for with_sortkeys in (False, True):
                options = ['WITHSORTKEYS'] if with_sortkeys else []
                result = env.cmd('FT.SEARCH', *args, *options)
                env.assertEqual(result, expected(order, offset, limit, with_sortkeys))
                profiled = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', *args[1:], *options)
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
            if sorting:
                field = sorting[1]

                def sortkey(key):
                    i = keys.index(key)
                    if field == 'distance':
                        return f'#{(i + 1) ** 2}'
                    if field == 'price':
                        return f'#{12 - i}'
                    return f'${12 - i:02}'

                with_keys = ({**expected, 'results': [dict(row, sortkey=sortkey(row['id']))
                                                     for row in expected['results']]}
                             if resp3 else [total, *[value for key in page
                                                   for value in (key, sortkey(key))]])
                env.assertEqual(env.cmd('FT.SEARCH', 'idx', *args, 'WITHSORTKEYS'), with_keys)
                profiled_keys = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY',
                                        *args, 'WITHSORTKEYS')
                env.assertEqual(profiled_keys['Results'] if resp3 else profiled_keys[0], with_keys)



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
    env.expect('FT.CREATE', 'idx', 'PAYLOAD_FIELD', 'payload', 'SCHEMA', 'v', 'VECTOR', 'FLAT', 6,
               'TYPE', 'FLOAT32', 'DIM', 1, 'DISTANCE_METRIC', 'L2',
               'label', 'TAG', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    # One nonempty shard makes the accepted candidate set independent of reply arrival order.
    keys = [f'doc:{{knn}}:{i:02}' for i in range(12)]
    for i, key in enumerate(keys):
        conn.execute_command('HSET', key, 'v', struct.pack('f', i + 1),
                             'label', f'{12 - i:02}', 'body', f'body\0{i}', 'payload', f'payload\0{i}')
    config = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)
    previous_policy = config[ON_TIMEOUT_CONFIG] if resp3 else config[1]
    env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict').ok()
    try:
        for offset, limit in ((0, 10), (1, 1)):
            outcome = []
            args = ['FT.SEARCH', 'idx', '*=>[KNN 3 @v $vec AS distance]',
                    'PARAMS', 2, 'vec', struct.pack('f', 0), 'SORTBY', 'label', 'ASC',
                    'RETURN', 1, 'body', 'WITHPAYLOADS', 'WITHSORTKEYS', 'LIMIT', offset, limit, 'TIMEOUT', 0, 'DIALECT', 2]

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
                             'results': [{'id': key, 'payload': f'payload\0{keys.index(key)}',
                                          'sortkey': f'${12 - keys.index(key):02}',
                                          'extra_attributes': {'body': f'body\0{keys.index(key)}'},
                                          'values': []} for key in page]}
                            if resp3 else [3, *[value for key in page for value in (
                                key, f'payload\0{keys.index(key)}', f'${12 - keys.index(key):02}',
                                ['body', f'body\0{keys.index(key)}'])]])
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


def _check_reply_admission_timeout(resp3):
    import threading
    from common import (debug_cmd, distinct_shard_tags, skipIfNoEnableAssert,
                        wait_for_condition, ON_TIMEOUT_CONFIG)
    from test_blocked_client_timeout import (
        _get_blocked_request_onfree_count, TIMEOUT_WARNING,
        wait_for_blocked_query_client)

    # Coordinator reduction still uses its pool; synchronous shard execution keeps
    # local shard free callbacks out of the coordinator lifetime counter.
    env = Env(moduleArgs='WORKERS 0', protocol=3 if resp3 else 2)
    skipIfNoEnableAssert(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    keys = [f'doc:{{{tag}}}' for tag in distinct_shard_tags(env.getConnection())]
    for key in keys:
        conn.execute_command('HSET', key, 't', 'hello')
    config = env.cmd('CONFIG', 'GET', ON_TIMEOUT_CONFIG)
    previous_policy = config[ON_TIMEOUT_CONFIG] if resp3 else config[1]
    env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, 'return-strict').ok()
    try:
        for checkpoint, accepted in (('BeforeCoordSearchReplyAccept', 0),
                                     ('BeforeCoordSearchReplyPublish', 0),
                                     ('BeforeCoordSearchNextReplyPublish', 1),
                                     ('AfterCoordSearchReplyAccept', 1)):
            for profile in (False, True):
                outcome = []
                args = (['FT.PROFILE', 'idx', 'SEARCH', 'QUERY'] if profile
                        else ['FT.SEARCH', 'idx']) + ['*', 'NOCONTENT', 'TIMEOUT', 0]

                def query():
                    try:
                        with env.getConnection().client() as client:
                            outcome.append((client.execute_command(*args), client.ping()))
                    except Exception as error:
                        outcome.append(error)

                free_before = _get_blocked_request_onfree_count(env)
                env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', checkpoint).ok()
                worker = threading.Thread(target=query, daemon=True)
                try:
                    worker.start()
                    wait_for_condition(
                        lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING',
                                         checkpoint) == 1, {}),
                        'No shard reply reached the admission checkpoint')
                    client_id = wait_for_blocked_query_client(env, args[0])
                    env.expect('CLIENT', 'UNBLOCK', client_id, 'TIMEOUT').equal(1)
                    worker.join(timeout=10)
                    env.assertFalse(worker.is_alive())
                    env.assertEqual(len(outcome), 1, message=outcome)
                    env.assertTrue(isinstance(outcome[0], tuple), message=outcome)
                    reply, ping = outcome[0]
                    env.assertTrue(ping)
                    if profile:
                        shards = reply['Profile']['Shards'] if resp3 else reply[1][1]
                        env.assertEqual(len(shards), accepted, message=reply)
                        reply = reply['Results'] if resp3 else reply[0]
                    if resp3:
                        env.assertEqual(reply['total_results'], accepted, message=reply)
                        env.assertEqual(reply['warning'], [TIMEOUT_WARNING], message=reply)
                        ids = [row['id'] for row in reply['results']]
                    else:
                        env.assertEqual(reply[0], accepted, message=reply)
                        ids = reply[1:]
                    env.assertEqual(len(ids), accepted, message=reply)
                    for key in ids:
                        env.assertIn(key, keys)
                    # The timeout reply did not release backing storage while callbacks remain.
                    env.assertEqual(_get_blocked_request_onfree_count(env), free_before)
                finally:
                    env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', checkpoint)
                    worker.join(timeout=10)
                wait_for_condition(
                    lambda: (_get_blocked_request_onfree_count(env) == free_before + 1, {}),
                    'Final shard callback did not free the timed-out request exactly once')
    finally:
        env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
        env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, previous_policy).ok()


@skip(cluster=False)
def test_reply_admission_timeout_resp2():
    """Strict timeout freezes accepted replies and keeps ownership until late callbacks finish."""
    _check_reply_admission_timeout(False)


@skip(cluster=False)
def test_reply_admission_timeout_resp3():
    """RESP3 search/profile serialize only the frozen replies, including timeout warnings."""
    _check_reply_admission_timeout(True)


def _check_zero_dispatch(resp3):
    from common import debug_cmd, skipIfNoEnableAssert, wait_for_condition
    from test_blocked_client_timeout import _get_blocked_request_onfree_count

    # Isolate coordinator cleanup from shard worker callbacks on the recovery query.
    env = Env(moduleArgs='WORKERS 0', protocol=3 if resp3 else 2)
    skipIfNoEnableAssert(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    getConnectionByEnv(env).execute_command('HSET', 'doc:{zero-dispatch}', 't', 'hello')
    args = ['*', 'NOCONTENT', 'TIMEOUT', 0]
    expected = env.cmd('FT.SEARCH', 'idx', *args)
    for profile in (False, True):
        free_before = _get_blocked_request_onfree_count(env)
        command = (['FT.PROFILE', 'idx', 'SEARCH', 'QUERY'] if profile
                   else ['FT.SEARCH', 'idx']) + args
        env.expect(debug_cmd(), 'SEND_ERROR', env.shardsCount).ok()
        try:
            env.expect(*command).error().contains('Could not send query to cluster')
            wait_for_condition(
                lambda: (_get_blocked_request_onfree_count(env) == free_before + 1, {}),
                'Zero-dispatch reducer did not free its unchained source exactly once')
            env.assertTrue(env.cmd('PING'))
            env.assertEqual(env.cmd('FT.SEARCH', 'idx', *args), expected)
            env.assertEqual(_get_blocked_request_onfree_count(env), free_before + 2)
        finally:
            env.cmd(debug_cmd(), 'SEND_ERROR', 0)


@skip(cluster=False)
def test_zero_dispatch_resp2():
    """Normal zero-dispatch search/profile complete through the reducer and clean up once."""
    _check_zero_dispatch(False)


@skip(cluster=False)
def test_zero_dispatch_resp3():
    """RESP3 zero-dispatch errors leave the coordinator ready for the next query."""
    _check_zero_dispatch(True)


def _check_borrowed_row_serialization(env, resp3):
    env.expect('FT.CREATE', 'idx', 'PAYLOAD_FIELD', 'payload', 'SCHEMA',
               'rank', 'TAG', 'SORTABLE', 'text', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    keys = [f'doc:{{{i}}}:\0{i}' for i in range(6)]
    binary = 'a\0b'
    large = '9007199254740993'
    for i, key in enumerate(keys):
        conn.execute_command('HSET', key, 'rank', f'r{i}', 'text', binary,
                             'large', large, 'payload', binary)
    for direction, selected in (('ASC', [0, 1]), ('DESC', [5, 4])):
        args = ['*', 'SCORER', 'DOCSCORE', 'WITHSCORES', 'EXPLAINSCORE',
                'WITHPAYLOADS', 'WITHSORTKEYS', 'SORTBY', 'rank', direction,
                'RETURN', 4, 'text', 'AS', 'val', 'large', 'LIMIT', 0, 2]
        if resp3:
            expected = {'attributes': [], 'warning': [], 'total_results': 6,
                        'format': 'STRING', 'results': [
                            {'id': keys[i], 'score': [1.0, "Document's score is 1.00"],
                             'payload': binary, 'sortkey': f'$r{i}',
                             'extra_attributes': {'val': binary, 'large': large},
                             'values': []} for i in selected]}
        else:
            expected = [6]
            for i in selected:
                expected.extend([keys[i], ['1', "Document's score is 1.00"], binary,
                                 f'$r{i}', ['val', binary, 'large', large]])
        env.assertEqual(env.cmd('FT.SEARCH', 'idx', *args), expected)
        profiled = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', *args)
        env.assertEqual(profiled['Results'] if resp3 else profiled[0], expected)


def test_borrowed_row_serialization_resp2(env):
    """Sorted rows retain binary fields, payloads, aliases and explanations after eviction."""
    _check_borrowed_row_serialization(env, False)


def test_borrowed_row_serialization_resp3():
    """The map reply preserves string numbers and the SEARCH/PROFILE row representation."""
    _check_borrowed_row_serialization(Env(protocol=3), True)
