# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

import numpy as np
import threading
from concurrent.futures import ThreadPoolExecutor

from redis.exceptions import ResponseError
from RLTest import Env
from common import *
from utils.hybrid import *


def setup_basic_index(env):
    """Setup basic index with test data"""
    conn = env.getClusterConnectionIfNeeded()
    env.expect(
        'FT.CREATE idx SCHEMA '
        'text TEXT '
        'type TAG '
        'vector VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2'
    ).ok

    # Load test data
    texts = [
        'hello world hello again',
        'hello another world',
        'hello amazing another world',
        'lorem ipsum dolor sit amet',
        'consectetur adipiscing elit',
    ]

    # Create 15 documents:
    # 5 with only text, 5 with only vector, and 5 with both fields
    for i in range(len(texts)):
        conn.execute_command(
            'HSET', f'text:{i+1}',
            # Add "text" to the text to make sure we get different scores
            'text', f'{texts[i]}',
            'type', 'text'
        )
        conn.execute_command(
            'HSET', f'vector:{i+1}',
            'type', 'vector',
            'vector', np.array([i+1, 0.0]).astype(np.float32).tobytes()
        )
        conn.execute_command(
            'HSET', f'both:{i+1}',
            # Add "both" to the text to make sure we get different scores
            'text', f'both: {texts[i]}',
            'type', 'both',
            # Add 0.1 to the vector value to make sure we get different scores
            'vector', np.array([i+1.1, 0.0]).astype(np.float32).tobytes()
        )


def test_hybrid_multithread():
    env = Env(moduleArgs='WORKERS 2 DEFAULT_DIALECT 2', enableDebugCommand=True)
    setup_basic_index(env)
    query_vector = np.array([1.3, 0.0]).astype(np.float32).tobytes()

    scenario = {
        "hybrid_query": (
            "SEARCH '@text:(hello|text)' "
            "VSIM @vector $BLOB "
        ),
        "search_equivalent": "@text:(hello|text)",
        "vector_equivalent": "*=>[KNN 10 @vector $BLOB AS vector_distance]"
    }

    # On start up the threadpool is not initialized.
    env.assertEqual(getWorkersThpoolStats(env)['numThreadsAlive'], 0)
    env.assertEqual(getWorkersThpoolNumThreads(env), 2)

    # Trigger thpool initialization.
    run_test_scenario(env, 'idx', scenario, query_vector)
    # Drain the thread pool to make sure all jobs are done.
    env.expect(debug_cmd(), 'WORKERS', 'DRAIN').ok()

    # At two workers hybrid submits one helper in addition to the request itself.
    first_run = 6 if env.isCluster() else 4
    env.assertEqual(getWorkersThpoolStats(env)['totalJobsDone'], first_run)

    env.assertEqual(getWorkersThpoolStats(env)['numThreadsAlive'], 2)

    # Decrease number of threads
    env.expect(config_cmd(), 'SET', 'WORKERS', '1').ok()
    env.assertEqual(getWorkersThpoolNumThreads(env), 1)
    run_test_scenario(env, 'idx', scenario, query_vector)

    # Drain the thread pool to make sure all jobs are done.
    env.expect(debug_cmd(), 'WORKERS', 'DRAIN').ok()
    # At one worker all children and the continuation run on the caller.
    env.assertEqual(getWorkersThpoolStats(env)['totalJobsDone'],
                    first_run + (5 if env.isCluster() else 3))

    env.assertEqual(getWorkersThpoolStats(env)['numThreadsAlive'], 1)


def hybrid_reply_keys(reply):
    results = reply[reply.index('results') + 1]
    return {row[row.index('__key') + 1] for row in results}


@skip(cluster=True)
def test_hybrid_empty_index_rejects_first_insert_after_build():
    """The common build watermark can be zero even when the vector index already exists."""
    env = Env(moduleArgs='WORKERS 1', enableDebugCommand=True)
    skipIfNoEnableAssert(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'text', 'TEXT', 'v', 'VECTOR', 'FLAT', '6',
               'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()
    env.cmd(debug_cmd(), 'VECSIM_INFO', 'idx', 'v')
    blob = np.array([1, 2], dtype=np.float32).tobytes()
    query = ('FT.HYBRID', 'idx', 'SEARCH', 'hello', 'VSIM', '@v', '$blob',
             'PARAMS', '2', 'blob', blob, 'TIMEOUT', '0')
    point = 'BeforeHybridDepletion'
    replies = []
    def run():
        replies.append(env.getConnection().execute_command(*query))
    worker = threading.Thread(target=run, daemon=True)
    env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', point).ok()
    try:
        worker.start()
        wait_for_condition(
            lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', point) == 1, {}),
            'query did not finish building on the empty index')
        env.cmd('HSET', 'first', 'text', 'hello', 'v', blob)
    finally:
        env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point)
        worker.join(timeout=10)
        env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
    env.assertFalse(worker.is_alive())
    env.assertEqual(len(replies), 1)
    if replies:
        env.assertEqual(hybrid_reply_keys(replies[0]), set(), message=replies[0])
    env.assertEqual(hybrid_reply_keys(env.cmd(*query)), {'first'})


@skip(cluster=True)
def test_hybrid_writer_not_blocked_before_depletion():
    """A writer must not wait for a background hybrid query that has built its
    pipelines but has not started its depleters yet, and the documents it adds
    stay out of that query.
    """
    env = Env(moduleArgs='WORKERS 2 DEFAULT_DIALECT 2', enableDebugCommand=True)
    skipIfNoEnableAssert(env)
    setup_basic_index(env)
    query_vector = np.array([1.3, 0.0]).astype(np.float32).tobytes()
    query = ('FT.HYBRID', 'idx', 'SEARCH', '@text:(hello)',
             'VSIM', '@vector', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector,
             'LIMIT', '0', '100')
    # One late document per subquery, each a top match for it.
    late_keys = {'late:text', 'late:vector'}

    sync_point = 'BeforeHybridDepletion'
    env.cmd(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point)

    outcome = []
    def run_hybrid_query(conn, out):
        try:
            # TIMEOUT 0 leaves the signal as the only release of the sync point.
            out.append(('ok', conn.execute_command(*query, 'TIMEOUT', '0')))
        except Exception as e:
            out.append(('err', str(e)))

    query_thread = threading.Thread(target=run_hybrid_query,
                                    args=(env.getConnection(), outcome), daemon=True)
    query_thread.start()
    wait_for_condition(
        lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sync_point) == 1, {}),
        f'Timeout waiting for {sync_point} sync point')

    def write_late_documents(conn):
        conn.execute_command('HSET', 'late:text', 'text', 'hello hello hello', 'type', 'text')
        conn.execute_command('HSET', 'late:vector', 'type', 'vector', 'vector', query_vector)

    writer_thread = threading.Thread(target=write_late_documents,
                                     args=(env.getConnection(),), daemon=True)
    writer_thread.start()
    writer_thread.join(timeout=10)
    env.assertFalse(writer_thread.is_alive(), message='Writer waited for the parked query')
    env.assertEqual(env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sync_point), 1)

    env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point)
    query_thread.join(timeout=30)
    env.assertFalse(query_thread.is_alive(), message='Query thread never completed')
    writer_thread.join(timeout=30)
    if not outcome or writer_thread.is_alive():
        return  # still blocked; the assertions above are the diagnostic

    kind, payload = outcome[0]
    env.assertEqual(kind, 'ok', message=f'expected the query to be served, got: {payload}')
    if kind != 'ok':
        return
    env.assertEqual(hybrid_reply_keys(payload) & late_keys, set())
    # The late documents match the query, so their absence above comes from the bound.
    env.assertEqual(hybrid_reply_keys(env.cmd(*query)) & late_keys, late_keys)


INDEX_CHANGED_ERROR = 'Index changed before the query started; retry'


def setup_numeric_index(env):
    conn = env.getConnection()
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC',
               'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2',
               'DISTANCE_METRIC', 'L2').ok()
    for i in range(1, 6):
        conn.execute_command('HSET', f'doc:{i}', 'n', i,
                             'v', np.array([i, 0.0]).astype(np.float32).tobytes())


def split_numeric_tree(env):
    """Add enough distinct values to split the numeric tree's only range, which bumps its
    revision and so aborts the numeric iterator on its next revalidation."""
    def revision():
        return to_dict(env.cmd(debug_cmd(), 'NUMIDX_SUMMARY', 'idx', 'n'))['revisionId']

    before = revision()
    conn = env.getConnection()
    for i in range(500):
        conn.execute_command('HSET', f'late:{i}', 'n', 2000 + i)
    env.assertNotEqual(revision(), before,
                       message='the inserts must change the numeric tree, or nothing aborts')


def run_in_thread(conn, query):
    outcome = []

    def run():
        try:
            outcome.append(('ok', conn.execute_command(*query)))
        except Exception as e:
            outcome.append(('err', str(e)))

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread, outcome


def hybrid_numeric_query(*extra):
    query_vector = np.array([1.3, 0.0]).astype(np.float32).tobytes()
    return ('SEARCH', '@n:[0 100]', 'VSIM', '@v', '$BLOB', *extra,
            'PARAMS', '2', 'BLOB', query_vector)


@skip(cluster=True)
def test_hybrid_fails_when_numeric_tree_changes_before_depletion():
    """A numeric tree change between the build and the depletion aborts the search
    subquery's iterators before they return a row, so the query fails rather than
    answering that subquery as empty."""
    env = Env(moduleArgs='WORKERS 2 DEFAULT_DIALECT 2', enableDebugCommand=True)
    skipIfNoEnableAssert(env)
    setup_numeric_index(env)

    sync_point = 'BeforeHybridDepletion'
    env.cmd(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point)
    # TIMEOUT 0 leaves the signal as the only release of the sync point.
    thread, outcome = run_in_thread(
        env.getConnection(), ('FT.HYBRID', 'idx', *hybrid_numeric_query(), 'TIMEOUT', '0'))
    wait_for_condition(
        lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sync_point) == 1, {}),
        f'Timeout waiting for {sync_point} sync point')

    split_numeric_tree(env)

    env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point)
    thread.join(timeout=30)
    env.assertFalse(thread.is_alive(), message='Query thread never completed')
    env.assertEqual(outcome[0][0] if outcome else None, 'err', message=outcome)
    if outcome:
        env.assertContains(INDEX_CHANGED_ERROR, outcome[0][1])


@skip(cluster=True)
def test_hybrid_cursor_reports_subquery_error():
    """The shard's cursor path replies with the failing subquery's own error."""
    env = Env(moduleArgs='WORKERS 2 DEFAULT_DIALECT 2', enableDebugCommand=True)
    skipIfNoEnableAssert(env)
    setup_numeric_index(env)

    shard_conn = env.getConnection()
    shard_conn.execute_command('DEBUG', 'MARK-INTERNAL-CLIENT')
    setPauseBeforeHybridStoreCursors(env, True)
    thread, outcome = run_in_thread(
        shard_conn,
        ('_FT.HYBRID', 'idx', *hybrid_numeric_query('WITHCURSOR'),
         '_COORD_DISPATCH_TIME', '1000000'))
    wait_for_condition(
        lambda: (getIsHybridStoreCursorsPaused(env) == 1, {}),
        'Timeout waiting for the hybrid cursor pause')

    split_numeric_tree(env)

    resetHybridStoreCursorsDebug(env)
    thread.join(timeout=30)
    env.assertFalse(thread.is_alive(), message='Query thread never completed')
    env.assertEqual(outcome[0][0] if outcome else None, 'err', message=outcome)
    if outcome:
        env.assertContains(INDEX_CHANGED_ERROR, outcome[0][1])


@skip(cluster=True)
def test_hybrid_filtered_knn_keeps_k_with_late_document():
    """A document added after the build of a background hybrid query must not
    take one of the K places of its filtered KNN subquery.
    """
    env = Env(moduleArgs='WORKERS 2 DEFAULT_DIALECT 2', enableDebugCommand=True)
    skipIfNoEnableAssert(env)
    setup_basic_index(env)
    query_vector = np.array([1.3, 0.0]).astype(np.float32).tobytes()
    k = 3
    # The SEARCH clause matches nothing, so the reply holds only the KNN results.
    query = ('FT.HYBRID', 'idx', 'SEARCH', '@text:(nomatch)',
             'VSIM', '@vector', '$BLOB', 'KNN', '2', 'K', str(k),
             'FILTER', '@type:{vector}', 'PARAMS', '2', 'BLOB', query_vector,
             'LIMIT', '0', '100')

    sync_point = 'BeforeHybridDepletion'
    env.cmd(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point)

    outcome = []
    def run_hybrid_query(conn, out):
        try:
            # TIMEOUT 0 leaves the signal as the only release of the sync point.
            out.append(('ok', conn.execute_command(*query, 'TIMEOUT', '0')))
        except Exception as e:
            out.append(('err', str(e)))

    query_thread = threading.Thread(target=run_hybrid_query,
                                    args=(env.getConnection(), outcome), daemon=True)
    query_thread.start()
    wait_for_condition(
        lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sync_point) == 1, {}),
        f'Timeout waiting for {sync_point} sync point')

    # The closest vector to the query, so it would take the first place.
    env.cmd('HSET', 'late:vector', 'type', 'vector', 'vector', query_vector)

    env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point)
    query_thread.join(timeout=30)
    env.assertFalse(query_thread.is_alive(), message='Query thread never completed')
    if not outcome:
        return  # still parked; the assertion above is the diagnostic

    kind, payload = outcome[0]
    env.assertEqual(kind, 'ok', message=f'expected the query to be served, got: {payload}')
    if kind != 'ok':
        return
    keys = hybrid_reply_keys(payload)
    env.assertNotContains('late:vector', keys)
    env.assertEqual(len(keys), k, message=f'expected {k} KNN results, got: {sorted(keys)}')
    # The late document is a top match, so its absence above comes from the bound.
    env.assertContains('late:vector', hybrid_reply_keys(env.cmd(*query)))

def test_hybrid_one_worker_completes_concurrent_requests():
    """A busy search pool and a single coordinator worker can finish every client's fusion."""
    env = Env(moduleArgs='WORKERS 1 MIN_OPERATION_WORKERS 1 SEARCH_THREADS 1', protocol=3)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'v', 'VECTOR', 'FLAT', '6',
               'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()
    blob = np.array([1, 2], dtype=np.float32).tobytes()
    conn = getConnectionByEnv(env)
    clients = 8
    for i in range(clients):
        conn.execute_command('HSET', f'doc:{{text{i}}}', 't', f'token{i}')
    conn.execute_command('HSET', 'doc:{vector}', 'v', blob)
    start = threading.Barrier(clients, timeout=15)

    def query(i):
        with env.getConnection() as client:
            client.connection_pool.connection_kwargs['socket_timeout'] = 15
            start.wait()
            return client.execute_command('FT.HYBRID', 'idx', 'SEARCH', f'token{i}', 'VSIM',
                                          '@v', '$blob', 'PARAMS', '2', 'blob', blob)

    with ThreadPoolExecutor(max_workers=clients) as pool:
        for i, response in enumerate(pool.map(query, range(clients))):
            rows, total = get_results_from_hybrid_response(response)
            env.assertEqual(total, 2, message=response)
            env.assertEqual(set(rows), {f'doc:{{text{i}}}', 'doc:{vector}'}, message=response)
            env.assertEqual(response['warnings'], [])


def test_hybrid_initial_dispatch_rejection():
    """A refused first job replies once and releases the blocked request and reserved cursors."""
    env = Env(moduleArgs='WORKERS 1', protocol=3, enableDebugCommand=True)
    skipIfNoEnableAssert(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'v', 'VECTOR', 'FLAT', '6',
               'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()
    public_query = ('FT.HYBRID', 'idx', *hybrid_numeric_query())
    internal_query = ('_FT.HYBRID', 'idx', *hybrid_numeric_query('WITHCURSOR'),
                      '_COORD_DISPATCH_TIME', '1000000')
    def free_count():
        return env.cmd(debug_cmd(), 'QUERY_CONTROLLER', 'GET_BLOCKED_REQUEST_ONFREE_COUNT')

    with env.getConnection() as client, env.getConnection() as shard:
        shard.execute_command('DEBUG', 'MARK-INTERNAL-CLIENT')
        for policy in ('return', 'fail', 'return-strict'):
            env.expect('CONFIG', 'SET', ON_TIMEOUT_CONFIG, policy).ok()
            env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', 'RejectHybridDispatch').ok()
            try:
                for connection, query in ((client, public_query), (shard, internal_query)):
                    before = free_count()
                    try:
                        connection.execute_command(*query)
                        env.assertTrue(False, message=f'{policy} accepted a refused dispatch')
                    except ResponseError as error:
                        env.assertContains('Unable to schedule hybrid query', str(error))
                    wait_for_condition(lambda: (free_count() == before + 1, {}),
                                       'refused request did not finish exactly once')
                    env.assertTrue(connection.ping())
                stats = shard.execute_command('_FT.INFO', 'idx')['cursor_stats']
                env.assertEqual(stats['index_total'], 0)
            finally:
                env.expect(debug_cmd(), 'SYNC_POINT', 'CLEAR').ok()
            env.assertEqual(client.execute_command(*public_query)['warnings'], [])
