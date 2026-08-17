from common import *
import threading


def _setup_hybrid_index(env, dim=2):
    """Create the standard hybrid index used by these tests and return
    (conn, query_vec, doc_vec)."""
    set_workers(env, 1)
    conn = getConnectionByEnv(env)
    env.expect(
        'FT.CREATE', 'idx', 'SCHEMA',
        'name', 'TEXT',
        'embedding', 'VECTOR', 'FLAT', '6',
        'TYPE', 'FLOAT32', 'DIM', str(dim), 'DISTANCE_METRIC', 'L2'
    ).ok()
    query_vec = np.array([0.0] * dim, dtype=np.float32).tobytes()
    doc_vec = np.array([1.0] * dim, dtype=np.float32).tobytes()
    return conn, query_vec, doc_vec


@skip(cluster=False)
@require_enable_assert
def test_dist_hybrid_index_drop_after_sctx_allocation(env):
    """MOD-14135: SearchCtx must be freed when index is dropped between
    sctx allocation and IndexSpecRef_Promote in RSExecDistHybrid.
    Leak detection relies on valgrind/sanitizers in CI."""
    conn, query_vec, doc_vec = _setup_hybrid_index(env)
    conn.execute_command('HSET', 'doc1', 'name', 'hello', 'embedding', doc_vec)

    sync_point = 'BeforeDistHybridPromote'
    env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
    env.cmd(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point)

    error_holder = []
    def run_hybrid_query(conn, errors):
        try:
            conn.execute_command(
                'FT.HYBRID', 'idx',
                'SEARCH', '*',
                'VSIM', '@embedding', '$BLOB',
                'PARAMS', '2', 'BLOB', query_vec
            )
        except Exception as e:
            errors.append(e)

    query_conn = env.getConnection()
    query_thread = threading.Thread(
        target=run_hybrid_query,
        args=(query_conn, error_holder),
        daemon=True
    )
    query_thread.start()

    wait_for_condition(
        lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sync_point) == 1, {}),
        f'Timeout waiting for {sync_point} sync point'
    )

    env.expect('FT.DROPINDEX', 'idx').ok()

    env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point)

    query_thread.join(timeout=10)
    env.assertFalse(query_thread.is_alive(), message='Query thread is still blocked after signal')

    env.assertEqual(len(error_holder), 1, message='Expected query to fail with an error')
    error_msg = str(error_holder[0])
    env.assertTrue(error_msg.startswith('SEARCH_INDEX_DROPPED_BG'),
                   message=f'Expected error to start with SEARCH_INDEX_DROPPED_BG, got: {error_msg}')


@skip(cluster=False)
@require_enable_assert
def test_dist_hybrid_shard_dispatch_failure_does_not_hang(env):
    """A no-reply shard dispatch failure must surface as an error
    rather than hanging FT.HYBRID on ProcessHybridCursorMappings' channel wait.
    FT.DEBUG SEND_ERROR forces the next MRCluster_SendCommand to return REDIS_ERR."""
    conn, query_vec, doc_vec = _setup_hybrid_index(env)
    for i in range(10):
        conn.execute_command('HSET', f'doc{i}', 'name', 'hello', 'embedding', doc_vec)

    # Arm the coordinator to fail the next shard dispatch (the FT.HYBRID fan-out).
    env.cmd(debug_cmd(), 'SEND_ERROR', '1')

    error_holder = []
    result_holder = []
    def run_hybrid_query(c, errors, results):
        try:
            results.append(c.execute_command(
                'FT.HYBRID', 'idx',
                'SEARCH', '*',
                'VSIM', '@embedding', '$BLOB',
                'PARAMS', '2', 'BLOB', query_vec
            ))
        except Exception as e:
            errors.append(e)

    query_conn = env.getConnection()
    query_thread = threading.Thread(
        target=run_hybrid_query,
        args=(query_conn, error_holder, result_holder),
        daemon=True
    )
    query_thread.start()
    query_thread.join(timeout=15)

    env.assertFalse(query_thread.is_alive(),
                    message='FT.HYBRID hung after a shard dispatch failure')
    env.assertEqual(len(error_holder), 1,
                    message=f'Expected a communication error, got results: {result_holder}')
    env.assertContains('Could not send query to cluster', str(error_holder[0]))


def _hybrid_score_groups(response):
    """Ordered list of (score, set-of-keys) groups for a hybrid reply.

    Docs sharing a combined RRF score are grouped together, so a comparison ignores the order
    within a tie (broken by doc id on the tail, which is intentionally not cross-shard stable)
    while still asserting the per-branch ranks: the score sequence and the docs at each score."""
    results, _ = get_results_from_hybrid_response(response)
    groups = []
    for key, fields in results.items():
        score = fields.get('__score')
        if groups and groups[-1][0] == score:
            groups[-1][1].add(key)
        else:
            groups.append((score, {key}))
    return groups


@skip(cluster=False)
def test_dist_hybrid_rrf_score_tie_order_is_stable(env):
    """RRF scoring must stay stable regardless of which shard serves as coordinator. The numeric
    SEARCH branch (all text scores 0) and low-cardinality vectors force per-branch ties."""
    n_docs = 1000
    dim = 3
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               'n', 'NUMERIC', 'SORTABLE',
               'text', 'TEXT',
               'vector', 'VECTOR', 'FLAT', '6',
               'TYPE', 'FLOAT32', 'DIM', str(dim), 'DISTANCE_METRIC', 'L2').ok()

    with env.getClusterConnectionIfNeeded() as con:
        for i in range(n_docs):
            vec = np.array([float(i % 10), float((i * 2) % 10), float((i * 3) % 10)], dtype=np.float32)
            con.execute_command('HSET', f'doc:{i}', 'n', i, 'text', f'document {i}', 'vector', vec.tobytes())
    waitForIndex(env, 'idx')

    # WINDOW = n_docs so every matching doc is returned, with no tie straddling the window cutoff.
    query_vec = np.array([5.0] * dim, dtype=np.float32).tobytes()
    query = ('FT.HYBRID', 'idx',
             'SEARCH', f'@n:[0 {n_docs}]',
             'VSIM', '@vector', '$BLOB', 'KNN', '2', 'K', str(n_docs),
             'COMBINE', 'RRF', '4', 'CONSTANT', '60', 'WINDOW', str(n_docs),
             'LIMIT', '0', str(n_docs),
             'PARAMS', '2', 'BLOB', query_vec)

    expected = _hybrid_score_groups(env.cmd(*query))
    env.assertGreater(len(expected), 0, message='query returned no results')

    for i, shard in enumerate(env.getOSSMasterNodesConnectionList()):
        got = _hybrid_score_groups(shard.execute_command(*query))
        env.assertEqual(got, expected, message=f'shard {i} returned different scores/docs', depth=1)
