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
    rather than hanging FT.HYBRID on ProcessHybridCursorMappings' completionCond.
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


def _extract_docs_and_scores(result):
    """Extract an ordered [(key, score), ...] list from an FT.HYBRID reply."""
    results_list = None
    for i in range(len(result) - 1):
        if result[i] == 'results':
            results_list = result[i + 1]
            break
    if results_list is None:
        return []
    docs = []
    for doc_result in results_list:
        doc_key = None
        score = None
        for i in range(0, len(doc_result) - 1, 2):
            if doc_result[i] == '__key':
                doc_key = doc_result[i + 1]
            elif doc_result[i] == '__score':
                score = float(doc_result[i + 1])
        if doc_key is not None and score is not None:
            docs.append((doc_key, score))
    return docs


@skip(cluster=False)
def test_dist_hybrid_tiebreak_deterministic_across_coordinators(env):
    """MOD-12438: when hybrid scores are equal, the result order must be
    deterministic (broken by the __key string) regardless of which master
    shard coordinates the query, for both RRF and LINEAR. Before the fix the
    equal-score tiebreak used the per-shard docId (always 0 on the coordinator),
    so ordering differed depending on which shard was the coordinator."""
    n_docs = 512
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               'n', 'NUMERIC', 'SORTABLE',
               'text', 'TEXT',
               'tag', 'TAG',
               'vector', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32',
               'DIM', '3', 'DISTANCE_METRIC', 'L2').ok()

    # Modulo-based vectors make many docs share identical distances, and the
    # numeric-filter SEARCH side yields a 0 text score for every match, so the
    # RRF/LINEAR fused scores tie pervasively -- the condition that exposed the bug.
    with env.getClusterConnectionIfNeeded() as con:
        for i in range(n_docs):
            vector = np.array([float(i % 10), float((i * 2) % 10), float((i * 3) % 10)],
                              dtype=np.float32)
            con.execute_command('HSET', f'doc:{i}',
                                 'n', i,
                                 'text', f'document {i} content',
                                 'tag', 'even' if i % 2 == 0 else 'odd',
                                 'vector', vector.tobytes())

    query_vector = np.array([5.0, 5.0, 5.0], dtype=np.float32).tobytes()

    for combine in (['RRF', '2', 'CONSTANT', '60'],
                    ['LINEAR', '4', 'ALPHA', '0.5', 'BETA', '0.5']):
        label = combine[0]
        query = ('FT.HYBRID', 'idx',
                 'SEARCH', '@n:[69 1420]',
                 'VSIM', '@vector', '$BLOB', 'KNN', '2', 'K', str(n_docs),
                 'COMBINE', *combine,
                 'PARAMS', '2', 'BLOB', query_vector)

        expected = _extract_docs_and_scores(env.cmd(*query))
        env.assertGreater(len(expected), 0, message=f'{label}: query returned no results')

        # Pin the tiebreak direction: within an equal-score run, keys ascend (strcmp).
        for a, b in zip(expected, expected[1:]):
            if a[1] == b[1]:
                env.assertLess(a[0], b[0],
                               message=f'{label}: keys not ascending within score tie: {a} vs {b}')

        # Every master shard, acting as coordinator, must return identical ordering.
        for idx, shard in enumerate(env.getOSSMasterNodesConnectionList()):
            got = _extract_docs_and_scores(shard.execute_command(*query))
            env.assertEqual(got, expected,
                            message=f'{label}: shard {idx} ordering differs from coordinator')
