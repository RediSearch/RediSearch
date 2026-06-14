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


@skip(cluster=False)
@require_enable_assert
def test_dist_hybrid_unresponsive_shard_does_not_hang(env):
    """A shard that received _FT.HYBRID and stays connected but never produces its
    cursor-mapping reply must not hang FT.HYBRID. The coordinator's setup-phase
    wait (ProcessHybridCursorMappings) must be bounded by the request timeout
    rather than blocking on the missing reply indefinitely.

    Distinct from test_dist_hybrid_shard_dispatch_failure_does_not_hang, where the
    shard never replies because the dispatch itself fails (the MR layer drives
    completion via the error callback). Here the shard is alive and the dispatch
    succeeds; only the bounded wait can unblock the coordinator.

    The BeforeHybridShardReply sync point parks the internal _FT.HYBRID worker
    before it emits the cursor-mapping reply; the shard's main thread stays
    responsive so IS_WAITING / SIGNAL still work."""
    conn, query_vec, doc_vec = _setup_hybrid_index(env)
    for i in range(20):
        conn.execute_command('HSET', f'doc{i}', 'name', 'hello', 'embedding', doc_vec)

    # A finite timeout arms the coordinator's cursor-setup deadline; the harness
    # defaults to TIMEOUT 0 (unlimited). (Bounding the unlimited case is separate.)
    env.expect(config_cmd(), 'SET', 'TIMEOUT', '500').ok()

    # Arm on every shard: the fan-out may land the internal _FT.HYBRID on any of
    # them (incl. the coordinator's own).
    sync_point = 'BeforeHybridShardReply'
    run_command_on_all_shards(env, debug_cmd(), 'SYNC_POINT', 'ARM', sync_point)
    try:
        done = threading.Event()
        def run_query():
            try:
                env.getConnection().execute_command(
                    'FT.HYBRID', 'idx', 'SEARCH', '*',
                    'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vec)
            except Exception:
                pass  # a timeout error is the expected outcome; we only assert it returns
            finally:
                done.set()
        threading.Thread(target=run_query, daemon=True).start()

        # Confirm the stall is actually exercised, then require the query to
        # return at the deadline rather than block on the missing reply.
        wait_for_condition(
            lambda: (1 in run_command_on_all_shards(env, debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sync_point), {}),
            f'Timeout waiting for {sync_point} sync point on any shard')
        env.assertTrue(done.wait(timeout=10),
                       message='FT.HYBRID hung waiting for an unresponsive shard')
    finally:
        # SIGNAL wakes a parked worker; CLEAR disarms so a worker arriving late
        # doesn't park, letting the background reply drain and the env tear down.
        run_command_on_all_shards(env, debug_cmd(), 'SYNC_POINT', 'SIGNAL', sync_point)
        run_command_on_all_shards(env, debug_cmd(), 'SYNC_POINT', 'CLEAR')
