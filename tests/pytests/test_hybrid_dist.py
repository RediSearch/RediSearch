from common import *
import threading


@skip(cluster=False)
@require_enable_assert
def test_dist_hybrid_index_drop_after_sctx_allocation(env):
    """MOD-14135: SearchCtx must be freed when index is dropped between
    sctx allocation and IndexSpecRef_Promote in RSExecDistHybrid.
    Leak detection relies on valgrind/sanitizers in CI."""
    set_workers(env, 1)

    dim = 2
    conn = getConnectionByEnv(env)

    env.expect(
        'FT.CREATE', 'idx', 'SCHEMA',
        'name', 'TEXT',
        'embedding', 'VECTOR', 'FLAT', '6',
        'TYPE', 'FLOAT32', 'DIM', str(dim), 'DISTANCE_METRIC', 'L2'
    ).ok()

    query_vec = np.array([0.0] * dim, dtype=np.float32).tobytes()
    doc_vec = np.array([1.0] * dim, dtype=np.float32).tobytes()
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
