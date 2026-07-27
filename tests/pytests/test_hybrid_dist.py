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
def test_dist_hybrid_shard_dispatch_failure_does_not_hang(env):
    """A no-reply shard dispatch failure must surface as an error
    rather than hanging FT.HYBRID on the cursor-setup wait.
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
