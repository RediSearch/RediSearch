import numpy as np
import threading
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

    if env.isCluster():
        # Expect 5 jobs done: 3 for the hybrid search + its depleters,
        # 1 for the search equivalent, and 1 for the vector equivalent
        env.assertEqual(getWorkersThpoolStats(env)['totalJobsDone'], 5)
    else:
        # Expect 3 jobs done: 1 for the hybrid search, 1 for the search
        # equivalent, and 1 for the vector equivalent
        env.assertEqual(getWorkersThpoolStats(env)['totalJobsDone'], 3)

    env.assertEqual(getWorkersThpoolStats(env)['numThreadsAlive'], 2)

    # Decrease number of threads
    env.expect(config_cmd(), 'SET', 'WORKERS', '1').ok()
    env.assertEqual(getWorkersThpoolNumThreads(env), 1)
    run_test_scenario(env, 'idx', scenario, query_vector)

    # Drain the thread pool to make sure all jobs are done.
    env.expect(debug_cmd(), 'WORKERS', 'DRAIN').ok()
    if env.isCluster():
        # Expect 10 jobs done: 5 more once the scenario is run again
        env.assertEqual(getWorkersThpoolStats(env)['totalJobsDone'], 10)
    else:
        # Expect 6 jobs done: 3 more once the scenario is run again
        env.assertEqual(getWorkersThpoolStats(env)['totalJobsDone'], 6)

    env.assertEqual(getWorkersThpoolStats(env)['numThreadsAlive'], 1)


# Skipped on musl, which admits a reader past a queued writer, so the writer never
# actually contends there and the test would pass whatever the locking does.
@skip(cluster=True, musl=True)
def test_hybrid_survives_writer_during_depletion():
    """A writer arriving while the depleters are reading the index must delay the
    query, not abort it.

    The execution thread takes one read lock and lends it to the sub-requests, so there
    is no per-depleter try-lock left for a queued writer to win. The park is placed
    before a depleter touches the index, which is the only window where the two designs
    differ: with a per-depleter try-lock the query fails there, with a borrowed lock the
    writer waits for depletion to finish instead.
    """
    env = Env(moduleArgs='WORKERS 2 DEFAULT_DIALECT 2', enableDebugCommand=True)
    skipIfNoEnableAssert(env)
    setup_basic_index(env)
    query_vector = np.array([1.3, 0.0]).astype(np.float32).tobytes()

    sync_point = 'DepleterBeforeIndexRead'
    env.cmd(debug_cmd(), 'SYNC_POINT', 'ARM', sync_point)

    outcome = []
    def run_hybrid_query(conn, out):
        try:
            # TIMEOUT 0 is load-bearing: it disables the sync point's other release
            # arm, leaving the queued writer as the only way out. Without it the
            # query can escape by timing out and the test passes vacuously.
            out.append(('ok', conn.execute_command(
                'FT.HYBRID', 'idx', 'SEARCH', '@text:(hello)',
                'VSIM', '@vector', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector,
                'TIMEOUT', '0')))
        except Exception as e:
            out.append(('err', str(e)))

    query_thread = threading.Thread(target=run_hybrid_query,
                                    args=(env.getConnection(), outcome), daemon=True)
    query_thread.start()
    wait_for_condition(
        lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sync_point) == 1, {}),
        f'Timeout waiting for {sync_point} sync point')

    # Indexing this queues a writer on the spec write lock behind the read lock the
    # depleters are reading under. The counter is bumped before the blocking acquire,
    # so it releases the parked depleters; it returns once the query has finished and
    # the read lock is dropped.
    env.cmd('HSET', 'text:99', 'text', 'hello queued writer', 'type', 'text')

    query_thread.join(timeout=30)
    env.assertFalse(query_thread.is_alive(), message='Query thread never completed')
    if not outcome:
        return  # still parked; the assertion above is the diagnostic

    kind, payload = outcome[0]
    env.assertEqual(kind, 'ok',
                    message=f'a queued writer aborted the query instead of waiting: {payload}')
    env.assertGreater(len(payload), 0, message='query returned no results')

    # The writer really did get through, so nothing leaked the read lock.
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'extra', 'TEXT').ok()
