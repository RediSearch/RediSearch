# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

import re
import threading

from common import *

DIM = 1024
N_INDEXES = 3
N_DOCS = 300
N_DELETED = 100
N_OVERWRITTEN = 100
N_LIVE = N_DOCS - N_DELETED
EF_RUNTIME = 2 * N_DOCS
# Keeps the periodic fork GC from clearing marked-deleted nodes while a test counts them.
NO_PERIODIC_GC = 'FORK_GC_RUN_INTERVAL 1000000'


def _vector(rng):
    return create_np_array_typed(rng.random(DIM)).tobytes()


def _create_indexes(env):
    for i in range(N_INDEXES):
        # EF_CONSTRUCTION and EF_RUNTIME above the index size make KNN exhaustive, so the result
        # assertions are deterministic.
        env.expect('FT.CREATE', f'idx{i}', 'PREFIX', 1, 'doc:', 'SCHEMA',
                   f'v{i}', 'VECTOR', 'HNSW', '8', 'TYPE', 'FLOAT32', 'DIM', DIM,
                   'DISTANCE_METRIC', 'L2', 'EF_CONSTRUCTION', EF_RUNTIME).ok()


def _hset_doc(conn, key, vectors):
    return conn.hset(key, mapping={f'v{i}': vec for i, vec in enumerate(vectors)})


def _load_docs(env, rng, n=N_DOCS):
    conn = getConnectionByEnv(env)
    docs = {}
    for d in range(n):
        docs[f'doc:{d}'] = [_vector(rng) for _ in range(N_INDEXES)]
        _hset_doc(conn, f'doc:{d}', docs[f'doc:{d}'])
    drain_workers(env)
    return docs


def _mutate_in_transaction(env, rng, docs, deleted, overwritten, conn=None):
    """Delete and overwrite documents in a single MULTI/EXEC, updating `docs` to the expected state."""
    pipe = (conn or getConnectionByEnv(env)).pipeline(transaction=True)
    for key in deleted:
        pipe.delete(key)
    new_vectors = {key: [_vector(rng) for _ in range(N_INDEXES)] for key in overwritten}
    for key, vectors in new_vectors.items():
        _hset_doc(pipe, key, vectors)
    replies = pipe.execute()
    env.assertEqual(replies, [1] * len(deleted) + [0] * len(overwritten), depth=1)
    for key in deleted:
        del docs[key]
    docs.update(new_vectors)


def _vecsim_info(env, i):
    return get_vecsim_debug_dict(env, f'idx{i}', f'v{i}')


def _marked_deleted(env, i):
    return to_dict(_vecsim_info(env, i)['BACKEND_INDEX'])['NUMBER_OF_MARKED_DELETED']


def _nearest(env, i, vector, conn=None):
    cmd = ['FT.SEARCH', f'idx{i}', f'*=>[KNN 1 @v{i} $b EF_RUNTIME {EF_RUNTIME} AS score]',
           'PARAMS', 2, 'b', vector, 'RETURN', 1, 'score', 'DIALECT', 2]
    res = conn.execute_command(*cmd) if conn else env.cmd(*cmd)
    return res[1], float(res[2][1])


def _assert_query_results(env, docs, removed_vectors):
    for i in range(N_INDEXES):
        env.assertEqual(env.cmd('FT.SEARCH', f'idx{i}', '*', 'LIMIT', 0, 0), [len(docs)], depth=1)
        for key, vectors in docs.items():
            env.assertEqual(_nearest(env, i, vectors[i]), (key, 0.0), depth=1)
        # A removed vector's exact match must be gone, whichever doc is now nearest to it.
        for vectors in removed_vectors.values():
            env.assertGreater(_nearest(env, i, vectors[i])[1], 0.0, depth=1)


def _converge(env, n_live):
    drain_workers(env)
    for i in range(N_INDEXES):
        # A GC pass executes at most TIERED_HNSW_SWAP_JOBS_THRESHOLD ready swap jobs, so a larger
        # backlog of deletes takes more than one.
        with TimeLimit(30, f'idx{i} kept marked-deleted vectors'):
            while True:
                forceInvokeGC(env, f'idx{i}')
                info = _vecsim_info(env, i)
                if to_dict(info['BACKEND_INDEX'])['NUMBER_OF_MARKED_DELETED'] == 0:
                    break
        backend = to_dict(info['BACKEND_INDEX'])
        env.assertEqual(info['BACKGROUND_INDEXING'], 0, depth=1)
        env.assertEqual(backend['NUMBER_OF_MARKED_DELETED'], 0, depth=1)
        env.assertEqual(backend['INDEX_LABEL_COUNT'], n_live, depth=1)


def _stats(env):
    return getWorkersThpoolStats(env)


def _log_path(env):
    return os.path.join(env.cmd('CONFIG', 'GET', 'dir')[1], env.cmd('CONFIG', 'GET', 'logfile')[1])


def _wait_for_pool_size(env, n):
    with TimeLimit(30, f'workers pool did not settle on {n} threads'):
        while getWorkersThpoolNumThreads(env) != n or _stats(env)['numThreadsAlive'] != n:
            time.sleep(0.1)


def _resize_paused_pool(env, set_cmd):
    """Run `set_cmd` on a paused pool and resume it, atomically. Returns the pool stats read after
    the command, while still paused. A shrink is deferred until the resume."""
    pipe = getConnectionByEnv(env).pipeline(transaction=True)
    pipe.execute_command(*set_cmd)
    pipe.execute_command(debug_cmd(), 'WORKERS', 'STATS')
    pipe.execute_command(debug_cmd(), 'WORKERS', 'RESUME')
    return to_dict(pipe.execute()[1])


def _queue_repairs(env, rng, docs, removed_vectors, n):
    """With the pool paused, delete and overwrite `n` docs each; returns the paused pool stats."""
    env.expect(debug_cmd(), 'WORKERS', 'PAUSE').ok()
    keys = list(docs)
    deleted, overwritten = keys[:n], keys[n:2 * n]
    removed_vectors.update({key: docs[key] for key in deleted + overwritten})
    _mutate_in_transaction(env, rng, docs, deleted, overwritten)
    stats = _stats(env)
    env.assertGreater(stats['totalPendingJobs'], 0, message=stats, depth=1)
    return stats


def _assert_ran_exactly_once(env, queued):
    """Every job queued at `queued` ran exactly once, and nothing else was submitted since."""
    drain_workers(env)
    stats = _stats(env)
    env.assertEqual(stats['totalPendingJobs'], 0, message=stats, depth=1)
    env.assertEqual(stats['totalJobsDone'], queued['totalJobsDone'] + queued['totalPendingJobs'],
                    message=(queued, stats), depth=1)


@skip(cluster=True)
def test_graph_repair_deferred_at_workers_0():
    """At WORKERS 0, deletes and overwrites in a MULTI/EXEC only mark HNSW nodes deleted; the graph
    repair runs on the maintenance worker while queries stay on the main thread."""
    env = Env(moduleArgs=f'WORKERS 0 {NO_PERIODIC_GC}', enableDebugCommand=True)
    env.assertEqual(getWorkersThpoolNumThreads(env), 1)
    rng = np.random.default_rng(18989)
    _create_indexes(env)
    docs = _load_docs(env, rng)

    keys = list(docs)
    deleted = keys[:N_DELETED]
    overwritten = keys[N_DELETED:N_DELETED + N_OVERWRITTEN]
    removed_vectors = {key: docs[key] for key in deleted + overwritten}

    # With the pool paused, anything routed to it would never complete: the transaction and the
    # queries below returning proves they ran on the main thread, and repair is still pending.
    with paused_workers(env):
        _mutate_in_transaction(env, rng, docs, deleted, overwritten)
        for i in range(N_INDEXES):
            env.assertEqual(_marked_deleted(env, i), N_DELETED + N_OVERWRITTEN)
        stats = _stats(env)
        env.assertGreater(stats['lowPriorityPendingJobs'], 0)
        _assert_query_results(env, docs, removed_vectors)
        env.assertEqual(_stats(env)['highPriorityPendingJobs'], 0)

    _converge(env, N_LIVE)
    _assert_query_results(env, docs, removed_vectors)


@skip(cluster=True)
def test_no_maintenance_workers_writes_in_place():
    """MIN_MAINTENANCE_WORKERS 0 keeps the pool empty at WORKERS 0, so vector deletes repair in place."""
    env = Env(moduleArgs='WORKERS 0 MIN_MAINTENANCE_WORKERS 0', enableDebugCommand=True)
    env.assertEqual(getWorkersThpoolNumThreads(env), 0)
    rng = np.random.default_rng(18989)
    _create_indexes(env)
    docs = _load_docs(env, rng)
    keys = list(docs)
    _mutate_in_transaction(env, rng, docs, keys[:N_DELETED], keys[N_DELETED:N_DELETED + N_OVERWRITTEN])
    for i in range(N_INDEXES):
        env.assertEqual(_marked_deleted(env, i), 0)
    env.assertEqual(_stats(env)['totalJobsDone'], 0)


@skip(cluster=True)
def test_workers_transitions_with_pending_repairs():
    """Resizing the pool while repair jobs are queued runs each of them exactly once, and returning
    to WORKERS 0 keeps the maintenance worker."""
    env = Env(moduleArgs='WORKERS 0', enableDebugCommand=True)
    rng = np.random.default_rng(18989)
    _create_indexes(env)
    docs = _load_docs(env, rng)
    removed_vectors = {}

    for workers in [2, 4, 1, 0, 3, 0]:
        queued = _queue_repairs(env, rng, docs, removed_vectors, 20)
        at_resize = _resize_paused_pool(env, [config_cmd(), 'SET', 'WORKERS', workers])
        env.assertEqual(at_resize['totalPendingJobs'], queued['totalPendingJobs'], message=workers)
        _assert_ran_exactly_once(env, queued)
        _wait_for_pool_size(env, max(workers, 1))

    _converge(env, len(docs))
    _assert_query_results(env, docs, removed_vectors)


@skip(cluster=True)
def test_workers_transitions_under_concurrent_load():
    """Queries and mutations from other clients keep working while WORKERS cycles between 0 and 2,
    and the indexes converge to the exact expected state. While repair and ingestion run
    concurrently with queries, HNSW recall is approximate even with an exhaustive EF, as it always
    was with WORKERS > 0, so exactness is only checked once converged."""
    env = Env(moduleArgs='WORKERS 0', enableDebugCommand=True)
    rng = np.random.default_rng(18989)
    _create_indexes(env)
    stable = _load_docs(env, rng, n=100)
    stop = threading.Event()
    errors = []
    progress = {'queries': 0, 'mutations': 0}
    mutated = {}  # key -> vectors, for the churned docs that currently exist

    def query_loop():
        conn = env.getConnection()
        qrng = np.random.default_rng(1)
        keys = list(stable)
        try:
            while not stop.is_set():
                key = keys[qrng.integers(len(keys))]
                i = int(qrng.integers(N_INDEXES))
                res = conn.execute_command(
                    'FT.SEARCH', f'idx{i}', f'*=>[KNN 1 @v{i} $b AS score]', 'PARAMS', 2,
                    'b', stable[key][i], 'RETURN', 1, 'score', 'DIALECT', 2)
                if res[0] != 1 or float(res[2][1]) < 0:
                    errors.append(('query', key, i, res))
                progress['queries'] += 1
        except Exception as e:
            errors.append(('query', repr(e)))

    def mutation_loop():
        conn = env.getConnection()
        mrng = np.random.default_rng(2)
        try:
            while not stop.is_set():
                pipe = conn.pipeline(transaction=True)
                for j in range(20):
                    key = f'doc:m{j}'
                    if key in mutated and mrng.random() < 0.5:
                        pipe.delete(key)
                        mutated.pop(key)
                    else:
                        mutated[key] = [_vector(mrng) for _ in range(N_INDEXES)]
                        _hset_doc(pipe, key, mutated[key])
                pipe.execute()
                progress['mutations'] += 1
        except Exception as e:
            errors.append(('mutation', repr(e)))

    def wait_for_progress():
        start = dict(progress)
        with TimeLimit(60, 'concurrent clients made no progress'):
            while any(progress[k] < start[k] + 5 for k in progress) and not errors:
                time.sleep(0.05)

    threads = [threading.Thread(target=query_loop), threading.Thread(target=mutation_loop)]
    for t in threads:
        t.start()
    try:
        for workers in [2, 0, 2, 0]:
            env.expect(config_cmd(), 'SET', 'WORKERS', workers).ok()
            wait_for_progress()
            _wait_for_pool_size(env, max(workers, 1))
            wait_for_progress()
    finally:
        stop.set()
        for t in threads:
            t.join(timeout=30)
    env.assertFalse(any(t.is_alive() for t in threads))
    env.assertEqual(errors, [])

    docs = {**stable, **mutated}
    _converge(env, len(docs))
    _assert_query_results(env, docs, {})


@skip(cluster=True)
def test_enable_maintenance_workers_at_runtime():
    """Raising MIN_MAINTENANCE_WORKERS at WORKERS 0 switches live indexes from in-place to deferred
    repair."""
    env = Env(moduleArgs=f'WORKERS 0 MIN_MAINTENANCE_WORKERS 0 {NO_PERIODIC_GC}', enableDebugCommand=True)
    rng = np.random.default_rng(18989)
    _create_indexes(env)
    docs = _load_docs(env, rng)
    removed_vectors = {}
    keys = list(docs)
    removed_vectors.update({key: docs[key] for key in keys[:40]})
    _mutate_in_transaction(env, rng, docs, keys[:20], keys[20:40])
    env.assertEqual(_marked_deleted(env, 0), 0)

    env.expect('CONFIG', 'SET', 'search-min-maintenance-workers', 1).ok()
    env.assertEqual(getWorkersThpoolNumThreads(env), 1)
    queued = _queue_repairs(env, rng, docs, removed_vectors, 20)
    for i in range(N_INDEXES):
        env.assertEqual(_marked_deleted(env, i), 40)
    env.expect(debug_cmd(), 'WORKERS', 'RESUME').ok()
    _assert_ran_exactly_once(env, queued)

    _converge(env, len(docs))
    _assert_query_results(env, docs, removed_vectors)


@skip(cluster=True)
def test_disable_maintenance_workers_with_pending_repairs():
    """Dropping MIN_MAINTENANCE_WORKERS to 0 with repairs queued lets them drain before the pool
    empties, while new writes already run in place."""
    env = Env(moduleArgs=f'WORKERS 0 {NO_PERIODIC_GC}', enableDebugCommand=True)
    rng = np.random.default_rng(18989)
    _create_indexes(env)
    docs = _load_docs(env, rng)
    removed_vectors = {}
    queued = _queue_repairs(env, rng, docs, removed_vectors, 40)

    at_resize = _resize_paused_pool(env, [config_cmd(), 'SET', 'MIN_MAINTENANCE_WORKERS', 0])
    env.assertEqual(at_resize['totalPendingJobs'], queued['totalPendingJobs'])
    marked = [_marked_deleted(env, i) for i in range(N_INDEXES)]
    keys = list(docs)
    removed_vectors.update({key: docs[key] for key in keys[:40]})
    _mutate_in_transaction(env, rng, docs, keys[:20], keys[20:40])
    env.assertEqual([_marked_deleted(env, i) for i in range(N_INDEXES)], marked)

    _wait_for_pool_size(env, 0)
    stats = _stats(env)
    env.assertEqual(stats['totalPendingJobs'], 0, message=stats)
    env.assertEqual(stats['totalJobsDone'], queued['totalJobsDone'] + queued['totalPendingJobs'],
                    message=(queued, stats))
    _converge(env, len(docs))
    _assert_query_results(env, docs, removed_vectors)


@skip(cluster=True)
def test_drop_index_with_pending_repairs():
    """Repair jobs queued for a dropped index are discarded safely once the worker runs them."""
    env = Env(moduleArgs='WORKERS 0', enableDebugCommand=True)
    rng = np.random.default_rng(18989)
    _create_indexes(env)
    docs = _load_docs(env, rng)
    keys = list(docs)

    with paused_workers(env):
        _mutate_in_transaction(env, rng, docs, keys[:N_DELETED], keys[N_DELETED:N_DELETED + N_OVERWRITTEN])
        env.assertGreater(_stats(env)['lowPriorityPendingJobs'], 0)
        for i in range(N_INDEXES):
            env.expect('FT.DROPINDEX', f'idx{i}').ok()

    drain_workers(env)
    env.assertEqual(_stats(env)['totalPendingJobs'], 0)
    env.expect('FT._LIST').equal([])


def _shutdown_with_pending_repairs(env):
    """Queue a repair backlog on a paused pool and shut down with the pool still paused. Returns
    the server log path."""
    rng = np.random.default_rng(18989)
    _create_indexes(env)
    docs = _load_docs(env, rng)
    _queue_repairs(env, rng, docs, {}, N_DELETED)
    log_path = _log_path(env)
    env.stop()
    return log_path


@skip(cluster=True)
def test_shutdown_with_pending_repairs():
    """The server shuts down cleanly with repair jobs still queued."""
    env = Env(moduleArgs='WORKERS 0', enableDebugCommand=True)
    _shutdown_with_pending_repairs(env)
    env.start()
    env.expect('PING').true()


@skip(cluster=True)
def test_shutdown_with_pending_repairs_releasing_resources():
    """With RS_GLOBAL_DTORS the module resumes, drains and destroys a paused pool on shutdown with
    a backlog."""
    prev = os.environ.get('RS_GLOBAL_DTORS')
    os.environ['RS_GLOBAL_DTORS'] = '1'
    try:
        env = Env(moduleArgs='WORKERS 0', enableDebugCommand=True)
    finally:
        if prev is None:
            os.environ.pop('RS_GLOBAL_DTORS')
        else:
            os.environ['RS_GLOBAL_DTORS'] = prev
    # An existing server neither inherits RS_GLOBAL_DTORS nor exposes its log.
    skipOnExistingEnv(env)
    log_path = _shutdown_with_pending_repairs(env)
    with open(log_path) as f:
        log = f.read()
    shutdown = log.rfind('Begin releasing RediSearch resources')
    env.assertGreater(shutdown, -1)
    env.assertContains('Draining workers thread pool with threshold 0', log[shutdown:])
    env.assertContains('End releasing RediSearch resources', log[shutdown:])
    env.start()
    env.expect('PING').true()


@skip(cluster=True)
def test_svs_deferred_at_workers_0():
    """Tiered SVS indexes also buffer writes and build the graph on the maintenance worker."""
    env = Env(moduleArgs='WORKERS 0', enableDebugCommand=True)
    dim = 16
    n = 2 * 1024  # over the default training threshold
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'SVS-VAMANA', '8', 'TYPE', 'FLOAT32',
               'DIM', dim, 'DISTANCE_METRIC', 'L2', 'SEARCH_WINDOW_SIZE', n).ok()
    rng = np.random.default_rng(18989)
    vectors = {f'doc:{d}': create_np_array_typed(rng.random(dim)).tobytes() for d in range(n)}

    def info():
        return get_vecsim_debug_dict(env, 'idx', 'v')

    def nearest(vector):
        res = env.cmd('FT.SEARCH', 'idx', '*=>[KNN 1 @v $b AS score]', 'PARAMS', 2, 'b', vector,
                      'RETURN', 1, 'score', 'DIALECT', 2)
        return res[1], float(res[2][1])

    conn = getConnectionByEnv(env)
    with paused_workers(env):
        pipe = conn.pipeline(transaction=False)
        for key, vector in vectors.items():
            pipe.hset(key, 'v', vector)
        pipe.execute()
        # In place, crossing the training threshold would have moved the vectors to the backend.
        env.assertEqual(to_dict(info()['FRONTEND_INDEX'])['INDEX_SIZE'], n)
        env.assertEqual(to_dict(info()['BACKEND_INDEX'])['INDEX_SIZE'], 0)
        env.assertGreater(_stats(env)['lowPriorityPendingJobs'], 0)
        env.assertEqual(nearest(vectors['doc:0']), ('doc:0', 0.0))

    drain_workers(env)
    env.assertEqual(to_dict(info()['FRONTEND_INDEX'])['INDEX_SIZE'], 0)
    env.assertEqual(to_dict(info()['BACKEND_INDEX'])['INDEX_SIZE'], n)

    deleted = list(vectors)[:n // 2]
    pipe = conn.pipeline(transaction=True)
    for key in deleted:
        pipe.delete(key)
    pipe.execute()
    drain_workers(env)
    env.assertEqual(env.cmd('FT.SEARCH', 'idx', '*', 'LIMIT', 0, 0), [n - len(deleted)])
    for key in list(vectors)[n // 2:n // 2 + 20]:
        env.assertEqual(nearest(vectors[key]), (key, 0.0))
    for key in deleted[:20]:
        env.assertGreater(nearest(vectors[key])[1], 0.0)


def test_loading_event_ends_at_maintenance_worker():
    """At WORKERS 0 a loading event raises every shard's pool to MIN_OPERATION_WORKERS and returns
    it to the maintenance worker, not to an empty pool."""
    env = Env(moduleArgs='WORKERS 0', enableDebugCommand=True)
    rng = np.random.default_rng(18989)
    _create_indexes(env)
    docs = _load_docs(env, rng, n=100)
    for _ in env.reloadingIterator():
        drain_workers(env)
        env.assertEqual(getWorkersThpoolNumThreadsFromAllShards(env), [1] * env.shardsCount)
        _assert_query_results(env, docs, {})


@skip(cluster=True)
def test_shrink_while_paused_is_deferred():
    """Shrinking a paused pool, by config or at the end of a loading event, waits for the resume
    instead of dropping threads under the pause."""
    env = Env(moduleArgs='WORKERS 0', enableDebugCommand=True)
    rng = np.random.default_rng(18989)
    _create_indexes(env)
    docs = _load_docs(env, rng, n=100)

    queued = _queue_repairs(env, rng, docs, {}, 10)
    env.expect(config_cmd(), 'SET', 'MIN_MAINTENANCE_WORKERS', 0).ok()
    env.assertEqual(getWorkersThpoolNumThreads(env), 1)
    env.expect(debug_cmd(), 'WORKERS', 'RESUME').ok()
    _wait_for_pool_size(env, 0)
    env.assertEqual(_stats(env)['totalJobsDone'], queued['totalJobsDone'] + queued['totalPendingJobs'])
    env.expect(config_cmd(), 'SET', 'MIN_MAINTENANCE_WORKERS', 1).ok()

    # The load grows the paused pool to MIN_OPERATION_WORKERS and queues the rebuild, and its end
    # must neither wait on the paused pool nor shrink it.
    env.expect(debug_cmd(), 'WORKERS', 'PAUSE').ok()
    env.expect('DEBUG', 'RELOAD').ok()
    env.assertGreater(_stats(env)['totalPendingJobs'], 0)
    env.assertEqual(getWorkersThpoolNumThreads(env), 4)
    # On resume, all of the load's workers drain the rebuild before the pool shrinks to the floor.
    pipe = getConnectionByEnv(env).pipeline(transaction=True)
    pipe.execute_command(debug_cmd(), 'WORKERS', 'RESUME')
    pipe.execute_command(debug_cmd(), 'WORKERS', 'N_THREADS')
    env.assertEqual(pipe.execute(), ['OK', 4])
    _wait_for_pool_size(env, 1)
    _converge(env, len(docs))
    _assert_query_results(env, docs, {})


@skip(cluster=True)
def test_loading_end_drains_with_event_workers():
    """At the end of a load at WORKERS 0 the whole MIN_OPERATION_WORKERS pool drains the rebuild
    backlog, and only then shrinks to the maintenance worker."""
    env = Env(moduleArgs='WORKERS 0 MIN_OPERATION_WORKERS 4', enableDebugCommand=True)
    skipOnExistingEnv(env)
    rng = np.random.default_rng(18989)
    _create_indexes(env)
    docs = _load_docs(env, rng, n=1000)
    env.expect('DEBUG', 'RELOAD').ok()
    with open(_log_path(env)) as f:
        log = f.read()
    wait = log.rfind('queued jobs on 4 workers before resizing the workers threadpool')
    env.assertGreater(wait, -1)
    env.assertGreater(log.find('Changing workers threadpool size from 4 to 1', wait), wait)
    env.assertEqual(getWorkersThpoolNumThreads(env), 1)
    _converge(env, len(docs))


def _queued(stats):
    """Queued non-admin jobs, as the deferred shrink counts them."""
    return stats['lowPriorityPendingJobs'] + stats['highPriorityPendingJobs']


def _sample_until_floor(env, floor=1):
    """Poll the pool size with the jobs-done count until the pool reaches `floor`. The shrink only
    happens on the main thread, so each transaction reads the two consistently."""
    conn = getConnectionByEnv(env)
    samples = []
    with TimeLimit(60, 'the pool did not shrink to the floor'):
        while True:
            pipe = conn.pipeline(transaction=True)
            pipe.execute_command(debug_cmd(), 'WORKERS', 'N_THREADS')
            pipe.execute_command(debug_cmd(), 'WORKERS', 'STATS')
            n_threads, stats = pipe.execute()
            stats = to_dict(stats)
            samples.append((n_threads, stats['totalJobsDone'], _queued(stats)))
            if n_threads == floor:
                return samples
            time.sleep(0.01)


@skip(cluster=True)
def test_shrink_to_floor_waits_for_queue():
    """Shrinking to the maintenance floor, as at the end of a trim or ASM event or on WORKERS N ->
    0, keeps every thread while jobs queued at that point wait, without blocking the main thread."""
    env = Env(moduleArgs='WORKERS 4', enableDebugCommand=True)
    rng = np.random.default_rng(18989)
    _create_indexes(env)
    docs = _load_docs(env, rng)
    queued = _queue_repairs(env, rng, docs, {}, N_DELETED)
    at_resize = _resize_paused_pool(env, [config_cmd(), 'SET', 'WORKERS', 0])
    env.assertEqual(_queued(at_resize), _queued(queued))
    samples = _sample_until_floor(env)
    env.assertEqual([s for s in samples if s[2] > 0 and s[0] != 4], [])
    env.assertEqual(samples[-1][2], 0, message=samples[-1])
    env.assertGreater(len(samples), 1)
    _assert_ran_exactly_once(env, queued)
    _wait_for_pool_size(env, 1)
    _converge(env, len(docs))


@skip(cluster=True)
def test_shrink_to_floor_not_postponed_by_later_jobs():
    """Jobs queued after the shrink was requested do not postpone it: the pool reaches the floor
    once the backlog of the request is done, even with later jobs still queued."""
    env = Env(moduleArgs='WORKERS 4', enableDebugCommand=True)
    rng = np.random.default_rng(18989)
    _create_indexes(env)
    docs = _load_docs(env, rng)
    queued = _queue_repairs(env, rng, docs, {}, N_DELETED)
    target = queued['totalJobsDone'] + _queued(queued)
    later = {f'doc:later{j}': [_vector(rng) for _ in range(N_INDEXES)] for j in range(50)}

    # One transaction, so the deferred-shrink timer cannot fire in between (DRAIN yields to clients
    # only). The resume snapshots the backlog, the drain completes it, and the later jobs are queued
    # on a paused pool, so the final resume sees the target reached with them all still queued.
    pipe = getConnectionByEnv(env).pipeline(transaction=True)
    pipe.execute_command(config_cmd(), 'SET', 'WORKERS', 0)
    pipe.execute_command(debug_cmd(), 'WORKERS', 'RESUME')
    pipe.execute_command(debug_cmd(), 'WORKERS', 'DRAIN')
    pipe.execute_command(debug_cmd(), 'WORKERS', 'PAUSE')
    for key, vectors in later.items():
        _hset_doc(pipe, key, vectors)
    pipe.execute_command(debug_cmd(), 'WORKERS', 'N_THREADS')
    pipe.execute_command(debug_cmd(), 'WORKERS', 'STATS')
    pipe.execute_command(debug_cmd(), 'WORKERS', 'RESUME')
    pipe.execute_command(debug_cmd(), 'WORKERS', 'N_THREADS')
    *_, before, stats, _, after = pipe.execute()
    docs.update(later)
    stats = to_dict(stats)

    env.assertEqual(before, 4)
    env.assertEqual(stats['totalJobsDone'], target, message=stats)
    env.assertGreater(_queued(stats), 0, message=stats)
    env.assertEqual(after, 1)
    _converge(env, len(docs))
    _assert_query_results(env, docs, {})


def _deferred_shrink_target(env):
    """The jobs-done target of the last deferred shrink and its running jobs, from its log line."""
    with open(_log_path(env)) as f:
        lines = re.findall(r'until (\d+) jobs are done in total \((\d+) queued and (\d+) running now\)',
                           f.read())
    assert lines, 'no deferred shrink was logged'
    target, _, running = map(int, lines[-1])
    return target, running


@skip(cluster=True)
def test_shrink_to_floor_on_running_pool():
    """A shrink requested while jobs run counts them toward its target. A job that cannot finish
    (a paused query) keeps the target out of reach, so the pool shrinks once the queue is empty."""
    env = Env(moduleArgs='WORKERS 2', enableDebugCommand=True)
    skipOnExistingEnv(env)
    rng = np.random.default_rng(18989)
    _create_indexes(env)
    docs = _load_docs(env, rng)
    # A separate index, so the paused query holds no lock the vector jobs need.
    env.expect('FT.CREATE', 'qidx', 'PREFIX', 1, 'q:', 'SCHEMA', 'name', 'TEXT').ok()
    env.expect('HSET', 'q:1', 'name', 'name1').equal(1)
    # The background scan holds the GIL while it waits for the index lock the paused query holds.
    waitForIndex(env, 'qidx')

    query_result = []
    query = threading.Thread(
        target=call_and_store,
        args=(runDebugQueryCommandPauseBeforeRPAfterN,
              (env, ['FT.SEARCH', 'qidx', '*'], 'Index', 0), query_result),
        daemon=True)
    query.start()
    with TimeLimit(30, 'the query did not pause'):
        while getIsRPPaused(env) != 1:
            time.sleep(0.05)

    later = {f'doc:later{j}': [_vector(rng) for _ in range(N_INDEXES)] for j in range(50)}
    pipe = getConnectionByEnv(env).pipeline(transaction=True)
    for key, vectors in later.items():
        _hset_doc(pipe, key, vectors)
    pipe.execute_command(config_cmd(), 'SET', 'WORKERS', 0)
    pipe.execute()
    docs.update(later)
    target, running = _deferred_shrink_target(env)
    env.assertGreaterEqual(running, 1)

    samples = _sample_until_floor(env)
    env.assertEqual([s for s in samples if s[2] > 0 and s[0] != 2], [])
    # The paused query still runs, so the target was not reached: the empty queue ended the wait.
    env.assertEqual(getIsRPPaused(env), 1)
    env.assertEqual(samples[-1][2], 0, message=samples[-1])
    env.assertLess(samples[-1][1], target, message=(samples[-1], target))

    setPauseRPResume(env)
    query.join(timeout=30)
    env.assertEqual(query_result, [[1, 'q:1', ['name', 'name1']]])
    drain_workers(env)
    # Every job pending or running at the request ran exactly once, and nothing else was submitted.
    env.assertEqual(_stats(env)['totalJobsDone'], target)
    _wait_for_pool_size(env, 1)
    _converge(env, len(docs))
    _assert_query_results(env, docs, {})


@skip(cluster=True)
def test_maintenance_floor_above_workers():
    """A floor above a nonzero WORKERS sizes the pool, both at startup and when WORKERS shrinks
    below it at runtime."""
    env = Env(moduleArgs='WORKERS 1 MIN_MAINTENANCE_WORKERS 2', enableDebugCommand=True)
    env.assertEqual(getWorkersThpoolNumThreads(env), 2)
    # The pool starts its threads on the first job.
    _create_indexes(env)
    _load_docs(env, np.random.default_rng(18989), n=1)
    _wait_for_pool_size(env, 2)
    env.expect(config_cmd(), 'SET', 'WORKERS', 4).ok()
    _wait_for_pool_size(env, 4)
    env.expect(config_cmd(), 'SET', 'WORKERS', 1).ok()
    _wait_for_pool_size(env, 2)


@skip(cluster=True)
def test_flat_buffer_fills_at_workers_0():
    """At WORKERS 0 ingestion is asynchronous too: a paused pool leaves TIERED_HNSW_BUFFER_LIMIT
    vectors in the flat buffer and inserts the rest directly into HNSW, and queries search both."""
    env = Env(moduleArgs='WORKERS 0', enableDebugCommand=True)
    dim = 16
    limit = int(env.cmd(config_cmd(), 'GET', 'TIERED_HNSW_BUFFER_LIMIT')[0][1])
    n = limit + 500
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'HNSW', '8', 'TYPE', 'FLOAT32',
               'DIM', dim, 'DISTANCE_METRIC', 'L2', 'EF_CONSTRUCTION', 2 * n).ok()
    rng = np.random.default_rng(18989)
    vectors = {f'doc:{d}': create_np_array_typed(rng.random(dim)).tobytes() for d in range(n)}

    def sizes():
        info = get_vecsim_debug_dict(env, 'idx', 'v')
        return (to_dict(info['FRONTEND_INDEX'])['INDEX_SIZE'],
                to_dict(info['BACKEND_INDEX'])['INDEX_SIZE'])

    def assert_exact(keys):
        for key in keys:
            res = env.cmd('FT.SEARCH', 'idx', f'*=>[KNN 1 @v $b EF_RUNTIME {2 * n} AS score]',
                          'PARAMS', 2, 'b', vectors[key], 'RETURN', 1, 'score', 'DIALECT', 2)
            env.assertEqual((res[1], float(res[2][1])), (key, 0.0), depth=1)

    conn = getConnectionByEnv(env)
    with paused_workers(env):
        pipe = conn.pipeline(transaction=False)
        for key, vector in vectors.items():
            pipe.hset(key, 'v', vector)
        pipe.execute()
        env.assertEqual(sizes(), (limit, n - limit))
        keys = list(vectors)
        assert_exact(keys[:20] + keys[-20:])

    drain_workers(env)
    env.assertEqual(sizes(), (0, n))
    env.assertEqual(env.cmd('FT.SEARCH', 'idx', '*', 'LIMIT', 0, 0), [n])
    assert_exact(vectors)


@skip(cluster=True)
def test_deprecated_only_on_operations_keeps_maintenance_worker():
    """MT_MODE_ONLY_ON_OPERATIONS is about query workers, so its repair still runs on the maintenance
    worker."""
    env = Env(moduleArgs=f'WORKERS 0 WORKER_THREADS 2 MT_MODE MT_MODE_ONLY_ON_OPERATIONS {NO_PERIODIC_GC}',
              enableDebugCommand=True)
    env.assertEqual(getWorkersThpoolNumThreads(env), 1)
    rng = np.random.default_rng(18989)
    _create_indexes(env)
    docs = _load_docs(env, rng, n=100)
    removed_vectors = {}
    queued = _queue_repairs(env, rng, docs, removed_vectors, 10)
    for i in range(N_INDEXES):
        env.assertEqual(_marked_deleted(env, i), 20)
    env.expect(debug_cmd(), 'WORKERS', 'RESUME').ok()
    _assert_ran_exactly_once(env, queued)
    _converge(env, len(docs))
    _assert_query_results(env, docs, removed_vectors)
