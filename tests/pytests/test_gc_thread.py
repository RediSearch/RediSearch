# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Flow tests for the THREAD GC policy (`GC_POLICY THREAD`).

The policy is load-time, so each test loads the module with it.
"""

import threading

from common import *

SYNC_POINT_GC_TASK_START = 'GCTaskStart'  # src/debug_commands.h

SCHEMA = ['SCHEMA', 't', 'TEXT', 'n', 'NUMERIC', 'g', 'TAG', 'm', 'TAG', 'INDEXMISSING']


def thread_gc_env(module_args='', fresh=False):
    # GC_POLICY is load-time, so it comes from module args. A test that changes a runtime config
    # other tests rely on asks for a server of its own.
    env = Env(moduleArgs=f'GC_POLICY THREAD {module_args}',
              freshEnv=fresh)
    env.expect(config_cmd(), 'SET', 'FORK_GC_CLEAN_THRESHOLD', 0).ok()
    return env


def total_forks(env):
    return int(env.cmd('INFO', 'stats')['total_forks'])


def add_docs(conn, prefix, count):
    pipe = conn.pipeline(transaction=False)
    for i in range(count):
        fields = ['t', f'hello word{i % 7}', 'n', i % 11, 'g', f'tag{i % 5}']
        if i % 3:
            fields += ['m', 'present']
        pipe.execute_command('HSET', f'{prefix}{i}', *fields)
    pipe.execute()


def assert_matches_fresh_index(env, idx, prefix):
    """GC left nothing behind: `idx` holds as many records as an index built
    from scratch over the documents that survived."""
    env.expect('FT.CREATE', 'fresh', 'PREFIX', 1, prefix, 'INDEXALL', 'ENABLE', *SCHEMA).ok()
    waitForIndex(env, 'fresh')
    env.assertEqual(index_info(env, idx)['num_records'], index_info(env, 'fresh')['num_records'])
    env.assertEqual(index_info(env, idx)['num_docs'], index_info(env, 'fresh')['num_docs'])


@skip(cluster=True)
def testThreadGCCollectsEveryIndexKindWithoutForking():
    """Every index kind is collected in-process: Redis records no fork, and the index ends up
    holding exactly what an index built from the surviving documents holds."""
    env = thread_gc_env()
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'PREFIX', 1, 'doc:', 'INDEXALL', 'ENABLE', *SCHEMA).ok()
    add_docs(conn, 'doc:', 300)
    for i in range(0, 300, 2):
        conn.execute_command('DEL', f'doc:{i}')

    forks_before = total_forks(env)
    forceInvokeGC(env, 'idx')

    env.assertEqual(total_forks(env), forks_before)
    env.assertEqual(env.cmd(debug_cmd(), 'DUMP_INVIDX', 'idx', 'hello'), list(range(2, 301, 2)))
    env.assertGreater(int(to_dict(index_info(env, 'idx')['gc_stats'])['bytes_collected']), 0)
    assert_matches_fresh_index(env, 'idx', 'doc:')


@skip(cluster=True)
def testThreadGCConsecutiveCyclesCollectWhatWasDeletedSince():
    """Each cycle works off the IDs deleted since the previous one and must leave exactly the
    surviving documents' postings."""
    env = thread_gc_env('FORK_GC_RUN_INTERVAL 3600')
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'PREFIX', 1, 'doc:', 'INDEXALL', 'ENABLE', *SCHEMA).ok()
    waitForIndex(env, 'idx')
    add_docs(conn, 'doc:', 300)
    for i in range(0, 300, 2):
        conn.execute_command('DEL', f'doc:{i}')

    forceInvokeGC(env, 'idx')
    env.assertEqual(env.cmd(debug_cmd(), 'DUMP_INVIDX', 'idx', 'hello'), list(range(2, 301, 2)))

    for i in range(1, 300, 4):
        conn.execute_command('DEL', f'doc:{i}')
    forceInvokeGC(env, 'idx')
    env.assertEqual(env.cmd(debug_cmd(), 'DUMP_INVIDX', 'idx', 'hello'), list(range(4, 301, 4)))
    assert_matches_fresh_index(env, 'idx', 'doc:')


@skip(cluster=True)
def testThreadGCCollectsAcrossSeveralReadLockChunks():
    """A read lock visits a bounded number of keys (index_gc::thread::KEYS_PER_HOLD, 64), so
    an index with more terms than that is scanned and applied in several chunks and must still
    come out exact."""
    env = thread_gc_env()
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'PREFIX', 1, 'doc:', 'INDEXALL', 'ENABLE', *SCHEMA).ok()
    for i in range(600):
        conn.execute_command('HSET', f'doc:{i}', 't', f'hello unique{i}', 'n', i)
    for i in range(0, 600, 3):
        conn.execute_command('DEL', f'doc:{i}')

    forceInvokeGC(env, 'idx')

    assert_matches_fresh_index(env, 'idx', 'doc:')


@skip(cluster=True)
def testThreadGCRemovesEmptiedTermsTagsAndMissingFields():
    """Terms, tag values and missing-field entries left with no documents are removed."""
    env = thread_gc_env()
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', *SCHEMA).ok()
    conn.execute_command('HSET', 'gone', 't', 'vanishing', 'g', 'onlytag')
    conn.execute_command('HSET', 'kept', 't', 'staying', 'g', 'keeptag', 'm', 'x')
    conn.execute_command('DEL', 'gone')

    forceInvokeGC(env, 'idx')

    env.expect(debug_cmd(), 'DUMP_INVIDX', 'idx', 'vanishing').error() \
        .contains('Can not find the inverted index')
    env.assertEqual(env.cmd(debug_cmd(), 'DUMP_TAGIDX', 'idx', 'g'), [['keeptag', [2]]])
    env.assertEqual(env.cmd('FT.SEARCH', 'idx', 'ismissing(@m)', 'NOCONTENT'), [0])


@skip(cluster=True)
def testThreadGCCollectsPastMaxmemory():
    # The fork cycle stands down above maxmemory, since a fork may double the memory; a THREAD
    # cycle allocates little, so it keeps collecting there.
    env = thread_gc_env(fresh=True)
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'PREFIX', 1, 'doc:', *SCHEMA).ok()
    add_docs(conn, 'doc:', 100)

    set_tight_maxmemory_for_oom(env)
    for i in range(100):
        conn.execute_command('DEL', f'doc:{i}')
    forceInvokeGC(env, 'idx')

    gc_stats = to_dict(index_info(env, 'idx')['gc_stats'])
    env.assertGreater(int(gc_stats['bytes_collected']), 0)
    set_unlimited_maxmemory_for_oom(env)


@skip(cluster=True)
def testThreadPolicyIsTakenAtLoad():
    """GC_POLICY THREAD is taken at load as given, reported by FT.CONFIG, and its cycles do not
    fork."""
    env = Env(moduleArgs='GC_POLICY THREAD')
    env.expect(config_cmd(), 'SET', 'FORK_GC_CLEAN_THRESHOLD', 0).ok()
    env.assertEqual(env.cmd(config_cmd(), 'GET', 'GC_POLICY'), [['GC_POLICY', 'thread']])
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', *SCHEMA).ok()
    add_docs(conn, 'doc:', 10)
    conn.execute_command('DEL', 'doc:0')

    forks_before = total_forks(env)
    forceInvokeGC(env, 'idx')

    env.assertEqual(total_forks(env), forks_before)
    env.assertEqual(int(to_dict(index_info(env, 'idx')['gc_stats'])['total_cycles']), 1)

@skip(cluster=True)
def testThreadGCThresholdGrowsWithTheIndex():
    """A scheduled THREAD cycle waits for search-gc-clean-ratio-ppm of the index's live documents
    to be deleted, not just FORK_GC_CLEAN_THRESHOLD."""
    env = Env(moduleArgs='FORK_GC_RUN_INTERVAL 1', freshEnv=True)
    env.expect(config_cmd(), 'SET', 'FORK_GC_CLEAN_THRESHOLD', 1).ok()
    env.expect('CONFIG', 'SET', 'search-gc-clean-ratio-ppm', 100_000).ok()
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    waitForIndex(env, 'idx')
    for i in range(100):
        conn.execute_command('HSET', f'doc:{i}', 't', f'word{i}')

    def total_cycles():
        return int(to_dict(index_info(env, 'idx')['gc_stats'])['total_cycles'])

    # 5 deletions leave 95 documents, whose 10% is 9.
    for i in range(5):
        conn.execute_command('DEL', f'doc:{i}')
    time.sleep(2.5)
    env.assertEqual(total_cycles(), 0)

    # 10 deletions reach the 9 of the 90 left.
    for i in range(5, 10):
        conn.execute_command('DEL', f'doc:{i}')
    with TimeLimit(10, 'a scheduled cycle should run once the threshold is reached'):
        while total_cycles() == 0:
            time.sleep(0.1)

@skip(cluster=True)
def testThreadGCRunsWhileAnotherChildHoldsTheForkSlot():
    """The fork GC has to wait for a BGSAVE child to exit; the thread GC needs no child of its
    own, so it collects while the BGSAVE is still running."""
    env = thread_gc_env()
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', *SCHEMA).ok()
    add_docs(conn, 'doc:', 20)
    conn.execute_command('DEL', 'doc:0')

    # A save that ends before the GC does proves nothing, so a slow host retries with
    # a longer one; the test fails only if the GC never runs inside a save.
    overlapped = False
    for delay_us in (100_000, 500_000, 2_000_000):  # per key, 20 keys
        env.expect('CONFIG', 'SET', 'rdb-key-save-delay', delay_us).ok()
        env.cmd('BGSAVE')
        env.assertEqual(env.cmd('INFO', 'persistence')['rdb_bgsave_in_progress'], 1)

        # forceInvokeGC waits for the BGSAVE to finish first, so call the debug
        # command directly.
        env.cmd(debug_cmd(), 'GC_FORCEINVOKE', 'idx')

        overlapped = env.cmd('INFO', 'persistence')['rdb_bgsave_in_progress'] == 1
        env.expect('CONFIG', 'SET', 'rdb-key-save-delay', 0).ok()
        waitForRdbSaveToFinish(env)
        if overlapped:
            break
    env.assertTrue(overlapped, message='no forced GC ran while a BGSAVE child held the fork slot')
    env.assertEqual(env.cmd(debug_cmd(), 'DUMP_INVIDX', 'idx', 'hello'), list(range(2, 21)))


@skip(cluster=True)
def testThreadGCKeepsConcurrentQueriesCorrect():
    """Queries hold the read lock between results and revalidate their readers when they re-take
    it; the thread GC applies its deltas in between. Every result stays a duplicate-free subset of
    the documents ever indexed, and the end state is empty."""
    env = thread_gc_env()
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', *SCHEMA).ok()
    add_docs(conn, 'doc:', 2000)
    # Never deleted, so every search for them must find exactly them while cycles
    # rewrite the blocks around them.
    stable = {f'stable:{i}' for i in range(200)}
    for key in stable:
        conn.execute_command('HSET', key, 't', 'steady', 'n', 100)

    errors = []
    stop = threading.Event()

    def check(reply, what):
        ids = reply[1:]
        if reply[0] != len(stable) or len(ids) != len(set(ids)) or {
                i.decode() if isinstance(i, bytes) else i for i in ids} != stable:
            errors.append(f'{what}: {reply[0]} results, {len(ids)} ids, {len(set(ids))} distinct')

    def query_loop():
        query_conn = getConnectionByEnv(env)
        while not stop.is_set():
            try:
                query_conn.execute_command('FT.SEARCH', 'idx', 'hello', 'LIMIT', 0, 1000, 'NOCONTENT')
                check(query_conn.execute_command('FT.SEARCH', 'idx', 'steady', 'LIMIT', 0, 1000, 'NOCONTENT'),
                      'text')
                check(query_conn.execute_command('FT.SEARCH', 'idx', '@n:[100 100]', 'LIMIT', 0, 1000,
                                                 'NOCONTENT'), 'numeric')
            except Exception as e:  # noqa: BLE001 - any error fails the test below
                errors.append(e)

    readers = [threading.Thread(target=query_loop) for _ in range(4)]
    for reader in readers:
        reader.start()
    try:
        for round_ in range(4):
            for i in range(round_, 2000, 4):
                conn.execute_command('DEL', f'doc:{i}')
            forceInvokeGC(env, 'idx')
    finally:
        stop.set()
        for reader in readers:
            reader.join()

    env.assertEqual(errors, [])
    env.assertEqual(env.cmd('FT.SEARCH', 'idx', 'hello', 'LIMIT', 0, 0), [0])
    # Only the stable docs' records are left: as many as a fresh index over them holds.
    env.expect('FT.CREATE', 'fresh', *SCHEMA).ok()
    waitForIndex(env, 'fresh')
    env.assertEqual(index_info(env, 'idx')['num_records'], index_info(env, 'fresh')['num_records'])


GC_THREADS = 4
N_INDEXES = 8


def create_indexes_with_garbage(env):
    conn = getConnectionByEnv(env)
    for n in range(N_INDEXES):
        env.expect('FT.CREATE', f'idx{n}', 'PREFIX', 1, f'i{n}:', *SCHEMA).ok()
        # Let the creation scan finish first: a key it re-indexes after being added gets a new
        # doc id, and one re-indexed after the GC cycle leaves garbage no single cycle can see.
        waitForIndex(env, f'idx{n}')
        add_docs(conn, f'i{n}:', 50)
        pipe = conn.pipeline(transaction=False)
        for i in range(0, 50, 2):
            pipe.execute_command('DEL', f'i{n}:{i}')
        pipe.execute()


def pool_env(module_args=''):
    env = thread_gc_env(module_args)
    env.expect('CONFIG', 'SET', 'search-gc-threads', GC_THREADS).ok()
    return env


def park_forced_runs_on_every_index(env, parked):
    """Queue a forced run on every index and wait until `parked` of them are held at the start of
    their pass. Only these runs reach the sync point: no scheduled run holds an index busy."""
    for n in range(N_INDEXES):
        env.expect(debug_cmd(), 'GC_STOP_SCHEDULE', f'idx{n}').ok()
    env.cmd(debug_cmd(), 'GC_WAIT_FOR_JOBS')
    env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
    env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', SYNC_POINT_GC_TASK_START, 30000).ok()
    waitForRdbSaveToFinish(env)
    for n in range(N_INDEXES):
        env.expect(debug_cmd(), 'GC_FORCEBGINVOKE', f'idx{n}').ok()
    hits = lambda: env.cmd(debug_cmd(), 'SYNC_POINT', 'HIT_COUNT', SYNC_POINT_GC_TASK_START)
    wait_for_condition(lambda: (hits() >= parked, {'parked': hits()}),
                       f'fewer than {parked} GC cycles ever ran at once')


def release_parked_runs(env):
    env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', SYNC_POINT_GC_TASK_START)
    env.expect(debug_cmd(), 'GC_WAIT_FOR_JOBS').equal('DONE')
    assert_all_indexes_collected(env)


def assert_all_indexes_collected(env):
    # Count what survived: without GC each posting list would still hold all 50 documents.
    for n in range(N_INDEXES):
        postings = env.cmd(debug_cmd(), 'DUMP_INVIDX', f'idx{n}', 'hello')
        env.assertEqual(len(postings), 25, message=f'idx{n}: {postings}')


@skip(cluster=True)
def testThreadGCWaitForJobsCoversEveryGCThread():
    """GC_WAIT_FOR_JOBS with nothing queued still waits for a cycle running on another GC thread.
    The cycle is parked, so the wait cannot pass by it finishing first."""
    env = pool_env('FORK_GC_RUN_INTERVAL 1')
    skipIfNoEnableAssert(env)
    env.expect('FT.CREATE', 'idx', *SCHEMA).ok()

    env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
    env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', SYNC_POINT_GC_TASK_START, 30000).ok()
    wait_for_condition(
        lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', SYNC_POINT_GC_TASK_START) == 1, {}),
        'Timeout waiting for a scheduled GC run to park')

    replies = []
    waiter_conn = getConnectionByEnv(env)
    waiter_conn.execute_command('CLIENT', 'SETNAME', 'gc-waiter')
    waiter = threading.Thread(target=lambda: replies.append(
        waiter_conn.execute_command(debug_cmd(), 'GC_WAIT_FOR_JOBS')))
    waiter.start()

    def waiter_blocked():
        clients = env.cmd('CLIENT', 'LIST').splitlines()
        mine = [c for c in clients if ' name=gc-waiter ' in f' {c} ']
        return bool(mine) and 'b' in mine[0].split(' flags=')[1].split(' ')[0], {'clients': mine}

    # The command reached the server and is blocked, so a reply now could only mean it stopped
    # waiting, rather than that it had not been sent yet.
    wait_for_condition(waiter_blocked, 'GC_WAIT_FOR_JOBS never blocked')
    env.assertEqual(replies, [], message='GC_WAIT_FOR_JOBS returned while a GC cycle was parked')

    env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', SYNC_POINT_GC_TASK_START)
    waiter.join(timeout=30)
    env.assertEqual(replies, ['DONE'])


@skip(cluster=True)
def testThreadGCWaitForJobsWithBackgroundRuns():
    """GC_WAIT_FOR_JOBS returns only after background forced runs on every index finished."""
    env = pool_env()
    create_indexes_with_garbage(env)

    waitForRdbSaveToFinish(env)
    for n in range(N_INDEXES):
        env.cmd(debug_cmd(), 'GC_FORCEBGINVOKE', f'idx{n}')
    env.expect(debug_cmd(), 'GC_WAIT_FOR_JOBS').equal('DONE')

    assert_all_indexes_collected(env)


@skip(cluster=True)
def testThreadGCCleansSeveralIndexesConcurrently():
    """Several GC threads clean several indexes at once, without forking. Every cycle parks
    before its pass, so two of them parked together are two in progress at once, and of
    different indexes, since one index never runs two."""
    env = pool_env()
    skipIfNoEnableAssert(env)
    create_indexes_with_garbage(env)

    forks_before = total_forks(env)
    park_forced_runs_on_every_index(env, parked=2)
    release_parked_runs(env)
    env.assertEqual(total_forks(env), forks_before)


@skip(cluster=True)
def testThreadGCPoolShrinksWhileItsThreadsAreBusy():
    """Lowering search-gc-threads while every GC thread is mid-cycle lets every cycle complete,
    and the pool ends at the new size."""
    env = pool_env()
    skipIfNoEnableAssert(env)
    create_indexes_with_garbage(env)

    park_forced_runs_on_every_index(env, parked=GC_THREADS)
    env.assertEqual(env.cmd(debug_cmd(), 'GC_THREADS_ALIVE'), GC_THREADS)

    env.expect('CONFIG', 'SET', 'search-gc-threads', 1).ok()
    release_parked_runs(env)
    wait_for_condition(
        lambda: (env.cmd(debug_cmd(), 'GC_THREADS_ALIVE') == 1,
                 {'alive': env.cmd(debug_cmd(), 'GC_THREADS_ALIVE')}),
        'the GC pool never shrank to one thread')


@skip(cluster=True)
def testThreadGCNeverRunsTwoCyclesOfOneIndexAtOnce():
    """Forced runs from many clients and the index's own schedule all target one index while
    several GC threads are free. Deltas address blocks by position, so an overlap would apply
    one cycle's delta to blocks the other already rewrote; the index must stay exact."""
    env = pool_env('FORK_GC_RUN_INTERVAL 1')
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'PREFIX', 1, 'doc:', 'INDEXALL', 'ENABLE', *SCHEMA).ok()
    add_docs(conn, 'doc:', 1000)

    errors = []

    def force_repeatedly():
        client = getConnectionByEnv(env)
        for _ in range(5):
            try:
                client.execute_command(debug_cmd(), 'GC_FORCEINVOKE', 'idx')
            except Exception as e:  # noqa: BLE001 - any error fails the test below
                # Losing the race to another run of the index is the expected refusal.
                if GC_RUN_IN_PROGRESS not in str(e):
                    errors.append(e)

    for round_ in range(3):
        for i in range(round_, 1000, 3):
            conn.execute_command('DEL', f'doc:{i}')
        clients = [threading.Thread(target=force_repeatedly) for _ in range(GC_THREADS * 2)]
        for client in clients:
            client.start()
        for client in clients:
            client.join()

    env.assertEqual(errors, [])
    add_docs(conn, 'doc:', 300)
    forceInvokeGC(env, 'idx')
    assert_matches_fresh_index(env, 'idx', 'doc:')


@skip(cluster=True)
def testScheduledThreadGCCollectsWithoutForking():
    """The index's own schedule, not only forced runs, collects in-process."""
    env = thread_gc_env('FORK_GC_RUN_INTERVAL 1')
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', *SCHEMA).ok()
    add_docs(conn, 'doc:', 50)
    forks_before = total_forks(env)
    for i in range(0, 50, 2):
        conn.execute_command('DEL', f'doc:{i}')

    wait_for_condition(
        lambda: (len(env.cmd(debug_cmd(), 'DUMP_INVIDX', 'idx', 'hello')) == 25, {}),
        'the scheduled thread GC did not collect')

    env.assertEqual(total_forks(env), forks_before)


@skip(cluster=True)
def testDumpDeletedIdsListsWhatTheNextCycleCollects():
    env = thread_gc_env()
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'PREFIX', 1, 'doc:', 'SCHEMA', 't', 'TEXT').ok()
    for i in range(1, 11):
        conn.execute_command('HSET', f'doc:{i}', 't', f'hello{i}')
    conn.execute_command('DEL', 'doc:2', 'doc:5', 'doc:7')

    env.expect(debug_cmd(), 'DUMP_DELETED_IDS', 'idx').equal([2, 5, 7])
    forceInvokeGC(env, 'idx')
    env.expect(debug_cmd(), 'DUMP_DELETED_IDS', 'idx').equal([])


@skip(cluster=True)
def testThreadGCSettingsBounds():
    """search-gc-threads is a CONFIG-only setting, changeable at runtime, between 1 and 64."""
    env = Env(moduleArgs='GC_POLICY THREAD', freshEnv=True)
    name = 'search-gc-threads'
    env.expect('CONFIG', 'GET', name).equal([name, '1'])
    for threads in [64, 2, 1]:
        env.expect('CONFIG', 'SET', name, threads).ok()
        env.expect('CONFIG', 'GET', name).equal([name, str(threads)])
    for threads in [0, 65]:
        env.expect('CONFIG', 'SET', name, threads).error().contains('CONFIG SET failed')
    env.expect(config_cmd(), 'GET', '_GC_THREADS').equal([])
