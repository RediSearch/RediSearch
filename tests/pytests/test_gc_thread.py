# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Flow tests for the in-process GC (`GC_POLICY THREAD`).

The policy is load-time and unstable: module load keeps it only when unstable
features are on by then, so each test loads the module with both.
"""

import threading
import time

from common import *

SYNC_POINT_GC_TASK_START = 'GCTaskStart'  # src/debug_commands.h

SCHEMA = ['SCHEMA', 't', 'TEXT', 'n', 'NUMERIC', 'g', 'TAG', 'm', 'TAG', 'INDEXMISSING']


def thread_gc_env(module_args='', fresh=False):
    # GC_POLICY is load-time and only kept when unstable features are on at load, so both come
    # from module args; `enable_unstable_features` would be too late. A test that changes a
    # runtime config other tests rely on asks for a server of its own.
    env = Env(moduleArgs=f'GC_POLICY THREAD ENABLE_UNSTABLE_FEATURES true {module_args}',
              freshEnv=fresh)
    env.expect(config_cmd(), 'SET', 'FORK_GC_CLEAN_THRESHOLD', 0).ok()
    return env


def total_forks(env):
    return int(env.cmd('INFO', 'stats')['total_forks'])


def add_docs(conn, prefix, count):
    for i in range(count):
        fields = ['t', f'hello word{i % 7}', 'n', i % 11, 'g', f'tag{i % 5}']
        if i % 3:
            fields += ['m', 'present']
        conn.execute_command('HSET', f'{prefix}{i}', *fields)


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
def testThreadGCFirstCycleUsesTheDocTableThenTheDeletedSet():
    """A new index's deleted set does not account for garbage it may have been loaded with, so
    the first cycle derives liveness from the doc table; from then on the cycles work off the
    IDs deleted since. Both paths must leave exactly the surviving documents' postings."""
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
def testThreadGCScansOneKeyPerLockWithAZeroBudget():
    """A zero budget releases the read lock after every key, the most scan/apply round trips a
    cycle can take, and still collects everything."""
    env = thread_gc_env(fresh=True)
    env.expect(config_cmd(), 'SET', '_GC_THREAD_SCAN_BUDGET_US', 0).ok()
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'PREFIX', 1, 'doc:', 'INDEXALL', 'ENABLE', *SCHEMA).ok()
    add_docs(conn, 'doc:', 200)
    for i in range(0, 200, 3):
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
def testThreadPolicyNeedsUnstableFeaturesAtLoad():
    """Without unstable features at load the policy stays fork, visibly, and turning the flag
    on later does not bring it back."""
    env = Env(moduleArgs='GC_POLICY THREAD')
    env.expect(config_cmd(), 'SET', 'FORK_GC_CLEAN_THRESHOLD', 0).ok()
    env.assertEqual(env.cmd(config_cmd(), 'GET', 'GC_POLICY'), [['GC_POLICY', 'fork']])
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', *SCHEMA).ok()
    add_docs(conn, 'doc:', 10)
    conn.execute_command('DEL', 'doc:0')

    # Turning the flag on later does not bring the load-time policy back.
    enable_unstable_features(env)
    forks_before = total_forks(env)
    forceInvokeGC(env, 'idx')

    env.assertEqual(total_forks(env), forks_before + 1)


@skip(cluster=True)
def testThreadGCFallsBackToForkWhenUnstableFeaturesTurnOff():
    """Switching unstable features off at runtime makes the next cycle fork again."""
    env = thread_gc_env(fresh=True)
    env.expect('CONFIG', 'SET', 'search-enable-unstable-features', 'no').ok()
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', *SCHEMA).ok()
    add_docs(conn, 'doc:', 10)
    conn.execute_command('DEL', 'doc:0')

    forks_before = total_forks(env)
    forceInvokeGC(env, 'idx')

    env.assertEqual(total_forks(env), forks_before + 1)
    env.assertEqual(env.cmd(debug_cmd(), 'DUMP_INVIDX', 'idx', 'hello'), list(range(2, 11)))


@skip(cluster=True)
def testThreadGCRunsWhileAnotherChildHoldsTheForkSlot():
    """The fork GC has to wait for a BGSAVE child to exit; the thread GC needs no child of its
    own, so it collects while the BGSAVE is still running."""
    env = thread_gc_env()
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', *SCHEMA).ok()
    add_docs(conn, 'doc:', 20)
    conn.execute_command('DEL', 'doc:0')

    # 0.5s per key keeps the child alive for about 10s, far longer than one forced cycle.
    env.expect('CONFIG', 'SET', 'rdb-key-save-delay', 500000).ok()
    env.cmd('BGSAVE')
    env.assertEqual(env.cmd('INFO', 'persistence')['rdb_bgsave_in_progress'], 1)

    # forceInvokeGC waits for the BGSAVE to finish first, so call the debug
    # command directly.
    env.cmd(debug_cmd(), 'GC_FORCEINVOKE', 'idx')

    env.assertEqual(env.cmd('INFO', 'persistence')['rdb_bgsave_in_progress'], 1)
    env.assertEqual(env.cmd(debug_cmd(), 'DUMP_INVIDX', 'idx', 'hello'), list(range(2, 21)))

    env.expect('CONFIG', 'SET', 'rdb-key-save-delay', 0).ok()
    waitForRdbSaveToFinish(env)


@skip(cluster=True)
def testThreadGCKeepsConcurrentQueriesCorrect():
    """Queries hold the read lock between results and revalidate their readers when they re-take
    it; the thread GC applies its deltas in between. Every result stays a duplicate-free subset of
    the documents ever indexed, and the end state is empty."""
    env = thread_gc_env(fresh=True)
    env.expect(config_cmd(), 'SET', '_GC_THREAD_SCAN_BUDGET_US', 0).ok()
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', *SCHEMA).ok()
    add_docs(conn, 'doc:', 2000)

    errors = []
    stop = threading.Event()

    universe = {f'doc:{i}' for i in range(2000)}

    def query_loop():
        query_conn = getConnectionByEnv(env)
        while not stop.is_set():
            try:
                for query in ('hello', '@n:[0 5]'):
                    ids = query_conn.execute_command(
                        'FT.SEARCH', 'idx', query, 'LIMIT', 0, 2000, 'NOCONTENT')[1:]
                    if len(set(ids)) != len(ids) or not set(ids) <= universe:
                        errors.append(f'{query}: inconsistent result {ids[:20]}')
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
    env.assertEqual(index_info(env, 'idx')['num_records'], 0)


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
        for i in range(0, 50, 2):
            conn.execute_command('DEL', f'i{n}:{i}')


def assert_all_indexes_collected(env):
    # Count what survived: without GC each posting list would still hold all 50 documents.
    for n in range(N_INDEXES):
        postings = env.cmd(debug_cmd(), 'DUMP_INVIDX', f'idx{n}', 'hello')
        env.assertEqual(len(postings), 25, message=f'idx{n}: {postings}')


def force_gc_from_parallel_clients(env):
    errors = []

    def force(n):
        try:
            getConnectionByEnv(env).execute_command(debug_cmd(), 'GC_FORCEINVOKE', f'idx{n}')
        except Exception as e:  # noqa: BLE001 - any error fails the test below
            errors.append(e)

    clients = [threading.Thread(target=force, args=(n,)) for n in range(N_INDEXES)]
    for client in clients:
        client.start()
    for client in clients:
        client.join()
    env.assertEqual(errors, [])


@skip(cluster=True)
def testThreadGCWaitForJobsCoversEveryGCThread():
    """GC_WAIT_FOR_JOBS with nothing queued still waits for a cycle running on another GC thread.
    The cycle is parked, so the wait cannot pass by it finishing first."""
    env = thread_gc_env(f'_GC_THREADS {GC_THREADS} FORK_GC_RUN_INTERVAL 1')
    skipIfNoEnableAssert(env)
    env.assertEqual(env.cmd(config_cmd(), 'GET', '_GC_THREADS'), [['_GC_THREADS', str(GC_THREADS)]])
    env.expect('FT.CREATE', 'idx', *SCHEMA).ok()

    env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
    env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', SYNC_POINT_GC_TASK_START, 30000).ok()
    wait_for_condition(
        lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', SYNC_POINT_GC_TASK_START) == 1, {}),
        'Timeout waiting for a scheduled GC run to park')

    replies = []
    waiter = threading.Thread(target=lambda: replies.append(
        getConnectionByEnv(env).execute_command(debug_cmd(), 'GC_WAIT_FOR_JOBS')))
    waiter.start()
    # Long enough for an idle GC pool to have answered many times over.
    waiter.join(timeout=1)
    env.assertEqual(replies, [], message='GC_WAIT_FOR_JOBS returned while a GC cycle was parked')

    env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', SYNC_POINT_GC_TASK_START)
    waiter.join(timeout=30)
    env.assertEqual(replies, ['DONE'])


@skip(cluster=True)
def testThreadGCWaitForJobsWithBackgroundRuns():
    """GC_WAIT_FOR_JOBS returns only after background forced runs on every index finished."""
    env = thread_gc_env(f'_GC_THREADS {GC_THREADS}')
    create_indexes_with_garbage(env)

    waitForRdbSaveToFinish(env)
    for n in range(N_INDEXES):
        env.cmd(debug_cmd(), 'GC_FORCEBGINVOKE', f'idx{n}')
    env.expect(debug_cmd(), 'GC_WAIT_FOR_JOBS').equal('DONE')

    assert_all_indexes_collected(env)


@skip(cluster=True)
def testThreadGCCleansSeveralIndexesConcurrently():
    """Several GC threads clean several indexes at once, without forking."""
    env = thread_gc_env(f'_GC_THREADS {GC_THREADS}')
    create_indexes_with_garbage(env)

    forks_before = total_forks(env)
    force_gc_from_parallel_clients(env)

    env.assertEqual(total_forks(env), forks_before)
    assert_all_indexes_collected(env)


@skip(cluster=True)
def testSeveralGCThreadsTakeTurnsToForkWithUnstableFeaturesOff():
    """With unstable features switched off, the GC threads' fork cycles take turns: every
    index is collected and each forked once."""
    env = thread_gc_env(f'_GC_THREADS {GC_THREADS}')
    env.expect('CONFIG', 'SET', 'search-enable-unstable-features', 'no').ok()
    create_indexes_with_garbage(env)

    forks_before = total_forks(env)
    force_gc_from_parallel_clients(env)

    env.assertEqual(total_forks(env), forks_before + N_INDEXES)
    assert_all_indexes_collected(env)


@skip(cluster=True)
def testThreadGCNeverRunsTwoCyclesOfOneIndexAtOnce():
    """Forced runs from many clients and the index's own schedule all target one index while
    several GC threads are free. Deltas address blocks by position, so an overlap would apply
    one cycle's delta to blocks the other already rewrote; the index must stay exact."""
    env = thread_gc_env(f'_GC_THREADS {GC_THREADS} FORK_GC_RUN_INTERVAL 1 _GC_THREAD_SCAN_BUDGET_US 0')
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

    # The dump reads the index without the spec lock, so wait for the run to end rather than
    # poll while it applies; a run that never came would show as a still-full posting list.
    wait_for_condition(lambda: (gc_total_cycles(env) > 0, {}), 'the scheduled thread GC did not run')
    env.expect(debug_cmd(), 'GC_WAIT_FOR_JOBS').equal('DONE')
    env.assertEqual(len(env.cmd(debug_cmd(), 'DUMP_INVIDX', 'idx', 'hello')), 25)

    env.assertEqual(total_forks(env), forks_before)


def early_starts(env):
    return int(env.cmd(debug_cmd(), 'GC_TIMER_ARMS', 'EARLY_STARTS'))


def gc_total_cycles(env, idx='idx'):
    return int(to_dict(index_info(env, idx)['gc_stats'])['total_cycles'])


def wait_for_a_tick_that_found_too_little(env):
    """A tick arrives within two intervals; with the index too quiet for the threshold it re-arms
    without running a cycle and leaves the GC waiting for deletions. A tick between two of the
    deletions that follow finds the count still short and keeps waiting, and the one that
    reaches the threshold runs synchronously, so the counter read right after it is
    deterministic."""
    cycles = gc_total_cycles(env)
    env.expect(debug_cmd(), 'GC_TIMER_ARMS', 'RESET').ok()
    wait_for_condition(lambda: (int(env.cmd(debug_cmd(), 'GC_TIMER_ARMS', 'TOTAL')) > 0, {}),
                       'no GC tick within two intervals', timeout=5)
    env.assertEqual(gc_total_cycles(env), cycles, message='the tick ran a cycle instead of waiting')


@skip(cluster=True)
def testThreadGCStartsEarlyOnceDeletionsReachTheThreshold():
    """A scheduled tick that finds too little to do does not make the garbage wait out another
    interval: the deletion that brings the count to FORK_GC_CLEAN_THRESHOLD starts the run."""
    env = thread_gc_env('FORK_GC_RUN_INTERVAL 1')
    env.expect(config_cmd(), 'SET', 'FORK_GC_CLEAN_THRESHOLD', 100).ok()
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', *SCHEMA).ok()
    add_docs(conn, 'doc:', 200)
    wait_for_a_tick_that_found_too_little(env)

    for i in range(99):
        conn.execute_command('DEL', f'doc:{i}')
    env.assertEqual(early_starts(env), 0, message='below the threshold nothing starts')
    conn.execute_command('DEL', 'doc:99')
    env.assertEqual(early_starts(env), 1, message='the deletion that reaches it does')

    # The dump reads the index without the spec lock, so let the run finish first.
    env.expect(debug_cmd(), 'GC_WAIT_FOR_JOBS').equal('DONE')
    env.assertEqual(len(env.cmd(debug_cmd(), 'DUMP_INVIDX', 'idx', 'hello')), 100)


@skip(cluster=True)
def testThreadGCWaitingForWorkStillNoticesADroppedIndex():
    """A GC left waiting for deletions by a quiet tick must still run once its index is dropped,
    since that run is what frees the GC; otherwise its timer would re-arm forever."""
    env = thread_gc_env('FORK_GC_RUN_INTERVAL 1')
    env.expect(config_cmd(), 'SET', 'FORK_GC_CLEAN_THRESHOLD', 100).ok()
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', *SCHEMA).ok()
    add_docs(conn, 'doc:', 10)
    wait_for_a_tick_that_found_too_little(env)

    env.expect('FT.DROPINDEX', 'idx').ok()

    # The armed tick runs, finds the index gone and frees the GC, so nothing arms a timer again;
    # a GC that kept waiting would re-arm once per interval instead.
    arms = int(env.cmd(debug_cmd(), 'GC_TIMER_ARMS', 'TOTAL'))
    time.sleep(2.5)
    env.assertEqual(int(env.cmd(debug_cmd(), 'GC_TIMER_ARMS', 'TOTAL')), arms,
                    message='the dropped index keeps re-arming its GC timer')


@skip(cluster=True)
def testThreadGCDoesNotStartEarlyWhileSchedulingIsStopped():
    """Stopping the schedule ends the wait a quiet tick began: deletions reaching the threshold
    start nothing until the schedule resumes, which runs the pending work right away."""
    env = thread_gc_env('FORK_GC_RUN_INTERVAL 1')
    env.expect(config_cmd(), 'SET', 'FORK_GC_CLEAN_THRESHOLD', 100).ok()
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', *SCHEMA).ok()
    add_docs(conn, 'doc:', 200)
    wait_for_a_tick_that_found_too_little(env)

    env.expect(debug_cmd(), 'GC_STOP_SCHEDULE', 'idx').ok()
    for i in range(100):
        conn.execute_command('DEL', f'doc:{i}')
    env.assertEqual(early_starts(env), 0, message='a stopped schedule starts nothing')

    env.expect(debug_cmd(), 'GC_CONTINUE_SCHEDULE', 'idx').ok()

    # Resuming queues the pending work at once; the dump reads the index without the spec lock,
    # so let that run finish first.
    env.expect(debug_cmd(), 'GC_WAIT_FOR_JOBS').equal('DONE')
    env.assertEqual(len(env.cmd(debug_cmd(), 'DUMP_INVIDX', 'idx', 'hello')), 100,
                    message='resuming the schedule did not run the pending work')


@skip(cluster=True)
def testThreadGCDoesNotStartEarlyAfterAForcedRun():
    """A forced run does the work a quiet tick put off, so it ends the wait: deletions that reach
    the threshold afterwards do not start another run before the next tick."""
    # The interval is changed at runtime below; keep that to a server of this test's own.
    env = thread_gc_env('FORK_GC_RUN_INTERVAL 1', fresh=True)
    env.expect(config_cmd(), 'SET', 'FORK_GC_CLEAN_THRESHOLD', 100).ok()
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', *SCHEMA).ok()
    add_docs(conn, 'doc:', 200)
    wait_for_a_tick_that_found_too_little(env)
    # Push the next tick an hour out: a waiting tick re-arms with the current interval, so once
    # one more has passed no tick can land during the rest of the test.
    env.expect(config_cmd(), 'SET', 'FORK_GC_RUN_INTERVAL', 3600).ok()
    wait_for_a_tick_that_found_too_little(env)

    forceInvokeGC(env, 'idx')
    for i in range(100):
        conn.execute_command('DEL', f'doc:{i}')

    env.assertEqual(early_starts(env), 0, message='the forced run ended the wait')


@skip(cluster=True)
def testForkGCKeepsToItsIntervalBelowTheThreshold():
    """The fork policy never waits for deletions: a fork is costly enough to keep to its
    interval, so reaching the threshold between ticks starts nothing early."""
    env = Env(moduleArgs='FORK_GC_RUN_INTERVAL 1 FORK_GC_CLEAN_THRESHOLD 100')
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', *SCHEMA).ok()
    add_docs(conn, 'doc:', 200)
    wait_for_a_tick_that_found_too_little(env)

    for i in range(100):
        conn.execute_command('DEL', f'doc:{i}')

    env.assertEqual(early_starts(env), 0)


@skip(cluster=True)
def testThreadGCSettingsBounds():
    """`_GC_THREADS` is load-time and between 1 and 64; the scan budget is runtime and may be 0."""
    env = Env(moduleArgs='GC_POLICY THREAD ENABLE_UNSTABLE_FEATURES true _GC_THREADS 64')
    env.assertEqual(env.cmd(config_cmd(), 'GET', '_GC_THREADS'), [['_GC_THREADS', '64']])
    env.expect(config_cmd(), 'SET', '_GC_THREADS', 2).error().contains('Not modifiable at runtime')

    env.expect(config_cmd(), 'SET', '_GC_THREAD_SCAN_BUDGET_US', 0).ok()
    env.assertEqual(env.cmd(config_cmd(), 'GET', '_GC_THREAD_SCAN_BUDGET_US'),
                    [['_GC_THREAD_SCAN_BUDGET_US', '0']])
    env.expect(config_cmd(), 'SET', '_GC_THREAD_SCAN_BUDGET_US', -1).error()
