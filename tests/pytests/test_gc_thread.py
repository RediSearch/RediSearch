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

from common import *

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
    # The fork GC has to wait for a BGSAVE child to exit; the thread GC needs
    # no child of its own, so it collects while the BGSAVE is still running.
    env = thread_gc_env()
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', *SCHEMA).ok()
    add_docs(conn, 'doc:', 20)
    conn.execute_command('DEL', 'doc:0')

    env.expect('CONFIG', 'SET', 'rdb-key-save-delay', 100000).ok()  # 0.1s per key
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
    # Queries hold the read lock between results and revalidate their readers
    # when they re-take it; the thread GC applies its deltas in between.
    env = thread_gc_env(fresh=True)
    env.expect(config_cmd(), 'SET', '_GC_THREAD_SCAN_BUDGET_US', 0).ok()
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', *SCHEMA).ok()
    add_docs(conn, 'doc:', 2000)

    errors = []
    stop = threading.Event()

    def query_loop():
        query_conn = getConnectionByEnv(env)
        while not stop.is_set():
            try:
                query_conn.execute_command('FT.SEARCH', 'idx', 'hello', 'LIMIT', 0, 1000, 'NOCONTENT')
                query_conn.execute_command('FT.SEARCH', 'idx', '@n:[0 5]', 'LIMIT', 0, 1000, 'NOCONTENT')
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



@skip(cluster=True)
def testScheduledThreadGCCollectsWithoutForking():
    # Every other test here forces its cycles; this one waits for the index's own schedule.
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
