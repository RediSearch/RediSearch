# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from common import *

# `search-defer-contended-indexing` lets an indexed write whose spec write lock
# is contended be queued and applied on a later event-loop iteration, instead of
# parking the main thread until in-flight queries release the read lock.
#
# The contract these tests pin down:
#   - default off, so nothing changes unless it is asked for;
#   - a write is always eventually indexed, deferred or not;
#   - index application keeps command order, so the last write to a key wins;
#   - the document type survives deferral (a JSON doc must not be re-indexed as
#     a hash);
#   - the queue drains rather than growing without bound.
#
# Contention is not forced here. Producing it deterministically needs a query
# holding the read lock across a write, which is inherently timing-dependent, so
# these tests assert the observable contract under both paths rather than
# asserting which path ran. The deferred path is covered by driving enough
# concurrent query and write load that it is taken in practice, then requiring
# the end state to be correct either way -- a test that passes when the write
# was synchronous and fails if a deferred write is ever lost or misordered.

DOC_COUNT = 200


def _conn(env):
    """Cluster-aware connection for keyed commands.

    `env.cmd` reaches one shard, so an `HSET`/`DEL`/`JSON.SET` for a slot that
    shard does not own returns MOVED rather than executing. `FT.*` needs no
    such handling -- any shard coordinates it.
    """
    return getConnectionByEnv(env)


def _set_defer(env, enabled):
    """The flag is a module config (`CONFIG SET search-*`), not an `_FT.CONFIG`
    option, so it must be set on every shard."""
    run_command_on_all_shards(env, 'CONFIG', 'SET',
                              'search-defer-contended-indexing', 'yes' if enabled else 'no')


def _get_defer(env):
    return env.cmd('CONFIG', 'GET', 'search-defer-contended-indexing')[1]


def _wait_until_indexed(env, idx, expected, timeout=10):
    """Poll until the index reports `expected` docs, or fail.

    Deferral makes indexing asynchronous, so a test must not assume a write is
    visible the instant the command returns.
    """
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        last = index_info(env, idx)['num_docs']
        if int(last) == expected:
            return
        time.sleep(0.05)
    env.assertEqual(int(last), expected, message=f'{idx} did not reach {expected} docs')


def testDeferredIndexingDefaultsOff(env):
    """The feature must be opt-in: it changes read-your-write semantics."""
    env.assertEqual(_get_defer(env), 'no')


def testDeferredIndexingIsRuntimeSettable(env):
    """Operators must be able to turn it off without a restart if it misbehaves."""
    _set_defer(env, True)
    env.assertEqual(_get_defer(env), 'yes')
    _set_defer(env, False)
    env.assertEqual(_get_defer(env), 'no')


def testWritesAreIndexedWhenDeferralEnabled(env):
    """Every write must end up indexed, whichever path it took."""
    _set_defer(env, True)
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'PREFIX', '1', 'doc:',
               'SCHEMA', 'n', 'NUMERIC', 't', 'TEXT').ok()

    con = _conn(env)
    for i in range(DOC_COUNT):
        env.assertEqual(con.execute_command('HSET', f'doc:{i}', 'n', i, 't', f'term{i}'), 2)

    _wait_until_indexed(env, 'idx', DOC_COUNT)
    env.expect('FT.SEARCH', 'idx', '*', 'LIMIT', '0', '0').equal([DOC_COUNT])
    # A specific document is findable by its own indexed content, not just counted.
    env.expect('FT.SEARCH', 'idx', '@n:[42 42]', 'NOCONTENT').equal([1, 'doc:42'])


def testLastWriteToAKeyWins(env):
    """Deferral must not let a later write be indexed ahead of an earlier one.

    The fast path is gated on the queue being empty precisely so that ordering
    survives; this is the observable consequence.
    """
    _set_defer(env, True)
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'PREFIX', '1', 'doc:',
               'SCHEMA', 'n', 'NUMERIC').ok()

    con = _conn(env)
    for i in range(DOC_COUNT):
        con.execute_command('HSET', 'doc:1', 'n', i)

    _wait_until_indexed(env, 'idx', 1)
    # The index must reflect the final value, never an intermediate one.
    last = DOC_COUNT - 1
    env.expect('FT.SEARCH', 'idx', f'@n:[{last} {last}]', 'NOCONTENT').equal([1, 'doc:1'])
    env.expect('FT.SEARCH', 'idx', f'@n:[0 {last - 1}]', 'NOCONTENT').equal([0])


def testDeletionIsNotReordedAgainstAWrite(env):
    """A delete after a write must not be overtaken by the queued write."""
    _set_defer(env, True)
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'PREFIX', '1', 'doc:',
               'SCHEMA', 'n', 'NUMERIC').ok()

    con = _conn(env)
    for i in range(DOC_COUNT):
        con.execute_command('HSET', f'doc:{i}', 'n', i)
    _wait_until_indexed(env, 'idx', DOC_COUNT)

    for i in range(DOC_COUNT):
        con.execute_command('DEL', f'doc:{i}')
    _wait_until_indexed(env, 'idx', 0)
    env.expect('FT.SEARCH', 'idx', '*', 'LIMIT', '0', '0').equal([0])


@skip(no_json=True)
def testJsonDocumentTypeSurvivesDeferral(env):
    """A deferred JSON document must not be re-indexed as a hash.

    The queue carries the notification's DocumentType for this reason; dropping
    it would silently corrupt a JSON index under load.
    """
    _set_defer(env, True)
    env.expect('FT.CREATE', 'jidx', 'ON', 'JSON', 'PREFIX', '1', 'j:',
               'SCHEMA', '$.n', 'AS', 'n', 'NUMERIC').ok()

    con = _conn(env)
    for i in range(DOC_COUNT):
        con.execute_command('JSON.SET', f'j:{i}', '$', f'{{"n":{i}}}')

    _wait_until_indexed(env, 'jidx', DOC_COUNT)
    env.expect('FT.SEARCH', 'jidx', '@n:[7 7]', 'NOCONTENT').equal([1, 'j:7'])


def testQueueDrainsUnderConcurrentQueryAndWriteLoad(env):
    """Drive the deferred path and require the end state to be exact.

    Concurrent queries are what make the write lock contended, so this is the
    case where deferral actually engages. The assertion is on the end state, so
    the test is meaningful whether or not contention occurred on any given run,
    and fails if a deferred write is dropped or the queue fails to drain.
    """
    _set_defer(env, True)
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'PREFIX', '1', 'doc:',
               'SCHEMA', 'n', 'NUMERIC', 't', 'TEXT').ok()

    stop = threading.Event()

    def query_loop():
        con = env.getConnection()
        while not stop.is_set():
            try:
                con.execute_command('FT.AGGREGATE', 'idx', '*', 'GROUPBY', '1', '@n',
                                    'REDUCE', 'COUNT', '0', 'LIMIT', '0', '1')
            except Exception:
                # The index may be mid-update; a failed query here is not the
                # property under test.
                pass

    readers = [threading.Thread(target=query_loop) for _ in range(4)]
    for t in readers:
        t.start()
    try:
        con = _conn(env)
        for i in range(DOC_COUNT):
            con.execute_command('HSET', f'doc:{i}', 'n', i % 10, 't', f'term{i}')
    finally:
        stop.set()
        for t in readers:
            t.join()

    _wait_until_indexed(env, 'idx', DOC_COUNT)
    env.expect('FT.SEARCH', 'idx', '*', 'LIMIT', '0', '0').equal([DOC_COUNT])


def testDisablingMidFlightStillDrainsQueuedWrites(env):
    """Turning the flag off must not orphan already-queued writes."""
    _set_defer(env, True)
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'PREFIX', '1', 'doc:',
               'SCHEMA', 'n', 'NUMERIC').ok()

    con = _conn(env)
    for i in range(DOC_COUNT):
        con.execute_command('HSET', f'doc:{i}', 'n', i)
    _set_defer(env, False)

    _wait_until_indexed(env, 'idx', DOC_COUNT)
    env.expect('FT.SEARCH', 'idx', '*', 'LIMIT', '0', '0').equal([DOC_COUNT])


# The tests above assert the contract without controlling which path ran. The
# ones below hold the spec read lock at a sync point, which makes the write lock
# genuinely unavailable to the main thread, so the deferred path is the only one
# it can take -- and `GET_DEFERRED_PENDING` makes that visible.
#
# Standalone only: the sync point and the queue are per-shard, and in a cluster
# the query fans out to shards whose write lock the tested write never touches.
SYNC_POINT = 'AfterIteratorCreate'

# How long the parked query holds the read lock. Release is by self-timeout
# rather than SIGNAL, because a deferred entry that sits at the head long enough
# makes the drain escalate and park the main thread on the write lock -- and a
# parked main thread can never process a SIGNAL. Same reasoning as the GC tests.
HOLD_MS = 1000


def _park_query_holding_read_lock(env, conn, results):
    """Arm the sync point and start a query that parks holding the read lock.

    `AfterIteratorCreate` is reached on the worker thread with the spec read
    lock still held (RAM-backed indexes only -- a disk index releases it once
    its snapshot exists), which is exactly the state that makes the main
    thread's write lock unavailable.
    """
    # The sync point sits on the worker-thread execution path, which the query
    # only takes when there is a worker to take it: with `WORKERS 0` it runs on
    # the main thread instead, where a park would freeze the shard.
    set_workers(env, 2)
    env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
    env.cmd(debug_cmd(), 'SYNC_POINT', 'ARM', SYNC_POINT, HOLD_MS)

    def run():
        try:
            results.append(conn.execute_command('FT.SEARCH', 'idx', '*', 'LIMIT', '0', '1'))
        except Exception as e:
            results.append(e)

    t = threading.Thread(target=run, daemon=True)
    t.start()
    wait_for_condition(
        lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', SYNC_POINT) == 1, {}),
        'query never reached the sync point holding the spec read lock')
    return t


def _write_and_read_pending(env, *write_cmd):
    """Issue a write and read the queue depth without an intervening drain.

    Both go out as one transaction, so no event-loop iteration -- and therefore
    no drain timer -- can run between them. Reading the depth in a second round
    trip would race the drain.
    """
    p = env.getConnection().pipeline()
    p.execute_command(*write_cmd)
    p.execute_command(debug_cmd(), 'GET_DEFERRED_PENDING')
    return p.execute()


@skip(cluster=True)
def testWriteIsDeferredWhileTheReadLockIsHeld(env):
    skipIfNoEnableAssert(env)
    _set_defer(env, True)
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'PREFIX', '1', 'doc:',
               'SCHEMA', 'n', 'NUMERIC').ok()
    env.cmd('HSET', 'doc:seed', 'n', 0)
    _wait_until_indexed(env, 'idx', 1)

    results = []
    t = _park_query_holding_read_lock(env, env.getConnection(), results)
    try:
        hset_rc, pending = _write_and_read_pending(env, 'HSET', 'doc:deferred', 'n', 1)
        # The client is answered as usual; only indexing was postponed.
        env.assertEqual(hset_rc, 1)
        env.assertGreater(pending, 0)
    finally:
        t.join(timeout=30)
        env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')

    # Once the reader is gone the queue must drain and the write must land.
    _wait_until_indexed(env, 'idx', 2)
    env.assertEqual(env.cmd(debug_cmd(), 'GET_DEFERRED_PENDING'), 0)
    env.expect('FT.SEARCH', 'idx', '@n:[1 1]', 'NOCONTENT').equal([1, 'doc:deferred'])


@skip(cluster=True)
def testDeIndexingIsDeferredWhileTheReadLockIsHeld(env):
    """Removals share the queue, so one under contention defers too.

    A write that makes a document stop matching the index FILTER de-indexes it.
    Had that stayed synchronous it would have parked the main thread -- the
    stall this feature removes -- and could have been applied ahead of an update
    already queued for the same key.

    A plain `DEL` is a different path: Redis unlinks the key before the
    notification, and de-indexing happens in that unlink callback, which this
    queue does not cover. It stays consistent anyway, because the drain re-reads
    the key: an update queued for a key that has since been deleted fails to
    load and de-indexes instead.
    """
    skipIfNoEnableAssert(env)
    _set_defer(env, True)
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'PREFIX', '1', 'doc:',
               'FILTER', '@n<100', 'SCHEMA', 'n', 'NUMERIC').ok()
    env.cmd('HSET', 'doc:gone', 'n', 1)
    _wait_until_indexed(env, 'idx', 1)

    results = []
    t = _park_query_holding_read_lock(env, env.getConnection(), results)
    try:
        hset_rc, pending = _write_and_read_pending(env, 'HSET', 'doc:gone', 'n', 200)
        env.assertEqual(hset_rc, 0)  # field existed, so no new field was added
        env.assertGreater(pending, 0)
    finally:
        t.join(timeout=30)
        env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')

    _wait_until_indexed(env, 'idx', 0)
    env.assertEqual(env.cmd(debug_cmd(), 'GET_DEFERRED_PENDING'), 0)


@skip(cluster=True)
def testNothingIsQueuedWhenTheFlagIsOff(env):
    """With the flag off the same contention must queue nothing.

    The write is issued from a second thread because with deferral off it parks
    the main thread on the write lock until the reader releases -- precisely the
    behaviour this feature exists to avoid.
    """
    skipIfNoEnableAssert(env)
    _set_defer(env, False)
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'PREFIX', '1', 'doc:',
               'SCHEMA', 'n', 'NUMERIC').ok()

    results = []
    t = _park_query_holding_read_lock(env, env.getConnection(), results)
    write_done = []

    def write():
        try:
            write_done.append(env.getConnection().execute_command('HSET', 'doc:sync', 'n', 1))
        except Exception as e:
            write_done.append(e)

    w = threading.Thread(target=write, daemon=True)
    w.start()
    w.join(timeout=30)
    t.join(timeout=30)
    env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')

    env.assertEqual(write_done, [1])
    _wait_until_indexed(env, 'idx', 1)
    env.assertEqual(env.cmd(debug_cmd(), 'GET_DEFERRED_PENDING'), 0)
