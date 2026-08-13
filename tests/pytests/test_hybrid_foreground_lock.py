"""
Spec-lock coverage for the single-threaded (WORKERS 0) FT.HYBRID path, which takes one
spec read lock for the whole execution and lends it to the sub-request contexts
(SPEC_LOCK_READ_BORROWED) instead of letting their query iterators re-acquire it.

test_hybrid_foreground_build_excludes_gc_writer is the MOD-16215 regression test: it
fails if the build stops holding the read lock. The rest pin the release protocol - the
lock is given back on success, after cursor creation, and on the error exits either side
of the borrow.
"""

from common import *

DIM = 4


def _create_index(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               'vector', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', DIM,
               'DISTANCE_METRIC', 'L2',
               'text', 'TEXT').ok()


def _load(env, n):
    conn = getConnectionByEnv(env)
    for i in range(n):
        conn.execute_command('HSET', f'doc:{i}',
                             'text', f'hello world {i % 5}',
                             'vector', create_np_array_typed([float(i)] * DIM).tobytes())


def _blob():
    return create_np_array_typed([0.0] * DIM).tobytes()


def _hybrid_cmd(*extra):
    return ['FT.HYBRID', 'idx',
            'SEARCH', '@text:(hello)',
            'VSIM', '@vector', '$BLOB',
            'PARAMS', '2', 'BLOB', _blob(), *extra]


_probe_seq = itertools.count()


def _assert_writer_not_blocked(env, when):
    """A leaked read lock is silent until the next writer, so probe with a forced GC: it
    replies DONE once the cycle ran, and an error if its blocked client never got the
    write lock within the timeout.

    The write lock is only taken per collected term (fork_gc/terms.c), so a cycle with
    nothing to collect never takes it - seed a deletion first or the probe passes either
    way.
    """
    conn = getConnectionByEnv(env)
    probe = f'probe:{next(_probe_seq)}'
    conn.execute_command('HSET', probe, 'text', 'hello probe')
    conn.execute_command('DEL', probe)

    waitForRdbSaveToFinish(env)
    try:
        reply = env.cmd(debug_cmd(), 'GC_FORCEINVOKE', 'idx')
    except Exception as e:
        reply = str(e)
    env.assertEqual(reply, 'DONE',
                    message=f'writer did not acquire the spec lock after {when}')


# The build park is deliberately generous: the test can only go red if GC reaches the
# write lock inside that window, so a wide margin over the few ms it actually needs is
# what keeps the red side reliable on a loaded host.
_BUILD_PARK_MS = 3000
_GC_HOLD_MS = 500
_GC_ARRIVAL_TIMEOUT_MS = 30000


@skip(cluster=True)
def test_hybrid_foreground_build_excludes_gc_writer():
    """Regression test for MOD-16215: the foreground build must hold the spec read lock,
    so a fork-GC writer cannot mutate the trie/stats that QAST_Iterate reads.

    The main thread is parked inside the query for the whole window, so no command can
    release either side - both parks release themselves, and how the build's park ended
    is the oracle:

      * read lock held    -> GC blocks in LockSpecWrite and never reaches its post-lock
        park, the build's predicate never fires, and it exits on TIMEOUT.
      * read lock missing -> GC takes the write lock and parks there, the build's
        predicate sees it, and it exits on PREDICATE.

    Recorded at the end of the park rather than inferred from how long the query took: a
    host stall can push the unlocked path past any elapsed-time threshold. Elapsed time
    is kept only as a diagnostic.
    """
    env = Env(moduleArgs='WORKERS 0 DEFAULT_DIALECT 2', enableDebugCommand=True)
    skipIfNoEnableAssert(env)
    _create_index(env)
    _load(env, 20)

    # Deleted docs give the term apply something to collect, which is what takes the
    # spec write lock.
    conn = getConnectionByEnv(env)
    for i in range(10):
        conn.execute_command('DEL', f'doc:{i}')

    env.expect(debug_cmd(), 'SYNC_POINT', 'ARM',
               'GcAfterSpecWriteLock', _GC_HOLD_MS).ok()
    env.expect(debug_cmd(), 'SYNC_POINT', 'ARM',
               'GcBeforeSpecWriteLock', _GC_ARRIVAL_TIMEOUT_MS).ok()
    env.expect(debug_cmd(), 'SYNC_POINT', 'ARM',
               'HybridForegroundBuild', _BUILD_PARK_MS).ok()

    # Fork now, while the main thread is idle: the fork needs the GIL, which the query is
    # about to hold. GC then parks just short of the write lock until the query is parked,
    # so the two are guaranteed to overlap.
    forceBGInvokeGC(env, 'idx')
    wait_for_condition(
        lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING',
                         'GcBeforeSpecWriteLock') == 1, {}),
        'Timeout waiting for GC to park before the spec write lock')

    start = time.time()
    res = env.cmd(*_hybrid_cmd())
    elapsed_ms = (time.time() - start) * 1000

    env.assertGreater(len(res), 0)
    exit_reason = env.cmd(debug_cmd(), 'SYNC_POINT', 'LAST_EXIT', 'HybridForegroundBuild')
    env.assertEqual(exit_reason, 'TIMEOUT',
                    message='GC acquired the spec write lock while the foreground build '
                            'was in progress, so the build did not hold the read lock '
                            f'(query took {elapsed_ms:.0f}ms)')

    env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
    env.cmd(debug_cmd(), 'GC_WAIT_FOR_JOBS')
    _assert_writer_not_blocked(env, 'the GC-writer exclusion test')


def test_hybrid_foreground_releases_spec_lock(env):
    _create_index(env)
    _load(env, 20)

    res = env.cmd(*_hybrid_cmd())
    env.assertGreater(len(res), 0)
    _assert_writer_not_blocked(env, 'a successful FT.HYBRID')

    # Both errors are raised before the borrow is taken, covering the plain
    # unlock-on-error path; the post-borrow exit is the cursor-limit test below.
    env.expect('FT.HYBRID', 'idx', 'SEARCH', '@nosuchfield:(hello)',
               'VSIM', '@vector', '$BLOB',
               'PARAMS', '2', 'BLOB', _blob()).error().contains('Unknown field')
    _assert_writer_not_blocked(env, 'an unknown-field error')

    env.expect('FT.HYBRID', 'idx', 'SEARCH', '@text:(hello)',
               'VSIM', '@vector', '$BLOB',
               'PARAMS', '2', 'BLOB', b'too-short').error().contains('blob size')
    _assert_writer_not_blocked(env, 'a bad vector blob')


# User-facing `FT.HYBRID WITHCURSOR` is rejected at the public boundary (see cursor.h),
# so the cursor path - inline depletion under the borrowed lock, then publish - is only
# reachable through the internal command.
def _internal_cursor_cmd(slots):
    return ['_FT.HYBRID', 'idx',
            'SEARCH', '@text:(hello)',
            'VSIM', '@vector', '$BLOB',
            'WITHCURSOR', '_SLOTS_INFO', slots,
            'PARAMS', '2', 'BLOB', _blob(),
            '_COORD_DISPATCH_TIME', '1000000']


@skip(cluster=True)
def test_hybrid_foreground_releases_spec_lock_with_cursor(env):
    _create_index(env)
    _load(env, 20)
    env.cmd('DEBUG', 'MARK-INTERNAL-CLIENT')
    slots = generate_slots()

    res = env.cmd(*_internal_cursor_cmd(slots))
    # One cursor per sub-request, published only after inline depletion finished.
    cursors = to_dict(res)
    env.assertContains('SEARCH', cursors)
    env.assertContains('VSIM', cursors)
    _assert_writer_not_blocked(env, '_FT.HYBRID WITHCURSOR')

    for name in ('SEARCH', 'VSIM'):
        env.cmd('_FT.CURSOR', 'DEL', 'idx', cursors[name])
    _assert_writer_not_blocked(env, '_FT.CURSOR DEL')


@skip(cluster=True)
def test_hybrid_foreground_releases_spec_lock_on_cursor_error():
    """Cursor reservation fails *after* the sub-requests borrowed the lock, so this is
    the error exit that has to return the borrows before unlocking."""
    env = Env(moduleArgs='WORKERS 0 DEFAULT_DIALECT 2')
    # One cursor available, two needed (SEARCH + VSIM).
    env.cmd('CONFIG', 'SET', 'search-index-cursor-limit', '1')
    _create_index(env)
    _load(env, 20)
    env.cmd('DEBUG', 'MARK-INTERNAL-CLIENT')
    slots = generate_slots()

    env.expect(*_internal_cursor_cmd(slots)).error().contains('INDEX_CURSOR_LIMIT')
    _assert_writer_not_blocked(env, 'a cursor-limit error')

    # The lock is genuinely free: a schema write also has to take it.
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'extra', 'TEXT').ok()


def _doc_keys(reply):
    """Every `doc:*` key anywhere in a hybrid reply, whatever the nesting or protocol."""
    found = set()

    def walk(node):
        if isinstance(node, bytes):
            node = node.decode(errors='ignore')
        if isinstance(node, str):
            if node.startswith('doc:'):
                found.add(node)
        elif isinstance(node, dict):
            for k, v in node.items():
                walk(k)
                walk(v)
        elif isinstance(node, (list, tuple)):
            for item in node:
                walk(item)

    walk(reply)
    return found


def test_hybrid_foreground_gc_churn(env):
    """GC mutates the trie/stats that the pipeline build reads. Run queries around forced
    GC cycles and check only live documents come back."""
    _create_index(env)
    _load(env, 60)
    conn = getConnectionByEnv(env)

    for round_idx in range(3):
        first = round_idx * 10
        for i in range(first, first + 10):
            conn.execute_command('DEL', f'doc:{i}')
        forceInvokeGC(env, 'idx')

        keys = _doc_keys(env.cmd(*_hybrid_cmd('LIMIT', '0', '100')))
        # Guards the guard: if key extraction ever stops finding keys, the deleted-doc
        # check below would silently pass on an empty set.
        env.assertGreater(len(keys), 0, message='no doc keys found in hybrid reply')
        deleted = {f'doc:{i}' for i in range(first + 10)}
        env.assertEqual(keys & deleted, set(),
                        message=f'deleted docs returned after GC: {keys & deleted}')
        _assert_writer_not_blocked(env, f'a query in churn round {round_idx}')
