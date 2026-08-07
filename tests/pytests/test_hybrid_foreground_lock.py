"""
Spec-lock coverage for the single-threaded (WORKERS 0) FT.HYBRID path.

That path takes one spec read lock for the whole execution and lends it to the
sub-request contexts (SPEC_LOCK_READ_BORROWED) rather than letting their query
iterators re-acquire it. Two properties of that protocol are pinned here:

  1. The lock is always released - on success, after cursor creation, and on the
     error exits, including the ones taken after the borrow. A leaked read lock is
     silent until the next writer, so every case is followed by a forced GC, which
     needs the write lock and would block forever if the lock or a borrow marker
     leaked.
  2. Results stay coherent when GC mutates the index around the query, which is
     the race the read lock is there to prevent (MOD-16215).

Not covered: the interleaving where a writer queues on the rwlock while the query
holds the read lock and before a sub-request's first read - the ordering that would
deadlock if the borrow were dropped and the iterators re-locked. It is not
deterministically reachable at WORKERS 0: the main thread is inside the query, so
no other client can be served to drive a sync point, and fork-GC has no sync point
at its write-lock acquisition to park a writer there. test_hybrid_foreground_gc_churn
reaches for it opportunistically by forcing GC between queries.
"""

from common import *

DIM = 4


def _create_index(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               'vector', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', DIM,
               'DISTANCE_METRIC', 'L2',
               'text', 'TEXT',
               'number', 'NUMERIC').ok()


def _load(env, n):
    conn = getConnectionByEnv(env)
    for i in range(n):
        conn.execute_command('HSET', f'doc:{i}',
                             'text', f'hello world {i % 5}',
                             'number', i,
                             'vector', create_np_array_typed([float(i)] * DIM).tobytes())


def _blob(value=0.0):
    return create_np_array_typed([value] * DIM).tobytes()


def _hybrid_cmd(*extra):
    return ['FT.HYBRID', 'idx',
            'SEARCH', '@text:(hello)',
            'VSIM', '@vector', '$BLOB',
            'PARAMS', '2', 'BLOB', _blob(), *extra]


def _assert_writer_not_blocked(env, when):
    """A forced GC needs the spec write lock, so it cannot complete while a read
    lock is still held. Hangs (rather than fails) if the lock leaked."""
    forceInvokeGC(env, 'idx')
    env.assertTrue(True, message=f'writer acquired the spec lock after {when}')


def test_hybrid_foreground_releases_spec_lock(env):
    _create_index(env)
    _load(env, 20)

    # Success, non-cursor: the lock is held across build, depletion and the tail
    # pipeline, then released once.
    res = env.cmd(*_hybrid_cmd())
    env.assertGreater(len(res), 0)
    _assert_writer_not_blocked(env, 'a successful FT.HYBRID')

    # Error exits: both are rejected before the borrow is taken, so they cover the
    # plain unlock-on-error path (the post-borrow exit is covered by the
    # cursor-limit test below).
    env.expect('FT.HYBRID', 'idx', 'SEARCH', '@nosuchfield:(hello)',
               'VSIM', '@vector', '$BLOB',
               'PARAMS', '2', 'BLOB', _blob()).error().contains('Unknown field')
    _assert_writer_not_blocked(env, 'an unknown-field error')

    env.expect('FT.HYBRID', 'idx', 'SEARCH', '@text:(hello)',
               'VSIM', '@vector', '$BLOB',
               'PARAMS', '2', 'BLOB', b'too-short').error().contains('blob size')
    _assert_writer_not_blocked(env, 'a bad vector blob')


# User-facing `FT.HYBRID WITHCURSOR` is rejected at the public boundary (see
# cursor.h), so the cursor path - the one that depletes inline under the borrowed
# lock and then publishes cursors - is only reachable through the internal command.
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
    """Cursor reservation fails *after* the sub-requests borrowed the lock, so this
    is the error exit that has to return the borrows before unlocking."""
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
    """Collect every `doc:*` key anywhere in a hybrid reply, whatever the nesting
    or protocol."""
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
    """GC mutates the trie/stats that the pipeline build reads. Run queries around
    forced GC cycles and check only live documents come back."""
    _create_index(env)
    _load(env, 60)
    conn = getConnectionByEnv(env)

    for round_idx in range(3):
        first = round_idx * 10
        for i in range(first, first + 10):
            conn.execute_command('DEL', f'doc:{i}')
        forceInvokeGC(env, 'idx')

        keys = _doc_keys(env.cmd(*_hybrid_cmd('LIMIT', '0', '100')))
        # Guards the guard: if key extraction ever stops finding keys, the
        # deleted-doc check below would silently pass on an empty set.
        env.assertGreater(len(keys), 0, message='no doc keys found in hybrid reply')
        deleted = {f'doc:{i}' for i in range(first + 10)}
        env.assertEqual(keys & deleted, set(),
                        message=f'deleted docs returned after GC: {keys & deleted}')
        _assert_writer_not_blocked(env, f'a query in churn round {round_idx}')
