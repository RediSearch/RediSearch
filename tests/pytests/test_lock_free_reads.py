"""
Tests for the experimental `_LOCK_FREE_READS` request config.

With `_LOCK_FREE_READS` enabled, a RAM cursor query releases the spec read lock right
after it takes its inverted-index snapshot (see `shouldReleaseSpecAfterSnapshot` in
`aggregate/aggregate_exec.c`). Because the reader then iterates from its own immutable
snapshot, a concurrent writer that needs the spec *write* lock can make progress while
the query is still holding a cursor mid-execution. Without the flag the query keeps the
read lock for its whole execution and such a writer blocks until the query finishes.

The test is made deterministic with the `AfterIteratorCreate` sync point, which fires
just after the snapshot is established (and, under the flag, just after the read lock is
released). Sync points only exist in `ENABLE_ASSERT` builds, so the test skips otherwise.
"""

from common import *
import threading


def _run_query_store_result(conn, cmd, result_list):
    """Execute a command on `conn` and stash the reply (or exception) for the caller."""
    try:
        result_list.append(conn.execute_command(*cmd))
    except Exception as e:
        result_list.append(e)


def test_lock_free_reads_writer_progresses_while_cursor_held():
    # WORKERS 1 so the aggregate runs on a worker thread, leaving the main thread free to
    # service SYNC_POINT / the concurrent write while the query is parked.
    env = Env(moduleArgs='WORKERS 1 _LOCK_FREE_READS true DEFAULT_DIALECT 2')
    skipIfNoEnableAssert(env)

    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'body', 'TEXT').ok()
    num_docs = 200
    for i in range(num_docs):
        conn.execute_command('HSET', f'doc:{i}', 'body', 'term')
    # HSET indexing is synchronous (Document_AddToIndexes runs inline in the keyspace
    # notification), so all num_docs are indexed once this loop returns and the cursor's
    # snapshot below captures exactly them.

    sp = 'AfterIteratorCreate'
    # Control connection: drives the sync point and issues the concurrent writer. Kept
    # separate from the reader connection (which will block at the sync point).
    ctl = env.getConnection()
    ctl.execute_command(debug_cmd(), 'SYNC_POINT', 'CLEAR')
    ctl.execute_command(debug_cmd(), 'SYNC_POINT', 'ARM', sp)

    try:
        # Start a cursor aggregate on a background thread; it parks at AfterIteratorCreate
        # (snapshot taken, read lock released because _LOCK_FREE_READS + cursor).
        reader_conn = env.getConnection()
        query_result = []
        cmd = ['FT.AGGREGATE', 'idx', 'term',
               'GROUPBY', 0, 'REDUCE', 'COUNT', 0, 'AS', 'n',
               'WITHCURSOR', 'COUNT', 1000]
        t = threading.Thread(target=_run_query_store_result,
                             args=(reader_conn, cmd, query_result), daemon=True)
        t.start()

        # Deterministically wait until the reader is parked at the sync point.
        wait_for_condition(
            lambda: (ctl.execute_command(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sp) == 1, {}),
            'Timeout waiting for the cursor query to reach AfterIteratorCreate')

        # The query is holding a cursor mid-execution. Add a NEW matching doc: indexing
        # runs inline in the keyspace notification and needs the spec WRITE lock. With
        # _LOCK_FREE_READS the read lock is already released, so the HSET completes
        # immediately; if the lock were still held the (single) main thread would block
        # and the test would deadlock — a regression here surfaces as a timeout.
        # (We use an append rather than FT.ALTER: an append only extends the live index
        # beyond the snapshot, so the reader's point-in-time view is unaffected and the
        # count stays deterministic. FT.ALTER concurrently mutates the index the reader is
        # scanning, which makes the counted total racy — see git history.)
        env.assertEqual(conn.execute_command('HSET', f'doc:{num_docs}', 'body', 'term'), 1)

        # ...and the reader is still parked: the writer made progress *while* the cursor
        # was held, which is only possible because the read lock was released.
        env.assertEqual(ctl.execute_command(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', sp), 1)

        # Release the reader and let it complete.
        ctl.execute_command(debug_cmd(), 'SYNC_POINT', 'SIGNAL', sp)
        t.join(timeout=30)
        env.assertFalse(t.is_alive(), message='cursor query did not finish after SIGNAL')

        # The cursor's snapshot is point-in-time (taken before the HSET), so the newly
        # added matching doc must NOT be counted — appends are absorbed by the COW
        # snapshot (append-immunity). The count is therefore exactly the original set,
        # deterministically. A count of num_docs+1 would mean the reader re-snapshotted
        # and saw the concurrent append; anything less means it dropped snapshot results.
        env.assertEqual(len(query_result), 1)
        res = query_result[0]
        env.assertFalse(isinstance(res, Exception), message=f'query failed: {res}')
        env.assertContains(str(num_docs), str(res))
    finally:
        ctl.execute_command(debug_cmd(), 'SYNC_POINT', 'CLEAR')
