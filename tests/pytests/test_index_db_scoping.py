# -*- coding: utf-8 -*-

import redis
from common import *


# Indexes used to be restricted to DB 0. They can now be created on any logical
# DB and are bound to it (sp->dbid): the background scan, keyspace notifications,
# query resolution, FLUSHDB and RDB persistence are all routed by that DB. This
# file consolidates every test that exercises that per-DB scoping.
#
# None of this is meaningful in cluster mode (a single logical DB), so every test
# here skips it.
#
# Connections opened with _conn_on_db() are always used inside a `with` block so
# they are closed even if an assertion raises mid-test - a leaked connection
# selected on a non-default DB would otherwise pollute later tests. The env's own
# connection (getConnectionByEnv) is owned by RLTest and is never closed here.


def _conn_on_db(env, db):
    """Open a fresh connection selected on logical DB `db`. The returned
    redis.Redis is a context manager; use it in a `with` block so it is closed
    deterministically."""
    port = getConnectionByEnv(env).connection_pool.connection_kwargs['port']
    return redis.Redis(host='localhost', port=port, db=db, decode_responses=True)


def _wait_for_index(conn, idx, timeout=30):
    """Poll FT.INFO over `conn` (so it targets the DB the connection is on) until
    the background scan finished. The common.py waitForIndex helper is bound to
    the env's DB-0 connection, which is not what we want for an index on DB N."""
    end = time.time() + timeout
    while time.time() < end:
        info = conn.execute_command('FT.INFO', idx)
        try:
            indexing = info[info.index('indexing') + 1]
        except (ValueError, TypeError):
            indexing = info['indexing']
        if int(indexing) == 0:
            return
        time.sleep(0.05)
    raise TimeoutError(f"index {idx} did not finish indexing in {timeout}s")


# =============================================================================
# Creation & query on a non-default DB
# =============================================================================

@skip(cluster=True)
def testCreateAndQueryIndexOnOtherDb(env):
    """An index created on DB 14 indexes and serves documents written on DB 14."""
    with _conn_on_db(env, 14) as db14:
        db14.execute_command('FT.CREATE', 'idx14', 'SCHEMA', 't', 'TEXT')
        db14.execute_command('HSET', 'doc1', 't', 'hello world')
        db14.execute_command('HSET', 'doc2', 't', 'goodbye world')

        res = db14.execute_command('FT.SEARCH', 'idx14', 'world', 'NOCONTENT')
        env.assertEqual(toSortedFlatList(res), toSortedFlatList([2, 'doc1', 'doc2']), message=res)


# =============================================================================
# Q1 - background indexing only indexes keys from its origin DB
# =============================================================================

@skip(cluster=True)
def testInitialScanRunsOnIndexDb(env):
    """Documents that already exist on DB 14 before FT.CREATE are picked up by the
    background initial scan, which must run against DB 14 (not DB 0)."""
    with _conn_on_db(env, 14) as db14:
        db14.execute_command('HSET', 'pre1', 't', 'alpha beta')
        db14.execute_command('HSET', 'pre2', 't', 'beta gamma')
        db14.execute_command('FT.CREATE', 'idxscan', 'SCHEMA', 't', 'TEXT')

        _wait_for_index(db14, 'idxscan')
        res = db14.execute_command('FT.SEARCH', 'idxscan', 'beta', 'NOCONTENT')
        env.assertEqual(toSortedFlatList(res), toSortedFlatList([2, 'pre1', 'pre2']), message=res)


@skip(cluster=True)
def testInitialScanIgnoresOtherDbKeys(env):
    """The initial background scan must only walk the keyspace of the index's own
    DB. Matching keys that exist on DB 0 must NOT be picked up by an index created
    on DB 14, even though they would match its schema/prefix."""
    conn0 = getConnectionByEnv(env)  # DB 0, env-owned
    with _conn_on_db(env, 14) as db14:
        # Decoys on DB 0 that match the same schema/value as the real DB-14 docs.
        conn0.execute_command('HSET', 'shared1', 't', 'beta')
        conn0.execute_command('HSET', 'shared2', 't', 'beta')
        # Real docs on DB 14, present before the index is created.
        db14.execute_command('HSET', 'real1', 't', 'beta')
        db14.execute_command('HSET', 'real2', 't', 'beta')

        db14.execute_command('FT.CREATE', 'idxscan', 'SCHEMA', 't', 'TEXT')
        _wait_for_index(db14, 'idxscan')

        # Only the DB-14 docs are indexed; the DB-0 decoys are invisible.
        res = db14.execute_command('FT.SEARCH', 'idxscan', 'beta', 'NOCONTENT')
        env.assertEqual(toSortedFlatList(res), toSortedFlatList([2, 'real1', 'real2']), message=res)


@skip(cluster=True)
def testNotificationsAreScopedPerDb(env):
    """A write on DB N must only feed indexes bound to DB N. Two indexes with the
    same PREFIX but on different DBs must not see each other's documents."""
    conn0 = getConnectionByEnv(env)  # DB 0, env-owned
    with _conn_on_db(env, 14) as db14:
        conn0.execute_command('FT.CREATE', 'idx0', 'PREFIX', '1', 'p:', 'SCHEMA', 't', 'TEXT')
        db14.execute_command('FT.CREATE', 'idx14', 'PREFIX', '1', 'p:', 'SCHEMA', 't', 'TEXT')

        conn0.execute_command('HSET', 'p:0', 't', 'on db zero')
        db14.execute_command('HSET', 'p:14', 't', 'on db fourteen')

        res0 = conn0.execute_command('FT.SEARCH', 'idx0', '*', 'NOCONTENT')
        env.assertEqual(toSortedFlatList(res0), toSortedFlatList([1, 'p:0']), message=res0)

        res14 = db14.execute_command('FT.SEARCH', 'idx14', '*', 'NOCONTENT')
        env.assertEqual(toSortedFlatList(res14), toSortedFlatList([1, 'p:14']), message=res14)


@skip(cluster=True)
def testDeleteIsScopedPerDb(env):
    """A DEL on DB 0 must not evict a same-named document from an index on DB 14.
    Delete notifications are routed by the DB the event fired on."""
    conn0 = getConnectionByEnv(env)  # DB 0, env-owned
    with _conn_on_db(env, 14) as db14:
        conn0.execute_command('FT.CREATE', 'idx0', 'SCHEMA', 't', 'TEXT')
        db14.execute_command('FT.CREATE', 'idx14', 'SCHEMA', 't', 'TEXT')

        # Same key name on both DBs.
        conn0.execute_command('HSET', 'doc', 't', 'value on zero')
        db14.execute_command('HSET', 'doc', 't', 'value on fourteen')

        # Delete on DB 0 only.
        conn0.execute_command('DEL', 'doc')

        # DB-0 index lost it...
        res0 = conn0.execute_command('FT.SEARCH', 'idx0', '*', 'NOCONTENT')
        env.assertEqual(toSortedFlatList(res0), toSortedFlatList([0]), message=res0)
        # ...but the DB-14 index still has its own doc.
        res14 = db14.execute_command('FT.SEARCH', 'idx14', '*', 'NOCONTENT')
        env.assertEqual(toSortedFlatList(res14), toSortedFlatList([1, 'doc']), message=res14)


@skip(cluster=True)
def testExpiryIsScopedPerDb(env):
    """A key expiring on DB 0 must not evict a same-named document from an index on
    DB 14. The 'expired' notification is routed by the DB it fired on."""
    conn0 = getConnectionByEnv(env)  # DB 0, env-owned
    with _conn_on_db(env, 14) as db14:
        conn0.execute_command('FT.CREATE', 'idx0', 'SCHEMA', 't', 'TEXT')
        db14.execute_command('FT.CREATE', 'idx14', 'SCHEMA', 't', 'TEXT')

        conn0.execute_command('HSET', 'doc', 't', 'value on zero')
        db14.execute_command('HSET', 'doc', 't', 'value on fourteen')

        # Expire the DB-0 key, then wait for the condition we actually assert: the
        # key is gone AND the resulting eviction notification has cleared it from
        # the DB-0 index. Reading EXISTS also drives lazy expiration on access.
        conn0.execute_command('PEXPIRE', 'doc', 1)

        def _db0_doc_evicted():
            exists = conn0.execute_command('EXISTS', 'doc')
            hits = conn0.execute_command('FT.SEARCH', 'idx0', '*', 'NOCONTENT')[0]
            return exists == 0 and hits == 0, {'exists': exists, 'idx0_hits': hits}

        wait_for_condition(_db0_doc_evicted,
                           'doc on DB 0 was not expired and evicted from idx0', timeout=10)

        # DB-0 index lost it...
        res0 = conn0.execute_command('FT.SEARCH', 'idx0', '*', 'NOCONTENT')
        env.assertEqual(toSortedFlatList(res0), toSortedFlatList([0]), message=res0)
        # ...but the DB-14 doc (no TTL) is untouched.
        res14 = db14.execute_command('FT.SEARCH', 'idx14', '*', 'NOCONTENT')
        env.assertEqual(toSortedFlatList(res14), toSortedFlatList([1, 'doc']), message=res14)


@skip(cluster=True, no_json=True)
def testJsonNotificationsAreScopedPerDb(env):
    """JSON documents are routed per DB exactly like HASH documents: a JSON.SET on
    DB 0 only feeds the DB-0 index, not a same-prefix index on DB 14."""
    conn0 = getConnectionByEnv(env)  # DB 0, env-owned
    with _conn_on_db(env, 14) as db14:
        conn0.execute_command('FT.CREATE', 'jidx0', 'ON', 'JSON', 'PREFIX', '1', 'j:',
                              'SCHEMA', '$.t', 'AS', 't', 'TEXT')
        db14.execute_command('FT.CREATE', 'jidx14', 'ON', 'JSON', 'PREFIX', '1', 'j:',
                             'SCHEMA', '$.t', 'AS', 't', 'TEXT')

        conn0.execute_command('JSON.SET', 'j:0', '$', '{"t":"on db zero"}')
        db14.execute_command('JSON.SET', 'j:14', '$', '{"t":"on db fourteen"}')

        res0 = conn0.execute_command('FT.SEARCH', 'jidx0', '*', 'NOCONTENT')
        env.assertEqual(toSortedFlatList(res0), toSortedFlatList([1, 'j:0']), message=res0)
        res14 = db14.execute_command('FT.SEARCH', 'jidx14', '*', 'NOCONTENT')
        env.assertEqual(toSortedFlatList(res14), toSortedFlatList([1, 'j:14']), message=res14)


@skip(cluster=True)
def testUpdateOnOtherDbDoesNotMutateIndexedDoc(env):
    """Updating a same-named key on DB 0 must not change the value indexed for the
    DB-14 document. Each DB's index reflects only its own DB's content."""
    conn0 = getConnectionByEnv(env)  # DB 0, env-owned
    with _conn_on_db(env, 14) as db14:
        conn0.execute_command('FT.CREATE', 'idx0', 'SCHEMA', 't', 'TEXT')
        db14.execute_command('FT.CREATE', 'idx14', 'SCHEMA', 't', 'TEXT')

        conn0.execute_command('HSET', 'doc', 't', 'original')
        db14.execute_command('HSET', 'doc', 't', 'fourteen')

        # Update the DB-0 doc; the DB-14 index must be unaffected.
        conn0.execute_command('HSET', 'doc', 't', 'changed')

        # DB-14 index still matches its own value, not DB 0's old or new value.
        env.assertEqual(db14.execute_command('FT.SEARCH', 'idx14', 'fourteen', 'NOCONTENT'),
                        [1, 'doc'])
        env.assertEqual(toSortedFlatList(db14.execute_command('FT.SEARCH', 'idx14', 'changed', 'NOCONTENT')),
                        toSortedFlatList([0]))


@skip(cluster=True)
def testMoveBetweenDbsUpdatesIndexes(env):
    """MOVE relocates a key between logical DBs and RediSearch keeps both DBs'
    indexes consistent: the 'move_from' event (source DB) removes the doc from the
    source index, and 'move_to' (destination DB) adds it to the destination index.

    The events carry no DB argument; routing relies entirely on the DB Redis
    selects on the notification context - source DB for move_from, destination DB
    for move_to (RedisModule_GetSelectedDb in indexes.c). A bystander index on a
    third DB (same prefix) must therefore stay empty throughout: the move only
    touches the source and destination DBs, never broadcasts.
    """
    conn0 = getConnectionByEnv(env)  # DB 0, env-owned
    with _conn_on_db(env, 14) as db14, _conn_on_db(env, 9) as db9:
        conn0.execute_command('FT.CREATE', 'idx0', 'PREFIX', '1', 'k:', 'SCHEMA', 't', 'TEXT')
        db14.execute_command('FT.CREATE', 'idx14', 'PREFIX', '1', 'k:', 'SCHEMA', 't', 'TEXT')
        # Bystander index on an uninvolved DB; must never see the moved doc.
        db9.execute_command('FT.CREATE', 'idx9', 'PREFIX', '1', 'k:', 'SCHEMA', 't', 'TEXT')

        def assert_only(idx_conn_with_doc):
            # Exactly the named (conn, idx) holds the doc; the other two are empty.
            for conn, idx in ((conn0, 'idx0'), (db14, 'idx14'), (db9, 'idx9')):
                expected = [1, 'k:x'] if (conn, idx) == idx_conn_with_doc else [0]
                res = conn.execute_command('FT.SEARCH', idx, 'beta', 'NOCONTENT')
                env.assertEqual(toSortedFlatList(res), toSortedFlatList(expected), message=(idx, res))

        # Doc starts on DB 0 and is indexed by idx0 only.
        conn0.execute_command('HSET', 'k:x', 't', 'beta')
        assert_only((conn0, 'idx0'))

        # MOVE it to DB 14: move_from clears idx0, move_to indexes it on idx14;
        # idx9 (DB 9) is untouched.
        conn0.execute_command('MOVE', 'k:x', 14)
        env.assertEqual(db14.execute_command('EXISTS', 'k:x'), 1)
        assert_only((db14, 'idx14'))

        # MOVE it back to DB 0: the indexes swap roles again; idx9 still empty.
        db14.execute_command('MOVE', 'k:x', 0)
        assert_only((conn0, 'idx0'))


@skip(cluster=True)
def testSwapdbMovesIndexWithItsData(env):
    """SWAPDB swaps two DBs' keyspaces in bulk. An index follows its data to the
    other DB: after SWAPDB 1 2 an index created on DB 1 is queryable on DB 2 (and
    gone from DB 1), still serving the same documents, and new writes on DB 2 feed
    it."""
    with _conn_on_db(env, 1) as db1, _conn_on_db(env, 2) as db2:
        db1.execute_command('FT.CREATE', 'idx', 'PREFIX', '1', 'k:', 'SCHEMA', 't', 'TEXT')
        db1.execute_command('HSET', 'k:1', 't', 'hello')
        env.assertEqual(db1.execute_command('FT.SEARCH', 'idx', 'hello', 'NOCONTENT'),
                        [1, 'k:1'])

        # SWAPDB: the index (and its doc) move from DB 1 to DB 2.
        db1.execute_command('SWAPDB', 1, 2)

        # Gone from DB 1, present and serving on DB 2.
        env.assertNotContains('idx', db1.execute_command('FT._LIST'))
        env.assertRaises(redis.ResponseError, db1.execute_command, 'FT.SEARCH', 'idx', 'hello')
        env.assertContains('idx', db2.execute_command('FT._LIST'))
        env.assertEqual(db2.execute_command('FT.SEARCH', 'idx', 'hello', 'NOCONTENT'),
                        [1, 'k:1'])

        # New write on DB 2 (where the index now lives) is indexed.
        db2.execute_command('HSET', 'k:2', 't', 'world')
        env.assertEqual(toSortedFlatList(db2.execute_command('FT.SEARCH', 'idx', '*', 'NOCONTENT')),
                        toSortedFlatList([2, 'k:1', 'k:2']))


@skip(cluster=True)
def testSwapdbSwapsSameNameIndexesSymmetrically(env):
    """When both swapped DBs hold a same-named index, SWAPDB swaps them as a clean
    permutation (the registry re-key must not transiently collide). Each name
    resolves on the other DB afterwards, serving its original documents."""
    with _conn_on_db(env, 1) as db1, _conn_on_db(env, 2) as db2:
        db1.execute_command('FT.CREATE', 'idx', 'PREFIX', '1', 'k:', 'SCHEMA', 't', 'TEXT')
        db2.execute_command('FT.CREATE', 'idx', 'PREFIX', '1', 'k:', 'SCHEMA', 't', 'TEXT')
        db1.execute_command('HSET', 'k:one', 't', 'fromone')
        db2.execute_command('HSET', 'k:two', 't', 'fromtwo')

        db1.execute_command('SWAPDB', 1, 2)

        # The 'idx' reachable from DB 1 now holds what was DB 2's doc, and vice versa.
        env.assertEqual(db1.execute_command('FT.SEARCH', 'idx', '*', 'NOCONTENT'), [1, 'k:two'])
        env.assertEqual(db2.execute_command('FT.SEARCH', 'idx', '*', 'NOCONTENT'), [1, 'k:one'])


@skip(cluster=True)
def testSwapdbThenNotificationsAndReload(env):
    """After SWAPDB, keyspace notifications follow the swapped index, and the new
    binding survives an RDB reload."""
    with _conn_on_db(env, 1) as db1, _conn_on_db(env, 2) as db2:
        db1.execute_command('FT.CREATE', 'idx', 'PREFIX', '1', 'k:', 'SCHEMA', 't', 'TEXT')
        db1.execute_command('HSET', 'k:1', 't', 'alpha')

        db1.execute_command('SWAPDB', 1, 2)

        # DEL on DB 2 (new home) evicts from the index; a write adds to it.
        db2.execute_command('DEL', 'k:1')
        env.assertEqual(toSortedFlatList(db2.execute_command('FT.SEARCH', 'idx', '*', 'NOCONTENT')),
                        toSortedFlatList([0]))
        db2.execute_command('HSET', 'k:2', 't', 'beta')
        env.assertEqual(db2.execute_command('FT.SEARCH', 'idx', 'beta', 'NOCONTENT'), [1, 'k:2'])

        # The DB-2 binding persists across reload.
        env.dumpAndReload()
        _wait_for_index(db2, 'idx')
        env.assertContains('idx', db2.execute_command('FT._LIST'))
        env.assertNotContains('idx', db1.execute_command('FT._LIST'))
        env.assertEqual(db2.execute_command('FT.SEARCH', 'idx', 'beta', 'NOCONTENT'), [1, 'k:2'])


@skip(cluster=True)
def testSwapdbMidScanCancelsAndRestarts(env):
    """If SWAPDB fires while an index's initial scan is still in progress, the
    stale scan (which selected the pre-swap DB) is cancelled and a fresh scan is
    run against the new DB, so the index ends up fully and correctly indexed.

    Uses the BG_SCAN_CONTROLLER debug hook to hold the scan in a paused, in-
    progress state across the SWAPDB. The debug status command resolves the index
    on the caller's selected DB, so it is issued on the connection for the DB the
    index currently lives on (DB 1 before the swap, DB 2 after)."""
    def scanner_status(conn, idx):
        return conn.execute_command(debug_cmd(), 'BG_SCAN_CONTROLLER',
                                    'GET_DEBUG_SCANNER_STATUS', idx)

    n = 200
    with _conn_on_db(env, 1) as db1, _conn_on_db(env, 2) as db2:
        for i in range(n):
            db1.execute_command('HSET', f'k:{i}', 't', 'v')

        # Pause before the scan starts: the scanner is created (scan_in_progress)
        # but has scanned nothing when SWAPDB fires.
        db1.execute_command(debug_cmd(), 'BG_SCAN_CONTROLLER', 'SET_PAUSE_BEFORE_SCAN', 'true')
        db1.execute_command('FT.CREATE', 'idx', 'PREFIX', '1', 'k:', 'SCHEMA', 't', 'TEXT')
        with TimeLimit(30, 'scan did not reach the paused (NEW) state on DB 1'):
            while scanner_status(db1, 'idx') != 'NEW':
                time.sleep(0.05)

        # SWAPDB while the scan is in progress: Phase 4 must cancel this scanner
        # and restart indexing on DB 2 (where the documents now live).
        db1.execute_command('SWAPDB', 1, 2)

        # Stop pausing future scans, then resume; the restarted scan runs on DB 2.
        db1.execute_command(debug_cmd(), 'BG_SCAN_CONTROLLER', 'SET_PAUSE_BEFORE_SCAN', 'false')
        db1.execute_command(debug_cmd(), 'BG_SCAN_CONTROLLER', 'SET_BG_INDEX_RESUME')

        # The index now lives on DB 2 and must contain ALL the documents - proving
        # the restarted scan walked the new DB to completion, not the stale one.
        _wait_for_index(db2, 'idx')
        res = db2.execute_command('FT.SEARCH', 'idx', '*', 'LIMIT', '0', '0')
        env.assertEqual(res[0], n, message=res)
        env.assertNotContains('idx', db1.execute_command('FT._LIST'))


# =============================================================================
# Q2 - queries only work on the selected DB
# =============================================================================

@skip(cluster=True)
def testIndexNotVisibleFromOtherDb(env):
    """An index created on DB 14 must not be visible/queryable from DB 0: lookups
    are scoped to the connection's selected DB."""
    conn0 = getConnectionByEnv(env)  # DB 0, env-owned
    with _conn_on_db(env, 14) as db14:
        db14.execute_command('FT.CREATE', 'idx14', 'SCHEMA', 't', 'TEXT')
        db14.execute_command('HSET', 'doc1', 't', 'hello world')

        # Visible and queryable from DB 14.
        env.assertContains('idx14', db14.execute_command('FT._LIST'))
        res = db14.execute_command('FT.SEARCH', 'idx14', 'world', 'NOCONTENT')
        env.assertEqual(toSortedFlatList(res), toSortedFlatList([1, 'doc1']), message=res)

        # Invisible from DB 0: FT.SEARCH and FT.INFO must report "no such index".
        env.assertRaises(redis.ResponseError, conn0.execute_command, 'FT.SEARCH', 'idx14', 'world')
        env.assertRaises(redis.ResponseError, conn0.execute_command, 'FT.INFO', 'idx14')
        # ...and the index name is not listed on DB 0.
        env.assertNotContains('idx14', conn0.execute_command('FT._LIST'))


@skip(cluster=True)
def testReadAdminCommandsCrossDbError(env):
    """Every per-index command must fail with "no such index" when issued from a DB
    that does not own the index. Covers the common read/admin surface beyond
    FT.SEARCH/FT.INFO."""
    conn0 = getConnectionByEnv(env)  # DB 0, env-owned
    with _conn_on_db(env, 14) as db14:
        db14.execute_command('FT.CREATE', 'idx14', 'SCHEMA', 't', 'TEXT', 'tg', 'TAG')
        db14.execute_command('HSET', 'doc1', 't', 'hello world', 'tg', 'a')

        cross_db_commands = [
            ('FT.AGGREGATE', 'idx14', '*'),
            ('FT.DROPINDEX', 'idx14'),
            ('FT.ALTER', 'idx14', 'SCHEMA', 'ADD', 'n', 'NUMERIC'),
            ('FT.TAGVALS', 'idx14', 'tg'),
            ('FT.EXPLAIN', 'idx14', '*'),
            ('FT.PROFILE', 'idx14', 'SEARCH', 'QUERY', '*'),
            ('FT.SPELLCHECK', 'idx14', 'hello'),
            ('FT.ALIASADD', 'somealias', 'idx14'),
        ]
        for cmd in cross_db_commands:
            env.assertRaises(redis.ResponseError, conn0.execute_command, *cmd)

        # The index is untouched and still serves DB 14 (e.g. FT.ALTER did not apply).
        env.assertContains('idx14', db14.execute_command('FT._LIST'))


@skip(cluster=True)
def testSameNameDifferentDbsAreIndependent(env):
    """The same index name can exist on two DBs as two independent indexes (the
    registry is keyed by (db, name)), each serving only its own DB's documents."""
    conn0 = getConnectionByEnv(env)  # DB 0, env-owned
    with _conn_on_db(env, 14) as db14:
        conn0.execute_command('FT.CREATE', 'shared', 'PREFIX', '1', 'k:', 'SCHEMA', 't', 'TEXT')
        db14.execute_command('FT.CREATE', 'shared', 'PREFIX', '1', 'k:', 'SCHEMA', 't', 'TEXT')

        conn0.execute_command('HSET', 'k:a', 't', 'zero db doc')
        db14.execute_command('HSET', 'k:b', 't', 'fourteen db doc')

        # Each 'shared' index only sees its own DB's documents.
        res0 = conn0.execute_command('FT.SEARCH', 'shared', '*', 'NOCONTENT')
        env.assertEqual(toSortedFlatList(res0), toSortedFlatList([1, 'k:a']), message=res0)
        res14 = db14.execute_command('FT.SEARCH', 'shared', '*', 'NOCONTENT')
        env.assertEqual(toSortedFlatList(res14), toSortedFlatList([1, 'k:b']), message=res14)

        # Dropping 'shared' on DB 14 leaves the DB-0 'shared' intact.
        db14.execute_command('FT.DROPINDEX', 'shared')
        env.assertEqual(conn0.execute_command('FT._LIST'), ['shared'])
        res0 = conn0.execute_command('FT.SEARCH', 'shared', '*', 'NOCONTENT')
        env.assertEqual(toSortedFlatList(res0), toSortedFlatList([1, 'k:a']), message=res0)


@skip(cluster=True)
def testContentLoadAndCursorOnOtherDb(env):
    """Returning document content (not NOCONTENT) and cursor reads both open the
    document keys; on DB 14 they must open them on DB 14. Exercises the query
    content-load path and the detached cursor-read context."""
    with _conn_on_db(env, 14) as db14:
        db14.execute_command('FT.CREATE', 'idxc', 'SCHEMA', 't', 'TEXT', 'n', 'NUMERIC')
        for i in range(5):
            db14.execute_command('HSET', f'doc{i}', 't', f'word{i}', 'n', i)

        # FT.SEARCH returning content must load field values from DB 14.
        res = db14.execute_command('FT.SEARCH', 'idxc', 'word3')
        env.assertEqual(res[0], 1, message=res)
        env.assertEqual(res[1], 'doc3', message=res)
        env.assertContains('word3', res[2])

        # FT.AGGREGATE WITHCURSOR + FT.CURSOR READ (detached read context on DB 14).
        rows, cursor = db14.execute_command(
            'FT.AGGREGATE', 'idxc', '*', 'LOAD', '1', '@t', 'WITHCURSOR', 'COUNT', '2')
        seen = max(0, len(rows) - 1)
        while cursor:
            more, cursor = db14.execute_command('FT.CURSOR', 'READ', 'idxc', str(cursor))
            seen += max(0, len(more) - 1)
        env.assertEqual(seen, 5, message=f'cursor returned {seen} rows')


@skip(cluster=True)
def testContentLoadUsesSelectedDb(env):
    """When a doc key with the same name exists on two DBs with different field
    values, FT.SEARCH-with-content on the DB-14 index must load the DB-14 value,
    never DB 0's. Guards against the content-load path reading from the wrong DB."""
    conn0 = getConnectionByEnv(env)  # DB 0, env-owned
    with _conn_on_db(env, 14) as db14:
        conn0.execute_command('FT.CREATE', 'idx0', 'SCHEMA', 't', 'TEXT')
        db14.execute_command('FT.CREATE', 'idx14', 'SCHEMA', 't', 'TEXT')

        # Same key name, different values per DB.
        conn0.execute_command('HSET', 'doc', 't', 'zero_value')
        db14.execute_command('HSET', 'doc', 't', 'fourteen_value')

        res = db14.execute_command('FT.SEARCH', 'idx14', '*')
        env.assertEqual(res[0], 1, message=res)
        env.assertEqual(res[1], 'doc', message=res)
        # Content comes from DB 14, not DB 0.
        fields = {res[2][i]: res[2][i + 1] for i in range(0, len(res[2]), 2)}
        env.assertEqual(fields['t'], 'fourteen_value', message=res)


@skip(cluster=True)
def testSelectSwitchingOnSingleConnection(env):
    """Index resolution follows the connection's currently-selected DB, switched at
    runtime via SELECT - not an artifact of using separate connection pools."""
    with _conn_on_db(env, 14) as conn:
        conn.execute_command('FT.CREATE', 'idx14', 'SCHEMA', 't', 'TEXT')
        conn.execute_command('HSET', 'doc1', 't', 'hello world')
        env.assertEqual(conn.execute_command('FT.SEARCH', 'idx14', 'world', 'NOCONTENT'),
                        [1, 'doc1'])

        # Switch the SAME connection to DB 0: the index is no longer visible.
        conn.execute_command('SELECT', 0)
        env.assertRaises(redis.ResponseError, conn.execute_command, 'FT.SEARCH', 'idx14', 'world')

        # Switch back to DB 14: visible again.
        conn.execute_command('SELECT', 14)
        env.assertEqual(conn.execute_command('FT.SEARCH', 'idx14', 'world', 'NOCONTENT'),
                        [1, 'doc1'])


@skip(cluster=True)
def testAliasResolutionIsScopedToTargetDb(env):
    """Aliases live in a single global table (so the alias namespace is shared
    across DBs), but resolution scopes the resolved spec to the caller's DB: an
    alias whose target index lives on DB 14 resolves from DB 14 and reports
    "no such index" from DB 0 (indexes.c, sp->dbid != options->db)."""
    conn0 = getConnectionByEnv(env)  # DB 0, env-owned
    with _conn_on_db(env, 14) as db14:
        conn0.execute_command('FT.CREATE', 'real0', 'SCHEMA', 't', 'TEXT')
        db14.execute_command('FT.CREATE', 'real14', 'SCHEMA', 't', 'TEXT')
        conn0.execute_command('HSET', 'd0', 't', 'zero')
        db14.execute_command('HSET', 'd14', 't', 'fourteen')

        db14.execute_command('FT.ALIASADD', 'a14', 'real14')

        # Resolves to its target index from the target's own DB...
        env.assertEqual(db14.execute_command('FT.SEARCH', 'a14', '*', 'NOCONTENT'), [1, 'd14'])
        # ...but is "no such index" from DB 0, where the resolved spec does not belong.
        env.assertRaises(redis.ResponseError, conn0.execute_command, 'FT.SEARCH', 'a14', '*')

        # The alias namespace is global: the same alias name cannot be reused for a
        # DB-0 index while it is taken by the DB-14 index.
        env.assertRaises(redis.ResponseError, conn0.execute_command, 'FT.ALIASADD', 'a14', 'real0')


@skip(cluster=True)
def testCursorReadFromWrongDb(env):
    """A cursor opened by FT.AGGREGATE on DB 14 must not be readable from DB 0:
    FT.CURSOR READ resolves the index by name on the caller's selected DB."""
    conn0 = getConnectionByEnv(env)  # DB 0, env-owned
    with _conn_on_db(env, 14) as db14:
        db14.execute_command('FT.CREATE', 'idxc', 'SCHEMA', 't', 'TEXT')
        for i in range(5):
            db14.execute_command('HSET', f'doc{i}', 't', f'word{i}')

        _rows, cursor = db14.execute_command(
            'FT.AGGREGATE', 'idxc', '*', 'WITHCURSOR', 'COUNT', '2')
        env.assertTrue(cursor != 0, message=f'expected an open cursor, got {cursor}')

        # Reading that cursor from DB 0 (where 'idxc' does not exist) must error.
        env.assertRaises(redis.ResponseError, conn0.execute_command,
                         'FT.CURSOR', 'READ', 'idxc', str(cursor))

        # The cursor is still readable from its own DB.
        more, cursor = db14.execute_command('FT.CURSOR', 'READ', 'idxc', str(cursor))
        env.assertTrue(len(more) >= 1, message=more)


@skip(cluster=True)
def testCursorReadFromWrongDbSameIndexName(env):
    """The hard case: an index with the SAME name exists on both DB 0 and DB 14.
    The DB-0 index passes the by-name ACL/index lookup, but a cursor opened on
    DB 14 still belongs to DB 14 and must not be drained, deleted, or profiled
    from DB 0 - the cursor id is global, so only the cursor's own DB binding
    keeps the two logical DBs isolated."""
    conn0 = getConnectionByEnv(env)  # DB 0, env-owned
    conn0.execute_command('FT.CREATE', 'idxc', 'SCHEMA', 't', 'TEXT')
    with _conn_on_db(env, 14) as db14:
        db14.execute_command('FT.CREATE', 'idxc', 'SCHEMA', 't', 'TEXT')
        for i in range(5):
            db14.execute_command('HSET', f'doc{i}', 't', f'word{i}')

        _rows, cursor = db14.execute_command(
            'FT.AGGREGATE', 'idxc', '*', 'WITHCURSOR', 'COUNT', '2')
        env.assertTrue(cursor != 0, message=f'expected an open cursor, got {cursor}')

        # READ / PROFILE / DEL of the DB-14 cursor from DB 0 must all fail, even
        # though 'idxc' resolves on DB 0.
        env.assertRaises(redis.ResponseError, conn0.execute_command,
                         'FT.CURSOR', 'READ', 'idxc', str(cursor))
        env.assertRaises(redis.ResponseError, conn0.execute_command,
                         'FT.CURSOR', 'DEL', 'idxc', str(cursor))

        # None of the above drained or freed the cursor: it is still fully
        # usable from its own DB.
        more, cursor = db14.execute_command('FT.CURSOR', 'READ', 'idxc', str(cursor))
        env.assertTrue(len(more) >= 1, message=more)

        # DEL from the right DB still works.
        env.assertEqual(db14.execute_command('FT.CURSOR', 'DEL', 'idxc', str(cursor)), 'OK')


# =============================================================================
# FLUSHDB / FLUSHALL scoping
# =============================================================================

@skip(cluster=True)
def testFlushdbOnIndexDbDropsIndex(env):
    """A FLUSHDB on the DB an index lives in must drop the index, just like
    FLUSHALL: flushing it removes both the documents and the index."""
    conn = getConnectionByEnv(env)  # DB 0, env-owned

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    conn.execute_command('HSET', 'doc1', 't', 'hello world')
    env.assertContains('idx', env.cmd('FT._LIST'))

    conn.execute_command('FLUSHDB')
    env.assertEqual(env.cmd('FT._LIST'), [])
    env.expect('FT.SEARCH', 'idx', 'world').error().contains('idx')


@skip(cluster=True)
def testFlushdbOnOtherDbKeepsIndex(env):
    """A FLUSHDB on a logical DB that holds no index must NOT drop indexes living on
    another DB. Regression test for the report that flushing an unrelated DB made
    FT.SEARCH return "no such index" (onFlush filters the event by dbnum)."""
    conn = getConnectionByEnv(env)  # DB 0, env-owned

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    conn.execute_command('HSET', 'doc1', 't', 'hello world')
    conn.execute_command('HSET', 'doc2', 't', 'goodbye world')

    res = env.cmd('FT.SEARCH', 'idx', 'world', 'NOCONTENT')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList([2, 'doc1', 'doc2']), message=res)

    with _conn_on_db(env, 14) as other_db:
        other_db.execute_command('SET', 'unrelated_key', 'value')
        env.assertEqual(other_db.execute_command('DBSIZE'), 1)
        other_db.execute_command('FLUSHDB')
        env.assertEqual(other_db.execute_command('DBSIZE'), 0)

    # The index in DB 0 must still exist and return the same results.
    env.assertContains('idx', env.cmd('FT._LIST'))
    res = env.cmd('FT.SEARCH', 'idx', 'world', 'NOCONTENT')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList([2, 'doc1', 'doc2']), message=res)
    env.assertEqual(conn.execute_command('DBSIZE'), 2)


@skip(cluster=True)
def testFlushdbDropsOnlyIndexesOfThatDb(env):
    """FLUSHDB on DB 14 drops only the index bound to DB 14; a DB-0 index survives.
    FLUSHALL afterwards drops the remaining DB-0 index."""
    conn0 = getConnectionByEnv(env)  # DB 0, env-owned
    with _conn_on_db(env, 14) as db14:
        conn0.execute_command('FT.CREATE', 'idx0', 'SCHEMA', 't', 'TEXT')
        db14.execute_command('FT.CREATE', 'idx14', 'SCHEMA', 't', 'TEXT')
        env.assertEqual(conn0.execute_command('FT._LIST'), ['idx0'])
        env.assertEqual(db14.execute_command('FT._LIST'), ['idx14'])

        # Flush DB 14 only -> idx14 gone, idx0 remains.
        db14.execute_command('FLUSHDB')
        env.assertEqual(db14.execute_command('FT._LIST'), [])
        env.assertEqual(conn0.execute_command('FT._LIST'), ['idx0'])

        # idx0 still serves queries.
        conn0.execute_command('HSET', 'd', 't', 'still here')
        res = conn0.execute_command('FT.SEARCH', 'idx0', 'here', 'NOCONTENT')
        env.assertEqual(toSortedFlatList(res), toSortedFlatList([1, 'd']), message=res)

        # FLUSHALL drops the rest.
        conn0.execute_command('FLUSHALL')
        env.assertEqual(conn0.execute_command('FT._LIST'), [])


@skip(cluster=True)
def testFlushallStillDropsIndex(env):
    """FLUSHALL must still drop indexes, so the DB-filtering fix in onFlush() does
    not accidentally make indexes survive a real flush of their own DB."""
    conn = getConnectionByEnv(env)  # DB 0, env-owned

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    conn.execute_command('HSET', 'doc1', 't', 'hello world')
    env.assertContains('idx', env.cmd('FT._LIST'))

    conn.execute_command('FLUSHALL')
    env.assertEqual(env.cmd('FT._LIST'), [])
    env.expect('FT.SEARCH', 'idx', 'world').error().contains('idx')


# =============================================================================
# RDB persistence of the index's DB binding
# =============================================================================

@skip(cluster=True)
def testIndexDbSurvivesRdbReload(env):
    """The DB an index is bound to is persisted in the RDB and restored on reload."""
    with _conn_on_db(env, 14) as db14:
        db14.execute_command('FT.CREATE', 'idxrdb', 'SCHEMA', 't', 'TEXT')
        db14.execute_command('HSET', 'doc1', 't', 'persisted value')

        env.dumpAndReload()

        # The index is still bound to DB 14: it serves the doc from DB 14...
        _wait_for_index(db14, 'idxrdb')
        res = db14.execute_command('FT.SEARCH', 'idxrdb', 'persisted', 'NOCONTENT')
        env.assertEqual(toSortedFlatList(res), toSortedFlatList([1, 'doc1']), message=res)

        # ...and a write on DB 14 after reload is still indexed (notification routing
        # uses the restored dbid).
        db14.execute_command('HSET', 'doc2', 't', 'persisted again')
        res = db14.execute_command('FT.SEARCH', 'idxrdb', 'persisted', 'NOCONTENT')
        env.assertEqual(toSortedFlatList(res), toSortedFlatList([2, 'doc1', 'doc2']), message=res)


# =============================================================================
# TEMPORARY index expiry on a non-default DB
# =============================================================================

@skip(cluster=True)
def testTemporaryIndexExpiresOnOtherDb(env):
    """A TEMPORARY index created on a non-zero DB must be dropped by its expiry
    timer, together with its documents (the timer drops with the "DD" flag).

    Regression: the timer fires IndexSpec_TimedOutProc, which self-calls
    FT.DROPINDEX through RSDummyContext. That context is pinned to DB 0, but the
    index lives on DB 14 and lookups are DB-scoped, so the drop used to fail with
    "no such index" and leak the index plus its documents past the TTL."""
    with _conn_on_db(env, 14) as db14:
        db14.execute_command('FT.CREATE', 'idxtmp', 'TEMPORARY', '1',
                             'SCHEMA', 't', 'TEXT')
        db14.execute_command('HSET', 'doc1', 't', 'hello world')
        env.assertContains('idxtmp', db14.execute_command('FT._LIST'))

        # Wait for the timer to expire and drop the index. Do not touch the index
        # while waiting (a query would reset its timer); poll FT._LIST instead.
        with TimeLimit(15, 'temporary index was not dropped by its expiry timer'):
            while 'idxtmp' in db14.execute_command('FT._LIST'):
                time.sleep(0.1)

        # The "DD" drop also removed the indexed document from DB 14.
        env.assertEqual(db14.execute_command('FT._LIST'), [])
        env.assertEqual(db14.execute_command('DBSIZE'), 0)


# =============================================================================
# Cluster mode (DB-0 only)
# =============================================================================

@skip(cluster=False)  # cluster-only: the rest of this file is standalone-only
def testClusterIsDb0Only(env):
    """In cluster mode there is a single logical DB. Redis core rejects SELECT to
    any non-zero DB, so every index is necessarily bound to DB 0 and the per-DB
    machinery (sp->dbid) collapses to DB 0. This pins that the feature degrades
    safely: non-zero DBs are unreachable, and index creation on DB 0 still works.

    MOVE (cross-DB) and SWAPDB (swaps two DBs) are likewise meaningless with a
    single DB and are not exercised here."""
    conn = getConnectionByEnv(env)

    # Redis forbids selecting a non-zero DB in cluster mode.
    env.assertRaises(redis.ResponseError, conn.execute_command, 'SELECT', 1)

    # Index creation on the only DB (0) works exactly as before.
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    env.assertContains('idx', env.cmd('FT._LIST'))
