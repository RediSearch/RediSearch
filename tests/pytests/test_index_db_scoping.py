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
def testMoveAndSwapdbInteraction(env):
    """MOVE/SWAPDB move keys between logical DBs but do not fire the keyspace
    notifications RediSearch subscribes to (move_from/move_to are not handled), so
    the index is NOT updated by the move itself. This test documents that behavior:
    a key MOVEd into the indexed DB is only reflected after a subsequent write that
    does fire a handled notification on that DB."""
    conn0 = getConnectionByEnv(env)  # DB 0, env-owned
    with _conn_on_db(env, 14) as db14:
        db14.execute_command('FT.CREATE', 'idx14', 'SCHEMA', 't', 'TEXT')

        # Write a matching key on DB 0 (no index there), then MOVE it into DB 14.
        conn0.execute_command('HSET', 'moved', 't', 'beta')
        conn0.execute_command('MOVE', 'moved', 14)
        env.assertEqual(db14.execute_command('EXISTS', 'moved'), 1)

        # MOVE alone does not index the key (no handled notification fired on DB 14).
        res = db14.execute_command('FT.SEARCH', 'idx14', 'beta', 'NOCONTENT')
        env.assertEqual(toSortedFlatList(res), toSortedFlatList([0]), message=res)

        # A subsequent write on DB 14 fires a handled notification and indexes it.
        db14.execute_command('HSET', 'moved', 't', 'beta')
        res = db14.execute_command('FT.SEARCH', 'idx14', 'beta', 'NOCONTENT')
        env.assertEqual(toSortedFlatList(res), toSortedFlatList([1, 'moved']), message=res)


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
