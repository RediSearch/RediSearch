# -*- coding: utf-8 -*-

import redis
from common import *


# Indexes used to be restricted to DB 0. They can now be created on any logical
# DB and are bound to it (sp->dbid): keyspace notifications, FLUSHDB and the
# background scan are all routed by that DB. These behaviors are not meaningful
# in cluster mode (single logical DB), so every test here skips it.


def _conn_on_db(env, db):
    """Open a fresh connection selected on logical DB `db`."""
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


@skip(cluster=True)
def testCreateAndQueryIndexOnOtherDb(env):
    """An index created on DB 14 indexes and serves documents written on DB 14."""
    db14 = _conn_on_db(env, 14)

    db14.execute_command('FT.CREATE', 'idx14', 'SCHEMA', 't', 'TEXT')
    db14.execute_command('HSET', 'doc1', 't', 'hello world')
    db14.execute_command('HSET', 'doc2', 't', 'goodbye world')

    res = db14.execute_command('FT.SEARCH', 'idx14', 'world', 'NOCONTENT')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList([2, 'doc1', 'doc2']), message=res)

    db14.close()


@skip(cluster=True)
def testInitialScanRunsOnIndexDb(env):
    """Documents that already exist on DB 14 before FT.CREATE are picked up by the
    background initial scan, which must run against DB 14 (not DB 0)."""
    db14 = _conn_on_db(env, 14)

    db14.execute_command('HSET', 'pre1', 't', 'alpha beta')
    db14.execute_command('HSET', 'pre2', 't', 'beta gamma')
    db14.execute_command('FT.CREATE', 'idxscan', 'SCHEMA', 't', 'TEXT')

    _wait_for_index(db14, 'idxscan')
    res = db14.execute_command('FT.SEARCH', 'idxscan', 'beta', 'NOCONTENT')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList([2, 'pre1', 'pre2']), message=res)

    db14.close()


@skip(cluster=True)
def testNotificationsAreScopedPerDb(env):
    """A write on DB N must only feed indexes bound to DB N. Two indexes with the
    same PREFIX but on different DBs must not see each other's documents."""
    conn0 = getConnectionByEnv(env)  # DB 0
    db14 = _conn_on_db(env, 14)

    conn0.execute_command('FT.CREATE', 'idx0', 'PREFIX', '1', 'p:', 'SCHEMA', 't', 'TEXT')
    db14.execute_command('FT.CREATE', 'idx14', 'PREFIX', '1', 'p:', 'SCHEMA', 't', 'TEXT')

    conn0.execute_command('HSET', 'p:0', 't', 'on db zero')
    db14.execute_command('HSET', 'p:14', 't', 'on db fourteen')

    res0 = conn0.execute_command('FT.SEARCH', 'idx0', '*', 'NOCONTENT')
    env.assertEqual(toSortedFlatList(res0), toSortedFlatList([1, 'p:0']), message=res0)

    res14 = db14.execute_command('FT.SEARCH', 'idx14', '*', 'NOCONTENT')
    env.assertEqual(toSortedFlatList(res14), toSortedFlatList([1, 'p:14']), message=res14)

    db14.close()


@skip(cluster=True)
def testIndexNotVisibleFromOtherDb(env):
    """An index created on DB 14 must not be visible/queryable from DB 0: lookups
    are scoped to the connection's selected DB."""
    conn0 = getConnectionByEnv(env)  # DB 0
    db14 = _conn_on_db(env, 14)

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

    db14.close()


@skip(cluster=True)
def testSameNameDifferentDbsAreIndependent(env):
    """The same index name can exist on two DBs as two independent indexes (the
    registry is keyed by (db, name)), each serving only its own DB's documents."""
    conn0 = getConnectionByEnv(env)
    db14 = _conn_on_db(env, 14)

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

    db14.close()


@skip(cluster=True)
def testFlushdbDropsOnlyIndexesOfThatDb(env):
    """FLUSHDB on DB 14 drops only the index bound to DB 14; a DB-0 index survives.
    FLUSHALL afterwards drops the remaining DB-0 index."""
    conn0 = getConnectionByEnv(env)
    db14 = _conn_on_db(env, 14)

    conn0.execute_command('FT.CREATE', 'idx0', 'SCHEMA', 't', 'TEXT')
    db14.execute_command('FT.CREATE', 'idx14', 'SCHEMA', 't', 'TEXT')
    # FT._LIST is scoped to the connection's DB.
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

    db14.close()


@skip(cluster=True)
def testContentLoadAndCursorOnOtherDb(env):
    """Returning document content (not NOCONTENT) and cursor reads both open the
    document keys; on DB 14 they must open them on DB 14. Exercises the query
    content-load path and the detached cursor-read context."""
    db14 = _conn_on_db(env, 14)

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

    db14.close()


@skip(cluster=True)
def testIndexDbSurvivesRdbReload(env):
    """The DB an index is bound to is persisted in the RDB and restored on reload."""
    db14 = _conn_on_db(env, 14)

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

    db14.close()
