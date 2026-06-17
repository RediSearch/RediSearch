# -*- coding: utf-8 -*-

import redis
from common import *


@skip(cluster=True)  # FLUSHDB on a specific DB is not meaningful in cluster mode
def testFlushdbOnIndexDbDropsIndex(env):
    """Baseline: a FLUSHDB on DB 0 (the DB indexes live in) must drop the index,
    just like FLUSHALL. Indexes can only be created on DB 0, so flushing it
    removes both the documents and the index.
    """
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    conn.execute_command('HSET', 'doc1', 't', 'hello world')
    env.assertContains('idx', env.cmd('FT._LIST'))

    # FLUSHDB on DB 0 clears the index DB -> the index must be gone afterwards.
    conn.execute_command('FLUSHDB')
    env.assertEqual(env.cmd('FT._LIST'), [])
    env.expect('FT.SEARCH', 'idx', 'world').error().contains('idx')


@skip(cluster=True)  # multiple logical DBs and FLUSHDB are not meaningful in cluster mode
def testFlushdbOnOtherDbKeepsIndex(env):
    """A FLUSHDB on a non-index logical DB (e.g. DB 14) must NOT drop RediSearch
    indexes, which always live in DB 0 (index creation is restricted to DB 0).

    Regression test for the report that flushing an unrelated DB made FT.SEARCH
    return "no such index". The fix filters the FLUSHDB server event by dbnum in
    onFlush() (src/indexes.c).
    """
    conn = getConnectionByEnv(env)

    # Create an index and populate it in DB 0 (the default DB used by env).
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    conn.execute_command('HSET', 'doc1', 't', 'hello world')
    conn.execute_command('HSET', 'doc2', 't', 'goodbye world')

    # Sanity: the index works before touching any other DB.
    res = env.cmd('FT.SEARCH', 'idx', 'world', 'NOCONTENT')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList([2, 'doc1', 'doc2']), message=res)

    # Open a separate connection to a different logical DB (DB 14), write a key
    # there, then FLUSHDB it. This must only affect DB 14.
    port = conn.connection_pool.connection_kwargs['port']
    other_db = redis.Redis(host='localhost', port=port, db=14, decode_responses=True)
    other_db.execute_command('SET', 'unrelated_key', 'value')
    env.assertEqual(other_db.execute_command('DBSIZE'), 1)
    other_db.execute_command('FLUSHDB')
    env.assertEqual(other_db.execute_command('DBSIZE'), 0)

    # The index in DB 0 must still exist and return the same results.
    env.assertContains('idx', env.cmd('FT._LIST'))
    res = env.cmd('FT.SEARCH', 'idx', 'world', 'NOCONTENT')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList([2, 'doc1', 'doc2']), message=res)

    # The underlying documents must also survive the flush of the other DB.
    env.assertEqual(conn.execute_command('DBSIZE'), 2)

    other_db.close()


@skip(cluster=True)  # multiple logical DBs and FLUSHALL semantics are not meaningful in cluster mode
def testFlushallStillDropsIndex(env):
    """Counterpart to testFlushdbOnOtherDbKeepsIndex: FLUSHALL must still drop the
    index, so the DB-filtering fix in onFlush() does not accidentally make indexes
    survive a real flush of their own DB (dbnum == -1).
    """
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    conn.execute_command('HSET', 'doc1', 't', 'hello world')
    env.assertContains('idx', env.cmd('FT._LIST'))

    # FLUSHALL clears DB 0 too -> the index must be gone afterwards.
    conn.execute_command('FLUSHALL')
    env.assertEqual(env.cmd('FT._LIST'), [])
    env.expect('FT.SEARCH', 'idx', 'world').error().contains('idx')
