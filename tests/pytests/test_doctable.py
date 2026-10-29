# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from RLTest import Env
from includes import *
from common import *


# mainly this test adding and removing docs while the doc table size is 100
# and make sure we are not crashing and not leaking memory (when runs with valgrind).
def testDocTable():
    env = Env(moduleArgs='MAXDOCTABLESIZE 100')
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text', 'body', 'text').ok()
    # doc table size is 100 so insearting 1000 docs should gives us 10 docs in each bucket
    con = env.getClusterConnectionIfNeeded()
    for i in range(1000):
        env.assertOk(con.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                         'title', 'hello world %d' % (i % 100),
                                         'body', 'lorem ist ipsum'))

    for i in range(100):
        res = env.cmd('ft.search', 'idx', 'hello world %d' % i)
        env.assertEqual(res[0], 10)

    # deleting the first 100 docs
    for i in range(100):
        env.assertEqual(con.execute_command('ft.del', 'idx', 'doc%d' % i), 1)

    for i in range(100):
        res = env.cmd('ft.search', 'idx', 'hello world %d' % i)
        env.assertEqual(res[0], 9)

    env.expect('ft.drop', 'idx').ok()


def _assertWildcardScanSkipsDeleted(env, numDocs):
    """A wildcard query walks every docId up to the table's maxDocId and prefetches metadata of
    the ids ahead of it. Holes in the id range (deleted docs) and the ids past the last doc must
    neither change the results nor read out of bounds."""
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE').ok()
    for i in range(numDocs):
        conn.execute_command('HSET', f'doc{i}', 'n', i)
    # Single holes, a run longer than the prefetch distance, and the tail of the table.
    deleted = set(range(0, numDocs, 7)) | set(range(100, 140)) | set(range(numDocs - 20, numDocs))
    for i in deleted:
        conn.execute_command('DEL', f'doc{i}')
    expected = [i for i in range(numDocs) if i not in deleted]

    res = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '*', 'NOCONTENT', 'LIMIT', 0, 0)
    env.assertEqual(res[1][1][0][3][:2], ['Type', 'WILDCARD'], message=res)

    # No SORTBY: a numeric sort can let the optimizer replace the wildcard iterator.
    res = env.cmd('FT.SEARCH', 'idx', '*', 'RETURN', 1, 'n', 'LIMIT', 0, numDocs)
    env.assertEqual(res[0], len(expected))
    env.assertEqual(sorted(int(fields[1]) for fields in res[2::2]), expected)

    res = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@n', 'LIMIT', 0, numDocs)
    env.assertEqual(sorted(int(row[1]) for row in res[1:]), expected)


@skip(cluster=True)
def testWildcardScanWithDeletedDocs(env):
    _assertWildcardScanSkipsDeleted(env, 1000)


@skip(cluster=True)
def testWildcardScanWithDeletedDocsInChainedTable():
    # A doc table smaller than the number of docs, so buckets hold chains of several docs.
    env = Env(moduleArgs='MAXDOCTABLESIZE 100')
    _assertWildcardScanSkipsDeleted(env, 1000)
