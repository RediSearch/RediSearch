from common import *

from time import sleep, time
from redis import ResponseError

from cmath import inf


def loadDocs(env, count=100, idx='idx', text='hello world'):
    env.expect('FT.CREATE', idx, 'ON', 'HASH', 'prefix', 1, idx, 'SCHEMA', 'f1', 'TEXT').ok()
    waitForIndex(env, idx)
    for x in range(count):
        cmd = ['FT.ADD', idx, '{}_doc{}'.format(idx, x), 1.0, 'FIELDS', 'f1', text]
        env.cmd(*cmd)
    r1 = env.cmd('ft.search', idx, text)
    r2 = list(set(map(lambda x: x[1], filter(lambda x: isinstance(x, list), r1))))
    env.assertEqual([text], r2)
    r3 = to_dict(env.cmd('ft.info', idx))
    env.assertEqual(count, int(r3['num_docs']))

def exhaustCursor(env, idx, res, *args):
    first, cid = res
    rows = [res]
    while cid:
        res, cid = env.cmd('FT.CURSOR', 'READ', idx, cid, *args)
        rows.append([res, cid])
    return rows

def getCursorStats(env, idx='idx'):
    info = env.cmd('FT.INFO', idx)
    try:
        info_dict = to_dict(info)['cursor_stats']
    except:
        return {'index_total' : 0, 'global_total' : 0}
    return to_dict(info_dict)

def testCursors(env):
    loadDocs(env)
    query = ['FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@f1', 'WITHCURSOR']
    res = env.cmd(*query)

    # Check info and see if there are other cursors
    info = getCursorStats(env)
    env.assertEqual(0, info['global_total'])

    res = exhaustCursor(env, 'idx', res)
    env.assertEqual(1, len(res)) # Only one response
    env.assertEqual(0, res[0][1])
    env.assertEqual(101, len(res[0][0]))

    # Issue the same query, but using a specified count
    res = env.cmd(*(query[::]+['COUNT', 10]))

    res = exhaustCursor(env, 'idx', res)
    env.assertEqual(11, len(res))

@skip(noWorkers=True)
def testCursorsBG():
    env = Env(moduleArgs='WORKER_THREADS 1 MT_MODE MT_MODE_FULL _PRINT_PROFILE_CLOCK FALSE')
    testCursors(env)


@skip(noWorkers=True)
def testCursorsBGEdgeCasesSanity():
    env = Env(moduleArgs='WORKER_THREADS 1 MT_MODE MT_MODE_FULL')
    env.skipOnCluster()
    count = 100
    loadDocs(env, count=count)
    # Add an extra field to every other document
    for x in range(0, count, 2):
        env.cmd('HSET', 'idx_doc{}'.format(x), 'foo', 'bar')

    queries = [
        f'FT.AGGREGATE idx * WITHCURSOR COUNT 10 SORTBY 1 @f1 MAX {count} LOAD 1 irrelevant',
        f'FT.AGGREGATE idx * WITHCURSOR COUNT 10 LOAD 1 @foo FILTER exists(@foo)',
        f'FT.AGGREGATE idx * WITHCURSOR COUNT 10 SORTBY 1 @f1 MAX {count} LOAD 1 foo FILTER exists(@foo)',
    ]

    # Sanity check - make sure that the queries not crashing or hanging
    for query in queries:
        resp = env.expect(query).noError().res
        resp = exhaustCursor(env, 'idx', resp)

def testMultipleIndexes(env):
    loadDocs(env, idx='idx2', text='goodbye')
    loadDocs(env, idx='idx1', text='hello')
    q1 = ['FT.AGGREGATE', 'idx1', '*', 'LOAD', 1, '@f1', 'WITHCURSOR', 'COUNT', 10 ]
    q2 = q1[::]
    q2[1] = 'idx2'
    waitForIndex(env, 'idx1')
    waitForIndex(env, 'idx2')
    r1 = exhaustCursor(env, 'idx1', env.cmd( * q1))
    r2 = exhaustCursor(env, 'idx2', env.cmd( * q2))
    env.assertEqual(11, len(r1[0][0]))
    env.assertEqual(11, len(r2[0][0]))
    # Compare last results
    last1 = r1[0][0][10]
    last2 = r2[0][0][10]
    env.assertEqual(['f1', 'hello'], last1)
    env.assertEqual(['f1', 'goodbye'], last2)

def testCapacities(env):
    if env.is_cluster():
        env.skip()

    loadDocs(env, idx='idx1')
    loadDocs(env, idx='idx2')
    q1 = ['FT.AGGREGATE', 'idx1', '*', 'LOAD', '1', '@f1', 'WITHCURSOR', 'COUNT', 10]
    q2 = q1[::]
    q2[1] = 'idx2'

    cursors1 = []
    cursors2 = []
    for _ in range(128):
        r1 = env.cmd(*q1)
        r2 = env.cmd(*q2)
        cursors1.append(r1)
        cursors2.append(r2)

    # Get info for the cursors
    info = getCursorStats(env, 'idx1')
    env.assertEqual(128, info['index_total'])
    env.assertEqual(256, info['global_total'])
    info = getCursorStats(env, 'idx2')
    env.assertEqual(128, info['index_total'])

    # Try to create another cursor
    env.assertRaises(ResponseError, env.cmd, * q1)
    env.assertRaises(ResponseError, env.cmd, * q2)

    # Clear all the cursors
    for c in cursors1:
        env.cmd('FT.CURSOR', 'DEL', 'idx1', c[-1])
    env.assertEqual(0, getCursorStats(env, 'idx1')['index_total'])

    # Check that we can create a new cursor
    c = env.cmd( * q1)
    env.cmd('FT.CURSOR', 'DEL', 'idx1', c[-1])

def testTimeout(env):
    # currently this test is only valid on one shard because coordinator creates more cursor which are not clean
    # with the same timeout
    env.skipOnCluster()
    loadDocs(env, idx='idx1')
    # Maximum idle of 1ms
    q1 = ['FT.AGGREGATE', 'idx1', '*', 'LOAD', '1', '@f1', 'WITHCURSOR', 'COUNT', 10, 'MAXIDLE', 1]
    res = env.cmd(*q1)
    exptime = time() + 2.5
    rv = 1
    while time() < exptime:
        sleep(0.01)
        env.cmd('FT.CURSOR', 'GC', 'idx1', '0')
        rv = getCursorStats(env, 'idx1')['index_total']
        if not rv:
            break
    env.assertEqual(0, rv)
'''
def testErrors(env):
    env.expect('ft.create idx schema name text').equal('OK')
    #env.expect('ft.add idx hotel 1.0 fields name hilton').equal('OK')
    env.expect('FT.AGGREGATE idx hilton withcursor').error()       \
        .contains('Index `idx` does not have cursors enabled')
'''
def testLeaked(env):
    # Test ensures in CursorList_Destroy() checks shutdown with remaining cursors
    loadDocs(env)
    env.expect('FT.AGGREGATE idx * LOAD 1 @f1 WITHCURSOR COUNT 1 MAXIDLE 1')

def testNumericCursor(env):
    conn = getConnectionByEnv(env)
    idx = 'foo'
    ff = 'ff'
    env.expect('FT.CREATE', idx, 'ON', 'HASH', 'SCHEMA', ff, 'NUMERIC').ok()
    for x in range(1000):
        conn.execute_command('HSET', f'{idx}_{x}', ff, x)

    # res = env.cmd('FT.AGGREGATE', idx, '*', 'LOAD', '*', 'SORTBY', 2, '@ff', 'ASC', 'LIMIT', 0, 1000)
    # env.assertIsNotNone(res)

    res, cursor = env.cmd('FT.AGGREGATE', idx, '*', 'LOAD', '*', 'SORTBY', 2, '@ff', 'ASC', 'WITHCURSOR', 'COUNT', 1, 'LIMIT', 0, 999999)
    # res, cursor = env.cmd('FT.AGGREGATE', idx, '*', 'LOAD', '*', 'WITHCURSOR', 'COUNT', 1)
    env.assertNotEqual(res, [0])
    env.assertNotEqual(cursor, 0)

    for x in range(1, 1000):
        res, cursor = env.cmd('FT.CURSOR', 'READ', idx, str(cursor))
        env.assertNotEqual(res, [0])
        env.assertNotEqual(cursor, 0)

    res, cursor = env.cmd('FT.CURSOR', 'READ', idx, str(cursor))
    env.assertEqual(res, [0])
    env.assertEqual(cursor, 0)


def testIndexDropWhileIdle(env):
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE idx SCHEMA t numeric').ok()

    num_docs = 3
    for i in range(num_docs):
        conn.execute_command('HSET', f'doc{i}' ,'t', i)

    count = 1
    res, cursor = conn.execute_command('FT.AGGREGATE', 'idx', '*', 'WITHCURSOR', 'COUNT', count)

    # Results length should equal the requested count + additional field for the number of results
    # (which is meaningless is ft.aggregate)
    env.assertEqual(len(res), count + 1)

    # drop the index while the cursor is idle/ running in bg
    conn.execute_command('ft.drop', 'idx')

    # Try to read from the cursor

    if env.is_cluster():
        res, cursor = env.cmd(f'FT.CURSOR READ idx {str(cursor)}')

        # Return the next results. count should equal the count at the first cursor's call.
        env.assertEqual(len(res), count + 1)

    else:
        env.expect(f'FT.CURSOR READ idx {str(cursor)}').error().contains('The index was dropped while the cursor was idle')

@skip(noWorkers=True)
def testIndexDropWhileIdleBG():
    env = Env(moduleArgs='WORKER_THREADS 1 MT_MODE MT_MODE_FULL')
    testIndexDropWhileIdle(env)

def testExceedCursorCapacity(env):
    env.skipOnCluster()

    env.expect('FT.CREATE idx SCHEMA t numeric').ok()
    env.cmd('HSET', 'doc1' ,'t', 1)

    index_cap = getCursorStats(env, 'idx')['index_capacity']

    # reach the spec's cursors maximum capacity
    for i in range(index_cap):
        env.cmd('FT.AGGREGATE', 'idx', '*', 'WITHCURSOR', 'COUNT', 1)

    # Trying to create another cursor should fail
    env.expect('FT.AGGREGATE', 'idx', '*', 'WITHCURSOR', 'COUNT', 1).error().contains('Too many cursors allocated for index')

@skip(noWorkers=True)
def testExceedCursorCapacityBG():
    env = Env(moduleArgs='WORKER_THREADS 1 MT_MODE MT_MODE_FULL')
    testExceedCursorCapacity(env)
