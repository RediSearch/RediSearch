from includes import *
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
    r3 = env.cmd('ft.info', idx)
    env.assertEqual(count, int(r3[r3.index('num_docs') + 1]))

def exhaustCursor(env, idx, resp, *args):
    first, cid = resp
    rows = [resp]
    while cid:
        resp, cid=env.cmd('FT.CURSOR', 'READ', idx, cid, *args)
        rows.append([resp, cid])
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
    resp = env.cmd(*query)

    # Check info and see if there are other cursors
    info = getCursorStats(env)
    env.assertEqual(0, info['global_total'])

    resp = exhaustCursor(env, 'idx', resp)
    env.assertEqual(1, len(resp)) # Only one response
    env.assertEqual(0, resp[0][1])
    env.assertEqual(101, len(resp[0][0]))

    # Issue the same query, but using a specified count
    resp = env.cmd(*(query[::]+['COUNT', 10]))

    resp = exhaustCursor(env, 'idx', resp)
    env.assertEqual(11, len(resp))

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
    resp = env.cmd(*q1)
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

# TODO: improve the test and add a case of timeout:
# 1. Coordinator's cursor times out before the shard's cursor
# 2. Some shard's cursor times out before the coordinator's cursor
# 3. All shards' cursors time out before the coordinator's cursor
def testCursorOnCoordinator(env):
    SkipOnNonCluster(env)
    env.expect('FT.CREATE idx SCHEMA n NUMERIC').ok()
    conn = getConnectionByEnv(env)

    # Verify that empty reply from some shard doesn't break the cursor
    conn.execute_command('HSET', 0 ,'n', 0)
    res, cursor = conn.execute_command('FT.AGGREGATE', 'idx', '*', 'LOAD', '*', 'WITHCURSOR', 'COUNT', 1)
    env.assertEqual(res, [1, ['n', '0']])
    env.expect(f'FT.CURSOR READ idx {cursor}').equal([[0], 0]) # empty reply from shard - 0 results and depleted cursor

    env.expect('FT.AGGREGATE', 'non-existing', '*', 'LOAD', '*', 'WITHCURSOR', 'COUNT', 1).equal([[0], 0]) # empty reply from coordinator - 0 results

    # Verify we can read from the cursor all the results.
    # The coverage proves that the `_FT.CURSOR READ` command is sent to the shards only when more results are needed.
    n_docs =  2               # some multiplier (to make sure we have enough results on each shard)
    n_docs *= 1000            # number of results per shard per cursor
    n_docs *= env.shardsCount # number of results per cursor

    for i in range(n_docs):
        conn.execute_command('HSET', i ,'n', i)

    result_set = set()
    def add_results(res):
        for r in res[1:]:
            cur_res = int(r[1])
            env.assertNotContains(cur_res, result_set)
            result_set.add(cur_res)

    with conn.monitor() as monitor:
        # Some periodic cluster commands are sent to the shards and also break the monitor.
        # This function skips them and returns the actual next command we want to observe.
        def next_command():
            try:
                return monitor.next_command()['command']
            except ValueError:
                return next_command()
        res, cursor = conn.execute_command('FT.AGGREGATE', 'idx', '*', 'LOAD', '*', 'WITHCURSOR', 'COUNT', 100)
        add_results(res)
        while cursor:
            res, cursor = env.cmd('FT.CURSOR', 'READ', 'idx', cursor)
            add_results(res)

        cmd = next_command()
        while not cmd.startswith('FT.AGGREGATE'):
            cmd = next_command()
        env.assertContains('_FT.AGGREGATE', next_command())

        # Verify that after the first chunk, we make `FT.CURSOR READ` without triggering `_FT.CURSOR READ`.
        # Each shard has more than 1000 results, and the initial aggregation request yielded in `nShards` * 1000 results
        # with `nShards` replies. We expect more ((`nShards` - 1) * 1000 / 100) - 1 `FT.CURSOR READ` before we need to
        # trigger the shards. On the next `FT.CURSOR READ` we expect to  trigger the next `_FT.CURSOR READ`.
        # ((`nShards` - 1) * 1000 / 100) - 1 + 1 => (`nShards` - 1) * 10
        exp = 'FT.CURSOR READ'
        for _ in range((env.shardsCount - 1) * 10):
            cmd = next_command()
            env.assertTrue(cmd.startswith(exp), message=f'expected `{exp}` but got `{cmd}`')
        # we expect to observe the next `_FT.CURSOR READ` in the next 11 commands (most likely the next command)
        found = False
        for i in range(11):
            cmd = next_command()
            if not cmd.startswith('FT.CURSOR'):
                exp = '_FT.CURSOR READ'
                env.assertTrue(cmd.startswith(exp), message=f'expected `{exp}` but got `{cmd}`')
                found = True
                break
        env.assertTrue(found, message=f'`_FT.CURSOR READ` was not observed within {i} commands')
        suffix = 'st' if i == 1 else 'nd' if i == 2 else 'rd' if i == 3 else 'th'
        env.debugPrint(f'Found `_FT.CURSOR READ` in the {i}{suffix} try')

    env.assertEqual(len(result_set), n_docs)
    for i in range(n_docs):
        env.assertContains(i, result_set)

    # Test cursor deletion before reply arrives
    _, cursor = conn.execute_command('FT.AGGREGATE', 'idx', '*', 'SORTBY', '1', '@n', 'MAX', '10000', 'WITHCURSOR')
    env.cmd('FT.CURSOR', 'DECIMATE', '"the cursor before getting the result"', cursor)
