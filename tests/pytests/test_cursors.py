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
    env = Env(moduleArgs='WORKERS 1 _PRINT_PROFILE_CLOCK FALSE')
    testCursors(env)


@skip(cluster=True, noWorkers=True)
def testCursorsBGEdgeCasesSanity():
    env = Env(moduleArgs='WORKERS 1')
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

@skip(cluster=True)
def testCapacities(env):
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

@skip(cluster=True)
def testTimeout(env):
    # currently this test is only valid on one shard because coordinator creates more cursors which are not cleaned
    # with the same timeout
    loadDocs(env, idx='idx1')
    # Maximum idle of 1ms
    q1 = ['FT.AGGREGATE', 'idx1', '*', 'LOAD', '1', '@f1', 'WITHCURSOR', 'COUNT', 10, 'MAXIDLE', 1]
    env.cmd(*q1)
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
    # Ensure that sanitizer doesn't report memory leak for idle cursors.
    n_docs = env.shardsCount * 1100
    loadDocs(env, count = n_docs)
    res, cursor = env.cmd('FT.AGGREGATE idx * WITHCURSOR COUNT 1')
    env.assertNotEqual(cursor, 0, message=f"result = {res}")

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

@skip(cluster=False)
def testCursorDifferentConnections(env: Env):
    if env.shardsCount < 2:
        raise SkipTest('This test requires at least 2 shards')
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE idx SCHEMA n numeric').ok()

    num_docs = 6
    for i in range(num_docs):
        conn.execute_command('HSET', i, 'n', i)

    con2 = env.getConnection(2) # assume we have at least 2 shards
    _, cursor = con2.execute_command('FT.AGGREGATE', 'idx', '*', 'WITHCURSOR', 'COUNT', 3)
    # env is connected to shard 1, con2 is connected to shard 2
    env.expect(f'FT.CURSOR READ idx {cursor}').error().contains('Cursor not found')

def testIndexDropWhileIdle(env: Env):
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE idx SCHEMA t numeric').ok()

    # Add documents to the index until we have more than one document on each shard
    num_docs = 0
    while not np.all([env.getConnection(i).execute_command('DBSIZE') > 1 for i in range(env.shardsCount)]):
        conn.execute_command('HSET', num_docs, 't', num_docs)
        num_docs += 1
    env.debugPrint(f'Added {num_docs} documents')

    count = num_docs - 1 # make sure we will have at least one result from each shard
    res, cursor = env.cmd('FT.AGGREGATE', 'idx', '*', 'WITHCURSOR', 'COUNT', count)

    # Results length should equal the requested count + additional field for the number of results
    # (which is meaningless with ft.aggregate)
    env.assertEqual(res[1:], [[]] * count, message=f'res == {res}')

    # drop the index while the cursor is idle/running in bg
    env.expect('FT.DROPINDEX', 'idx').ok()

    # Try to read from the cursor
    if env.isCluster():
        res, cursor = env.cmd(f'FT.CURSOR READ idx {cursor} COUNT 1') # read the last result
        env.assertEqual(res[1:], [[]] , message=f'res == {res}')
    else:
        env.expect(f'FT.CURSOR READ idx {cursor}').error().contains('The index was dropped while the cursor was idle')

@skip(noWorkers=True)
def testIndexDropWhileIdleBG():
    env = Env(moduleArgs='WORKERS 1')
    testIndexDropWhileIdle(env)

def exceedCursorCapacity(env):
    env.expect('FT.CREATE idx SCHEMA t numeric').ok()
    env.cmd('HSET', 'doc1' ,'t', 1)

    index_cap = getCursorStats(env, 'idx')['index_capacity']

    # reach the spec's cursors maximum capacity
    for i in range(index_cap):
        env.cmd('FT.AGGREGATE', 'idx', '*', 'WITHCURSOR', 'COUNT', 1)

    # Trying to create another cursor should fail
    env.expect('FT.AGGREGATE', 'idx', '*', 'WITHCURSOR', 'COUNT', 1).error().contains('Too many cursors allocated for index')

@skip(cluster=True)
def testExceedCursorCapacity(env):
    exceedCursorCapacity(env)

@skip(cluster=True, noWorkers=True)
def testExceedCursorCapacityBG():
    env = Env(moduleArgs='WORKERS 1')
    exceedCursorCapacity(env)

@skip(noWorkers=True, cluster=False)
def testCursorOnCoordinatorBG():
    env = Env(moduleArgs='WORKERS 1')
    CursorOnCoordinator(env)

@skip(cluster=False)
def testCursorOnCoordinator(env):
    CursorOnCoordinator(env)

# TODO: improve the test and add a case of timeout:
# 1. Coordinator's cursor times out before the shard's cursor
# 2. Some shard's cursor times out before the coordinator's cursor
# 3. All shards' cursors time out before the coordinator's cursor
def CursorOnCoordinator(env: Env):
    env.expect('FT.CREATE idx SCHEMA n NUMERIC').ok()
    conn = getConnectionByEnv(env)

    # Verify that empty reply from some shard doesn't break the cursor
    conn.execute_command('HSET', 0 ,'n', 0)
    res, cursor = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', '*', 'WITHCURSOR', 'COUNT', 1)
    env.assertEqual(res, [1, ['n', '0']])
    env.expect(f'FT.CURSOR READ idx {cursor}').equal([[0], 0]) # empty reply from shard - 0 results and depleted cursor

    env.expect(
        'FT.AGGREGATE', 'non-existing', '*', 'LOAD', '*', 'WITHCURSOR', 'COUNT', 1
    ).error().contains('non-existing: no such index')

    # Verify we can read from the cursor all the results.
    # The coverage proves that the `_FT.CURSOR READ` command is sent to the shards only when more results are needed.
    n_docs =  1.1             # some multiplier (to make sure we have enough results on each shard)
    n_docs *= 1000            # number of results per shard per cursor
    n_docs *= env.shardsCount # number of results per cursor
    n_docs = int(n_docs)

    count = 100
    expected_reads = n_docs // count

    for i in range(n_docs):
        conn.execute_command('HSET', i ,'n', i)

    default = int(env.cmd(config_cmd(), 'GET', 'CURSOR_REPLY_THRESHOLD')[0][1])
    configs = {default, 1, env.shardsCount - 1, env.shardsCount}
    for threshold in configs:
        env.expect(config_cmd(), 'SET', 'CURSOR_REPLY_THRESHOLD', threshold).ok()

        result_set = set()
        def add_results(res):
            for cur_res in [int(r[1]) for r in res[1:]]:
                env.assertNotContains(cur_res, result_set)
                result_set.add(cur_res)

        _, cursor = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', '*', 'WITHCURSOR', 'COUNT', count)
        env.cmd('FT.CURSOR', 'DEL', 'idx', cursor)
        # We expect that deleting the cursor will trigger the shards to delete their cursors as well.
        # Since none of the cursors is expected to be expired, we don't expect `FT.CURSOR GC` to return a positive number.
        # `FT.CURSOR GC` will return -1 if there are no cursors to delete, and 0 if the cursor list was empty.
        env.expect('FT.CURSOR', 'GC', '42', '42').equal(0)

        with env.getConnection().monitor() as monitor:
            # Some periodic cluster commands are sent to the shards and also break the monitor.
            # This function skips them and returns the actual next command we want to observe.
            def next_command():
                while True:
                    try:
                        command = monitor.next_command()['command']
                    except ValueError:
                        continue
                    # Filter out the periodic cluster commands
                    if command.startswith('_FT.') or command.startswith('FT.'):
                        return command

            # Generate the cursor and read all the results
            res, cursor = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', '*', 'WITHCURSOR', 'COUNT', count)
            add_results(res)
            while cursor:
                res, cursor = env.cmd('FT.CURSOR', 'READ', 'idx', cursor)
                add_results(res)

            # Check the monitor for the expected commands

            env.assertContains('FT.AGGREGATE', next_command())
            env.assertContains('_FT.AGGREGATE', next_command())

            # Verify that after the first chunk, we make `FT.CURSOR READ` without triggering `_FT.CURSOR READ`.
            # Each shard has more than 1000 results, and the initial aggregation request yielded in `nShards` * 1000 results
            # with `nShards` replies. We expect more ((`nShards` - `threshold`) * 1000 / 100) - 1 `FT.CURSOR READ` before we
            # need to trigger the shards. On the next `FT.CURSOR READ` we expect to  trigger the next `_FT.CURSOR READ`.
            # ((`nShards` - `threshold`) * 1000 / 100) - 1 + 1 => (`nShards` - `threshold`) * 10
            exp = 'FT.CURSOR READ'
            for _ in range((env.shardsCount - threshold) * 10):
                cmd = next_command()
                env.assertTrue(cmd.startswith(exp), message=f'expected `{exp}` but got `{cmd}`')
            # we expect to observe the next "_FT.CURSOR READ" in the next `expected_reads` "FT.CURSOR READ"
            # commands (most likely the next command).
            found = False
            for i in range(1, expected_reads + 1 + 1):
                cmd = next_command()
                if not cmd.startswith('FT.CURSOR'):
                    exp = '_FT.CURSOR READ'
                    env.assertTrue(cmd.startswith(exp), message=f'expected `{exp}` but got `{cmd}`')
                    found = True
                    break
            env.assertTrue(found, message=f'`_FT.CURSOR READ` was not observed within {expected_reads + 1} commands')
            if found:
                env.debugPrint(f'Found `_FT.CURSOR READ` in the {number_to_ordinal(i)} try')

            env.assertEqual(len(result_set), n_docs)
            for i in range(n_docs):
                env.assertContains(i, result_set)

def testCursorDepletionNonStrictTimeoutPolicy(env):
    """Tests that the cursor id is returned in case the timeout policy is
    non-strict (i.e., the default `RETURN`), even when a timeout is experienced"""

    conn = getConnectionByEnv(env)

    # Create the index
    env.expect('FT.CREATE idx SCHEMA t text').ok()

    # Populate the index
    num_docs = 1500 * env.shardsCount
    for i in range(num_docs):
        conn.execute_command('HSET', f'doc{i}' ,'t', i)

    # Create a cursor with a small `timeout` and large `count`, and read from
    # it until depleted
    res, cursor = env.cmd('FT.AGGREGATE', 'idx', '*', 'WITHCURSOR', 'COUNT', '10000', 'TIMEOUT', '1')
    n_recieved = len(res) - 1
    while cursor:
        res, cursor = env.cmd('FT.CURSOR', 'READ', 'idx', cursor)
        n_recieved += len(res) - 1

    env.assertEqual(n_recieved, num_docs)

def testCursorDepletionStrictTimeoutPolicy():
    """Tests that the cursor returns a timeout error in case of a timeout, when
    the timeout policy is `ON_TIMEOUT FAIL`"""

    env = Env(moduleArgs='ON_TIMEOUT FAIL')
    conn = getConnectionByEnv(env)

    # Create an index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()

    # Populate the index
    num_docs = 10000 * env.shardsCount
    for i in range(num_docs):
        conn.execute_command('HSET', f'doc{i}', 't', str(i))

    # Create a cursor with a small timeout and a large count (so it will time
    # out during pipeline execution)
    env.expect(
        'FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@t', 'GROUPBY', '1', '@t', 'WITHCURSOR', 'COUNT', str(num_docs), 'TIMEOUT', '1'
    ).error().contains('Timeout limit was reached')

@skip(cluster=True)
def test_mod_6597(env):
    """Tests that we update the numeric index appropriately upon deleting
    documents from a numeric index, and are able to query an invalid cursor in
    such case getting an empty result instead of a crash."""
    conn = getConnectionByEnv(env)

    # Create an index with a numeric field.
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'NUMERIC').equal('OK')

    # Populate the db (and index) with enough documents for the GC to work (one
    # more than `FORK_GC_CLEAN_THRESHOLD`).
    res = env.cmd(config_cmd(), 'GET', 'FORK_GC_CLEAN_THRESHOLD')[0][1]
    num_docs = int(res) + 1
    for i in range(num_docs):
        conn.execute_command('hset', f'doc{i}', 'test', str(i))

    # Initialize a cursor
    res, cid = env.execute_command('ft.aggregate', 'idx', f'@test:[1 {num_docs}]', 'LOAD', '1', '@test', 'WITHCURSOR', 'COUNT', '1')
    n = len(res) - 1

    # Make sure GC is not self-invoked (periodic run).
    env.expect(config_cmd(), 'SET', 'FORK_GC_RUN_INTERVAL', 3600).equal('OK')

    # Delete all documents of the index. The same effect is achieved if a split
    # occurred and a whole NumericRangeNode is deleted.
    for i in range(1, num_docs, 1):
        env.cmd('DEL', f'doc{i}')

    # Invoke the GC, cleaning the index
    forceInvokeGC(env, 'idx')

    # Deplete the cursor
    while cid:
        res, cid = env.cmd('ft.cursor', 'read', 'idx', cid)
        n += len(res)-1

    # We are not supposed to get any new results from the above query, since the
    # index is already invalidated.
    env.assertEqual(n, 1)

def testCountArgValidation(env):
    """Tests that an error is returned upon dispatching a `CURSOR READ` command
    with an invalid fourth argument (i.e., instead of `COUNT`)"""

    conn = getConnectionByEnv(env)

    # Create an index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TAG').ok()

    # Populate the index
    for i in range(5):
        conn.execute_command('HSET', f'h{i}', 't', f'foo{i}')

    # Create a cursor with a bad value for the `COUNT` argument
    env.expect(
        'FT.AGGREGATE', 'idx', '*', 'LOAD', '*', 'WITHCURSOR', 'COUNT', '2.3'
    ).error().contains('Bad arguments for COUNT: Could not convert argument to expected type')

    # Create a cursor
    res, cid = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', '*', 'WITHCURSOR', 'COUNT', '1')
    env.assertEqual(len(res), 2)

    # Query the cursor with a bad `COUNT` argument
    env.expect('FT.CURSOR', 'READ', 'idx', str(cid), 'LOVE', '3').error().contains('Unknown argument `LOVE`')

    # Query the cursor with bad subcommand
    env.expect(
        'FT.CURSOR', 'READS', 'idx', str(cid)
    ).error().contains('Unknown subcommand')
    env.expect(
        'FT.CURSOR', 'DELS', 'idx', str(cid)
    ).error().contains('Unknown subcommand')
    env.expect(
        'FT.CURSOR', 'GCS', 'idx', str(cid)
    ).error().contains('Unknown subcommand')

    # Query the cursor with a bad value for the `COUNT` argument
    env.expect(
        'FT.CURSOR', 'READ', 'idx', str(cid), 'COUNT', '2.3'
    ).error().contains('Bad value for COUNT: `2.3`')

    # Query with lowercase `COUNT`
    res, cid = env.cmd('FT.CURSOR', 'READ', 'idx', str(cid), 'count', '2')
    env.assertEqual(len(res), 3)

    # Query with uppercase `COUNT`
    res, cid = env.cmd('FT.CURSOR', 'READ', 'idx', str(cid), 'COUNT', '2')
    env.assertEqual(len(res), 3)

    # Make sure cursor is depleted
    res, cid = env.cmd('FT.CURSOR', 'READ', 'idx', str(cid), 'COUNT', '2')
    env.assertEqual(cid, 0)
    env.assertEqual(res, [0])
