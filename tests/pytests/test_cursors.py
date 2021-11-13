from time import sleep
import unittest
from redis import ResponseError
from includes import *


def to_dict(res):
    d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
    return d


def loadDocs(env, count=100, idx='idx', text='hello world'):
    env.cmd('FT.CREATE', idx, 'SCHEMA', 'f1', 'TEXT')
    for x in range(count):
        cmd = ['FT.ADD', idx, '{}_doc{}'.format(idx, x), 1.0, 'FIELDS', 'f1', text]
        env.cmd(*cmd)

def exhaustCursor(env, idx, resp, *args):
    first, cid = resp
    rows = [resp]
    while cid:
        resp, cid=env.cmd('FT.CURSOR', 'READ', idx, cid, *args)
        rows.append([resp, cid])
    return rows

def getCursorStats(env, idx='idx'):
    return to_dict(to_dict(env.cmd('FT.INFO', idx))['cursor_stats'])

def testCursors(env):
    loadDocs(env)
    query = ['FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@f1', 'WITHCURSOR']
    resp = env.cmd(*query)
    resp = exhaustCursor(env, 'idx', resp)
    env.assertEqual(1, len(resp)) # Only one response
    env.assertEqual(0, resp[0][1])
    env.assertEqual(101, len(resp[0][0]))

    # Check info and see if there are other cursors
    info = getCursorStats(env)
    env.assertEqual(0, info['global_total'])

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
        raise unittest.SkipTest()
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
    loadDocs(env, idx='idx1')
    # Maximum idle of 1ms
    q1 = ['FT.AGGREGATE', 'idx1', '*', 'LOAD', '1', '@f1', 'WITHCURSOR', 'COUNT', 10, 'MAXIDLE', 1]
    resp = env.cmd( * q1)
    sleep(0.01)
    env.cmd('FT.CURSOR', 'GC', 'idx1', '0')
    env.assertEqual(0, getCursorStats(env, 'idx1')['index_total'])
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