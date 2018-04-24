from rmtest import ModuleTestCase
import redis
import unittest
import pprint
from redis import ResponseError
from time import sleep, time

def to_dict(res):
    d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
    return d

class CursorTestCase(ModuleTestCase('../redisearch.so')):

    def loadDocs(self, count=100, idx='idx', text='hello world'):
        self.cmd('FT.CREATE', idx, 'SCHEMA', 'f1', 'TEXT')
        for x in range(count):
            cmd = ['FT.ADD', idx, '{}_doc{}'.format(idx, x), 1.0, 'FIELDS', 'f1', text]
            self.cmd(*cmd)


    def exhaustCursor(self, idx, resp, *args):
        first, cid=resp
        rows = [resp]
        while cid:
            resp, cid=self.cmd('FT.CURSOR', 'READ', idx, cid, *args)
            rows.append([resp, cid])
        return rows
    
    def getCursorStats(self, idx='idx'):
        return to_dict(to_dict(self.cmd('FT.INFO', idx))['cursor_stats'])

    def testCursors(self):
        self.loadDocs()
        query = ['FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@f1', 'WITHCURSOR']
        resp = self.cmd(*query)
        resp = self.exhaustCursor('idx', resp)
        self.assertEqual(1, len(resp)) # Only one response
        self.assertEqual(0, resp[0][1])
        self.assertEqual(101, len(resp[0][0]))

        # Check info and see if there are other cursors
        info = self.getCursorStats()
        self.assertEqual(0, info['global_total'])

        # Issue the same query, but using a specified count
        resp = self.cmd(*(query[::]+['COUNT', 10]))
        resp = self.exhaustCursor('idx', resp)
        # pprint.pprint(resp)
        self.assertEqual(11, len(resp))
    
    def testMultipleIndexes(self):
        self.loadDocs(idx='idx2', text='goodbye')
        self.loadDocs(idx='idx1', text='hello')
        q1 = ['FT.AGGREGATE', 'idx1', '*', 'LOAD', 1, '@f1', 'WITHCURSOR', 'COUNT', 10 ]
        q2 = q1[::]
        q2[1] = 'idx2'
        r1 = self.exhaustCursor('idx1', self.cmd( * q1))
        r2 = self.exhaustCursor('idx2', self.cmd( * q2))
        self.assertEqual(11, len(r1[0][0]))
        self.assertEqual(11, len(r2[0][0]))
        # Compare last results
        last1 = r1[0][0][10]
        last2 = r2[0][0][10]
        self.assertEqual(['f1', 'hello'], last1)
        self.assertEqual(['f1', 'goodbye'], last2)
    
    def testCapacities(self):
        self.loadDocs(idx='idx1')
        self.loadDocs(idx='idx2')
        q1 = ['FT.AGGREGATE', 'idx1', '*', 'LOAD', '1', '@f1', 'WITHCURSOR', 'COUNT', 10]
        q2 = q1[::]
        q2[1] = 'idx2'

        cursors1 = []
        cursors2 = []
        for _ in range(128):
            cursors1.append(self.cmd( * q1))
            cursors2.append(self.cmd( * q2))
        
        # Get info for the cursors
        info = self.getCursorStats('idx1')
        self.assertEqual(128, info['index_total'])
        self.assertEqual(256, info['global_total'])
        info = self.getCursorStats('idx2')
        self.assertEqual(128, info['index_total'])

        # Try to create another cursor
        self.assertRaises(ResponseError, self.cmd, * q1)
        self.assertRaises(ResponseError, self.cmd, * q2)
        
        # Clear all the cursors
        for c in cursors1:
            self.cmd('FT.CURSOR', 'DEL', 'idx1', c[-1])
        self.assertEqual(0, self.getCursorStats('idx1')['index_total'])
        # Check that we can create a new cursor
        c = self.cmd( * q1)
        self.cmd('FT.CURSOR', 'DEL', 'idx1', c[-1])
    
    def testTimeout(self):
        self.loadDocs(idx='idx1')
        # Maximum idle of 1ms
        q1 = ['FT.AGGREGATE', 'idx1', '*', 'LOAD', '1', '@f1', 'WITHCURSOR', 'COUNT', 10, 'MAXIDLE', 1]
        resp = self.cmd( * q1)
        sleep(0.01)
        self.cmd('FT.CURSOR', 'GC', 'idx1', '0')
        self.assertEqual(0, self.getCursorStats('idx1')['index_total'])