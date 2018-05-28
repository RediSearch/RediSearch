from rmtest import ModuleTestCase
import redis
import unittest
from hotels import hotels
import random
import time


class SearchGCTestCase(ModuleTestCase('../redisearch.so')):

    def testBasicGC(self):
        self.cmd('flushdb')
        self.assertOk(self.cmd('ft.create', 'idx', 'schema', 'title', 'text', 'id', 'numeric'))
        self.assertOk(self.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields',
                               'title', 'hello world',
                               'id', '5'))

        self.assertOk(self.cmd('ft.add', 'idx', 'doc2', 1.0, 'fields',
                               'title', 'hello world 1',
                               'id', '7'))

        self.assertEqual(self.cmd('ft.debug', 'DUMP_INVIDX', 'idx', 'world'), [1, 2])
        self.assertEqual(self.cmd('ft.debug', 'DUMP_NUMIDX', 'idx', 'id'), [[1, 2], [2], [1]])

        self.assertEqual(self.cmd('ft.del', 'idx', 'doc2'), 1)

        time.sleep(1)

        # check that the gc collected the deleted docs
        self.assertEqual(self.cmd('ft.debug', 'DUMP_INVIDX', 'idx', 'world'), [1])
        self.assertEqual(self.cmd('ft.debug', 'DUMP_NUMIDX', 'idx', 'id'), [[1], [], [1]])

    def testNumerciGCIntensive(self):
        NumberOfDocs = 1000
        self.cmd('flushdb')
        self.assertOk(self.cmd('ft.create', 'idx', 'schema', 'id', 'numeric'))

        for i in range(NumberOfDocs):
            self.assertOk(self.cmd('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields', 'id', str(i)))

        for i in range(0, NumberOfDocs, 2):
            self.assertEqual(self.cmd('ft.del', 'idx', 'doc%d' % i), 1)

        time.sleep(1)

        res = self.cmd('ft.debug', 'DUMP_NUMIDX', 'idx', 'id')
        for r1 in res:
            for r2 in r1:
                self.assertEqual(r2 % 2, 0)
