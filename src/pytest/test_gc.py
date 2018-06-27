from rmtest import BaseModuleTestCase
import redis
import unittest
from hotels import hotels
import random
import time


class SearchGCTestCase(BaseModuleTestCase):

    def testBasicGC(self):
        self.cmd('flushdb')
        self.assertOk(self.cmd('ft.create', 'idx', 'schema', 'title', 'text', 'id', 'numeric', 't', 'tag'))
        self.assertOk(self.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields',
                               'title', 'hello world',
                               'id', '5',
                               't', 'tag1'))

        self.assertOk(self.cmd('ft.add', 'idx', 'doc2', 1.0, 'fields',
                               'title', 'hello world 1',
                               'id', '7',
                               't', 'tag2'))

        self.assertEqual(self.cmd('ft.debug', 'DUMP_INVIDX', 'idx', 'world'), [1, 2])
        self.assertEqual(self.cmd('ft.debug', 'DUMP_NUMIDX', 'idx', 'id'), [[1, 2], [2], [1]])
        self.assertEqual(self.cmd('ft.debug', 'DUMP_TAGIDX', 'idx', 't'), [['tag1', [1]], ['tag2', [2]]])

        self.assertEqual(self.cmd('ft.del', 'idx', 'doc2'), 1)

        time.sleep(1)

        # check that the gc collected the deleted docs
        self.assertEqual(self.cmd('ft.debug', 'DUMP_INVIDX', 'idx', 'world'), [1])
        self.assertEqual(self.cmd('ft.debug', 'DUMP_NUMIDX', 'idx', 'id'), [[1], [], [1]])
        self.assertEqual(self.cmd('ft.debug', 'DUMP_TAGIDX', 'idx', 't'), [['tag1', [1]], ['tag2', []]])

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

    def testTagGC(self):
        NumberOfDocs = 10
        self.cmd('flushdb')
        self.assertOk(self.cmd('ft.create', 'idx', 'schema', 't', 'tag'))

        for i in range(NumberOfDocs):
            self.assertOk(self.cmd('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields', 't', str(i)))

        for i in range(0, NumberOfDocs, 2):
            self.assertEqual(self.cmd('ft.del', 'idx', 'doc%d' % i), 1)

        time.sleep(1)

        res = self.cmd('ft.debug', 'DUMP_TAGIDX', 'idx', 't')
        for r1 in res:
            for r2 in r1[1]:
                self.assertEqual(r2 % 2, 0)
