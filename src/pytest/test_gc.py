from rmtest import ModuleTestCase
import redis
import unittest
from hotels import hotels
import random
import time


class SearchGCTestCase(ModuleTestCase('../redisearch.so')):

    def testBasicGC(self):
        with self.redis() as r:
            r.flushdb()
            self.assertOk(r.execute_command(
                'ft.create', 'idx', 'schema', 'title', 'text', 'id', 'numeric'))
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                            'title', 'hello world',
                                            'id', '5'))

            self.assertOk(r.execute_command('ft.add', 'idx', 'doc2', 1.0, 'fields',
                                            'title', 'hello world 1',
                                            'id', '7'))

            self.assertEqual(r.execute_command('ft.debug', 'DUMP_INVIDX', 'idx', 'world'), [1, 2])
            self.assertEqual(r.execute_command('ft.debug', 'DUMP_NUMIDX', 'idx', 'id'), [[1, 2], [2], [1]])

            self.assertEqual(r.execute_command('ft.del', 'idx', 'doc2'), 1)

            time.sleep(1)

            # check that the gc collected the deleted docs
            self.assertEqual(r.execute_command('ft.debug', 'DUMP_INVIDX', 'idx', 'world'), [1])
            self.assertEqual(r.execute_command('ft.debug', 'DUMP_NUMIDX', 'idx', 'id'), [[1], [], [1]])
