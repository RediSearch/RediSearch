import redis
import unittest
from hotels import hotels
import random
import time
from base_case import BaseSearchTestCase


class SearchTestCase(BaseSearchTestCase):
    @classmethod
    def get_module_args(cls):
        return super(SearchTestCase, cls).get_module_args () + ['MAXDOCTABLESIZE', '100']

    # mainly this test adding and removing docs while the doc table size is 100
    # and make sure we are not crashing and not leaking memory (when runs with valgrind).
    def testDocTable(self):
        r = self
        self.assertOk(r.execute_command(
            'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
        # doc table size is 100 so insearting 1000 docs should gives us 10 docs in each bucket
        for i in range(1000):
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                            'title', 'hello world %d' % (i % 100),
                                            'body', 'lorem ist ipsum'))

        for i in range(100):
            res = r.execute_command('ft.search', 'idx', 'hello world %d' % i)
            self.assertEqual(res[0], 10)

        # deleting the first 100 docs
        for i in range(100):
            self.assertEqual(r.execute_command('ft.del', 'idx', 'doc%d' % i), 1)

        for i in range(100):
            res = r.execute_command('ft.search', 'idx', 'hello world %d' % i)
            self.assertEqual(res[0], 9)

        self.assertOk(r.execute_command('ft.drop', 'idx'))
