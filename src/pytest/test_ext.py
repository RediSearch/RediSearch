from rmtest import config
import redis
import unittest
from hotels import hotels
import random
import time
import subprocess
import os
import os.path
from base_case import BaseSearchTestCase

SELF_DIR = os.path.abspath(os.path.dirname(__file__))
DEFAULT_TEST_MODULE = SELF_DIR + '/../tests/ext-example/example.so'
TEST_MODULE = config.get_param('EXT_TEST_PATH', DEFAULT_TEST_MODULE)


class ExtensionTestCase(BaseSearchTestCase):
    @classmethod
    def get_module_args(cls):
        return super(ExtensionTestCase, cls).get_module_args() + ['EXTLOAD', TEST_MODULE]

    def testExt(self):
        if not os.path.exists(TEST_MODULE):
            subprocess.call(['make', '-C', os.path.dirname(TEST_MODULE)])

        r = self
        N = 100
        self.assertOk(r.execute_command(
            'ft.create', 'idx', 'schema', 'f', 'text'))
        for i in range(N):

            self.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                            'f', 'hello world'))
        res = r.execute_command('ft.search', 'idx', 'hello world')
        self.assertEqual(N, res[0])
        res = r.execute_command('ft.search', 'idx', 'hello world', 'scorer', 'filterout_scorer')
        self.assertEqual(0, res[0])


if __name__ == '__main__':
    unittest.main()
