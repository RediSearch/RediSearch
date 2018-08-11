import redis
import unittest
from hotels import hotels
import random
import time
from base_case import BaseSearchTestCase


class AofTestCase(BaseSearchTestCase):
    @classmethod
    def get_server_args(self):
        args = super(AofTestCase, self).get_server_args()
        args['use_aof'] = True
        args['aof-use-rdb-preamble'] = 'yes'
        return args
    
    def setUp(self):
        super(AofTestCase, self).setUp()
        if self.is_external_server():
            raise unittest.SkipTest('Cannot run AOF tests on external server')

    def aofTestCommon(self, reloadfn):
        # TODO: Change this attribute in rmtest

        self.cmd('ft.create', 'idx', 'schema',
                 'field1', 'text', 'field2', 'numeric')
        reloadfn()
        for x in range(1, 10):
            self.assertCmdOk('ft.add', 'idx', 'doc{}'.format(x), 1.0 / x, 'fields',
                             'field1', 'myText{}'.format(x), 'field2', 20 * x)
        exp = [9L, 'doc1', ['field1', 'myText1', 'field2', '20'], 'doc2', ['field1', 'myText2', 'field2', '40'], 'doc3', ['field1', 'myText3', 'field2', '60'], 'doc4', ['field1', 'myText4', 'field2', '80'], 'doc5', ['field1',
                                                                                                                                                                                                                        'myText5', 'field2', '100'], 'doc6', ['field1', 'myText6', 'field2', '120'], 'doc7', ['field1', 'myText7', 'field2', '140'], 'doc8', ['field1', 'myText8', 'field2', '160'], 'doc9', ['field1', 'myText9', 'field2', '180']]
        reloadfn()
        ret = self.cmd('ft.search', 'idx', 'myt*')
        self.assertEqual(exp, ret)

    def testAof(self):
        self.aofTestCommon(lambda: self.restart_and_reload())

    def testRawAof(self):
        self.aofTestCommon(lambda: self.cmd('debug loadaof'))

    def testRewriteAofSortables(self):
        self.cmd('FT.CREATE', 'idx', 'schema', 'field1', 'TEXT',
                 'SORTABLE', 'num1', 'NUMERIC', 'SORTABLE')
        self.cmd('FT.ADD', 'idx', 'doc', 1.0,
                 'FIELDS', 'field1', 'Hello World')
        self.restart_and_reload()
        self.cmd('SAVE')

        from random import randint

        # Load some documents
        for x in xrange(100):
            self.cmd('FT.ADD', 'idx', 'doc{}'.format(x), 1.0, 'FIELDS',
                     'field1', 'txt{}'.format(random.random()),
                     'num1', random.random())
        for sspec in [('field1', 'asc'), ('num1', 'desc')]:
            cmd = ['FT.SEARCH', 'idx', 'txt', 'SORTBY', sspec[0], sspec[1]]
            res = self.cmd(*cmd)
            self.restart_and_reload()
            res2 = self.cmd(*cmd)
            self.assertEqual(res, res2)

    def testAofRewriteSortkeys(self):
        self.cmd('FT.CREATE', 'idx', 'SCHEMA', 'foo',
                 'TEXT', 'SORTABLE', 'bar', 'TAG')
        self.cmd('FT.ADD', 'idx', '1', '1', 'FIELDS', 'foo', 'A', 'bar', '1')
        self.cmd('FT.ADD', 'idx', '2', '1', 'fields', 'foo', 'B', 'bar', '1')

        res_exp = self.cmd('FT.SEARCH', 'idx', '@bar:{1}', 'SORTBY', 'foo', 'ASC',
                           'RETURN', '1', 'foo', 'WITHSORTKEYS')

        self.restart_and_reload()
        res_got = self.cmd('FT.SEARCH', 'idx', '@bar:{1}', 'SORTBY', 'foo', 'ASC',
                           'RETURN', '1', 'foo', 'WITHSORTKEYS')

        self.assertEqual(res_exp, res_got)

    def testAofRewriteTags(self):
        self.cmd('FT.CREATE', 'idx', 'SCHEMA', 'foo',
                 'TEXT', 'SORTABLE', 'bar', 'TAG')
        self.cmd('FT.ADD', 'idx', '1', '1', 'FIELDS', 'foo', 'A', 'bar', '1')
        self.cmd('FT.ADD', 'idx', '2', '1', 'fields', 'foo', 'B', 'bar', '1')

        info_a = to_dict(self.cmd('FT.INFO', 'idx'))
        self.restart_and_reload()
        info_b = to_dict(self.cmd('FT.INFO', 'idx'))
        self.assertEqual(info_a['fields'], info_b['fields'])

        # Try to drop the schema
        self.cmd('FT.DROP', 'idx')

        # Try to create it again - should work!
        self.cmd('FT.CREATE', 'idx', 'SCHEMA', 'foo',
                 'TEXT', 'SORTABLE', 'bar', 'TAG')
        self.cmd('FT.ADD', 'idx', '1', '1', 'FIELDS', 'foo', 'A', 'bar', '1')
        self.cmd('FT.ADD', 'idx', '2', '1', 'fields', 'foo', 'B', 'bar', '1')
        res = self.cmd('FT.SEARCH', 'idx', '@bar:{1}', 'SORTBY', 'foo', 'ASC',
                       'RETURN', '1', 'foo', 'WITHSORTKEYS')
        self.assertEqual([2L, '1', '$a', ['foo', 'A'],
                          '2', '$b', ['foo', 'B']], res)

def to_dict(r):
    return {r[i]: r[i + 1] for i in range(0, len(r), 2)}
