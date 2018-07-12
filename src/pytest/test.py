# -*- coding: utf-8 -*-

from rmtest import BaseModuleTestCase
import redis
import unittest
from hotels import hotels
import random
import time


class SearchTestCase(BaseModuleTestCase):
    def testAdd(self):
        r = self
        self.assertOk(r.execute_command(
            'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
        self.assertTrue(r.exists('idx:idx'))
        self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                        'title', 'hello world',
                                        'body', 'lorem ist ipsum'))

        for _ in r.retry_with_rdb_reload():
            prefix = 'ft'
            self.assertExists(r, prefix + ':idx/hello')
            self.assertExists(r, prefix + ':idx/world')
            self.assertExists(r, prefix + ':idx/lorem')

    def testConditionalUpdate(self):
        self.assertOk(self.cmd(
            'ft.create', 'idx', 'schema', 'foo', 'text', 'bar', 'numeric', 'sortable'))
        self.assertOk(self.cmd('ft.add', 'idx', '1', '1',
                               'fields', 'foo', 'hello', 'bar', '123'))
        self.assertOk(self.cmd('ft.add', 'idx', '1', '1', 'replace', 'if',
                               '@foo == "hello"', 'fields', 'foo', 'world', 'bar', '123'))
        self.assertEqual('NOADD', self.cmd('ft.add', 'idx', '1', '1', 'replace',
                                           'if', '@foo == "hello"', 'fields', 'foo', 'world', 'bar', '123'))
        self.assertEqual('NOADD', self.cmd('ft.add', 'idx', '1', '1', 'replace',
                                           'if', '1 == 2', 'fields', 'foo', 'world', 'bar', '123'))
        self.assertOk(self.cmd('ft.add', 'idx', '1', '1', 'replace', 'partial', 'if',
                               '@foo == "world"', 'fields', 'bar', '234'))
        self.assertOk(self.cmd('ft.add', 'idx', '1', '1', 'replace', 'if',
                               '@bar == 234', 'fields', 'foo', 'hello', 'bar', '123'))
        
        # Ensure that conditionals are ignored if the document doesn't exist
        self.assertOk(self.cmd('FT.ADD', 'idx', '666', '1', 'IF', '@bar > 42', 'FIELDS', 'bar', '15'))
        # Ensure that it fails if we try again, because it already exists
        self.assertEqual('NOADD', self.cmd('FT.ADD', 'idx', '666', '1', 'REPLACE', 'IF', '@bar > 42', 'FIELDS', 'bar', '15'))
        # Ensure that it fails because we're not using 'REPLACE'
        with self.assertResponseError():
            self.assertOk(self.cmd('FT.ADD', 'idx', '666', '1', 'IF', '@bar > 42', 'FIELDS', 'bar', '15'))

    def testUnionIdList(self):
        # Regression test for https://github.com/RedisLabsModules/RediSearch/issues/306
        r = self
        N = 100
        self.assertOk(r.execute_command(
            "ft.create", "test", "SCHEMA",  "tags", "TAG", "waypoint", "GEO"))
        self.assertOk(r.execute_command(
            "ft.add", "test", "1", "1", "FIELDS", "tags", "alberta", "waypoint", "-113.524,53.5244"))
        self.assertOk(r.execute_command(
            "ft.add", "test", "2", "1", "FIELDS", "tags", "ontario", "waypoint", "-79.395,43.661667"))
        res = r.execute_command(
            'ft.search', 'test', "@waypoint:[-113.52 53.52 20 mi]|@tags:{ontario}", 'nocontent')
        self.assertEqual(res, [2, '2', '1'])

    def testAttributes(self):
        self.assertOk(self.cmd('ft.create', 'idx', 'schema',
                               'title', 'text', 'body', 'text'))
        self.assertOk(self.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                                'title', 't1 t2', 'body', 't3 t4 t5'))
        self.assertOk(self.cmd('ft.add', 'idx', 'doc2', 1.0, 'fields',
                               'body', 't1 t2', 'title', 't3 t5'))

        res = self.cmd(
            'ft.search', 'idx', '(@title:(t1 t2) => {$weight: 0.2}) |(@body:(t1 t2) => {$weight: 0.5})', 'nocontent')
        self.assertListEqual([2L, 'doc2', 'doc1'], res)
        res = self.cmd(
            'ft.search', 'idx', '(@title:(t1 t2) => {$weight: 2.5}) |(@body:(t1 t2) => {$weight: 0.5})', 'nocontent')
        self.assertListEqual([2L, 'doc1', 'doc2'], res)

        res = self.cmd(
            'ft.search', 'idx', '(t3 t5) => {$slop: 4}', 'nocontent')
        self.assertListEqual([2L, 'doc2', 'doc1'], res)
        res = self.cmd(
            'ft.search', 'idx', '(t5 t3) => {$slop: 0}', 'nocontent')
        self.assertListEqual([1L, 'doc2'], res)
        res = self.cmd(
            'ft.search', 'idx', '(t5 t3) => {$slop: 0; $inorder:true}', 'nocontent')
        self.assertListEqual([0], res)

    def testUnion(self):
        N = 100
        r = self
        self.assertOk(r.execute_command(
            'ft.create', 'idx', 'schema', 'f', 'text'))
        for i in range(N):

            self.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                            'f', 'hello world' if i % 2 == 0 else 'hallo werld'))

        for _ in r.retry_with_rdb_reload():
            res = r.execute_command(
                'ft.search', 'idx', 'hello|hallo', 'nocontent', 'limit', '0', '100')
            self.assertEqual(N + 1, len(res))
            self.assertEqual(N, res[0])

            res = r.execute_command(
                'ft.search', 'idx', 'hello|world', 'nocontent', 'limit', '0', '100')
            self.assertEqual(51, len(res))
            self.assertEqual(50, res[0])

            res = r.execute_command('ft.search', 'idx', '(hello|hello)(world|world)',
                                    'nocontent', 'verbatim', 'limit', '0', '100')
            self.assertEqual(51, len(res))
            self.assertEqual(50, res[0])

            res = r.execute_command(
                'ft.search', 'idx', '(hello|hallo)(werld|world)', 'nocontent', 'verbatim', 'limit', '0', '100')
            self.assertEqual(101, len(res))
            self.assertEqual(100, res[0])

            res = r.execute_command(
                'ft.search', 'idx', '(hallo|hello)(world|werld)', 'nocontent', 'verbatim', 'limit', '0', '100')
            self.assertEqual(101, len(res))
            self.assertEqual(100, res[0])

            res = r.execute_command(
                'ft.search', 'idx', '(hello|werld)(hallo|world)', 'nocontent', 'verbatim', 'limit', '0', '100')
            self.assertEqual(101, len(res))
            self.assertEqual(100, res[0])

            res = r.execute_command(
                'ft.search', 'idx', '(hello|hallo) world', 'nocontent', 'verbatim', 'limit', '0', '100')
            self.assertEqual(51, len(res))
            self.assertEqual(50, res[0])

            res = r.execute_command(
                'ft.search', 'idx', '(hello world)|((hello world)|(hallo world|werld) | hello world werld)', 'nocontent', 'verbatim', 'limit', '0', '100')
            self.assertEqual(101, len(res))
            self.assertEqual(100, res[0])

    def testSearch(self):
        r = self
        self.assertOk(r.execute_command(
            'ft.create', 'idx', 'schema', 'title', 'text', 'weight', 10.0, 'body', 'text'))
        self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 0.5, 'fields',
                                        'title', 'hello world',
                                        'body', 'lorem ist ipsum'))
        self.assertOk(r.execute_command('ft.add', 'idx', 'doc2', 1.0, 'fields',
                                        'title', 'hello another world',
                                        'body', 'lorem ist ipsum lorem lorem'))
        for _ in r.retry_with_rdb_reload():

            res = r.execute_command('ft.search', 'idx', 'hello')

            self.assertTrue(len(res) == 5)
            self.assertEqual(res[0], 2L)
            self.assertEqual(res[1], "doc2")
            self.assertTrue(isinstance(res[2], list))
            self.assertTrue('title' in res[2])
            self.assertTrue('hello another world' in res[2])
            self.assertEqual(res[3], "doc1")
            self.assertTrue('hello world' in res[4])

            # Test empty query
            res = r.execute_command('ft.search', 'idx', '')
            self.assertListEqual([0], res)

            # Test searching with no content
            res = r.execute_command(
                'ft.search', 'idx', 'hello', 'nocontent')
            self.assertTrue(len(res) == 3)
            self.assertEqual(res[0], 2L)
            self.assertEqual(res[1], "doc2")
            self.assertEqual(res[2], "doc1")

            # Test searching WITHSCORES
            res = r.execute_command(
                'ft.search', 'idx', 'hello', 'WITHSCORES')
            self.assertEqual(len(res), 7)
            self.assertEqual(res[0], 2L)
            self.assertEqual(res[1], "doc2")
            self.assertTrue(float(res[2]) > 0)
            self.assertEqual(res[4], "doc1")
            self.assertTrue(float(res[5]) > 0)

            # Test searching WITHSCORES NOCONTENT
            res = r.execute_command(
                'ft.search', 'idx', 'hello', 'WITHSCORES', 'NOCONTENT')
            self.assertEqual(len(res), 5)
            self.assertEqual(res[0], 2L)
            self.assertEqual(res[1], "doc2")
            self.assertTrue(float(res[2]) > 0)
            self.assertEqual(res[3], "doc1")
            self.assertTrue(float(res[4]) > 0)

    def testSearchNosave(self):
        # Check to see what happens when we try to return unsaved documents
        self.cmd('ft.create', 'idx', 'SCHEMA', 'f1', 'text')
        # Add 3 documents
        for x in range(3):
            self.cmd('ft.add', 'idx', 'doc{}'.format(x),
                     1.0, 'NOSAVE', 'FIELDS', 'f1', 'value')

        # Now query the results
        res = self.cmd('ft.search', 'idx', 'value')
        self.assertEqual(3, res[0])
        for content in res[2::2]:
            self.assertEqual([], content)

    def testGet(self):
        r = self
        self.assertOk(r.execute_command(
            'ft.create', 'idx', 'schema', 'foo', 'text', 'bar', 'text'))

        for i in range(100):
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                            'foo', 'hello world', 'bar', 'wat wat'))

        for i in range(100):
            res = r.execute_command('ft.get', 'idx', 'doc%d' % i)
            self.assertIsNotNone(res)
            self.assertListEqual(
                ['foo', 'hello world', 'bar', 'wat wat'], res)
            self.assertIsNone(r.execute_command(
                'ft.get', 'idx', 'doc%dsdfsd' % i))

        rr = r.execute_command(
            'ft.mget', 'idx', *('doc%d' % i for i in range(100)))
        self.assertEqual(len(rr), 100)
        for res in rr:
            self.assertIsNotNone(res)
            self.assertListEqual(
                ['foo', 'hello world', 'bar', 'wat wat'], res)
        rr = r.execute_command(
            'ft.mget', 'idx', *('doc-%d' % i for i in range(100)))
        self.assertEqual(len(rr), 100)
        for res in rr:
            self.assertIsNone(res)

    def testDelete(self):
        r = self
        self.assertOk(r.execute_command(
            'ft.create', 'idx', 'schema', 'f', 'text'))

        for i in range(100):
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                            'f', 'hello world'))

        for i in range(100):
            # the doc hash should exist now
            self.assertTrue(r.exists('doc%d' % i))
            # Delete the actual docs only half of the time
            self.assertEqual(1, r.execute_command(
                'ft.del', 'idx', 'doc%d' % i, 'DD' if i % 2 == 0 else ''))
            # second delete should return 0
            self.assertEqual(0, r.execute_command(
                'ft.del', 'idx', 'doc%d' % i))

            # After del with DD the doc hash should not exist
            if i % 2 == 0:
                self.assertFalse(r.exists('doc%d' % i))
            else:
                self.assertTrue(r.exists('doc%d' % i))
            res = r.execute_command(
                'ft.search', 'idx', 'hello', 'nocontent', 'limit', 0, 100)
            self.assertNotIn('doc%d' % i, res)
            self.assertEqual(res[0], 100 - i - 1)
            self.assertEqual(len(res), 100 - i)

            # test reinsertion
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                            'f', 'hello world'))
            res = r.execute_command(
                'ft.search', 'idx', 'hello', 'nocontent', 'limit', 0, 100)
            self.assertIn('doc%d' % i, res)
            self.assertEqual(1, r.execute_command(
                'ft.del', 'idx', 'doc%d' % i))
        for _ in r.retry_with_rdb_reload():
            did = 'rrrr'
            self.assertOk(r.execute_command('ft.add', 'idx', did, 1, 'fields',
                                            'f', 'hello world'))
            self.assertEqual(1, r.execute_command('ft.del', 'idx', did))
            self.assertEqual(0, r.execute_command('ft.del', 'idx', did))
            self.assertOk(r.execute_command('ft.add', 'idx', did, 1, 'fields',
                                            'f', 'hello world'))
            self.assertEqual(1, r.execute_command('ft.del', 'idx', did))
            self.assertEqual(0, r.execute_command('ft.del', 'idx', did))

    def testReplace(self):
        r = self

        self.assertOk(r.execute_command(
            'ft.create', 'idx', 'schema', 'f', 'text'))

        self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                        'f', 'hello world'))
        self.assertOk(r.execute_command('ft.add', 'idx', 'doc2', 1.0, 'fields',
                                        'f', 'hello world'))
        res = r.execute_command(
            'ft.search', 'idx', 'hello world')
        self.assertEqual(2, res[0])

        with self.assertResponseError():
            # make sure we can't insert a doc twice
            res = r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                    'f', 'hello world')

        # now replace doc1 with a different content
        self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'replace', 'fields',
                                        'f', 'goodbye universe'))

        for _ in r.retry_with_rdb_reload():
            # make sure the query for hello world does not return the replaced
            # document
            res = r.execute_command(
                'ft.search', 'idx', 'hello world', 'nocontent')
            self.assertEqual(1, res[0])
            self.assertEqual('doc2', res[1])

            # search for the doc's new content
            res = r.execute_command(
                'ft.search', 'idx', 'goodbye universe', 'nocontent')
            self.assertEqual(1, res[0])
            self.assertEqual('doc1', res[1])

    def testDrop(self):
        r = self
        self.assertOk(r.execute_command(
            'ft.create', 'idx', 'schema', 'f', 'text', 'n', 'numeric', 't', 'tag', 'g', 'geo'))

        for i in range(100):
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                            'f', 'hello world', 'n', 666, 't', 'foo bar',
                                            'g', '19.04,47.497'))
        keys = r.keys('*')
        self.assertEqual(106, len(keys))

        self.assertOk(r.execute_command('ft.drop', 'idx'))
        keys = r.keys('*')
        self.assertEqual(0, len(keys))

        # Now do the same with KEEPDOCS
        self.assertOk(r.execute_command(
            'ft.create', 'idx', 'schema', 'f', 'text', 'n', 'numeric', 't', 'tag', 'g', 'geo'))

        for i in range(100):
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                            'f', 'hello world', 'n', 666, 't', 'foo bar',
                                            'g', '19.04,47.497'))
        keys = r.keys('*')
        self.assertEqual(106, len(keys))

        self.assertOk(r.execute_command('ft.drop', 'idx', 'KEEPDOCS'))
        keys = r.keys('*')
        self.assertListEqual(['doc0', 'doc1', 'doc10', 'doc11', 'doc12', 'doc13', 'doc14', 'doc15', 'doc16', 'doc17', 'doc18', 'doc19', 'doc2', 'doc20', 'doc21', 'doc22', 'doc23', 'doc24', 'doc25', 'doc26', 'doc27', 'doc28', 'doc29', 'doc3', 'doc30', 'doc31', 'doc32', 'doc33', 'doc34', 'doc35', 'doc36', 'doc37', 'doc38', 'doc39', 'doc4', 'doc40', 'doc41', 'doc42', 'doc43', 'doc44', 'doc45', 'doc46', 'doc47', 'doc48', 'doc49', 'doc5', 'doc50', 'doc51', 'doc52', 'doc53',
                              'doc54', 'doc55', 'doc56', 'doc57', 'doc58', 'doc59', 'doc6', 'doc60', 'doc61', 'doc62', 'doc63', 'doc64', 'doc65', 'doc66', 'doc67', 'doc68', 'doc69', 'doc7', 'doc70', 'doc71', 'doc72', 'doc73', 'doc74', 'doc75', 'doc76', 'doc77', 'doc78', 'doc79', 'doc8', 'doc80', 'doc81', 'doc82', 'doc83', 'doc84', 'doc85', 'doc86', 'doc87', 'doc88', 'doc89', 'doc9', 'doc90', 'doc91', 'doc92', 'doc93', 'doc94', 'doc95', 'doc96', 'doc97', 'doc98', 'doc99'], sorted(keys))

    def testCustomStopwords(self):
        r = self
        # Index with default stopwords
        self.assertOk(r.execute_command(
            'ft.create', 'idx', 'schema', 'foo', 'text'))

        # Index with custom stopwords
        self.assertOk(r.execute_command('ft.create', 'idx2', 'stopwords', 2, 'hello', 'world',
                                        'schema', 'foo', 'text'))
        # Index with NO stopwords
        self.assertOk(r.execute_command('ft.create', 'idx3', 'stopwords', 0,
                                        'schema', 'foo', 'text'))

        for idx in ('idx', 'idx2', 'idx3'):
            self.assertOk(r.execute_command(
                'ft.add', idx, 'doc1', 1.0, 'fields', 'foo', 'hello world'))
            self.assertOk(r.execute_command(
                'ft.add', idx, 'doc2', 1.0, 'fields', 'foo', 'to be or not to be'))

        for _ in r.retry_with_rdb_reload():
            # Normal index should return results just for 'hello world'
            self.assertEqual([1, 'doc1'],  r.execute_command(
                'ft.search', 'idx', 'hello world', 'nocontent'))
            self.assertEqual([0],  r.execute_command(
                'ft.search', 'idx', 'to be or not', 'nocontent'))

            # Custom SW index should return results just for 'to be or not'
            self.assertEqual([0],  r.execute_command(
                'ft.search', 'idx2', 'hello world', 'nocontent'))
            self.assertEqual([1, 'doc2'],  r.execute_command(
                'ft.search', 'idx2', 'to be or not', 'nocontent'))

            # No SW index should return results for both
            self.assertEqual([1, 'doc1'],  r.execute_command(
                'ft.search', 'idx3', 'hello world', 'nocontent'))
            self.assertEqual([1, 'doc2'],  r.execute_command(
                'ft.search', 'idx3', 'to be or not', 'nocontent'))

    def testStopwords(self):
        # This test was taken from Python's tests, and failed due to some changes
        # made earlier
        self.cmd('ft.create', 'idx', 'stopwords', 3, 'foo',
                 'bar', 'baz', 'schema', 'txt', 'text')
        self.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields', 'txt', 'foo bar')
        self.cmd('ft.add', 'idx', 'doc2', 1.0, 'fields', 'txt', 'hello world')

        r1 = self.cmd('ft.search', 'idx', 'foo bar', 'nocontent')
        r2 = self.cmd('ft.search', 'idx', 'foo bar hello world', 'nocontent')
        self.assertEqual(0, r1[0])
        self.assertEqual(1, r2[0])

    def testNoStopwords(self):
        # This test taken from Java's test suite
        self.cmd('ft.create', 'idx', 'schema', 'title', 'text')
        for i in range(100):
            self.cmd('ft.add', 'idx', 'doc{}'.format(i), 1.0, 'fields',
                     'title', 'hello world' if i % 2 == 0 else 'hello worlds')

        res = self.cmd('ft.search', 'idx', 'hello a world', 'NOCONTENT')
        self.assertEqual(100, res[0])

        res = self.cmd('ft.search', 'idx', 'hello a world',
                       'VERBATIM', 'NOCONTENT')
        self.assertEqual(50, res[0])

        res = self.cmd('ft.search', 'idx', 'hello a world', 'NOSTOPWORDS')
        self.assertEqual(0, res[0])

    def testOptional(self):
        r = self
        self.assertOk(r.execute_command(
            'ft.create', 'idx', 'schema', 'foo', 'text'))
        self.assertOk(r.execute_command('ft.add', 'idx',
                                        'doc1', 1.0, 'fields', 'foo', 'hello wat woot'))
        self.assertOk(r.execute_command('ft.add', 'idx', 'doc2',
                                        1.0, 'fields', 'foo', 'hello world woot'))
        self.assertOk(r.execute_command('ft.add', 'idx', 'doc3',
                                        1.0, 'fields', 'foo', 'hello world werld'))

        res = r.execute_command('ft.search', 'idx', 'hello', 'nocontent')
        self.assertEqual([3L, 'doc3', 'doc2', 'doc1'], res)
        res = r.execute_command(
            'ft.search', 'idx', 'hello world', 'nocontent', 'scorer', 'DISMAX')
        self.assertEqual([2L, 'doc3', 'doc2'], res)
        res = r.execute_command(
            'ft.search', 'idx', 'hello ~world', 'nocontent', 'scorer', 'DISMAX')
        self.assertEqual([3L, 'doc3', 'doc2', 'doc1'], res)
        res = r.execute_command(
            'ft.search', 'idx', 'hello ~world ~werld', 'nocontent', 'scorer', 'DISMAX')
        self.assertEqual([3L, 'doc3', 'doc2', 'doc1'], res)

    def testExplain(self):
        r = self
        self.assertOk(r.execute_command(
            'ft.create', 'idx', 'schema', 'foo', 'text', 'bar', 'numeric', 'sortable'))
        q = '(hello world) "what what" hello|world @bar:[10 100]|@bar:[200 300]'
        res = r.execute_command('ft.explain', 'idx', q)
        # print res.replace('\n', '\\n')
        expected = """INTERSECT {\n  UNION {\n    hello\n    +hello(expanded)\n  }\n  UNION {\n    world\n    +world(expanded)\n  }\n  EXACT {\n    what\n    what\n  }\n  UNION {\n    UNION {\n      hello\n      +hello(expanded)\n    }\n    UNION {\n      world\n      +world(expanded)\n    }\n  }\n  UNION {\n    NUMERIC {10.000000 <= @bar <= 100.000000}\n    NUMERIC {200.000000 <= @bar <= 300.000000}\n  }\n}\n"""
        self.assertEqual(res, expected)

    def testNoIndex(self):
        r = self
        self.assertOk(r.execute_command(
            'ft.create', 'idx', 'schema',
            'foo', 'text',
            'num', 'numeric', 'sortable', 'noindex',
            'extra', 'text', 'noindex', 'sortable'))
        self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', '0.1', 'fields',
                                        'foo', 'hello world', 'num', 1, 'extra', 'hello lorem ipsum'))
        res = r.execute_command(
            'ft.search', 'idx', 'hello world', 'nocontent')
        self.assertListEqual([1, 'doc1'], res)
        res = r.execute_command(
            'ft.search', 'idx', 'lorem ipsum', 'nocontent')
        self.assertListEqual([0], res)
        res = r.execute_command(
            'ft.search', 'idx', '@extra:hello', 'nocontent')
        self.assertListEqual([0], res)
        res = r.execute_command(
            'ft.search', 'idx', '@num:[1 1]', 'nocontent')
        self.assertListEqual([0], res)

    def testPartial(self):
        r = self
        self.assertOk(r.execute_command(
            'ft.create', 'idx', 'schema',
            'foo', 'text',
            'num', 'numeric', 'sortable', 'noindex',
            'extra', 'text', 'noindex'))
        # print r.execute_command('ft.info', 'idx')

        self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', '0.1', 'fields',
                                        'foo', 'hello world', 'num', 1, 'extra', 'lorem ipsum'))
        self.assertOk(r.execute_command('ft.add', 'idx', 'doc2', '0.1', 'fields',
                                        'foo', 'hello world', 'num', 2, 'extra', 'abba'))
        res = r.execute_command('ft.search', 'idx', 'hello world',
                                'sortby', 'num', 'asc', 'nocontent', 'withsortkeys')
        self.assertListEqual([2L, 'doc1', '#1', 'doc2', '#2'], res)
        res = r.execute_command('ft.search', 'idx', 'hello world',
                                'sortby', 'num', 'desc', 'nocontent', 'withsortkeys')
        self.assertListEqual([2L, 'doc2', '#2', 'doc1', '#1'], res)

        # Updating non indexed fields doesn't affect search results
        self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', '0.1', 'replace', 'partial',
                                        'fields', 'num', 3, 'extra', 'jorem gipsum'))
        res = r.execute_command(
            'ft.search', 'idx', 'hello world', 'sortby', 'num', 'desc',)
        self.assertListEqual([2L, 'doc1', ['foo', 'hello world', 'num', '3', 'extra', 'jorem gipsum'],
                              'doc2', ['foo', 'hello world', 'num', '2', 'extra', 'abba']], res)
        res = r.execute_command(
            'ft.search', 'idx', 'hello', 'nocontent', 'withscores')
        # Updating only indexed field affects search results
        self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', '0.1', 'replace', 'partial',
                                        'fields', 'foo', 'wat wet'))
        res = r.execute_command(
            'ft.search', 'idx', 'hello world', 'nocontent')
        self.assertListEqual([1L, 'doc2'], res)
        res = r.execute_command('ft.search', 'idx', 'wat', 'nocontent')
        self.assertListEqual([1L, 'doc1'], res)

        # Test updating of score and no fields
        res = r.execute_command(
            'ft.search', 'idx', 'wat', 'nocontent', 'withscores')
        self.assertLess(float(res[2]), 1)
        # self.assertListEqual([1L, 'doc1'], res)
        self.assertOk(r.execute_command('ft.add', 'idx',
                                        'doc1', '1.0', 'replace', 'partial', 'fields'))
        res = r.execute_command(
            'ft.search', 'idx', 'wat', 'nocontent', 'withscores')
        self.assertGreater(float(res[2]), 1)

        # Test updating payloads
        res = r.execute_command(
            'ft.search', 'idx', 'wat', 'nocontent', 'withpayloads')
        self.assertIsNone(res[2])
        self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', '1.0',
                                        'replace', 'partial', 'payload', 'foobar', 'fields'))
        res = r.execute_command(
            'ft.search', 'idx', 'wat', 'nocontent', 'withpayloads')
        self.assertEqual('foobar', res[2])

    def testPaging(self):
        r = self
        self.assertOk(r.execute_command(
            'ft.create', 'idx', 'schema', 'foo', 'text', 'bar', 'numeric', 'sortable'))
        N = 100
        for i in range(N):
            self.assertOk(r.execute_command('ft.add', 'idx', '%d' % i, 1, 'fields',
                                            'foo', 'hello', 'bar', i))

        chunk = 7
        offset = 0
        while True:

            res = r.execute_command(
                'ft.search', 'idx', 'hello', 'nocontent', 'limit', offset, chunk)
            self.assertEqual(res[0], N)

            if offset + chunk > N:
                self.assertTrue(len(res) - 1 <= chunk)
                break
            self.assertEqual(len(res), chunk + 1)
            for n, id in enumerate(res[1:]):
                self.assertEqual(int(id), N - 1 - (offset + n))
            offset += chunk
            chunk = random.randrange(1, 10)
        res = r.execute_command(
            'ft.search', 'idx', 'hello', 'nocontent', 'sortby', 'bar', 'asc', 'limit', N, 10)
        self.assertEqual(res[0], N)
        self.assertEqual(len(res), 1)

        with self.assertResponseError():
            r.execute_command(
                'ft.search', 'idx', 'hello', 'nocontent', 'limit', 0, -1)
        with self.assertResponseError():
            r.execute_command(
                'ft.search', 'idx', 'hello', 'nocontent', 'limit', -1, 10)
        with self.assertResponseError():
            r.execute_command(
                'ft.search', 'idx', 'hello', 'nocontent', 'limit', 0, 2000000)

    def testPrefix(self):
        r = self
        self.assertOk(r.execute_command(
            'ft.create', 'idx', 'schema', 'foo', 'text'))
        N = 100
        for i in range(N):
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                            'foo', 'constant term%d' % (random.randrange(0, 5))))
        for _ in r.retry_with_rdb_reload():
            res = r.execute_command(
                'ft.search', 'idx', 'constant term', 'nocontent')
            self.assertEqual([0], res)
            res = r.execute_command(
                'ft.search', 'idx', 'constant term*', 'nocontent')
            self.assertEqual(N, res[0])
            res = r.execute_command(
                'ft.search', 'idx', 'const* term*', 'nocontent')
            self.assertEqual(N, res[0])
            res = r.execute_command(
                'ft.search', 'idx', 'constant term1*', 'nocontent')
            self.assertGreater(res[0], 2)
            res = r.execute_command(
                'ft.search', 'idx', 'const* -term*', 'nocontent')
            self.assertEqual([0], res)
            res = r.execute_command(
                'ft.search', 'idx', 'constant term9*', 'nocontent')
            self.assertEqual([0], res)

    def testSortBy(self):
        r = self
        self.assertOk(r.execute_command(
            'ft.create', 'idx', 'schema', 'foo', 'text', 'sortable', 'bar', 'numeric', 'sortable'))
        N = 100
        for i in range(N):
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                            'foo', 'hello%03d world' % i, 'bar', 100 - i))
        for _ in r.retry_with_rdb_reload():

            res = r.execute_command(
                'ft.search', 'idx', 'world', 'nocontent', 'sortby', 'foo')
            self.assertEqual([100L, 'doc0', 'doc1', 'doc2', 'doc3',
                              'doc4', 'doc5', 'doc6', 'doc7', 'doc8', 'doc9'], res)
            res = r.execute_command(
                'ft.search', 'idx', 'world', 'nocontent', 'sortby', 'foo', 'desc')
            self.assertEqual([100L, 'doc99', 'doc98', 'doc97', 'doc96',
                              'doc95', 'doc94', 'doc93', 'doc92', 'doc91', 'doc90'], res)
            res = r.execute_command(
                'ft.search', 'idx', 'world', 'nocontent', 'sortby', 'bar', 'desc')
            self.assertEqual([100L, 'doc0', 'doc1', 'doc2', 'doc3',
                              'doc4', 'doc5', 'doc6', 'doc7', 'doc8', 'doc9'], res)
            res = r.execute_command(
                'ft.search', 'idx', 'world', 'nocontent', 'sortby', 'bar', 'asc')
            self.assertEqual([100L, 'doc99', 'doc98', 'doc97', 'doc96',
                              'doc95', 'doc94', 'doc93', 'doc92', 'doc91', 'doc90'], res)
            res = r.execute_command('ft.search', 'idx', 'world', 'nocontent',
                                    'sortby', 'bar', 'desc', 'withscores', 'limit', '2', '5')
            self.assertEqual(
                [100L, 'doc2', '0', 'doc3', '0', 'doc4', '0', 'doc5', '0', 'doc6', '0'], res)

            res = r.execute_command('ft.search', 'idx', 'world', 'nocontent',
                                    'sortby', 'bar', 'desc', 'withsortkeys', 'limit', 0, 5)
            self.assertListEqual(
                [100L, 'doc0', '#100', 'doc1', '#99', 'doc2', '#98', 'doc3', '#97', 'doc4', '#96'], res)
            res = r.execute_command('ft.search', 'idx', 'world', 'nocontent',
                                    'sortby', 'foo', 'desc', 'withsortkeys', 'limit', 0, 5)
            self.assertListEqual([100L, 'doc99', '$hello099 world', 'doc98', '$hello098 world', 'doc97', '$hello097 world', 'doc96',
                                  '$hello096 world', 'doc95', '$hello095 world'], res)

    def testNot(self):
        r = self
        self.assertOk(r.execute_command(
            'ft.create', 'idx', 'schema', 'foo', 'text'))
        N = 10
        for i in range(N):
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                            'foo', 'constant term%d' % (random.randrange(0, 5))))

        for i in range(5):
            inclusive = r.execute_command(
                'ft.search', 'idx', 'constant term%d' % i, 'nocontent', 'limit', 0, N)

            exclusive = r.execute_command(
                'ft.search', 'idx', 'constant -term%d' % i, 'nocontent', 'limit', 0, N)
            exclusive2 = r.execute_command(
                'ft.search', 'idx', '-(term%d)' % i, 'nocontent', 'limit', 0, N)
            exclusive3 = r.execute_command(
                'ft.search', 'idx', '(-term%d) (constant)' % i, 'nocontent', 'limit', 0, N)

            self.assertNotEqual(inclusive[0], N)
            self.assertEqual(inclusive[0] + exclusive[0], N)
            self.assertEqual(exclusive3[0], exclusive2[0])
            self.assertEqual(exclusive3[0], exclusive[0])

            s1, s2, s3, s4 = set(inclusive[1:]), set(
                exclusive[1:]), set(exclusive2[1:]), set(exclusive3[1:])
            self.assertTrue(s1.difference(s2) == s1)
            self.assertTrue(s1.difference(s3) == s1)
            self.assertTrue(s1.difference(s4) == s1)
            self.assertTrue(s2 == s3)
            self.assertTrue(s2 == s4)
            self.assertTrue(s2.intersection(s1) == set())
            self.assertTrue(s3.intersection(s1) == set())
            self.assertTrue(s4.intersection(s1) == set())

        # NOT on a non existing term
        self.assertEqual(r.execute_command(
            'ft.search', 'idx', 'constant -dasdfasdf', 'nocontent')[0], N)
        # not on self term
        self.assertEqual(r.execute_command(
            'ft.search', 'idx', 'constant -constant', 'nocontent'), [0])

        self.assertEqual(r.execute_command(
            'ft.search', 'idx', 'constant -(term0|term1|term2|term3|term4|nothing)', 'nocontent'), [0])
        # self.assertEqual(r.execute_command('ft.search', 'idx', 'constant -(term1 term2)', 'nocontent')[0], N)

    def testNestedIntersection(self):
        r = self
        self.assertOk(r.execute_command(
            'ft.create', 'idx', 'schema', 'a', 'text', 'b', 'text', 'c', 'text', 'd', 'text'))
        for i in range(20):
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                            'a', 'foo', 'b', 'bar', 'c', 'baz', 'd', 'gaz'))
        res = [
            r.execute_command('ft.search', 'idx',
                              'foo bar baz gaz', 'nocontent'),
            r.execute_command('ft.search', 'idx',
                              '@a:foo @b:bar @c:baz @d:gaz', 'nocontent'),
            r.execute_command('ft.search', 'idx',
                              '@b:bar @a:foo @c:baz @d:gaz', 'nocontent'),
            r.execute_command('ft.search', 'idx',
                              '@c:baz @b:bar @a:foo @d:gaz', 'nocontent'),
            r.execute_command('ft.search', 'idx',
                              '@d:gaz @c:baz @b:bar @a:foo', 'nocontent'),
            r.execute_command(
                'ft.search', 'idx', '@a:foo (@b:bar (@c:baz @d:gaz))', 'nocontent'),
            r.execute_command(
                'ft.search', 'idx', '@c:baz (@a:foo (@b:bar (@c:baz @d:gaz)))', 'nocontent'),
            r.execute_command(
                'ft.search', 'idx', '@b:bar (@a:foo (@c:baz @d:gaz))', 'nocontent'),
            r.execute_command(
                'ft.search', 'idx', '@d:gaz (@a:foo (@c:baz @b:bar))', 'nocontent'),
            r.execute_command('ft.search', 'idx',
                              'foo (bar baz gaz)', 'nocontent'),
            r.execute_command('ft.search', 'idx',
                              'foo (bar (baz gaz))', 'nocontent'),
            r.execute_command('ft.search', 'idx',
                              'foo (bar (foo bar) (foo bar))', 'nocontent'),
            r.execute_command('ft.search', 'idx',
                              'foo (foo (bar baz (gaz)))', 'nocontent'),
            r.execute_command('ft.search', 'idx', 'foo (foo (bar (baz (gaz (foo bar (gaz))))))', 'nocontent')]

        for i, r in enumerate(res):
            # print i, res[0], r
            self.assertListEqual(res[0], r)

    def testInKeys(self):
        r = self
        self.assertOk(r.execute_command(
            'ft.create', 'idx', 'schema', 'foo', 'text'))

        for i in range(200):
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                            'foo', 'hello world'))

        for _ in r.retry_with_rdb_reload():

            for keys in (
                ['doc%d' % i for i in range(10)], ['doc%d' % i for i in range(0, 30, 2)], [
                    'doc%d' % i for i in range(99, 0, -5)]
            ):
                res = r.execute_command(
                    'ft.search', 'idx', 'hello world', 'NOCONTENT', 'LIMIT', 0, 100, 'INKEYS', len(keys), *keys)
                self.assertEqual(len(keys), res[0])
                self.assertTrue(all((k in res for k in keys)))

            self.assertEqual(0, r.execute_command(
                'ft.search', 'idx', 'hello world', 'NOCONTENT', 'LIMIT', 0, 100, 'INKEYS', 3, 'foo', 'bar', 'baz')[0])

        with self.assertResponseError():
            self.cmd('ft.search', 'idx', 'hello', 'INKEYS', 99)
        with self.assertResponseError():
            self.cmd('ft.search', 'idx', 'hello', 'INKEYS', -1)
        with self.assertResponseError():
            self.cmd('ft.search', 'idx', 'hello', 'inkeys', 4, 'foo')

    def testSlopInOrder(self):
        r = self
        self.assertOk(r.execute_command(
            'ft.create', 'idx', 'schema', 'title', 'text'))
        self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1, 'fields',
                                        'title', 't1 t2'))
        self.assertOk(r.execute_command('ft.add', 'idx', 'doc2', 1, 'fields',
                                        'title', 't1 t3 t2'))
        self.assertOk(r.execute_command('ft.add', 'idx', 'doc3', 1, 'fields',
                                        'title', 't1 t3 t4 t2'))
        self.assertOk(r.execute_command('ft.add', 'idx', 'doc4', 1, 'fields',
                                        'title', 't1 t3 t4 t5 t2'))

        res = r.execute_command(
            'ft.search', 'idx', 't1|t4 t3|t2', 'slop', '0', 'inorder', 'nocontent')
        self.assertEqual({'doc3', 'doc4', 'doc2', 'doc1'}, set(res[1:]))
        res = r.execute_command(
            'ft.search', 'idx', 't2 t1', 'slop', '0', 'nocontent')
        self.assertEqual(1, res[0])
        self.assertEqual('doc1', res[1])
        self.assertEqual(0, r.execute_command(
            'ft.search', 'idx', 't2 t1', 'slop', '0', 'inorder')[0])
        self.assertEqual(1, r.execute_command(
            'ft.search', 'idx', 't1 t2', 'slop', '0', 'inorder')[0])

        self.assertEqual(2, r.execute_command(
            'ft.search', 'idx', 't1 t2', 'slop', '1', 'inorder')[0])
        self.assertEqual(3, r.execute_command(
            'ft.search', 'idx', 't1 t2', 'slop', '2', 'inorder')[0])
        self.assertEqual(4, r.execute_command(
            'ft.search', 'idx', 't1 t2', 'slop', '3', 'inorder')[0])
        self.assertEqual(4, r.execute_command(
            'ft.search', 'idx', 't1 t2', 'inorder')[0])
        self.assertEqual(0, r.execute_command(
            'ft.search', 'idx', 't t1', 'inorder')[0])
        self.assertEqual(2, r.execute_command(
            'ft.search', 'idx', 't1 t2 t3 t4')[0])
        self.assertEqual(0, r.execute_command(
            'ft.search', 'idx', 't1 t2 t3 t4', 'inorder')[0])

    def testExact(self):
        r = self
        self.assertOk(r.execute_command(
            'ft.create', 'idx', 'schema', 'title', 'text', 'weight', 10.0, 'body', 'text'))
        self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 0.5, 'fields',
                                        'title', 'hello world',
                                        'body', 'lorem ist ipsum'))
        self.assertOk(r.execute_command('ft.add', 'idx', 'doc2', 1.0, 'fields',
                                        'title', 'hello another world',
                                        'body', 'lorem ist ipsum lorem lorem'))

        res = r.execute_command(
            'ft.search', 'idx', '"hello world"', 'verbatim')
        self.assertEqual(3, len(res))
        self.assertEqual(1, res[0])
        self.assertEqual("doc1", res[1])

        res = r.execute_command(
            'ft.search', 'idx', "hello \"another world\"", 'verbatim')
        self.assertEqual(3, len(res))
        self.assertEqual(1, res[0])
        self.assertEqual("doc2", res[1])

    def testGeo(self):
        r = self
        gsearch = lambda query, lon, lat, dist, unit='km': r.execute_command(
            'ft.search', 'idx', query, 'geofilter', 'location', lon, lat, dist, unit)

        gsearch_inline = lambda query, lon, lat, dist, unit='km': r.execute_command(
            'ft.search', 'idx', '{} @location:[{} {} {} {}]'.format(query,  lon, lat, dist, unit))

        self.assertOk(r.execute_command('ft.create', 'idx',
                                        'schema', 'name', 'text', 'location', 'geo'))

        for i, hotel in enumerate(hotels):
            self.assertOk(r.execute_command('ft.add', 'idx', 'hotel{}'.format(i), 1.0, 'fields', 'name',
                                            hotel[0], 'location', '{},{}'.format(hotel[2], hotel[1])))

        for _ in r.retry_with_rdb_reload():
            res = r.execute_command('ft.search', 'idx', 'hilton')
            self.assertEqual(len(hotels), res[0])

            res = gsearch('hilton', "-0.1757", "51.5156", '1')
            self.assertEqual(3, res[0])
            self.assertEqual('hotel2', res[5])
            self.assertEqual('hotel21', res[3])
            self.assertEqual('hotel79', res[1])
            res2 = gsearch_inline('hilton', "-0.1757", "51.5156", '1')
            self.assertListEqual(res, res2)

            res = gsearch('hilton', "-0.1757", "51.5156", '10')
            self.assertEqual(14, res[0])
            self.assertEqual('hotel93', res[1])
            self.assertEqual('hotel92', res[3])
            self.assertEqual('hotel79', res[5])

            res2 = gsearch('hilton', "-0.1757", "51.5156", '10000', 'm')
            self.assertListEqual(res, res2)
            res2 = gsearch_inline('hilton', "-0.1757", "51.5156", '10')
            self.assertListEqual(res, res2)

            res = gsearch('heathrow', -0.44155, 51.45865, '10', 'm')
            self.assertEqual(1, res[0])
            self.assertEqual('hotel94', res[1])
            res2 = gsearch_inline(
                'heathrow', -0.44155, 51.45865, '10', 'm')
            self.assertListEqual(res, res2)

            res = gsearch('heathrow', -0.44155, 51.45865, '10', 'km')
            self.assertEqual(5, res[0])
            self.assertIn('hotel94', res)
            res2 = gsearch_inline(
                'heathrow', -0.44155, 51.45865, '10', 'km')
            self.assertListEqual(res, res2)

            res = gsearch('heathrow', -0.44155, 51.45865, '5', 'km')
            self.assertEqual(3, res[0])
            self.assertIn('hotel94', res)
            res2 = gsearch_inline(
                'heathrow', -0.44155, 51.45865, '5', 'km')
            self.assertListEqual(res, res2)

    def testAddHash(self):
        r = self
        self.assertOk(r.execute_command('ft.create', 'idx', 'schema',
                                        'title', 'text', 'weight', 10.0, 'body', 'text', 'price', 'numeric'))

        self.assertTrue(
            r.hmset('doc1', {"title": "hello world", "body": "lorem ipsum", "price": 2}))
        self.assertTrue(
            r.hmset('doc2', {"title": "hello werld", "body": "lorem ipsum", "price": 5}))

        self.assertOk(r.execute_command('ft.addhash', 'idx', 'doc1', 1.0))
        self.assertOk(r.execute_command('ft.addhash', 'idx', 'doc2', 1.0))

        res = r.execute_command('ft.search', 'idx', "hello", "nocontent")
        self.assertEqual(3, len(res))
        self.assertEqual(2, res[0])
        self.assertEqual("doc1", res[2])
        self.assertEqual("doc2", res[1])

        res = r.execute_command(
            'ft.search', 'idx', "hello", "filter", "price", "0", "3")
        self.assertEqual(3, len(res))
        self.assertEqual(1, res[0])
        self.assertEqual("doc1", res[1])
        self.assertListEqual(
            ['body', 'lorem ipsum', 'price', '2', 'title', 'hello world'], res[2])

        res = r.execute_command(
            'ft.search', 'idx', "hello werld", "nocontent")
        self.assertEqual(2, len(res))
        self.assertEqual(1, res[0])
        self.assertEqual("doc2", res[1])

    def testInfields(self):
        r = self
        self.assertOk(r.execute_command(
            'ft.create', 'idx', 'schema', 'title', 'text', 'weight', 10.0, 'body', 'text', 'weight', 1.0))
        self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 0.5, 'fields',
                                        'title', 'hello world',
                                        'body', 'lorem ipsum'))

        self.assertOk(r.execute_command('ft.add', 'idx', 'doc2', 1.0, 'fields',
                                        'title', 'hello world lorem ipsum',
                                        'body', 'hello world'))

        res = r.execute_command(
            'ft.search', 'idx', 'hello world', 'verbatim', "infields", 1, "title", "nocontent")
        self.assertEqual(3, len(res))
        self.assertEqual(2, res[0])
        self.assertEqual("doc2", res[1])
        self.assertEqual("doc1", res[2])

        res = r.execute_command(
            'ft.search', 'idx', 'hello world', 'verbatim', "infields", 1, "body", "nocontent")
        self.assertEqual(2, len(res))
        self.assertEqual(1, res[0])
        self.assertEqual("doc2", res[1])

        res = r.execute_command(
            'ft.search', 'idx', 'hello', 'verbatim', "infields", 1, "body", "nocontent")
        self.assertEqual(2, len(res))
        self.assertEqual(1, res[0])
        self.assertEqual("doc2", res[1])

        res = r.execute_command(
            'ft.search', 'idx',  '\"hello world\"', 'verbatim', "infields", 1, "body", "nocontent")

        self.assertEqual(2, len(res))
        self.assertEqual(1, res[0])
        self.assertEqual("doc2", res[1])

        res = r.execute_command(
            'ft.search', 'idx', '\"lorem ipsum\"', 'verbatim', "infields", 1, "body", "nocontent")
        self.assertEqual(2, len(res))
        self.assertEqual(1, res[0])
        self.assertEqual("doc1", res[1])

        res = r.execute_command(
            'ft.search', 'idx', 'lorem ipsum', "infields", 2, "body", "title", "nocontent")
        self.assertEqual(3, len(res))
        self.assertEqual(2, res[0])
        self.assertEqual("doc2", res[1])
        self.assertEqual("doc1", res[2])

    def testScorerSelection(self):
        r = self
        self.assertOk(r.execute_command(
            'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))

        # this is the default scorer
        res = r.execute_command(
            'ft.search', 'idx', 'foo', 'scorer', 'TFIDF')
        self.assertEqual(res, [0])
        with self.assertResponseError():
            res = r.execute_command(
                'ft.search', 'idx', 'foo', 'scorer', 'NOSUCHSCORER')

    def testFieldSelectors(self):
        r = self
        self.assertOk(r.execute_command(
            'ft.create', 'idx', 'schema', 'TiTle', 'text', 'BoDy', 'text', "יוניקוד", 'text', 'field.with,punct', 'text'))
        self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1, 'fields',
                                        'title', 'hello world', 'body', 'foo bar', 'יוניקוד', 'unicode', 'field.with,punct', 'punt'))
        self.assertOk(r.execute_command('ft.add', 'idx', 'doc2', 0.5, 'fields',
                                        'body', 'hello world', 'title', 'foo bar', 'יוניקוד', 'unicode', 'field.with,punct', 'punt'))

        res = r.execute_command(
            'ft.search', 'idx', '@title:hello world', 'nocontent')
        self.assertEqual(res, [1, 'doc1'])
        res = r.execute_command(
            'ft.search', 'idx', '@body:hello world', 'nocontent')
        self.assertEqual(res, [1, 'doc2'])

        res = r.execute_command(
            'ft.search', 'idx', '@body:hello @title:world', 'nocontent')
        self.assertEqual(res, [0])

        res = r.execute_command(
            'ft.search', 'idx', '@body:hello world @title:world', 'nocontent')
        self.assertEqual(res, [0])
        res = r.execute_command(
            'ft.search', 'idx', '@BoDy:(hello|foo) @Title:(world|bar)', 'nocontent')
        self.assertEqual(res, [2, 'doc1', 'doc2'])

        res = r.execute_command(
            'ft.search', 'idx', '@body:(hello|foo world|bar)', 'nocontent')
        self.assertEqual(res, [2, 'doc1', 'doc2'])

        res = r.execute_command(
            'ft.search', 'idx', '@body|title:(hello world)', 'nocontent')
        self.assertEqual(res, [2, 'doc1', 'doc2'])

        res = r.execute_command(
            'ft.search', 'idx', '@יוניקוד:(unicode)', 'nocontent')
        self.assertEqual(res, [2, 'doc1', 'doc2'])

        res = r.execute_command(
            'ft.search', 'idx', '@field\\.with\\,punct:(punt)', 'nocontent')
        self.assertEqual(res, [2, 'doc1', 'doc2'])

    def testStemming(self):
        r = self
        self.assertOk(r.execute_command(
            'ft.create', 'idx', 'schema', 'title', 'text'))
        self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 0.5, 'fields',
                                        'title', 'hello kitty'))
        self.assertOk(r.execute_command('ft.add', 'idx', 'doc2', 1.0, 'fields',
                                        'title', 'hello kitties'))

        res = r.execute_command(
            'ft.search', 'idx', 'hello kitty', "nocontent")
        self.assertEqual(3, len(res))
        self.assertEqual(2, res[0])

        res = r.execute_command(
            'ft.search', 'idx', 'hello kitty', "nocontent", "verbatim")
        self.assertEqual(2, len(res))
        self.assertEqual(1, res[0])

        # test for unknown language
        with self.assertResponseError():
            res = r.execute_command(
                'ft.search', 'idx', 'hello kitty', "nocontent", "language", "foofoofian")

    def testExpander(self):
        r = self
        self.assertOk(r.execute_command(
            'ft.create', 'idx', 'schema', 'title', 'text'))
        self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 0.5, 'fields',
                                        'title', 'hello kitty'))

        res = r.execute_command(
            'ft.search', 'idx', 'kitties', "nocontent", "expander", "SBSTEM")
        self.assertEqual(2, len(res))
        self.assertEqual(1, res[0])

        res = r.execute_command(
            'ft.search', 'idx', 'kitties', "nocontent", "expander", "noexpander")
        self.assertEqual(1, len(res))
        self.assertEqual(0, res[0])

        res = r.execute_command(
            'ft.search', 'idx', 'kitti', "nocontent")
        self.assertEqual(2, len(res))
        self.assertEqual(1, res[0])

        res = r.execute_command(
            'ft.search', 'idx', 'kitti', "nocontent", 'verbatim')
        self.assertEqual(1, len(res))
        self.assertEqual(0, res[0])

        # Calling a stem directly works even with VERBATIM.
        # You need to use the + prefix escaped
        res = r.execute_command(
            'ft.search', 'idx', '\\+kitti', "nocontent", 'verbatim')
        self.assertEqual(2, len(res))
        self.assertEqual(1, res[0])

    def testNumericRange(self):
        r = self
        self.assertOk(r.execute_command(
            'ft.create', 'idx', 'schema', 'title', 'text', 'score', 'numeric', 'price', 'numeric'))
        for i in xrange(100):
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1, 'fields',
                                            'title', 'hello kitty', 'score', i, 'price', 100 + 10 * i))

        for _ in r.retry_with_rdb_reload():
            res = r.execute_command('ft.search', 'idx', 'hello kitty', "nocontent",
                                    "filter", "score", 0, 100)

            self.assertEqual(11, len(res))
            self.assertEqual(100, res[0])

            res = r.execute_command('ft.search', 'idx', 'hello kitty', "nocontent",
                                    "filter", "score", 0, 50)
            self.assertEqual(51, res[0])

            res = r.execute_command('ft.search', 'idx', 'hello kitty', 'verbatim', "nocontent", "limit", 0, 100,
                                    "filter", "score", "(0", "(50")

            self.assertEqual(49, res[0])
            res = r.execute_command('ft.search', 'idx', 'hello kitty', "nocontent",
                                    "filter", "score", "-inf", "+inf")
            self.assertEqual(100, res[0])

            # test multi filters
            scrange = (19, 90)
            prrange = (290, 385)
            res = r.execute_command('ft.search', 'idx', 'hello kitty',
                                    "filter", "score", scrange[
                                        0], scrange[1],
                                    "filter", "price", prrange[0], prrange[1])

            # print res
            for doc in res[2::2]:

                sc = int(doc[doc.index('score') + 1])
                pr = int(doc[doc.index('price') + 1])

                self.assertTrue(sc >= scrange[0] and sc <= scrange[1])
                self.assertGreaterEqual(pr, prrange[0])
                self.assertLessEqual(pr, prrange[1])

            self.assertEqual(10, res[0])

            res = r.execute_command('ft.search', 'idx', 'hello kitty',
                                    "filter", "score", "19", "90",
                                    "filter", "price", "90", "185")

            self.assertEqual(0, res[0])

            # Test numeric ranges as part of query syntax
            res = r.execute_command(
                'ft.search', 'idx', 'hello kitty @score:[0 100]', "nocontent")

            self.assertEqual(11, len(res))
            self.assertEqual(100, res[0])

            res = r.execute_command(
                'ft.search', 'idx', 'hello kitty  @score:[0 50]', "nocontent")
            self.assertEqual(51, res[0])
            res = r.execute_command(
                'ft.search', 'idx', 'hello kitty @score:[(0 (50]', 'verbatim', "nocontent")
            self.assertEqual(49, res[0])
            res = r.execute_command(
                'ft.search', 'idx', '@score:[(0 (50]', 'verbatim', "nocontent")
            self.assertEqual(49, res[0])
            res = r.execute_command(
                'ft.search', 'idx', 'hello kitty -@score:[(0 (50]', 'verbatim', "nocontent")
            self.assertEqual(51, res[0])
            res = r.execute_command(
                'ft.search', 'idx', 'hello kitty @score:[-inf +inf]', "nocontent")
            self.assertEqual(100, res[0])

    def testSuggestions(self):
        r = self
        self.assertEqual(1, r.execute_command(
            'ft.SUGADD', 'ac', 'hello world', 1))
        self.assertEqual(1, r.execute_command(
            'ft.SUGADD', 'ac', 'hello world', 1, 'INCR'))

        res = r.execute_command("FT.SUGGET", "ac", "hello")
        self.assertEqual(1, len(res))
        self.assertEqual("hello world", res[0])

        terms = ["hello werld", "hallo world",
                 "yellow world", "wazzup", "herp", "derp"]
        sz = 2
        for term in terms:
            self.assertEqual(sz, r.execute_command(
                'ft.SUGADD', 'ac', term, sz - 1))
            sz += 1

        for _ in r.retry_with_rdb_reload():

            self.assertEqual(7, r.execute_command('ft.SUGLEN', 'ac'))

            # search not fuzzy
            self.assertEqual(["hello world", "hello werld"],
                             r.execute_command("ft.SUGGET", "ac", "hello"))

            # print  r.execute_command("ft.SUGGET", "ac", "hello", "FUZZY", "MAX", "1", "WITHSCORES")
            # search fuzzy - shuold yield more results
            self.assertEqual(['hello world', 'hello werld', 'yellow world', 'hallo world'],
                             r.execute_command("ft.SUGGET", "ac", "hello", "FUZZY"))

            # search fuzzy with limit of 1
            self.assertEqual(['hello world'],
                             r.execute_command("ft.SUGGET", "ac", "hello", "FUZZY", "MAX", "1"))

            # scores should return on WITHSCORES
            rc = r.execute_command(
                "ft.SUGGET", "ac", "hello", "WITHSCORES")
            self.assertEqual(4, len(rc))
            self.assertTrue(float(rc[1]) > 0)
            self.assertTrue(float(rc[3]) > 0)

        rc = r.execute_command("ft.SUGDEL", "ac", "hello world")
        self.assertEqual(1L, rc)
        rc = r.execute_command("ft.SUGDEL", "ac", "world")
        self.assertEqual(0L, rc)

        rc = r.execute_command("ft.SUGGET", "ac", "hello")
        self.assertEqual(['hello werld'], rc)

    def testSuggestPayload(self):
        r = self
        self.assertEqual(1, r.execute_command(
            'ft.SUGADD', 'ac', 'hello world', 1, 'PAYLOAD', 'foo'))
        self.assertEqual(2, r.execute_command(
            'ft.SUGADD', 'ac', 'hello werld', 1, 'PAYLOAD', 'bar'))
        self.assertEqual(3, r.execute_command(
            'ft.SUGADD', 'ac', 'hello nopayload', 1, 'PAYLOAD', ''))
        self.assertEqual(4, r.execute_command(
            'ft.SUGADD', 'ac', 'hello nopayload2', 1))

        res = r.execute_command("FT.SUGGET", "ac", "hello", 'WITHPAYLOADS')
        self.assertListEqual(['hello world', 'foo', 'hello werld', 'bar', 'hello nopayload', None, 'hello nopayload2', None],
                             res)
        res = r.execute_command("FT.SUGGET", "ac", "hello")
        self.assertListEqual(['hello world',  'hello werld', 'hello nopayload', 'hello nopayload2'],
                             res)
        res = r.execute_command(
            "FT.SUGGET", "ac", "hello", 'WITHPAYLOADS', 'WITHSCORES')
        # we don't compare the scores beause they may change
        self.assertEqual(12, len(res))

    def testPayload(self):
        r = self
        self.assertOk(r.execute_command(
            'ft.create', 'idx', 'schema', 'f', 'text'))
        for i in range(10):

            self.assertOk(r.execute_command('ft.add', 'idx', '%d' % i, 1.0,
                                            'payload', 'payload %d' % i,
                                            'fields', 'f', 'hello world'))

        for x in r.retry_with_rdb_reload():

            res = r.execute_command(
                'ft.search', 'idx', 'hello world')
            self.assertEqual(21, len(res))

            res = r.execute_command(
                'ft.search', 'idx', 'hello world', 'withpayloads')

            self.assertEqual(31, len(res))
            self.assertEqual(10, res[0])
            for i in range(1, 30, 3):
                self.assertEqual(res[i + 1], 'payload %s' % res[i])

    def testGarbageCollector(self):
        N = 100
        r = self
        self.assertOk(r.execute_command(
            'ft.create', 'idx', 'schema', 'foo', 'text'))
        for i in range(N):

            self.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0,
                                            'fields', 'foo', ' '.join(('term%d' % random.randrange(0, 10) for i in range(10)))))

        def get_stats(r):
            res = r.execute_command('ft.info', 'idx')
            d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
            gc_stats = {d['gc_stats'][x]: float(
                d['gc_stats'][x + 1]) for x in range(0, len(d['gc_stats']), 2)}
            d['gc_stats'] = gc_stats
            return d

        stats = get_stats(r)
        self.assertGreater(stats['gc_stats']['current_hz'], 8)
        self.assertEqual(0, stats['gc_stats']['bytes_collected'])
        self.assertGreater(int(stats['num_records']), 0)

        initialIndexSize = float(stats['inverted_sz_mb']) * 1024 * 1024
        for i in range(N):
            self.assertEqual(1, r.execute_command(
                'ft.del', 'idx', 'doc%d' % i))
        st = time.time()
        while st + 2 > time.time():
            time.sleep(0.1)
            stats = get_stats(r)
            if stats['num_records'] == '0':
                break
        self.assertEqual('0', stats['num_docs'])
        self.assertEqual('0', stats['num_records'])
        self.assertEqual('100', stats['max_doc_id'])
        self.assertGreater(stats['gc_stats']['current_hz'], 50)
        currentIndexSize = float(stats['inverted_sz_mb']) * 1024 * 1024
        # print initialIndexSize, currentIndexSize,
        # stats['gc_stats']['bytes_collected']
        self.assertGreater(initialIndexSize, currentIndexSize)
        self.assertGreater(stats['gc_stats'][
                           'bytes_collected'], currentIndexSize)

        for i in range(10):

            res = r.execute_command('ft.search', 'idx', 'term%d' % i)
            self.assertEqual([0], res)

    def testReturning(self):
        self.assertCmdOk('ft.create', 'idx', 'schema', 'f1',
                         'text', 'f2', 'text', 'n1', 'numeric', 'f3', 'text')
        for i in range(10):
            self.assertCmdOk('ft.add', 'idx', 'DOC_{0}'.format(i), 1.0, 'fields',
                             'f2', 'val2', 'f1', 'val1', 'f3', 'val3',
                             'n1', i)

        # RETURN 0. Simplest case
        for x in self.retry_with_reload():
            res = self.cmd('ft.search', 'idx', 'val*', 'return', '0')
            self.assertEqual(11, len(res))
            self.assertEqual(10, res[0])
            for r in res[1:]:
                self.assertTrue(r.startswith('DOC_'))

        for field in ('f1', 'f2', 'f3', 'n1'):
            res = self.cmd('ft.search', 'idx', 'val*', 'return', 1, field)
            self.assertEqual(21, len(res))
            self.assertEqual(10, res[0])
            for pair in grouper(res[1:], 2):
                docname, fields = pair
                self.assertEqual(2, len(fields))
                self.assertEqual(field, fields[0])
                self.assertTrue(docname.startswith('DOC_'))

        # Test when field is not found
        res = self.cmd('ft.search', 'idx', 'val*', 'return', 1, 'nonexist')
        self.assertEqual(21, len(res))
        self.assertEqual(10, res[0])
        for pair in grouper(res[1:], 2):
            _, pair = pair
            self.assertEqual(None, pair[1])

        # Test that we don't crash if we're given the wrong number of fields
        with self.assertResponseError():
            res = self.cmd('ft.search', 'idx', 'val*', 'return', 2, 'nonexist')

    def _test_create_options_real(self, *options):
        options = [x for x in options if x]
        has_offsets = 'NOOFFSETS' not in options
        has_fields = 'NOFIELDS' not in options
        has_freqs = 'NOFREQS' not in options

        try:
            self.cmd('ft.drop', 'idx')
        except:
            pass

        options = ['idx'] + options + ['schema', 'f1', 'text', 'f2', 'text']
        self.assertCmdOk('ft.create', *options)
        for i in range(10):
            self.assertCmdOk('ft.add', 'idx', 'doc{}'.format(
                i), 0.5, 'fields', 'f1', 'value for {}'.format(i))

        # Query
        res = self.cmd('ft.search', 'idx', "value for 3")
        if not has_offsets:
            self.assertFalse(res)
        else:
            self.assertTrue(res)

        # Frequencies:
        self.assertCmdOk('ft.add', 'idx', 'doc100',
                         1.0, 'fields', 'f1', 'foo bar')
        self.assertCmdOk('ft.add', 'idx', 'doc200', 1.0,
                         'fields', 'f1', ('foo ' * 10) + ' bar')
        res = self.cmd('ft.search', 'idx', 'foo')
        self.assertEqual(2, res[0])
        if has_offsets:
            docname = res[1]
            if has_freqs:
                self.assertEqual('doc200', docname)
            else:
                self.assertEqual('doc100', docname)

        self.assertCmdOk('ft.add', 'idx', 'doc300',
                         1.0, 'fields', 'f1', 'Hello')
        res = self.cmd('ft.search', 'idx', '@f2:Hello')
        if has_fields:
            self.assertEqual(1, len(res))
        else:
            self.assertEqual(3, len(res))

    def testCreationOptions(self):
        from itertools import combinations
        for x in range(1, 5):
            for combo in combinations(('NOOFSETS', 'NOFREQS', 'NOFIELDS', ''), x):
                self._test_create_options_real(*combo)

    def testInfoCommand(self):
        from itertools import combinations
        r = self
        self.assertOk(r.execute_command(
            'ft.create', 'idx', 'NOFIELDS', 'schema', 'title', 'text'))
        N = 50
        for i in xrange(N):
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1, 'replace', 'fields',
                                            'title', 'hello term%d' % i))
        for _ in r.retry_with_rdb_reload():

            res = r.execute_command('ft.info', 'idx')
            d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}

            self.assertEqual(d['index_name'], 'idx')
            self.assertEqual(d['index_options'], ['NOFIELDS'])
            self.assertListEqual(
                d['fields'], [['title', 'type', 'TEXT', 'WEIGHT', '1']])
            self.assertEquals(int(d['num_docs']), N)
            self.assertEquals(int(d['num_terms']), N + 1)
            self.assertEquals(int(d['max_doc_id']), N)
            self.assertEquals(int(d['records_per_doc_avg']), 2)
            self.assertEquals(int(d['num_records']), N * 2)

            self.assertGreater(float(d['offset_vectors_sz_mb']), 0)
            self.assertGreater(float(d['key_table_size_mb']), 0)
            self.assertGreater(float(d['inverted_sz_mb']), 0)
            self.assertGreater(float(d['bytes_per_record_avg']), 0)
            self.assertGreater(float(d['doc_table_size_mb']), 0)

        for x in range(1, 5):
            for combo in combinations(('NOOFFSETS', 'NOFREQS', 'NOFIELDS', ''), x):
                combo = list(filter(None, combo))
                options = combo + ['schema', 'f1', 'text']
                try:
                    self.cmd('ft.drop', 'idx')
                except:
                    pass
                self.assertCmdOk('ft.create', 'idx', *options)
                info = self.cmd('ft.info', 'idx')
                ix = info.index('index_options')
                self.assertFalse(ix == -1)

                opts = info[ix + 1]
                # make sure that an empty opts string returns no options in
                # info
                if not combo:
                    self.assertListEqual([], opts)

                for option in filter(None, combo):
                    self.assertTrue(option in opts)

    def testNoStem(self):
        self.cmd('ft.create', 'idx', 'schema', 'body',
                 'text', 'name', 'text', 'nostem')
        for _ in self.retry_with_reload():
            try:
                self.cmd('ft.del', 'idx', 'doc')
            except redis.ResponseError:
                pass

            # Insert a document
            self.assertCmdOk('ft.add', 'idx', 'doc', 1.0, 'fields',
                             'body', "located",
                             'name', "located")

            # Now search for the fields
            res_body = self.cmd('ft.search', 'idx', '@body:location')
            res_name = self.cmd('ft.search', 'idx', '@name:location')
            self.assertEqual(0, res_name[0])
            self.assertEqual(1, res_body[0])

    def testSearchNonexistField(self):
        # GH Issue 133
        self.cmd('ft.create', 'idx', 'schema', 'title', 'text',
                 'weight', 5.0, 'body', 'text', 'url', 'text')
        self.cmd('ft.add', 'idx', 'd1', 1.0, 'nosave', 'fields', 'title',
                 'hello world', 'body', 'lorem dipsum', 'place', '-77.0366 38.8977')
        self.cmd('ft.search', 'idx', 'Foo', 'GEOFILTER',
                 'place', '-77.0366', '38.8977', '1', 'km')

    def testSortbyMissingField(self):
        # GH Issue 131
        self.cmd('ft.create', 'ix', 'schema', 'txt',
                 'text', 'num', 'numeric', 'sortable')
        self.cmd('ft.add', 'ix', 'doc1', 1.0, 'fields', 'txt', 'foo')
        self.cmd('ft.search', 'ix', 'foo', 'sortby', 'num')

    def testParallelIndexing(self):
        # GH Issue 207
        self.cmd('ft.create', 'idx', 'schema', 'txt', 'text')
        from threading import Thread
        self.server.client()
        ndocs = 100

        def runner(tid):
            cli = self.server.client()
            for num in range(ndocs):
                cli.execute_command('ft.add', 'idx', 'doc{}_{}'.format(tid, num), 1.0,
                                    'fields', 'txt', 'hello world' * 20)
        ths = []
        for tid in range(10):
            ths.append(Thread(target=runner, args=(tid,)))

        [th.start() for th in ths]
        [th.join() for th in ths]
        res = self.cmd('ft.info', 'idx')
        d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
        self.assertEqual(1000, int(d['num_docs']))

    def testDoubleAdd(self):
        # Tests issue #210
        self.cmd('ft.create', 'idx', 'schema', 'txt', 'text')
        self.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields', 'txt', 'hello world')
        with self.assertResponseError():
            self.cmd('ft.add', 'idx', 'doc1', 1.0,
                     'fields', 'txt', 'goodbye world')

        self.assertEqual('hello world', self.cmd('ft.get', 'idx', 'doc1')[1])
        self.assertEqual(0, self.cmd('ft.search', 'idx', 'goodbye')[0])
        self.assertEqual(1, self.cmd('ft.search', 'idx', 'hello')[0])

        # Now with replace
        self.cmd('ft.add', 'idx', 'doc1', 1.0, 'replace',
                 'fields', 'txt', 'goodbye world')
        self.assertEqual(1, self.cmd('ft.search', 'idx', 'goodbye')[0])
        self.assertEqual(0, self.cmd('ft.search', 'idx', 'hello')[0])
        self.assertEqual('goodbye world', self.cmd('ft.get', 'idx', 'doc1')[1])

    def testConcurrentErrors(self):
        from multiprocessing import Process
        import random

        self.cmd('ft.create', 'idx', 'schema', 'txt', 'text')
        docs_per_thread = 100
        num_threads = 50

        docIds = ['doc{}'.format(x) for x in range(docs_per_thread)]

        def thrfn():
            myIds = docIds[::]
            random.shuffle(myIds)
            cli = self.server.client()
            with cli.pipeline(transaction=False) as pl:
                for x in myIds:
                    pl.execute_command('ft.add', 'idx', x, 1.0,
                                       'fields', 'txt', ' hello world ' * 50)
                try:
                    pl.execute()
                except Exception as e:
                    pass
                    # print e

        thrs = [Process(target=thrfn) for x in range(num_threads)]
        [th.start() for th in thrs]
        [th.join() for th in thrs]
        res = self.cmd('ft.info', 'idx')
        d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
        self.assertEqual(100, int(d['num_docs']))

    def testBinaryKeys(self):
        self.cmd('ft.create', 'idx', 'schema', 'txt', 'text')
        # Insert a document
        self.cmd('ft.add', 'idx', 'Hello', 1.0, 'fields', 'txt', 'NoBin match')
        self.cmd('ft.add', 'idx', 'Hello\x00World',
                 1.0, 'fields', 'txt', 'Bin match')
        for _ in self.reloading_iterator():
            res = self.cmd('ft.search', 'idx', 'match')
            self.assertEqual(res, [2L, 'Hello\x00World', [
                             'txt', 'Bin match'], 'Hello', ['txt', 'NoBin match']])

    def testNonDefaultDb(self):
        # Should be ok
        self.cmd('FT.CREATE', 'idx1', 'schema', 'txt', 'text')
        self.cmd('SELECT 1')

        # Should fail
        with self.assertResponseError():
            self.cmd('FT.CREATE', 'idx2', 'schema', 'txt', 'text')

    def testDuplicateNonspecFields(self):
        self.cmd('FT.CREATE', 'idx', 'schema', 'txt', 'text')
        self.cmd('FT.ADD', 'idx', 'doc', 1.0, 'fields',
                 'f1', 'f1val', 'f1', 'f1val2', 'F1', 'f1Val3')

        res = self.cmd('ft.get', 'idx', 'doc')
        res = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
        self.assertTrue(res['f1'] in ('f1val', 'f1val2'))
        self.assertEqual('f1Val3', res['F1'])

    def testDuplicateFields(self):
        self.cmd('FT.CREATE', 'idx', 'SCHEMA', 'txt',
                 'TEXT', 'num', 'NUMERIC', 'SORTABLE')
        for _ in self.retry_with_reload():
            # Ensure the index assignment is correct after an rdb load
            with self.assertResponseError():
                self.cmd('FT.ADD', 'idx', 'doc', 1.0, 'FIELDS',
                         'txt', 'foo', 'txt', 'bar', 'txt', 'baz')

            # Try add hash
            self.cmd('HMSET', 'newDoc', 'txt', 'foo',
                     'Txt', 'bar', 'txT', 'baz')
            # Get the actual value:

            from redis import ResponseError
            caught = False
            try:
                self.cmd('FT.ADDHASH', 'idx', 'newDoc', 1.0)
            except ResponseError as err:
                caught = True
                self.assertTrue('twice' in err.message)
            self.assertTrue(caught)

            # Try with REPLACE
            with self.assertResponseError():
                self.cmd('FT.ADD', 'idx', 'doc2', 1.0, 'REPLACE', 'FIELDS',
                         'txt', 'foo', 'txt', 'bar')

            # With replace partial
            self.cmd('FT.ADD', 'idx', 'doc2', 1.0, 'REPLACE',
                     'PARTIAL', 'FIELDS', 'num', 42)
            with self.assertResponseError():
                self.cmd('FT.ADD', 'idx', 'doc2', 1.0, 'REPLACE',
                         'PARTIAL', 'FIELDS', 'num', 42, 'num', 32)

    def testDuplicateSpec(self):
        with self.assertResponseError():
            self.cmd('FT.CREATE', 'idx', 'SCHEMA', 'f1',
                     'text', 'n1', 'numeric', 'f1', 'text')

    def testSortbyMissingFieldSparse(self):
        # Note, the document needs to have one present sortable field in
        # order for the indexer to give it a sort vector
        self.cmd('ft.create', 'idx', 'SCHEMA', 'lastName', 'text',
                 'SORTABLE', 'firstName', 'text', 'SORTABLE')
        self.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields', 'lastName', 'mark')
        res = self.cmd('ft.search', 'idx', 'mark', 'WITHSORTKEYS', "SORTBY",
                       "firstName", "ASC", "lastName", "DESC", "limit", 0, 100)
        self.assertEqual([1L, 'doc1', None, ['lastName', 'mark']], res)

    def testLuaAndMulti(self):
        # Ensure we can work in Lua and Multi environments without crashing
        self.cmd('FT.CREATE', 'idx', 'SCHEMA', 'f1', 'text', 'n1', 'numeric')
        self.cmd('HMSET', 'hashDoc', 'f1', 'v1', 'n1', 4)
        self.cmd('HMSET', 'hashDoc2', 'f1', 'v1', 'n1', 5)

        r = self.client

        r.eval(
            "return redis.call('ft.add', 'idx', 'doc1', 1.0, 'fields', 'f1', 'bar')", "0")
        r.eval("return redis.call('ft.addhash', 'idx', 'hashDoc', 1.0)", 0)

        # Try in a pipeline:
        with r.pipeline(transaction=True) as pl:
            pl.execute_command('ft.add', 'idx', 'doc2',
                               1.0, 'fields', 'f1', 'v3')
            pl.execute_command('ft.add', 'idx', 'doc3',
                               1.0, 'fields', 'f1', 'v4')
            pl.execute_command('ft.addhash', 'idx', 'hashdoc2', 1.0)
        pl.execute()

    def testLanguageField(self):
        self.cmd('FT.CREATE', 'idx', 'SCHEMA', 'language', 'TEXT')
        self.cmd('FT.ADD', 'idx', 'doc1', 1.0,
                 'FIELDS', 'language', 'gibberish')
        res = self.cmd('FT.SEARCH', 'idx', 'gibberish')
        self.assertEqual([1L, 'doc1', ['language', 'gibberish']], res)
        # The only way I can verify that LANGUAGE is parsed twice is ensuring we
        # provide a wrong language. This is much easier to test than trying to
        # figure out how a given word is stemmed
        with self.assertResponseError():
            self.cmd('FT.ADD', 'idx', 'doc1', 1.0, 'LANGUAGE',
                     'blah', 'FIELDS', 'language', 'gibber')

    def testUninitSortvector(self):
        # This would previously crash
        self.cmd('FT.CREATE', 'idx', 'SCHEMA', 'f1', 'TEXT')
        for x in range(2000):
            self.cmd('FT.ADD', 'idx', 'doc{}'.format(
                x), 1.0, 'FIELDS', 'f1', 'HELLO')

        self.cmd('SAVE')
        for x in range(10):
            self.cmd('DEBUG RELOAD')

    def testAlterIndex(self):
        self.cmd('FT.CREATE', 'idx', 'SCHEMA', 'f1', 'TEXT')
        self.cmd('FT.ADD', 'idx', 'doc1', 1.0, 'FIELDS', 'f1', 'hello', 'f2', 'world')
        self.cmd('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'f2', 'TEXT')
        self.cmd('FT.ADD', 'idx', 'doc2', 1.0, 'FIELDS', 'f1', 'hello', 'f2', 'world')
        for _ in self.retry_with_reload():
            ret = self.cmd('FT.SEARCH', 'idx', 'world')
            self.assertEqual([1, 'doc2', ['f1', 'hello', 'f2', 'world']], ret)

        self.cmd('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'f3', 'TEXT', 'SORTABLE')
        for x in range(10):
            self.cmd('FT.ADD', 'idx', 'doc{}'.format(x + 3), 1.0, 'FIELDS', 'f1', 'hello', 'f3', 'val{}'.format(x))
        
        for _ in self.retry_with_reload():
            # Test that sortable works
            res = self.cmd('FT.SEARCH', 'idx', 'hello', 'SORTBY', 'f3', 'DESC')
            self.assertEqual(
                [12, 'doc12', ['f1', 'hello', 'f3', 'val9'], 'doc11', ['f1', 'hello', 'f3', 'val8'], 'doc10', ['f1', 'hello', 'f3', 'val7'], 'doc9', ['f1', 'hello', 'f3', 'val6'], 'doc8', ['f1', 'hello', 'f3', 'val5'], 'doc7', ['f1', 'hello', 'f3', 'val4'], 'doc6', ['f1', 'hello', 'f3', 'val3'], 'doc5', ['f1', 'hello', 'f3', 'val2'], 'doc4', ['f1', 'hello', 'f3', 'val1'], 'doc3', ['f1', 'hello', 'f3', 'val0']],
                res)

        # Test that we can add a numeric field
        self.cmd('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'n1', 'NUMERIC')
        self.cmd('FT.ADD', 'idx', 'docN1', 1.0, 'FIELDS', 'n1', 50)
        self.cmd('FT.ADD', 'idx', 'docN2', 1.0, 'FIELDS', 'n1', 250)
        for _ in self.retry_with_reload():
            res = self.cmd('FT.SEARCH', 'idx', '@n1:[0 100]')
            self.assertEqual([1, 'docN1', ['n1', '50']], res)

    def testAlterValidation(self):
        # Test that constraints for ALTER comand
        self.cmd('FT.CREATE', 'idx1', 'SCHEMA', 'f0', 'TEXT')
        for x in range(1, 32):
            self.cmd('FT.ALTER', 'idx1', 'SCHEMA', 'ADD', 'f{}'.format(x), 'TEXT')
        # OK for now.

        # Should be too many indexes
        self.assertRaises(redis.ResponseError, self.cmd, 'FT.ALTER', 'idx1', 'SCHEMA', 'ADD', 'tooBig', 'TEXT')

        self.cmd('FT.CREATE', 'idx2', 'MAXTEXTFIELDS', 'SCHEMA', 'f0', 'TEXT')
        # print self.cmd('FT.INFO', 'idx2')
        for x in range(1, 50):
            self.cmd('FT.ALTER', 'idx2', 'SCHEMA', 'ADD', 'f{}'.format(x + 1), 'TEXT')
        
        self.cmd('FT.ADD', 'idx2', 'doc1', 1.0, 'FIELDS', 'f50', 'hello')
        for _ in self.retry_with_reload():
            ret = self.cmd('FT.SEARCH', 'idx2', '@f50:hello')
            self.assertEqual([1, 'doc1', ['f50', 'hello']], ret)
        

        self.cmd('FT.CREATE', 'idx3', 'SCHEMA', 'f0', 'text')
        # Try to alter the index with garbage
        self.assertRaises(redis.ResponseError, self.cmd, 'FT.ALTER', 'idx3', 'SCHEMA', 'ADD', 'f1', 'TEXT', 'f2', 'garbage')
        ret = to_dict(self.cmd('ft.info', 'idx3'))
        self.assertEqual(1, len(ret['fields']))

        self.assertRaises(redis.ResponseError, self.cmd, 'FT.ALTER', 'nonExist', 'SCHEMA', 'ADD', 'f1', 'TEXT')

        # test with no fields!
        self.assertRaises(redis.ResponseError, self.cmd, 'FT.ALTER', 'idx2', 'SCHEMA', 'ADD')


    def testIssue366(self):
        # Test random RDB regressions, see GH 366
        self.cmd('FT.CREATE', 'idx1', 'SCHEMA', 'textfield', 'TEXT', 'numfield', 'NUMERIC')
        self.cmd('HSET', 'foo', 'textfield', 'blah', 'numfield', 1)
        self.cmd('FT.ADDHASH', 'idx1', 'foo', 1, 'replace')
        self.cmd('FT.DEL', 'idx1', 'foo')
        for _ in self.retry_with_reload():
            pass  #  --just ensure it doesn't crash
        
# FT.CREATE atest SCHEMA textfield TEXT numfield NUMERIC
# FT.ADD atest anId 1 PAYLOAD '{"hello":"world"}' FIELDS textfield sometext numfield 1234
# FT.ADD atest anId 1 PAYLOAD '{"hello":"world2"}' REPLACE PARTIAL FIELDS numfield 1111
# shutdown
        self.cmd('FT.CREATE', 'idx2', 'SCHEMA', 'textfield', 'TEXT', 'numfield', 'NUMERIC')
        self.cmd('FT.ADD', 'idx1', 'doc1', 1, 'PAYLOAD', '{"hello":"world"}',
            'FIELDS', 'textfield', 'sometext', 'numfield', 1234)
        self.cmd('ft.add', 'idx1', 'doc1', 1,
            'PAYLOAD', '{"hello":"world2"}',
            'REPLACE', 'PARTIAL',
            'FIELDS', 'textfield', 'sometext', 'numfield', 1111)
        for _ in self.retry_with_reload():
            pass  #

    def testReplaceReload(self):
        self.cmd('FT.CREATE', 'idx2', 'SCHEMA', 'textfield', 'TEXT', 'numfield', 'NUMERIC')
        # Create a document and then replace it.
        self.cmd('FT.ADD', 'idx2', 'doc2', 1.0, 'FIELDS', 'textfield', 's1', 'numfield', 99)
        self.cmd('FT.ADD', 'idx2', 'doc2', 1.0, 'REPLACE', 'PARTIAL', 'FIELDS', 'textfield', 's100', 'numfield', 990)
        self.server.dump_and_reload()
        # RDB Should still be fine

        self.cmd('FT.ADD', 'idx2', 'doc2', 1.0, 'REPLACE', 'PARTIAL', 'FIELDS', 'textfield', 's200', 'numfield', 1090)
        doc = to_dict(self.cmd('FT.GET', 'idx2', 'doc2'))
        self.assertEqual('s200', doc['textfield'])
        self.assertEqual('1090', doc['numfield'])

def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    from itertools import izip_longest
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)

def to_dict(r):
    return {r[i]: r[i + 1] for i in range(0, len(r), 2)}


if __name__ == '__main__':
    unittest.main()
