from rmtest import ModuleTestCase
import redis
import unittest
from hotels import hotels
import random
import time


class SearchTestCase(ModuleTestCase('../redisearch.so')):

    def testAdd(self):
        with self.redis() as r:
            r.flushdb()
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

    def testUnion(self):

        with self.redis() as r:
            r.flushdb()
            N = 100
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
                self.assertEqual(51, len(res))
                self.assertEqual(50, res[0])

    def testSearch(self):
        with self.redis() as r:
            r.flushdb()
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
            self.cmd('ft.add', 'idx', 'doc{}'.format(x), 1.0, 'NOSAVE', 'FIELDS', 'f1', 'value')

        # Now query the results
        res = self.cmd('ft.search', 'idx', 'value')
        self.assertEqual(3, res[0])
        for content in res[2::2]:
            self.assertEqual([], content)

    def testDelete(self):
        with self.redis() as r:
            r.flushdb()
            self.assertOk(r.execute_command(
                'ft.create', 'idx', 'schema', 'f', 'text'))

            for i in range(100):
                self.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                                'f', 'hello world'))

            for i in range(100):
                self.assertEqual(1, r.execute_command(
                    'ft.del', 'idx', 'doc%d' % i))
                # second delete should return 0
                self.assertEqual(0, r.execute_command(
                    'ft.del', 'idx', 'doc%d' % i))

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

        with self.redis() as r:
            r.flushdb()

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
        with self.redis() as r:
            r.flushdb()
            self.assertOk(r.execute_command(
                'ft.create', 'idx', 'schema', 'f', 'text', 'n', 'numeric'))

            for i in range(200):
                self.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                                'f', 'hello world', 'n', 666))

            keys = r.keys('*')
            self.assertEqual(204, len(keys))

            self.assertOk(r.execute_command('ft.drop', 'idx'))
            keys = r.keys('*')
            self.assertEqual(0, len(keys))

    def testCustomStopwords(self):
        with self.redis() as r:
            r.flushdb()

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

    def testOptional(self):
        with self.redis() as r:
            r.flushdb()
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
        with self.redis() as r:
            r.flushdb()
            self.assertOk(r.execute_command(
                'ft.create', 'idx', 'schema', 'foo', 'text', 'bar', 'numeric', 'sortable'))
            q = '(hello world) "what what" hello|world @bar:[10 100]|@bar:[200 300]'
            res = r.execute_command('ft.explain', 'idx', q)

            self.assertEqual(res, """INTERSECT {
  hello
  world
  EXACT {
    what
    what
  }
  UNION {
    hello
    world
  }
  UNION {
    NUMERIC {10.000000 <= @bar <= 100.000000}
    NUMERIC {200.000000 <= @bar <= 300.000000}
  }
}
""")


    def testNoIndex(self):
        with self.redis() as r:
            r.flushdb()
            self.assertOk(r.execute_command(
                'ft.create', 'idx', 'schema', 
                    'foo', 'text', 
                    'num', 'numeric', 'sortable', 'noindex',
                    'extra', 'text', 'noindex', 'sortable'))
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', '0.1', 'fields',
                                                'foo', 'hello world', 'num', 1, 'extra', 'lorem ipsum'))
            res = r.execute_command('ft.search', 'idx', 'hello world', 'nocontent')
            self.assertListEqual([1, 'doc1'], res)
            res = r.execute_command('ft.search', 'idx', 'lorem ipsum', 'nocontent')
            self.assertListEqual([0], res)
            res = r.execute_command('ft.search', 'idx', '@num:[1 1]', 'nocontent')
            self.assertListEqual([0], res)

    def testPartial(self):
        with self.redis() as r:
            r.flushdb()
            self.assertOk(r.execute_command(
                'ft.create', 'idx', 'schema', 
                        'foo', 'text', 
                        'num', 'numeric', 'sortable', 'noindex', 
                        'extra', 'text', 'noindex'))
            #print r.execute_command('ft.info', 'idx')

            self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', '0.1', 'fields',
                                                'foo', 'hello world', 'num', 1, 'extra', 'lorem ipsum'))
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc2', '0.1', 'fields',
                                                'foo', 'hello world', 'num', 2, 'extra', 'abba'))
            res = r.execute_command('ft.search', 'idx', 'hello world', 'sortby', 'num', 'asc', 'nocontent', 'withsortkeys')
            self.assertListEqual([2L, 'doc1', '1', 'doc2', '2'], res)
            res = r.execute_command('ft.search', 'idx', 'hello world', 'sortby', 'num', 'desc', 'nocontent', 'withsortkeys')
            self.assertListEqual([2L, 'doc2', '2', 'doc1', '1'], res)
            
            
            # Updating non indexed fields doesn't affect search results
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', '0.1', 'replace', 'partial',
                                                'fields', 'num', 3, 'extra', 'jorem gipsum'))
            res = r.execute_command('ft.search', 'idx', 'hello world','sortby', 'num', 'desc',)
            self.assertListEqual([2L,'doc1', ['foo', 'hello world', 'num', '3', 'extra', 'jorem gipsum'],
                                     'doc2', ['foo', 'hello world', 'num', '2', 'extra', 'abba']], res)
            res = r.execute_command('ft.search', 'idx', 'hello', 'nocontent', 'withscores')
            # Updating only indexed field affects search results
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', '0.1', 'replace', 'partial',
                                                'fields', 'foo', 'wat wet'))
            res = r.execute_command('ft.search', 'idx', 'hello world', 'nocontent')
            self.assertListEqual([1L, 'doc2'], res)
            res = r.execute_command('ft.search', 'idx', 'wat', 'nocontent')
            self.assertListEqual([1L, 'doc1'], res)

            # Test updating of score and no fields
            res = r.execute_command('ft.search', 'idx', 'wat', 'nocontent', 'withscores')
            self.assertLess(float(res[2]), 1)
            #self.assertListEqual([1L, 'doc1'], res)
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', '1.0', 'replace', 'partial', 'fields'))
            res = r.execute_command('ft.search', 'idx', 'wat', 'nocontent', 'withscores')
            self.assertGreater(float(res[2]), 1)

            # Test updating payloads
            res = r.execute_command('ft.search', 'idx', 'wat', 'nocontent', 'withpayloads')
            self.assertIsNone(res[2])
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', '1.0', 'replace', 'partial', 'payload', 'foobar', 'fields'))
            res = r.execute_command('ft.search', 'idx', 'wat', 'nocontent', 'withpayloads')
            self.assertEqual('foobar', res[2])
            
            
    def testPaging(self):
        with self.redis() as r:
            r.flushdb()
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
        with self.redis() as r:
            r.flushdb()
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
        with self.redis() as r:
            r.flushdb()
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
                    [100L, 'doc2', '5', 'doc3', '4', 'doc4', '3', 'doc5', '2', 'doc6', '1'], res)

                res = r.execute_command('ft.search', 'idx', 'world', 'nocontent',
                                        'sortby', 'bar', 'desc', 'withsortkeys', 'limit', 0, 5)
                self.assertListEqual(
                    [100L, 'doc0', '100', 'doc1', '99', 'doc2', '98', 'doc3', '97', 'doc4', '96'], res)
                res = r.execute_command('ft.search', 'idx', 'world', 'nocontent',
                                        'sortby', 'foo', 'desc', 'withsortkeys', 'limit', 0, 5)
                self.assertListEqual([100L, 'doc99', 'hello099 world', 'doc98', 'hello098 world', 'doc97', 'hello097 world', 'doc96',
                                      'hello096 world', 'doc95', 'hello095 world'], res)

    def testNot(self):
        with self.redis() as r:
            r.flushdb()
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

                s1, s2, s3, s4 = set(inclusive[1:]), set(exclusive[1:]), set(exclusive2[1:]), set(exclusive3[1:])
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
        with self.redis() as r:
            r.flushdb()
            self.assertOk(r.execute_command(
                'ft.create', 'idx', 'schema', 'a', 'text', 'b', 'text', 'c', 'text', 'd', 'text'))
            for i in range(20):
                self.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                                'a', 'foo', 'b', 'bar', 'c', 'baz', 'd', 'gaz'))
            res = [
                r.execute_command('ft.search', 'idx', 'foo bar baz gaz', 'nocontent'),
                r.execute_command('ft.search', 'idx', '@a:foo @b:bar @c:baz @d:gaz', 'nocontent'),
                r.execute_command('ft.search', 'idx', '@b:bar @a:foo @c:baz @d:gaz', 'nocontent'),
                r.execute_command('ft.search', 'idx', '@c:baz @b:bar @a:foo @d:gaz', 'nocontent'),
                r.execute_command('ft.search', 'idx', '@d:gaz @c:baz @b:bar @a:foo', 'nocontent'),
                r.execute_command('ft.search', 'idx', '@a:foo (@b:bar (@c:baz @d:gaz))', 'nocontent'),
                r.execute_command('ft.search', 'idx', '@c:baz (@a:foo (@b:bar (@c:baz @d:gaz)))', 'nocontent'),
                r.execute_command('ft.search', 'idx', '@b:bar (@a:foo (@c:baz @d:gaz))', 'nocontent'),
                r.execute_command('ft.search', 'idx', '@d:gaz (@a:foo (@c:baz @b:bar))', 'nocontent'),
                r.execute_command('ft.search', 'idx', 'foo (bar baz gaz)', 'nocontent'),
                r.execute_command('ft.search', 'idx', 'foo (bar (baz gaz))', 'nocontent'),
                r.execute_command('ft.search', 'idx', 'foo (bar (foo bar) (foo bar))', 'nocontent'),
                r.execute_command('ft.search', 'idx', 'foo (foo (bar baz (gaz)))', 'nocontent'),
                r.execute_command('ft.search', 'idx', 'foo (foo (bar (baz (gaz (foo bar (gaz))))))', 'nocontent')]
            
            for r in res:
                self.assertListEqual(res[0], r)
            



    def testInKeys(self):
        with self.redis() as r:
            r.flushdb()
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
        with self.redis() as r:
            r.flushdb()
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
        with self.redis() as r:
            r.flushdb()
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

        with self.redis() as r:

            gsearch = lambda query, lon, lat, dist, unit='km': r.execute_command(
                'ft.search', 'idx', query, 'geofilter', 'location', lon, lat, dist, unit)

            gsearch_inline = lambda query, lon, lat, dist, unit='km': r.execute_command(
                'ft.search', 'idx', '{} @location:[{} {} {} {}]'.format(query,  lon, lat, dist, unit))

            r.flushdb()
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
                self.assertListEqual(res,res2)

                res = gsearch('hilton', "-0.1757", "51.5156", '10')
                self.assertEqual(14, res[0])
                self.assertEqual('hotel93', res[1])
                self.assertEqual('hotel92', res[3])
                self.assertEqual('hotel79', res[5])
                
                res2 = gsearch('hilton', "-0.1757", "51.5156", '10000', 'm')
                self.assertListEqual(res, res2)
                res2 = gsearch_inline('hilton', "-0.1757", "51.5156", '10')
                self.assertListEqual(res,res2)


                res = gsearch('heathrow', -0.44155, 51.45865, '10', 'm')
                self.assertEqual(1, res[0])
                self.assertEqual('hotel94', res[1])
                res2 = gsearch_inline('heathrow', -0.44155, 51.45865, '10', 'm')
                self.assertListEqual(res,res2)

                res = gsearch('heathrow', -0.44155, 51.45865, '10', 'km')
                self.assertEqual(5, res[0])
                self.assertIn('hotel94', res)
                res2 = gsearch_inline('heathrow', -0.44155, 51.45865, '10', 'km')
                self.assertListEqual(res,res2)

                res = gsearch('heathrow', -0.44155, 51.45865, '5', 'km')
                self.assertEqual(3, res[0])
                self.assertIn('hotel94', res)
                res2 = gsearch_inline('heathrow', -0.44155, 51.45865, '5', 'km')
                self.assertListEqual(res,res2)

    def testAddHash(self):

        with self.redis() as r:
            r.flushdb()
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
        with self.redis() as r:
            r.flushdb()
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
        with self.redis() as r:
            r.flushdb()
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

        with self.redis() as r:
            r.flushdb()
            self.assertOk(r.execute_command(
                'ft.create', 'idx', 'schema', 'TiTle', 'text', 'BoDy', 'text'))
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1, 'fields',
                                            'title', 'hello world', 'body', 'foo bar'))
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc2', 0.5, 'fields',
                                            'body', 'hello world', 'title', 'foo bar'))

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

    def testStemming(self):
        with self.redis() as r:
            r.flushdb()
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

        with self.redis() as r:
            r.flushdb()
            self.assertOk(r.execute_command(
                'ft.create', 'idx', 'schema', 'title', 'text'))
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 0.5, 'fields',
                                            'title', 'hello kitty'))

            res = r.execute_command(
                'ft.search', 'idx', 'hellos', "nocontent", "expander", "SBSTEM")
            self.assertEqual(2, len(res))
            self.assertEqual(1, res[0])

            res = r.execute_command(
                'ft.search', 'idx', 'hellos', "nocontent", "expander", "noexpander")
            self.assertEqual(1, len(res))
            self.assertEqual(0, res[0])

    def testNumericRange(self):

        with self.redis() as r:
            r.flushdb()
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

        with self.redis() as r:
            r.flushdb()
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
        with self.redis() as r:
            r.flushdb()
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

        with self.redis() as r:
            r.flushdb()
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
        with self.redis() as r:
            r.flushdb()
            self.assertOk(r.execute_command(
                'ft.create', 'idx', 'schema', 'foo', 'text'))
            for i in range(N):
                
                self.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0,
                                                'fields', 'foo', ' '.join(('term%d' % random.randrange(0, 10) for i in range(10)))))

            def get_stats(r):
                res = r.execute_command('ft.info', 'idx')
                d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
                gc_stats = {d['gc_stats'][x]: float(d['gc_stats'][x+1]) for x in range(0, len(d['gc_stats']), 2)}
                d['gc_stats'] = gc_stats
                return d
            
            stats = get_stats(r)
            self.assertGreater(stats['gc_stats']['current_hz'], 8)
            self.assertEqual(0, stats['gc_stats']['bytes_collected'])
            self.assertGreater(int(stats['num_records']), 0)
            
            initialIndexSize = float(stats['inverted_sz_mb'])*1024*1024
            for i in range(N):
                self.assertEqual(1, r.execute_command('ft.del', 'idx', 'doc%d' % i))
            st = time.time()
            while st + 2 > time.time():
                time.sleep(0.1)
                stats = get_stats(r)
                if stats['num_records'] == '0': 
                    break
            self.assertEqual('0', stats['num_docs'])
            self.assertEqual('0', stats['num_records'])
            self.assertEqual('100', stats['max_doc_id'])
            self.assertGreater( stats['gc_stats']['current_hz'], 50)
            currentIndexSize = float(stats['inverted_sz_mb'])*1024*1024
            #print initialIndexSize, currentIndexSize, stats['gc_stats']['bytes_collected']
            self.assertGreater(initialIndexSize, currentIndexSize)
            self.assertGreater(stats['gc_stats']['bytes_collected'], currentIndexSize)

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
        self.assertCmdOk('ft.add', 'idx', 'doc100', 1.0, 'fields', 'f1', 'foo bar')
        self.assertCmdOk('ft.add', 'idx', 'doc200', 1.0, 'fields', 'f1', ('foo ' * 10) + ' bar')
        res = self.cmd('ft.search', 'idx', 'foo')
        self.assertEqual(2, res[0])
        if has_offsets:
            docname = res[1]
            if has_freqs:
                self.assertEqual('doc200', docname)
            else:
                self.assertEqual('doc100', docname)

        self.assertCmdOk('ft.add', 'idx', 'doc300', 1.0, 'fields', 'f1', 'Hello')
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

        with self.redis() as r:
            r.flushdb()
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
                self.assertListEqual(d['fields'], [['title', 'type', 'TEXT', 'WEIGHT', '1']])
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
                # make sure that an empty opts string returns no options in info
                if not combo:
                    self.assertListEqual([], opts)

                for option in filter(None, combo):
                    self.assertTrue(option in opts)

    def testAof(self):
        self.spawn_server(use_aof=True)
        self.cmd('ft.create', 'idx', 'schema', 'field1', 'text', 'field2', 'numeric')
        self.restart_and_reload()
        for x in range(1, 10):
            self.assertCmdOk('ft.add', 'idx', 'doc{}'.format(x), 1.0 / x, 'fields',
                             'field1', 'myText{}'.format(x), 'field2', 20 * x)

        exp = [9L, 'doc1', ['field1', 'myText1', 'field2', '20'], 'doc2', ['field1', 'myText2', 'field2', '40'], 'doc3', ['field1', 'myText3', 'field2', '60'], 'doc4', ['field1', 'myText4', 'field2', '80'], 'doc5', ['field1',
                                                                                                                                                                                                                        'myText5', 'field2', '100'], 'doc6', ['field1', 'myText6', 'field2', '120'], 'doc7', ['field1', 'myText7', 'field2', '140'], 'doc8', ['field1', 'myText8', 'field2', '160'], 'doc9', ['field1', 'myText9', 'field2', '180']]
        self.restart_and_reload()
        ret = self.cmd('ft.search', 'idx', 'myt*')
        self.assertEqual(exp, ret)

    def testNoStem(self):
        self.cmd('ft.create', 'idx', 'schema', 'body', 'text', 'name', 'text', 'nostem')
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
        self.cmd('ft.search', 'idx', 'Foo', 'GEOFILTER', 'place', '-77.0366', '38.8977', '1', 'km')

    def testSortbyMissingField(self):
        # GH Issue 131
        self.cmd('ft.create', 'ix', 'schema', 'txt', 'text', 'num', 'numeric', 'sortable')
        self.cmd('ft.add', 'ix', 'doc1', 1.0, 'fields', 'txt', 'foo')
        self.cmd('ft.search', 'ix', 'foo', 'sortby', 'num')


def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    from itertools import izip_longest
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)


if __name__ == '__main__':

    unittest.main()
