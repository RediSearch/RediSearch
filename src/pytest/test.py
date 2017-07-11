from rmtest import ModuleTestCase
import redis
import unittest
from hotels import hotels
import random


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
                    'ft.search', 'idx', '(hello|world)(hallo|werld)', 'nocontent', 'verbatim', 'limit', '0', '100')
                self.assertEqual(1, len(res))
                self.assertEqual(0, res[0])

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
            N = 100
            for i in range(N):
                self.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                                'foo', 'constant term%d' % (random.randrange(0, 5))))

            for i in range(5):
                inclusive = r.execute_command(
                    'ft.search', 'idx', 'constant term%d' % i, 'nocontent', 'limit', 0, N)

                exclusive = r.execute_command(
                    'ft.search', 'idx', 'constant -term%d' % i, 'nocontent', 'limit', 0, N)
                # print inclusive, exclusive
                self.assertNotEqual(inclusive[0], N)
                self.assertEqual(inclusive[0] + exclusive[0], N)

                s1, s2 = set(inclusive[1:]), set(exclusive[1:])
                self.assertTrue(s1.difference(s2) == s1)
                self.assertTrue(s2.intersection(s1) == set())

            # NOT on a non existing term
            self.assertEqual(r.execute_command(
                'ft.search', 'idx', 'constant -dasdfasdf', 'nocontent')[0], N)
            # not on self term
            self.assertEqual(r.execute_command(
                'ft.search', 'idx', 'constant -constant', 'nocontent'), [0])

            self.assertEqual(r.execute_command(
                'ft.search', 'idx', 'constant -(term0|term1|term2|term3|term4|nothing)', 'nocontent'), [0])
            #self.assertEqual(r.execute_command('ft.search', 'idx', 'constant -(term1 term2)', 'nocontent')[0], N)

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

                res = gsearch('hilton', "-0.1757", "51.5156", '10')
                self.assertEqual(14, res[0])
                self.assertEqual('hotel93', res[1])
                self.assertEqual('hotel92', res[3])
                self.assertEqual('hotel79', res[5])

                res2 = gsearch('hilton', "-0.1757", "51.5156", '10000', 'm')
                self.assertListEqual(res, res2)

                res = gsearch('heathrow', -0.44155, 51.45865, '10', 'm')
                self.assertEqual(1, res[0])
                self.assertEqual('hotel94', res[1])

                res = gsearch('heathrow', -0.44155, 51.45865, '10', 'km')
                self.assertEqual(5, res[0])
                self.assertIn('hotel94', res)

                res = gsearch('heathrow', -0.44155, 51.45865, '5', 'km')
                self.assertEqual(3, res[0])
                self.assertIn('hotel94', res)

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


def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    from itertools import izip_longest
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)


if __name__ == '__main__':

    unittest.main()
