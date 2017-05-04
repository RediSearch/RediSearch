from rmtest import ModuleTestCase
import redis
import unittest
from hotels import hotels
import random

class SearchTestCase(ModuleTestCase('../module.so')):

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
                'ft.create', 'idx', 'schema', 'f', 'text'))

            for i in range(200):
                self.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                                'f', 'hello world'))

            keys = r.keys('*')
            self.assertEqual(203, len(keys))

            self.assertOk(r.execute_command('ft.drop', 'idx'))
            keys = r.keys('*')
            self.assertEqual(0, len(keys))


    def testOptional(self):
        with self.redis() as r:
            r.flushdb()
            self.assertOk(r.execute_command(
                'ft.create', 'idx', 'schema', 'foo', 'text'))
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields', 'foo', 'hello wat woot'))
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc2', 1.0, 'fields', 'foo', 'hello world woot'))
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc3', 1.0, 'fields', 'foo', 'hello world werld'))

            res  = r.execute_command('ft.search', 'idx', 'hello', 'nocontent')
            self.assertEqual([3L, 'doc1', 'doc2', 'doc3'], res)
            res  = r.execute_command('ft.search', 'idx', 'hello world', 'nocontent', 'scorer', 'DISMAX')
            self.assertEqual([2L, 'doc2', 'doc3'], res)
            res  = r.execute_command('ft.search', 'idx', 'hello ~world', 'nocontent', 'scorer', 'DISMAX')
            self.assertEqual([3L, 'doc2', 'doc3', 'doc1'], res)
            res  = r.execute_command('ft.search', 'idx', 'hello ~world ~werld', 'nocontent', 'scorer', 'DISMAX')
            self.assertEqual([3L, 'doc3', 'doc2', 'doc1'], res)

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
                res = r.execute_command('ft.search', 'idx', 'constant term', 'nocontent')
                self.assertEqual([0], res)
                res = r.execute_command('ft.search', 'idx', 'constant term*', 'nocontent')
                self.assertEqual(N, res[0])
                res = r.execute_command('ft.search', 'idx', 'const* term*', 'nocontent')
                self.assertEqual(N, res[0])
                res = r.execute_command('ft.search', 'idx', 'constant term1*', 'nocontent')
                self.assertGreater(res[0], 2)
                res = r.execute_command('ft.search', 'idx', 'const* -term*', 'nocontent')
                self.assertEqual([0], res)
                res = r.execute_command('ft.search', 'idx', 'constant term9*', 'nocontent')
                self.assertEqual([0], res)

    def testSortBy(self):
        with self.redis() as r:
            r.flushdb()
            self.assertOk(r.execute_command(
                'ft.create', 'idx', 'schema', 'foo', 'text', 'sortable', 'bar', 'numeric', 'sortable'))
            N = 100
            for i in range(N):
                self.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                                'foo', 'hello%03d world' % i, 'bar', 100-i))
            for _ in r.retry_with_rdb_reload():
          
                res = r.execute_command('ft.search', 'idx', 'world', 'nocontent', 'sortby', 'foo')
                self.assertEqual([100L, 'doc0', 'doc1', 'doc2', 'doc3', 'doc4', 'doc5', 'doc6', 'doc7', 'doc8', 'doc99'], res)
                res = r.execute_command('ft.search', 'idx', 'world', 'nocontent', 'sortby', 'foo', 'desc')
                self.assertEqual([100L, 'doc99', 'doc98', 'doc97', 'doc96', 'doc95', 'doc94', 'doc93', 'doc92', 'doc91', 'doc90'], res)
                res = r.execute_command('ft.search', 'idx', 'world', 'nocontent', 'sortby', 'bar', 'desc')
                self.assertEqual([100L, 'doc0', 'doc1', 'doc2', 'doc3', 'doc4', 'doc5', 'doc6', 'doc7', 'doc8', 'doc99'], res)
                res = r.execute_command('ft.search', 'idx', 'world', 'nocontent', 'sortby', 'bar', 'asc')
                self.assertEqual([100L, 'doc99', 'doc98', 'doc97', 'doc96', 'doc95', 'doc94', 'doc93', 'doc92', 'doc91', 'doc90'], res)
                res = r.execute_command('ft.search', 'idx', 'world', 'nocontent', 'sortby', 'bar', 'desc', 'withscores', 'limit', '2', '5')
                self.assertEqual([100L, 'doc2', '5', 'doc3', '4', 'doc4', '3', 'doc5', '2', 'doc99', '1'], res)



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
                inclusive = r.execute_command('ft.search', 'idx', 'constant term%d' % i, 'nocontent', 'limit', 0, N)
                
                exclusive = r.execute_command('ft.search', 'idx', 'constant -term%d' % i, 'nocontent', 'limit', 0, N)
                #print inclusive, exclusive
                self.assertNotEqual(inclusive[0], N)
                self.assertEqual(inclusive[0] + exclusive[0], N)

                s1, s2 = set(inclusive[1:]), set(exclusive[1:])
                self.assertTrue(s1.difference(s2) == s1)
                self.assertTrue(s2.intersection(s1) == set())
            
            # NOT on a non existing term
            self.assertEqual(r.execute_command('ft.search', 'idx', 'constant -dasdfasdf', 'nocontent')[0], N)
            #not on self term
            self.assertEqual(r.execute_command('ft.search', 'idx', 'constant -constant', 'nocontent'), [0])

            self.assertEqual(r.execute_command('ft.search', 'idx', 'constant -(term0|term1|term2|term3|term4|nothing)', 'nocontent'), [0])
            #self.assertEqual(r.execute_command('ft.search', 'idx', 'constant -(term1 term2)', 'nocontent')[0], N)

    def testInKeys(self):
        with self.redis() as r:
            r.flushdb()
            self.assertOk(r.execute_command(
                'ft.create', 'idx', 'schema', 'foo', 'text'))

            for i in range(200):
                self.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                                'foo', 'hello world'))

            for keys in (
                ['doc%d' % i for i in range(10)], ['doc%d' % i for i in range(0, 30, 2)], [
                    'doc%d' % i for i in range(99, 0, -5)]
            ):
                res = r.execute_command(
                    'ft.search', 'idx', 'hello world', 'NOCONTENT', 'LIMIT', 0, 100, 'INKEYS', len(keys), *keys)
                self.assertEqual(len(keys), res[0])
                self.assertTrue(all((k in res for k in keys)))
            
            self.assertEqual(0, r.execute_command(
                'ft.search', 'idx', 'hello world', 'NOCONTENT', 'LIMIT', 0, 100, 'INKEYS', 4, 'foo', 'bar', 'baz')[0])

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
            self.assertEqual([4L, 'doc3', 'doc4', 'doc2', 'doc1'], res)
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
                self.assertEqual('hotel2', res[1])
                self.assertEqual('hotel21', res[3])
                self.assertEqual('hotel79', res[5])

                res = gsearch('hilton', "-0.1757", "51.5156", '10')
                self.assertEqual(14, res[0])
                self.assertEqual('hotel1', res[1])
                self.assertEqual('hotel2', res[3])
                self.assertEqual('hotel21', res[5])

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
            self.assertEqual("doc1", res[1])
            self.assertEqual("doc2", res[2])

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
            res = r.execute_command('ft.search', 'idx', 'foo', 'scorer', 'TFIDF')
            self.assertEqual(res, [0])
            with self.assertResponseError():
                res = r.execute_command('ft.search', 'idx', 'foo', 'scorer', 'NOSUCHSCORER')
            
            
    def testFieldSelectors(self):

        with self.redis() as r:
            r.flushdb()
            self.assertOk(r.execute_command(
                'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1, 'fields',
                                            'title', 'hello world', 'body', 'foo bar'))
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc2', 0.5, 'fields',
                                            'body', 'hello world', 'title', 'foo bar'))

            res = r.execute_command('ft.search', 'idx', '@title:hello world', 'nocontent')
            self.assertEqual(res, [1, 'doc1'])
            res = r.execute_command('ft.search', 'idx', '@body:hello world', 'nocontent')
            self.assertEqual(res, [1, 'doc2'])

            res = r.execute_command('ft.search', 'idx', '@body:hello @title:world', 'nocontent')
            self.assertEqual(res, [0])

            res = r.execute_command('ft.search', 'idx', '@body:hello world @title:world', 'nocontent')
            self.assertEqual(res, [0])
            res = r.execute_command('ft.search', 'idx', '@body:(hello|foo) @title:(world|bar)', 'nocontent')
            self.assertEqual(res, [2, 'doc1', 'doc2'])

            res = r.execute_command('ft.search', 'idx', '@body:(hello|foo world|bar)', 'nocontent')
            self.assertEqual(res, [2, 'doc1', 'doc2'])

            res = r.execute_command('ft.search', 'idx', '@body|title:(hello world)', 'nocontent')
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


if __name__ == '__main__':

    unittest.main()
