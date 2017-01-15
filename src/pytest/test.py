from rmtest import ModuleTestCase
import redis
import unittest


class SearchTestCase(ModuleTestCase('../module.so', fixed_port=6379)):

    def testAdd(self):
        with self.redis() as r:
            r.flushdb()
            self.assertOk(r.execute_command(
                'ft.create', 'idx', 'schema', 'title', 'text', 'body', 'text'))
            self.assertTrue(r.exists('idx:idx'))
            self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                            'title', 'hello world',
                                            'body', 'lorem ist ipsum'))

            for prefix in ('ft', 'ss'):
                self.assertExists(r, prefix + ':idx/hello')
                self.assertExists(r, prefix + ':idx/world')
                self.assertExists(r, prefix + ':idx/lorem')

    def testUnion(self):

        with self.redis() as r:
            r.flushdb()
            self.assertOk(r.execute_command(
                'ft.create', 'idx', 'schema', 'f', 'text'))
            for i in range(100):

                self.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                                                'f', 'hello world' if i % 2 == 0 else 'hallo werld'))

            res = r.execute_command(
                'ft.search', 'idx', 'hello|hallo|world|werld', 'nocontent', 'limit', '0', '100')
            self.assertEqual(101, len(res))
            self.assertEqual(100, res[0])

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
            res = r.execute_command('ft.search', 'idx', 'hello', 'nocontent')
            self.assertTrue(len(res) == 3)
            self.assertEqual(res[0], 2L)
            self.assertEqual(res[1], "doc2")
            self.assertEqual(res[2], "doc1")

            # Test searching WITHSCORES
            res = r.execute_command('ft.search', 'idx', 'hello', 'WITHSCORES')
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
                res = r.execute_command(
                    'ft.search', 'idx', 'hello', 'nocontent', 'limit', 0, 100)
                self.assertNotIn('doc%d' % i, res)
                self.assertEqual(res[0], 100 - i - 1)
                self.assertEqual(len(res), 100 - i)

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
                'ft.search', 'idx', "hello werld", "filter", "nocontent")
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
                'ft.search', 'idx', 'hellos', "nocontent", "expander", "stem")
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
                'ft.create', 'idx', 'schema', 'title', 'text', 'score', 'numeric'))
            for i in xrange(100):
                self.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % i, 1, 'fields',
                                                'title', 'hello kitty', 'score', i))

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
            rc = r.execute_command("ft.SUGGET", "ac", "hello", "WITHSCORES")
            self.assertEqual(4, len(rc))
            self.assertTrue(float(rc[1]) > 0)
            self.assertTrue(float(rc[3]) > 0)

            rc = r.execute_command("ft.SUGDEL", "ac", "hello world")
            self.assertEqual(1L, rc)
            rc = r.execute_command("ft.SUGDEL", "ac", "world")
            self.assertEqual(0L, rc)

            rc = r.execute_command("ft.SUGGET", "ac", "hello")
            self.assertEqual(['hello werld'], rc)


if __name__ == '__main__':

    unittest.main()
