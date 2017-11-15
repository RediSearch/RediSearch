from rmtest import ModuleTestCase
import redis
import unittest
import platform


class TagsTestCase(ModuleTestCase('../redisearch.so')):

    def search(self, r, *args):
        return r.execute_command('ft.search', *args)

    def testTagIndex(self):
        with self.redis() as r:
            r.flushdb()

            self.assertOk(r.execute_command(
                'ft.create', 'idx', 'schema', 'title', 'text', 'tags', 'tag'))
            N = 10
            for n in range(N):

                self.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % n, 1.0, 'fields',
                                                'title', 'hello world term%d' % n, 'tags', 'foo bar,xxx,tag %d' % n))
            for _ in r.retry_with_rdb_reload():
                res = self.search(r, 'idx', 'hello world')
                self.assertEqual(10, res[0])

                res = self.search(r, 'idx', 'foo bar')
                self.assertEqual(0, res[0])

                res = self.search(r, 'idx', '@tags:{foo bar}')
                self.assertEqual(N, res[0])

                # inorder should not affect tags
                res = self.search(
                    r, 'idx', '@tags:{tag 1} @tags:{foo bar}', 'slop 0', 'inorder')
                self.assertEqual(1, res[0])

                for n in range(N - 1):
                    res = self.search(
                        r, 'idx', '@tags:{tag %d}' % n, 'nocontent')
                    self.assertEqual(1, res[0])
                    self.assertEqual('doc%d' % n, res[1])
                    res = self.search(
                        r, 'idx', '@tags:{tag\\ %d}' % n, 'nocontent')
                    self.assertEqual(1, res[0])

                    res = self.search(
                        r, 'idx', 'hello world @tags:{tag\\ %d|tag %d}' % (n, n + 1), 'nocontent')
                    self.assertEqual(2, res[0])
                    self.assertEqual('doc%d' % n, res[2])
                    self.assertEqual('doc%d' % (n + 1), res[1])

                    res = self.search(
                        r, 'idx', 'term%d @tags:{tag %d}' % (n, n), 'nocontent')
                    self.assertEqual(1, res[0])
                    self.assertEqual('doc%d' % n, res[1])

    def testSeparator(self):
        with self.redis() as r:
            r.flushdb()

            self.assertOk(r.execute_command(
                'ft.create', 'idx', 'schema', 'title', 'text', 'tags', 'tag', 'separator', ':'))

            self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                            'title', 'hello world', 'tags', 'hello world: fooz bar:foo,bar:BOO FAR'))
            for _ in r.retry_with_rdb_reload():

                for q in ('@tags:{hello world}', '@tags:{fooz bar}', '@tags:{foo\\,bar}', '@tags:{boo\\ far}'):
                    res = self.search(r, 'idx', q)
                    self.assertEqual(
                        1, res[0], msg='Error trying {}'.format(q))

            r.flushdb()

    def testInvalidSyntax(self):
        with self.redis() as r:
            r.flushdb()
            # invalid syntax
            with self.assertResponseError():
                r.execute_command(
                    'ft.create', 'idx', 'schema', 'title', 'text', 'tags', 'tag', 'separator')
            with self.assertResponseError():
                r.execute_command(
                    'ft.create', 'idx', 'schema', 'title', 'text', 'tags', 'tag', 'separator', "foo")
            with self.assertResponseError():
                r.execute_command(
                    'ft.create', 'idx', 'schema', 'title', 'text', 'tags', 'tag', 'separator', "")

    def testTagVals(self):
        with self.redis() as r:
            r.flushdb()

            r.execute_command(
                'ft.create', 'idx', 'schema', 'title', 'text', 'tags', 'tag', 'othertags', 'tag')

            N = 100
            alltags = set()
            for n in range(N):
                tags = ('foo %d' % n, 'bar %d' % n)
                alltags.add(tags[0])
                alltags.add(tags[1])
                self.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % n, 1.0, 'fields',
                                                'tags', ','.join(tags), 'othertags', 'baz %d' % int(n // 2)))
            for _ in r.retry_with_rdb_reload():
                res = r.execute_command('ft.tagvals', 'idx', 'tags')
                self.assertEqual(N * 2, len(res))
                self.assertSetEqual(alltags, set(res))

                res = r.execute_command('ft.tagvals', 'idx', 'othertags')
                self.assertEqual(N / 2, len(res))


if __name__ == '__main__':

    unittest.main()
