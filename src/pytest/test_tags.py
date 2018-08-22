import redis
import unittest
import platform
from base_case import BaseSearchTestCase


class TagsTestCase(BaseSearchTestCase):

    def search(self, r, *args):
        return r.execute_command('ft.search', *args)

    def testTagIndex(self):
        r = self
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
                r, 'idx', '@tags:{tag 1} @tags:{foo bar}', 'slop', '0', 'inorder')
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
        r = self
        self.assertOk(r.execute_command(
            'ft.create', 'idx', 'schema', 'title', 'text', 'tags', 'tag', 'separator', ':'))

        self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                        'title', 'hello world', 'tags', 'x:hello world: fooz bar:foo,bar:BOO FAR'))
        for _ in r.retry_with_rdb_reload():

            for q in ('@tags:{hello world}', '@tags:{fooz bar}', '@tags:{foo\\,bar}', '@tags:{boo\\ far}', '@tags:{x}'):
                res = self.search(r, 'idx', q)
                self.assertEqual(
                    1, res[0], msg='Error trying {}'.format(q))

    def testTagPrefix(self):
        r = self
        self.assertOk(r.execute_command(
            'ft.create', 'idx', 'schema', 'title', 'text', 'tags', 'tag', 'separator', ','))

        self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                        'title', 'hello world', 'tags', 'hello world,hello-world,hell,jell'))
        for _ in r.retry_with_rdb_reload():

            for q in ('@tags:{hello world}', '@tags:{hel*}', '@tags:{hello\\-*}', '@tags:{he*}'):
                res = self.search(r, 'idx', q)
                self.assertEqual(
                    res[0], 1, msg='Error trying {}, got {}'.format(q, res))

    def testTagFieldCase(self):
        r = self
        self.assertOk(r.execute_command(
            'ft.create', 'idx', 'schema', 'title', 'text', 'TAgs', 'tag'))

        self.assertOk(r.execute_command('ft.add', 'idx', 'doc1', 1.0, 'fields',
                                        'title', 'hello world', 'TAgs', 'HELLO WORLD,FOO BAR'))
        for _ in r.retry_with_rdb_reload():

            self.assertListEqual([0], r.execute_command(
                'FT.SEARCH', 'idx', '@tags:{HELLO WORLD}'))
            self.assertListEqual([1, 'doc1'], r.execute_command(
                'FT.SEARCH', 'idx', '@TAgs:{HELLO WORLD}', 'NOCONTENT'))
            self.assertListEqual([1, 'doc1'], r.execute_command(
                'FT.SEARCH', 'idx', '@TAgs:{foo bar}', 'NOCONTENT'))
            self.assertListEqual([0], r.execute_command(
                'FT.SEARCH', 'idx', '@TAGS:{foo bar}', 'NOCONTENT'))

    def testInvalidSyntax(self):
        r = self
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
        r = self
        r.execute_command(
            'ft.create', 'idx', 'schema', 'title', 'text', 'tags', 'tag', 'othertags', 'tag')

        N = 100
        alltags = set()
        for n in range(N):
            tags = ('foo %d' % n, 'bar %d' % n, 'x')
            alltags.add(tags[0])
            alltags.add(tags[1])
            alltags.add(tags[2])

            self.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' % n, 1.0, 'fields',
                                            'tags', ','.join(tags), 'othertags', 'baz %d' % int(n // 2)))
        for _ in r.retry_with_rdb_reload():
            res = r.execute_command('ft.tagvals', 'idx', 'tags')
            self.assertEqual(N * 2 + 1, len(res))

            self.assertSetEqual(alltags, set(res))

            res = r.execute_command('ft.tagvals', 'idx', 'othertags')
            self.assertEqual(N / 2, len(res))


if __name__ == '__main__':
    unittest.main()
