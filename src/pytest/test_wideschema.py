from rmtest import ModuleTestCase
import redis
import unittest
import platform


class SearchTestCase(ModuleTestCase('../redisearch.so')):

    def search(self, r, *args):
        return r.execute_command('ft.search', *args)

    def testWideSchema(self):
        with self.redis() as r:
            r.flushdb()
            schema = []
            FIELDS = 128 if platform.architecture()[0] == '64bit' else 64
            for i in range(FIELDS):
                schema.extend(('field_%d' % i, 'TEXT'))
            self.assertOk(r.execute_command(
                'ft.create', 'idx', 'schema', *schema))
            N = 10
            for n in range(N):
                fields = []
                for i in range(FIELDS):
                    fields.extend(('field_%d' % i, 'hello token_%d' % i))
                self.assertOk(r.execute_command('ft.add', 'idx', 'doc%d' %n, 1.0, 'fields', *fields))
            for _ in r.retry_with_rdb_reload():
                for i in range(FIELDS):

                    res = self.search(r,'idx', '@field_%d:token_%d' % (i, i), 'NOCONTENT')
                    self.assertEqual(res[0], N)

                    res = r.execute_command('ft.explain','idx', '@field_%d:token_%d' % (i, i)).strip()
                    self.assertEqual('@field_%d:token_%d' % (i, i), res)

                    res = self.search(r,'idx', 'hello @field_%d:token_%d' % (i, i), 'NOCONTENT')
                    self.assertEqual(res[0], N)

                res = self.search(r, 'idx', ' '.join(('@field_%d:token_%d' % (i, i) for i in range(FIELDS))))
                self.assertEqual(res[0], N)

                res = self.search(r, 'idx', ' '.join(('token_%d' % (i) for i in range(FIELDS))))
                self.assertEqual(res[0], N)

if __name__ == '__main__':

    unittest.main()
