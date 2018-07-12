import unittest
import platform
from base_case import BaseSearchTestCase


class SearchTestCase(BaseSearchTestCase):
    def testWideSchema(self):
        r = self
        schema = []
        FIELDS = 128 if platform.architecture()[0] == '64bit' else 64
        for i in range(FIELDS):
            schema.extend(('field_%d' % i, 'TEXT'))
        self.assertOk(self.cmd('ft.create', 'idx', 'schema', *schema))
        N = 10
        for n in range(N):
            fields = []
            for i in range(FIELDS):
                fields.extend(('field_%d' % i, 'hello token_%d' % i))
            self.assertOk(r.execute_command('ft.add', 'idx',
                                            'doc%d' % n, 1.0, 'fields', *fields))
        for _ in self.reloading_iterator():
            for i in range(FIELDS):

                res = self.search('idx', '@field_%d:token_%d' % (i, i), 'NOCONTENT')
                self.assertEqual(res[0], N)

                res = r.execute_command(
                    'ft.explain', 'idx', '@field_%d:token_%d' % (i, i), 'VERBATIM').strip()
                self.assertEqual('@field_%d:token_%d' % (i, i), res)

                res = self.search('idx', 'hello @field_%d:token_%d' % (i, i), 'NOCONTENT')
                self.assertEqual(res[0], N)

            res = self.search('idx', ' '.join(
                ('@field_%d:token_%d' % (i, i) for i in range(FIELDS))))
            self.assertEqual(res[0], N)

            res = self.search('idx', ' '.join(
                ('token_%d' % (i) for i in range(FIELDS))))
            self.assertEqual(res[0], N)

if __name__ == '__main__':

    unittest.main()
