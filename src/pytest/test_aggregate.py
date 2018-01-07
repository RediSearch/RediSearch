from rmtest import ModuleTestCase
import redis
import bz2
import json
import unittest
import itertools


class AggregateTestCase(ModuleTestCase('../src/module-oss.so')):

    ingested = False

    def ingest(self):
        if self.ingested:
            return
        self.cmd('flushdb')

        self.ingested = True
        self.cmd('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT', 'BRAND', 'TEXT',  'NOSTEM', 'SORTABLE',
                 'description', 'TEXT', 'price', 'NUMERIC', 'SORTABLE', 'categories', 'TAG')
        fp = bz2.BZ2File('games.json.bz2')

        for line in fp:
            obj = json.loads(line)
            id = obj['asin']
            del obj['asin']
            obj['price'] = obj.get('price') or 0
            obj['categories'] = ','.join(obj['categories'])
            cmd = ['FT.ADD', 'idx', id, 1, 'FIELDS', ] + \
                [str(x) if x is not None else '' for x in itertools.chain(
                    *obj.items())]
            # print cmd
            self.cmd(*cmd)

    def setUp(self):

        self.ingest()

    def testGroupBy(self):
        return
        cmd = ['ft.aggregate', 'idx', 'sony',
               'SELECT', '2', '@brand', '@price',
               'GROUPBY', '1', '@brand',
               'REDUCE', 'count', '0',
               'GROUPBY', '1', '@count',
               'REDUCE', 'tolist', '1', '@brand',
               'SORTBY', '1', '@count']

        res = self.cmd(*cmd)
        self.assertIsNotNone(res)
        self.assertEqual(7, len(res))
        print res

if __name__ == '__main__':

    unittest.main()
