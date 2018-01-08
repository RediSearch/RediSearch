from rmtest import ModuleTestCase
import redis
import bz2
import json
import unittest
import itertools
import pprint


class AggregateTestCase(ModuleTestCase('../redisearch.so')):

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
               'SORTBY', '1', '@price']

        res = self.cmd(*cmd)
        self.assertIsNotNone(res)
        self.assertEqual(7, len(res))
        print res

    def testMin(self):
        cmd = ['ft.aggregate', 'idx', 'sony',
            'SELECT', '2', '@brand', '@price',
            'GROUPBY', '1', '@brand',
            'REDUCE', 'min', '1', '@price',
            'SORTYBY', '1', 'min(price)']
        res = self.cmd(*cmd)
        self.assertIsNotNone(res)
        self.assertEqual(172, res[0])
        print ""
        pprint.pprint(res[1:])
        # self.assertEqual(7, len(res))
    
    def testAvg(self):
        cmd = ['ft.aggregate', 'idx', 'sony',
            'SELECT', '2', '@brand', '@price',
            'GROUPBY', '1', '@brand',
            'REDUCE', 'avg', '1', '@price',
            'REDUCE', 'count', '0',
            'SORTYBY', '1', 'avg(price)']
        res = self.cmd(*cmd)
        self.assertIsNotNone(res)
        self.assertEqual(172, res[0])
        print ""
        pprint.pprint(res[1:])
        # self.assertEqual(7, len(res))


if __name__ == '__main__':

    unittest.main()
