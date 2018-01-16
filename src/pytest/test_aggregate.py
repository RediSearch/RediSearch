from rmtest import ModuleTestCase
import redis
import bz2
import json
import unittest
import itertools
import pprint


class AggregateTestCase(ModuleTestCase('../redisearch.so', module_args=['SAFEMODE'])):

    ingested = False

    def ingest(self):
        if self.ingested:
            return
        self.cmd('flushdb')

        self.ingested = True
        self.cmd('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT', 'SORTABLE', 'BRAND', 'TEXT',  'NOSTEM', 'SORTABLE',
                 'description', 'TEXT', 'price', 'NUMERIC', 'SORTABLE', 'categories', 'TAG')
        client = self.client
        fp = open('games.json', 'r')

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

    def testCountDistinct(self):
        cmd = ['FT.AGGREGATE', 'idx', '-@brand:lkjdklsa',
            'SELECT', '3', '@brand', '@categories', '@title',
            'GROUPBY', '1', '@categories',
            'REDUCE', 'COUNT_DISTINCT', '1', '@title',
            'REDUCE', 'COUNT', '0'
            ]
        self.cmd('PING')
        pprint.pprint(self.cmd(*cmd)[1:])

    def testCountDistinctish(self):
        self.cmd('PING')
        cmd = ['FT.AGGREGATE', 'idx', '-@brand:lkjdklsa',
            'SELECT', '3', '@brand', '@categories', '@title',
            'GROUPBY', '1', '@categories',
            'REDUCE', 'COUNT_DISTINCTISH', '1', '@title',
            'REDUCE', 'COUNT', '0'
            ]
        res = self.cmd(*cmd)[1:]
        # pprint.pprint(res)
    
    def testQuantile(self):
        self.cmd('PING')
        cmd = ['FT.AGGREGATE', 'idx', '-@brand:aadsfgds',
            'GROUPBY', '1', '@brand',
            'REDUCE', 'QUANTILE', '2', '@price', '0.50',
            'REDUCE', 'AVG', '1', '@price',
            'REDUCE', 'COUNT', '0']
        pprint.pprint(self.cmd(*cmd))


if __name__ == '__main__':

    unittest.main()
