from rmtest import ModuleTestCase
import redis
import bz2
import json
import unittest
import itertools
import pprint


def to_dict(res):
    d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
    return d

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
        cmd = ['ft.aggregate', 'idx', 'sony',
               'GROUPBY', '1', '@brand',
               'REDUCE', 'count', '0',
               'GROUPBY', '1', '@count',
               'REDUCE', 'tolist', '1', '@brand',
               'SORTBY', '1', '@price']

        res = self.cmd(*cmd)
        self.assertIsNotNone(res)
        self.assertEqual(3, len(res))
        # print res

    def testMinMax(self):
        cmd = ['ft.aggregate', 'idx', 'sony',
            'GROUPBY', '1', '@brand',
            'REDUCE', 'count', '0',
            'REDUCE', 'min', '1', '@price', 'as', 'minPrice',
            'SORTBY', '1', '@minPrice']
        res = self.cmd(*cmd)
        self.assertIsNotNone(res)
        row = to_dict(res[1])
        self.assertEqual(88, int(float(row['minPrice'])))

        cmd = ['ft.aggregate', 'idx', 'sony',
            'GROUPBY', '1', '@brand',
            'REDUCE', 'count', '0',
            'REDUCE', 'max', '1', '@price', 'as', 'maxPrice',
            'SORTBY', '1', '@maxPrice']
        res = self.cmd(*cmd)
        row = to_dict(res[1])
        self.assertEqual(695, int(float(row['maxPrice'])))

    def testAvg(self):
        cmd = ['ft.aggregate', 'idx', 'sony',
            'GROUPBY', '1', '@brand',
            'REDUCE', 'avg', '1', '@price',
            'REDUCE', 'count', '0',
            'SORTBY', '1', '@avg(price)',]
        res = self.cmd(*cmd)
        self.assertIsNotNone(res)
        self.assertEqual(26, res[0])
        # Ensure the formatting actually exists

        first_row = to_dict(res[1])
        self.assertEqual(109, int(float(first_row['avg(price)'])))

        for row in res[1:]:
            row = to_dict(row)
            self.assertTrue('avg(price)' in row)

        # Test aliasing
        cmd = ['FT.AGGREGATE', 'idx', 'sony', 'GROUPBY', '1', '@brand',
            'REDUCE', 'avg', '1', '@price', 'AS', 'avgPrice']
        res = self.cmd(*cmd)
        first_row = to_dict(res[1])
        self.assertEqual(17, int(float(first_row['avgPrice'])))

    def testCountDistinct(self):
        cmd = ['FT.AGGREGATE', 'idx', '-@brand:lkjdklsa',
            'GROUPBY', '1', '@categories',
            'REDUCE', 'COUNT_DISTINCT', '1', '@title',
            'REDUCE', 'COUNT', '0'
            ]
        res = self.cmd(*cmd)[1:]
        row = to_dict(res[0])
        self.assertEqual(2207, int(row['count_distinct(title)']))

        cmd = ['FT.AGGREGATE', 'idx', '-@brand:lkjdklsa',
            'GROUPBY', '1', '@categories',
            'REDUCE', 'COUNT_DISTINCTISH', '1', '@title',
            'REDUCE', 'COUNT', '0'
            ]
        res = self.cmd(*cmd)[1:]
        row = to_dict(res[0])
        self.assertEqual(2207, int(row['count_distinctish(title)']))

    def testQuantile(self):
        cmd = ['FT.AGGREGATE', 'idx', '-@brand:aadsfgds',
            'GROUPBY', '1', '@brand',
            'REDUCE', 'QUANTILE', '2', '@price', '0.50', 'AS', 'q50',
            'REDUCE', 'QUANTILE', '2', '@price', '0.90', 'AS', 'q90',
            'REDUCE', 'QUANTILE', '2', '@price', '0.95', 'AS', 'q95',
            'REDUCE', 'AVG', '1', '@price',
            'REDUCE', 'COUNT', '0',
            'SORTBY', '1', '@count',
            'LIMIT', '0', '10']

        res = self.cmd(*cmd)
        row = to_dict(res[1])
        self.assertEqual('14.99', row['q50'])
        self.assertEqual(106, int(float(row['q90'])))
        self.assertEqual(99, int(float(row['q95'])))
    
    def testStdDev(self):
        cmd = ['FT.AGGREGATE', 'idx', '-@brand:aadsfgds',
            'GROUPBY', '1', '@brand',
            'REDUCE', 'STDDEV', '1', '@price',
            'REDUCE', 'AVG', '1', '@price', 'AS', 'avgPrice',
            'REDUCE', 'QUANTILE', '2', '@price', '0.50', 'AS', 'q50Price',
            'REDUCE', 'COUNT', '0',
            'SORTBY', '1', '@count',
            'LIMIT', '0', '10']
        res = self.cmd(*cmd)
        row = to_dict(res[1])

        self.assertEqual(14, int(float(row['q50Price'])))
        self.assertEqual(53, int(float(row['stddev(price)'])))
        self.assertEqual(29, int(float(row['avgPrice'])))

    def testParseTime(self):
        cmd = ['FT.AGGREGATE', 'idx', '-@nonexist:nonexist',
            'GROUPBY', '1', '@brand',
            'REDUCE', 'COUNT', '0',
            'APPLY', 'parse_time("%FT%TZ", time(1517417144))', 'as', 'now',
            'LIMIT', '0', '10']
        res = self.cmd(*cmd)
        print res
    
    def testStringFormat(self):
        cmd = ['FT.AGGREGATE', 'idx', '@brand:sony',
            'GROUPBY', '1', '@title',
            'REDUCE', 'COUNT', '0',
            'APPLY', 'FORMAT("{{%s}} ==BY== {{%s}} (Hi: %s)", @title, @brand, "Mark")', 'as', 'titleBrand',
            'LIMIT', '0', '10']
        res = self.cmd(*cmd)
        pprint.pprint(res)

if __name__ == '__main__':

    unittest.main()
