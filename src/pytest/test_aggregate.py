from rmtest import ModuleTestCase
import redis
import bz2
import json
import unittest
import itertools
import pprint
import sys


def to_dict(res):
    d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
    return d


class AggregateTestCase(ModuleTestCase('../redisearch.so', module_args=['SAFEMODE'])):

    # ingested = False

    def ingest(self):
        try:
            self.cmd('FT.CREATE', 'games', 'SCHEMA', 'title', 'TEXT', 'SORTABLE', 'brand', 'TEXT',  'NOSTEM', 'SORTABLE',
                     'description', 'TEXT', 'price', 'NUMERIC', 'SORTABLE', 'categories', 'TAG')
        except:
            return
        client = self.client
        fp = bz2.BZ2File('games.json.bz2', 'r')

        for line in fp:
            obj = json.loads(line)
            id = obj['asin']
            del obj['asin']
            obj['price'] = obj.get('price') or 0
            obj['categories'] = ','.join(obj['categories'])
            cmd = ['FT.ADD', 'games', id, 1, 'FIELDS', ] + \
                [str(x) if x is not None else '' for x in itertools.chain(
                    *obj.items())]
            # print cmd
            self.cmd(*cmd)

    def setUp(self):

        self.ingest()

    def _testGroupBy(self):
        cmd = ['ft.aggregate', 'games', '*',
               'GROUPBY', '1', '@brand',
               'REDUCE', 'count', '0', 'AS', 'count',
               'SORTBY', 2, '@count', 'desc',
               'LIMIT', '0', '5'
               ]

        res = self.cmd(*cmd)
        self.assertIsNotNone(res)
        self.assertEqual([292L, ['brand', '', 'count', '1518'], ['brand', 'mad catz', 'count', '43'], [
                         'brand', 'generic', 'count', '40'], ['brand', 'steelseries', 'count', '37'], ['brand', 'logitech', 'count', '35']], res)

    def _testMinMax(self):
        cmd = ['ft.aggregate', 'games', 'sony',
               'GROUPBY', '1', '@brand',
               'REDUCE', 'count', '0',
               'REDUCE', 'min', '1', '@price', 'as', 'minPrice',
               'SORTBY', '2', '@minPrice', 'DESC']
        res = self.cmd(*cmd)
        self.assertIsNotNone(res)
        row = to_dict(res[1])
        self.assertEqual(88, int(float(row['minPrice'])))

        cmd = ['ft.aggregate', 'games', 'sony',
               'GROUPBY', '1', '@brand',
               'REDUCE', 'count', '0',
               'REDUCE', 'max', '1', '@price', 'as', 'maxPrice',
               'SORTBY', '2', '@maxPrice', 'DESC']
        res = self.cmd(*cmd)
        row = to_dict(res[1])
        self.assertEqual(695, int(float(row['maxPrice'])))

    def _testAvg(self):
        cmd = ['ft.aggregate', 'games', 'sony',
               'GROUPBY', '1', '@brand',
               'REDUCE', 'avg', '1', '@price', 'AS', 'avg_price',
               'REDUCE', 'count', '0',
               'SORTBY', '2', '@avg_price', 'DESC']
        res = self.cmd(*cmd)
        self.assertIsNotNone(res)
        self.assertEqual(26, res[0])
        # Ensure the formatting actually exists

        first_row = to_dict(res[1])
        self.assertEqual(109, int(float(first_row['avg_price'])))

        for row in res[1:]:
            row = to_dict(row)
            self.assertIn('avg_price', row)

        # Test aliasing
        cmd = ['FT.AGGREGATE', 'games', 'sony', 'GROUPBY', '1', '@brand',
               'REDUCE', 'avg', '1', '@price', 'AS', 'avgPrice']
        res = self.cmd(*cmd)
        first_row = to_dict(res[1])
        self.assertEqual(17, int(float(first_row['avgPrice'])))

    def _testCountDistinct(self):
        cmd = ['FT.AGGREGATE', 'games', '*',
               'GROUPBY', '1', '@categories',
               'REDUCE', 'COUNT_DISTINCT', '1', '@title', 'AS', 'count_distinct(title)',
               'REDUCE', 'COUNT', '0'
               ]
        res = self.cmd(*cmd)[1:]
        row = to_dict(res[0])
        self.assertEqual(2207, int(row['count_distinct(title)']))

        cmd = ['FT.AGGREGATE', 'games', '*',
               'GROUPBY', '1', '@categories',
               'REDUCE', 'COUNT_DISTINCTISH', '1', '@title', 'AS', 'count_distinctish(title)',
               'REDUCE', 'COUNT', '0'
               ]
        res = self.cmd(*cmd)[1:]
        row = to_dict(res[0])
        self.assertEqual(2144, int(row['count_distinctish(title)']))

    def _testQuantile(self):
        cmd = ['FT.AGGREGATE', 'games', '*',
               'GROUPBY', '1', '@brand',
               'REDUCE', 'QUANTILE', '2', '@price', '0.50', 'AS', 'q50',
               'REDUCE', 'QUANTILE', '2', '@price', '0.90', 'AS', 'q90',
               'REDUCE', 'QUANTILE', '2', '@price', '0.95', 'AS', 'q95',
               'REDUCE', 'AVG', '1', '@price',
               'REDUCE', 'COUNT', '0',
               'SORTBY', '2', '@count', 'DESC', 'MAX', '10',
               'LIMIT', '0', '10']

        res = self.cmd(*cmd)
        row = to_dict(res[1])
        self.assertEqual('14.99', row['q50'])
        self.assertEqual(106, int(float(row['q90'])))
        self.assertEqual(99, int(float(row['q95'])))

    def _testStdDev(self):
        cmd = ['FT.AGGREGATE', 'games', '*',
               'GROUPBY', '1', '@brand',
               'REDUCE', 'STDDEV', '1', '@price', 'AS', 'stddev(price)',
               'REDUCE', 'AVG', '1', '@price', 'AS', 'avgPrice',
               'REDUCE', 'QUANTILE', '2', '@price', '0.50', 'AS', 'q50Price',
               'REDUCE', 'COUNT', '0',
               'SORTBY', '2', '@count', 'DESC',
               'LIMIT', '0', '10']
        res = self.cmd(*cmd)
        row = to_dict(res[1])

        self.assertEqual(14, int(float(row['q50Price'])))
        self.assertEqual(53, int(float(row['stddev(price)'])))
        self.assertEqual(29, int(float(row['avgPrice'])))

    def _testParseTime(self):
        cmd = ['FT.AGGREGATE', 'games', '*',
               'GROUPBY', '1', '@brand',
               'REDUCE', 'COUNT', '0', 'AS', 'count',
               'APPLY', 'timefmt(1517417144)', 'AS', 'dt',
               'APPLY', 'parse_time("%FT%TZ", @dt)', 'as', 'parsed_dt',
               'LIMIT', '0', '1']
        res = self.cmd(*cmd)

        self.assertEqual(['brand', '', 'count', '1518', 'dt',
                          '2018-01-31T16:45:44Z', 'parsed_dt', '1517417144'], res[1])

    def _testRandomSample(self):
        cmd = ['FT.AGGREGATE',  'games', '*', 'GROUPBY', '1', '@brand',
               'REDUCE', 'COUNT', '0', 'AS', 'num',
               'REDUCE', 'RANDOM_SAMPLE', '2', '@price', '10',
               'SORTBY', '2', '@num', 'DESC', 'MAX', '10']
        for row in self.cmd(*cmd)[1:]:
            self.assertIsInstance(row[5], list)
            self.assertGreater(len(row[5]), 0)
            self.assertGreaterEqual(row[3], len(row[5]))

            self.assertLessEqual(len(row[5]), 10)

    def _testTimeFunctions(self):

        cmd = ['FT.AGGREGATE',  'games', '*',

               'APPLY', '1517417144', 'AS', 'dt',
               'APPLY', 'timefmt(@dt)', 'AS', 'timefmt',
               'APPLY', 'day(@dt)', 'AS', 'day',
               'APPLY', 'hour(@dt)', 'AS', 'hour',
               'APPLY', 'minute(@dt)', 'AS', 'minute',
               'APPLY', 'month(@dt)', 'AS', 'month',
               'APPLY', 'dayofweek(@dt)', 'AS', 'dayofweek',
               'APPLY', 'dayofmonth(@dt)', 'AS', 'dayofmonth',
               'APPLY', 'dayofyear(@dt)', 'AS', 'dayofyear',
               'APPLY', 'year(@dt)', 'AS', 'year',

               'LIMIT', '0', '1']
        res = self.cmd(*cmd)
        self.assertListEqual([1L, ['dt', '1517417144', 'timefmt', '2018-01-31T16:45:44Z', 'day', '1517356800', 'hour', '1517414400',
                                   'minute', '1517417100', 'month', '1514764800', 'dayofweek', '3', 'dayofmonth', '31', 'dayofyear', '30', 'year', '2018']], res)

    def _testStringFormat(self):
        cmd = ['FT.AGGREGATE', 'games', '@brand:sony',
               'GROUPBY', '2', '@title', '@brand',
               'REDUCE', 'COUNT', '0',
               'REDUCE', 'MAX', '1', 'PRICE', 'AS', 'price',
               'APPLY', 'format("%s|%s|%s|%s", @title, @brand, "Mark", @price)', 'as', 'titleBrand',
               'LIMIT', '0', '10']
        res = self.cmd(*cmd)
        for row in res[1:]:
            row = to_dict(row)
            expected = '%s|%s|%s|%g' % (
                row['title'], row['brand'], 'Mark', float(row['price']))
            self.assertEqual(expected, row['titleBrand'])

    def _testSum(self):

        cmd = ['ft.aggregate', 'games', '*',
               'GROUPBY', '1', '@brand',
               'REDUCE', 'count', '0', 'AS', 'count',
               'REDUCE', 'sum', 1, '@price', 'AS', 'sum(price)',
               'SORTBY', 2, '@sum(price)', 'desc',
               'LIMIT', '0', '5'
               ]
        res = self.cmd(*cmd)
        self.assertEqual([292L, ['brand', '', 'count', '1518', 'sum(price)', '44780.69'],
                          ['brand', 'mad catz', 'count',
                              '43', 'sum(price)', '3973.48'],
                          ['brand', 'razer', 'count', '26',
                              'sum(price)', '2558.58'],
                          ['brand', 'logitech', 'count',
                              '35', 'sum(price)', '2329.21'],
                          ['brand', 'steelseries', 'count', '37', 'sum(price)', '1851.12']], res)

    def _testToList(self):

        cmd = ['ft.aggregate', 'games', '*',
               'GROUPBY', '1', '@brand',
               'REDUCE', 'count_distinct', '1', '@price', 'as', 'count',
               'REDUCE', 'tolist', 1, '@price', 'as', 'prices',
               'SORTBY', 2, '@count', 'desc',
               'LIMIT', '0', '5'
               ]
        res = self.cmd(*cmd)

        for row in res[1:]:
            row = to_dict(row)
            self.assertEqual(int(row['count']), len(row['prices']))

    def _testSortBy(self):

        res = self.cmd('ft.aggregate', 'games', '*', 'GROUPBY', '1', '@brand',
                       'REDUCE', 'sum', 1, '@price', 'as', 'price',
                       'SORTBY', 2, '@price', 'desc',
                       'LIMIT', '0', '2')
        self.assertListEqual([292L, ['brand', '', 'price', '44780.69'], [
                             'brand', 'mad catz', 'price', '3973.48']], res)

        res = self.cmd('ft.aggregate', 'games', '*', 'GROUPBY', '1', '@brand',
                       'REDUCE', 'sum', 1, '@price', 'as', 'price',
                       'SORTBY', 2, '@price', 'asc',
                       'LIMIT', '0', '2')
        self.assertListEqual([292L, ['brand', 'myiico', 'price', '0.23'], [
                             'brand', 'crystal dynamics', 'price', '0.25']], res)

        # Test MAX with limit higher than it
        res = self.cmd('ft.aggregate', 'games', '*', 'GROUPBY', '1', '@brand',
                       'REDUCE', 'sum', 1, '@price', 'as', 'price',
                       'SORTBY', 2, '@price', 'asc', 'MAX', 2,
                       'LIMIT', '0', '10')

        self.assertListEqual([292L, ['brand', 'myiico', 'price', '0.23'], [
                             'brand', 'crystal dynamics', 'price', '0.25']], res)

        # Test Sorting by multiple properties
        res = self.cmd('ft.aggregate', 'games', '*', 'GROUPBY', '1', '@brand',
                       'REDUCE', 'sum', 1, '@price', 'as', 'price',
                       'APPLY', '(@price % 10)', 'AS', 'price',
                       'SORTBY', 4, '@price', 'asc', '@brand', 'desc', 'MAX', 10,
                       )
        self.assertListEqual([292L, ['brand', 'zps', 'price', '0'], ['brand', 'zalman', 'price', '0'], ['brand', 'yoozoo', 'price', '0'], ['brand', 'white label', 'price', '0'], ['brand', 'stinky', 'price', '0'], [
                             'brand', 'polaroid', 'price', '0'], ['brand', 'plantronics', 'price', '0'], ['brand', 'ozone', 'price', '0'], ['brand', 'oooo', 'price', '0'], ['brand', 'neon', 'price', '0']], res)

    def _testExpressions(self):
        pass

    def _testNoGroup(self):
        res = self.cmd('ft.aggregate', 'games', '*', 'LOAD', '2', '@brand', '@price',
                       'APPLY', 'floor(sqrt(@price)) % 10', 'AS', 'price',
                       'SORTBY', 4, '@price', 'desc', '@brand', 'desc', 'MAX', 5,
                       )
        self.assertListEqual([2265L, ['brand', 'Xbox', 'price', '9'], ['brand', 'Turtle Beach', 'price', '9'], [
                             'brand', 'Trust', 'price', '9'], ['brand', 'SteelSeries', 'price', '9'], ['brand', 'Speedlink', 'price', '9']],
                             res)

    def _testLoad(self):
        res = self.cmd('ft.aggregate', 'games', '*', 'LOAD', '3', '@brand', '@price', '@nonexist',
                       'LIMIT', 0, 5
                       )
        self.assertListEqual([1L, ['brand', 'Dark Age Miniatures', 'price', '31.23', 'nonexist', None], ['brand', 'Palladium Books', 'price', '9.55', 'nonexist', None], [
                             'brand', '', 'price', '0', 'nonexist', None], ['brand', 'Evil Hat Productions', 'price', '15.48', 'nonexist', None], ['brand', 'Fantasy Flight Games', 'price', '33.96', 'nonexist', None]], res)

    def _testSplit(self):

        res = self.cmd('ft.aggregate', 'games', '*', 'APPLY', 'split("hello world,  foo,,,bar,", ",", " ")', 'AS', 'strs',
                       'APPLY', 'split("hello world,  foo,,,bar,", " ", ",")', 'AS', 'strs2',
                       'APPLY', 'split("hello world,  foo,,,bar,", "", "")', 'AS', 'strs3',
                       'APPLY', 'split("hello world,  foo,,,bar,")', 'AS', 'strs4',
                       'APPLY', 'split("hello world,  foo,,,bar,",",")', 'AS', 'strs5',
                       'APPLY', 'split("")', 'AS', 'empty',
                       'LIMIT', '0', '1'
                       )
        self.assertListEqual([1L, ['strs', ['hello world', 'foo', 'bar'],
                                   'strs2', ['hello', 'world', 'foo,,,bar'],
                                   'strs3', ['hello world,  foo,,,bar,'],
                                   'strs4', ['hello world', 'foo', 'bar'],
                                   'strs5', ['hello world', 'foo', 'bar'],
                                   'empty', []]], res)

    def _testFirstValue(self):
        res = self.cmd('ft.aggregate', 'games', '@brand:(sony|matias|beyerdynamic|(mad catz))',
                       'GROUPBY', 1, '@brand',
                       'REDUCE', 'FIRST_VALUE', 4, '@title', 'BY', '@price', 'DESC', 'AS', 'top_item',
                       'REDUCE', 'FIRST_VALUE', 4, '@price', 'BY', '@price', 'DESC', 'AS', 'top_price',
                       'REDUCE', 'FIRST_VALUE', 4, '@title', 'BY', '@price', 'ASC', 'AS', 'bottom_item',
                       'REDUCE', 'FIRST_VALUE', 4, '@price', 'BY', '@price', 'ASC', 'AS', 'bottom_price',
                       'SORTBY', 2, '@top_price', 'DESC', 'MAX', 5
                       )
        self.assertListEqual([4L, ['brand', 'sony', 'top_item', 'sony psp slim &amp; lite 2000 console', 'top_price', '695.8', 'bottom_item', 'sony dlchd20p high speed hdmi cable for playstation 3', 'bottom_price', '5.88'],
                              ['brand', 'matias', 'top_item', 'matias halfkeyboard usb', 'top_price',
                                  '559.99', 'bottom_item', 'matias halfkeyboard usb', 'bottom_price', '559.99'],
                              ['brand', 'beyerdynamic', 'top_item', 'beyerdynamic mmx300 pc gaming premium digital headset with microphone', 'top_price', '359.74',
                                  'bottom_item', 'beyerdynamic headzone pc gaming digital surround sound system with mmx300 digital headset with microphone', 'bottom_price', '0'],
                              ['brand', 'mad catz', 'top_item', 'mad catz s.t.r.i.k.e.7 gaming keyboard', 'top_price', '295.95', 'bottom_item', 'madcatz mov4545 xbox replacement breakaway cable', 'bottom_price', '3.49']], res)

    def testAll(self):

        for name, f in self.__class__.__dict__.iteritems():
            if name.startswith('_test'):
                f(self)
                sys.stdout.write('Aggregate.{} ... '.format(f.__name__[1:]))
                sys.stdout.flush()
                print('ok')


if __name__ == '__main__':

    unittest.main()
