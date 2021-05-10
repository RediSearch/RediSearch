import bz2
import json
import itertools
import os
from RLTest import Env
import pprint
from includes import *
from common import getConnectionByEnv, waitForIndex, sortedResults, toSortedFlatList

def to_dict(res):
    d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
    return d


GAMES_JSON = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'games.json.bz2')


def add_values(env, number_of_iterations=1):
    env.execute_command('FT.CREATE', 'games', 'ON', 'HASH',
                        'SCHEMA', 'title', 'TEXT', 'SORTABLE',
                        'brand', 'TEXT', 'NOSTEM', 'SORTABLE',
                        'description', 'TEXT', 'price', 'NUMERIC',
                        'categories', 'TAG')

    for i in range(number_of_iterations):
        fp = bz2.BZ2File(GAMES_JSON, 'r')
        for line in fp:
            obj = json.loads(line)
            id = obj['asin'] + (str(i) if i > 0 else '')
            del obj['asin']
            obj['price'] = obj.get('price') or 0
            obj['categories'] = ','.join(obj['categories'])
            cmd = ['FT.ADD', 'games', id, 1, 'FIELDS', ] + \
                [str(x) if x is not None else '' for x in itertools.chain(
                    *obj.items())]
            env.execute_command(*cmd)
        fp.close()


class TestAggregate():
    def __init__(self):
        self.env = Env()
        add_values(self.env)

    def testGroupBy(self):
        cmd = ['ft.aggregate', 'games', '*',
               'GROUPBY', '1', '@brand',
               'REDUCE', 'count', '0', 'AS', 'count',
               'SORTBY', 2, '@count', 'desc',
               'LIMIT', '0', '5'
               ]

        res = self.env.cmd(*cmd)
        self.env.assertIsNotNone(res)
        self.env.assertEqual([292L, ['brand', '', 'count', '1518'], ['brand', 'mad catz', 'count', '43'],
                                    ['brand', 'generic', 'count', '40'], ['brand', 'steelseries', 'count', '37'],
                                    ['brand', 'logitech', 'count', '35']], res)

    def testMinMax(self):
        cmd = ['ft.aggregate', 'games', 'sony',
               'GROUPBY', '1', '@brand',
               'REDUCE', 'count', '0',
               'REDUCE', 'min', '1', '@price', 'as', 'minPrice',
               'SORTBY', '2', '@minPrice', 'DESC']
        res = self.env.cmd(*cmd)
        self.env.assertIsNotNone(res)
        row = to_dict(res[1])
        self.env.assertEqual(88, int(float(row['minPrice'])))

        cmd = ['ft.aggregate', 'games', 'sony',
               'GROUPBY', '1', '@brand',
               'REDUCE', 'count', '0',
               'REDUCE', 'max', '1', '@price', 'as', 'maxPrice',
               'SORTBY', '2', '@maxPrice', 'DESC']
        res = self.env.cmd(*cmd)
        row = to_dict(res[1])
        self.env.assertEqual(695, int(float(row['maxPrice'])))

    def testAvg(self):
        cmd = ['ft.aggregate', 'games', 'sony',
               'GROUPBY', '1', '@brand',
               'REDUCE', 'avg', '1', '@price', 'AS', 'avg_price',
               'REDUCE', 'count', '0',
               'SORTBY', '2', '@avg_price', 'DESC']
        res = self.env.cmd(*cmd)
        self.env.assertIsNotNone(res)
        self.env.assertEqual(26, res[0])
        # Ensure the formatting actually exists

        first_row = to_dict(res[1])
        self.env.assertEqual(109, int(float(first_row['avg_price'])))

        for row in res[1:]:
            row = to_dict(row)
            self.env.assertIn('avg_price', row)

        # Test aliasing
        cmd = ['FT.AGGREGATE', 'games', 'sony', 'GROUPBY', '1', '@brand',
               'REDUCE', 'avg', '1', '@price', 'AS', 'avgPrice']
        res = self.env.cmd(*cmd)
        first_row = to_dict(res[1])
        self.env.assertEqual(17, int(float(first_row['avgPrice'])))

    def testCountDistinct(self):
        cmd = ['FT.AGGREGATE', 'games', '*',
               'GROUPBY', '1', '@brand',
               'REDUCE', 'COUNT_DISTINCT', '1', '@title', 'AS', 'count_distinct(title)',
               'REDUCE', 'COUNT', '0'
               ]
        res = self.env.cmd(*cmd)[1:]
        # print res
        row = to_dict(res[0])
        self.env.assertEqual(1484, int(row['count_distinct(title)']))

        cmd = ['FT.AGGREGATE', 'games', '*',
               'GROUPBY', '1', '@brand',
               'REDUCE', 'COUNT_DISTINCTISH', '1', '@title', 'AS', 'count_distinctish(title)',
               'REDUCE', 'COUNT', '0'
               ]
        res = self.env.cmd(*cmd)[1:]
        # print res
        row = to_dict(res[0])
        self.env.assertEqual(1461, int(row['count_distinctish(title)']))

    def testQuantile(self):
        cmd = ['FT.AGGREGATE', 'games', '*',
               'GROUPBY', '1', '@brand',
               'REDUCE', 'QUANTILE', '2', '@price', '0.50', 'AS', 'q50',
               'REDUCE', 'QUANTILE', '2', '@price', '0.90', 'AS', 'q90',
               'REDUCE', 'QUANTILE', '2', '@price', '0.95', 'AS', 'q95',
               'REDUCE', 'AVG', '1', '@price',
               'REDUCE', 'COUNT', '0', 'AS', 'rowcount',
               'SORTBY', '2', '@rowcount', 'DESC', 'MAX', '1']

        res = self.env.cmd(*cmd)
        row = to_dict(res[1])
        # TODO: Better samples
        self.env.assertAlmostEqual(14.99, float(row['q50']), delta=3)
        self.env.assertAlmostEqual(70, float(row['q90']), delta=50)
        self.env.assertAlmostEqual(110, (float(row['q95'])), delta=50)

    def testStdDev(self):
        cmd = ['FT.AGGREGATE', 'games', '*',
               'GROUPBY', '1', '@brand',
               'REDUCE', 'STDDEV', '1', '@price', 'AS', 'stddev(price)',
               'REDUCE', 'AVG', '1', '@price', 'AS', 'avgPrice',
               'REDUCE', 'QUANTILE', '2', '@price', '0.50', 'AS', 'q50Price',
               'REDUCE', 'COUNT', '0', 'AS', 'rowcount',
               'SORTBY', '2', '@rowcount', 'DESC',
               'LIMIT', '0', '10']
        res = self.env.cmd(*cmd)
        row = to_dict(res[1])

        self.env.assertTrue(10 <= int(
            float(row['q50Price'])) <= 20)
        self.env.assertAlmostEqual(53, int(float(row['stddev(price)'])), delta=50)
        self.env.assertEqual(29, int(float(row['avgPrice'])))

    def testParseTime(self):
        cmd = ['FT.AGGREGATE', 'games', '*',
               'GROUPBY', '1', '@brand',
               'REDUCE', 'COUNT', '0', 'AS', 'count',
               'APPLY', 'timefmt(1517417144)', 'AS', 'dt',
               'APPLY', 'parsetime(@dt, "%FT%TZ")', 'as', 'parsed_dt',
               'LIMIT', '0', '1']
        res = self.env.cmd(*cmd)

        self.env.assertEqual(['brand', '', 'count', '1518', 'dt',
                              '2018-01-31T16:45:44Z', 'parsed_dt', '1517417144'], res[1])

    def testRandomSample(self):
        cmd = ['FT.AGGREGATE', 'games', '*', 'GROUPBY', '1', '@brand',
               'REDUCE', 'COUNT', '0', 'AS', 'num',
               'REDUCE', 'RANDOM_SAMPLE', '2', '@price', '10',
               'SORTBY', '2', '@num', 'DESC', 'MAX', '10']
        for row in self.env.cmd(*cmd)[1:]:
            self.env.assertIsInstance(row[5], list)
            self.env.assertGreater(len(row[5]), 0)
            self.env.assertGreaterEqual(row[3], len(row[5]))

            self.env.assertLessEqual(len(row[5]), 10)

    def testTimeFunctions(self):
        cmd = ['FT.AGGREGATE', 'games', '*',

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
        res = self.env.cmd(*cmd)
        self.env.assertListEqual([1L, ['dt', '1517417144', 'timefmt', '2018-01-31T16:45:44Z', 'day', '1517356800', 'hour', '1517414400',
                                       'minute', '1517417100', 'month', '1514764800', 'dayofweek', '3', 'dayofmonth', '31', 'dayofyear', '30', 'year', '2018']], res)

    def testStringFormat(self):
        cmd = ['FT.AGGREGATE', 'games', '@brand:sony',
               'GROUPBY', '2', '@title', '@brand',
               'REDUCE', 'COUNT', '0',
               'REDUCE', 'MAX', '1', '@price', 'AS', 'price',
               'APPLY', 'format("%s|%s|%s|%s", @title, @brand, "Mark", @price)', 'as', 'titleBrand',
               'LIMIT', '0', '10']
        res = self.env.cmd(*cmd)
        for row in res[1:]:
            row = to_dict(row)
            expected = '%s|%s|%s|%g' % (
                row['title'], row['brand'], 'Mark', float(row['price']))
            self.env.assertEqual(expected, row['titleBrand'])

    def testSum(self):
        cmd = ['ft.aggregate', 'games', '*',
               'GROUPBY', '1', '@brand',
               'REDUCE', 'count', '0', 'AS', 'count',
               'REDUCE', 'sum', 1, '@price', 'AS', 'sum(price)',
               'SORTBY', 2, '@sum(price)', 'desc',
               'LIMIT', '0', '5'
               ]
        res = self.env.cmd(*cmd)
        self.env.assertEqual([292L, ['brand', '', 'count', '1518', 'sum(price)', '44780.69'],
                             ['brand', 'mad catz', 'count',
                                 '43', 'sum(price)', '3973.48'],
                             ['brand', 'razer', 'count', '26',
                                 'sum(price)', '2558.58'],
                             ['brand', 'logitech', 'count',
                                 '35', 'sum(price)', '2329.21'],
                             ['brand', 'steelseries', 'count', '37', 'sum(price)', '1851.12']], res)

    def testFilter(self):
        cmd = ['ft.aggregate', 'games', '*',
               'GROUPBY', '1', '@brand',
               'REDUCE', 'count', '0', 'AS', 'count',
               'FILTER', '@count > 5'
               ]

        res = self.env.cmd(*cmd)
        for row in res[1:]:
            row = to_dict(row)
            self.env.assertGreater(int(row['count']), 5)

        cmd = ['ft.aggregate', 'games', '*',
               'GROUPBY', '1', '@brand',
               'REDUCE', 'count', '0', 'AS', 'count',
               'FILTER', '@count < 5',
               'FILTER', '@count > 2 && @brand != ""'
               ]

        res = self.env.cmd(*cmd)
        for row in res[1:]:
            row = to_dict(row)
            self.env.assertLess(int(row['count']), 5)
            self.env.assertGreater(int(row['count']), 2)

    def testToList(self):
        cmd = ['ft.aggregate', 'games', '*',
               'GROUPBY', '1', '@brand',
               'REDUCE', 'count_distinct', '1', '@price', 'as', 'count',
               'REDUCE', 'tolist', 1, '@price', 'as', 'prices',
               'SORTBY', 2, '@count', 'desc',
               'LIMIT', '0', '5'
               ]
        res = self.env.cmd(*cmd)

        for row in res[1:]:
            row = to_dict(row)
            self.env.assertEqual(int(row['count']), len(row['prices']))

    def testSortBy(self):
        res = self.env.cmd('ft.aggregate', 'games', '*', 'GROUPBY', '1', '@brand',
                           'REDUCE', 'sum', 1, '@price', 'as', 'price',
                           'SORTBY', 2, '@price', 'desc',
                           'LIMIT', '0', '2')

        self.env.assertListEqual([292L, ['brand', '', 'price', '44780.69'], [
                                 'brand', 'mad catz', 'price', '3973.48']], res)

        res = self.env.cmd('ft.aggregate', 'games', '*', 'GROUPBY', '1', '@brand',
                           'REDUCE', 'sum', 1, '@price', 'as', 'price',
                           'SORTBY', 2, '@price', 'asc',
                           'LIMIT', '0', '2')

        self.env.assertListEqual([292L, ['brand', 'myiico', 'price', '0.23'], [
                                 'brand', 'crystal dynamics', 'price', '0.25']], res)

        # Test MAX with limit higher than it
        res = self.env.cmd('ft.aggregate', 'games', '*', 'GROUPBY', '1', '@brand',
                           'REDUCE', 'sum', 1, '@price', 'as', 'price',
                           'SORTBY', 2, '@price', 'asc', 'MAX', 2)

        self.env.assertListEqual([292L, ['brand', 'myiico', 'price', '0.23'], [
                                 'brand', 'crystal dynamics', 'price', '0.25']], res)

        # Test Sorting by multiple properties
        res = self.env.cmd('ft.aggregate', 'games', '*', 'GROUPBY', '1', '@brand',
                           'REDUCE', 'sum', 1, '@price', 'as', 'price',
                           'APPLY', '(@price % 10)', 'AS', 'price',
                           'SORTBY', 4, '@price', 'asc', '@brand', 'desc', 'MAX', 10,
                           )
        self.env.assertListEqual([292L, ['brand', 'zps', 'price', '0'], ['brand', 'zalman', 'price', '0'], ['brand', 'yoozoo', 'price', '0'], ['brand', 'white label', 'price', '0'], ['brand', 'stinky', 'price', '0'], [
                                 'brand', 'polaroid', 'price', '0'], ['brand', 'plantronics', 'price', '0'], ['brand', 'ozone', 'price', '0'], ['brand', 'oooo', 'price', '0'], ['brand', 'neon', 'price', '0']], res)

    def testExpressions(self):
        pass

    def testNoGroup(self):
        res = self.env.cmd('ft.aggregate', 'games', '*', 'LOAD', '2', '@brand', '@price',
                           'APPLY', 'floor(sqrt(@price)) % 10', 'AS', 'price',
                           'SORTBY', 4, '@price', 'desc', '@brand', 'desc', 'MAX', 5,
                           )
        exp = [2265L,
 ['brand', 'Xbox', 'price', '9'],
 ['brand', 'turtle beach', 'price', '9'],
 ['brand', 'trust', 'price', '9'],
 ['brand', 'steelseries', 'price', '9'],
 ['brand', 'speedlink', 'price', '9']]
        # exp = [2265L, ['brand', 'Xbox', 'price', '9'], ['brand', 'Turtle Beach', 'price', '9'], [
                            #  'brand', 'Trust', 'price', '9'], ['brand', 'SteelSeries', 'price', '9'], ['brand', 'Speedlink', 'price', '9']]
        self.env.assertListEqual(exp[1], res[1])

    def testLoad(self):
        res = self.env.cmd('ft.aggregate', 'games', '*',
                           'LOAD', '3', '@brand', '@price', '@nonexist',
                           'SORTBY', 2, '@price', 'DESC',
                           'MAX', 2)
        exp = [3L, ['brand', '', 'price', '759.12'], ['brand', 'Sony', 'price', '695.8']]
        self.env.assertEqual(exp[1], res[1])
        self.env.assertEqual(exp[2], res[2])

    def testLoadWithDocId(self):
        res = self.env.cmd('ft.aggregate', 'games', '*',
                           'LOAD', '3', '@brand', '@price', '@__key',
                           'SORTBY', 2, '@price', 'DESC',
                           'MAX', 4)
        exp = [3L, ['brand', '', 'price', '759.12', '__key', 'B00006JJIC'], 
                   ['brand', 'Sony', 'price', '695.8', '__key', 'B000F6W1AG']]
        self.env.assertEqual(exp[1], res[1])
        self.env.assertEqual(exp[2], res[2])

        res = self.env.cmd('ft.aggregate', 'games', '*',
                           'LOAD', '3', '@brand', '@price', '@__key',
                           'FILTER', '@__key == "B000F6W1AG"')
        self.env.assertEqual(res[1], ['brand', 'Sony', 'price', '695.8', '__key', 'B000F6W1AG'])

    def testLoadImplicit(self):
        # same as previous
        res = self.env.cmd('ft.aggregate', 'games', '*',
                           'LOAD', '1', '@brand',
                           'SORTBY', 2, '@price', 'DESC')
        exp = [3L, ['brand', '', 'price', '759.12'], ['brand', 'Sony', 'price', '695.8']]
        self.env.assertEqual(exp[1], res[1])

    def testSplit(self):
        res = self.env.cmd('ft.aggregate', 'games', '*', 'APPLY', 'split("hello world,  foo,,,bar,", ",", " ")', 'AS', 'strs',
                           'APPLY', 'split("hello world,  foo,,,bar,", " ", ",")', 'AS', 'strs2',
                           'APPLY', 'split("hello world,  foo,,,bar,", "", "")', 'AS', 'strs3',
                           'APPLY', 'split("hello world,  foo,,,bar,")', 'AS', 'strs4',
                           'APPLY', 'split("hello world,  foo,,,bar,",",")', 'AS', 'strs5',
                           'APPLY', 'split("")', 'AS', 'empty',
                           'LIMIT', '0', '1'
                           )
        # print "Got {} results".format(len(res))
        # return
        # pprint.pprint(res)
        self.env.assertListEqual([1L, ['strs', ['hello world', 'foo', 'bar'],
                                       'strs2', ['hello', 'world', 'foo,,,bar'],
                                       'strs3', ['hello world,  foo,,,bar,'],
                                       'strs4', ['hello world', 'foo', 'bar'],
                                       'strs5', ['hello world', 'foo', 'bar'],
                                       'empty', []]], res)

    def testFirstValue(self):
        res = self.env.cmd('ft.aggregate', 'games', '@brand:(sony|matias|beyerdynamic|(mad catz))',
                           'GROUPBY', 1, '@brand',
                           'REDUCE', 'FIRST_VALUE', 4, '@title', 'BY', '@price', 'DESC', 'AS', 'top_item',
                           'REDUCE', 'FIRST_VALUE', 4, '@price', 'BY', '@price', 'DESC', 'AS', 'top_price',
                           'REDUCE', 'FIRST_VALUE', 4, '@title', 'BY', '@price', 'ASC', 'AS', 'bottom_item',
                           'REDUCE', 'FIRST_VALUE', 4, '@price', 'BY', '@price', 'ASC', 'AS', 'bottom_price',
                           'SORTBY', 2, '@top_price', 'DESC', 'MAX', 5
                           )
        expected = [4L, ['brand', 'sony', 'top_item', 'sony psp slim &amp; lite 2000 console', 'top_price', '695.8', 'bottom_item', 'sony dlchd20p high speed hdmi cable for playstation 3', 'bottom_price', '5.88'],
                                 ['brand', 'matias', 'top_item', 'matias halfkeyboard usb', 'top_price',
                                     '559.99', 'bottom_item', 'matias halfkeyboard usb', 'bottom_price', '559.99'],
                                 ['brand', 'beyerdynamic', 'top_item', 'beyerdynamic mmx300 pc gaming premium digital headset with microphone', 'top_price', '359.74',
                                     'bottom_item', 'beyerdynamic headzone pc gaming digital surround sound system with mmx300 digital headset with microphone', 'bottom_price', '0'],
                                 ['brand', 'mad catz', 'top_item', 'mad catz s.t.r.i.k.e.7 gaming keyboard', 'top_price', '295.95', 'bottom_item', 'madcatz mov4545 xbox replacement breakaway cable', 'bottom_price', '3.49']]
        
        # hack :(
        def mklower(result):
            for arr in result[1:]:
                for x in range(len(arr)):
                    arr[x] = arr[x].lower()
        mklower(expected)
        mklower(res)
        self.env.assertListEqual(expected, res)

    def testLoadAfterGroupBy(self):
        with self.env.assertResponseError():
            self.env.cmd('ft.aggregate', 'games', '*',
                         'GROUPBY', 1, '@brand',
                         'LOAD', 1, '@brand')
    
    def testReducerGeneratedAliasing(self):
        rv = self.env.cmd('ft.aggregate', 'games', '*',
                          'GROUPBY', 1, '@brand',
                          'REDUCE', 'MIN', 1, '@price',
                          'LIMIT', 0, 1)
        self.env.assertEqual([292L, ['brand', '', '__generated_aliasminprice', '0']], rv)

        rv = self.env.cmd('ft.aggregate', 'games', '@brand:(sony|matias|beyerdynamic|(mad catz))',
                          'GROUPBY', 1, '@brand',
                          'REDUCE', 'FIRST_VALUE', 4, '@title', 'BY', '@price', 'DESC',
                          'SORTBY', 2, '@brand', 'ASC')
        self.env.assertEqual('__generated_aliasfirst_valuetitle,by,price,desc', rv[1][2])

    def testIssue1125(self):
        self.env.skipOnCluster()
        # SEARCH should fail
        self.env.expect('ft.search', 'games', '*', 'limit', 0, 2000000).error()     \
                .contains('LIMIT exceeds maximum of 1000000')
        # SEARCH should succeed
        self.env.expect('ft.config', 'set', 'MAXSEARCHRESULTS', -1).ok()
        rv = self.env.cmd('ft.search', 'games', '*',
                          'LIMIT', 0, 12345678)
        self.env.assertEqual(4531, len(rv))
        # AGGREGATE should succeed
        rv = self.env.cmd('ft.aggregate', 'games', '*',
                          'LIMIT', 0, 12345678)
        self.env.assertEqual(2266, len(rv))
        # AGGREGATE should fail
        self.env.expect('ft.config', 'set', 'MAXAGGREGATERESULTS', 1000000).ok()
        self.env.expect('ft.aggregate', 'games', '*', 'limit', 0, 2000000).error()     \
                .contains('LIMIT exceeds maximum of 1000000')

        # force global limit on aggregate
        num = 10
        self.env.expect('ft.config', 'set', 'MAXAGGREGATERESULTS', num).ok()
        rv = self.env.cmd('ft.aggregate', 'games', '*')
        self.env.assertEqual(num + 1, len(rv))

    def testMultiSortBy(self):
        self.env.expect('ft.aggregate', 'games', '*',
                           'LOAD', '2', '@brand', '@price',
                           'SORTBY', 2, '@brand', 'DESC',
                           'SORTBY', 2, '@price', 'DESC').error()\
                            .contains('Multiple SORTBY steps are not allowed. Sort multiple fields in a single step')

    def testCountError(self):
        # With 0 values
        res = self.env.cmd('ft.aggregate', 'games', '*',
                           'GROUPBY', '2', '@brand', '@price',
                           'REDUCE', 'COUNT', 0)
        self.env.assertEqual(len(res), 1245)

        # With count 1 and 1 value
        res = self.env.expect('ft.aggregate', 'games', '*',
                           'GROUPBY', '2', '@brand', '@price',
                           'REDUCE', 'COUNT', 1, '@brand').error()      \
                            .contains('Count accepts 0 values only')

        # With count 1 and 0 values
        res = self.env.expect('ft.aggregate', 'games', '*',
                           'GROUPBY', '2', '@brand', '@price',
                           'REDUCE', 'COUNT', 1).error()        \
                            .contains('Bad arguments for COUNT: Expected an argument, but none provided')

    # def testLoadAfterSortBy(self):
    #     with self.env.assertResponseError():
    #         self.env.cmd('ft.aggregate', 'games', '*',
    #                      'SORTBY', 1, '@brand',
    #                      'LOAD', 1, '@brand')

    # def testLoadAfterApply(self):
    #     with self.env.assertResponseError():
    #         self.env.cmd('ft.aggregate', 'games', '*',
    #                      'APPLY', 'timefmt(1517417144)', 'AS', 'dt',
    #                      'LOAD', 1, '@brand')

    # def testLoadAfterFilter(self):
    #     with self.env.assertResponseError():
    #         self.env.cmd('ft.aggregate', 'games', '*',
    #                      'FILTER', '@count > 5',
    #                      'LOAD', 1, '@brand')

    # def testLoadAfterLimit(self):
    #     with self.env.assertResponseError():
    #         self.env.cmd('ft.aggregate', 'games', '*',
    #                      'LIMIT', '0', '5',
    #                      'LOAD', 1, '@brand')


class TestAggregateSecondUseCases():
    def __init__(self):
        self.env = Env()
        add_values(self.env, 2)

    def testSimpleAggregate(self):
        res = self.env.cmd('ft.aggregate', 'games', '*')
        self.env.assertIsNotNone(res)
        self.env.assertEqual(len(res), 4531)

    def testSimpleAggregateWithCursor(self):
        res = self.env.cmd('ft.aggregate', 'games', '*', 'WITHCURSOR', 'COUNT', 1000)
        self.env.assertTrue(res[1] != 0)

def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    from itertools import izip_longest
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)

def testAggregateGroupByOnEmptyField(env):
    env.cmd('ft.create', 'idx', 'ON', 'HASH',
            'SCHEMA', 'f', 'TEXT', 'SORTABLE', 'test', 'TEXT', 'SORTABLE')
    env.cmd('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'f', 'field', 'test', 'test1,test2,test3')
    env.cmd('ft.add', 'idx', 'doc2', '1.0', 'FIELDS', 'f', 'field', 'test', '')
    res = env.cmd('ft.aggregate', 'idx', 'field', 'APPLY', 'split(@test)', 'as', 'check',
                  'GROUPBY', '1', '@check', 'REDUCE', 'COUNT', '0', 'as', 'count')

    expected = [4L, ['check', 'test3', 'count', '1'],
                    ['check', None, 'count', '1'], ['check', 'test1', 'count', '1'], ['check', 'test2', 'count', '1']]
    for var in expected:
        env.assertIn(var, res)

def testGroupbyNoReduce(env):
    env.cmd('ft.create', 'idx', 'ON', 'HASH',
            'SCHEMA', 'primaryName', 'TEXT', 'SORTABLE',
            'birthYear', 'NUMERIC', 'SORTABLE')
    
    for x in range(10):
        env.cmd('ft.add', 'idx', 'doc{}'.format(x), 1, 'fields',
            'primaryName', 'sarah number{}'.format(x))

    rv = env.cmd('ft.aggregate', 'idx', 'sarah', 'groupby', 1, '@primaryName')
    env.assertEqual(11, len(rv))
    for row in rv[1:]:
        env.assertEqual('primaryName', row[0])
        env.assertTrue('sarah' in row[1])

def testStartsWith(env):
    conn = getConnectionByEnv(env)
    env.execute_command('ft.create', 'idx', 'SCHEMA', 't', 'TEXT', 'SORTABLE')    
    conn.execute_command('hset', 'doc1', 't', 'aa')
    conn.execute_command('hset', 'doc2', 't', 'aaa')
    conn.execute_command('hset', 'doc3', 't', 'ab')

    res = env.cmd('ft.aggregate', 'idx', '*', 'load', 1, 't', 'apply', 'startswith(@t, "aa")', 'as', 'prefix')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList([1L, ['t', 'aa', 'prefix', '1'], \
                                                                 ['t', 'aaa', 'prefix', '1'], \
                                                                 ['t', 'ab', 'prefix', '0']]))

def testContains(env):
    conn = getConnectionByEnv(env)
    env.execute_command('ft.create', 'idx', 'SCHEMA', 't', 'TEXT', 'SORTABLE')
    conn.execute_command('hset', 'doc1', 't', 'aa')
    conn.execute_command('hset', 'doc2', 't', 'bba')
    conn.execute_command('hset', 'doc3', 't', 'aba')
    conn.execute_command('hset', 'doc4', 't', 'abb')
    conn.execute_command('hset', 'doc5', 't', 'abba')
    conn.execute_command('hset', 'doc6', 't', 'abbabb')

    res = env.cmd('ft.aggregate', 'idx', '*', 'load', 1, 't', 'apply', 'contains(@t, "bb")', 'as', 'substring')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList([1L, ['t', 'aa', 'substring', '0'], \
                                                             ['t', 'bba', 'substring', '1'], \
                                                             ['t', 'aba', 'substring', '0'], \
                                                             ['t', 'abb', 'substring', '1'], \
                                                             ['t', 'abba', 'substring', '1'], \
                                                             ['t', 'abbabb', 'substring', '2']]))

def testLimitIssue(env):
    #ticket 66895
    conn = getConnectionByEnv(env)
    conn.execute_command('ft.create', 'idx', 'SCHEMA', 'PrimaryKey', 'TEXT', 'SORTABLE',
                                           'CreatedDateTimeUTC', 'NUMERIC', 'SORTABLE')
    conn.execute_command('HSET', 'doc1', 'PrimaryKey', '9::362330', 'CreatedDateTimeUTC', '637387878524969984')
    conn.execute_command('HSET', 'doc2', 'PrimaryKey', '9::362329', 'CreatedDateTimeUTC', '637387875859270016')
    conn.execute_command('HSET', 'doc3', 'PrimaryKey', '9::362326', 'CreatedDateTimeUTC', '637386176589869952')
    conn.execute_command('HSET', 'doc4', 'PrimaryKey', '9::362311', 'CreatedDateTimeUTC', '637383865971600000')
    conn.execute_command('HSET', 'doc5', 'PrimaryKey', '9::362310', 'CreatedDateTimeUTC', '637383864050669952')
    conn.execute_command('HSET', 'doc6', 'PrimaryKey', '9::362309', 'CreatedDateTimeUTC', '637242254008029952')
    conn.execute_command('HSET', 'doc7', 'PrimaryKey', '9::362308', 'CreatedDateTimeUTC', '637242253551670016')
    conn.execute_command('HSET', 'doc8', 'PrimaryKey', '9::362306', 'CreatedDateTimeUTC', '637166988081200000')

    _res = [8L,
          ['PrimaryKey', '9::362330', 'CreatedDateTimeUTC', '637387878524969984'],
          ['PrimaryKey', '9::362329', 'CreatedDateTimeUTC', '637387875859270016'],
          ['PrimaryKey', '9::362326', 'CreatedDateTimeUTC', '637386176589869952'],
          ['PrimaryKey', '9::362311', 'CreatedDateTimeUTC', '637383865971600000'],
          ['PrimaryKey', '9::362310', 'CreatedDateTimeUTC', '637383864050669952'],
          ['PrimaryKey', '9::362309', 'CreatedDateTimeUTC', '637242254008029952'],
          ['PrimaryKey', '9::362308', 'CreatedDateTimeUTC', '637242253551670016'],
          ['PrimaryKey', '9::362306', 'CreatedDateTimeUTC', '637166988081200000']]

    actual_res = conn.execute_command('FT.AGGREGATE', 'idx', '*',
                'APPLY', '@PrimaryKey', 'AS', 'PrimaryKey',
                'SORTBY', '2', '@CreatedDateTimeUTC', 'DESC', 'LIMIT', '0', '8')
    env.assertEqual(actual_res, _res)

    res = [_res[0]] + _res[1:3]
    actual_res = conn.execute_command('FT.AGGREGATE', 'idx', '*',
                'APPLY', '@PrimaryKey', 'AS', 'PrimaryKey',
                'SORTBY', '2', '@CreatedDateTimeUTC', 'DESC', 'LIMIT', '0', '2')
    env.assertEqual(actual_res, res)

    res = [_res[0]] + _res[2:4]
    actual_res = conn.execute_command('FT.AGGREGATE', 'idx', '*',
                'APPLY', '@PrimaryKey', 'AS', 'PrimaryKey',
                'SORTBY', '2', '@CreatedDateTimeUTC', 'DESC', 'LIMIT', '1', '2')
    env.assertEqual(actual_res, res)

    res = [_res[0]] + _res[3:5]
    actual_res = conn.execute_command('FT.AGGREGATE', 'idx', '*',
                'APPLY', '@PrimaryKey', 'AS', 'PrimaryKey',
                'SORTBY', '2', '@CreatedDateTimeUTC', 'DESC', 'LIMIT', '2', '2')
    env.assertEqual(actual_res, res)

def testMaxAggResults():
    env = Env(moduleArgs="MAXAGGREGATERESULTS 100")
    conn = getConnectionByEnv(env)
    conn.execute_command('ft.create', 'idx', 'SCHEMA', 't', 'TEXT')
    env.expect('ft.aggregate', 'idx', '*', 'LIMIT', '0', '10000').error()   \
                .contains('LIMIT exceeds maximum of 100')
