from common import *

import bz2
import json
import unittest



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
        self.env.assertEqual([292, ['brand', '', 'count', '1518'], ['brand', 'mad catz', 'count', '43'],
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
            self.env.assertGreaterEqual(int(row[3]), len(row[5]))

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
        self.env.assertListEqual([1, ['dt', '1517417144', 'timefmt', '2018-01-31T16:45:44Z', 'day', '1517356800', 'hour', '1517414400',
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
        self.env.assertEqual([292, ['brand', '', 'count', '1518', 'sum(price)', '44780.69'],
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

    def testFilterBeforeLoad(self):
        cmd = ['ft.aggregate', 'games', '*',
               'FILTER', '@price > 500',
               'SORTBY', 2, '@price', 'desc',
               'LOAD', '1', '@categories',
               'LIMIT', '0', '5']

        # FIXME: should yield the same results in standalone cluster modes
        if self.env.isCluster():
            # On cluster, filter can implicitly load any field
            res = self.env.cmd(*cmd)
            self.env.assertEqual([
                ['price', '759.12', 'categories', 'Accessories,Controllers,PC,Steering Wheels,Video Games'],
                ['price', '695.8', 'categories', 'Consoles,Sony PSP,Video Games'],
                ['price', '599.99', 'categories', 'Accessories,PC,Video Games'],
                ['price', '559.99', 'categories', 'Accessories,Gaming Keyboards,Mac,Video Games'],
                ['price', '518.48', 'categories', 'Consoles,Sony PSP,Video Games']
            ], res[1:])
        else:
            # On standalone, filter can only refer to fields that available in the pipeline
            self.env.expect(*cmd).error().contains('Property `price` not loaded nor in pipeline')

        cmd = ['ft.aggregate', 'games', '*',
               'FILTER', 'lower(@brand) == "sony"',
               'LOAD', '1', '@categories',
               'SORTBY', '1', '@price',
               'LIMIT', '0', '5']

        # FIXME: should yield the same results in standalone cluster modes (sony vs Sony)
        res = self.env.cmd(*cmd)
        if self.env.isCluster():
            self.env.assertEqual([
                ['brand', 'Sony', 'categories', 'Accessories,Cables,Cables & Adapters,PlayStation 3,Video Games', 'price', '5.88'],
                ['brand', 'Sony', 'categories', 'Games,PC,Video Games', 'price', '9.19'],
                ['brand', 'Sony', 'categories', 'Accessories,Adapters,Cables & Adapters,Sony PSP,Video Games', 'price', '11.74'],
                ['brand', 'Sony', 'categories', 'Accessories,Headsets,Sony PSP,Video Games', 'price', '12.99'],
                ['brand', 'Sony', 'categories', 'Movies & TV,Sony PSP,TV,Video Games', 'price', '25.99']
            ], res[1:])
        else:
            self.env.assertEqual([
                ['brand', 'sony', 'categories', 'Accessories,Cables,Cables & Adapters,PlayStation 3,Video Games', 'price', '5.88'],
                ['brand', 'sony', 'categories', 'Games,PC,Video Games', 'price', '9.19'],
                ['brand', 'sony', 'categories', 'Accessories,Adapters,Cables & Adapters,Sony PSP,Video Games', 'price', '11.74'],
                ['brand', 'sony', 'categories', 'Accessories,Headsets,Sony PSP,Video Games', 'price', '12.99'],
                ['brand', 'sony', 'categories', 'Movies & TV,Sony PSP,TV,Video Games', 'price', '25.99']
            ], res[1:])

    def testBadFilter(self):
        cmd = ['ft.aggregate', 'games', '*',
               'FILTER', 'bad filter',]
        self.env.expect(*cmd).error().contains('Syntax error at offset')

        cmd = ['ft.aggregate', 'games', '*',
               'FILTER', '@price++',]
        self.env.expect(*cmd).error().contains('Syntax error at offset')

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

        self.env.assertListEqual([292, ['brand', '', 'price', '44780.69'], [
                                 'brand', 'mad catz', 'price', '3973.48']], res)

        res = self.env.cmd('ft.aggregate', 'games', '*', 'GROUPBY', '1', '@brand',
                           'REDUCE', 'sum', 1, '@price', 'as', 'price',
                           'SORTBY', 2, '@price', 'asc',
                           'LIMIT', '0', '2')

        self.env.assertListEqual([292, ['brand', 'myiico', 'price', '0.23'], [
                                 'brand', 'crystal dynamics', 'price', '0.25']], res)

        # Test MAX with limit higher than it
        res = self.env.cmd('ft.aggregate', 'games', '*', 'GROUPBY', '1', '@brand',
                           'REDUCE', 'sum', 1, '@price', 'as', 'price',
                           'SORTBY', 2, '@price', 'asc', 'MAX', 2)

        self.env.assertListEqual([292, ['brand', 'myiico', 'price', '0.23'], [
                                 'brand', 'crystal dynamics', 'price', '0.25']], res)

        # Test Sorting by multiple properties
        res = self.env.cmd('ft.aggregate', 'games', '*', 'GROUPBY', '1', '@brand',
                           'REDUCE', 'sum', 1, '@price', 'as', 'price',
                           'APPLY', '(@price % 10)', 'AS', 'price',
                           'SORTBY', 4, '@price', 'asc', '@brand', 'desc', 'MAX', 10,
                           )
        self.env.assertListEqual([292, ['brand', 'zps', 'price', '0'], ['brand', 'zalman', 'price', '0'], ['brand', 'yoozoo', 'price', '0'], ['brand', 'white label', 'price', '0'], ['brand', 'stinky', 'price', '0'], [
                                 'brand', 'polaroid', 'price', '0'], ['brand', 'plantronics', 'price', '0'], ['brand', 'ozone', 'price', '0'], ['brand', 'oooo', 'price', '0'], ['brand', 'neon', 'price', '0']], res)

        # Test Sorting by multiple properties with missing values
        res = self.env.cmd('ft.aggregate', 'games', '*', 'LOAD', '1', '@nonexist',
                           'SORTBY', 2, '@nonexist', '@price', 'MAX', 10,
                           )
            # We should get a tie for all the results on the nonexist property, and therefore sort by the second property and get the top 10
            # docs with the lowest price
        self.env.assertListEqual([2265, ['price', '0'], ['price', '0'], ['price', '0'], ['price', '0'], ['price', '0'],
                                        ['price', '0'], ['price', '0'], ['price', '0'], ['price', '0'], ['price', '0']], res)

            # make sure we get results sorted by the second property and not by doc ID (which is the default fallback)
        res1 = self.env.cmd('ft.aggregate', 'games', '*', 'LOAD', '2', '@nonexist', '@price',
                            'SORTBY', 2, '@nonexist', '@price', 'MAX', 10,
                            'LOAD', '3', '@__key', 'AS', 'key',
                           )
        res2 = self.env.cmd('ft.aggregate', 'games', '*', 'LOAD', '2', '@nonexist', '@price',
                            'SORTBY', 1, '@nonexist', 'MAX', 10,
                            'LOAD', '3', '@__key', 'AS', 'key',
                           )
        self.env.assertNotEqual(res1, res2)


        # test LOAD with SORTBY
        expected_res = [2265, ['title', 'Logitech MOMO Racing - Wheel and pedals set - 6 button(s) - PC, MAC - black', 'price', '759.12'],
                               ['title', 'Sony PSP Slim &amp; Lite 2000 Console', 'price', '695.8']]
        res = self.env.cmd('ft.aggregate', 'games', '*',
                           'LOAD', 1, '@title',
                           'SORTBY', 2, '@price', 'desc',
                           'LIMIT', '0', '2')
        self.env.assertListEqual(toSortedFlatList(res), toSortedFlatList(expected_res))

        res = self.env.cmd('ft.aggregate', 'games', '*',
                           'SORTBY', 2, '@price', 'desc',
                           'LOAD', 1, '@title',
                           'LIMIT', '0', '2')
        self.env.assertListEqual(toSortedFlatList(res), toSortedFlatList(expected_res))

        # test with non-sortable filed
        expected_res = [2265, ['description', 'world of warcraft:the burning crusade-expansion set'],
                               ['description', 'wired playstation 3 controller, third party product with high quality.']]
        res = self.env.cmd('ft.aggregate', 'games', '*',
                           'SORTBY', 2, '@description', 'desc',
                           'LOAD', 1, '@description',
                           'LIMIT', '0', '2')
        self.env.assertListEqual(toSortedFlatList(res), toSortedFlatList(expected_res))

        res = self.env.cmd('ft.aggregate', 'games', '*',
                           'LOAD', 1, '@description',
                           'SORTBY', 2, '@description', 'desc',
                           'LIMIT', '0', '2')
        self.env.assertListEqual(toSortedFlatList(res), toSortedFlatList(expected_res))

    def testExpressions(self):
        pass

    def testNoGroup(self):
        res = self.env.cmd('ft.aggregate', 'games', '*', 'LOAD', '2', '@brand', '@price',
                           'APPLY', 'floor(sqrt(@price)) % 10', 'AS', 'price',
                           'SORTBY', 4, '@price', 'desc', '@brand', 'desc', 'MAX', 5,
                           )
        exp = [2265,
                ['brand', 'Xbox', 'price', '9'],
                ['brand', 'turtle beach', 'price', '9'],
                ['brand', 'trust', 'price', '9'],
                ['brand', 'steelseries', 'price', '9'],
                ['brand', 'speedlink', 'price', '9']]
        # exp = [2265, ['brand', 'Xbox', 'price', '9'], ['brand', 'Turtle Beach', 'price', '9'], [
                            #  'brand', 'Trust', 'price', '9'], ['brand', 'SteelSeries', 'price', '9'], ['brand', 'Speedlink', 'price', '9']]
        self.env.assertListEqual(exp[1], res[1])

    def testLoad(self):
        res = self.env.cmd('ft.aggregate', 'games', '*',
                           'LOAD', '3', '@brand', '@price', '@nonexist',
                           'SORTBY', 2, '@price', 'DESC',
                           'MAX', 2)
        exp = [3, ['brand', '', 'price', '759.12'], ['brand', 'Sony', 'price', '695.8']]
        self.env.assertEqual(exp[1], res[1])
        self.env.assertEqual(exp[2], res[2])

    def testLoadWithDocId(self):
        res = self.env.cmd('ft.aggregate', 'games', '*',
                           'LOAD', '3', '@brand', '@price', '@__key',
                           'SORTBY', 2, '@price', 'DESC',
                           'MAX', 4)
        exp = [3, ['brand', '', 'price', '759.12', '__key', 'B00006JJIC'],
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
        exp = [3, ['brand', '', 'price', '759.12'], ['brand', 'Sony', 'price', '695.8']]
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
        self.env.assertListEqual([1, ['strs', ['hello world', 'foo', 'bar'],
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
        expected = [4, ['brand', 'sony', 'top_item', 'sony psp slim &amp; lite 2000 console', 'top_price',
                        '695.8', 'bottom_item', 'sony dlchd20p high speed hdmi cable for playstation 3', 'bottom_price', '5.88'],
                       ['brand', 'matias', 'top_item', 'matias halfkeyboard usb', 'top_price',
                        '559.99', 'bottom_item', 'matias halfkeyboard usb', 'bottom_price', '559.99'],
                       ['brand', 'beyerdynamic', 'top_item', 'beyerdynamic mmx300 pc gaming premium digital headset with microphone', 'top_price', '359.74',
                        'bottom_item', 'beyerdynamic headzone pc gaming digital surround sound system with mmx300 digital headset with microphone', 'bottom_price', '0'],
                       ['brand', 'mad catz', 'top_item', 'mad catz s.t.r.i.k.e.7 gaming keyboard', 'top_price', '295.95', 'bottom_item',
                        'madcatz mov4545 xbox replacement breakaway cable', 'bottom_price', '3.49']]

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
        self.env.assertEqual([292, ['brand', '', '__generated_aliasminprice', '0']], rv)

        rv = self.env.cmd('ft.aggregate', 'games', '@brand:(sony|matias|beyerdynamic|(mad catz))',
                          'GROUPBY', 1, '@brand',
                          'REDUCE', 'FIRST_VALUE', 4, '@title', 'BY', '@price', 'DESC',
                          'SORTBY', 2, '@brand', 'ASC')
        self.env.assertEqual('__generated_aliasfirst_valuetitle,by,price,desc', rv[1][2])

    def testIssue1125(self):
        self.env.skipOnCluster()
        if VALGRIND:
            self.env.skip()
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

        self.env.expect('ft.config', 'set', 'MAXAGGREGATERESULTS', -1).ok()
        self.env.expect('ft.config', 'set', 'MAXSEARCHRESULTS', 1000000).ok()

    def testMultiSortByStepsError(self):
        self.env.expect('ft.aggregate', 'games', '*',
                           'LOAD', '2', '@brand', '@price',
                           'SORTBY', 2, '@brand', 'DESC',
                           'SORTBY', 2, '@price', 'DESC').error()\
                            .contains('Multiple SORTBY steps are not allowed. Sort multiple fields in a single step')


    def testLoadWithSortBy(self):
        self.env.expect('ft.aggregate', 'games', '*',
                           'LOAD', '2', '@brand', '@price',
                           'SORTBY', 2, '@brand', 'DESC',
                           'SORTBY', 2, '@price', 'DESC').error()\
                            .contains('Multiple SORTBY steps are not allowed. Sort multiple fields in a single step')

    def testCountError(self):
        # With 0 values
        conn = getConnectionByEnv(self.env)
        res = self.env.execute_command('ft.aggregate', 'games', '*',
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


    def testModulo(self):
        conn = getConnectionByEnv(self.env)

        # With MIN_INF % -1
        res = self.env.execute_command('ft.aggregate', 'games', '*',
                                       'APPLY', '-9223372036854775808 % -1')
        self.env.assertEqual(res[1][1], '0')

        # With Integers
        res = self.env.execute_command('ft.aggregate', 'games', '*',
                                       'APPLY', '439974354 % 5')
        self.env.assertEqual(res[1][1], '4')

        # With Negative
        res = self.env.execute_command('ft.aggregate', 'games', '*',
                                       'APPLY', '-54775808 % -5')
        self.env.assertEqual(res[1][1], '-3')

        res = self.env.execute_command('ft.aggregate', 'games', '*',
                                       'APPLY', '-14275897 % 5')
        self.env.assertEqual(res[1][1], '-2')

        # With Floats
        res = self.env.execute_command('ft.aggregate', 'games', '*',
                                       'APPLY', '547758.3 % 5.1')
        self.env.assertEqual(res[1][1], '3')


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
    from itertools import zip_longest
    args = [iter(iterable)] * n
    return zip_longest(fillvalue=fillvalue, *args)

def testAggregateGroupByOnEmptyField(env):
    env.cmd('ft.create', 'idx', 'ON', 'HASH',
            'SCHEMA', 'f', 'TEXT', 'SORTABLE', 'test', 'TEXT', 'SORTABLE')
    env.cmd('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'f', 'field', 'test', 'test1,test2,test3')
    env.cmd('ft.add', 'idx', 'doc2', '1.0', 'FIELDS', 'f', 'field', 'test', '')
    res = env.cmd('ft.aggregate', 'idx', 'field', 'APPLY', 'split(@test)', 'as', 'check',
                  'GROUPBY', '1', '@check', 'REDUCE', 'COUNT', '0', 'as', 'count')

    expected = [4, ['check', 'test3', 'count', '1'],
                   ['check', None, 'count', '1'],
                   ['check', 'test1', 'count', '1'],
                   ['check', 'test2', 'count', '1']]
    for var in expected:
        env.assertIn(var, res)

def testMultiSortBy(env):
    conn = getConnectionByEnv(env)
    env.execute_command('FT.CREATE', 'sb_idx', 'SCHEMA', 't1', 'TEXT', 't2', 'TEXT')
    conn.execute_command('hset', 'doc1', 't1', 'a', 't2', 'a')
    conn.execute_command('hset', 'doc2', 't1', 'a', 't2', 'b')
    conn.execute_command('hset', 'doc3', 't1', 'a', 't2', 'c')
    conn.execute_command('hset', 'doc4', 't1', 'b', 't2', 'a')
    conn.execute_command('hset', 'doc5', 't1', 'b', 't2', 'b')
    conn.execute_command('hset', 'doc6', 't1', 'b', 't2', 'c')
    conn.execute_command('hset', 'doc7', 't1', 'c', 't2', 'a')
    conn.execute_command('hset', 'doc8', 't1', 'c', 't2', 'b')
    conn.execute_command('hset', 'doc9', 't1', 'c', 't2', 'c')

    # t1 ASC t2 ASC
    res = [9, ['t1', 'a', 't2', 'a'], ['t1', 'a', 't2', 'b'], ['t1', 'a', 't2', 'c'],
               ['t1', 'b', 't2', 'a'], ['t1', 'b', 't2', 'b'], ['t1', 'b', 't2', 'c'],
               ['t1', 'c', 't2', 'a'], ['t1', 'c', 't2', 'b'], ['t1', 'c', 't2', 'c']]
    env.expect('FT.AGGREGATE', 'sb_idx', '*',
                'LOAD', '2', '@t1', '@t2',
                'SORTBY', '4', '@t1', 'ASC', '@t2', 'ASC').equal(res)

    # t1 DESC t2 ASC
    res = [9, ['t1', 'c', 't2', 'a'], ['t1', 'c', 't2', 'b'], ['t1', 'c', 't2', 'c'],
               ['t1', 'b', 't2', 'a'], ['t1', 'b', 't2', 'b'], ['t1', 'b', 't2', 'c'],
               ['t1', 'a', 't2', 'a'], ['t1', 'a', 't2', 'b'], ['t1', 'a', 't2', 'c']]
    env.expect('FT.AGGREGATE', 'sb_idx', '*',
                'LOAD', '2', '@t1', '@t2',
                'SORTBY', '4', '@t1', 'DESC', '@t2', 'ASC').equal(res)

    # t2 ASC t1 ASC
    res = [9, ['t1', 'a', 't2', 'a'], ['t1', 'b', 't2', 'a'], ['t1', 'c', 't2', 'a'],
               ['t1', 'a', 't2', 'b'], ['t1', 'b', 't2', 'b'], ['t1', 'c', 't2', 'b'],
               ['t1', 'a', 't2', 'c'], ['t1', 'b', 't2', 'c'], ['t1', 'c', 't2', 'c']]
    env.expect('FT.AGGREGATE', 'sb_idx', '*',
                'LOAD', '2', '@t1', '@t2',
                'SORTBY', '4', '@t2', 'ASC', '@t1', 'ASC').equal(res)
    # t2 ASC t1 DESC
    env.expect('FT.AGGREGATE', 'sb_idx', '*',
                'LOAD', '2', '@t1', '@t2',
                'SORTBY', '4', '@t2', 'ASC', '@t1', 'ASC').equal(res)

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
    env.assertEqual(toSortedFlatList(res), toSortedFlatList([1, ['t', 'aa', 'prefix', '1'], \
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

    # check count of contains
    res = env.cmd('ft.aggregate', 'idx', '*', 'load', 1, 't', 'apply', 'contains(@t, "bb")', 'as', 'substring')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList([1, ['t', 'aa', 'substring', '0'], \
                                                                ['t', 'bba', 'substring', '1'], \
                                                                ['t', 'aba', 'substring', '0'], \
                                                                ['t', 'abb', 'substring', '1'], \
                                                                ['t', 'abba', 'substring', '1'], \
                                                                ['t', 'abbabb', 'substring', '2']]))

    # check filter by contains
    res = env.cmd('ft.aggregate', 'idx', '*', 'load', 1, 't', 'filter', 'contains(@t, "bb")')
    env.assertEqual(toSortedFlatList(res)[1:], toSortedFlatList([['t', 'bba'], \
                                                                 ['t', 'abb'], \
                                                                 ['t', 'abba'], \
                                                                 ['t', 'abbabb']]))

    # check count of contains with empty string. (returns length of string + 1)
    res = env.cmd('ft.aggregate', 'idx', '*', 'load', 1, 't', 'apply', 'contains(@t, "")', 'as', 'substring')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList([1, ['t', 'aa', 'substring', '3'], \
                                                             ['t', 'bba', 'substring', '4'], \
                                                             ['t', 'aba', 'substring', '4'], \
                                                             ['t', 'abb', 'substring', '4'], \
                                                             ['t', 'abba', 'substring', '5'], \
                                                             ['t', 'abbabb', 'substring', '7']]))

    # check filter by contains with empty string
    res = env.cmd('ft.aggregate', 'idx', '*', 'load', 1, 't', 'filter', 'contains(@t, "")')
    env.assertEqual(toSortedFlatList(res)[1:], toSortedFlatList([['t', 'aa'], \
                                                                 ['t', 'bba'], \
                                                                 ['t', 'aba'], \
                                                                 ['t', 'abb'], \
                                                                 ['t', 'abba'], \
                                                                 ['t', 'abbabb']]))

def testStrLen(env):
    conn = getConnectionByEnv(env)
    env.execute_command('ft.create', 'idx', 'SCHEMA', 't', 'TEXT', 'SORTABLE')
    conn.execute_command('hset', 'doc1', 't', 'aa')
    conn.execute_command('hset', 'doc2', 't', 'aaa')
    conn.execute_command('hset', 'doc3', 't', '')

    res = env.cmd('ft.aggregate', 'idx', '*', 'load', 1, 't', 'apply', 'strlen(@t)', 'as', 'length')
    exp = [1, ['t', 'aa', 'length', '2'],
              ['t', 'aaa', 'length', '3'],
              ['t', '', 'length', '0']]
    env.assertEqual(toSortedFlatList(res), toSortedFlatList(exp))

def testLoadAll(env):
    conn = getConnectionByEnv(env)
    env.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'n', 'NUMERIC')
    conn.execute_command('HSET', 'doc1', 't', 'hello', 'n', 42, 'notIndexed', 'ccc')
    conn.execute_command('HSET', 'doc2', 't', 'world', 'n', 3.141, 'notIndexed', 'bbb')
    conn.execute_command('HSET', 'doc3', 't', 'hello world', 'n', 17.8, 'notIndexed', 'aaa')
    # without LOAD
    env.expect('FT.AGGREGATE', 'idx', '*').equal([1, [], [], []])
    # use LOAD with narg or ALL
    res = [3, ['__key', 'doc1', 't', 'hello', 'n', '42', 'notIndexed', 'ccc'],
              ['__key', 'doc2', 't', 'world', 'n', '3.141', 'notIndexed', 'bbb'],
              ['__key', 'doc3', 't', 'hello world', 'n', '17.8', 'notIndexed', 'aaa']]

    env.expect('FT.AGGREGATE', 'idx', '*', 'LOAD', 4, '__key', 't', 'n', 'notIndexed', 'SORTBY', 1, '@__key').equal(res)
    env.expect('FT.AGGREGATE', 'idx', '*', 'LOAD', '*', 'LOAD', 1, '@__key', 'SORTBY', 1, '@__key').equal(res)

    if not env.isCluster(): # TODO: fix error message in cluster
        env.expect('FT.AGGREGATE', 'idx', '*', 'LOAD', '*', 'SORTBY', 1, '@notIndexed').error().contains('not loaded nor in schema') # can be enabled in the future
        env.expect('FT.AGGREGATE', 'idx', '*', 'SORTBY', 1, '@notIndexed').error().contains('not loaded nor in schema') # without LOAD it's an error (unless we enable implicit LOAD of any field for SORTBY)
        env.expect('FT.AGGREGATE', 'idx', '*', 'LOAD', '*', 'SORTBY', 1, '@notExists').error().contains('not loaded nor in schema') # can be enabled in the future - should pass even if notExists doesn't exist
        env.expect('FT.AGGREGATE', 'idx', '*', 'SORTBY', 1, '@notExists').error().contains('not loaded nor in schema') # without LOAD it's an error (unless we enable implicit LOAD of any field for SORTBY)

def testLimitIssue(env):
    #ticket 66895
    conn = getConnectionByEnv(env)
    env.execute_command('ft.create', 'idx', 'SCHEMA', 'PrimaryKey', 'TEXT', 'SORTABLE',
                        'CreatedDateTimeUTC', 'NUMERIC', 'SORTABLE')
    conn.execute_command('HSET', 'doc1', 'PrimaryKey', '9::362330', 'CreatedDateTimeUTC', '637387878524969984')
    conn.execute_command('HSET', 'doc2', 'PrimaryKey', '9::362329', 'CreatedDateTimeUTC', '637387875859270016')
    conn.execute_command('HSET', 'doc3', 'PrimaryKey', '9::362326', 'CreatedDateTimeUTC', '637386176589869952')
    conn.execute_command('HSET', 'doc4', 'PrimaryKey', '9::362311', 'CreatedDateTimeUTC', '637383865971600000')
    conn.execute_command('HSET', 'doc5', 'PrimaryKey', '9::362310', 'CreatedDateTimeUTC', '637383864050669952')
    conn.execute_command('HSET', 'doc6', 'PrimaryKey', '9::362309', 'CreatedDateTimeUTC', '637242254008029952')
    conn.execute_command('HSET', 'doc7', 'PrimaryKey', '9::362308', 'CreatedDateTimeUTC', '637242253551670016')
    conn.execute_command('HSET', 'doc8', 'PrimaryKey', '9::362306', 'CreatedDateTimeUTC', '637166988081200000')

    _res = [8,
          ['PrimaryKey', '9::362330', 'CreatedDateTimeUTC', '637387878524969984'],
          ['PrimaryKey', '9::362329', 'CreatedDateTimeUTC', '637387875859270016'],
          ['PrimaryKey', '9::362326', 'CreatedDateTimeUTC', '637386176589869952'],
          ['PrimaryKey', '9::362311', 'CreatedDateTimeUTC', '637383865971600000'],
          ['PrimaryKey', '9::362310', 'CreatedDateTimeUTC', '637383864050669952'],
          ['PrimaryKey', '9::362309', 'CreatedDateTimeUTC', '637242254008029952'],
          ['PrimaryKey', '9::362308', 'CreatedDateTimeUTC', '637242253551670016'],
          ['PrimaryKey', '9::362306', 'CreatedDateTimeUTC', '637166988081200000']]

    actual_res = env.execute_command('FT.AGGREGATE', 'idx', '*',
                                     'APPLY', '@PrimaryKey', 'AS', 'PrimaryKey',
                                     'SORTBY', '2', '@CreatedDateTimeUTC', 'DESC', 'LIMIT', '0', '8')
    env.assertEqual(actual_res, _res)

    res = [_res[0]] + _res[1:3]
    actual_res = env.execute_command('FT.AGGREGATE', 'idx', '*',
                                     'APPLY', '@PrimaryKey', 'AS', 'PrimaryKey',
                                     'SORTBY', '2', '@CreatedDateTimeUTC', 'DESC', 'LIMIT', '0', '2')
    env.assertEqual(actual_res, res)

    res = [_res[0]] + _res[2:4]
    actual_res = env.execute_command('FT.AGGREGATE', 'idx', '*',
                                     'APPLY', '@PrimaryKey', 'AS', 'PrimaryKey',
                                     'SORTBY', '2', '@CreatedDateTimeUTC', 'DESC', 'LIMIT', '1', '2')
    env.assertEqual(actual_res, res)

    res = [_res[0]] + _res[3:5]
    actual_res = env.execute_command('FT.AGGREGATE', 'idx', '*',
                                     'APPLY', '@PrimaryKey', 'AS', 'PrimaryKey',
                                     'SORTBY', '2', '@CreatedDateTimeUTC', 'DESC', 'LIMIT', '2', '2')
    env.assertEqual(actual_res, res)

def testMaxAggResults(env):
    if env.env == 'existing-env':
        env.skip()
    env = Env(moduleArgs="MAXAGGREGATERESULTS 100")
    conn = getConnectionByEnv(env)
    env.execute_command('ft.create', 'idx', 'SCHEMA', 't', 'TEXT')
    env.expect('ft.aggregate', 'idx', '*', 'LIMIT', '0', '10000').error()   \
       .contains('LIMIT exceeds maximum of 100')

def testMaxAggInf(env):
    env.skipOnCluster()
    env.expect('ft.config', 'set', 'MAXAGGREGATERESULTS', -1).ok()
    env.expect('ft.config', 'get', 'MAXAGGREGATERESULTS').equal([['MAXAGGREGATERESULTS', 'unlimited']])

def testLoadPosition(env):
    conn = getConnectionByEnv(env)
    env.execute_command('ft.create', 'idx', 'SCHEMA', 't1', 'TEXT', 't2', 'TEXT')
    conn.execute_command('hset', 'doc1', 't1', 'hello', 't2', 'world')

    # LOAD then SORTBY
    env.expect('ft.aggregate', 'idx', '*', 'LOAD', '1', 't1', 'SORTBY', '2', '@t1', 'ASC') \
        .equal([1, ['t1', 'hello']])

    # SORTBY then LOAD
    env.expect('ft.aggregate', 'idx', '*', 'SORTBY', '2', '@t1', 'ASC', 'LOAD', '1', 't1') \
        .equal([1, ['t1', 'hello']])

    # two LOADs
    env.expect('ft.aggregate', 'idx', '*', 'LOAD', '1', 't1', 'LOAD', '1', 't2') \
        .equal([1, ['t1', 'hello', 't2', 'world']])

    # two LOADs with an apply for error
    # TODO: fix cluster error message
    if not env.isCluster():
        env.expect('ft.aggregate', 'idx', '*', 'LOAD', '1', 't1',
                   'APPLY', '@t2', 'AS', 'load_error',
                   'LOAD', '1', 't2').error().contains('not loaded nor in pipeline')


def testAggregateGroup0Field(env):
    conn = getConnectionByEnv(env)
    env.execute_command('ft.create', 'idx', 'ON', 'HASH', 'SCHEMA', 'num', 'NUMERIC', 'SORTABLE')
    for i in range(101):
        conn.execute_command('HSET', 'doc%s' % i, 't', 'text', 'num', i)

    res = env.cmd('ft.aggregate', 'idx', '*', 'GROUPBY', 0,
                                    'REDUCE', 'QUANTILE', '2', 'num', '0.95', 'AS', 'q95')
    env.assertEqual(res, [1, ['q95', '95']])
    res = env.cmd('ft.aggregate', 'idx', '*', 'GROUPBY', 0,
                  'REDUCE', 'QUANTILE', '2', 'num', '0.9', 'AS', 'q90')
    env.assertEqual(res, [1, ['q90', '90']])
    res = env.cmd('ft.aggregate', 'idx', '*', 'GROUPBY', 0,
                  'REDUCE', 'QUANTILE', '2', 'num', '0.5', 'AS', 'q50')
    env.assertEqual(res, [1, ['q50', '50']])


    conn.execute_command('FLUSHALL')
    env.execute_command('ft.create', 'idx', 'ON', 'HASH', 'SCHEMA', 'num', 'NUMERIC', 'SORTABLE')

    values = [880000.0, 685000.0, 590000.0, 1200000.0, 1170000.0, 1145000.0,
              3950000.0, 620000.0, 758000.0, 4850000.0, 800000.0, 340000.0,
              530000.0, 500000.0, 540000.0, 2500000.0, 330000.0, 525000.0,
              2500000.0, 350000.0, 590000.0, 1250000.0, 799000.0, 1380000.0]
    for i in range(len(values)):
        conn.execute_command('HSET', 'doc%s' % i, 't', 'text', 'num', values[i])


    res = env.cmd('ft.aggregate', 'idx', '*', 'GROUPBY', 0,
                  'REDUCE', 'QUANTILE', '2', 'num', '0.95', 'AS', 'q95')
    env.assertEqual(res, [1, ['q95', '3950000']])
    res = env.cmd('ft.aggregate', 'idx', '*', 'GROUPBY', 0,
                  'REDUCE', 'QUANTILE', '2', 'num', '0.9', 'AS', 'q90')
    env.assertEqual(res, [1, ['q90', '2500000']])
    res = env.cmd('ft.aggregate', 'idx', '*', 'GROUPBY', 0,
                  'REDUCE', 'QUANTILE', '2', 'num', '0.8', 'AS', 'q80')
    env.assertEqual(res, [1, ['q80', '1380000']])
    res = env.cmd('ft.aggregate', 'idx', '*', 'GROUPBY', 0,
                  'REDUCE', 'QUANTILE', '2', 'num', '0.7', 'AS', 'q70')
    env.assertEqual(res, [1, ['q70', '1170000']])
    res = env.cmd('ft.aggregate', 'idx', '*', 'GROUPBY', 0,
                  'REDUCE', 'QUANTILE', '2', 'num', '0.6', 'AS', 'q60')
    env.assertEqual(res, [1, ['q60', '880000']])
    res = env.cmd('ft.aggregate', 'idx', '*', 'GROUPBY', 0,
                  'REDUCE', 'QUANTILE', '2', 'num', '0.5', 'AS', 'q50')
    env.assertEqual(res, [1, ['q50', '758000']])

def testResultCounter(env):
    # Issue 436
    # https://github.com/RediSearch/RediSearch/issues/436
    env.skip()
    conn = getConnectionByEnv(env)
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't1', 'TEXT', 'SORTABLE')
    conn.execute_command('HSET', 'doc1', 't1', 'hello')
    conn.execute_command('HSET', 'doc2', 't1', 'hello')
    conn.execute_command('HSET', 'doc3', 't1', 'world')
    conn.execute_command('HSET', 'doc4', 't1', 'hello world')

    # first document is a match
    env.expect('FT.AGGREGATE', 'idx', '*', 'FILTER', '@t1 == "hello"').equal([1, ['t1', 'hello'], ['t1', 'hello']])
    #env.expect('FT.AGGREGATE', 'idx', '*', 'FILTER', '@t1 == "hello"').equal([2, ['t1', 'hello'], ['t1', 'hello']])

    # 3rd document is a match
    env.expect('FT.AGGREGATE', 'idx', '*', 'FILTER', '@t1 == "world"').equal([3, ['t1', 'world']])
    #env.expect('FT.AGGREGATE', 'idx', '*', 'FILTER', '@t1 == "world"').equal([1, ['t1', 'world']])

    # no match. max docID is 4
    env.expect('FT.AGGREGATE', 'idx', '*', 'FILTER', '@t1 == "foo"').equal([4])
    #env.expect('FT.AGGREGATE', 'idx', '*', 'FILTER', '@t1 == "foo"').equal([0])

def test_aggregate_timeout():
    if VALGRIND:
        # You don't want to run this under valgrind, it will take forever
        raise unittest.SkipTest("Skipping timeout test under valgrind")
    env = Env(moduleArgs='DEFAULT_DIALECT 2 ON_TIMEOUT FAIL')
    conn = getConnectionByEnv(env)
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't1', 'TEXT', 'SORTABLE')
    nshards = env.shardsCount
    num_docs = 10000 * nshards
    pipeline = conn.pipeline(transaction=False)
    for i, t1 in enumerate(np.random.randint(1, 1024, num_docs)):
        pipeline.hset (i, 't1', str(t1))
        if i % 1000 == 0:
            pipeline.execute()
            pipeline = conn.pipeline(transaction=False)
    pipeline.execute()

    # On coordinator, we currently get a single empty result on timeout,
    # because all the shards timed out but the coordinator doesn't report it.
    env.expect('FT.AGGREGATE', 'idx', '*',
               'LOAD', '2', '@t1', '@__key',
               'APPLY', '@t1 ^ @t1', 'AS', 't1exp',
               'groupby', '2', '@t1', '@t1exp',
                    'REDUCE', 'tolist', '1', '@__key', 'AS', 'keys',
               'TIMEOUT', '1',
        ).equal( ['Timeout limit was reached'] if not env.isCluster() else [1, ['t1', None, 't1exp', None, 'keys', []]])


def testGroupProperties(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'SORTABLE', 'n', 'NUMERIC', 'SORTABLE', 'tt', 'TAG')
    conn.execute_command('HSET', 'doc1', 't', 'hello', 'n', '1', 'tt', 'foo')

    # Check groupby properties
    env.expect('FT.AGGREGATE', 'idx', '*', 'GROUPBY', '3', 't', 'n', 'tt').error().contains(
                    'Bad arguments for GROUPBY: Unknown property `t`. Did you mean `@t`?')

    env.expect('FT.AGGREGATE', 'idx', '*', 'GROUPBY', '3', '@t', 'n', '@tt').error().contains(
                    'Bad arguments for GROUPBY: Unknown property `n`. Did you mean `@n`?')

    env.expect('FT.AGGREGATE', 'idx', '*', 'GROUPBY', '3', '@t', '@n', '@tt').noError()

    # Verify that we fail and not returning results from `t`
    env.expect('FT.AGGREGATE', 'idx', '*', 'GROUPBY', '1', 'tt').error().contains('Bad arguments for GROUPBY: Unknown property `tt`. Did you mean `@tt`?')
    env.expect('FT.AGGREGATE', 'idx', '*', 'GROUPBY', '1', '@tt').equal([1, ['tt', 'foo']])

    # Verify we fail on grouping by the same property twice
    env.expect('FT.AGGREGATE', 'idx', '*', 'GROUPBY', '2', '@t', '@t').error().contains('Property `t` specified more than once')

    # Verify we fail on having the same reducer output twice
    env.expect('FT.AGGREGATE', 'idx', '*', 'GROUPBY', '1', '@t',
                                           'REDUCE', 'COUNT', '0', 'AS', 't').error().contains(
                    'Property `t` specified more than once')

    env.expect('FT.AGGREGATE', 'idx', '*', 'GROUPBY', '1', '@t',
                                           'REDUCE', 'COUNT', '0', 'AS', 'my_count',
                                           'REDUCE', 'COUNT', '0', 'AS', 'my_count').error().contains(
                    'Property `my_count` specified more than once')

    env.expect('FT.AGGREGATE', 'idx', '*', 'GROUPBY', '1', '@t',
                                           'REDUCE', 'COUNT', '0',
                                           'REDUCE', 'COUNT', '0',).error().contains('specified more than once')
    # Same reducer with a different alias is ok
    env.expect('FT.AGGREGATE', 'idx', '*', 'GROUPBY', '1', '@t',
                                           'REDUCE', 'COUNT', '0', 'AS', 'my_output',
                                           'REDUCE', 'COUNT', '0', 'AS', 'my_count').noError()

    # Should behave the same in cluster and standalone, but on coordinator the AVG is translated to COUNT and SUM in the shards, and
    # two SUMs and an APPLY in the coordinator, which usually could override the same name but here we expect it to fail
    env.expect('FT.AGGREGATE', 'idx', '*', 'GROUPBY', '1', '@t',
                                           'REDUCE', 'COUNT', '0', 'AS', 'my_output',
                                           'REDUCE', 'AVG', '1', '@n', 'AS', 'my_output').error().contains(
               'Property `my_output` specified more than once')

def testGroupAfterSort(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TAG', 'n', 'NUMERIC')
    conn.execute_command('HSET', 'doc1', 't', 'AAAA', 'n', '0')
    conn.execute_command('HSET', 'doc2', 't', 'AAAA', 'n', '1')
    conn.execute_command('HSET', 'doc3', 't', 'BBBB', 'n', '0')
    conn.execute_command('HSET', 'doc4', 't', 'BBBB', 'n', '1')

    # CASE 1 #
    res = conn.execute_command('FT.AGGREGATE', 'idx', '*',
                               'SORTBY', '1', '@n', 'MAX', '2',
                               'GROUPBY', '1', '@t',
                               'REDUCE', 'COUNT', '0', 'AS', 'c')

    # On a standalone mode this is strait forward:
    # 1. we sort by `n` and take the first 2 results (doc1 and doc3)
    # 2. we group by `t` and add a `COUNT` reducer as `c`
    # 3. since doc1 and doc3 has different `t` value, we get two rows, each of COUNT 1.
    # so the expected result is:
    expected = [2, ['t', 'AAAA', 'c', '1'], ['t', 'BBBB', 'c', '1']]

    # We expect to get the same results from the coordinator, no matter what is the distribution of the docs between the shards.
    # Before the logic fix, the pipeline of ->sortby(n, limit(2))->group(t, COUNT() AS c) was changed to
    #
    # |--------------- on the shards ---------------|->|----------- on the coordinator -----------|
    # ->sortby(n, limit(2))->group(t, COUNT() AS tmp)->sortby(n, limit(2))->group(t, SUM(tmp) AS c)
    #
    # and since `n` is not in the scope when we get to the second sorter, the query fails. ([0] is returned)

    env.assertEqual(res, expected)

    # CASE 2 #
    conn.execute_command('HSET', 'doc5', 't', 'AAAA', 'n', '0')
    conn.execute_command('HSET', 'doc6', 't', 'BBBB', 'n', '1')

    res = conn.execute_command('FT.AGGREGATE', 'idx', '*',
                               'SORTBY', '3', '@n', '@t', 'DESC', 'MAX', '3',
                               'GROUPBY', '2', '@t', '@n',
                               'REDUCE', 'COUNT', '0', 'AS', 'c')

    # On a standalone mode this is strait forward:
    # 1. we sort by `n` and take the first 3 results (doc1, doc3 and doc5)
    # 2. we group by `t` and `n`, and add a `COUNT` reducer as `c`
    # 3. since doc1, doc3 and doc5 has different `t` value, we get two rows, one of COUNT 2 and one of 1.
    # 4. both rows has `n == 0` so it does not affect the aggregation
    # so the expected result is:
    expected = [2, ['t', 'AAAA', 'n', '0', 'c', '2'], ['t', 'BBBB', 'n', '0', 'c', '1']]

    # We expect to get the same results from the coordinator, no matter what is the distribution of the docs between the shards.
    # Before the logic fix, the pipeline of ->sortby(n, t, limit(3)->group(t, n, COUNT() AS c) was changed to
    #
    # |------------------ on the shards ------------------|->|-------------- on the coordinator --------------|
    # ->sortby(n, t, limit(3))->group(t, n, COUNT() AS tmp)->sortby(n, t, limit(3))->group(t, n, SUM(tmp) AS c)
    #
    # now, no matter the docs distribution (unless they all in the same shard), some rows with `n == 1` will pass the first limit,
    # will get their own row and get to the coordinator. then, we have 2 options:
    # 1. doc1 doc3 and doc5 are all in different shards. the coordinator will get 3 rows with `n == 0` and only them will pass the second
    #    sort and limit, and the second aggregation will results with the same result as in a standalone (lucky).
    # 2. some of doc1 doc3 and doc5 are in the same shard. we won't get 3 rows of `n == 0` at the second sort and limit, so a row
    #    with `n == 1` will get the the last aggregation and the final result will include 3 row:
    #    one for (t == AAAA, n == 0), one for (t == BBBB, n == 0), and one for (t == ????, n == 1)

    env.assertEqual(res, expected)


def testWithKNN(env):
    conn = getConnectionByEnv(env)
    dim = 4
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT', '6', 'DIM', dim, 'DISTANCE_METRIC', 'L2',
               'TYPE', 'FLOAT32', 'n', 'NUMERIC').ok()

    # Use {1} and {3} hash slot to verify the distribution of the documents among 2 different shards.
    conn.execute_command('HSET', 'doc1{1}', 'v', create_np_array_typed([1] * dim).tobytes(), 'n', '3')
    conn.execute_command('HSET', 'doc5{1}', 'v', create_np_array_typed([5] * dim).tobytes(), 'n', '2')
    conn.execute_command('HSET', 'doc6{1}', 'v', create_np_array_typed([6] * dim).tobytes(), 'n', '1')

    conn.execute_command('HSET', 'doc2{3}', 'v', create_np_array_typed([2] * dim).tobytes(), 'n', '5')
    conn.execute_command('HSET', 'doc3{3}', 'v', create_np_array_typed([3] * dim).tobytes(), 'n', '4')
    conn.execute_command('HSET', 'doc4{3}', 'v', create_np_array_typed([4] * dim).tobytes(), 'n', '6')

    # CASE 1 #
    # Run KNN with SORTBY. We expect that the top 3 documents in terms of vector distance will be doc1, doc2 and doc3,
    # and that after we sort by @n, we'll get doc1 and doc3 as the query results (with minial value of n among the 3
    # documents). Note that here we are testing that in coordinator know NOT to run the sort by step in the shards, but
    # run them ONLY, since there was a KNN step. Otherwise, we would get in-correct results, as doc1 would be filtered
    # out in the first shard after the sortby step.
    res = conn.execute_command('FT.AGGREGATE', 'idx', '*=>[KNN 3 @v $blob]=>{$yield_distance_as: dist}',
                               'SORTBY', '1', '@n', 'MAX', '2',
                               'PARAMS', '2', 'blob', create_np_array_typed([0] * dim).tobytes(), 'DIALECT', '2')
    expected_res = [['dist', '4', 'n', '3'], ['dist', '36', 'n', '4']]
    env.assertEqual(res[1:], expected_res)

    # CASE 2 #
    # Run KNN with APPLY - make sure that the pipeline is built correctly - APPLY should be distributed, while
    # KNN is local (and the upcoming SORTBY steps).
    res = conn.execute_command('FT.AGGREGATE', 'idx', '*=>[KNN 3 @v $blob]=>{$yield_distance_as: square_dist}',
                               "APPLY", "sqrt(@square_dist)", "AS", "L2_dist", 'SORTBY', '1', '@n', 'MAX', '2',
                               'PARAMS', '2', 'blob', create_np_array_typed([0] * dim).tobytes(), 'DIALECT', '2')
    expected_res = [{'L2_dist': '2', 'square_dist': '4', 'n': '3'}, {'L2_dist': '6', 'square_dist': '36', 'n': '4'}]
    env.assertEqual([to_dict(res_item) for res_item in res[1:]], expected_res)

    # CASE 3 #
    # Run GROUPBY after KNN. Validate that here as well we have the group by step run only local,
    # otherwise, if the groupby+reduce had ran in each shard, we would get that the count is 2 for every value of @n
    # (100 and 200), and that we would have seen in the 'c' value.
    conn.execute_command('HSET', 'doc1{1}', 'v', create_np_array_typed([1] * dim).tobytes(), 'n', '100')
    conn.execute_command('HSET', 'doc2{1}', 'v', create_np_array_typed([2] * dim).tobytes(), 'n', '100')
    conn.execute_command('HSET', 'doc3{1}', 'v', create_np_array_typed([3] * dim).tobytes(), 'n', '100')

    conn.execute_command('HSET', 'doc4{3}', 'v', create_np_array_typed([1] * dim).tobytes(), 'n', '200')
    conn.execute_command('HSET', 'doc5{3}', 'v', create_np_array_typed([2] * dim).tobytes(), 'n', '200')
    conn.execute_command('HSET', 'doc6{3}', 'v', create_np_array_typed([3] * dim).tobytes(), 'n', '200')

    expected_res = [['n', '100', 'c', '1'], ['n', '200', 'c', '1']]
    res = conn.execute_command('FT.AGGREGATE', 'idx', '*=>[KNN 2 @v $blob]=>{$yield_distance_as: dist}',
                               'GROUPBY', '1', '@n',
                               'REDUCE', 'COUNT', '0', 'AS', 'c', 'SORTBY', '1', '@n',
                               'PARAMS', '2', 'blob', create_np_array_typed([0] * dim).tobytes(), 'DIALECT', '2')
    env.assertEqual(res[1:], expected_res)
