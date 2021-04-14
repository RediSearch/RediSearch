# -*- coding: utf-8 -*-
import bz2
import json
import itertools
import unittest
from includes import *
from common import getConnectionByEnv, waitForIndex, sortedResults, toSortedFlatList, check_server_version
from time import sleep
from RLTest import Env

GAMES_JSON = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'games.json.bz2')


class TestJSON():
    def __init__(self):
        self.env = Env()
        self.conn = getConnectionByEnv(self.env)

    def testModuleLoadOrder1(self):
        res = self.conn.execute_command('MODULE', 'LIST')
        if 'ReJSON' not in res and 'search' not in res:
            # Get from env var? the path to Search Module redisearch.so
            # Get from env var? the path to ReJSON Module librejson.so
            # self.conn.execute_command('MODULE', 'LOAD', pathToSearchModuleLib)
            # res = self.conn.execute_command('MODULE', 'LIST')
            # self.env.assertNotContains('search', res)
            # self.env.expect('FT.CREATE', 'idxShouldFail', 'ON', 'JSON', 'SCHEMA', '$.t', 'TEXT').error().contains("some error")

            # TODO: test when rejson module is loaded after search
            # Expected to still be able to search json data (`search` module is able to detect the loading of `ReJSON` module)
            # Needs to be launched without `--module` parameters in RLTest
            pass

    def testModuleLoadOrder2(self):
        # TODO: test when rejson module is loaded before search
        # Once both modules are loaded, expected to still be able to search json data
        # Even data that was set before `search` module was loaded

        # Needs to be launched without `--module` parameters in RLTest
        pass

    def testNoJsonModule(self):
        # TODO: test when rejson module is not loaded (fail gracefully with error messages)
        pass

    def testSearchParams(self):
        # TODO: Test PREFIX, SORTBY, NOSTEM, Fuzzy, Pagination, Limit 0 0, Score
        # Not just repeat all search on hash tests - Only when json path is relevant?
        pass

    def testOrderOfIndexAndSet(self):

        # Set a value before index is defined
        plain_val_1 = r'{"t":"rex","n":12}'
        self.self.env.expect('json.set', 'doc:1', '$', plain_val_1).ok()
        self.self.env.expect('json.get', 'doc:1', '$').equal(plain_val_1)

        # Index creation (PM-889)
        # FIXME: Enable next line to use a numeric value - currently crash when index is defined with NUMERIC
        self.conn.execute_command('FT.CREATE', 'idx1', 'ON', 'JSON', 'SCHEMA', '$.t', 'TEXT')
        waitForIndex(self.env, 'idx1')

        # No results before ingestion
        self.env.expect('ft.search', 'idx1', 'rice*').equal([0L])

        # Set another value after index was defined
        plain_val_2 = r'{"t":"riceratops","n":9}'
        self.env.expect('json.set', 'doc:2', '$', plain_val_2).ok()
        self.env.expect('json.get', 'doc:2', '$').equal(plain_val_2)
        self.env.expect('json.get', 'doc:2', '$.n').equal('9')
        self.env.expect('json.get', 'doc:2', '$.t').equal('"riceratops"')

        # Find both values (set before and after index creation)
        self.env.expect('ft.search', 'idx1', '*', 'RETURN', '1', '$.t').equal(
            [2L, 'doc:1', ['$.t', '"rex"'], 'doc:2', ['$.t', '"riceratops"']])
        self.env.expect('ft.search', 'idx1', 're*').equal([1L, 'doc:1', ['$', r'{"t":"rex","n":12}']])

    def testUpdate(self):

        # Update an existing text value
        plain_val_3 = '"hescelosaurus"'
        self.env.expect('json.set', 'doc:1', '$.t', plain_val_3).ok()
        self.env.expect('json.get', 'doc:1', '$.t').equal(plain_val_3)

        # TODO: Update an existing numeric value
        self.env.expect('ft.search', 'idx1', 'he*').equal([1L, 'doc:1', ['$', r'{"t":"hescelosaurus","n":12}']])

    def testNonEnglish(self):

        # Test json in non-English languages
        japanese_value_1 = 'ドラゴン'
        japanese_doc_value = r'{"t":"' + japanese_value_1 + r'","n":5}'
        self.env.expect('json.set', 'doc:4', '$', japanese_doc_value).ok()
        self.env.expect('json.get', 'doc:4', '$').equal(japanese_doc_value)
        self.env.expect('json.get', 'doc:4', '$.t').equal('"' + japanese_value_1 + '"')

        chinese_value_1 = r'{"t":"踪迹","n":5}'
        self.env.expect('json.set', 'doc:5', '$', chinese_value_1).ok()
        self.env.expect('json.get', 'doc:5', '$').equal(chinese_value_1)

    def testNoContent(self):

        # Test NOCONTENT
        self.env.expect('ft.search', 'idx1', 're*', 'NOCONTENT').equal([0L])
        self.env.expect('ft.search', 'idx1', 'he*', 'NOCONTENT').equal([1L, 'doc:1'])

    def testIndexWithAsLabel(self):
        # FIXME: Enable next line when RETURN param supports AS
        # self.env.expect('ft.search', 'idx1', '*', 'RETURN', '$.t', 'AS', 'MyReturnLabel').equal([1L, 'doc:1', ['MyReturnLabel', '\"hescelosaurus\"']])

        # If label is defined at schema field - should not be found using a specific 'AS' in the RETURN param in the Search query
        # FIXME: Enable next line when RETURN param supports AS
        # self.env.expect('ft.search', 'idx1', '*', 'RETURN', '2', 'labelT', '$.t').equal([1L, 'doc:1', ['labelT', r'"hescelosaurus"']])
        self.env.expect('ft.search', 'idx1', '*').equal([4L, 'doc:2', ['$', '{"t":"riceratops","n":9}'],
                                                         'doc:1', ['$', '{"t":"hescelosaurus","n":12}'],
                                                         'doc:4', ['$',
                                                                   '{"t":"\xe3\x83\x89\xe3\x83\xa9\xe3\x82\xb4\xe3\x83\xb3","n":5}'],
                                                         'doc:5', ['$', '{"t":"\xe8\xb8\xaa\xe8\xbf\xb9","n":5}']])

        # for now, can't load a field which was not specified
        self.env.expect('ft.search', 'idx1', 'riceratops', 'RETURN', '1', '$').equal(
            [1L, 'doc:2', ['$', '{"t":"riceratops","n":9}']])
        self.env.expect('ft.search', 'idx1', 'riceratops', 'RETURN', '1', '$.n').equal([1L, 'doc:2', ['$.n', '9']])
        self.env.expect('ft.search', 'idx1', 'riceratops', 'RETURN', '1', '$.t').equal(
            [1L, 'doc:2', ['$.t', '"riceratops"']])

    def add_values(env, number_of_iterations=1):
        res = env.execute_command('FT.CREATE', 'games', 'ON', 'JSON',
                                  'SCHEMA', '$.title', 'TEXT', 'SORTABLE',
                                  '$.brand', 'TEXT', 'NOSTEM', 'SORTABLE',
                                  )  # ,'$.description', 'AS', 'description', 'TEXT', 'price', 'NUMERIC',
        # 'categories', 'TAG')
        waitForIndex(env, 'games')

        for i in range(number_of_iterations):
            fp = bz2.BZ2File(GAMES_JSON, 'r')
            for line in fp:
                obj = json.loads(line)
                id = obj['asin'] + (str(i) if i > 0 else '')
                del obj['asin']
                b = obj.get('brand')
                obj['brand'] = str(b) if b else ""
                # FIXME: When NUMERIC is restored, restore 'price'
                del obj['price']
                # obj['price'] = obj.get('price') or 0
                str_val = json.dumps(obj)
                cmd = ['JSON.SET', id, '$', str_val]
                env.execute_command(*cmd)
            fp.close()

    def testAggregate(self):
        # TODO: Move back to test_aggregate.py together with testing Aggregate with Hash
        self.add_values()

        cmd = ['ft.aggregate', 'games', '*',
               'GROUPBY', '1', '@$.brand',
               'REDUCE', 'count', '0', 'AS', 'count',
               'SORTBY', 2, '@count', 'desc',
               'LIMIT', '0', '5'
               ]
        self.env.expect(*cmd).equal([292L, ['$.brand', '""', 'count', '1518'],
                                     ['$.brand', '"mad catz"', 'count', '43'],
                                     ['$.brand', '"generic"', 'count', '40'],
                                     ['$.brand', '"steelseries"', 'count', '37'],
                                     ['$.brand', '"logitech"', 'count', '35']])
        # FIXME: Test FT.AGGREGATE params

    def testDemo(self):

        # Set a value before index is defined
        tlv = r'{"iata":"TLV","name":"Ben Gurion International Airport","location":"34.8866997,32.01139832"}'
        sfo = r'{"iata":"SFO","name":"San Francisco International Airport","location":"-122.375,37.6189995"}'
        tlv_doc = [1L, 'A:TLV', ['$', tlv]]
        sfo_doc = [1L, 'A:SFO', ['$', sfo]]

        self.env.expect('json.set', 'A:TLV', '$', tlv).ok()
        self.env.expect('json.set', 'A:SFO', '$', sfo).ok()

        self.env.expect('FT.CREATE airports ON JSON SCHEMA $.iata AS iata TAG SORTABLE                 \
                                                      $.iata AS iata_txt TEXT NOSTEM              \
                                                      $.name AS name TEXT NOSTEM PHONETIC dm:en   \
                                                      $.location AS location GEO').ok()

        self.env.expect('json.set', 'A:TLV', '$', tlv).ok()
        self.env.expect('json.set', 'A:SFO', '$', sfo).ok()

        info = self.env.cmd('FT.INFO airports')
        self.env.assertEqual(info[0:2], ['index_name', 'airports'])
        self.env.assertEqual(info[5][0:2], ['key_type', 'JSON'])
        self.env.assertEqual(info[7], [['iata', 'type', 'TAG', 'SEPARATOR', ',', 'SORTABLE'],
                                       ['iata_txt', 'type', 'TEXT', 'WEIGHT', '1', 'NOSTEM'],
                                       ['name', 'type', 'TEXT', 'WEIGHT', '1', 'NOSTEM'],
                                       ['location', 'type', 'GEO']])
        self.env.assertEqual(info[8:10], ['num_docs', '2'])

        self.env.expect('FT.SEARCH', 'airports', 'TLV').equal(tlv_doc)
        self.env.expect('FT.SEARCH', 'airports', 'TL*').equal(tlv_doc)
        self.env.expect('FT.SEARCH', 'airports', 'sen frensysclo').equal(sfo_doc)
        self.env.expect('FT.SEARCH', 'airports', '@location:[-122.41 37.77 100 km]').equal(sfo_doc)
        self.env.expect('FT.SEARCH', 'airports', 'sfo', 'RETURN', '3', '$.name', 'AS', 'name') \
            .equal([1L, 'A:SFO', ['name', '"San Francisco International Airport"']])

    def testIncrBy(self):
        # TODO: TODO2: test JSON.NUMINCRBY (JSON.NUMMULTBY will be removed)
        pass

    def testScalars(self):
        # Find All scalars types - need bool, int, double, float and null
        scalars = '{"str":"top string", "bool":true, "int":1, "float":1.0, "null":null, "nested_scalars": {"str":"nested1 string", "bool":false, "int":24, "float":2.723, "null":null, "nested2": {"str":"nested2 string", "bool":false, "nested int":3496, "nested float":3.3333, "null":null, "arr": ["nested3 arr string", false, 402, 41.048, null, {"str":"nested4 string"}]}}}'
        '''
        '{
            "str":"top string",
            "bool":true,
            "int":1,
            "float":1.0,
            "null":null,
            "nested scalars":
            {
                "str":"nested1 string",
                "bool":false,
                "int":24,
                "float":2.723,
                "null":null,
                "nested2":
                {
                    "str":"nested2 string",
                    "bool":false,
                    "nested int":3496,
                    "nested float":3.3333,
                    "null":null,
                    "arr":
                    [
                        "nested3 arr string", false, 402, 41.048, null,
                        {
                            "str":"nested4 string"
                        }
                    ]
                }
            }
        }'  
        '''
        self.env.expect('json.set', 'scalar', '$', scalars).ok()
        self.env.expect('json.get', 'scalar', '$') \
            .equal(
            r'"{"str":"top string","bool":true,"int":1,"float":1.0,"null":null,"nested_scalars":{"str":"nested1 string","bool":false,"int":24,"float":2.723,"null":null,"nested2":{"str":"nested2 string","bool":false,"nested int":3496,"nested float":3.3333,"null":null,"arr":["nested3 arr string",false,402,41.048,null,{"str":"nested4 string"}]}}}""')

        self.env.expect('ft.create scalars ON JSON SCHEMA \
                        $.nested_scalars.nested2.arr[1] AS bool TEXT SORTABLE \
                        $.nested_scalars.nested2.arr[2] AS int TEXT SORTABLE \
                        $.nested_scalars.nested2.arr[3] AS flt TEXT SORTABLE \
                        $.nested_scalars.nested2.arr[4] AS nil TEXT SORTABLE')\
            .ok()


        self.env.expect('ft.search', 'scalars', 'fa*', 'RETURN', '3', '$.nested_scalars.nested2.arr[1]', 'AS', 'bo') \
            .equal([1L, 'scalar', ['bo', 'false']])
        self.env.expect('ft.search', 'scalars', '*', 'RETURN', '3', '$.nested_scalars.nested2.arr[2]', 'AS', 'in') \
            .equal([1L, 'scalar', ['in', 402]])
        self.env.expect('ft.search', 'scalars', '*', 'RETURN', '3', '$.nested_scalars.nested2.arr[3]', 'AS', 'fl') \
            .equal([1L, 'scalar', ['fl', 41.048]])
        self.env.expect('ft.search', 'scalars', 'nu*', 'RETURN', '3', '$.nested_scalars.nested2.arr[4]', 'AS', 'nl') \
            .equal([1L, 'scalar', ['nl', 'null']])

        # TODO: test path with spaces in its name?, e.g., '$.nested scalars'

    def testArray(self):
        # Find a result which is an array (bulk string)
        # Array slot which is array
        pass

    def testObject(self):
        # Find a result which is an object (bulk string)
        pass

        # TODO: Check null values

    def testDel(self):
        # Delete a key and see it is not found
        pass

        # Nested paths (with hierarchy)
        # Array slot which is scalar
        # Array slot which is Object - expect an err

        # Same for Object - Object value which is Object
        # Object value which is an Arrary
        # Illegal json syntax (starting with illegal char - not $ or ., missing colon comma, parens, wrong metatype, e.g. numeric in key name,...)
        # AS Label containing $ or .  - error
        # Jsonpath starting with . (BWC) - OK
        # Missing Jsonpath (BWC) ==> $ assumed?
        # Out of bound index
        # Index for non-array
        #     Hash content not indexed together with json - currently they are mixed together?
        # Highlight
        # Summarize
        # Rejson unload?
        # Drop index and not find previous results
        # RSCoordinator
        # Cluster
        # Enterprise?
        # CRDT?
