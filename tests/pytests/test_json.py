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

# def testSanity(env):
#     conn = getConnectionByEnv(env)
#     conn.execute_command('ft.create', 'idx', 'ON', 'JSON', 'SCHEMA', 't', 'text')

def testSearch(env):
    conn = getConnectionByEnv(env)

    # TODO: test when rejson module is loaded after search
    # TODO: test when rejson module is loaded before search
    # TODO: test when rejson module is not loaded (fail gracefully with error messages)

    # Set a value before index is defined
    plain_val_1 = r'{"t":"rex","n":12}'
    env.expect('json.set', 'doc:1', '$', plain_val_1).ok()
    env.expect('json.get', 'doc:1', '$').equal(plain_val_1)

    # Index creation (PM-889)
    # FIXME: Enable next line to use a numeric value - currently crash when index is defined with NUMERIC
    #conn.execute_command('FT.CREATE', 'idx1', 'ON', 'JSON', 'SCHEMA', '$.t', 'AS', 'labelT', 'TEXT', '$.n', 'AS', 'labelN', 'NUMERIC')
    conn.execute_command('FT.CREATE', 'idx1', 'ON', 'JSON', 'SCHEMA', '$.t', 'TEXT')
    waitForIndex(env, 'idx1')
    # TODO: Test PREFIX, SORTBY, NOSTEM, Fuzzy, Pagination, Limit 0 0, Score - Or just repeat all search on hash tests?

    # No results before ingestion
    #json.ge    env.expect('ft.search', 'idx1', 'rice*').equal([0L])

    # Set another value after index was defined
    plain_val_2 = r'{"t":"riceratops","n":9}'
    env.expect('json.set', 'doc:2', '$', plain_val_2).ok()
    env.expect('json.get', 'doc:2', '$').equal(plain_val_2)
    env.expect('json.get', 'doc:2', '$.n').equal('9')
    env.expect('json.get', 'doc:2', '$.t').equal('"riceratops"')
    '''
    # FIXME: Enable next line when json bulk string is printed in the result
    #env.assertEquals(res, [2L, 'doc:1', ['$', plain_val_1], 'doc:2', ['$', plain_val_2]])
    env.expect('ft.search', 'idx1', '*').equal([2L, 'doc:1', ['$', plain_val_1], 'doc:2', ['$', plain_val_2]])
    env.expect('ft.search', 'idx1', '*', 'RETURN', '1', '$.t').equal([2L, 'doc:1', ['$.t', '"rex"'], 'doc:2', ['$.t', '"riceratops"']])

    # FIXME: Enable next line when json bulk string is printed in the result
    env.expect('ft.search', 'idx1', 're*').equal([1L, 'doc:1', ['$', r'{"t":"rex","n":12}']])

    # Update an existing text value
    plain_val_3 = '"hescelosaurus"'
    env.expect('json.set', 'doc:1', '$.t', plain_val_3).ok()
    env.expect('json.get', 'doc:1', '$.t').equal(plain_val_3)

    # TODO: Update an existing numeric value
    # TODO: test JSON.NUMINCRBY and JSON.NUMMULTBY
    # TODO: Check null values
    # TODO: Check arrays
    # TODO: Check Object/Map
    # TODO: Fail when result is an Object/Map

    # FIXME: Enable next line when json bulk string is printed in the result
    env.expect('ft.search', 'idx1', 'he*').equal([1L, 'doc:1', ['$', r'{"t":"hescelosaurus","n":12}']])

    # Test json in non-English languages
    japanese_value_1 = 'ドラゴン'
    japanese_doc_value = r'{"t":"' + japanese_value_1 + r'","n":5}'
    env.expect('json.set', 'doc:4', '$', japanese_doc_value).ok()
    env.expect('json.get', 'doc:4', '$').equal(japanese_doc_value)
    env.expect('json.get', 'doc:4', '$.t').equal('"' + japanese_value_1 + '"')

    chinese_value_1 = r'{"t":"踪迹","n":5}'
    env.expect('json.set', 'doc:5', '$', chinese_value_1).ok()
    env.expect('json.get', 'doc:5', '$').equal(chinese_value_1)

    # Test NOCONTENT
    env.expect('ft.search', 'idx1', 're*', 'NOCONTENT').equal([0L])
    env.expect('ft.search', 'idx1', 'he*', 'NOCONTENT').equal([1L, 'doc:1'])


    # FIXME: Enable next line when RETURN param supports AS
    #env.expect('ft.search', 'idx1', '*', 'RETURN', '$.t', 'AS', 'MyReturnLabel').equal([1L, 'doc:1', ['MyReturnLabel', '\"hescelosaurus\"']])

    # If label is defined at schema field - should not be found using a specific 'AS' in the RETURN param in the Search query
    # FIXME: Enable next line when RETURN param supports AS
    #env.expect('ft.search', 'idx1', '*', 'RETURN', '2', 'labelT', '$.t').equal([1L, 'doc:1', ['labelT', r'"hescelosaurus"']])
    env.expect('ft.search', 'idx1', '*').equal([4L, 'doc:2', ['$', '{"t":"riceratops","n":9}'],
                                                    'doc:1', ['$', '{"t":"hescelosaurus","n":12}'],
                                                    'doc:4', ['$', '{"t":"\xe3\x83\x89\xe3\x83\xa9\xe3\x82\xb4\xe3\x83\xb3","n":5}'],
                                                    'doc:5', ['$', '{"t":"\xe8\xb8\xaa\xe8\xbf\xb9","n":5}']])
    '''
    # for now, can't load a fieldwhich was not specified
    env.expect('ft.search', 'idx1', 'riceratops', 'RETURN', '1', '$').equal([1L, 'doc:2', ['$', '{"t":"riceratops","n":9}']])
    env.expect('ft.search', 'idx1', 'riceratops', 'RETURN', '1', '$.n').equal([1L, 'doc:2', ['$.n', '9']])
    env.expect('ft.search', 'idx1', 'riceratops', 'RETURN', '1', '$.t').equal([1L, 'doc:2', ['$.t', '"riceratops"']])

def add_values(env, number_of_iterations=1):
    res = env.execute_command('FT.CREATE', 'games', 'ON', 'JSON',
                        'SCHEMA', '$.title', 'TEXT', 'SORTABLE',
                        '$.brand', 'TEXT', 'NOSTEM', 'SORTABLE',
                              )#,'$.description', 'AS', 'description', 'TEXT', 'price', 'NUMERIC',
                        #'categories', 'TAG')
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
            #obj['price'] = obj.get('price') or 0
            str_val = json.dumps(obj)
            cmd = ['JSON.SET', id, '$', str_val]
            env.execute_command(*cmd)
        fp.close()

def testAggregate(env):

    add_values(env)

    cmd = ['ft.aggregate', 'games', '*',
           'GROUPBY', '1', '@$.brand',
           'REDUCE', 'count', '0', 'AS', 'count',
           'SORTBY', 2, '@count', 'desc',
           'LIMIT', '0', '5'
           ]
    env.expect(*cmd).equal([292L, ['$.brand', '""', 'count', '1518'],
                                  ['$.brand', '"mad catz"', 'count', '43'],
                                  ['$.brand', '"generic"', 'count', '40'],
                                  ['$.brand', '"steelseries"', 'count', '37'],
                                  ['$.brand', '"logitech"', 'count', '35']])
    # FIXME: Test FT.AGGREGATE params

def testDemo(env):
    conn = getConnectionByEnv(env)

    # Set a value before index is defined
    tlv = r'{"iata":"TLV","name":"Ben Gurion International Airport","location":"34.8866997,32.01139832"}'
    sfo = r'{"iata":"SFO","name":"San Francisco International Airport","location":"-122.375,37.6189995"}'
    tlv_doc = [1L, 'A:TLV', ['$', tlv]]
    sfo_doc = [1L, 'A:SFO', ['$', sfo]]

    env.expect('json.set', 'A:TLV', '$', tlv).ok()
    env.expect('json.set', 'A:SFO', '$', sfo).ok()

    env.expect('FT.CREATE airports ON JSON SCHEMA $.iata AS iata TAG SORTABLE                 \
                                                  $.iata AS iata_txt TEXT NOSTEM              \
                                                  $.name AS name TEXT NOSTEM PHONETIC dm:en   \
                                                  $.location AS location GEO').ok()

    env.expect('json.set', 'A:TLV', '$', tlv).ok()
    env.expect('json.set', 'A:SFO', '$', sfo).ok()

    info = env.cmd('FT.INFO airports')
    env.assertEqual(info[0:2], ['index_name', 'airports'])
    env.assertEqual(info[5][0:2], ['key_type', 'JSON'])
    env.assertEqual(info[7], [['iata', 'type', 'TAG', 'SEPARATOR', ',', 'SORTABLE'],
                              ['iata_txt', 'type', 'TEXT', 'WEIGHT', '1', 'NOSTEM'],
                              ['name', 'type', 'TEXT', 'WEIGHT', '1', 'NOSTEM'],
                              ['location', 'type', 'GEO']])
    env.assertEqual(info[8:10], ['num_docs', '2'])

    env.expect('FT.SEARCH', 'airports', 'TLV').equal(tlv_doc)
    env.expect('FT.SEARCH', 'airports', 'TL*').equal(tlv_doc)
    env.expect('FT.SEARCH', 'airports', 'sen frensysclo').equal(sfo_doc)
    env.expect('FT.SEARCH', 'airports', '@location:[-122.41 37.77 100 km]').equal(sfo_doc)
    env.expect('FT.SEARCH', 'airports', 'sfo', 'RETURN', '3', '$.name', 'AS', 'name')       \
                .equal([1L, 'A:SFO', ['name', '"San Francisco International Airport"']])
