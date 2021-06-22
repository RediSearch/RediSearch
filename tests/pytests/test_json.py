# -*- coding: utf-8 -*-
import json

import bz2

from common import getConnectionByEnv, waitForIndex
from includes import *

UNSTABLE_TESTS = os.getenv('UNSTABLE_TESTS', '0') == '1'

GAMES_JSON = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'games.json.bz2')

doc1_content = r'''{"string": "gotcha1",
                "null": null,
                "boolT": true,
                "boolN": true,
                "int": 972,
                "flt": 9.72,
                "geo": "1.23,4.56",
                "obj": {"int": 1, "string": "gotcha6","c": null},
                "complex_arr": [42, null, -1.2, false, {"nested_array":["sub", "array", "gotcha2"]}, {"nested_obj": "gotcha3"}, "gotcha4"],
                "scalar_arr": [42, null, -1.2, false, "gotcha5"],
                "string_arr": ["a", "b", "c", "d", "e", "f", "gotcha6"]
            }'''

def testSearchUpdatedContent(env):
    conn = getConnectionByEnv(env)

    # TODO: test when rejson module is loaded after search
    # TODO: test when rejson module is loaded before search
    # TODO: test when rejson module is not loaded (fail gracefully with error messages)

    # Set a value before index is defined
    plain_val_1 = r'{"t":"rex","n":12}'
    env.expect('json.get', 'doc:1', '$').equal(None)
    env.expect('json.set', 'doc:1', '$', plain_val_1).ok()
    env.expect('json.get', 'doc:1', '$').equal(plain_val_1)

    # Index creation
    conn.execute_command('FT.CREATE', 'idx1', 'ON', 'JSON', 'SCHEMA', '$.t', 'AS', 'labelT', 'TEXT', '$.n', 'AS',
                         'labelN', 'NUMERIC')
    waitForIndex(env, 'idx1')

    # No results before ingestion
    env.expect('ft.search', 'idx1', 'rice*').equal([0L])

    # Set another value after index was defined
    plain_val_2 = r'{"t":"riceratops","n":9}'
    env.expect('json.set', 'doc:2', '$', plain_val_2).ok()
    env.expect('json.get', 'doc:2', '$').equal(plain_val_2)
    env.expect('json.get', 'doc:2', '$.n').equal('9')
    env.expect('json.get', 'doc:2', '$.t').equal('"riceratops"')

    # Test updated values are found
    env.expect('ft.search', 'idx1', '*').equal([2L, 'doc:1', ['$', plain_val_1], 'doc:2', ['$', plain_val_2]])
    env.expect('ft.search', 'idx1', 're*').equal([1L, 'doc:1', ['$', plain_val_1]])

    # TODO: Why does the following result look like that? (1 count and 2 arrays of result pairs)
    env.expect('ft.aggregate', 'idx1', '*', 'LOAD', '1', 'labelT').equal(
        [1L, ['labelT', '"rex"'], ['labelT', '"riceratops"']])
    env.expect('ft.aggregate', 'idx1', 're*', 'LOAD', '1', 'labelT').equal([1L, ['labelT', '"rex"']])

    res = env.execute_command('ft.aggregate', 'idx1', '*', 'LOAD', '1', 'labelT')

    # Update an existing text value
    plain_text_val_3 = '"hescelosaurus"'
    env.expect('json.set', 'doc:1', '$.t', plain_text_val_3).ok()
    env.expect('json.get', 'doc:1', '$.t').equal(plain_text_val_3)

    # Update an existing int value
    plain_int_val_3 = '13'
    int_incrby_3 = '2'
    plain_int_res_val_3 = str(int(plain_int_val_3) + int(int_incrby_3))
    env.expect('json.set', 'doc:1', '$.n', plain_int_val_3).ok()
    # test JSON.NUMINCRBY
    env.expect('json.numincrby', 'doc:1', '$.n', int_incrby_3).equal(plain_int_res_val_3)

    env.expect('ft.search', 'idx1', 'he*').equal(
        [1L, 'doc:1', ['$', r'{"t":"hescelosaurus","n":' + plain_int_res_val_3 + '}']])

    env.expect('ft.search', 'idx1', 'riceratops', 'RETURN', '1', '$').equal(
        [1L, 'doc:2', ['$', '{"t":"riceratops","n":9}']])
    env.expect('ft.search', 'idx1', 'riceratops', 'RETURN', '1', '$.n').equal([1L, 'doc:2', ['$.n', '9']])
    env.expect('ft.search', 'idx1', 'riceratops', 'RETURN', '1', '$.t').equal([1L, 'doc:2', ['$.t', '"riceratops"']])


# FIXME: Test PREFIX, SORTBY, NOSTEM, Fuzzy, Pagination, Limit 0 0, Score - Need to repeat all search testing as done on hash?
# FIXME: Test Aggregate - Need to repeat all aggregate testing as done on hash?

# TODO: Check null values
# TODO: Check arrays
# TODO: Check Object/Map

def testHandleUnindexedTypes(env):
    # TODO: Ignore and resume indexing when encountering an Object/Array/null
    # TODO: Except for array of only scalars which is defined as a TAG in the schema
    # ... FT.CREATE idx SCHEMA $.arr TAG
    if not UNSTABLE_TESTS:
        env.skip()
    env.expect('JSON.SET', 'doc:1', '$', doc1_content).ok()

    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
                        '$.string', 'AS', 'string', 'TEXT',
                        '$.null', 'AS', 'nil', 'TEXT',
                        '$.boolT', 'AS', 'boolT', 'TEXT',
                        '$.boolN', 'AS', 'boolN', 'NUMERIC',
                        '$.int', 'AS', 'int', 'NUMERIC',
                        '$.flt', 'AS', 'flt', 'NUMERIC',
                        '$.geo', 'AS', 'geo', 'GEO',
                        '$.obj', 'AS', 'obj', 'TEXT',
                        '$.complex_arr', 'AS', 'complex_arr', 'TEXT',
                        '$.scalar_arr', 'AS', 'scalar_arr', 'TAG',
                        '$.int_arr', 'AS', 'int_arr', 'TAG',
                        ).ok()
    waitForIndex(env, 'idx')
    # FIXME: Why does the following search return zero results?
    env.expect('ft.search', 'idx', '*', 'RETURN', '2', 'string', 'int_arr')\
        .equal([1L, 'doc:1', ['string', '"gotcha1"', 'int_arr', ["a", "b", "c", "d", "e", "f", "gotcha6"]]])

    # TODO: test TAGVALS ?
    pass


def testReturnAllTypes(env):
    # Test returning all JSON types
    # (even if some of them are not able to be indexed/found,
    # they can be returned together with other fields which are indexed)

    env.expect('JSON.SET', 'doc:1', '$', doc1_content).ok()
    env.execute_command('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.string', 'AS', 'string', 'TEXT')


    # TODO: Make sure TAG can be used as a label in "FT.SEARCH idx "*" RETURN $.t As Tag"
    pass


def testOldJsonPathSyntax(env):
    # Make sure root path '.' is working
    # For example, '$.t' should also work as '.t' and 't'
    pass


def testNoContent(env):
    # Test NOCONTENT
    env.execute_command('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.t', 'TEXT', '$.flt', 'NUMERIC')
    waitForIndex(env, 'idx')
    env.execute_command('JSON.SET', 'doc:1', '$', r'{"t":"riceratops","n":"9072","flt":97.2}')
    env.expect('ft.search', 'idx', 're*', 'NOCONTENT').equal([0L])
    env.expect('ft.search', 'idx', 'ri*', 'NOCONTENT').equal([1L, 'doc:1'])


def testNonEnglish(env):
    # Test json in non-English languages
    env.execute_command('FT.CREATE', 'idx1', 'ON', 'JSON', 'SCHEMA', '$.t', 'AS', 'labelT', 'TEXT', '$.n', 'AS',
                        'labelN', 'NUMERIC')
    waitForIndex(env, 'idx1')
    japanese_value_1 = 'ドラゴン'
    japanese_doc_value = r'{"t":"' + japanese_value_1 + r'","n":5}'
    env.expect('json.set', 'doc:4', '$', japanese_doc_value).ok()
    env.expect('json.get', 'doc:4', '$').equal(japanese_doc_value)
    env.expect('json.get', 'doc:4', '$.t').equal('"' + japanese_value_1 + '"')

    chinese_value_1 = r'{"t":"踪迹","n":5}'
    env.expect('json.set', 'doc:5', '$', chinese_value_1).ok()
    env.expect('json.get', 'doc:5', '$').equal(chinese_value_1)

    env.expect('ft.search', 'idx1', '*', 'RETURN', '3', '$.t', 'AS', 'MyReturnLabel') \
        .equal([2L,
                'doc:4', ['MyReturnLabel', '"\xe3\x83\x89\xe3\x83\xa9\xe3\x82\xb4\xe3\x83\xb3"'],
                'doc:5', ['MyReturnLabel', '"\xe8\xb8\xaa\xe8\xbf\xb9"']])


def testSet(env):
    # JSON.SET (either set the entire key or a sub-value)
    # Can also do multiple changes/side-effects, such as converting an object to a scalar
    env.execute_command('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.t', 'TEXT')
    waitForIndex(env, 'idx')
    env.execute_command('JSON.SET', 'doc:1', '$', r'{"t":"ReJSON"}')
    env.expect('ft.search', 'idx', 're*', 'NOCONTENT').equal([1L, 'doc:1'])

def testDel(env):
    # JSON.DEL and JSON.FORGET
    env.execute_command('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.t', 'TEXT')
    env.execute_command('JSON.SET', 'doc:1', '$', r'{"t":"ReJSON"}')
    env.execute_command('JSON.SET', 'doc:2', '$', r'{"t":"RediSearch"}')
    env.expect('ft.search', 'idx', 're*', 'NOCONTENT').equal([2L, 'doc:1', 'doc:2'])
    env.expect('JSON.DEL', 'doc:2', '$.t').equal(1L)
    env.expect('ft.search', 'idx', 're*', 'NOCONTENT').equal([1L, 'doc:1'])
    env.expect('JSON.FORGET', 'doc:1', '$.t').equal(1L)
    env.expect('ft.search', 'idx', 're*', 'NOCONTENT').equal([0L])

def testToggle(env):
    # JSON.TOGGLE
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
               '$.string', 'AS', 'string', 'TEXT',
               '$.boolT', 'AS', 'boolT', 'TEXT').ok()
    waitForIndex(env, 'idx')
    env.expect('JSON.SET', 'doc:1', '.', '{"foo":true, "boolT":false}').ok()
    env.expect('ft.search', 'idx', '*').equal([1L, 'doc:1', ['$', '{"foo":true,"boolT":false}']])
    env.expect('JSON.TOGGLE','doc:1','$.boolT').equal('true')
    env.expect('ft.search', 'idx', '*').equal([1L, 'doc:1', ['$', '{"foo":true,"boolT":true}']])

def testStrappend(env):
    # JSON.STRAPPEND

    env.execute_command('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.t', 'TEXT')
    waitForIndex(env, 'idx')
    env.execute_command('JSON.SET', 'doc:1', '$', r'{"t":"Redis"}')
    env.expect('ft.search', 'idx', 'Redis').equal([1L, 'doc:1', ['$', '{"t":"Redis"}']])
    env.execute_command('JSON.STRAPPEND', 'doc:1', '.t', '"Labs"')
    env.expect('ft.search', 'idx', '*').equal([1L, 'doc:1', ['$', '{"t":"RedisLabs"}']])
    env.expect('ft.search', 'idx', 'RedisLabs').equal([1L, 'doc:1', ['$', '{"t":"RedisLabs"}']])
    env.expect('ft.search', 'idx', 'Redis').equal([0L])

def testArrappend(env):
    # JSON.ARRAPPEND
    # FIXME: Currently unsupported
    pass


def testArrInsert(env):
    # JSON.ARRINSERT
    # FIXME: Currently unsupported
    pass


def testArrpop(env):
    # JSON.ARRPOP
    env.execute_command('FT.CREATE', 'idx1', 'ON', 'JSON', 'SCHEMA', '$.t', 'AS', 'labelT', 'TEXT')
    waitForIndex(env, 'idx1')
    env.expect('JSON.SET', 'doc:1', '$', '{"t":["foo", "bar", "back"]}').ok()

    # FIXME: Enable the following line: Should we search in array content? Need TAG for that?
    #env.expect('FT.SEARCH', 'idx1', 'ba*', 'RETURN', '1', 'labelT').equal([1L, 'doc:1', ['labelT', '"bar"']])

    # FIXME: Why aggregate 'ba*' returns zero results?
    # env.expect('FT.AGGREGATE', 'idx1', 'ba*', 'LOAD', '3', '@$.t', 'AS', 't').equal([1L, ['t', '["foo","bar","back"]']])

    env.expect('FT.SEARCH', 'idx1', '*').equal([1L, 'doc:1', ['$', '{"t":["foo","bar","back"]}']])
    env.expect('FT.AGGREGATE', 'idx1', '*', 'LOAD', '3', '@$.t', 'AS', 't').equal([1L, ['t', '["foo","bar","back"]']])

    env.expect('JSON.ARRPOP', 'doc:1', '$.t').equal('"back"')
    env.expect('FT.SEARCH', 'idx1', '*').equal([1L, 'doc:1', ['$', '{"t":["foo","bar"]}']])
    env.expect('JSON.ARRPOP', 'doc:1', '$.t', 0).equal('"foo"')
    env.expect('FT.SEARCH', 'idx1', '*').equal([1L, 'doc:1', ['$', '{"t":["bar"]}']])


def testRootValues(env):
    # Search all JSON types as a top-level element
    # FIXME:
    pass


def testArrtrim(env):
    # json.arrtrim
    # FIXME:
    pass


def testAsTag(env):
    # Index with Tag for array with multi-values
    # FIXME:
    pass


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
    # FIXME: Test FT.AGGREGATE params - or alternatively reuse test_aggregate.py to also run on json content


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
    env.expect('FT.SEARCH', 'airports', 'sfo', 'RETURN', '1', '$.name') \
        .equal([1L, 'A:SFO', ['$.name', '"San Francisco International Airport"']])


def testIndexSeparation(env):
    # Test results from different indexes do not mix (either JSON with JSON and JSON with HASH)
    env.expect('HSET', 'hash:1', 't', 'telmatosaurus', 'n', '9', 'f', '9.72').equal(3)
    env.execute_command('FT.CREATE', 'idxHash', 'ON', 'HASH', 'SCHEMA', 't', 'TEXT', 'n', 'NUMERIC', 'f', 'NUMERIC')
    waitForIndex(env, 'idxHash')
    env.execute_command('FT.CREATE', 'idxJson', 'ON', 'JSON', 'SCHEMA', '$.t', 'TEXT', '$.flt', 'NUMERIC')
    waitForIndex(env, 'idxJson')
    env.execute_command('JSON.SET', 'doc:1', '$', r'{"t":"riceratops","t2":"telmatosaurus","n":9072,"flt":97.2}')
    env.execute_command('FT.CREATE', 'idxJson2', 'ON', 'JSON', 'SCHEMA', '$.t2', 'TEXT', '$.flt', 'NUMERIC')
    waitForIndex(env, 'idxJson2')

    # FIXME: Probably a bug where HASH key is found when searching a JSON index
    env.expect('FT.SEARCH', 'idxJson', '*', 'RETURN', '3', '$.t', 'AS', 'txt').equal(
        [1L, 'doc:1', ['txt', '"riceratops"']])
    env.expect('FT.SEARCH', 'idxJson2', '*', 'RETURN', '3', '$.t2', 'AS', 'txt').equal(
        [1L, 'doc:1', ['txt', '"telmatosaurus"']])
    env.expect('FT.SEARCH', 'idxHash', '*', 'RETURN', '3', 't', 'AS', 'txt').equal(
        [1L, 'hash:1', ['txt', 'telmatosaurus']])


def testMapProjectionAsToSchemaAs(env):
    # Test that label defined in the schema can be used in the search query
    env.execute_command('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.t', 'AS', 'labelT', 'TEXT', '$.flt', 'AS',
                        'labelFlt', 'NUMERIC')
    waitForIndex(env, 'idx')
    env.execute_command('JSON.SET', 'doc:1', '$', r'{"t":"riceratops","n":"9072","flt":97.2}')

    env.expect('FT.SEARCH', 'idx', '*', 'RETURN', '1', 'labelT').equal(
        [1L, 'doc:1', ['labelT', '"riceratops"']])  # use $.t value


def testAsProjection(env):
    # Test RETURN and LOAD with label/alias from schema
    env.execute_command('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.t', 'TEXT', '$.flt', 'NUMERIC')
    waitForIndex(env, 'idx')
    env.execute_command('JSON.SET', 'doc:1', '$', r'{"t":"riceratops","n":"9072","flt":97.2, "sub":{"t":"rex"}}')

    # Test RETURN with label from schema
    env.expect('FT.SEARCH', 'idx', '*', 'RETURN', '3', '$.t', 'AS', 'txt').equal([1L, 'doc:1', ['txt', '"riceratops"']])
    # Test LOAD with label from schema
    env.expect('FT.AGGREGATE', 'idx', '*', 'LOAD', '3', '@$.t', 'AS', 'txt').equal([1L, ['txt', '"riceratops"']])

    # Test RETURN with label not from schema
    env.expect('FT.SEARCH', 'idx', '*', 'RETURN', '3', '$.n', 'AS', 'num').equal([1L, 'doc:1', ['num', '"9072"']])
    # FIXME:: enable next line - why not found?
    #env.expect('FT.SEARCH', 'idx', '907*', 'RETURN', '3', '$.n', 'AS', 'num').equal([1L, 'doc:1', ['num', '"9072"']])

    # Test LOAD with label not from schema
    env.expect('FT.AGGREGATE', 'idx', '*', 'LOAD', '6', '@$.n', 'AS', 'num', '$.sub.t', 'AS', 'subt').equal(
        [1L, ['num', '"9072"', 'subt', '"rex"']])
    # FIXME:: enable next line - why not found?
    # env.expect('FT.AGGREGATE', 'idx', '907*', 'LOAD', '3', '@$.n', 'AS', 'num').equal([1L, ['num', '"9072"']])

    # TODO: Search for numeric field 'flt'


def testAsProjectionRedefinedLabel(env):
    # Test redefining projection 'AS' label in query params RETURN and LOAD
    # FIXME: Should we fail SEARCH/AGGREGATE command with RETURN/LOAD alias duplication
    # (as with FT.CREATE)
    # BTW, iN SQLite, it is allowed, e.g., SELECT F1 AS Label1, F2 AS Label1 FROM doc;
    # (different values for fields F1 and F2 were retrieved with the same label Label1)

    # FIXME: Handle Numeric - In the following line, change '$.n' to: 'AS', 'labelN', 'NUMERIC'
    env.execute_command('FT.CREATE', 'idx2', 'ON', 'JSON', 'SCHEMA', '$.t', 'AS', 'labelT', 'TEXT', '$.n', 'AS',
                        'labelN', 'TEXT')
    waitForIndex(env, 'idx2')
    env.execute_command('JSON.SET', 'doc:1', '$', r'{"t":"riceratops","n":"9072"}')

    # Allow redefining a new label for a field which has a label in the schema
    env.expect('ft.search', 'idx2', '*', 'RETURN', '3', '$.t', 'AS', 'MyOnTheFlyReturnLabel').equal(
        [1L, 'doc:1', ['MyOnTheFlyReturnLabel', '"riceratops"']])
    env.expect('ft.aggregate', 'idx2', '*', 'LOAD', '3', '@$.t', 'AS', 'MyOnTheFlyReturnLabel').equal(
        [1L, ['MyOnTheFlyReturnLabel', '"riceratops"']])

    # Allow redefining a label with existing label found in another field in the schema
    env.expect('ft.search', 'idx2', '*', 'RETURN', '3', '$.t', 'AS', 'labelN').equal(
        [1L, 'doc:1', ['labelN', '"riceratops"']])
    env.expect('ft.aggregate', 'idx2', '*', 'LOAD', '3', '@$.t', 'AS', 'labelN').equal([1L, ['labelN', '"riceratops"']])

    # (?) Allow redefining a label with existing label found in another field in the schema, together with just a label from the schema
    env.expect('ft.search', 'idx2', '*', 'RETURN', '4', '$.n', 'AS', 'labelT', 'labelT').equal(
        [1L, 'doc:1', ['labelT', '"9072"']])
    env.expect('ft.aggregate', 'idx2', '*', 'LOAD', '4', '@$.n', 'AS', 'labelT', 'labelT').equal(
        [1L, ['labelT', '"9072"', 'labelT', '"riceratops"']])

    env.expect('ft.search', 'idx2', '*', 'RETURN', '4', '$.n', 'AS', 'labelT', 'labelN').equal(
        [1L, 'doc:1', ['labelT', '"9072"', 'labelN', '"9072"']])
    env.expect('ft.aggregate', 'idx2', '*', 'LOAD', '4', '@$.n', 'AS', 'labelT', 'labelN').equal(
        [1L, ['labelT', '"9072"', 'labelN', '"9072"']])

def testNumeric(env):
    env.execute_command('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.n', 'AS', 'n', 'NUMERIC', "$.f", 'AS', 'f', 'NUMERIC')
    env.execute_command('JSON.SET', 'doc:1', '$', r'{"n":9, "f":9.72}')
    env.expect('FT.SEARCH', 'idx', '*', 'RETURN', '3', '$.n', 'AS', 'int').equal([1L, 'doc:1', ['int', '9']])
    env.expect('FT.SEARCH', 'idx', '@n:[0 10]', 'RETURN', '3', '$.n', 'AS', 'int').equal([1L, 'doc:1', ['int', '9']])
    env.expect('FT.SEARCH', 'idx', '@f:[9.5 9.9]', 'RETURN', '1', 'f') \
        .equal([1L, 'doc:1', ['f', '9.72']])    
    env.expect('FT.SEARCH', 'idx', '@f:[9.5 9.9]', 'RETURN', '3', '$.f', 'AS', 'flt') \
        .equal([1L, 'doc:1', ['flt', '9.72']])

def testLanguage(env):
    if not UNSTABLE_TESTS:
        env.skip()
    # TODO: Check stemming? e.g., trad is stem of traduzioni and tradurre ?
    env.execute_command('FT.CREATE', 'idx', 'ON', 'JSON', 'LANGUAGE_FIELD', '$.lang', 'SCHEMA', '$.t', 'TEXT')
    env.execute_command('FT.CREATE', 'idx2', 'ON', 'JSON', 'LANGUAGE', 'Italian', 'SCHEMA', '$.domanda', 'TEXT')
    waitForIndex(env, 'idx')
    waitForIndex(env, 'idx2')

    env.execute_command('JSON.SET', 'doc:1', '$', r'{"t":"traduzioni", "lang":"Italian"}')
    env.expect('ft.search', 'idx', 'tradu*', 'RETURN', '1', '$.t' ).equal([1L, 'doc:1', ['$.t', '"traduzioni"']])

    env.execute_command('JSON.SET', 'doc:2', '$', r'{"domanda":"perché"}')
    env.expect('ft.search', 'idx2', 'per*', 'RETURN', '1', '$.domanda' ).equal([1L, 'doc:2', ['$.domanda', '"perch\xc3\xa9"']])

def testDifferentType(env):
    env.execute_command('FT.CREATE', 'hidx', 'ON', 'HASH', 'SCHEMA', '$.t', 'TEXT')
    env.execute_command('FT.CREATE', 'jidx', 'ON', 'JSON', 'SCHEMA', '$.t', 'TEXT')
    env.execute_command('HSET', 'doc:1', '$.t', 'hello world')
    env.execute_command('JSON.SET', 'doc:2', '$', r'{"t":"hello world"}')
    env.expect('FT.SEARCH', 'hidx', '*', 'NOCONTENT').equal([1L, 'doc:1'])
    env.expect('FT.SEARCH', 'jidx', '*', 'NOCONTENT').equal([1L, 'doc:2'])
