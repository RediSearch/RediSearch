# -*- coding: utf-8 -*-

import json
import bz2
import numpy as np

from common import *
from includes import *
from RLTest import Env


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
                "string_arr": ["a", "b", "c", "d", "e", "f", "gotcha6"],
                "vector": [1, 2, 3, 0.1, 0.2, 0.3]
            }'''


@no_msan
def testSearchUpdatedContent(env):
    conn = getConnectionByEnv(env)

    # TODO: test when rejson module is loaded after search
    # TODO: test when rejson module is loaded before search
    # TODO: test when rejson module is not loaded (fail gracefully with error messages)

    # Set a value before index is defined
    plain_val_1_raw = r'{"t":"rex","n":12}'
    plain_val_1 = '['+plain_val_1_raw+']'
    res = conn.execute_command('json.get', 'doc:1', '$')
    env.assertEqual(res, None)
    conn.execute_command('json.set', 'doc:1', '$', plain_val_1_raw)
    res = conn.execute_command('json.get', 'doc:1', '$')
    env.assertEqual(json.loads(res), json.loads(plain_val_1))
    res = conn.execute_command('json.get', 'doc:1', '.')
    env.assertEqual(json.loads(res), json.loads(plain_val_1_raw))

    # Index creation
    env.execute_command('FT.CREATE', 'idx1', 'ON', 'JSON', 'SCHEMA',
                        '$.t', 'AS', 'labelT', 'TEXT', '$.n', 'AS', 'labelN', 'NUMERIC')
    waitForIndex(env, 'idx1')

    # No results before ingestion
    env.expect('ft.search', 'idx1', 'rice*').equal([0])

    # Set another value after index was defined
    plain_val_2_raw = r'{"t":"riceratops","n":9}'
    plain_val_2 = '[' + plain_val_2_raw + ']'

    conn.execute_command('json.set', 'doc:2', '$', plain_val_2_raw)
    res = conn.execute_command('json.get', 'doc:2', '$')
    env.assertEqual(json.loads(res), json.loads(plain_val_2))
    res = conn.execute_command('json.get', 'doc:2', '.')
    env.assertEqual(json.loads(res), json.loads(plain_val_2_raw))
    res = conn.execute_command('json.get', 'doc:2', '$.n')
    env.assertEqual(res, '[9]')
    res = conn.execute_command('json.get', 'doc:2', '.n')
    env.assertEqual(res, '9')
    res = conn.execute_command('json.get', 'doc:2', '$.t')
    env.assertEqual(res, '["riceratops"]')
    res = conn.execute_command('json.get', 'doc:2', '.t')
    env.assertEqual(res, '"riceratops"')

    # Test updated values are found
    expected = [2, 'doc:1', ['$', json.loads(plain_val_1_raw)], 'doc:2', ['$', json.loads(plain_val_2_raw)]]
    res = env.cmd('ft.search', 'idx1', '*')
    res[2][1] = json.loads(res[2][1])
    res[4][1] = json.loads(res[4][1])
    env.assertEqual(res, expected)

    expected = [1, 'doc:1', ['$', json.loads(plain_val_1_raw)]]
    res = env.cmd('ft.search', 'idx1', 're*')
    res[2][1] = json.loads(res[2][1])
    env.assertEqual(res, expected)

    # TODO: Why does the following result look like that? (1 count and 2 arrays of result pairs)
    res = env.execute_command('ft.aggregate', 'idx1', '*', 'LOAD', '1', 'labelT')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList([1, ['labelT', 'rex'], ['labelT', 'riceratops']]))
    env.expect('ft.aggregate', 'idx1', 're*', 'LOAD', '1', 'labelT').equal([1, ['labelT', 'rex']])

    res = env.execute_command('ft.aggregate', 'idx1', '*', 'LOAD', '1', 'labelT')

    # Update an existing text value
    plain_text_val_3_raw = '"hescelosaurus"'
    plain_text_val_3 = '[' +plain_text_val_3_raw + ']'

    env.expect('json.set', 'doc:1', '$.t', plain_text_val_3_raw).ok()
    env.expect('json.get', 'doc:1', '$.t').equal(plain_text_val_3)

    # Update an existing int value
    plain_int_val_3 = '13'
    int_incrby_3 = '2'
    plain_int_res_val_3 = str(int(plain_int_val_3) + int(int_incrby_3))
    env.expect('json.set', 'doc:1', '$.n', plain_int_val_3).ok()
    # test JSON.NUMINCRBY
    env.expect('json.numincrby', 'doc:1', '$.n', int_incrby_3).equal('[' + plain_int_res_val_3 + ']')

    expected = [1, 'doc:1', ['$', json.loads(r'{"t":"hescelosaurus","n":' + plain_int_res_val_3 + '}')]]
    res = env.cmd('ft.search', 'idx1', 'he*')
    res[2][1] = json.loads(res[2][1])
    env.assertEqual(res, expected)

    expected = [1, 'doc:2', ['$', json.loads('{"t":"riceratops","n":9}')]]
    res = env.cmd('ft.search', 'idx1', 'riceratops', 'RETURN', '1', '$')
    res[2][1] = json.loads(res[2][1])
    env.assertEqual(res, expected)

    env.expect('ft.search', 'idx1', 'riceratops', 'RETURN', '1', '$.n').equal([1, 'doc:2', ['$.n', '9']])
    env.expect('ft.search', 'idx1', 'riceratops', 'RETURN', '1', '$.t').equal([1, 'doc:2', ['$.t', 'riceratops']])


# FIXME: Test PREFIX, SORTBY, NOSTEM, Fuzzy, Pagination, Limit 0 0, Score - Need to repeat all search testing as done on hash?
# FIXME: Test Aggregate - Need to repeat all aggregate testing as done on hash?

# TODO: Check null values
# TODO: Check arrays
# TODO: Check Object/Map

@skip
def testHandleUnindexedTypes(env):
    # TODO: Ignore and resume indexing when encountering an Object/Array/null
    # TODO: Except for array of only scalars which is defined as a TAG in the schema
    # ... FT.CREATE idx SCHEMA $.arr TAG

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
                        '$.vector', 'AS', 'vec', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2'
                        ).ok()
    waitForIndex(env, 'idx')
    # FIXME: Why does the following search return zero results?
    env.expect('ft.search', 'idx', '*', 'RETURN', '2', 'string', 'int_arr')\
        .equal([1, 'doc:1', ['string', '"gotcha1"', 'int_arr', ["a", "b", "c", "d", "e", "f", "gotcha6"]]])

    # TODO: test TAGVALS ?
    pass

@no_msan
def testReturnAllTypes(env):
    # Test returning all JSON types
    # (even if some of them are not able to be indexed/found,
    # they can be returned together with other fields which are indexed)

    env.expect('JSON.SET', 'doc:1', '$', doc1_content).ok()
    env.execute_command('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.string', 'AS', 'string', 'TEXT')

    # TODO: Make sure TAG can be used as a label in "FT.SEARCH idx "*" RETURN $.t As Tag"
    pass

@no_msan
def testOldJsonPathSyntax(env):
    # Make sure root path '.' is working
    # For example, '$.t' should also work as '.t' and 't'
    pass

@no_msan
def testNoContent(env):
    # Test NOCONTENT
    env.execute_command('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.t', 'TEXT', '$.flt', 'NUMERIC')
    env.execute_command('JSON.SET', 'doc:1', '$', r'{"t":"riceratops","n":"9072","flt":97.2}')
    env.expect('ft.search', 'idx', 're*', 'NOCONTENT').equal([0])
    env.expect('ft.search', 'idx', 'ri*', 'NOCONTENT').equal([1, 'doc:1'])

@no_msan
def testDocNoFullSchema(env):
    # Test NOCONTENT
    env.cmd('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.t1', 'TEXT', '$.t2', 'TEXT')
    env.cmd('JSON.SET', 'doc:1', '$', r'{"t1":"riceratops"}')
    env.expect('ft.search', 'idx', 're*', 'NOCONTENT').equal([0])
    env.expect('ft.search', 'idx', 'ri*', 'NOCONTENT').equal([1, 'doc:1'])

@no_msan
def testReturnRoot(env):
    # Test NOCONTENT
    env.execute_command('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.t', 'TEXT')
    env.execute_command('JSON.SET', 'doc:1', '$', r'{"t":"foo"}')
    env.expect('ft.search', 'idx', 'foo', 'RETURN', '1', '$').equal([1, 'doc:1', ['$', '{"t":"foo"}']])

@no_msan
def testNonEnglish(env):
    # Test json in non-English languages
    env.execute_command('FT.CREATE', 'idx1', 'ON', 'JSON', 'SCHEMA', '$.t', 'AS', 'labelT', 'TEXT', '$.n', 'AS',
                        'labelN', 'NUMERIC')
    japanese_value_1 = 'ドラゴン'
    japanese_doc_value_raw = r'{"t":"' + japanese_value_1 + r'","n":5}'
    japanese_doc_value = [ json.loads(japanese_doc_value_raw) ]

    env.expect('json.set', 'doc:4', '$', japanese_doc_value_raw).ok()
    env.assertEqual(json.loads(env.cmd('json.get', 'doc:4', '$')), japanese_doc_value)
    env.assertEqual(json.loads(env.cmd('json.get', 'doc:4', '.')), json.loads(japanese_doc_value_raw))
    env.expect('json.get', 'doc:4', '$.t').equal('["' + japanese_value_1 + '"]')
    env.expect('json.get', 'doc:4', '.t').equal('"' + japanese_value_1 + '"')

    chinese_value_1_raw = r'{"t":"踪迹","n":5}'
    chinese_value_1 = [ json.loads(chinese_value_1_raw)]
    env.expect('json.set', 'doc:5', '$', chinese_value_1_raw).ok()
    env.assertEqual(json.loads(env.cmd('json.get', 'doc:5', '$')), chinese_value_1)
    env.assertEqual(json.loads(env.cmd('json.get', 'doc:5', '.')), json.loads(chinese_value_1_raw))

    env.expect('ft.search', 'idx1', '*', 'RETURN', '3', '$.t', 'AS', 'MyReturnLabel') \
        .equal([2,
                'doc:4', ['MyReturnLabel', 'ドラゴン'],
                'doc:5', ['MyReturnLabel', '踪迹']])

@no_msan
def testSet(env):
    # JSON.SET (either set the entire key or a sub-value)
    # Can also do multiple changes/side-effects, such as converting an object to a scalar
    env.execute_command('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.t', 'TEXT')
    env.execute_command('JSON.SET', 'doc:1', '$', r'{"t":"ReJSON"}')

    res = [1, 'doc:1', ['$', '{"t":"ReJSON"}']]
    env.expect('ft.search', 'idx', 'rejson').equal(res)
    env.expect('ft.search', 'idx', 'ReJSON').equal(res)
    env.expect('ft.search', 'idx', 're*').equal(res)
    env.expect('ft.search', 'idx', 're*', 'NOCONTENT').equal([1, 'doc:1'])

@no_msan
def testDel(env):
    conn = getConnectionByEnv(env)

    # JSON.DEL and JSON.FORGET
    env.execute_command('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.t', 'TEXT')
    conn.execute_command('JSON.SET', 'doc:1', '$', r'{"t":"ReJSON"}')
    conn.execute_command('JSON.SET', 'doc:2', '$', r'{"t":"RediSearch"}')
    env.expect('ft.search', 'idx', 're*', 'NOCONTENT').equal([2, 'doc:1', 'doc:2'])
    res = conn.execute_command('JSON.DEL', 'doc:2', '$.t')
    env.assertEqual(res, 1)
    env.expect('ft.search', 'idx', 're*', 'NOCONTENT').equal([1, 'doc:1'])
    res = conn.execute_command('JSON.FORGET', 'doc:1', '$.t')
    env.assertEqual(res, 1)
    env.expect('ft.search', 'idx', 're*', 'NOCONTENT').equal([0])

@no_msan
def testToggle(env):
    # JSON.TOGGLE
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
               '$.boolT', 'AS', 'boolT', 'TAG').ok()
    env.expect('JSON.SET', 'doc:1', '$', r'{"boolT":false}').ok()
    env.expect('ft.search', 'idx', '*').equal([1, 'doc:1', ['$', '{"boolT":false}']])
    env.expect('JSON.TOGGLE','doc:1','$.boolT').equal([1])
    env.expect('ft.search', 'idx', '*').equal([1, 'doc:1', ['$', '{"boolT":true}']])

@no_msan
def testStrappend(env):
    # JSON.STRAPPEND

    env.execute_command('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.t', 'TEXT')
    env.execute_command('JSON.SET', 'doc:1', '$', r'{"t":"Redis"}')
    env.expect('json.get', 'doc:1', '$').equal('[{"t":"Redis"}]')
    env.expect('ft.search', 'idx', '*').equal([1, 'doc:1', ['$', '{"t":"Redis"}']])
    env.expect('ft.search', 'idx', 'Redis').equal([1, 'doc:1', ['$', '{"t":"Redis"}']])
    env.execute_command('JSON.STRAPPEND', 'doc:1', '.t', '"Labs"')
    env.expect('json.get', 'doc:1', '$').equal('[{"t":"RedisLabs"}]')
    env.expect('ft.search', 'idx', '*').equal([1, 'doc:1', ['$', '{"t":"RedisLabs"}']])
    env.expect('ft.search', 'idx', 'RedisLabs').equal([1, 'doc:1', ['$', '{"t":"RedisLabs"}']])
    env.expect('ft.search', 'idx', 'Redis').equal([0])

@no_msan
def testArrayCommands(env):
    conn = getConnectionByEnv(env)
    env.execute_command('FT.CREATE', 'idx', 'ON', 'JSON',
                        'SCHEMA', '$.tag[*]', 'AS', 'tag', 'TAG')

    env.assertOk(conn.execute_command('JSON.SET', 'doc:1', '$', '{"tag":["foo"]}'))
    env.assertEqual(conn.execute_command('JSON.ARRAPPEND', 'doc:1', '$.tag', '"bar"'), [2])
    env.assertEqual(conn.execute_command('JSON.GET', 'doc:1', '$.tag[*]'), '["foo","bar"]')
    env.assertEqual(conn.execute_command('JSON.ARRLEN', 'doc:1', '$.tag'), [2])
    res = [1, 'doc:1', ['$', '{"tag":["foo","bar"]}']]
    env.expect('FT.SEARCH', 'idx', '@tag:{foo}').equal(res)
    env.expect('FT.SEARCH', 'idx', '@tag:{bar}').equal(res)
    env.expect('FT.SEARCH', 'idx', '@tag:{baz}').equal([0])

    # use JSON.ARRINSERT
    env.assertEqual(conn.execute_command('JSON.ARRINSERT', 'doc:1', '$.tag', '2', '"baz"'), [3])
    env.assertEqual(conn.execute_command('JSON.GET', 'doc:1', '$.tag[*]'), '["foo","bar","baz"]')
    env.assertEqual(conn.execute_command('JSON.ARRLEN', 'doc:1', '$.tag'), [3])
    res = [1, 'doc:1', ['$', '{"tag":["foo","bar","baz"]}']]
    env.expect('FT.SEARCH', 'idx', '@tag:{foo}').equal(res)
    env.expect('FT.SEARCH', 'idx', '@tag:{bar}').equal(res)
    env.expect('FT.SEARCH', 'idx', '@tag:{baz}').equal(res)

    # use JSON.ARRPOP
    env.assertEqual(conn.execute_command('JSON.ARRPOP', 'doc:1', '$.tag', '1'), ['"bar"'])
    env.assertEqual(conn.execute_command('JSON.GET', 'doc:1', '$.tag[*]'), '["foo","baz"]')
    env.assertEqual(conn.execute_command('JSON.ARRLEN', 'doc:1', '$.tag'), [2])
    res = [1, 'doc:1', ['$', '{"tag":["foo","baz"]}']]
    env.expect('FT.SEARCH', 'idx', '@tag:{foo}').equal(res)
    env.expect('FT.SEARCH', 'idx', '@tag:{bar}').equal([0])
    env.expect('FT.SEARCH', 'idx', '@tag:{baz}').equal(res)

    # use JSON.ARRTRIM
    env.assertEqual(conn.execute_command('JSON.ARRINSERT', 'doc:1', '$.tag', '0', '"1"'), [3])
    env.assertEqual(conn.execute_command('JSON.ARRINSERT', 'doc:1', '$.tag', '0', '"2"'), [4])
    env.assertEqual(conn.execute_command('JSON.ARRAPPEND', 'doc:1', '$.tag', '"3"', '"4"'), [6])
    env.assertEqual(conn.execute_command('JSON.ARRLEN', 'doc:1', '$.tag'), [6])

    env.assertEqual(conn.execute_command('JSON.ARRTRIM', 'doc:1', '$.tag', '2', '3'), [2])
    env.assertEqual(conn.execute_command('JSON.GET', 'doc:1', '$.tag[*]'), '["foo","baz"]')
    env.assertEqual(conn.execute_command('JSON.ARRLEN', 'doc:1', '$.tag'), [2])
    env.expect('FT.SEARCH', 'idx', '@tag:{1}').equal([0])
    env.expect('FT.SEARCH', 'idx', '@tag:{foo}').equal(res)
    env.expect('FT.SEARCH', 'idx', '@tag:{baz}').equal(res)

@no_msan
def testArrayCommands_withVector(env):
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    dim = 2

    for data_type in ['FLOAT32', 'FLOAT64']:
        conn.execute_command('FT.CREATE', 'idx', 'ON', 'JSON',
                            'SCHEMA', '$.v', 'AS', 'vec', 'VECTOR', 'FLAT', '6', 'TYPE', data_type, 'DIM', dim, 'DISTANCE_METRIC', 'L2')

        env.assertOk(conn.execute_command('JSON.SET', 'doc:1', '$', '{"v":[1]}'))
        env.assertEqual(conn.execute_command('JSON.ARRAPPEND', 'doc:1', '$.v', '2'), [2])
        env.assertEqual(conn.execute_command('JSON.GET', 'doc:1', '$.v'), '[[1,2]]')
        env.assertEqual(conn.execute_command('JSON.GET', 'doc:1', '$.v[*]'), '[1,2]')
        env.assertEqual(conn.execute_command('JSON.ARRLEN', 'doc:1', '$.v'), [2])
        res = [1, 'doc:1', ['$', '{"v":[1,2]}']]
        waitForIndex(env, 'idx')
        env.expect('FT.SEARCH', 'idx', '*').equal(res)
        query_vec = create_np_array_typed([1]*dim, data_type)
        env.expect('FT.SEARCH', 'idx', '*=>[KNN 1 @vec $B]', 'PARAMS', '2', 'B', query_vec.tobytes(), 'RETURN', '1', '$').equal(res)

        # use JSON.ARRINSERT
        env.assertEqual(conn.execute_command('JSON.ARRINSERT', 'doc:1', '$.v', '2', '3'), [3])
        env.assertEqual(conn.execute_command('JSON.GET', 'doc:1', '$.v[*]'), '[1,2,3]')
        env.assertEqual(conn.execute_command('JSON.ARRLEN', 'doc:1', '$.v'), [3])
        waitForIndex(env, 'idx')
        # Index should be empty as the vector length doesn't match the dimension of the field.
        env.expect('FT.SEARCH', 'idx', '*').equal([0])
        env.expect('FT.SEARCH', 'idx', '*=>[KNN 1 @vec $B]', 'PARAMS', '2', 'B', query_vec.tobytes(), 'RETURN', '1', '$').equal([0])

        # use JSON.ARRPOP
        env.assertEqual(conn.execute_command('JSON.ARRPOP', 'doc:1', '$.v', '1'), ['2'])
        env.assertEqual(conn.execute_command('JSON.GET', 'doc:1', '$.v[*]'), '[1,3]')
        env.assertEqual(conn.execute_command('JSON.ARRLEN', 'doc:1', '$.v'), [2])
        res = [1, 'doc:1', ['$', '{"v":[1,3]}']]
        waitForIndex(env, 'idx')
        # Index should have one doc as the vector length now matches the dimension of the field.
        env.expect('FT.SEARCH', 'idx', '*').equal(res)
        env.expect('FT.SEARCH', 'idx', '*=>[KNN 1 @vec $B]', 'PARAMS', '2', 'B', query_vec.tobytes(), 'RETURN', '1', '$').equal(res)

        # use JSON.ARRTRIM
        env.assertEqual(conn.execute_command('JSON.ARRINSERT', 'doc:1', '$.v', '0', '"a"'), [3])
        env.assertEqual(conn.execute_command('JSON.ARRINSERT', 'doc:1', '$.v', '0', '"b"'), [4])
        env.assertEqual(conn.execute_command('JSON.ARRAPPEND', 'doc:1', '$.v', '"c"', '"d"'), [6])
        env.assertEqual(conn.execute_command('JSON.ARRLEN', 'doc:1', '$.v'), [6])
        waitForIndex(env, 'idx')
        # Index should be empty again as the vector length doesn't match the dimension of the field.
        env.expect('FT.SEARCH', 'idx', '*').equal([0])
        env.expect('FT.SEARCH', 'idx', '*=>[KNN 1 @vec $B]', 'PARAMS', '2', 'B', query_vec.tobytes(), 'RETURN', '1', '$').equal([0])

        env.assertEqual(conn.execute_command('JSON.ARRTRIM', 'doc:1', '$.v', '2', '3'), [2])
        env.assertEqual(conn.execute_command('JSON.GET', 'doc:1', '$.v[*]'), '[1,3]')
        env.assertEqual(conn.execute_command('JSON.ARRLEN', 'doc:1', '$.v'), [2])
        waitForIndex(env, 'idx')
        # Index should have one doc again as the vector length now matches the dimension of the field.
        env.expect('FT.SEARCH', 'idx', '*').equal(res)
        env.expect('FT.SEARCH', 'idx', '*=>[KNN 1 @vec $B]', 'PARAMS', '2', 'B', query_vec.tobytes(), 'RETURN', '1', '$').equal(res)

        env.assertEqual(conn.execute_command('JSON.NUMINCRBY', 'doc:1', '$.v[0]', '1'), '[2]')
        env.assertEqual(conn.execute_command('JSON.GET', 'doc:1', '$.v[*]'), '[2,3]')
        env.assertEqual(conn.execute_command('JSON.ARRLEN', 'doc:1', '$.v'), [2])
        res = [1, 'doc:1', ['$', '{"v":[2,3]}']]
        waitForIndex(env, 'idx')
        # Index should have one doc, and its vector should be updated.
        env.expect('FT.SEARCH', 'idx', '*').equal(res)
        env.expect('FT.SEARCH', 'idx', '*=>[KNN 1 @vec $B]', 'PARAMS', '2', 'B', query_vec.tobytes(), 'RETURN', '1', '$').equal(res)

        env.assertEqual(conn.execute_command('JSON.SET', 'doc:1', '$.v[1]', 'true'), 'OK')
        env.assertEqual(conn.execute_command('JSON.GET', 'doc:1', '$.v[*]'), '[2,true]')
        waitForIndex(env, 'idx')
        # Index should be empty as some of the vector's elements are not numeric.
        env.expect('FT.SEARCH', 'idx', '*').equal([0])
        env.expect('FT.SEARCH', 'idx', '*=>[KNN 1 @vec $B]', 'PARAMS', '2', 'B', query_vec.tobytes(), 'RETURN', '1', '$').equal([0])

        conn.execute_command('FT.DROPINDEX', 'idx', 'DD')

@no_msan
def testRootValues(env):
    # Search all JSON types as a top-level element
    # FIXME:
    pass

@no_msan
def testAsTag(env):
    res = env.execute_command('FT.CREATE', 'idx', 'ON', 'JSON',
                              'SCHEMA', '$.tag', 'AS', 'tag', 'TAG', 'SEPARATOR', ',')

    env.expect('JSON.SET', 'doc:1', '$', '{"tag":"foo,bar,baz"}').ok()

    env.expect('JSON.GET', 'doc:1', '$').equal('[{"tag":"foo,bar,baz"}]')
    env.expect('JSON.GET', 'doc:1', '$.tag').equal('["foo,bar,baz"]')

    res = [1, 'doc:1', ['$', '{"tag":"foo,bar,baz"}']]
    env.expect('FT.SEARCH', 'idx', '@tag:{foo}').equal(res)
    env.expect('FT.SEARCH', 'idx', '@tag:{bar}').equal(res)
    env.expect('FT.SEARCH', 'idx', '@tag:{baz}').equal(res)

    env.expect('FT.SEARCH', 'idx', '@tag:{foo\\,bar\\,baz}').equal([0])

@no_msan
def testMultiValueTag(env):
    conn = getConnectionByEnv(env)

    # Index with Tag for array with multi-values
    res = env.execute_command('FT.CREATE', 'idx', 'ON', 'JSON',
                              'SCHEMA', '$.tag[*]', 'AS', 'tag', 'TAG', 'SEPARATOR', ',')

    # multivalue without a separator
    #
    env.assertOk(conn.execute_command('JSON.SET', 'doc:1', '$', '{"tag":["foo", "bar", "baz"]}'))
    env.assertOk(conn.execute_command('JSON.SET', 'doc:2', '$', '{"tag":["foo, bar", "baz"]}'))
    env.assertOk(conn.execute_command('JSON.SET', 'doc:3', '$', '{"tag":["foo, bar, baz"]}'))

    env.assertEqual(conn.execute_command('JSON.GET', 'doc:1', '$'), '[{"tag":["foo","bar","baz"]}]')
    env.assertEqual(conn.execute_command('JSON.GET', 'doc:1', '$.tag'), '[["foo","bar","baz"]]')
    env.assertEqual(conn.execute_command('JSON.GET', 'doc:1', '$.tag[*]'), '["foo","bar","baz"]')

    env.assertEqual(conn.execute_command('JSON.GET', 'doc:2', '$'), '[{"tag":["foo, bar","baz"]}]')
    env.assertEqual(conn.execute_command('JSON.GET', 'doc:2', '$.tag'), '[["foo, bar","baz"]]')
    env.assertEqual(conn.execute_command('JSON.GET', 'doc:2', '$.tag[*]'), '["foo, bar","baz"]')

    env.assertEqual(conn.execute_command('JSON.GET', 'doc:3', '$'), '[{"tag":["foo, bar, baz"]}]')
    env.assertEqual(conn.execute_command('JSON.GET', 'doc:3', '$.tag'), '[["foo, bar, baz"]]')
    env.assertEqual(conn.execute_command('JSON.GET', 'doc:3', '$.tag[*]'), '["foo, bar, baz"]')

    res = [3, 'doc:1', ['$', '{"tag":["foo","bar","baz"]}'],
               'doc:2', ['$', '{"tag":["foo, bar","baz"]}'],
               'doc:3', ['$', '{"tag":["foo, bar, baz"]}']]
    env.expect('FT.SEARCH', 'idx', '@tag:{foo}').equal(res)
    env.expect('FT.SEARCH', 'idx', '@tag:{bar}').equal(res)
    env.expect('FT.SEARCH', 'idx', '@tag:{baz}').equal(res)
    env.expect('FT.SEARCH', 'idx', '@tag:{foo/,bar/,baz}').equal([0])

@no_msan
def testMultiValueTag_Recursive_Decent(env):
    conn = getConnectionByEnv(env)
    env.execute_command('FT.CREATE', 'idx', 'ON', 'JSON',
                        'SCHEMA', '$..name', 'AS', 'name', 'TAG')
    conn.execute_command('JSON.SET', 'doc:1', '$', '{"name":"foo", "in" : {"name":"bar"}}')

    res = [1, 'doc:1', ['$', '{"name":"foo","in":{"name":"bar"}}']]
    env.expect('FT.SEARCH', 'idx', '@name:{foo}').equal(res)
    env.expect('FT.SEARCH', 'idx', '@name:{bar}').equal(res)

@no_msan
def testMultiValueErrors(env):
    # Multi-value is unsupported with the following
    env.execute_command('FT.CREATE', 'idxvector', 'ON', 'JSON',
                        'SCHEMA', '$.vec', 'AS', 'vec', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '3','DISTANCE_METRIC', 'L2')

    env.expect('JSON.SET', 'doc:1', '$', '{"text":["foo, bar","baz"],                       \
                                           "num":[1,2,3,3.14],                              \
                                           "vec":[[1],[2,3],[3.14]],                              \
                                           "geo":["1.234, 4.321", "0.123, 3.210"]}').ok()

    # test non-tag non-text indexes fail to index multivalue
    indexes = ['idxvector']
    for index in indexes:
        res_actual = env.cmd('FT.INFO', index)
        res_actual = {res_actual[i]: res_actual[i + 1] for i in range(0, len(res_actual), 2)}
        env.assertEqual(str(res_actual['hash_indexing_failures']), '1')

@no_msan
def add_values(env, number_of_iterations=1):
    res = env.execute_command('FT.CREATE', 'games', 'ON', 'JSON',
                              'SCHEMA', '$.title', 'TEXT', 'SORTABLE',
                              '$.brand', 'TEXT', 'NOSTEM', 'SORTABLE',
                              )  # ,'$.description', 'AS', 'description', 'TEXT', 'price', 'NUMERIC',
    # 'categories', 'TAG')

    conn = getConnectionByEnv(env)
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
            conn.execute_command(*cmd)
        fp.close()

@no_msan
def testAggregate(env):
    add_values(env)

    cmd = ['ft.aggregate', 'games', '*',
           'GROUPBY', '1', '@$.brand',
           'REDUCE', 'count', '0', 'AS', 'count',
           'SORTBY', 2, '@count', 'desc',
           'LIMIT', '0', '5'
           ]
    env.expect(*cmd).equal([292, ['$.brand', '', 'count', '1518'],
                                  ['$.brand', 'Mad Catz', 'count', '43'],
                                  ['$.brand', 'Generic', 'count', '40'],
                                  ['$.brand', 'SteelSeries', 'count', '37'],
                                  ['$.brand', 'Logitech', 'count', '35']])
    # FIXME: Test FT.AGGREGATE params - or alternatively reuse test_aggregate.py to also run on json content

@no_msan
def testDemo(env):
    conn = getConnectionByEnv(env)

    # Set a value before index is defined
    tlv = r'{"iata":"TLV","name":"Ben Gurion International Airport","location":"34.8866997,32.01139832"}'
    sfo = r'{"iata":"SFO","name":"San Francisco International Airport","location":"-122.375,37.6189995"}'
    tlv_doc = [1, 'A:TLV', ['$', json.loads(tlv)]]
    sfo_doc = [1, 'A:SFO', ['$', json.loads(sfo)]]

    conn.execute_command('json.set', 'A:TLV', '$', tlv)
    conn.execute_command('json.set', 'A:SFO', '$', sfo)

    env.expect('FT.CREATE airports ON JSON SCHEMA $.iata AS iata TAG                          \
                                                  $.iata AS iata_txt TEXT NOSTEM              \
                                                  $.name AS name TEXT NOSTEM PHONETIC dm:en   \
                                                  $.location AS location GEO').ok()

    conn.execute_command('json.set', 'A:TLV', '$', tlv)
    conn.execute_command('json.set', 'A:SFO', '$', sfo)

    info = env.cmd('FT.INFO airports')
    env.assertEqual(slice_at(info, 'index_name')[0], 'airports')
    env.assertEqual(slice_at(slice_at(info, 'index_definition')[0], 'key_type')[0], 'JSON')
    env.assertEqual(slice_at(info, 'attributes')[0],
        [['identifier', '$.iata', 'attribute', 'iata', 'type', 'TAG', 'SEPARATOR', ''],
         ['identifier', '$.iata', 'attribute', 'iata_txt', 'type', 'TEXT', 'WEIGHT', '1', 'NOSTEM'],
         ['identifier', '$.name', 'attribute', 'name', 'type', 'TEXT', 'WEIGHT', '1', 'NOSTEM'],
         ['identifier', '$.location', 'attribute', 'location', 'type', 'GEO']])
    env.assertEqual(int(slice_at(info, 'num_docs')[0]), 2)

    res = env.cmd('FT.SEARCH', 'airports', 'TLV')
    res[2][1] = json.loads(res[2][1])
    env.assertEqual(res, tlv_doc)

    res = env.cmd('FT.SEARCH', 'airports', 'TL*')
    res[2][1] = json.loads(res[2][1])
    env.assertEqual(res, tlv_doc)

    res = env.cmd('FT.SEARCH', 'airports', 'sen frensysclo')
    res[2][1] = json.loads(res[2][1])
    env.assertEqual(res, sfo_doc)

    res = env.cmd('FT.SEARCH', 'airports', '@location:[-122.41 37.77 100 km]')
    res[2][1] = json.loads(res[2][1])
    env.assertEqual(res, sfo_doc)

    env.expect('FT.SEARCH', 'airports', 'sfo', 'RETURN', '1', '$.name') \
        .equal([1, 'A:SFO', ['$.name', 'San Francisco International Airport']])

    expected_res = [1, ['iata', 'SFO', '$', '{"iata":"SFO","name":"San Francisco International Airport","location":"-122.375,37.6189995"}']]
    res = env.cmd('FT.AGGREGATE', 'airports', 'sfo', 'LOAD', '1', '$', 'SORTBY', '1', '@iata')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList(expected_res))

    res =env.cmd('FT.AGGREGATE', 'airports', 'sfo', 'SORTBY', '1', '@iata', 'LOAD', '1', '$')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList(expected_res))

@no_msan
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
        [1, 'doc:1', ['txt', 'riceratops']])
    env.expect('FT.SEARCH', 'idxJson2', '*', 'RETURN', '3', '$.t2', 'AS', 'txt').equal(
        [1, 'doc:1', ['txt', 'telmatosaurus']])
    env.expect('FT.SEARCH', 'idxHash', '*', 'RETURN', '3', 't', 'AS', 'txt').equal(
        [1, 'hash:1', ['txt', 'telmatosaurus']])

@no_msan
def testMapProjectionAsToSchemaAs(env):
    # Test that label defined in the schema can be used in the search query
    env.execute_command('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.t', 'AS', 'labelT', 'TEXT', '$.flt', 'AS',
                        'labelFlt', 'NUMERIC')
    env.execute_command('JSON.SET', 'doc:1', '$', r'{"t":"riceratops","n":"9072","flt":97.2}')

    env.expect('FT.SEARCH', 'idx', '*', 'RETURN', '1', 'labelT').equal(
        [1, 'doc:1', ['labelT', 'riceratops']])  # use $.t value

@no_msan
def testAsProjection(env):
    # Test RETURN and LOAD with label/alias from schema
    env.execute_command('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.t', 'TEXT', '$.flt', 'NUMERIC')
    env.execute_command('JSON.SET', 'doc:1', '$', r'{"t":"riceratops","n":"9072","flt":97.2, "sub":{"t":"rex"}}')

    # Test RETURN with label from schema
    env.expect('FT.SEARCH', 'idx', '*', 'RETURN', '3', '$.t', 'AS', 'txt').equal([1, 'doc:1', ['txt', 'riceratops']])
    # Test LOAD with label from schema
    env.expect('FT.AGGREGATE', 'idx', '*', 'LOAD', '3', '@$.t', 'AS', 'txt').equal([1, ['txt', 'riceratops']])

    # Test RETURN with label not from schema
    env.expect('FT.SEARCH', 'idx', '*', 'RETURN', '3', '$.n', 'AS', 'num').equal([1, 'doc:1', ['num', '9072']])
    # FIXME:: enable next line - why not found?
    #env.expect('FT.SEARCH', 'idx', '907*', 'RETURN', '3', '$.n', 'AS', 'num').equal([1, 'doc:1', ['num', '"9072"']])

    # Test LOAD with label not from schema
    env.expect('FT.AGGREGATE', 'idx', '*', 'LOAD', '6', '@$.n', 'AS', 'num', '$.sub.t', 'AS', 'subt').equal(
        [1, ['num', '9072', 'subt', 'rex']])
    # FIXME:: enable next line - why not found?
    # env.expect('FT.AGGREGATE', 'idx', '907*', 'LOAD', '3', '@$.n', 'AS', 'num').equal([1, ['num', '"9072"']])

    # TODO: Search for numeric field 'flt'

@no_msan
def testAsProjectionRedefinedLabel(env):
    conn = getConnectionByEnv(env)

    # Test redefining projection 'AS' label in query params RETURN and LOAD
    # FIXME: Should we fail SEARCH/AGGREGATE command with RETURN/LOAD alias duplication
    # (as with FT.CREATE)
    # BTW, iN SQLite, it is allowed, e.g., SELECT F1 AS Label1, F2 AS Label1 FROM doc;
    # (different values for fields F1 and F2 were retrieved with the same label Label1)

    # FIXME: Handle Numeric - In the following line, change '$.n' to: 'AS', 'labelN', 'NUMERIC'
    env.execute_command('FT.CREATE', 'idx2', 'ON', 'JSON', 'SCHEMA',
                        '$.t', 'AS', 'labelT', 'TEXT', '$.n', 'AS', 'labelN', 'TEXT')
    conn.execute_command('JSON.SET', 'doc:1', '$', r'{"t":"riceratops","n":"9072"}')

    # Allow redefining a new label for a field which has a label in the schema
    env.expect('ft.search', 'idx2', '*', 'RETURN', '3', '$.t', 'AS', 'MyOnTheFlyReturnLabel').equal(
        [1, 'doc:1', ['MyOnTheFlyReturnLabel', 'riceratops']])
    env.expect('ft.aggregate', 'idx2', '*', 'LOAD', '3', '@$.t', 'AS', 'MyOnTheFlyReturnLabel').equal(
        [1, ['MyOnTheFlyReturnLabel', 'riceratops']])

    # Allow redefining a label with existing label found in another field in the schema
    env.expect('ft.search', 'idx2', '*', 'RETURN', '3', '$.t', 'AS', 'labelN').equal(
        [1, 'doc:1', ['labelN', 'riceratops']])
    env.expect('ft.aggregate', 'idx2', '*', 'LOAD', '3', '@$.t', 'AS', 'labelN').equal(
        [1, ['labelN', 'riceratops']])

    # (?) Allow redefining a label with existing label found in another field in the schema,
    # together with just a label from the schema
    env.expect('ft.search', 'idx2', '*', 'RETURN', '4', '$.n', 'AS', 'labelT', 'labelT').equal(
        [1, 'doc:1', ['labelT', '9072']])

    # TODO: re-enable this
    if False: # UNSTABLE_TEST
        env.expect('ft.aggregate', 'idx2', '*', 'LOAD', '4', '@$.n', 'AS', 'labelT', 'labelT').equal(
            [1, ['labelT', '"9072"', 'labelT', 'riceratops']])

    env.expect('ft.search', 'idx2', '*', 'RETURN', '4', '$.n', 'AS', 'labelT', 'labelN').equal(
        [1, 'doc:1', ['labelT', '9072', 'labelN', '9072']])
    env.expect('ft.aggregate', 'idx2', '*', 'LOAD', '4', '@$.n', 'AS', 'labelT', 'labelN').equal(
        [1, ['labelT', '9072', 'labelN', '9072']])

@skip(msan=True)
def testNumeric(env):
    conn = getConnectionByEnv(env)
    env.execute_command('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.n', 'AS', 'n', 'NUMERIC', "$.f", 'AS', 'f', 'NUMERIC')
    conn.execute_command('JSON.SET', 'doc:1', '$', r'{"n":9, "f":9.72}')
    env.expect('FT.SEARCH', 'idx', '*', 'RETURN', '3', '$.n', 'AS', 'int').equal([1, 'doc:1', ['int', '9']])
    env.expect('FT.SEARCH', 'idx', '@n:[0 10]', 'RETURN', '3', '$.n', 'AS', 'int').equal([1, 'doc:1', ['int', '9']])
    env.expect('FT.SEARCH', 'idx', '@f:[9.5 9.9]', 'RETURN', '1', 'f') \
        .equal([1, 'doc:1', ['f', '9.72']])
    env.expect('FT.SEARCH', 'idx', '@f:[9.5 9.9]', 'RETURN', '3', '$.f', 'AS', 'flt') \
        .equal([1, 'doc:1', ['flt', '9.72']])

@skip
def testLanguage(env):
    # TODO: Check stemming? e.g., trad is stem of traduzioni and tradurre ?
    env.execute_command('FT.CREATE', 'idx', 'ON', 'JSON', 'LANGUAGE_FIELD', '$.lang', 'SCHEMA', '$.t', 'TEXT')
    env.execute_command('FT.CREATE', 'idx2', 'ON', 'JSON', 'LANGUAGE', 'Italian', 'SCHEMA', '$.domanda', 'TEXT')

    env.execute_command('JSON.SET', 'doc:1', '$', r'{"t":"traduzioni", "lang":"Italian"}')
    env.expect('ft.search', 'idx', 'tradu*', 'RETURN', '1', '$.t' ).equal([1, 'doc:1', ['$.t', '"traduzioni"']])

    env.execute_command('JSON.SET', 'doc:2', '$', r'{"domanda":"perché"}')
    env.expect('ft.search', 'idx2', 'per*', 'RETURN', '1', '$.domanda' ).equal([1, 'doc:2', ['$.domanda', '"perch\xc3\xa9"']])

@skip(msan=True)
def testDifferentType(env):
    conn = getConnectionByEnv(env)
    env.execute_command('FT.CREATE', 'hidx', 'ON', 'HASH', 'SCHEMA', '$.t', 'TEXT')
    env.execute_command('FT.CREATE', 'jidx', 'ON', 'JSON', 'SCHEMA', '$.t', 'TEXT')
    conn.execute_command('HSET', 'doc:1', '$.t', 'hello world')
    conn.execute_command('JSON.SET', 'doc:2', '$', r'{"t":"hello world"}')
    env.expect('FT.SEARCH', 'hidx', '*', 'NOCONTENT').equal([1, 'doc:1'])
    env.expect('FT.SEARCH', 'jidx', '*', 'NOCONTENT').equal([1, 'doc:2'])

@no_msan
def test_WrongJsonType(env):
    # test all possible errors in processing a field
    # we test that all documents failed to index
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
        '$.object1', 'TEXT',
        '$.object2', 'TAG',
        '$.object3', 'NUMERIC',
        '$.object4', 'GEO',
        '$.object5', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2',

        '$.array1', 'NUMERIC',
        '$.array2', 'GEO',
        '$.array3', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2', # wrong sub-types

        '$.numeric1', 'TEXT',
        '$.numeric2', 'TAG',
        '$.numeric3', 'GEO',
        '$.numeric4', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2',

        '$.bool1', 'TEXT',
        '$.bool2', 'NUMERIC',
        '$.bool3', 'GEO',
        '$.bool4', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2',

        '$.geo1', 'NUMERIC',
        '$.geo2', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2',

        '$.text1', 'NUMERIC',
        '$.text2', 'GEO',
        '$.text3', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2').ok()

    env.assertOk(conn.execute_command('JSON.SET', 'doc', '$', '{"object1":{"1":"foo", "2":"bar"}}'))
    env.assertOk(conn.execute_command('JSON.SET', 'doc', '$', '{"object2":{"1":"foo", "2":"bar"}}'))
    env.assertOk(conn.execute_command('JSON.SET', 'doc', '$', '{"object3":{"1":"foo", "2":"bar"}}'))
    env.assertOk(conn.execute_command('JSON.SET', 'doc', '$', '{"object4":{"1":"foo", "2":"bar"}}'))
    env.assertOk(conn.execute_command('JSON.SET', 'doc', '$', '{"object5":{"1":"foo", "2":"bar"}}'))

    env.assertOk(conn.execute_command('JSON.SET', 'doc', '$', '{"array1":["foo", "bar"]}'))
    env.assertOk(conn.execute_command('JSON.SET', 'doc', '$', '{"array2":["foo", "bar"]}'))
    env.assertOk(conn.execute_command('JSON.SET', 'doc', '$', '{"array3":["foo", "bar"]}'))

    env.assertOk(conn.execute_command('JSON.SET', 'doc', '$', '{"numeric1":3.141}'))
    env.assertOk(conn.execute_command('JSON.SET', 'doc', '$', '{"numeric2":3.141}'))
    env.assertOk(conn.execute_command('JSON.SET', 'doc', '$', '{"numeric3":3.141}'))
    env.assertOk(conn.execute_command('JSON.SET', 'doc', '$', '{"numeric4":3.141}'))

    env.assertOk(conn.execute_command('JSON.SET', 'doc', '$', '{"bool1":true}'))
    env.assertOk(conn.execute_command('JSON.SET', 'doc', '$', '{"bool2":true}'))
    env.assertOk(conn.execute_command('JSON.SET', 'doc', '$', '{"bool3":true}'))
    env.assertOk(conn.execute_command('JSON.SET', 'doc', '$', '{"bool4":true}'))

    env.assertOk(conn.execute_command('JSON.SET', 'doc', '$', '{"geo1":"1.23,2.34"}'))
    env.assertOk(conn.execute_command('JSON.SET', 'doc', '$', '{"geo2":"1.23,2.34"}'))

    env.assertOk(conn.execute_command('JSON.SET', 'doc', '$', '{"text1":"foo"}'))
    env.assertOk(conn.execute_command('JSON.SET', 'doc', '$', '{"text2":"foo"}'))
    env.assertOk(conn.execute_command('JSON.SET', 'doc', '$', '{"text3":"foo"}'))

    # no field was indexed
    env.expect('FT.SEARCH', 'idx', '*').equal([0])

    # check indexing failed on all field in schema
    res = index_info(env, 'idx')
    env.assertEqual(int(res['hash_indexing_failures']), len(res['attributes']))

@no_msan
def testTagNoSeparetor(env):
    conn = getConnectionByEnv(env)

    env.cmd('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
                            '$.tag1', 'AS', 'tag_list', 'TAG',
                            '$.tag2[*]', 'AS', 'tag_array', 'TAG')
    env.assertOk(conn.execute_command('JSON.SET', 'doc:1', '$', '{"tag1":"foo,bar,baz"}'))
    env.assertOk(conn.execute_command('JSON.SET', 'doc:2', '$', '{"tag2":["foo","bar,baz"]}'))

    env.assertEqual(conn.execute_command('JSON.GET', 'doc:1', '$'), '[{"tag1":"foo,bar,baz"}]')
    env.assertEqual(conn.execute_command('JSON.GET', 'doc:1', '$.tag1'), '["foo,bar,baz"]')
    env.assertEqual(conn.execute_command('JSON.GET', 'doc:2', '$'), '[{"tag2":["foo","bar,baz"]}]')
    env.assertEqual(conn.execute_command('JSON.GET', 'doc:2', '$.tag2[*]'), '["foo","bar,baz"]')

    env.expect('FT.SEARCH', 'idx', '@tag_list:{foo\\,bar\\,baz}').equal([1, 'doc:1', ['$', '{"tag1":"foo,bar,baz"}']])
    env.expect('FT.SEARCH', 'idx', '@tag_array:{bar\\,baz}').equal([1, 'doc:2', ['$', '{"tag2":["foo","bar,baz"]}']])

@no_msan
def testMixedTagError(env):
    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE', 'idx1', 'ON', 'JSON', 'SCHEMA', '$.tag[*]', 'AS', 'tag', 'TAG')
    #field has a combination of a single tag, array and object
    env.assertOk(conn.execute_command('JSON.SET', 'doc1', '$', '{"tag":["good result",         \
                                                ["bad result"],         \
                                                {"another":"bad result"}]}'))
    env.expect('FT.SEARCH', 'idx1', '*').equal([0])

@no_msan
def testImplicitUNF(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx_json', 'ON', 'JSON', 'SCHEMA',  \
        '$.a', 'AS', 'a', 'TEXT', 'SORTABLE',               \
        '$.b', 'AS', 'b', 'TEXT', 'SORTABLE', 'UNF',        \
        '$.c', 'AS', 'c', 'TEXT').ok()
    info_res = index_info(env, 'idx_json')
    env.assertEqual(info_res['attributes'][0][-1], 'UNF') # UNF is implicit with SORTABLE on JSON
    env.assertEqual(info_res['attributes'][1][-1], 'UNF')
    env.assertNotEqual(info_res['attributes'][2][-1], 'UNF')

    env.expect('FT.CREATE', 'idx_hash', 'ON', 'HASH', 'SCHEMA',  \
        '$.a', 'AS', 'a', 'TEXT', 'SORTABLE',               \
        '$.b', 'AS', 'b', 'TEXT', 'SORTABLE', 'UNF',        \
        '$.c', 'AS', 'c', 'TEXT').ok()
    info_res = index_info(env, 'idx_hash')
    env.assertNotEqual(info_res['attributes'][0][-1], 'UNF')
    env.assertEqual(info_res['attributes'][1][-1], 'UNF')
    env.assertNotEqual(info_res['attributes'][2][-1], 'UNF')

@no_msan
def testNotExistField(env):
    conn = getConnectionByEnv(env)
    env.execute_command('FT.CREATE', 'idx1', 'ON', 'JSON', 'SCHEMA', '$.t', 'AS', 't', 'TEXT')
    conn.execute_command('JSON.SET', 'doc1', '$', '{"t":"foo"}')
    env.expect('FT.SEARCH', 'idx1', '*', 'RETURN', 1, 'name').equal([1, 'doc1', []])

@no_msan
def testScoreField(env):
    conn = getConnectionByEnv(env)
    env.execute_command('FT.CREATE', 'permits1', 'ON', 'JSON', 'PREFIX', '1', 'tst:', 'SCORE_FIELD', '$._score', 'SCHEMA', '$._score', 'AS', '_score', 'NUMERIC', '$.description', 'AS', 'description', 'TEXT')
    env.execute_command('FT.CREATE', 'permits2', 'ON', 'JSON', 'PREFIX', '1', 'tst:', 'SCORE_FIELD', '$._score', 'SCHEMA', '$.description', 'AS', 'description', 'TEXT')
    env.assertOk(conn.execute_command('JSON.SET', 'tst:permit1', '$', r'{"_score":0.8, "description":"Fix the facade"}'))
    env.assertOk(conn.execute_command('JSON.SET', 'tst:permit2', '$', r'{"_score":0.7, "description":"Fix the facade"}'))
    env.assertOk(conn.execute_command('JSON.SET', 'tst:permit3', '$', r'{"_score":0.9, "description":"Fix the facade"}'))

    res = [3, 'tst:permit3', ['$', '{"_score":0.9,"description":"Fix the facade"}'],
               'tst:permit1', ['$', '{"_score":0.8,"description":"Fix the facade"}'],
               'tst:permit2', ['$', '{"_score":0.7,"description":"Fix the facade"}']]
    env.expect('FT.SEARCH', 'permits1', '*').equal(res)
    env.expect('FT.SEARCH', 'permits2', '*').equal(res)
    env.expect('FT.SEARCH', 'permits1', 'facade').equal(res)
    env.expect('FT.SEARCH', 'permits2', 'facade').equal(res)

@no_msan
def testMOD1853(env):
    # test numeric with 0 value
    conn = getConnectionByEnv(env)
    env.execute_command('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.sid', 'AS', 'sid', 'NUMERIC')
    env.assertOk(conn.execute_command('JSON.SET', 'json1', '$', r'{"sid":0}'))
    env.assertOk(conn.execute_command('JSON.SET', 'json2', '$', r'{"sid":1}'))
    res = [2, 'json1', ['sid', '0', '$', '{"sid":0}'], 'json2', ['sid', '1', '$', '{"sid":1}']]
    env.expect('FT.SEARCH', 'idx', '@sid:[0 1]', 'SORTBY', 'sid').equal(res)

@no_msan
def testTagArrayLowerCase(env):
    # test tag field change string to lower case independent of separator
    conn = getConnectionByEnv(env)

    env.assertOk(conn.execute_command('JSON.SET', 'json1', '$', r'{"attributes":[{"name":"Brand1","value":"Vivo"}]}'))
    env.assertOk(conn.execute_command('JSON.SET', 'json2', '$', r'{"attributes":[{"name":"Brand2","value":"Ext,vivo"}]}'))
    res =  [1, 'json1', ['$', '{"attributes":[{"name":"Brand1","value":"Vivo"}]}']]

    env.execute_command('FT.CREATE', 'idx1', 'ON', 'JSON', 'SCHEMA', '$.attributes[*].value', 'AS', 'attrs', 'TAG')
    waitForIndex(env, 'idx1')
    env.expect('FT.SEARCH', 'idx1', '@attrs:{Vivo}').equal(res)
    env.expect('FT.SEARCH', 'idx1', '@attrs:{vivo}').equal(res)

    env.execute_command('FT.CREATE', 'idx2', 'ON', 'JSON', 'SCHEMA', '$.attributes[*].value', 'AS', 'attrs', 'TAG', 'SEPARATOR', ',', '$.attributes[*].name', 'AS', 'name', 'TAG')
    waitForIndex(env, 'idx2')
    env.expect('FT.SEARCH', 'idx2', '@attrs:{Vivo}', 'SORTBY', 'name', 'NOCONTENT').equal([2, 'json1', 'json2'])
    env.expect('FT.SEARCH', 'idx2', '@attrs:{vivo}', 'SORTBY', 'name', 'NOCONTENT').equal([2, 'json1', 'json2'])

    env.execute_command('FT.CREATE', 'idx3', 'ON', 'JSON', 'SCHEMA', '$.attributes[*].value', 'AS', 'attrs', 'TAG', 'CASESENSITIVE')
    waitForIndex(env, 'idx3')
    env.expect('FT.SEARCH', 'idx3', '@attrs:{Vivo}').equal(res)
    env.expect('FT.SEARCH', 'idx3', '@attrs:{vivo}').equal([0])

    env.execute_command('FT.CREATE', 'idx4', 'ON', 'JSON', 'SCHEMA', '$.attributes[*].value', 'AS', 'attrs', 'TAG', 'SEPARATOR', ',', 'CASESENSITIVE')
    waitForIndex(env, 'idx4')
    env.expect('FT.SEARCH', 'idx4', '@attrs:{Vivo}', 'NOCONTENT').equal([1, 'json1'])
    env.expect('FT.SEARCH', 'idx4', '@attrs:{vivo}', 'NOCONTENT').equal([1, 'json2'])

def check_index_with_null(env, idx):
    expected = [5, 'doc1', ['sort', '1', '$', '{"sort":1,"num":null,"txt":"hello","tag":"world","geo":"1.23,4.56","vec":[0,1]}'],
                    'doc2', ['sort', '2', '$', '{"sort":2,"num":0.8,"txt":null,"tag":"world","geo":"1.23,4.56","vec":[0,1]}'],
                    'doc3', ['sort', '3', '$', '{"sort":3,"num":0.8,"txt":"hello","tag":null,"geo":"1.23,4.56","vec":[0,1]}'],
                    'doc4', ['sort', '4', '$', '{"sort":4,"num":0.8,"txt":"hello","tag":"world","geo":null,"vec":[0,1]}'],
                    'doc5', ['sort', '5', '$', '{"sort":5,"num":0.8,"txt":"hello","tag":"world","geo":"1.23,4.56","vec":null}']]

    res = env.execute_command('FT.SEARCH', idx, '*', 'SORTBY', "sort")
    env.assertEqual(res, expected, message = '{} * sort'.format(idx))

    res = env.execute_command('FT.SEARCH', idx, '@sort:[1 5]', 'SORTBY', "sort")
    env.assertEqual(res, expected, message = '{} [1 5] sort'.format(idx))

    info_res = index_info(env, idx)
    env.assertEqual(int(info_res['hash_indexing_failures']), 0)

@no_msan
def testNullValue(env):
    # check JSONType_Null is ignored, not failing
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.num', 'AS', 'num', 'NUMERIC',
                                                           '$.sort', 'AS', 'sort', 'NUMERIC',
                                                           '$.txt', 'AS', 'txt', 'TEXT',
                                                           '$.tag', 'AS', 'tag', 'TAG',
                                                           '$.vec', 'AS', 'vec', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2',
                                                           '$.geo', 'AS', 'geo', 'GEO').ok()

    env.expect('FT.CREATE', 'idx_sortable', 'ON', 'JSON', 'SCHEMA', '$.num', 'AS', 'num', 'NUMERIC', 'SORTABLE',
                                                                    '$.sort', 'AS', 'sort', 'NUMERIC', 'SORTABLE',
                                                                    '$.txt', 'AS', 'txt', 'TEXT', 'SORTABLE',
                                                                    '$.geo', 'AS', 'geo', 'GEO', 'SORTABLE').ok()

    env.expect('FT.CREATE', 'idx_separator', 'ON', 'JSON', 'SCHEMA', '$.sort', 'AS', 'sort', 'NUMERIC',
                                                                     '$.tag', 'AS', 'tag', 'TAG', 'SEPARATOR', '|').ok()

    env.expect('FT.CREATE', 'idx_casesensitive', 'ON', 'JSON', 'SCHEMA', '$.sort', 'AS', 'sort', 'NUMERIC',
                                                                         '$.tag', 'AS', 'tag', 'TAG', 'CASESENSITIVE').ok()

    conn.execute_command('JSON.SET', 'doc1', '$', r'{"sort":1, "num":null, "txt":"hello", "tag":"world", "geo":"1.23,4.56", "vec":[0,1]}')
    conn.execute_command('JSON.SET', 'doc2', '$', r'{"sort":2, "num":0.8, "txt":null, "tag":"world", "geo":"1.23,4.56", "vec":[0,1]}')
    conn.execute_command('JSON.SET', 'doc3', '$', r'{"sort":3, "num":0.8, "txt":"hello", "tag":null, "geo":"1.23,4.56", "vec":[0,1]}')
    conn.execute_command('JSON.SET', 'doc4', '$', r'{"sort":4, "num":0.8, "txt":"hello", "tag":"world", "geo":null, "vec":[0,1]}')
    conn.execute_command('JSON.SET', 'doc5', '$', r'{"sort":5, "num":0.8, "txt":"hello", "tag":"world", "geo":"1.23,4.56", "vec":null}')

    check_index_with_null(env, 'idx')
    check_index_with_null(env, 'idx_sortable')
    check_index_with_null(env, 'idx_separator')
    check_index_with_null(env, 'idx_casesensitive')

def testNullValueSkipped(env):
    ''' check null values are skipped from indexing '''

    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.num', 'AS', 'num', 'NUMERIC',
                                                           '$.txt', 'AS', 'txt', 'TEXT',
                                                           '$.tag', 'AS', 'tag', 'TAG',
                                                           '$.vec', 'AS', 'vec', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2',
                                                           '$.geo', 'AS', 'geo', 'GEO').ok()
    
    conn.execute_command('JSON.SET', 'doc1', '$', r'{"num": null}')
    conn.execute_command('JSON.SET', 'doc2', '$', r'{"txt": null}')
    conn.execute_command('JSON.SET', 'doc3', '$', r'{"tag": null}')
    conn.execute_command('JSON.SET', 'doc4', '$', r'{"geo": null}')
    conn.execute_command('JSON.SET', 'doc5', '$', r'{"vec": null}')

    info_res = index_info(env, 'idx')
    env.assertEqual(int(info_res['hash_indexing_failures']), 0)
    env.assertEqual(int(info_res['num_records']), 0)
    env.assertEqual(int(info_res['num_terms']), 0)


@no_msan
def testVector_empty_array(env):
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'ON', 'JSON',
               'SCHEMA', '$.vec', 'AS', 'vec', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2').ok()
    env.assertOk(conn.execute_command('JSON.SET', 'json1', '$', r'{"vec":[]}'))
    assertInfoField(env, 'idx', 'hash_indexing_failures', '1')

@no_msan
def testVector_correct_eval(env):
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    dim = 2
    for data_type in ['FLOAT32', 'FLOAT64']:
        env.expect('FT.CREATE', 'idx', 'ON', 'JSON',
                   'SCHEMA', '$.vec', 'AS', 'vec', 'VECTOR', 'FLAT', '6', 'TYPE', data_type, 'DIM', dim, 'DISTANCE_METRIC', 'L2').ok()
        env.assertOk(conn.execute_command('JSON.SET', 'j1', '$', r'{"vec":[1,1]}'))
        env.assertOk(conn.execute_command('JSON.SET', 'j2', '$', r'{"vec":[1,-0.189207144]}'))
        env.assertOk(conn.execute_command('JSON.SET', 'j3', '$', r'{"vec":[2.772453851,1]}'))
        env.assertOk(conn.execute_command('JSON.SET', 'j4', '$', r'{"vec":[-1,1]}'))
        query_vec = create_np_array_typed([1]*dim, data_type)

        expected_res = [4, 'j1', ['score', spatial.distance.sqeuclidean(np.array([1, 1]), query_vec)],
                        'j2', ['score', spatial.distance.sqeuclidean(np.array([1, -0.189207144]), query_vec)],
                        'j3', ['score', spatial.distance.sqeuclidean(np.array([2.772453851, 1]), query_vec)],
                        'j4', ['score', spatial.distance.sqeuclidean(np.array([-1, 1]), query_vec)]]
        actual_res = env.expect('FT.SEARCH', 'idx', '*=>[KNN 4 @vec $b AS scores]', 'PARAMS', '2', 'b', query_vec.tobytes(),
                   'RETURN', '1', 'scores').res
        env.assertEqual(expected_res[0], actual_res[0], message=data_type)
        for i in range(1, len(expected_res), 2):
            # For each result, assert its id and its distance (use float equality)
            env.assertEqual(expected_res[i], actual_res[i], message=data_type)
            if data_type == 'FLOAT32':
                env.assertAlmostEqual(expected_res[i+1][1], float(actual_res[i+1][1]), 1E-6)
            else:  # data type is float64, expect higher precision
                env.assertAlmostEqual(expected_res[i+1][1], float(actual_res[i+1][1]), 1E-9)
        conn.execute_command('FT.DROPINDEX', 'idx', 'DD')


@no_msan
def testVector_bad_values(env):
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON',
               'SCHEMA', '$.vec', 'AS', 'vec', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT64', 'DIM', '5','DISTANCE_METRIC', 'L2').ok()

    env.assertOk(conn.execute_command('JSON.SET', 'j1', '$', r'{"vec":[1,2,3,4,"ab"]}'))
    env.assertOk(conn.execute_command('JSON.SET', 'j2', '$', r'{"vec":[1,2,3,true,5]}'))
    env.assertOk(conn.execute_command('JSON.SET', 'j2', '$', r'{"vec":[1,2,null,4,5]}'))
    env.assertOk(conn.execute_command('JSON.SET', 'j3', '$', r'{"vec":[1,2,3,4]}'))
    env.assertOk(conn.execute_command('JSON.SET', 'j3', '$', r'{"vec":[1,2,3,4,5,6]}'))

    assertInfoField(env, 'idx', 'hash_indexing_failures', '5')
    assertInfoField(env, 'idx', 'num_docs', '0')

@no_msan
def testVector_delete(env):
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    dim = 2

    for data_type in ['FLOAT32', 'FLOAT64']:
        env.expect('FT.CREATE', 'idx', 'ON', 'JSON',
                   'SCHEMA', '$.vec', 'AS', 'vec', 'VECTOR', 'HNSW', '6', 'TYPE', data_type, 'DIM', '2','DISTANCE_METRIC', 'L2').ok()

        env.assertOk(conn.execute_command('JSON.SET', 'j1', '$', r'{"vec":[0.1,0.1]}'))
        env.assertOk(conn.execute_command('JSON.SET', 'j2', '$', r'{"vec":[0.2,0.3]}'))
        env.assertOk(conn.execute_command('JSON.SET', 'j3', '$', r'{"vec":[0.3,0.3]}'))
        env.assertOk(conn.execute_command('JSON.SET', 'j4', '$', r'{"vec":[0.4,0.4]}'))
        env.assertOk(conn.execute_command('JSON.SET', 'j5', '$', r'{"vec":[0.5,0.5]}'))

        env.assertOk(conn.execute_command('JSON.SET', 'j6', '$', r'{"vec":[1,1]}'))
        blob = create_np_array_typed([1]*dim, data_type).tobytes()

        q = ['FT.SEARCH', 'idx', '*=>[KNN 6 @vec $b]', 'PARAMS', '2', 'b', blob, 'RETURN', '0', 'SORTBY', '__vec_score']
        env.expect(*q).equal([6, 'j6', 'j5', 'j4', 'j3', 'j2', 'j1'])

        q = ['FT.SEARCH', 'idx', '*=>[KNN 1 @vec $b]', 'PARAMS', '2', 'b', blob, 'RETURN', '0', 'SORTBY', '__vec_score']
        env.expect(*q).equal([1, 'j6'])

        env.assertEqual(conn.execute_command('JSON.DEL', 'j3'), 1)
        env.assertEqual(conn.execute_command('JSON.DEL', 'j4'), 1)
        env.assertEqual(conn.execute_command('JSON.DEL', 'j5'), 1)
        env.assertEqual(conn.execute_command('JSON.DEL', 'j6'), 1)

        env.expect(*q).equal([1, 'j2'])
        conn.execute_command('FT.DROPINDEX', 'idx', 'DD')

@no_msan
def testRedisCommands(env):
    env.skipOnCluster()

    env.execute_command('FT.CREATE', 'idx', 'ON', 'JSON', 'PREFIX', '1', 'doc:', 'SCHEMA', '$.t', 'TEXT', '$.flt', 'NUMERIC')
    env.execute_command('JSON.SET', 'doc:1', '$', r'{"t":"riceratops","n":"9072","flt":97.2}')
    env.expect('ft.search', 'idx', 'ri*', 'NOCONTENT').equal([1, 'doc:1'])

    # Test Redis COPY
    if server_version_at_least(env, "6.2.0"):
        env.execute_command('COPY', 'doc:1', 'doc:2')
        env.execute_command('COPY', 'doc:2', 'dos:3')
    else:
        env.execute_command('JSON.SET', 'doc:2', '$', r'{"t":"riceratops","n":"9072","flt":97.2}')
        env.execute_command('JSON.SET', 'dos:3', '$', r'{"t":"riceratops","n":"9072","flt":97.2}')

    env.expect('ft.search', 'idx', 'ri*', 'NOCONTENT').equal([2, 'doc:1', 'doc:2'])


    # Test Redis DEL
    env.execute_command('DEL', 'doc:1')
    env.expect('ft.search', 'idx', 'ri*', 'NOCONTENT').equal([1, 'doc:2'])

    # Test Redis RENAME
    env.execute_command('RENAME', 'dos:3', 'doc:3')
    env.expect('ft.search', 'idx', 'ri*', 'NOCONTENT').equal([2, 'doc:2', 'doc:3'])

    # Test Redis UNLINK
    env.execute_command('UNLINK', 'doc:3')
    env.expect('ft.search', 'idx', 'ri*', 'NOCONTENT').equal([1, 'doc:2'])

    # Test Redis EXPIRE
    if UNSTABLE:
        env.execute_command('PEXPIRE', 'doc:2', 1)
        time.sleep(0.1)
        env.expect('JSON.GET', 'doc:1', '$').equal(None)
        env.expect('ft.search', 'idx', 'ri*', 'NOCONTENT').equal([0])
