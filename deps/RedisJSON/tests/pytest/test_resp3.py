# -*- coding: utf-8 -*-
import sys
import os
import redis
import json
from RLTest import Env

from common import *
from includes import *

from RLTest import Defaults

from functools import reduce

Defaults.decode_responses = True


class testResp3():
    def __init__(self):
        self.env = Env(protocol=3)

    def test_resp3_set_get_json_format(self):
        r = self.env
        r.skipOnVersionSmaller('7.0')

        r.assertTrue(r.execute_command('SET', 'test_not_JSON', 'test_not_JSON'))

        # Test JSON.SET RESP3
        r.assertOk(r.execute_command('JSON.SET', 'test_resp3', '$', '{"a1":{"b":{"c":true,"d":null}},"a2":{"b":{"c":2}}}'))

        # Test JSON.GET RESP3
        r.assertEqual(r.execute_command('JSON.GET', 'test_resp3', 'FORMAT', 'EXPAND1', '$'), [['{"a1":{"b":{"c":true,"d":null}},"a2":{"b":{"c":2}}}']])
        r.assertEqual(r.execute_command('JSON.GET', 'test_resp3', 'FORMAT', 'EXPAND1', '$..b'), [['{"c":true,"d":null}', '{"c":2}']])
        r.assertEqual(r.execute_command('JSON.GET', 'test_resp3', 'FORMAT', 'EXPAND1', '$.a1', '$.a2'),  [['{"b":{"c":true,"d":null}}'], ['{"b":{"c":2}}']])
        r.assertEqual(r.execute_command('JSON.GET', 'test_resp3', 'FORMAT', 'EXPAND1', '$.a1', '$.a3', '$.a2'),  [['{"b":{"c":true,"d":null}}'], [], ['{"b":{"c":2}}']])
        r.assertEqual(r.execute_command('JSON.GET', 'test_resp3', 'FORMAT', 'EXPAND1', '$.a3'), [[]])

        # TEST JSON.GET with none existent key
        r.assertEqual(r.execute_command('JSON.GET', 'test_no_such_key', '$.a3'), None)

        # TEST JSON.GET with not a JSON key
        r.expect('JSON.GET', 'test_not_JSON', '$.a3').raiseError()

        # Test wrong FORMAT
        r.expect('JSON.GET', 'test_resp3', 'FORMAT', 'JSON', '.a[1]').raiseError().contains("wrong reply format")
        r.expect('JSON.GET', 'test_resp3', 'FORMAT', 'JSON', '$.a[1]').raiseError().contains("wrong reply format")
        # Currently STRINGS is not supported (only STRING)
        r.expect('JSON.GET', 'test_resp3', 'FORMAT', 'STRINGS', '$.a[1]').raiseError().contains("wrong reply format")
        r.expect('JSON.GET', 'test_resp3', 'FORMAT', 'STRINGS', '$a[1]').raiseError().contains("wrong reply format")

    def test_resp3_set_get_expand_format(self):
        r = self.env
        r.skipOnVersionSmaller('7.0')

        r.assertTrue(r.execute_command('SET', 'test_not_JSON', 'test_not_JSON'))

        # Test JSON.SET RESP3
        json_doc = {"a1":{"b":{"c":True,"d":None}},"a2":{"b":{"c":2, "e":[1,True, {"f":None}]}}}
        r.assertOk(r.execute_command('JSON.SET', 'test_resp3', '$', json.dumps(json_doc)))

        # Test JSON.GET RESP3
        r.assertEqual(r.execute_command('JSON.GET', 'test_resp3', 'FORMAT', 'EXPAND', '$'), [[{'a1': {'b': {'c': True, 'd': None}}, 'a2': {'b': {'e': [1, True, {'f': None}], 'c': 2}}}]])
        r.assertEqual(r.execute_command('JSON.GET', 'test_resp3', 'FORMAT', 'EXPAND','$..b'), [[{'d': None, 'c': True}, {'c': 2, 'e': [1, True, {'f': None}]}]])
        r.assertEqual(r.execute_command('JSON.GET', 'test_resp3', 'FORMAT', 'EXPAND','$.a1', '$.a2'),  [[{'b': {'d': None, 'c': True}}], [{'b': {'e': [1, True, {'f': None}], 'c': 2}}]])
        r.assertEqual(r.execute_command('JSON.GET', 'test_resp3', 'FORMAT', 'EXPAND','$.a1', '$.a3', '$.a2'),  [[{'b': {'c': True, 'd': None}}], [], [{'b': {'c': 2, 'e': [1, True, {'f': None}]}}]])
        r.assertEqual(r.execute_command('JSON.GET', 'test_resp3', 'FORMAT', 'EXPAND','$.a3'), [[]])

        # Test JSON.GET RESP3 with default format (STRING)
        r.assertEqual(json.loads(r.execute_command('JSON.GET', 'test_resp3', '$')),
                      [json_doc])
        r.assertEqual(json.loads(r.execute_command('JSON.GET', 'test_resp3','$..b')),
                      [{'c': True, 'd': None}, {'c': 2, 'e': [1, True, {'f': None}]}])
        r.assertEqual(json.loads(r.execute_command('JSON.GET', 'test_resp3','$.a1', '$.a2')),
                      {'$.a1': [{'b': {'c': True, 'd': None}}], '$.a2': [{'b': {'c': 2, 'e': [1, True, {'f': None}]}}]})
        r.assertEqual(json.loads(r.execute_command('JSON.GET', 'test_resp3','$.a1', '$.a3', '$.a2')),
                      {'$.a1': [{'b': {'c': True, 'd': None}}], '$.a3': [], '$.a2': [{'b': {'c': 2, 'e': [1, True, {'f': None}]}}]})
        r.assertEqual(json.loads(r.execute_command('JSON.GET', 'test_resp3','$.a3')),
                      [])

        # TEST JSON.GET with none existent key
        r.assertEqual(r.execute_command('JSON.GET', 'test_no_such_key', 'FORMAT', 'EXPAND','$.a3'), None)

        # TEST JSON.GET with not a JSON key
        r.expect('JSON.GET', 'test_not_JSON', 'FORMAT', 'EXPAND','$.a3').raiseError()


    def test_resp3_set_get_string_format(self):
        r = self.env
        r.skipOnVersionSmaller('7.0')

        r.assertTrue(r.execute_command('SET', 'test_not_JSON', 'test_not_JSON'))

        # Test JSON.SET RESP3
        r.assertOk(r.execute_command('JSON.SET', 'test_resp3', '$', '{"a1":{"b":{"c":true,"d":null}},"a2":{"b":{"c":2}}}'))

        # Test JSON.GET RESP3
        r.assertEqual(r.execute_command('JSON.GET', 'test_resp3', 'FORMAT', 'STRING', '$'), '[{"a1":{"b":{"c":true,"d":null}},"a2":{"b":{"c":2}}}]')
        r.assertEqual(r.execute_command('JSON.GET', 'test_resp3', 'FORMAT', 'STRING', '$..b'), '[{"c":true,"d":null},{"c":2}]')
        r.assertEqual(json.loads(r.execute_command('JSON.GET', 'test_resp3', 'FORMAT', 'STRING', '$.a1', '$.a2')), {"$.a2":[{"b":{"c":2}}],"$.a1":[{"b":{"c":True,"d":None}}]})
        r.assertEqual(json.loads(r.execute_command('JSON.GET', 'test_resp3', 'FORMAT', 'STRING', '.a1', '$.a2')), {"$.a2":[{"b":{"c":2}}],".a1":[{"b":{"c":True,"d":None}}]})
        r.assertEqual(json.loads(r.execute_command('JSON.GET', 'test_resp3', 'FORMAT', 'STRING', '$.a1', '$.a3', '$.a2')),  {"$.a3":[],"$.a2":[{"b":{"c":2}}],"$.a1":[{"b":{"c":True,"d":None}}]})
        r.assertEqual(r.execute_command('JSON.GET', 'test_resp3', 'FORMAT', 'STRING', '$.a3'), '[]')

        # TEST JSON.GET with none existent key
        r.assertEqual(r.execute_command('JSON.GET', 'test_no_such_key', '$.a3'), None)

        # TEST JSON.GET with not a JSON key
        r.expect('JSON.GET', 'test_not_JSON', '$.a3').raiseError()


    # Test JSON.DEL RESP3
    def test_resp_json_del(self):
        r = self.env
        r.skipOnVersionSmaller('7.0')

        r.assertTrue(r.execute_command('SET', 'test_not_JSON', 'test_not_JSON'))

        r.assertOk(r.execute_command('JSON.SET', 'test_resp3', '$', '{"a1":{"b":{"c":1}},"a2":{"b":{"c":2}}}'))
        
        r.assertEqual(r.execute_command('JSON.DEL', 'test_resp3', '$..b'), 2)
        
        # Test none existing path
        r.assertEqual(r.execute_command('JSON.DEL', 'test_resp3', '$.a1.b'), 0)

        # Test none existing key
        r.assertEqual(r.execute_command('JSON.DEL', 'test_no_such_key', '$.a1.b'), 0)

        # Test not a JSON key
        r.expect('JSON.DEL', 'test_not_JSON', '$.a1.b').raiseError()

    # Test JSON.NUMINCRBY RESP3
    def test_resp_json_num_ops(self):
        r = self.env
        r.skipOnVersionSmaller('7.0')

        r.assertTrue(r.execute_command('SET', 'test_not_JSON', 'test_not_JSON'))

        r.assertOk(r.execute_command('JSON.SET', 'test_resp3', '$', '{"a1":{"b":{"c":1}},"a2":{"b":{"c":2.2}},"a3":{"b":{"c":"val"}}}'))

        # Test NUMINCRBY
        r.assertEqual(r.execute_command('JSON.NUMINCRBY', 'test_resp3', '$.a1.b.c', 1), [2])
        r.assertEqual(r.execute_command('JSON.NUMINCRBY', 'test_resp3', '$..c', 2), [4, 4.2, None])

        # Test NUMMULTBY
        r.assertEqual(r.execute_command('JSON.NUMMULTBY', 'test_resp3', '$.a2.b.c', 2.3), [9.66])
        r.assertEqual(r.execute_command('JSON.NUMMULTBY', 'test_resp3', '$..c', 2), [8, 19.32, None])

        # Test none existing key
        r.expect('JSON.NUMINCRBY', 'test_no_such_key', '$.a1.b', '1').raiseError()

        # Test not a JSON key
        r.expect('JSON.NUMMULTBY', 'test_not_JSON', '$.a1.b', '1').raiseError()


    # Test JSON.MSET RESP3
    def test_resp_json_mset(self):
        r = self.env
        r.skipOnVersionSmaller('7.0')

        r.assertTrue(r.execute_command('SET', 'test_not_JSON', 'test_not_JSON'))

        r.assertOk(r.execute_command('JSON.MSET', 'test_resp3_1{s}', '$', '{"a1":{"b":{"c":1}},"a2":{"b":{"c":2}}}', 'test_resp3_2{s}', '$', '{"a1":{"b":{"c":1}},"a2":{"b":{"c":2}}}'))

        # Test none existing key
        r.expect('JSON.MSET', 'test_no_such_key', '$.a1.b', '1').raiseError()

        # Test not a JSON key
        r.expect('JSON.MSET', 'test_not_JSON', '$.a1.b', '1').raiseError()

    # Test JSON.MERGE RESP3
    def test_resp_json_merge(self):
        r = self.env
        r.skipOnVersionSmaller('7.0')

        r.assertTrue(r.execute_command('SET', 'test_not_JSON', 'test_not_JSON'))

        r.assertOk(r.execute_command('JSON.SET', 'test_resp3', '$', '{"a1":{"b":{"c":1}},"a2":{"b":{"c":2}}}'))

        r.assertOk(r.execute_command('JSON.MERGE', 'test_resp3', '$', '{"a3":4}'))

        # Test none existing key
        r.expect('JSON.MERGE', 'test_no_such_key', 'test_resp3_1', '$', '{"a3":4}').raiseError()

        # Test not a JSON key
        r.expect('JSON.MERGE', 'test_not_JSON', '$', '{"a3":4}').raiseError()

    # Test JSON.TYPE RESP3
    def test_resp_json_type(self):
        r = self.env
        r.skipOnVersionSmaller('7.0')
            
        r.assertTrue(r.execute_command('SET', 'test_not_JSON', 'test_not_JSON'))

        r.assertOk(r.execute_command('JSON.SET', 'test_resp3', '$', '{"a1":{"b":{"c":1}},"a2":{"b":{"c":true}}, "a4":[1.2,2,3.32], "c":null}'))

        # Test JSON.TYPE RESP3
        r.assertEqual(r.execute_command('JSON.TYPE', 'test_resp3', '$.a1.b.c'), [['integer']])
        r.assertEqual(r.execute_command('JSON.TYPE', 'test_resp3', '$..b'), [['object', 'object']])
        r.assertEqual(r.execute_command('JSON.TYPE', 'test_resp3', '$'), [['object']])
        r.assertEqual(r.execute_command('JSON.TYPE', 'test_resp3', '$.a3'), [[]])
        r.assertEqual(r.execute_command('JSON.TYPE', 'test_resp3', '$.a4'), [['array']])
        r.assertEqual(r.execute_command('JSON.TYPE', 'test_resp3', '$.a4[*]'), [['number', 'integer', 'number']])
        r.assertEqual(r.execute_command('JSON.TYPE', 'test_resp3', '$..c'), [['null', 'integer', 'boolean']])

        # Test none existing key
        r.assertEqual(r.execute_command('JSON.TYPE', 'test_no_such_key', '$.a1.b.c'), [None])

        # Test not a JSON key
        r.expect('JSON.TYPE', 'test_not_JSON', '$.a1.b.c').raiseError()


    # Test JSON.ARRPOP RESP3 (Default FORMAT STRING)
    def test_resp_json_arrpop(self):
        r = self.env
        r.skipOnVersionSmaller('7.0')

        r.assertTrue(r.execute_command('JSON.SET', 'test_resp3', '$', '{"a":[{"b":2},{"g":[1,2]},3]}'))

        # Test JSON.ARRPOP DEFAULT FORMAT (STRINGS)

        r.assertEqual(list(map(lambda x:json.loads(x), r.execute_command('JSON.ARRPOP', 'test_resp3', '$.a', 1))), [{'g':[1,2]}])
        r.assertEqual(list(map(lambda x:json.loads(x), r.execute_command('JSON.ARRPOP', 'test_resp3', '$.a'))), [3])
        r.assertEqual(list(map(lambda x:json.loads(x), r.execute_command('JSON.ARRPOP', 'test_resp3', '$.a', 0))), [{'b': 2}])

        # Test JSON.ARRPOP FORMAT STRINGS
        r.assertTrue(r.execute_command('JSON.SET', 'test_resp3', '$', '{"a":[{"b":2},{"g":[1,2]},3]}'))
        r.assertEqual(list(map(lambda x:json.loads(x), r.execute_command('JSON.ARRPOP', 'test_resp3', 'FORMAT', 'STRINGS', '$.a', 1))), [{'g':[1,2]}])
        r.assertEqual(list(map(lambda x:json.loads(x), r.execute_command('JSON.ARRPOP', 'test_resp3', 'FORMAT', 'STRINGS', '$.a'))), [3])
        r.assertEqual(list(map(lambda x:json.loads(x), r.execute_command('JSON.ARRPOP', 'test_resp3', 'FORMAT', 'STRINGS', '$.a', 0))), [{'b': 2}])

        # Test not a JSON key
        r.assertTrue(r.execute_command('SET', 'test_not_JSON', 'test_not_JSON'))
        r.expect('JSON.ARRPOP', 'test_not_JSON', '$.a1').raiseError()

        # Test FORMAT is set with at least one key
        r.expect('JSON.ARRPOP', 'FORMAT', 'EXPAND',  '$.a1').raiseError()

        # Test wrong FORMAT
        r.expect('JSON.ARRPOP', 'test_resp3', 'FORMAT', 'JSON', '.a[1]').raiseError().contains("wrong reply format")
        r.expect('JSON.ARRPOP', 'test_resp3', 'FORMAT', 'JSON', '$.a[1]').raiseError().contains("wrong reply format")
        r.expect('JSON.ARRPOP', 'test_resp3', 'FORMAT', 'STRING', '$.a[1]').raiseError().contains("wrong reply format")
        r.expect('JSON.ARRPOP', 'test_resp3', 'FORMAT', 'STRING', '$a[1]').raiseError().contains("wrong reply format")


    # Test JSON.ARRPOP RESP3 with FORMAT EXPAND
    def test_resp_json_arrpop_format_expand(self):
        r = self.env
        r.skipOnVersionSmaller('7.0')

        r.assertTrue(r.execute_command('JSON.SET', 'test_resp3', '$', '{"a":[{"b":2},{"g":[1,2]},3]}'))

        # Test JSON.TYPE RESP3
        r.assertEqual(r.execute_command('JSON.ARRPOP', 'test_resp3', 'FORMAT', 'EXPAND', '$.a', 1), [{'g':[1,2]}])
        r.assertEqual(r.execute_command('JSON.ARRPOP', 'test_resp3', 'FORMAT', 'EXPAND', '$.a'), [3])
        r.assertEqual(r.execute_command('JSON.ARRPOP', 'test_resp3', 'FORMAT', 'EXPAND', '$.a', 0), [{'b': 2}])

        # Test FORMAT EXPAND with legacy path
        r.expect('JSON.ARRPOP', 'test_resp3', 'FORMAT', 'EXPAND', '.a[1]').raiseError()

        # Test not a JSON key
        r.assertTrue(r.execute_command('SET', 'test_not_JSON', 'test_not_JSON'))
        r.expect('JSON.ARRPOP', 'test_not_JSON', 'FORMAT', 'EXPAND',  '$.a1').raiseError()

    # Test JSON.ARRPOP RESP3 with FORMAT EXPAND1
    def test_resp_json_arrpop_format_expand1(self):
        r = self.env
        r.skipOnVersionSmaller('7.0')

        r.assertTrue(r.execute_command('JSON.SET', 'test_resp3', '$', '{"a":[{"b":2},{"g":[1,2]},3]}'))
        
        # Test JSON.TYPE RESP3
        r.assertEqual(r.execute_command('JSON.ARRPOP', 'test_resp3', 'FORMAT', 'EXPAND1', '$.a', 1), ['{"g":[1,2]}'])
        r.assertEqual(r.execute_command('JSON.ARRPOP', 'test_resp3', 'FORMAT', 'EXPAND1', '$.a'), [3])
        r.assertEqual(r.execute_command('JSON.ARRPOP', 'test_resp3', 'FORMAT', 'EXPAND1', '$.a', 0), ['{"b":2}'])

        # Test not a JSON key
        r.assertTrue(r.execute_command('SET', 'test_not_JSON', 'test_not_JSON'))
        r.expect('JSON.ARRPOP', 'test_not_JSON', 'FORMAT', 'JSON',  '$.a1').raiseError()

    # Test JSON.ARRPOP RESP3 with FORMAT STRING
    def test_resp_json_arrpop_format_strings(self):
        r = self.env
        r.skipOnVersionSmaller('7.0')

        r.assertTrue(r.execute_command('JSON.SET', 'test_resp3', '$', '{"a":[{"b":2},{"g":[1,2]},3]}'))

        # Test JSON.TYPE RESP3
        r.assertEqual(r.execute_command('JSON.ARRPOP', 'test_resp3', 'FORMAT', 'STRINGS', '$.a', 1), ['{"g":[1,2]}'])
        r.assertEqual(r.execute_command('JSON.ARRPOP', 'test_resp3', 'FORMAT', 'STRINGS', '$.a'), ['3'])
        r.assertEqual(r.execute_command('JSON.ARRPOP', 'test_resp3', 'FORMAT', 'STRINGS', '$.a', 0), ['{"b":2}'])

        # Test FORMAT JSON with legacy path
        r.expect('JSON.ARRPOP', 'test_resp3', 'FORMAT', 'STRINGS', '.a[1]').raiseError()

        # Test not a JSON key
        r.assertTrue(r.execute_command('SET', 'test_not_JSON', 'test_not_JSON'))
        r.expect('JSON.ARRPOP', 'test_not_JSON', 'FORMAT', 'STRINGS',  '$.a1').raiseError()

        # Test FORMAT with wrong argument
        r.expect('JSON.ARRPOP', 'test_resp3', 'FORMAT', 'JSON', '.a[1]').raiseError()

    # Test JSON.MGET RESP3 default format
    def test_resp_json_mget(self):
        r = self.env
        r.skipOnVersionSmaller('7.0')

        r.assertTrue(r.execute_command('JSON.SET', '{test}resp3_1', '$', '{"a":1, "b":{"f":"g"}, "c":3}'))
        r.assertTrue(r.execute_command('JSON.SET', '{test}resp3_2', '$', '{"a":5, "b":[true, 3, null], "d":7}'))

        # Test JSON.MGET RESP3 with default FORMAT STRING
        r.assertEqual(list(map(lambda x:json.loads(x) if x else None, r.execute_command('JSON.MGET', '{test}resp3_1', '{test}resp3_2', '$.not'))), [[], []])
        r.assertEqual(list(map(lambda x:json.loads(x) if x else None, r.execute_command('JSON.MGET', '{test}resp3_1', '{test}resp3_2', '{test}not_JSON', '$.b'))), [[{'f': 'g'}], [[True, 3, None]], None])
        r.assertEqual(list(map(lambda x:json.loads(x), r.execute_command('JSON.MGET', '{test}resp3_1', '{test}resp3_2', '$'))), [[{'a': 1, 'b': {'f': 'g'}, 'c': 3}], [{'b': [True, 3, None], 'd': 7, 'a': 5}]])
        r.assertEqual(list(map(lambda x:json.loads(x), r.execute_command('JSON.MGET', '{test}resp3_1', '{test}resp3_2', '$..*'))), [[1, {'f': 'g'}, 3, 'g'], [5, [True, 3, None], 7, True, 3, None]])

    # Test different commands with RESP3 when default path is used
    def test_resp_default_path(self):
        r = self.env
        r.skipOnVersionSmaller('7.0')

        # Test JSON.X commands on object type when default path is used
        r.assertTrue(r.execute_command('JSON.SET', 'test_resp3', '$', '{"a":[{"b":2},{"g":[1,2]},3]}'))
        r.assertEqual(r.execute_command('JSON.GET', 'test_resp3', 'FORMAT', 'EXPAND'), [[{"a":[{"b":2},{"g":[1,2]},3]}]])
        r.assertEqual(json.loads(r.execute_command('JSON.GET', 'test_resp3')), {"a":[{"b":2},{"g":[1,2]},3]})
        r.assertEqual(r.execute_command('JSON.OBJKEYS', 'test_resp3'), ['a'])
        r.assertEqual(r.execute_command('JSON.OBJLEN', 'test_resp3'), 1)
        r.assertEqual(r.execute_command('JSON.TYPE', 'test_resp3'), ['object'])
        r.assertEqual(r.execute_command('JSON.DEBUG', 'MEMORY', 'test_resp3'), 424)
        r.assertEqual(r.execute_command('JSON.DEL', 'test_resp3'), 1)

        # Test JSON.strX commands on object type when default path is used
        string = 'test_resp3_str'
        length = len(string)
        r.assertTrue(r.execute_command('JSON.SET', 'test_resp3_str', '$', fr'"{string}"'))
        r.assertEqual(r.execute_command('JSON.STRLEN', 'test_resp3_str'), length)
        string = '_append'
        length = length + len(string)
        r.assertTrue(r.execute_command('JSON.STRAPPEND', 'test_resp3_str', fr'"{string}"'))
        r.assertEqual(r.execute_command('JSON.STRLEN', 'test_resp3_str'), length)

        # Test JSON.arrX commands on object type when default path is used
        r.assertTrue(r.execute_command('JSON.SET', 'test_resp3_arr', '$', '[true, 1, "dud"]'))
        r.assertEqual(r.execute_command('JSON.ARRLEN', 'test_resp3_arr'), 3)
        r.assertEqual(r.executeCommand('JSON.ARRPOP', 'test_resp3_arr'), '"dud"')
        r.assertEqual(r.execute_command('JSON.ARRLEN', 'test_resp3_arr'), 2)
        
def test_fail_with_resp2():
    r = Env(protocol=2)
    r.assertOk(r.execute_command('JSON.SET', 'doc', '$', '{"a":[1, 2, 3], "FORMAT": [1]}'))
    
    # JSON.GET key [INDENT indent] [NEWLINE newline] [SPACE space] [FORMAT STRING|EXPAND] [path [path ...]]
    r.expect('JSON.GET', 'doc', 'FORMAT', 'EXPAND', '$').error().contains('not supported on RESP2')
    # Token beyond the first path are not considered as subcommands anymore
    r.assertEqual(json.loads(r.execute_command('JSON.GET', 'doc', 'INDENT', ' ', '$.a[0]', 'FORMAT', 'EXPAND')), [1]) 
    
    # JSON.ARRPOP <key> [FORMAT {STRINGS|EXPAND1|EXPAND}] [path [index]]
    r.expect('JSON.ARRPOP', 'doc', 'FORMAT', 'STRINGS', '$', 0).error().contains('not supported on RESP2')
    r.expect('JSON.ARRPOP', 'doc', 'FORMAT', '$', 0).error().contains('wrong reply format')
    r.assertEqual(r.execute_command('JSON.ARRPOP', 'doc', 'FORMAT'), '1')

