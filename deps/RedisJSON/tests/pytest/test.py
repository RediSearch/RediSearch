# -*- coding: utf-8 -*-

from functools import reduce
import random
import sys
import os
import redis
import json
import time
from RLTest import Env
from includes import *
from redis.client import NEVER_DECODE
from RLTest import Defaults

Defaults.decode_responses = True

# ----------------------------------------------------------------------------------------------

# Path to JSON test case files
HERE = os.path.abspath(os.path.dirname(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "../.."))
TESTS_ROOT = os.path.abspath(os.path.join(HERE, ".."))
JSON_PATH = os.path.join(TESTS_ROOT, 'files')

# ----------------------------------------------------------------------------------------------

# TODO: these are currently not supported so ignore them
json_ignore = [
    'pass-json-parser-0002.json',   # UTF-8 to Unicode
    'pass-json-parser-0005.json',   # big numbers
    'pass-json-parser-0006.json',   # UTF-8 to Unicode
    'pass-json-parser-0007.json',   # UTF-8 to Unicode
    'pass-json-parser-0012.json',   # UTF-8 to Unicode
    'pass-jsonsl-1.json',           # big numbers
    'pass-jsonsl-yelp.json',        # float precision
]

# Some basic documents to use in the tests
docs = {
    'simple': {
        'foo': 'bar',
    },
    'basic': {
        'string': 'string value',
        'none': None,
        'bool': True,
        'int': 42,
        'num': 4.2,
        'arr': [42, None, -1.2, False, ['sub', 'array'], {'subdict': True}],
        'dict': {
            'a': 1,
            'b': '2',
            'c': None,
        }
    },
    'scalars': {
        'unicode': 'string value',
        'NoneType': None,
        'bool': True,
        'int': 42,
        'float': -1.2,
    },
    'values': {
        'str': 'string value',
        'NoneType': None,
        'bool': True,
        'int': 42,
        'float': -1.2,
        'dict': {},
        'list': []
    },
    'types': {
        'null':     None,
        'boolean':  False,
        'integer':  42,
        'number':   1.2,
        'string':   'str',
        'object':   {},
        'array':    [],
    },
}

#----------------------------------------------------------------------------------------------

# def getCacheInfo(env):
#     r = env
#     res = r.cmd('JSON._CACHEINFO')
#     ret = {}
#     for x in range(0, len(res), 2):
#         ret[res[x]] = res[x+1]
#     return ret


def assertOk(r, x, msg=None):
    r.assertOk(x, message=msg)

def assertExists(r, key, msg=None):
    r.assertTrue(r.exists(key), message=msg)

def assertNotExists(r, key, msg=None):
    r.assertFalse(r.exists(key), message=msg)

#----------------------------------------------------------------------------------------------

def testSetRootWithInvalidJSONValuesShouldFail(env):
    """Test that setting the root of a ReJSON key with invalid JSON values fails"""
    r = env
    invalid = ['{', '}', '[', ']', '{]', '[}', '\\', '\\\\', '',
               ' ', '\\"', '\'', '\[', '\x00', '\x0a', '\x0c',
               # '\xff' TODO pending https://github.com/RedisLabsModules/redismodule-rs/pull/15
               ]
    for i in invalid:
        r.expect('JSON.SET', 'test', '.', i).raiseError()
        assertNotExists(r, 'test%s' % i)

def testSetInvalidPathShouldFail(env):
    """Test that invalid paths fail"""
    r = env

    invalid = ['', ' ', '\x00', '\x0a', '\x0c',
               # '\xff' TODO pending https://github.com/RedisLabsModules/redismodule-rs/pull/15
               '."', '.\x00', '.\x0a\x0c', '.-foo', '.43',
               '.foo\n.bar']
    for i in invalid:
        r.expect('JSON.SET', 'test', i, 'null').raiseError()
        assertNotExists(r, 'test%s' % i)

def testSetRootWithJSONValuesShouldSucceed(env):
    """Test that the root of a JSON key can be set with any valid JSON"""
    r = env
    for v in ['string', 1, -2, 3.14, None, True, False, [], {}]:
        r.cmd('DEL', 'test')
        j = json.dumps(v)
        r.assertOk(r.execute_command('JSON.SET', 'test', '.', j))
        r.assertExists('test')
        s = json.loads(r.execute_command('JSON.GET', 'test'))
        r.assertEqual(v, s)

def testSetAddNewImmediateChild(env):

    r = env
    r.assertOk(r.execute_command('JSON.SET', 'test', '$', json.dumps(docs)))
    # Make sure children are initially missing
    r.assertEqual(r.execute_command('JSON.GET', 'test', '$.basic.dict.new_child_1'), '[]')
    r.assertEqual(r.execute_command('JSON.GET', 'test', '$.basic.dict.new_child_2'), '[]')
    r.assertEqual(r.execute_command('JSON.GET', 'test', '$.basic.dict.new_child_3'), '[]')
    # Add a new child (immediately/directly under an existing element)
    r.assertOk(r.execute_command('JSON.SET', 'test', '$.basic.dict.new_child_1', '"new_child_1_val"'))
    # Make sure child was added
    r.assertEqual(r.execute_command('JSON.GET', 'test', '$.basic.dict.new_child_1'), '["new_child_1_val"]')

    # Add a new child as none-existing (immediately/directly under an existing element)
    r.assertOk(r.execute_command('JSON.SET', 'test', '$.basic.dict.new_child_2', '"new_child_2_val"', 'NX'))
    # Make sure child was added
    r.assertEqual(r.execute_command('JSON.GET', 'test', '$.basic.dict.new_child_2'), '["new_child_2_val"]')

    # Do not add a new child as already-existing (immediately/directly under an existing element)
    r.assertIsNone(r.execute_command('JSON.SET', 'test', '$.basic.dict.new_child_3', '"new_child_2_val"', 'XX'))

    # Do not add a new none-direct/none-immediate child
    r.assertIsNone(r.execute_command('JSON.SET', 'test', '$.basic.dict.new_child_3.new_grandchild_1', '"new_grandchild_3_val"'))

def testSetReplaceRootShouldSucceed(env):
    """Test replacing the root of an existing key with a valid object succeeds"""
    r = env
    r.assertOk(r.execute_command('JSON.SET', 'test', '.', json.dumps(docs['basic'])))
    r.assertOk(r.execute_command('JSON.SET', 'test', '.', json.dumps(docs['simple'])))
    raw = r.execute_command('JSON.GET', 'test', '.')
    r.assertEqual(json.loads(raw), docs['simple'])
    for k, v in iter(docs['values'].items()):
        r.assertOk(r.execute_command('JSON.SET', 'test', '.', json.dumps(v)))
        data = json.loads(r.execute_command('JSON.GET', 'test', '.'))
        r.assertEqual(str(type(data)), '<class \'{}\'>'.format(k))
        r.assertEqual(data, v)

def testSetGetWholeBasicDocumentShouldBeEqual(env):
    """Test basic JSON.GET/JSON.SET"""
    r = env
    data = json.dumps(docs['basic'])
    r.assertOk(r.execute_command('JSON.SET', 'test', '.', data))
    r.assertExists('test')
    r.assertEqual(json.dumps(json.loads(r.execute_command('JSON.GET', 'test'))), data)

def testSetBehaviorModifyingSubcommands(env):
    """Test JSON.SET's NX and XX subcommands"""
    r = env

    # test against the root
    r.assertIsNone(r.execute_command('JSON.SET', 'test', '.', '{}', 'XX'))
    r.assertOk(r.execute_command('JSON.SET', 'test', '.', '{}', 'NX'))
    r.assertIsNone(r.execute_command('JSON.SET', 'test', '.', '{}', 'NX'))
    r.assertOk(r.execute_command('JSON.SET', 'test', '.', '{}', 'XX'))

    # test an object key
    r.assertIsNone(r.execute_command('JSON.SET', 'test', '.foo', '[]', 'XX'))
    r.assertOk(r.execute_command('JSON.SET', 'test', '.foo', '[]', 'NX'))
    r.assertIsNone(r.execute_command('JSON.SET', 'test', '.foo', '[]', 'NX'))
    r.assertOk(r.execute_command('JSON.SET', 'test', '.foo', '[1]', 'XX'))

    # verify failure for arrays
    r.expect('JSON.SET', 'test', '.foo[1]', 'null', 'NX').raiseError()
    # r.expect('JSON.SET', 'test', '.foo[1]', 'null', 'XX').raiseError()

    # Wrong arguments
    r.expect('JSON.SET', 'test', '.foo', '[]', '').raiseError()
    r.expect('JSON.SET', 'test', '.foo', '[]', 'NN').raiseError()
    r.expect('JSON.SET', 'test', '.foo', '[]', 'FORMAT', 'TT').raiseError()
    r.expect('JSON.SET', 'test', '.foo', '[]', 'XX', 'FORMAT', '').raiseError()
    r.expect('JSON.SET', 'test', '.foo', '[]', 'XX', 'XN').raiseError()
    r.expect('JSON.SET', 'test', '.foo', '[]', 'XX', '').raiseError()

def testSetWithBracketNotation(env):
    r = env

    r.assertOk(r.execute_command('JSON.SET', 'x', '.', '{}'))
    r.assertOk(r.execute_command('JSON.SET', 'x', '.["f1"]', '{}'))  # Simple bracket notation
    r.assertOk(r.execute_command('JSON.SET', 'x', '.["f1"].f2', '[0,0,0]'))  # Mixed with dot notation
    r.assertOk(r.execute_command('JSON.SET', 'x', '.["f1"].f2[1]', '{}'))  # Replace in array
    r.assertOk(r.execute_command('JSON.SET', 'x', '.["f1"].f2[1]["f.]$.f"]', '{}'))  # Dots and invalid chars in the brackets
    r.assertOk(r.execute_command('JSON.SET', 'x', '.["f1"]["f2"][1]["f.]$.f"]', '1'))  # Replace existing value
    r.assertIsNone(r.execute_command('JSON.SET', 'x', '.["f3"].f2', '1'))  # Fail trying to set f2 when f3 doesn't exist
    r.assertEqual(json.loads(r.execute_command('JSON.GET', 'x')), {'f1': {'f2': [0, {'f.]$.f': 1}, 0]}})  # Make sure it worked

def testGetWithBracketNotation(env):
    r = env

    r.assertOk(r.execute_command('JSON.SET', 'x', '.', '[1,2,3]'))
    r.assertEqual(json.loads(r.execute_command('JSON.GET', 'x', '.[1]')), 2) # dot notation - single value
    r.assertEqual(json.loads(r.execute_command('JSON.GET', 'x', '[1]')), 2) # implicit dot notation - single value
    r.assertEqual(json.loads(r.execute_command('JSON.GET', 'x', '$.[1]')), [2]) # dollar notation - array
    r.assertEqual(json.loads(r.execute_command('JSON.GET', 'x', '$[1]')), [2]) # dollar notation - array

def testSetGetWithSpecialKey(env):
    """ Test JSON.GET/JSON.SET with special keys also with legacy syntax """
    r = env

    doc = {
        "$": "$",
        "a": "a",
        "$a": "$a",
        "$a[": "$a["
    }

    # Set doc using individual keys using legacy syntax (with implicit `$` root)
    r.assertOk(r.execute_command('JSON.SET', 'x', '$', '{"$":"$"}'))
    r.assertOk(r.execute_command('JSON.SET', 'x', 'a', '"a"'))
    r.assertOk(r.execute_command('JSON.SET', 'x', '$a', '"$a"'))
    r.assertOk(r.execute_command('JSON.SET', 'x', '$["$a["]', '"$a["'))
    r.assertEqual(json.loads(r.execute_command('JSON.GET', 'x', '$')), [doc])
    # Set doc using individual keys using legacy syntax (with explicit `.` root)
    r.assertOk(r.execute_command('JSON.SET', 'x', '.a', '"a"'))
    r.assertOk(r.execute_command('JSON.SET', 'x', '.$a', '"$a"'))
    r.assertEqual(json.loads(r.execute_command('JSON.GET', 'x', '$')), [doc])

    # Get key "$"
    r.assertEqual(json.loads(r.execute_command('JSON.GET', 'x', '$.$')), ["$"])         # dot notation
    r.assertEqual(json.loads(r.execute_command('JSON.GET', 'x', '$["$"]')), ["$"])      # bracket notation
    r.assertEqual(json.loads(r.execute_command('JSON.GET', 'x', '$')), [doc])
    # Get key "a"
    r.assertEqual(json.loads(r.execute_command('JSON.GET', 'x', '$.a')), ["a"])         # dot notation
    r.assertEqual(json.loads(r.execute_command('JSON.GET', 'x', '$["a"]')), ["a"])      # bracket notation
    r.assertEqual(json.loads(r.execute_command('JSON.GET', 'x', 'a')), "a")             # legacy
    # Get key "$a"
    r.assertEqual(json.loads(r.execute_command('JSON.GET', 'x', '$.$a')), ["$a"])       # dot notation
    r.assertEqual(json.loads(r.execute_command('JSON.GET', 'x', '$["$a"]')), ["$a"])    # bracket notation
    r.assertEqual(json.loads(r.execute_command('JSON.GET', 'x', '$a')), "$a")           # legacy
    # Get key "$a["
    r.assertEqual(json.loads(r.execute_command('JSON.GET', 'x', '$["$a["]')), ["$a["])  # bracket notation (cannot use dot notation)


def testSetWithPathErrors(env):
    r = env

    r.expect('JSON.SET', 'x', '.', '{}').ok()

    # Add to non static path
    r.expect('JSON.SET', 'x', '$..f', 1).raiseError()
    # r.assertEqual(str(e.exception), 'Err: wrong static path')

    # Treat object as array
    r.expect('JSON.SET', 'x', '$[0]', 1).raiseError()
    # r.assertEqual(str(e.exception), 'Err: path not an object')

def testGetWithPathErrors(env):
    r = env

    r.expect('JSON.SET', 'x', '.', '{}').ok()

    # None-existing paths are reported with an error message
    # If paths contain illegal characters, the error message must not contain them

    # Path (and error message) with embedded nulls in path
    r.expect('JSON.GET', 'x', 'gar\x00\x00bage').raiseError().contains("expected one of the following")

    # Path (and error message) with end of line delimiters
    r.expect('JSON.GET', 'x', 'not\x0d\x0aallowed by protocol').raiseError()

def testGetNonExistantPathsFromBasicDocumentShouldFail(env):
    """Test failure of getting non-existing values"""

    r = env

    r.assertOk(r.execute_command('JSON.SET', 'test', '.', json.dumps(docs['scalars'])))

    # Paths that do not exist
    paths = ['.foo', 'boo', '.key1[0]', '.key2.bar', '.key5[99]', '.key5["moo"]']
    for p in paths:
        r.expect('JSON.GET', 'test', p).raiseError()
    # TODO uncomment
    # # Test failure in multi-path get
    #     r.expect('JSON.GET', 'test', '.bool', paths[0]).raiseError()

def testGetPartsOfValuesDocumentOneByOne(env):
    """Test type and value returned by JSON.GET"""
    r = env
    r.expect('JSON.SET', 'test', '.', json.dumps(docs['values'])).ok()
    for k, v in iter(docs['values'].items()):
        data = json.loads(r.execute_command('JSON.GET', 'test', '.{}'.format(k)))
        r.assertEqual(str(type(data)), '<class \'{}\'>'.format(k), message=k)
        r.assertEqual(data, v, message=k)

def testGetPartsOfValuesDocumentMultiple(env):
    """Test correctness of an object returned by JSON.GET"""
    r = env
    r.expect('JSON.SET', 'test', '.', json.dumps(docs['values'])).ok()
    data = json.loads(r.execute_command('JSON.GET', 'test', *docs['values'].keys()))
    r.assertEqual(data, docs['values'])

def testGetFormatting(env):
    r = env

    objects_to_test = [
        {'obj': {'f': 'v'}},
        {'arr': [0, 1]}
    ]
    formatted_objects = [
        '{{{newline}{indent}"obj":{space}{{{newline}{indent}{indent}"f":{space}"v"{newline}{indent}}}{newline}}}',
        '{{{newline}{indent}"arr":{space}[{newline}{indent}{indent}0,{newline}{indent}{indent}1{newline}{indent}]{newline}}}'
    ]

    for o in objects_to_test:
        r.assertOk(r.execute_command('JSON.SET', list(o.keys()).pop(), '$', json.dumps(o)))

    for space in ['', ' ', '\t', '  ']:
        for indent in ['', ' ', '\t', '  ']:
            for newline in ['', '\n', '\r\n']:
                for o, f in zip(objects_to_test, formatted_objects):
                    res = r.execute_command('JSON.GET', list(o.keys()).pop(), 'INDENT', indent, 'NEWLINE', newline, 'SPACE', space)
                    r.assertEqual(res, f.format(newline=newline, space=space, indent=indent))

def testBackwardRDB(env):
    env.skipOnCluster()
    if env.useAof:
        env.skip()
    dbFileName = env.cmd('config', 'get', 'dbfilename')[1]
    dbDir = env.cmd('config', 'get', 'dir')[1]
    rdbFilePath = os.path.join(dbDir, dbFileName)
    env.stop()
    try:
        os.unlink(rdbFilePath)
    except OSError:
        pass
    filePath = os.path.join(JSON_PATH, 'backward.rdb')
    os.symlink(filePath, rdbFilePath)
    env.start()

    r = env
    res = r.execute_command('JSON.GET', 'complex')
    data = json.loads(res)
    r.assertEqual(data, {"a":{"b":[{"c":{"d":[1,'2'],"e":None}},True],"a":'a'},"b":1,"c":True,"d":None})

def testSetBSON(env):
    r = env
    bson = open(os.path.join(JSON_PATH , 'bson_bytes_1.bson'), 'rb').read()
    r.assertOk(r.execute_command('JSON.SET', 'test', '.', bson, 'FORMAT', 'BSON'))
    r.expect('JSON.GET', 'test', *docs['values'].keys()).raiseError()

def testMgetCommand(env):
    """Test REJSON.MGET command"""
    r = env

    # Set up a few keys
    for d in range(0, 5):
        key = '{{doc}}:{}'.format(d)
        r.cmd('DEL', key)
        r.expect('JSON.SET', key, '.', json.dumps(docs['basic'])).ok()

    # Test an MGET that succeeds on all keys
    raw = r.execute_command('JSON.MGET', *['{{doc}}:{}'.format(d) for d in range(0, 5)] + ['.'])
    r.assertEqual(len(raw), 5)
    for d in range(0, 5):
        key = '{{doc}}:{}'.format(d)
        r.assertEqual(json.loads(raw[d]), docs['basic'], d)

    # Test an MGET that fails for one key
    r.cmd('DEL', 'test')
    r.assertOk(r.execute_command('JSON.SET', '{doc}:test', '.', '{"bool":false}'))
    raw = r.execute_command('JSON.MGET', '{doc}:test', '{doc}:0', '{doc}:foo', '.bool')
    r.assertEqual(len(raw), 3)
    r.assertFalse(json.loads(raw[0]))
    r.assertTrue(json.loads(raw[1]))
    r.assertEqual(raw[2], None)

    # Test that MGET on missing path
    raw = r.execute_command('JSON.MGET', '{doc}:0', '{doc}:1', '42isnotapath')
    r.assertEqual(len(raw), 2)
    r.assertEqual(raw[0], None)
    r.assertEqual(raw[1], None)

    # Test that MGET fails on path errors
    r.cmd('DEL', 'test')
    r.assertOk(r.execute_command('JSON.SET', 'test', '.', '{"bull":4.2}'))
    raw = r.execute_command('JSON.MGET', '{doc}:0', 'test', '{doc}:1', '.bool')
    r.assertEqual(len(raw), 3)
    r.assertTrue(json.loads(raw[0]))
    r.assertEqual(raw[1], None)
    r.assertTrue(json.loads(raw[2]))

def testToggleCommand(env):
    """Test REJSON.TOGGLE command"""
    r = env
    r.assertOk(r.execute_command('JSON.SET', 'test', '.', '{"foo":true}'))
    r.assertEqual(r.execute_command('JSON.TOGGLE','test','.foo'), 'false')
    r.assertEqual(r.execute_command('JSON.TOGGLE','test','.foo'), 'true')

    # Test Toggeling Empty Path
    r.assertOk(r.execute_command('JSON.SET', 'test', '.', '{"foo":"bar"}'))
    r.expect('JSON.TOGGLE', 'test', '.bar').raiseError()

    # Test Toggeling Non Boolean
    r.assertOk(r.execute_command('JSON.SET', 'test', '.', '{"foo":"bar"}'))
    r.expect('JSON.TOGGLE','test','.foo').raiseError()

def testDelCommand(env):
    """Test REJSON.DEL command"""
    r = env
    # Test deleting an empty object
    r.assertOk(r.execute_command('JSON.SET', 'test', '.', '{}'))
    r.assertEqual(r.execute_command('JSON.DEL', 'test', '.'), 1)
    assertNotExists(r, 'test')

    # Test deleting an empty object
    r.assertOk(r.execute_command('JSON.SET', 'test', '.', '{"foo": "bar", "baz": "qux"}'))
    r.assertEqual(r.execute_command('JSON.DEL', 'test', '.baz'), 1)
    r.assertEqual(r.execute_command('JSON.OBJLEN', 'test', '.'), 1)
    r.assertIsNone(r.execute_command('JSON.TYPE', 'test', '.baz'))
    r.assertEqual(r.execute_command('JSON.DEL', 'test', '.foo'), 1)
    r.assertIsNone(r.execute_command('JSON.GET', 'test'))

    # Test deleting some keys from an object
    r.assertOk(r.execute_command('JSON.SET', 'test', '.', '{}'))
    r.assertOk(r.execute_command('JSON.SET', 'test', '.foo', '"bar"'))
    r.assertOk(r.execute_command('JSON.SET', 'test', '.baz', '"qux"'))
    r.assertEqual(r.execute_command('JSON.DEL', 'test', '.baz'), 1)
    r.assertEqual(r.execute_command('JSON.OBJLEN', 'test', '.'), 1)
    r.assertIsNone(r.execute_command('JSON.TYPE', 'test', '.baz'))
    r.assertEqual(r.execute_command('JSON.DEL', 'test', '.foo'), 1)
    r.assertIsNone(r.execute_command('JSON.GET', 'test'))

    # Test with an array
    r.assertOk(r.execute_command('JSON.SET', 'test', '.', '{}'))
    r.assertOk(r.execute_command('JSON.SET', 'test', '.foo', '"bar"'))
    r.assertOk(r.execute_command('JSON.SET', 'test', '.baz', '"qux"'))
    r.assertOk(r.execute_command('JSON.SET', 'test', '.arr', '[1.2,1,2]'))
    r.assertEqual(r.execute_command('JSON.DEL', 'test', '.arr[1]'), 1)
    r.assertEqual(r.execute_command('JSON.OBJLEN', 'test', '.'), 3)
    r.assertEqual(r.execute_command('JSON.ARRLEN', 'test', '.arr'), 2)
    r.assertEqual(r.execute_command('JSON.TYPE', 'test', '.arr'), 'array')
    r.assertEqual(r.execute_command('JSON.DEL', 'test', '.arr'), 1)
    r.assertEqual(r.execute_command('JSON.OBJLEN', 'test', '.'), 2)
    r.assertEqual(r.execute_command('JSON.DEL', 'test', '.'), 1)
    r.assertIsNone(r.execute_command('JSON.GET', 'test'))

def testObjectCRUD(env):
    r = env

    # Create an object
    r.assertOk(r.execute_command('JSON.SET', 'test', '.', '{ }'))
    r.assertEqual('object', r.execute_command('JSON.TYPE', 'test', '.'))
    r.assertEqual(0, r.execute_command('JSON.OBJLEN', 'test', '.'))
    raw = r.execute_command('JSON.GET', 'test')
    data = json.loads(raw)
    r.assertEqual(data, {})

    # Test failure to access a non-existing element
    r.expect('JSON.GET', 'test', '.foo').raiseError()

    # Test setting a key in the oject
    r.assertOk(r.execute_command('JSON.SET', 'test', '.foo', '"bar"'))
    r.assertEqual(1, r.execute_command('JSON.OBJLEN', 'test', '.'))
    raw = r.execute_command('JSON.GET', 'test', '.')
    data = json.loads(raw)
    r.assertEqual(data, {u'foo': u'bar'})

    # Test replacing a key's value in the object
    r.assertOk(r.execute_command('JSON.SET', 'test', '.foo', '"baz"'))
    raw = r.execute_command('JSON.GET', 'test', '.')
    data = json.loads(raw)
    r.assertEqual(data, {u'foo': u'baz'})

    # Test adding another key to the object
    r.assertOk(r.execute_command('JSON.SET', 'test', '.boo', '"far"'))
    r.assertEqual(2, r.execute_command('JSON.OBJLEN', 'test', '.'))
    raw = r.execute_command('JSON.GET', 'test', '.')
    data = json.loads(raw)
    r.assertEqual(data, {u'foo': u'baz', u'boo': u'far'})

    # Test deleting a key from the object
    r.assertEqual(1, r.execute_command('JSON.DEL', 'test', '.foo'))
    raw = r.execute_command('JSON.GET', 'test', '.')
    data = json.loads(raw)
    r.assertEqual(data, {u'boo': u'far'})

    # Test replacing the object
    r.assertOk(r.execute_command('JSON.SET', 'test', '.', '{"foo": "bar"}'))
    raw = r.execute_command('JSON.GET', 'test', '.')
    data = json.loads(raw)
    r.assertEqual(data, {u'foo': u'bar'})

    # Test deleting the object
    r.assertEqual(1, r.execute_command('JSON.DEL', 'test', '.'))
    r.assertIsNone(r.execute_command('JSON.GET', 'test', '.'))

    # Test deleting with default (root) path
    r.assertOk(r.execute_command('JSON.SET', 'test', '.', '{"foo": "bar"}'))
    r.assertEqual(1, r.execute_command('JSON.DEL', 'test'))
    r.assertIsNone(r.execute_command('JSON.GET', 'test', '.'))

def testClear(env):
    """Test JSON.CLEAR command"""

    r = env
    multi_content = r'{"n":42,"s":"42","arr":[{"n":44},"s",{"n":{"a":1,"b":2}},{"n2":{"x":3.02,"n":["to","be","cleared",4],"y":4.91}},null]}'
    r.expect('JSON.SET', 'test', '.', multi_content).ok()

    # Test get multi results (using .. recursive descent)
    r.expect('JSON.GET', 'test', '$..n').equal(r'[42,44,{"a":1,"b":2},["to","be","cleared",4]]')

    # Make sure specific obj content exists before clear
    obj_content = r'[{"a":1,"b":2}]'
    obj_content_legacy = r'{"a":1,"b":2}'
    r.expect('JSON.GET', 'test', '$.arr[2].n').equal(obj_content)
    r.expect('JSON.GET', 'test', '.arr[2].n').equal(obj_content_legacy)
    # Make sure specific arr content exists before clear
    arr_content = r'[["to","be","cleared",4]]'
    arr_content_legacy = r'["to","be","cleared",4]'
    r.expect('JSON.GET', 'test', '$.arr[3].n2.n').equal(arr_content)
    r.expect('JSON.GET', 'test', '.arr[3].n2.n').equal(arr_content_legacy)

    # Clear obj and arr with specific paths
    r.expect('JSON.CLEAR', 'test', '$.arr[2].n').equal(1)
    r.expect('JSON.CLEAR', 'test', '$.arr[3].n2.n').equal(1)

    # No clear on inappropriate path (not null)
    r.expect('JSON.CLEAR', 'test', '$.arr[4]').equal(0)

    # Make sure specific obj content was cleared
    r.expect('JSON.GET', 'test', '$.arr[2].n').equal('[{}]')
    r.expect('JSON.GET', 'test', '.arr[2].n').equal('{}')
    # Make sure specific arr content was cleared
    r.expect('JSON.GET', 'test', '$.arr[3].n2.n').equal('[[]]')
    r.expect('JSON.GET', 'test', '.arr[3].n2.n').equal('[]')

    # Make sure only appropriate content (obj and arr) was cleared
    r.expect('JSON.GET', 'test', '$..n').equal('[42,44,{},[]]')

    # Clear dynamic path
    r.expect('JSON.SET', 'test', '.', r'{"n":42,"s":"42","arr":[{"n":44},"s",{"n":{"a":1,"b":2}},{"n2":{"x":3.02,"n":["to","be","cleared",4],"y":4.91}}]}') \
        .ok()
    r.expect('JSON.CLEAR', 'test', '$.arr.*').equal(3)
    r.expect('JSON.GET', 'test', '$').equal('[{"n":42,"s":"42","arr":[{},"s",{},{}]}]')

    # Clear root
    r.expect('JSON.SET', 'test', '.', r'{"n":42,"s":"42","arr":[{"n":44},"s",{"n":{"a":1,"b":2}},{"n2":{"x":3.02,"n":["to","be","cleared",4],"y":4.91}}]}') \
        .ok()
    # TODO: switch order of the following paths and expect .equals(2) when supporting multi-paths in JSON.CLEAR
    r.expect('JSON.CLEAR', 'test', '$', '$.arr[2].n').equal(1)
    r.expect('JSON.GET', 'test', '$').equal('[{}]')

    r.expect('JSON.SET', 'test', '$', obj_content_legacy).ok()
    r.expect('JSON.CLEAR', 'test').equal(1)
    r.expect('JSON.GET', 'test', '$').equal('[{}]')

    # Clear none existing path
    r.expect('JSON.SET', 'test', '.', r'{"a":[1,2], "b":{"c":"d"}}').ok()
    r.expect('JSON.CLEAR', 'test', '$.c').equal(0)
    r.expect('JSON.GET', 'test', '$').equal('[{"a":[1,2],"b":{"c":"d"}}]')

    r.expect('JSON.CLEAR', 'test', '$.b..a').equal(0)
    r.expect('JSON.GET', 'test', '$').equal('[{"a":[1,2],"b":{"c":"d"}}]')

    # Key doesn't exist
    r.expect('JSON.CLEAR', 'not_test_key', '$').raiseError()

def testClearScalar(env):
    """Test JSON.CLEAR command for scalars"""

    r = env
    r.assertOk(r.execute_command('JSON.SET', 'test', '$', json.dumps(docs['basic'])))
    # Clear numeric values
    r.assertEqual(r.execute_command('JSON.CLEAR', 'test', '$.int'), 1)
    r.assertEqual(r.execute_command('JSON.GET', 'test', '$.int'), '[0]')

    r.assertEqual(r.execute_command('JSON.CLEAR', 'test', '$.num'), 1)
    r.assertEqual(r.execute_command('JSON.GET', 'test', '$.num'), '[0]')

    r.assertEqual(r.execute_command('JSON.CLEAR', 'test', '$..a'), 1)
    r.assertEqual(r.execute_command('JSON.GET', 'test', '$..a'), '[0]')

    r.assertOk(r.execute_command('JSON.SET', 'test', '$', json.dumps(docs['scalars'])))
    r.assertEqual(r.execute_command('JSON.CLEAR', 'test', '$.*'), 2)
    res = r.execute_command('JSON.GET', 'test', '$.*')
    r.assertEqual(json.loads(res), ['string value', None, True, 0, 0])

    # Do not clear already cleared values
    r.assertEqual(r.execute_command('JSON.CLEAR', 'test', '$.*'), 0)

    # Do not clear null scalar
    r.assertEqual(r.execute_command('JSON.CLEAR', 'test', '$.NoneType'), 0)


def testArrayCRUD(env):
    """Test JSON Array CRUDness"""

    r = env

    # Test creation of an empty array
    r.assertOk(r.execute_command('JSON.SET', 'test', '.', '[]'))
    r.assertEqual('array', r.execute_command('JSON.TYPE', 'test', '.'))
    r.assertEqual(0, r.execute_command('JSON.ARRLEN', 'test', '.'))

    # Test failure of setting an element at different positons in an empty array
    r.expect('JSON.SET', 'test', '[0]', 0).raiseError()
    r.expect('JSON.SET', 'test', '[19]', 0).raiseError()
    r.expect('JSON.SET', 'test', '[-1]', 0).raiseError()

    # Test appending and inserting elements to the array
    r.assertEqual(1, r.execute_command('JSON.ARRAPPEND', 'test', '.', 1))
    r.assertEqual(1, r.execute_command('JSON.ARRLEN', 'test', '.'))
    r.assertEqual(2, r.execute_command('JSON.ARRINSERT', 'test', '.', 0, -1))
    r.assertEqual(2, r.execute_command('JSON.ARRLEN', 'test', '.'))
    data = json.loads(r.execute_command('JSON.GET', 'test', '.'))
    r.assertListEqual([-1, 1, ], data)
    r.assertEqual(3, r.execute_command('JSON.ARRINSERT', 'test', '.', -1, 0))
    data = json.loads(r.execute_command('JSON.GET', 'test', '.'))
    r.assertListEqual([-1, 0, 1, ], data)
    r.assertEqual(5, r.execute_command('JSON.ARRINSERT', 'test', '.', -3, -3, -2))
    data = json.loads(r.execute_command('JSON.GET', 'test', '.'))
    r.assertListEqual([-3, -2, -1, 0, 1, ], data)
    r.assertEqual(7, r.execute_command('JSON.ARRAPPEND', 'test', '.', 2, 3))
    data = json.loads(r.execute_command('JSON.GET', 'test', '.'))
    r.assertListEqual([-3, -2, -1, 0, 1, 2, 3], data)

    # Test replacing elements in the array
    r.assertOk(r.execute_command('JSON.SET', 'test', '[0]', '"-inf"'))
    r.assertOk(r.execute_command('JSON.SET', 'test', '[-1]', '"+inf"'))
    r.assertOk(r.execute_command('JSON.SET', 'test', '[3]', 'null'))
    data = json.loads(r.execute_command('JSON.GET', 'test', '.'))
    r.assertListEqual([u'-inf', -2, -1, None, 1, 2, u'+inf'], data)

    # Test deleting from the array
    r.assertEqual(1, r.execute_command('JSON.DEL', 'test', '[1]'))
    r.assertEqual(1, r.execute_command('JSON.DEL', 'test', '[-2]'))
    data = json.loads(r.execute_command('JSON.GET', 'test', '.'))
    r.assertListEqual([u'-inf', -1, None, 1, u'+inf'], data)

    # TODO: Should not be needed once DEL works
    r.assertOk(r.execute_command('JSON.SET', 'test', '.', '["-inf", -1, null, 1, "+inf"]'))

    # Test trimming the array
    r.assertEqual(4, r.execute_command('JSON.ARRTRIM', 'test', '.', 1, -1))
    data = json.loads(r.execute_command('JSON.GET', 'test', '.'))
    r.assertListEqual([-1, None, 1, u'+inf'], data)
    r.assertEqual(3, r.execute_command('JSON.ARRTRIM', 'test', '.', 0, -2))
    data = json.loads(r.execute_command('JSON.GET', 'test', '.'))
    r.assertListEqual([-1, None, 1], data)
    r.assertEqual(1, r.execute_command('JSON.ARRTRIM', 'test', '.', 1, 1))
    data = json.loads(r.execute_command('JSON.GET', 'test', '.'))
    r.assertListEqual([None], data)

    # Test replacing the array
    r.assertOk(r.execute_command('JSON.SET', 'test', '.', '[true]'))
    r.assertEqual('array', r.execute_command('JSON.TYPE', 'test', '.'))
    r.assertEqual(1, r.execute_command('JSON.ARRLEN', 'test', '.'))
    r.assertEqual('true', r.execute_command('JSON.GET', 'test', '[0]'))

def testArrIndexCommand(env):
    """Test JSON.ARRINDEX command"""
    r = env

    r.assertOk(r.execute_command('JSON.SET', 'test',
                                    '.', '{ "arr": [0, 1, 2, 3, 2, 1, 0] }'))
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test', '.arr', 0), 0)
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test', '.arr', 3), 3)
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test', '.arr', 4), -1)
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test', '.arr', 0, 1), 6)
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test', '.arr', 0, -1), 6)
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test', '.arr', 0, 6), 6)
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test', '.arr', 0, 4, -0), 6)
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test', '.arr', 0, 5, -1), -1)
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test', '.arr', 0, 5, 0), 6)
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test', '.arr', 2, -2, 6), -1)
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test', '.arr', '"foo"'), -1)

    r.assertEqual(r.execute_command('JSON.ARRINSERT', 'test', '.arr', 4, '[4]'), 8)
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test', '.arr', 3), 3)
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test', '.arr', 2, 3), 5)
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test', '.arr', '[4]'), 4)

    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test', '.arr', 1), 1)

    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test', '$.arr', 1), [1])
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test', '$.arr', 2, 1, 4), [2])
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test', '$.arr', 6), [-1])
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test', '$.arr', 3, 0, 2), [-1])


def testArrInsertCommand(env):
    """Test JSON.ARRINSERT command"""
    r = env

    for (initial_value, jpath) in [('{ "arr": [] }', '.arr'), ('[]', '.')]:
        r.assertOk(r.execute_command('JSON.SET', 'test', '.', initial_value))
        r.assertEqual(r.execute_command('JSON.ARRINSERT', 'test', jpath, 0, '1'), 1)
        r.assertEqual(r.execute_command('JSON.ARRINSERT', 'test', jpath, -1, '2'), 2)
        r.assertEqual(r.execute_command('JSON.ARRINSERT', 'test', jpath, -2, '3'), 3)
        r.assertEqual(r.execute_command('JSON.ARRINSERT', 'test', jpath, 3, '4'), 4)
        r.assertEqual(r.execute_command('JSON.GET', 'test', jpath), "[3,2,1,4]")

        r.assertEqual(r.execute_command('JSON.ARRINSERT', 'test', jpath, 1, '5'), 5)
        r.assertEqual(r.execute_command('JSON.ARRINSERT', 'test', jpath, -2, '6'), 6)
        r.assertEqual(r.execute_command('JSON.GET', 'test', jpath), "[3,5,2,6,1,4]")

        r.assertEqual(r.execute_command('JSON.ARRINSERT', 'test', jpath, -3, '7', '{"A":"Z"}', '9'), 9)
        r.assertEqual(r.execute_command('JSON.GET', 'test', jpath), '[3,5,2,7,{"A":"Z"},9,6,1,4]')

        r.expect('JSON.ARRINSERT', 'test', jpath, -10, '10').raiseError()

        # Value should remain
        r.assertEqual(r.execute_command('JSON.GET', 'test', jpath), '[3,5,2,7,{"A":"Z"},9,6,1,4]')
        r.expect('JSON.ARRINSERT', 'test', jpath, 10, '10').raiseError()

        # Value should remain
        r.assertEqual(r.execute_command('JSON.GET', 'test', jpath), '[3,5,2,7,{"A":"Z"},9,6,1,4]')


def testArrIndexMixCommand(env):
    """Test JSON.ARRINDEX command with mixed values"""
    r = env

    r.assertOk(r.execute_command('JSON.SET', 'test',
                                    '.', '{ "arr": [0, 1, 2, 3, 2, 1, 0, {"val": 4}, {"val": 9}, [3,4,8], ["a", "b", 8]] }'))
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test', '.arr', 0), 0)
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test', '.arr', 3), 3)
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test', '.arr', 4), -1)
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test', '.arr', 0, 1), 6)
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test', '.arr', 0, -5), 6)
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test', '.arr', 0, 6), 6)
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test', '.arr', 0, 4, -0), 6)
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test', '.arr', 0, 5, -1), 6)
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test', '.arr', 2, -2, 6), -1)
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test', '.arr', '"foo"'), -1)

    r.assertEqual(r.execute_command('JSON.ARRINSERT', 'test', '.arr', 4, '[4]'), 12)
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test', '.arr', 3), 3)
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test', '.arr', 2, 3), 5)
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test', '.arr', '[4]'), 4)
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test', '.arr', '{\"val\":4}'), 8)
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test', '$.arr', '{\"val\":9}'), [9])
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test', '.arr', '["a", "b", 8]'), 11)
    r.assertEqual(r.execute_command('JSON.ARRINDEX', 'test', '$.arr', '[3, 4, 8]'), [10])

def testArrTrimCommand(env):
    """Test JSON.ARRTRIM command"""

    r = env
    r.assertOk(r.execute_command('JSON.SET', 'test',
                                    '.', '{ "arr": [0, 1, 2, 3, 2, 1, 0] }'))
    r.assertEqual(r.execute_command('JSON.ARRTRIM', 'test', '.arr', 1, -2), 5)
    r.assertListEqual(json.loads(r.execute_command(
        'JSON.GET', 'test', '.arr')), [1, 2, 3, 2, 1])
    r.assertEqual(r.execute_command('JSON.ARRTRIM', 'test', '.arr', 0, 99), 5)
    r.assertListEqual(json.loads(r.execute_command(
        'JSON.GET', 'test', '.arr')), [1, 2, 3, 2, 1])
    r.assertEqual(r.execute_command('JSON.ARRTRIM', 'test', '.arr', 0, 2), 3)
    r.assertListEqual(json.loads(r.execute_command(
        'JSON.GET', 'test', '.arr')), [1, 2, 3])
    r.assertEqual(r.execute_command('JSON.ARRTRIM', 'test', '.arr', 99, 2), 0)
    r.assertListEqual(json.loads(r.execute_command('JSON.GET', 'test', '.arr')), [])

    r.assertEqual(r.execute_command('JSON.ARRTRIM', 'test', '.arr', -1, 0), 0)

    r.assertOk(r.execute_command('JSON.SET', 'test',
                                 '.', '{ "arr": [0, 1, 2, 3, 2, 1, 0] }'))
    r.assertEqual(r.execute_command('JSON.ARRTRIM', 'test', '.arr', -1, 0), 0)

    r.assertOk(r.execute_command('JSON.SET', 'test',
                                 '.', '{ "arr": [0, 1, 2, 3, 2, 1, 0] }'))
    r.assertEqual(r.execute_command('JSON.ARRTRIM', 'test', '.arr', -4, 1), 0)


def testArrPopCommand(env):
    """Test JSON.ARRPOP command"""

    r = env

    r.assertOk(r.execute_command('JSON.SET', 'test',
                                    '.', '[1,2,3,4,5,6,7,8,9]'))
    r.assertEqual('9', r.execute_command('JSON.ARRPOP', 'test'))
    r.assertEqual('8', r.execute_command('JSON.ARRPOP', 'test', '.'))
    r.assertEqual('7', r.execute_command('JSON.ARRPOP', 'test', '.', -1))
    r.assertEqual('5', r.execute_command('JSON.ARRPOP', 'test', '.', -2))
    r.assertEqual('1', r.execute_command('JSON.ARRPOP', 'test', '.', 0))
    r.assertEqual('4', r.execute_command('JSON.ARRPOP', 'test', '.', 2))
    r.assertEqual('6', r.execute_command('JSON.ARRPOP', 'test', '.', 99))
    r.assertEqual('2', r.execute_command('JSON.ARRPOP', 'test', '.', -99))
    r.assertEqual('3', r.execute_command('JSON.ARRPOP', 'test'))
    r.assertIsNone(r.execute_command('JSON.ARRPOP', 'test'))
    r.assertIsNone(r.execute_command('JSON.ARRPOP', 'test', '.'))
    r.assertIsNone(r.execute_command('JSON.ARRPOP', 'test', '.', 2))

def testArrPopErrors(env):
    r = env

    r.assertOk(r.execute_command('JSON.SET', 'test','.', '1'))
    r.expect('JSON.ARRPOP', 'test').error().contains("not an array")

def testArrWrongChars(env):
    r = env

    r.assertOk(r.execute_command('JSON.SET', 'test','.', '{"arr":[1,2]}'))
    r.expect('JSON.ARRINSERT', 'test', '.arr', 0, b'\x80abc').error().contains("Couldn't parse as UTF-8 string")
    r.expect('JSON.ARRAPPEND', 'test', '.arr', b'\x80abc').error().contains("Couldn't parse as UTF-8 string")

def testArrTrimErrors(env):
    r = env

    r.assertOk(r.execute_command('JSON.SET', 'test','.', '1'))
    r.expect('JSON.ARRTRIM', 'test', '.', '0', '1').error().contains("not an array")

def testTypeCommand(env):
    """Test JSON.TYPE command"""
    r = env
    for k, v in iter(docs['types'].items()):
        r.cmd('DEL', 'test')
        r.assertOk(r.execute_command('JSON.SET', 'test', '.', json.dumps(v)))
        reply = r.execute_command('JSON.TYPE', 'test', '.')
        r.assertEqual(reply, k)

def testLenCommands(env):
    """Test the JSON.ARRLEN, JSON.OBJLEN and JSON.STRLEN commands"""
    r = env

    # test that nothing is returned for empty keys
    r.assertEqual(r.execute_command('JSON.ARRLEN', 'foo', '.bar'), None)

    # test elements with valid lengths
    r.assertOk(r.execute_command('JSON.SET', 'test', '.', json.dumps(docs['basic'])))
    r.assertEqual(r.execute_command('JSON.STRLEN', 'test', '.string'), 12)
    r.assertEqual(r.execute_command('JSON.OBJLEN', 'test', '.dict'), 3)
    r.assertEqual(r.execute_command('JSON.ARRLEN', 'test', '.arr'), 6)

    # test elements with undefined lengths
    r.expect('JSON.ARRLEN', 'test', '.bool').raiseError().contains("not an array")
    r.expect('JSON.STRLEN', 'test', '.none').raiseError().contains("expected string but found null")
    r.expect('JSON.OBJLEN', 'test', '.int').raiseError().contains("expected object but found integer")
    r.expect('JSON.STRLEN', 'test', '.num').raiseError().contains("expected string but found number")

    # test a non existing key
    r.expect('JSON.ARRLEN', 'test', '.foo').raiseError().contains("does not exist")

    # test an out of bounds index
    r.expect('JSON.ARRLEN', 'test', '.arr[999]').raiseError().contains("does not exist")

    # test an infinite index
    r.expect('JSON.ARRLEN', 'test', '.arr[-inf]').raiseError().contains("Error occurred")
    r.expect('JSON.ARRLEN', 'test', '.arr[4294967295]').raiseError().contains("does not exist")

def testObjKeysCommand(env):
    """Test JSON.OBJKEYS command"""
    r = env

    r.expect('JSON.SET', 'test', '.', json.dumps(docs['types'])).ok()
    data = r.execute_command('JSON.OBJKEYS', 'test', '.')
    r.assertEqual(len(data), len(docs['types']))
    for k in data:
        r.assertTrue(k in docs['types'], message=k)

    # test a wrong type
    r.expect('JSON.OBJKEYS', 'test', '.null').raiseError()

def testNumIncrCommand(env):
    """Test JSON.NUMINCRBY command"""
    r = env

    r.assertOk(r.execute_command('JSON.SET', 'test', '.', '{ "foo": 0, "bar": "baz" }'))
    r.assertEqual('1', r.execute_command('JSON.NUMINCRBY', 'test', '.foo', 1))
    r.assertEqual('1', r.execute_command('JSON.GET', 'test', '.foo'))
    r.assertEqual('3', r.execute_command('JSON.NUMINCRBY', 'test', '.foo', 2))
    r.assertEqual('3.5', r.execute_command('JSON.NUMINCRBY', 'test', '.foo', .5))

    # test a wrong type
    r.expect('JSON.NUMINCRBY', 'test', '.bar', 1).raiseError()

    # test a missing path
    r.expect('JSON.NUMINCRBY', 'test', '.fuzz', 1).raiseError()

    # test issue #9
    r.assertOk(r.execute_command('JSON.SET', 'num', '.', '0'))
    r.assertEqual('1', r.execute_command('JSON.NUMINCRBY', 'num', '.', 1))
    r.assertEqual('2.5', r.execute_command('JSON.NUMINCRBY', 'num', '.', 1.5))

    # test issue 55
    r.assertOk(r.execute_command('JSON.SET', 'foo', '.', '{"foo":0,"bar":42}'))
    # Get the document once
    r.execute_command('JSON.GET', 'foo', '.')
    r.assertEqual('1', r.execute_command('JSON.NUMINCRBY', 'foo', 'foo', 1))
    r.assertEqual('84', r.execute_command('JSON.NUMMULTBY', 'foo', 'bar', 2))
    res = json.loads(r.execute_command('JSON.GET', 'foo', '.'))
    r.assertEqual(1, res['foo'])
    r.assertEqual(84, res['bar'])

def testNumCommandOverflow(env):
    """Test JSON.NUMINCRBY and JSON.NUMMULTBY commands overflow """
    r = env

    # test overflow on root
    r.assertOk(r.execute_command('JSON.SET', 'big_num', '.', '1.6350000000001313e+308'))
    r.expect('JSON.NUMINCRBY', 'big_num', '.', '1.6350000000001313e+308').raiseError()
    r.expect('JSON.NUMMULTBY', 'big_num', '.', '2').raiseError()
    # (value remains)
    r.assertEqual(r.execute_command('JSON.GET', 'big_num', '.'), '1.6350000000001313e308')

    # test overflow on nested object value
    r.assertOk(r.execute_command('JSON.SET', 'nested_obj_big_num', '$', '{"l1":{"l2_a":1.6350000000001313e+308,"l2_b":2}}'))
    r.expect('JSON.NUMINCRBY', 'nested_obj_big_num', '$.l1.l2_a', '1.6350000000001313e+308').raiseError()
    r.expect('JSON.NUMMULTBY', 'nested_obj_big_num', '$.l1.l2_a', '2').raiseError()
    # (value remains)
    r.assertEqual(r.execute_command('JSON.GET', 'nested_obj_big_num', '$'), '[{"l1":{"l2_a":1.6350000000001313e308,"l2_b":2}}]')

    # test overflow on nested arr value
    r.assertOk(r.execute_command('JSON.SET', 'nested_arr_big_num', '$', '{"l1":{"l2":[0,1.6350000000001313e+308]}}'))
    r.expect('JSON.NUMINCRBY', 'nested_arr_big_num', '$.l1.l2[1]', '1.6350000000001313e+308').raiseError()
    r.expect('JSON.NUMMULTBY', 'nested_arr_big_num', '$.l1.l2[1]', '2').raiseError()
    # (value remains)
    r.assertEqual(r.execute_command('JSON.GET', 'nested_arr_big_num', '$'), '[{"l1":{"l2":[0,1.6350000000001313e308]}}]')


def testStrCommands(env):
    """Test JSON.STRAPPEND and JSON.STRLEN commands"""
    r = env

    r.assertOk(r.execute_command('JSON.SET', 'test', '.', '"foo"'))
    r.assertEqual('string', r.execute_command('JSON.TYPE', 'test', '.'))
    r.assertEqual(3, r.execute_command('JSON.STRLEN', 'test', '.'))
    r.assertEqual(6, r.execute_command('JSON.STRAPPEND', 'test', '.', '"bar"'))
    r.assertEqual('"foobar"', r.execute_command('JSON.GET', 'test', '.'))

def testRespCommand(env):
    """Test JSON.RESP command"""
    r = env

    r.assertOk(r.execute_command('JSON.SET', 'test', '.', 'null'))
#   r.assertIsNone(r.execute_command('JSON.RESP', 'test'))
#   r.assertOk(r.execute_command('JSON.SET', 'test', '.', 'true'))
#   r.assertEquals('true', r.execute_command('JSON.RESP', 'test'))
#   r.assertOk(r.execute_command('JSON.SET', 'test', '.', 42))
#   r.assertEquals(42, r.execute_command('JSON.RESP', 'test'))
#   r.assertOk(r.execute_command('JSON.SET', 'test', '.', 2.5))
#   r.assertEquals('2.5', r.execute_command('JSON.RESP', 'test'))
#   r.assertOk(r.execute_command('JSON.SET', 'test', '.', '"foo"'))
#   r.assertEquals('foo', r.execute_command('JSON.RESP', 'test'))
#   r.assertOk(r.execute_command('JSON.SET', 'test', '.', '{"foo":"bar"}'))
#   resp = r.execute_command('JSON.RESP', 'test')
#   r.assertEqual(2, len(resp))
#   r.assertEqual('{', resp[0])
#   r.assertEqual(2, len(resp[1]))
#   r.assertEqual('foo', resp[1][0])
#   r.assertEqual('bar', resp[1][1])
#   r.assertOk(r.execute_command('JSON.SET', 'test', '.', '[1,2]'))
#   resp = r.execute_command('JSON.RESP', 'test')
#   r.assertEqual(3, len(resp))
#   r.assertEqual('[', resp[0])
#   r.assertEqual(1, resp[1])
#   r.assertEqual(2, resp[2])

# def testAllJSONCaseFiles(env):
#     """Test using all JSON test case files"""
#     r.maxDiff = None
#     with r.redis() as r:
#         r.client_setname(r._testMethodName)
#         r.flushdb()

#         for jsonfile in os.listdir(JSON_PATH):
#             if jsonfile.endswith('.json'):
#                 path = '{}/{}'.format(JSON_PATH, jsonfile)
#                 with open(path) as f:
#                     value = f.read()
#                     if jsonfile.startswith('pass-'):
#                         r.assertOk(r.execute_command('JSON.SET', jsonfile, '.', value), path)
#                     elif jsonfile.startswith('fail-'):
#                         r.expect('JSON.SET', jsonfile, '.', value).raiseError()
#                         assertNotExists(r, jsonfile, path)

def testSetGetComparePassJSONCaseFiles(env):
    """Test setting, getting, saving and loading passable JSON test case files"""
    env.skipOnSlave() # work around to avoid fail on "Background save already in progress"
    r = env

    for jsonfile in os.listdir(JSON_PATH):
        r.maxDiff = None
        if jsonfile.startswith('pass-') and jsonfile.endswith('.json') and jsonfile not in json_ignore:
            path = '{}/{}'.format(JSON_PATH, jsonfile)
            r.flush()
            with open(path) as f:
                value = f.read()
                r.expect('JSON.SET', jsonfile, '.', value).ok()
                d1 = json.loads(value)
                for _ in r.retry_with_rdb_reload():
                    r.assertExists(jsonfile)
                    raw = r.execute_command('JSON.GET', jsonfile)
                    d2 = json.loads(raw)
                    r.assertEqual(d1, d2, message=path)

def testIssue_13(env):
    """https://github.com/RedisJSON/RedisJSON/issues/13"""
    r = env

    r.assertOk(r.execute_command('JSON.SET', 'test', '.', json.dumps(docs['simple'])))
    # This shouldn't crash Redis
    r.execute_command('JSON.GET', 'test', 'foo', 'foo')

def testIssue_74(env):
    """https://github.com/RedisJSON/RedisJSON2/issues/74"""
    r = env

    r.assertOk(r.execute_command('JSON.SET', 'test', '.', '{}'))
    # This shouldn't crash Redis
    r.expect('JSON.SET', 'test', '$a', '12').equal("OK")    # using legacy path
    r.expect('JSON.GET', 'test', '$a').equal('12')          # using legacy path
    r.expect('JSON.GET', 'test', '$.$a').equal('[12]')

def testDoubleParse(env):
    r = env
    r.cmd('JSON.SET', 'dblNum', '.', '[1512060373.222988]')
    res = r.cmd('JSON.GET', 'dblNum', '[0]')
    r.assertEqual(1512060373.222988, float(res))
    r.assertEqual('1512060373.222988', res)

def testIssue_80(env):
    """https://github.com/RedisJSON/RedisJSON2/issues/80"""
    r = env
    r.assertOk(r.execute_command('JSON.SET', 'test', '.', '[{"code":"1"}, {"code":"2"}]'))
    r.execute_command('JSON.GET', 'test', '.[?(@.code=="2")]')

    # This shouldn't crash Redis
    r.execute_command('JSON.GET', 'test', '$.[?(@.code=="2")]')


def testMultiPathResults(env):
    env.expect("JSON.SET", "k", '$', '[1,2,3]').ok()
    env.expect("JSON.GET", "k", '$[*]').equal('[1,2,3]')
    env.expect("JSON.SET", "k", '$', '{"a":[1,2,3],"b":["c","d","e"],"c":"k"}').ok()
    env.expect("JSON.GET", "k", '$.*[0,2]').equal('[1,3,"c","e"]')

    # make sure legacy json path returns single result
    env.expect("JSON.GET", "k", '.*[0,2]').equal('1')


def testMSET(env):
    env.expect("JSON.MSET", "a{s}", '$', '"a_val"').ok()
    env.expect("JSON.GET", "a{s}", '$').equal('["a_val"]')

    env.expect("JSON.MSET", "a{s}", '$', '{"aa":"a_val"}', "b{s}", '$', '{"bb":"b_val"}').ok()
    env.expect("JSON.MGET", "a{s}", "b{s}", '$').equal(['[{"aa":"a_val"}]', '[{"bb":"b_val"}]'])

    env.expect("JSON.MSET", "a{s}", '$.ab', '"a_val2"', "b{s}", '$..bb', '"b_val2"').ok()
    env.expect("JSON.MGET", "a{s}", "b{s}", '$').equal(['[{"aa":"a_val","ab":"a_val2"}]', '[{"bb":"b_val2"}]'])


def testMSET_Partial(env):
    # Make sure MSET doesn't stop on the first update that can't be updated
    env.expect("JSON.SET", "a{s}", '$', '{"x": {"y":[10,20], "z":[30,40]}}').ok()
    env.expect("JSON.SET", "b{s}", '$', '{"x": 60}').ok()
    env.expect("JSON.MSET", "a{s}", '$.x', '{}', "a{s}", '$.x.z[1]', '50', 'b{s}', '$.x', '70').ok()
    env.expect("JSON.GET", "a{s}", '$').equal('[{"x":{}}]')
    env.expect("JSON.GET", "b{s}", '$').equal('[{"x":70}]')

    # Update the same key twice with a failure in the middle
    env.expect("JSON.SET", "a{s}", '$', '{"x": {"y":[10,20], "z":[30,40]}}').ok()
    env.expect("JSON.MSET", "a{s}", '$.x', '{}', "a{s}", '$.x.z[1]', '50', "a{s}",  '$.u', '70').ok()
    env.expect("JSON.GET", "a{s}", '$').equal('[{"x":{},"u":70}]')

def testMSET_Error(env):
    env.expect("JSON.SET", "a{s}", '$', '"a_val"').ok()

    env.expect("JSON.MSET", "a{s}").raiseError()
    # make sure value was not changed
    env.expect("JSON.GET", "a{s}", '$').equal('["a_val"]')

    env.expect("JSON.MSET", "a{s}", '$', '"a_val2"', "b{s}", '$').raiseError()
    # make sure value was not changed
    env.expect("JSON.GET", "a{s}", '$').equal('["a_val"]')
    env.expect("JSON.GET", "b{s}", '$').equal(None)

    env.expect("JSON.MSET", "a{s}", '$', '"a_val2"', "b{s}", '$.....a', '"b_val"').raiseError()
    # make sure value was not changed
    env.expect("JSON.GET", "a{s}", '$').equal('["a_val"]')
    env.expect("JSON.GET", "b{s}", '$').equal(None)

def testIssue_597(env):
    env.expect("JSON.SET", "test", ".", "[0]").ok()
    env.assertEqual(env.execute_command("JSON.SET", "test", ".[0]", "[0]", "NX"), None)
    env.expect("JSON.SET", "test", ".[1]", "[0]", "NX").raiseError()
    # make sure value was not changed
    env.expect("JSON.GET", "test", ".").equal('[0]')

def testCrashInParserMOD2099(env):

    r = env
    r.assertOk(r.execute_command('JSON.SET', 'test', '$', '{"a":{"x":{"i":10}}, "b":{"x":{"i":20, "j":5}}}'))

    res = r.execute_command('JSON.GET', 'test', '$..x[?(@>10)]')
    r.assertEqual(res, '[20]')

    res = r.execute_command('JSON.GET', 'test', '$..x[?($>10)]')
    r.assertEqual(res, '[]')


def testInfoEverything(env):

    r = env
    res = r.execute_command('INFO', 'EVERYTHING')
    r.assertFalse(res['modules'] is None)

def testCopyCommand(env):
    """Test COPY command and make sure behavior of json keys is similar to hash keys"""

    env.skipOnCluster()
    env.skipOnVersionSmaller('6.2')
    r = env

    values = {"foo": "bar", "fu": "wunderbar"}

    ### Copy json to a new key (from json1 to json2)
    r.assertOk(r.execute_command('JSON.SET', 'json1', '$', json.dumps(values)))
    r.assertTrue(r.execute_command('COPY', 'json1', 'json2'))
    # Check new values
    res = r.execute_command('JSON.GET', 'json1', '$')
    r.assertEqual(json.loads(res), [values])
    res = r.execute_command('JSON.GET', 'json2', '$')
    r.assertEqual(json.loads(res), [values])

    ### Copy hash to a new key (from hash1 to hash2)
    hash_values = list(reduce(lambda acc, v: acc + v, values.items()))
    r.assertEqual(r.execute_command('HSET', 'hash1', *hash_values), int(len(hash_values) / 2))
    r.assertTrue(r.execute_command('COPY', 'hash1', 'hash2'))
    # Check new values
    r.assertEqual(r.execute_command('HGETALL', 'hash1'), values)
    r.assertEqual(r.execute_command('HGETALL', 'hash2'), values)

    new_values = {"ganz": "neue"}

    ### Copy hash to an existing key
    hash_values = list(reduce(lambda acc, v: acc + v, new_values.items()))
    r.assertEqual(r.execute_command('HSET', 'hash3', *hash_values), int(len(hash_values) / 2))
    # Do not overwrite without REPLACE (from hash to hash)
    r.assertFalse(r.execute_command('COPY', 'hash3', 'hash2'))
    # Do not overwrite without REPLACE (from hash to json)
    r.assertFalse(r.execute_command('COPY', 'hash3', 'json2'))
    # Overwrite with REPLACE (from hash to hash)
    r.assertTrue(r.execute_command('COPY', 'hash3', 'hash2', 'REPLACE'))
    # Overwrite with REPLACE (from hash to json)
    r.assertTrue(r.execute_command('COPY', 'hash3', 'json2', 'REPLACE'))
    # Check new values
    r.assertEqual(r.execute_command('HGETALL', 'hash2'), new_values)
    r.assertEqual(r.execute_command('HGETALL', 'json2'), new_values)

    ### Copy json to an existing key
    r.assertOk(r.execute_command('JSON.SET', 'json3', '$', json.dumps(new_values)))
    # Do not overwrite without REPLACE (from json to json)
    r.assertFalse(r.execute_command('COPY', 'json3', 'json2'))
    # Do not overwrite without REPLACE (from json to hash)
    r.assertFalse(r.execute_command('COPY', 'json3', 'hash2'))
    # Overwrite with REPLACE (from json to json)
    r.assertTrue(r.execute_command('COPY', 'json3', 'json2', 'REPLACE'))
    # Overwrite with REPLACE (from json to hash)
    r.assertTrue(r.execute_command('COPY', 'json3', 'hash2', 'REPLACE'))
    # Check new values
    res = r.execute_command('JSON.GET', 'json2', '$')
    r.assertEqual(json.loads(res), [new_values])
    res = r.execute_command('JSON.GET', 'hash2', '$')
    r.assertEqual(json.loads(res), [new_values])

def nest_object(depth, max_name_len, leaf_key, leaf_val):
    """ Return a string of a Python object with `depth` nesting level, such as {"a":{"b":{"c":{"leaf":42}}}} """

    res = {}
    cur = res
    for i in range(1, depth - 1):
        name = ''.join([chr(random.randint(ord('a'), ord('z')))
                       for _ in range(0, random.randint(1, max_name_len))])
        cur[name] = {}
        cur = cur[name]
    cur[leaf_key] = leaf_val
    return json.dumps(res)

def testNesting(env):
    """ Test JSONPath Object nesting depth """
    r = env

    # Max nesting level for a single JSON value
    depth = 128
    doc = nest_object(depth, 5, "__leaf", 42)
    r.assertOk(r.execute_command('JSON.SET', 'test', '$', doc))
    res = r.execute_command('JSON.GET', 'test', '$..__leaf')
    r.assertEqual(res, '[42]')

    # No overall max nesting level (can exceeded the single value max nesting level)
    doc = nest_object(depth, 5, "__deep_leaf", 420)
    r.execute_command('JSON.SET', 'test', '$..__leaf', doc)
    res = r.execute_command('JSON.GET', 'test', '$..__deep_leaf')
    r.assertEqual(res, '[420]')

    doc = nest_object(depth, 5, "__helms_deep_leaf", 42000)
    r.execute_command('JSON.SET', 'test', '$..__deep_leaf', doc)
    res = r.execute_command('JSON.GET', 'test', '$..__helms_deep_leaf')
    r.assertEqual(res, '[42000]')

    # Max nesting level for a single JSON value cannot be exceeded
    depth = 129
    doc = nest_object(depth, 5, "__leaf", 42)
    r.expect('JSON.SET', 'test', '$', doc).raiseError().contains("recursion limit exceeded")


def testEscape(env):
    # Test json escape characters

    r = env

    # Escaped control characters \b, \f, \n, \r, \t \/, \\
    r.expect('JSON.SET', 'doc', '$', r'{"val": "escaped control here:\b \f \n \r \t \/ \\"}').ok()
    r.expect('JSON.GET', 'doc', '$.val').equal(r'["escaped control here:\b \f \n \r \t / \\"]')

    # Escaped quotes
    r.expect('JSON.SET', 'doc', '$', '{"val": "escaped quote here:\\""}').ok()
    r.expect('JSON.GET', 'doc', '$.val').equal('["escaped quote here:\\""]')

    # Escaped unicode
    r.expect('JSON.SET', 'doc', '$', '{"val": "escaped unicode here:\u2B50"}').ok()
    r.expect('JSON.GET', 'doc', '$.val').equal('["escaped unicode here:⭐"]')

def testFilter(env):
    # Test JSONPath filter
    r = env

    doc = {
        "arr": ["kaboom", "kafoosh", "four", "bar", 7.0, "foolish", ["food", "foo", "FoO", "fight"], -9, {"in" : "fooctious"}, "ffool", "(?i)^[f][o][o]$", False, None],
        "pat_regex": ".*foo",
        "pat_plain": "(?i)^[f][o][o]$",
        "pat_bad": "[f.*",
        "pat_not_str1": 42,
        "pat_not_str2": None,
        "pat_not_str3": True,
        "pat_not_str4": {"p":".*foo"},
        "pat_not_str5": [".*foo"],
    }
    r.expect('JSON.SET', 'doc', '$', json.dumps(doc)).ok()

    # regex match using a static regex pattern
    r.expect('JSON.GET', 'doc', '$.arr[?(@ =~ ".*foo")]').equal('["kafoosh","foolish","ffool"]')

    # regex match using a field
    r.expect('JSON.GET', 'doc', '$.arr[?(@ =~ $.pat_regex)]').equal('["kafoosh","foolish","ffool"]')

    # regex case-insensitive match using a field (notice the `.*` before the filter)
    r.expect('JSON.GET', 'doc', '$.arr.*[?(@ =~ $.pat_plain)]').equal('["foo","FoO"]')

    # regex match using field after being modified
    r.expect('JSON.SET', 'doc', '$.pat_regex', '"k.*foo"').ok()
    r.expect('JSON.GET', 'doc', '$.arr[?(@ =~ $.pat_regex)]').equal('["kafoosh"]')

    # regex mismatch (illegal pattern)
    r.expect('JSON.GET', 'doc', '$.arr[?(@ == $.pat_bad)]').equal('[]')
    r.expect('JSON.GET', 'doc', '$.arr[?(@ == $.pat_bad || @>4.5)]').equal('[7.0]')

    # regex mismatch (missing pattern)
    r.expect('JSON.GET', 'doc', '$.arr[?(@ =~ $.pat_missing)]').equal('[]')

    # regex mismatch (not a string pattern)
    for i in range(1, 6):
        r.expect('JSON.GET', 'doc', '$.arr[?(@ =~ $.pat_not_str{})]'.format(i)).equal('[]')

    # plain string match
    r.expect('JSON.GET', 'doc', '$.arr[?(@ == $.pat_plain)]').equal('["(?i)^[f][o][o]$"]')

def testFilterExpression(env):
    # Test JSONPath filter with 3 or more operands
    r = env
    doc = [
        {"foo":1, "bar":2, "baz": 3, "quux": 4},
        {"foo":2, "bar":4, "baz": 6, "quux": 9},
        {"foo":2, "bar":3, "baz": 6, "quux": 10}
    ]
    r.expect('JSON.SET', 'doc', '$', json.dumps(doc)).ok()
    res = r.execute_command('JSON.GET', 'doc', '$[?(@.foo>1 && @.quux>8 && @.bar>3 && @.baz>4)]')
    r.assertEqual(json.loads(res), [doc[1]])

def testFilterPrecedence(env):
    # Test JSONPath filter precedence (&& precedes ||)
    r = env
    doc = [{"t": True, "f": False, "one": 1},{"t": True, "f": False, "one": 2}]
    r.expect('JSON.SET', 'doc', '$', json.dumps(doc)).ok()
    res = r.execute_command('JSON.GET', 'doc', '$[?(@.f==true || @.one==1 && @.t==true)]')
    r.assertEqual(json.loads(res), [doc[0]])
    r.expect('JSON.GET', 'doc', '$[?(@.f==true || @.one==1 && @.t==false)]').equal('[]')

def testMerge(env):
    # Test JSON.MERGE
    r = env

    # Test with root path $
    r.assertOk(r.execute_command('JSON.SET', 'test_merge', '$', '{"a":{"b":{"c":"d"}}}'))
    r.assertOk(r.execute_command('JSON.MERGE', 'test_merge', '$', '{"a":{"b":{"e":"f"}}}'))
    r.expect('JSON.GET', 'test_merge').equal('{"a":{"b":{"c":"d","e":"f"}}}')

    # Test with root path path $.a.b
    r.assertOk(r.execute_command('JSON.MERGE', 'test_merge', '$.a.b', '{"h":"i"}'))
    r.expect('JSON.GET', 'test_merge').equal('{"a":{"b":{"c":"d","e":"f","h":"i"}}}')

    # Test with null value to delete a value
    r.assertOk(r.execute_command('JSON.MERGE', 'test_merge', '$.a.b', '{"c":null}'))
    r.expect('JSON.GET', 'test_merge').equal('{"a":{"b":{"h":"i","e":"f"}}}')

    # Test merge error - invalid JSON
    r.expect('JSON.MERGE', 'test_merge', '$.a', '{"b":{"h":"i" "bye"}}').error().contains("expected")

    # Test with none existing key with path $.a
    r.expect('JSON.MERGE', 'test_merge_new', '$.a', '{"a":"i"}').raiseError()

    # Test with none existing key -> create key
    r.assertOk(r.execute_command('JSON.MERGE', 'test_merge_new', '$', '{"h":"i"}'))
    r.expect('JSON.GET', 'test_merge_new').equal('{"h":"i"}')


def testMergeArray(env):
    # Test JSON.MERGE with arrays
    r = env

    # Test merge on an array
    r.assertOk(r.execute_command('JSON.SET', 'test_merge_array', '$', '{"a":{"b":{"c":["d","e"]}}}'))
    r.assertOk(r.execute_command('JSON.MERGE', 'test_merge_array', '$.a.b.c', '["f"]'))
    r.expect('JSON.GET', 'test_merge_array').equal('{"a":{"b":{"c":["f"]}}}')

    # Test merge an array on a value
    r.assertOk(r.execute_command('JSON.SET', 'test_merge_array', '$', '{"a":{"b":{"c":"d"}}}'))
    r.assertOk(r.execute_command('JSON.MERGE', 'test_merge_array', '$.a.b.c', '["f"]'))
    r.expect('JSON.GET', 'test_merge_array').equal('{"a":{"b":{"c":["f"]}}}')

    # Test with null value to delete an array value
    r.assertOk(r.execute_command('JSON.SET', 'test_merge_array', '$', '{"a":{"b":{"c":["d","e"]}}}'))
    r.assertOk(r.execute_command('JSON.MERGE', 'test_merge_array', '$.a.b', '{"c":null}'))
    r.expect('JSON.GET', 'test_merge_array').equal('{"a":{"b":{}}}')


def testMergeDynamicPath(env):
    # Test JSON.MERGE with dynamic jsonpath
    r = env

    # Test with simple dynamic path
    r.assertOk(r.execute_command('JSON.SET', 'test_merge_dynamic', '$', '{"a1":{"b":{"c":1}},"a2":{"b":{"c":2}}}'))
    r.assertOk(r.execute_command('JSON.MERGE', 'test_merge_dynamic', '$..b', '{"f":3}'))
    r.expect('JSON.GET', 'test_merge_dynamic', '$..b').equal('[{\"c\":1,\"f\":3},{\"c\":2,\"f\":3}]')

    # Test with overlapping dynamic path
    r.assertOk(r.execute_command('JSON.SET', 'test_merge_dynamic', '$', '{"a1":{"b":{"b":1}}}'))
    r.assertOk(r.execute_command('JSON.MERGE', 'test_merge_dynamic', '$..b', '{"f":3}'))
    r.expect('JSON.GET', 'test_merge_dynamic').equal('{"a1":{"b":{"b":{"f":3},"f":3}}}')

    # Test with overriding dynamic path
    r.assertOk(r.execute_command('JSON.SET', 'test_merge_dynamic', '$', '{"a1":{"b":{"f":{"b":1}}}}'))
    r.assertOk(r.execute_command('JSON.MERGE', 'test_merge_dynamic', '$..b', '{"f":3}'))
    r.expect('JSON.GET', 'test_merge_dynamic').equal('{"a1":{"b":{"f":3}}}')


def testMergeNested(env):
    # Test JSON.MERGE with nested value
    r = env

    # Test with simple nested merge
    r.assertOk(r.execute_command('JSON.SET', 'test_merge_nested', '$', '{"a":{"b":{"f":{"b":1}}}}'))
    r.assertOk(r.execute_command('JSON.MERGE', 'test_merge_nested', '$.a', '{"b":{"f":{"c":3}}}'))
    r.expect('JSON.GET', 'test_merge_nested').equal('{"a":{"b":{"f":{"b":1,"c":3}}}}')

    # Test with simple nested merge
    r.assertOk(r.execute_command('JSON.SET', 'test_merge_nested', '$', '{"a":{"b":{"f1":{"b1":1},"f2":{"b2":2}}}}'))
    r.assertOk(r.execute_command('JSON.MERGE', 'test_merge_nested', '$.a', '{"b":{"f1":{"c1":3},"f2":{"c2":4}}}'))
    r.expect('JSON.GET', 'test_merge_nested').equal('{"a":{"b":{"f1":{"b1":1,"c1":3},"f2":{"b2":2,"c2":4}}}}')

    # Test with simple nested merge on dynamic path
    r.assertOk(r.execute_command('JSON.SET', 'test_merge_nested', '$', '{"a":{"b1":{"f":{"c1":1}}, "b2":{"f":{"c2":2}}}}}'))
    r.assertOk(r.execute_command('JSON.MERGE', 'test_merge_nested', '$.a.*', '{"f":{"t":6}}'))
    r.expect('JSON.GET', 'test_merge_nested').equal('{"a":{"b1":{"f":{"c1":1,"t":6}},"b2":{"f":{"c2":2,"t":6}}}}')


def testRDBUnboundedDepth(env):
    # Test RDB Unbounded Depth load
    r = env
    json_value = nest_object(128, 5, "__leaf", 42)
    r.expect('JSON.SET', 'doc', '$', json_value).ok()

    # concat the string_126 at the end of itself
    json_value = nest_object(3, 5, "__deep_leaf", 420)
    r.expect('JSON.SET', 'doc', '$..__leaf', json_value).ok()

    # RDB dump and restore the key 'doc' and check that the key is still valid
    dump = env.execute_command('dump', 'doc', **{NEVER_DECODE: []})
    r.expect('RESTORE', 'doc1', 0, dump).ok()
    r.expect('JSON.GET', 'doc1', '$..__leaf..__deep_leaf').equal('[420]')

def testUnicodeCharacters(env):
    # Test unicode strings parsing and processing.

    r = env

    r.assertOk(r.execute_command('JSON.SET', 'test', '$', '{"\u00a0": "⍢∪⇰"}'))
    r.expect('JSON.GET', 'test', '$').equal('[{"\u00a0":"⍢∪⇰"}]')

    r.assertOk(r.execute_command('JSON.SET', 'test', '$', '{"\u00a0": { "name": "\u00a0\u00a0" } }'))
    r.expect('JSON.GET', 'test', '$').equal('[{"\u00a0":{"name":"\u00a0\u00a0"}}]')

def test_promote_u64_to_f64(env):
    r = env
    i64max = 2 ** 63 - 1
    
    # i64 + i64 behaves normally
    r.expect('JSON.SET', 'num', '$', 0).ok()
    r.expect('JSON.TYPE', 'num', '$').equal(['integer'])
    res = r.execute_command('JSON.GET', 'num', '$')
    val = json.loads(res)[0]
    r.assertEqual(val, 0)
    res = r.execute_command('JSON.NUMINCRBY', 'num', '$', i64max)
    val = json.loads(res)[0]
    r.assertEqual(val, i64max)                            # i64 + i64 no overflow
    r.assertNotEqual(val, float(i64max))                  # i64max is not representable as f64
    r.expect('JSON.TYPE', 'num', '$').equal(['integer'])  # no promotion
    res = r.execute_command('JSON.NUMINCRBY', 'num', '$', 1)
    val = json.loads(res)[0]
    r.assertEqual(val, -(i64max + 1))                     # i64 + i64 overflow wraps. as prior, not breaking
    r.assertNotEqual(val, i64max + 1)                     # i64 + i64 is not promoted to u64
    r.assertNotEqual(val, float(i64max) + float(1))       # i64 + i64 is not promoted to f64
    r.expect('JSON.TYPE', 'num', '$').equal(['integer'])  # no promotion

    # i64 + u64 used to have inconsistent behavior
    r.expect('JSON.SET', 'num', '$', 0).ok()
    res = r.execute_command('JSON.NUMINCRBY', 'num', '$', i64max + 2)
    val = json.loads(res)[0]
    r.assertNotEqual(val, -(i64max + 1) + 1)             # i64 + u64 is not i64
    r.assertNotEqual(val, i64max + 2)                    # i64 + u64 is not u64
    r.assertEqual(val, float(i64max + 2))                # i64 + u64 promotes to f64. as prior, not breaking
    r.expect('JSON.TYPE', 'num', '$').equal(['number'])  # promoted

    # u64 + i64 used to crash
    r.expect('JSON.SET', 'num', '$', i64max + 1).ok()
    r.expect('JSON.TYPE', 'num', '$').equal(['integer']) # as prior, not breaking
    res = r.execute_command('JSON.GET', 'num', '$')
    val = json.loads(res)[0]
    r.assertNotEqual(val, -(i64max + 1))                 # not i64
    r.assertEqual(val, i64max + 1)                       # as prior, not breaking
    res = r.execute_command('JSON.NUMINCRBY', 'num', '$', 1)
    val = json.loads(res)[0]
    r.assertNotEqual(val, -(i64max + 1) + 1)             # u64 + i64 is not i64
    r.assertNotEqual(val, i64max + 2)                    # u64 + i64 is not u64
    r.assertEqual(val, float(i64max + 2))                # u64 + i64 promotes to f64. used to crash
    r.expect('JSON.TYPE', 'num', '$').equal(['number'])  # promoted

    # u64 + u64 used to have inconsistent behavior
    r.expect('JSON.SET', 'num', '$', i64max + 1).ok()
    r.expect('JSON.CLEAR', 'num', '$').equal(1)          # clear u64 used to crash
    r.expect('JSON.SET', 'num', '$', i64max + 1).ok()
    res = r.execute_command('JSON.NUMINCRBY', 'num', '$', i64max + 2)
    val = json.loads(res)[0]
    r.assertNotEqual(val, -(i64max + 1) + i64max + 2)    # u64 + u64 is not i64
    r.assertNotEqual(val, 2)                             # u64 + u64 is not u64
    r.assertEqual(val, float(2 * i64max + 3))            # u64 + u64 promotes to f64. as prior, not breaking
    r.expect('JSON.TYPE', 'num', '$').equal(['number'])  # promoted


def test_mset_replication_in_aof(env):
    env.skipOnCluster()
    env = Env(useAof=True)
    with env.getClusterConnectionIfNeeded() as r:
        r.execute_command('config', 'set', 'notify-keyspace-events', 'KEA')

        pubsub = r.pubsub()
        pubsub.psubscribe('__key*')

        time.sleep(1)
        env.assertEqual('psubscribe', pubsub.get_message(timeout=1)['type'])

        command = [b'JSON.MSET']
        data = 'a' * 100
        num_params = 5
        for i in range(0, num_params):
            key = f'k:{i}'
            value = json.dumps({"data": data})
            command.append(key.encode())
            command.append(b'$')
            command.append(value.encode())
        env.expect(*command).ok()

        for i in range(0, num_params):
            msg = pubsub.get_message(timeout=1)
            env.assertEqual(msg['data'], 'json.mset')
            env.assertEqual(msg['type'], 'pmessage')
            msg = pubsub.get_message(timeout=1)
            env.assertEqual(msg['data'], f'k:{i}')
            env.assertEqual(msg['type'], 'pmessage')

    # verify JSON.MSET is appearing in the AOF once only
    role = 'master'
    aof_fn = env.envRunner._getFileName(role, '.aof.1.incr.aof')
    with open(f'{env.logDir}/appendonlydir/{aof_fn}', 'r') as fd:
        aof_content = [l for l in fd.readlines() if 'JSON.MSET' in l]
        assert(len(aof_content) == 1)


def test_recursive_descent(env):
    r = env
    r.expect('JSON.SET', 'k', '$', '[{"a":1}]').ok()
    r.expect('JSON.SET', 'k', '$..*', '[{"a":1}]').ok()
    r.expect('JSON.GET', 'k', '$').equal('[[[{"a":1}]]]')

def test_json_del_matches_with_numeric_pathes(env):
    r = env
    r.expect(
        "JSON.SET",
        "k",
        "$",
        '[{"x":1},{"x":2},{"x":3},{"x":4},{"x":5},{"x":6},{"x":7},{"x":8},{"x":9},{"x":10},{"x":11},{"x":12}]',
    ).ok()
    r.expect("JSON.DEL", "k", "$[?(@.x>0)]").equal(12)

    r.expect(
        "JSON.SET",
        "k",
        "$",
        '[{"x":11},{"x":2},{"x":3},{"x":4},{"x":5},{"x":6},{"x":7},{"x":8},{"x":9},{"x":10},{"x":20},{"x":30},{"x":40},{"x":50},{"x":60},{"x":70},{"x":80},{"x":90},{"x":100}]',
    ).ok()
    r.expect("JSON.DEL", "k", "$[?(@.x>10)]").equal(10)

    r.expect(
        "JSON.SET",
        "k",
        "$",
        '[{"x":10},{"x":20},{"x":30},{"x":40},{"x":50},{"x":60},{"x":70},{"x":80},{"x":90},{"x":100},{"x":9},{"x":120}]',
    ).ok()
    r.expect("JSON.DEL", "k", "$[?(@.x>10)]").equal(10)

    r.expect(
        "JSON.SET",
        "k",
        "$",
        '[{"x":11}, {"x":11},{"x":11},{"x":11},{"x":11},{"x":11},{"x":11},{"x":11},{"x":11},{"x":11},{"x":11},{"x":11},{"x":11},{"x":11},{"x":11},{"x":11},{"x":11},{"x":11},{"x":11},{"x":11},{"x":11}]',
    ).ok()
    r.expect("JSON.DEL", "k", "$[?(@.x>10)]").equal(21)

def test_json_del_matches_with_object_pathes(env):
    r = env
    r.expect("JSON.SET", "test", "$", '{"root" : {"1" : {"value":1},"2" : {"value":2},"3" : {"value":3},"4" : {"value":4},"5" : {"value":5}}}').ok()
    r.expect("JSON.DEL", "test", "$.root[?(@.value > 2)]").equal(3)
    r.expect("JSON.GET", "test", "$").equal('[{"root":{"1":{"value":1},"2":{"value":2}}}]')

def testArrtNumericArrayTypesOperations(env):
    """Test commands with numeric array types"""
    r = env

    numeric_types = {
        # type -> (array, index to check, value to insert)
        'I8'   : ([-128, 0, 127, -1], 2, -64),     
        'U8'   : ([180, 181, 160, 42], 2, 75),       
        'I16'  : ([-32768, 0, 32767, -12345], 2, 12345),
        'U16'  : ([32768, 65535, 65534], 2, 54321), 
        'I32'  : ([-214748364, 0, 214748364, -100000], 2, 100000),
        'U32'  : ([2147483648, 2147483650, 3147483648, 3247483648], 2, 3147483649),  
        'F32'  : ([0.0, 1.5, -113.75, 0.74], 2, 2.71828),       
        'I64'  : ([0, 3294967295, -4294967295], 2, 12345645), 
        'U64'  : ([9223372036854775808, 9323372036854775809, 9423372036854775909, 9523372036854775909], 2, 25666),
        'F64'  : ([0.0, 1.5, -1.7e308, 1.7e308], 2, 3.141592653589793),
    }
    for (numeric_type, (array, index, value)) in numeric_types.items():
        r.assertOk(r.execute_command('JSON.SET', f'test_{numeric_type}',
                                    '.', json.dumps(array)))
        r.assertEqual(r.execute_command('JSON.ARRINDEX', f'test_{numeric_type}', '.', array[index]), index)
        r.assertEqual(r.execute_command('JSON.ARRAPPEND', f'test_{numeric_type}', '.', value), len(array) + 1)
        r.assertEqual(r.execute_command('JSON.ARRINDEX', f'test_{numeric_type}', '.', value), len(array))
        r.assertEqual(r.execute_command('JSON.GET', f'test_{numeric_type}', f'.[{len(array)}]'), str(value))
        r.assertEqual(r.execute_command('JSON.NUMINCRBY', f'test_{numeric_type}', f'.[{len(array)}]', 1), str(value + 1))
        r.assertEqual(r.execute_command('JSON.GET', f'test_{numeric_type}', f'.[{len(array)}]'), str(value + 1))
        r.assertEqual(r.execute_command('JSON.NUMINCRBY', f'test_{numeric_type}', f'.[{len(array)}]', 1.0), str(float(value + 2)))
        r.assertEqual(r.execute_command('JSON.GET', f'test_{numeric_type}', f'.[{len(array)}]'), str(float(value + 2)))
        r.assertOk(r.execute_command('JSON.SET', f'test_{numeric_type}', '.[0]', value))
        r.assertEqual(r.execute_command('JSON.GET', f'test_{numeric_type}', f'.[0]'), str(value))
        r.assertEqual(r.execute_command('JSON.ARRINSERT', f'test_{numeric_type}', '.', 0, value), len(array) + 2)
        r.assertEqual(r.execute_command('JSON.GET', f'test_{numeric_type}', f'.[0]'), str(value))
        r.assertEqual(r.execute_command('JSON.ARRTRIM', f'test_{numeric_type}', '.', 0, len(array) + 2), len(array) + 2)

def testArrNumericArrayNumericOperstionsWithDoubleValues(env):
    r = env
    r.assertOk(r.execute_command('JSON.SET', 'test', '.', '[1,2,3,4,5]'))
    r.assertEqual(r.execute_command('JSON.NUMINCRBY', 'test', '.[0]', 1.5), str(2.5))
    r.assertEqual(r.execute_command('JSON.GET', 'test', '.[0]'), str(2.5))
    r.assertEqual(r.execute_command('JSON.NUMMULTBY', 'test', '.[1]', 3.2), str(6.4))
    r.assertEqual(r.execute_command('JSON.GET', 'test', '.[1]'), str(6.4))
   

# class CacheTestCase(BaseReJSONTest):
#     @property
#     def module_args(env):
#         return ['CACHE', 'ON']
#
#     def testLruCache(self):
#         def cacheItems():
#             return getCacheInfo(r)['items']
#         def cacheBytes():
#             return getCacheInfo(r)['bytes']
#
#         r.cmd('JSON.SET', 'myDoc', '.', json.dumps({
#             'foo': 'fooValue',
#             'bar': 'barValue',
#             'baz': 'bazValue',
#             'key\\': 'escapedKey'
#         }))
#
#         res = r.cmd('JSON.GET', 'myDoc', 'foo')
#         r.assertEqual(1, cacheItems())
#         r.assertEqual('"fooValue"', res)
#         r.assertEqual('"fooValue"', r.cmd('JSON.GET', 'myDoc', 'foo'))
#         r.assertEqual('"fooValue"', r.cmd('JSON.GET', 'myDoc', '.foo'))
#         # Get it again - item count should be the same
#         r.cmd('JSON.GET', 'myDoc', 'foo')
#         r.assertEqual(1, cacheItems())
#
#         res = r.cmd('JSON.GET', 'myDoc', '.')
#         # print repr(json.loads(res))
#         r.assertEqual({u'bar': u'barValue', u'foo': u'fooValue', u'baz': u'bazValue', u'key\\': u'escapedKey'},
#                          json.loads(res))
#
#         # Try to issue multiple gets
#         r.cmd('JSON.GET', 'myDoc', '.foo')
#         r.cmd('JSON.GET', 'myDoc', 'foo')
#         r.cmd('JSON.GET', 'myDoc', '.bar')
#         r.cmd('JSON.GET', 'myDoc', 'bar')
#
#         res = r.cmd('JSON.GET', 'myDoc', '.foo', 'foo', '.bar', 'bar', '["key\\"]')
#         # print repr(json.loads(res))
#         r.assertEqual({u'.foo': u'fooValue', u'foo': u'fooValue', u'bar': u'barValue', u'.bar': u'barValue', u'["key\\"]': u'escapedKey'}, json.loads(res))
#
#         r.cmd('JSON.DEL', 'myDoc', '.')
#         r.assertEqual(0, cacheItems())
#         r.assertEqual(0, cacheBytes())
#
#         # Try with an array document
#         r.cmd('JSON.SET', 'arr', '.', '[{}, 1,2,3,4]')
#         r.assertEqual('{}', r.cmd('JSON.GET', 'arr', '[0]'))
#         r.assertEqual(1, cacheItems())
#         r.assertEqual('{}', r.cmd('JSON.GET', 'arr', '[0]'))
#         r.assertEqual(1, cacheItems())
#         r.assertEqual('{}', r.cmd('JSON.GET', 'arr', '[0]'))
#
#         r.assertEqual('[{},1,2,3,4]', r.cmd('JSON.GET', 'arr', '.'))
#         r.assertEqual(2, cacheItems())
#
#         r.cmd('JSON.SET', 'arr', '[0].key', 'null')
#         r.assertEqual(0, cacheItems())
#
#         r.assertEqual('null', r.cmd('JSON.GET', 'arr', '[0].key'))
#         # NULL is still not cached!
#         r.assertEqual(0, cacheItems())
#
#         # Try with a document that contains top level object with an array child
#         r.cmd('JSON.DEL', 'arr', '.')
#         r.cmd('JSON.SET', 'mixed', '.', '{"arr":[{},\"Hello\",2,3,null]}')
#         r.assertEqual("\"Hello\"", r.cmd('JSON.GET', 'mixed', '.arr[1]'))
#         r.assertEqual(1, cacheItems())
#
#         r.cmd('JSON.ARRAPPEND', 'mixed', 'arr', '42')
#         r.assertEqual(0, cacheItems())
#         r.assertEqual("\"Hello\"", r.cmd('JSON.GET', 'mixed', 'arr[1]'))
#
#         # Test cache eviction
#         r.cmd('json._cacheinit', 4096, 20, 0)
#         keys = ['json_{}'.format(x) for x in range(10)]
#         paths = ['path_{}'.format(x) for x in xrange(100)]
#         doc = json.dumps({ p: "some string" for p in paths})
#
#         # 100k different path/key combinations
#         for k in keys:
#             r.cmd('JSON.SET', k, '.', doc)
#
#         # Now get 'em back all
#         for k in keys:
#             for p in paths:
#                 r.cmd('JSON.GET', k, p)
#         r.assertEqual(20, cacheItems())
#
#         r.cmd('json._cacheinit')

# class NoCacheTestCase(BaseReJSONTest):
#     def testNoCache(self):
#         def cacheItems():
#             return getCacheInfo(r)['items']
#         def cacheBytes():
#             return getCacheInfo(r)['bytes']
#
#         r.cmd('JSON.SET', 'myDoc', '.', json.dumps({
#             'foo': 'fooValue',
#             'bar': 'barValue',
#             'baz': 'bazValue',
#             'key\\': 'escapedKey'
#         }))
#
#         res = r.cmd('JSON.GET', 'myDoc', 'foo')
#         r.assertEqual(0, cacheItems())
