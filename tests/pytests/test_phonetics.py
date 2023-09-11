import unittest
from includes import *
from common import getConnectionByEnv, toSortedFlatList, waitForIndex


def testBasicPoneticCase(env):
    env.assertOk(env.cmd('ft.create', 'idx', 'ON', 'HASH',
                         'schema', 'text', 'TEXT', 'PHONETIC', 'dm:en', 'SORTABLE'))
    env.assertOk(env.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields',
                           'text', 'morfix'))

    env.assertEquals(env.cmd('ft.search', 'idx', 'morphix'), [1, 'doc1', ['text', 'morfix']])
    env.assertEquals(env.cmd('ft.search', 'idx', '@text:morphix'), [1, 'doc1', ['text', 'morfix']])
    env.assertEquals(env.cmd('ft.search', 'idx', '@text:morphix=>{$phonetic:true}'), [1, 'doc1', ['text', 'morfix']])
    env.assertEquals(env.cmd('ft.search', 'idx', '@text:morphix=>{$phonetic:false}'), [0])

def testBasicPoneticWrongDeclaration(env):
    with env.assertResponseError():
        env.cmd('ft.create', 'idx', 'ON', 'HASH',
                'schema', 'text', 'TEXT', 'PHONETIC', 'something', 'SORTABLE')
    with env.assertResponseError():
        env.cmd('ft.create', 'idx', 'ON', 'HASH',
                'schema', 'text', 'TEXT', 'PHONETIC', 'kk:tt', 'SORTABLE')
    with env.assertResponseError():
        env.cmd('ft.create', 'idx', 'ON', 'HASH',
                'schema', 'text', 'TEXT', 'PHONETIC', 'dm:tt', 'SORTABLE')
    with env.assertResponseError():
        env.cmd('ft.create', 'idx', 'ON', 'HASH',
                'schema', 'text', 'TEXT', 'PHONETIC', 'll:en', 'SORTABLE')

def testPoneticOnNonePhoneticField(env):
    env.assertOk(env.cmd('ft.create', 'idx', 'ON', 'HASH',
                         'schema', 'text', 'TEXT', 'PHONETIC', 'dm:en', 'SORTABLE', 'text1', 'TEXT', 'SORTABLE'))
    env.assertOk(env.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields',
                           'text', 'morfix',
                           'text1', 'phonetic'))

    env.assertEquals(toSortedFlatList(env.cmd('ft.search', 'idx', 'morphix')), toSortedFlatList([1, 'doc1', ['text', 'morfix', 'text1', 'phonetic']]))
    env.assertEquals(toSortedFlatList(env.cmd('ft.search', 'idx', '@text:morphix')), toSortedFlatList([1, 'doc1', ['text', 'morfix', 'text1', 'phonetic']]))
    env.assertEquals(toSortedFlatList(env.cmd('ft.search', 'idx', 'phonetic')), toSortedFlatList([1, 'doc1', ['text', 'morfix', 'text1', 'phonetic']]))
    env.assertEquals(env.cmd('ft.search', 'idx', 'fonetic'), [0])
    env.assertEquals(env.cmd('ft.search', 'idx', '@text1:morphix'), [0])
    with env.assertResponseError():
        env.cmd('ft.search', 'idx', '@text1:morphix=>{$phonetic:true}')
    with env.assertResponseError():
        env.cmd('ft.search', 'idx', '@text1:morphix=>{$phonetic:false}')

def testPoneticWithAggregation(env):
    env.assertOk(env.cmd('ft.create', 'idx', 'ON', 'HASH',
                         'schema', 'text', 'TEXT', 'PHONETIC', 'dm:en', 'SORTABLE', 'text1', 'TEXT', 'SORTABLE'))
    env.assertOk(env.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields',
                           'text', 'morfix',
                           'text1', 'phonetic'))

    env.assertEquals(env.cmd('ft.aggregate', 'idx', 'morphix', 'LOAD', 2, '@text', '@text1'), [1, ['text', 'morfix', 'text1', 'phonetic']])
    env.assertEquals(env.cmd('ft.aggregate', 'idx', '@text:morphix', 'LOAD', 2, '@text', '@text1'), [1, ['text', 'morfix', 'text1', 'phonetic']])
    env.assertEquals(env.cmd('ft.aggregate', 'idx', 'phonetic', 'LOAD', 2, '@text', '@text1'), [1, ['text', 'morfix', 'text1', 'phonetic']])
    env.assertEquals(env.cmd('ft.aggregate', 'idx', '@text1:morphix', 'LOAD', 2, '@text', '@text1'), [0])
    if not env.is_cluster():
        with env.assertResponseError():
            env.cmd('ft.aggregate', 'idx', '@text1:morphix=>{$phonetic:true}')
        with env.assertResponseError():
            env.cmd('ft.aggregate', 'idx', '@text1:morphix=>{$phonetic:false}')
    else:
        raise unittest.SkipTest("FIXME: Aggregation error propagation broken on cluster mode")

def testPoneticWithSchemaAlter(env):
    #remove support 2.0
    env.skip()
    env.assertOk(env.cmd('ft.create', 'idx', 'ON', 'HASH',
                         'schema', 'text', 'TEXT', 'PHONETIC', 'dm:en', 'SORTABLE', 'text1', 'TEXT', 'SORTABLE'))
    env.assertOk(env.cmd('ft.alter', 'idx', 'SCHEMA', 'ADD', 'text2', 'TEXT', 'PHONETIC', 'dm:en'))

    env.assertOk(env.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields',
                           'text', 'morfix',
                           'text1', 'check',
                           'text2', 'phonetic'))

    env.assertEquals(env.cmd('ft.search', 'idx', 'fonetic'), [1, 'doc1', ['text', 'morfix', 'text1', 'check', 'text2', 'phonetic']])
    env.assertEquals(env.cmd('ft.search', 'idx', '@text2:fonetic'), [1, 'doc1', ['text', 'morfix', 'text1', 'check', 'text2', 'phonetic']])
    env.assertEquals(env.cmd('ft.search', 'idx', '@text1:fonetic'), [0])
    env.assertEquals(env.cmd('ft.search', 'idx', '@text2:fonetic=>{$phonetic:false}'), [0])
    env.assertEquals(env.cmd('ft.search', 'idx', '@text2:fonetic=>{$phonetic:true}'), [1, 'doc1', ['text', 'morfix', 'text1', 'check', 'text2', 'phonetic']])

def testPoneticWithSmallTerm(env):
    env.assertOk(env.cmd('ft.create', 'complainants', 'ON', 'HASH',
                         'SCHEMA', 'name', 'text', 'PHONETIC', 'dm:en', 'almamater', 'text', 'PHONETIC', 'dm:en'))

    env.assertOk(env.cmd('ft.add', 'complainants', 'foo64', 1.0, 'FIELDS', 'name', 'jon smith', 'almamater', 'Trent'))
    env.assertOk(env.cmd('ft.add', 'complainants', 'foo65', 1.0, 'FIELDS', 'name', 'john jones', 'almamater', 'Toronto'))

    res = env.cmd('ft.search', 'complainants', '@name:(john=>{$phonetic:true})')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList([2, 'foo64', ['name', 'jon smith', 'almamater', 'Trent'], 'foo65', ['name', 'john jones', 'almamater', 'Toronto']]))

def testPoneticOnNumbers(env):
    env.assertOk(env.cmd('ft.create', 'idx', 'ON', 'HASH',
                         'SCHEMA', 'test', 'TEXT', 'PHONETIC', 'dm:en'))
    env.assertOk(env.cmd('ft.add', 'idx', 'doc1', 1.0, 'FIELDS', 'test', 'this is 2015 test'))
    env.assertOk(env.cmd('ft.add', 'idx', 'doc2', 1.0, 'FIELDS', 'test', 'this is 04 test'))
    res = env.cmd('ft.search', 'idx', '04')
    env.assertEqual(res, [1, 'doc2', ['test', 'this is 04 test']])

def testIssue1313(env):
    env.expect('FT.CREATE test ON HASH SCHEMA topic2 TEXT NOINDEX topic TEXT PHONETIC dm:en').ok()
    env.expect('FT.SEARCH', 'test', '@topic:(tmp)=>{$phonetic:true}').equal([0])

    env.expect('FT.CREATE test1 ON HASH SCHEMA topic TEXT PHONETIC dm:en topic2 TEXT NOINDEX').ok()
    env.expect('FT.SEARCH', 'test1', '@topic:(tmp)=>{$phonetic: true}').equal([0])

def testIssue3836(env):
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE idx schema text TEXT PHONETIC dm:en SORTABLE').ok()
    waitForIndex(env, 'idx')
    conn.execute_command('HSET', 'doc1', 'text', 'morfix')
    res = conn.execute_command('FT.SEARCH', 'idx', '@text:morphix=>{$phonetic:true}')
    env.assertEqual(res, [1, 'doc1', ['text', 'morfix']])

    template = "@text:{0}=>{{$phonetic:true}}"
    poc = [
        "FT.SEARCH",
        "idx",
        template.format("A" * (65535*128)),
    ]
    res = env.execute_command(*poc)
    env.assertEqual(res, [0])

