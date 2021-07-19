# -*- coding: utf-8 -*-

import unittest
from includes import *
from common import getConnectionByEnv, waitForIndex, sortedResults, toSortedFlatList
from time import sleep
from RLTest import Env

def testSyntax1(env):
    conn = getConnectionByEnv(env)
    env.expect('ft.create', 'idx',
               'ONfoo*',
               'SCHEMA', 'foo', 'text').equal('Unknown argument `ONfoo*`')

    env.expect('ft.create', 'idx2',
               'LANGUAGE', 'eng'
               'SCHEMA', 'foo', 'text').equal('Invalid language')

    env.expect('ft.create', 'idx2',
               'SCORE', '1.0'
               'SCHEMA', 'foo', 'text').equal('Unknown argument `foo`')

    env.expect('ft.create', 'idx2',
               'PAYLOAD_FIELD', 'awfw'
               'SCHEMA', 'foo', 'text').equal('Unknown argument `foo`')

    env.expect('ft.create', 'idx2',
               'FILTER', 'a'
               'SCHEMA', 'foo', 'text').equal("Unknown symbol 'aSCHEMA'")

def testFilter1(env):
    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'things',
            'ON', 'HASH',
            'FILTER', 'startswith(@__key, "thing:")',
            'SCHEMA', 'name', 'text')

    conn.execute_command('hset', 'thing:bar', 'name', 'foo')

    env.expect('ft.search', 'things', 'foo') \
       .equal([1L, 'thing:bar', ['name', 'foo']])

def testPrefix0a(env):
    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'things', 'ON', 'HASH',
            'PREFIX', '1', '',
            'SCHEMA', 'name', 'text')

    conn.execute_command('hset', 'thing:bar', 'name', 'foo')
    env.expect('ft.search', 'things', 'foo').equal([1L, 'thing:bar', ['name', 'foo']])

def testPrefix0b(env):
    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'things', 'ON', 'HASH', 'SCHEMA', 'name', 'text')
    conn.execute_command('hset', 'thing:bar', 'name', 'foo')
    env.expect('ft.search', 'things', 'foo').equal([1L, 'thing:bar', ['name', 'foo']])

def testPrefix1(env):
    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'things', 'ON', 'HASH',
            'PREFIX', '1', 'thing:',
            'SCHEMA', 'name', 'text')

    conn.execute_command('hset', 'thing:bar', 'name', 'foo')

    env.expect('ft.search', 'things', 'foo') \
       .equal([1L, 'thing:bar', ['name', 'foo']])

def testPrefix2(env):
    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'things', 'ON', 'HASH',
            'PREFIX', '2', 'this:', 'that:',
            'SCHEMA', 'name', 'text')

    conn.execute_command('hset', 'this:foo', 'name', 'foo')
    conn.execute_command('hset', 'that:foo', 'name', 'foo')

    res = env.cmd('ft.search', 'things', 'foo')
    env.assertIn('that:foo', res)
    env.assertIn('this:foo', res)

def testFilter2(env):
    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'stuff', 'ON', 'HASH',
            'FILTER', 'startswith(@__key, "stuff:")',
            'SCHEMA', 'name', 'text', 'age', 'numeric')

    env.cmd('ft.create', 'things', 'ON', 'HASH',
            'FILTER', 'startswith(@__key, "thing:")',
            'SCHEMA', 'name', 'text', 'age', 'numeric')

    conn.execute_command('hset', 'thing:bar', 'name', 'foo')
    conn.execute_command('hset', 'object:jojo', 'name', 'vivi')
    conn.execute_command('hset', 'thing:bar', 'age', '42')

    env.expect('ft.search', 'things', 'foo') \
       .equal([1L, 'thing:bar', ['name', 'foo', 'age', '42']])

def testPrefix3(env):
    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'stuff',
            'ON', 'HASH',
            'PREFIX', '1', 'stuff:',
            'SCHEMA', 'name', 'text', 'age', 'numeric')

    env.cmd('ft.create', 'things', 'ON', 'HASH',
            'PREFIX', '1', 'thing:',
            'SCHEMA', 'name', 'text', 'age', 'numeric')

    conn.execute_command('hset', 'thing:bar', 'name', 'foo')
    conn.execute_command('hset', 'object:jojo', 'name', 'vivi')
    conn.execute_command('hset', 'thing:bar', 'age', '42')

    env.expect('ft.search', 'things', 'foo') \
       .equal([1L, 'thing:bar', ['name', 'foo', 'age', '42']])

def testIdxField(env):
    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'idx1',
            'ON', 'HASH',
            'PREFIX', 1, 'doc',
            'FILTER', '@indexName=="idx1"',
            'SCHEMA', 'name', 'text', 'indexName', 'text')
    env.cmd('ft.create', 'idx2',
            'ON', 'HASH',
            'FILTER', '@indexName=="idx2"',
            'SCHEMA', 'name', 'text', 'indexName', 'text')

    conn.execute_command('hset', 'doc1', 'name', 'foo', 'indexName', 'idx1')
    conn.execute_command('hset', 'doc2', 'name', 'bar', 'indexName', 'idx2')

    env.expect('ft.search', 'idx1', '*').equal([1L, 'doc1', ['name', 'foo', 'indexName', 'idx1']])
    env.expect('ft.search', 'idx2', '*').equal([1L, 'doc2', ['name', 'bar', 'indexName', 'idx2']])

def testDel(env):
    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'things', 'ON', 'HASH',
            'PREFIX', '1', 'thing:',
            'SCHEMA', 'name', 'text')

    env.expect('ft.search', 'things', 'foo').equal([0L])
    conn.execute_command('hset', 'thing:bar', 'name', 'foo')
    env.expect('ft.search', 'things', 'foo').equal([1L, 'thing:bar', ['name', 'foo']])
    conn.execute_command('del', 'thing:bar')
    env.expect('ft.search', 'things', 'foo').equal([0L])

def testSet(env):
    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'things',
            'PREFIX', '1', 'thing:',
            'SCHEMA', 'name', 'text')

    env.expect('ft.search', 'things', 'foo').equal([0L])
    conn.execute_command('hset', 'thing:bar', 'name', 'foo')
    env.expect('ft.search', 'things', 'foo').equal([1L, 'thing:bar', ['name', 'foo']])
    env.expect('set', 'thing:bar', "bye bye")
    env.expect('ft.search', 'things', 'foo').equal([0L])

def testRename(env):
    env.skipOnCluster()
    conn = getConnectionByEnv(env)
    env.cmd('ft.create things PREFIX 1 thing: SCHEMA name text')
    env.expect('ft.search things foo').equal([0L])

    conn.execute_command('hset thing:bar name foo')
    env.expect('ft.search things foo').equal([1L, 'thing:bar', ['name', 'foo']])

    env.expect('RENAME thing:bar thing:foo').ok()
    env.expect('ft.search things foo').equal([1L, 'thing:foo', ['name', 'foo']])

    env.cmd('ft.create otherthings PREFIX 1 otherthing: SCHEMA name text')
    env.expect('RENAME thing:foo otherthing:foo').ok()
    env.expect('ft.search things foo').equal([0L])
    env.expect('ft.search otherthings foo').equal([1L, 'otherthing:foo', ['name', 'foo']])

    env.cmd('SET foo bar')
    env.cmd('RENAME foo fubu')

def testFlush(env):
    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'things', 'ON', 'HASH',
            'PREFIX', '1', 'thing:',
            'FILTER', 'startswith(@__key, "thing:")',
            'SCHEMA', 'name', 'text')

    conn.execute_command('FLUSHALL')

    conn.execute_command('hset', 'thing:bar', 'name', 'foo')

    env.expect('ft.search', 'things', 'foo').equal('things: no such index')

def testNotExist(env):
    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'things', 'ON', 'HASH',
            'PREFIX', '1', 'thing:',
            'FILTER', 'startswith(@__key, "thing:")',
            'SCHEMA', 'txt', 'text')

    conn.execute_command('hset', 'thing:bar', 'not_text', 'foo')
    env.expect('ft.search', 'things', 'foo').equal([0L])

def testPayload(env):
    conn = getConnectionByEnv(env)
    env.expect('ft.create', 'things', 'ON', 'HASH',
                'PREFIX', '1', 'thing:',
                'PAYLOAD_FIELD', 'payload',
                'SCHEMA', 'name', 'text').ok()
    conn.execute_command('hset', 'thing:foo', 'name', 'foo', 'payload', 'stuff')

    for _ in env.retry_with_rdb_reload():
        waitForIndex(env, 'things')
        res = env.cmd('ft.search', 'things', 'foo')
        env.assertEqual(toSortedFlatList(res), toSortedFlatList([1L, 'thing:foo', ['name', 'foo']]))

        res = env.cmd('ft.search', 'things', 'foo', 'withpayloads')
        env.assertEqual(toSortedFlatList(res), toSortedFlatList([1L, 'thing:foo', 'stuff', ['name', 'foo']]))

def testBinaryPayload(env):
    conn = getConnectionByEnv(env)
    env.expect('ft.create', 'things', 'ON', 'HASH',
                'PREFIX', '1', 'thing:',
                'PAYLOAD_FIELD', 'payload',
                'SCHEMA', 'name', 'text').ok()
    conn.execute_command('hset', 'thing:foo', 'name', 'foo', 'payload', '\x00\xAB\x20')

    for _ in env.retry_with_rdb_reload():
        waitForIndex(env, 'things')
        res = env.cmd('ft.search', 'things', 'foo')
        env.assertEqual(toSortedFlatList(res), toSortedFlatList([1L, 'thing:foo', ['name', 'foo']]))

        res = env.cmd('ft.search', 'things', 'foo', 'withpayloads')
        env.assertEqual(toSortedFlatList(res), toSortedFlatList([1L, 'thing:foo', '\x00\xAB\x20', ['name', 'foo']]))

def testDuplicateFields(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH',
               'SCHEMA', 'txt', 'TEXT', 'num', 'NUMERIC', 'SORTABLE').ok()
    env.cmd('FT.ADD', 'idx', 'doc', 1.0,
            'FIELDS', 'txt', 'foo', 'txt', 'bar', 'txt', 'baz')
    env.expect('ft.search', 'idx', 'baz').equal([1L, 'doc', ['txt', 'baz']])
    env.expect('ft.search', 'idx', 'foo').equal([0L])

def testReplace(env):
    conn = getConnectionByEnv(env)
    r = env

    r.expect('ft.create idx schema f text').ok()

    res = conn.execute_command('HSET', 'doc1', 'f', 'hello world')
    env.assertEqual(res, 1)
    res = conn.execute_command('HSET', 'doc2', 'f', 'hello world')
    env.assertEqual(res, 1)
    res = r.execute_command('ft.search', 'idx', 'hello world')
    r.assertEqual(2, res[0])

    # now replace doc1 with a different content
    res = conn.execute_command('HSET', 'doc1', 'f', 'goodbye universe')
    env.assertEqual(res, 0)

    for _ in r.retry_with_rdb_reload():
        waitForIndex(env, 'idx')
        # make sure the query for hello world does not return the replaced document
        r.expect('ft.search', 'idx', 'hello world', 'nocontent').equal([1, 'doc2'])

        # search for the doc's new content
        r.expect('ft.search', 'idx', 'goodbye universe', 'nocontent').equal([1, 'doc1'])

def testSortable(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'FILTER', 'startswith(@__key, "")',
                'SCHEMA', 'test', 'TEXT', 'SORTABLE').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo1').equal('OK')

def testMissingArgs(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'SCHEMA', 'txt', 'TEXT', 'num', 'NUMERIC').error()
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'FILTER', 'SCHEMA', 'txt', 'TEXT', 'num', 'NUMERIC').error()

def testWrongArgs(env):
    env.expect('FT.CREATE', 'idx', 'SCORE', 'SCHEMA', 'txt', 'TEXT', 'num', 'NUMERIC').error().contains('Invalid score')
    env.expect('FT.CREATE', 'idx', 'SCORE', 10, 'SCHEMA', 'txt', 'TEXT', 'num', 'NUMERIC').error().contains('Invalid score')
    env.expect('FT.CREATE', 'idx', 'LANGUAGE', 'SCHEMA', 'txt', 'TEXT', 'num', 'NUMERIC').error().contains('Invalid language')
    env.expect('FT.CREATE', 'idx', 'LANGUAGE', 'none', 'SCHEMA', 'txt', 'TEXT', 'num', 'NUMERIC').error().contains('Invalid language')

def testLanguageDefaultAndField(env):
    conn = getConnectionByEnv(env)
    env.cmd('FT.CREATE', 'idxTest1', 'LANGUAGE_FIELD', 'lang', 'SCHEMA', 'body', 'TEXT')
    env.cmd('FT.CREATE', 'idxTest2', 'LANGUAGE', 'hindi', 'SCHEMA', 'body', 'TEXT')
    conn.execute_command('HSET', 'doc1', 'lang', 'hindi', 'body', u'अँगरेजी अँगरेजों अँगरेज़')

    for _ in env.retry_with_rdb_reload():
        waitForIndex(env, 'idxTest1')
        waitForIndex(env, 'idxTest2')
        #test for language field
        res = env.cmd('FT.SEARCH', 'idxTest1', u'अँगरेज़')
        res1 = {res[2][i]:res[2][i + 1] for i in range(0, len(res[2]), 2)}
        env.assertEqual(u'अँगरेजी अँगरेजों अँगरेज़', unicode(res1['body'], 'utf-8'))
        # test for default langauge
        res = env.cmd('FT.SEARCH', 'idxTest2', u'अँगरेज़')
        res1 = {res[2][i]:res[2][i + 1] for i in range(0, len(res[2]), 2)}
        env.assertEqual(u'अँगरेजी अँगरेजों अँगरेज़', unicode(res1['body'], 'utf-8'))

def testScoreDecimal(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx1', 'SCORE', '0.5', 'schema', 'title', 'text').ok()
    env.expect('FT.CREATE', 'idx2', 'SCORE_FIELD', 'score', 'schema', 'title', 'text').ok()
    res = conn.execute_command('HSET', 'doc1', 'title', 'hello', 'score', '0.25')
    env.assertEqual(res, 2)

    for _ in env.retry_with_rdb_reload():
        waitForIndex(env, 'idx1')
        waitForIndex(env, 'idx2')
        res = env.cmd('ft.search', 'idx1', 'hello', 'withscores', 'nocontent')
        env.assertEqual(float(res[2]), 0.5)
        res = env.cmd('ft.search', 'idx2', 'hello', 'withscores', 'nocontent')
        env.assertEqual(float(res[2]), 0.25)

def testMultiFilters1(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'test', 'ON', 'HASH',
               'PREFIX', '2', 'student:', 'pupil:',
               'FILTER', 'startswith(@__key, "student:")',
               'SCHEMA', 'first', 'TEXT', 'last', 'TEXT', 'age', 'NUMERIC').ok()
    conn.execute_command('HSET', 'student:yes1', 'first', 'yes1', 'last', 'yes1', 'age', '17')
    conn.execute_command('HSET', 'student:yes2', 'first', 'yes2', 'last', 'yes2', 'age', '15')
    conn.execute_command('HSET', 'pupil:no1', 'first', 'no1', 'last', 'no1', 'age', '17')
    conn.execute_command('HSET', 'pupil:no2', 'first', 'no2', 'last', 'no2', 'age', '15')
    res1 = [2L, 'student:yes2', ['first', 'yes2', 'last', 'yes2', 'age', '15'],
                'student:yes1', ['first', 'yes1', 'last', 'yes1', 'age', '17']]
    res = env.cmd('ft.search test *')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList(res1))

def testMultiFilters2(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'test', 'ON', 'HASH',
               'PREFIX', '2', 'student:', 'pupil:',
               'FILTER', '@age > 16',
               'SCHEMA', 'first', 'TEXT', 'last', 'TEXT', 'age', 'NUMERIC').ok()
    conn.execute_command('HSET', 'student:yes1', 'first', 'yes1', 'last', 'yes1', 'age', '17')
    conn.execute_command('HSET', 'student:no1', 'first', 'no1', 'last', 'no1', 'age', '15')
    conn.execute_command('HSET', 'pupil:yes2', 'first', 'yes2', 'last', 'yes2', 'age', '17')
    conn.execute_command('HSET', 'pupil:no2', 'first', 'no2', 'last', 'no2', 'age', '15')
    res1 = [2L, 'pupil:yes2', ['first', 'yes2', 'last', 'yes2', 'age', '17'],
                'student:yes1', ['first', 'yes1', 'last', 'yes1', 'age', '17']]
    res = env.cmd('ft.search test *')
    env.assertEqual(toSortedFlatList(res), toSortedFlatList(res1))

def testInfo(env):
    env.skipOnCluster()

    env.expect('FT.CREATE', 'test', 'ON', 'HASH',
               'PREFIX', '2', 'student:', 'pupil:',
               'FILTER', '@age > 16',
               'language', 'hindi',
               'language_field', 'lang',
               'score', '0.5',
               'score_field', 'score',
               'payload_field', 'pl',
               'SCHEMA', 't', 'TEXT').ok()
    res_actual = env.cmd('FT.INFO test')
    res_expected = ['key_type', 'HASH',
                    'prefixes', ['student:', 'pupil:'],
                    'filter', '@age > 16',
                    'default_language', 'hindi',
                    'language_field', 'lang',
                    'default_score', '0.5',
                    'score_field', 'score',
                    'payload_field', 'pl']
    env.assertEqual(res_actual[5], res_expected)

    env.expect('ft.drop test').ok()

    env.expect('FT.CREATE', 'test', 'SCHEMA', 't', 'TEXT').ok()
    res_actual = env.cmd('FT.INFO test')
    res_expected = ['key_type', 'HASH',
                    'prefixes', [''],
                    'default_score', '1']
    env.assertEqual(res_actual[5], res_expected)

def testCreateDropCreate(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('hset', 'thing:bar', 'name', 'foo')
    env.expect('ft.create', 'things', 'ON', 'HASH',
               'PREFIX', '1', 'thing:', 'SCHEMA', 'name', 'text').ok()
    waitForIndex(conn, 'things')
    env.expect('ft.search', 'things', 'foo') \
       .equal([1L, 'thing:bar', ['name', 'foo']])
    env.expect('ft.dropindex things').ok()
    env.expect('ft.create', 'things', 'ON', 'HASH',
               'PREFIX', '1', 'thing:', 'SCHEMA', 'name', 'text').ok()
    waitForIndex(conn, 'things')
    env.expect('ft.search', 'things', 'foo') \
       .equal([1L, 'thing:bar', ['name', 'foo']])

def testPartial(env):
    if env.env == 'existing-env':
        env.skip()
    env.skipOnCluster()
    env = Env(moduleArgs='PARTIAL_INDEXED_DOCS 1')

    # HSET
    env.expect('FT.CREATE idx SCHEMA test TEXT').equal('OK')
    env.expect('HSET doc1 test foo').equal(1)
    env.expect('FT.DEBUG docidtoid idx doc1').equal(1)
    env.expect('HSET doc1 testtest foo').equal(1)
    env.expect('FT.DEBUG docidtoid idx doc1').equal(1)
    env.expect('HSET doc1 test bar').equal(0)
    env.expect('FT.DEBUG docidtoid idx doc1').equal(2)
    env.expect('FT.SEARCH idx bar').equal([1L, 'doc1', ['test', 'bar', 'testtest', 'foo']])

    # HMSET
    env.expect('HMSET doc2 test foo').ok()
    env.expect('FT.DEBUG docidtoid idx doc2').equal(3)
    env.expect('HMSET doc2 testtest foo').ok()
    env.expect('FT.DEBUG docidtoid idx doc2').equal(3)
    env.expect('HMSET doc2 test baz').ok()
    env.expect('FT.DEBUG docidtoid idx doc2').equal(4)
    env.expect('FT.SEARCH idx baz').equal([1L, 'doc2', ['test', 'baz', 'testtest', 'foo']])

    # HSETNX
    env.expect('HSETNX doc3 test foo').equal(1)
    env.expect('FT.DEBUG docidtoid idx doc3').equal(5)
    env.expect('HSETNX doc3 testtest foo').equal(1)
    env.expect('FT.DEBUG docidtoid idx doc3').equal(5)
    env.expect('HSETNX doc3 test bad').equal(0)
    env.expect('FT.DEBUG docidtoid idx doc3').equal(5)
    env.expect('FT.SEARCH idx foo').equal([1L, 'doc3', ['test', 'foo', 'testtest', 'foo']])

    # HINCRBY
    env.expect('HINCRBY doc4 test 5').equal(5)
    env.expect('FT.DEBUG docidtoid idx doc4').equal(6)
    env.expect('HINCRBY doc4 testtest 5').equal(5)
    env.expect('FT.DEBUG docidtoid idx doc4').equal(6)
    env.expect('HINCRBY doc4 test 6').equal(11)
    env.expect('FT.DEBUG docidtoid idx doc4').equal(7)
    env.expect('HINCRBY doc4 test 5.5').error(). contains('value is not an integer or out of range')
    env.expect('FT.DEBUG docidtoid idx doc4').equal(7)
    env.expect('FT.SEARCH idx 11').equal([1L, 'doc4', ['test', '11', 'testtest', '5']])

    # HINCRBYFLOAT
    env.expect('HINCRBYFLOAT doc5 test 5.5').equal('5.5')
    env.expect('FT.DEBUG docidtoid idx doc5').equal(8)
    env.expect('HINCRBYFLOAT doc5 testtest 5.5').equal('5.5')
    env.expect('FT.DEBUG docidtoid idx doc5').equal(8)
    env.expect('HINCRBYFLOAT doc5 test 6.6').equal('12.1')
    env.expect('FT.DEBUG docidtoid idx doc5').equal(9)
    env.expect('HINCRBYFLOAT doc5 test 5').equal('17.1')
    env.expect('FT.DEBUG docidtoid idx doc5').equal(10)
    env.expect('FT.SEARCH idx *').equal([5L, 'doc1', ['test', 'bar', 'testtest', 'foo'],
                                             'doc2', ['test', 'baz', 'testtest', 'foo'],
                                             'doc3', ['test', 'foo', 'testtest', 'foo'],
                                             'doc4', ['test', '11', 'testtest', '5'],
                                             'doc5', ['test', '17.1', 'testtest', '5.5']])

def testHDel(env):
    if env.env == 'existing-env':
        env.skip()
    env.skipOnCluster()
    env = Env(moduleArgs='PARTIAL_INDEXED_DOCS 1')

    env.expect('FT.CREATE idx SCHEMA test1 TEXT test2 TEXT').equal('OK')
    env.expect('FT.CREATE idx2 SCHEMA test1 TEXT test2 TEXT').equal('OK')
    env.expect('HSET doc1 test1 foo test2 bar test3 baz').equal(3)
    env.expect('FT.DEBUG docidtoid idx doc1').equal(1)
    env.expect('HDEL doc1 test1').equal(1)
    env.expect('FT.DEBUG docidtoid idx doc1').equal(2)
    env.expect('HDEL doc1 test3').equal(1)
    env.expect('FT.DEBUG docidtoid idx doc1').equal(2)
    env.expect('FT.SEARCH idx bar').equal([1L, 'doc1', ['test2', 'bar']])
    env.expect('HDEL doc1 test2').equal(1)
    env.expect('FT.SEARCH idx bar').equal([0L])

def testRestore(env):
    if env.env == 'existing-env':
        env.skip()
    env.skipOnCluster()
    env.expect('FT.CREATE idx SCHEMA test TEXT').equal('OK')
    env.expect('HSET doc1 test foo').equal(1)
    env.expect('FT.SEARCH idx foo').equal([1L, 'doc1', ['test', 'foo']])
    dump = env.cmd('dump doc1')
    env.expect('DEL doc1').equal(1)
    env.expect('FT.SEARCH idx foo').equal([0L])
    env.expect('RESTORE', 'doc1', 0, dump)
    env.expect('FT.SEARCH idx foo').equal([1L, 'doc1', ['test', 'foo']])

def testExpire(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE idx SCHEMA test TEXT').equal('OK')
    conn.execute_command('HSET', 'doc1', 'test', 'foo')
    env.expect('FT.SEARCH idx foo').equal([1L, 'doc1', ['test', 'foo']])
    conn.execute_command('EXPIRE', 'doc1', '1')
    env.expect('FT.SEARCH idx foo').equal([1L, 'doc1', ['test', 'foo']])
    sleep(1.1)
    env.expect('FT.SEARCH idx foo').equal([0L])

def testEvicted(env):
    env.skipOnCluster()
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE idx SCHEMA test TEXT').equal('OK')

    memory = 0
    info = conn.execute_command('INFO MEMORY')
    for line in info.splitlines():
        if 'used_memory:' in line:
            sub = line.split(':')
            memory = int(sub[1])

    conn.execute_command('CONFIG', 'SET', 'MAXMEMORY-POLICY', 'ALLKEYS-RANDOM')
    conn.execute_command('CONFIG', 'SET', 'MAXMEMORY', memory + 100000)
    for i in range(1000):
        env.expect('HSET', 'doc{}'.format(i), 'test', 'foo').equal(1)
    res = env.cmd('FT.SEARCH idx foo limit 0 0')
    env.assertLess(res[0], 1000)
    env.assertGreater(res[0], 0)

def createExpire(env, N):
  env.flush()
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE idx SCHEMA txt1 TEXT n NUMERIC').ok()
  for i in range(N):
    conn.execute_command('HSET', 'doc%d' % i, 'txt1', 'hello%i' % i, 'n', i)
    conn.execute_command('PEXPIRE', 'doc%d' % i, '100')
  conn.execute_command('HSET', 'foo', 'txt1', 'hello', 'n', 0)
  conn.execute_command('HSET', 'bar', 'txt1', 'hello', 'n', 20)
  waitForIndex(env, 'idx')
  env.expect('FT.SEARCH', 'idx', 'hello*', 'limit', '0', '0').noEqual([2L])
  res = conn.execute_command('HGETALL', 'doc99')
  if type(res) is list:
    res = {res[i]:res[i + 1] for i in range(0, len(res), 2)}
  env.assertEqual(res, {'txt1': 'hello99', 'n': '99'})
  sleep(0.1)
  res = conn.execute_command('HGETALL', 'doc99')
  if isinstance(res, list):
    res = {res[i]:res[i + 1] for i in range(0, len(res), 2)}
  env.assertEqual(res, {})

def testExpiredDuringSearch(env):
  N = 100
  createExpire(env, N)
  res = env.cmd('FT.SEARCH', 'idx', 'hello*', 'nocontent', 'limit', '0', '200')
  env.assertGreater(103, len(res))
  env.assertLess(1, len(res))

  createExpire(env, N)
  res = env.cmd('FT.SEARCH', 'idx', 'hello*', 'limit', '0', '200')
  env.assertEqual(toSortedFlatList(res[1:]), toSortedFlatList(['bar', ['txt1', 'hello', 'n', '20'], 
                                                               'foo', ['txt1', 'hello', 'n', '0']]))

def testExpiredDuringAggregate(env):
  N = 100
  res = [1L, ['txt1', 'hello', 'COUNT', '2']]
  
  createExpire(env, N)
  _res = env.cmd('FT.AGGREGATE idx hello*')
  env.assertGreater(len(_res), 2)

  createExpire(env, N)
  env.expect('FT.AGGREGATE idx hello* GROUPBY 1 @txt1 REDUCE count 0 AS COUNT').equal(res)

  createExpire(env, N)
  env.expect('FT.AGGREGATE idx hello* LOAD 1 @txt1 GROUPBY 1 @txt1 REDUCE count 0 AS COUNT').equal(res)

  createExpire(env, N)
  env.expect('FT.AGGREGATE idx @txt1:hello* LOAD 1 @txt1 GROUPBY 1 @txt1 REDUCE count 0 AS COUNT').equal(res)

def testSkipInitialScan(env):
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'a', 'test', 'hello', 'text', 'world')
    
    # Regular
    env.expect('FT.CREATE idx SCHEMA test TEXT').ok()
    waitForIndex(env, 'idx')
    env.expect('FT.SEARCH idx hello').equal([1L, 'a', ['test', 'hello', 'text', 'world']])
    # SkipInitialIndex
    env.expect('FT.CREATE idx_no_scan SKIPINITIALSCAN SCHEMA test TEXT').ok()
    waitForIndex(env, 'idx_no_scan')
    env.expect('FT.SEARCH idx_no_scan hello').equal([0L])
    # Temporary
    env.expect('FT.CREATE temp_idx TEMPORARY 10 SCHEMA test TEXT').ok()
    waitForIndex(env, 'temp_idx')
    env.expect('FT.SEARCH temp_idx hello').equal([1L, 'a', ['test', 'hello', 'text', 'world']])
    # Temporary & NoInitialIndex
    env.expect('FT.CREATE temp_idx_no_scan SKIPINITIALSCAN TEMPORARY 10 SCHEMA test TEXT').equal('OK')
    waitForIndex(env, 'temp_idx_no_scan')
    env.expect('FT.SEARCH temp_idx_no_scan hello').equal([0L])

def testWrongFieldType(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE idx SCHEMA t TEXT n NUMERIC').ok()
    conn.execute_command('HSET', 'a', 't', 'hello', 'n', '42')
    conn.execute_command('HSET', 'b', 't', 'hello', 'n', 'world')

    env.expect('FT.SEARCH idx hello').equal([1L, 'a', ['t', 'hello', 'n', '42']])

    res_actual = env.cmd('FT.INFO idx')
    res_actual = {res_actual[i]: res_actual[i + 1] for i in range(0, len(res_actual), 2)}
    env.assertEqual(str(res_actual['hash_indexing_failures']), '1')
    
def testDocIndexedInTwoIndexes():
    env = Env(moduleArgs='MAXDOCTABLESIZE 50')
    env.skipOnCluster()
    env.expect('FT.CREATE idx1 SCHEMA t TEXT').ok()
    env.expect('FT.CREATE idx2 SCHEMA t TEXT').ok()

    for i in range(1000):
        env.expect('HSET', 'doc%d' % i, 't', 'foo').equal(1L)

    env.expect('FT.DROPINDEX idx2 DD').ok()
    env.expect('FT.SEARCH idx1 foo').equal([0L])

    env.expect('FT.DROPINDEX idx1 DD').ok()

def testCountry(env):
    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'idx1',
            'PREFIX', 1, 'address:',
            'FILTER', '@country=="usa"',
            'SCHEMA', 'business', 'text', 'country', 'text')

    conn.execute_command('hset', 'address:1', 'business', 'foo', 'country', 'usa')
    conn.execute_command('hset', 'address:2', 'business', 'bar', 'country', 'israel')

    env.expect('ft.search', 'idx1', '*').equal([1L, 'address:1', ['business', 'foo', 'country', 'usa']])

def testIssue1571(env):
    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'idx',
            'FILTER', '@index=="yes"',
            'SCHEMA', 't', 'TEXT')

    conn.execute_command('hset', 'doc1', 't', 'foo1', 'index', 'yes')

    env.expect('ft.search', 'idx', 'foo*').equal([1L, 'doc1', ['t', 'foo1', 'index', 'yes']])

    conn.execute_command('hset', 'doc1', 'index', 'no')

    env.expect('ft.search', 'idx', 'foo*').equal([0L])

    conn.execute_command('hset', 'doc1', 't', 'foo2')

    env.expect('ft.search', 'idx', 'foo*').equal([0L])

    conn.execute_command('hset', 'doc1', 'index', 'yes')

    env.expect('ft.search', 'idx', 'foo*').equal([1L, 'doc1', ['t', 'foo2', 'index', 'yes']])

def testIssue1571WithRename(env):
    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'idx1',
            'PREFIX', '1', 'idx1',
            'FILTER', '@index=="yes"',
            'SCHEMA', 't', 'TEXT')
    env.cmd('ft.create', 'idx2',
            'PREFIX', '1', 'idx2',
            'FILTER', '@index=="yes"',
            'SCHEMA', 't', 'TEXT')

    conn.execute_command('hset', 'idx1:{doc}1', 't', 'foo1', 'index', 'yes')

    env.expect('ft.search', 'idx1', 'foo*').equal([1L, 'idx1:{doc}1', ['t', 'foo1', 'index', 'yes']])
    env.expect('ft.search', 'idx2', 'foo*').equal([0L])

    conn.execute_command('rename', 'idx1:{doc}1', 'idx2:{doc}1')

    env.expect('ft.search', 'idx2', 'foo*').equal([1L, 'idx2:{doc}1', ['t', 'foo1', 'index', 'yes']])
    env.expect('ft.search', 'idx1', 'foo*').equal([0L])

    conn.execute_command('hset', 'idx2:{doc}1', 'index', 'no')

    env.expect('ft.search', 'idx1', 'foo*').equal([0L])
    env.expect('ft.search', 'idx2', 'foo*').equal([0L])

    conn.execute_command('rename', 'idx2:{doc}1', 'idx1:{doc}1')

    env.expect('ft.search', 'idx1', 'foo*').equal([0L])
    env.expect('ft.search', 'idx2', 'foo*').equal([0L])

    conn.execute_command('hset', 'idx1:{doc}1', 'index', 'yes')

    env.expect('ft.search', 'idx1', 'foo*').equal([1L, 'idx1:{doc}1', ['t', 'foo1', 'index', 'yes']])
    env.expect('ft.search', 'idx2', 'foo*').equal([0L])


