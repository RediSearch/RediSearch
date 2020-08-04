# -*- coding: utf-8 -*-

import unittest
from includes import *
from common import getConnectionByEnv, waitForIndex, sortedResults

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

def testDel(env):
    conn = getConnectionByEnv(env)
    env.cmd('ft.create', 'things', 'ON', 'HASH',
            'PREFIX', '1', 'thing:',
            'SCHEMA', 'name', 'text')

    env.expect('ft.search', 'things', 'foo') \
       .equal([0L])

    conn.execute_command('hset', 'thing:bar', 'name', 'foo')

    env.expect('ft.search', 'things', 'foo') \
       .equal([1L, 'thing:bar', ['name', 'foo']])

    conn.execute_command('del', 'thing:bar')

    env.expect('ft.search', 'things', 'foo') \
       .equal([0L])

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
        env.expect('ft.search', 'things', 'foo') \
           .equal([1L, 'thing:foo', ['name', 'foo', 'payload', 'stuff']])

        env.expect('ft.search', 'things', 'foo', 'withpayloads') \
           .equal([1L, 'thing:foo', 'stuff', ['name', 'foo', 'payload', 'stuff']])

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

    r.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'f', 'text').ok()

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
    env.assertEqual(sortedResults(res), sortedResults(res1))

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
    env.assertEqual(sortedResults(res), sortedResults(res1))

def testInfo(env):
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
                    'language_field', '__language',
                    'default_score', '1',
                    'score_field', '__score',
                    'payload_field', '__payload']
    env.assertEqual(res_actual[5], res_expected)

def testPartial(env):
    env.expect('FT.CREATE idx SCHEMA test TEXT').equal('OK')
    env.expect('HSET doc1 test foo').equal(1)
    env.expect('FT.DEBUG docidtoid idx doc1').equal(1)
    env.expect('HSET doc1 testtest foo').equal(1)
    env.expect('FT.DEBUG docidtoid idx doc1').equal(1)
    env.expect('HSET doc1 test bar').equal(0)
    env.expect('FT.DEBUG docidtoid idx doc1').equal(2)

    env.expect('HMSET doc2 test foo').ok()
    env.expect('FT.DEBUG docidtoid idx doc2').equal(3)
    env.expect('HMSET doc2 testtest foo').ok()
    env.expect('FT.DEBUG docidtoid idx doc2').equal(3)
    env.expect('HMSET doc2 test bar').ok()
    env.expect('FT.DEBUG docidtoid idx doc2').equal(4)

def testHDel(env):
    env.expect('FT.CREATE idx SCHEMA test1 TEXT test2 TEXT').equal('OK')
    env.expect('HSET doc1 test1 foo test2 bar').equal(2)
    env.expect('FT.DEBUG docidtoid idx doc1').equal(1)
    env.expect('HDEL doc1 test1 foo').equal(1)
    env.expect('FT.DEBUG docidtoid idx doc1').equal(2)

