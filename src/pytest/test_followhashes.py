import unittest
from includes import *
from common import waitForIndex

def getConnectionByEnv(env):
    conn = None
    if env.env == 'oss-cluster':
        conn = env.envRunner.getClusterConnection()
    else:
        conn = env.getConnection()
    return conn

def testSyntax1(env):
    conn = getConnectionByEnv(env)
    env.expect('ft.create', 'idx', 
               'ONfoo*',
            'SCHEMA', 'foo', 'text').equal('Unknown argument `ONfoo*`')
            
    env.expect('ft.create', 'idx2',
            'LANGUAGE', 'eng' 
            'SCHEMA', 'foo', 'text').equal('Unknown argument `foo`')
            
    env.expect('ft.create', 'idx2',
            'SCORE', '1.0' 
            'SCHEMA', 'foo', 'text').equal('Unknown argument `foo`')
            
    env.expect('ft.create', 'idx2',
            'PAYLOAD', 'awfw' 
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
                'PAYLOAD', 'payload',
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
