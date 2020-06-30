import unittest
from includes import *

def testFilter1(env):
    env.cmd('ft.create', 'things',
            'ON', 'HASH',
            'FILTER', 'startswith(@__key, "thing:")',
            'SCHEMA', 'name', 'text')

    env.cmd('hset', 'thing:bar', 'name', 'foo')

    env.expect('ft.search', 'things', 'foo') \
       .equal([1L, 'thing:bar', ['name', 'foo']])

def testPrefix0a(env):
    env.cmd('ft.create', 'things', 'ON', 'HASH',
            'PREFIX', '1', '',
            'SCHEMA', 'name', 'text')

    env.cmd('hset', 'thing:bar', 'name', 'foo')
    env.expect('ft.search', 'things', 'foo').equal([1L, 'thing:bar', ['name', 'foo']])

def testPrefix0b(env):
    env.cmd('ft.create', 'things', 'ON', 'HASH', 'SCHEMA', 'name', 'text')
    env.cmd('hset', 'thing:bar', 'name', 'foo')
    env.expect('ft.search', 'things', 'foo').equal([1L, 'thing:bar', ['name', 'foo']])

def testPrefix1(env):
    env.cmd('ft.create', 'things', 'ON', 'HASH',
            'PREFIX', '1', 'thing:',
            'SCHEMA', 'name', 'text')

    env.cmd('hset', 'thing:bar', 'name', 'foo')

    env.expect('ft.search', 'things', 'foo') \
       .equal([1L, 'thing:bar', ['name', 'foo']])

def testPrefix2(env):
    env.cmd('ft.create', 'things', 'ON', 'HASH',
            'PREFIX', '2', 'this:', 'that:',
            'SCHEMA', 'name', 'text')

    env.cmd('hset', 'this:foo', 'name', 'foo')
    env.cmd('hset', 'that:foo', 'name', 'foo')

    env.expect('ft.search', 'things', 'foo') \
       .equal([2L, 'that:foo', ['name', 'foo'], 'this:foo', ['name', 'foo']])

def testFilter2(env):
    env.cmd('ft.create', 'stuff', 'ON', 'HASH',
            'FILTER', 'startswith(@__key, "stuff:")',
            'SCHEMA', 'name', 'text', 'age', 'numeric')

    env.cmd('ft.create', 'things', 'ON', 'HASH',
            'FILTER', 'startswith(@__key, "thing:")',
            'SCHEMA', 'name', 'text', 'age', 'numeric')

    env.cmd('hset', 'thing:bar', 'name', 'foo')
    env.cmd('hset', 'object:jojo', 'name', 'vivi')
    env.cmd('hset', 'thing:bar', 'age', '42')

    env.expect('ft.search', 'things', 'foo') \
       .equal([1L, 'thing:bar', ['name', 'foo', 'age', '42']])

def testPrefix3(env):
    env.cmd('ft.create', 'stuff',
            'ON', 'HASH',
            'PREFIX', '1', 'stuff:',
            'SCHEMA', 'name', 'text', 'age', 'numeric')

    env.cmd('ft.create', 'things', 'ON', 'HASH',
            'PREFIX', '1', 'thing:',
            'SCHEMA', 'name', 'text', 'age', 'numeric')

    env.cmd('hset', 'thing:bar', 'name', 'foo')
    env.cmd('hset', 'object:jojo', 'name', 'vivi')
    env.cmd('hset', 'thing:bar', 'age', '42')

    env.expect('ft.search', 'things', 'foo') \
       .equal([1L, 'thing:bar', ['name', 'foo', 'age', '42']])

def testDel(env):
    env.cmd('ft.create', 'things', 'ON', 'HASH',
            'PREFIX', '1', 'thing:',
            'SCHEMA', 'name', 'text')

    env.expect('ft.search', 'things', 'foo') \
       .equal([0L])

    env.cmd('hset', 'thing:bar', 'name', 'foo')

    env.expect('ft.search', 'things', 'foo') \
       .equal([1L, 'thing:bar', ['name', 'foo']])

    env.cmd('del', 'thing:bar')

    env.expect('ft.search', 'things', 'foo') \
       .equal([0L])

def testFlush(env):
    env.cmd('ft.create', 'things', 'ON', 'HASH',
            'PREFIX', '1', 'thing:',
            'FILTER', 'startswith(@__key, "thing:")',
            'SCHEMA', 'name', 'text')

    env.cmd('FLUSHALL')

    env.cmd('hset', 'thing:bar', 'name', 'foo')

    env.expect('ft.search', 'things', 'foo').equal('things: no such index')

def testNotExist(env):
    env.cmd('ft.create', 'things', 'ON', 'HASH',
            'PREFIX', '1', 'thing:',
            'FILTER', 'startswith(@__key, "thing:")',
            'SCHEMA', 'txt', 'text')

    env.cmd('hset', 'thing:bar', 'not_text', 'foo')
    env.expect('ft.search', 'things', 'foo').equal([0L])

def testPayload(env):
    env.expect('ft.create', 'things', 'ON', 'HASH',
                'PREFIX', '1', 'thing:',
                'PAYLOAD', 'payload',
                'SCHEMA', 'name', 'text').ok()
    env.cmd('hset', 'thing:foo', 'name', 'foo', 'payload', 'stuff')

    for _ in env.retry_with_rdb_reload():
        env.expect('ft.search', 'things', 'foo') \
        .equal([1L, 'thing:foo', ['name', 'foo', 'payload', 'stuff']])

        env.expect('ft.search', 'things', 'foo', 'withpayloads') \
           .equal([1L, 'thing:foo', 'stuff', ['name', 'foo', 'payload', 'stuff']])

def testDuplicateFields(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH',
               'SCHEMA', 'txt', 'TEXT', 'num', 'NUMERIC', 'SORTABLE').ok()
    for _ in env.retry_with_reload():
        # Ensure the index assignment is correct after an rdb load
        with env.assertResponseError():
            env.cmd('FT.ADD', 'idx', 'doc', 1.0,
                    'FIELDS', 'txt', 'foo', 'txt', 'bar', 'txt', 'baz')

def testReplace(env):
    r = env

    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'f', 'text').ok()

    env.expect('HSET', 'doc1', 'f', 'hello world').equal(1)
    env.expect('HSET', 'doc2', 'f', 'hello world').equal(1)
    res = r.execute_command('ft.search', 'idx', 'hello world')
    env.assertEqual(2, res[0])

    # now replace doc1 with a different content
    env.expect('HSET', 'doc1', 'f', 'goodbye universe').equal(0)

    for _ in r.retry_with_rdb_reload():
        # make sure the query for hello world does not return the replaced document
        r.expect('ft.search', 'idx', 'hello world', 'nocontent').equal([1, 'doc2'])

        # search for the doc's new content
        r.expect('ft.search', 'idx', 'goodbye universe', 'nocontent').equal([1, 'doc1'])

def testSortable(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'test', 'TEXT', 'SORTABLE').ok()
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo1').ok()
