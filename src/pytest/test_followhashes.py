import unittest
from includes import *

def testHashes_filter1(env):
    env.cmd('ft.create', 'things',
            'ON', 'HASH',
            'FILTER', 'startswith(@__key, "thing:")',
            'SCHEMA', 'name', 'text')

    env.cmd('hset', 'thing:bar', 'name', 'foo')

    env.expect('ft.search', 'things', 'foo') \
       .equal([1L, 'thing:bar', ['name', 'foo']])

def testHashes_prefix1(env):
    env.cmd('ft.create', 'things',
            'ON', 'HASH',
            'PREFIX', '1', 'thing:',
            'SCHEMA', 'name', 'text')

    env.cmd('hset', 'thing:bar', 'name', 'foo')

    env.expect('ft.search', 'things', 'foo') \
       .equal([1L, 'thing:bar', ['name', 'foo']])

def testHashes_prefix2(env):
    env.cmd('ft.create', 'things',
            'ON', 'HASH',
            'PREFIX', '2', 'this:', 'that:',
            'SCHEMA', 'name', 'text')

    env.cmd('hset', 'this:foo', 'name', 'foo')
    env.cmd('hset', 'that:foo', 'name', 'foo')

    env.expect('ft.search', 'things', 'foo') \
       .equal([2L, 'that:foo', ['name', 'foo'], 'this:foo', ['name', 'foo']])

def testHashes_filter2(env):
    env.cmd('ft.create', 'stuff',
            'ON', 'HASH',
            'FILTER', 'startswith(@__key, "stuff:")',
            'SCHEMA', 'name', 'text', 'age', 'numeric')

    env.cmd('ft.create', 'things',
            'ON', 'HASH',
            'FILTER', 'startswith(@__key, "thing:")',
            'SCHEMA', 'name', 'text', 'age', 'numeric')

    env.cmd('hset', 'thing:bar', 'name', 'foo')
    env.cmd('hset', 'object:jojo', 'name', 'vivi')
    env.cmd('hset', 'thing:bar', 'age', '42')

    env.expect('ft.search', 'things', 'foo') \
       .equal([1L, 'thing:bar', ['name', 'foo', 'age', '42']])

def testHashes_prefix3(env):
    env.cmd('ft.create', 'stuff',
            'ON', 'HASH',
            'PREFIX', '1', 'stuff:',
            'SCHEMA', 'name', 'text', 'age', 'numeric')

    env.cmd('ft.create', 'things',
            'ON', 'HASH',
            'PREFIX', '1', 'thing:',
            'SCHEMA', 'name', 'text', 'age', 'numeric')

    env.cmd('hset', 'thing:bar', 'name', 'foo')
    env.cmd('hset', 'object:jojo', 'name', 'vivi')
    env.cmd('hset', 'thing:bar', 'age', '42')

    env.expect('ft.search', 'things', 'foo') \
       .equal([1L, 'thing:bar', ['name', 'foo', 'age', '42']])

def testHashes_del(env):
    env.cmd('ft.create', 'things',
            'ON', 'HASH',
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

def testHashes_flush(env):
    env.cmd('ft.create', 'things',
            'ON', 'HASH',
            'PREFIX', '1', 'thing:',
            'FILTER', 'startswith(@__key, "thing:")',
            'SCHEMA', 'name', 'text')

    env.cmd('FLUSHALL')

    env.cmd('hset', 'thing:bar', 'name', 'foo')

    env.expect('ft.search', 'things', 'foo') \
       .equal('things: no such index')

def testHashes_notExist(env):
    env.cmd('ft.create', 'things',
            'ON', 'HASH',
            'PREFIX', '1', 'thing:',
            'FILTER', 'startswith(@__key, "thing:")',
            'SCHEMA', 'txt', 'text')

    env.cmd('hset', 'thing:bar', 'not_text', 'foo')
    env.expect('ft.search', 'things', 'foo').equal([])

def testHashes_sortable(env):
    env.expect('FT.CREATE', 'idx',
                'ON', 'HASH',
                'FILTER', 'startswith(@__key, "")',
                'SCHEMA', 'test', 'TEXT', 'SORTABLE').equal('OK')
    env.expect('ft.add', 'idx', 'doc1', '1.0', 'FIELDS', 'test', 'foo1').equal('OK')
