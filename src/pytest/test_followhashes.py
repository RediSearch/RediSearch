import unittest
from includes import *

def testHashes1(env):
    env.cmd('ft.create', 'things', 
            'EXPRESSION', 'prefix("thing:", @__key)',
            'SCHEMA', 'name', 'text')

    env.cmd('hset', 'thing:bar', 'name', 'foo')

    env.expect('ft.search', 'things', 'foo') \
            .equal([1L, 'thing:bar', ['name', 'foo']])

def testHashes2(env):
    env.cmd('ft.create', 'stuff', 
            'EXPRESSION', 'prefix("stuff:", @__key)',
            'SCHEMA', 'name', 'text', 'age', 'numeric')

    env.cmd('ft.create', 'things', 
            'EXPRESSION', 'prefix("thing:", @__key)',
            'SCHEMA', 'name', 'text', 'age', 'numeric')

    env.cmd('hset', 'thing:bar', 'name', 'foo')
    env.cmd('hset', 'object:jojo', 'name', 'vivi')
    env.cmd('hset', 'thing:bar', 'age', '42')

    env.expect('ft.search', 'things', 'foo') \
            .equal([1L, 'thing:bar', ['name', 'foo', 'age', '42']])
