# -*- coding: utf-8 -*-

import unittest
from includes import *
from common import getConnectionByEnv, waitForIndex, sortedResults, toSortedFlatList, server_version_less_than, server_version_at_least
from time import sleep
from RLTest import Env

def testProfileSearch(env):
  env.skipOnCluster()
  conn = getConnectionByEnv(env)
  env.cmd('FT.CONFIG', 'SET', '_PRINT_PROFILE_CLOCK', 'false')

  env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'text')
  conn.execute_command('hset', '1', 't', 'hello')
  conn.execute_command('hset', '2', 't', 'world')

  env.expect('ft.profile', 'profile', 'idx', '*', 'nocontent').error().contains('No `SEARCH` or `AGGREGATE` provided')

  # test WILDCARD
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '*', 'nocontent')
  env.assertEqual(actual_res[1][3], ['Iterators profile', ['Type', 'WILDCARD', 'Counter', 2L]])

  # test EMPTY
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', 'redis', 'nocontent')
  env.assertEqual(actual_res[1][3], ['Iterators profile', ['Type', 'EMPTY', 'Counter', 0L]])

  # test single term
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', 'hello', 'nocontent')
  env.assertEqual(actual_res[1][3], ['Iterators profile', ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1L, 'Size', 1L]])

  # test UNION
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', 'hello|world', 'nocontent')
  expected_res = ['Iterators profile', ['Type', 'UNION', 'Query type', 'UNION', 'Counter', 2L, 'Child iterators',
                    ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1L, 'Size', 1L],
                    ['Type', 'TEXT', 'Term', 'world', 'Counter', 1L, 'Size', 1L]]] 
  env.assertEqual(actual_res[1][3], expected_res)

  # test INTERSECT
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', 'hello world', 'nocontent')
  expected_res = ['Iterators profile', ['Type', 'INTERSECT', 'Counter', 0L, 'Child iterators',
                    ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1L, 'Size', 1L],
                    ['Type', 'TEXT', 'Term', 'world', 'Counter', 1L, 'Size', 1L]]]
  env.assertEqual(actual_res[1][3], expected_res)

  # test NOT
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '-hello', 'nocontent')
  expected_res = ['Iterators profile', ['Type', 'NOT', 'Counter', 1L, 'Child iterator',
                    ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1L, 'Size', 1L]]]
  env.assertEqual(actual_res[1][3], expected_res)

  # test OPTIONAL
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '~hello', 'nocontent')
  expected_res = ['Iterators profile', ['Type', 'OPTIONAL', 'Counter', 2L, 'Child iterator',
                    ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1L, 'Size', 1L]]]
  env.assertEqual(actual_res[1][3], expected_res)

  # test PREFIX
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', 'hel*', 'nocontent')
  expected_res = ['Iterators profile', ['Type', 'UNION', 'Query type', 'PREFIX - hel', 'Counter', 1L, 'Child iterators',
                    ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1L, 'Size', 1L]]]
  env.assertEqual(actual_res[1][3], expected_res)

  # test FUZZY
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '%%hel%%', 'nocontent')
  expected_res = ['Iterators profile', ['Type', 'UNION', 'Query type', 'FUZZY - hel', 'Counter', 1L, 'Child iterators',
                    ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1L, 'Size', 1L]]]
  env.assertEqual(actual_res[1][3], expected_res)

  # test ID LIST iter with INKEYS
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', 'hello', 'inkeys', 1, '1')
  expected_res = ['Iterators profile', ['Type', 'INTERSECT', 'Counter', 1L, 'Child iterators',
                    ['Type', 'ID-LIST', 'Counter', 1L],
                    ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1L, 'Size', 1L]]]
  env.assertEqual(actual_res[1][3], expected_res)

  # test no crash on reaching deep reply array
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', 'hello(hello(hello(hello(hello))))', 'nocontent')
  expected_res = ['Iterators profile', ['Type', 'INTERSECT', 'Counter', 1L, 'Child iterators',
                    ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1L, 'Size', 1L],
                    ['Type', 'INTERSECT', 'Counter', 1L, 'Child iterators',
                      ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1L, 'Size', 1L],
                      ['Type', 'INTERSECT', 'Counter', 1L, 'Child iterators',
                        ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1L, 'Size', 1L],
                        ['Type', 'INTERSECT', 'Counter', 1L, 'Child iterators',
                          ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1L, 'Size', 1L],
                          ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1L, 'Size', 1L]]]]]]
  env.assertEqual(actual_res[1][3], expected_res)

  if server_version_less_than(env, '6.2.0'):
    return

  actual_res = env.execute_command('ft.profile', 'idx', 'search', 'query',  'hello(hello(hello(hello(hello(hello)))))', 'nocontent')
  expected_res = ['Iterators profile',
                  ['Type', 'INTERSECT', 'Counter', 1L, 'Child iterators',
                    ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1L, 'Size', 1L],
                    ['Type', 'INTERSECT', 'Counter', 1L, 'Child iterators',
                      ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1L, 'Size', 1L],
                      ['Type', 'INTERSECT', 'Counter', 1L, 'Child iterators',
                        ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1L, 'Size', 1L],
                        ['Type', 'INTERSECT', 'Counter', 1L, 'Child iterators',
                          ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1L, 'Size', 1L],
                          ['Type', 'INTERSECT', 'Counter', 1L, 'Child iterators',
                            ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1L, 'Size', 1L],
                            ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1L, 'Size', 1L]]]]]]]
  env.assertEqual(actual_res[1][3], expected_res)

def testProfileSearchLimited(env):
  env.skipOnCluster()
  conn = getConnectionByEnv(env)
  env.cmd('FT.CONFIG', 'SET', '_PRINT_PROFILE_CLOCK', 'false')

  env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'text')
  conn.execute_command('hset', '1', 't', 'hello')
  conn.execute_command('hset', '2', 't', 'hell')
  conn.execute_command('hset', '3', 't', 'help')
  conn.execute_command('hset', '4', 't', 'helowa')

  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'limited', 'query',  '%hell% hel*')
  expected_res = ['Iterators profile', ['Type', 'INTERSECT', 'Counter', 3L, 'Child iterators',
                  ['Type', 'UNION', 'Query type', 'FUZZY - hell', 'Counter', 3L, 'Child iterators', 'The number of iterators in the union is 3'],
                  ['Type', 'UNION', 'Query type', 'PREFIX - hel', 'Counter', 3L, 'Child iterators', 'The number of iterators in the union is 4']]]
  env.assertEqual(actual_res[1][3], expected_res)

def testProfileAggregate(env):
  env.skipOnCluster()
  conn = getConnectionByEnv(env)
  env.cmd('FT.CONFIG', 'SET', '_PRINT_PROFILE_CLOCK', 'false')

  env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'text')
  conn.execute_command('hset', '1', 't', 'hello')
  conn.execute_command('hset', '2', 't', 'world')

  expected_res = ['Result processors profile',
                  ['Type', 'Index', 'Counter', 1L],
                  ['Type', 'Loader', 'Counter', 1L],
                  ['Type', 'Grouper', 'Counter', 1L]]
  actual_res = conn.execute_command('ft.profile', 'idx', 'aggregate', 'query', 'hello',
                                    'groupby', 1, '@t',
                                    'REDUCE', 'count', '0', 'as', 'sum')
  env.assertEqual(actual_res[1][4], expected_res)

  expected_res = ['Result processors profile',
                  ['Type', 'Index', 'Counter', 2L],
                  ['Type', 'Loader', 'Counter', 2L],
                  ['Type', 'Projector - Function startswith', 'Counter', 2L]]
  actual_res = env.cmd('ft.profile', 'idx', 'aggregate', 'query', '*',
                'load', 1, 't',
                'apply', 'startswith(@t, "hel")', 'as', 'prefix')
  env.assertEqual(actual_res[1][4], expected_res)

def testProfileCursor(env):
  conn = getConnectionByEnv(env)
  env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'text')
  env.expect('ft.profile', 'idx', 'aggregate', 'query', '*', 'WITHCURSOR').error().contains('FT.PROFILE does not support cursor')


def testProfileErrors(env):
  conn = getConnectionByEnv(env)
  env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'text')
  # missing args
  env.expect('ft.profile', 'idx').error().contains("wrong number of arguments for 'ft.profile'")
  env.expect('ft.profile', 'idx', 'SEARCH').error().contains("wrong number of arguments for 'ft.profile'")
  env.expect('ft.profile', 'idx', 'SEARCH', 'QUERY').error().contains("wrong number of arguments for 'ft.profile'")
  # wrong `query` type
  env.expect('ft.profile', 'idx', 'redis', 'QUERY', '*').error().contains('No `SEARCH` or `AGGREGATE` provided')
  # miss `QUERY` keyword
  if not env.isCluster():
    env.expect('ft.profile', 'idx', 'SEARCH', 'FIND', '*').error().contains('The QUERY keyward is expected')

def testProfileNumeric(env):
  env.skipOnCluster()
  conn = getConnectionByEnv(env)
  env.cmd('FT.CONFIG', 'SET', '_PRINT_PROFILE_CLOCK', 'false')

  env.cmd('ft.create', 'idx', 'SCHEMA', 'n', 'numeric')
  conn.execute_command('hset', '1', 'n', '1.2')
  conn.execute_command('hset', '2', 'n', '1.5')
  conn.execute_command('hset', '3', 'n', '8.2')
  conn.execute_command('hset', '4', 'n', '6.7')
  conn.execute_command('hset', '5', 'n', '-14')

  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '@n:[0,100]', 'nocontent')
  expected_res = ['Iterators profile', ['Type', 'UNION', 'Query type', 'NUMERIC', 'Counter', 4L, 'Child iterators',
                    ['Type', 'NUMERIC', 'Term', '-14 - 1.35', 'Counter', 1L, 'Size', 2L],
                    ['Type', 'NUMERIC', 'Term', '1.35 - 8.2', 'Counter', 3L, 'Size', 3L]]]
  env.assertEqual(actual_res[1][3], expected_res)

def testProfileTag(env):
  env.skipOnCluster()
  conn = getConnectionByEnv(env)
  env.cmd('FT.CONFIG', 'SET', '_PRINT_PROFILE_CLOCK', 'false')

  env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'tag')
  conn.execute_command('hset', '1', 't', 'foo,bar')
  conn.execute_command('hset', '2', 't', 'food,bag')
  conn.execute_command('hset', '3', 't', 'foo')

  # tag profile
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '@t:{foo}', 'nocontent')
  env.assertEqual(actual_res[1][3], ['Iterators profile', ['Type', 'TAG', 'Term', 'foo', 'Counter', 2L, 'Size', 2L]])

def testResultProcessorCounter(env):
  env.skipOnCluster()
  conn = getConnectionByEnv(env)
  env.cmd('FT.CONFIG', 'SET', '_PRINT_PROFILE_CLOCK', 'false')

  env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'text')
  conn.execute_command('hset', '1', 't', 'foo')
  conn.execute_command('hset', '2', 't', 'bar')

  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', 'foo|bar', 'limit', '0', '0')
  env.assertEqual(actual_res[0], [2L])
  res =  ['Result processors profile',
            ['Type', 'Index', 'Counter', 2L],
            ['Type', 'Counter', 'Counter', 1L]]
  env.assertEqual(actual_res[1][4], res)

def testProfileMaxPrefixExpansion(env):
  env.skipOnCluster()
  conn = getConnectionByEnv(env)
  env.cmd('FT.CONFIG', 'SET', 'MAXPREFIXEXPANSIONS', 2)
  env.cmd('FT.CONFIG', 'SET', '_PRINT_PROFILE_CLOCK', 'false')

  env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'text')
  conn.execute_command('hset', '1', 't', 'foo1')
  conn.execute_command('hset', '2', 't', 'foo2')
  conn.execute_command('hset', '3', 't', 'foo3')

  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', 'foo*', 'limit', '0', '0')
  env.assertEqual(actual_res[1][3][1][6:8], ['Warning', 'Max prefix expansion reached'])

  env.cmd('FT.CONFIG', 'SET', 'MAXPREFIXEXPANSIONS', 200)

def testNotIterator(env):
  env.skipOnCluster()
  conn = getConnectionByEnv(env)
  env.cmd('FT.CONFIG', 'SET', 'MAXPREFIXEXPANSIONS', 2)
  env.cmd('FT.CONFIG', 'SET', '_PRINT_PROFILE_CLOCK', 'false')
  conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'text')
  conn.execute_command('HSET', '1', 't', 'foo')
  conn.execute_command('HSET', '2', 't', 'bar')

  #before the fix, we would not get an empty iterator
  res = [[1L, '1', ['t', 'foo']],
         [['Total profile time'], 
          ['Parsing time'],
          ['Pipeline creation time'],
          ['Iterators profile',
            ['Type', 'INTERSECT', 'Counter', 1L, 'Child iterators',
              ['Type', 'TEXT', 'Term', 'foo', 'Counter', 1L, 'Size', 1L],
              ['Type', 'NOT', 'Counter', 1L, 'Child iterator',
                ['Type', 'EMPTY', 'Counter', 0L]]]],
          ['Result processors profile',
            ['Type', 'Index', 'Counter', 1L],
            ['Type', 'Scorer', 'Counter', 1L],
            ['Type', 'Sorter', 'Counter', 1L], ['Type',
            'Loader', 'Counter', 1L]]]]

  env.expect('ft.profile', 'idx', 'search', 'query', 'foo -@t:baz').equal(res)
