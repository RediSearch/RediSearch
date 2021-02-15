# -*- coding: utf-8 -*-

import unittest
from includes import *
from common import getConnectionByEnv, waitForIndex, sortedResults, toSortedFlatList, check_server_version
from time import sleep
from RLTest import Env

string1 = 'For the exchange of decimal floating-point numbers, \
           interchange formats of any multiple of 32 bits are defined. \
           As with binary interchange, the encoding scheme for the decimal interchange formats encodes the sign, exponent, and significand. \
           Two different bit-level encodings are defined, and interchange is complicated by the fact that some external indicator of the encoding in use may be required. \
           The two options allow the significand to be encoded as a compressed sequence of decimal digits using densely packed decimal or, alternatively, as a binary integer. \
           The former is more convenient for direct hardware implementation of the standard, while the latter is more suited to software emulation on a binary computer. \
           In either case, the set of numbers (combinations of sign, significand, and exponent) \
           that may be encoded is identical, and special values (±zero with the minimum exponent, ±infinity, quiet NaNs, and signaling NaNs) have identical encodings.'

string2 = 'For the binary formats, the representation is made unique by choosing the smallest representable exponent allowing the value to be represented exactly. \
           Further, the exponent is not represented directly, but a bias is added so that the smallest representable exponent is represented as 1, with 0 used for subnormal numbers. \
           For numbers with an exponent in the normal range (the exponent field being neither all ones nor all zeros), \
           the leading bit of the significand will always be 1. \
           Consequently, a leading 1 can be implied rather than explicitly present in the memory encoding, \
           and under the standard the explicitly represented part of the significand will lie between 0 and 1. \
           This rule is called leading bit convention, implicit bit convention, or hidden bit convention. \
           This rule allows the binary format to have an extra bit of precision. \
           The leading bit convention cannot be used for the subnormal numbers as they have an exponent outside \
           the normal exponent range and scale by the smallest represented exponent as used for the smallest normal numbers.'

def testProfileSearch(env):
  env.skipOnCluster()
  conn = getConnectionByEnv(env)
  env.cmd('FT.CONFIG', 'SET', 'MAXPREFIXEXPANSIONS', 1000000)
  env.cmd('FT.CONFIG', 'SET', '_PRINT_PROFILE_CLOCK', 'false')

  env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'text')
  conn.execute_command('hset', '1', 't', 'hello')
  conn.execute_command('hset', '2', 't', 'world')

  env.expect('ft.profile', 'profile', 'idx', '*', 'nocontent').error().contains('Bad command type')

  # test WILDCARD
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '*', 'nocontent')
  expected_res = [[2L, '1', '2'],
                  [['Total profile time'],
                    ['Parsing and iterator creation time'],
                    ['Iterators profile',
                      ['Wildcard iterator', 2L]],
                    ['Result processors profile',
                      ['Index', 2L],
                      ['Scorer', 2L],
                      ['Sorter', 2L]]]]
  env.assertEqual(actual_res, expected_res)

  # test EMPTY
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', 'redis', 'nocontent')
  expected_res = [[0L],
                 [['Total profile time'],
                  ['Parsing and iterator creation time'],
                  ['Iterators profile',
                    ['Empty iterator', 0L]],
                  ['Result processors profile',
                    ['Index', 0L],
                    ['Scorer', 0L],
                    ['Sorter', 0L]]]]
  env.assertEqual(actual_res, expected_res)

  # test single term
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', 'hello', 'nocontent')
  expected_res = [[1L, '1'],
                  [['Total profile time'],
                    ['Parsing and iterator creation time'],
                    ['Iterators profile',
                      ['Term reader', 'hello', 1L]],
                    ['Result processors profile',
                      ['Index', 1L],
                      ['Scorer', 1L],
                      ['Sorter', 1L]]]]
  env.assertEqual(actual_res, expected_res)

  # test UNION
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', 'hello|world', 'nocontent')
  expected_res = [[2L, '1', '2'],
                  [['Total profile time'],
                    ['Parsing and iterator creation time'],
                    ['Iterators profile',
                    ['Union iterator - UNION', 2L,
                      ['Term reader', 'hello', 1L],
                      ['Term reader', 'world', 1L]]],
                    ['Result processors profile',
                      ['Index', 2L],
                      ['Scorer', 2L],
                      ['Sorter', 2L]]]]
  env.assertEqual(actual_res, expected_res)

  # test INTERSECT
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', 'hello world', 'nocontent')
  expected_res = [[0L],
                  [['Total profile time'],
                    ['Parsing and iterator creation time'],
                    ['Iterators profile',
                    ['Intersect iterator', 0L,
                      ['Term reader', 'hello', 1L],
                      ['Term reader', 'world', 1L]]],
                    ['Result processors profile',
                      ['Index', 0L],
                      ['Scorer', 0L],
                      ['Sorter', 0L]]]]
  env.assertEqual(actual_res, expected_res)

  # test NOT
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '-hello', 'nocontent')
  expected_res = [[1L, '2'],
                  [['Total profile time'],
                    ['Parsing and iterator creation time'],
                    ['Iterators profile',
                      ['Not iterator', 1L,
                        ['Term reader', 'hello', 0L]]],
                    ['Result processors profile',
                      ['Index', 1L],
                      ['Scorer', 1L],
                      ['Sorter', 1L]]]]
  env.assertEqual(actual_res, expected_res)

  # test OPTIONAL
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '~hello', 'nocontent')
  expected_res = [[2L, '1', '2'],
                 [['Total profile time'],
                    ['Parsing and iterator creation time'],
                    ['Iterators profile',
                      ['Optional iterator', 2L,
                        ['Term reader', 'hello', 0L]]],
                    ['Result processors profile',
                      ['Index', 2L],
                      ['Scorer', 2L],
                      ['Sorter', 2L]]]]
  env.assertEqual(actual_res, expected_res)

  # test PREFIX
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', 'hel*', 'nocontent')
  expected_res = [[1L, '1'],
                 [['Total profile time'],
                    ['Parsing and iterator creation time'],
                    ['Iterators profile',
                      ['Union iterator - PREFIX - hel', 1L,
                        ['Term reader', 'hello', 1L]]],
                    ['Result processors profile',
                      ['Index', 1L],
                      ['Scorer', 1L],
                      ['Sorter', 1L]]]]
  env.assertEqual(actual_res, expected_res)

  # test FUZZY
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '%%hel%%', 'nocontent')
  expected_res = [[1L, '1'],
                 [['Total profile time'],
                 ['Parsing and iterator creation time'],
                 ['Iterators profile',
                    ['Union iterator - FUZZY - hel', 1L,
                      ['Term reader', 'hello', 1L]]],
                      ['Result processors profile',
                      ['Index', 1L],
                      ['Scorer', 1L],
                      ['Sorter', 1L]]]]
  env.assertEqual(actual_res, expected_res)

  # test ID LIST iter with INKEYS
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '%%hel%%', 'inkeys', 1, '1')
  expected_res = [[1L, '1', ['t', 'hello']], 
                 [['Total profile time'],
                  ['Parsing and iterator creation time'],
                  ['Iterators profile',
                    ['Intersect iterator', 1L,
                      ['ID-List iterator', 1L],
                      ['Union iterator - FUZZY - hel', 1L,
                        ['Term reader', 'hello', 1L]]]],
                  ['Result processors profile',
                    ['Index', 1L],
                    ['Scorer', 1L],
                    ['Sorter', 1L],
                    ['Loader', 1L]]]]
  env.assertEqual(actual_res, expected_res)

  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', 'hello(hello(hello(hello(hello))))', 'nocontent')
  expected_res = [[1L, '1'], 
                  [['Total profile time'],
                      ['Parsing and iterator creation time'],
                      ['Iterators profile',
                        ['Intersect iterator', 1L,
                          ['Term reader', 'hello', 1L],
                          ['Intersect iterator', 1L,
                            ['Term reader', 'hello', 1L],
                            ['Intersect iterator', 1L,
                              ['Term reader', 'hello', 1L],
                              ['Intersect iterator', 1L,
                                  ['Term reader', 'hello', 1L],
                                  ['Term reader', 'hello',1L]]]]]],
                      ['Result processors profile',
                        ['Index', 1L],
                        ['Scorer', 1L],
                        ['Sorter', 1L]]]]
  env.assertEqual(actual_res, expected_res)

  if not check_server_version(env, '6.00.20'):
    return

  actual_res = env.expect('ft.profile', 'idx', 'search', 'query',  'hello(hello(hello(hello(hello(hello)))))', 'nocontent')
  expected_res = [1L, '1', 
                      ['Total profile time'],
                      ['Parsing and iterator creation time'],
                      ['Iterators profile',
                        ['Intersect iterator', 2L,
                          ['Term reader', 'hello', 2L],
                          ['Intersect iterator', 1L,
                            ['Term reader', 'hello', 1L],
                            ['Intersect iterator', 1L,
                              ['Term reader', 'hello', 1L],
                              ['Intersect iterator', 1L,
                                ['Term reader', 'hello', 1L],
                                ['Intersect iterator', 1L,
                                  ['Term reader', 'hello', 1L],
                                  ['Intersect iterator', 1L, None, None]]]]]]],
                      ['Result processors profile',
                        ['Index', 2L],
                        ['Scorer', 2L],
                        ['Sorter', 2L]]]

def testProfileSearchLimited(env):
  env.skipOnCluster()
  conn = getConnectionByEnv(env)
  env.cmd('FT.CONFIG', 'SET', 'MAXPREFIXEXPANSIONS', 1000000)
  env.cmd('FT.CONFIG', 'SET', '_PRINT_PROFILE_CLOCK', 'false')

  env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'text')
  conn.execute_command('hset', '1', 't', 'hello')
  conn.execute_command('hset', '2', 't', 'hell')
  conn.execute_command('hset', '3', 't', 'help')
  conn.execute_command('hset', '4', 't', 'helowa')

  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'limited', 'query',  '%hell% hel*')
  expected_res = [[3L, '1', ['t', 'hello'], '2', ['t', 'hell'], '3', ['t', 'help']],
                 [['Total profile time'],
                  ['Parsing and iterator creation time'],
                  ['Iterators profile',
                    ['Intersect iterator', 3L,
                      ['Union iterator - FUZZY - hell', 3L, 'The number of iterators in union is 3'],
                      ['Union iterator - PREFIX - hel', 3L, 'The number of iterators in union is 4']]],
                  ['Result processors profile',
                    ['Index', 3L],
                    ['Scorer', 3L],
                    ['Sorter', 3L],
                    ['Loader', 3L]]]]
  env.assertEqual(actual_res, expected_res)

def testProfileAggregate(env):
  env.skipOnCluster()
  conn = getConnectionByEnv(env)
  env.cmd('FT.CONFIG', 'SET', 'MAXPREFIXEXPANSIONS', 1000000)
  env.cmd('FT.CONFIG', 'SET', '_PRINT_PROFILE_CLOCK', 'false')

  env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'text')
  conn.execute_command('hset', '1', 't', 'hello')
  conn.execute_command('hset', '2', 't', 'world')

  actual_res = conn.execute_command('ft.profile', 'idx', 'aggregate', 'query', 'hello',
                                    'groupby', 1, '@t',
                                    'REDUCE', 'count', '0', 'as', 'sum')
  expected_res = [[1L, ['t', 'hello', 'sum', '1']],
                  [ ['Total profile time'],
                    ['Parsing and iterator creation time'],
                    ['Iterators profile',
                      ['Term reader', 'hello', 1L]],
                    ['Result processors profile',
                      ['Index', 1L],
                      ['Loader', 1L],
                      ['Grouper', 1L]]]]
  env.assertEqual(actual_res, expected_res)

  actual_res = env.cmd('ft.profile', 'idx', 'aggregate', 'query', '*',
                'load', 1, 't',
                'apply', 'startswith(@t, "hel")', 'as', 'prefix')
  expected_res = [[1L, ['t', 'hello', 'prefix', '1'], ['t', 'world', 'prefix', '0']],
                  [['Total profile time'],
                    ['Parsing and iterator creation time'],
                    ['Iterators profile',
                      ['Wildcard iterator', 2L]],
                    ['Result processors profile',
                      ['Index', 2L],
                      ['Loader', 2L],
                      ['Projector - Function startswith', 2L]]]]
  env.assertEqual(actual_res, expected_res)

def testProfileCursor(env):
  env.skipOnCluster()
  conn = getConnectionByEnv(env)
  env.cmd('FT.CONFIG', 'SET', 'MAXPREFIXEXPANSIONS', 1000000)
  env.cmd('FT.CONFIG', 'SET', '_PRINT_PROFILE_CLOCK', 'false')

  env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'text')
  conn.execute_command('hset', '1', 't', 'hello')
  conn.execute_command('hset', '2', 't', 'world')

  actual_res = conn.execute_command('ft.profile', 'idx', 'aggregate', 'query', '*',
                                    'load', 1, '@t',
                                    'WITHCURSOR', 'COUNT', 10)
  expected_res = [[1L, ['t', 'hello'], ['t', 'world']],
                   0L, # cursorID
                  [['Total profile time'],
                    ['Parsing and iterator creation time'],
                    ['Iterators profile',
                      ['Wildcard iterator', 2L]],
                    ['Result processors profile',
                      ['Index', 2L],
                      ['Loader', 2L]]]]
  env.assertEqual(actual_res, expected_res)

  actual_res = conn.execute_command('ft.profile', 'idx', 'aggregate', 'query', '*',
                                    'load', 1, '@t',
                                    'WITHCURSOR', 'COUNT', 1)
  # test initial result                                    
  env.assertEqual(actual_res[0], [1L, ['t', 'hello']])
  env.assertGreater(actual_res[1], 0)
  env.assertEqual(actual_res[2], None)

  # test second result
  actual_res = conn.execute_command('ft.cursor', 'read', 'idx', actual_res[1])
  env.assertEqual(actual_res[0], [1L, ['t', 'world']])
  env.assertGreater(actual_res[1], 0)
  env.assertEqual(actual_res[2], None)

  # test final result with profile
  actual_res = conn.execute_command('ft.cursor', 'read', 'idx', actual_res[1])
  res = [[0L], 0L,
         [['Total profile time'],
          ['Parsing and iterator creation time'],
          ['Iterators profile',
            ['Wildcard iterator', 2L]],
          ['Result processors profile',
            ['Index', 2L],
            ['Loader', 2L]]]]
  env.assertEqual(actual_res, res)

def testProfileNumeric(env):
  env.skipOnCluster()
  conn = getConnectionByEnv(env)
  env.cmd('FT.CONFIG', 'SET', 'MAXPREFIXEXPANSIONS', 1000000)
  env.cmd('FT.CONFIG', 'SET', '_PRINT_PROFILE_CLOCK', 'false')

  env.cmd('ft.create', 'idx', 'SCHEMA', 'n', 'numeric')
  conn.execute_command('hset', '1', 'n', '1.2')
  conn.execute_command('hset', '2', 'n', '1.5')
  conn.execute_command('hset', '3', 'n', '8.2')
  conn.execute_command('hset', '4', 'n', '6.7')
  conn.execute_command('hset', '5', 'n', '-14')

  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '@n:[0,100]', 'nocontent')
  expected_res = [[4L, '1', '2', '3', '4'],
                  [['Total profile time'],
                    ['Parsing and iterator creation time'],
                    ['Iterators profile',
                      ['Union iterator - NUMERIC', 4L,
                        ['Numeric reader', '-14 - 1.35', 1L],
                        ['Numeric reader', '1.35 - 8.2', 3L]]],
                    ['Result processors profile',
                      ['Index', 4L],
                      ['Scorer', 4L],
                      ['Sorter', 4L]]]]
  env.assertEqual(actual_res, expected_res)

def testProfileTag(env):
  env.skipOnCluster()
  conn = getConnectionByEnv(env)
  env.cmd('FT.CONFIG', 'SET', 'MAXPREFIXEXPANSIONS', 1000000)
  env.cmd('FT.CONFIG', 'SET', '_PRINT_PROFILE_CLOCK', 'false')

  env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'tag')
  conn.execute_command('hset', '1', 't', 'foo,bar')
  conn.execute_command('hset', '2', 't', 'food,bag')
  conn.execute_command('hset', '3', 't', 'foo')

  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '@t:{foo}', 'nocontent')
  #actual_res = conn.execute_command('ft.search', 'idx', '@t:{foo} | @t:{bar}', 'nocontent')
  expected_res = [[2L, '1', '3'],
                 [['Total profile time'],
                  ['Parsing and iterator creation time'],
                  ['Iterators profile',
                    ['Tag reader', 'foo', 2L]],
                  ['Result processors profile',
                    ['Index', 2L],
                    ['Scorer', 2L],
                    ['Sorter', 2L]]]]
  env.assertEqual(actual_res, expected_res)

def testProfileOutput(env):
  env.skip()
  docs = 10000
  copies = 10
  queries = 0

  conn = getConnectionByEnv(env)
  pl = conn.pipeline()
  env.cmd('FT.CONFIG', 'SET', 'MAXPREFIXEXPANSIONS', 1000000)
  env.cmd('FT.CONFIG', 'SET', '_PRINT_PROFILE_CLOCK', 'false')
  env.cmd('FT.CONFIG', 'SET', 'UNION_ITERATOR_HEAP', 1)

  env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'text')
  for i in range(docs):
    pl.execute_command('hset', i, 't', str(i / copies), 'hello', string1, 'world', string2)
    if (i % 999) is 0:
      pl.execute()
  pl.execute()
  
  print "finished loading"
  search_string = '12*|87*|42*'
  #search_string = '(4|5) (5|6)'
  #search_string = '1(1(1(1(1(1(1))))))'
  #search_string = '1(1(1(1(1))))'
  #print env.cmd('FT.search', 'idx', '12*|69*', 'limit', 0, 0)
  for i in range(queries):
    pl.execute_command('ft.profile', 'idx', 'search', 'query',  search_string, 'limit', 0, 1000)
    if (i % 999) is 0:
      pl.execute()
  pl.execute()
  res = env.cmd('ft.profile', 'idx', 'search', 'query',  search_string, 'limit', 0, 0, 'nocontent')
  print res
