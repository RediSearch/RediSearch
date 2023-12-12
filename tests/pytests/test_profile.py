# -*- coding: utf-8 -*-

import unittest
from includes import *
from common import *
from time import sleep
from RLTest import Env

@skip(cluster=True)
def testProfileSearch(env):
  conn = getConnectionByEnv(env)
  env.cmd('FT.CONFIG', 'SET', '_PRINT_PROFILE_CLOCK', 'false')

  env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'text')
  conn.execute_command('hset', '1', 't', 'hello')
  conn.execute_command('hset', '2', 't', 'world')

  env.expect('ft.profile', 'profile', 'idx', '*', 'nocontent').error().contains('No `SEARCH` or `AGGREGATE` provided')

  # test WILDCARD
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '*', 'nocontent')
  env.assertEqual(actual_res[1][4], ['Iterators profile', ['Type', 'WILDCARD', 'Counter', 2]])

  # test EMPTY
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', 'redis', 'nocontent')
  env.assertEqual(actual_res[1][4], ['Iterators profile', ['Type', 'EMPTY', 'Counter', 0]])

  # test single term
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', 'hello', 'nocontent')
  env.assertEqual(actual_res[1][4], ['Iterators profile', ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1, 'Size', 1]])

  # test UNION
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', 'hello|world', 'nocontent')
  expected_res = ['Iterators profile', ['Type', 'UNION', 'Query type', 'UNION', 'Counter', 2, 'Child iterators',
                    ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1, 'Size', 1],
                    ['Type', 'TEXT', 'Term', 'world', 'Counter', 1, 'Size', 1]]]
  env.assertEqual(actual_res[1][4], expected_res)

  # test INTERSECT
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', 'hello world', 'nocontent')
  expected_res = ['Iterators profile', ['Type', 'INTERSECT', 'Counter', 0, 'Child iterators',
                    ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1, 'Size', 1],
                    ['Type', 'TEXT', 'Term', 'world', 'Counter', 1, 'Size', 1]]]
  env.assertEqual(actual_res[1][4], expected_res)

  # test NOT
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '-hello', 'nocontent')
  expected_res = ['Iterators profile', ['Type', 'NOT', 'Counter', 1, 'Child iterator',
                    ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1, 'Size', 1]]]
  env.assertEqual(actual_res[1][4], expected_res)

  # test OPTIONAL
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '~hello', 'nocontent')
  expected_res = ['Iterators profile', ['Type', 'OPTIONAL', 'Counter', 2, 'Child iterator',
                    ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1, 'Size', 1]]]
  env.assertEqual(actual_res[1][4], expected_res)

  # test PREFIX
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', 'hel*', 'nocontent')
  expected_res = ['Iterators profile', ['Type', 'UNION', 'Query type', 'PREFIX - hel', 'Counter', 1, 'Child iterators',
                    ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1, 'Size', 1]]]
  env.assertEqual(actual_res[1][4], expected_res)

  # test FUZZY
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '%%hel%%', 'nocontent')
  expected_res = ['Iterators profile', ['Type', 'UNION', 'Query type', 'FUZZY - hel', 'Counter', 1, 'Child iterators',
                    ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1, 'Size', 1]]]
  env.assertEqual(actual_res[1][4], expected_res)

  # test ID LIST iter with INKEYS
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', 'hello', 'inkeys', 1, '1')
  expected_res = ['Iterators profile', ['Type', 'INTERSECT', 'Counter', 1, 'Child iterators',
                    ['Type', 'ID-LIST', 'Counter', 1],
                    ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1, 'Size', 1]]]
  env.assertEqual(actual_res[1][4], expected_res)

  # test no crash on reaching deep reply array
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', 'hello(hello(hello(hello(hello))))', 'nocontent')
  expected_res = ['Iterators profile', ['Type', 'INTERSECT', 'Counter', 1, 'Child iterators',
                                        ['Type', 'INTERSECT', 'Counter', 1, 'Child iterators',
                                         ['Type', 'INTERSECT', 'Counter', 1, 'Child iterators',
                                          ['Type', 'INTERSECT', 'Counter', 1, 'Child iterators',
                                           ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1, 'Size', 1],
                                           ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1, 'Size', 1]],
                                          ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1, 'Size', 1]],
                                         ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1, 'Size', 1]],
                                        ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1, 'Size', 1]]]
  env.assertEqual(actual_res[1][4], expected_res)

  if server_version_less_than(env, '6.2.0'):
    return

  actual_res = env.execute_command('ft.profile', 'idx', 'search', 'query',  'hello(hello(hello(hello(hello(hello)))))', 'nocontent')
  expected_res = ['Iterators profile', ['Type', 'INTERSECT', 'Counter', 1, 'Child iterators',
                                        ['Type', 'INTERSECT', 'Counter', 1, 'Child iterators',
                                         ['Type', 'INTERSECT', 'Counter', 1, 'Child iterators',
                                          ['Type', 'INTERSECT', 'Counter', 1, 'Child iterators',
                                           ['Type', 'INTERSECT', 'Counter', 1, 'Child iterators',
                                            ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1, 'Size', 1],
                                            ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1, 'Size', 1]],
                                           ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1, 'Size', 1]],
                                          ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1, 'Size', 1]],
                                         ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1, 'Size', 1]],
                                        ['Type', 'TEXT', 'Term', 'hello', 'Counter', 1, 'Size', 1]]]
  env.assertEqual(actual_res[1][4], expected_res)

@skip(cluster=True)
def testProfileSearchLimited(env):
  conn = getConnectionByEnv(env)
  env.cmd('FT.CONFIG', 'SET', '_PRINT_PROFILE_CLOCK', 'false')

  env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'text')
  conn.execute_command('hset', '1', 't', 'hello')
  conn.execute_command('hset', '2', 't', 'hell')
  conn.execute_command('hset', '3', 't', 'help')
  conn.execute_command('hset', '4', 't', 'helowa')

  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'limited', 'query',  '%hell% hel*')
  expected_res = ['Iterators profile', ['Type', 'INTERSECT', 'Counter', 3, 'Child iterators',
                  ['Type', 'UNION', 'Query type', 'FUZZY - hell', 'Counter', 3, 'Child iterators', 'The number of iterators in the union is 3'],
                  ['Type', 'UNION', 'Query type', 'PREFIX - hel', 'Counter', 3, 'Child iterators', 'The number of iterators in the union is 4']]]
  env.assertEqual(actual_res[1][4], expected_res)

@skip(cluster=True)
def testProfileAggregate(env):
  conn = getConnectionByEnv(env)
  env.cmd('FT.CONFIG', 'SET', '_PRINT_PROFILE_CLOCK', 'false')

  env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'text')
  conn.execute_command('hset', '1', 't', 'hello')
  conn.execute_command('hset', '2', 't', 'world')

  expected_res = ['Result processors profile',
                  ['Type', 'Index', 'Counter', 1],
                  ['Type', 'Loader', 'Counter', 1],
                  ['Type', 'Grouper', 'Counter', 1]]
  actual_res = conn.execute_command('ft.profile', 'idx', 'aggregate', 'query', 'hello',
                                    'groupby', 1, '@t',
                                    'REDUCE', 'count', '0', 'as', 'sum')
  env.assertEqual(actual_res[1][5], expected_res)

  expected_res = ['Result processors profile',
                  ['Type', 'Index', 'Counter', 2],
                  ['Type', 'Loader', 'Counter', 2],
                  ['Type', 'Projector - Function startswith', 'Counter', 2]]
  actual_res = env.cmd('ft.profile', 'idx', 'aggregate', 'query', '*',
                'load', 1, 't',
                'apply', 'startswith(@t, "hel")', 'as', 'prefix')
  env.assertEqual(actual_res[1][5], expected_res)

  expected_res = ['Result processors profile',
                  ['Type', 'Index', 'Counter', 2],
                  ['Type', 'Loader', 'Counter', 2],
                  ['Type', 'Sorter', 'Counter', 2],
                  ['Type', 'Loader', 'Counter', 2]]
  actual_res = env.cmd('ft.profile', 'idx', 'aggregate', 'query', '*', 'sortby', 2, '@t', 'asc', 'limit', 0, 10, 'LOAD', 2, '@__key', '@t')
  env.assertEqual(actual_res[1][5], expected_res)

def testProfileCursor(env):
  conn = getConnectionByEnv(env)
  env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'text')
  env.expect('ft.profile', 'idx', 'aggregate', 'query', '*', 'WITHCURSOR').error().contains('FT.PROFILE does not support cursor')


def testProfileErrors(env):
  env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'text')
  # missing args
  env.expect('ft.profile', 'idx').error().contains('wrong number of arguments')
  env.expect('ft.profile', 'idx', 'SEARCH').error().contains('wrong number of arguments')
  env.expect('ft.profile', 'idx', 'SEARCH', 'QUERY').error().contains('wrong number of arguments')
  # wrong `query` type
  env.expect('ft.profile', 'idx', 'redis', 'QUERY', '*').error().contains('No `SEARCH` or `AGGREGATE` provided')
  # miss `QUERY` keyword
  if not env.isCluster():
    env.expect('ft.profile', 'idx', 'SEARCH', 'FIND', '*').error().contains('The QUERY keyword is expected')

@skip(cluster=True)
def testProfileNumeric(env):
  conn = getConnectionByEnv(env)
  env.cmd('FT.CONFIG', 'SET', '_PRINT_PROFILE_CLOCK', 'false')

  env.cmd('ft.create', 'idx', 'SCHEMA', 'n', 'numeric')
  for i in range(10000):
    conn.execute_command('hset', i, 'n', 50 - float(i % 1000) / 10)

  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '@n:[0,100]', 'nocontent')
  expected_res = ['Iterators profile', ['Type', 'UNION', 'Query type', 'NUMERIC', 'Counter', 5010, 'Child iterators',
                    ['Type', 'NUMERIC', 'Term', '-2.9 - 14.4', 'Counter', 1450, 'Size', 1740],
                    ['Type', 'NUMERIC', 'Term', '14.5 - 30.7', 'Counter', 1630, 'Size', 1630],
                    ['Type', 'NUMERIC', 'Term', '30.8 - 38', 'Counter', 730, 'Size', 730],
                    ['Type', 'NUMERIC', 'Term', '38.1 - 44.6', 'Counter', 660, 'Size', 660],
                    ['Type', 'NUMERIC', 'Term', '44.7 - 46.7', 'Counter', 210, 'Size', 210],
                    ['Type', 'NUMERIC', 'Term', '46.8 - 48.5', 'Counter', 180, 'Size', 180],
                    ['Type', 'NUMERIC', 'Term', '48.6 - 50', 'Counter', 150, 'Size', 150]]]

  env.assertEqual(actual_res[1][4], expected_res)

@skip(cluster=True)
def testProfileNegativeNumeric():
  env = Env(protocol=3)
  conn = getConnectionByEnv(env)
  env.cmd('FT.CONFIG', 'SET', '_PRINT_PROFILE_CLOCK', 'false')

  docs = 1_000
  # values_ranges[i] = (min_val , range description)
  values_ranges = [{"min_val": - docs , "title":"only negatives"},
                   {"min_val": - docs / 2 , "title":"from negative to positive"},
                   {"min_val": docs , "title":"only positives"},]

  for values_range in values_ranges:
    env.cmd('ft.create', 'idx', 'SCHEMA', 'n', 'numeric')
    title = values_range['title']
    # Add values
    min_val = values_range['min_val']
    for i in range(docs):
      val =  min_val + i
      conn.execute_command('hset', i, 'n',val)

    actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '@n:[-inf +inf]', 'nocontent')
    Iterators_profile = actual_res['profile']['Iterators profile'][0]
    child_iter_list = Iterators_profile['Child iterators']

    def extract_child_range(child: dict):
      iter_term = child['Term']
      res_range = iter_term.split(" - ")
      range_dict = {"min":float(res_range[0]), "max": float(res_range[1])}
      env.assertEqual(range_dict['max'], range_dict['min'] + child['Size'] - 1, message=f"{title}: range_max should equal range_min + (range_size - 1)")
      return range_dict

    # The first child iterator should contain the min val
    range_dict = extract_child_range(child_iter_list[0])
    actual_min_val = range_dict['min']
    env.assertEqual(float(actual_min_val), min_val, message=f"{title}: The first child iterator should contain the tree min val")
    range_last = range_dict['max']

    for child in child_iter_list[1::]:
      range_dict = extract_child_range(child)
      # The first value of this range should be bigger from the previous's last value by 1.
      env.assertEqual(range_dict['min'], range_last + 1.0,
                      message=f"{title}: The min value of this range should bigger from the previous's last value ({range_last}) by 1")
      range_last = range_dict['max']

    # The last child should contain the max val
    max_val = min_val + docs - 1
    env.assertEqual(max_val, range_last, message=f"{title}: The max value of the last child should equal the max val of the tree")
    env.cmd('flushall')


@skip(cluster=True)
def testProfileTag(env):
  conn = getConnectionByEnv(env)
  env.cmd('FT.CONFIG', 'SET', '_PRINT_PROFILE_CLOCK', 'false')

  env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'tag')
  conn.execute_command('hset', '1', 't', 'foo,bar')
  conn.execute_command('hset', '2', 't', 'food,bag')
  conn.execute_command('hset', '3', 't', 'foo')

  # tag profile
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '@t:{foo}', 'nocontent')
  env.assertEqual(actual_res[1][4], ['Iterators profile', ['Type', 'TAG', 'Term', 'foo', 'Counter', 2, 'Size', 2]])

@skip(cluster=True)
def testProfileVector(env):
  conn = getConnectionByEnv(env)
  env.cmd('FT.CONFIG', 'SET', '_PRINT_PROFILE_CLOCK', 'false')
  env.cmd('FT.CONFIG', 'SET', 'DEFAULT_DIALECT', '2')

  env.expect('FT.CREATE idx SCHEMA v VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2 t TEXT').ok()
  conn.execute_command('hset', '1', 'v', 'bababaca', 't', "hello")
  conn.execute_command('hset', '2', 'v', 'babababa', 't', "hello")
  conn.execute_command('hset', '3', 'v', 'aabbaabb', 't', "hello")
  conn.execute_command('hset', '4', 'v', 'bbaabbaa', 't', "hello world")
  conn.execute_command('hset', '5', 'v', 'aaaabbbb', 't', "hello world")

  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '*=>[KNN 3 @v $vec]',
                                    'SORTBY', '__v_score', 'PARAMS', '2', 'vec', 'aaaaaaaa', 'nocontent')
  expected_iterators_res = ['Iterators profile', ['Type', 'VECTOR', 'Counter', 3]]
  expected_vecsim_rp_res = ['Type', 'Metrics Applier', 'Counter', 3]
  env.assertEqual(actual_res[0], [3, '4', '2', '1'])
  env.assertEqual(actual_res[1][4], expected_iterators_res)
  env.assertEqual(actual_res[1][5][2], expected_vecsim_rp_res)
  env.assertEqual(to_dict(env.cmd("FT.DEBUG", "VECSIM_INFO", "idx", "v"))['LAST_SEARCH_MODE'], 'STANDARD_KNN')

  # Range query - uses metric iterator. Radius is set so that the closest 2 vectors will be inthe range
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '@v:[VECTOR_RANGE 3e36 $vec]=>{$yield_distance_as:dist}',
                                    'SORTBY', 'dist', 'PARAMS', '2', 'vec', 'aaaaaaaa', 'nocontent')
  expected_iterators_res = ['Iterators profile', ['Type', 'METRIC - VECTOR DISTANCE', 'Counter', 2]]
  expected_vecsim_rp_res = ['Type', 'Metrics Applier', 'Counter', 2]
  env.assertEqual(actual_res[0], [2, '4', '2'])
  env.assertEqual(actual_res[1][4], expected_iterators_res)
  env.assertEqual(actual_res[1][5][2], expected_vecsim_rp_res)
  env.assertEqual(to_dict(env.cmd("FT.DEBUG", "VECSIM_INFO", "idx", "v"))['LAST_SEARCH_MODE'], 'RANGE_QUERY')

# Test with hybrid query variations
  # Expect ad-hoc BF to take place - going over child iterator exactly once (reading 2 results)
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '(@t:hello world)=>[KNN 3 @v $vec]',
                                    'SORTBY', '__v_score', 'PARAMS', '2', 'vec', 'aaaaaaaa', 'nocontent')
  expected_iterators_res = ['Iterators profile', ['Type', 'VECTOR', 'Counter', 2, 'Child iterator',
                                                 ['Type', 'INTERSECT', 'Counter', 2, 'Child iterators',
                                                 ['Type', 'TEXT', 'Term', 'world', 'Counter', 2, 'Size', 2],
                                                 ['Type', 'TEXT', 'Term', 'hello', 'Counter', 2, 'Size', 5]]]]
  expected_vecsim_rp_res = ['Type', 'Metrics Applier', 'Counter', 2]
  env.assertEqual(actual_res[0], [2, '4', '5'])
  env.assertEqual(actual_res[1][4], expected_iterators_res)
  env.assertEqual(actual_res[1][5][2], expected_vecsim_rp_res)
  env.assertEqual(to_dict(env.cmd("FT.DEBUG", "VECSIM_INFO", "idx", "v"))['LAST_SEARCH_MODE'], 'HYBRID_ADHOC_BF')

  for i in range(6, 10001):
    conn.execute_command('hset', str(i), 'v', 'bababada', 't', "hello world")
  # Expect batched search to take place - going over child iterator exactly once (reading 2 results)
  # Expect in the first batch to get 1, 2, 4, 6 and then ask for one more batch - and get 7 in the next results.
  # In the second batch - batch size is determined to be 2
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '(@t:hello world)=>[KNN 3 @v $vec]',
                                    'SORTBY', '__v_score', 'PARAMS', '2', 'vec', 'aaaaaaaa', 'nocontent')
  env.assertEqual(actual_res[0], [3, '4', '6', '7'])
  expected_iterators_res = ['Iterators profile', ['Type', 'VECTOR', 'Counter', 3, 'Batches number', 2, 'Child iterator',
                                                 ['Type', 'INTERSECT', 'Counter', 8, 'Child iterators',
                                                 ['Type', 'TEXT', 'Term', 'world', 'Counter', 8, 'Size', 9997],
                                                 ['Type', 'TEXT', 'Term', 'hello', 'Counter', 8, 'Size', 10000]]]]
  expected_vecsim_rp_res = ['Type', 'Metrics Applier', 'Counter', 3]
  env.assertEqual(actual_res[1][4], expected_iterators_res)
  env.assertEqual(actual_res[1][5][2], expected_vecsim_rp_res)
  env.assertEqual(to_dict(env.cmd("FT.DEBUG", "VECSIM_INFO", "idx", "v"))['LAST_SEARCH_MODE'], 'HYBRID_BATCHES')

  # Add another 10K vectors with a different tag.
  for i in range(10001, 20001):
    conn.execute_command('hset', str(i), 'v', '????????', 't', "other")

  # expected results that pass the filter is index_size/2. after two iterations with no results,
  # we should move ad-hoc BF.
  expected_iterators_res = ['Iterators profile', ['Type', 'VECTOR', 'Counter', 0, 'Batches number', 2, 'Child iterator',
                                                  ['Type', 'INTERSECT', 'Counter', 2, 'Child iterators',
                                                   ['Type', 'TEXT', 'Term', 'hello', 'Counter', 5, 'Size', 10000],
                                                   ['Type', 'TEXT', 'Term', 'other', 'Counter', 3, 'Size', 10000]]]]
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '(@t:hello other)=>[KNN 3 @v $vec]',
                                      'SORTBY', '__v_score', 'PARAMS', '2', 'vec', '????????', 'nocontent')
  env.assertEqual(actual_res[1][4], expected_iterators_res)
  env.assertEqual(to_dict(env.cmd("FT.DEBUG", "VECSIM_INFO", "idx", "v"))['LAST_SEARCH_MODE'], 'HYBRID_BATCHES_TO_ADHOC_BF')

  # Ask explicitly to run in batches mode, without asking for a certain batch size.
  # First batch size is 4, and every batch should be double in its size from its previous one. We go over the entire
  # index after the 13th batch.
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '(@t:hello other)=>[KNN 2 @v $vec HYBRID_POLICY BATCHES]',
                                    'SORTBY', '__v_score', 'PARAMS', '2', 'vec', '????????', 'nocontent')
  expected_iterators_res = ['Iterators profile', ['Type', 'VECTOR', 'Counter', 0, 'Batches number', 13, 'Child iterator',
                                                   ['Type', 'INTERSECT', 'Counter', 12, 'Child iterators',
                                                    ['Type', 'TEXT', 'Term', 'hello', 'Counter', 25, 'Size', 10000],
                                                    ['Type', 'TEXT', 'Term', 'other', 'Counter', 13, 'Size', 10000]]]]
  env.assertEqual(actual_res[1][4], expected_iterators_res)
  env.assertEqual(to_dict(env.cmd("FT.DEBUG", "VECSIM_INFO", "idx", "v"))['LAST_SEARCH_MODE'], 'HYBRID_BATCHES')

  # Ask explicitly to run in batches mode, with batch size of 100.
  # After 200 iterations, we should go over the entire index.
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '(@t:hello other)=>[KNN 2 @v $vec HYBRID_POLICY BATCHES BATCH_SIZE 100]',
                                    'SORTBY', '__v_score', 'PARAMS', '2', 'vec', '????????', 'nocontent', 'timeout', '100000')
  expected_iterators_res = ['Iterators profile', ['Type', 'VECTOR', 'Counter', 0, 'Batches number', 200, 'Child iterator',
                                                  ['Type', 'INTERSECT', 'Counter', 199, 'Child iterators',
                                                   ['Type', 'TEXT', 'Term', 'hello', 'Counter', 399, 'Size', 10000],
                                                   ['Type', 'TEXT', 'Term', 'other', 'Counter', 200, 'Size', 10000]]]]
  env.assertEqual(actual_res[1][4], expected_iterators_res)
  env.assertEqual(to_dict(env.cmd("FT.DEBUG", "VECSIM_INFO", "idx", "v"))['LAST_SEARCH_MODE'], 'HYBRID_BATCHES')

  # Asking only for a batch size without asking for batches policy. While batchs mode is on, the bacth size will be as
  # requested, but the mode can change dynamically to ADHOC-BF.
  # Note that the batch_size here as no effect, since the child_num_estimated will always be decreased in half after
  # every iteration that returned 0 results.
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '(@t:hello other)=>[KNN 2 @v $vec BATCH_SIZE 100]',
                                    'SORTBY', '__v_score', 'PARAMS', '2', 'vec', '????????', 'nocontent')
  expected_iterators_res = ['Iterators profile', ['Type', 'VECTOR', 'Counter', 0, 'Batches number', 2, 'Child iterator',
                                                  ['Type', 'INTERSECT', 'Counter', 2, 'Child iterators',
                                                   ['Type', 'TEXT', 'Term', 'hello', 'Counter', 5, 'Size', 10000],
                                                   ['Type', 'TEXT', 'Term', 'other', 'Counter', 3, 'Size', 10000]]]]
  env.assertEqual(actual_res[1][4], expected_iterators_res)
  env.assertEqual(to_dict(env.cmd("FT.DEBUG", "VECSIM_INFO", "idx", "v"))['LAST_SEARCH_MODE'], 'HYBRID_BATCHES_TO_ADHOC_BF')

@skip(cluster=True)
def testResultProcessorCounter(env):
  conn = getConnectionByEnv(env)
  env.cmd('FT.CONFIG', 'SET', '_PRINT_PROFILE_CLOCK', 'false')

  env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'text')
  conn.execute_command('hset', '1', 't', 'foo')
  conn.execute_command('hset', '2', 't', 'bar')

  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', 'foo|bar', 'limit', '0', '0')
  env.assertEqual(actual_res[0], [2])
  res =  ['Result processors profile',
            ['Type', 'Index', 'Counter', 2],
            ['Type', 'Counter', 'Counter', 1]]
  env.assertEqual(actual_res[1][5], res)

@skip(cluster=True)
def testProfileMaxPrefixExpansion(env):
  conn = getConnectionByEnv(env)
  env.cmd('FT.CONFIG', 'SET', 'MAXPREFIXEXPANSIONS', 2)
  env.cmd('FT.CONFIG', 'SET', '_PRINT_PROFILE_CLOCK', 'false')

  env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'text')
  conn.execute_command('hset', '1', 't', 'foo1')
  conn.execute_command('hset', '2', 't', 'foo2')
  conn.execute_command('hset', '3', 't', 'foo3')

  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', 'foo*', 'limit', '0', '0')
  env.assertEqual(actual_res[1][4][1][6:8], ['Warning', 'Max prefix expansion reached'])

  env.cmd('FT.CONFIG', 'SET', 'MAXPREFIXEXPANSIONS', 200)

@skip(cluster=True)
def testNotIterator(env):
  conn = getConnectionByEnv(env)
  env.cmd('FT.CONFIG', 'SET', 'MAXPREFIXEXPANSIONS', 2)
  env.cmd('FT.CONFIG', 'SET', '_PRINT_PROFILE_CLOCK', 'false')
  conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'text')
  conn.execute_command('HSET', '1', 't', 'foo')
  conn.execute_command('HSET', '2', 't', 'bar')

  #before the fix, we would not get an empty iterator
  res = [[1, '1', ['t', 'foo']],
         [['Total profile time'],
          ['Parsing time'],
          ['Pipeline creation time'],
          ['Warning'],
          ['Iterators profile',
            ['Type', 'INTERSECT', 'Counter', 1, 'Child iterators',
              ['Type', 'TEXT', 'Term', 'foo', 'Counter', 1, 'Size', 1],
              ['Type', 'NOT', 'Counter', 1, 'Child iterator',
                ['Type', 'EMPTY', 'Counter', 0]]]],
          ['Result processors profile',
            ['Type', 'Index', 'Counter', 1],
            ['Type', 'Scorer', 'Counter', 1],
            ['Type', 'Sorter', 'Counter', 1], ['Type',
            'Loader', 'Counter', 1]]]]

  env.expect('ft.profile', 'idx', 'search', 'query', 'foo -@t:baz').equal(res)

def TimeoutWarningInProfile(env):
  """
  Tests the behavior of `FT.PROFILE` when a timeout occurs.
  We expect the same behavior for both strict and non-strict timeout policies.
  """

  conn = getConnectionByEnv(env)

  # Create an index
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()

  # Populate the index
  num_docs = 10000
  for i in range(num_docs):
      conn.execute_command('HSET', f'doc{i}', 't', str(i))

  # Test that we get a regular profile results with partial results when a
  # timeout is experienced on non-strict timeout policy.
  expected_res_search = [
    ANY,
    [['Total profile time', ANY],
     ['Parsing time', ANY],
     ['Pipeline creation time', ANY],
     ['Warning', 'Timeout limit was reached'],
     ['Iterators profile',
       ['Type', 'WILDCARD', 'Time', ANY, 'Counter', ANY]],
     ['Result processors profile',
       ['Type', 'Index', 'Time', ANY, 'Counter', ANY],
       ['Type', 'Scorer', 'Time', ANY, 'Counter', ANY],
       ['Type', 'Sorter', 'Time', ANY, 'Counter', ANY],
       ['Type', 'Loader', 'Time', ANY, 'Counter', ANY],
      ]]
  ]

  expected_res_aggregate = [
    ANY,
    [['Total profile time', ANY],
     ['Parsing time', ANY],
     ['Pipeline creation time', ANY],
     ['Warning', 'Timeout limit was reached'],
     ['Iterators profile',
       ['Type', 'WILDCARD', 'Time', ANY, 'Counter', ANY]],
     ['Result processors profile',
       ['Type', 'Index', 'Time', ANY, 'Counter', ANY],
       ['Type', 'Pager/Limiter', 'Time', ANY, 'Counter', ANY]
      ]]
  ]

  env.expect(
    'FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '*', 'LIMIT', '0', str(num_docs), 'TIMEOUT', '1'
  ).equal(expected_res_search)

  env.expect(
    'FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*', 'LIMIT', '0', str(num_docs), 'TIMEOUT', '1'
  ).equal(expected_res_aggregate)

@skip(cluster=True)
def testFailOnTimeout_nonStrict(env):
  TimeoutWarningInProfile(env)

@skip(cluster=True)
def testFailOnTimeout_strict():
  # The profile output is the same for strict timeout policy, i.e., the timeout
  # error becomes a warning for the `FT.PROFILE` command.
  TimeoutWarningInProfile(Env(moduleArgs="ON_TIMEOUT FAIL"))

def TimedoutTest_resp3(env):
  """Tests that the `Timedout` value of the profile response is correct"""

  conn = getConnectionByEnv(env)

  # Create an index
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()

  # Populate the index
  num_docs = 10000
  for i in range(num_docs):
      env.cmd('HSET', f'doc{i}', 't', str(i))

  # Simple `SEARCH` command
  res = conn.execute_command(
    'FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '*', 'LIMIT', '0', str(num_docs), 'TIMEOUT', '1'
  )

  env.assertEqual(res['profile']['Warning'], 'Timeout limit was reached')

  # Simple `AGGREGATE` command
  res = conn.execute_command(
    'FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*', 'TIMEOUT', '1'
  )

  env.assertEqual(res['profile']['Warning'], 'Timeout limit was reached')

def TimedOutWarningtestCoord(env):
  """Tests the `FT.PROFILE` response for the cluster build (coordinator)"""

  conn = getConnectionByEnv(env)

  # Create an index
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()

  # Populate the index
  num_docs = 20000 * env.shardsCount
  for i in range(num_docs):
      conn.execute_command('HSET', f'doc{i}', 't', str(i))

  # Simple `SEARCH` command
  res = env.cmd(
    'FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '*', 'LIMIT', '0', str(num_docs), 'TIMEOUT', '1'
  )

  # Test that a timeout warning is returned for all shards
  if env.protocol == 2:
    profile = res[-1]
    for i in range(env.shardsCount):
      shard_profile_idx = i * 7
      warning = profile[shard_profile_idx + 4]
      env.assertEqual(len(warning), 2)
      env.assertEqual(warning[1], 'Timeout limit was reached')
  else:
    profile = res['shards']
    for i in range(1, env.shardsCount + 1):
      shard_profile = profile[f'Shard #{i}']
      env.assertEquals(shard_profile['Warning'], 'Timeout limit was reached')

  # Add a test for `FT.AGGREGATE` once the profile response for it is stable.

def testTimedOutWarningCoord(env):
  SkipOnNonCluster(env)
  TimedOutWarningtestCoord(env)
