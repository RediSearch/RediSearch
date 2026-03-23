# -*- coding: utf-8 -*-

import math
import unittest
from includes import *
from common import *
from time import sleep
from RLTest import Env

@skip(cluster=True)
def testProfileSearch(env):
  conn = getConnectionByEnv(env)
  env.cmd(config_cmd(), 'SET', '_PRINT_PROFILE_CLOCK', 'false')
  dialect = int(env.cmd(config_cmd(), 'GET', 'DEFAULT_DIALECT')[0][1])

  env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'text')
  conn.execute_command('hset', '1', 't', 'hello')
  conn.execute_command('hset', '2', 't', 'world')

  env.expect('ft.profile', 'profile', 'idx', '*', 'nocontent').error().contains('SEARCH_INDEX_NOT_FOUND Index not found')
  env.expect('FT.PROFILE', 'idx', 'Puffin', '*', 'nocontent').error().contains('No `SEARCH`, `AGGREGATE`, or `HYBRID` provided')

  # test WILDCARD
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '*', 'nocontent')
  env.assertEqual(actual_res[1][1][0][3], ['Type', 'WILDCARD', 'Number of reading operations', 2])

  # test EMPTY
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', 'redis', 'nocontent')
  env.assertEqual(actual_res[1][1][0][3], ['Type', 'EMPTY', 'Number of reading operations', 0])

  # test single term
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', 'hello', 'nocontent')
  env.assertEqual(actual_res[1][1][0][3], ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 1, 'Estimated number of matches', 1])

  # test UNION
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', 'hello|world', 'nocontent')
  expected_res = ['Type', 'UNION', 'Query type', 'UNION', 'Number of reading operations', 2, 'Child iterators', [
                    ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 1, 'Estimated number of matches', 1],
                    ['Type', 'TEXT', 'Term', 'world', 'Number of reading operations', 1, 'Estimated number of matches', 1]]]
  env.assertEqual(actual_res[1][1][0][3], expected_res)

  # test INTERSECT
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', 'hello world', 'nocontent')
  expected_res = ['Type', 'INTERSECT', 'Number of reading operations', 0, 'Child iterators', [
                    ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 1, 'Estimated number of matches', 1],
                    ['Type', 'TEXT', 'Term', 'world', 'Number of reading operations', 1, 'Estimated number of matches', 1]]]
  env.assertEqual(actual_res[1][1][0][3], expected_res)

  # test NOT
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '-hello', 'nocontent')
  expected_res = ['Type', 'NOT', 'Number of reading operations', 1, 'Child iterator',
                    ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 1, 'Estimated number of matches', 1]]
  env.assertEqual(actual_res[1][1][0][3], expected_res)

  # test OPTIONAL
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '~hello', 'nocontent')
  expected_res = ['Type', 'OPTIONAL', 'Number of reading operations', 2, 'Child iterator',
                    ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 1, 'Estimated number of matches', 1]]
  env.assertEqual(actual_res[1][1][0][3], expected_res)

  # test PREFIX
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', 'hel*', 'nocontent')
  expected_res = ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 1, 'Estimated number of matches', 1]
  env.assertEqual(actual_res[1][1][0][3], expected_res)

  # test FUZZY
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '%%hel%%', 'nocontent') # codespell:ignore hel
  expected_res = ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 1, 'Estimated number of matches', 1]
  env.assertEqual(actual_res[1][1][0][3], expected_res)

  # test ID LIST iter with INKEYS
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', 'hello', 'inkeys', 1, '1')
  expected_res = ['Type', 'INTERSECT', 'Number of reading operations', 1, 'Child iterators', [
                    ['Type', 'ID-LIST-SORTED', 'Number of reading operations', 1],
                    ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 1, 'Estimated number of matches', 1]]]
  env.assertEqual(actual_res[1][1][0][3], expected_res)

  # test no crash on reaching deep reply array
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', 'hello(hello(hello(hello(hello))))', 'nocontent')
  expected_res = ['Type', 'INTERSECT', 'Number of reading operations', 1, 'Child iterators', [
                    ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 1, 'Estimated number of matches', 1],
                    ['Type', 'INTERSECT', 'Number of reading operations', 1, 'Child iterators', [
                        ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 1, 'Estimated number of matches', 1],
                        ['Type', 'INTERSECT', 'Number of reading operations', 1, 'Child iterators', [
                            ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 1, 'Estimated number of matches', 1],
                            ['Type', 'INTERSECT', 'Number of reading operations', 1, 'Child iterators', [
                                ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 1, 'Estimated number of matches', 1],
                                ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 1, 'Estimated number of matches', 1]]]]]]]]]
  expected_res_d2 = ['Type', 'INTERSECT', 'Number of reading operations', 1, 'Child iterators', [
                      ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 1, 'Estimated number of matches', 1],
                      ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 1, 'Estimated number of matches', 1],
                      ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 1, 'Estimated number of matches', 1],
                      ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 1, 'Estimated number of matches', 1],
                      ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 1, 'Estimated number of matches', 1]]]
  env.assertEqual(actual_res[1][1][0][3], expected_res if dialect == 1 else expected_res_d2)

  if server_version_less_than(env, '6.2.0'):
    return

  actual_res = env.cmd('ft.profile', 'idx', 'search', 'query',  'hello(hello(hello(hello(hello(hello)))))', 'nocontent')
  expected_res = ['Type', 'INTERSECT', 'Number of reading operations', 1, 'Child iterators', [
                    ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 1, 'Estimated number of matches', 1],
                    ['Type', 'INTERSECT', 'Number of reading operations', 1, 'Child iterators', [
                        ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 1, 'Estimated number of matches', 1],
                        ['Type', 'INTERSECT', 'Number of reading operations', 1, 'Child iterators', [
                            ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 1, 'Estimated number of matches', 1],
                            ['Type', 'INTERSECT', 'Number of reading operations', 1, 'Child iterators', [
                                ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 1, 'Estimated number of matches', 1],
                                ['Type', 'INTERSECT', 'Number of reading operations', 1, 'Child iterators', [
                                    ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 1, 'Estimated number of matches', 1],
                                    ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 1, 'Estimated number of matches', 1]]]]]]]]]]]
  expected_res_d2 = ['Type', 'INTERSECT', 'Number of reading operations', 1, 'Child iterators', [
                      ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 1, 'Estimated number of matches', 1],
                      ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 1, 'Estimated number of matches', 1],
                      ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 1, 'Estimated number of matches', 1],
                      ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 1, 'Estimated number of matches', 1],
                      ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 1, 'Estimated number of matches', 1],
                      ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 1, 'Estimated number of matches', 1]]]
  env.assertEqual(actual_res[1][1][0][3], expected_res if dialect == 1 else expected_res_d2)

@skip(cluster=True)
def testProfileSearchLimited(env):
  conn = getConnectionByEnv(env)
  env.cmd(config_cmd(), 'SET', '_PRINT_PROFILE_CLOCK', 'false')

  env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'text')
  conn.execute_command('hset', '1', 't', 'hello')
  conn.execute_command('hset', '2', 't', 'hell')
  conn.execute_command('hset', '3', 't', 'help')
  conn.execute_command('hset', '4', 't', 'helowa')

  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'limited', 'query',  '%hell% hel*') # codespell:ignore hel
  expected_res = ['Type', 'INTERSECT', 'Number of reading operations', 3, 'Child iterators', [
                  ['Type', 'UNION', 'Query type', 'FUZZY - hell', 'Number of reading operations', 3, 'Child iterators', 'The number of iterators in the union is 3'],
                  ['Type', 'UNION', 'Query type', 'PREFIX - hel', 'Number of reading operations', 3, 'Child iterators', 'The number of iterators in the union is 4']]]
  env.assertEqual(actual_res[1][1][0][3], expected_res)

@skip(cluster=True)
def testProfileAggregate(env):
  conn = getConnectionByEnv(env)
  env.cmd(config_cmd(), 'SET', '_PRINT_PROFILE_CLOCK', 'false')

  env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'text')
  conn.execute_command('hset', '1', 't', 'hello')
  conn.execute_command('hset', '2', 't', 'world')

  expected_res = [['Type', 'Index', 'Results processed', 1],
                  ['Type', 'Loader', 'Results processed', 1],
                  ['Type', 'Grouper', 'Results processed', 1]]
  actual_res = conn.execute_command('ft.profile', 'idx', 'aggregate', 'query', 'hello',
                                    'groupby', 1, '@t',
                                    'REDUCE', 'count', '0', 'as', 'sum')
  env.assertEqual(actual_res[1][1][0][5], expected_res)

  expected_res = [['Type', 'Index', 'Results processed', 2],
                  ['Type', 'Loader', 'Results processed', 2],
                  ['Type', 'Projector - Function startswith', 'Results processed', 2]]
  actual_res = env.cmd('ft.profile', 'idx', 'aggregate', 'query', '*',
                'load', 1, 't',
                'apply', 'startswith(@t, "hel")', 'as', 'prefix') # codespell:ignore hel
  env.assertEqual(actual_res[1][1][0][5], expected_res)

  expected_res = [['Type', 'Index', 'Results processed', 2],
                  ['Type', 'Loader', 'Results processed', 2],
                  ['Type', 'Projector - Literal banana', 'Results processed', 2]]
  actual_res = env.cmd('ft.profile', 'idx', 'aggregate', 'query', '*',
                'load', 1, 't',
                'apply', '"banana"', 'as', 'prefix')
  env.assertEqual(actual_res[1][1][0][5], expected_res)

  expected_res = [['Type', 'Index', 'Results processed', 2],
                  ['Type', 'Loader', 'Results processed', 2],
                  ['Type', 'Sorter', 'Results processed', 2],
                  ['Type', 'Loader', 'Results processed', 2]]
  actual_res = env.cmd('ft.profile', 'idx', 'aggregate', 'query', '*', 'sortby', 2, '@t', 'asc', 'limit', 0, 10, 'LOAD', 2, '@__key', '@t')
  env.assertEqual(actual_res[1][1][0][5], expected_res)

def testProfileCursor(env):
  conn = getConnectionByEnv(env)
  env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'text')
  env.expect('ft.profile', 'idx', 'search', 'bad_arg1', 'bad_arg2').error() # This also should not crash nor fail on memory checks
  env.expect('ft.profile', 'idx', 'aggregate', 'query', '*', 'WITHCURSOR').error().contains('FT.PROFILE does not support cursor')


def testProfileErrors(env):
  env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'text')
  # missing args
  env.expect('ft.profile', 'idx').error().contains('wrong number of arguments')
  env.expect('ft.profile', 'idx', 'SEARCH').error().contains('wrong number of arguments')
  env.expect('ft.profile', 'idx', 'SEARCH', 'QUERY').error().contains('wrong number of arguments')
  # wrong `query` type
  env.expect('ft.profile', 'idx', 'redis', 'QUERY', '*').error().contains('No `SEARCH`, `AGGREGATE`, or `HYBRID` provided')
  # miss `QUERY` keyword
  env.expect('ft.profile', 'idx', 'SEARCH', 'FIND', '*').error().contains('The QUERY keyword is expected')

@skip(cluster=True)
def testProfileNumeric(env):
  conn = getConnectionByEnv(env)
  env.cmd(config_cmd(), 'SET', '_PRINT_PROFILE_CLOCK', 'false')

  env.cmd('ft.create', 'idx', 'SCHEMA', 'n', 'numeric')
  for i in range(10000):
    conn.execute_command('hset', i, 'n', 50 - float(i % 1000) / 10)

  expected_res = ['Iterators profile', ['Type', 'UNION', 'Query type', 'NUMERIC', 'Number of reading operations', 5010, 'Child iterators', [
                    ['Type', 'NUMERIC', 'Term', '-49.9 - 32.6', 'Number of reading operations', 3270, 'Estimated number of matches', 8260],
                    ['Type', 'NUMERIC', 'Term', '32.7 - 45.4', 'Number of reading operations', 1280, 'Estimated number of matches', 1280],
                    ['Type', 'NUMERIC', 'Term', '45.5 - 49.1', 'Number of reading operations', 370, 'Estimated number of matches', 370],
                    ['Type', 'NUMERIC', 'Term', '49.2 - 50', 'Number of reading operations', 90, 'Estimated number of matches', 90]]]]
  # [1] (Profile data) -> [1] (`Shards` value) -> [0] (single shard/standalone) -> [2:4] (Iterators profile - key+value)
  env.expect('ft.profile', 'idx', 'search', 'query', '@n:[0,100]', 'nocontent').apply(
    lambda x: x[1][1][0][2:4]).equal(expected_res)

@skip(cluster=True)
def testProfileNegativeNumeric():
  env = Env(protocol=3)
  conn = getConnectionByEnv(env)
  env.cmd(config_cmd(), 'SET', '_PRINT_PROFILE_CLOCK', 'false')

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
    Iterators_profile = actual_res['Profile']['Shards'][0]['Iterators profile']
    child_iter_list = Iterators_profile['Child iterators']

    def extract_child_range(child: dict):
      iter_term = child['Term']
      res_range = iter_term.split(" - ")
      range_dict = {"min":float(res_range[0]), "max": float(res_range[1])}
      env.assertEqual(range_dict['max'], range_dict['min'] + child['Estimated number of matches'] - 1, message=f"{title}: range_max should equal range_min + (range_size - 1)")
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
  env.cmd(config_cmd(), 'SET', '_PRINT_PROFILE_CLOCK', 'false')

  env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'tag')
  conn.execute_command('hset', '1', 't', 'foo,bar')
  conn.execute_command('hset', '2', 't', 'food,bag')
  conn.execute_command('hset', '3', 't', 'foo')

  # tag profile
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '@t:{foo}', 'nocontent')
  env.assertEqual(actual_res[1][1][0][3], ['Type', 'TAG', 'Term', 'foo', 'Number of reading operations', 2, 'Estimated number of matches', 2])

@skip(cluster=True)
def testProfileVector(env):
  conn = getConnectionByEnv(env)
  env.cmd(config_cmd(), 'SET', '_PRINT_PROFILE_CLOCK', 'false')
  env.cmd(config_cmd(), 'SET', 'DEFAULT_DIALECT', '2')

  env.expect('FT.CREATE idx SCHEMA v VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2 t TEXT').ok()
  conn.execute_command('hset', '1', 'v', 'bababaca', 't', "hello")
  conn.execute_command('hset', '2', 'v', 'babababa', 't', "hello")
  conn.execute_command('hset', '3', 'v', 'aabbaabb', 't', "hello")
  conn.execute_command('hset', '4', 'v', 'bbaabbaa', 't', "hello world")
  conn.execute_command('hset', '5', 'v', 'aaaabbbb', 't', "hello world")

  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '*=>[KNN 3 @v $vec]',
                                    'SORTBY', '__v_score', 'PARAMS', '2', 'vec', 'aaaaaaaa', 'nocontent')
  expected_iterators_res = ['Type', 'VECTOR', 'Number of reading operations', 3, 'Vector search mode', 'STANDARD_KNN']
  expected_vecsim_rp_res = ['Type', 'Metrics Applier', 'Results processed', 3]
  env.assertEqual(actual_res[0], [3, '4', '2', '1'])
  actual_profile = to_dict(actual_res[1][1][0])
  env.assertEqual(actual_profile['Iterators profile'], expected_iterators_res)
  env.assertEqual(actual_profile['Result processors profile'][1], expected_vecsim_rp_res)
  env.assertEqual(to_dict(env.cmd(debug_cmd(), "VECSIM_INFO", "idx", "v"))['LAST_SEARCH_MODE'], 'STANDARD_KNN')

  # Range query - uses metric iterator. Radius is set so that the closest 2 vectors will be in the range
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '@v:[VECTOR_RANGE 3e36 $vec]=>{$yield_distance_as:dist}',
                                    'SORTBY', 'dist', 'PARAMS', '2', 'vec', 'aaaaaaaa', 'nocontent')
  expected_iterators_res = ['Type', 'METRIC SORTED BY ID - VECTOR DISTANCE', 'Number of reading operations', 2, 'Vector search mode', 'RANGE_QUERY']
  expected_vecsim_rp_res = ['Type', 'Metrics Applier', 'Results processed', 2]
  env.assertEqual(actual_res[0], [2, '4', '2'])
  actual_profile = to_dict(actual_res[1][1][0])
  env.assertEqual(actual_profile['Iterators profile'], expected_iterators_res)
  env.assertEqual(actual_profile['Result processors profile'][1], expected_vecsim_rp_res)
  env.assertEqual(to_dict(env.cmd(debug_cmd(), "VECSIM_INFO", "idx", "v"))['LAST_SEARCH_MODE'], 'RANGE_QUERY')

# Test with hybrid query variations
  # Expect ad-hoc BF to take place - going over child iterator exactly once (reading 2 results)
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '(@t:hello world)=>[KNN 3 @v $vec]',
                                    'SORTBY', '__v_score', 'PARAMS', '2', 'vec', 'aaaaaaaa', 'nocontent')
  expected_iterators_res = ['Type', 'VECTOR', 'Number of reading operations', 2, 'Vector search mode', 'HYBRID_ADHOC_BF', 'Child iterator',
                            ['Type', 'INTERSECT', 'Number of reading operations', 2, 'Child iterators', [
                              ['Type', 'TEXT', 'Term', 'world', 'Number of reading operations', 2, 'Estimated number of matches', 2],
                              ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 2, 'Estimated number of matches', 5]]]]
  expected_vecsim_rp_res = ['Type', 'Metrics Applier', 'Results processed', 2]
  env.assertEqual(actual_res[0], [2, '4', '5'])
  actual_profile = to_dict(actual_res[1][1][0])
  env.assertEqual(actual_profile['Iterators profile'], expected_iterators_res)
  env.assertEqual(actual_profile['Result processors profile'][1], expected_vecsim_rp_res)
  env.assertEqual(to_dict(env.cmd(debug_cmd(), "VECSIM_INFO", "idx", "v"))['LAST_SEARCH_MODE'], 'HYBRID_ADHOC_BF')

  for i in range(6, 10001):
    conn.execute_command('hset', str(i), 'v', 'bababada', 't', "hello world")
  # Expect batched search to take place - going over child iterator exactly once (reading 2 results)
  # Expect in the first batch to get 1, 2, 4, 6 and then ask for one more batch - and get 7 in the next results.
  # In the second batch - batch size is determined to be 2
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '(@t:hello world)=>[KNN 3 @v $vec]',
                                    'SORTBY', '__v_score', 'PARAMS', '2', 'vec', 'aaaaaaaa', 'nocontent')
  env.assertEqual(actual_res[0], [3, '4', '6', '7'])
  expected_iterators_res = ['Type', 'VECTOR', 'Number of reading operations', 3, 'Vector search mode', 'HYBRID_BATCHES', 'Batches number', 2, 'Largest batch size', 4, 'Largest batch iteration (zero based)', 0, 'Child iterator',
                            ['Type', 'INTERSECT', 'Number of reading operations', 8, 'Child iterators', [
                              ['Type', 'TEXT', 'Term', 'world', 'Number of reading operations', 8, 'Estimated number of matches', 9997],
                              ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 8, 'Estimated number of matches', 10000]]]]
  expected_vecsim_rp_res = ['Type', 'Metrics Applier', 'Results processed', 3]
  actual_profile = to_dict(actual_res[1][1][0])
  env.assertEqual(actual_profile['Iterators profile'], expected_iterators_res)
  env.assertEqual(actual_profile['Result processors profile'][1], expected_vecsim_rp_res)
  env.assertEqual(to_dict(env.cmd(debug_cmd(), "VECSIM_INFO", "idx", "v"))['LAST_SEARCH_MODE'], 'HYBRID_BATCHES')

  # Add another 10K vectors with a different tag.
  for i in range(10001, 20001):
    conn.execute_command('hset', str(i), 'v', '????????', 't', "other")

  # expected results that pass the filter is index_size/2. after two iterations with no results,
  # we should move ad-hoc BF.
  expected_iterators_res = ['Type', 'VECTOR', 'Number of reading operations', 0, 'Vector search mode', 'HYBRID_BATCHES_TO_ADHOC_BF', 'Batches number', 2, 'Largest batch size', 13, 'Largest batch iteration (zero based)', 1, 'Child iterator',
                            ['Type', 'INTERSECT', 'Number of reading operations', 2, 'Child iterators', [
                             ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 5, 'Estimated number of matches', 10000],
                             ['Type', 'TEXT', 'Term', 'other', 'Number of reading operations', 3, 'Estimated number of matches', 10000]]]]
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '(@t:hello other)=>[KNN 3 @v $vec]',
                                      'SORTBY', '__v_score', 'PARAMS', '2', 'vec', '????????', 'nocontent')
  actual_profile = to_dict(actual_res[1][1][0])
  env.assertEqual(actual_profile['Iterators profile'], expected_iterators_res)
  env.assertEqual(to_dict(env.cmd(debug_cmd(), "VECSIM_INFO", "idx", "v"))['LAST_SEARCH_MODE'], 'HYBRID_BATCHES_TO_ADHOC_BF')

  # Ask explicitly to run in batches mode, without asking for a certain batch size.
  # First batch size is 4, and every batch should be double in its size from its previous one. We go over the entire
  # index after the 13th batch.
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '(@t:hello other)=>[KNN 2 @v $vec HYBRID_POLICY BATCHES]',
                                    'SORTBY', '__v_score', 'PARAMS', '2', 'vec', '????????', 'nocontent')
  expected_iterators_res = ['Type', 'VECTOR', 'Number of reading operations', 0, 'Vector search mode', 'HYBRID_BATCHES', 'Batches number', 13, 'Largest batch size', 20001, 'Largest batch iteration (zero based)', 12, 'Child iterator',
                             ['Type', 'INTERSECT', 'Number of reading operations', 12, 'Child iterators', [
                              ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 25, 'Estimated number of matches', 10000],
                              ['Type', 'TEXT', 'Term', 'other', 'Number of reading operations', 13, 'Estimated number of matches', 10000]]]]
  actual_profile = to_dict(actual_res[1][1][0])
  env.assertEqual(actual_profile['Iterators profile'], expected_iterators_res)
  env.assertEqual(to_dict(env.cmd(debug_cmd(), "VECSIM_INFO", "idx", "v"))['LAST_SEARCH_MODE'], 'HYBRID_BATCHES')

  # Ask explicitly to run in batches mode, with batch size of 100.
  # After 200 iterations, we should go over the entire index.
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '(@t:hello other)=>[KNN 2 @v $vec HYBRID_POLICY BATCHES BATCH_SIZE 100]',
                                    'SORTBY', '__v_score', 'PARAMS', '2', 'vec', '????????', 'nocontent', 'timeout', '100000')
  expected_iterators_res = ['Type', 'VECTOR', 'Number of reading operations', 0, 'Vector search mode', 'HYBRID_BATCHES', 'Batches number', 200, 'Largest batch size', 100, 'Largest batch iteration (zero based)', 0, 'Child iterator',
                            ['Type', 'INTERSECT', 'Number of reading operations', 199, 'Child iterators', [
                             ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 399, 'Estimated number of matches', 10000],
                             ['Type', 'TEXT', 'Term', 'other', 'Number of reading operations', 200, 'Estimated number of matches', 10000]]]]
  actual_profile = to_dict(actual_res[1][1][0])
  env.assertEqual(actual_profile['Iterators profile'], expected_iterators_res)
  env.assertEqual(to_dict(env.cmd(debug_cmd(), "VECSIM_INFO", "idx", "v"))['LAST_SEARCH_MODE'], 'HYBRID_BATCHES')

  # Asking only for a batch size without asking for batches policy. While batchs mode is on, the bacth size will be as
  # requested, but the mode can change dynamically to ADHOC-BF.
  # Note that the batch_size here as no effect, since the child_num_estimated will always be decreased in half after
  # every iteration that returned 0 results.
  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', '(@t:hello other)=>[KNN 2 @v $vec BATCH_SIZE 100]',
                                    'SORTBY', '__v_score', 'PARAMS', '2', 'vec', '????????', 'nocontent')
  expected_iterators_res = ['Type', 'VECTOR', 'Number of reading operations', 0, 'Vector search mode', 'HYBRID_BATCHES_TO_ADHOC_BF', 'Batches number', 2, 'Largest batch size', 100, 'Largest batch iteration (zero based)', 0, 'Child iterator',
                            ['Type', 'INTERSECT', 'Number of reading operations', 2, 'Child iterators', [
                             ['Type', 'TEXT', 'Term', 'hello', 'Number of reading operations', 5, 'Estimated number of matches', 10000],
                             ['Type', 'TEXT', 'Term', 'other', 'Number of reading operations', 3, 'Estimated number of matches', 10000]]]]
  actual_profile = to_dict(actual_res[1][1][0])
  env.assertEqual(actual_profile['Iterators profile'], expected_iterators_res)
  env.assertEqual(to_dict(env.cmd(debug_cmd(), "VECSIM_INFO", "idx", "v"))['LAST_SEARCH_MODE'], 'HYBRID_BATCHES_TO_ADHOC_BF')

@skip(cluster=True)
def testResultProcessorCounter(env):
  conn = getConnectionByEnv(env)
  env.cmd(config_cmd(), 'SET', '_PRINT_PROFILE_CLOCK', 'false')

  env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'text')
  conn.execute_command('hset', '1', 't', 'foo')
  conn.execute_command('hset', '2', 't', 'bar')

  actual_res = conn.execute_command('ft.profile', 'idx', 'search', 'query', 'foo|bar', 'limit', '0', '0')
  env.assertEqual(actual_res[0], [2])
  res = [['Type', 'Index', 'Results processed', 2],
         ['Type', 'Counter', 'Results processed', 1]]
  env.assertEqual(actual_res[1][1][0][5], res)

@skip(cluster=True)
def testNotIterator(env):
  conn = getConnectionByEnv(env)
  env.cmd(config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', 2)
  env.cmd(config_cmd(), 'SET', '_PRINT_PROFILE_CLOCK', 'false')
  conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'text')
  conn.execute_command('HSET', '1', 't', 'foo')
  conn.execute_command('HSET', '2', 't', 'bar')

  #before the fix, we would not get an empty iterator
  res = [[1, '1', ['t', 'foo']],
         ['Shards', [[
            'Warning', ['None'],
            'Iterators profile', # Static query optimization: foo && -@t:baz => foo && -(EMPTY) => foo && ALL => foo
            ['Type', 'TEXT', 'Term', 'foo', 'Number of reading operations', 1, 'Estimated number of matches', 1],
            'Result processors profile',
             [['Type', 'Index',  'Results processed', 1],
              ['Type', 'Scorer', 'Results processed', 1],
              ['Type', 'Sorter', 'Results processed', 1],
              ['Type', 'Loader', 'Results processed', 1]]
            ]],
          'Coordinator', []
        ]]


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
    ['Shards',
     [['Total profile time', ANY,
       'Parsing time', ANY,
       'Workers queue time', ANY,
       'Pipeline creation time', ANY,
       'Warning', ['Timeout limit was reached'],
       'Iterators profile',
         ['Type', 'WILDCARD', 'Time', ANY, 'Number of reading operations', ANY],
       'Result processors profile',
         [['Type', 'Index',  'Time', ANY, 'Results processed', ANY],
          ['Type', 'Scorer', 'Time', ANY, 'Results processed', ANY],
          ['Type', 'Sorter', 'Time', ANY, 'Results processed', ANY],
          ['Type', 'Loader', 'Time', ANY, 'Results processed', ANY],
         ]
      ]],
     'Coordinator', []
    ]
  ]

  expected_res_aggregate = [
    ANY,
    ['Shards',
     [['Total profile time', ANY,
       'Parsing time', ANY,
       'Workers queue time', ANY,
       'Pipeline creation time', ANY,
       'Warning', ['Timeout limit was reached'],
       'Iterators profile',
        ['Type', 'WILDCARD', 'Time', ANY, 'Number of reading operations', ANY],
       'Result processors profile',
        [['Type', 'Index', 'Time', ANY, 'Results processed', ANY],
         ['Type', 'Pager/Limiter', 'Time', ANY, 'Results processed', ANY]]
      ]],
     'Coordinator', []]
  ]

  env.expect(
    'FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '*', 'LIMIT', '0', str(num_docs), 'TIMEOUT', '1'
  ).equal(expected_res_search)

  env.expect(
    'FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*', 'LIMIT', '0', str(num_docs), 'TIMEOUT', '1'
  ).equal(expected_res_aggregate)

@skip(cluster=True)
def testFailOnTimeout_nonStrict(env):
  TimeoutWarningInProfile(Env(moduleArgs="ON_TIMEOUT RETURN"))

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

  for shard_profile in res['Profile']['Shards']:
    env.assertEqual(shard_profile['Warning'], ['Timeout limit was reached'])

  # Simple `AGGREGATE` command
  res = conn.execute_command(
    'FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*', 'TIMEOUT', '1'
  )

  for shard_profile in res['Profile']['Shards']:
    env.assertEqual(shard_profile['Warning'], ['Timeout limit was reached'])

def TimedOutWarningtestCoord(env):
  """Tests the `FT.PROFILE` response for the cluster build (coordinator)"""

  conn = getConnectionByEnv(env)

  # Create an index
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()

  # Populate the index
  num_docs = 30000 * env.shardsCount
  for i in range(num_docs):
      conn.execute_command('HSET', f'doc{i}', 't', str(i))

  # Simple `SEARCH` command
  res = env.cmd(
    'FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '*', 'LIMIT', '0', str(num_docs), 'TIMEOUT', '1'
  )

  # Test that a timeout warning is returned for all shards
  if env.protocol == 2:
    for shard_profile in res[1][1]:
      env.assertEqual(to_dict(shard_profile)['Warning'], ['Timeout limit was reached'])
  else:
    for shard_profile in res['Profile']['Shards']:
      env.assertEquals(shard_profile['Warning'], ['Timeout limit was reached'])

  res = env.cmd('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*', 'TIMEOUT', '1')
  coord_profile = None
  shards_profile = None
  if env.protocol == 2:
    coord_profile = to_dict(res[-1][-1])
    shards_profile = res[1][1]
  else:
    coord_profile = res['Profile']['Coordinator']
    shards_profile = res['Profile']['Shards']

  env.assertEqual(coord_profile['Warning'], ['Timeout limit was reached'])
  env.assertEqual(len(shards_profile), env.shardsCount)

@skip(asan=True, msan=True, cluster=False)
def testTimedOutWarningCoordResp3():
  TimedOutWarningtestCoord(Env(protocol=3))

@skip(asan=True, msan=True, cluster=False)
def testTimedOutWarningCoordResp2():
  TimedOutWarningtestCoord(Env(protocol=2))

def InternalCursorReadsInProfile(protocol):
  """Tests that 'Internal cursor reads' appears in shard profiles for AGGREGATE."""
  # Limit number of shards to avoid creating too many docs
  env = Env(shardsCount=2, protocol=protocol)
  conn = getConnectionByEnv(env)
  env.cmd(config_cmd(), 'SET', '_PRINT_PROFILE_CLOCK', 'false')

  env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()

  # Insert docs - with default cursorReadSize=1000, each shard needs more than 1000 to require 2 reads
  num_docs = int(1000 * 1.1 * env.shardsCount)
  for i in range(num_docs):
    conn.execute_command('HSET', f'doc{i}', 't', f'hello{i}')

  # Run FT.PROFILE AGGREGATE - coordinator uses internal cursors to shards
  res = env.cmd('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*')

  shards_profile = get_shards_profile(env, res)
  env.assertEqual(len(shards_profile), env.shardsCount, message=f"unexpected number of shards. full reply output: {res}")

  # Each shard should have exactly 2 cursor reads (1000+ docs per shard, default cursorReadSize=1000)
  for shard_profile in shards_profile:
    env.assertContains('Internal cursor reads', shard_profile)
    env.assertEqual(shard_profile['Internal cursor reads'], 2)

@skip(cluster=False)
def testInternalCursorReadsInProfileResp3():
  InternalCursorReadsInProfile(protocol=3)

@skip(cluster=False)
def testInternalCursorReadsInProfileResp2():
  InternalCursorReadsInProfile(protocol=2)

@skip(cluster=False)
def testInternalCursorReadsWithTimeoutResp3():
  """Tests 'Internal cursor reads' with timeout - RESP3 coordinator detects timeout and stops early."""
  env = Env(protocol=3)
  conn = getConnectionByEnv(env)
  run_command_on_all_shards(env, config_cmd(), 'SET', '_PRINT_PROFILE_CLOCK', 'false')

  env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()

  num_docs = 100
  for i in range(num_docs):
    conn.execute_command('HSET', f'doc{i}', 't', f'hello{i}')

  # Run FT.PROFILE AGGREGATE with simulated timeout on shards only
  query = ['FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*']
  timeout_after_n = 5
  res = runDebugQueryCommandTimeoutAfterN(env, query, timeout_after_n, internal_only=True)

  # RESP3: coordinator detects shard timeout and stops early after reading first shard's reply
  # Results count equals first shard's reply length (timeout_after_n)
  env.assertEqual(len(res['Results']['results']), timeout_after_n)

  shards_profile = get_shards_profile(env, res)
  for shard_profile in shards_profile:
    env.assertContains('Internal cursor reads', shard_profile, message=f"full reply output: {res}")
    # Coordinator stops after first timeout, so only 1 cursor read per shard
    env.assertEqual(shard_profile['Internal cursor reads'], 1, message=f"full reply output: {res}")
    env.assertEqual(shard_profile['Warning'], ['Timeout limit was reached'], message=f"full reply output: {res}")

@skip(cluster=False)
def testInternalCursorReadsWithTimeoutResp2():
  """Tests 'Internal cursor reads' with timeout - RESP2 coordinator doesn't detect timeout, reads until EOF."""
  env = Env(shardsCount=2, protocol=2)
  conn = getConnectionByEnv(env)
  run_command_on_all_shards(env, config_cmd(), 'SET', '_PRINT_PROFILE_CLOCK', 'false')

  env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()

  num_docs = 100
  for i in range(num_docs):
    conn.execute_command('HSET', f'doc{i}', 't', f'hello{i}')

  # Run FT.PROFILE AGGREGATE with simulated timeout on shards only
  query = ['FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*']
  timeout_after_n = 5
  res = runDebugQueryCommandTimeoutAfterN(env, query, timeout_after_n, internal_only=True)

  # RESP2: coordinator doesn't check shard timeout, reads until EOF
  # All docs are returned
  env.assertEqual(len(res[0]) - 1, num_docs)

  shards_profile = get_shards_profile(env, res)
  env.assertEqual(len(shards_profile), env.shardsCount, message=f"unexpected number of shards. full reply output: {res}")

  # Verify total cursor reads matches expected (order of shards may differ)
  total_expected_reads = 0
  for shard_conn in env.getOSSMasterNodesConnectionList():
    docs_on_shard = shard_conn.execute_command('DBSIZE')
    total_expected_reads += math.ceil(docs_on_shard / timeout_after_n)

  # The order of shards in the profile response may differ, so we can't check per-shard
  total_actual_reads = sum(sp['Internal cursor reads'] for sp in shards_profile)
  env.assertEqual(total_actual_reads, total_expected_reads, message=f"full reply output: {res}")

  # Verify each shard has warning
  for shard_profile in shards_profile:
    env.assertContains('Internal cursor reads', shard_profile, message=f"full reply output: {res}")
    env.assertEqual(shard_profile['Warning'], ['Timeout limit was reached'], message=f"full reply output: {res}")

  # Coordinator should NOT have timeout warning (it doesn't detect it in RESP2)
  coord_profile = to_dict(res[-1][-1])
  env.assertEqual(coord_profile['Warning'], ['None'], message=f"full reply output: {res}")

@skip(cluster=False)
def testPersistProfileWarning_MaxPrefixExpansions():
  """
  Tests that max prefix expansion warning triggered on the first internal cursor read
  is persisted and appears in the final profile output.

  In cluster mode, FT.AGGREGATE uses internal cursors between coordinator and shards.
  The warning is set during query parsing on the first read. This test verifies the
  warning is preserved across multiple cursor reads and appears in the shard profile.
  """
  env = Env(protocol=3)
  conn = getConnectionByEnv(env)

  env.cmd(config_cmd(), 'SET', '_PRINT_PROFILE_CLOCK', 'false')
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()

  # Create 1100 docs per shard to exceed default cursorReadSize (1000), forcing multiple cursor reads
  for i in range(1100 * env.shardsCount):
    conn.execute_command('HSET', f'doc{i}', 't', f'hello{i}')

  # Set MAXPREFIXEXPANSIONS limit to exceed the cursorReadSize, but fewer than total docs.
  # This ensures: (1) the warning is triggered, (2) results span multiple cursor reads
  run_command_on_all_shards(env, config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', '1001')
  res = env.cmd('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', 'hell*')

  # Verify warning appears in the top-level response
  env.assertContains('Max prefix expansions limit was reached', res['Results']['warning'])
  # Verify warning is persisted in each shard's profile (printed on last cursor read)
  for shard_profile in get_shards_profile(env, res):
    env.assertContains('Max prefix expansions limit was reached', shard_profile['Warning'])

# This test is currently skipped due to flaky behavior of some of the machines'
# timers. MOD-6436
@skip()
def testNonZeroTimers(env):
  """Tests that the timers' values of the `FT.PROFILE` response are populated
  with non-zero values.
  On cluster mode, we test only the cluster's timer, and on standalone mode we
  test the timers of the shard"""

  # Heavy test
  if VALGRIND:
    env.skip()

  # Populate the db (with index and docs)
  n_docs = 60000 if not env.isCluster() else 5000
  populate_db(env, text=True, tag=True, numeric=True, n_per_shard=n_docs)

  query = "@text1:lala*"
  search_command = "FT.PROFILE idx SEARCH QUERY".split(' ')
  search_command += [query, 'LIMIT', '0', str(n_docs), 'DIALECT', '2']
  aggregate_command = "FT.PROFILE idx AGGREGATE QUERY".split(' ')
  aggregate_command += [query, 'LOAD', '1', 'text1', 'DIALECT', '2']

  def test_timers(res):
    """Tests that the timers of the profile response of a shard are non-zero."""
    # Query iterators
    env.assertGreater(int(res[1][0][1]), 0)
    iterators_profile = res[1][1][0][3]
    union_qi = iterators_profile[1]
    env.assertGreater(int(union_qi[5]), 0)
    term_qi = union_qi[9]
    env.assertGreater(int(term_qi[5]), 0)

    # Result processors
    rps_profile = res[1][5][1:]
    for i in range(len(rps_profile)):
      rp_profile = rps_profile[i]
      env.assertGreater(int(rp_profile[3]), 0)

  def test_cluster_timer(env):
    res = env.cmd(*search_command)
    # Check that the total time is larger than 0
    env.assertGreater(float(res[-1][-1][1]), 0)

    res = env.cmd(*aggregate_command)
    # Check that the total time is larger than 0
    env.assertGreater(float(res[-1][-1]), 0)

  def test_shard_timers(env):
    for cmd in [search_command, aggregate_command]:
      res = env.cmd(*cmd)
      test_timers(res)

  if env.isCluster():
    test_cluster_timer(env)
  else:
    test_shard_timers(env)

def extract_profile_coordinator_and_shards(env, res):
  # Extract coordinator and shards from FT.PROFILE response based on protocol.
  if env.protocol == 3:
    return res['Profile']['Coordinator'], res['Profile']['Shards']
  else:
    # RESP2: res[-1] is ['Shards', [...], 'Coordinator', {...}]
    # res[-1][1] is shards array, res[-1][-1] is coordinator
    return to_dict(res[-1][-1]), [to_dict(s) for s in res[-1][1]]

def find_threadsafe_loader(env, shard):
  # Find the Threadsafe-Loader entry in shard's Result processors profile.
  rp_profile = shard['Result processors profile']
  if env.protocol == 3:
    return next((rp for rp in rp_profile if rp.get('Type') == 'Threadsafe-Loader'), None)
  else:
    # RESP2: rp_profile is a list of lists like [['Type', 'Index', ...], ['Type', 'Threadsafe-Loader', ...], ...]
    return next((to_dict(rp) for rp in rp_profile if 'Threadsafe-Loader' in rp), None)

def sum_rp_times(env, shard):
  # Sum all Result Processor times from a shard profile.
  rp_profile = shard['Result processors profile']
  total = 0.0
  if env.protocol == 3:
    for rp in rp_profile:
      total += float(rp.get('Time', 0))
  else:
    for rp in rp_profile:
      rp_dict = to_dict(rp)
      # In RESP2, Time is returned as a string
      total += float(rp_dict.get('Time', 0))
  return total

def ProfileTotalTimeConsistency(env, num_docs):
  """Tests that Total profile time >= sum of Result Processor times.

  Tests multiple commands with various result processors to ensure timing
  consistency across different query types:
  - FT.SEARCH with Scorer, Sorter, Loader
  - FT.AGGREGATE with Loader, Grouper, Sorter, Projector (APPLY), Pager/Limiter
  """
  conn = getConnectionByEnv(env)
  run_command_on_all_shards(env, config_cmd(), 'SET', '_PRINT_PROFILE_CLOCK', 'true')

  # Create index with TEXT and NUMERIC fields for diverse query options
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'n', 'NUMERIC', 'SORTABLE').ok()

  for i in range(num_docs):
    conn.execute_command('HSET', f'doc{i}', 't', f'hello{i % 100}', 'n', i)

  def verify_timing_consistency(res, command_desc):
    """Helper to verify total time >= sum of RP times for all shards."""
    _, shards = extract_profile_coordinator_and_shards(env, res)
    for shard in shards:
      # In RESP2, Total profile time is returned as a string
      total_time = float(shard['Total profile time'])
      rp_times_sum = sum_rp_times(env, shard)
      env.assertGreaterEqual(total_time, rp_times_sum,
        message=f"{command_desc}: Total profile time ({total_time}) < sum of RP times ({rp_times_sum}). Full response: {res}")

  # Test 1: Simple FT.AGGREGATE with wildcard query
  # Result processors: Index, Pager/Limiter
  res = env.cmd('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*')
  verify_timing_consistency(res, "FT.AGGREGATE wildcard")

  # Test 2: FT.AGGREGATE with LOAD, GROUPBY, REDUCE
  # Result processors: Index, Loader, Grouper
  res = env.cmd('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*',
                'LOAD', '1', '@t',
                'GROUPBY', '1', '@t',
                'REDUCE', 'COUNT', '0', 'AS', 'count')
  verify_timing_consistency(res, "FT.AGGREGATE with GROUPBY")

  # Test 3: FT.AGGREGATE with LOAD, APPLY, SORTBY, LIMIT
  # Result processors: Index, Loader, Projector, Sorter, Pager/Limiter
  res = env.cmd('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*',
                'LOAD', '2', '@t', '@n',
                'APPLY', '@n * 2', 'AS', 'doubled',
                'SORTBY', '2', '@n', 'ASC',
                'LIMIT', '0', '100')
  verify_timing_consistency(res, "FT.AGGREGATE with APPLY/SORTBY/LIMIT")

  # Test 4: FT.SEARCH with default options
  # Result processors: Index, Scorer, Sorter, Loader
  res = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '*',
                'LIMIT', '0', '100')
  verify_timing_consistency(res, "FT.SEARCH wildcard")

  # Test 5: FT.SEARCH with SORTBY on numeric field
  # Result processors: Index, Scorer, Sorter, Loader
  res = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '*',
                'SORTBY', 'n', 'ASC',
                'LIMIT', '0', '100')
  verify_timing_consistency(res, "FT.SEARCH with SORTBY")

  # Test 6: FT.SEARCH with text query and NOCONTENT
  # Result processors: Index, Scorer, Sorter (fewer processors, faster)
  res = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', 'hello0',
                'NOCONTENT',
                'LIMIT', '0', '100')
  verify_timing_consistency(res, "FT.SEARCH text query NOCONTENT")

@skip(cluster=False)
def testProfileTotalTimeConsistencyClusterResp3():
  """Tests timing consistency in cluster mode with multiple cursor reads - RESP3."""
  # Use enough docs to trigger multiple cursor reads (>1000 per shard)
  env = Env(shardsCount=2, protocol=3)
  num_docs = int(1000 * 1.5 * env.shardsCount)
  ProfileTotalTimeConsistency(env, num_docs)

@skip(cluster=False)
def testProfileTotalTimeConsistencyClusterResp2():
  """Tests timing consistency in cluster mode with multiple cursor reads - RESP2."""
  env = Env(shardsCount=2, protocol=2)
  num_docs = int(1000 * 1.5 * env.shardsCount)
  ProfileTotalTimeConsistency(env, num_docs)

@skip(cluster=True)
def testProfileTotalTimeConsistencyStandaloneResp3():
  """Tests timing consistency in standalone mode - RESP3."""
  env = Env(protocol=3)
  # Use enough docs to ensure meaningful timing data and avoid flakiness.
  # Serialization time is not counted in result processor times, so we need
  # enough results to make the timing difference significant across machines.
  ProfileTotalTimeConsistency(env, num_docs=1500)

@skip(cluster=True)
def testProfileTotalTimeConsistencyStandaloneResp2():
  """Tests timing consistency in standalone mode - RESP2."""
  env = Env(protocol=2)
  # Use enough docs to ensure meaningful timing data and avoid flakiness.
  # Serialization time is not counted in result processor times, so we need
  # enough results to make the timing difference significant across machines.
  ProfileTotalTimeConsistency(env, num_docs=1500)

def ProfileGILTime(env):
  # Test FT.PROFILE GIL time reporting across all worker combinations.
  # (Standalone and Coordinator behave the same)

  # Test matrix and expected behavior:
  # +---------+-----------+---------------------------+--------------------------------------+
  # | Workers | With Load | Coordinator               | Shard                                |
  # +---------+-----------+---------------------------+--------------------------------------+
  # | 0       | N/A       | No "Total GIL time"       | No "Total GIL time"                  |
  # | 1       | No        | No "Total GIL time"       | "Total GIL time" > 0                 |
  # | 1       | Yes       | No "Total GIL time"       | "Total GIL time" >= Loader GIL > 0   |
  # +---------+-----------+---------------------------+--------------------------------------+

  conn = getConnectionByEnv(env)
  is_cluster = env.isCluster()
  num_shards = env.shardsCount
  protocol = env.protocol

  env.expect('ft.create', 'idx', 'SCHEMA', 'f', 'TEXT').ok()

  # Populate db
  for i in range(10):
    conn.execute_command('hset', f'doc{i}', 'f', 'hello world')

  for workers in [0, 1]:
    run_command_on_all_shards(env, config_cmd(), 'SET', 'WORKERS', workers)

    # with_load is only meaningful when workers=1 (causes Threadsafe-Loader usage)
    load_options = [True, False] if workers == 1 else [False]

    for with_load in load_options:
      scenario = f"protocol={protocol}, cluster={is_cluster}, workers={workers}, with_load={with_load}"

      res = env.cmd('FT.PROFILE', 'idx', 'AGGREGATE', 'query', 'hello', *(['LOAD', 1, '@f'] if with_load else []))
      coordinator, shards = extract_profile_coordinator_and_shards(env, res)

      # Validate Coordinator section - should never contain "Total GIL time"
      env.assertNotContains('Total GIL time', coordinator, message=f"{scenario}: Coordinator should not have Total GIL time")

      # Validate each shard
      for shard in shards:
        if workers == 0:
          # workers=0: No "Total GIL Time"
          env.assertNotContains('Total GIL time', shard, message=f"{scenario}: Shard should not have Total GIL time when workers=0")
        else:
          # workers=1: "Total GIL Time" exists and >= Threadsafe-Loader GIL time
          env.assertContains('Total GIL time', shard, message=f"{scenario}: Shard should have Total GIL time when workers=1")
          total_gil_time = float(shard['Total GIL time'])

          if with_load:
            # Verify Threadsafe-Loader is in profile and has GIL time > 0
            threadsafe_loader = find_threadsafe_loader(env, shard)
            env.assertIsNotNone(threadsafe_loader, message=f"{scenario}: Threadsafe-Loader should be in profile when loading")
            loader_gil_time = float(threadsafe_loader['GIL-Time'])
            env.assertGreater(loader_gil_time, 0, message=f"{scenario}: Threadsafe-Loader GIL-Time should be > 0 when loading")
            # Total GIL time should be >= Threadsafe-Loader GIL time
            env.assertGreaterEqual(total_gil_time, loader_gil_time, message=f"{scenario}: Total GIL time should be >= Threadsafe-Loader GIL time")
          else:
            # Without load: Total GIL Time should be greater than 0 since there is processing time on the main thread before moving to the background
            env.assertGreater(total_gil_time, 0, message=f"{scenario}: Total GIL time should be greater than 0 without load")

def testProfileGILTimeResp2():
  ProfileGILTime(Env(protocol=2))

def testProfileGILTimeResp3():
  ProfileGILTime(Env(protocol=3))

def testProfileBM25NormMax(env):
  #create index
  env.cmd('ft.create', 'idx', 'SCHEMA', 't', 'TEXT')

  # Populate db
  with env.getClusterConnectionIfNeeded() as conn:
    conn.execute_command('HSET', 'doc1', 't', 'hello world')
    conn.execute_command('HSET', 'doc2', 't', 'hello space world')
    conn.execute_command('HSET', 'doc3', 't', 'hello more space world')

  aggregate_response = env.cmd('FT.PROFILE', 'idx', 'AGGREGATE', 'query', 'hello', 'ADDSCORES', 'SCORER', 'BM25STD.NORM')
  env.assertTrue(recursive_contains(aggregate_response, "Score Max Normalizer"))
  search_response = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'query', 'hello', 'WITHSCORES', 'SCORER', 'BM25STD.NORM')
  env.assertTrue(recursive_contains(search_response, "Score Max Normalizer"))


def testProfileVectorSearchMode():
  """Test Vector search mode field in FT.PROFILE for both SEARCH and AGGREGATE"""
  env = Env(moduleArgs='DEFAULT_DIALECT 2', protocol=3)  # Use RESP3 for easier dict access
  conn = getConnectionByEnv(env)

  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2', 't', 'TEXT').ok()

  conn.execute_command('hset', '1', 'v', 'bababaca', 't', "hello")
  conn.execute_command('hset', '2', 'v', 'babababa', 't', "hello")
  conn.execute_command('hset', '3', 'v', 'aabbaabb', 't', "hello")
  conn.execute_command('hset', '4', 'v', 'bbaabbaa', 't', "hello world")
  conn.execute_command('hset', '5', 'v', 'aaaabbbb', 't', "hello world")

  # Helper function to test both SEARCH and AGGREGATE
  def verify_search_mode(query_type, query, params, expected_mode, expected_iterator_type='VECTOR'):
    scenario_message = f"query_type: {query_type}, query: {query}, params: {params}, expected_mode: {expected_mode}"
    """
    Verify that Vector search mode appears in profile for both SEARCH and AGGREGATE
    query_type: 'SEARCH' or 'AGGREGATE'
    query: the query string
    params: list of params (e.g., ['vec', 'aaaaaaaa'])
    expected_mode: expected search mode string
    expected_iterator_type: 'VECTOR' or 'METRIC SORTED BY ID - VECTOR DISTANCE'
    """
    cmd = ['FT.PROFILE', 'idx', query_type, 'QUERY', query]
    cmd.extend(['PARAMS'] + [str(len(params))] + params)

    res = env.cmd(*cmd)

    # Navigate to iterator profile (RESP3 dict structure)
    shards = res['Profile']['Shards']
    env.assertGreater(len(shards), 0, message=scenario_message)

    # Check at least one shard has the expected search mode
    # res['Profile']['Shards'][0]['Iterators profile']['Vector search mode']
    found = False
    for shard in shards:
      iter_profile = shard['Iterators profile']
      if iter_profile['Type'] == expected_iterator_type:
        env.assertEqual(iter_profile['Vector search mode'], expected_mode, message=scenario_message)
        found = True
        break
    env.assertTrue(found, message=f"{scenario_message}: Expected iterator type {expected_iterator_type} not found")

  # Test 1: STANDARD_KNN
  verify_search_mode('SEARCH', '*=>[KNN 3 @v $vec]', ['vec', 'aaaaaaaa'], 'STANDARD_KNN')
  verify_search_mode('AGGREGATE', '*=>[KNN 3 @v $vec]', ['vec', 'aaaaaaaa'], 'STANDARD_KNN')

  # Test 2: HYBRID_ADHOC_BF
  verify_search_mode('SEARCH', '(@t:hello world)=>[KNN 3 @v $vec]', ['vec', 'aaaaaaaa'], 'HYBRID_ADHOC_BF')
  verify_search_mode('AGGREGATE', '(@t:hello world)=>[KNN 3 @v $vec]', ['vec', 'aaaaaaaa'], 'HYBRID_ADHOC_BF')

  # Test 3: RANGE_QUERY (uses METRIC_ITERATOR)
  verify_search_mode('SEARCH', '@v:[VECTOR_RANGE 3e36 $vec]=>{$yield_distance_as:dist}',
                     ['vec', 'aaaaaaaa'], 'RANGE_QUERY', 'METRIC SORTED BY ID - VECTOR DISTANCE')
  verify_search_mode('AGGREGATE', '@v:[VECTOR_RANGE 3e36 $vec]=>{$yield_distance_as:dist}',
                     ['vec', 'aaaaaaaa'], 'RANGE_QUERY', 'METRIC SORTED BY ID - VECTOR DISTANCE')

  # Test 4: HYBRID_BATCHES
  verify_search_mode('SEARCH', '(@t:hello world)=>[KNN 3 @v $vec HYBRID_POLICY BATCHES BATCH_SIZE 100]', ['vec', 'aaaaaaaa'], 'HYBRID_BATCHES')
  verify_search_mode('AGGREGATE', '(@t:hello world)=>[KNN 3 @v $vec HYBRID_POLICY BATCHES BATCH_SIZE 100]', ['vec', 'aaaaaaaa'], 'HYBRID_BATCHES')

  # Running HYBRID_BATCHES_TO_ADHOC_BF on cluster requires much more data and doesn't add a significant value
  if env.isCluster():
    return

  for i in range(6, 5000):
    conn.execute_command('hset', str(i), 'v', 'bababada', 't', "hello")

  # Add another 10K docs with "other" tag for HYBRID_BATCHES_TO_ADHOC_BF test
  for i in range(5000, 10001):
    conn.execute_command('hset', str(i), 'v', '????????', 't', "other")

  # Test 5: HYBRID_BATCHES_TO_ADHOC_BF
  # Query: "hello" (10K docs) AND "other" (10K docs) → intersection is 0 (disjoint sets)
  # High estimated results → starts BATCHES, but 0 actual results → switches to ADHOC_BF
  verify_search_mode('SEARCH', '(@t:hello other)=>[KNN 3 @v $vec BATCH_SIZE 100]', ['vec', '????????'], 'HYBRID_BATCHES_TO_ADHOC_BF')
  verify_search_mode('AGGREGATE', '(@t:hello other)=>[KNN 3 @v $vec BATCH_SIZE 100]', ['vec', '????????'], 'HYBRID_BATCHES_TO_ADHOC_BF')


def ShardIdInProfile(env):
  """Tests that 'shard_id' field appears in shard profiles."""

  # Run FT.PROFILE SEARCH
  res = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '*')

  shards_profile = get_shards_profile(env, res)
  env.assertEqual(len(shards_profile), env.shardsCount, message=f"unexpected number of shards. full reply output: {res}")

  # Each shard should have a shard_id field
  for shard_profile in shards_profile:
    env.assertContains('Shard ID', shard_profile, message=f"shard_id not found in profile. full reply output: {res}")
    # Verify shard_id is a non-empty string
    env.assertTrue(isinstance(shard_profile['Shard ID'], (str, bytes)), message=f"shard_id is not a string. full reply output: {res}")
    env.assertTrue(len(shard_profile['Shard ID']) > 0, message=f"shard_id is empty. full reply output: {res}")

  # Run FT.PROFILE AGGREGATE
  res = env.cmd('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*')

  shards_profile = get_shards_profile(env, res)
  env.assertEqual(len(shards_profile), env.shardsCount, message=f"unexpected number of shards. full reply output: {res}")

  # Each shard should have a shard_id field
  for shard_profile in shards_profile:
    env.assertContains('Shard ID', shard_profile, message=f"shard_id not found in profile. full reply output: {res}")
    # Verify shard_id is a non-empty string
    env.assertTrue(isinstance(shard_profile['Shard ID'], (str, bytes)), message=f"shard_id is not a string. full reply output: {res}")
    env.assertTrue(len(shard_profile['Shard ID']) > 0, message=f"shard_id is empty. full reply output: {res}")

@skip(cluster=False)
def testShardIdInProfileResp3():
    env = Env(protocol=3)
    conn = getConnectionByEnv(env)
    run_command_on_all_shards(env, config_cmd(), 'SET', '_PRINT_PROFILE_CLOCK', 'false')

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()

    # Insert some docs
    num_docs = 10
    for i in range(num_docs):
        conn.execute_command('HSET', f'doc{i}', 't', f'hello{i}')

    ShardIdInProfile(env)

@skip(cluster=False)
def testShardIdInProfileResp2():
    env = Env(protocol=2)
    conn = getConnectionByEnv(env)
    run_command_on_all_shards(env, config_cmd(), 'SET', '_PRINT_PROFILE_CLOCK', 'false')

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()

    # Insert some docs
    num_docs = 10
    for i in range(num_docs):
        conn.execute_command('HSET', f'doc{i}', 't', f'hello{i}')

    ShardIdInProfile(env)


# Run testShardIdInProfileResp3 for a few seconds to ensure that we update the node ID in search.clusterset (expected
# every 1 sec) in parallel to running profile (and synchronously)
@skip(cluster=False)
def testConcurrentSetClusterAndProfile():
    env = Env(protocol=3)
    conn = getConnectionByEnv(env)
    run_command_on_all_shards(env, config_cmd(), 'SET', '_PRINT_PROFILE_CLOCK', 'false')

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()

    # Insert some docs
    num_docs = 10
    for i in range(num_docs):
        conn.execute_command('HSET', f'doc{i}', 't', f'hello{i}')
    # Run ShardIdInProfile from parallel 3 threads, where each running the call repeatedly for 3 seconds
    def run_shard_id_in_profile_in_loop():
        now = time.time()
        while time.time() - now < 3:
            ShardIdInProfile(env)

    threads = []
    for _ in range(3):
        thread = threading.Thread(target=run_shard_id_in_profile_in_loop)
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

def CoordDispatchTimeInProfile(env):
  """
  Tests that 'Coordinator dispatch time [ms]' field appears in shard profiles
  for FT.AGGREGATE, FT.SEARCH, and FT.HYBRID.
  For HYBRID queries, verifies dispatch time is present for both SEARCH and
  VSIM subqueries.
  """

  # Helper to verify dispatch time in profile result
  def verify_dispatch_time_in_profile(profile_result, cmd_name):
    """
    Verifies that 'Coordinator dispatch time [ms]' field appears in all shard profiles,
    all shards have the same dispatch time, and the value is >= pause duration.
    """
    shards_profile = get_shards_profile(env, profile_result)
    env.assertEqual(len(shards_profile), env.shardsCount,
                    message=f"{cmd_name}: unexpected number of shards. full reply output: {profile_result}")

    # Collect all dispatch times
    dispatch_times = []
    for i, shard_profile in enumerate(shards_profile):
      env.assertContains('Coordinator dispatch time [ms]', shard_profile,
                         message=f"{cmd_name} shard {i}: 'Coordinator dispatch time' not found. full reply: {profile_result}")
      dispatch_times.append(shard_profile['Coordinator dispatch time [ms]'])

    # All shards should have the exact same dispatch time
    for i, dispatch_time in enumerate(dispatch_times[1:], start=1):
      env.assertEqual(dispatch_time, dispatch_times[0],
        message=f"{cmd_name} shard {i} dispatch time differs from shard 0. all shards: {dispatch_times}")

  # --- Test AGGREGATE profile should have dispatch time ---
  res_agg = env.cmd('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*')
  verify_dispatch_time_in_profile(res_agg, 'AGGREGATE')

  # --- Test SEARCH profile should have dispatch time ---
  res_search = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '*', 'NOCONTENT')
  verify_dispatch_time_in_profile(res_search, 'SEARCH')

  # --- Test HYBRID profile dispatch time ---
  # HYBRID profile should include dispatch time for both SEARCH and VSIM subqueries
  query_vector = np.array([0, 0], dtype=np.float32).tobytes()
  res_hybrid = env.cmd('FT.PROFILE', 'idx', 'HYBRID', 'QUERY',
                       'SEARCH', 'hello0',
                       'VSIM', '@v', '$BLOB',
                       'PARAMS', '2', 'BLOB', query_vector)

  # Extract HYBRID shard profiles
  # HYBRID has different structure: each shard contains
  # ['Shard ID', ANY, 'SEARCH', [...], 'VSIM', [...]]
  if env.protocol == 3:
    hybrid_shards = res_hybrid['Profile']['Shards']
  else:
    # RESP2: res_hybrid[-1] is ['Shards', [...], 'Coordinator', {...}]
    hybrid_shards = res_hybrid[-1][1]

  env.assertEqual(len(hybrid_shards), env.shardsCount,
                  message=f"HYBRID: unexpected number of shards. full reply: {res_hybrid}")

  # For each shard, verify both SEARCH and VSIM subqueries have dispatch time
  for shard_idx, shard_profile in enumerate(hybrid_shards):
    if env.protocol == 2:
      # RESP2: shard_profile is a list like
      # ['Shard ID', value, 'SEARCH', {...}, 'VSIM', {...}]
      env.assertEqual(len(shard_profile), 6,
                      message=f"HYBRID shard {shard_idx}: unexpected structure. full reply: {res_hybrid}")
      env.assertEqual(shard_profile[0], 'Shard ID')
      env.assertEqual(shard_profile[2], 'SEARCH')
      env.assertEqual(shard_profile[4], 'VSIM')

      # Extract SEARCH and VSIM subprofiles (indices 3 and 5)
      search_profile = to_dict(shard_profile[3])
      vsim_profile = to_dict(shard_profile[5])
    else:
      # RESP3: shard_profile is a dict with 'Shard ID', 'SEARCH', 'VSIM' keys
      env.assertContains('Shard ID', shard_profile)
      env.assertContains('SEARCH', shard_profile)
      env.assertContains('VSIM', shard_profile)

      search_profile = shard_profile['SEARCH']
      vsim_profile = shard_profile['VSIM']

    # Verify SEARCH subquery has dispatch time
    env.assertContains('Coordinator dispatch time [ms]', search_profile,
                       message=f"HYBRID shard {shard_idx} SEARCH: 'Coordinator dispatch time [ms]' not found. full reply: {res_hybrid}")

    # Verify VSIM subquery has dispatch time
    env.assertContains('Coordinator dispatch time [ms]', vsim_profile,
                       message=f"HYBRID shard {shard_idx} VSIM: 'Coordinator dispatch time [ms]' not found. full reply: {res_hybrid}")

  # Verify all shards have consistent dispatch times for SEARCH
  search_dispatch_times = []
  vsim_dispatch_times = []
  for shard_idx, shard_profile in enumerate(hybrid_shards):
    if env.protocol == 2:
      search_profile = to_dict(shard_profile[3])
      vsim_profile = to_dict(shard_profile[5])
    else:
      search_profile = shard_profile['SEARCH']
      vsim_profile = shard_profile['VSIM']

    search_dispatch_times.append(search_profile['Coordinator dispatch time [ms]'])
    vsim_dispatch_times.append(vsim_profile['Coordinator dispatch time [ms]'])

  # All shards should have the same SEARCH dispatch time
  for i, dispatch_time in enumerate(search_dispatch_times[1:], start=1):
    env.assertEqual(dispatch_time, search_dispatch_times[0],
      message=f"HYBRID SEARCH shard {i} dispatch time differs from shard 0. all shards: {search_dispatch_times}")

  # All shards should have the same VSIM dispatch time
  for i, dispatch_time in enumerate(vsim_dispatch_times[1:], start=1):
    env.assertEqual(dispatch_time, vsim_dispatch_times[0],
      message=f"HYBRID VSIM shard {i} dispatch time differs from shard 0. all shards: {vsim_dispatch_times}")


@skip(cluster=False)
def testCoordDispatchTimeInProfileResp3():
  """Tests coordinator dispatch time in profile output - RESP3."""
  env = Env(protocol=3)
  conn = getConnectionByEnv(env)
  run_command_on_all_shards(env, config_cmd(), 'SET', '_PRINT_PROFILE_CLOCK', 'true')

  dim = 2
  env.expect('FT.CREATE', 'idx', 'SCHEMA',
             't', 'TEXT',
             'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', str(dim), 'DISTANCE_METRIC', 'L2').ok()

  # Add some documents
  num_docs = 10 * env.shardsCount
  for i in range(num_docs):
    vec = np.array([float(i), float(i)], dtype=np.float32).tobytes()
    conn.execute_command('HSET', f'doc:{i}', 't', f'hello{i}', 'v', vec)

  CoordDispatchTimeInProfile(env)

@skip(cluster=False)
def testCoordDispatchTimeInProfileResp2():
  """Tests coordinator dispatch time in profile output - RESP2."""
  env = Env(protocol=2)
  conn = getConnectionByEnv(env)
  run_command_on_all_shards(env, config_cmd(), 'SET', '_PRINT_PROFILE_CLOCK', 'true')

  dim = 2
  env.expect('FT.CREATE', 'idx', 'SCHEMA',
             't', 'TEXT',
             'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', str(dim), 'DISTANCE_METRIC', 'L2').ok()

  # Add some documents
  num_docs = 10 * env.shardsCount
  for i in range(num_docs):
    vec = np.array([float(i), float(i)], dtype=np.float32).tobytes()
    conn.execute_command('HSET', f'doc:{i}', 't', f'hello{i}', 'v', vec)

  CoordDispatchTimeInProfile(env)

# =============================================================================
# Queue Time Tests - Validation tests for queue time tracking in FT.PROFILE
# =============================================================================

def run_profile_with_paused_pool(env, pause_cmd, resume_cmd, pause_duration_ms=100):
  """
  Helper to run FT.PROFILE while a thread pool is paused.
  Returns the profile result after resuming the pool.

  Args:
    env: Test environment
    pause_cmd: Command to pause the pool (e.g., ['_FT.DEBUG', 'WORKERS', 'PAUSE'])
    resume_cmd: Command to resume the pool (e.g., ['_FT.DEBUG', 'WORKERS', 'RESUME'])
    pause_duration_ms: How long to keep the pool paused (in milliseconds)

  Returns:
    The FT.PROFILE result
  """
  result = [None]
  error = [None]

  def run_profile():
    try:
      result[0] = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '*', 'NOCONTENT', 'LIMIT', '0', '1')
    except Exception as e:
      error[0] = e

  # Pause the pool
  env.cmd(*pause_cmd)

  # Start the profile command in a background thread
  profile_thread = threading.Thread(target=run_profile)
  profile_thread.start()

  # Wait for the pause duration
  time.sleep(pause_duration_ms / 1000.0)

  # Resume the pool
  env.cmd(*resume_cmd)

  # Wait for the profile command to complete
  profile_thread.join(timeout=10)

  if error[0]:
    raise error[0]

  return result[0]

def get_shard_parsing_time(env, profile_result):
  """Extract Parsing time from shard profile."""
  if env.protocol == 3:
    # RESP3: profile is under 'Profile' -> 'Shards' (list) for both cluster and standalone
    shards = profile_result['Profile']['Shards']
    return float(shards[0]['Parsing time'])
  else:
    # RESP2
    if env.isCluster():
      _, shards = extract_profile_coordinator_and_shards(env, profile_result)
      return float(shards[0]['Parsing time'])
    else:
      profile_dict = to_dict(profile_result[-1])
      return float(profile_dict['Parsing time'])

@skip(cluster=False)
def testParsingTimeDoesNotIncludeCoordQueueTime():
  """Confirms coordinator queue time is NOT included in shard's Parsing time."""
  env = Env(protocol=3, shardsCount=2, moduleArgs='WORKERS 1')
  conn = getConnectionByEnv(env)
  # Enable verbose profile output to get Parsing time
  run_command_on_all_shards(env, config_cmd(), 'SET', '_PRINT_PROFILE_CLOCK', 'true')

  env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
  conn.execute_command('HSET', 'doc1', 't', 'hello')

  pause_duration_ms = 100

  result = run_profile_with_paused_pool(
    env,
    pause_cmd=[debug_cmd(), 'COORD_THREADS', 'PAUSE'],
    resume_cmd=[debug_cmd(), 'COORD_THREADS', 'RESUME'],
    pause_duration_ms=pause_duration_ms
  )

  parsing_time = get_shard_parsing_time(env, result)

  # Coordinator queue time should NOT be in shard's Parsing time
  # Parsing time should be much less than the pause duration
  # Note: This assertion can be removed if this test becomes flaky
  env.assertLess(parsing_time, pause_duration_ms * 0.5,
    message=f"Parsing time ({parsing_time}ms) should NOT include coordinator queue wait. "
            f"Expected < {pause_duration_ms * 0.5}ms. Full result: {result}")

def get_shard_workers_queue_time(profile_result):
  """Extract 'Workers queue time' from the first shard's profile result (RESP3 only)."""
  profile = profile_result.get('Profile', profile_result.get('profile', {}))
  shards = profile.get('Shards', profile.get('shards', []))
  if isinstance(shards, list) and len(shards) > 0:
    return shards[0].get('Workers queue time', 0)
  raise ValueError(f"Could not find Workers queue time in profile result: {profile_result}")

def get_coordinator_queue_time(profile_result):
  """
  Extract 'Coordinator queue time' from the coordinator's profile result.
  Only applicable in cluster mode.
  """
  profile = profile_result.get('Profile', profile_result.get('profile', {}))

  # Get coordinator profile
  coordinator = profile.get('Coordinator', profile.get('coordinator', {}))
  if isinstance(coordinator, dict):
    return coordinator.get('Coordinator queue time', 0)

  raise ValueError(f"Could not find Coordinator queue time in profile result: {profile_result}")

@skip(cluster=True)
def testWorkersQueueTimeInProfile():
  """Verifies Workers queue time is captured and separated from Parsing time."""
  env = Env(protocol=3, moduleArgs='WORKERS 1')
  conn = getConnectionByEnv(env)
  # Enable verbose profile output to get timing details
  run_command_on_all_shards(env, config_cmd(), 'SET', '_PRINT_PROFILE_CLOCK', 'true')

  env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
  conn.execute_command('HSET', 'doc1', 't', 'hello')

  pause_duration_ms = 100

  result = run_profile_with_paused_pool(
    env,
    pause_cmd=[debug_cmd(), 'WORKERS', 'PAUSE'],
    resume_cmd=[debug_cmd(), 'WORKERS', 'RESUME'],
    pause_duration_ms=pause_duration_ms
  )

  parsing_time = get_shard_parsing_time(env, result)
  workers_queue_time = get_shard_workers_queue_time(result)

  # Workers queue time should capture the queue wait time
  env.assertGreaterEqual(workers_queue_time, pause_duration_ms * 0.8,  # Allow 20% tolerance
    message=f"Workers queue time ({workers_queue_time}ms) should capture queue wait. "
            f"Expected >= {pause_duration_ms * 0.8}ms. Full result: {result}")

  # Parsing time should NOT include queue wait time anymore
  # Note: This assertion can be removed if this test becomes flaky
  env.assertLess(parsing_time, pause_duration_ms * 0.5,
    message=f"Parsing time ({parsing_time}ms) should NOT include queue wait time. "
            f"Expected < {pause_duration_ms * 0.5}ms. Full result: {result}")

@skip(cluster=False)
def testCoordinatorQueueTimeInProfile():
  """Verifies Coordinator queue time is correctly captured in cluster mode."""
  env = Env(protocol=3, shardsCount=2)
  conn = getConnectionByEnv(env)
  # Enable verbose profile output to get timing details
  run_command_on_all_shards(env, config_cmd(), 'SET', '_PRINT_PROFILE_CLOCK', 'true')

  env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
  conn.execute_command('HSET', 'doc1', 't', 'hello')

  pause_duration_ms = 100

  result = run_profile_with_paused_pool(
    env,
    pause_cmd=[debug_cmd(), 'COORD_THREADS', 'PAUSE'],
    resume_cmd=[debug_cmd(), 'COORD_THREADS', 'RESUME'],
    pause_duration_ms=pause_duration_ms
  )

  coord_queue_time = get_coordinator_queue_time(result)

  # Coordinator queue time should capture the queue wait time
  env.assertGreaterEqual(coord_queue_time, pause_duration_ms * 0.8,  # Allow 20% tolerance
    message=f"Coordinator queue time ({coord_queue_time}ms) should capture queue wait. "
            f"Expected >= {pause_duration_ms * 0.8}ms. Full result: {result}")

