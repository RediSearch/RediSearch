from RLTest import Env
from common import *
import json

def sortResultByKeyName(res):
  '''
    Sorts the result by name
    res = [<COUNT>, '<NAME_1>, '<VALUE_1>', '<NAME_2>, '<VALUE_2>', ...] 
  '''
  # Sort name and value pairs by name
  pairs = [(name,value) for name,value in zip(res[1::2], res[2::2])]
  pairs = [i for i in sorted(pairs, key=lambda x: x[0])]
  # Flatten the sorted pairs to a list
  pairs = [i for pair in pairs for i in pair]
  res = [res[0], *pairs]
  return res

def array_of_key_value_to_map(res):
  '''
    Insert the result of an array of keys and values to a map
  '''
  return {res[i]: res[i+1] for i in range(0, len(res), 2)}


def assert_index_num_docs(env, idx, attr, num_docs):
  if not env.isCluster():
    res = env.cmd('FT.DEBUG', 'DUMP_GEOMIDX', idx, attr)
    res = env.execute_command('FT.DEBUG', 'DUMP_GEOMIDX', idx, attr)
    res = array_of_key_value_to_map(res)
    env.assertEqual(res['num_docs'], num_docs)

def testSanitySearchHashWithin(env):
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'geom', 'GEOMETRY').ok()
  
  conn.execute_command('HSET', 'small', 'geom', 'POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))')
  conn.execute_command('HSET', 'large', 'geom', 'POLYGON((1 1, 1 200, 200 200, 200 1, 1 1))')
  expected = ['geom', 'POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))']
  env.expect('FT.SEARCH', 'idx', '@geom:[within POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))]', 'DIALECT', 3).equal([1, 'small', expected])
  res = env.execute_command('FT.SEARCH', 'idx', '@geom:[contains POLYGON((2 2, 2 50, 50 50, 50 2, 2 2))]', 'DIALECT', 3)
  env.assertEqual(res[0], 2)
  
  # TODO: GEOMETRY - Use params
  #env.expect('FT.SEARCH', 'idx', '@geom:[within $POLY]', 'PARAMS', '2', 'POLY', 'POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))').equal([1, 'small', expected])

  res = env.execute_command('FT.SEARCH', 'idx', '@geom:[within POLYGON((0 0, 0 250, 250 250, 250 0, 0 0))]', 'NOCONTENT', 'DIALECT', 3)
  env.assertEqual(toSortedFlatList(res), [2, 'large', 'small'])


def testSanitySearchJsonWithin(env):
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE idx ON JSON SCHEMA $.geom AS geom GEOMETRY').ok()

  conn.execute_command('JSON.SET', 'small', '$', '{"geom": "POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))"}')
  conn.execute_command('JSON.SET', 'large', '$', '{"geom": "POLYGON((1 1, 1 200, 200 200, 200 1, 1 1))"}')
  expected = ['$', '[{"geom":"POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))"}]']
  env.expect('FT.SEARCH', 'idx', '@geom:[within POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))]', 'DIALECT', 3).equal([1, 'small', expected])
  
  # TODO: GEOMETRY - Use params
  #env.expect('FT.SEARCH', 'idx', '@geom:[within $POLY]', 'PARAMS', '2', 'POLY', 'POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))').equal([1, 'small', expected])

  env.expect('FT.SEARCH', 'idx', '@geom:[within POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))]', 'RETURN', 1, 'geom', 'DIALECT', 3).equal([1, 'small', ['geom', json.dumps([json.loads(expected[1])[0]['geom']])]])
  res = env.execute_command('FT.SEARCH', 'idx', '@geom:[within POLYGON((0 0, 0 250, 250 250, 250 0, 0 0))]', 'NOCONTENT', 'DIALECT', 3)
  env.assertEqual(toSortedFlatList(res), [2, 'large', 'small'])


def testSanitySearchJsonCombined(env):
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE idx ON JSON SCHEMA $.geom AS geom GEOMETRY $.name as name TEXT').ok()

  conn.execute_command('JSON.SET', 'p1', '$', '{"geom": "POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))", "name": "Homer"}')
  conn.execute_command('JSON.SET', 'p2', '$', '{"geom": "POLYGON((1 1, 1 100, 90 90, 100 1, 1 1))", "name": "Bart"}')
  env.expect('FT.SEARCH', 'idx', '@name:(Ho*) @geom:[within POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))]', 'NOCONTENT', 'DIALECT', 2).equal([1, 'p1'])
  

# TODO: GEOMETRY - Enable with sanitizer (MOD-5182)
@skip(asan=True)
def testWKTIngestError(env):
  ''' Test ingest error '''

  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE idx ON JSON SCHEMA $.geom AS geom GEOMETRY $.name as name TEXT').ok()

  # Wrong keyword
  conn.execute_command('JSON.SET', 'p1', '$', '{"geom": "POLIKON((1 1, 1 100, 100 100, 100 1, 1 1))", "name": "Homer"}')
  # Missing parenthesis
  conn.execute_command('JSON.SET', 'p2', '$', '{"geom": "POLYGON(1 1, 1 100, 100 100, 100 1, 1 1))", "name": "Patty"}')

  
  # TODO: GEOMETRY - understand why the following WKTs do not fail?
  # Missing Y coordinate
  conn.execute_command('JSON.SET', 'p3', '$', '{"geom": "POLYGON((1 1, 1 , 100 100, 100 1, 1 1))", "name": "Moe"}')
  # Redundant coordinate
  conn.execute_command('JSON.SET', 'p4', '$', '{"geom": "POLYGON((1 1, 1 100 100 100, 100 1, 1 1))", "name": "Seymour"}')
  # Missing coma separator
  conn.execute_command('JSON.SET', 'p5', '$', '{"geom": "POLYGON((1 1 1 100, 100 100, 100 1, 1 1))", "name": "Ned"}')
  # Too few coordinates (not a polygon)
  conn.execute_command('JSON.SET', 'p6', '$', '{"geom": "POLYGON((1 1, 1 100, 1 1))", "name": "Milhouse"}')
  # Zero coordinates
  conn.execute_command('JSON.SET', 'p7', '$', '{"geom": "POLYGON(()())", "name": "Mr. Burns"}')

  # Indexing failures
  res = env.cmd('FT.INFO', 'idx')
  d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
  env.assertEqual(int(d['hash_indexing_failures']), 2)


# TODO: GEOMETRY - Enable with sanitizer (MOD-5182)
@skip(asan=True)
def testWKTQueryError(env):
  ''' Test query error '''
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE idx ON JSON SCHEMA $.geom AS geom GEOMETRY $.name as name TEXT').ok()

  env.expect('FT.SEARCH', 'idx', '@name:(Ho*) @geom:[within POLIGON((0 0, 0 150, 150 150, 150 0, 0 0))]', 'NOCONTENT', 'DIALECT', 3).error().contains('POLIGON')

def testSimpleUpdate(env):
  ''' Test updating geometries '''
  
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'geom', 'GEOMETRY', 'geom2', 'GEOMETRY').ok()
  conn.execute_command('HSET', 'k1', 'geom', 'POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))')
  conn.execute_command('HSET', 'k2', 'geom', 'POLYGON((1 1, 1 200, 200 200, 200 1, 1 1))')
  conn.execute_command('HSET', 'k3', 'geom', 'POLYGON((1 1, 1 200, 200 200, 200 1, 1 1))', 'geom2', 'POLYGON((1 1, 1 140, 140 140, 140 1, 1 1))')
  
  expected1 = ['geom', 'POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))']
  expected2 = ['geom', 'POLYGON((1 1, 1 120, 120 120, 120 1, 1 1))']
  expected3 = ['geom', 'POLYGON((1 1, 1 200, 200 200, 200 1, 1 1))', 'geom2', 'POLYGON((1 1, 1 140, 140 140, 140 1, 1 1))']
  
  # Dump geometry index
  assert_index_num_docs(env, 'idx', 'geom', 3)
  assert_index_num_docs(env, 'idx', 'geom2', 1)
  # Search
  env.expect('FT.SEARCH', 'idx', '@geom:[within POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))]', 'DIALECT', 3).equal([1, 'k1', expected1])
  
  # Update
  conn.execute_command('HSET', 'k2', 'geom', 'POLYGON((1 1, 1 120, 120 120, 120 1, 1 1))')
  # Dump geometry index
  assert_index_num_docs(env, 'idx', 'geom', 3)
  assert_index_num_docs(env, 'idx', 'geom2', 1)
  # Search after update
  res = env.execute_command('FT.SEARCH', 'idx', '@geom:[within POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))]', 'DIALECT', 3)
  env.assertEqual(sortResultByKeyName(res), sortResultByKeyName([2, 'k1', expected1, 'k2', expected2]))

  if not SANITIZER:
    # TODO: GEOMETRY - Enable with sanitizer (MOD-5182)
    # Set illegal data to field geom (indexing fails, field should be removed from index)
    conn.execute_command('HSET', 'k2', 'geom', '')
    # Dump geometry index
    assert_index_num_docs(env, 'idx', 'geom', 2)
    assert_index_num_docs(env, 'idx', 'geom2', 1)
    # Search
    env.expect('FT.SEARCH', 'idx', '@geom:[within POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))]', 'DIALECT', 3).equal([1, 'k1', expected1])

  # Delete key
  conn.execute_command('DEL', 'k2')
  # Dump geometry index
  assert_index_num_docs(env, 'idx', 'geom', 2)
  assert_index_num_docs(env, 'idx', 'geom2', 1)
  # Search within after delete
  env.expect('FT.SEARCH', 'idx', '@geom:[within POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))]', 'DIALECT', 3).equal([1, 'k1', expected1])

  # Delete key
  conn.execute_command('DEL', 'k1')
  # Dump geometry index
  assert_index_num_docs(env, 'idx', 'geom', 1)
  # Search within
  env.expect('FT.SEARCH', 'idx', '@geom:[within POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))]', 'DIALECT', 3).equal([0])
  # Search contains
  env.expect('FT.SEARCH', 'idx', '@geom:[contains POLYGON((2 2, 2 150, 150 150, 150 2, 2 2))]', 'DIALECT', 3).equal([1, 'k3', expected3])

  # Delete field
  conn.execute_command('HDEL', 'k3', 'geom2')
  expected3 = ['geom', 'POLYGON((1 1, 1 200, 200 200, 200 1, 1 1))']
  # Search contains
  env.expect('FT.SEARCH', 'idx', '@geom:[contains POLYGON((2 2, 2 150, 150 150, 150 2, 2 2))]', 'DIALECT', 3).equal([1, 'k3', expected3])

  # Delete key
  conn.execute_command('DEL', 'k3')
  # Dump geometry index
  assert_index_num_docs(env, 'idx', 'geom', 0)
  # Search contains
  env.expect('FT.SEARCH', 'idx', '@geom:[contains POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))]', 'DIALECT', 3).equal([0])


def testFieldUpdate(env):
  ''' Test updating a field, keeping the rest intact '''
  
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'geom1', 'GEOMETRY', 'geom2', 'GEOMETRY').ok()
  field1 = ['geom1',  'POLYGON((1 1, 1 200, 200 200, 200 1, 1 1))']
  field2 = ['geom2', 'POLYGON((1 1, 1 140, 140 140, 140 1, 1 1))']
  conn.execute_command('HSET', 'k1', *field1, *field2)

  # Dump geometry index
  assert_index_num_docs(env, 'idx', 'geom1', 1)
  assert_index_num_docs(env, 'idx', 'geom2', 1)

  # Search contains on geom field
  env.expect('FT.SEARCH', 'idx', '@geom1:[contains POLYGON((1 1, 1 150, 150 150, 150 1, 1 1))]', 'DIALECT', 3).equal([1, 'k1', [*field1, *field2]])
  # Search within on geom2 field
  env.expect('FT.SEARCH', 'idx', '@geom2:[within POLYGON((1 1, 1 170, 170 170, 170 1, 1 1))]', 'DIALECT', 3).equal([1, 'k1', [*field1, *field2]])

  # Update - make geom2 smaller
  field2 = ['geom2', 'POLYGON((1 1, 1 120, 120 120, 120 1, 1 1))']
  conn.execute_command('HSET', 'k1', *field2)

  # Search contains on geom field
  env.expect('FT.SEARCH', 'idx', '@geom1:[contains POLYGON((1 1, 1 150, 150 150, 150 1, 1 1))]', 'DIALECT', 3).equal([1, 'k1', [*field1, *field2]])
  # Search within on geom2 field
  env.expect('FT.SEARCH', 'idx', '@geom2:[within POLYGON((1 1, 1 170, 170 170, 170 1, 1 1))]', 'DIALECT', 3).equal([1, 'k1', [*field1, *field2]])

  # Update - make geom2 larger
  field2 = ['geom2', 'POLYGON((1 1, 1 180, 180 180, 180 1, 1 1))']
  conn.execute_command('HSET', 'k1', *field2)
  # Search contains on geom field
  env.expect('FT.SEARCH', 'idx', '@geom1:[contains POLYGON((1 1, 1 150, 150 150, 150 1, 1 1))]', 'DIALECT', 3).equal([1, 'k1', [*field1, *field2]])
  # Search within on geom2 field
  env.expect('FT.SEARCH', 'idx', '@geom2:[within POLYGON((1 1, 1 170, 170 170, 170 1, 1 1))]', 'DIALECT', 3).equal([0])

  # Update - make geom1 smaller
  field1 = ['geom1', 'POLYGON((1 1, 1 149, 149 149, 149 1, 1 1))']
  conn.execute_command('HSET', 'k1', *field1)
  # Search contains on geom field
  env.expect('FT.SEARCH', 'idx', '@geom1:[contains POLYGON((1 1, 1 150, 150 150, 150 1, 1 1))]', 'DIALECT', 3).equal([0])
  # Search within on geom2 field
  env.expect('FT.SEARCH', 'idx', '@geom2:[within POLYGON((1 1, 1 170, 170 170, 170 1, 1 1))]', 'DIALECT', 3).equal([0])


  

    
