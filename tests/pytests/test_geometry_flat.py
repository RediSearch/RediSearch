from RLTest import Env
from common import *
import json

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

@skip()  # TODO: GEOMETRY - Enable when can select FLAT index (MOD-5304)
def testSanitySearchHashWithin(env):
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'geom', 'GEOSHAPE').ok()
  
  conn.execute_command('HSET', 'small', 'geom', 'POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))')
  conn.execute_command('HSET', 'large', 'geom', 'POLYGON((1 1, 1 200, 200 200, 200 1, 1 1))')
  expected = ['geom', 'POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))']
  env.expect('FT.SEARCH', 'idx', '@geom:[within $poly]', 'PARAMS', 2, 'poly', 'POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))', 'DIALECT', 3).equal([1, 'small', expected])
  res = env.execute_command('FT.SEARCH', 'idx', '@geom:[contains $poly]', 'PARAMS', 2, 'poly', 'POLYGON((2 2, 2 50, 50 50, 50 2, 2 2))', 'DIALECT', 3)
  env.assertEqual(res[0], 2)
  
  res = env.execute_command('FT.SEARCH', 'idx', '@geom:[within $poly]', 'PARAMS', 2, 'poly', 'POLYGON((0 0, 0 250, 250 250, 250 0, 0 0))', 'NOCONTENT', 'DIALECT', 3)
  env.assertEqual(toSortedFlatList(res), [2, 'large', 'small'])

@skip()  # TODO: GEOMETRY - Enable when can select FLAT index (MOD-5304)
def testSanitySearchJsonWithin(env):
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE idx ON JSON SCHEMA $.geom AS geom GEOSHAPE').ok()

  conn.execute_command('JSON.SET', 'small', '$', '{"geom": "POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))"}')
  conn.execute_command('JSON.SET', 'large', '$', '{"geom": "POLYGON((1 1, 1 200, 200 200, 200 1, 1 1))"}')
  expected = ['$', '[{"geom":"POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))"}]']
  env.expect('FT.SEARCH', 'idx', '@geom:[within $poly]', 'PARAMS', 2, 'poly', 'POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))', 'DIALECT', 3).equal([1, 'small', expected])
  
  env.expect('FT.SEARCH', 'idx', '@geom:[within $poly]', 'PARAMS', 2, 'poly', 'POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))', 'RETURN', 1, 'geom', 'DIALECT', 3).equal([1, 'small', ['geom', json.dumps([json.loads(expected[1])[0]['geom']])]])
  res = env.execute_command('FT.SEARCH', 'idx', '@geom:[within $poly]', 'PARAMS', 2, 'poly', 'POLYGON((0 0, 0 250, 250 250, 250 0, 0 0))', 'NOCONTENT', 'DIALECT', 3)
  env.assertEqual(toSortedFlatList(res), [2, 'large', 'small'])

@skip()  # TODO: GEOMETRY - Enable when can select FLAT index (MOD-5304)
def testSanitySearchJsonCombined(env):
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE idx ON JSON SCHEMA $.geom AS geom GEOSHAPE $.name as name TEXT').ok()

  conn.execute_command('JSON.SET', 'p1', '$', '{"geom": "POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))", "name": "Homer"}')
  conn.execute_command('JSON.SET', 'p2', '$', '{"geom": "POLYGON((1 1, 1 100, 90 90, 100 1, 1 1))", "name": "Bart"}')
  env.expect('FT.SEARCH', 'idx', '@name:(Ho*) @geom:[within $poly]', 'PARAMS', 2, 'poly', 'POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))', 'NOCONTENT', 'DIALECT', 2).equal([1, 'p1'])
  

# TODO: GEOMETRY - Enable with sanitizer (MOD-5182)
@skip(asan=True)
def testWKTIngestError(env):
  ''' Test ingest error '''

  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE idx ON JSON SCHEMA $.geom AS geom GEOSHAPE $.name as name TEXT').ok()

  # Wrong keyword
  conn.execute_command('JSON.SET', 'p1', '$', '{"geom": "POLIKON((1 1, 1 100, 100 100, 100 1, 1 1))", "name": "Homer"}')
  # Missing parenthesis
  conn.execute_command('JSON.SET', 'p2', '$', '{"geom": "POLYGON(1 1, 1 100, 100 100, 100 1, 1 1))", "name": "Patty"}')
  # Zero coordinates
  conn.execute_command('JSON.SET', 'p7', '$', '{"geom": "POLYGON(()())", "name": "Mr. Burns"}')

  
  # TODO: GEOMETRY - understand why the following WKTs do not fail?
  # Missing Y coordinate
  conn.execute_command('JSON.SET', 'p3', '$', '{"geom": "POLYGON((1 1, 1 , 100 100, 100 1, 1 1))", "name": "Moe"}')
  # Redundant coordinate
  conn.execute_command('JSON.SET', 'p4', '$', '{"geom": "POLYGON((1 1, 1 100 100 100, 100 1, 1 1))", "name": "Seymour"}')
  # Missing coma separator
  conn.execute_command('JSON.SET', 'p5', '$', '{"geom": "POLYGON((1 1 1 100, 100 100, 100 1, 1 1))", "name": "Ned"}')
  # Too few coordinates (not a polygon)
  conn.execute_command('JSON.SET', 'p6', '$', '{"geom": "POLYGON((1 1, 1 100, 1 1))", "name": "Milhouse"}')

  # Indexing failures
  res = env.cmd('FT.INFO', 'idx')
  d = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
  env.assertEqual(int(d['hash_indexing_failures']), 3)


# TODO: GEOMETRY - Enable with sanitizer (MOD-5182)
@skip(asan=True)
def testWKTQueryError(env):
  ''' Test query error '''
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE idx ON JSON SCHEMA $.geom AS geom GEOSHAPE $.name as name TEXT').ok()

  # Bad predicate
  env.expect('FT.SEARCH', 'idx', '@name:(Ho*) @geom:[wizin $poly]', 'PARAMS', 2, 'poly', 'POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))]', 'NOCONTENT', 'DIALECT', 3).error().contains('Syntax error')
  env.expect('FT.SEARCH', 'idx', '@name:(Ho*) @geom:[containss $poly]', 'PARAMS', 2, 'poly', 'POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))]', 'NOCONTENT', 'DIALECT', 3).error().contains('Syntax error')
  # Bad param name
  env.expect('FT.SEARCH', 'idx', '@name:(Ho*) @geom:[within POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))]', 'NOCONTENT', 'DIALECT', 3).error().contains('Syntax error')
  env.expect('FT.SEARCH', 'idx', '@name:(Ho*) @geom:[within $]', 'NOCONTENT', 'DIALECT', 3).error().contains('Syntax error')
  env.expect('FT.SEARCH', 'idx', '@name:(Ho*) @geom:[within poly]', 'NOCONTENT', 'DIALECT', 3).error().contains('Syntax error')
  # Bad Polygon
  env.expect('FT.SEARCH', 'idx', '@name:(Ho*) @geom:[within $poly]', 'PARAMS', 2, 'poly', 'POLIGON((0 0, 0 150, 150 150, 150 0, 0 0))', 'NOCONTENT', 'DIALECT', 3).error().contains('POLIGON')
  env.expect('FT.SEARCH', 'idx', '@name:(Ho*) @geom:[contains $poly]', 'PARAMS', 2, 'poly', '', 'NOCONTENT', 'DIALECT', 3).error()
  env.expect('FT.SEARCH', 'idx', '@name:(Ho*) @geom:[contains $poly]', 'PARAMS', 2, 'poly', '0 0', 'NOCONTENT', 'DIALECT', 3).error()
  env.expect('FT.SEARCH', 'idx', '@name:(Ho*) @geom:[contains $poly]', 'PARAMS', 2, 'poly', 'POLYGON', 'NOCONTENT', 'DIALECT', 3).error().contains('Expected')
  env.expect('FT.SEARCH', 'idx', '@name:(Ho*) @geom:[within $poly]', 'PARAMS', 2, 'poly', 'POLYGON(0 0, 0 150, 150 150, 150 0, 0 0))', 'NOCONTENT', 'DIALECT', 3).error()
  env.expect('FT.SEARCH', 'idx', '@name:(Ho*) @geom:[within $poly]', 'PARAMS', 2, 'poly', 'POLIGON((0 0, a 150, 150 150, 150 0, 0 0))', 'NOCONTENT', 'DIALECT', 3).error()
  # Bad/missing param
  env.expect('FT.SEARCH', 'idx', '@name:(Ho*) @geom:[contains $poly]', 'PARAMS', 2, 'moly', 'POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))]', 'NOCONTENT', 'DIALECT', 3).error().contains('No such parameter')
  env.expect('FT.SEARCH', 'idx', '@name:(Ho*) @geom:[within $poly]', 'NOCONTENT', 'DIALECT', 3).error().contains('No such parameter')
  
@skip()  # TODO: GEOMETRY - Enable when can select FLAT index (MOD-5304)
def testSimpleUpdate(env):
  ''' Test updating geometries '''
  
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'geom', 'GEOSHAPE', 'geom2', 'GEOSHAPE').ok()
  conn.execute_command('HSET', 'k1', 'geom', 'POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))')
  conn.execute_command('HSET', 'k2', 'geom', 'POLYGON((1 1, 1 200, 200 200, 200 1, 1 1))')
  conn.execute_command('HSET', 'k3', 'geom', 'POLYGON((1 1, 1 200, 200 200, 200 1, 1 1))', 'geom2', 'POLYGON((1 1, 1 140, 140 140, 140 1, 1 1))')
  
  expected1 = ['geom', 'POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))']
  expected2 = ['geom', 'POLYGON((1 1, 1 120, 120 120, 120 1, 1 1))']
  expected3 = ['geom', 'POLYGON((1 1, 1 200, 200 200, 200 1, 1 1))', 'geom2', 'POLYGON((1 1, 1 140, 140 140, 140 1, 1 1))']
  
  # Dump < index
  assert_index_num_docs(env, 'idx', 'geom', 3)
  assert_index_num_docs(env, 'idx', 'geom2', 1)
  # Search
  env.expect('FT.SEARCH', 'idx', '@geom:[within $poly]', 'PARAMS', 2, 'poly', 'POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))', 'DIALECT', 3).equal([1, 'k1', expected1])
  
  # Update
  conn.execute_command('HSET', 'k2', 'geom', 'POLYGON((1 1, 1 120, 120 120, 120 1, 1 1))')
  # Dump geoshape index
  assert_index_num_docs(env, 'idx', 'geom', 3)
  assert_index_num_docs(env, 'idx', 'geom2', 1)
  # Search after update
  res = env.execute_command('FT.SEARCH', 'idx', '@geom:[within $poly]', 'PARAMS', 2, 'poly', 'POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))', 'DIALECT', 3)
  env.assertEqual(sortResultByKeyName(res), sortResultByKeyName([2, 'k1', expected1, 'k2', expected2]))

  if not SANITIZER:
    # TODO: GEOMETRY - Enable with sanitizer (MOD-5182)
    # Set illegal data to field geom (indexing fails, field should be removed from index)
    conn.execute_command('HSET', 'k2', 'geom', '')
    # Dump geoshape index
    assert_index_num_docs(env, 'idx', 'geom', 2)
    assert_index_num_docs(env, 'idx', 'geom2', 1)
    # Search
    env.expect('FT.SEARCH', 'idx', '@geom:[within $poly]', 'PARAMS', 2, 'poly', 'POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))', 'DIALECT', 3).equal([1, 'k1', expected1])

  # Delete key
  conn.execute_command('DEL', 'k2')
  # Dump geoshape index
  assert_index_num_docs(env, 'idx', 'geom', 2)
  assert_index_num_docs(env, 'idx', 'geom2', 1)
  # Search within after delete
  env.expect('FT.SEARCH', 'idx', '@geom:[within $poly]', 'PARAMS', 2, 'poly', 'POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))', 'DIALECT', 3).equal([1, 'k1', expected1])

  # Delete key
  conn.execute_command('DEL', 'k1')
  # Dump geoshape index
  assert_index_num_docs(env, 'idx', 'geom', 1)
  # Search within
  env.expect('FT.SEARCH', 'idx', '@geom:[within $poly]', 'PARAMS', 2, 'poly', 'POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))', 'DIALECT', 3).equal([0])
  # Search contains
  res = env.execute_command('FT.SEARCH', 'idx', '@geom:[contains $poly]', 'PARAMS', 2, 'poly', 'POLYGON((2 2, 2 150, 150 150, 150 2, 2 2))', 'DIALECT', 3)
  env.assertEqual(sortResultByKeyName(res), sortResultByKeyName([1, 'k3', expected3]))

  # Delete field
  conn.execute_command('HDEL', 'k3', 'geom2')
  expected3 = ['geom', 'POLYGON((1 1, 1 200, 200 200, 200 1, 1 1))']
  # Search contains
  env.expect('FT.SEARCH', 'idx', '@geom:[contains $poly]', 'PARAMS', 2, 'poly', 'POLYGON((2 2, 2 150, 150 150, 150 2, 2 2))', 'DIALECT', 3).equal([1, 'k3', expected3])

  # Delete key
  conn.execute_command('DEL', 'k3')
  # Dump geoshape index
  assert_index_num_docs(env, 'idx', 'geom', 0)
  # Search contains
  env.expect('FT.SEARCH', 'idx', '@geom:[contains $poly]', 'PARAMS', 2, 'poly', 'POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))', 'DIALECT', 3).equal([0])

@skip()  # TODO: GEOMETRY - Enable when can select FLAT index (MOD-5304)
def testFieldUpdate(env):
  ''' Test updating a field, keeping the rest intact '''
  
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'geom1', 'GEOSHAPE', 'geom2', 'GEOSHAPE').ok()
  field1 = ['geom1',  'POLYGON((1 1, 1 200, 200 200, 200 1, 1 1))']
  field2 = ['geom2', 'POLYGON((1 1, 1 140, 140 140, 140 1, 1 1))']
  conn.execute_command('HSET', 'k1', *field1, *field2)

  # Dump geoshape index
  assert_index_num_docs(env, 'idx', 'geom1', 1)
  assert_index_num_docs(env, 'idx', 'geom2', 1)

  # Search contains on geom field
  env.expect('FT.SEARCH', 'idx', '@geom1:[contains $poly]', 'PARAMS', 2, 'poly', 'POLYGON((1 1, 1 150, 150 150, 150 1, 1 1))', 'DIALECT', 3).equal([1, 'k1', [*field1, *field2]])
  # Search within on geom2 field
  env.expect('FT.SEARCH', 'idx', '@geom2:[within $poly]', 'PARAMS', 2, 'poly', 'POLYGON((1 1, 1 170, 170 170, 170 1, 1 1))', 'DIALECT', 3).equal([1, 'k1', [*field1, *field2]])

  # Update - make geom2 smaller
  field2 = ['geom2', 'POLYGON((1 1, 1 120, 120 120, 120 1, 1 1))']
  conn.execute_command('HSET', 'k1', *field2)

  # Search contains on geom field
  env.expect('FT.SEARCH', 'idx', '@geom1:[contains $poly]', 'PARAMS', 2, 'poly', 'POLYGON((1 1, 1 150, 150 150, 150 1, 1 1))', 'DIALECT', 3).equal([1, 'k1', [*field1, *field2]])
  # Search within on geom2 field
  env.expect('FT.SEARCH', 'idx', '@geom2:[within $poly]', 'PARAMS', 2, 'poly', 'POLYGON((1 1, 1 170, 170 170, 170 1, 1 1))', 'DIALECT', 3).equal([1, 'k1', [*field1, *field2]])

  # Update - make geom2 larger
  field2 = ['geom2', 'POLYGON((1 1, 1 180, 180 180, 180 1, 1 1))']
  conn.execute_command('HSET', 'k1', *field2)
  # Search contains on geom field
  env.expect('FT.SEARCH', 'idx', '@geom1:[contains $poly]', 'PARAMS', 2, 'poly', 'POLYGON((1 1, 1 150, 150 150, 150 1, 1 1))', 'DIALECT', 3).equal([1, 'k1', [*field1, *field2]])
  # Search within on geom2 field
  env.expect('FT.SEARCH', 'idx', '@geom2:[within $poly]', 'PARAMS', 2, 'poly', 'POLYGON((1 1, 1 170, 170 170, 170 1, 1 1))', 'DIALECT', 3).equal([0])

  # Update - make geom1 smaller
  field1 = ['geom1', 'POLYGON((1 1, 1 149, 149 149, 149 1, 1 1))']
  conn.execute_command('HSET', 'k1', *field1)
  # Search contains on geom field
  env.expect('FT.SEARCH', 'idx', '@geom1:[contains $poly]', 'PARAMS', 2, 'poly', 'POLYGON((1 1, 1 150, 150 150, 150 1, 1 1))', 'DIALECT', 3).equal([0])
  # Search within on geom2 field
  env.expect('FT.SEARCH', 'idx', '@geom2:[within $poly]', 'PARAMS', 2, 'poly', 'POLYGON((1 1, 1 170, 170 170, 170 1, 1 1))', 'DIALECT', 3).equal([0])

@skip()  # TODO: GEOMETRY - Enable when can select FLAT index (MOD-5304)
def testFtInfo(env):
  ''' Test FT.INFO on GEOSHAPE '''
  
  conn = getConnectionByEnv(env)
  info_key_name = 'total_geoshapes_index_size_mb'
  
  env.expect('FT.CREATE', 'idx1', 'SCHEMA', 'geom', 'GEOSHAPE', 'txt', 'TEXT').ok()
  env.expect('FT.CREATE', 'idx2_no_geom', 'SCHEMA', 'txt', 'TEXT').ok()
  res = to_dict(env.cmd('FT.INFO idx1'))
  env.assertEqual(int(res[info_key_name]), 0)

  # Ingest of a non-geoshape attribute should not affect mem usage
  conn.execute_command('HSET', 'doc1', 'txt', 'Not a real POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))')
  res = to_dict(env.cmd('FT.INFO idx1'))
  env.assertEqual(int(res[info_key_name]), 0)

  doc_num = 100

  # Memory usage should increase
  usage = 0
  for i in range(1, doc_num + 1):
    conn.execute_command('HSET', f'doc{i}', 'geom', f'POLYGON(({2*i} {2*i}, {2*i} {100+2*i}, {100+2*i} {100+2*i}, {2*i} {100+2*i}, {2*i} {2*i}))')
    # Ingest of geoshape attribute should increase mem usage
    res = to_dict(env.cmd('FT.INFO idx1'))
    cur_usage = float(res[info_key_name])
    env.assertGreater(cur_usage, usage)
    usage = cur_usage

  # Memory usage should decrease
  for i in range(1, int(doc_num / 2)):
    conn.execute_command('DEL', f'doc{i}')
    res = to_dict(env.cmd('FT.INFO idx1'))
    cur_usage = float(res[info_key_name])
    env.assertLess(cur_usage, usage)
    usage = cur_usage

  # Dropping the geoshape index should reset memory usage
  conn.execute_command('FT.DROPINDEX', 'idx1')
  waitForNoCleanup(env, 'idx2_no_geom')
  res = to_dict(env.cmd('FT.INFO idx2_no_geom'))
  cur_usage = float(res[info_key_name])
  if not env.isCluster():
    env.assertEqual(cur_usage, 0)
  else:
    # TODO: in cluster - be able to wait for cleaning of the index (would wait for freeing the geoshape index memory)
    env.assertLess(cur_usage, usage)
