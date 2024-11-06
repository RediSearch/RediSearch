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
    res = env.cmd(debug_cmd(), 'DUMP_GEOMIDX', idx, attr)
    res = env.cmd(debug_cmd(), 'DUMP_GEOMIDX', idx, attr)
    res = array_of_key_value_to_map(res)
    env.assertEqual(res['num_docs'], num_docs)

def testSanitySearchHashWithin(env):
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'geom', 'GEOSHAPE', 'FLAT').ok()

  small = 'POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))'
  large = 'POLYGON((1 1, 1 200, 200 200, 200 1, 1 1), (2 2, 49 2, 49 49, 2 49, 2 2))' # contains hole
  conn.execute_command('HSET', 'small', 'geom', small)
  conn.execute_command('HSET', 'large', 'geom', large)

  query = 'POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))'
  env.expect('FT.SEARCH', 'idx', '@geom:[within $poly]', 'PARAMS', 2, 'poly', query, 'DIALECT', 3).equal([1, 'small', ['geom', small]])
  env.expect('FT.SEARCH', 'idx', '@geom:[contains $poly]', 'PARAMS', 2, 'poly', query, 'DIALECT', 3).equal([0])

  query = 'POLYGON((50 50, 50 99, 99 99, 99 50, 50 50))'
  env.expect('FT.SEARCH', 'idx', '@geom:[within $poly]', 'PARAMS', 2, 'poly', query, 'DIALECT', 3).equal([0])
  res = env.cmd('FT.SEARCH', 'idx', '@geom:[contains $poly]', 'PARAMS', 2, 'poly', query, 'NOCONTENT', 'DIALECT', 3)
  env.assertEqual(toSortedFlatList(res), [2, 'large', 'small'])

  query = 'POLYGON((0 0, 0 250, 250 250, 250 0, 0 0))'
  res = env.cmd('FT.SEARCH', 'idx', '@geom:[within $poly]', 'PARAMS', 2, 'poly', query, 'NOCONTENT', 'DIALECT', 3)
  env.assertEqual(toSortedFlatList(res), [2, 'large', 'small'])
  env.expect('FT.SEARCH', 'idx', '@geom:[contains $poly]', 'PARAMS', 2, 'poly', query, 'DIALECT', 3).equal([0])

def testSanitySearchPointWithin(env):
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'geom', 'GEOSHAPE', 'FLAT').ok()

  point = 'POINT(10 10)'
  small = 'POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))'
  large = 'POLYGON((1 1, 1 200, 200 200, 200 1, 1 1))'

  conn.execute_command('HSET', 'point', 'geom', point)
  conn.execute_command('HSET', 'small', 'geom', small)
  conn.execute_command('HSET', 'large', 'geom', large)

  expected = [2, 'point', ['geom', point], 'small', ['geom', small]]
  env.expect('FT.SEARCH', 'idx', '@geom:[within $poly]', 'PARAMS', 2, 'poly', 'POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))', 'DIALECT', 3).equal(expected)

  res = env.cmd('FT.SEARCH', 'idx', '@geom:[contains $poly]', 'PARAMS', 2, 'poly', 'POLYGON((2 2, 2 50, 50 50, 50 2, 2 2))', 'DIALECT', 3)
  env.assertEqual(res[0], 2)
  res = env.cmd('FT.SEARCH', 'idx', '@geom:[contains $poly]', 'PARAMS', 2, 'poly', 'POINT(50 50)', 'DIALECT', 3)
  env.assertEqual(res[0], 2)

  res = env.cmd('FT.SEARCH', 'idx', '@geom:[within $poly]', 'PARAMS', 2, 'poly', 'POLYGON((0 0, 0 250, 250 250, 250 0, 0 0))', 'NOCONTENT', 'DIALECT', 3)
  env.assertEqual(toSortedFlatList(res), [3, 'large', 'point', 'small'])

  conn.execute_command('HSET', 'point', 'geom', 'POINT(255, 255)')
  res = env.cmd('FT.SEARCH', 'idx', '@geom:[within $poly]', 'PARAMS', 2, 'poly', 'POLYGON((0 0, 0 250, 250 250, 250 0, 0 0))', 'NOCONTENT', 'DIALECT', 3)
  env.assertEqual(toSortedFlatList(res), [2, 'large', 'small'])

@skip(no_json=True)
def testSanitySearchJsonWithin(env):
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE idx ON JSON SCHEMA $.geom AS geom GEOSHAPE FLAT').ok()

  conn.execute_command('JSON.SET', 'small', '$', '{"geom": "POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))"}')
  conn.execute_command('JSON.SET', 'large', '$', '{"geom": "POLYGON((1 1, 1 200, 200 200, 200 1, 1 1))"}')
  expected = ['$', '[{"geom":"POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))"}]']
  env.expect('FT.SEARCH', 'idx', '@geom:[within $poly]', 'PARAMS', 2, 'poly', 'POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))', 'DIALECT', 3).equal([1, 'small', expected])

  env.expect('FT.SEARCH', 'idx', '@geom:[within $poly]', 'PARAMS', 2, 'poly', 'POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))', 'RETURN', 1, 'geom', 'DIALECT', 3).equal([1, 'small', ['geom', json.dumps([json.loads(expected[1])[0]['geom']])]])
  res = env.cmd('FT.SEARCH', 'idx', '@geom:[within $poly]', 'PARAMS', 2, 'poly', 'POLYGON((0 0, 0 250, 250 250, 250 0, 0 0))', 'NOCONTENT', 'DIALECT', 3)
  env.assertEqual(toSortedFlatList(res), [2, 'large', 'small'])

@skip(no_json=True)
def testSanitySearchJsonCombined(env):
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE idx ON JSON SCHEMA $.geom AS geom GEOSHAPE FLAT $.name as name TEXT').ok()

  conn.execute_command('JSON.SET', 'p1', '$', '{"geom": "POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))", "name": "Homer"}')
  conn.execute_command('JSON.SET', 'p2', '$', '{"geom": "POLYGON((1 1, 1 100, 90 90, 100 1, 1 1))", "name": "Bart"}')
  env.expect('FT.SEARCH', 'idx', '@name:(Ho*) @geom:[within $poly]', 'PARAMS', 2, 'poly', 'POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))', 'NOCONTENT', 'DIALECT', 2).equal([1, 'p1'])


def testSanitySearchHashIntersectsDisjoint(env):
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'geom', 'GEOSHAPE', 'FLAT').ok()

  wide = 'POLYGON((1 1, 1 200, 100 200, 100 1, 1 1))'
  tall = 'POLYGON((1 1, 1 100, 200 100, 200 1, 1 1))'
  conn.execute_command('HSET', 'wide', 'geom', wide)
  conn.execute_command('HSET', 'tall', 'geom', tall)

  res = env.cmd('FT.SEARCH', 'idx', '@geom:[intersects $poly]', 'PARAMS', 2, 'poly', wide, 'NOCONTENT', 'DIALECT', 3)
  env.assertEqual(toSortedFlatList(res), [2, 'tall', 'wide'])
  res = env.cmd('FT.SEARCH', 'idx', '@geom:[disjoint $poly]', 'PARAMS', 2, 'poly', wide, 'NOCONTENT', 'DIALECT', 3)
  env.assertEqual(toSortedFlatList(res), [0])

  query = 'POLYGON((0 101, 0 150, 150 150, 150 101, 0 101))'
  env.expect('FT.SEARCH', 'idx', '@geom:[intersects $poly]', 'PARAMS', 2, 'poly', query, 'DIALECT', 3).equal([1, 'wide', ['geom', wide]])
  env.expect('FT.SEARCH', 'idx', '@geom:[disjoint $poly]', 'PARAMS', 2, 'poly', query, 'DIALECT', 3).equal([1, 'tall', ['geom', tall]])

  query = 'POLYGON((101 101, 101 150, 150 150, 150 101, 101 101))'
  res = env.cmd('FT.SEARCH', 'idx', '@geom:[disjoint $poly]', 'PARAMS', 2, 'poly', query, 'NOCONTENT', 'DIALECT', 3)
  env.assertEqual(toSortedFlatList(res), [2, 'tall', 'wide'])

def test_MOD_7126(env):
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'geom', 'GEOSHAPE', 'FLAT').ok()

  point1 = 'POINT(10 10)'
  point2 = 'POINT(50 50)'
  triangle = 'POLYGON((20 20, 25 35, 35 25, 20 20))'
  rectangle = 'POLYGON((60 60, 65 75, 70 70, 65 55, 60 60))'
  conn.execute_command('HSET', 'point1', 'geom', point1)
  conn.execute_command('HSET', 'point2', 'geom', point2)
  conn.execute_command('HSET', 'triangle', 'geom', triangle)
  conn.execute_command('HSET', 'rectangle', 'geom', rectangle)

  query = 'POLYGON((15 15, 75 15, 50 70, 20 40, 15 15))'

  res = env.cmd('FT.SEARCH', 'idx', '@geom:[intersects $poly]', 'PARAMS', 2, 'poly', query, 'NOCONTENT', 'DIALECT', 3)
  env.assertEqual(toSortedFlatList(res), [2, 'point2', 'triangle'])
  res = env.cmd('FT.SEARCH', 'idx', '@geom:[disjoint $poly]', 'PARAMS', 2, 'poly', query, 'NOCONTENT', 'DIALECT', 3)
  env.assertEqual(toSortedFlatList(res), [2, 'point1', 'rectangle'])



# TODO: GEOMETRY - Enable with sanitizer (MOD-5182)
@skip(asan=True, no_json=True)
def testWKTIngestError(env):
  ''' Test ingest error '''

  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE idx ON JSON SCHEMA $.geom AS geom GEOSHAPE $.name as name TEXT').ok()

  def get_last_error():
    return to_dict(index_info(env)['Index Errors'])['last indexing error']
  # Wrong keyword
  conn.execute_command('JSON.SET', 'p1', '$', '{"geom": "POLIKON((1 1, 1 100, 100 100, 100 1, 1 1))", "name": "Homer"}')
  env.assertContains("Error indexing geoshape: Should start with 'POLYGON'", get_last_error())
  # Missing parenthesis
  conn.execute_command('JSON.SET', 'p2', '$', '{"geom": "POLYGON(1 1, 1 100, 100 100, 100 1, 1 1))", "name": "Patty"}')
  env.assertContains("Error indexing geoshape: Expected '(' at '1'", get_last_error())
  # Zero coordinates
  conn.execute_command('JSON.SET', 'p6', '$', '{"geom": "POLYGON(()())", "name": "Mr. Burns"}')
  env.assertContains("Error indexing geoshape: invalid geometry: Geometry has too few points", get_last_error())
  # Too few coordinates
  conn.execute_command('JSON.SET', 'p7', '$', '{"geom": "POLYGON((1 1, 1 100, 1 1))", "name": "Milhouse"}')
  env.assertContains("Error indexing geoshape: invalid geometry: Geometry has too few points", get_last_error())
  # Spike
  conn.execute_command('JSON.SET', 'p8', '$', '{"geom": "POLYGON((1 1, 1 200, 1 100, 100 1, 1 1))", "name": "Marge"}')
  env.assertContains("Error indexing geoshape: invalid geometry: Geometry has spikes", get_last_error())
  # Self-intersection
  conn.execute_command('JSON.SET', 'p9', '$', '{"geom": "POLYGON((1 1, 1 100, 50 50, 50 -50, 1 150, 100 50, 1 1))", "name": "Lisa"}')
  env.assertContains("Error indexing geoshape: invalid geometry: Geometry has invalid self-intersections", get_last_error())
  # Interior outside exterior
  conn.execute_command('JSON.SET', 'p10', '$', '{"geom": "POLYGON((1 1, 1 100, 100 100, 100 1, 1 1) (25 150, 75 150, 75 250, 25 250, 25 150))", "name": "Sideshow Bob"}')
  env.assertContains("Error indexing geoshape: invalid geometry: Geometry has interior rings defined outside the outer boundary", get_last_error())
  # Nested interiors
  conn.execute_command('JSON.SET', 'p11', '$', '{"geom": "POLYGON((1 1, 1 100, 100 100, 100 1, 1 1) (25 25, 75 25, 75 75, 25 75, 25 25) (49 49, 51 49, 51 51, 49 51, 49 49))", "name": "Krusty"}')
  env.assertContains("Error indexing geoshape: invalid geometry: Geometry has nested interior rings", get_last_error())
  # Invalid coordinates
  conn.execute_command('JSON.SET', 'p14', '$', '{"geom": "POLYGON((1 1, 1 100, 100 100, bart, 1 1))", "name": "Bart"}')
  env.assertContains("Error indexing geoshape: bad lexical cast", get_last_error())


  # TODO: GEOMETRY - understand why the following WKTs do not fail?
  # Missing Y coordinate
  conn.execute_command('JSON.SET', 'p3', '$', '{"geom": "POLYGON((1 1, 1 , 100 100, 100 1, 1 1))", "name": "Moe"}')
  # Redundant coordinate
  conn.execute_command('JSON.SET', 'p4', '$', '{"geom": "POLYGON((1 1, 1 100 100 100, 100 1, 1 1))", "name": "Seymour"}')
  # Missing comma separator
  conn.execute_command('JSON.SET', 'p5', '$', '{"geom": "POLYGON((1 1 1 100, 100 100, 100 1, 1 1))", "name": "Ned"}')
  # Duplicate points - we remove duplicates with bg::correct
  conn.execute_command('JSON.SET', 'p13', '$', '{"geom": "POLYGON((1 1, 1 100, 100 100, 100 1, 100 1, 1 1))", "name": "Selma"}')
  # Hourglass
  conn.execute_command('JSON.SET', 'p15', '$', '{"geom": "POLYGON((1 1, 1 100, 50 50, 50 -50, 1 1))", "name": "Maggie"}')

  # Indexing failures
  env.assertEqual(index_info(env)['hash_indexing_failures'], 9)


# TODO: GEOMETRY - Enable with sanitizer (MOD-5182)
@skip(asan=True, no_json=True)
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
  env.expect('FT.SEARCH', 'idx', '@name:(Ho*) @geom:[within $poly]', 'PARAMS', 2, 'poly', 'POLIGON((0 0, 0 150, 150 150, 150 0, 0 0))', 'NOCONTENT', 'DIALECT', 3).error()
  env.expect('FT.SEARCH', 'idx', '@name:(Ho*) @geom:[contains $poly]', 'PARAMS', 2, 'poly', '', 'NOCONTENT', 'DIALECT', 3).error()
  env.expect('FT.SEARCH', 'idx', '@name:(Ho*) @geom:[contains $poly]', 'PARAMS', 2, 'poly', '0 0', 'NOCONTENT', 'DIALECT', 3).error()
  env.expect('FT.SEARCH', 'idx', '@name:(Ho*) @geom:[contains $poly]', 'PARAMS', 2, 'poly', 'POLYGON', 'NOCONTENT', 'DIALECT', 3).error().contains('Expected')
  env.expect('FT.SEARCH', 'idx', '@name:(Ho*) @geom:[within $poly]', 'PARAMS', 2, 'poly', 'POLYGON(0 0, 0 150, 150 150, 150 0, 0 0))', 'NOCONTENT', 'DIALECT', 3).error()
  env.expect('FT.SEARCH', 'idx', '@name:(Ho*) @geom:[within $poly]', 'PARAMS', 2, 'poly', 'POLIGON((0 0, a 150, 150 150, 150 0, 0 0))', 'NOCONTENT', 'DIALECT', 3).error()
  # Bad/missing param
  env.expect('FT.SEARCH', 'idx', '@name:(Ho*) @geom:[contains $poly]', 'PARAMS', 2, 'moly', 'POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))]', 'NOCONTENT', 'DIALECT', 3).error().contains('No such parameter')
  env.expect('FT.SEARCH', 'idx', '@name:(Ho*) @geom:[within $poly]', 'NOCONTENT', 'DIALECT', 3).error().contains('No such parameter')

def testSimpleUpdate(env):
  ''' Test updating geometries '''

  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'geom', 'GEOSHAPE', 'FLAT', 'geom2', 'GEOSHAPE', 'FLAT').ok()
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
  res = env.cmd('FT.SEARCH', 'idx', '@geom:[within $poly]', 'PARAMS', 2, 'poly', 'POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))', 'DIALECT', 3)
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
  res = env.cmd('FT.SEARCH', 'idx', '@geom:[contains $poly]', 'PARAMS', 2, 'poly', 'POLYGON((2 2, 2 150, 150 150, 150 2, 2 2))', 'DIALECT', 3)
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

def testFieldUpdate(env):
  ''' Test updating a field, keeping the rest intact '''

  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'geom1', 'GEOSHAPE', 'FLAT', 'geom2', 'GEOSHAPE', 'FLAT').ok()
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

def testFtInfo(env):
  ''' Test FT.INFO on GEOSHAPE '''

  conn = getConnectionByEnv(env)
  info_key_name = 'geoshapes_sz_mb'

  env.expect('FT.CREATE', 'idx1', 'SCHEMA', 'geom', 'GEOSHAPE', 'FLAT', 'txt', 'TEXT').ok()
  env.expect('FT.CREATE', 'idx2_no_geom', 'SCHEMA', 'txt', 'TEXT').ok()
  res = to_dict(env.cmd('FT.INFO idx1'))
  cur_usage = float(res[info_key_name]) # index is not lazily built. even an empty index consumes some memory

  # Ingest of a non-geoshape attribute should not affect mem usage
  conn.execute_command('HSET', 'doc1', 'txt', 'Not a real POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))')
  res = to_dict(env.cmd('FT.INFO idx1'))
  env.assertEqual(float(res[info_key_name]), cur_usage)

  doc_num = 10000

  # Memory usage should increase
  usage = 0
  for i in range(1, doc_num + 1):
    conn.execute_command('HSET', f'doc{i}', 'geom', f'POLYGON(({2*i} {2*i}, {2*i} {100+2*i}, {100+2*i} {100+2*i}, {100+2*i} {2*i}, {2*i} {2*i}))')
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
