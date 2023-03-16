from RLTest import Env
from common import *
import json

def sortResultByKeyName(res):
  '''
    Sorts the result by name
    res = [<COUNT>, '<NAME_1>, '<VALUE_1>', '<NAME_2>, '<VALUE_2>', ...] 
  '''
  pairs = [(name,value) for name,value in zip(res[1::2], res[2::2])]
  pairs = [i for i in sorted(pairs, key=lambda x: x[0])]
  pairs = [i for pair in pairs for i in pair]
  res = [res[0], *pairs]
  return res


def testSanitySearchHashWithin(env):
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'geom', 'GEOMETRY').ok()
  
  conn.execute_command('HSET', 'small', 'geom', 'POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))')
  conn.execute_command('HSET', 'large', 'geom', 'POLYGON((1 1, 1 200, 200 200, 200 1, 1 1))')
  expected = ['geom', 'POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))']
  env.expect('FT.SEARCH', 'idx', '@geom:[within:POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))]', 'DIALECT', 3).equal([1, 'small', expected])
  
  # TODO: GEOMETRY - Use params
  #env.expect('FT.SEARCH', 'idx', '@geom:[within $POLY]', 'PARAMS', '2', 'POLY', 'POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))').equal([1, 'small', expected])

  res = env.execute_command('FT.SEARCH', 'idx', '@geom:[within:POLYGON((0 0, 0 250, 250 250, 250 0, 0 0))]', 'NOCONTENT', 'DIALECT', 3)
  env.assertEqual(toSortedFlatList(res), [2, 'large', 'small'])


def testSanitySearchJsonWithin(env):
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE idx ON JSON SCHEMA $.geom AS geom GEOMETRY').ok()

  conn.execute_command('JSON.SET', 'small', '$', '{"geom": "POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))"}')
  conn.execute_command('JSON.SET', 'large', '$', '{"geom": "POLYGON((1 1, 1 200, 200 200, 200 1, 1 1))"}')
  expected = ['$', '[{"geom":"POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))"}]']
  env.expect('FT.SEARCH', 'idx', '@geom:[within:POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))]', 'DIALECT', 3).equal([1, 'small', expected])
  
  # TODO: GEOMETRY - Use params
  #env.expect('FT.SEARCH', 'idx', '@geom:[within $POLY]', 'PARAMS', '2', 'POLY', 'POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))').equal([1, 'small', expected])

  env.expect('FT.SEARCH', 'idx', '@geom:[within:POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))]', 'RETURN', 1, 'geom', 'DIALECT', 3).equal([1, 'small', ['geom', json.dumps([json.loads(expected[1])[0]['geom']])]])
  res = env.execute_command('FT.SEARCH', 'idx', '@geom:[within:POLYGON((0 0, 0 250, 250 250, 250 0, 0 0))]', 'NOCONTENT', 'DIALECT', 3)
  env.assertEqual(toSortedFlatList(res), [2, 'large', 'small'])


def testSanitySearchJsonCombined(env):
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE idx ON JSON SCHEMA $.geom AS geom GEOMETRY $.name as name TEXT').ok()

  conn.execute_command('JSON.SET', 'p1', '$', '{"geom": "POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))", "name": "Homer"}')
  conn.execute_command('JSON.SET', 'p2', '$', '{"geom": "POLYGON((1 1, 1 100, 90 90, 100 1, 1 1))", "name": "Bart"}')
  env.expect('FT.SEARCH', 'idx', '@name:(Ho*) @geom:[within:POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))]', 'NOCONTENT', 'DIALECT', 2).equal([1, 'p1'])
  

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


def testWKTQueryError(env):
  ''' Test query error '''
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE idx ON JSON SCHEMA $.geom AS geom GEOMETRY $.name as name TEXT').ok()

  env.expect('FT.SEARCH', 'idx', '@name:(Ho*) @geom:[within:POLIGON((0 0, 0 150, 150 150, 150 0, 0 0))]', 'NOCONTENT', 'DIALECT', 3).error().contains('POLIGON')

def testSimpleUpdate(env):
  ''' Test updating geometries '''
  
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'geom', 'GEOMETRY').ok()
  conn.execute_command('HSET', 'k1', 'geom', 'POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))')
  conn.execute_command('HSET', 'k2', 'geom', 'POLYGON((1 1, 1 200, 200 200, 200 1, 1 1))')
  
  expected1 = ['geom', 'POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))']
  expected2 = ['geom', 'POLYGON((1 1, 1 120, 120 120, 120 1, 1 1))']
  
  env.expect('FT.SEARCH', 'idx', '@geom:[within:POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))]', 'DIALECT', 3).equal([1, 'k1', expected1])
  
  res = conn.execute_command('FT.DEBUG', 'DUMP_GEOMIDX', 'idx', 'geom')
  env.debugPrint(str(res), force=True)

  # Update
  conn.execute_command('HSET', 'k2', 'geom', 'POLYGON((1 1, 1 120, 120 120, 120 1, 1 1))')
  res = env.execute_command('FT.SEARCH', 'idx', '@geom:[within:POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))]', 'DIALECT', 3)
  env.assertEqual(sortResultByKeyName(res), sortResultByKeyName([2, 'k1', expected1, 'k2', expected2]))
