from RLTest import Env
from common import *
import json

def testSanitySearchHashWithin(env):
  
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'geom', 'GEOMETRY').ok()
  
  conn.execute_command('HSET', 'small', 'geom', 'POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))')
  conn.execute_command('HSET', 'large', 'geom', 'POLYGON((1 1, 1 200, 200 200, 200 1, 1 1))')
  expected = ['geom', 'POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))']
  env.expect('FT.SEARCH', 'idx', '@geom:[within:POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))]', 'DIALECT', 3).equal([1, 'small', expected])
  
  # TODO: GEOMETRY - Use params
  #env.expect('FT.SEARCH', 'idx', '@geom:[within $POLY]', 'PARAMS', '2', 'POLY', 'POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))').equal([1, 'small', expected])

  env.expect('FT.SEARCH', 'idx', '@geom:[within:POLYGON((0 0, 0 250, 250 250, 250 0, 0 0))]', 'NOCONTENT', 'DIALECT', 3).equal([2, 'small', 'large'])


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
  env.expect('FT.SEARCH', 'idx', '@geom:[within:POLYGON((0 0, 0 250, 250 250, 250 0, 0 0))]', 'NOCONTENT', 'DIALECT', 3).equal([2, 'small', 'large'])


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
