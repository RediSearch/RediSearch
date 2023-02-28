from RLTest import Env
from common import *

def testSanitySearchHashWithin(env):
  
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'geom', 'GEOMETRY').ok()
  
  conn.execute_command('HSET', 'small', 'geom', 'POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))')
  conn.execute_command('HSET', 'large', 'geom', 'POLYGON((1 1, 1 200, 200 200, 200 1, 1 1))')
  expected = ['geom', 'POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))']
  env.expect('FT.SEARCH', 'idx', '@geom:[within:POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))]', 'DIALECT', 3).equal([1, 'small', expected])
  #env.expect('FT.SEARCH', 'idx', '@geom:[within $POLY]', 'PARAMS', '2', 'POLY', 'POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))').equal([1, 'small', expected])


def testSanitySearchJsonWithin(env):
  
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE idx ON JSON SCHEMA $.geom AS geom GEOMETRY').ok()

  conn.execute_command('JSON.SET', 'small', '$', '{"geom": "POLYGON((1 1, 1 100, 100 100, 100 1, 1 1))"}')
  conn.execute_command('JSON.SET', 'large', '$', '{"geom": "POLYGON((1 1, 1 200, 200 200, 200 1, 1 1))"}')
  expected = ['geom', 'POLYGON((1 1, 0 100, 100 100, 100 0, 1 1))']
  env.expect('FT.SEARCH', 'idx', '@geom:[within:POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))]', 'DIALECT', 3).equal([1, 'small', expected])
  #env.expect('FT.SEARCH', 'idx', '@geom:[within $POLY]', 'PARAMS', '2', 'POLY', 'POLYGON((0 0, 0 150, 150 150, 150 0, 0 0))').equal([1, 'small', expected])

