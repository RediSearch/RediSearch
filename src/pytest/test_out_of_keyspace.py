from common import getConnectionByEnv

def testFlushall(env):
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 't', 'TEXT').ok()
  env.expect('FT.ADD idx doc1 1 FIELDS t RediSearch').ok()
  env.expect('FT.SEARCH idx *').equal([1L, 'doc1', ['t', 'RediSearch']])
  res = conn.execute_command('KEYS', '*')
  env.assertEqual(res, ['doc1'])
  
  env.flush()

  env.expect('FT.SEARCH idx *').equal('idx: no such index')
  env.expect('KEYS *').equal([])

  env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 't', 'TEXT').ok()
  env.expect('FT.ADD idx doc1 1 FIELDS t RediSearch').ok()
  env.expect('FT.SEARCH idx *').equal([1L, 'doc1', ['t', 'RediSearch']])
  res = conn.execute_command('KEYS', '*')
  env.assertEqual(res, ['doc1'])