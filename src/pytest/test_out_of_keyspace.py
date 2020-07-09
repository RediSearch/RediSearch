def testFlushall(env):
  env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 't', 'TEXT').ok()
  env.expect('FT.ADD idx doc1 1 FIELDS t RediSearch').ok()
  env.expect('FT.SEARCH idx *').equal([1L, 'doc1', ['t', 'RediSearch']])
  env.expect('KEYS *').equal(['doc1'])
  
  env.expect('FLUSHALL').equal(1)

  env.expect('FT.SEARCH idx *').equal('idx: no such index')
  env.expect('KEYS *').equal([])

  env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 't', 'TEXT').ok()
  env.expect('FT.ADD idx doc1 1 FIELDS t RediSearch').ok()
  env.expect('FT.SEARCH idx *').equal([1L, 'doc1', ['t', 'RediSearch']])
  env.expect('KEYS *').equal(['doc1'])