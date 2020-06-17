def test_1282(env):
  env.expect('FT.CREATE idx SCHEMA txt1 TEXT txt2 TEXT').equal('OK')
  env.expect('FT.ADD idx doc1 1.0 FIELDS txt1 foo txt2 bar').equal('OK')
  env.expect('FT.ADD idx doc2 1.0 FIELDS txt1 baz txt2 xyz').equal('OK')
  env.expect('FT.ADD idx doc3 1.0 FIELDS txt1 hello txt2 world').equal('OK')
  env.expect('FT.ADD idx doc4 1.0 FIELDS txt1 jim txt2 burie').equal('OK')

  res = [1L, 'doc3', ['txt1', 'hello', 'txt2', 'world']]
  env.expect('FT.SEARCH idx', 'hello ~in ~ter').equal(res)
  #env.expect('FT.SEARCH idx', '~in hello ~ter').equal(res)
  #env.expect('FT.SEARCH idx', '~in ~ter hello').equal(res)