def test_1282(env):
  env.expect('FT.CREATE idx SCHEMA txt1 TEXT txt2 TAG').equal('OK')
  env.expect('FT.ADD idx doc1 1.0 FIELDS txt1 foo txt2 bar').equal('OK')
  env.expect('FT.ADD idx doc2 1.0 FIELDS txt1 baz txt2 xyz').equal('OK')
  env.expect('FT.ADD idx doc3 1.0 FIELDS txt1 hello txt2 world').equal('OK')
  env.expect('FT.ADD idx doc4 1.0 FIELDS txt1 jim txt2 burie').equal('OK')


  # test with tags would crash server
  env.expect('FT.SEARCH idx', '~world ~bar')

#  res = [4L, 'doc3', ['txt1', 'hello', 'txt2', 'world'],
#             'doc1', ['txt1', 'foo', 'txt2', 'bar'],
#             'doc4', ['txt1', 'jim', 'txt2', 'burie']]
#  env.expect('FT.SEARCH idx', 'hello ~foo ~jim').equal(res)
#  env.expect('FT.SEARCH idx', '~foo hello ~jim').equal(res)
#  env.expect('FT.SEARCH idx', '~foo ~jim hello').equal(res)