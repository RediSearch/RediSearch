from RLTest import Env

def testGeoHset(env):
  env.expect('FT.CREATE idx SCHEMA g GEO').ok()
  env.expect('HSET geo1 g 1.23 4.56').error().contains('wrong number of arguments for HMSET')
  env.expect('HSET geo2 g 1.23,4.56').equal(1)
  env.expect('HSET geo3 g "1.23,4.56"').equal(1)
  env.expect('FT.SEARCH', 'idx', '@g:[1.23 4.56 1 km]').equal([1L, 'geo2', ['g', '1.23,4.56']])

def testGeoFtAdd(env):
  env.expect('FT.CREATE idx SCHEMA g GEO').ok()
  env.expect('FT.ADD idx geo1 1 FIELDS g 1.23 4.56').error().contains('Fields must be specified in FIELD VALUE pairs')
  env.expect('FT.ADD idx geo2 1 FIELDS g 1.23,4.56').ok()
  env.expect('FT.ADD idx geo3 1 FIELDS g "1.23,4.56"').ok()
  env.expect('FT.SEARCH', 'idx', '@g:[1.23 4.56 1 km]').equal([1L, 'geo2', ['g', '1.23,4.56']])
