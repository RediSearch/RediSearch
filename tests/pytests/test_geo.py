from RLTest import Env
from common import *

def testGeoHset(env):
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE idx SCHEMA g GEO').ok()
  conn.execute_command('HSET', 'geo2', 'g', '1.23,4.56')
  conn.execute_command('HSET', 'geo3', 'g', '"1.23,4.56"')
  conn.execute_command('HSET', 'geo4', 'g', '\"1.23,4.56\"')
  conn.execute_command('HSET', 'geo5', 'g', '\\"1.23,4.56\\"')
  env.expect('FT.SEARCH', 'idx', '@g:[1.23 4.56 1 km]').equal([1, 'geo2', ['g', '1.23,4.56']])

def testGeoSortable(env):
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE idx SCHEMA location GEO SORTABLE').ok()
  conn.execute_command('HSET', 'geo2', 'location', '1.23,4.56')

  env.expect('ft.aggregate', 'idx', '*',
             'APPLY', 'geodistance(@location,1.25,4.5)', 'AS', 'distance',
             'GROUPBY', '1', '@distance').equal([1, ['distance', '7032.37']])

def testGeoFtAdd(env):
  env.expect('FT.CREATE idx SCHEMA g GEO').ok()
  env.expect('FT.ADD', 'idx', 'geo1', '1', 'FIELDS', 'g', '1.23', '4.56').error().contains('Fields must be specified in FIELD VALUE pairs')
  env.expect('FT.ADD', 'idx', 'geo2', '1', 'FIELDS', 'g', '1.23,4.56').ok()
  env.expect('FT.ADD', 'idx', 'geo3', '1', 'FIELDS', 'g', '"1.23,4.56"').ok() # this is an error and won't index
  env.expect('FT.SEARCH', 'idx', '@g:[1.23 4.56 1 km]').equal([1, 'geo2', ['g', '1.23,4.56']])

def testGeoLong(env):
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE idx SCHEMA g GEO').ok()
  conn.execute_command('HSET', 'geo', 'g',  '1.2345678901234567890,4.5678901234567890')
  env.expect('FT.SEARCH', 'idx', '*').equal([1, 'geo', ['g', '1.2345678901234567890,4.5678901234567890']])
  env.expect('FT.SEARCH', 'idx', '@g:[1.23 4.56 2 km]').equal([1, 'geo', ['g', '1.2345678901234567890,4.5678901234567890']])

@skip(cluster=True)
def testGeoDistanceSimple(env):
  env.expect('ft.create', 'idx', 'schema', 'name', 'text', 'location', 'geo', 'hq', 'geo').ok()
  env.expect('HSET', 'geo1', 'location', '1.22,4.56', 'hq', '1.25,4.5').equal(2)
  env.expect('HSET', 'geo2', 'location', '1.24,4.56', 'hq', '1.25,4.5').equal(2)
  env.expect('HSET', 'geo3', 'location', '1.23,4.55', 'hq', '1.25,4.5').equal(2)
  env.expect('HSET', 'geo4', 'location', '1.23,4.57', 'hq', '1.25,4.5').equal(2)
  env.expect('FT.SEARCH', 'idx', '@location:[1.23 4.56 10 km]', 'nocontent').equal([4, 'geo1', 'geo2', 'geo3', 'geo4'])

  # documents with values out of range fail to index
  assertInfoField(env, 'idx', 'hash_indexing_failures', 0)
  env.expect('HSET', 'geo5', 'location', '181,4.56', 'hq', '1.25,4.5').equal(2)
  env.expect('HSET', 'geo6', 'location', '1.23,86', 'hq', '1.25,4.5').equal(2)
  assertInfoField(env, 'idx', 'hash_indexing_failures', 2)

  # querying for invalid value fails with a message
  env.expect('FT.SEARCH', 'idx', '@location:[1.23 4.56 -10 km]').error().contains('Invalid GeoFilter radius')
  env.expect('FT.SEARCH', 'idx', '@location:[181 4.56 10 km]').error().contains('Invalid GeoFilter lat/lon')
  env.expect('FT.SEARCH', 'idx', '@location:[1.23 86 10 km]').equal([0])
  # test profile
  env.cmd(config_cmd(), 'SET', '_PRINT_PROFILE_CLOCK', 'false')
  res = ['Type', 'GEO', 'Term', '1.23,4.55 - 1.24,4.56', 'Counter', 4, 'Size', 4]

  act_res = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '@location:[1.23 4.56 10 km]', 'nocontent')
  env.assertEqual(act_res[1][1][0][3], res)

  res = [4, ['distance', '5987.15'], ['distance', '6765.06'], ['distance', '7456.63'], ['distance', '8095.49']]

  # geodistance(@field,@field)
  env.expect('ft.aggregate', 'idx', '*',
             'LOAD', '2', '@location', '@hq',
             'APPLY', 'geodistance(@location,@hq)', 'AS', 'distance',
             'GROUPBY', '1', '@distance',
             'SORTBY', 2, '@distance', 'ASC').equal(res)

  # geodistance(@field,lon,lat)
  env.expect('ft.aggregate', 'idx', '*',
             'LOAD', '1', '@location',
             'APPLY', 'geodistance(@location,1.25,4.5)', 'AS', 'distance',
             'GROUPBY', '1', '@distance',
             'SORTBY', 2, '@distance', 'ASC').equal(res)

  # geodistance(@field,"lon,lat")
  env.expect('ft.aggregate', 'idx', '*',
             'LOAD', '1', '@location',
             'APPLY', 'geodistance(@location,"1.25,4.5")', 'AS', 'distance',
             'GROUPBY', '1', '@distance',
             'SORTBY', 2, '@distance', 'ASC').equal(res)

  # geodistance(lon,lat,lon,lat)
  env.expect('ft.aggregate', 'idx', '*',
             'LOAD', '1', '@location',
             'APPLY', 'geodistance(1.2,4.5,1.25,4.5)', 'AS', 'distance',
             'GROUPBY', '1', '@distance',
             'SORTBY', 2, '@distance', 'ASC').equal([1, ['distance', '5544.17']])

  # geodistance("lon,lat","lon,lat")
  env.expect('ft.aggregate', 'idx', '*',
             'LOAD', '1', '@location',
             'APPLY', 'geodistance("1.26,4.5","1.25,4.5")', 'AS', 'distance',
             'GROUPBY', '1', '@distance',
             'SORTBY', 2, '@distance', 'ASC').equal([1, ['distance', '1108.83']])

  # geodistance(lon,lat, @field)
  env.expect('ft.aggregate', 'idx', '*',
             'LOAD', '1', '@location',
             'APPLY', 'geodistance(1.25,4.5, @location)', 'AS', 'distance',
             'GROUPBY', '1', '@distance',
             'SORTBY', 2, '@distance', 'ASC').equal(res)

  # geodistance("lon,lat", @field)
  env.expect('ft.aggregate', 'idx', '*',
             'LOAD', '1', '@location',
             'APPLY', 'geodistance("1.25,4.5", @location)', 'AS', 'distance',
             'GROUPBY', '1', '@distance',
             'SORTBY', 2, '@distance', 'ASC').equal(res)

from hotels import hotels
def testGeoDistanceFile(env):
  env.expect('ft.create', 'idx', 'schema', 'name', 'text', 'location', 'geo').ok()

  for i, hotel in enumerate(hotels):
    env.expect('ft.add', 'idx', 'hotel{}'.format(i), 1.0, 'fields', 'name',
                  hotel[0], 'location', '{},{}'.format(hotel[2], hotel[1])).ok()

  res = [102, ['distance', '0'], ['distance', '95.43'], ['distance', '399.66'], ['distance', '1896.44'],
               ['distance', '2018.14'], ['distance', '2073.48'], ['distance', '2640.42'],
               ['distance', '2715.46'], ['distance', '3657.74'], ['distance', '4047.96']]

  env.expect('ft.aggregate', 'idx', 'hilton',
              'LOAD', '1', '@location',
              'APPLY', 'geodistance(@location,-0.15036,51.50566)', 'AS', 'distance',
              'GROUPBY', '1', '@distance',
              'SORTBY', 2, '@distance', 'ASC').equal(res)

# causes server crash before MOD-5646 fix
@skip(cluster=True)
def testGeoOnReopen(env):
  env.expect(config_cmd(), 'SET', 'FORK_GC_CLEAN_THRESHOLD', 0).ok()
  env.expect(config_cmd(), 'SET', 'FORK_GC_RUN_INTERVAL', 1000).ok()
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT', 'location', 'GEO').ok()
  conn = getConnectionByEnv(env)

  n = 2000

  def loadDocs(num = len(hotels), offset = 0):
    for i in range(num):
      (hotel_name, lat, long) = hotels[(i + offset) % len(hotels)]
      conn.execute_command('HSET', i + offset, 'name', hotel_name, 'location', f'{long},{lat}')

  loadDocs(n)
  forceInvokeGC(env) # to ensure the timer is set
  loadDocs(n // 2) # overwriting half of the docs

  ids = set()
  def checkResults(res):
    for id in [int(r[1]) for r in res[1:]]:
      env.assertNotContains(id, ids)
      ids.add(id)

  res, cursor = conn.execute_command('FT.AGGREGATE', 'idx', '@location:[-0.15036 51.50566 10000 km]',
                                     'LOAD', 3, '@__key', 'AS', 'id',
                                     'WITHCURSOR', 'COUNT', 100)
  checkResults(res)

  forceInvokeGC(env) # trigger the GC to clean all the overwritten docs

  while cursor != 0:
    res, cursor = conn.execute_command('FT.CURSOR', 'READ', 'idx', cursor)
    checkResults(res)

  env.assertEqual(len(ids), n)
