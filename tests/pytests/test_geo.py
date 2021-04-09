from RLTest import Env
from common import getConnectionByEnv

def testGeoHset(env):
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE idx SCHEMA g GEO').ok()
  conn.execute_command('HSET', 'geo2', 'g', '1.23,4.56')
  conn.execute_command('HSET', 'geo3', 'g', '"1.23,4.56"')
  conn.execute_command('HSET', 'geo4', 'g', '\"1.23,4.56\"')
  conn.execute_command('HSET', 'geo5', 'g', '\\"1.23,4.56\\"')
  env.expect('FT.SEARCH', 'idx', '@g:[1.23 4.56 1 km]').equal([3L, 'geo2', ['g', '1.23,4.56'],
                                                                   'geo3', ['g', '"1.23,4.56"'],
                                                                   'geo4', ['g', '"1.23,4.56"']])

def testGeoFtAdd(env):
  env.expect('FT.CREATE idx SCHEMA g GEO').ok()
  env.expect('FT.ADD', 'idx', 'geo1', '1', 'FIELDS', 'g', '1.23', '4.56').error().contains('Fields must be specified in FIELD VALUE pairs')
  env.expect('FT.ADD', 'idx', 'geo2', '1', 'FIELDS', 'g', '1.23,4.56').ok()
  env.expect('FT.ADD', 'idx', 'geo3', '1', 'FIELDS', 'g', '"1.23,4.56"').ok() # this is an error and won't index
  env.expect('FT.SEARCH', 'idx', '@g:[1.23 4.56 1 km]').equal([2L, 'geo2', ['g', '1.23,4.56'],
                                                                   'geo3', ['g', '"1.23,4.56"']] )

def testGeoDistanceSimple(env):
  env.skipOnCluster()
  env.expect('ft.create', 'idx', 'schema', 'name', 'text', 'location', 'geo', 'hq', 'geo').ok()
  env.expect('FT.ADD', 'idx', 'geo1', '1', 'FIELDS', 'location', '1.22,4.56', 'hq', '1.25,4.5').ok()
  env.expect('FT.ADD', 'idx', 'geo2', '1', 'FIELDS', 'location', '1.24,4.56', 'hq', '1.25,4.5').ok()
  env.expect('FT.ADD', 'idx', 'geo3', '1', 'FIELDS', 'location', '1.23,4.55', 'hq', '1.25,4.5').ok()
  env.expect('FT.ADD', 'idx', 'geo4', '1', 'FIELDS', 'location', '1.23,4.57', 'hq', '1.25,4.5').ok()
  env.expect('FT.SEARCH', 'idx', '@location:[1.23 4.56 10 km]', 'nocontent').equal([4L, 'geo1', 'geo2', 'geo3', 'geo4'])

  # test profile
  env.cmd('FT.CONFIG', 'SET', '_PRINT_PROFILE_CLOCK', 'false')
  res = ['Iterators profile',
          ['Type', 'UNION', 'Query type', 'GEO', 'Counter', 4L, 'Children iterators',
            ['Type', 'GEO', 'Term', '1.23,4.55 - 1.21176,4.57724', 'Counter', 2L, 'Size', 2L],
            ['Type', 'GEO', 'Term', '1.21176,4.57724 - 1.24,4.56', 'Counter', 2L, 'Size', 2L]]]

  act_res = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '@location:[1.23 4.56 10 km]', 'nocontent')
  env.assertEqual(act_res[1][3], res)

  res = [4L, ['distance', '5987.15'], ['distance', '6765.06'], ['distance', '7456.63'], ['distance', '8095.49']]

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
             'SORTBY', 2, '@distance', 'ASC').equal,(res)

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
             'SORTBY', 2, '@distance', 'ASC').equal,(res)

  # geodistance("lon,lat","lon,lat")
  env.expect('ft.aggregate', 'idx', '*',
             'LOAD', '1', '@location',
             'APPLY', 'geodistance("1.26,4.5","1.25,4.5")', 'AS', 'distance',
             'GROUPBY', '1', '@distance',
             'SORTBY', 2, '@distance', 'ASC').equal([1L, ['distance', '1108.83']])

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

  res = [102L, ['distance', '0'], ['distance', '95.43'], ['distance', '399.66'], ['distance', '1896.44'],
               ['distance', '2018.14'], ['distance', '2073.48'], ['distance', '2640.42'],
               ['distance', '2715.46'], ['distance', '3657.74'], ['distance', '4047.96']]

  env.expect('ft.aggregate', 'idx', 'hilton',
              'LOAD', '1', '@location',
              'APPLY', 'geodistance(@location,-0.15036,51.50566)', 'AS', 'distance',
              'GROUPBY', '1', '@distance',
              'SORTBY', 2, '@distance', 'ASC').equal(res)