# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

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

@skip(cluster=True)
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
  res = ['Type', 'GEO', 'Term', '1.23,4.55 - 1.24,4.56', 'Number of reading operations', 4, 'Estimated number of matches', 4]

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

  con = env.getClusterConnectionIfNeeded()
  for i, hotel in enumerate(hotels):
    env.assertOk(con.execute_command('ft.add', 'idx', f'hotel{i}', 1.0, 'fields', 'name',
                  hotel[0], 'location', f'{hotel[2]},{hotel[1]}'))

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
def testGeoOnReopen(env:Env):
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
      env.assertNotContains(id, ids, depth=1, message=f"Duplicate id {id} found in results")
      ids.add(id)

  res, cursor = conn.execute_command('FT.AGGREGATE', 'idx', '@location:[-0.15036 51.50566 10000 km]',
                                     'LOAD', 3, '@__key', 'AS', 'id',
                                     'WITHCURSOR', 'COUNT', 1000)
  checkResults(res)

  forceInvokeGC(env) # trigger the GC to clean all the overwritten docs

  while cursor != 0:
    res, cursor = conn.execute_command('FT.CURSOR', 'READ', 'idx', cursor)
    checkResults(res)

  env.assertEqual(len(ids), n)

@skip(cluster=True)
def testGeoLargeRadiusDecreaseStep(env):
  """Exercise the decrease_step path in geohashGetAreasByRadius.
  At high latitudes, longitude cells are physically compressed
  (cos(85°) ≈ 0.087), so a 620 km radius at lat=85 exceeds the
  east/west neighbor cell boundaries at step 3 (~45° cells =
  ~556 km physical width), triggering the step decrease."""
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'g', 'GEO').ok()

  points = [
    ('doc1', '30.0,85.0'),   # at the query center
    ('doc2', '31.0,84.5'),   # very close to center
    ('doc3', '0.0,85.0'),    # ~288 km west, within radius
    ('doc4', '0.0,0.0'),     # equator, well outside radius
  ]
  for name, loc in points:
    conn.execute_command('HSET', name, 'g', loc)
  waitForIndex(env, 'idx')

  # 620 km radius at (30, 85): at step 3 (313-626 km range), east neighbor
  # far edge at 90° lon is ~556 km from center — less than 620 km radius,
  # so decrease_step triggers.
  res = env.cmd('FT.SEARCH', 'idx', '@g:[30.0 85.0 620 km]', 'NOCONTENT')
  env.assertGreaterEqual(res[0], 2, message=res)
  env.assertNotContains('doc4', res)

@skip(cluster=True)
def testGeoParseNaN(env):
  """NaN is rejected by Rust parseGeo (R64 rejects non-finite values),
  so documents with NaN coordinates fail to index."""
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'g', 'GEO').ok()

  conn.execute_command('HSET', 'nan_lon', 'g', 'NaN,1.0')
  conn.execute_command('HSET', 'nan_lat', 'g', '1.0,NaN')
  conn.execute_command('HSET', 'nan_both', 'g', 'NaN,NaN')

  assertInfoField(env, 'idx', 'hash_indexing_failures', 3)

@skip(cluster=True)
def testGeoParseInfinity(env):
  """inf/-inf/infinity are rejected by Rust parseGeo (R64 rejects
  non-finite values), so documents with infinite coordinates fail to
  index."""
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'g', 'GEO').ok()

  conn.execute_command('HSET', 'inf_lon', 'g', 'inf,1.0')
  conn.execute_command('HSET', 'neg_inf_lon', 'g', '-inf,1.0')
  conn.execute_command('HSET', 'inf_lat', 'g', '1.0,inf')
  conn.execute_command('HSET', 'infinity_lon', 'g', 'infinity,1.0')

  assertInfoField(env, 'idx', 'hash_indexing_failures', 4)

@skip(cluster=True)
def testGeoParseTrailingWhitespace(env):
  """Trailing whitespace after a coordinate value is accepted by Rust
  parseGeo (trim() strips it before parsing)."""
  conn = getConnectionByEnv(env)
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'g', 'GEO').ok()

  conn.execute_command('HSET', 'ws1', 'g', '1.23,4.56 ')
  conn.execute_command('HSET', 'ws2', 'g', '1.23, 4.56 ')

  assertInfoField(env, 'idx', 'hash_indexing_failures', 0)

@skip(cluster=True)
def testGeoFilterUnits(env):
  """Literal, parameter and legacy unit parsing select the same radius."""
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'g', 'GEO').ok()
  conn = getConnectionByEnv(env)
  conn.execute_command('HSET', 'near', 'g', '13.001,52')
  conn.execute_command('HSET', 'far', 'g', '13.02,52')
  for unit, radius in [('m', 100), ('KM', 0.1), ('Ft', 328.084), ('mI', 0.0621373)]:
    for dialect in (1, 2):
      env.expect('FT.SEARCH', 'idx', f'@g:[13 52 {radius} {unit}]',
                 'NOCONTENT', 'DIALECT', dialect).equal([1, 'near'])
      env.expect('FT.SEARCH', 'idx', '*', 'GEOFILTER', 'g', 13, 52, radius, unit,
                 'NOCONTENT', 'DIALECT', dialect).equal([1, 'near'])
    env.expect('FT.SEARCH', 'idx', '@g:[13 52 $r $u]', 'PARAMS', 4,
               'r', radius, 'u', unit, 'NOCONTENT', 'DIALECT', 2).equal([1, 'near'])
    explanation = env.cmd('FT.EXPLAIN', 'idx', f'@g:[13 52 {radius} {unit}]', 'DIALECT', 2)
    env.assertContains(unit.lower(), explanation)

@skip(cluster=True)
def testGeoFilterValidation(env):
  """Validation order and geographic bounds are distinct from indexing bounds."""
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'g', 'GEO').ok()
  getConnectionByEnv(env).execute_command('HSET', 'doc', 'g', '13,52')
  cases = [
    ('unit first', 181, 91, -1, 'bad', 'Invalid GeoFilter unit'),
    ('longitude before radius', 181, 0, -1, 'm', 'Invalid GeoFilter lat/lon'),
    ('latitude outside', 0, -91, 1, 'm', 'Invalid GeoFilter lat/lon'),
    ('zero radius', 13, 52, 0, 'm', 'Invalid GeoFilter radius'),
    ('negative radius', 13, 52, -1, 'm', 'Invalid GeoFilter radius'),
  ]
  for name, lon, lat, radius, unit, error in cases:
    env.expect('FT.SEARCH', 'idx', '@g:[$lon $lat $r $u]', 'PARAMS', 8,
               'lon', lon, 'lat', lat, 'r', radius, 'u', unit, 'DIALECT', 2).error().contains(error)
  for lon, lat in [(180, 90), (-180, -90), (13, 86)]:
    env.expect('FT.SEARCH', 'idx', f'@g:[{lon} {lat} 1 m]', 'DIALECT', 2).equal([0])
  env.expect('FT.SEARCH', 'idx', '@g:[13 85 10000 km]', 'NOCONTENT', 'DIALECT', 2).equal([1, 'doc'])
  env.expect('FT.SEARCH', 'idx', '@g:[13 86 10000 km]', 'NOCONTENT', 'DIALECT', 2).equal([0])
  env.expect('FT.SEARCH', 'idx', '*', 'GEOFILTER', 'g', 13, 52, 'inf', 'm',
             'NOCONTENT', 'DIALECT', 2).equal([1, 'doc'])

@skip(cluster=True)
def testLegacyGeoFilterErrors(env):
  """Legacy argument conversion retains its exact diagnostics and rejects partial numbers."""
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'g', 'GEO').ok()
  prefix = ['FT.SEARCH', 'idx', '*', 'GEOFILTER']
  env.expect(*prefix, 'g', 13, 52, 1).error().contains('GEOFILTER requires 5 arguments')
  for offset, label in [(1, 'lon'), (2, 'lat'), (3, 'radius')]:
    for invalid in ['abc', '1junk', ' 1', '1 ', 'nan', '1\x00junk']:
      args = ['g', 13, 52, 1, 'm']
      args[offset] = invalid
      env.expect(*prefix, *args).error().contains(
        f'Bad arguments for <{label}>: Could not convert argument to expected type')
  env.expect(*prefix, 'g', 13, 52, 1, 'yards').error().contains('Unknown distance unit yards')

@skip(cluster=True)
def testGeoFilterBinaryArguments(env):
  """Legacy and parameter strings retain their C-string prefix interpretation."""
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'g', 'GEO').ok()
  getConnectionByEnv(env).execute_command('HSET', 'doc', 'g', '13,52')
  env.expect('FT.SEARCH', 'idx', '*', 'GEOFILTER', 'g\x00unused', 13, 52, 1,
             'm\x00unused', 'NOCONTENT', 'DIALECT', 2).equal([1, 'doc'])
  env.expect('FT.SEARCH', 'idx', '@g:[13 52 1 $u]', 'PARAMS', 2,
             'u', 'm\x00' + 'x' * 20, 'NOCONTENT', 'DIALECT', 2).equal([1, 'doc'])
  # redis-py decodes errors with replacement even with decode_responses=False.
  pool = env.getConnection().connection_pool
  connection = pool.get_connection()
  try:
    connection.send_command('FT.SEARCH', 'idx', '*', 'GEOFILTER', 'g', 13, 52, 1, b'\xff\x00ignored')
    with connection._sock.makefile('rb') as stream:
      env.assertEqual(stream.readline(), b'-SEARCH_PARSE_ARGS Unknown distance unit \xff\r\n')
  finally:
    connection.disconnect()
    pool.release(connection)
