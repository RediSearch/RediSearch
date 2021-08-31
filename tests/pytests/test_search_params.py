from includes import *
from common import *

def test_geo(env):
    conn = getConnectionByEnv(env)

    env.assertOk(conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'g', 'GEO', 'SORTABLE'))
    waitForIndex(env, 'idx')
    env.assertEqual(conn.execute_command('HSET', 'geo1', 'g', '29.69465, 34.95126'), 1L)
    env.assertEqual(conn.execute_command('HSET', 'geo2', 'g', '29.69350, 34.94737'), 1L)
    env.assertEqual(conn.execute_command('HSET', 'geo3', 'g', '29.68746, 34.94882'), 1L)

    res = conn.execute_command('FT.SEARCH', 'idx', '@g:[29.69465 34.95126 500 m]', 'NOCONTENT')
    env.assertEqual(res, [2L, 'geo1', 'geo2'])

    res = conn.execute_command('FT.SEARCH', 'idx', '@g:[29.69465 34.95126 10 km]', 'NOCONTENT')
    env.assertEqual(res, [3L, 'geo1', 'geo2', 'geo3'])

    # res = conn.execute_command('FT.SEARCH', 'idx', '@g:[29.69465 34.95126 $radius $units]', 'PARAMS', '4', 'radius', '500', 'units', 'm')
    # env.assertEqual(res, [2L, 'geo1', 'geo2'])
    #
    # res = conn.execute_command('FT.SEARCH', 'idx', '@g:[29.69465 34.95126 $radius $units]', 'PARAMS', '4', 'radius', '10', 'units', 'km')
    # env.assertEqual(res, [3L, 'geo1', 'geo2', 'geo3'])

    res = conn.execute_command('ft.aggregate', 'idx', '*',
                               'AP', 'geodistance(@g,29.69,34.94)', 'AS', 'dist',
                               'GROUPBY', '1', '@dist',
                               'SORTBY', '2', '@dist', 'ASC')
    env.assertEqual(res, [3L, ['dist', '879.66'], ['dist', '1007.98'], ['dist', '1322.22']])
    res = conn.execute_command('ft.aggregate', 'idx', '*',
                               'APPLY', 'geodistance(@g, $loc)', 'AS', 'dist',
                               'GROUPBY', '1', '@dist',
                               'SORTBY', '2', '@dist', 'ASC',
                               'PARAMS', '2', 'loc', '29.69,34.94')
    env.assertEqual(res, [3L, ['dist', '879.66'], ['dist', '1007.98'], ['dist', '1322.22']])


