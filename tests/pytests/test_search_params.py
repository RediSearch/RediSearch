# coding=utf-8
from includes import *
from common import *

def test_geo(env):
    conn = getConnectionByEnv(env)

    env.assertOk(conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'g', 'GEO', 'SORTABLE'))
    waitForIndex(env, 'idx')
    env.assertEqual(conn.execute_command('HSET', 'geo1', 'g', '29.69465, 34.95126'), 1L)
    env.assertEqual(conn.execute_command('HSET', 'geo2', 'g', '29.69350, 34.94737'), 1L)
    env.assertEqual(conn.execute_command('HSET', 'geo3', 'g', '29.68746, 34.94882'), 1L)

    # res = conn.execute_command('FT.SEARCH', 'idx', '@g:[29.69465 34.95126 500 m]', 'NOCONTENT')
    # env.assertEqual(res, [2L, 'geo1', 'geo2'])
    #
    # res = conn.execute_command('FT.SEARCH', 'idx', '@g:[29.69465 34.95126 10 km]', 'NOCONTENT')
    # env.assertEqual(res, [3L, 'geo1', 'geo2', 'geo3'])

    res = conn.execute_command('FT.SEARCH', 'idx', '@g:[29.69465 34.95126 $radius $units]', 'NOCONTENT', 'PARAMS', '4', 'radius', '500', 'units', 'm')
    env.assertEqual(res, [2L, 'geo1', 'geo2'])

    res = conn.execute_command('FT.SEARCH', 'idx', '@g:[29.69465 34.95126 $radius $units]', 'NOCONTENT', 'PARAMS', '4', 'radius', '10', 'units', 'km')
    env.assertEqual(res, [3L, 'geo1', 'geo2', 'geo3'])

    res = conn.execute_command('ft.aggregate', 'idx', '*',
                               'APPLY', 'geodistance(@g,29.69,34.94)', 'AS', 'dist',
                               'GROUPBY', '1', '@dist',
                               'SORTBY', '2', '@dist', 'ASC')
    env.assertEqual(res, [3L, ['dist', '879.66'], ['dist', '1007.98'], ['dist', '1322.22']])
    # FIXME: add param support in APPLY
    # res = conn.execute_command('ft.aggregate', 'idx', '*',
    #                            'APPLY', 'geodistance(@g, $loc)', 'AS', 'dist',
    #                            'GROUPBY', '1', '@dist',
    #                            'SORTBY', '2', '@dist', 'ASC',
    #                            'PARAMS', '2', 'loc', '29.69,34.94')
    env.assertEqual(res, [3L, ['dist', '879.66'], ['dist', '1007.98'], ['dist', '1322.22']])


    # Test errors (in usage: missing param, wrong param value, ...)
    env.expect('FT.SEARCH', 'idx', '@g:[29.69465 34.95126 $rapido $units]', 'NOCONTENT', 'PARAMS', '4', 'radius', '500', 'units', 'm').raiseError().equal('No such parameter `rapido`')
    env.expect('FT.SEARCH', 'idx', '@g:[29.69465 34.95126 $rapido $units]', 'NOCONTENT', 'PARAMS', '4', 'rapido', 'bad', 'units', 'm').raiseError().equal('Invalid numeric value (bad) for parameter `rapido`')
    env.expect('FT.SEARCH', 'idx', '@g:[29.69465 34.95126 $radius $units]', 'NOCONTENT', 'PARAMS', '4', 'radius', '500', 'units', 'badm').raiseError().contains('Invalid GeoFilter unit')
    # Test errors (in definition: duplicated param, missing param value, wrong count)
    env.expect('FT.SEARCH', 'idx', '@g:[29.69465 34.95126 $rapido $units]', 'NOCONTENT', 'PARAMS', '4', 'units', 'm').raiseError().contains('Bad arguments for PARAMS: Expected an argument')
    env.expect('FT.SEARCH', 'idx', '@g:[29.69465 34.95126 $rapido $units]', 'NOCONTENT', 'PARAMS', '2', 'units').raiseError().contains('Bad arguments for PARAMS: Expected an argument')
    env.expect('FT.SEARCH', 'idx', '@g:[29.69465 34.95126 $rapido $units]', 'NOCONTENT', 'PARAMS', '4', 'units', 'm', 'units', 'km').raiseError().contains('Duplicated parameter `units`')


def test_binary_data(env):
    conn = getConnectionByEnv(env)

    env.assertOk(conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'bin', 'TEXT'))
    waitForIndex(env, 'idx')

    bin_data1 = b'\xd7\x93\xd7\x90\xd7\x98\xd7\x94\xd7\x91\xd7\x99\xd7\xa0\xd7\x90\xd7\xa8\xd7\x99\xd7\x90\xd7\xa8\xd7\x95\xd7\x9a\xd7\x95\xd7\x9e\xd7\xa2\xd7\xa0\xd7\x99\xd7\x99\xd7\x9f'
    data_2 = '10010101001010101100101011001101010101'
    bin_data2 = b'10010101001010101100101011001101010101'

    env.assertEqual(conn.execute_command('HSET', 'key1', 'bin', bin_data1), 1L)
    env.assertEqual(conn.execute_command('HSET', 'key2', 'bin', bin_data2), 1L)

    res1 = conn.execute_command('FT.SEARCH', 'idx', '@bin:10010101001010101100101011001101010101', 'NOCONTENT')
    env.assertEqual(res1, [1L, 'key2'])

    res2 = conn.execute_command('FT.SEARCH', 'idx', '@bin:$val', 'NOCONTENT', 'PARAMS', '2', 'val', '10010101001010101100101011001101010101')
    env.assertEqual(res2, res1)

    res1 = conn.execute_command('FT.SEARCH', 'idx', '@bin:\xd7\x93\xd7\x90*', 'NOCONTENT')
    env.assertEqual(res1, [1L, 'key1'])

    # FIXME: Not evaluated as Prefix Node after parameter is substituted
    res2 = conn.execute_command('FT.SEARCH', 'idx', '@bin:$val', 'NOCONTENT', 'PARAMS', '2', 'val', '\xd7\x93\xd7\x90*')
    #env.assertEqual(res2, res1)


def test_expression(env):
    conn = getConnectionByEnv(env)

    env.assertOk(conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT', 'id', 'NUMERIC'))
    waitForIndex(env, 'idx')

    env.assertEqual(conn.execute_command('HSET', 'key1', 'name', 'Bob', 'id', '17'), 1L)
    env.assertEqual(conn.execute_command('HSET', 'key2', 'name', 'Alice', 'id', '31'), 1L)
    env.assertEqual(conn.execute_command('HSET', 'key3', 'name', 'Carol', 'id', '13'), 1L)
    env.assertEqual(conn.execute_command('HSET', 'key4', 'name', 'John Doe', 'id', '0'), 1L)

    res1 = conn.execute_command('FT.SEARCH', 'idx', '@name:($val1|Bob)', 'NOCONTENT', 'PARAMS', '2', 'val1', 'Alice')
    env.assertEqual(res1, [2L, 'key1', 'key2'])


