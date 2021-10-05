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

    res2 = conn.execute_command('FT.SEARCH', 'idx', '@g:[$lon $lat $radius km]', 'NOCONTENT', 'PARAMS', '8', 'lon', '29.69465', 'lat', '34.95126', 'units', 'km', 'radius', '10')
    env.assertEqual(res, res2)
    res2 = conn.execute_command('FT.SEARCH', 'idx', '@g:[29.69465 $lat 10 $units]', 'NOCONTENT', 'PARAMS', '8', 'lon', '29.69465', 'lat', '34.95126', 'units', 'km', 'radius', '10')
    env.assertEqual(res, res2)
    res2 = conn.execute_command('FT.SEARCH', 'idx', '@g:[$lon $lat $radius km]', 'NOCONTENT', 'PARAMS', '8', 'lon', '29.69465', 'lat', '34.95126', 'units', 'km', 'radius', '10')
    env.assertEqual(res, res2)
    res2 = conn.execute_command('FT.SEARCH', 'idx', '@g:[$lon 34.95126 $radius $units]', 'NOCONTENT', 'PARAMS', '8', 'lon', '29.69465', 'lat', '34.95126', 'units', 'km', 'radius', '10')
    env.assertEqual(res, res2)
    res2 = conn.execute_command('FT.SEARCH', 'idx', '@g:[$lon 34.95126 $radius km]', 'NOCONTENT', 'PARAMS', '8', 'lon', '29.69465', 'lat', '34.95126', 'units', 'km', 'radius', '10')
    env.assertEqual(res, res2)

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


def test_errors(env):
    conn = getConnectionByEnv(env)

    env.assertOk(conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'foo', 'TEXT', 'bar', 'TAG', 'g', 'GEO', 'num', 'NUMERIC'))
    waitForIndex(env, 'idx')
    env.assertEqual(conn.execute_command('HSET', 'key1', 'foo', 'PARAMS', 'bar', 'PARAMS'), 2L)

    # Test errors in PARAMS definition: duplicated param, missing param value, wrong count
    env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'PARAMS', '4', 'param1', 'val1').raiseError().contains('Bad arguments for PARAMS: Expected an argument')
    env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'PARAMS', '2', 'param1').raiseError().contains('Bad arguments for PARAMS: Expected an argument')
    env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'PARAMS', '4', 'param1', 'val1', 'param1', 'val2').raiseError().contains('Duplicated parameter `param1`')
    #X env.expect('FT.SEARCH', 'idx', '*', 'PARAMS', '4', 'foo', 'x', 'bar', '100', 'NOCONTENT').raiseError().contains('Unknown argument `NOCONTENT`')
    # FIXME: Should next line be supported?
    # env.assertEqual(conn.execute_command('FT.SEARCH', 'idx', 'PARAMS', 'PARAMS', '4', 'foo', 'x', 'bar', '100'), [1L, 'key1', ['foo', 'PARAMS', 'bar', 'PARAMS']])
    # FIXME: Add erroneos tests: PARAMS as first command arg, param name with none-alphanumeric, param value with illegal character such as star, paren, etc.

    # Test errors in param usage: missing param, wrong param value
    env.expect('FT.SEARCH', 'idx', '@g:[29.69465 34.95126 $rapido $units]', 'NOCONTENT', 'PARAMS', '4', 'radius', '500', 'units', 'm').raiseError().equal('No such parameter `rapido`')
    env.expect('FT.SEARCH', 'idx', '@g:[29.69465 34.95126 $rapido $units]', 'NOCONTENT').raiseError().equal('No such parameter `rapido`')
    env.expect('FT.SEARCH', 'idx', '@g:[29.69465 34.95126 $rapido $units]', 'NOCONTENT', 'PARAMS', '4', 'rapido', 'bad', 'units', 'm').raiseError().equal('Invalid numeric value (bad) for parameter `rapido`')
    env.expect('FT.SEARCH', 'idx', '@g:[29.69465 34.95126 $radius $units]', 'NOCONTENT', 'PARAMS', '4', 'radius', '500', 'units', 'badm').raiseError().contains('Invalid GeoFilter unit')
    env.expect('FT.SEARCH', 'idx', '@num:[$min $max]', 'NOCONTENT', 'PARAMS', '4', 'min', '102', 'max', '-inf').raiseError().contains('Bad upper range')


def test_binary_data(env):
    conn = getConnectionByEnv(env)

    env.assertOk(conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'bin', 'TEXT'))
    waitForIndex(env, 'idx')

    bin_data1 = b'\xd7\x93\xd7\x90\xd7\x98\xd7\x94\xd7\x91\xd7\x99\xd7\xa0\xd7\x90\xd7\xa8\xd7\x99\xd7\x90\xd7\xa8\xd7\x95\xd7\x9a\xd7\x95\xd7\x9e\xd7\xa2\xd7\xa0\xd7\x99\xd7\x99\xd7\x9f'
    bin_data2 = b'10010101001010101100101011001101010101'

    env.assertEqual(conn.execute_command('HSET', 'key1', 'bin', bin_data1), 1L)
    env.assertEqual(conn.execute_command('HSET', 'key2', 'bin', bin_data2), 1L)

    # Compare results with and without param - data1
    res1 = conn.execute_command('FT.SEARCH', 'idx', '@bin:10010101001010101100101011001101010101', 'NOCONTENT')
    env.assertEqual(res1, [1L, 'key2'])
    res2 = conn.execute_command('FT.SEARCH', 'idx', '@bin:$val', 'NOCONTENT', 'PARAMS', '2', 'val', '10010101001010101100101011001101010101')
    env.assertEqual(res2, res1)

    # Compare results with and without param - data2
    res1 = conn.execute_command('FT.SEARCH', 'idx', '@bin:' + bin_data1, 'NOCONTENT')
    env.assertEqual(res1, [1L, 'key1'])
    res2 = conn.execute_command('FT.SEARCH', 'idx', '@bin:$val', 'NOCONTENT', 'PARAMS', '2', 'val', bin_data1)
    env.assertEqual(res2, res1)

    # Compare results with and without param using Prefix - data1
    res1 = conn.execute_command('FT.SEARCH', 'idx', '@bin:10010*', 'NOCONTENT')
    env.assertEqual(res1, [1L, 'key2'])

    res2 = conn.execute_command('FT.SEARCH', 'idx', '@bin:$val*', 'NOCONTENT', 'PARAMS', '2', 'val', '10010')
    env.assertEqual(res2, res1)

    # Compare results with and without param using Prefix - data2
    res1 = conn.execute_command('FT.SEARCH', 'idx', '@bin:\xd7\x93\xd7\x90*', 'NOCONTENT')
    env.assertEqual(res1, [1L, 'key1'])

    res2 = conn.execute_command('FT.SEARCH', 'idx', '@bin:$val*', 'NOCONTENT', 'PARAMS', '2', 'val', '\xd7\x93\xd7\x90')
    env.assertEqual(res2, res1)


def test_expression(env):
    conn = getConnectionByEnv(env)

    env.assertOk(conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT', 'id', 'NUMERIC'))
    waitForIndex(env, 'idx')

    env.assertEqual(conn.execute_command('HSET', 'key1', 'name', 'Bob', 'id', '17'), 2L)
    env.assertEqual(conn.execute_command('HSET', 'key2', 'name', 'Alice', 'id', '31'), 2L)
    env.assertEqual(conn.execute_command('HSET', 'key3', 'name', 'Carol', 'id', '13'), 2L)
    env.assertEqual(conn.execute_command('HSET', 'key4', 'name', 'John\\ Doe', 'id', '0'), 2L)
    env.assertEqual(conn.execute_command('HSET', 'key5', 'name', '$val1', 'id', '99'), 2L)

    res1 = conn.execute_command('FT.SEARCH', 'idx', '@name:(Alice|Bob)', 'NOCONTENT')
    env.assertEqual(res1, [2L, 'key2', 'key1'])
    res2 = conn.execute_command('FT.SEARCH', 'idx', '@name:($val1|Bob)', 'NOCONTENT', 'PARAMS', '2', 'val1', 'Alice')
    env.assertEqual(res2, res1)

    # # FIXME: Avoid parameterization in verbatim string (whether if a param is defined or not)
    # res1 = conn.execute_command('FT.SEARCH', 'idx', '@name:("$var1")', 'NOCONTENT')
    # env.assertEqual(res1, [1L, 'key5'])
    # res2 = conn.execute_command('FT.SEARCH', 'idx', '@name:("$val1")', 'NOCONTENT', 'PARAMS', '2', 'val1', 'Alice')
    # env.assertEqual(res2, res1)

    res1 = conn.execute_command('FT.SEARCH', 'idx', '@name:(Alice)', 'NOCONTENT')
    env.assertEqual(res1, [1L, 'key2'])
    res2 = conn.execute_command('FT.SEARCH', 'idx', '@name:($val1)', 'NOCONTENT', 'PARAMS', '2', 'val1', 'Alice')
    env.assertEqual(res2, res1)

    res1 = conn.execute_command('FT.SEARCH', 'idx', '@name:(John\\ Doe)', 'NOCONTENT')
    env.assertEqual(res1, [1L, 'key4'])
    res2 = conn.execute_command('FT.SEARCH', 'idx', '@name:($val1)', 'NOCONTENT', 'PARAMS', '2', 'val1', 'John\\ Doe')
    env.assertEqual(res2, res1)


def test_tags(env):
    conn = getConnectionByEnv(env)

    env.assertOk(conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'tags', 'TAG'))
    waitForIndex(env, 'idx')

    env.assertEqual(conn.execute_command('HSET', 'key1', 'tags', 't100,t200'), 1L)
    env.assertEqual(conn.execute_command('HSET', 'key2', 'tags', 't100,t300'), 1L)
    env.assertEqual(conn.execute_command('HSET', 'key3', 'tags', 't200,t300'), 1L)
    env.assertEqual(conn.execute_command('HSET', 'key4', 'tags', 't100 t200'), 1L)
    env.assertEqual(conn.execute_command('HSET', 'key5', 'tags', 't100 200'), 1L)

    res1 = conn.execute_command('FT.SEARCH', 'idx', '@tags:{t200|t100}', 'NOCONTENT')
    env.assertEqual(res1, [3L, 'key1', 'key2', 'key3'])
    res2 = conn.execute_command('FT.SEARCH', 'idx', '@tags:{$myT1|$myT2}', 'NOCONTENT', 'PARAMS', '4', 'myT1', 't100', 'myT2', 't200')
    env.assertEqual(res2, res1)

    res1 = conn.execute_command('FT.SEARCH', 'idx', '@tags:{t200}', 'NOCONTENT', 'PARAMS', '2', 'myT', 't200')
    env.assertEqual(res1, [2L, 'key1', 'key3'])
    res2 = conn.execute_command('FT.SEARCH', 'idx', '@tags:{$myT}', 'NOCONTENT', 'PARAMS', '2', 'myT', 't200')
    env.assertEqual(res2, res1)

    res1 = conn.execute_command('FT.SEARCH', 'idx', '@tags:{t100 t200}', 'NOCONTENT')
    env.assertEqual(res1, [1L, 'key4'])
    res2 = conn.execute_command('FT.SEARCH', 'idx', '@tags:{$myT1 $myT2}', 'NOCONTENT', 'PARAMS', '4', 'myT1', 't100', 'myT2', 't200')
    env.assertEqual(res2, res1)

    res1 = conn.execute_command('FT.SEARCH', 'idx', '@tags:{t100 200}', 'NOCONTENT')
    env.assertEqual(res1, [1L, 'key5'])
    res2 = conn.execute_command('FT.SEARCH', 'idx', '@tags:{$myT1 $myT2}', 'NOCONTENT', 'PARAMS', '4', 'myT1', 't100', 'myT2', '200')
    env.assertEqual(res2, res1)

    res2 = conn.execute_command('FT.SEARCH', 'idx', '@tags:{$myT1 200}', 'NOCONTENT', 'PARAMS', '4', 'myT1', 't100', 'myT2', '200')
    env.assertEqual(res2, res1)
    res2 = conn.execute_command('FT.SEARCH', 'idx', '@tags:{t100 $myT2}', 'NOCONTENT', 'PARAMS', '4', 'myT1', 't100', 'myT2', '200')
    env.assertEqual(res2, res1)


def test_numeric_range(env):
    conn = getConnectionByEnv(env)

    env.assertOk(conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 'numval', 'NUMERIC'))
    waitForIndex(env, 'idx')

    env.assertEqual(conn.execute_command('HSET', 'key1', 'numval', '101'), 1L)
    env.assertEqual(conn.execute_command('HSET', 'key2', 'numval', '102'), 1L)
    env.assertEqual(conn.execute_command('HSET', 'key3', 'numval', '103'), 1L)
    env.assertEqual(conn.execute_command('HSET', 'key4', 'numval', '104'), 1L)
    env.assertEqual(conn.execute_command('HSET', 'key5', 'numval', '105'), 1L)

    res1 = conn.execute_command('FT.SEARCH', 'idx', '@numval:[102 104]', 'NOCONTENT')
    env.assertEqual(res1, [3L, 'key2', 'key3', 'key4'])
    res2 = conn.execute_command('FT.SEARCH', 'idx', '@numval:[$min $max]', 'NOCONTENT', 'PARAMS', '4', 'min', '102', 'max', '104')
    env.assertEqual(res2, res1)

    res1 = conn.execute_command('FT.SEARCH', 'idx', '@numval:[(102 104]', 'NOCONTENT')
    env.assertEqual(res1, [2L, 'key3', 'key4'])
    res2 = conn.execute_command('FT.SEARCH', 'idx', '@numval:[($min $max]', 'NOCONTENT', 'PARAMS', '4', 'min', '102', 'max', '104')
    env.assertEqual(res2, res1)

    res1 = conn.execute_command('FT.SEARCH', 'idx', '@numval:[102 (104]', 'NOCONTENT')
    env.assertEqual(res1, [2L, 'key2', 'key3'])
    res2 = conn.execute_command('FT.SEARCH', 'idx', '@numval:[$min ($max]', 'NOCONTENT', 'PARAMS', '4', 'min', '102', 'max', '104')
    env.assertEqual(res2, res1)

    res1 = conn.execute_command('FT.SEARCH', 'idx', '@numval:[(102 (104]', 'NOCONTENT')
    env.assertEqual(res1, [1L, 'key3'])
    res2 = conn.execute_command('FT.SEARCH', 'idx', '@numval:[($min ($max]', 'NOCONTENT', 'PARAMS', '4', 'min', '102', 'max', '104')
    env.assertEqual(res2, res1)

    res1 = conn.execute_command('FT.SEARCH', 'idx', '@numval:[(102 +inf]', 'NOCONTENT')
    env.assertEqual(res1, [3L, 'key3', 'key4', 'key5'])
    res2 = conn.execute_command('FT.SEARCH', 'idx', '@numval:[($min $max]', 'NOCONTENT', 'PARAMS', '4', 'min', '102', 'max', '+inf')
    env.assertEqual(res2, res1)

    res1 = conn.execute_command('FT.SEARCH', 'idx', '@numval:[-inf, (105]', 'NOCONTENT')
    env.assertEqual(res1, [4L, 'key1', 'key2', 'key3', 'key4'])
    res2 = conn.execute_command('FT.SEARCH', 'idx', '@numval:[$min ($max]', 'NOCONTENT', 'PARAMS', '4', 'min', '-inf', 'max', '105')
    env.assertEqual(res2, res1)

    # Test error when param is not actually being used
    env.expect('FT.SEARCH', 'idx', '@numval:[-inf max]', 'NOCONTENT', 'PARAMS', '4', 'min', '-inf', 'max', '105').raiseError().contains('Expecting numeric or parameter')
    env.expect('FT.SEARCH', 'idx', '@numval:[min 105]', 'NOCONTENT', 'PARAMS', '4', 'min', '-inf', 'max', '105').raiseError().contains('Expecting numeric or parameter')

def test_vector(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE idx SCHEMA v VECTOR INT32 2 L2 HNSW').ok()
    conn.execute_command('HSET', 'a', 'v', 'abcdefgh')
    conn.execute_command('HSET', 'b', 'v', 'abcdefgg')
    conn.execute_command('HSET', 'c', 'v', 'zzzzxxxx')
    conn.execute_command('HSET', 'd', 'v', 'abbdefgh')

    res1 = [2L, 'd', ['v', 'abbdefgh'], 'a', ['v', 'abcdefgh']]
    res2 = conn.execute_command('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK 2]')
    env.assertEqual(res2, res1)
    res2 = conn.execute_command('FT.SEARCH', 'idx', '@v:[abcdefgh TOPK $k]', 'PARAMS', '6', 'vec', 'abcdefgh', 'type', 'TOPK', 'k', '2')
    env.assertEqual(res2, res1)
    res2 = conn.execute_command('FT.SEARCH', 'idx', '@v:[abcdefgh $type 2]', 'PARAMS', '6', 'vec', 'abcdefgh', 'type', 'TOPK', 'k', '2')
    env.assertEqual(res2, res1)
    res2 = conn.execute_command('FT.SEARCH', 'idx', '@v:[abcdefgh $type $k]', 'PARAMS', '6', 'vec', 'abcdefgh', 'type', 'TOPK', 'k', '2')
    env.assertEqual(res2, res1)
    res2 = conn.execute_command('FT.SEARCH', 'idx', '@v:[$vec TOPK 2]', 'PARAMS', '6', 'vec', 'abcdefgh', 'type', 'TOPK', 'k', '2')
    env.assertEqual(res2, res1)
    res2 = conn.execute_command('FT.SEARCH', 'idx', '@v:[$vec TOPK $k]', 'PARAMS', '6', 'vec', 'abcdefgh', 'type', 'TOPK', 'k', '2')
    env.assertEqual(res2, res1)
    res2 = conn.execute_command('FT.SEARCH', 'idx', '@v:[$vec $type 2]', 'PARAMS', '6', 'vec', 'abcdefgh', 'type', 'TOPK', 'k', '2')
    env.assertEqual(res2, res1)
    res2 = conn.execute_command('FT.SEARCH', 'idx', '@v:[$vec $type $k]', 'PARAMS', '6', 'vec', 'abcdefgh', 'type', 'TOPK', 'k', '2')
    env.assertEqual(res2, res1)

