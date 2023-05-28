# coding=utf-8
from includes import *
from common import *
from RLTest import Env


def test_geo(env):
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'g', 'GEO', 'SORTABLE').ok()
    waitForIndex(env, 'idx')

    env.assertEqual(conn.execute_command('HSET', 'geo1', 'g', '29.69465, 34.95126'), 1)
    env.assertEqual(conn.execute_command('HSET', 'geo2', 'g', '29.69350, 34.94737'), 1)
    env.assertEqual(conn.execute_command('HSET', 'geo3', 'g', '29.68746, 34.94882'), 1)

    # res = env.execute_command('FT.SEARCH', 'idx', '@g:[29.69465 34.95126 500 m]', 'NOCONTENT')
    # env.assertEqual(res, [2, 'geo1', 'geo2'])
    #
    # res = env.execute_command('FT.SEARCH', 'idx', '@g:[29.69465 34.95126 10 km]', 'NOCONTENT')
    # env.assertEqual(res, [3, 'geo1', 'geo2', 'geo3'])

    res = env.execute_command('FT.SEARCH', 'idx', '@g:[29.69465 34.95126 $radius $units]', 'NOCONTENT', 'PARAMS', '4', 'radius', '500', 'units', 'm')
    env.assertEqual(res, [2, 'geo1', 'geo2'])

    res = env.execute_command('FT.SEARCH', 'idx', '@g:[29.69465 34.95126 $radius $units]', 'NOCONTENT', 'PARAMS', '4', 'radius', '10', 'units', 'km')
    env.assertEqual(res, [3, 'geo1', 'geo2', 'geo3'])

    res2 = env.execute_command('FT.SEARCH', 'idx', '@g:[$lon $lat $radius km]', 'NOCONTENT', 'PARAMS', '8', 'lon', '29.69465', 'lat', '34.95126', 'units', 'km', 'radius', '10')
    env.assertEqual(res, res2)
    res2 = env.execute_command('FT.SEARCH', 'idx', '@g:[29.69465 $lat 10 $units]', 'NOCONTENT', 'PARAMS', '8', 'lon', '29.69465', 'lat', '34.95126', 'units', 'km', 'radius', '10')
    env.assertEqual(res, res2)
    res2 = env.execute_command('FT.SEARCH', 'idx', '@g:[$lon $lat $radius km]', 'NOCONTENT', 'PARAMS', '8', 'lon', '29.69465', 'lat', '34.95126', 'units', 'km', 'radius', '10')
    env.assertEqual(res, res2)
    res2 = env.execute_command('FT.SEARCH', 'idx', '@g:[$lon 34.95126 $radius $units]', 'NOCONTENT', 'PARAMS', '8', 'lon', '29.69465', 'lat', '34.95126', 'units', 'km', 'radius', '10')
    env.assertEqual(res, res2)
    res2 = env.execute_command('FT.SEARCH', 'idx', '@g:[$lon 34.95126 $radius km]', 'NOCONTENT', 'PARAMS', '8', 'lon', '29.69465', 'lat', '34.95126', 'units', 'km', 'radius', '10')
    env.assertEqual(res, res2)

    res = env.execute_command('FT.AGGREGATE', 'idx', '*',
                               'APPLY', 'geodistance(@g,29.69,34.94)', 'AS', 'dist',
                               'GROUPBY', '1', '@dist',
                               'SORTBY', '2', '@dist', 'ASC')
    env.assertEqual(res, [3, ['dist', '879.66'], ['dist', '1007.98'], ['dist', '1322.22']])
    env.assertEqual(res, [3, ['dist', '879.66'], ['dist', '1007.98'], ['dist', '1322.22']])


def test_param_errors(env):
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'foo', 'TEXT', 'bar', 'TAG', 'g', 'GEO', 'num', 'NUMERIC',
               'v', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()
    waitForIndex(env, 'idx')

    env.assertEqual(conn.execute_command('HSET', 'key1', 'foo', 'PARAMS', 'bar', 'PARAMS'), 2)

    # Test errors in PARAMS definition: duplicated param, missing param value, wrong count
    env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'PARAMS', '4', 'param1', 'val1').raiseError().contains('Bad arguments for PARAMS: Expected an argument')
    env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'PARAMS', '2', 'param1').raiseError().contains('Bad arguments for PARAMS: Expected an argument')
    env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'PARAMS', '4', 'param1', 'val1', 'param1', 'val2').raiseError().contains('Duplicate parameter `param1`')
    env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'PARAMS', '3').raiseError()
    env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'PARAMS').raiseError()

    # The search query can be literally 'PARAMS'
    env.assertEqual(env.execute_command('FT.SEARCH', 'idx', 'PARAMS', 'PARAMS', '4', 'foo', 'x', 'bar', '100'), [1, 'key1', ['foo', 'PARAMS', 'bar', 'PARAMS']])
    env.assertEqual(env.execute_command('FT.AGGREGATE', 'idx', 'PARAMS', 'PARAMS', '4', 'foo', 'x', 'bar', '100', 'LOAD', 2, '@foo', '@bar'), [1, ['foo', 'PARAMS', 'bar', 'PARAMS']])

    # Parameter definitions cannot come before the search query
    env.expect('FT.SEARCH', 'idx', 'PARAMS', '4', 'foo', 'x', 'bar', '100', 'PARAMS').raiseError()
    env.expect('FT.AGGREGATE', 'idx', 'PARAMS', '4', 'foo', 'x', 'bar', '100', 'PARAMS').raiseError()

    # Parameters can be defined only once
    env.expect('FT.SEARCH', 'idx', '*', 'PARAMS', '4', 'foo', 'x', 'bar', '100', 'PARAMS', '4', 'goo', 'y', 'baz', '900').raiseError()
    env.expect('FT.AGGREGATE', 'idx', '*', 'PARAMS', '4', 'foo', 'x', 'bar', '100', 'PARAMS', '4', 'goo', 'y', 'baz', '900').raiseError()

    # Test errors in param usage: missing param, wrong param value
    env.expect('FT.SEARCH', 'idx', '@g:[29.69465 34.95126 $radius 100]', 'NOCONTENT').raiseError().contains('No such parameter `radius`')
    env.expect('FT.SEARCH', 'idx', '@g:[29.69465 34.95126 $rapido $units]', 'NOCONTENT', 'PARAMS', '4', 'radius', '500', 'units', 'm').raiseError().equal('No such parameter `rapido`')
    env.expect('FT.SEARCH', 'idx', '@g:[29.69465 34.95126 $rapido $units]', 'NOCONTENT').raiseError().equal('No such parameter `rapido`')
    env.expect('FT.SEARCH', 'idx', '@g:[29.69465 34.95126 $rapido $units]', 'NOCONTENT', 'PARAMS', '4', 'rapido', 'bad', 'units', 'm').raiseError().equal('Invalid numeric value (bad) for parameter `rapido`')
    env.expect('FT.SEARCH', 'idx', '@g:[29.69465 34.95126 200 100]', 'NOCONTENT').raiseError().contains('Invalid GeoFilter unit')
    env.expect('FT.SEARCH', 'idx', '@g:[29.69465 34.95126 $radius $units]', 'NOCONTENT', 'PARAMS', '4', 'radius', '500', 'units', 'badm').raiseError().contains('Invalid GeoFilter unit')
    env.expect('FT.SEARCH', 'idx', '@num:[$min $max]', 'NOCONTENT', 'PARAMS', '4', 'min', '102', 'max', '-inf').raiseError().contains('Bad upper range')

    # Test parsing errors
    env.expect('FT.SEARCH', 'idx', '@g:[29.69465 badval $rapido $units]', 'NOCONTENT', 'PARAMS', '4', 'rapido', 'bad', 'units', 'm').raiseError().contains('Syntax error')
    env.expect('FT.SEARCH', 'idx', '@g:[foo bar $radius $units]', 'NOCONTENT', 'PARAMS', '4', 'radius', '500', 'units', 'badm').raiseError().contains('Syntax error')
    env.expect('FT.SEARCH', 'idx', '@g:[29.69465 34.95126 badval $units]', 'NOCONTENT').raiseError().contains('Syntax error')

    env.expect('FT.SEARCH', 'idx', '@numval:[-inf max]', 'NOCONTENT', 'PARAMS', '4', 'min', '-inf', 'max', '105').raiseError().contains('Syntax error')
    env.expect('FT.SEARCH', 'idx', '@numval:[min 105]', 'NOCONTENT', 'PARAMS', '4', 'min', '-inf', 'max', '105').raiseError().contains('Syntax error')

    env.expect('FT.SEARCH', 'idx', '*=>[TKOO 4 @v $B]').raiseError().contains('Syntax error')
    env.expect('FT.SEARCH', 'idx', '*=>[KNN badval @v $B]').raiseError().contains('Syntax error')
    env.expect('FT.SEARCH', 'idx', '*=>[KNN $k @v $B]', 'PARAMS', '2', 'k', 'TKOO').raiseError().contains('No such parameter `B`')

    # Test Attribute errors
    env.expect('FT.SEARCH', 'idx', '* => [KNN $k @v $vec EF_RUNTIME $EF]', 'PARAMS', '6', 'vec', 'aaaaaaaa', 'k', 'zzz', 'EF', '10').raiseError().contains('Invalid numeric value')
    env.expect('FT.SEARCH', 'idx', '* => [KNN $k @v $vec EF_RUNTIME $EF]', 'PARAMS', '6', 'vec', 'aaaaaaaa', 'k', '2.71', 'EF', '10').raiseError().contains('Invalid numeric value')
    env.expect('FT.SEARCH', 'idx', '* => [KNN $k @v $vec EF_RUNTIME $EF]', 'PARAMS', '6', 'vec', 'aaaaaaaa', 'k', '-3', 'EF', '10').raiseError().contains('Invalid numeric value')
    env.expect('FT.SEARCH', 'idx', '* => [KNN $k @v $vec EF_RUNTIME $EF]', 'PARAMS', '6', 'vec', 'aaaaaaaa', 'k', '2', 'lunchtime', 'zzz').raiseError().contains('No such parameter')
    env.expect('FT.SEARCH', 'idx', '* => [KNN $k @v $vec]=>{$EF_RUNTIME: $EF;}', 'PARAMS', '6', 'vec', 'aaaaaaaa', 'k', 'zzz', 'EF', '10').raiseError().contains('Invalid numeric value')
    env.expect('FT.SEARCH', 'idx', '* => [KNN $k @v $vec]=>{$EF_RUNTIME: $EF;}', 'PARAMS', '6', 'vec', 'aaaaaaaa', 'k', '2.71', 'EF', '10').raiseError().contains('Invalid numeric value')
    env.expect('FT.SEARCH', 'idx', '* => [KNN $k @v $vec]=>{$EF_RUNTIME: $EF;}', 'PARAMS', '6', 'vec', 'aaaaaaaa', 'k', '-3', 'EF', '10').raiseError().contains('Invalid numeric value')
    env.expect('FT.SEARCH', 'idx', '* => [KNN $k @v $vec]=>{$EF_RUNTIME: $EF;}', 'PARAMS', '6', 'vec', 'aaaaaaaa', 'k', '2', 'lunchtime', 'zzz').raiseError().contains('No such parameter')
    env.expect('FT.SEARCH', 'idx', '@pron:(jon) => { $slop:1; $phonetic:$ph}', 'NOCONTENT', 'PARAMS', '6', 'min', '102', 'max', '204', 'ph', 'maybe').raiseError().contains('Invalid value')
    env.expect('FT.SEARCH', 'idx', '@pron:(jon) => { $slop:1; $phonetic:$ph;} => [KNN $k @v $vec]', 'NOCONTENT', 'PARAMS', '2', 'ph', 'maybe').raiseError().contains('Invalid value')
    env.expect('FT.SEARCH', 'idx', '@pron:(jon) => { $slop:1; $phonetic:$ph;} => [KNN $k @v $vec]', 'NOCONTENT', 'PARAMS', '2', 'ph').raiseError().contains('Bad arguments for PARAMS: Expected an argument, but none provided')
    env.expect('FT.SEARCH', 'idx', '@pron:(jon) => { $slop:1; $phonetic:$ph;} => [KNN $k @v $vec]', 'NOCONTENT', 'PARAMS', '1', 'ph').raiseError().contains('Parameters must be specified in PARAM VALUE pairs')

    # # Test Attribute names must begin with alphanumeric?
    # env.expect('FT.SEARCH', 'idx', '@g:[$3 $_4 $p_5 $_]', 'NOCONTENT',
    #            'PARAMS', '8', '3', '10', '_4', '20', 'p_5', '30', '_', 'km').raiseError()


def test_attr(env):
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name_ph', 'TEXT', 'PHONETIC', 'dm:en', 'name', 'TEXT').ok()
    waitForIndex(env, 'idx')

    env.assertEqual(conn.execute_command('HSET', 'key1', 'name_ph', 'John', 'name', 'John'), 2)
    env.assertEqual(conn.execute_command('HSET', 'key2', 'name_ph', 'Jon', 'name', 'Jon'), 2)
    env.assertEqual(conn.execute_command('HSET', 'key3', 'name_ph', 'Joy', 'name', 'Joy'), 2)
    env.assertEqual(conn.execute_command('HSET', 'key4', 'name_ph', 'Lucy', 'name', 'Lucy'), 2)

    # Error: field does not support phonetics
    env.expect('FT.SEARCH', 'idx', '@name:($name) => { $slop:$slop; $phonetic:$ph}', 'NOCONTENT', 'PARAMS', '6', 'name', 'jon', 'slop', '0', 'ph', 'true').raiseError()

    # With phonetic
    res1 = env.execute_command('FT.SEARCH', 'idx', '(@name_ph:(jon) => { $weight: 1; $phonetic:true}) | (@name_ph:(jon) => { $weight: 2; $phonetic:false})', 'NOCONTENT')
    env.assertEqual(res1, [2, 'key2', 'key1'])
    res2 = env.execute_command('FT.SEARCH', 'idx', '(@name_ph:($name) => { $weight: $w1; $phonetic:$ph1}) | (@name_ph:($name) => { $weight: $w2; $phonetic:false})', 'NOCONTENT', 'PARAMS', '12', 'name', 'jon', 'slop', '0', 'ph1', 'true', 'ph2', 'false', 'w1', '1', 'w2', '2')
    env.assertEqual(res2, res1)

    # Without phonetic
    res1 = env.execute_command('FT.SEARCH', 'idx', '@name_ph:(jon) => { $weight: 1; $phonetic:false}', 'NOCONTENT')
    env.assertEqual(res1, [1, 'key2'])
    res2 = env.execute_command('FT.SEARCH', 'idx', '@name_ph:($name) => { $weight: $w1; $phonetic:$ph1}', 'NOCONTENT', 'PARAMS', '6', 'name', 'jon', 'w1', '1', 'ph1', 'false')
    env.assertEqual(res2, res1)


def test_binary_data(env):
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'bin', 'TEXT').ok()
    waitForIndex(env, 'idx')

    bin_data1 = b'\xd7\x93\xd7\x90\xd7\x98\xd7\x94\xd7\x91\xd7\x99\xd7\xa0\xd7\x90\xd7\xa8\xd7\x99\xd7\x90\xd7\xa8\xd7\x95\xd7\x9a\xd7\x95\xd7\x9e\xd7\xa2\xd7\xa0\xd7\x99\xd7\x99\xd7\x9f'
    bin_data2 = b'10010101001010101100101011001101010101'

    env.assertEqual(conn.execute_command('HSET', 'key1', 'bin', bin_data1), 1)
    env.assertEqual(conn.execute_command('HSET', 'key2', 'bin', bin_data2), 1)

    # Compare results with and without param - data1
    res1 = env.execute_command('FT.SEARCH', 'idx', b'@bin:' + bin_data2, 'NOCONTENT')
    env.assertEqual(res1, [1, 'key2'])
    res2 = env.execute_command('FT.SEARCH', 'idx', '@bin:$val', 'NOCONTENT', 'PARAMS', '2', 'val', '10010101001010101100101011001101010101')
    env.assertEqual(res2, res1)

    # Compare results with and without param - data2
    res1 = env.execute_command('FT.SEARCH', 'idx', b'@bin:' + bin_data1, 'NOCONTENT')
    env.assertEqual(res1, [1, 'key1'])
    res2 = env.execute_command('FT.SEARCH', 'idx', '@bin:$val', 'NOCONTENT', 'PARAMS', '2', 'val', bin_data1)
    env.assertEqual(res2, res1)

    # Compare results with and without param using Prefix - data1
    res1 = env.execute_command('FT.SEARCH', 'idx', '@bin:10010*', 'NOCONTENT')
    env.assertEqual(res1, [1, 'key2'])

    res2 = env.execute_command('FT.SEARCH', 'idx', '@bin:$val*', 'NOCONTENT', 'PARAMS', '2', 'val', '10010')
    env.assertEqual(res2, res1)

    # Compare results with and without param using Prefix - data2
    res1 = env.execute_command('FT.SEARCH', 'idx', b'@bin:\xd7\x93\xd7\x90*', 'NOCONTENT')
    env.assertEqual(res1, [1, 'key1'])

    res2 = env.execute_command('FT.SEARCH', 'idx', '@bin:$val*', 'NOCONTENT', 'PARAMS', '2', 'val', b'\xd7\x93\xd7\x90')
    env.assertEqual(res2, res1)


def test_expression(env):
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT', 'id', 'NUMERIC').ok()
    waitForIndex(env, 'idx')

    env.assertEqual(conn.execute_command('HSET', 'key1', 'name', 'Bob', 'id', '17'), 2)
    env.assertEqual(conn.execute_command('HSET', 'key2', 'name', 'Alice', 'id', '31'), 2)
    env.assertEqual(conn.execute_command('HSET', 'key3', 'name', 'Carol', 'id', '13'), 2)
    env.assertEqual(conn.execute_command('HSET', 'key4', 'name', 'John\\ Doe', 'id', '0'), 2)
    env.assertEqual(conn.execute_command('HSET', 'key5', 'name', '$val1', 'id', '99'), 2)
    env.assertEqual(conn.execute_command('HSET', 'key6', 'name', 'John Doh', 'id', '100'), 2)
    env.assertEqual(conn.execute_command('HSET', 'key7', 'name', 'John', 'id', '100'), 2)

    # Test expression
    res1 = env.execute_command('FT.SEARCH', 'idx', '@name:(Alice|Bob)', 'NOCONTENT')
    env.assertEqual(res1, [2, 'key2', 'key1'])
    res2 = env.execute_command('FT.SEARCH', 'idx', '@name:($val1|Bob)', 'NOCONTENT', 'PARAMS', '2', 'val1', 'Alice')
    env.assertEqual(res2, res1)
    res2 = env.execute_command('FT.SEARCH', 'idx', '@name:(Alice|$val1)', 'NOCONTENT', 'PARAMS', '2', 'val1', 'Bob')
    env.assertEqual(res2, res1)

    res1 = env.execute_command('FT.SEARCH', 'idx', '@name:(Alice)', 'NOCONTENT')
    env.assertEqual(res1, [1, 'key2'])
    res2 = env.execute_command('FT.SEARCH', 'idx', '@name:($val1)', 'NOCONTENT', 'PARAMS', '2', 'val1', 'Alice')
    env.assertEqual(res2, res1)

    res1 = env.execute_command('FT.SEARCH', 'idx', '@name:(John\\ Doe)', 'NOCONTENT')
    env.assertEqual(res1, [1, 'key4'])
    res2 = env.execute_command('FT.SEARCH', 'idx', '@name:($val1)', 'NOCONTENT', 'PARAMS', '2', 'val1', 'John\\ Doe')
    env.assertEqual(res2, res1)

    # Test negative expression
    res1 = env.execute_command('FT.SEARCH', 'idx', '-(@name:(Alice|Bob))', 'NOCONTENT')
    env.assertEqual(res1, [5, 'key3', 'key4', 'key5', 'key6', 'key7'])
    res2 = env.execute_command('FT.SEARCH', 'idx', '-(@name:($val1|Bob))', 'NOCONTENT', 'PARAMS', '2', 'val1', 'Alice')
    env.assertEqual(res2, res1)
    res2 = env.execute_command('FT.SEARCH', 'idx', '-(@name:($val1|Bob))', 'NOCONTENT', 'PARAMS', '2', 'val1', 'Alice')
    env.assertEqual(res2, res1)

    # Test optional token
    res1 = env.execute_command('FT.SEARCH', 'idx', '@name:(John ~Doh)', 'NOCONTENT')
    env.assertEqual(res1, [2, 'key6', 'key7'])
    res2 = env.execute_command('FT.SEARCH', 'idx', '@name:(John ~$val1)', 'NOCONTENT', 'PARAMS', '2', 'val1', 'Doh')
    env.assertEqual(res2, res1)

    # FIXME: Avoid parameterization in verbatim string (whether a param is defined or not)
    #  Parser seems OK
    #  (need to review indexing, in previous versions the following search query was syntactically illegal)
    # res1 = env.execute_command('FT.SEARCH', 'idx', '@name:("$val1")', 'NOCONTENT')
    # env.assertEqual(res1, [1, 'key5'])
    # res2 = env.execute_command('FT.SEARCH', 'idx', '@name:("$val1")', 'NOCONTENT', 'PARAMS', '2', 'val1', 'Alice')
    # env.assertEqual(res2, res1)


def test_tags(env):
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'tags', 'TAG').ok()
    waitForIndex(env, 'idx')

    env.assertEqual(conn.execute_command('HSET', 'key1', 'tags', 't100,t200'), 1)
    env.assertEqual(conn.execute_command('HSET', 'key2', 'tags', 't100,t300'), 1)
    env.assertEqual(conn.execute_command('HSET', 'key3', 'tags', 't200,t300'), 1)
    env.assertEqual(conn.execute_command('HSET', 'key4', 'tags', 't100 t200'), 1)
    env.assertEqual(conn.execute_command('HSET', 'key5', 'tags', 't100 200'), 1)
    env.assertEqual(conn.execute_command('HSET', 'key6', 'tags', '$t100 t300'), 1)
    env.assertEqual(conn.execute_command('HSET', 'key7', 'tags', '$t100,$t200'), 1)

    res1 = env.execute_command('FT.SEARCH', 'idx', '@tags:{t200|t100}', 'NOCONTENT')
    env.assertEqual(res1, [3, 'key1', 'key2', 'key3'])
    res2 = env.execute_command('FT.SEARCH', 'idx', '@tags:{$myT1|$myT2}', 'NOCONTENT', 'PARAMS', '4', 'myT1', 't100', 'myT2', 't200')
    env.assertEqual(res2, res1)

    res1 = env.execute_command('FT.SEARCH', 'idx', '@tags:{t200}', 'NOCONTENT', 'PARAMS', '2', 'myT', 't200')
    env.assertEqual(res1, [2, 'key1', 'key3'])
    res2 = env.execute_command('FT.SEARCH', 'idx', '@tags:{$myT}', 'NOCONTENT', 'PARAMS', '2', 'myT', 't200')
    env.assertEqual(res2, res1)

    res1 = env.execute_command('FT.SEARCH', 'idx', '@tags:{t100 t200}', 'NOCONTENT')
    env.assertEqual(res1, [1, 'key4'])
    res2 = env.execute_command('FT.SEARCH', 'idx', '@tags:{$myT1 $myT2}', 'NOCONTENT', 'PARAMS', '4', 'myT1', 't100', 'myT2', 't200')
    env.assertEqual(res2, res1)

    res1 = env.execute_command('FT.SEARCH', 'idx', '@tags:{t100 200}', 'NOCONTENT')
    env.assertEqual(res1, [1, 'key5'])
    res2 = env.execute_command('FT.SEARCH', 'idx', '@tags:{$myT1 $myT2}', 'NOCONTENT', 'PARAMS', '4', 'myT1', 't100', 'myT2', '200')
    env.assertEqual(res2, res1)

    res2 = env.execute_command('FT.SEARCH', 'idx', '@tags:{$myT1 200}', 'NOCONTENT', 'PARAMS', '4', 'myT1', 't100', 'myT2', '200')
    env.assertEqual(res2, res1)
    res2 = env.execute_command('FT.SEARCH', 'idx', '@tags:{t100 $myT2}', 'NOCONTENT', 'PARAMS', '4', 'myT1', 't100', 'myT2', '200')
    env.assertEqual(res2, res1)

    res1 = env.execute_command('FT.SEARCH', 'idx', '@tags:{\\$t200|t200}', 'NOCONTENT')
    env.assertEqual(res1, [3, 'key1', 'key3', 'key7'])
    res2 = env.execute_command('FT.SEARCH', 'idx', '@tags:{\\$t200|$t100}', 'NOCONTENT', 'PARAMS', '2', 't100', 't200')
    env.assertEqual(res2, res1)


def test_numeric_range(env):
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'numval', 'NUMERIC').ok()
    waitForIndex(env, 'idx')

    env.assertEqual(conn.execute_command('HSET', 'key1', 'numval', '101'), 1)
    env.assertEqual(conn.execute_command('HSET', 'key2', 'numval', '102'), 1)
    env.assertEqual(conn.execute_command('HSET', 'key3', 'numval', '103'), 1)
    env.assertEqual(conn.execute_command('HSET', 'key4', 'numval', '104'), 1)
    env.assertEqual(conn.execute_command('HSET', 'key5', 'numval', '105'), 1)

    res1 = env.execute_command('FT.SEARCH', 'idx', '@numval:[102 104]', 'NOCONTENT')
    env.assertEqual(res1, [3, 'key2', 'key3', 'key4'])
    res2 = env.execute_command('FT.SEARCH', 'idx', '@numval:[$min $max]', 'NOCONTENT', 'PARAMS', '4', 'min', '102', 'max', '104')
    env.assertEqual(res2, res1)

    res1 = env.execute_command('FT.SEARCH', 'idx', '@numval:[(102 104]', 'NOCONTENT')
    env.assertEqual(res1, [2, 'key3', 'key4'])
    res2 = env.execute_command('FT.SEARCH', 'idx', '@numval:[($min $max]', 'NOCONTENT', 'PARAMS', '4', 'min', '102', 'max', '104')
    env.assertEqual(res2, res1)

    res1 = env.execute_command('FT.SEARCH', 'idx', '@numval:[102 (104]', 'NOCONTENT')
    env.assertEqual(res1, [2, 'key2', 'key3'])
    res2 = env.execute_command('FT.SEARCH', 'idx', '@numval:[$min ($max]', 'NOCONTENT', 'PARAMS', '4', 'min', '102', 'max', '104')
    env.assertEqual(res2, res1)

    res1 = env.execute_command('FT.SEARCH', 'idx', '@numval:[(102 (104]', 'NOCONTENT')
    env.assertEqual(res1, [1, 'key3'])
    res2 = env.execute_command('FT.SEARCH', 'idx', '@numval:[($min ($max]', 'NOCONTENT', 'PARAMS', '4', 'min', '102', 'max', '104')
    env.assertEqual(res2, res1)

    res1 = env.execute_command('FT.SEARCH', 'idx', '@numval:[(102 +inf]', 'NOCONTENT')
    env.assertEqual(res1, [3, 'key3', 'key4', 'key5'])
    res2 = env.execute_command('FT.SEARCH', 'idx', '@numval:[($min $max]', 'NOCONTENT', 'PARAMS', '4', 'min', '102', 'max', '+inf')
    env.assertEqual(res2, res1)

    res1 = env.execute_command('FT.SEARCH', 'idx', '@numval:[-inf, (105]', 'NOCONTENT')
    env.assertEqual(res1, [4, 'key1', 'key2', 'key3', 'key4'])
    res2 = env.execute_command('FT.SEARCH', 'idx', '@numval:[$min ($max]', 'NOCONTENT', 'PARAMS', '4', 'min', '-inf', 'max', '105')
    env.assertEqual(res2, res1)


def test_vector(env):
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    args = ['SORTBY', '__v_score', 'ASC', 'RETURN', 1, '__v_score', 'LIMIT', 0, 2]

    env.expect('FT.CREATE idx SCHEMA v VECTOR HNSW 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2 t TEXT').ok()
    waitForIndex(env, 'idx')

    conn.execute_command('HSET', 'b', 'v', 'aaaabaaa', 't', 'title')
    conn.execute_command('HSET', 'c', 'v', 'aaaaabaa', 't', 'title')
    conn.execute_command('HSET', 'd', 'v', 'aaaaaaba', 't', 'title')
    conn.execute_command('HSET', 'a', 'v', 'aaaaaaaa', 't', 'title')

    res1 = ['a', ['__v_score', '0'], 'b', ['__v_score', '3.09485009821e+26']]
    res2 = env.execute_command('FT.SEARCH', 'idx', '*=>[KNN 2 @v $vec]', 'PARAMS', '2', 'vec', 'aaaaaaaa', *args)
    env.assertEqual(res2[1:], res1)
    res2 = env.execute_command('FT.SEARCH', 'idx', '*=>[KNN $k @v $vec]', 'PARAMS', '4', 'vec', 'aaaaaaaa', 'k', '2', *args)
    env.assertEqual(res2[1:], res1)
    res2 = env.execute_command('FT.SEARCH', 'idx', '*=>[KNN 2 @v $vec AS __v_score]', 'PARAMS', '4', 'vec', 'aaaaaaaa', 'k', '2', *args)
    env.assertEqual(res2[1:], res1)
    res2 = env.execute_command('FT.SEARCH', 'idx', '*=>[KNN 2 @v $vec AS $score]', 'PARAMS', '6', 'vec', 'aaaaaaaa', 'k', '2', 'score', '__v_score', *args)
    env.assertEqual(res2[1:], res1)
    res2 = env.execute_command('FT.SEARCH', 'idx', '*=>[KNN $k @v $vec EF_RUNTIME $runtime]', 'PARAMS', '6', 'vec', 'aaaaaaaa', 'k', '2', 'runtime', '100', *args)
    env.assertEqual(res2[1:], res1)
    res2 = env.execute_command('FT.SEARCH', 'idx', '*=>[KNN $k @v $vec EF_RUNTIME 100]', 'PARAMS', '6', 'vec', 'aaaaaaaa', 'k', '2', 'runtime', '100', *args)
    env.assertEqual(res2[1:], res1)
    res2 = env.execute_command('FT.SEARCH', 'idx', '*=>[KNN $k @v $vec EF_RUNTIME 100]', 'PARAMS', '6', 'vec', 'aaaaaaaa', 'k', '2', 'runtime', '100', *args)
    env.assertEqual(res2[1:], res1)
    res2 = env.execute_command('FT.SEARCH', 'idx', '@t:$text=>[KNN 2 @v $vec EF_RUNTIME 100]', 'PARAMS', '4', 'vec', 'aaaaaaaa', 'text', 'title', *args)
    env.assertEqual(res2[1:], res1)
    res2 = env.execute_command('FT.SEARCH', 'idx', '@t:$text=>{$weight:$w}=>[KNN 2 @v $vec EF_RUNTIME 100]', 'PARAMS', '6', 'vec', 'aaaaaaaa', 'text', 'title', 'w', '2.0', *args)
    env.assertEqual(res2[1:], res1)

    # with query attributes syntax
    res2 = env.execute_command('FT.SEARCH', 'idx', '*=>[KNN 2 @v $vec]=>{$yield_distance_as:$score; $EF_RUNTIME:100;}', 'PARAMS', '4', 'vec', 'aaaaaaaa', 'score', '__v_score', *args)
    env.assertEqual(res2[1:], res1)
    res2 = env.execute_command('FT.SEARCH', 'idx', '*=>[KNN 2 @v $vec]=>{$yield_distance_as:$score; $EF_RUNTIME:$ef;}', 'PARAMS', '6', 'vec', 'aaaaaaaa', 'ef', '100', 'score', '__v_score', *args)
    env.assertEqual(res2[1:], res1)
    res2 = env.execute_command('FT.SEARCH', 'idx', '*=>[KNN 2 @v $vec]=>{$yield_distance_as:__v_score; $EF_RUNTIME:$ef;}', 'PARAMS', '4', 'vec', 'aaaaaaaa', 'ef', '100', *args)
    env.assertEqual(res2[1:], res1)
    res2 = env.execute_command('FT.SEARCH', 'idx', '*=>[KNN 2 @v $vec AS __v_score]=>{$EF_RUNTIME:$ef;}', 'PARAMS', '4', 'vec', 'aaaaaaaa', 'ef', '100', *args)
    env.assertEqual(res2[1:], res1)
    res2 = env.execute_command('FT.SEARCH', 'idx', '*=>[KNN 2 @v $vec EF_RUNTIME $ef]=>{$yield_distance_as:__v_score;}', 'PARAMS', '4', 'vec', 'aaaaaaaa', 'ef', '100', *args)
    env.assertEqual(res2[1:], res1)

def test_fuzzy(env):
    env = Env(moduleArgs = 'DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT', 'prop', 'TEXT').ok()
    waitForIndex(env, 'idx')

    env.assertEqual(conn.execute_command('HSET', 'key1', 'name', 'Fozzie Bear', 'prop', 'Hat'), 2)
    env.assertEqual(conn.execute_command('HSET', 'key2', 'name', 'Beaker', 'prop', 'Fan'), 2)
    env.assertEqual(conn.execute_command('HSET', 'key3', 'name', 'Beard'), 1)
    env.assertEqual(conn.execute_command('HSET', 'key4', 'name', 'Rizzo the Rat', 'prop', 'Mop'), 2)

    res1 = env.execute_command('FT.SEARCH', 'idx', '@name:(%Bear%)')
    res2 = env.execute_command('FT.SEARCH', 'idx', '@name:(%$tok%)', 'PARAMS', 2, 'tok', 'Bear')
    env.assertEqual(res2, res1)

    res1 = env.execute_command('FT.SEARCH', 'idx', '@name:(%%Bear%%)')
    res2 = env.execute_command('FT.SEARCH', 'idx', '@name:(%%$tok%%)', 'PARAMS', 2, 'tok', 'Bear')
    env.assertEqual(res2, res1)

    res1 = env.execute_command('FT.SEARCH', 'idx', '@name:(%%%Fozzi%%%)')
    res2 = env.execute_command('FT.SEARCH', 'idx', '@name:(%%%$tok%%%)', 'PARAMS', 2, 'tok', 'Fozzi')
    env.assertEqual(res2, res1)

    res1 = env.execute_command('FT.SEARCH', 'idx', '%Rat%')
    res2 = env.execute_command('FT.SEARCH', 'idx', '%$tok%', 'PARAMS', 2, 'tok', 'Rat')
    env.assertEqual(res2, res1)

    # Fuzzy stopwords
    res1 = env.execute_command('FT.SEARCH', 'idx', '%not%')
    res2 = env.execute_command('FT.SEARCH', 'idx', '%$tok%', 'PARAMS', 2, 'tok', 'not')
    env.assertEqual(res2, res1)

    res1 = env.execute_command('FT.SEARCH', 'idx', '%%not%%')
    res2 = env.execute_command('FT.SEARCH', 'idx', '%%$tok%%', 'PARAMS', 2, 'tok', 'not')
    env.assertEqual(res2, res1)

    res1 = env.execute_command('FT.SEARCH', 'idx', '%%%their%%%')
    res2 = env.execute_command('FT.SEARCH', 'idx', '%%%$tok%%%', 'PARAMS', 2, 'tok', 'their')
    env.assertEqual(res2, res1)

''' Test aliasing behavior.
# Aliasing guidelines:
    # If the SCHEMA contains `a AS b`, `a` is only used to load values from redis, if required. This field should be
      applied by its name (b). Meaning:
        # `SORTBY a` is not allowed (not in schema),
          `SORTBY b` is OK.
        # if `b` is SORTABLE HASH field, or SORTABLE JSON and `b` is UNF (not normalized),
          and the query uses DIALECT 3 or greater,
          the value will not be loaded from redis but taken from the sorting vector.
        # `RETURN a` always loads `a` from redis, even if `b` is sortable.
          For optimized performance the user should use `RETURN b`
        # `RETURN b as x
                  b as c` will return:
            title = x, with the value of field b
            title = c, with the value of field b
        # `RETURN b as x
                  x as y` is allowed and yields:
            title = x with the value of field b
            title = y with the value of field x
            '''

def aliasing(env, is_sortable, is_sortable_unf):
    env.skipOnCluster()
    conn = getConnectionByEnv(env)

    sortable_param = ['SORTABLE', 'UNF'] if is_sortable_unf else (['SORTABLE'] if is_sortable else [])
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'numval', 'AS', 'numval_name', 'NUMERIC', *sortable_param,
                                              'text', 'AS', 'text_name', 'TEXT',*sortable_param).ok()

    #indexed
    env.assertEqual(conn.execute_command('HSET', 'key1', 'numval', '110'), 1)
    env.assertEqual(conn.execute_command('HSET', 'key2', 'numval', '109'), 1)
    env.assertEqual(conn.execute_command('HSET', 'key5', 'text', 'Meow'), 1)

    # Not part of the schema
    env.assertEqual(conn.execute_command('HSET', 'key3', 'numval_name', '108'), 1)
    env.assertEqual(conn.execute_command('HSET', 'key4', 'x', '107'), 1)

    docs_num = 5

    # `SORTBY numval_name` is allowed, key1 and key2 will be sorted, key5, key3 and key4 order is determined by the order of creation.
    # As no return is specified, returns indexed fields + all the documents' fields.
    res = env.execute_command('FT.SEARCH', 'idx', '*', 'sortby', 'numval_name', 'ASC')
    unsorted_expected = ['key5', ['text', 'Meow'],
                'key3', ['numval_name', '108'],
                'key4', ['x', '107']]

    # First results should be the indexed documents that contains the numeric that determines the sorting order,
    # sorted by their value in ascending order
    env.assertEqual(res[1:5], ['key2', ['numval_name', '109', 'numval', '109'],
                             'key1', ['numval_name', '110', 'numval', '110']])
    # Next, all other documents in the database, no order is guaranteed.
    for val in unsorted_expected:
        env.assertContains(val, res[5::])
    # `SORTBY numval_name` and `RETURN` specific fields with new name. Return only the indexed fields, not loading
    # `numval_name` for key3 because alias names of indexed fields have higher priority.
    # TEXT field should return the original value.

    res = env.execute_command('FT.SEARCH', 'idx', '*', 'sortby', 'numval_name', 'ASC',
                            'RETURN', 8,'numval_name',
                                        'numval_name', 'AS', 'numval_new_name',
                                        'numval_name', 'AS', 'numval_new_name2',
                                        'text_name')
    unsorted_expected = ['key5', ['text_name', 'Meow'],
                        'key3', [],
                        'key4', []]
    # First results should be the indexed documents that contains the numeric that determines the sorting order,
    # sorted by their value in ascending order
    env.assertEqual(res[1:5], ['key2', ['numval_name', '109', 'numval_new_name', '109', 'numval_new_name2', '109'],
                             'key1', ['numval_name', '110', 'numval_new_name', '110', 'numval_new_name2', '110']])
    # Next, all other documents in the database, no order is guaranteed.
    for val in unsorted_expected:
        env.assertContains(val, res[5::])

    # If no `SORTBY', we expect the same results, different order.
    # Because the first RETURN is the original path, the values are taken from redis and not from the
    # index.
    res = env.execute_command('FT.SEARCH', 'idx', '*',
                              'RETURN', 4,'numval',
                                          'numval_name', 'AS', 'numval_new_name')
    env.assertEqual(res, [docs_num, 'key1', ['numval', '110', 'numval_new_name', '110'],
                             'key2', ['numval', '109', 'numval_new_name', '109'],
                             'key3', [],
                             'key4', [],
                             'key5', []])

    # `RETURN b as x
    #         x as y` is allowed and yields: title = x, val = b title = y, val = x
    res = env.execute_command('FT.SEARCH', 'idx', '*',
                              'RETURN', 6,'numval_name','AS', 'x',
                                          'x', 'AS', 'y')
    env.assertEqual(res, [docs_num, 'key1', ['x', '110'],
                             'key2', ['x', '109'],
                             'key3', [],
                             'key4', ['y', '107'],
                             'key5', []])

    # Test order of return - shouldn't change the result.
    res2 = env.execute_command('FT.SEARCH', 'idx', '*',
                              'RETURN', 6,'x', 'AS', 'y',
                                        'numval_name','AS', 'x')
    env.assertEqual(res2, res)

def test_aliasing_sortables(env):
    aliasing(env, is_sortable = True, is_sortable_unf = False)

def test_aliasing_NOTsortables(env):

    aliasing(env, is_sortable = False, is_sortable_unf = False)

def test_aliasing_sortables_UNF(env):
    aliasing(env, is_sortable = True, is_sortable_unf = True)


def unf(env, is_sortable_unf):
    conn = getConnectionByEnv(env)

    sortable_param = ['SORTABLE', 'UNF'] if is_sortable_unf else ['SORTABLE']
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'text', 'AS', 'text_name', 'TEXT',*sortable_param).ok()

    original_value1 = 'Meow'
    original_value2 = 'aMeow'
    hashed_field1 = ['text', original_value1]
    hashed_field2 = ['text', original_value2]
    env.assertEqual(conn.execute_command('HSET', 'key1', *hashed_field1), 1)
    env.assertEqual(conn.execute_command('HSET', 'key2', *hashed_field2), 1)

    def expected_res(is_explicit_return):
        loaded_fields = [hashed_field1, hashed_field2] if not is_explicit_return else [[],[]]
        sort_output_fields = [['text_name', 'Meow'], ['text_name', 'aMeow']] if is_sortable_unf  or is_explicit_return \
            else [['text_name', 'meow'], ['text_name', 'ameow']]
        # Meow < aMeow < meow
        # When we `SORTBY text_name`:
        # if text_name is UNF, the indexed value equals the original and Meow < aMeow
        if is_sortable_unf:
            first = ['key1', [*sort_output_fields[0], *loaded_fields[0]]]
            second = ['key2', [*sort_output_fields[1], *loaded_fields[1]]]
        # Otherwise, the indexed value is formated the original and ameow < meow
        else :
            first = ['key2', [*sort_output_fields[1], *loaded_fields[1]]]
            second = ['key1', [*sort_output_fields[0], *loaded_fields[0]]]

        return [*first, *second]

    # Anyway, the original value is returned.
    res = env.execute_command('FT.SEARCH', 'idx', '*', 'sortby', 'text_name', 'ASC',
                              'RETURN', 1,'text_name')
    env.assertEqual(res, [2, *expected_res(True)])

    # Printing both sortby values and loaded values.
    res = env.execute_command('FT.SEARCH', 'idx', '*', 'sortby', 'text_name', 'ASC')
    env.assertEqual(res, [2, *expected_res(False)])

def test_sortable_unf(env):
    unf(env, is_sortable_unf=True)

def test_sortable_NOunf(env):
    unf(env, is_sortable_unf=False)
