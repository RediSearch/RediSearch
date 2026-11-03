# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from common import getConnectionByEnv, set_max_dialect, skip


@skip(cluster=True)
def test_geo_contract_units_and_dialects(env):
    """Inline, parameterized and legacy filters preserve unit conversion and validation."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'g', 'GEO').ok()
    conn = getConnectionByEnv(env)
    for key, point in [('center', '0,0'), ('inside', '0.001,0'), ('outside', '0.1,0')]:
        conn.execute_command('HSET', key, 'g', point)
    units = [('1000', 'm'), ('1', 'KM'),
             (str(1000 / 0.3048), 'ft'), (str(1000 / 1609.34), 'MI')]
    for dialect in range(1, set_max_dialect(env) + 1):
        for radius, unit in units:
            commands = [('@g:[0 0 %s %s]' % (radius, unit),),
                        ('*', 'GEOFILTER', 'g', '0', '0', radius, unit)]
            if dialect >= 2:
                commands.append(('@g:[$lon $lat $radius $unit]', 'PARAMS', 8,
                                 'lon', '0', 'lat', '0', 'radius', radius, 'unit', unit))
            for args in commands:
                result = env.cmd('FT.SEARCH', 'idx', *args, 'NOCONTENT', 'WITHCOUNT',
                                 'DIALECT', dialect)
                env.assertEqual([result[0], *sorted(result[1:])], [2, 'center', 'inside'])
        env.expect('FT.SEARCH', 'idx', '@g:[0 86 1 km]', 'DIALECT', dialect).equal([0])
        for lon, lat, radius, message in [('181', '0', '1', 'lat/lon'),
                                          ('0', '91', '1', 'lat/lon'),
                                          ('0', '0', '0', 'radius'),
                                          ('0', '0', '-1', 'radius')]:
            env.expect('FT.SEARCH', 'idx', '*', 'GEOFILTER', 'g', lon, lat,
                       radius, 'm', 'DIALECT', dialect).error().contains(
                           'Invalid GeoFilter ' + message)


@skip(cluster=True)
def test_geo_contract_legacy_empty_and_errors(env):
    """Only dialect 1 coalesces empty legacy coordinates; parser failures stay distinct."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'g', 'GEO').ok()
    getConnectionByEnv(env).execute_command('HSET', 'center', 'g', '0,0')
    for dialect in range(1, set_max_dialect(env) + 1):
        for lon, lat in [('', '0'), ('0', '')]:
            args = ('GEOFILTER', 'g', lon, lat, '1', 'km', 'DIALECT', dialect)
            if dialect == 1:
                env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT', *args).equal([1, 'center'])
            else:
                env.expect('FT.SEARCH', 'idx', '*', *args).error().contains(
                    'Numeric/Geo filter value/s cannot be empty')
    for slot, message in [(0, '<lon>'), (1, '<lat>'), (2, '<radius>')]:
        values = ['0', '0', '1', 'm']
        values[slot] = 'bad'
        env.expect('FT.SEARCH', 'idx', '*', 'GEOFILTER', 'g', *values).error().contains(message)
    env.expect('FT.SEARCH', 'idx', '*', 'GEOFILTER', 'g', '0', '0', '1',
               'yards').error().contains('Unknown distance unit yards')
    env.expect('FT.SEARCH', 'idx', '*', 'GEOFILTER', 'g', '0', '0', '', 'm',
               'DIALECT', 1).error().contains('Invalid GeoFilter radius')


@skip(cluster=True)
def test_geo_contract_aggregate_overloads_and_rounding(env):
    """All arities, both three-argument branches, and sortable numeric geo values agree."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'g', 'GEO', 'SORTABLE').ok()
    getConnectionByEnv(env).execute_command('HSET', 'point', 'g', '1.23,4.56')
    expressions = ['geodistance("1.23,4.56","1.25,4.5")',
                   'geodistance("1.23,4.56",1.25,4.5)',
                   'geodistance(1.23,4.56,"1.25,4.5")',
                   'geodistance(1.23,4.56,1.25,4.5)',
                   'geodistance(@g,"1.25,4.5")',
                   'geodistance(@g,1.25,4.5)',
                   'geodistance(1.25,4.5,@g)']
    for expression in expressions:
        env.expect('FT.AGGREGATE', 'idx', '*', 'APPLY', expression, 'AS', 'd',
                   'GROUPBY', 1, '@d').equal([1, ['d', '7032.37']])
    env.expect('FT.AGGREGATE', 'idx', '*', 'APPLY', 'geodistance(0,0,0,0)', 'AS', 'd',
               'GROUPBY', 1, '@d').equal([1, ['d', '0']])


@skip(cluster=True)
def test_geo_contract_aggregate_bad_values_and_arity(env):
    """Numeric conversion failures yield NaN, but coordinate strings propagate parse errors."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    getConnectionByEnv(env).execute_command('HSET', 'point', 't', 'present')
    for expression in ['geodistance(0,"bad",0,0)', 'geodistance(0,0,0,"bad")',
                       'geodistance("0,0","bad",0)', 'geodistance(0,"bad","0,0")']:
        env.expect('FT.AGGREGATE', 'idx', '*', 'APPLY', expression, 'AS', 'd',
                   'GROUPBY', 1, '@d').equal([1, ['d', 'nan']])
    for expression in ['geodistance("bad","0,0")', 'geodistance("0,0","bad")',
                       'geodistance("bad",0,0)', 'geodistance(0,0,"bad")']:
        env.expect('FT.AGGREGATE', 'idx', '*', 'APPLY', expression, 'AS', 'd').error().contains(
            'Invalid geo string: missing separator')
    env.expect('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@missing', 'APPLY',
               'geodistance(@missing,"0,0")', 'AS', 'd').error().contains('SEARCH_VALUE_NOT_FOUND')
    for expression in ['geodistance(0)', 'geodistance(0,0,0,0,0)']:
        env.expect('FT.AGGREGATE', 'idx', '*', 'APPLY', expression, 'AS', 'd').error()
