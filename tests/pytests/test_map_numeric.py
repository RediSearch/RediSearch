# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Flow tests for the schema of MAP NUMERIC fields."""

from common import *

GATE_ERROR = 'ENABLE_UNSTABLE_FEATURES'


def set_unstable_features(env, on):
    run_command_on_all_shards(env, 'CONFIG', 'SET', 'search-enable-unstable-features',
                              'yes' if on else 'no')


def create_index(env, *extra):
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'PREFIX', 1, 'company:', 'SCHEMA',
               '$.hq', 'AS', 'hq', 'TAG',
               '$.metrics', 'AS', 'metrics', 'MAP', 'NUMERIC', *extra).ok()


def metrics_attribute(env):
    attrs = [to_dict(a) for a in index_info(env)['attributes']]
    return [a for a in attrs if a['attribute'] == 'metrics'][0]


def metrics_field_stats(env):
    stats = [to_dict(s) for s in index_info(env)['field statistics']]
    return [s for s in stats if s['attribute'] == 'metrics'][0]


def testCreateAndInfo(env):
    enable_unstable_features(env)
    create_index(env)
    env.assertEqual(metrics_attribute(env)['type'], 'MAP NUMERIC')
    stats = metrics_field_stats(env)
    env.assertEqual(stats['num_keys'], 0)
    env.assertEqual(stats['memory'], 0)


def testFieldNeighboursStillParse(env):
    """A MAP NUMERIC field may be followed by other fields."""
    enable_unstable_features(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
               '$.a', 'AS', 'a', 'MAP', 'NUMERIC',
               '$.b', 'AS', 'b', 'NUMERIC', 'SORTABLE',
               '$.c', 'AS', 'c', 'MAP', 'NUMERIC').ok()
    types = [to_dict(a)['type'] for a in index_info(env)['attributes']]
    env.assertEqual(types, ['MAP NUMERIC', 'NUMERIC', 'MAP NUMERIC'])


def testInvalidArguments(env):
    enable_unstable_features(env)
    base = ['FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.m', 'AS', 'm']
    for opt in ['SORTABLE', 'NOINDEX', 'INDEXMISSING', 'INDEXEMPTY', 'UNF']:
        env.expect(*base, 'MAP', 'NUMERIC', opt).error().contains('do not take options')
    env.expect(*base, 'MAP').error()
    env.expect(*base, 'MAP', 'TAG').error().contains('MAP must be followed by NUMERIC')
    env.expect(*base, 'MAP', 'NUMERIC', 'TAG').error()
    env.expect(*base, 'MAP', 'NUMERIC', 'NUMERIC').error()
    env.expect(*base, 'MAP', 'NUMERIC', 'WEIGHT', 2).error()
    # Nothing was created by the failed attempts.
    env.expect('FT._LIST').equal([])


def testHashIndexRejected(env):
    enable_unstable_features(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'm', 'MAP', 'NUMERIC').error().contains('JSON')
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'm', 'MAP', 'NUMERIC') \
        .error().contains('JSON')


def testGateOffRejectsCreateAndAlter(env):
    set_unstable_features(env, False)
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.m', 'AS', 'm', 'MAP', 'NUMERIC') \
        .error().contains(GATE_ERROR)

    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.t', 'AS', 't', 'TAG').ok()
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', '$.m', 'AS', 'm', 'MAP', 'NUMERIC') \
        .error().contains(GATE_ERROR)

    set_unstable_features(env, True)
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', '$.m', 'AS', 'm', 'MAP', 'NUMERIC').ok()
    env.assertEqual([to_dict(a)['type'] for a in index_info(env)['attributes']],
                    ['TAG', 'MAP NUMERIC'])


def testReloadKeepsField(env):
    enable_unstable_features(env)
    create_index(env)
    env.broadcast('SAVE')
    env.broadcast('DEBUG RELOAD')
    env.assertEqual(metrics_attribute(env)['type'], 'MAP NUMERIC')
    env.assertEqual(metrics_field_stats(env)['num_keys'], 0)


def testReloadWithFlagOff(env):
    """Only creating a field is gated: an existing one still loads."""
    enable_unstable_features(env)
    create_index(env)
    env.broadcast('SAVE')
    set_unstable_features(env, False)
    env.broadcast('DEBUG RELOAD')
    env.assertEqual(metrics_attribute(env)['type'], 'MAP NUMERIC')


def testExistingFieldTypesUnchanged(env):
    """Adding a field type must not shift the other types' reporting."""
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
               '$.a', 'AS', 'a', 'TEXT', '$.b', 'AS', 'b', 'TAG', '$.c', 'AS', 'c', 'NUMERIC',
               '$.d', 'AS', 'd', 'GEO', '$.e', 'AS', 'e', 'GEOSHAPE',
               '$.f', 'AS', 'f', 'VECTOR', 'FLAT', 6, 'TYPE', 'FLOAT32', 'DIM', 2,
               'DISTANCE_METRIC', 'L2').ok()
    types = [to_dict(a)['type'] for a in index_info(env)['attributes']]
    env.assertEqual(types, ['TEXT', 'TAG', 'NUMERIC', 'GEO', 'GEOSHAPE', 'VECTOR'])
