# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Flow tests for NESTED fields: schema, indexing, persistence. Queries come with the read side."""

import json

from common import *

GATE_ERROR = 'NESTED is unavailable when `ENABLE_UNSTABLE_FEATURES` is off'

# `$.items[*] AS items NESTED 3 ... INDEX 3 color size price`.
ITEMS = ['$.items[*]', 'AS', 'items', 'NESTED', 3,
         '$.color', 'AS', 'color', 'TAG',
         '$.size', 'AS', 'size', 'TAG',
         '$.price', 'AS', 'price', 'NUMERIC',
         'INDEX', 3, 'color', 'size', 'price']


def create(env, *nested):
    return env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'PREFIX', 1, 'doc:', 'SCHEMA',
                      *(nested or ITEMS))


def put(conn, key, items):
    conn.execute_command('JSON.SET', key, '$', json.dumps({'items': items}))


def nested_attribute(env):
    attribute = [to_dict(a) for a in index_info(env)['attributes']
                 if to_dict(a)['type'] == 'NESTED']
    return attribute[0]


def errors(env):
    return to_dict(index_info(env)['Index Errors'])


@skip(no_json=True)
def testRequiresUnstableFeatures(env):
    run_command_on_all_shards(env, 'CONFIG', 'SET', 'search-enable-unstable-features', 'no')
    create(env).error().contains(GATE_ERROR)
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.t', 'AS', 't', 'TEXT').ok()
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', *ITEMS).error().contains(GATE_ERROR)


@skip(no_json=True)
def testInfoShowsTheDefinition(env):
    enable_unstable_features(env)
    create(env, '$.items[*]', 'AS', 'items', 'NESTED', 3,
           '$.color', 'AS', 'color', 'TAG', 'CASESENSITIVE', 'MAXCARD', 8,
           '$.year', 'AS', 'year', 'NUMERIC', 'BUCKETS', 3, 2020, 2022, 2024,
           '$.price', 'AS', 'price', 'NUMERIC', 'RANGE', 0, 1000,
           'INDEX', 3, 'color', 'year', 'price').ok()

    attribute = nested_attribute(env)
    env.assertEqual(attribute['identifier'], '$.items[*]')
    env.assertEqual(attribute['attribute'], 'items')
    env.assertEqual(attribute['index'], ['color', 'year', 'price'])
    env.assertEqual(attribute['terminals'], 0)
    color, year, price = [to_dict(s) for s in attribute['sub_fields']]
    env.assertEqual((color['identifier'], color['type'], color['case_sensitive'],
                     color['max_cardinality']), ('$.color', 'TAG', 'true', 8))
    env.assertEqual([float(b) for b in year['buckets']], [2020.0, 2022.0, 2024.0])
    env.assertEqual((float(price['range_min']), float(price['range_max'])), (0.0, 1000.0))


@skip(no_json=True)
def testInvalidDefinitionsAreRejected(env):
    enable_unstable_features(env)

    def sub(path, name, *rest):
        return [path, 'AS', name, *rest]

    cases = [
        ('only JSON indexes', None, 'supported only on JSON indexes'),
        ('a sub-field count of zero',
         ['NESTED', 0, 'INDEX', 0], 'number of sub-fields'),
        ('too many sub-fields',
         ['NESTED', 5], 'number of sub-fields'),
        ('a path not relative to the element',
         ['NESTED', 2, *sub('color', 'color', 'TAG'), *sub('$.p', 'p', 'NUMERIC'),
          'INDEX', 2, 'color', 'p'], 'relative to the element'),
        ('an unsupported sub-field type',
         ['NESTED', 2, *sub('$.c', 'c', 'TEXT'), *sub('$.p', 'p', 'NUMERIC'),
          'INDEX', 2, 'c', 'p'], 'must be TAG or NUMERIC'),
        ('a duplicate sub-field',
         ['NESTED', 2, *sub('$.c', 'c', 'TAG'), *sub('$.d', 'c', 'NUMERIC'),
          'INDEX', 2, 'c', 'c'], 'duplicate sub-field'),
        ('a missing INDEX',
         ['NESTED', 2, *sub('$.c', 'c', 'TAG'), *sub('$.p', 'p', 'NUMERIC')],
         'expected `INDEX'),
        ('an INDEX leaving a sub-field out',
         ['NESTED', 2, *sub('$.c', 'c', 'TAG'), *sub('$.p', 'p', 'NUMERIC'),
          'INDEX', 1, 'p'], 'every sub-field exactly once'),
        ('an INDEX naming an unknown sub-field',
         ['NESTED', 2, *sub('$.c', 'c', 'TAG'), *sub('$.p', 'p', 'NUMERIC'),
          'INDEX', 2, 'c', 'x'], 'every sub-field exactly once'),
        ('a single sub-field',
         ['NESTED', 1, *sub('$.p', 'p', 'NUMERIC'), 'INDEX', 1, 'p'],
         'at least one routing sub-field'),
        ('a NUMERIC routing level without BUCKETS',
         ['NESTED', 2, *sub('$.y', 'y', 'NUMERIC'), *sub('$.c', 'c', 'TAG'),
          'INDEX', 2, 'y', 'c'], 'must declare BUCKETS'),
        ('BUCKETS on the last level',
         ['NESTED', 2, *sub('$.c', 'c', 'TAG'), *sub('$.p', 'p', 'NUMERIC', 'BUCKETS', 2, 0, 1),
          'INDEX', 2, 'c', 'p'], 'not supported yet'),
        ('MAXCARD on the last level',
         ['NESTED', 2, *sub('$.p', 'p', 'NUMERIC', 'BUCKETS', 2, 0, 1),
          *sub('$.c', 'c', 'TAG', 'MAXCARD', 2), 'INDEX', 2, 'p', 'c'],
         'MAXCARD applies to routing'),
        ('RANGE on a routing level',
         ['NESTED', 2, *sub('$.p', 'p', 'NUMERIC', 'BUCKETS', 2, 0, 1, 'RANGE', 0, 1),
          *sub('$.c', 'c', 'TAG'), 'INDEX', 2, 'p', 'c'], 'RANGE applies to the last'),
        ('an inverted RANGE',
         ['NESTED', 2, *sub('$.c', 'c', 'TAG'), *sub('$.p', 'p', 'NUMERIC', 'RANGE', 5, 1),
          'INDEX', 2, 'c', 'p'], 'not greater than the second'),
        ('decreasing bucket boundaries',
         ['NESTED', 2, *sub('$.y', 'y', 'NUMERIC', 'BUCKETS', 2, 5, 1), *sub('$.c', 'c', 'TAG'),
          'INDEX', 2, 'y', 'c'], 'not greater than the previous'),
    ]
    for name, definition, error in cases:
        if definition is None:
            env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'items', *ITEMS[3:]) \
                .error().contains(error)
        else:
            create(env, '$.items[*]', 'AS', 'items', *definition).error().contains(error)
        env.expect('FT._LIST').equal([])


@skip(no_json=True, cluster=True)
def testElementsAreIndexedByPath(env):
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)
    create(env).ok()

    put(conn, 'doc:1', [{'color': 'red', 'size': 'M', 'price': 20},
                        {'color': 'blue', 'size': 'L', 'price': 90}])
    put(conn, 'doc:2', [{'color': 'red', 'size': 'M', 'price': 35}])
    info = index_info(env)
    # (red, M) and (blue, L): two terminals, three elements.
    env.assertEqual(nested_attribute(env)['terminals'], 2)
    env.assertEqual(int(info['num_records']), 3)
    env.assertEqual(int(info['num_docs']), 2)
    env.assertEqual(int(errors(env)['indexing failures']), 0)


@skip(no_json=True, cluster=True)
def testAnArrayPathIndexesEveryElement(env):
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)
    create(env, '$.items', 'AS', 'items', *ITEMS[3:]).ok()
    put(conn, 'doc:1', [{'color': 'red', 'size': 'M', 'price': 1},
                        {'color': 'red', 'size': 'L', 'price': 2}])
    env.assertEqual(nested_attribute(env)['terminals'], 2)


@skip(no_json=True, cluster=True)
def testElementsMissingAnAttributeAreSkipped(env):
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)
    create(env).ok()
    put(conn, 'doc:1', [{'color': 'red', 'size': 'M'},
                        {'color': 'red', 'price': 3},
                        {'color': None, 'size': 'M', 'price': 3},
                        {'color': 'red', 'size': 'M', 'price': 4}])
    env.assertEqual(nested_attribute(env)['terminals'], 1)
    env.assertEqual(int(index_info(env)['num_records']), 1)
    env.assertEqual(int(errors(env)['indexing failures']), 0)


@skip(no_json=True, cluster=True)
def testTagValuesAreCaseFoldedUnlessCaseSensitive(env):
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)
    create(env, '$.items[*]', 'AS', 'items', 'NESTED', 2,
           '$.color', 'AS', 'color', 'TAG',
           '$.material', 'AS', 'material', 'TAG', 'CASESENSITIVE',
           'INDEX', 2, 'color', 'material').ok()
    put(conn, 'doc:1', [{'color': 'Red', 'material': 'Steel'},
                        {'color': 'RED', 'material': 'steel'}])
    # One color after folding, so one terminal; its two materials stay distinct.
    env.assertEqual(nested_attribute(env)['terminals'], 1)
    env.assertEqual(int(index_info(env)['num_records']), 2)


@skip(no_json=True, cluster=True)
def testInvalidValuesRejectTheWholeDocument(env):
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)
    create(env, '$.items[*]', 'AS', 'items', 'NESTED', 2,
           '$.color', 'AS', 'color', 'TAG',
           '$.price', 'AS', 'price', 'NUMERIC', 'RANGE', 0, 100,
           'INDEX', 2, 'color', 'price').ok()
    cases = [
        ('an element that is not an object', [1], 'elements must be objects'),
        ('a TAG sub-field holding a number', [{'color': 5, 'price': 1}], 'must hold a string'),
        ('a NUMERIC sub-field holding a string', [{'color': 'red', 'price': 'x'}],
         'must hold a number'),
        ('a value outside RANGE', [{'color': 'red', 'price': 500}], 'outside its RANGE'),
        ('a sub-field with several values', [{'color': 'red', 'price': [1, 2]}],
         'must hold a number'),
    ]
    for n, (name, items, error) in enumerate(cases, start=1):
        # A valid element first: the whole document is rejected, not just the bad element.
        put(conn, f'doc:{n}', [{'color': 'ok', 'price': 1}, *items])
        env.assertEqual(int(errors(env)['indexing failures']), n, message=name)
        env.assertContains(error, errors(env)['last indexing error'], message=name)
    env.assertEqual(nested_attribute(env)['terminals'], 0)
    env.assertEqual(int(index_info(env)['num_docs']), 0)


@skip(no_json=True, cluster=True)
def testMaxCardinalityCountsEarlierDocuments(env):
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)
    create(env, '$.items[*]', 'AS', 'items', 'NESTED', 2,
           '$.color', 'AS', 'color', 'TAG', 'MAXCARD', 1,
           '$.price', 'AS', 'price', 'NUMERIC',
           'INDEX', 2, 'color', 'price').ok()
    put(conn, 'doc:1', [{'color': 'red', 'price': 1}])
    put(conn, 'doc:2', [{'color': 'blue', 'price': 1}])
    put(conn, 'doc:3', [{'color': 'red', 'price': 2}])
    env.assertEqual(nested_attribute(env)['terminals'], 1)
    env.assertEqual(int(errors(env)['indexing failures']), 1)
    env.assertContains('more than 1 distinct values', errors(env)['last indexing error'])
    env.assertEqual(errors(env)['last indexing error key'], 'doc:2')


@skip(no_json=True)
def testReloadKeepsTheDefinitionAndReindexes(env):
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)
    create(env, '$.items[*]', 'AS', 'items', 'NESTED', 3,
           '$.color', 'AS', 'color', 'TAG', 'CASESENSITIVE', 'MAXCARD', 8,
           '$.year', 'AS', 'year', 'NUMERIC', 'BUCKETS', 3, 2020, 2022, 2024,
           '$.price', 'AS', 'price', 'NUMERIC', 'RANGE', 0, 1000,
           'INDEX', 3, 'color', 'year', 'price').ok()
    for n in range(10):
        put(conn, f'doc:{n}', [{'color': f'c{n % 3}', 'year': 2020 + n % 4, 'price': n}])
    before = nested_attribute(env)

    for _ in env.reloadingIterator():
        waitForIndex(env)
        env.assertEqual(nested_attribute(env), before)


@skip(no_json=True, cluster=True)
def testAlterAddsANestedFieldAndIndexesExistingDocuments(env):
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'PREFIX', 1, 'doc:', 'SCHEMA',
               '$.t', 'AS', 't', 'TEXT').ok()
    put(conn, 'doc:1', [{'color': 'red', 'size': 'M', 'price': 1}])
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', *ITEMS).ok()
    waitForIndex(env)
    env.assertEqual(nested_attribute(env)['terminals'], 1)


@skip(no_json=True, cluster=True)
def testQueriesOnANestedFieldDoNotReadItAsAnotherType(env):
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)
    create(env).ok()
    put(conn, 'doc:1', [{'color': 'red', 'size': 'M', 'price': 1}])
    # DIALECT 2 rejects the field type; DIALECT 1 builds the node and finds nothing.
    env.expect('FT.SEARCH', 'idx', '@items:{red}', 'DIALECT', 2).error() \
        .contains('Expected a TAG field')
    env.expect('FT.SEARCH', 'idx', '@items:{red}', 'DIALECT', 1).equal([0])
    env.expect('FT.SEARCH', 'idx', '@items:[0 10]', 'DIALECT', 1).equal([0])
