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


def set_unstable_features(env, on):
    """Set the flag, and return a function that puts back the value it had."""
    before = env.cmd('CONFIG', 'GET', 'search-enable-unstable-features')[1]
    run_command_on_all_shards(env, 'CONFIG', 'SET', 'search-enable-unstable-features',
                              'yes' if on else 'no')
    return lambda: run_command_on_all_shards(env, 'CONFIG', 'SET',
                                             'search-enable-unstable-features', before)

# The obfuscated name of an index named `idx`, as `_FT.DEBUG INFO` takes it.
OBFUSCATED_IDX = 'Index@4e7f626df794f6491574a236f22c100c34ed804f'


@skip(no_json=True, cluster=True)
def testRequiresUnstableFeatures(env):
    restore = set_unstable_features(env, False)
    try:
        create(env).error().contains(GATE_ERROR)
        env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.t', 'AS', 't', 'TEXT').ok()
        env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', *ITEMS).error().contains(GATE_ERROR)
    finally:
        restore()


@skip(cluster=True)
def testAFieldNamedNestedKeepsItsMeaningWithTheFlagOff(env):
    # `NOSTEM` stays an option of `title`, and `nested` is a TEXT field, as before NESTED existed.
    restore = set_unstable_features(env, False)
    try:
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT', 'NOSTEM', 'nested', 'TEXT').ok()
        # A NOSTEM attribute ends in a bare flag, so read the name by position, not with to_dict.
        names = [a[a.index('attribute') + 1] for a in index_info(env)['attributes']]
        env.assertEqual(names, ['title', 'nested'])
    finally:
        restore()


@skip(no_json=True, cluster=True)
def testNestedEndsTheOptionsOfThePreviousFieldWithTheFlagOn(env):
    enable_unstable_features(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.t', 'AS', 't', 'TEXT', 'NOSTEM',
               '$.items[*]', 'NESTED', 2, '$.c', 'AS', 'c', 'TAG', '$.p', 'AS', 'p', 'NUMERIC',
               'INDEX', 2, 'c', 'p').ok()
    types = [a[a.index('type') + 1] for a in index_info(env)['attributes']]
    env.assertEqual(types, ['TEXT', 'NESTED'])


@skip(no_json=True, cluster=True)
def testInfoShowsTheDefinition(env):
    enable_unstable_features(env)
    create(env, '$.items[*]', 'AS', 'items', 'NESTED', 3,
           '$.color', 'AS', 'color', 'TAG', 'CASESENSITIVE',
           '$.year', 'AS', 'year', 'NUMERIC', 'BUCKETS', 3, 2020, 2022, 2024,
           '$.price', 'AS', 'price', 'NUMERIC', 'RANGE', 0, 1000,
           'INDEX', 3, 'color', 'year', 'price').ok()

    attribute = nested_attribute(env)
    env.assertEqual(attribute['identifier'], '$.items[*]')
    env.assertEqual(attribute['attribute'], 'items')
    env.assertEqual(attribute['index'], ['color', 'year', 'price'])
    env.assertEqual(attribute['terminals'], 0)
    color, year, price = [to_dict(s) for s in attribute['sub_fields']]
    env.assertEqual((color['identifier'], color['type'], color['case_sensitive']),
                    ('$.color', 'TAG', 'true'))
    env.assertEqual([float(b) for b in year['buckets']], [2020.0, 2022.0, 2024.0])
    env.assertEqual((float(price['range_min']), float(price['range_max'])), (0.0, 1000.0))


@skip(no_json=True, cluster=True)
def testInvalidDefinitionsAreRejected(env):
    enable_unstable_features(env)

    def sub(path, name, *rest):
        return [path, 'AS', name, *rest]

    cases = [
        ('only JSON indexes', None, 'supported only on JSON indexes'),
        # Both fail before the first sub-field has a name, which once freed a garbage pointer.
        ('a first sub-field without AS',
         ['NESTED', 2, '$.a'], 'expected `<path> AS <name>`'),
        ('a sub-field count with nothing after it',
         ['NESTED', 1], 'expected `<path> AS <name>`'),
        ('an empty sub-field name',
         ['NESTED', 2, *sub('$.c', '', 'TAG'), *sub('$.p', 'p', 'NUMERIC'),
          'INDEX', 2, 'c', 'p'], 'both non-empty'),
        ('BUCKETS given twice',
         ['NESTED', 2, *sub('$.y', 'y', 'NUMERIC', 'BUCKETS', 2, 0, 1, 'BUCKETS', 2, 0, 1),
          *sub('$.c', 'c', 'TAG'), 'INDEX', 2, 'y', 'c'], 'BUCKETS given twice'),
        ('more buckets than allowed',
         ['NESTED', 2, *sub('$.y', 'y', 'NUMERIC', 'BUCKETS', 1026, *range(1026)),
          *sub('$.c', 'c', 'TAG'), 'INDEX', 2, 'y', 'c'], 'at most 1024 buckets'),
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
          'INDEX', 2, 'c', 'x'], 'unknown sub-field'),
        ('a single sub-field',
         ['NESTED', 1, *sub('$.p', 'p', 'NUMERIC'), 'INDEX', 1, 'p'],
         'at least one routing sub-field'),
        ('a NUMERIC routing level without BUCKETS',
         ['NESTED', 2, *sub('$.y', 'y', 'NUMERIC'), *sub('$.c', 'c', 'TAG'),
          'INDEX', 2, 'y', 'c'], 'must declare BUCKETS'),
        ('BUCKETS on the last level',
         ['NESTED', 2, *sub('$.c', 'c', 'TAG'), *sub('$.p', 'p', 'NUMERIC', 'BUCKETS', 2, 0, 1),
          'INDEX', 2, 'c', 'p'], 'not supported yet'),
        ('MAXCARD, until garbage collection frees stale values',
         ['NESTED', 2, *sub('$.c', 'c', 'TAG', 'MAXCARD', 2), *sub('$.p', 'p', 'NUMERIC'),
          'INDEX', 2, 'c', 'p'], 'MAXCARD (on `c`) is not supported yet'),
        ('RANGE on a routing level',
         ['NESTED', 2, *sub('$.p', 'p', 'NUMERIC', 'BUCKETS', 2, 0, 1, 'RANGE', 0, 1),
          *sub('$.c', 'c', 'TAG'), 'INDEX', 2, 'p', 'c'], 'RANGE applies to the last'),
        ('an inverted RANGE',
         ['NESTED', 2, *sub('$.c', 'c', 'TAG'), *sub('$.p', 'p', 'NUMERIC', 'RANGE', 5, 1),
          'INDEX', 2, 'c', 'p'], 'the first not greater than the second'),
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
                        None,
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
def testBooleanTagValuesAreIndexedAsText(env):
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)
    create(env, '$.items[*]', 'AS', 'items', 'NESTED', 2,
           '$.in_stock', 'AS', 'in_stock', 'TAG',
           '$.price', 'AS', 'price', 'NUMERIC',
           'INDEX', 2, 'in_stock', 'price').ok()
    put(conn, 'doc:1', [{'in_stock': True, 'price': 1}, {'in_stock': False, 'price': 2},
                        {'in_stock': 'true', 'price': 3}])
    # true and "true" are one value: two terminals.
    env.assertEqual(nested_attribute(env)['terminals'], 2)
    env.assertEqual(int(errors(env)['indexing failures']), 0)


@skip(no_json=True, cluster=True)
def testIndexingAddsToTheInvertedSize(env):
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)
    create(env).ok()
    before = float(index_info(env)['inverted_sz_mb'])
    for n in range(200):
        put(conn, f'doc:{n}', [{'color': 'red', 'size': 'M', 'price': n}])
    env.assertGreater(float(index_info(env)['inverted_sz_mb']), before)


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
        ('a NUMERIC sub-field holding an array', [{'color': 'red', 'price': [1, 2]}],
         'must hold a number'),
        ('a NUL byte in a routing tag', [{'color': 'r\u0000d', 'price': 1}],
         'must not contain a NUL byte'),
    ]
    for n, (name, items, error) in enumerate(cases, start=1):
        # A valid element first: the whole document is rejected, not just the bad element.
        put(conn, f'doc:{n}', [{'color': 'ok', 'price': 1}, *items])
        env.assertEqual(int(errors(env)['indexing failures']), n, message=name)
        env.assertContains(error, errors(env)['last indexing error'], message=name)
    env.assertEqual(nested_attribute(env)['terminals'], 0)
    env.assertEqual(int(index_info(env)['num_docs']), 0)


@skip(no_json=True, cluster=True)
def testASubFieldWithSeveralValuesRejectsTheDocument(env):
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)
    create(env, '$.items[*]', 'AS', 'items', 'NESTED', 2,
           '$.tags[*]', 'AS', 'tag', 'TAG',
           '$.price', 'AS', 'price', 'NUMERIC',
           'INDEX', 2, 'tag', 'price').ok()
    put(conn, 'doc:1', [{'tags': ['a', 'b'], 'price': 1}])
    env.assertContains('has several values in one element', errors(env)['last indexing error'])
    env.assertEqual(nested_attribute(env)['terminals'], 0)


@skip(no_json=True, cluster=True)
def testAValueOutsideTheRoutingBucketsRejectsTheDocument(env):
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'PREFIX', 1, 'doc:', 'SCHEMA',
               '$.t', 'AS', 't', 'TEXT',
               '$.items[*]', 'AS', 'items', 'NESTED', 2,
               '$.year', 'AS', 'year', 'NUMERIC', 'BUCKETS', 3, 2020, 2022, 2024,
               '$.price', 'AS', 'price', 'NUMERIC',
               'INDEX', 2, 'year', 'price').ok()
    conn.execute_command('JSON.SET', 'doc:1', '$', json.dumps(
        {'t': 'hello', 'items': [{'year': 2021, 'price': 1}, {'year': 2030, 'price': 2}]}))
    env.assertContains('outside the level', errors(env)['last indexing error'])
    env.assertEqual(nested_attribute(env)['terminals'], 0)
    # Rejected whole: its TEXT field is not indexed either.
    env.assertEqual(int(index_info(env)['num_docs']), 0)
    env.expect('FT.SEARCH', 'idx', 'hello', 'NOCONTENT').equal([0])


# `terminals` is a per-shard count, so the comparison below holds on one shard only.
@skip(no_json=True, cluster=True)
def testReloadKeepsTheDefinitionAndReindexes(env):
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)
    create(env, '$.items[*]', 'AS', 'items', 'NESTED', 3,
           '$.color', 'AS', 'color', 'TAG', 'CASESENSITIVE',
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
    # DIALECT 2 rejects the field type; DIALECT 1 builds the node and finds nothing. A DIALECT 1
    # tag query on a non-TAG field is fixed for every field type in #12455.
    env.expect('FT.SEARCH', 'idx', '@items:{red}', 'DIALECT', 2).error() \
        .contains('Expected a TAG field')
    env.expect('FT.SEARCH', 'idx', '@items:[0 10]', 'DIALECT', 1).equal([0])


@skip(no_json=True, cluster=True)
def testObfuscatedInfoHidesSubFieldNames(env):
    enable_unstable_features(env)
    create(env).ok()
    info = to_dict(env.cmd(debug_cmd(), 'INFO', OBFUSCATED_IDX)[0])
    nested = [to_dict(a) for a in info['attributes'] if to_dict(a)['type'] == 'NESTED'][0]
    sub_fields = [to_dict(s) for s in nested['sub_fields']]
    # Named by the field's position and the sub-field's, never like a top-level `Field@<n>`.
    env.assertEqual([s['attribute'] for s in sub_fields], ['Field@0.0', 'Field@0.1', 'Field@0.2'])
    env.assertEqual([s['identifier'] for s in sub_fields],
                    ['FieldPath@0.0', 'FieldPath@0.1', 'FieldPath@0.2'])
    env.assertEqual(nested['index'], ['Field@0.0', 'Field@0.1', 'Field@0.2'])


@skip(no_json=True, cluster=True)
def testSubFieldNamesAreEscapedInInfo(env):
    enable_unstable_features(env)
    create(env, '$.items[*]', 'AS', 'items', 'NESTED', 2,
           '$.c', 'AS', 'a\r\nb', 'TAG', '$.p', 'AS', 'p', 'NUMERIC',
           'INDEX', 2, 'a\r\nb', 'p').ok()
    # The reply still parses, and the name does not inject a line of its own.
    names = [to_dict(s)['attribute'] for s in nested_attribute(env)['sub_fields']]
    env.assertEqual(len(names), 2)
    env.assertNotContains('\r\n', names[0])


def schema_fingerprint(env, idx='idx'):
    con = env.getConnection()
    con.execute_command('DEBUG', 'MARK-INTERNAL-CLIENT')
    return dict(con.execute_command('_FT._LIST', 'WITHCLUSTERSTATE')[3])[idx]


@skip(no_json=True, cluster=True)
def testSchemaFingerprintCoversTheNestedDefinition(env):
    enable_unstable_features(env)
    variants = [
        ITEMS,
        # Another INDEX order.
        ITEMS[:-3] + ['size', 'color', 'price'],
        # Another sub-field path.
        ITEMS[:5] + ['$.colour'] + ITEMS[6:],
        # An option on a sub-field.
        ITEMS[:17] + ['RANGE', 0, 10] + ITEMS[17:],
    ]
    fingerprints = set()
    for variant in variants:
        create(env, *variant).ok()
        fingerprints.add(schema_fingerprint(env))
        env.expect('FT.DROPINDEX', 'idx').ok()
    env.assertEqual(len(fingerprints), len(variants))


@skip(no_json=True, cluster=True)
def testLoadRejectsAnInvalidDefinition(env):
    enable_unstable_features(env)
    # `_FT._RESTOREIFNX SCHEMA` runs the RDB loader on a payload, for internal clients only.
    env.cmd('DEBUG', 'MARK-INTERNAL-CLIENT')
    # Two-letter names keep every string short and its bytes easy to find in the dump.
    create(env, '$.items[*]', 'AS', 'items', 'NESTED', 2,
           '$.qa', 'AS', 'qa', 'TAG', '$.qb', 'AS', 'qb', 'NUMERIC', 'INDEX', 2, 'qa', 'qb').ok()
    dump, encode = env.cmd(debug_cmd(), 'DUMP_SCHEMA', 'idx', NEVER_DECODE=True)
    env.expect('FT.DROPINDEX', 'idx').ok()

    def restore(payload):
        return env.expect('_FT._RESTOREIFNX', 'SCHEMA', encode, payload)

    # Module values are written as an opcode, 2 for an unsigned and 4 for a double, then the
    # value. After `qb`'s path: its type, CASESENSITIVE, MAXCARD, bucket count and RANGE flag, two
    # RANGE doubles, then the level count and the levels.
    UINT = b'\x02'
    qb_path = dump.index(b'$.qb\x00')
    levels = qb_path + len(b'$.qb\x00') + 5 * 2 + 2 * 9
    env.assertEqual(dump[levels:levels + 6], UINT + b'\x02' + UINT + b'\x00' + UINT + b'\x01')

    def replace(old, new, at=None):
        at = dump.index(old) if at is None else at
        env.assertEqual(len(old), len(new))
        return dump[:at] + new + dump[at + len(old):]

    corruptions = [
        ('a NUL inside a path', replace(b'$.qa\x00', b'$\x00qa\x00')),
        ('a duplicate name', replace(b'\x03qb\x00', b'\x03qa\x00')),
        ('a sub-field type other than TAG or NUMERIC',
         replace(b'$.qb\x00' + UINT + b'\x02', b'$.qb\x00' + UINT + b'\x04')),
        ('MAXCARD', replace(b'$.qa\x00' + UINT + b'\x08' + UINT + b'\x00' + UINT + b'\x00',
                            b'$.qa\x00' + UINT + b'\x08' + UINT + b'\x00' + UINT + b'\x05')),
        ('a level listed twice', replace(UINT + b'\x01', UINT + b'\x00', at=levels + 4)),
    ]
    for name, payload in corruptions:
        restore(payload).error().contains('Failed to deserialize schema')
        env.assertEqual(env.cmd('FT._LIST'), [], message=name)
    # Every proper prefix fails too, wherever the cut falls in the definition.
    for cut in range(len(dump)):
        restore(dump[:cut]).error().contains('Failed to deserialize schema')
    env.assertEqual(env.cmd('FT._LIST'), [])

    # The untouched dump restores the definition.
    restore(dump).ok()
    env.assertEqual(nested_attribute(env)['index'], ['qa', 'qb'])
