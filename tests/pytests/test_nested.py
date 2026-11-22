# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Flow tests for NESTED fields: schema, indexing, persistence. Queries come with the read side."""

import functools
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

def unstable_features(test):
    """Run `test` with the flag on, then put back the value it had: the tests share a server."""
    @functools.wraps(test)
    def run(env):
        restore = set_unstable_features(env, True)
        try:
            test(env)
        finally:
            restore()
    return run


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
@unstable_features
def testNestedEndsTheOptionsOfThePreviousFieldWithTheFlagOn(env):
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.t', 'AS', 't', 'TEXT', 'NOSTEM',
               '$.items[*]', 'NESTED', 2, '$.c', 'AS', 'c', 'TAG', '$.p', 'AS', 'p', 'NUMERIC',
               'INDEX', 2, 'c', 'p').ok()
    types = [a[a.index('type') + 1] for a in index_info(env)['attributes']]
    env.assertEqual(types, ['TEXT', 'NESTED'])


@skip(no_json=True, cluster=True)
@unstable_features
def testInfoShowsTheDefinition(env):
    create(env, '$.items[*]', 'AS', 'items', 'NESTED', 3,
           '$.color', 'AS', 'color', 'TAG', 'CASESENSITIVE',
           '$.size', 'AS', 'size', 'TAG',
           '$.price', 'AS', 'price', 'NUMERIC', 'RANGE', 0, 1000,
           'INDEX', 3, 'color', 'size', 'price').ok()

    attribute = nested_attribute(env)
    env.assertEqual(attribute['identifier'], '$.items[*]')
    env.assertEqual(attribute['attribute'], 'items')
    env.assertEqual(attribute['index'], ['color', 'size', 'price'])
    env.assertEqual(attribute['terminals'], 0)
    color, size, price = [to_dict(s) for s in attribute['sub_fields']]
    env.assertEqual((color['identifier'], color['type'], color['case_sensitive']),
                    ('$.color', 'TAG', 'true'))
    env.assertEqual((size['type'], 'case_sensitive' in size), ('TAG', False))
    env.assertEqual((float(price['range_min']), float(price['range_max'])), (0.0, 1000.0))


@skip(no_json=True, cluster=True)
@unstable_features
def testInvalidDefinitionsAreRejected(env):

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
        ('a NUMERIC routing level',
         ['NESTED', 2, *sub('$.y', 'y', 'NUMERIC'), *sub('$.c', 'c', 'TAG'),
          'INDEX', 2, 'y', 'c'], 'NUMERIC sub-field `y` must be last in INDEX'),
        # Not an option: it reads as the next sub-field's path, which has no `AS`.
        ('BUCKETS',
         ['NESTED', 2, *sub('$.c', 'c', 'TAG'), *sub('$.p', 'p', 'NUMERIC', 'BUCKETS', 2, 0, 1),
          'INDEX', 2, 'c', 'p'], 'expected `INDEX'),
        ('MAXCARD, until garbage collection frees stale values',
         ['NESTED', 2, *sub('$.c', 'c', 'TAG', 'MAXCARD', 2), *sub('$.p', 'p', 'NUMERIC'),
          'INDEX', 2, 'c', 'p'], 'MAXCARD (on `c`) is not supported yet'),
        ('RANGE on a TAG sub-field',
         ['NESTED', 2, *sub('$.c', 'c', 'TAG', 'RANGE', 0, 1), *sub('$.p', 'p', 'NUMERIC'),
          'INDEX', 2, 'c', 'p'], 'expected `<path> AS <name>`'),
        ('an inverted RANGE',
         ['NESTED', 2, *sub('$.c', 'c', 'TAG'), *sub('$.p', 'p', 'NUMERIC', 'RANGE', 5, 1),
          'INDEX', 2, 'c', 'p'], 'the first not greater than the second'),
    ]
    for name, definition, error in cases:
        if definition is None:
            env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'items', *ITEMS[3:]) \
                .error().contains(error)
        else:
            create(env, '$.items[*]', 'AS', 'items', *definition).error().contains(error)
        env.expect('FT._LIST').equal([])


@skip(no_json=True, cluster=True)
@unstable_features
def testElementsAreIndexedByPath(env):
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
@unstable_features
def testAnArrayPathIndexesEveryElement(env):
    conn = getConnectionByEnv(env)
    create(env, '$.items', 'AS', 'items', *ITEMS[3:]).ok()
    put(conn, 'doc:1', [{'color': 'red', 'size': 'M', 'price': 1},
                        {'color': 'red', 'size': 'L', 'price': 2}])
    env.assertEqual(nested_attribute(env)['terminals'], 2)


@skip(no_json=True, cluster=True)
@unstable_features
def testElementsMissingAnAttributeAreSkipped(env):
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
@unstable_features
def testTagValuesAreCaseFoldedUnlessCaseSensitive(env):
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
@unstable_features
def testBooleanTagValuesAreIndexedAsText(env):
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
@unstable_features
def testIndexingAddsToTheInvertedSize(env):
    conn = getConnectionByEnv(env)
    create(env).ok()
    before = float(index_info(env)['inverted_sz_mb'])
    for n in range(200):
        put(conn, f'doc:{n}', [{'color': 'red', 'size': 'M', 'price': n}])
    env.assertGreater(float(index_info(env)['inverted_sz_mb']), before)


@skip(no_json=True, cluster=True)
@unstable_features
def testInvalidValuesRejectTheWholeDocument(env):
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
@unstable_features
def testASubFieldWithSeveralValuesRejectsTheDocument(env):
    conn = getConnectionByEnv(env)
    create(env, '$.items[*]', 'AS', 'items', 'NESTED', 2,
           '$.tags[*]', 'AS', 'tag', 'TAG',
           '$.price', 'AS', 'price', 'NUMERIC',
           'INDEX', 2, 'tag', 'price').ok()
    put(conn, 'doc:1', [{'tags': ['a', 'b'], 'price': 1}])
    env.assertContains('has several values in one element', errors(env)['last indexing error'])
    env.assertEqual(nested_attribute(env)['terminals'], 0)


@skip(no_json=True, cluster=True)
@unstable_features
def testARejectedDocumentHasNoneOfItsFieldsIndexed(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'PREFIX', 1, 'doc:', 'SCHEMA',
               '$.t', 'AS', 't', 'TEXT',
               '$.items[*]', 'AS', 'items', 'NESTED', 2,
               '$.color', 'AS', 'color', 'TAG',
               '$.price', 'AS', 'price', 'NUMERIC', 'RANGE', 0, 100,
               'INDEX', 2, 'color', 'price').ok()
    conn.execute_command('JSON.SET', 'doc:1', '$', json.dumps(
        {'t': 'hello', 'items': [{'color': 'red', 'price': 1}, {'color': 'red', 'price': 500}]}))
    env.assertContains('outside its RANGE', errors(env)['last indexing error'])
    env.assertEqual(nested_attribute(env)['terminals'], 0)
    # Rejected whole: its TEXT field is not indexed either.
    env.assertEqual(int(index_info(env)['num_docs']), 0)
    env.expect('FT.SEARCH', 'idx', 'hello', 'NOCONTENT').equal([0])


# `terminals` is a per-shard count, so the comparison below holds on one shard only.
@skip(no_json=True, cluster=True)
@unstable_features
def testReloadKeepsTheDefinitionAndReindexes(env):
    conn = getConnectionByEnv(env)
    create(env, '$.items[*]', 'AS', 'items', 'NESTED', 3,
           '$.color', 'AS', 'color', 'TAG', 'CASESENSITIVE',
           '$.size', 'AS', 'size', 'TAG',
           '$.price', 'AS', 'price', 'NUMERIC', 'RANGE', 0, 1000,
           'INDEX', 3, 'color', 'size', 'price').ok()
    for n in range(10):
        put(conn, f'doc:{n}', [{'color': f'c{n % 3}', 'size': f's{n % 4}', 'price': n}])
    before = nested_attribute(env)

    for _ in env.reloadingIterator():
        waitForIndex(env)
        env.assertEqual(nested_attribute(env), before)


@skip(no_json=True, cluster=True)
@unstable_features
def testAlterAddsANestedFieldAndIndexesExistingDocuments(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'PREFIX', 1, 'doc:', 'SCHEMA',
               '$.t', 'AS', 't', 'TEXT').ok()
    put(conn, 'doc:1', [{'color': 'red', 'size': 'M', 'price': 1}])
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', *ITEMS).ok()
    waitForIndex(env)
    env.assertEqual(nested_attribute(env)['terminals'], 1)


@skip(no_json=True, cluster=True)
@unstable_features
def testQueriesOnANestedFieldDoNotReadItAsAnotherType(env):
    conn = getConnectionByEnv(env)
    create(env).ok()
    put(conn, 'doc:1', [{'color': 'red', 'size': 'M', 'price': 1}])
    # DIALECT 2 rejects the field type; DIALECT 1 builds the node and finds nothing. A DIALECT 1
    # tag query on a non-TAG field is fixed for every field type in #12455.
    env.expect('FT.SEARCH', 'idx', '@items:{red}', 'DIALECT', 2).error() \
        .contains('Expected a TAG field')
    env.expect('FT.SEARCH', 'idx', '@items:[0 10]', 'DIALECT', 1).equal([0])


@skip(no_json=True, cluster=True)
@unstable_features
def testObfuscatedInfoHidesSubFieldNames(env):
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
@unstable_features
def testSubFieldNamesAreEscapedInInfo(env):
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
@unstable_features
def testSchemaFingerprintCoversTheNestedDefinition(env):
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
@unstable_features
def testLoadRejectsAnInvalidDefinition(env):
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
    # value. After `qb`'s path: its type, CASESENSITIVE, MAXCARD and RANGE flag, two RANGE
    # doubles, then the level count and the levels.
    UINT = b'\x02'
    qb_path = dump.index(b'$.qb\x00')
    levels = qb_path + len(b'$.qb\x00') + 4 * 2 + 2 * 9
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


# Queries: `@items:( … )` matches a document when one of its elements matches the whole group.

def ids(env, query, *args):
    res = env.cmd('FT.SEARCH', 'idx', query, 'NOCONTENT', 'LIMIT', 0, 10000, 'DIALECT', 2, *args)
    return sorted(res[1:])


# Two documents with the same values, paired differently across their elements.
PAIRED = {
    'doc:1': [{'color': 'red', 'size': 'M', 'price': 10}, {'color': 'blue', 'size': 'L', 'price': 50}],
    'doc:2': [{'color': 'red', 'size': 'L', 'price': 50}, {'color': 'blue', 'size': 'M', 'price': 10}],
}


@skip(no_json=True)
@unstable_features
def testAGroupMatchesValuesOfOneElement(env):
    """Only a document with one element holding every value of the group matches."""
    conn = getConnectionByEnv(env)
    create(env).ok()
    for key, items in PAIRED.items():
        put(conn, key, items)
    # Both documents hold a red element and an M element; only doc:1 holds a red M one.
    env.assertEqual(ids(env, '@items:( @color:{red} @size:{M} )'), ['doc:1'])
    env.assertEqual(ids(env, '@items:( @color:{red} @price:[0 20] )'), ['doc:1'])
    env.assertEqual(ids(env, '@items:( @color:{blue} @size:{M} @price:[(5 10] )'), ['doc:2'])
    env.assertEqual(ids(env, '@items:( @color:{green} )'), [])


@skip(no_json=True, cluster=True)
@unstable_features
def testEveryShapeOfAGroup(env):
    """Skipped levels, value lists, prefixes, negations on the element and on the document,
    ORs across sub-fields, merged clauses, and two groups on one field."""
    conn = getConnectionByEnv(env)
    create(env).ok()
    for key, items in PAIRED.items():
        put(conn, key, items)
    put(conn, 'doc:3', [{'color': 'green', 'size': 'S', 'price': 5}])
    cases = [
        # A level skipped: every size under red.
        ('@items:( @color:{red} @price:[40 60] )', ['doc:2']),
        # The first level skipped: every color.
        ('@items:( @size:{M} @price:[0 10] )', ['doc:1', 'doc:2']),
        # Only the terminal: every path.
        ('@items:( @price:[0 6] )', ['doc:3']),
        ('@items:( @price:[-inf +inf] )', ['doc:1', 'doc:2', 'doc:3']),
        # Several values on one level.
        ('@items:( @color:{red|green} @size:{S} )', ['doc:3']),
        # A prefix.
        ('@items:( @color:{gr*} )', ['doc:3']),
        # A negation inside the group is on the element: an element that is not red.
        ('@items:( -@color:{red} @size:{M} )', ['doc:2']),
        ('@items:( -@price:[0 20] @color:{red} )', ['doc:2']),
        # An OR across sub-fields: one element red, or one element S.
        ('@items:( @color:{red} @size:{M} | @size:{S} )', ['doc:1', 'doc:3']),
        # A negation outside the group is on the document: no element is a red M.
        ('-@items:( @color:{red} @size:{M} )', ['doc:2', 'doc:3']),
        # Clauses on one level intersect.
        ('@items:( @color:{red|blue} @color:{blue} @size:{L} )', ['doc:1']),
        ('@items:( @price:[0 50] @price:[(10 +inf] )', ['doc:1', 'doc:2']),
        # Two groups on the same field: each is matched by its own element.
        ('@items:( @color:{red} @size:{M} ) @items:( @color:{blue} @size:{L} )', ['doc:1']),
    ]
    for query, expected in cases:
        env.assertEqual(ids(env, query), expected, message=query)


@skip(no_json=True, cluster=True)
@unstable_features
def testAGroupComposesWithTopLevelFields(env):
    """A group intersects and unions with clauses on other fields, and takes PARAMS."""
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'PREFIX', 1, 'doc:', 'SCHEMA',
               '$.t', 'AS', 't', 'TEXT', *ITEMS).ok()
    for n, (key, items) in enumerate(PAIRED.items()):
        conn.execute_command('JSON.SET', key, '$', json.dumps({'t': f'word{n}', 'items': items}))
    env.assertEqual(ids(env, '@t:word0 @items:( @color:{red} )'), ['doc:1'])
    env.assertEqual(ids(env, '@t:word1 | @items:( @color:{red} @size:{M} )'), ['doc:1', 'doc:2'])
    env.assertEqual(ids(env, '@items:( @size:{$s} @price:[$lo $hi] )', 'PARAMS', 6,
                        's', 'L', 'lo', 40, 'hi', 60), ['doc:1', 'doc:2'])


@skip(no_json=True, cluster=True)
@unstable_features
def testTagValuesInAGroupAreCaseFoldedLikeTheSubField(env):
    """A query value is folded unless its sub-field is CASESENSITIVE, as at indexing."""
    conn = getConnectionByEnv(env)
    create(env, '$.items[*]', 'AS', 'items', 'NESTED', 3,
           '$.color', 'AS', 'color', 'TAG', '$.size', 'AS', 'size', 'TAG', 'CASESENSITIVE',
           '$.price', 'AS', 'price', 'NUMERIC', 'INDEX', 3, 'color', 'size', 'price').ok()
    put(conn, 'doc:1', [{'color': 'Red', 'size': 'Xl', 'price': 1}])
    env.assertEqual(ids(env, '@items:( @color:{RED} @size:{Xl} )'), ['doc:1'])
    env.assertEqual(ids(env, '@items:( @size:{XL} )'), [])


@skip(no_json=True, cluster=True)
@unstable_features
def testATagTerminal(env):
    """With a TAG last in INDEX, values, negations and prefixes apply to the terminal."""
    conn = getConnectionByEnv(env)
    create(env, '$.items[*]', 'AS', 'items', 'NESTED', 2,
           '$.color', 'AS', 'color', 'TAG', '$.material', 'AS', 'material', 'TAG',
           'INDEX', 2, 'color', 'material').ok()
    put(conn, 'doc:1', [{'color': 'red', 'material': 'steel'}, {'color': 'blue', 'material': 'wood'}])
    put(conn, 'doc:2', [{'color': 'red', 'material': 'wood'}, {'color': 'red', 'material': 'stone'}])
    cases = [
        ('@items:( @color:{red} @material:{wood} )', ['doc:2']),
        ('@items:( @material:{steel|stone} )', ['doc:1', 'doc:2']),
        ('@items:( @color:{blue} )', ['doc:1']),
        ('@items:( @color:{red} -@material:{steel} )', ['doc:2']),
        ('@items:( @color:{red} @material:{st*} )', ['doc:1', 'doc:2']),
        ('@items:( @color:{blue} @material:{st*} )', []),
    ]
    for query, expected in cases:
        env.assertEqual(ids(env, query), expected, message=query)


@skip(no_json=True, cluster=True)
@unstable_features
def testABucketLevel(env):
    """A range on a BUCKETS level selects whole buckets, and one cutting a bucket fails."""
    conn = getConnectionByEnv(env)
    create(env, '$.items[*]', 'AS', 'items', 'NESTED', 2,
           '$.year', 'AS', 'year', 'NUMERIC', 'BUCKETS', 3, 2020, 2022, 2024,
           '$.price', 'AS', 'price', 'NUMERIC', 'INDEX', 2, 'year', 'price').ok()
    put(conn, 'doc:1', [{'year': 2020, 'price': 5}, {'year': 2023, 'price': 50}])
    put(conn, 'doc:2', [{'year': 2021, 'price': 50}, {'year': 2023, 'price': 5}])
    # Buckets are half-open: `[2022 (2024]` is exactly the second one.
    env.assertEqual(ids(env, '@items:( @year:[2022 (2024] @price:[0 10] )'), ['doc:2'])
    env.assertEqual(ids(env, '@items:( @year:[2020 (2022] @price:[0 10] )'), ['doc:1'])
    env.assertEqual(ids(env, '@items:( @year:[-inf +inf] @price:[40 60] )'), ['doc:1', 'doc:2'])
    # A range cutting a bucket cannot be answered from the buckets.
    env.expect('FT.SEARCH', 'idx', '@items:( @year:[2021 2023] )', 'DIALECT', 2).error() \
        .contains('must cover whole buckets')


@skip(no_json=True, cluster=True)
@unstable_features
def testInvalidGroupsAreRejected(env):
    """Unknown sub-fields, wrong types, unsupported clauses and limits are query errors."""
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'PREFIX', 1, 'doc:', 'SCHEMA',
               '$.t', 'AS', 't', 'TEXT', *ITEMS).ok()
    cases = [
        ('@items:( @t:hello )', 'Unknown sub-field'),
        ('@items:( @colour:{red} )', 'Unknown sub-field'),
        ('@items:( @price:{red} )', 'Expected a TAG field'),
        ('@items:( @color:[0 1] )', 'Expected a NUMERIC field'),
        ('@items:( @color:hello )', 'not supported on a sub-field of a NESTED group'),
        ('@items:( hello )', 'not supported on a sub-field of a NESTED group'),
        ('@items:( @color:{*ed} )', 'supported inside a NESTED group'),
        ('@items:( -@color:{re*} )', 'prefix cannot be negated'),
        ('@items:( @items:( @color:{red} ) )', 'cannot be nested'),
        # Five ORs of two: 32 alternatives.
        ('@items:( ' + ' '.join('(@color:{%s} | @size:{%s})' % (v, v) for v in 'abcde') + ' )',
         'more than 16 alternatives'),
    ]
    for query, error in cases:
        env.expect('FT.SEARCH', 'idx', query, 'DIALECT', 2).error().contains(error)
    # A NESTED field outside a group keeps the type error of any non-TAG field.
    env.expect('FT.SEARCH', 'idx', '@items:{red}', 'DIALECT', 2).error() \
        .contains('Expected a TAG field')
    # A text group on a TEXT field is unchanged.
    env.expect('FT.SEARCH', 'idx', '@t:(hello world)', 'DIALECT', 2).equal([0])


EXPLAINED_GROUP = """NESTED:@items {
  INTERSECT {
    TAG:@items.color {
      red
    }
    NUMERIC {1.000000 <= @items.price <= 2.000000}
  }
}
"""


@skip(no_json=True, cluster=True)
@unstable_features
def testExplainShowsTheGroup(env):
    """FT.EXPLAIN shows the group and names its sub-fields `@items.<sub-field>`."""
    create(env).ok()
    explain = env.cmd('FT.EXPLAIN', 'idx', '@items:( @color:{red} @price:[1 2] )', 'DIALECT', 2)
    env.assertEqual(explain, EXPLAINED_GROUP)


# A cluster reply holds one profile per shard, each over that shard's documents.
@skip(no_json=True, cluster=True)
@unstable_features
def testProfileShowsTheGroup(env):
    """FT.PROFILE shows the group as a UNION of query type NESTED over the terminals."""
    conn = getConnectionByEnv(env)
    create(env).ok()
    for key, items in PAIRED.items():
        put(conn, key, items)
    env.cmd(config_cmd(), 'SET', '_PRINT_PROFILE_CLOCK', 'false')
    res = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '@items:( @size:{M} )', 'NOCONTENT',
                  'DIALECT', 2)
    # [1] profile -> [1] shards -> [0] the only shard -> the iterators profile, key and value.
    profile = to_dict(res[1][1][0][3])
    env.assertEqual((profile['Type'], profile['Query type']), ('UNION', 'NESTED'))
    # Size M sits under both colors: one terminal each, read by one numeric range each.
    children = [to_dict(child) for child in profile['Child iterators']]
    env.assertEqual([c['Type'] for c in children], ['NUMERIC', 'NUMERIC'])


@skip(no_json=True, cluster=True)
@unstable_features
def testAnUpdatedDocumentMatchesOnlyItsNewElements(env):
    """Replacing or deleting a document drops its old elements from every group."""
    conn = getConnectionByEnv(env)
    create(env).ok()
    put(conn, 'doc:1', [{'color': 'red', 'size': 'M', 'price': 1}])
    put(conn, 'doc:1', [{'color': 'blue', 'size': 'M', 'price': 1}])
    env.assertEqual(ids(env, '@items:( @color:{red} )'), [])
    env.assertEqual(ids(env, '@items:( @color:{blue} )'), ['doc:1'])
    conn.execute_command('DEL', 'doc:1')
    env.assertEqual(ids(env, '@items:( @size:{M} )'), [])


# The group against a brute-force evaluation of the same query, on random documents.

COLORS = ['red', 'blue', 'green']
SIZES = ['S', 'M', 'L']


def random_clause(rng):
    kind = rng.randrange(4)
    if kind == 0:
        values = rng.sample(COLORS, rng.randint(1, 2))
        return '@color:{%s}' % '|'.join(values), lambda e: e['color'] in values
    if kind == 1:
        values = rng.sample(SIZES, rng.randint(1, 2))
        return '@size:{%s}' % '|'.join(values), lambda e: e['size'] in values
    if kind == 2:
        lo, hi = sorted(rng.sample(range(0, 20), 2))
        lo_ex, hi_ex = rng.random() < 0.5, rng.random() < 0.5
        def in_range(e):
            p = e['price']
            return (p > lo if lo_ex else p >= lo) and (p < hi if hi_ex else p <= hi)
        return '@price:[%s%d %s%d]' % ('(' if lo_ex else '', lo, '(' if hi_ex else '', hi), in_range
    # At least `MINPREFIX` (2) long: a shorter prefix matches nothing.
    prefix = rng.choice(['re', 'bl', 'gr', 'blu'])
    return '@color:{%s*}' % prefix, lambda e: e['color'].startswith(prefix)


def random_group(rng, depth=0):
    """An inner expression and the element predicate it stands for."""
    roll = rng.random()
    if depth >= 2 or roll < 0.4:
        return random_clause(rng)
    if roll < 0.7:
        parts = [random_group(rng, depth + 1) for _ in range(rng.randint(2, 3))]
        return ' '.join('(%s)' % q for q, _ in parts), \
            lambda e, parts=parts: all(p(e) for _, p in parts)
    if roll < 0.9:
        parts = [random_group(rng, depth + 1) for _ in range(2)]
        return ' | '.join('(%s)' % q for q, _ in parts), \
            lambda e, parts=parts: any(p(e) for _, p in parts)
    # A NOT never wraps a prefix: a negated prefix is rejected.
    query, pred = random_clause(rng)
    while '*' in query:
        query, pred = random_clause(rng)
    return '-%s' % query, lambda e, pred=pred: not pred(e)


@skip(no_json=True, cluster=True)
@unstable_features
def testGroupsAgreeWithBruteForce(env):
    """Random groups return what a direct evaluation over the documents' elements does."""
    import random
    rng = random.Random(20261121)
    conn = getConnectionByEnv(env)
    create(env).ok()
    docs = {}
    for n in range(60):
        items = [{'color': rng.choice(COLORS), 'size': rng.choice(SIZES),
                  'price': rng.randrange(0, 20)} for _ in range(rng.randint(1, 4))]
        docs['doc:%d' % n] = items
        put(conn, 'doc:%d' % n, items)
    checked = 0
    while checked < 150:
        inner, pred = random_group(rng)
        query = '@items:( %s )' % inner
        try:
            res = env.cmd('FT.SEARCH', 'idx', query, 'NOCONTENT', 'LIMIT', 0, 1000, 'DIALECT', 2)
        except Exception as err:
            # Distributing the ORs may go past the limit, or put a prefix and a negation on one
            # level, which no walk expresses; nothing to compare then.
            if 'alternatives' in str(err) or 'prefix cannot be negated' in str(err):
                continue
            raise
        expected = sorted(k for k, items in docs.items() if any(pred(e) for e in items))
        env.assertEqual(sorted(res[1:]), expected, message=query)
        checked += 1


@skip(no_json=True, cluster=True)
@unstable_features
def testGroupsRequireUnstableFeatures(env):
    """With the flag off, `@items:( … )` is the text group it always was, so it fails to parse."""
    conn = getConnectionByEnv(env)
    create(env).ok()
    put(conn, 'doc:1', [{'color': 'red', 'size': 'M', 'price': 1}])
    restore = set_unstable_features(env, False)
    try:
        env.expect('FT.SEARCH', 'idx', '@items:( @color:{red} )', 'DIALECT', 2).error() \
            .contains('Syntax error')
    finally:
        restore()
    env.assertEqual(ids(env, '@items:( @color:{red} )'), ['doc:1'])


@skip(no_json=True)
@unstable_features
def testAGroupInAKnnQuery(env):
    """The coordinator parses a query mentioning KNN without the schema, yet still reads a group,
    whether the KNN is real or only a value inside the group."""
    import struct
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'PREFIX', 1, 'doc:', 'SCHEMA',
               '$.v', 'AS', 'v', 'VECTOR', 'FLAT', 6, 'TYPE', 'FLOAT32', 'DIM', 2,
               'DISTANCE_METRIC', 'L2', *ITEMS).ok()
    for n, color in enumerate(['red', 'knn', 'red', 'blue']):
        conn.execute_command('JSON.SET', 'doc:%d' % n, '$', json.dumps(
            {'v': [float(n), 0.0], 'items': [{'color': color, 'size': 'M', 'price': n}]}))
    blob = struct.pack('<2f', 0.0, 0.0)
    res = env.cmd('FT.SEARCH', 'idx', '(@items:( @color:{red} ))=>[KNN 10 @v $b]', 'PARAMS', 2,
                  'b', blob, 'NOCONTENT', 'DIALECT', 2)
    env.assertEqual(sorted(res[1:]), ['doc:0', 'doc:2'])
    env.expect('FT.SEARCH', 'idx', '(@items:( @items:( @color:{red} ) ))=>[KNN 10 @v $b]',
               'PARAMS', 2, 'b', blob, 'DIALECT', 2).error().contains('cannot be nested')
    env.assertEqual(ids(env, '@items:( @color:{knn} )'), ['doc:1'])
    res = env.cmd('FT.AGGREGATE', 'idx', '@items:( @color:{knn} )', 'LOAD', 1, '@__key',
                  'DIALECT', 2)
    env.assertEqual(len(res) - 1, 1)


@skip(no_json=True)
@unstable_features
def testACursorSurvivesWritesBetweenReads(env):
    """A suspended group revalidates its numeric and tag readers after documents change under it."""
    conn = getConnectionByEnv(env)
    # A numeric terminal (the default definition) and a tag terminal, each over 'M' elements.
    create(env).ok()
    env.expect('FT.CREATE', 'tags', 'ON', 'JSON', 'PREFIX', 1, 'doc:', 'SCHEMA',
               '$.items[*]', 'AS', 'items', 'NESTED', 2,
               '$.size', 'AS', 'size', 'TAG', '$.color', 'AS', 'color', 'TAG',
               'INDEX', 2, 'size', 'color').ok()
    for n in range(50):
        put(conn, 'doc:%d' % n, [{'color': 'c%d' % (n % 5), 'size': 'M', 'price': n}])
    for index in ['idx', 'tags']:
        res, cursor = env.cmd('FT.AGGREGATE', index, '@items:( @size:{M} )', 'LOAD', 1, '@__key',
                              'WITHCURSOR', 'COUNT', 5, 'DIALECT', 2)
        keys = [row[1] for row in res[1:]]
        reads = 0
        while cursor:
            # Grow the terminals the readers are parked on, and add new ones, past range splits.
            # Only for a few reads: the new documents match, so the cursor would never end.
            if reads < 3:
                for written in range(40):
                    n = 1000 * (reads + 1) + written + (0 if index == 'idx' else 500)
                    put(conn, 'doc:%d' % n, [{'color': 'c%d' % (n % 7), 'size': 'M', 'price': n}])
            reads += 1
            res, cursor = env.cmd('FT.CURSOR', 'READ', index, cursor)
            keys += [row[1] for row in res[1:]]
        env.assertEqual(len(keys), len(set(keys)), message=index)
        env.assertTrue(all(k.startswith('doc:') for k in keys), message=index)
        if index == 'tags':
            # A tag reader re-finds its value after the writes: nothing indexed before the cursor
            # opened is lost.
            env.assertEqual(set('doc:%d' % n for n in range(50)) - set(keys), set())
        # A numeric reader aborts once its range tree changes shape, ending its results early,
        # exactly as on a top-level NUMERIC field.


@skip(no_json=True, cluster=True)
@unstable_features
def testGroupNamesMayBeEscapedOrSpaced(env):
    """An escaped field name opens a group, and spaces may surround its colon."""
    conn = getConnectionByEnv(env)
    create(env, '$.items[*]', 'AS', 'my-items', 'NESTED', 2,
           '$.color', 'AS', 'color', 'TAG', '$.price', 'AS', 'price', 'NUMERIC',
           'INDEX', 2, 'color', 'price').ok()
    conn.execute_command('JSON.SET', 'doc:1', '$', json.dumps(
        {'items': [{'color': 'red', 'price': 1}]}))
    env.assertEqual(ids(env, '@my\\-items:( @color:{red} )'), ['doc:1'])
    env.assertEqual(ids(env, '@my\\-items : ( @price:[0 5] )'), ['doc:1'])


@skip(no_json=True, cluster=True)
@unstable_features
def testUnsupportedClausesInAGroupSayWhy(env):
    """A clause a group cannot answer names the group, not a wrong field type."""
    # A TEXT field makes the query validated, which is where a range with min > max fails.
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'PREFIX', 1, 'doc:', 'SCHEMA',
               '$.t', 'AS', 't', 'TEXT', *ITEMS).ok()
    for query in ['@items:( @color:>{m} )', '@items:( ismissing(@color) )',
                  '@items:( @color:{red} the )', '@items:( @price:[1 2 3 km] )',
                  '@items:( @price:[VECTOR_RANGE 1 $b] )']:
        env.expect('FT.SEARCH', 'idx', query, 'DIALECT', 2).error() \
            .contains('not supported on a sub-field of a NESTED group')
    env.expect('FT.SEARCH', 'idx', '@items:( @price:[5 1] )', 'DIALECT', 2).error() \
        .contains('@items.price')


@skip(no_json=True, cluster=True)
@unstable_features
def testAnEmptyTagValueMatchesWhateverTheSchema(env):
    """Sub-fields index empty tag values, so `{""}` finds them, with or without other fields."""
    conn = getConnectionByEnv(env)
    for schema in [ITEMS, ['$.t', 'AS', 't', 'TEXT', *ITEMS]]:
        env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'PREFIX', 1, 'doc:', 'SCHEMA', *schema).ok()
        put(conn, 'doc:1', [{'color': '', 'size': 'M', 'price': 1}])
        put(conn, 'doc:2', [{'color': 'red', 'size': 'M', 'price': 1}])
        env.assertEqual(ids(env, '@items:( @color:{""} )'), ['doc:1'], message=schema[0])
        env.expect('FT.DROPINDEX', 'idx', 'DD').ok()


@skip(no_json=True, cluster=True)
@unstable_features
def testAGroupLeavesOtherTagsCheckedForEmptyValues(env):
    """The empty values a group accepts do not exempt a top-level TAG field without INDEXEMPTY."""
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'PREFIX', 1, 'doc:', 'SCHEMA',
               '$.t', 'AS', 't', 'TAG', *ITEMS).ok()
    for query in ['@items:( @color:{""} ) @t:{""}', '@items:( @color:{red} ) @t:{""}',
                  '@t:{""} @items:( @color:{red} )']:
        env.expect('FT.SEARCH', 'idx', query, 'DIALECT', 2).error().contains('INDEXEMPTY')


@skip(no_json=True, cluster=True)
@unstable_features
def testAPrefixInAGroupFollowsThePrefixSettings(env):
    """MINPREFIX and MAXPREFIXEXPANSIONS bound a prefix on a tag terminal as on a TAG field."""
    conn = getConnectionByEnv(env)
    create(env, '$.items[*]', 'AS', 'items', 'NESTED', 2,
           '$.color', 'AS', 'color', 'TAG', '$.material', 'AS', 'material', 'TAG',
           'INDEX', 2, 'color', 'material').ok()
    for n, material in enumerate(['steel', 'stone', 'straw']):
        put(conn, 'doc:%d' % n, [{'color': 'red', 'material': material}])
    query = '@items:( @material:{st*} )'
    env.assertEqual(ids(env, query), ['doc:0', 'doc:1', 'doc:2'])
    min_prefix = env.cmd(config_cmd(), 'GET', 'MINPREFIX')[0][1]
    max_expansions = env.cmd(config_cmd(), 'GET', 'MAXPREFIXEXPANSIONS')[0][1]
    try:
        env.expect(config_cmd(), 'SET', 'MINPREFIX', 3).ok()
        env.assertEqual(ids(env, query), [])
        env.expect(config_cmd(), 'SET', 'MINPREFIX', 2).ok()
        env.expect(config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', 2).ok()
        env.assertEqual(len(ids(env, query)), 2)
    finally:
        env.expect(config_cmd(), 'SET', 'MINPREFIX', min_prefix).ok()
        env.expect(config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', max_expansions).ok()


@skip(no_json=True, cluster=True)
@unstable_features
def testTextClausesInAGroupSayWhy(env):
    """Any text clause inside a group, on a sub-field or bare, names the group."""
    create(env).ok()
    for query in ['@items:( @color:red* )', '@items:( @color:"red" )', '@items:( @color:%red% )',
                  '@items:( red* )', '@items:( "red" )', '@items:( @color:{red} ~@size:{M} )']:
        env.expect('FT.SEARCH', 'idx', query, 'DIALECT', 2).error() \
            .contains('not supported on a sub-field of a NESTED group')


@skip(no_json=True)
@unstable_features
def testAKnnQueryKeepsTextGroups(env):
    """Without the schema, the coordinator still reads a text group, quoted `@` included, as text."""
    import struct
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'PREFIX', 1, 'doc:', 'SCHEMA',
               '$.t', 'AS', 't', 'TEXT', '$.v', 'AS', 'v', 'VECTOR', 'FLAT', 6, 'TYPE', 'FLOAT32',
               'DIM', 2, 'DISTANCE_METRIC', 'L2', *ITEMS).ok()
    for n, text in enumerate(['hello world', 'mail jo@ex', 'goodbye']):
        conn.execute_command('JSON.SET', 'doc:%d' % n, '$', json.dumps(
            {'t': text, 'v': [float(n), 0.0], 'items': [{'color': 'red', 'size': 'M', 'price': n}]}))
    blob = struct.pack('<2f', 0.0, 0.0)
    for query, expected in [('(@t:(hello world))=>[KNN 10 @v $b]', ['doc:0']),
                            ('(@t:("jo@ex"))=>[KNN 10 @v $b]', ['doc:1']),
                            ('(@t:(mail | jo\\@ex))=>[KNN 10 @v $b]', ['doc:1']),
                            ('(@t:((hello) world))=>[KNN 10 @v $b]', ['doc:0'])]:
        res = env.cmd('FT.SEARCH', 'idx', query, 'PARAMS', 2, 'b', blob, 'NOCONTENT', 'DIALECT', 2)
        env.assertEqual(sorted(res[1:]), expected, message=query)
