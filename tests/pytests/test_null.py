# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from common import *
import json

# Field, Type, TypeOptions, Value
fields_and_values = [
    ('ta', 'TAG', [], 'foo'),
    ('te', 'TEXT', [], 'foo'),
    ('n', 'NUMERIC', [], 42),
    ('loc', 'GEO', [], '1.23, 4.56'),
    ('gs', 'GEOSHAPE', [], 'POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'),
    ('v', 'VECTOR', ['FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2'], [0.0, 0.0]),
]

def testNullValidations():
    """Tests the validations for null values indexing"""

    env = Env(moduleArgs='DEFAULT_DIALECT 2')

    # Every field type accepts `INDEXNULL`, alone or alongside `INDEXMISSING`,
    # in either order.
    for i, (field, ftype, type_options, _) in enumerate(fields_and_values):
        env.expect('FT.CREATE', f'idx_null_{i}', 'ON', 'JSON', 'SCHEMA',
                   f'$.{field}', 'AS', field, ftype, *type_options, 'INDEXNULL').ok()
        env.expect('FT.CREATE', f'idx_null_missing_{i}', 'ON', 'JSON', 'SCHEMA',
                   f'$.{field}', 'AS', field, ftype, *type_options, 'INDEXNULL', 'INDEXMISSING').ok()
        env.expect('FT.CREATE', f'idx_missing_null_{i}', 'ON', 'JSON', 'SCHEMA',
                   f'$.{field}', 'AS', field, ftype, *type_options, 'INDEXMISSING', 'INDEXNULL').ok()

    # Same with SORTABLE, WITHSUFFIXTRIE
    env.expect('FT.CREATE', 'idx_opts', 'ON', 'JSON', 'SCHEMA',
               '$.ta', 'AS', 'ta', 'TAG', 'INDEXNULL', 'WITHSUFFIXTRIE', 'SORTABLE',
               '$.te', 'AS', 'te', 'TEXT', 'INDEXNULL', 'WITHSUFFIXTRIE', 'SORTABLE',
               '$.n', 'AS', 'n', 'NUMERIC', 'INDEXNULL', 'SORTABLE').ok()

    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
               '$.ta', 'AS', 'ta', 'TAG', '$.te', 'AS', 'te', 'TEXT',
               '$.n', 'AS', 'n', 'NUMERIC').ok()

    # Using `isnull(@field)` when `field` does not index null values is an error.
    for field in ('ta', 'te', 'n'):
        env.expect('FT.SEARCH', 'idx', f'isnull(@{field})').error().contains(
            "'isnull' requires defining the field with 'INDEXNULL'"
        )

    env.expect('FT.CREATE', 'idx_noindex', 'SCHEMA', 'f1', 'TAG', 'INDEXNULL', 'NOINDEX').error().contains(
        'Field cannot be defined with both `NOINDEX` and `INDEXNULL` `f1`'
    )

def testNullInfo():
    """Tests that we get the `INDEXNULL` keyword in the INFO response for fields
    that index null values."""

    env = Env(moduleArgs='DEFAULT_DIALECT 2')

    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
               '$.ta', 'AS', 'ta', 'TAG', 'INDEXNULL',
               '$.n', 'AS', 'n', 'NUMERIC', 'INDEXNULL').ok()

    res = env.cmd('FT.INFO', 'idx')
    fields = res[res.index('attributes') + 1]
    env.assertEqual(len(fields), 2)
    for field in fields:
        env.assertEqual(field[-1], 'INDEXNULL')

@skip(no_json=True)
def testNullBasic():
    """`isnull(@field)` matches exactly the documents holding a JSON `null` in
    the field: not those holding a value, and not those lacking the field, for
    every field type."""

    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    for field, ftype, type_options, value in fields_and_values:
        env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
                   f'$.{field}', 'AS', field, ftype, *type_options, 'INDEXNULL', 'INDEXMISSING').ok()

        conn.execute_command('JSON.SET', 'null', '$', json.dumps({field: None}))
        conn.execute_command('JSON.SET', 'value', '$', json.dumps({field: value}))
        conn.execute_command('JSON.SET', 'missing', '$', json.dumps({'other': 'x'}))
        waitForIndex(env, 'idx')

        env.expect('FT.SEARCH', 'idx', f'isnull(@{field})', 'NOCONTENT').equal([1, 'null'])
        env.expect('FT.SEARCH', 'idx', f'ismissing(@{field})', 'NOCONTENT').equal([1, 'missing'])

        res = env.cmd('FT.SEARCH', 'idx', f'-isnull(@{field})', 'NOCONTENT')
        env.assertEqual(res[0], 2, message=ftype)
        env.assertEqual(set(res[1:]), {'value', 'missing'}, message=ftype)

        res = env.cmd('FT.SEARCH', 'idx', f'isnull(@{field}) | ismissing(@{field})', 'NOCONTENT')
        env.assertEqual(res[0], 2, message=ftype)
        env.assertEqual(set(res[1:]), {'null', 'missing'}, message=ftype)

        env.flush()

@skip(no_json=True)
def testNullUpdates():
    """A document stops matching `isnull` once its field is given a value, and
    starts matching once the value is replaced with `null`."""

    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.n', 'AS', 'n', 'NUMERIC', 'INDEXNULL').ok()

    conn.execute_command('JSON.SET', 'doc', '$', json.dumps({'n': None}))
    env.expect('FT.SEARCH', 'idx', 'isnull(@n)', 'NOCONTENT').equal([1, 'doc'])

    conn.execute_command('JSON.SET', 'doc', '$.n', '42')
    env.expect('FT.SEARCH', 'idx', 'isnull(@n)', 'NOCONTENT').equal([0])

    conn.execute_command('JSON.SET', 'doc', '$.n', 'null')
    env.expect('FT.SEARCH', 'idx', 'isnull(@n)', 'NOCONTENT').equal([1, 'doc'])

@skip(cluster=True, no_json=True)
def testNullExplainAndProfile():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.ta', 'AS', 'ta', 'TAG', 'INDEXNULL').ok()
    conn.execute_command('JSON.SET', 'doc', '$', json.dumps({'ta': None}))

    env.expect('FT.EXPLAIN', 'idx', 'isnull(@ta)').equal('ISNULL{ta}\n')

    res = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', 'isnull(@ta)', 'NOCONTENT')
    iterators = to_dict(to_dict(res[1])['Shards'][0])['Iterators profile']
    env.assertEqual(to_dict(iterators)['Type'], 'NULL')

@skip(cluster=True, no_json=True)
def testNullGC():
    """Tests that the GC cleans the null-values inverted index of deleted
    documents."""

    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.t', 'AS', 't', 'TEXT', 'INDEXNULL').ok()

    conn.execute_command('JSON.SET', 'doc1', '$', json.dumps({'t': 'hello'}))
    conn.execute_command('JSON.SET', 'doc2', '$', json.dumps({'t': None}))
    waitForIndex(env, 'idx')

    env.expect('FT.SEARCH', 'idx', 'isnull(@t)', 'NOCONTENT').equal([1, 'doc2'])

    env.expect(config_cmd(), 'SET', 'FORK_GC_CLEAN_THRESHOLD', '0').ok()
    env.expect(debug_cmd(), 'GC_STOP_SCHEDULE', 'idx').ok()

    conn.execute_command('DEL', 'doc2')

    env.expect(debug_cmd(), 'GC_FORCEINVOKE', 'idx').equal('DONE')
    env.expect(debug_cmd(), 'GC_WAIT_FOR_JOBS').equal('DONE')

    env.expect('FT.SEARCH', 'idx', 'isnull(@t)', 'NOCONTENT').equal([0])

    res = env.cmd('FT.INFO', 'idx')
    gc_sec = res[res.index('gc_stats') + 1]
    bytes_collected = gc_sec[gc_sec.index('bytes_collected') + 1]
    env.assertGreater(int(bytes_collected), 0)

    # The null-values bookkeeping is recreated for documents indexed after the
    # field's inverted index was collected.
    conn.execute_command('JSON.SET', 'doc3', '$', json.dumps({'t': None}))
    env.expect('FT.SEARCH', 'idx', 'isnull(@t)', 'NOCONTENT').equal([1, 'doc3'])

    env.cmd(debug_cmd(), 'GC_CONTINUE_SCHEDULE', 'idx')

@skip(no_json=True)
def testNullAlterAndReload():
    """A field given INDEXNULL through FT.ALTER indexes documents holding
    `null` in it, both for documents re-scanned after the ALTER and for
    documents added later, and keeps doing so after an RDB reload."""

    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.ta', 'AS', 'ta', 'TAG').ok()
    conn.execute_command('JSON.SET', 'before_alter', '$', json.dumps({'ta': 'foo', 'te': None}))

    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', '$.te', 'AS', 'te', 'TEXT', 'INDEXNULL').ok()
    waitForIndex(env, 'idx')
    conn.execute_command('JSON.SET', 'after_alter', '$', json.dumps({'ta': 'foo', 'te': None}))
    conn.execute_command('JSON.SET', 'has_te', '$', json.dumps({'ta': 'foo', 'te': 'bar'}))

    res = env.cmd('FT.SEARCH', 'idx', 'isnull(@te)', 'NOCONTENT')
    env.assertEqual(res[0], 2)
    env.assertEqual(set(res[1:]), {'before_alter', 'after_alter'})

    env.dumpAndReload()
    waitForIndex(env, 'idx')

    res = env.cmd('FT.SEARCH', 'idx', 'isnull(@te)', 'NOCONTENT')
    env.assertEqual(res[0], 2)
    env.assertEqual(set(res[1:]), {'before_alter', 'after_alter'})

    conn.execute_command('JSON.SET', 'after_reload', '$', json.dumps({'ta': 'foo', 'te': None}))
    res = env.cmd('FT.SEARCH', 'idx', 'isnull(@te)', 'NOCONTENT')
    env.assertEqual(res[0], 3)
    env.assertEqual(set(res[1:]), {'before_alter', 'after_alter', 'after_reload'})

JSON_MULTI_VALUE_NULL_DOCS = {
    'missing': {'other': 'x'},
    'null': {'f': None},
    'empty_str': {'f': ''},
    'empty_arr': {'f': []},
    'all_nulls': {'f': [None, None]},
    'some_nulls': {'f': ['foo', None]},
}

# Path -> docs matching `isnull`. Only a path yielding a single `null` result
# makes the field null; `null`s among several results are skipped, and `$.f[*]`
# yields no results at all for a scalar `null`.
JSON_MULTI_VALUE_EXPECTED_NULL = {
    '$.f': {'null'},
    '$.f[*]': set(),
}

@skip(cluster=True, no_json=True)
def testJsonMultiValueNull():
    """Pins which JSON multi-value shapes INDEXNULL treats as null, for each
    path form."""

    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    for path, expected in JSON_MULTI_VALUE_EXPECTED_NULL.items():
        for field_type in ('TAG', 'TEXT'):
            case = f'{path} {field_type}'
            env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'PREFIX', '1', 'doc:', 'SCHEMA',
                       path, 'AS', 'f', field_type, 'INDEXNULL').ok()
            for suffix, doc in JSON_MULTI_VALUE_NULL_DOCS.items():
                conn.execute_command('JSON.SET', f'doc:{suffix}', '$', json.dumps(doc))

            res = env.cmd('FT.SEARCH', 'idx', 'isnull(@f)', 'NOCONTENT', 'LIMIT', '0', '100')
            env.assertEqual(res[0], len(expected), message=f'{case}: {res}')
            env.assertEqual(set(res[1:]), {f'doc:{s}' for s in expected}, message=case)
            env.flush()
