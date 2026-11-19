# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Flow tests for the schema of MAP NUMERIC fields."""

import json

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
    env.expect(*base, 'MAP').error().contains('MAP must be followed by NUMERIC')
    env.expect(*base, 'MAP', 'TAG').error().contains('MAP must be followed by NUMERIC')
    env.expect(*base, 'MAP', 'NUMERIC', 'TAG').error().contains('Field `TAG` does not have a type')
    env.expect(*base, 'MAP', 'NUMERIC', 'NUMERIC').error().contains('Field `NUMERIC` does not have a type')
    env.expect(*base, 'MAP', 'NUMERIC', 'WEIGHT', 2).error().contains('Invalid field type for field `WEIGHT`')
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
    env.broadcast('DEBUG RELOAD')
    env.assertEqual(metrics_attribute(env)['type'], 'MAP NUMERIC')
    env.assertEqual(metrics_field_stats(env)['num_keys'], 0)


def testReloadWithFlagOff(env):
    """Only creating a field is gated: an existing one still loads."""
    enable_unstable_features(env)
    create_index(env)
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


def testHashAlterRejected(env):
    enable_unstable_features(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TAG').ok()
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'm', 'MAP', 'NUMERIC').error().contains('JSON')


def attributes(env, idx):
    """(name, type) per attribute; attributes with flags are not key-value pairs."""
    return [(a[a.index('attribute') + 1], a[a.index('type') + 1])
            for a in index_info(env, idx)['attributes']]


def testGateOffKeepsMapAsFieldName(env):
    """With the flag off, `MAP NUMERIC` after a field parses as it always did:
    `MAP` is the name of a following NUMERIC field."""
    set_unstable_features(env, False)
    env.expect('FT.CREATE', 'i1', 'SCHEMA', 't', 'TEXT', 'SORTABLE', 'NOSTEM',
               'MAP', 'NUMERIC').ok()
    env.assertEqual(attributes(env, 'i1'), [('t', 'TEXT'), ('MAP', 'NUMERIC')])
    env.expect('FT.CREATE', 'i2', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE', 'INDEXMISSING',
               'MAP', 'NUMERIC').ok()
    env.assertEqual(attributes(env, 'i2'), [('n', 'NUMERIC'), ('MAP', 'NUMERIC')])
    env.expect('FT.CREATE', 'i3', 'SCHEMA', 'MAP', 'NUMERIC').ok()
    env.assertEqual(attributes(env, 'i3'), [('MAP', 'NUMERIC')])


# ---- JSON indexing ----

def set_doc(env, i, metrics, hq='US'):
    conn = getConnectionByEnv(env)
    doc = {'hq': hq}
    if metrics is not None:
        doc['metrics'] = metrics
    env.assertEqual(conn.execute_command('JSON.SET', f'company:{i}', '$', json.dumps(doc)), 'OK')


def index_errors(env, field=None):
    """Index-level error counters, or the ones of the attribute `field`."""
    if field is None:
        errors = index_info(env)['Index Errors']
    else:
        stats = [to_dict(s) for s in index_info(env)['field statistics']]
        errors = [s for s in stats if s['attribute'] == field][0]['Index Errors']
    return to_dict(errors)


def num_docs(env):
    return int(index_info(env)['num_docs'])


def metrics_stats(env):
    stats = metrics_field_stats(env)
    return int(stats['num_keys']), int(stats['memory'])


@skip(cluster=True)
def testIndexingCountsKeysAndMemory(env):
    enable_unstable_features(env)
    create_index(env)
    set_doc(env, 1, {'rev:fy0': 48200000, 'rev:fym1': 41000000.5, 'headcount': 212})
    set_doc(env, 2, {'rev:fy0': 1, 'other': -3})
    env.assertEqual(num_docs(env), 2)
    num_keys, memory = metrics_stats(env)
    env.assertEqual(num_keys, 4)
    env.assertGreater(memory, 0)
    env.assertEqual(index_errors(env)['indexing failures'], 0)

    # A key already present grows its tree, not the key count.
    set_doc(env, 3, {'rev:fy0': 5})
    env.assertEqual(metrics_stats(env)[0], 4)
    env.assertGreater(metrics_stats(env)[1], memory)


@skip(cluster=True)
def testBackfillIndexesExistingDocuments(env):
    enable_unstable_features(env)
    set_doc(env, 1, {'a': 1, 'b': 2})
    set_doc(env, 2, {'a': 3})
    create_index(env)
    waitForIndex(env)
    env.assertEqual(num_docs(env), 2)
    env.assertEqual(metrics_stats(env)[0], 2)


@skip(cluster=True)
def testEmptyObjectIsIndexedWithoutKeys(env):
    enable_unstable_features(env)
    create_index(env)
    set_doc(env, 1, {})
    env.assertEqual(num_docs(env), 1)
    env.assertEqual(metrics_stats(env)[0], 0)
    env.assertEqual(index_errors(env)['indexing failures'], 0)
    env.expect('FT.SEARCH', 'idx', '@hq:{US}', 'NOCONTENT').equal([1, 'company:1'])


@skip(cluster=True)
def testMissingAndNullFieldIsAbsent(env):
    enable_unstable_features(env)
    create_index(env)
    set_doc(env, 1, None)
    set_doc(env, 2, None)
    getConnectionByEnv(env).execute_command('JSON.SET', 'company:2', '$.metrics', 'null')
    env.assertEqual(num_docs(env), 2)
    env.assertEqual(metrics_stats(env)[0], 0)
    env.assertEqual(index_errors(env)['indexing failures'], 0)


@skip(cluster=True)
def testInvalidValuesFailTheDocument(env):
    enable_unstable_features(env)
    create_index(env)
    bad = [
        ('string member', {'ok': 1, 'bad': 'x'}),
        ('numeric string member', {'bad': '12'}),
        ('bool member', {'bad': True}),
        ('null member', {'bad': None}),
        ('array member', {'bad': [1, 2]}),
        ('nested object member', {'bad': {'inner': 1}}),
        ('array of objects', [{'a': 1}]),
        ('scalar', 5),
        ('string', 'text'),
    ]
    for i, (_, metrics) in enumerate(bad, start=1):
        set_doc(env, i, metrics)

    # A failing document is not indexed, and none of its members is.
    env.assertEqual(num_docs(env), 0)
    env.assertEqual(metrics_stats(env)[0], 0)
    env.assertEqual(index_errors(env)['indexing failures'], len(bad))
    env.assertEqual(index_errors(env, 'metrics')['indexing failures'], len(bad))
    env.assertEqual(int(index_info(env)['hash_indexing_failures']), len(bad))

    # A good document next to the bad ones is indexed normally.
    set_doc(env, 100, {'a': 1})
    env.assertEqual(num_docs(env), 1)
    env.assertEqual(metrics_stats(env)[0], 1)


@skip(cluster=True)
def testMultipleMatchesFailTheDocument(env):
    enable_unstable_features(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'PREFIX', 1, 'company:', 'SCHEMA',
               '$..metrics', 'AS', 'metrics', 'MAP', 'NUMERIC').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('JSON.SET', 'company:1', '$', '{"metrics": {"a": 1}}')
    conn.execute_command('JSON.SET', 'company:2', '$',
                         '{"metrics": {"a": 1}, "sub": {"metrics": {"b": 2}}}')
    env.assertEqual(num_docs(env), 1)
    env.assertEqual(metrics_stats(env)[0], 1)
    env.assertEqual(index_errors(env)['indexing failures'], 1)


@skip(cluster=True)
def testFailureLeavesOtherFieldsOfGoodDocsIndexed(env):
    enable_unstable_features(env)
    create_index(env)
    set_doc(env, 1, {'a': 1})
    set_doc(env, 2, {'a': 'bad'}, hq='UK')
    env.expect('FT.SEARCH', 'idx', '@hq:{US}', 'NOCONTENT').equal([1, 'company:1'])
    env.expect('FT.SEARCH', 'idx', '@hq:{UK}', 'NOCONTENT').equal([0])


@skip(cluster=True)
def testUpdateReindexesTheDocument(env):
    enable_unstable_features(env)
    create_index(env)
    set_doc(env, 1, {'a': 1, 'b': 2})
    env.assertEqual(metrics_stats(env)[0], 2)

    set_doc(env, 1, {'a': 5, 'c': 7})
    env.assertEqual(num_docs(env), 1)
    # Old entries stay until GC; the new key is added.
    env.assertEqual(metrics_stats(env)[0], 3)

    # An update that fails removes the document, as for the other types.
    set_doc(env, 1, {'a': 'bad'})
    env.assertEqual(num_docs(env), 0)
    env.assertEqual(index_errors(env)['indexing failures'], 1)


@skip(cluster=True)
def testDelete(env):
    enable_unstable_features(env)
    create_index(env)
    set_doc(env, 1, {'a': 1})
    env.assertEqual(num_docs(env), 1)
    env.expect('DEL', 'company:1').equal(1)
    env.assertEqual(num_docs(env), 0)


@skip(cluster=True)
def testExpire(env):
    enable_unstable_features(env)
    create_index(env)
    set_doc(env, 1, {'a': 1})
    env.assertEqual(num_docs(env), 1)
    env.expect('PEXPIRE', 'company:1', 1).equal(1)
    with TimeLimit(30, 'document was not expired'):
        while num_docs(env) != 0:
            time.sleep(0.05)


@skip(cluster=True)
def testReloadReindexes(env):
    enable_unstable_features(env)
    create_index(env)
    set_doc(env, 1, {'a': 1, 'b': 2})
    set_doc(env, 2, {'a': 3})
    set_doc(env, 3, {'a': 'bad'})
    before = metrics_stats(env)[0]
    env.assertEqual(before, 2)

    env.broadcast('DEBUG RELOAD')
    waitForIndex(env)
    env.assertEqual(num_docs(env), 2)
    env.assertEqual(metrics_stats(env)[0], before)
    env.assertGreater(metrics_stats(env)[1], 0)
