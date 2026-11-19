# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Flow tests for MAP NUMERIC fields: schema, JSON indexing, keyed queries and syntax compatibility."""

import json
import random
import struct

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


def modules_info_line(env, name):
    """The value of `name` in INFO MODULES, or None if absent."""
    res = getConnectionByEnv(env).execute_command('INFO MODULES')
    for line in res.splitlines():
        if line.startswith(name + ':'):
            return line.split(':', 1)[1]
    return None


def map_numeric_modules_info(env):
    return modules_info_line(env, 'search_fields_map_numeric')


def testInfoModulesCountsMapNumericFields(env):
    """INFO MODULES reports MAP NUMERIC fields only while some exist."""
    enable_unstable_features(env)
    env.expect('FT.CREATE', 'other', 'ON', 'JSON', 'SCHEMA', '$.t', 'AS', 't', 'TAG').ok()
    env.assertEqual(map_numeric_modules_info(env), None, message='absent without the field')
    create_index(env)
    env.assertContains('MapNumeric=1', map_numeric_modules_info(env))
    env.assertEqual(modules_info_line(env, 'search_total_indexing_ops_map_numeric_fields'), None)
    set_doc(env, 1, {'a': 1})
    set_doc(env, 2, {'a': 2})
    env.assertEqual(modules_info_line(env, 'search_total_indexing_ops_map_numeric_fields'), '2')
    env.expect('FT.DROPINDEX', 'idx').ok()
    env.assertEqual(map_numeric_modules_info(env), None, message='gone after the index drops')


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
    t = index_info(env, 'i1')['attributes'][0]
    env.assertContains('SORTABLE', t, message='t keeps SORTABLE')
    env.assertContains('NOSTEM', t, message='t keeps NOSTEM')
    env.expect('FT.CREATE', 'i2', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE', 'INDEXMISSING',
               'MAP', 'NUMERIC').ok()
    env.assertEqual(attributes(env, 'i2'), [('n', 'NUMERIC'), ('MAP', 'NUMERIC')])
    n = index_info(env, 'i2')['attributes'][0]
    env.assertContains('SORTABLE', n, message='n keeps SORTABLE')
    env.assertContains('INDEXMISSING', n, message='n keeps INDEXMISSING')
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


def info_num_records(env):
    return int(index_info(env)['num_records'])


def metrics_stats(env):
    stats = metrics_field_stats(env)
    return int(stats['num_keys']), int(stats['memory'])


@skip(cluster=True)
def testIndexingCountsKeysAndMemory(env):
    """Keys, memory and record counts follow the indexed members."""
    enable_unstable_features(env)
    create_index(env)
    set_doc(env, 1, {'rev:fy0': 48200000, 'rev:fym1': 41000000.5, 'headcount': 212})
    set_doc(env, 2, {'rev:fy0': 1, 'other': -3})
    env.assertEqual(num_docs(env), 2)
    num_keys, memory = metrics_stats(env)
    env.assertEqual(num_keys, 4)
    env.assertGreater(memory, 0)
    # 5 (doc, key) records plus one `hq` tag record per document.
    env.assertEqual(info_num_records(env), 7)
    env.assertEqual(index_errors(env)['indexing failures'], 0)

    # A key already present grows its tree, not the key count.
    set_doc(env, 3, {'rev:fy0': 5})
    env.assertEqual(metrics_stats(env)[0], 4)
    env.assertGreater(metrics_stats(env)[1], memory)


@skip(cluster=True)
def testBackfillIndexesExistingDocuments(env):
    """Documents that exist before FT.CREATE are indexed by the background scan."""
    enable_unstable_features(env)
    set_doc(env, 1, {'a': 1, 'b': 2})
    set_doc(env, 2, {'a': 3})
    create_index(env)
    waitForIndex(env)
    env.assertEqual(num_docs(env), 2)
    env.assertEqual(metrics_stats(env)[0], 2)


@skip(cluster=True)
def testAlterBackfillsTheNewField(env):
    """FT.ALTER ... ADD scans existing documents for the new MAP NUMERIC field."""
    enable_unstable_features(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'PREFIX', 1, 'company:', 'SCHEMA',
               '$.hq', 'AS', 'hq', 'TAG').ok()
    set_doc(env, 1, {'a': 1, 'b': 2})
    set_doc(env, 2, {'a': 3})
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', '$.metrics', 'AS', 'metrics',
               'MAP', 'NUMERIC').ok()
    waitForIndex(env)
    env.assertEqual(num_docs(env), 2)
    env.assertEqual(metrics_stats(env)[0], 2)
    env.assertEqual(search_ids(env, '@metrics[a]:[1 3]'), {1, 2})
    env.assertEqual(search_ids(env, '@metrics[b]:[2 2]'), {1})


@skip(cluster=True)
def testEmptyObjectIsIndexedWithoutKeys(env):
    """An empty object indexes the document, with no key."""
    enable_unstable_features(env)
    create_index(env)
    set_doc(env, 1, {})
    env.assertEqual(num_docs(env), 1)
    env.assertEqual(metrics_stats(env)[0], 0)
    env.assertEqual(index_errors(env)['indexing failures'], 0)
    env.expect('FT.SEARCH', 'idx', '@hq:{US}', 'NOCONTENT').equal([1, 'company:1'])


@skip(cluster=True)
def testMissingAndNullFieldIsAbsent(env):
    """A missing or null attribute leaves the field absent, which is not a failure."""
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
    """Each kind of invalid value fails its document, with its own error text."""
    enable_unstable_features(env)
    create_index(env)
    member_error = 'MAP NUMERIC members must be numbers'
    bad = [
        ('string member', {'ok': 1, 'bad': 'x'}, member_error),
        ('numeric string member', {'bad': '12'}, member_error),
        ('bool member', {'bad': True}, member_error),
        ('null member', {'bad': None}, member_error),
        ('array member', {'bad': [1, 2]}, member_error),
        ('nested object member', {'bad': {'inner': 1}}, member_error),
        ('array of objects', [{'a': 1}], 'must be a single JSON object'),
        ('scalar', 5, 'Invalid JSON type'),
        ('string', 'text', 'Invalid JSON type'),
    ]
    for i, (name, metrics, expected_error) in enumerate(bad, start=1):
        set_doc(env, i, metrics)
        env.assertContains(expected_error, index_errors(env, 'metrics')['last indexing error'],
                           message=name)
        env.assertEqual(index_errors(env, 'metrics')['last indexing error key'], f'company:{i}',
                        message=name)

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
    """A path matching several objects fails the document, so a (doc, key) is added once."""
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
    env.assertContains('exactly one JSON object', index_errors(env, 'metrics')['last indexing error'])


@skip(cluster=True)
def testFailureLeavesOtherFieldsOfGoodDocsIndexed(env):
    """A document that fails on MAP NUMERIC is not indexed for its other fields either."""
    enable_unstable_features(env)
    create_index(env)
    set_doc(env, 1, {'a': 1})
    set_doc(env, 2, {'a': 'bad'}, hq='UK')
    env.expect('FT.SEARCH', 'idx', '@hq:{US}', 'NOCONTENT').equal([1, 'company:1'])
    env.expect('FT.SEARCH', 'idx', '@hq:{UK}', 'NOCONTENT').equal([0])


@skip(cluster=True)
def testUpdateReindexesTheDocument(env):
    """An update adds the new members; the old entries stay in the tree until GC."""
    enable_unstable_features(env)
    create_index(env)
    set_doc(env, 1, {'a': 1, 'b': 2})
    env.assertEqual(metrics_stats(env)[0], 2)
    # Two (doc, key) records plus the document's `hq` tag record.
    env.assertEqual(info_num_records(env), 3)

    set_doc(env, 1, {'a': 5, 'c': 7})
    env.assertEqual(num_docs(env), 1)
    env.assertEqual(metrics_stats(env)[0], 3, message='the new key is added')
    # The update indexes a new document version: two more (doc, key) records and a tag record.
    env.assertEqual(info_num_records(env), 6, message='old entries stay until GC')
    env.assertEqual(search_ids(env, '@metrics[a]:[5 5]'), {1})
    env.assertEqual(search_ids(env, '@metrics[c]:[7 7]'), {1})

    # An update that fails removes the document, as for the other types.
    set_doc(env, 1, {'a': 'bad'})
    env.assertEqual(num_docs(env), 0)
    env.assertEqual(index_errors(env)['indexing failures'], 1)
    env.assertEqual(search_ids(env, '@metrics[a]:[-inf +inf]'), set())


@skip(cluster=True)
def testUpdateDropsTheOldValueFromQueries(env):
    """After an update the old value no longer matches, though its entry stays in the tree."""
    enable_unstable_features(env)
    create_index(env)
    set_doc(env, 1, {'a': 1, 'b': 2})
    env.assertEqual(search_ids(env, '@metrics[a]:[1 1]'), {1})
    env.assertEqual(search_ids(env, '@metrics[b]:[2 2]'), {1})

    set_doc(env, 1, {'a': 5})
    env.assertEqual(search_ids(env, '@metrics[a]:[1 1]'), set(), message='old value')
    env.assertEqual(search_ids(env, '@metrics[a]:[5 5]'), {1}, message='new value')
    env.assertEqual(search_ids(env, '@metrics[b]:[-inf +inf]'), set(), message='dropped key')


@skip(cluster=True)
def testEdgeCaseNumbers(env):
    """Large integers, -0.0, negatives and floats match the ranges a double comparison gives."""
    enable_unstable_features(env)
    create_index(env)
    conn = getConnectionByEnv(env)
    raw = ('{"metrics": {"above_2_53": 9007199254740993, "i64_max": 9223372036854775807,'
           ' "i64_min": -9223372036854775808, "neg_zero": -0.0, "neg": -7,'
           ' "frac": 0.1, "neg_frac": -2.5, "big": 1.5e300}}')
    env.assertEqual(conn.execute_command('JSON.SET', 'company:1', '$', raw), 'OK')
    env.assertEqual(index_errors(env)['indexing failures'], 0)
    env.assertEqual(metrics_stats(env)[0], 8)

    def matches(key, lo, hi):
        return search_ids(env, f'@metrics[{key}]:[{lo} {hi}]') == {1}

    # An integer beyond 2^53 is stored as the nearest double, 2^53.
    env.assertTrue(matches('above_2_53', 9007199254740992, 9007199254740992))
    env.assertTrue(matches('above_2_53', 9007199254740990, 9007199254740994))
    env.assertFalse(matches('above_2_53', 9007199254740994, 9007199254741000))
    env.assertTrue(matches('i64_max', 9223372036854775000, 9223372036854776000))
    env.assertFalse(matches('i64_max', 0, 9223372036854770000))
    env.assertTrue(matches('i64_min', '-inf', -9223372036854770000))
    env.assertFalse(matches('i64_min', -9223372036854770000, 0))
    # -0.0 equals 0.
    env.assertTrue(matches('neg_zero', 0, 0))
    env.assertTrue(matches('neg_zero', -1, 1))
    env.assertEqual(search_ids(env, '@metrics[neg_zero]:[(0 +inf]'), set())
    env.assertEqual(search_ids(env, '@metrics[neg_zero]:[-inf (0]'), set())
    env.assertTrue(matches('neg', -10, -1))
    env.assertFalse(matches('neg', -6, 0))
    env.assertTrue(matches('frac', 0.05, 0.15))
    env.assertFalse(matches('frac', 0.2, 1))
    env.assertTrue(matches('neg_frac', -2.6, -2.4))
    env.assertEqual(search_ids(env, '@metrics[neg_frac]:[(-2.5 +inf]'), set())
    env.assertTrue(matches('big', 1e299, 1e301))
    env.assertTrue(matches('big', '-inf', '+inf'))


@skip(cluster=True)
def testEmptyStringKey(env):
    """The empty key indexes without failure and is queried through a parameter."""
    enable_unstable_features(env)
    create_index(env)
    set_doc(env, 1, {'': 1, 'a': 2})
    env.assertEqual(index_errors(env)['indexing failures'], 0)
    env.assertEqual(num_docs(env), 1)
    env.assertEqual(metrics_stats(env)[0], 2)
    env.assertEqual(search_ids(env, '@metrics[$k]:[1 1]', 'PARAMS', 2, 'k', '', 'DIALECT', 2), {1})


@skip(cluster=True)
def testDelete(env):
    """A deleted document leaves the index at once; its keys stay until GC."""
    enable_unstable_features(env)
    create_index(env)
    set_doc(env, 1, {'a': 1})
    env.assertEqual(num_docs(env), 1)
    env.expect('DEL', 'company:1').equal(1)
    env.assertEqual(num_docs(env), 0)
    env.assertEqual(metrics_stats(env)[0], 1, message='the key stays until GC')
    env.assertEqual(search_ids(env, '@metrics[a]:[-inf +inf]'), set())


@skip(cluster=True)
def testExpire(env):
    """An expired document leaves the index. Active expiry is off, so the read below expires it."""
    enable_unstable_features(env)
    create_index(env)
    conn = getConnectionByEnv(env)
    conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')
    try:
        set_doc(env, 1, {'a': 1})
        env.assertEqual(num_docs(env), 1)
        env.expect('PEXPIRE', 'company:1', 1).equal(1)
        time.sleep(0.01)
        env.expect('EXISTS', 'company:1').equal(0)
        env.assertEqual(num_docs(env), 0)
        env.assertEqual(search_ids(env, '@metrics[a]:[-inf +inf]'), set())
    finally:
        conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '1')


@skip(cluster=True)
def testReloadReindexes(env):
    """Reloading rebuilds the index from the documents; failing documents stay out."""
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


# ---- Queries ----

KEYS = ['rev:fy0', 'rev:fym1', 'headcount:m2026_06', 'Rev:FY0']
VALUES = [-5, 0, 1, 2.5, 10, 50, 100, 48200000]


def gen_docs(seed=7, n=60):
    """doc id -> (hq, metrics), with a random subset of keys per doc."""
    rnd = random.Random(seed)
    docs = {}
    for i in range(1, n + 1):
        metrics = {k: rnd.choice(VALUES) for k in KEYS if rnd.random() < 0.6}
        docs[i] = (rnd.choice(['US', 'EU']), metrics)
    return docs


def load_docs(env, docs):
    for i, (hq, metrics) in docs.items():
        set_doc(env, i, metrics, hq)


def in_range(v, lo, hi, lo_incl=True, hi_incl=True):
    return (v > lo or (lo_incl and v == lo)) and (v < hi or (hi_incl and v == hi))


def ref(docs, key, lo, hi, lo_incl=True, hi_incl=True, hq=None):
    return {i for i, (h, m) in docs.items()
            if (hq is None or h == hq) and key in m and in_range(m[key], lo, hi, lo_incl, hi_incl)}


def search_ids(env, query, *args):
    res = env.cmd('FT.SEARCH', 'idx', query, 'NOCONTENT', 'LIMIT', 0, 1000, *args)
    env.assertEqual(res[0], len(res) - 1)
    return {int(doc.split(':')[1]) for doc in res[1:]}


def setup_docs(env):
    enable_unstable_features(env)
    create_index(env)
    docs = gen_docs()
    load_docs(env, docs)
    return docs


@skip(cluster=True)
def testKeyAndRangeMatchesReference(env):
    docs = setup_docs(env)
    inf = float('inf')
    cases = [
        ('[1 10]', 1, 10, True, True),
        ('[(1 10]', 1, 10, False, True),
        ('[1 (10]', 1, 10, True, False),
        ('[(1 (10]', 1, 10, False, False),
        ('[-inf +inf]', -inf, inf, True, True),
        ('[(0 +inf]', 0, inf, False, True),
        ('[-inf (0]', -inf, 0, True, False),
        ('[100 100]', 100, 100, True, True),
        ('[48200000]', 48200000, 48200000, True, True),
    ]
    for key in KEYS:
        for text, lo, hi, lo_incl, hi_incl in cases:
            expected = ref(docs, key, lo, hi, lo_incl, hi_incl)
            env.assertEqual(search_ids(env, f'@metrics[{key}]:{text}'), expected,
                            message=f'{key} {text}')
    # Keys are case sensitive.
    env.assertNotEqual(search_ids(env, '@metrics[rev:fy0]:[-inf +inf]'),
                       search_ids(env, '@metrics[Rev:FY0]:[-inf +inf]'))


@skip(cluster=True)
def testMissingKeyMatchesNothing(env):
    setup_docs(env)
    env.expect('FT.SEARCH', 'idx', '@metrics[nokey]:[-inf +inf]', 'NOCONTENT').equal([0])
    env.expect('FT.SEARCH', 'idx', '@metrics[rev:fy]:[-inf +inf]', 'NOCONTENT').equal([0])


@skip(cluster=True)
def testEmptyIndexMatchesNothing(env):
    enable_unstable_features(env)
    create_index(env)
    env.expect('FT.SEARCH', 'idx', '@metrics[a]:[-inf +inf]', 'NOCONTENT').equal([0])


@skip(cluster=True)
def testCombinedWithTagAndMultipleKeys(env):
    docs = setup_docs(env)
    env.assertEqual(search_ids(env, '@hq:{US} @metrics[rev:fy0]:[(1 +inf]'),
                    ref(docs, 'rev:fy0', 1, float('inf'), False, True, hq='US'))
    both = ref(docs, 'rev:fy0', 1, 100) & ref(docs, 'headcount:m2026_06', -float("inf"), 10)
    env.assertEqual(search_ids(env, '@metrics[rev:fy0]:[1 100] @metrics[headcount:m2026_06]:[-inf 10]'),
                    both)
    env.assertGreater(len(both), 0)
    # The same key twice narrows to the intersection of the ranges.
    env.assertEqual(search_ids(env, '@metrics[rev:fy0]:[0 50] @metrics[rev:fy0]:[10 100]'),
                    ref(docs, 'rev:fy0', 10, 50))


@skip(cluster=True)
def testNotAndOr(env):
    docs = setup_docs(env)
    all_ids = set(docs)
    a = ref(docs, 'rev:fy0', 10, 100)
    b = ref(docs, 'headcount:m2026_06', -5, 0)
    env.assertEqual(search_ids(env, '-@metrics[rev:fy0]:[10 100]'), all_ids - a)
    env.assertEqual(search_ids(env, '@metrics[rev:fy0]:[10 100] | @metrics[headcount:m2026_06]:[-5 0]'),
                    a | b)
    env.assertEqual(search_ids(env, '@metrics[rev:fy0]:[10 100] -@metrics[headcount:m2026_06]:[-5 0]'),
                    a - b)
    env.assertEqual(search_ids(env, '-@metrics[nokey]:[-inf +inf]'), all_ids)
    env.assertEqual(search_ids(env, '(@metrics[rev:fy0]:[10 100] | @metrics[rev:fym1]:[10 100]) @hq:{EU}'),
                    {i for i in (a | ref(docs, 'rev:fym1', 10, 100)) if docs[i][0] == 'EU'})


@skip(cluster=True)
def testParamKey(env):
    docs = setup_docs(env)
    env.assertEqual(search_ids(env, '@metrics[$k]:[1 100]', 'PARAMS', 2, 'k', 'rev:fy0', 'DIALECT', 2),
                    ref(docs, 'rev:fy0', 1, 100))
    env.assertEqual(search_ids(env, '@metrics[$k]:[$lo $hi]', 'PARAMS', 6, 'k', 'Rev:FY0',
                               'lo', 1, 'hi', 100, 'DIALECT', 2),
                    ref(docs, 'Rev:FY0', 1, 100))
    # A key outside the unquoted charset only works as a parameter.
    set_doc(env, 1000, {'weird key,[1]': 3})
    env.assertEqual(search_ids(env, '@metrics[$k]:[3 3]', 'PARAMS', 2, 'k', 'weird key,[1]',
                               'DIALECT', 2), {1000})
    env.expect('FT.SEARCH', 'idx', '@metrics[$k]:[1 2]', 'NOCONTENT', 'DIALECT', 2).error() \
        .contains('Parameter not found')


@skip(cluster=True)
def testAggregateLoadSort(env):
    docs = setup_docs(env)
    res = env.cmd('FT.AGGREGATE', 'idx',
                  '@hq:{US} @metrics[rev:fy0]:[(1 +inf] @metrics[headcount:m2026_06]:[(-inf +inf]',
                  'LOAD', 3, '$.metrics["headcount:m2026_06"]', 'AS', 'hc',
                  'SORTBY', 2, '@hc', 'DESC', 'LIMIT', 0, 1000, 'DIALECT', 2)
    expected = ref(docs, 'rev:fy0', 1, float('inf'), False, True, hq='US') & \
        ref(docs, 'headcount:m2026_06', -float("inf"), float("inf"))
    rows = [to_dict(r) for r in res[1:]]
    env.assertEqual(len(rows), len(expected))
    got = [float(r['hc']) for r in rows]
    env.assertEqual(got, sorted((docs[i][1]['headcount:m2026_06'] for i in expected), reverse=True))


@skip(cluster=True)
def testProfileShowsKeyedNode(env):
    setup_docs(env)
    res = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '@metrics[rev:fy0]:[1 100]',
                  'NOCONTENT', 'LIMIT', 0, 5)
    text = str(res)
    env.assertContains('NUMERIC', text)
    explain = env.cmd('FT.EXPLAIN', 'idx', '@metrics[rev:fy0]:[1 100]', 'DIALECT', 2)
    env.assertContains('@metrics[rev:fy0]', explain)


@skip(cluster=True)
def testUpdateAndDelete(env):
    enable_unstable_features(env)
    create_index(env)
    set_doc(env, 1, {'a': 5, 'b': 1})
    set_doc(env, 2, {'a': 6})
    env.assertEqual(search_ids(env, '@metrics[a]:[5 6]'), {1, 2})
    set_doc(env, 1, {'a': 100})
    env.assertEqual(search_ids(env, '@metrics[a]:[5 6]'), {2})
    env.assertEqual(search_ids(env, '@metrics[a]:[100 100]'), {1})
    env.assertEqual(search_ids(env, '@metrics[b]:[-inf +inf]'), set())
    env.expect('DEL', 'company:2').equal(1)
    env.assertEqual(search_ids(env, '@metrics[a]:[5 6]'), set())
    set_doc(env, 1, {})
    env.assertEqual(search_ids(env, '@metrics[a]:[-inf +inf]'), set())


@skip(cluster=True)
def testErrors(env):
    enable_unstable_features(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.hq', 'AS', 'hq', 'TAG',
               '$.n', 'AS', 'n', 'NUMERIC', '$.metrics', 'AS', 'metrics', 'MAP', 'NUMERIC').ok()
    env.expect('FT.SEARCH', 'idx', '@metrics:[1 2]', 'DIALECT', 2).error().contains('NUMERIC')
    env.expect('FT.SEARCH', 'idx', '@n[a]:[1 2]', 'DIALECT', 2).error().contains('MAP NUMERIC')
    env.expect('FT.SEARCH', 'idx', '@hq[a]:[1 2]', 'DIALECT', 2).error().contains('MAP NUMERIC')
    env.expect('FT.SEARCH', 'idx', '@nofield[a]:[1 2]', 'DIALECT', 2).error().contains('Unknown field')
    env.expect('FT.SEARCH', 'idx', '@metrics[a]:[5 1]', 'DIALECT', 2).error().contains('min > max')
    env.expect('FT.SEARCH', 'idx', '@metrics[a]:hello', 'DIALECT', 2).error()


@skip(cluster=True)
def testQueryGateOff(env):
    enable_unstable_features(env)
    create_index(env)
    set_doc(env, 1, {'a': 1})
    set_unstable_features(env, False)
    env.expect('FT.SEARCH', 'idx', '@metrics[a]:[1 2]', 'DIALECT', 2).error().contains(GATE_ERROR)
    env.expect('FT.SEARCH', 'idx', '@metrics[$k]:[1 2]', 'PARAMS', 2, 'k', 'a',
               'DIALECT', 2).error().contains(GATE_ERROR)
    env.expect('FT.AGGREGATE', 'idx', '@metrics[a]:[1 2]', 'DIALECT', 2).error().contains(GATE_ERROR)
    set_unstable_features(env, True)
    env.assertEqual(search_ids(env, '@metrics[a]:[1 2]'), {1})


# ---- Existing query syntax is unchanged ----

def run_compat_checks(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'cidx', 'SCHEMA', 'txt', 'TEXT', 'tg', 'TAG', 'n', 'NUMERIC',
               'g', 'GEO', 'v', 'VECTOR', 'FLAT', 6, 'TYPE', 'FLOAT32', 'DIM', 2,
               'DISTANCE_METRIC', 'L2').ok()
    vec = lambda x, y: struct.pack('<2f', x, y)
    conn.execute_command('HSET', 'd1', 'txt', 'hello world', 'tg', 'a[b', 'n', 5,
                         'g', '1.0,1.0', 'v', vec(0, 0))
    conn.execute_command('HSET', 'd2', 'txt', 'hello [there]', 'tg', 'c', 'n', 15,
                         'g', '50.0,50.0', 'v', vec(1, 1))
    conn.execute_command('HSET', 'd3', 'txt', 'other', 'tg', 'a', 'n', 25,
                         'g', '1.1,1.1', 'v', vec(5, 5))

    def ids(*args):
        res = env.cmd('FT.SEARCH', 'cidx', *args, 'NOCONTENT', 'DIALECT', 2)
        return sorted(res[1:])

    env.assertEqual(ids('@n:[0 10]'), ['d1'])
    env.assertEqual(ids('@n:[(5 25]'), ['d2', 'd3'])
    env.assertEqual(ids('@n:[-inf +inf]'), ['d1', 'd2', 'd3'])
    env.assertEqual(ids('@n:[$lo $hi]', 'PARAMS', 4, 'lo', 10, 'hi', 20), ['d2'])
    env.assertEqual(ids('@n:[0 10] | @n:[20 30]'), ['d1', 'd3'])
    env.assertEqual(ids('@txt:$p', 'PARAMS', 2, 'p', 'hello'), ['d1', 'd2'])
    env.assertEqual(ids('@tg:{$p}', 'PARAMS', 2, 'p', 'c'), ['d2'])
    env.assertEqual(ids('@tg:{a\\[b}'), ['d1'])
    # The escaped brackets make one term, which the tokenizer never indexed.
    env.assertEqual(ids('@txt:hello\\[there\\]'), [])
    env.assertEqual(ids('@txt:hello \\[there\\]'), [])
    env.assertEqual(ids('@g:[1.0 1.0 50 km]'), ['d1', 'd3'])
    res = env.cmd('FT.SEARCH', 'cidx', '*=>[KNN 2 @v $b]', 'PARAMS', 2, 'b', vec(0.1, 0.1),
                  'SORTBY', '__v_score', 'NOCONTENT', 'DIALECT', 2)
    env.assertEqual(res[1:], ['d1', 'd2'])
    res = env.cmd('FT.SEARCH', 'cidx', '@v:[VECTOR_RANGE 3 $b]=>{$YIELD_DISTANCE_AS: dist}',
                  'PARAMS', 2, 'b', vec(0, 0), 'SORTBY', 'dist', 'NOCONTENT', 'DIALECT', 2)
    env.assertEqual(res[1:], ['d1', 'd2'])
    # A bracket after a field name without the key syntax is still the same syntax error.
    env.expect('FT.SEARCH', 'cidx', '@n[1]', 'DIALECT', 2).error()


@skip(cluster=True)
def testExistingSyntaxFlagOff(env):
    set_unstable_features(env, False)
    run_compat_checks(env)


@skip(cluster=True)
def testExistingSyntaxFlagOn(env):
    set_unstable_features(env, True)
    run_compat_checks(env)
