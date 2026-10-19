# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

import json

from redis.exceptions import ResponseError

from common import *
from test_alter_vector_relabel import get_internal_id

# FT.ALTER ... SCHEMA ADD skips documents that lack the added fields and moves or relabels the
# pre-existing vector and geometry entries of the documents it replaces. Either way the index must
# end up holding what an index created with the final schema holds. These tests build both and
# compare them. Doc-ids differ between the two, so every dump is translated to document keys.

DOC_COUNT = 7
# `geom` and `vec` exist before the ALTER, on every document, so the ALTER moves or relabels them
# on the documents it replaces. The ALTER adds the fields below; ADDED_FIELDS lists which of them
# each document carries. doc:0 and doc:6 carry none, so the ALTER skips them; every other document
# is replaced.
ADDED = ('tag', 'num', 'geom2', 'vec2')
ADDED_FIELDS = {
    0: (),
    1: ('tag',),
    2: ('num',),
    3: ('geom2',),
    4: ('vec2',),
    5: ADDED,
    6: (),
}
SKIPPED = [f'doc:{i}' for i, fields in ADDED_FIELDS.items() if not fields]
REPLACED = [f'doc:{i}' for i, fields in ADDED_FIELDS.items() if fields]

WITHIN_ALL = 'POLYGON((0 0, 0 20, 20 20, 20 0, 0 0))'
WITHIN_ALL2 = 'POLYGON((40 40, 40 70, 70 70, 70 40, 40 40))'
VEC_ARGS = ['VECTOR', 'FLAT', 6, 'TYPE', 'FLOAT32', 'DIM', 2, 'DISTANCE_METRIC', 'L2']


def _polygon(x):
    return f'POLYGON(({x} {x}, {x} {x + 1}, {x + 1} {x + 1}, {x + 1} {x}, {x} {x}))'


def _values(i):
    """doc:i's value for every field, distinct per document and per field."""
    return {
        'title': f'alpha beta{i}',
        'tag': f'tag{i}',
        'num': 100 + i,
        'geom': _polygon(i),
        'geom2': _polygon(i + 50),
        # Powers of two, so no two documents are equally far from a third and KNN order does not
        # depend on doc-ids, which differ between the two indexes.
        'vec': [float(2**i), 0.0],
        'vec2': [0.0, float(2**i)],
    }


def _blob(vec):
    return create_np_array_typed(vec, 'FLOAT32').tobytes()


class Flavor:
    """What differs between a Hash and a JSON index over the same logical documents."""

    def __init__(self, on):
        self.on = on

    def schema(self, field):
        """The schema entry for `field`, where `title` is the one the index is created with."""
        specs = {
            'title': ['TEXT'],
            'tag': ['TAG'],
            'num': ['NUMERIC'],
            'geom': ['GEOSHAPE', 'FLAT'],
            'geom2': ['GEOSHAPE', 'FLAT'],
            'vec': VEC_ARGS,
            'vec2': VEC_ARGS,
        }
        if self.on == 'HASH':
            return [field, *specs[field]]
        return [f'$.{field}', 'AS', field, *specs[field]]

    def write(self, conn, i):
        fields = {f: _values(i)[f] for f in ('title', 'geom', 'vec', *ADDED_FIELDS[i])}
        if self.on == 'HASH':
            for f in ('vec', 'vec2'):
                if f in fields:
                    fields[f] = _blob(fields[f])
            conn.execute_command('HSET', f'doc:{i}', *[x for kv in fields.items() for x in kv])
        else:
            conn.execute_command('JSON.SET', f'doc:{i}', '$', json.dumps(fields))


def _key_of(env, idx, doc_id, cache):
    """The key behind `doc_id`, or None for an id that was replaced or deleted: such an entry
    stays in the inverted indexes until GC and is not part of the index's content."""
    if doc_id not in cache:
        try:
            cache[doc_id] = env.cmd(debug_cmd(), 'IDTODOCID', idx, doc_id)
        except ResponseError:
            cache[doc_id] = None
    return cache[doc_id]


def _flatten(ids):
    for x in ids:
        if isinstance(x, list):
            yield from _flatten(x)
        else:
            yield x


def _live_keys(env, idx, ids, cache):
    keys = (_key_of(env, idx, i, cache) for i in _flatten(ids))
    return sorted(k for k in keys if k is not None)


def _geo_by_key(env, idx, field, cache):
    res = env.cmd(debug_cmd(), 'DUMP_GEOMIDX', idx, field)
    res = {res[i]: res[i + 1] for i in range(0, len(res), 2)}
    shapes = {}
    for doc in res['docs']:
        doc = {doc[i]: doc[i + 1] for i in range(0, len(doc), 2)}
        key = _key_of(env, idx, doc['id'], cache)
        # Unlike the inverted indexes, the R-tree drops a replaced document's entry inline, so an
        # id that no longer resolves is a stale entry, not content waiting for GC.
        env.assertIsNotNone(key, message=f'{idx}.{field}: stale R-tree entry for id {doc["id"]}')
        env.assertNotContains(key, shapes, message=f'{idx}.{field}: {key} has two R-tree entries')
        shapes[key] = doc['geoshape']
    return shapes


def _content(env, idx):
    """Everything `idx` holds, keyed by document key instead of doc-id."""
    cache = {}
    terms = sorted(env.cmd(debug_cmd(), 'DUMP_TERMS', idx))
    return {
        'terms': terms,
        'text': {t: _live_keys(env, idx, env.cmd(debug_cmd(), 'DUMP_INVIDX', idx, t), cache)
                 for t in terms},
        'tags': {tag: _live_keys(env, idx, ids, cache)
                 for tag, ids in env.cmd(debug_cmd(), 'DUMP_TAGIDX', idx, 'tag')},
        'numeric': _live_keys(env, idx, env.cmd(debug_cmd(), 'DUMP_NUMIDX', idx, 'num'), cache),
        'geom': _geo_by_key(env, idx, 'geom', cache),
        'geom2': _geo_by_key(env, idx, 'geom2', cache),
    }


def _queries(env, idx):
    """What queries over every field type return from `idx`."""
    def search(query, *args, ordered=False):
        # Without SORTBY the order is doc-id order, which differs between the two indexes.
        res = env.cmd('FT.SEARCH', idx, query, 'NOCONTENT', 'LIMIT', 0, 100, *args)
        return res if ordered else [res[0], *sorted(res[1:])]

    results = {
        'all': search('*'),
        'text': search('@title:alpha'),
        'tag': [search(f'@tag:{{tag{i}}}') for i in range(DOC_COUNT)],
        'numeric': search('@num:[100 110]'),
        # The dump holds ids only, so exact values are probed one document at a time.
        'numeric values': [search(f'@num:[{100 + i} {100 + i}]') for i in range(DOC_COUNT)],
        # Phrases need the terms' positions, which neither the dump nor a single-term query shows.
        'phrase': [search(f'@title:"alpha beta{i}"') for i in range(DOC_COUNT)],
        'geom': search('@geom:[WITHIN $q]', 'PARAMS', 2, 'q', WITHIN_ALL, 'DIALECT', 3),
        'geom2': search('@geom2:[WITHIN $q]', 'PARAMS', 2, 'q', WITHIN_ALL2, 'DIALECT', 3),
    }
    # KNN around every vector, which also checks that no vector is missing or indexed twice.
    for field in ('vec', 'vec2'):
        results[f'knn {field}'] = [
            search(f'*=>[KNN 10 @{field} $b AS s]', 'PARAMS', 2, 'b', _blob(_values(i)[field]),
                   'SORTBY', 's', 'DIALECT', 2, ordered=True)
            for i in range(DOC_COUNT)
        ]
    return results


def _build_alter_and_fresh(env, flavor):
    """Indexes `alt` (created with the pre-existing fields, then ALTERed to add the others) and
    `fresh` (created with the final schema) over the same documents. Returns the internal ids of
    the documents in `alt` before and after the ALTER."""
    conn = getConnectionByEnv(env)
    base = [x for f in ('title', 'geom', 'vec') for x in flavor.schema(f)]
    env.expect('FT.CREATE', 'alt', 'ON', flavor.on, 'SCHEMA', *base).ok()
    for i in range(DOC_COUNT):
        flavor.write(conn, i)
    keys = [f'doc:{i}' for i in range(DOC_COUNT)]
    ids_before = {k: get_internal_id(env, k, 'alt') for k in keys}

    added = [x for f in ADDED for x in flavor.schema(f)]
    env.expect('FT.ALTER', 'alt', 'SCHEMA', 'ADD', *added).ok()
    waitForIndexFinishScan(env, 'alt')

    final = [x for f in ('title', 'geom', 'vec', *ADDED) for x in flavor.schema(f)]
    env.expect('FT.CREATE', 'fresh', 'ON', flavor.on, 'SCHEMA', *final).ok()
    waitForIndexFinishScan(env, 'fresh')

    ids_after = {k: get_internal_id(env, k, 'alt') for k in keys}
    return ids_before, ids_after


def _check_equivalent(env, on):
    ids_before, ids_after = _build_alter_and_fresh(env, Flavor(on))

    # The ALTER did what the comparison below is meant to cover: it skipped the documents without
    # an added field and replaced the rest.
    for key in SKIPPED:
        env.assertEqual(ids_after[key], ids_before[key], message=f'{key} was not skipped')
    for key in REPLACED:
        env.assertGreater(ids_after[key], ids_before[key], message=f'{key} was not replaced')

    alt, fresh = _content(env, 'alt'), _content(env, 'fresh')
    for part in alt:
        env.assertEqual(alt[part], fresh[part], message=f'{on}: {part} differs from a fresh index')
    # Not vacuous: the pre-existing fields still hold every document, the added ones only the
    # documents that carry them.
    env.assertEqual(sorted(alt['geom']), [f'doc:{i}' for i in range(DOC_COUNT)])
    env.assertEqual(sorted(alt['geom2']), ['doc:3', 'doc:5'])
    env.assertEqual(alt['numeric'], ['doc:2', 'doc:5'])
    env.assertEqual(sorted(alt['tags']), ['tag1', 'tag5'])

    alt_queries, fresh_queries = _queries(env, 'alt'), _queries(env, 'fresh')
    for name in alt_queries:
        env.assertEqual(alt_queries[name], fresh_queries[name],
                        message=f'{on}: {name} query differs from a fresh index')
    env.assertEqual(alt_queries['all'][0], DOC_COUNT)
    env.assertEqual(alt_queries['numeric values'],
                    [[1, f'doc:{i}'] if 'num' in ADDED_FIELDS[i] else [0] for i in range(DOC_COUNT)])
    env.assertEqual(alt_queries['phrase'], [[1, f'doc:{i}'] for i in range(DOC_COUNT)])
    env.assertEqual(alt_queries['knn vec'][0][0], DOC_COUNT)
    env.assertEqual(alt_queries['knn vec2'][4], [2, 'doc:4', 'doc:5'])


@skip(cluster=True)
def testAlterMatchesFreshIndexHash(env):
    """FT.ALTER ADD of TAG, NUMERIC, GEOSHAPE and VECTOR fields to an index that already has a
    GEOSHAPE and a VECTOR field leaves the content of a fresh index created with the final schema:
    terms, tags, numeric values, shapes, vectors and query results, for Hash documents."""
    _check_equivalent(env, 'HASH')


@skip(cluster=True, no_json=True)
def testAlterMatchesFreshIndexJson(env):
    """Same as testAlterMatchesFreshIndexHash, for JSON documents indexed through JSONPath."""
    _check_equivalent(env, 'JSON')
