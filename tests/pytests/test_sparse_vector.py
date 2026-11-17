# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Flow tests for SPARSEVECTOR fields and the `@field:[SPARSE $q]` query node."""

import json
import math
import random
import struct

from common import *


def sparse_blob(pairs):
    """The HASH/query encoding: little-endian (u32 dim_id, f32 weight) pairs."""
    return b''.join(struct.pack('<If', i, w) for i, w in pairs)


def f32(x):
    return struct.unpack('<f', struct.pack('<f', x))[0]


def dot(q, d):
    d = dict(d)
    shared = [f32(w) * f32(d[i]) for i, w in q if i in d]
    return sum(shared) if shared else None


def create_index(env, metric='IP', dim=100, on='HASH', path='sp'):
    env.expect('FT.CREATE', 'idx', 'ON', on, 'PREFIX', 1, 'doc:', 'SCHEMA',
               '$.title' if on == 'JSON' else 'title', 'AS', 'title', 'TEXT',
               '$.cat' if on == 'JSON' else 'cat', 'AS', 'cat', 'TAG',
               path, 'AS', 'sp', 'SPARSEVECTOR', 4, 'DIM', dim, 'DISTANCE_METRIC', metric).ok()


def search(env, query, q, *args):
    return env.cmd('FT.SEARCH', 'idx', query, *args, 'PARAMS', 2, 'q', sparse_blob(q), 'DIALECT', 2)


def ids_and_scores(res):
    """`[total, id, score, id, score, ...]` from a NOCONTENT WITHSCORES reply."""
    return [(res[i], float(res[i + 1])) for i in range(1, len(res), 2)]


def testCreateValidation(env):
    enable_unstable_features(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'sp', 'SPARSEVECTOR', 4,
               'DIM', 10, 'DISTANCE_METRIC', 'IP').ok()
    env.expect('FT.CREATE', 'idx2', 'SCHEMA', 'sp', 'SPARSEVECTOR', 6,
               'DISTANCE_METRIC', 'COSINE', 'TYPE', 'FLOAT32', 'DIM', 4294967296).ok()

    for args, msg in [
        (['DIM', 10], 'DISTANCE_METRIC'),
        (['DISTANCE_METRIC', 'IP'], 'DIM'),
        (['DIM', 0, 'DISTANCE_METRIC', 'IP'], 'DIM'),
        (['DIM', 4294967297, 'DISTANCE_METRIC', 'IP'], 'DIM'),
        (['DIM', 10, 'DISTANCE_METRIC', 'L2'], 'DISTANCE_METRIC'),
        (['DIM', 10, 'DISTANCE_METRIC', 'IP', 'TYPE', 'FLOAT64'], 'TYPE'),
        (['DIM', 10, 'DISTANCE_METRIC', 'IP', 'FOO', 'BAR'], 'unknown attribute'),
    ]:
        env.expect('FT.CREATE', 'bad', 'SCHEMA', 'sp', 'SPARSEVECTOR', len(args), *args) \
            .error().contains(msg)
    env.expect('FT.CREATE', 'bad', 'SCHEMA', 'sp', 'SPARSEVECTOR', 3, 'DIM', 10, 'DISTANCE_METRIC') \
        .error().contains('attribute-value pairs')


def testInfo(env):
    enable_unstable_features(env)
    create_index(env, metric='COSINE', dim=30522)
    sp = [a for a in index_info(env)['attributes'] if 'sp' in a][0]
    env.assertEqual(sp, ['identifier', 'sp', 'attribute', 'sp', 'type', 'SPARSEVECTOR',
                         'data_type', 'FLOAT32', 'dim', 30522, 'distance_metric', 'COSINE'])


def testTopKMatchesBruteForce(env):
    """The top LIMIT results by the default score are the exact best dot products."""
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)
    create_index(env, dim=200)
    rnd = random.Random(1234)
    docs = {}
    for n in range(300):
        dims = sorted(rnd.sample(range(200), rnd.randint(1, 12)))
        docs[f'doc:{n}'] = [(i, f32(rnd.uniform(0.01, 3.0))) for i in dims]
        conn.execute_command('HSET', f'doc:{n}', 'sp', sparse_blob(docs[f'doc:{n}']))

    for _ in range(10):
        q = [(i, f32(rnd.uniform(0.01, 2.0))) for i in sorted(rnd.sample(range(200), 8))]
        expected = sorted(((d, s) for d, s in ((k, dot(q, v)) for k, v in docs.items())
                           if s is not None), key=lambda x: -x[1])
        res = search(env, '@sp:[SPARSE $q]', q, 'WITHSCORES', 'NOCONTENT', 'LIMIT', 0, 10)
        # total_results is the whole union, as for a full-text OR query.
        env.assertEqual(res[0], len(expected))
        got = ids_and_scores(res)
        env.assertEqual([d for d, _ in got], [d for d, _ in expected[:10]])
        for (_, g), (_, e) in zip(got, expected):
            env.assertAlmostEqual(g, e, 1e-4)


def testPartialOverlapRanksFirst(env):
    """A document with one of the query dimensions can still be the best match."""
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)
    create_index(env)
    conn.execute_command('HSET', 'doc:1', 'sp', sparse_blob([(1, 10.0)]))
    conn.execute_command('HSET', 'doc:2', 'sp', sparse_blob([(1, 0.1), (2, 0.1), (3, 0.1)]))
    conn.execute_command('HSET', 'doc:3', 'sp', sparse_blob([(50, 1.0)]))
    res = search(env, '@sp:[SPARSE $q]', [(1, 1.0), (2, 1.0), (3, 1.0)], 'WITHSCORES', 'NOCONTENT')
    env.assertEqual(res[0], 2)
    got = ids_and_scores(res)
    env.assertEqual([d for d, _ in got], ['doc:1', 'doc:2'])
    env.assertAlmostEqual(got[0][1], 10.0, 1e-6)
    env.assertAlmostEqual(got[1][1], 0.3, 1e-6)

    # Only dimensions no document has: no results, not an error.
    env.assertEqual(search(env, '@sp:[SPARSE $q]', [(77, 1.0)]), [0])


def testSparseScorerCannotBeNamed(env):
    """SPARSE is only ever chosen by the query itself: as a default scorer it
    would score every text query 0, and naming it adds nothing."""
    enable_unstable_features(env)
    create_index(env)
    error = 'selected automatically for sparse vector queries'
    env.expect('CONFIG', 'SET', 'search-default-scorer', 'SPARSE').error().contains(error)
    env.expect(config_cmd(), 'SET', 'DEFAULT_SCORER', 'SPARSE').error().contains(error)
    for query in ['hello', '@sp:[SPARSE $q]']:
        env.expect('FT.SEARCH', 'idx', query, 'SCORER', 'SPARSE', 'PARAMS', 2,
                   'q', sparse_blob([(1, 1.0)]), 'DIALECT', 2).error().contains(error)
    env.expect('FT.AGGREGATE', 'idx', 'hello', 'SCORER', 'SPARSE', 'ADDSCORES',
               'DIALECT', 2).error().contains(error)


def testCombinedWithOtherClauses(env):
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)
    create_index(env)
    conn.execute_command('HSET', 'doc:1', 'title', 'running shoes', 'cat', 'footwear',
                         'sp', sparse_blob([(1, 3.0)]))
    conn.execute_command('HSET', 'doc:2', 'title', 'running shirt', 'cat', 'apparel',
                         'sp', sparse_blob([(1, 5.0)]))
    conn.execute_command('HSET', 'doc:3', 'title', 'boots', 'cat', 'footwear',
                         'sp', sparse_blob([(1, 1.0), (2, 1.0)]))
    q = [(1, 1.0), (2, 1.0)]

    # A filter restricts the candidates exactly; the score is still the dot product.
    res = search(env, '@cat:{footwear} @sp:[SPARSE $q]', q, 'WITHSCORES', 'NOCONTENT')
    env.assertEqual(ids_and_scores(res), [('doc:1', 3.0), ('doc:3', 2.0)])

    # With a text clause the default score stays the sparse score alone.
    res = search(env, '@title:running @sp:[SPARSE $q]', q, 'WITHSCORES', 'NOCONTENT')
    env.assertEqual(ids_and_scores(res), [('doc:2', 5.0), ('doc:1', 3.0)])

    # An explicit SCORER ranks by the text clauses instead.
    res = search(env, '@title:running @sp:[SPARSE $q]', q, 'SCORER', 'BM25STD', 'WITHSCORES',
                 'NOCONTENT')
    env.assertEqual(res[0], 2)
    env.assertNotEqual(sorted(s for _, s in ids_and_scores(res)), [3.0, 5.0])

    # Inside a union, with a negation, and as an optional clause.
    res = search(env, '(@sp:[SPARSE $q]) -@cat:{apparel}', q, 'NOCONTENT')
    env.assertEqual(sorted(res[1:]), ['doc:1', 'doc:3'])
    res = search(env, '@title:boots | @sp:[SPARSE $q]', q, 'NOCONTENT')
    env.assertEqual(res[0], 3)


def testYieldScoreAs(env):
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)
    create_index(env)
    conn.execute_command('HSET', 'doc:1', 'title', 'a', 'sp', sparse_blob([(1, 3.0)]))
    conn.execute_command('HSET', 'doc:2', 'title', 'b', 'sp', sparse_blob([(1, 1.0)]))
    conn.execute_command('HSET', 'doc:3', 'title', 'c', 'sp', sparse_blob([(1, 2.0)]))
    q = [(1, 2.0)]

    res = search(env, '@sp:[SPARSE $q]=>{$YIELD_SCORE_AS: s}', q, 'RETURN', 1, 's',
                 'SORTBY', 's', 'ASC')
    env.assertEqual(res, [3, 'doc:2', ['s', '2'], 'doc:3', ['s', '4'], 'doc:1', ['s', '6']])

    res = env.cmd('FT.AGGREGATE', 'idx', '@sp:[SPARSE $q]=>{$YIELD_SCORE_AS: s}',
                  'LOAD', 1, '@title', 'SORTBY', 2, '@s', 'DESC',
                  'PARAMS', 2, 'q', sparse_blob(q), 'DIALECT', 2)
    # Rows in score order; the field order within a row differs in cluster mode.
    env.assertEqual([to_dict(row) for row in res[1:]],
                    [{'s': '6', 'title': 'a'}, {'s': '4', 'title': 'c'}, {'s': '2', 'title': 'b'}])

    # A query that matches nothing still knows the score's name, so SORTBY on it
    # returns an empty result instead of an unknown-property error.
    res = search(env, '@sp:[SPARSE $q]=>{$YIELD_SCORE_AS: s}', [(77, 1.0)], 'SORTBY', 's', 'DESC')
    env.assertEqual(res, [0])

    # The score name may not shadow a schema field.
    env.expect('FT.SEARCH', 'idx', '@sp:[SPARSE $q]=>{$YIELD_SCORE_AS: title}',
               'PARAMS', 2, 'q', sparse_blob(q), 'DIALECT', 2).error().contains('already exists')


def testInvalidQueries(env):
    enable_unstable_features(env)
    create_index(env, dim=100)
    for q, msg in [
        (sparse_blob([(100, 1.0)]), 'out of range'),
        (sparse_blob([(2, 1.0), (1, 1.0)]), 'strictly increasing'),
        (sparse_blob([(2, 1.0), (2, 1.0)]), 'strictly increasing'),
        (sparse_blob([(1, float('nan'))]), 'not finite'),
        (b'abc', 'multiple of 8'),
    ]:
        env.expect('FT.SEARCH', 'idx', '@sp:[SPARSE $q]', 'PARAMS', 2, 'q', q, 'DIALECT', 2) \
            .error().contains(msg)
    env.expect('FT.SEARCH', 'idx', '@title:[SPARSE $q]', 'PARAMS', 2, 'q',
               sparse_blob([(1, 1.0)]), 'DIALECT', 2).error().contains('Expected a SPARSEVECTOR field')
    env.expect('FT.SEARCH', 'idx', '@sp:[DENSE $q]', 'PARAMS', 2, 'q',
               sparse_blob([(1, 1.0)]), 'DIALECT', 2).error().contains('Syntax error')


@skip(cluster=True)
def testInvalidDocuments(env):
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)
    create_index(env, dim=100)
    conn.execute_command('HSET', 'doc:ok', 'sp', sparse_blob([(1, 1.0)]))
    bad = [
        sparse_blob([(100, 1.0)]),
        sparse_blob([(5, 1.0), (4, 1.0)]),
        sparse_blob([(1, float('inf'))]),
        b'\x01\x02\x03',
    ]
    for n, v in enumerate(bad):
        conn.execute_command('HSET', f'doc:bad{n}', 'sp', v)
    info = index_info(env)
    env.assertEqual(int(info['num_docs']), 1)
    env.assertEqual(int(info['hash_indexing_failures']), len(bad))
    # Zero weights are dropped, so a vector of zeros indexes nothing under IP.
    conn.execute_command('HSET', 'doc:zero', 'sp', sparse_blob([(1, 0.0)]))
    res = search(env, '@sp:[SPARSE $q]', [(1, 1.0)], 'NOCONTENT')
    env.assertEqual(res, [1, 'doc:ok'])


def testUpdateAndDelete(env):
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)
    create_index(env)
    conn.execute_command('HSET', 'doc:1', 'sp', sparse_blob([(1, 5.0)]))
    conn.execute_command('HSET', 'doc:2', 'sp', sparse_blob([(1, 1.0)]))
    conn.execute_command('HSET', 'doc:3', 'sp', sparse_blob([(1, 3.0)]))
    q = [(1, 1.0), (2, 1.0)]

    # An update replaces the whole vector; a delete removes the document.
    conn.execute_command('HSET', 'doc:1', 'sp', sparse_blob([(2, 0.5)]))
    conn.execute_command('DEL', 'doc:3')
    res = search(env, '@sp:[SPARSE $q]', q, 'WITHSCORES', 'NOCONTENT')
    env.assertEqual(ids_and_scores(res), [('doc:2', 1.0), ('doc:1', 0.5)])

    # Removing the field from the hash removes the document from the field.
    conn.execute_command('HSET', 'doc:2', 'title', 'x')
    conn.execute_command('HDEL', 'doc:2', 'sp')
    res = search(env, '@sp:[SPARSE $q]', q, 'NOCONTENT')
    env.assertEqual(res, [1, 'doc:1'])


def testCosine(env):
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)
    create_index(env, metric='COSINE', dim=10)
    conn.execute_command('HSET', 'doc:1', 'sp', sparse_blob([(1, 3.0), (2, 4.0)]))
    conn.execute_command('HSET', 'doc:2', 'sp', sparse_blob([(1, 100.0)]))
    # Magnitude does not count: doc:2 points exactly along the query.
    res = search(env, '@sp:[SPARSE $q]', [(1, 5.0)], 'WITHSCORES', 'NOCONTENT')
    got = ids_and_scores(res)
    env.assertEqual([d for d, _ in got], ['doc:2', 'doc:1'])
    env.assertAlmostEqual(got[0][1], 1.0, 1e-6)
    env.assertAlmostEqual(got[1][1], 0.6, 1e-6)
    # A zero vector has no direction.
    env.expect('FT.SEARCH', 'idx', '@sp:[SPARSE $q]', 'PARAMS', 2, 'q', sparse_blob([]),
               'DIALECT', 2).error().contains('zero norm')


def testRdbReload(env):
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)
    create_index(env, metric='COSINE', dim=50)
    conn.execute_command('HSET', 'doc:1', 'sp', sparse_blob([(1, 1.0)]))
    conn.execute_command('HSET', 'doc:2', 'sp', sparse_blob([(1, 1.0), (2, 1.0)]))
    for _ in env.reloadingIterator():
        waitForIndex(env)
        sp = [a for a in index_info(env)['attributes'] if 'sp' in a][0]
        env.assertEqual(sp[-4:], ['dim', 50, 'distance_metric', 'COSINE'])
        res = search(env, '@sp:[SPARSE $q]', [(1, 1.0)], 'NOCONTENT')
        env.assertEqual(res, [2, 'doc:1', 'doc:2'])
        # DIM still applies after the reload.
        env.expect('FT.SEARCH', 'idx', '@sp:[SPARSE $q]', 'PARAMS', 2, 'q',
                   sparse_blob([(50, 1.0)]), 'DIALECT', 2).error().contains('out of range')


@skip(cluster=True)
def testProfileAndExplain(env):
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)
    create_index(env)
    conn.execute_command('HSET', 'doc:1', 'sp', sparse_blob([(1, 1.0), (2, 1.0)]))
    q = sparse_blob([(1, 1.0), (2, 1.0), (3, 1.0)])
    env.expect('FT.EXPLAIN', 'idx', '@sp:[SPARSE $q]=>{$YIELD_SCORE_AS: s}', 'PARAMS', 2, 'q', q,
               'DIALECT', 2).equal('SPARSE_VECTOR{@sp: 3 dimensions} => {$YIELD_SCORE_AS: s}\n')
    res = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '@sp:[SPARSE $q]', 'NOCONTENT',
                  'PARAMS', 2, 'q', q, 'DIALECT', 2)
    iterators = to_dict(res[1][1][0])['Iterators profile']
    it = to_dict(iterators)
    env.assertEqual(it['Type'], 'SPARSE_VECTOR')
    # Dimension 3 has no posting list, so only two of the query's three are iterated.
    env.assertEqual(it['Query dimensions'], '3')
    env.assertEqual(it['Dimensions'], '2')


def sparse_field_stats(env, field='sp'):
    stats = [to_dict(f) for f in index_info(env)['field statistics']]
    return [f for f in stats if f['attribute'] == field][0]


def testInfoFieldStatistics(env):
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)
    create_index(env)
    # A field no document has written to yet reports zeros.
    stats = sparse_field_stats(env)
    env.assertEqual([stats['memory'], stats['num_dimensions'], stats['num_records']], [0, 0, 0])
    env.assertEqual(float(index_info(env)['sparse_vector_index_sz_mb']), 0)

    conn.execute_command('HSET', 'doc:1', 'sp', sparse_blob([(1, 1.0), (2, 1.0)]))
    conn.execute_command('HSET', 'doc:2', 'sp', sparse_blob([(2, 1.0), (3, 1.0)]))
    conn.execute_command('HSET', 'doc:3', 'title', 'no vector')
    stats = sparse_field_stats(env)
    env.assertEqual(stats['num_records'], 4)
    # In a cluster each shard counts the dimensions it holds, so a dimension shared by documents
    # on different shards is counted once per shard.
    env.assertGreaterEqual(stats['num_dimensions'], 3)
    env.assertLessEqual(stats['num_dimensions'], 4)
    env.assertGreater(stats['memory'], 0)

    info = index_info(env)
    sparse_mb = float(info['sparse_vector_index_sz_mb'])
    env.assertAlmostEqual(sparse_mb, stats['memory'] / float(0x100000), 1e-6)
    # Sparse posting lists are part of the inverted index size, not reported on top of it.
    env.assertLessEqual(sparse_mb, float(info['inverted_sz_mb']))
    # The text and tag fields report no sparse metrics.
    title = [to_dict(f) for f in info['field statistics'] if to_dict(f)['attribute'] == 'title'][0]
    env.assertFalse('num_dimensions' in title)


@skip(cluster=True)
def testInfoModules(env):
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)
    create_index(env)
    ops_before = conn.execute_command('INFO', 'MODULES')[
        'search_total_indexing_ops_sparse_vector_fields']
    conn.execute_command('HSET', 'doc:1', 'sp', sparse_blob([(1, 1.0), (2, 1.0)]))
    conn.execute_command('HSET', 'doc:2', 'sp', sparse_blob([(2, 1.0), (3, 1.0)]))
    conn.execute_command('HSET', 'doc:bad', 'sp', sparse_blob([(500, 1.0)]))
    info = conn.execute_command('INFO', 'MODULES')
    env.assertEqual(info['search_fields_sparse_vector'], {'SparseVector': 1, 'IndexErrors': 1})
    # A rejected document is an index error, not an indexing operation.
    env.assertEqual(info['search_total_indexing_ops_sparse_vector_fields'] - ops_before, 2)
    env.assertEqual(info['search_sparse_vector_dimensions'], 3)
    env.assertEqual(info['search_sparse_vector_records'], 4)
    env.assertEqual(info['search_used_memory_sparse_vector_index'],
                    sparse_field_stats(env)['memory'])
    env.assertLess(info['search_used_memory_sparse_vector_index'],
                   info['search_used_memory_indexes'])

    env.expect('FT.DROPINDEX', 'idx').ok()
    info = conn.execute_command('INFO', 'MODULES')
    env.assertFalse('search_fields_sparse_vector' in info)


@skip(cluster=True)
def testDebugSparseIndex(env):
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)
    create_index(env)
    # Before any document the field has no index, and reads as empty.
    env.expect(debug_cmd(), 'INFO_SPARSEIDX', 'idx', 'sp').equal(
        ['num_dimensions', 0, 'num_records', 0, 'memory', 0, 'longest_dimensions', []])
    env.expect(debug_cmd(), 'DUMP_SPARSEIDX', 'idx', 'sp', 1).equal([])

    conn.execute_command('HSET', 'doc:1', 'sp', sparse_blob([(1, 0.5), (7, 1.0)]))
    conn.execute_command('HSET', 'doc:2', 'sp', sparse_blob([(7, 2.0)]))
    conn.execute_command('HSET', 'doc:3', 'sp', sparse_blob([(3, 1.0), (7, 0.25)]))

    res = to_dict(env.cmd(debug_cmd(), 'INFO_SPARSEIDX', 'idx', 'sp'))
    env.assertEqual([res['num_dimensions'], res['num_records']], [3, 5])
    env.assertEqual(res['memory'], sparse_field_stats(env)['memory'])
    longest = [to_dict(d) for d in res['longest_dimensions']]
    # Longest first; dimensions 1 and 3 tie and are ordered by ID.
    env.assertEqual([(d['dim_id'], d['num_entries']) for d in longest], [(7, 3), (1, 1), (3, 1)])
    env.assertEqual([d['num_blocks'] for d in longest], [1, 1, 1])

    res = to_dict(env.cmd(debug_cmd(), 'INFO_SPARSEIDX', 'idx', 'sp', 'TOP', 1))
    env.assertEqual([to_dict(d)['dim_id'] for d in res['longest_dimensions']], [7])

    # Doc IDs follow insertion order here: doc:1 is 1, doc:3 is 3.
    env.expect(debug_cmd(), 'DUMP_SPARSEIDX', 'idx', 'sp', 7).equal(
        [1, '1', 2, '2', 3, '0.25'])
    env.expect(debug_cmd(), 'DUMP_SPARSEIDX', 'idx', 'sp', 99).equal([])

    env.expect(debug_cmd(), 'INFO_SPARSEIDX', 'idx', 'title').error().contains(
        'Could not find given field')
    env.expect(debug_cmd(), 'INFO_SPARSEIDX', 'idx', 'sp', 'TOP', -1).error().contains(
        'Expected TOP')
    env.expect(debug_cmd(), 'INFO_SPARSEIDX', 'idx', 'sp', 'LIMIT', 1).error().contains(
        'Expected TOP')
    env.expect(debug_cmd(), 'DUMP_SPARSEIDX', 'idx', 'sp', 2**32).error().contains(
        'Invalid dimension ID')
    env.expect(debug_cmd(), 'DUMP_SPARSEIDX', 'idx', 'sp', 'x').error().contains(
        'Invalid dimension ID')
    env.expect(debug_cmd(), 'DUMP_SPARSEIDX', 'idx', 'nope', 1).error().contains(
        'Could not find given field')


@skip(no_json=True)
def testJson(env):
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)
    create_index(env, on='JSON', path='$.sp')
    conn.execute_command('JSON.SET', 'doc:1', '$', json.dumps(
        {'title': 'a', 'cat': 'x', 'sp': {'indices': [3, 10], 'values': [0.5, 2]}}))
    conn.execute_command('JSON.SET', 'doc:2', '$', json.dumps(
        {'title': 'b', 'cat': 'x', 'sp': {'indices': [10], 'values': [1.5]}}))
    res = search(env, '@sp:[SPARSE $q]', [(3, 2.0), (10, 1.0)], 'WITHSCORES', 'NOCONTENT')
    env.assertEqual(ids_and_scores(res), [('doc:1', 3.0), ('doc:2', 1.5)])


@skip(no_json=True, cluster=True)
def testJsonInvalidValues(env):
    """Each malformed value is rejected as an indexing failure with its own error,
    rather than indexed as something else (an empty or reordered vector)."""
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)
    create_index(env, on='JSON', path='$.sp')
    for n, (bad, error) in enumerate([
        ({'indices': [1, 2], 'values': [1.0]}, 'must have the same length'),
        ({'indices': [1.5], 'values': [1.0]}, 'Invalid sparse vector index at index 0'),
        ({'indices': [-1], 'values': [1.0]}, 'Invalid sparse vector index at index 0'),
        ({'indices': [1]}, 'must have `indices` and `values` arrays'),
        ({'indices': [2, 1], 'values': [1.0, 1.0]}, 'strictly increasing'),
        ([1, 2], 'Unsupported field type'),
    ], start=1):
        conn.execute_command('JSON.SET', f'doc:bad{n}', '$', json.dumps({'sp': bad}))
        errors = to_dict(index_info(env)['Index Errors'])
        env.assertEqual(errors['indexing failures'], n, message=bad)
        env.assertContains(error, errors['last indexing error'], message=bad)
        env.assertEqual(errors['last indexing error key'], f'doc:bad{n}', message=bad)
    env.assertEqual(index_info(env)['num_docs'], 0)


def check_gc_collects_sparse_entries(env):
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)
    env.expect(config_cmd(), 'SET', 'FORK_GC_CLEAN_THRESHOLD', 0).ok()
    create_index(env, dim=200)
    # Every doc shares one of ten common dimensions, and has one of its own.
    for n in range(60):
        conn.execute_command('HSET', f'doc:{n}', 'sp', sparse_blob([(n % 10, 1.0), (100 + n, 2.0)]))
    q = [(i, 1.0) for i in range(10)]
    records_before = int(index_info(env)['num_records'])
    blocks_before = int(index_info(env)['total_inverted_index_blocks'])
    env.assertEqual(records_before, 120)
    stats_before = sparse_field_stats(env)
    env.assertEqual(stats_before['num_dimensions'], 70)

    # Delete the even docs and give doc:1 a new vector (an update is a new doc ID).
    for n in range(0, 60, 2):
        conn.execute_command('DEL', f'doc:{n}')
    conn.execute_command('HSET', 'doc:1', 'sp', sparse_blob([(1, 3.0)]))

    forceInvokeGC(env)

    # 29 untouched odd docs keep 2 entries each; doc:1 now has 1.
    info = index_info(env)
    env.assertEqual(int(info['num_records']), 29 * 2 + 1)
    env.assertLess(int(info['total_inverted_index_blocks']), blocks_before)
    # The field's own counters follow GC. Gone are the 30 own dimensions of the deleted even docs,
    # the 5 even common dimensions only they had, and dimension 101, which doc:1 dropped on update.
    stats = sparse_field_stats(env)
    env.assertEqual(stats['num_records'], 29 * 2 + 1)
    env.assertEqual(stats['num_dimensions'], 70 - 30 - 5 - 1)
    env.assertLess(stats['memory'], stats_before['memory'])

    res = search(env, '@sp:[SPARSE $q]', q, 'WITHSCORES', 'NOCONTENT', 'LIMIT', 0, 100)
    env.assertEqual(res[0], 30)
    got = dict(ids_and_scores(res))
    env.assertEqual(got.pop('doc:1'), 3.0)
    env.assertEqual(sorted(got), sorted(f'doc:{n}' for n in range(3, 60, 2)))
    # A dimension only a deleted doc had is gone, and one an updated doc dropped is too.
    env.assertEqual(search(env, '@sp:[SPARSE $q]', [(100, 1.0), (101, 1.0)]), [0])
    # A second cycle with nothing new to collect changes nothing.
    forceInvokeGC(env)
    env.assertEqual(int(index_info(env)['num_records']), 29 * 2 + 1)


@skip(cluster=True)
def testGcFork(env):
    check_gc_collects_sparse_entries(env)


@skip(cluster=True)
def testGcThread():
    check_gc_collects_sparse_entries(Env(moduleArgs='GC_POLICY THREAD'))


def schema_fingerprint(env, idx='idx'):
    con = env.getConnection()
    con.execute_command('DEBUG', 'MARK-INTERNAL-CLIENT')
    return dict(con.execute_command('_FT._LIST', 'WITHCLUSTERSTATE')[3])[idx]


@skip(cluster=True)
def testSchemaFingerprintCoversSparseAttributes(env):
    """Shards must not report schema agreement when they disagree on DIM or the
    metric, since those decide how a query vector is checked and scored."""
    enable_unstable_features(env)
    fingerprints = set()
    for dim, metric in [(10, 'IP'), (11, 'IP'), (10, 'COSINE')]:
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 'sp', 'SPARSEVECTOR', 4,
                   'DIM', dim, 'DISTANCE_METRIC', metric).ok()
        fingerprints.add(schema_fingerprint(env))
        env.expect('FT.DROPINDEX', 'idx').ok()
    env.assertEqual(len(fingerprints), 3)


def testHybridRejectsSparseQueries(env):
    """Sparse vector queries are not supported in FT.HYBRID yet, in either clause."""
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'sp', 'SPARSEVECTOR', 4, 'DIM', 10, 'DISTANCE_METRIC', 'IP',
               'emb', 'VECTOR', 'FLAT', 6, 'TYPE', 'FLOAT32', 'DIM', 2, 'DISTANCE_METRIC', 'L2').ok()
    conn.execute_command('HSET', 'doc:1', 'sp', sparse_blob([(1, 1.0)]),
                         'emb', struct.pack('<2f', 1.0, 1.0))
    params = ['PARAMS', 4, 'q', sparse_blob([(1, 1.0)]), 'v', struct.pack('<2f', 1.0, 1.0)]
    env.expect('FT.HYBRID', 'idx', 'SEARCH', '@sp:[SPARSE $q]', 'VSIM', '@emb', '$v', *params) \
        .error().contains('SPARSE vector queries are not supported in FT.HYBRID SEARCH')
    env.expect('FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM', '@emb', '$v', 'FILTER', '@sp:[SPARSE $q]',
               *params).error().contains('SPARSE vector queries are not supported in FT.HYBRID VSIM FILTER')
    # The same index still serves FT.HYBRID without a sparse clause.
    res = env.cmd('FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM', '@emb', '$v', *params)
    env.assertEqual(to_dict(res)['total_results'], 1)


def testDenseRangeDistanceIsNotASparseScore(env):
    """A dense VECTOR_RANGE clause yielding its distance does not change the
    default sparse score of the documents it also matches."""
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'sp', 'SPARSEVECTOR', 4, 'DIM', 10, 'DISTANCE_METRIC', 'IP',
               'r', 'VECTOR', 'FLAT', 6, 'TYPE', 'FLOAT32', 'DIM', 2, 'DISTANCE_METRIC', 'L2').ok()
    conn.execute_command('HSET', 'doc:a', 'sp', sparse_blob([(1, 3.0)]), 'r', struct.pack('<2f', 0, 0))
    conn.execute_command('HSET', 'doc:b', 'sp', sparse_blob([(1, 2.0)]), 'r', struct.pack('<2f', 10, 0))
    res = env.cmd('FT.SEARCH', 'idx', '@sp:[SPARSE $q] @r:[VECTOR_RANGE 200 $v]=>{$YIELD_DISTANCE_AS: d}',
                  'WITHSCORES', 'RETURN', 1, 'd',
                  'PARAMS', 4, 'q', sparse_blob([(1, 1.0)]), 'v', struct.pack('<2f', 0, 0), 'DIALECT', 2)
    # Scores are the dot products alone (3 and 2), not 3 + 0 and 2 + 100.
    env.assertEqual(res[0], 2)
    env.assertEqual([res[1], float(res[2]), res[4], float(res[5])], ['doc:a', 3.0, 'doc:b', 2.0])
    env.assertEqual([float(res[3][1]), float(res[6][1])], [0.0, 100.0])


# ---------------------------------------------------------------------------
# ENABLE_UNSTABLE_FEATURES gate
# ---------------------------------------------------------------------------

GATE_ERROR = 'SPARSEVECTOR is unavailable when `ENABLE_UNSTABLE_FEATURES` is off'


def set_unstable_features(env, on):
    run_command_on_all_shards(env, 'CONFIG', 'SET', 'search-enable-unstable-features',
                              'yes' if on else 'no')


def sparse_info_modules_keys(conn):
    return sorted(k for k in conn.execute_command('INFO', 'MODULES') if 'sparse' in k)


def testRequiresUnstableFeatures(env):
    """With the flag off, a sparse field cannot be created or queried, and an
    index without one reports exactly what it reported before the feature."""
    set_unstable_features(env, False)
    conn = getConnectionByEnv(env)
    sparse = ['sp', 'SPARSEVECTOR', 4, 'DIM', 10, 'DISTANCE_METRIC', 'IP']

    env.expect('FT.CREATE', 'idx', 'SCHEMA', *sparse).error().contains(GATE_ERROR)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT').ok()
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', *sparse).error().contains(GATE_ERROR)
    env.assertEqual(len(index_info(env)['attributes']), 1)
    env.expect('FT.SEARCH', 'idx', '@title:[SPARSE $q]', 'PARAMS', 2, 'q',
               sparse_blob([(1, 1.0)]), 'DIALECT', 2).error().contains(GATE_ERROR)

    env.assertFalse('sparse_vector_index_sz_mb' in index_info(env))
    env.assertEqual(sparse_info_modules_keys(conn), [])


def check_sparse_index_after_reload(env, expected_docs):
    """The reloaded index has its attributes and its documents."""
    waitForIndex(env)
    sp = [a for a in index_info(env)['attributes'] if 'sp' in a][0]
    env.assertEqual(sp[-4:], ['dim', 100, 'distance_metric', 'COSINE'])
    env.assertEqual(index_info(env)['num_docs'], len(expected_docs))
    env.assertEqual(sparse_field_stats(env)['num_records'], 2 * len(expected_docs))
    # Observability follows the field, not the flag.
    env.assertTrue('sparse_vector_index_sz_mb' in index_info(env))


def index_sparse_docs(conn, ids):
    for i in ids:
        conn.execute_command('HSET', f'doc:{i}', 'sp', sparse_blob([(1, 1.0), (i + 10, 1.0)]))


def search_ids(env):
    res = search(env, '@sp:[SPARSE $q]', [(1, 1.0)], 'NOCONTENT', 'LIMIT', 0, 100)
    return sorted(res[1:])


def testRdbSavedWithFlagOnLoadsWithFlagOff(env):
    """Turning the flag off must not leave an index created under it unloadable
    or stale: it loads, keeps indexing, and only querying is refused."""
    set_unstable_features(env, True)
    conn = getConnectionByEnv(env)
    create_index(env, metric='COSINE')
    index_sparse_docs(conn, [1, 2])

    env.broadcast('SAVE')
    set_unstable_features(env, False)
    env.broadcast('DEBUG RELOAD NOSAVE')
    check_sparse_index_after_reload(env, [1, 2])

    # Indexing is not gated: a document written now is in the index.
    index_sparse_docs(conn, [3])
    env.assertEqual(sparse_field_stats(env)['num_records'], 6)
    env.expect('FT.SEARCH', 'idx', '@sp:[SPARSE $q]', 'PARAMS', 2, 'q',
               sparse_blob([(1, 1.0)]), 'DIALECT', 2).error().contains(GATE_ERROR)

    set_unstable_features(env, True)
    env.assertEqual(search_ids(env), ['doc:1', 'doc:2', 'doc:3'])


def testRdbSavedWithFlagOffLoadsWithFlagOn(env):
    """An RDB written with the flag off, holding a sparse index and a plain one,
    loads with the flag on, including documents indexed while it was off."""
    set_unstable_features(env, True)
    conn = getConnectionByEnv(env)
    create_index(env, metric='COSINE')
    env.expect('FT.CREATE', 'plain', 'PREFIX', 1, 'plain:', 'SCHEMA', 't', 'TEXT').ok()
    index_sparse_docs(conn, [1])

    set_unstable_features(env, False)
    index_sparse_docs(conn, [2])
    conn.execute_command('HSET', 'plain:1', 't', 'hello')
    env.broadcast('SAVE')
    set_unstable_features(env, True)
    env.broadcast('DEBUG RELOAD NOSAVE')

    check_sparse_index_after_reload(env, [1, 2])
    env.assertEqual(search_ids(env), ['doc:1', 'doc:2'])
    waitForIndex(env, 'plain')
    env.assertEqual(env.cmd('FT.SEARCH', 'plain', 'hello', 'NOCONTENT'), [1, 'plain:1'])
    env.assertFalse('sparse_vector_index_sz_mb' in index_info(env, 'plain'))


@skip(cluster=True)
def testRestartResetsFlagButKeepsSparseIndex(env):
    """`CONFIG SET` is not persisted, so a restart comes back with the flag off:
    the most likely way a user ends up loading sparse data with it off."""
    set_unstable_features(env, True)
    conn = getConnectionByEnv(env)
    create_index(env, metric='COSINE')
    index_sparse_docs(conn, [1, 2])

    env.restartAndReload()
    env.assertEqual(env.cmd('CONFIG', 'GET', 'search-enable-unstable-features'),
                    ['search-enable-unstable-features', 'no'])
    check_sparse_index_after_reload(env, [1, 2])
    env.expect('FT.SEARCH', 'idx', '@sp:[SPARSE $q]', 'PARAMS', 2, 'q',
               sparse_blob([(1, 1.0)]), 'DIALECT', 2).error().contains(GATE_ERROR)

    set_unstable_features(env, True)
    env.assertEqual(search_ids(env), ['doc:1', 'doc:2'])
