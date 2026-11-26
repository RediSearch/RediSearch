# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Index loading on TAG clauses: `@t:{a|b} => {$as: alias}` puts the tag the clause matched into
each result as `alias`, read from the index rather than the document. An unstable feature; the
NUMERIC side is in test_index_loading.py."""

from common import *
from index_loading_utils import (aggregate_rows, iterators_profile, set_unstable_features,
                                 with_unstable_features)

# id -> t. `t` uses the default `,` separator and folds case; `cs` holds the same values and keeps
# their case, and `st` holds them with a suffix trie.
DOCS = {1: 'red', 2: 'blue', 3: 'Green', 4: 'red,green', 5: 'yellow', 6: 'blue'}


def setup_hash_index(env):
    env.flush()
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'PREFIX', '1', 'doc:', 'SCHEMA',
               't', 'TAG', 'cs', 'TAG', 'CASESENSITIVE', 'st', 'TAG', 'WITHSUFFIXTRIE',
               'n', 'NUMERIC', 'id', 'NUMERIC', 'SORTABLE', 'title', 'TEXT').ok()
    for i, t in DOCS.items():
        conn.execute_command('HSET', f'doc:{i}', 't', t, 'cs', t, 'st', t, 'n', str(i * 10),
                             'id', str(i), 'title', 'hello')
    return conn


def loaded(env, query, *args):
    """{id: the alias `c`, or None} for every row of `query`, in id order."""
    rows = aggregate_rows(env, query, 'LOAD', '1', '@id', *args, 'SORTBY', '2', '@id', 'ASC')
    return [(int(row['id']), row.get('c')) for row in rows]


def setup_vector_index(env):
    """An index of DOCS' tags and of a vector per document, doc:i at distance i² from the origin,
    returned as the query blob."""
    env.flush()
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TAG', 'vec', 'VECTOR', 'FLAT', '6',
               'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()
    for i, t in DOCS.items():
        conn.execute_command('HSET', f'doc:{i}', 't', t,
                             'vec', np.array([float(i), 0.0]).astype(np.float32).tobytes())
    return np.array([0.0, 0.0]).astype(np.float32).tobytes()


def hybrid_aliases(res):
    """{key: the alias `c`} for every FT.HYBRID result that has it."""
    results, _ = get_results_from_hybrid_response(res)
    return {key: fields['c'] for key, fields in results.items() if 'c' in fields}


@with_unstable_features
def testTagRequiresUnstableFeatures(env):
    """With the gate off, `$as` on a TAG clause is rejected with an error naming the config that
    enables it."""
    setup_hash_index(env)
    set_unstable_features(env, False)
    env.expect('FT.AGGREGATE', 'idx', '@t:{red} => {$as: c}').error() \
        .contains('`CONFIG SET search-enable-unstable-features yes`')

    set_unstable_features(env, True)
    env.assertEqual(loaded(env, '@t:{red} => {$as: c}'), [(1, 'red'), (4, 'red')])


@skip(cluster=True)
@with_unstable_features
def testTagLoadsTheValueThatMatched(env):
    """An IN-list loads, for each document, the value of the list it matched."""
    setup_hash_index(env)
    env.assertEqual(loaded(env, '@t:{red|blue} => {$as: c}'),
                    [(1, 'red'), (2, 'blue'), (4, 'red'), (6, 'blue')])


@skip(cluster=True)
@with_unstable_features
def testTagLoadsTheIndexedValue(env):
    """The loaded value is the tag as the index holds it: case-folded without CASESENSITIVE, and
    the piece of a separated value that matched."""
    setup_hash_index(env)
    env.assertEqual(loaded(env, '@t:{GREEN} => {$as: c}'), [(3, 'green'), (4, 'green')])
    env.assertEqual(loaded(env, '@cs:{Green} => {$as: c}'), [(3, 'Green')])
    env.assertEqual(loaded(env, '@cs:{green} => {$as: c}'), [(4, 'green')])


@skip(cluster=True)
@with_unstable_features
def testTagPatternLoadsTheFullTag(env):
    """Prefix, suffix, infix and wildcard patterns load the whole indexed tag that matched."""
    setup_hash_index(env)
    for query, expected in [
        ('@t:{gr*} => {$as: c}', [(3, 'green'), (4, 'green')]),
        ('@t:{*ow} => {$as: c}', [(5, 'yellow')]),
        ('@t:{*ell*} => {$as: c}', [(5, 'yellow')]),
        ("@t:{w'b?ue'} => {$as: c}", [(2, 'blue'), (6, 'blue')]),
    ]:
        env.assertEqual(loaded(env, query, 'DIALECT', '2'), expected, message=query)


@skip(cluster=True)
@with_unstable_features
def testTagSuffixTriePatternLoadsTheFullTag(env):
    """On a WITHSUFFIXTRIE field, suffix and contains patterns, which the suffix trie answers, load
    the whole indexed tag that matched."""
    setup_hash_index(env)
    env.assertEqual(loaded(env, '@st:{*ed} => {$as: c}'), [(1, 'red'), (4, 'red')])
    env.assertEqual(loaded(env, '@st:{*lu*} => {$as: c}'), [(2, 'blue'), (6, 'blue')])
    env.assertEqual(loaded(env, '@st:{*ee*} => {$as: c}'), [(3, 'green'), (4, 'green')])


@skip(cluster=True)
@with_unstable_features
def testTagLexRangeLoadsTheTagInRange(env):
    """A lex-range clause loads the indexed tag that fell in the range."""
    setup_hash_index(env)
    env.assertEqual(loaded(env, '@t:>{red} => {$as: c}'), [(5, 'yellow')])
    env.assertEqual(loaded(env, '@t:<{c} => {$as: c}'), [(2, 'blue'), (6, 'blue')])


@skip(cluster=True)
@with_unstable_features
def testTagPhraseLoadsTheWholeValue(env):
    """A multi-word tag is one value, loaded whole."""
    env.flush()
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TAG', 'id', 'NUMERIC', 'SORTABLE').ok()
    conn.execute_command('HSET', 'doc:1', 't', 'red apple', 'id', '1')
    conn.execute_command('HSET', 'doc:2', 't', 'red', 'id', '2')
    env.assertEqual(loaded(env, '@t:{red apple} => {$as: c}'), [(1, 'red apple')])


@skip(cluster=True)
@with_unstable_features
def testTagWildcardLoadsTheEmptyValue(env):
    """A wildcard that matches the empty tag of an INDEXEMPTY field loads it as an empty string."""
    env.flush()
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TAG', 'INDEXEMPTY', 'id', 'NUMERIC', 'SORTABLE').ok()
    conn.execute_command('HSET', 'doc:1', 't', '', 'id', '1')
    conn.execute_command('HSET', 'doc:2', 't', 'red', 'id', '2')
    env.assertEqual(loaded(env, "@t:{w'*'} => {$as: c}", 'DIALECT', '2'), [(1, ''), (2, 'red')])


@skip(cluster=True)
@with_unstable_features
def testTagWithWeightZeroStillLoads(env):
    """A weight of 0 lets the clause's union stop at the first matching value, and a value is still
    loaded."""
    setup_hash_index(env)
    env.assertEqual(loaded(env, '@t:{red|blue} => {$weight: 0; $as: c}'),
                    [(1, 'red'), (2, 'blue'), (4, 'red'), (6, 'blue')])


@skip(cluster=True)
@with_unstable_features
def testTagUnderOptional(env):
    """Under an optional node, only the results the clause matched have the value."""
    setup_hash_index(env)
    env.assertEqual(loaded(env, '@title:hello ~(@t:{red} => {$as: c})'),
                    [(1, 'red'), (2, None), (3, None), (4, 'red'), (5, None), (6, None)])


@skip(cluster=True)
def testTagCursorReadsAfterRevalidation():
    """A cursor that resumes after documents were deleted, overwritten and collected revalidates
    the tag readers, and every later row carries the tag its own document matched."""
    env = Env(redisConfigFile=thread_gc_conf(clean_threshold=1, run_interval=1000000000))
    with_unstable_features(tag_cursor_reads_after_revalidation)(env)


def tag_cursor_reads_after_revalidation(env):
    conn = getConnectionByEnv(env)
    # (query, the tag each id loads once doc:4 reads "blue", or None when it matches otherwise)
    cases = [
        ('@t:{red|blue} => {$as: c}', {4: 'blue', 6: 'blue'}),
        ('@title:hello @t:{red|blue} => {$as: c} | @n:[50 50]', {4: 'blue', 5: None, 6: 'blue'}),
    ]
    for query, expected in cases:
        setup_hash_index(env)
        # No SORTBY: a sorter would drain the iterators before the first row.
        res, cursor = env.cmd('FT.AGGREGATE', 'idx', query, 'LOAD', '1', '@id',
                              'WITHCURSOR', 'COUNT', '1')
        env.assertEqual([to_dict(row) for row in res[1:]], [{'id': '1', 'c': 'red'}], message=query)
        conn.execute_command('DEL', 'doc:2')
        conn.execute_command('HSET', 'doc:4', 't', 'blue')
        forceInvokeGC(env)
        gc_stats = to_dict(index_info(env)['gc_stats'])
        env.assertGreater(int(gc_stats['bytes_collected']), 0, message=(query, gc_stats))

        rows = []
        while cursor:
            res, cursor = env.cmd('FT.CURSOR', 'READ', 'idx', cursor)
            rows.extend(to_dict(row) for row in res[1:])
        loaded_by_id = {int(row['id']): row.get('c') for row in rows}
        env.assertEqual(len(loaded_by_id), len(rows), message=(query, rows))
        # doc:4 is reindexed under a new id, which the cursor may or may not still reach.
        env.assertTrue(set(expected) - {4} <= set(loaded_by_id) <= set(expected),
                       message=(query, rows))
        for doc_id, c in loaded_by_id.items():
            env.assertEqual(c, expected[doc_id], message=(query, doc_id, rows))


@skip(cluster=True)
def testTagSeveralMatchingValuesLoadOneWithoutWarning():
    """A document with several values matching the clause loads one of them, with no warning."""
    env = Env(protocol=3)  # RESP3 exposes the reply's warnings
    with_unstable_features(several_matching_values_load_one)(env)


def several_matching_values_load_one(env):
    setup_hash_index(env)
    res = env.cmd('FT.AGGREGATE', 'idx', '@t:{red|green} => {$as: c}', 'LOAD', '1', '@id',
                  'SORTBY', '2', '@id', 'ASC')
    env.assertEqual(res['warning'], [])
    values = {row['extra_attributes']['id']: row['extra_attributes']['c'] for row in res['results']}
    env.assertEqual(set(values), {'1', '3', '4'}, message=res)
    env.assertEqual(values['1'], 'red')
    env.assertEqual(values['3'], 'green')
    env.assertContains(values['4'], ['red', 'green'])


@skip(cluster=True, no_json=True)
@with_unstable_features
def testTagLoadsMatchedElementOnJsonArray(env):
    """On a JSON array of tags the loaded value is the element the clause matched."""
    env.flush()
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'PREFIX', '1', 'j:', 'SCHEMA',
               '$.t[*]', 'AS', 't', 'TAG', '$.id', 'AS', 'id', 'NUMERIC', 'SORTABLE').ok()
    conn.execute_command('JSON.SET', 'j:1', '$', '{"t": ["red", "blue"], "id": 1}')
    conn.execute_command('JSON.SET', 'j:2', '$', '{"t": ["yellow"], "id": 2}')
    conn.execute_command('JSON.SET', 'j:3', '$', '{"t": ["green", "BLUE"], "id": 3}')
    env.assertEqual(loaded(env, '@t:{blue} => {$as: c}'), [(1, 'blue'), (3, 'blue')])


@skip(cluster=True)
@with_unstable_features
def testTagPlacementUnderOr(env):
    """Under OR only the results matched through the loading clause have the value."""
    setup_hash_index(env)
    env.assertEqual(loaded(env, '@t:{yellow} => {$as: c} | @n:[10 20]'),
                    [(1, None), (2, None), (5, 'yellow')])
    env.assertEqual(loaded(env, '@title:hello @t:{blue} => {$as: c}'), [(2, 'blue'), (6, 'blue')])


@with_unstable_features
def testTagSortByAliasPutsMissingValuesLast(env):
    """SORTBY orders the loaded tags as strings, rows without one last in both directions, also
    after a cluster merge."""
    setup_hash_index(env)
    query = '@t:{red|blue} => {$as: c} | @n:[50 50]'
    rows = aggregate_rows(env, query, 'LOAD', '1', '@id', 'SORTBY', '4', '@c', 'ASC', '@id', 'ASC')
    env.assertEqual([(row['id'], row.get('c')) for row in rows],
                    [('2', 'blue'), ('6', 'blue'), ('1', 'red'), ('4', 'red'), ('5', None)])
    rows = aggregate_rows(env, query, 'LOAD', '1', '@id', 'SORTBY', '4', '@c', 'DESC', '@id', 'ASC')
    env.assertEqual([(row['id'], row.get('c')) for row in rows],
                    [('1', 'red'), ('4', 'red'), ('2', 'blue'), ('6', 'blue'), ('5', None)])


@with_unstable_features
def testTagApplyAndFilterReadTheAlias(env):
    """APPLY and FILTER read the loaded tag like any string field."""
    setup_hash_index(env)
    rows = aggregate_rows(env, '@t:{red|blue} => {$as: c} | @n:[50 50]', 'LOAD', '1', '@id',
                          'APPLY', 'exists(@c)', 'AS', 'has', 'FILTER', '@id <= 5',
                          'SORTBY', '2', '@id', 'ASC')
    env.assertEqual([(row['id'], row['has']) for row in rows],
                    [('1', '1'), ('2', '1'), ('4', '1'), ('5', '0')])

    rows = aggregate_rows(env, '@t:{red|blue} => {$as: c}', 'LOAD', '1', '@id',
                          'APPLY', 'upper(@c)', 'AS', 'up', 'FILTER', '@c == "red"',
                          'SORTBY', '2', '@id', 'ASC')
    env.assertEqual(rows, [{'id': '1', 'c': 'red', 'up': 'RED'}, {'id': '4', 'c': 'red', 'up': 'RED'}])


@with_unstable_features
def testTagAndNumericAliasesInOneQuery(env):
    """TAG and NUMERIC loading clauses each fill their own alias."""
    setup_hash_index(env)
    rows = aggregate_rows(env, '@t:{red|blue} => {$as: c} @n:[15 45] => {$as: v}',
                          'SORTBY', '2', '@v', 'ASC')
    env.assertEqual(rows, [{'c': 'blue', 'v': '20'}, {'c': 'red', 'v': '40'}])


@with_unstable_features
def testTagAliasDefinedWhenNothingMatches(env):
    """The alias exists for SORTBY and APPLY even when the clause matches nothing."""
    setup_hash_index(env)
    env.assertEqual(aggregate_rows(env, '@t:{purple} => {$as: c}', 'SORTBY', '1', '@c'), [])
    env.assertEqual(aggregate_rows(env, '@t:{pur*} => {$as: c}', 'APPLY', 'upper(@c)', 'AS', 'u',
                                   'SORTBY', '1', '@c'), [])


@with_unstable_features
def testTagSearchReturnsTheAlias(env):
    """FT.SEARCH sorts by the loaded tag and returns it when RETURN lists it."""
    setup_hash_index(env)
    res = env.cmd('FT.SEARCH', 'idx', '@cs:{Green|yellow} => {$as: c}', 'SORTBY', 'c', 'DESC',
                  'RETURN', '1', 'c')
    env.assertEqual(res, [2, 'doc:5', ['c', 'yellow'], 'doc:3', ['c', 'Green']])


@with_unstable_features
def testTagHybridSearchSubqueryLoadsTheAlias(env):
    """The search subquery of FT.HYBRID keeps the loaded tag through the merge."""
    blob = setup_vector_index(env)
    res = env.cmd('FT.HYBRID', 'idx', 'SEARCH', '@t:{red|yellow} => {$as: c}',
                  'VSIM', '@vec', '$BLOB', 'LOAD', '2', '@__key', '@c', 'PARAMS', '2', 'BLOB', blob)
    env.assertEqual(hybrid_aliases(res), {'doc:1': 'red', 'doc:4': 'red', 'doc:5': 'yellow'},
                    message=res)


@with_unstable_features
def testTagHybridVectorFilterLoadsTheAlias(env):
    """A loading TAG clause in the FILTER of FT.HYBRID's VSIM subquery loads the tag each vector
    result matched."""
    blob = setup_vector_index(env)
    res = env.cmd('FT.HYBRID', 'idx', 'SEARCH', '@t:{red|blue|yellow}', 'VSIM', '@vec', '$BLOB',
                  'FILTER', '@t:{blue|yellow} => {$as: c}', 'LOAD', '2', '@__key', '@c',
                  'PARAMS', '2', 'BLOB', blob)
    env.assertEqual(hybrid_aliases(res), {'doc:2': 'blue', 'doc:5': 'yellow', 'doc:6': 'blue'},
                    message=res)


@with_unstable_features
def testTagLoadsInsideVectorFilter(env):
    """A loading TAG clause in the filter of a KNN query keeps its value next to the distance,
    under both hybrid policies."""
    blob = setup_vector_index(env)
    for policy in ['ADHOC_BF', 'BATCHES']:
        query = f'(@t:{{blue|yellow}} => {{$as: c}})=>[KNN 2 @vec $b HYBRID_POLICY {policy} AS dist]'
        res = env.cmd('FT.SEARCH', 'idx', query, 'PARAMS', '2', 'b', blob, 'SORTBY', 'dist',
                      'RETURN', '2', 'c', 'dist')
        env.assertEqual(res[1::2], ['doc:2', 'doc:5'], message=(policy, res))
        env.assertEqual([to_dict(fields) for fields in res[2::2]],
                        [{'c': 'blue', 'dist': '4'}, {'c': 'yellow', 'dist': '25'}], message=(policy, res))


@with_unstable_features
def testTagOptimizedQueryKeepsTheAlias(env):
    """With the query optimizer on (DIALECT 4, or WITHOUTCOUNT on FT.SEARCH) and a numeric SORTBY,
    a query with a loading TAG clause runs unoptimized and keeps the alias."""
    setup_hash_index(env)
    query = '@t:{red|blue} => {$as: c} @n:[15 65]'
    rows = aggregate_rows(env, query, 'LOAD', '1', '@id', 'SORTBY', '2', '@n', 'ASC',
                          'DIALECT', '4')
    env.assertEqual([(row['id'], row['c']) for row in rows], [('2', 'blue'), ('4', 'red'), ('6', 'blue')])
    for optimize in [['WITHOUTCOUNT'], ['DIALECT', '4']]:
        res = env.cmd('FT.SEARCH', 'idx', query, 'SORTBY', 'n', 'ASC', 'RETURN', '1', 'c', *optimize)
        env.assertEqual(res[1:], ['doc:2', ['c', 'blue'], 'doc:4', ['c', 'red'], 'doc:6', ['c', 'blue']],
                        message=(optimize, res))


@skip(cluster=True)
@with_unstable_features
def testTagRejectedUnderNegation(env):
    """The documents a NOT yields are those its child did not match, so there is no tag to load."""
    setup_hash_index(env)
    env.expect('FT.AGGREGATE', 'idx', '-(@t:{red} => {$as: c})').error() \
        .contains('`$as` is not supported under a negation')


@skip(cluster=True)
@with_unstable_features
def testTagAliasErrors(env):
    """A TAG alias follows the NUMERIC alias rules: it may not collide with a schema field or
    another alias, nor take the `__` prefix the pipeline reserves."""
    setup_hash_index(env)
    env.expect('FT.AGGREGATE', 'idx', '@t:{red} => {$as: n}').error() \
        .contains('Property `n` already exists in schema')
    env.expect('FT.AGGREGATE', 'idx', '@t:{red} => {$as: c} | @n:[10 20] => {$as: c}').error() \
        .contains('Property `c` specified more than once')
    env.expect('FT.AGGREGATE', 'idx', '@t:{red} => {$as: __c}').error() \
        .contains('`$as` aliases starting with `__` are reserved')


@skip(cluster=True)
@with_unstable_features
def testTagExplainShowsTheAlias(env):
    """FT.EXPLAIN prints the alias on the TAG clause that loads it."""
    setup_hash_index(env)
    res = env.cmd('FT.EXPLAIN', 'idx', '@t:{red|blue} => {$as: c}')
    env.assertEqual(res, 'TAG:@t {\n  red\n  blue\n} => {$as: c}\n')


@skip(cluster=True)
@with_unstable_features
def testTagProfileIteratorTreeIsUnchanged(env):
    """The decorators are invisible in FT.PROFILE: the iterator tree is the one the clause builds
    without `$as`."""
    setup_hash_index(env)

    for clause in ['@t:{red}', '@t:{red|blue}', '@t:{gr*}']:
        for query in [clause, f'@title:hello {clause}']:
            plain = env.cmd('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', query)
            loading = env.cmd('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY',
                              query.replace(clause, f'{clause} => {{$as: c}}'))
            env.assertIsNotNone(iterators_profile(plain), message=plain)
            env.assertEqual(iterators_profile(loading), iterators_profile(plain), message=query)
