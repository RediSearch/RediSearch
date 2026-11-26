# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Index loading: `@num:[lo hi] => {$as: alias}` puts the value the clause matched into each
result as `alias`, read from the index rather than the document. An unstable feature."""

from common import *

# id -> (n, t). doc:3's value is spelled with a trailing zero, which the index does not keep.
DOCS = {i: (str(i * 10), 'a' if i % 2 else 'b') for i in range(1, 9)}
DOCS[3] = ('30.50', 'a')


def set_unstable_features(env, on):
    run_command_on_all_shards(env, 'CONFIG', 'SET', 'search-enable-unstable-features',
                              'yes' if on else 'no')


def with_unstable_features(test):
    """Runs `test` with unstable features on, and turns them back off after it, so no other test
    in the file inherits them."""
    @wraps(test)
    def wrapper(env, *args, **kwargs):
        set_unstable_features(env, True)
        try:
            return test(env, *args, **kwargs)
        finally:
            set_unstable_features(env, False)
    return wrapper


def setup_hash_index(env):
    env.flush()
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'PREFIX', '1', 'doc:', 'SCHEMA',
               'n', 'NUMERIC', 'm', 'NUMERIC', 'id', 'NUMERIC', 'SORTABLE', 't', 'TAG', 'SORTABLE',
               'title', 'TEXT', 'g', 'GEO').ok()
    for i, (n, t) in DOCS.items():
        conn.execute_command('HSET', f'doc:{i}', 'n', n, 'm', str(100 + i), 'id', str(i), 't', t,
                             'title', 'hello', 'g', '1,1')
    return conn


def aggregate_rows(env, query, *args):
    """FT.AGGREGATE rows as dicts, after the leading count (which a cluster may report
    differently)."""
    res = env.cmd('FT.AGGREGATE', 'idx', query, *args)
    return [to_dict(row) for row in res[1:]]


@with_unstable_features
def testRequiresUnstableFeatures(env):
    """With the gate off, `$as` is rejected with an error naming the config that enables it."""
    setup_hash_index(env)
    set_unstable_features(env, False)
    env.expect('FT.AGGREGATE', 'idx', '@n:[15 25] => {$as: v}').error() \
        .contains('`CONFIG SET search-enable-unstable-features yes`')
    for cmd in ['FT.SEARCH', 'FT.EXPLAIN']:
        env.expect(cmd, 'idx', '@n:[15 25] => {$as: v}').error() \
            .contains('ENABLE_UNSTABLE_FEATURES')

    set_unstable_features(env, True)
    env.assertEqual(aggregate_rows(env, '@n:[15 25] => {$as: v}'), [{'v': '20'}])


@with_unstable_features
def testLoadsMatchedValueOnHash(env):
    """The loaded value is the number the index holds, not the document's spelling."""
    setup_hash_index(env)
    rows = aggregate_rows(env, '@n:[25 45] => {$as: v}', 'LOAD', '1', '@id', 'SORTBY', '2', '@v', 'ASC')
    env.assertEqual(rows, [{'v': '30.5', 'id': '3'}, {'v': '40', 'id': '4'}])


@skip(no_json=True)
def testLoadsMatchedElementOnJsonMultiValue():
    """On a multi-value field the loaded value is one of the elements in range, with no warning."""
    env = Env(protocol=3)  # RESP3 exposes the reply's warnings
    with_unstable_features(json_multi_value_loads_one_element)(env)


def json_multi_value_loads_one_element(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'PREFIX', '1', 'j:', 'SCHEMA',
               '$.n', 'AS', 'n', 'NUMERIC', '$.id', 'AS', 'id', 'NUMERIC', 'SORTABLE').ok()
    conn.execute_command('JSON.SET', 'j:1', '$', '{"n": [5, 25, 100], "id": 1}')
    conn.execute_command('JSON.SET', 'j:2', '$', '{"n": [21, 29], "id": 2}')
    conn.execute_command('JSON.SET', 'j:3', '$', '{"n": 7, "id": 3}')
    conn.execute_command('JSON.SET', 'j:4', '$', '{"n": 22.5, "id": 4}')

    res = env.cmd('FT.AGGREGATE', 'idx', '@n:[20 30] => {$as: v}', 'LOAD', '1', '@id',
                  'SORTBY', '2', '@id', 'ASC')
    env.assertEqual(res['warning'], [])
    values = {row['extra_attributes']['id']: float(row['extra_attributes']['v'])
              for row in res['results']}
    env.assertEqual(set(values), {'1', '2', '4'}, message=res)
    env.assertEqual(values['1'], 25)
    env.assertContains(values['2'], [21, 29])
    env.assertEqual(values['4'], 22.5)


@with_unstable_features
def testPlacementUnderAndOrOptional(env):
    """At the root or under AND every result has the value; under OR or an optional node only the
    results matched through the clause have it."""
    setup_hash_index(env)

    rows = aggregate_rows(env, '@t:{a} @n:[25 75] => {$as: v}', 'LOAD', '1', '@id',
                          'SORTBY', '2', '@id', 'ASC')
    env.assertEqual(rows, [{'v': '30.5', 'id': '3'}, {'v': '50', 'id': '5'}, {'v': '70', 'id': '7'}])

    rows = aggregate_rows(env, '@n:[35 45] => {$as: v} | @t:{a}', 'LOAD', '1', '@id',
                          'SORTBY', '2', '@id', 'ASC')
    env.assertEqual(rows, [{'id': '1'}, {'id': '3'}, {'v': '40', 'id': '4'}, {'id': '5'}, {'id': '7'}])

    rows = aggregate_rows(env, '@t:{b} ~(@n:[35 65] => {$as: v})', 'LOAD', '1', '@id',
                          'SORTBY', '2', '@id', 'ASC')
    env.assertEqual(rows, [{'id': '2'}, {'v': '40', 'id': '4'}, {'v': '60', 'id': '6'}, {'id': '8'}])


@with_unstable_features
def testDocumentMatchedByEveryBranchKeepsTheValue(env):
    """A union with a loading branch is evaluated in full, even with weight 0, so a document
    matched by several branches keeps the value."""
    setup_hash_index(env)
    for query in ['(@t:{a} | @n:[25 35] => {$as: v}) => {$weight: 0}',
                  '((@t:{a} | @n:[25 35] => {$as: v}) => {$weight: 0} | @t:{zzz}) => {$weight: 0}']:
        rows = aggregate_rows(env, query, 'LOAD', '1', '@id', 'FILTER', '@id == 3')
        env.assertEqual(rows, [{'v': '30.5', 'id': '3'}], message=query)


@with_unstable_features
def testRejectedUnderNegation(env):
    """The documents a NOT yields are those its child did not match, so there is no value to
    load."""
    setup_hash_index(env)
    env.expect('FT.AGGREGATE', 'idx', '-(@n:[25 45] => {$as: v})').error() \
        .contains('`$as` is not supported under a negation')
    env.expect('FT.AGGREGATE', 'idx', '@t:{a} -(@t:{b} | @n:[25 45] => {$as: v})').error() \
        .contains('`$as` is not supported under a negation')


@with_unstable_features
def testSortByAliasPutsMissingValuesLast(env):
    """Rows the clause did not match have no value, and sort last in both directions, also after a
    cluster merge of shard-local top-k."""
    setup_hash_index(env)
    query = '@n:[15 45] => {$as: v} | @t:{a}'
    rows = aggregate_rows(env, query, 'LOAD', '1', '@id', 'SORTBY', '4', '@v', 'ASC', '@id', 'ASC',
                          'LIMIT', '0', '4')
    env.assertEqual(rows, [{'v': '20', 'id': '2'}, {'v': '30.5', 'id': '3'}, {'v': '40', 'id': '4'},
                           {'id': '1'}])
    rows = aggregate_rows(env, query, 'LOAD', '1', '@id', 'SORTBY', '4', '@v', 'DESC', '@id', 'ASC',
                          'LIMIT', '0', '5')
    env.assertEqual(rows, [{'v': '40', 'id': '4'}, {'v': '30.5', 'id': '3'}, {'v': '20', 'id': '2'},
                           {'id': '1'}, {'id': '5'}])


@with_unstable_features
def testSortByAliasWithTieBreaker(env):
    """Equal loaded values are ordered by the tie-breaker, on a single shard and after a merge."""
    env.flush()
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'n', 'NUMERIC', 'id', 'NUMERIC', 'SORTABLE').ok()
    for i in range(1, 31):
        conn.execute_command('HSET', f'k{i}', 'n', str(i % 5), 'id', str(i))
    rows = aggregate_rows(env, '@n:[1 3] => {$as: v}', 'LOAD', '1', '@id',
                          'SORTBY', '4', '@v', 'DESC', '@id', 'ASC', 'LIMIT', '0', '8')
    expected = sorted(((i % 5, i) for i in range(1, 31) if 1 <= i % 5 <= 3),
                      key=lambda p: (-p[0], p[1]))[:8]
    env.assertEqual(rows, [{'v': str(v), 'id': str(i)} for v, i in expected])


@with_unstable_features
def testApplyAndFilterReadTheAlias(env):
    """APPLY and FILTER read the alias like any field, and exists() tells rows without it."""
    setup_hash_index(env)
    rows = aggregate_rows(env, '@n:[35 45] => {$as: v} | @t:{a}', 'LOAD', '1', '@id',
                          'APPLY', 'exists(@v)', 'AS', 'has', 'FILTER', '@id <= 4',
                          'SORTBY', '2', '@id', 'ASC')
    env.assertEqual(rows, [{'id': '1', 'has': '0'}, {'id': '3', 'has': '0'},
                           {'v': '40', 'id': '4', 'has': '1'}])

    rows = aggregate_rows(env, '@n:[15 35] => {$as: v}', 'APPLY', '@v * 2', 'AS', 'twice',
                          'FILTER', '@v > 25', 'SORTBY', '2', '@v', 'ASC')
    env.assertEqual(rows, [{'v': '30.5', 'twice': '61'}])


@with_unstable_features
def testSeveralAliases(env):
    """Each loading clause has its own alias, under OR and under AND."""
    setup_hash_index(env)
    rows = aggregate_rows(env, '@n:[15 25] => {$as: lo} | @n:[65 75] => {$as: hi}',
                          'LOAD', '1', '@id', 'SORTBY', '2', '@id', 'ASC')
    env.assertEqual(rows, [{'lo': '20', 'id': '2'}, {'hi': '70', 'id': '7'}])

    rows = aggregate_rows(env, '@n:[15 25] => {$as: a} @m:[100 200] => {$as: b}')
    env.assertEqual(rows, [{'a': '20', 'b': '102'}])


@with_unstable_features
def testAliasDefinedWhenNothingMatches(env):
    """The alias exists for SORTBY and APPLY even when the clause, or a sibling, matches nothing."""
    setup_hash_index(env)
    env.assertEqual(aggregate_rows(env, '@n:[1000 2000] => {$as: v}', 'SORTBY', '1', '@v'), [])
    env.assertEqual(aggregate_rows(env, '@t:{zzz} @n:[15 25] => {$as: v}',
                                   'APPLY', '@v + 1', 'AS', 'w', 'SORTBY', '1', '@v'), [])


@with_unstable_features
def testLoadOfSameNameIsServedByTheAlias(env):
    """A document field named like the alias is not loaded: the clause's value wins."""
    setup_hash_index(env)
    getConnectionByEnv(env).execute_command('HSET', 'doc:2', 'v', 'from-document')
    rows = aggregate_rows(env, '@n:[15 25] => {$as: v}', 'LOAD', '1', '@v')
    env.assertEqual(rows, [{'v': '20'}])


@with_unstable_features
def testAliasCollisions(env):
    """An alias that collides with a schema field or another alias is rejected."""
    setup_hash_index(env)
    env.expect('FT.AGGREGATE', 'idx', '@n:[15 25] => {$as: m}').error() \
        .contains('Property `m` already exists in schema')
    env.expect('FT.AGGREGATE', 'idx', '@n:[15 25] => {$as: v} | @m:[100 200] => {$as: v}').error() \
        .contains('Property `v` specified more than once')


@with_unstable_features
def testReservedAliasPrefix(env):
    """The pipeline's own row keys start with `__`, such as the `__key` FT.HYBRID merges by, so an
    alias may not; a single leading `_` is fine."""
    setup_hash_index(env)
    for alias in ['__key', '__score', '__v']:
        env.expect('FT.AGGREGATE', 'idx', f'@n:[15 25] => {{$as: {alias}}}').error() \
            .contains('`$as` aliases starting with `__` are reserved')
    env.assertEqual(aggregate_rows(env, '@n:[15 25] => {$as: _v}'), [{'_v': '20'}])

    env.expect('FT.CREATE', 'vidx', 'PREFIX', '1', 'vdoc:', 'SCHEMA', 'n', 'NUMERIC',
               'emb', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()
    blob = np.array([0.0, 0.0]).astype(np.float32).tobytes()
    env.expect('FT.HYBRID', 'vidx', 'SEARCH', '@n:[1 9] => {$as: __key}', 'VSIM', '@emb', '$BLOB',
               'PARAMS', '2', 'BLOB', blob).error() \
        .contains('`$as` aliases starting with `__` are reserved')


@skip(cluster=True)
@with_unstable_features
def testAliasErrors(env):
    """An invalid property name, and `$as` on anything but a single NUMERIC clause, are rejected
    at parse time."""
    setup_hash_index(env)
    for name in ['a b', 'a.b', '@v', 'a-b', 'a\tb', 'a\x01b', 'a\x7fb']:
        env.expect('FT.AGGREGATE', 'idx', '@n:[15 25] => {$as: $name}', 'PARAMS', '2', 'name', name,
                   'DIALECT', '2').error().contains('`$as` requires a valid property name')
    # The parser rejects an empty attribute value before `$as` sees it.
    env.expect('FT.AGGREGATE', 'idx', '@n:[15 25] => {$as: $name}', 'PARAMS', '2', 'name', '',
               'DIALECT', '2').error().contains('Invalid value () for `as`')
    for query in ['@title:hello => {$as: v}', '@t:{a} => {$as: v}', '@g:[1 1 10 km] => {$as: v}',
                  '(@n:[15 25] @t:{b}) => {$as: v}', '@n != 20 => {$as: v}']:
        env.expect('FT.AGGREGATE', 'idx', query).error() \
            .contains('`$as` is supported only on NUMERIC clauses')


@skip(cluster=True)
@with_unstable_features
def testRejectedOnVectorClause(env):
    """A vector range clause yields distances through `$yield_distance_as`, not `$as`."""
    env.flush()
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'vec', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2',
               'DISTANCE_METRIC', 'L2').ok()
    blob = np.array([0.0, 0.0]).astype(np.float32).tobytes()
    env.expect('FT.AGGREGATE', 'idx', '@vec:[VECTOR_RANGE 1 $b] => {$as: v}', 'PARAMS', '2', 'b', blob) \
        .error().contains('`$as` is supported only on NUMERIC clauses')


@skip(cluster=True)
@with_unstable_features
def testLoadsTheValueACompressedTreeStores(env):
    """A range tree built with float compression may store a value slightly off the document's,
    and `$as` loads the stored one."""
    env.expect('CONFIG', 'SET', 'search-_numeric-compress', 'yes').ok()
    try:
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC').ok()
        env.cmd('HSET', 'doc:1', 'n', '1.1')
        [row] = aggregate_rows(env, '@n:[1 2] => {$as: v}')
        env.assertAlmostEqual(float(row['v']), 1.1, delta=0.01, message=row)
    finally:
        env.expect('CONFIG', 'SET', 'search-_numeric-compress', 'no').ok()


@with_unstable_features
def testSearchReturnsTheAliasAmongTheDocumentFields(env):
    """Without RETURN, FT.SEARCH returns the alias next to the document's fields, and a document
    field of the same name is replaced by the loaded value."""
    setup_hash_index(env)
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc:2', 'v', 'from-document')
    res = env.cmd('FT.SEARCH', 'idx', '@n:[15 25] => {$as: v}')
    env.assertEqual(res[:2], [1, 'doc:2'], message=res)
    env.assertEqual(to_dict(res[2]), {'v': '20', 'n': '20', 'm': '102', 'id': '2', 't': 'b',
                                      'title': 'hello', 'g': '1,1'}, message=res)


@with_unstable_features
def testSearchReturnsTheAlias(env):
    """FT.SEARCH sorts by the alias and returns it when RETURN lists it."""
    setup_hash_index(env)
    res = env.cmd('FT.SEARCH', 'idx', '@n:[25 45] => {$as: v}', 'SORTBY', 'v', 'ASC', 'RETURN', '1', 'v')
    env.assertEqual(res, [2, 'doc:3', ['v', '30.5'], 'doc:4', ['v', '40']])


@with_unstable_features
def testHybridSearchSubqueryLoadsTheAlias(env):
    """The search subquery of FT.HYBRID keeps the alias through the merge, with the value
    FT.AGGREGATE returns."""
    env.flush()
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'emb', 'VECTOR', 'FLAT', '6',
               'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()
    for i in range(1, 6):
        conn.execute_command('HSET', f'doc:{i}', 'n', str(i * 10),
                             'emb', np.array([float(i), 0.0]).astype(np.float32).tobytes())
    blob = np.array([0.0, 0.0]).astype(np.float32).tobytes()

    res = env.cmd('FT.HYBRID', 'idx', 'SEARCH', '@n:[25 45] => {$as: v}',
                  'VSIM', '@emb', '$BLOB', 'LOAD', '2', '@__key', '@v', 'PARAMS', '2', 'BLOB', blob)
    results, _ = get_results_from_hybrid_response(res)
    loaded = {key: fields['v'] for key, fields in results.items() if 'v' in fields}
    env.assertEqual(loaded, {'doc:3': '30', 'doc:4': '40'}, message=res)

    rows = aggregate_rows(env, '@n:[25 45] => {$as: v}', 'SORTBY', '2', '@v', 'ASC')
    env.assertEqual([row['v'] for row in rows], ['30', '40'])


@skip(cluster=True)
@with_unstable_features
def testProfileShowsNoKeyspaceLoader(env):
    """A query that returns only loaded and sortable fields never reads the document."""
    setup_hash_index(env)

    def rp_types(reply):
        types = []
        if isinstance(reply, list):
            for i, item in enumerate(reply):
                if item == 'Type' and i + 1 < len(reply) and isinstance(reply[i + 1], str):
                    types.append(reply[i + 1])
                else:
                    types.extend(rp_types(item))
        return types

    loaded = env.cmd('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '@n:[25 45] => {$as: v}',
                     'LOAD', '1', '@id', 'SORTBY', '1', '@v')
    from_document = env.cmd('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '@n:[25 45]',
                            'LOAD', '1', '@n', 'SORTBY', '1', '@n')
    env.assertContains('Loader', rp_types(from_document[1]), message=from_document)
    types = rp_types(loaded[1])
    env.assertContains('Metrics Applier', types, message=loaded)
    for loader in ['Loader', 'Threadsafe-Loader']:
        env.assertNotContains(loader, types, message=loaded)


@skip(cluster=True)
@with_unstable_features
def testExplainShowsTheAlias(env):
    """FT.EXPLAIN prints the alias on the NUMERIC clause that loads it."""
    setup_hash_index(env)
    res = env.cmd('FT.EXPLAIN', 'idx', '@n:[15 25] => {$as: v}')
    env.assertContains('NUMERIC {15.000000 <= @n <= 25.000000} => {$as: v}', res)


@with_unstable_features
def testOptimizedQueryKeepsTheAlias(env):
    """With the query optimizer on (DIALECT 4, or WITHOUTCOUNT on FT.SEARCH) and a numeric SORTBY,
    the optimizer would replace the iterator tree's root with a numeric top-k that drops the
    loaded value. A query with a loading clause therefore runs unoptimized."""
    setup_hash_index(env)
    query = '@t:{a} @n:[25 75] => {$as: v}'

    def without(rows, field):
        return [{k: v for k, v in row.items() if k != field} for row in rows]

    for sort_field in ['m', 'n']:
        rows = aggregate_rows(env, query, 'LOAD', '1', '@id', 'SORTBY', '2', f'@{sort_field}', 'ASC',
                              'DIALECT', '4')
        env.assertEqual(without(rows, sort_field), [{'v': '30.5', 'id': '3'}, {'v': '50', 'id': '5'},
                                                    {'v': '70', 'id': '7'}], message=sort_field)
        rows = aggregate_rows(env, query, 'LOAD', '1', '@id', 'FILTER', '@v > 40',
                              'SORTBY', '2', f'@{sort_field}', 'ASC', 'DIALECT', '4')
        env.assertEqual(without(rows, sort_field), [{'v': '50', 'id': '5'}, {'v': '70', 'id': '7'}],
                        message=sort_field)

        for optimize in [['WITHOUTCOUNT'], ['DIALECT', '4']]:
            res = env.cmd('FT.SEARCH', 'idx', query, 'SORTBY', sort_field, 'ASC', 'RETURN', '1', 'v',
                          *optimize)
            env.assertEqual(res[1:], ['doc:3', ['v', '30.5'], 'doc:5', ['v', '50'],
                                      'doc:7', ['v', '70']], message=(sort_field, optimize, res))


@skip(cluster=True)
@with_unstable_features
def testLoadsOnEquality(env):
    """`@n == x` is a NUMERIC clause too."""
    setup_hash_index(env)
    env.assertEqual(aggregate_rows(env, '@n == 40 => {$as: v}', 'LOAD', '1', '@id'),
                    [{'v': '40', 'id': '4'}])


@skip(cluster=True)
@with_unstable_features
def testProfileIteratorTreeIsUnchanged(env):
    """The index-loading decorator is invisible in FT.PROFILE: the iterator tree is the one the
    clause builds without `$as`."""
    setup_hash_index(env)

    def without_time(node):
        if not isinstance(node, list):
            return node
        out, i = [], 0
        while i < len(node):
            if node[i] == 'Time':
                i += 2
                continue
            out.append(without_time(node[i]))
            i += 1
        return out

    def iterators_profile(reply):
        if isinstance(reply, list):
            for i, item in enumerate(reply):
                if item == 'Iterators profile':
                    return without_time(reply[i + 1])
                found = iterators_profile(item)
                if found is not None:
                    return found
        return None

    for query in ['@n:[25 75]', '@t:{a} @n:[25 75]', '@n:[25 35] | @t:{b}']:
        plain = env.cmd('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', query)
        loading = env.cmd('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY',
                          query.replace('@n:[25 75]', '@n:[25 75] => {$as: v}')
                               .replace('@n:[25 35]', '@n:[25 35] => {$as: v}'))
        env.assertIsNotNone(iterators_profile(plain), message=plain)
        env.assertEqual(iterators_profile(loading), iterators_profile(plain), message=query)


@with_unstable_features
def testLoadsInsideVectorFilter(env):
    """A loading clause in the filter of a KNN query keeps its value next to the distance, under
    both hybrid policies, whichever syntax names the distance field."""
    env.flush()
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 't', 'TAG', 'vec', 'VECTOR', 'FLAT', '6',
               'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()
    for i in range(1, 9):
        conn.execute_command('HSET', f'doc:{i}', 'n', str(i * 10), 't', 'a' if i % 2 else 'b',
                             'vec', np.array([float(i), 0.0]).astype(np.float32).tobytes())
    blob = np.array([0.0, 0.0]).astype(np.float32).tobytes()

    # (filter, docs the KNN returns, nearest first, mapped to the value each loads or None)
    cases = [
        ('@n:[25 75] => {$as: v}', [(3, '30'), (4, '40'), (5, '50')]),
        # doc:1 passes the filter through the TAG branch only, so it loads nothing.
        ('@n:[25 45] => {$as: v} | @t:{a}', [(1, None), (3, '30'), (4, '40')]),
    ]
    for policy in ['ADHOC_BF', 'BATCHES']:
        for flt, expected in cases:
            for knn in [f'=>[KNN 3 @vec $b HYBRID_POLICY {policy} AS dist]',
                        f'=>[KNN 3 @vec $b HYBRID_POLICY {policy}]=>{{$yield_distance_as: dist}}']:
                query = f'({flt}){knn}'
                expected_rows = [{'dist': str(i * i), **({'v': v} if v else {})} for i, v in expected]

                res = env.cmd('FT.SEARCH', 'idx', query, 'PARAMS', '2', 'b', blob,
                              'SORTBY', 'dist', 'RETURN', '2', 'v', 'dist')
                env.assertEqual(res[1::2], [f'doc:{i}' for i, _ in expected], message=(query, res))
                env.assertEqual([to_dict(fields) for fields in res[2::2]], expected_rows,
                                message=(query, res))

                rows = aggregate_rows(env, query, 'PARAMS', '2', 'b', blob, 'SORTBY', '2', '@dist', 'ASC')
                env.assertEqual(rows, expected_rows, message=(query, rows))


@skip(cluster=True)
@with_unstable_features
def testLoadsUnderDialect1(env):
    """The DIALECT 1 parser applies `$as` like the later ones."""
    setup_hash_index(env)
    env.assertEqual(aggregate_rows(env, '@n:[15 25] => {$as: v}', 'DIALECT', '1'), [{'v': '20'}])


@skip(cluster=True)
@with_unstable_features
def testRepeatedAsKeepsTheLast(env):
    """A clause given `$as` twice loads under the last alias only."""
    setup_hash_index(env)
    env.assertEqual(aggregate_rows(env, '@n:[15 25] => {$as: a; $as: b}'), [{'b': '20'}])


@skip(cluster=True)
@with_unstable_features
def testNonAsciiAlias(env):
    """An alias of non-ASCII letters works like any other, in SORTBY, APPLY and FILTER."""
    setup_hash_index(env)
    rows = aggregate_rows(env, '@n:[15 35] => {$as: $name}', 'PARAMS', '2', 'name', 'ערך',
                          'APPLY', '@ערך * 2', 'AS', 'twice', 'FILTER', '@ערך > 25',
                          'SORTBY', '2', '@ערך', 'ASC', 'DIALECT', '2')
    env.assertEqual(rows, [{'ערך': '30.5', 'twice': '61'}])


@with_unstable_features
def testHybridRejections(env):
    """FT.HYBRID's search subquery reports the same errors as FT.SEARCH: the gate, and the
    rejections only evaluation can make."""
    env.flush()
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'emb', 'VECTOR', 'FLAT',
               '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()
    # Enough keys that every shard of a cluster has documents.
    for i in range(20):
        conn.execute_command('HSET', f'doc:{i}', 'n', '10', 'emb', np.array([1.0, 0.0]).astype(np.float32).tobytes())
    blob = np.array([0.0, 0.0]).astype(np.float32).tobytes()

    def hybrid(query):
        return env.expect('FT.HYBRID', 'idx', 'SEARCH', query, 'VSIM', '@emb', '$BLOB',
                          'PARAMS', '2', 'BLOB', blob)

    hybrid('-(@n:[5 15] => {$as: v})').error().contains('`$as` is not supported under a negation')
    set_unstable_features(env, False)
    hybrid('@n:[5 15] => {$as: v}').error().contains('ENABLE_UNSTABLE_FEATURES')


@with_unstable_features
def testOptimizerKeepsTheScorerDecision(env):
    """Without SORTBY the optimizer only decides whether results need scoring and sorting, which
    `$as` must not change: an optimized query replies the same, total, order and scores, with or
    without a loading clause."""
    setup_hash_index(env)

    for optimize in [['WITHOUTCOUNT'], ['DIALECT', '4']]:
        for args in [[], ['WITHSCORES']]:
            plain = env.cmd('FT.SEARCH', 'idx', '@t:{a} @n:[5 85]', *args, 'NOCONTENT',
                            'LIMIT', '0', '2', *optimize)
            loading = env.cmd('FT.SEARCH', 'idx', '@t:{a} @n:[5 85] => {$as: v}', *args,
                              'NOCONTENT', 'LIMIT', '0', '2', *optimize)
            env.assertEqual(loading, plain, message=(optimize, args))


@with_unstable_features
def testGateErrorPrecedesTheClauseCheck(env):
    """With the gate off, `$as` on a clause it never supports reports the gate, not the clause."""
    env.flush()
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT', 'vec', 'VECTOR', 'FLAT', '6',
               'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()
    blob = np.array([0.0, 0.0]).astype(np.float32).tobytes()
    set_unstable_features(env, False)
    for query in ['@title:hello => {$as: v}', '@vec:[VECTOR_RANGE 1 $b] => {$as: v}']:
        env.expect('FT.AGGREGATE', 'idx', query, 'PARAMS', '2', 'b', blob, 'DIALECT', '2').error() \
            .contains('SEARCH_QUERY_BAD `$as` is unavailable when `ENABLE_UNSTABLE_FEATURES` is off')


@with_unstable_features
def testAliasCollidesWithKnnDistance(env):
    """An alias naming the KNN distance, by `AS` or by `$yield_distance_as`, is a duplicate."""
    env.flush()
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'vec', 'VECTOR', 'FLAT', '6',
               'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()
    blob = np.array([0.0, 0.0]).astype(np.float32).tobytes()
    for knn in ['[KNN 3 @vec $b AS v]', '[KNN 3 @vec $b]=>{$yield_distance_as: v}']:
        for cmd in ['FT.SEARCH', 'FT.AGGREGATE']:
            env.expect(cmd, 'idx', f'(@n:[0 100] => {{$as: v}})=>{knn}', 'PARAMS', '2', 'b', blob,
                       'DIALECT', '2').error().contains('Property `v` specified more than once')


@with_unstable_features
def testApplyOverridesTheAlias(env):
    """An APPLY naming the alias replaces the loaded value."""
    setup_hash_index(env)
    rows = aggregate_rows(env, '@n:[15 25] => {$as: v}', 'APPLY', '@v * 2', 'AS', 'v')
    env.assertEqual(rows, [{'v': '40'}])


@with_unstable_features
def testHybridVsimFilterLoadsTheAlias(env):
    """A loading clause in the VSIM FILTER of FT.HYBRID loads onto the vector results."""
    env.flush()
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'emb', 'VECTOR', 'FLAT', '6',
               'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()
    for i in range(1, 6):
        conn.execute_command('HSET', f'doc:{i}', 'n', str(i * 10),
                             'emb', np.array([float(i), 0.0]).astype(np.float32).tobytes())
    blob = np.array([0.0, 0.0]).astype(np.float32).tobytes()

    res = env.cmd('FT.HYBRID', 'idx', 'SEARCH', '@n:[1000 2000]', 'VSIM', '@emb', '$BLOB',
                  'FILTER', '@n:[15 35] => {$as: v}', 'LOAD', '2', '@__key', '@v',
                  'PARAMS', '2', 'BLOB', blob)
    results, _ = get_results_from_hybrid_response(res)
    env.assertEqual({key: fields.get('v') for key, fields in results.items()},
                    {'doc:2': '20', 'doc:3': '30'}, message=res)


@with_unstable_features
def testSearchSortByAliasPutsMissingValuesLast(env):
    """FT.SEARCH sorts the documents the clause did not match last in both directions, also after
    a cluster merge."""
    setup_hash_index(env)
    query = '@n:[15 45] => {$as: v} | @t:{a}'
    res = env.cmd('FT.SEARCH', 'idx', query, 'SORTBY', 'v', 'ASC', 'RETURN', '1', 'v',
                  'LIMIT', '0', '4')
    env.assertEqual(res[1:7], ['doc:2', ['v', '20'], 'doc:3', ['v', '30.5'], 'doc:4', ['v', '40']],
                    message=res)
    env.assertContains(res[7], ['doc:1', 'doc:5', 'doc:7'], message=res)
    res = env.cmd('FT.SEARCH', 'idx', query, 'SORTBY', 'v', 'DESC', 'RETURN', '1', 'v',
                  'LIMIT', '0', '4')
    env.assertEqual(res[1:7], ['doc:4', ['v', '40'], 'doc:3', ['v', '30.5'], 'doc:2', ['v', '20']],
                    message=res)
    env.assertContains(res[7], ['doc:1', 'doc:5', 'doc:7'], message=res)


@with_unstable_features
def testLoadsInfinities(env):
    """An unbounded range loads the infinities documents hold."""
    env.flush()
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC').ok()
    for key, n in [('doc:1', 'inf'), ('doc:2', '-inf'), ('doc:3', '5')]:
        conn.execute_command('HSET', key, 'n', n)
    rows = aggregate_rows(env, '@n:[-inf +inf] => {$as: v}', 'SORTBY', '2', '@v', 'ASC')
    env.assertEqual(rows, [{'v': '-inf'}, {'v': '5'}, {'v': 'inf'}])


@with_unstable_features
def testGroupByReducesTheAlias(env):
    """GROUPBY groups by the alias and REDUCE reduces it."""
    setup_hash_index(env)
    rows = aggregate_rows(env, '@n:[15 45] => {$as: v}', 'GROUPBY', '0',
                          'REDUCE', 'SUM', '1', '@v', 'AS', 'sum', 'REDUCE', 'MAX', '1', '@v', 'AS', 'max')
    env.assertEqual(rows, [{'sum': '90.5', 'max': '40'}])
    rows = aggregate_rows(env, '@n:[15 45] => {$as: v}', 'GROUPBY', '1', '@v',
                          'REDUCE', 'COUNT', '0', 'AS', 'count', 'SORTBY', '2', '@v', 'ASC')
    env.assertEqual(rows, [{'v': '20', 'count': '1'}, {'v': '30.5', 'count': '1'},
                           {'v': '40', 'count': '1'}])


@skip(cluster=True)
def testCursorReadsAfterRevalidation():
    """A cursor that resumes after documents were deleted and collected revalidates its iterators,
    and every later row still carries the value its own document matched."""
    env = Env(redisConfigFile=thread_gc_conf(clean_threshold=1, run_interval=1000000000))
    with_unstable_features(cursor_reads_after_revalidation)(env)


def cursor_reads_after_revalidation(env):
    conn = getConnectionByEnv(env)
    # (query, documents deleted after the first row, the (id, value) rows read after them)
    cases = [
        # The document just returned, and the next one the clause matches.
        ('@t:{a} @n:[5 85] => {$as: v}', ['doc:1', 'doc:3'], [('5', '50'), ('7', '70')]),
        ('@n:[5 45] => {$as: v} | @t:{a}', ['doc:1', 'doc:2'],
         [('3', '30.5'), ('4', '40'), ('5', None), ('7', None)]),
    ]
    for query, deleted, expected in cases:
        setup_hash_index(env)
        # No SORTBY: a sorter would drain the iterators before the first row.
        res, cursor = env.cmd('FT.AGGREGATE', 'idx', query, 'LOAD', '1', '@id',
                              'WITHCURSOR', 'COUNT', '1')
        env.assertEqual([to_dict(row)['id'] for row in res[1:]], ['1'], message=query)
        for key in deleted:
            conn.execute_command('DEL', key)
        forceInvokeGC(env)
        # The resumed iterators revalidate only if GC changed the index under them.
        env.assertGreater(int(to_dict(index_info(env)['gc_stats'])['bytes_collected']), 0,
                          message=query)

        rows = []
        while cursor:
            res, cursor = env.cmd('FT.CURSOR', 'READ', 'idx', cursor)
            rows.extend(to_dict(row) for row in res[1:])
        env.assertEqual([(row['id'], row.get('v')) for row in rows], expected, message=query)
