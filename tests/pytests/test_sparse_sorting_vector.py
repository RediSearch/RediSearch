# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).


# With search-sparse-sorting-vector on, a document of an index with more than 1024 sortable fields
# that can fill fewer than 10% of them keeps its sorting values as (sort index, value) pairs.
# Every other document, and every document with the flag off, keeps one slot per sortable field.
# The layout must never show in a result, so each scenario runs with the flag off and on.

from common import *

FLAG = 'search-sparse-sorting-vector'
N_NUMERIC = 1100  # 1103 sortable fields with the three below


def _set_flag(env, on):
    verify_command_OK_on_all_shards(env, 'CONFIG', 'SET', FLAG, 'yes' if on else 'no')


def _create(env, n_numeric=N_NUMERIC):
    schema = ['title', 'TEXT', 'SORTABLE', 'tag', 'TAG', 'SORTABLE',
              'ns0', 'NUMERIC', 'SORTABLE', 'NOINDEX']
    for i in range(n_numeric):
        schema += [f'n{i}', 'NUMERIC', 'SORTABLE']
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'PREFIX', 1, 'doc:', 'SCHEMA', *schema).ok()


def _layout(env, key):
    info = env.cmd(debug_cmd(), 'DOCINFO', 'idx', key, 'REVEAL')
    info = dict(zip(info[::2], info[1::2]))
    return info.get('sortables_layout', 'dense')


def _sortables_mb(env):
    info = env.cmd('FT.INFO', 'idx')
    return float(dict(zip(info[::2], info[1::2]))['sortable_values_size_mb'])


def _load_docs(conn):
    conn.execute_command('HSET', 'doc:s1', 'title', 'bravo', 'tag', 'x', 'n5', '5', 'n1000', '7')
    conn.execute_command('HSET', 'doc:s2', 'title', 'alpha', 'n5', '3', 'ns0', '40')
    conn.execute_command('HSET', 'doc:s3', 'n1000', '1', 'tag', 'y')
    conn.execute_command('HSET', 'doc:s4', 'title', 'Zulu', 'n1099', '9.5')
    dense = ['title', 'charlie', 'tag', 'x', 'n5', '4']
    for i in range(200):
        if i != 5:
            dense += [f'n{i}', str(i + 100)]
    conn.execute_command('HSET', 'doc:d1', *dense)
    conn.execute_command('HSET', 'doc:e1', 'unrelated', 'x')


def _queries(env):
    out = {}

    def search(name, *args):
        out[name] = env.cmd('FT.SEARCH', 'idx', *args)

    search('sort_num_asc', '*', 'SORTBY', 'n5', 'ASC', 'NOCONTENT')
    search('sort_num_desc', '*', 'SORTBY', 'n5', 'DESC', 'NOCONTENT')
    search('sort_high_index', '*', 'SORTBY', 'n1099', 'DESC', 'NOCONTENT')
    search('sort_text', '*', 'SORTBY', 'title', 'ASC', 'NOCONTENT')
    search('sort_tag', '@n5:[0 100]', 'SORTBY', 'tag', 'DESC', 'NOCONTENT')
    search('sortkeys', '*', 'SORTBY', 'n1000', 'ASC', 'WITHSORTKEYS', 'NOCONTENT')
    search('sort_noindex', '*', 'SORTBY', 'ns0', 'DESC', 'NOCONTENT')
    search('limit', '*', 'SORTBY', 'n5', 'ASC', 'LIMIT', 1, 2, 'NOCONTENT')
    # Fields that are sortable are returned from the sorting vector without a LOAD.
    out['agg_sort'] = env.cmd('FT.AGGREGATE', 'idx', '*', 'SORTBY', 4, '@n1000', 'DESC', '@title', 'ASC')
    out['agg_apply'] = env.cmd('FT.AGGREGATE', 'idx', '@n5:[0 100]', 'APPLY', '@n5 * 2 + 1', 'AS', 'x',
                               'SORTBY', 2, '@x', 'ASC')
    out['agg_group'] = _sorted_rows(env.cmd('FT.AGGREGATE', 'idx', '*', 'GROUPBY', 1, '@tag',
                                      'REDUCE', 'SUM', 1, '@n5', 'AS', 's',
                                      'REDUCE', 'COUNT', 0, 'AS', 'c'))
    out['agg_load'] = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', 3, '@n5', '@n1000', '@title',
                              'SORTBY', 2, '@n5', 'ASC')
    out['agg_filter'] = env.cmd('FT.AGGREGATE', 'idx', '@n1000:[0 100]', 'FILTER', '@n1000 > 0', 'SORTBY', 2,
                                '@n1000', 'ASC')
    return out


def _sorted_rows(res):
    return [res[0]] + sorted(res[1:], key=str)


def _ids(res):
    return [x for x in res[1:] if isinstance(x, str)]


@skip(cluster=True)
def testResultsDoNotDependOnTheFlag(env):
    conn = getConnectionByEnv(env)
    results = []
    for flag in (False, True):
        env.flush()
        _set_flag(env, flag)
        _create(env)
        _load_docs(conn)
        waitForIndex(env, 'idx')
        results.append(_queries(env))
    env.assertEqual(results[1], results[0])

    # Spot-check the values rather than only the agreement.
    on = results[1]
    env.assertEqual(on['sort_num_asc'][1:4], ['doc:s2', 'doc:d1', 'doc:s1'])
    env.assertEqual(on['sort_num_desc'][1:4], ['doc:s1', 'doc:d1', 'doc:s2'])
    env.assertEqual(on['sort_high_index'][1], 'doc:s4')
    env.assertEqual(on['sort_noindex'][1], 'doc:s2')


@skip(cluster=True)
def testLayoutPerDocument(env):
    conn = getConnectionByEnv(env)
    _set_flag(env, True)
    _create(env)
    _load_docs(conn)
    waitForIndex(env, 'idx')
    for key in ('doc:s1', 'doc:s2', 'doc:s3', 'doc:s4'):
        env.assertEqual(_layout(env, key), 'sparse', message=key)
    # 201 of 1103 sortable fields is past the cutoff.
    env.assertEqual(_layout(env, 'doc:d1'), 'dense')

    # Just under and just over the cutoff, counted in the document's fields.
    under = ['n%d' % i for i in range(110)]
    over = ['n%d' % i for i in range(111)]
    conn.execute_command('HSET', 'doc:under', *[x for f in under for x in (f, '1')])
    conn.execute_command('HSET', 'doc:over', *[x for f in over for x in (f, '1')])
    waitForIndex(env, 'idx')
    env.assertEqual(_layout(env, 'doc:under'), 'sparse')
    env.assertEqual(_layout(env, 'doc:over'), 'dense')

    # Rewriting a document re-decides its layout.
    conn.execute_command('DEL', 'doc:s1')
    conn.execute_command('HSET', 'doc:s1', *dense_args(150))
    waitForIndex(env, 'idx')
    env.assertEqual(_layout(env, 'doc:s1'), 'dense')

    _set_flag(env, False)
    conn.execute_command('HSET', 'doc:s2', 'title', 'alpha', 'n5', '3')
    waitForIndex(env, 'idx')
    env.assertEqual(_layout(env, 'doc:s2'), 'dense')
    # Documents written before the flag went off are still read.
    env.assertEqual(_layout(env, 'doc:s3'), 'sparse')
    env.assertEqual(_ids(env.cmd('FT.SEARCH', 'idx', '@tag:{y}', 'SORTBY', 'n1000', 'NOCONTENT')),
                    ['doc:s3'])
    res = env.cmd('FT.AGGREGATE', 'idx', '*', 'SORTBY', 2, '@n1000', 'ASC', 'MAX', 1)
    env.assertEqual(dict(zip(res[1][::2], res[1][1::2]))['n1000'], '1')


def dense_args(n):
    return [x for i in range(n) for x in (f'n{i}', str(i))]


@skip(cluster=True)
def testSmallSchemasStayDense(env):
    conn = getConnectionByEnv(env)
    _set_flag(env, True)
    _create(env, n_numeric=1020)  # 1023 sortable fields
    conn.execute_command('HSET', 'doc:a', 'n5', '1')
    waitForIndex(env, 'idx')
    env.assertEqual(_layout(env, 'doc:a'), 'dense')
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'extra1', 'NUMERIC', 'SORTABLE', 'extra2',
               'NUMERIC', 'SORTABLE').ok()
    waitForIndexFinishScan(env)
    # 1025 sortable fields: the next write is sparse, the old document keeps being read.
    conn.execute_command('HSET', 'doc:b', 'n5', '2')
    waitForIndex(env, 'idx')
    env.assertEqual(_layout(env, 'doc:b'), 'sparse')
    res = env.cmd('FT.SEARCH', 'idx', '*', 'SORTBY', 'n5', 'DESC', 'NOCONTENT')
    env.assertEqual(res[1:], ['doc:b', 'doc:a'])


@skip(cluster=True)
def testAlterSortableFieldOverSparseDocuments(env):
    conn = getConnectionByEnv(env)
    _set_flag(env, True)
    _create(env)
    _load_docs(conn)
    waitForIndex(env, 'idx')
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'late', 'NUMERIC', 'SORTABLE').ok()
    waitForIndexFinishScan(env)
    conn.execute_command('HSET', 'doc:s2', 'late', '11')
    conn.execute_command('HSET', 'doc:d1', 'late', '12')
    waitForIndex(env, 'idx')
    res = env.cmd('FT.SEARCH', 'idx', '@late:[0 100]', 'SORTBY', 'late', 'DESC', 'NOCONTENT')
    env.assertEqual(res[1:], ['doc:d1', 'doc:s2'])
    # The values the documents had before the field existed are still there.
    res = env.cmd('FT.SEARCH', 'idx', '@late:[0 100]', 'SORTBY', 'n5', 'ASC', 'NOCONTENT')
    env.assertEqual(res[1:], ['doc:s2', 'doc:d1'])
    res = env.cmd('FT.AGGREGATE', 'idx', '@late:[0 100]', 'APPLY', '@late + @n5', 'AS', 'sum',
                  'SORTBY', 2, '@sum', 'ASC')
    env.assertEqual([dict(zip(r[::2], r[1::2]))['sum'] for r in res[1:]], ['14', '16'])


def _queries_without_ties(env):
    """Sorts over documents that all have the sort key, so that their order is fully defined."""
    out = {}
    out['n5'] = env.cmd('FT.SEARCH', 'idx', '@n5:[0 100]', 'SORTBY', 'n5', 'DESC', 'NOCONTENT')
    out['n1000'] = env.cmd('FT.SEARCH', 'idx', '@n1000:[0 100]', 'SORTBY', 'n1000', 'ASC',
                           'WITHSORTKEYS', 'NOCONTENT')
    out['title'] = env.cmd('FT.SEARCH', 'idx', '@n5:[0 100]', 'SORTBY', 'title', 'ASC', 'NOCONTENT')
    out['apply'] = env.cmd('FT.AGGREGATE', 'idx', '@n5:[0 100]', 'APPLY', '@n5 * 2 + 1', 'AS', 'x',
                           'SORTBY', 2, '@x', 'ASC')
    return out


@skip(cluster=True)
def testReloadRebuildsTheVectors(env):
    conn = getConnectionByEnv(env)
    _set_flag(env, True)
    _create(env)
    _load_docs(conn)
    waitForIndex(env, 'idx')
    before = _queries_without_ties(env)
    env.assertEqual(before['n5'][1:], ['doc:s1', 'doc:d1', 'doc:s2'])
    env.cmd('DEBUG', 'RELOAD')
    waitForIndex(env, 'idx')
    env.assertEqual(_queries_without_ties(env), before)
    env.assertEqual(_layout(env, 'doc:s1'), 'sparse')
    env.assertEqual(_layout(env, 'doc:d1'), 'dense')


@skip(cluster=True)
def testPartialUpdateOfSortableValues(env):
    conn = getConnectionByEnv(env)
    for flag in (False, True):
        env.flush()
        _set_flag(env, flag)
        _create(env)
        _load_docs(conn)
        waitForIndex(env, 'idx')
        # FT.ADD ... PARTIAL touching only a NOINDEX sortable field updates the vector in place.
        env.expect('FT.ADD', 'idx', 'doc:s1', '1.0', 'REPLACE', 'PARTIAL', 'FIELDS', 'ns0', '55').ok()
        env.expect('FT.ADD', 'idx', 'doc:d1', '1.0', 'REPLACE', 'PARTIAL', 'FIELDS', 'ns0', '30').ok()
        env.expect('FT.ADD', 'idx', 'doc:s2', '1.0', 'REPLACE', 'PARTIAL', 'FIELDS', 'ns0', '41').ok()
        # A document that had no vector gets one holding just the updated value.
        env.expect('FT.ADD', 'idx', 'doc:e1', '1.0', 'REPLACE', 'PARTIAL', 'FIELDS', 'ns0', '99').ok()
        res = env.cmd('FT.SEARCH', 'idx', '*', 'SORTBY', 'ns0', 'DESC', 'NOCONTENT')
        env.assertEqual(res[1:5], ['doc:e1', 'doc:s1', 'doc:s2', 'doc:d1'], message=f'flag={flag}')
        # The other values of the document are untouched.
        res = env.cmd('FT.SEARCH', 'idx', '*', 'SORTBY', 'n5', 'ASC', 'NOCONTENT')
        env.assertEqual(res[1:4], ['doc:s2', 'doc:d1', 'doc:s1'], message=f'flag={flag}')
        res = env.cmd('FT.AGGREGATE', 'idx', '@tag:{x}', 'SORTBY', 2, '@ns0', 'DESC')
        env.assertEqual([dict(zip(r[::2], r[1::2]))['ns0'] for r in res[1:]], ['55', '30'])


@skip(cluster=True)
def testSortablesSizeFollowsTheLayout(env):
    conn = getConnectionByEnv(env)
    n_docs = 1500
    sizes = {}
    for flag in (False, True):
        env.flush()
        _set_flag(env, flag)
        _create(env)
        p = conn.pipeline(transaction=False)
        for i in range(n_docs):
            p.execute_command('HSET', f'doc:{i}', 'title', f't{i}', 'n7', i, 'n900', i, 'n1099', i)
        p.execute()
        waitForIndex(env, 'idx')
        sizes[flag] = _sortables_mb(env)
        # Partial updates on top of every document, then remove them all: nothing is left over.
        for i in range(0, n_docs, 3):
            env.expect('FT.ADD', 'idx', f'doc:{i}', '1.0', 'REPLACE', 'PARTIAL', 'FIELDS',
                       'ns0', i).ok()
        for i in range(n_docs):
            conn.execute_command('DEL', f'doc:{i}')
        waitForIndex(env, 'idx')
        env.assertEqual(_sortables_mb(env), 0.0, message=f'flag={flag}')
    # 1103 slots of 8 bytes against 4 values: the dense layout is over 8 KB a document.
    env.assertGreater(sizes[False], 10.0)
    env.assertLess(sizes[True], 2.0)


def _create_with_noindex_sortables(env, n_noindex=16):
    schema = ['title', 'TEXT', 'SORTABLE']
    for i in range(N_NUMERIC):
        schema += [f'n{i}', 'NUMERIC', 'SORTABLE']
    for i in range(n_noindex):
        schema += [f'm{i}', 'NUMERIC', 'SORTABLE', 'NOINDEX']
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'PREFIX', 1, 'doc:', 'SCHEMA', *schema).ok()


def _rows(res):
    return [dict(zip(r[::2], r[1::2])) for r in res]


@skip(cluster=True)
def testCursorHoldingASparseDocumentSurvivesPartialUpdates(env):
    # A cursor keeps the rows of the documents it has not returned yet, and the vector of a document
    # grows with every sortable value a partial update adds to it. Reading the document through the
    # cursor afterwards must see its vector as it is now, and must not touch freed memory (the
    # sanitizer build is what tells the two apart).
    conn = getConnectionByEnv(env)
    n_noindex = 16
    for flag in (False, True):
        env.flush()
        _set_flag(env, flag)
        _create_with_noindex_sortables(env, n_noindex)
        for i, key in enumerate(('doc:a', 'doc:b', 'doc:c', 'doc:d')):
            conn.execute_command('HSET', key, 'n5', i + 1, 'n6', 10 * (i + 1))
        waitForIndex(env, 'idx')
        if flag:
            env.assertEqual(_layout(env, 'doc:b'), 'sparse')

        res, cursor = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', 2, '@n5', '@n6', 'SORTBY', 2, '@n5', 'ASC',
                              'MAX', 100, 'WITHCURSOR', 'COUNT', 1)
        first = _rows(res[1:])
        env.assertEqual(first, [{'n5': '1', 'n6': '10'}], message=f'flag={flag}')
        env.assertNotEqual(cursor, 0)

        # Each update adds a value the documents in the cursor did not have, forcing the vector to grow.
        for i in range(n_noindex):
            for key in ('doc:b', 'doc:c', 'doc:d'):
                env.expect('FT.ADD', 'idx', key, '1.0', 'REPLACE', 'PARTIAL', 'FIELDS', f'm{i}', 100 + i).ok()
            # Take the freed memory back, so that a stale pointer would find something else in it.
            conn.execute_command('HSET', f'doc:junk{i}', 'title', 'x' * (16 * (i + 1)), 'n1', i)

        rest = []
        while cursor:
            res, cursor = env.cmd('FT.CURSOR', 'READ', 'idx', cursor, 'COUNT', 1)
            rest += _rows(res[1:])
        env.assertEqual(rest, [{'n5': '2', 'n6': '20'}, {'n5': '3', 'n6': '30'}, {'n5': '4', 'n6': '40'}],
                        message=f'flag={flag}')

        # A new read sees the values the partial updates stored.
        res = env.cmd('FT.AGGREGATE', 'idx', '@n5:[2 4]', 'LOAD', 3, '@n5', '@m0', '@m15', 'SORTBY', 2, '@n5', 'ASC')
        env.assertEqual(_rows(res[1:]), [{'n5': str(i), 'm0': '100', 'm15': '115'} for i in (2, 3, 4)],
                        message=f'flag={flag}')


@skip(cluster=True)
def testAlterCrossingTheSparseThresholdKeepsBothLayoutsReadable(env):
    conn = getConnectionByEnv(env)
    for flag in (False, True):
        env.flush()
        _set_flag(env, flag)
        _create(env, n_numeric=1018)  # 1021 sortable fields, 1024 is the last that is dense
        conn.execute_command('HSET', 'doc:old1', 'title', 'b', 'n5', '5', 'n900', '1')
        conn.execute_command('HSET', 'doc:old2', 'title', 'a', 'n5', '3')
        waitForIndex(env, 'idx')
        env.assertEqual(_layout(env, 'doc:old1'), 'dense')

        # SKIPINITIALSCAN leaves the documents that are there with the vectors they have, so that
        # both layouts live side by side.
        env.expect('FT.ALTER', 'idx', 'SKIPINITIALSCAN', 'SCHEMA', 'ADD', 'x1', 'NUMERIC', 'SORTABLE',
                   'x2', 'NUMERIC', 'SORTABLE', 'x3', 'NUMERIC', 'SORTABLE', 'x4', 'NUMERIC', 'SORTABLE').ok()  # 1025
        waitForIndexFinishScan(env)
        env.assertEqual(_layout(env, 'doc:old1'), 'dense')
        conn.execute_command('HSET', 'doc:new1', 'title', 'c', 'n5', '4', 'x1', '9')
        conn.execute_command('HSET', 'doc:new2', 'x2', '2', 'n5', '1')
        waitForIndex(env, 'idx')
        env.assertEqual(_layout(env, 'doc:old1'), 'dense')
        env.assertEqual(_layout(env, 'doc:new1'), 'sparse' if flag else 'dense')

        res = env.cmd('FT.SEARCH', 'idx', '@n5:[0 100]', 'SORTBY', 'n5', 'ASC', 'NOCONTENT')
        env.assertEqual(res[1:], ['doc:new2', 'doc:old2', 'doc:new1', 'doc:old1'], message=f'flag={flag}')
        res = env.cmd('FT.AGGREGATE', 'idx', '@n5:[0 100]', 'LOAD', 3, '@n5', '@x1', '@x2', 'SORTBY', 2, '@n5', 'ASC')
        env.assertEqual(_rows(res[1:]),
                        [{'n5': '1', 'x2': '2'}, {'n5': '3'}, {'n5': '4', 'x1': '9'}, {'n5': '5'}],
                        message=f'flag={flag}')
        # Every document accepts the new fields, whichever layout it has.
        for key, val in (('doc:old1', '7'), ('doc:new1', '8')):
            conn.execute_command('HSET', key, 'x3', val)
        waitForIndex(env, 'idx')
        res = env.cmd('FT.SEARCH', 'idx', '@x3:[0 100]', 'SORTBY', 'x3', 'DESC', 'NOCONTENT')
        env.assertEqual(res[1:], ['doc:new1', 'doc:old1'], message=f'flag={flag}')
        # A plain ALTER re-reads every document, which gives them all the layout of the schema.
        env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'x5', 'NUMERIC', 'SORTABLE').ok()
        waitForIndexFinishScan(env)
        env.assertEqual(_layout(env, 'doc:old1'), 'sparse' if flag else 'dense')
        res = env.cmd('FT.SEARCH', 'idx', '@n5:[0 100]', 'SORTBY', 'n5', 'ASC', 'NOCONTENT')
        env.assertEqual(res[1:], ['doc:new2', 'doc:old2', 'doc:new1', 'doc:old1'], message=f'flag={flag}')
        res = env.cmd('FT.SEARCH', 'idx', '@x3:[0 100]', 'SORTBY', 'x3', 'DESC', 'NOCONTENT')
        env.assertEqual(res[1:], ['doc:new1', 'doc:old1'], message=f'flag={flag}')
