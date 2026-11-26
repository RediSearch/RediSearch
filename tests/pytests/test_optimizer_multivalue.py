# -*- coding: utf-8 -*-

import json
from includes import *
from common import *

# A multivalue numeric field stores one range-tree entry per value, so a doc id
# can repeat with different scores. These tests pin how the numeric SORTBY
# optimizer collapses that back to one result per doc.

# Each doc's first array element is deliberately neither its min nor its max, so
# the returned order alone reveals which value the engine ranks a doc by.
DOCS = {
    'doc:1': [90, 5],
    'doc:2': [40, 60],
    'doc:3': [10],   # single-value control
    'doc:4': [70, 20],
    'doc:5': [50],   # sits between doc:2's first element and its max, separating the two
}

# Ground truth: one result per doc, ranked by the first array element. Holds
# while every value shares one range-tree leaf; values spanning split leaves are
# a separate regime, not covered here.
ASC_IDS = ['doc:3', 'doc:2', 'doc:5', 'doc:4', 'doc:1']
DESC_IDS = list(reversed(ASC_IDS))


def _load(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
               '$.n[*]', 'AS', 'n', 'NUMERIC').ok()
    env.expect('FT.CREATE', 'idx_s', 'ON', 'JSON', 'SCHEMA',
               '$.n[*]', 'AS', 'n', 'NUMERIC', 'SORTABLE').ok()
    for key, arr in DOCS.items():
        conn.execute_command('JSON.SET', key, '$', json.dumps({'n': arr}))


def _search(env, idx, direction, optimized, query='@n:[-inf +inf]', limit=10):
    cmd = ['ft.search', idx, query, 'SORTBY', 'n', direction,
           'LIMIT', 0, limit, 'NOCONTENT']
    if optimized:
        cmd.append('WITHOUTCOUNT')
    return env.cmd(*cmd)


@skip(no_json=True, cluster=True)
def testMultivalueOptimizerDedupsAndRanksByFirst(env):
    """A multivalue numeric doc is returned once by the SORTBY optimizer,
    ranked by its first array element; sortable and non-sortable agree."""
    _load(env)
    for idx in ('idx', 'idx_s'):
        opt_asc = _search(env, idx, 'ASC', optimized=True)
        opt_desc = _search(env, idx, 'DESC', optimized=True)

        env.assertEqual(opt_asc, [len(DOCS)] + ASC_IDS, message=f'{idx} ASC')
        env.assertEqual(opt_desc, [len(DOCS)] + DESC_IDS, message=f'{idx} DESC')

        # No document appears more than once.
        ids = opt_asc[1:]
        env.assertEqual(sorted(ids), sorted(DOCS.keys()), message=f'{idx} distinct')


@skip(no_json=True, cluster=True)
def testMultivalueOptimizerMatchesNonOptimized(env):
    """The optimized (WITHOUTCOUNT) and non-optimized SORTBY paths return the
    same ids in the same order for a multivalue numeric field."""
    _load(env)
    for idx in ('idx', 'idx_s'):
        for direction in ('ASC', 'DESC'):
            plain = _search(env, idx, direction, optimized=False)
            opt = _search(env, idx, direction, optimized=True)
            # plain carries the real count; compare id lists only.
            env.assertEqual(opt[1:], plain[1:],
                            message=f'{idx} {direction} optimized vs plain')


@skip(no_json=True, cluster=True)
def testMultivalueTopKSelectsByBestValue(env):
    """The top-k optimizer picks the docs that make LIMIT by each doc's best
    value, min for ASC and max for DESC, where the plain path picks by the first
    array element. Returned rows are ordered by the first element either way.

    A wildcard query reaches the top-k iterator; a numeric range query takes the
    trimmed-union path instead."""
    conn = getConnectionByEnv(env)
    # In each pair the doc that wins on its best value loses on its first element.
    cases = {
        'DESC': {'best': [1, 30], 'first': [28, 3]},
        'ASC': {'best': [30, 1], 'first': [3, 28]},
    }
    for direction, docs in cases.items():
        idx = f'idx_{direction}'
        env.expect('FT.CREATE', idx, 'ON', 'JSON', 'PREFIX', 1, f'{direction}:',
                   'SCHEMA', '$.n[*]', 'AS', 'n', 'NUMERIC').ok()
        for key, arr in docs.items():
            conn.execute_command('JSON.SET', f'{direction}:{key}', '$',
                                 json.dumps({'n': arr}))
        waitForIndex(env, idx)

        query = ['FT.SEARCH', idx, '*', 'SORTBY', 'n', direction,
                 'LIMIT', 0, 1, 'NOCONTENT']
        env.assertEqual(env.cmd(*query, 'WITHOUTCOUNT')[1:], [f'{direction}:best'],
                        message=f'{direction} optimized')
        env.assertEqual(env.cmd(*query)[1:], [f'{direction}:first'],
                        message=f'{direction} plain')


# Fillers 1..30 split the range tree into several leaves, so the multivalue docs'
# values straddle leaves. As in DOCS, `dB`'s first element is neither its min nor
# its max.
SPLIT_FILLERS = {f'f{i:02d}': [i] for i in range(1, 31)}
SPLIT_MULTI = {'dA': [1, 30], 'dB': [28, 3], 'dC': [1, 3]}


@skip(no_json=True)
def testMultivalueDedupAcrossSplitLeaves(env):
    """A multivalue doc whose values sit in different range-tree leaves is still
    returned once, ranked by its first array element, by both SORTBY paths."""
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
               '$.n[*]', 'AS', 'n', 'NUMERIC', 'SORTABLE').ok()
    for key, arr in {**SPLIT_FILLERS, **SPLIT_MULTI}.items():
        conn.execute_command('JSON.SET', key, '$', json.dumps({'n': arr}))
    waitForIndex(env, 'idx')

    def multi(ids):
        return [i for i in ids if i in SPLIT_MULTI]

    # `dC` has no value in [10 30].
    for query, present in (('@n:[-inf +inf]', ['dA', 'dB', 'dC']), ('@n:[10 30]', ['dA', 'dB'])):
        for direction in ('ASC', 'DESC'):
            plain = _search(env, 'idx', direction, False, query, 100)[1:]
            opt = _search(env, 'idx', direction, True, query, 100)[1:]
            msg = f'{query} {direction}'
            for ids in (plain, opt):
                env.assertEqual(len(ids), len(set(ids)), message=f'duplicate: {msg}')
                env.assertEqual(sorted(multi(ids)), present, message=msg)
            env.assertEqual(sorted(opt), sorted(plain), message=f'id set: {msg}')
            env.assertEqual(multi(opt), multi(plain), message=f'order: {msg}')

    asc = _search(env, 'idx', 'ASC', True, limit=100)[1:]
    desc = _search(env, 'idx', 'DESC', True, limit=100)[1:]
    env.assertEqual([i for i in asc if i in ('dA', 'dB')], ['dA', 'dB'])
    env.assertEqual([i for i in desc if i in ('dA', 'dB')], ['dB', 'dA'])


@skip(no_json=True)
def testMultivalueCrossWindowDedup(env):
    """A multivalue doc matched in two windows of the filtered expand-and-retry
    path takes a single top-k slot.

    The tag clause makes the query an intersection, routing it to the filtered
    path, and the numeric bound excludes 1 and 150 so the tree root is not fully
    contained and the window subdivides. Two matchers sit at the low end and the
    rest at the top, so the first window drains short of k and the source
    expands; `dX`'s values (3 and 140) land in different windows.
    """
    k = 3
    matchers = {'mlo': [2], 'dX': [3, 140], **{f'mhi{v}': [v] for v in range(141, 149)}}
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'xw', 'ON', 'JSON', 'SCHEMA',
               '$.n[*]', 'AS', 'n', 'NUMERIC', 'SORTABLE',
               '$.t', 'AS', 't', 'TAG').ok()
    matcher_values = {v for arr in matchers.values() for v in arr}
    # Fillers take every other value in 1..150, so the tree splits into several leaves.
    for v in range(1, 151):
        if v not in matcher_values:
            conn.execute_command('JSON.SET', f'x{v:03d}', '$',
                                 json.dumps({'n': [v], 't': 'x'}))
    for key, arr in matchers.items():
        conn.execute_command('JSON.SET', key, '$', json.dumps({'n': arr, 't': 'm'}))
    waitForIndex(env, 'xw')

    for extra in ((), ('WITHOUTCOUNT',)):
        ids = env.cmd('FT.SEARCH', 'xw', '@n:[2 149] @t:{m}', 'SORTBY', 'n', 'ASC',
                      'LIMIT', 0, k, 'NOCONTENT', *extra)[1:]
        env.assertEqual(len(ids), len(set(ids)), message=f'duplicate: {extra}')
        env.assertEqual(sorted(ids), sorted(['mlo', 'dX', 'mhi141']), message=f'{extra}')


@skip(cluster=True, no_json=True)
def testFilteredRetryReachesMatchPastMultivalueInflatedRanges(env):
    """The retry window is sized from document entries, not unique documents.

    A multivalue field counts a doc once per value range, so capping the retry
    at the unique doc count would stop short of a selective filter's only match
    in a low range past the inflated high ranges.
    """
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
               '$.vals[*]', 'AS', 'v', 'NUMERIC',
               '$.tag', 'AS', 'tag', 'TAG').ok()

    conn.execute_command('JSON.SET', 'doc:match', '$',
                         json.dumps({'vals': [0.5], 'tag': 'want'}))
    # Each filler holds 100 high values interleaved across the high span, so
    # every high leaf holds all four fillers.
    for d in range(1, 5):
        vals = [1000 + 100 * j + d for j in range(100)]
        conn.execute_command('JSON.SET', f'doc:f{d}', '$',
                             json.dumps({'vals': vals, 'tag': 'other'}))

    res = env.cmd('FT.SEARCH', 'idx', '@tag:{want}',
                  'SORTBY', 'v', 'DESC', 'LIMIT', '0', '1',
                  'NOCONTENT', 'DIALECT', '4')
    env.assertEqual(res, [1, 'doc:match'])
