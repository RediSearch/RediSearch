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


def _search(env, idx, direction, optimized):
    query = ['ft.search', idx, '@n:[-inf +inf]', 'SORTBY', 'n', direction,
             'LIMIT', 0, 10, 'NOCONTENT']
    if optimized:
        query.append('WITHOUTCOUNT')
    return env.cmd(*query)


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
