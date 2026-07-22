# -*- coding: utf-8 -*-
#
# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

import json

from common import *
from includes import *

# Guards the multivalue numeric SORTBY dedup contract across the C -> Rust
# numeric top-k swap.
#
# A multivalue numeric doc (a JSON array field) must appear exactly once in a
# SORTBY result even when its values land in different range-tree leaves. The
# assertions are split into two tiers:
#
#   Tier 1 (impl-agnostic): no duplicate doc ids, distinct-doc count, and
#     presence/absence by range membership. These must hold on C today and on
#     the Rust value-ordered batching path after the swap.
#
#   Tier 2 (C semantic pin): the exact ASC/DESC ordering of the multivalue
#     docs. C ranks a multivalue numeric field by its FIRST array element. The
#     Rust value-ordered path instead ranks by min (ASC) / max (DESC) and cannot
#     cheaply reproduce "first element", so a Tier 2 assertion is expected to
#     change on swap. Each such assertion is tagged RUST_SWAP below.
#
# Coverage boundary. `NumericRangeTree::find` deliberately minimizes the ranges
# it returns (a fully contained subtree collapses to one range), and a single
# numeric interval has only two boundaries, so a plain SORTBY query yields far
# fewer than DEFAULT_RANGE_BATCH_SIZE (8) ranges. `testMultivalueNumericSortbyDedup`
# therefore only exercises the Rust *within-batch* coalesce, never the
# *cross-batch* `emitted` set. The deterministic cross-batch guard is the Rust
# integration test `multivalue_doc_spanning_batches_is_scored_once...`
# (numeric_score_source), which drives `next_n(1)`. From pytest the only reachable
# path that spans `emitted` across windows is the filtered ExpandWindow retry,
# covered by `testMultivalueCrossWindowDedup` below.

IDX = 'idx'

# id -> array value for `n`. Filler docs span 1..30 with one value each so the
# range tree splits into multiple leaves (MINIMUM_RANGE_CARDINALITY = 16 plus
# HLL slack). The multivalue docs then straddle those leaves.
FILLERS = {f'f{i:02d}': [i] for i in range(1, 31)}

# Multivalue docs. `dB` is the discriminator: first(28), min(3) and max(28) all
# disagree, so its observed rank reveals which ranking key the engine uses.
MULTI = {
    'dA': [1, 30],   # first 1,  min 1,  max 30
    'dB': [28, 3],   # first 28, min 3,  max 28
    'dC': [1, 3],    # both values low; absent from a [10 30] window
}


def _load(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', IDX, 'ON', 'JSON', 'SCHEMA',
               '$.n[*]', 'AS', 'n', 'NUMERIC', 'SORTABLE').ok()
    for doc_id, arr in {**FILLERS, **MULTI}.items():
        conn.execute_command('JSON.SET', doc_id, '$', json.dumps({'n': arr}))
    waitForIndex(env, IDX)


def _search_ids(env, query, order, extra=()):
    """Return the ordered doc-id list of a NOCONTENT SORTBY search.

    `extra` carries optimizer toggles such as ('WITHOUTCOUNT',). Any error
    reply raises, satisfying the Tier 1 "no crash / no error" assertion.
    """
    res = env.cmd('FT.SEARCH', IDX, query, 'SORTBY', 'n', order,
                  'LIMIT', 0, 100, 'NOCONTENT', *extra)
    # NOCONTENT layout: [total, id1, id2, ...]
    return res[1:]


def _relative(ids, subset):
    """Order of `subset` ids as they appear in `ids` (fillers stripped out)."""
    return [i for i in ids if i in subset]


@skip(no_json=True)
def testMultivalueNumericSortbyDedup(env):
    _load(env)

    multi_ids = set(MULTI)
    # Docs matching the [10 30] window: dA via 30, dB via 28; dC has neither.
    window = '@n:[10 30]'
    window_expected_present = {'dA', 'dB'}
    window_expected_absent = {'dC'}

    ranges = ['@n:[-inf +inf]', window]
    orders = ['ASC', 'DESC']
    variants = [('non-optimized', ()), ('optimized', ('WITHOUTCOUNT',))]

    # ---- Tier 1: impl-agnostic invariants (must survive the Rust swap) ----
    for rng in ranges:
        for order in orders:
            for label, extra in variants:
                ids = _search_ids(env, rng, order, extra)
                msg = f'{label} {rng} {order}'

                # 1. No duplicate doc id.
                env.assertEqual(len(ids), len(set(ids)), message=f'dup: {msg}')

                # 3. Presence/absence by range membership.
                for present in (window_expected_present if rng == window
                                else multi_ids):
                    env.assertContains(present, ids, message=f'present {present}: {msg}')
                if rng == window:
                    for absent in window_expected_absent:
                        env.assertNotIn(absent, ids, message=f'absent {absent}: {msg}')

            # 2. Distinct-doc count: optimized and non-optimized return the same
            #    set of ids, each exactly once.
            not_opt = _search_ids(env, rng, order, ())
            opt = _search_ids(env, rng, order, ('WITHOUTCOUNT',))
            env.assertEqual(sorted(not_opt), sorted(opt),
                            message=f'id-set parity {rng} {order}')

    # ---- Tier 2: C semantic pin (RUST_SWAP assertions change on swap) ----
    # C ranks a multivalue field by its first array element.
    #   ASC  by first: dA(1) < dC(1, id-tiebreak) < dB(28)
    #   DESC by first: dB(28) > dA(1) > dC(1)
    # The Rust min/max path would instead put dB after dA on DESC (max 28 < 30),
    # so the DESC pin is the one that flips.
    asc = _search_ids(env, '@n:[-inf +inf]', 'ASC')
    desc = _search_ids(env, '@n:[-inf +inf]', 'DESC')

    # RUST_SWAP: min(ASC) agrees with first here (dA/dC both min 1), so this pin
    # happens to survive; kept for symmetry with the DESC pin.
    env.assertEqual(_relative(asc, {'dA', 'dB'}), ['dA', 'dB'],
                    message='Tier2 ASC first-element order')

    # RUST_SWAP: C first-element ranks dB(28) ahead of dA(1) on DESC; the Rust
    # max path ranks dA(30) ahead of dB(28). Flip to ['dA', 'dB'] after swap.
    env.assertEqual(_relative(desc, {'dA', 'dB'}), ['dB', 'dA'],
                    message='Tier2 DESC first-element order')

    # 6. Optimized/non-optimized ordering parity on C. A disagreement is a
    #    latent C bug to report separately, not a dedup regression.
    for order in orders:
        not_opt = _search_ids(env, '@n:[-inf +inf]', order)
        opt = _search_ids(env, '@n:[-inf +inf]', order, ('WITHOUTCOUNT',))
        env.assertEqual(_relative(not_opt, {'dA', 'dB', 'dC'}),
                        _relative(opt, {'dA', 'dB', 'dC'}),
                        message=f'Tier2 opt/non-opt order parity {order}')


# Index for the cross-window case. A tag filter child routes the numeric SORTBY
# through the filtered ExpandWindow path (a plain numeric sort reads one unbounded
# window and never retries). The initial value window drains with heap_count < k
# and the source advances to the next disjoint window. `dX`'s two matching values
# sit in different windows, so it is emitted in the first and re-read in the later
# one; the persistent `emitted` set is what must collapse that second read.
#
# Three structural conditions are required for the filtered window to subdivide
# and reach `dX` twice; all are baked into the data and query below:
#
#   * Bounded numeric filter. `find` returns a fully contained subtree as one
#     range, so a `[-inf +inf]` filter collapses to the root and never windows.
#     The query uses `@n:[2 149]`, strictly inside the 1..150 value span, so the
#     root is not contained and `filter.limit` subdivides the tree.
#   * Intersection routing. The Rust filtered child path is only taken when the
#     query root is not a bare numeric node, so the query is `@n:[...] @t:{m}`
#     under WITHOUTCOUNT (the optimizer is off without it, and a plain SORTBY
#     goes to the C sorter).
#   * Far-apart straddle. The first overlapping leaf contributes only 1 to the
#     window's running total, and the initial limit is >= k, so window 1 always
#     spans several low leaves. `dX`'s two values sit at opposite ends (3 and
#     140) so the low value is in window 1 and the high value in a later window.
XW_IDX = 'xw'
XW_K = 3
XW_MAX = 150
# Numeric bound excludes 1 and 150 so the tree root is not contained; the tag
# clause makes the query an intersection so it routes to the Rust filtered path.
XW_QUERY = '@n:[2 149] @t:{m}'
# Matching docs (tag 'm'). Two matchers sit at the low end; the rest are packed
# at the top so window 1 drains with heap_count < k and the source expands.
# `dX` straddles the lowest and highest leaves.
XW_LOW = {'mlo': [2], 'dX': [3, 140]}
XW_HIGH = {f'mhi{v}': [v] for v in range(141, 149)}   # 141..148
XW_MATCHERS = {**XW_LOW, **XW_HIGH}
# After dedup, ASC top-k is the k smallest distinct matchers by best (min) value:
# mlo(2), dX(3), then the smallest high matcher.
XW_EXPECTED = sorted(['mlo', 'dX', 'mhi141'])


@skip(no_json=True)
def testMultivalueCrossWindowDedup(env):
    """Cross-window (ExpandWindow) dedup: `dX` must not occupy two heap slots.

    Validation. This only guards the `emitted` set if the filtered window
    actually expands across `dX`'s two values, which depends on internal
    heuristics (estimate_limit, MIN_SUCCESS_RATIO, MAX_ITERATIONS) and on
    HLL-estimated leaf boundaries that pytest cannot observe. To confirm it is a
    real guard, revert the cross-window fix (drop `emitted`, or make `refind`
    clear it) on the Rust build and check the WITHOUTCOUNT case fails with `dX`
    duplicated. If it still passes, the window did not split `dX` for this
    dataset; tune, in order: raise XW_MAX for more leaves, lower the matcher
    count so `estimate_limit` yields a narrower initial window, or push the high
    matchers further from `dX`'s high value (140).
    """
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', XW_IDX, 'ON', 'JSON', 'SCHEMA',
               '$.n[*]', 'AS', 'n', 'NUMERIC', 'SORTABLE',
               '$.t', 'AS', 't', 'TAG').ok()

    matcher_values = {v for arr in XW_MATCHERS.values() for v in arr}
    # Fillers (tag 'x') occupy every value a matcher does not, giving 150 distinct
    # values so the range tree splits into several leaves.
    for v in range(1, XW_MAX + 1):
        if v not in matcher_values:
            conn.execute_command('JSON.SET', f'x{v:03d}', '$',
                                 json.dumps({'n': [v], 't': 'x'}))
    for doc_id, arr in XW_MATCHERS.items():
        conn.execute_command('JSON.SET', doc_id, '$',
                             json.dumps({'n': arr, 't': 'm'}))
    waitForIndex(env, XW_IDX)

    # Non-optimized goes through the C sorter (always dedups); optimized engages
    # the Rust filtered top-k, the path under test. Both must return `dX` once.
    for extra in ((), ('WITHOUTCOUNT',)):
        res = env.cmd('FT.SEARCH', XW_IDX, XW_QUERY,
                      'SORTBY', 'n', 'ASC', 'LIMIT', 0, XW_K, 'NOCONTENT', *extra)
        ids = res[1:]
        label = 'opt' if extra else 'non-opt'
        env.assertEqual(len(ids), len(set(ids)), message=f'dup: {label}')
        env.assertEqual(sorted(ids), XW_EXPECTED, message=f'id-set: {label}')
