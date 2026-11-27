# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Flow tests for FT.HYBRID with any non-empty set of arms: SEARCH, a dense VSIM and a sparse
VSIM, in one query."""

import random
import struct

from common import *

RRF_CONSTANT = 60


def sparse_blob(pairs):
    """The HASH/query encoding: little-endian (u32 dim_id, f32 weight) pairs."""
    return b''.join(struct.pack('<If', i, w) for i, w in pairs)


def setup_index(env, n=60, seed=7):
    """Every other document matches `red`, each with a title of its own length, so no two text
    scores tie; dense and sparse vectors are random, so neither do their scores. Ranks are then
    the same on every run, in standalone and in cluster."""
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT',
               'emb', 'VECTOR', 'FLAT', 6, 'TYPE', 'FLOAT32', 'DIM', 2, 'DISTANCE_METRIC', 'L2',
               'sp', 'SPARSEVECTOR', 4, 'DIM', 50, 'DISTANCE_METRIC', 'IP').ok()
    rnd = random.Random(seed)
    for k in range(n):
        title = 'red' + ' coat' * (k // 2) if k % 2 == 0 else 'blue shoes'
        dims = sorted(rnd.sample(range(50), rnd.randint(1, 8)))
        conn.execute_command('HSET', f'doc:{k}', 'title', title,
                             'emb', struct.pack('<2f', rnd.uniform(0, 10), rnd.uniform(0, 10)),
                             'sp', sparse_blob([(i, rnd.uniform(0.01, 3.0)) for i in dims]))
    return sparse_blob([(i, 1.0) for i in range(0, 50, 3)])


ARMS = {
    'text': ['SEARCH', 'red'],
    'dense': ['VSIM', '@emb', '$v'],
    'sparse': ['VSIM', '@sp', '$q'],
}


def hybrid(env, q, arms, *args):
    """`{key: score}` of an FT.HYBRID over `arms`, in that command order: each an ARMS key, or an
    arm's whole clause as a list."""
    clauses = [token for arm in arms for token in (ARMS[arm] if isinstance(arm, str) else arm)]
    res = to_dict(env.cmd('FT.HYBRID', 'idx', len(arms), *clauses, *args, 'LIMIT', 0, 100,
                          'PARAMS', 4, 'v', struct.pack('<2f', 5.0, 5.0), 'q', q))
    return {row['__key']: float(row['__score'])
            for row in (to_dict(r) for r in res['results'])}


def arm_ranks(env, q, arm):
    """`{key: rank}` of one arm alone: RRF scores a lone arm's document 1 / (CONSTANT + rank)."""
    return {key: round(1 / score - RRF_CONSTANT) for key, score in hybrid(env, q, [arm]).items()}


def reference_rrf(ranks_by_arm, weights=None):
    weights = weights or [1.0] * len(ranks_by_arm)
    fused = {}
    for w, ranks in zip(weights, ranks_by_arm):
        for key, rank in ranks.items():
            fused[key] = fused.get(key, 0.0) + w / (RRF_CONSTANT + rank)
    return fused


def assert_scores(env, got, expected):
    env.assertEqual(sorted(got), sorted(expected))
    for key, score in expected.items():
        env.assertAlmostEqual(got[key], score, 1e-9)


def testThreeArmsMatchReference(env):
    """Text + dense + sparse fuses each arm's rank, as each arm ranks alone."""
    enable_unstable_features(env)
    q = setup_index(env)
    ranks = [arm_ranks(env, q, arm) for arm in ('text', 'dense', 'sparse')]
    for ranking in ranks:
        env.assertEqual(sorted(ranking.values()), list(range(1, len(ranking) + 1)))
    assert_scores(env, hybrid(env, q, ['text', 'dense', 'sparse']), reference_rrf(ranks))
    # Command order does not matter without WEIGHTS: the arms fuse in slot order.
    assert_scores(env, hybrid(env, q, ['text', 'sparse', 'dense']), reference_rrf(ranks))


def testEverySubsetOfArms(env):
    """Any non-empty set of arms is a query, including dense + sparse with no text arm."""
    enable_unstable_features(env)
    q = setup_index(env)
    ranks = {arm: arm_ranks(env, q, arm) for arm in ('text', 'dense', 'sparse')}
    for arms in (['dense', 'sparse'], ['text', 'sparse'], ['text', 'dense'], ['sparse'],
                 # SEARCH last: the tail's arguments follow it.
                 ['sparse', 'text'], ['dense', 'sparse', 'text']):
        assert_scores(env, hybrid(env, q, arms), reference_rrf([ranks[a] for a in arms]))


def testOptionsStayWithTheirArm(env):
    """Each VSIM arm runs with its own options, in standalone and on every shard: a VSIM arm
    without KNN keeps the default K though the next arm sets one, a RANGE arm stays a range query,
    and a dense-only option on the second arm does not reach the first."""
    enable_unstable_features(env)
    q = setup_index(env)
    sparse_k3 = ['VSIM', '@sp', '$q', 'KNN', 2, 'K', 3]
    dense_range = ['VSIM', '@emb', '$v', 'RANGE', 2, 'RADIUS', 8]
    dense_ratio = ['VSIM', '@emb', '$v', 'KNN', 4, 'K', 10, 'SHARD_K_RATIO', 0.9]
    for arms in (['dense', sparse_k3], [dense_range, sparse_k3], ['sparse', dense_ratio]):
        expected = reference_rrf([arm_ranks(env, q, arm) for arm in arms])
        assert_scores(env, hybrid(env, q, arms), expected)
    # The K that applied, per arm.
    env.assertEqual(len(arm_ranks(env, q, sparse_k3)), 3)
    env.assertEqual(len(arm_ranks(env, q, 'dense')), 10)


def testWeightsFollowCommandOrder(env):
    """WEIGHTS are given in command order, whatever order the arms fuse in."""
    enable_unstable_features(env)
    q = setup_index(env)
    ranks = {arm: arm_ranks(env, q, arm) for arm in ('text', 'dense', 'sparse')}
    got = hybrid(env, q, ['sparse', 'dense'], 'COMBINE', 'RRF', 4, 'WEIGHTS', 2, 2, 0.5)
    assert_scores(env, got, reference_rrf([ranks['sparse'], ranks['dense']], [2, 0.5]))
    got = hybrid(env, q, ['text', 'sparse', 'dense'], 'COMBINE', 'RRF', 5, 'WEIGHTS', 3, 1, 0, 3)
    assert_scores(env, got, reference_rrf([ranks['text'], ranks['sparse'], ranks['dense']],
                                          [1, 0, 3]))


def testLinearWithThreeArms(env):
    """LINEAR with three arms weighs each arm's normalized score by its WEIGHTS entry."""
    enable_unstable_features(env)
    q = setup_index(env)
    normalized = {arm: hybrid(env, q, [arm], 'COMBINE', 'LINEAR', 3, 'WEIGHTS', 1, 1)
                  for arm in ('text', 'dense', 'sparse')}
    weights = {'text': 0.2, 'dense': 0.5, 'sparse': 0.3}
    expected = {}
    for arm, scores in normalized.items():
        for key, score in scores.items():
            expected[key] = expected.get(key, 0.0) + weights[arm] * score
    got = hybrid(env, q, ['text', 'dense', 'sparse'], 'COMBINE', 'LINEAR', 5,
                 'WEIGHTS', 3, weights['text'], weights['dense'], weights['sparse'])
    env.assertEqual(sorted(got), sorted(expected))
    for key, score in expected.items():
        env.assertAlmostEqual(got[key], score, 1e-6)


def testThreeArmsExplainScore(env):
    """EXPLAINSCORE has one branch per arm, in slot order."""
    enable_unstable_features(env)
    q = setup_index(env)
    res = to_dict(env.cmd('FT.HYBRID', 'idx', 3, 'VSIM', '@sp', '$q', 'SEARCH', 'red',
                          'VSIM', '@emb', '$v', 'EXPLAINSCORE', 'LIMIT', 0, 1,
                          'PARAMS', 4, 'v', struct.pack('<2f', 5.0, 5.0), 'q', q))
    explain = to_dict(res['results'][0])['score']
    hybrid_node = explain[1][1][0]
    # With a dense and a sparse arm, a "no match" placeholder names which one.
    res = to_dict(env.cmd('FT.HYBRID', 'idx', 2, 'VSIM', '@emb', '$v', 'VSIM', '@sp', '$q',
                          'EXPLAINSCORE', 'LIMIT', 0, 100,
                          'PARAMS', 4, 'v', struct.pack('<2f', 5.0, 5.0), 'q', q))
    finals = [to_dict(row)['score'][1][0] for row in res['results']]
    placeholders = {p for line in finals for p in ('[dense: no match]', '[sparse: no match]',
                                                   '[vector: no match]') if p in line}
    env.assertEqual(placeholders, {'[dense: no match]', '[sparse: no match]'}, message=finals)
    branches = hybrid_node[1]
    env.assertEqual(len(branches), 3, message=explain)
    heads = [b if isinstance(b, str) else b[0] for b in branches]
    env.assertTrue(heads[0].startswith('text rank'), message=heads)
    env.assertEqual(heads[1:], ['vector branch (KNN)',
                                'sparse vector branch (KNN, IP: raw dot product)'])


def testProfileLabelsEachVsimArm(env):
    """With two VSIM arms, FT.PROFILE names each by its field, per shard in a cluster."""
    enable_unstable_features(env)
    q = setup_index(env)
    res = env.cmd('FT.PROFILE', 'idx', 'HYBRID', 'QUERY', 3, 'SEARCH', 'red',
                  'VSIM', '@emb', '$v', 'VSIM', '@sp', '$q',
                  'PARAMS', 4, 'v', struct.pack('<2f', 5.0, 5.0), 'q', q)
    shards = [to_dict(shard) for shard in to_dict(res[-1])['Shards']]
    env.assertEqual(len(shards), env.shardsCount)
    for shard in shards:
        env.assertEqual([k for k in shard if k != 'Shard ID'], ['SEARCH', 'VSIM @emb', 'VSIM @sp'])
        env.assertEqual(to_dict(to_dict(shard['VSIM @sp'])['Iterators profile'])['Type'],
                        'SPARSE_VECTOR')


def testPrefixWarningNamesItsVsimArm(env):
    """A FILTER hitting the prefix-expansion limit warns for its own VSIM arm only."""
    enable_unstable_features(env)
    conn = getConnectionByEnv(env)
    q = setup_index(env)
    for k, title in enumerate(['cola', 'cold', 'colt']):
        conn.execute_command('HSET', f'extra:{k}', 'title', title,
                             'emb', struct.pack('<2f', 5.0, 5.0), 'sp', sparse_blob([(0, 1.0)]))
    run_command_on_all_shards(env, config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', '1')
    try:
        res = to_dict(env.cmd('FT.HYBRID', 'idx', 2, 'VSIM', '@emb', '$v',
                              'VSIM', '@sp', '$q', 'FILTER', '@title:col*',
                              'PARAMS', 4, 'v', struct.pack('<2f', 5.0, 5.0), 'q', q))
        prefix = [w for w in res['warnings'] if w.startswith('Max prefix expansions')]
        env.assertEqual(prefix, ['Max prefix expansions limit was reached (VSIM @sp)'],
                        message=res['warnings'])
    finally:
        run_command_on_all_shards(env, config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', '200')


def testVsimTimeoutAppliesToEveryVsimArm():
    """TIMEOUT_AFTER_N_VSIM stops each VSIM arm after N results."""
    env = Env(enableDebugCommand=True)  # for _FT.DEBUG FT.HYBRID
    enable_unstable_features(env)
    q = setup_index(env)
    res = to_dict(env.cmd('_FT.DEBUG', 'FT.HYBRID', 'idx', 2, 'VSIM', '@emb', '$v',
                          'VSIM', '@sp', '$q', 'LIMIT', 0, 100,
                          'PARAMS', 4, 'v', struct.pack('<2f', 5.0, 5.0), 'q', q,
                          'TIMEOUT_AFTER_N_VSIM', 1, 'DEBUG_PARAMS_COUNT', 2))
    # One result per arm per shard at most, against ten per arm without the timeout.
    env.assertLessEqual(res['total_results'], 2 * env.shardsCount, message=res)
    # Each VSIM arm reports its own timeout, tagged with its label.
    env.assertEqual(sorted(w for w in res['warnings'] if w.startswith('Timeout limit was reached')),
                    ['Timeout limit was reached (VSIM @emb)', 'Timeout limit was reached (VSIM @sp)'],
                    message=res['warnings'])


def testArmCountErrors(env):
    """The declared count must match the arms, and at most one VSIM arm per kind of field."""
    enable_unstable_features(env)
    q = setup_index(env)
    params = ['PARAMS', 4, 'v', struct.pack('<2f', 5.0, 5.0), 'q', q]
    env.expect('FT.HYBRID', 'idx', 2, 'VSIM', '@emb', '$v', 'VSIM', '@emb', '$v', *params) \
        .error().contains('at most one VSIM subquery on a VECTOR field and one on a SPARSEVECTOR')
    env.expect('FT.HYBRID', 'idx', 2, 'SEARCH', 'red', 'VSIM', '@emb', '$v',
               'VSIM', '@sp', '$q', *params).error().contains('declares 2 subqueries but has more')
    env.expect('FT.HYBRID', 'idx', 3, 'SEARCH', 'red', 'VSIM', '@emb', '$v', *params) \
        .error().contains('VSIM argument is required')
    env.expect('FT.HYBRID', 'idx', 4, 'SEARCH', 'red', 'VSIM', '@emb', '$v', *params) \
        .error().contains('at most 3 subqueries')
    env.expect('FT.HYBRID', 'idx', 2, 'LIMIT', 0, 10, *params) \
        .error().contains('SEARCH or VSIM keyword is required')
    env.expect('FT.HYBRID', 'idx', 3, 'SEARCH', 'red', 'VSIM', '@emb', '$v', 'VSIM', '@sp', '$q',
               'COMBINE', 'LINEAR', 4, 'ALPHA', 0.5, 'BETA', 0.5, *params) \
        .error().contains('LINEAR with 3 subqueries requires WEIGHTS')


def testNArmsRequireUnstableFeatures(env):
    """With the flag off, FT.HYBRID keeps the errors of the two-arm syntax."""
    run_command_on_all_shards(env, 'CONFIG', 'SET', 'search-enable-unstable-features', 'no')
    try:
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT', 'emb', 'VECTOR', 'FLAT', 6,
                   'TYPE', 'FLOAT32', 'DIM', 2, 'DISTANCE_METRIC', 'L2').ok()
        params = ['PARAMS', 2, 'v', struct.pack('<2f', 5.0, 5.0)]
        env.expect('FT.HYBRID', 'idx', 1, 'VSIM', '@emb', '$v', *params) \
            .error().contains('FT.HYBRID currently supports only two subqueries')
        env.expect('FT.HYBRID', 'idx', 2, 'VSIM', '@emb', '$v', 'VSIM', '@emb', '$v', *params) \
            .error().contains('SEARCH keyword is required')
        # A repeated VSIM is an unknown tail argument, as it always was.
        for count in ([], [2]):
            try:
                env.cmd('FT.HYBRID', 'idx', *count, 'SEARCH', 'red', 'VSIM', '@emb', '$v',
                        'VSIM', '@emb', '$v', *params)
                env.assertTrue(False, message='expected an error')
            except Exception as e:
                env.assertNotContains('declares', str(e))
                env.assertContains('VSIM', str(e))
    finally:
        run_command_on_all_shards(env, 'CONFIG', 'SET', 'search-enable-unstable-features', 'yes')
