# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""Flow tests for `FT.HYBRID ... COMBINE RRF|LINEAR ... WEIGHTS n w1 ... wn`."""

import random
import re
import struct

from common import *

RRF_CONSTANT = 60
GATE_ERROR = '`WEIGHTS` is unavailable when `ENABLE_UNSTABLE_FEATURES` is off'
WORDS = ['red', 'blue', 'green', 'shoes', 'hat', 'coat']


def setup_index(env, n=60, seed=99):
    """Documents with random titles over a small vocabulary and random 2-d vectors."""
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT',
               'emb', 'VECTOR', 'FLAT', 6, 'TYPE', 'FLOAT32', 'DIM', 2, 'DISTANCE_METRIC', 'L2').ok()
    rnd = random.Random(seed)
    for k in range(n):
        title = ' '.join(rnd.choice(WORDS) for _ in range(rnd.randint(1, 6)))
        conn.execute_command('HSET', f'doc:{k}', 'title', title,
                             'emb', struct.pack('<2f', rnd.uniform(0, 10), rnd.uniform(0, 10)))


def hybrid(env, *combine):
    res = to_dict(env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'red', 'VSIM', '@emb', '$v',
                          *combine, 'LIMIT', 0, 100,
                          'PARAMS', 2, 'v', struct.pack('<2f', 5.0, 5.0)))
    return [to_dict(row) for row in res['results']]


def flatten(x):
    if isinstance(x, list):
        for item in x:
            yield from flatten(item)
    else:
        yield x


def arm_ranks(rows):
    """Each document's (text rank, vector rank), read from its EXPLAINSCORE lines; None when the
    arm did not return it."""
    ranks = {}
    for row in rows:
        lines = ' | '.join(str(s) for s in flatten(row['score']))
        text = re.search(r'text rank = (\d+)', lines)
        vector = re.search(r'vector rank = (\d+)', lines)
        ranks[row['__key']] = (int(text.group(1)) if text else None,
                               int(vector.group(1)) if vector else None)
    return ranks


def testWeightedRrfMatchesReference(env):
    """Weighted RRF scores a document Σ wᵢ / (CONSTANT + rankᵢ) over the arms that returned it.
    The ranks come from an unweighted run, since weights do not change either arm's ranking."""
    enable_unstable_features(env)
    setup_index(env)
    ranks = arm_ranks(hybrid(env, 'EXPLAINSCORE'))
    env.assertGreater(sum(1 for t, v in ranks.values() if t and v), 0, message=ranks)

    for weights in ([2.0, 0.5], [0.0, 1.0], [1.0, 1.0]):
        rows = hybrid(env, 'COMBINE', 'RRF', 4, 'WEIGHTS', 2, *weights)
        env.assertEqual(len(rows), len(ranks))
        got = {row['__key']: float(row['__score']) for row in rows}
        for key, arm in ranks.items():
            expected = sum(w / (RRF_CONSTANT + r) for w, r in zip(weights, arm) if r is not None)
            env.assertAlmostEqual(got[key], expected, 1e-9)
        scores = [float(row['__score']) for row in rows]
        env.assertEqual(scores, sorted(scores, reverse=True))

    # Every weight 1 is the unweighted fusion.
    env.assertEqual(hybrid(env, 'COMBINE', 'RRF', 4, 'WEIGHTS', 2, 1, 1), hybrid(env))


def testLinearWeightsSpellAlphaBeta(env):
    """LINEAR `WEIGHTS 2 a b` gives the same results as `ALPHA a BETA b`."""
    enable_unstable_features(env)
    setup_index(env)
    for a, b in [(0.3, 0.7), (1.0, 0.0), (0.25, 2.5)]:
        env.assertEqual(hybrid(env, 'COMBINE', 'LINEAR', 4, 'WEIGHTS', 2, a, b),
                        hybrid(env, 'COMBINE', 'LINEAR', 4, 'ALPHA', a, 'BETA', b))


def testWeightsWithOtherCombineArguments(env):
    """WEIGHTS sits in the counted COMBINE block next to CONSTANT, WINDOW and YIELD_SCORE_AS."""
    enable_unstable_features(env)
    setup_index(env)
    rows = hybrid(env, 'COMBINE', 'RRF', 8, 'CONSTANT', 60, 'WEIGHTS', 2, 1, 1, 'WINDOW', 20)
    env.assertEqual(rows, hybrid(env))
    rows = hybrid(env, 'COMBINE', 'RRF', 6, 'WEIGHTS', 2, 1, 1, 'YIELD_SCORE_AS', 'fused')
    env.assertEqual([r['fused'] for r in rows], [r['__score'] for r in hybrid(env)])


def testWeightsExplainScore(env):
    """EXPLAINSCORE shows each weight in the RRF hybrid node and in its term of the formula."""
    enable_unstable_features(env)
    setup_index(env)
    rows = hybrid(env, 'COMBINE', 'RRF', 4, 'WEIGHTS', 2, 2, 0.5, 'EXPLAINSCORE')
    lines = [str(s) for s in flatten(rows[0]['score'])]
    env.assertContains('Hybrid score (RRF: window=20, constant=60.00, weights=[2.0000, 0.5000])',
                       lines)
    final = [s for s in lines if s.startswith('final score: ')][0]
    env.assertTrue(re.search(r'(2\.0000|0\.5000) \* 1 / \(constant 60\.00 \+ rank \d+\)', final),
                   message=final)


def testWeightsErrors(env):
    """WEIGHTS needs one finite weight per subquery and cannot be mixed with ALPHA or BETA."""
    enable_unstable_features(env)
    setup_index(env)
    blob = struct.pack('<2f', 5.0, 5.0)

    def expect_error(*combine):
        return env.expect('FT.HYBRID', 'idx', 'SEARCH', 'red', 'VSIM', '@emb', '$v', *combine,
                          'PARAMS', 2, 'v', blob).error()

    # A weight count other than two makes the block's count odd, which WEIGHTS allows.
    expect_error('COMBINE', 'RRF', 3, 'WEIGHTS', 1, 1) \
        .contains('WEIGHTS expects one weight per subquery: 1 given, 2 expected')
    expect_error('COMBINE', 'RRF', 5, 'WEIGHTS', 3, 1, 1, 1) \
        .contains('WEIGHTS expects one weight per subquery: 3 given, 2 expected')
    expect_error('COMBINE', 'RRF', 4, 'WEIGHTS', 2, 1, 'x').contains('Invalid WEIGHTS value')
    expect_error('COMBINE', 'LINEAR', 4, 'WEIGHTS', 2, 1, 'inf').contains('Invalid WEIGHTS value')
    expect_error('COMBINE', 'LINEAR', 6, 'ALPHA', 1, 'WEIGHTS', 2, 1, 1) \
        .contains('WEIGHTS cannot be combined with ALPHA or BETA')
    expect_error('COMBINE', 'RRF', 4, 'WEIGHTS', 3, 1, 1).contains('WEIGHTS')
    # An odd count without WEIGHTS is still a key without its value.
    expect_error('COMBINE', 'RRF', 1, 'WINDOW').contains('argument count must be an even number')


def testWeightsRequireUnstableFeatures(env):
    """With the flag off, WEIGHTS is rejected on every node and the rest of COMBINE works."""
    run_command_on_all_shards(env, 'CONFIG', 'SET', 'search-enable-unstable-features', 'no')
    setup_index(env)
    blob = struct.pack('<2f', 5.0, 5.0)
    for method in ('RRF', 'LINEAR'):
        env.expect('FT.HYBRID', 'idx', 'SEARCH', 'red', 'VSIM', '@emb', '$v',
                   'COMBINE', method, 4, 'WEIGHTS', 2, 1, 1, 'PARAMS', 2, 'v', blob) \
            .error().contains(GATE_ERROR)
    env.assertGreater(len(hybrid(env, 'COMBINE', 'LINEAR', 4, 'ALPHA', 1, 'BETA', 1)), 0)
