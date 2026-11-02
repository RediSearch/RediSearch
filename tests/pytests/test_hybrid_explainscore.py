# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""End-to-end shape tests for FT.HYBRID EXPLAINSCORE (MOD-10044).

The explain tree follows a fixed layout:

    final score: <S>
    └── Hybrid score (<METHOD>: …fusion params…)
        ├── <text branch>
        │   └── Text scorer: <NAME>
        │       └── …existing scorer subtree…
        └── vector branch (<MODE>[: …])
            └── …per-mode rows…

These tests assert the salient tokens in each variant rather than the full
tree, so the existing TEXT-scorer subtree can evolve without breaking us.
"""

import json
import re

from RLTest import Env
# `includes` configures RLTest defaults as a side effect (e.g. decode_responses);
# the import is required even though we don't reference its names directly.
import includes  # noqa: F401
from common import get_results_from_hybrid_response, np


def setup_index(env, vec_algo='FLAT'):
    """Index with one text field and one vector field, plus four deterministic docs."""
    conn = env.getClusterConnectionIfNeeded()
    if vec_algo == 'HNSW':
        env.expect('FT.CREATE', 'idx', 'SCHEMA',
                   'description', 'TEXT',
                   'embedding', 'VECTOR', 'HNSW', 8, 'TYPE', 'FLOAT32', 'DIM', 2,
                   'DISTANCE_METRIC', 'L2', 'EF_RUNTIME', '10').ok()
    else:
        env.expect('FT.CREATE', 'idx', 'SCHEMA',
                   'description', 'TEXT',
                   'embedding', 'VECTOR', 'FLAT', 6, 'TYPE', 'FLOAT32', 'DIM', 2,
                   'DISTANCE_METRIC', 'L2').ok()
    docs = {
        'doc:1': ('red shoes',                              [0.0, 0.0]),
        'doc:2': ('red running shoes',                      [1.0, 0.0]),
        'doc:3': ('running gear and many different shoes',  [0.0, 1.0]),
        'doc:4': ('blue shoes',                             [1.0, 1.0]),
    }
    for key, (text, vec) in docs.items():
        conn.execute_command('HSET', key, 'description', text,
                             'embedding', np.array(vec).astype(np.float32).tobytes())


def _walk_explain(node, found):
    """Flatten every string token in an explain tree (regardless of nesting)."""
    if isinstance(node, (bytes, bytearray)):
        found.append(node.decode('utf-8', errors='replace'))
    elif isinstance(node, str):
        found.append(node)
    elif isinstance(node, (list, tuple)):
        for child in node:
            _walk_explain(child, found)


def _flatten_tokens(score_field):
    """Return all string tokens from the [score_value, explain_tree] pair."""
    tokens = []
    _walk_explain(score_field, tokens)
    return tokens


def _score_and_tokens(env, fields, key):
    """Verify that `fields['score']` is `[<value>, <explain_tree>]` and return both."""
    env.assertTrue('score' in fields, message=f'no score in {key}: {fields}')
    score_field = fields['score']
    env.assertTrue(isinstance(score_field, (list, tuple)),
                   message=f'score should be array, got {type(score_field)}: {score_field}')
    env.assertEqual(len(score_field), 2,
                    message=f'score should be [value, explain], got {score_field}')
    score_value, explain = score_field
    float(score_value)  # parses cleanly
    return score_value, explain, _flatten_tokens(score_field)


def test_hybrid_explainscore_rrf_knn():
    """RRF + KNN: per-branch rank lines, KNN envelope, BM25STD default scorer.
    The outer "final score:" line carries the RRF formula (1/(K+rank))."""
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    setup_index(env)
    blob = np.array([0.5, 0.5]).astype(np.float32).tobytes()
    response = env.cmd('FT.HYBRID', 'idx',
                       'SEARCH', 'shoes',
                       'VSIM', '@embedding', '$BLOB',
                       'EXPLAINSCORE',
                       'PARAMS', '2', 'BLOB', blob)
    results, total = get_results_from_hybrid_response(response)
    env.assertGreater(total, 0)

    for key, fields in results.items():
        _, _, tokens = _score_and_tokens(env, fields, key)
        joined = ' | '.join(tokens)

        # Outer line is a formula, not just the value.
        env.assertTrue(any(t.startswith('final score: 1 / (constant ')
                           and ' + 1 / (constant ' in t and ' = ' in t for t in tokens),
                       message=f'missing RRF formula in outer line, got {joined!r}')
        env.assertTrue(any('Hybrid score (RRF: window=' in t and 'constant=' in t for t in tokens),
                       message=f'missing RRF envelope in {joined!r}')
        env.assertTrue(any(t.startswith('text rank = ') for t in tokens),
                       message=f'missing "text rank = …" in {joined!r}')
        env.assertTrue(any(t == 'vector branch (KNN)' for t in tokens),
                       message=f'missing KNN envelope in {joined!r}')
        env.assertTrue(any(t.startswith('vector rank = ') for t in tokens),
                       message=f'missing "vector rank = …" in {joined!r}')
        env.assertTrue(any(t.startswith('Text scorer: ') for t in tokens),
                       message=f'missing scorer name in {joined!r}')


def test_hybrid_explainscore_rrf_range():
    """RRF + RANGE: vector branch envelope identifies RANGE with radius/epsilon.
    Also assert the outer RRF formula and per-branch rank lines (mirrors what
    we check for RRF+KNN so each variant locks down its own scoring shape)."""
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    setup_index(env, vec_algo='HNSW')
    blob = np.array([0.5, 0.5]).astype(np.float32).tobytes()
    response = env.cmd('FT.HYBRID', 'idx',
                       'SEARCH', 'shoes',
                       'VSIM', '@embedding', '$BLOB',
                       'RANGE', '4', 'RADIUS', '0.7', 'EPSILON', '0.5',
                       'EXPLAINSCORE',
                       'PARAMS', '2', 'BLOB', blob)
    results, total = get_results_from_hybrid_response(response)
    env.assertGreater(total, 0)

    for key, fields in results.items():
        _, _, tokens = _score_and_tokens(env, fields, key)
        joined = ' | '.join(tokens)

        # Outer line is the RRF formula: "1 / (constant K + rank N) + …".
        env.assertTrue(any(t.startswith('final score: 1 / (constant ')
                           and ' + 1 / (constant ' in t and ' = ' in t for t in tokens),
                       message=f'missing RRF formula in outer line, got {joined!r}')
        env.assertTrue(any('Hybrid score (RRF: window=' in t and 'constant=' in t for t in tokens),
                       message=f'missing RRF envelope in {joined!r}')
        env.assertTrue(any(t.startswith('text rank = ') for t in tokens),
                       message=f'missing "text rank = …" in {joined!r}')
        env.assertTrue(any(t.startswith('vector rank = ') for t in tokens),
                       message=f'missing "vector rank = …" in {joined!r}')
        # Envelope must call out RANGE and surface radius + epsilon.
        env.assertTrue(any('vector branch (RANGE:' in t and 'radius=0.7000' in t
                           and 'epsilon=0.5' in t for t in tokens),
                       message=f'missing RANGE envelope in {joined!r}')
        env.assertTrue(any('matched within radius = ' in t for t in tokens),
                       message=f'missing "matched within radius" in {joined!r}')


def test_hybrid_explainscore_linear_knn():
    """LINEAR + KNN: envelope shows alpha/beta/window, contribution formula visible.
    The outer "final score:" line shows the weighted-sum formula."""
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    setup_index(env)
    blob = np.array([0.5, 0.5]).astype(np.float32).tobytes()
    response = env.cmd('FT.HYBRID', 'idx',
                       'SEARCH', 'shoes',
                       'VSIM', '@embedding', '$BLOB',
                       'COMBINE', 'LINEAR', '4', 'ALPHA', '0.7', 'BETA', '0.3',
                       'EXPLAINSCORE',
                       'PARAMS', '2', 'BLOB', blob)
    results, total = get_results_from_hybrid_response(response)
    env.assertGreater(total, 0)

    for key, fields in results.items():
        _, _, tokens = _score_and_tokens(env, fields, key)
        joined = ' | '.join(tokens)

        # Outer line: "final score: 0.7000 * X + 0.3000 * Y = S"
        env.assertTrue(any(t.startswith('final score: 0.7000 * ')
                           and ' + 0.3000 * ' in t and ' = ' in t for t in tokens),
                       message=f'missing LINEAR formula in outer line, got {joined!r}')
        env.assertTrue(any('Hybrid score (LINEAR: alpha=0.7000' in t
                           and 'beta=0.3000' in t and 'window=' in t for t in tokens),
                       message=f'missing LINEAR envelope in {joined!r}')
        env.assertTrue(any('text contribution = 0.7000 *' in t for t in tokens),
                       message=f'missing text-contribution formula in {joined!r}')
        env.assertTrue(any('vector contribution = 0.3000 *' in t for t in tokens),
                       message=f'missing vector-contribution formula in {joined!r}')
        env.assertTrue(any(t.startswith('normalized text score = ') for t in tokens),
                       message=f'missing normalized text score in {joined!r}')
        env.assertTrue(any(t.startswith('normalized vector score = ') for t in tokens),
                       message=f'missing normalized vector score in {joined!r}')


def test_hybrid_explainscore_linear_range():
    """LINEAR + RANGE: combines the LINEAR envelope with a RANGE vector branch.
    Also assert the outer LINEAR formula and the per-branch contribution lines,
    so the scoring shape is locked down for this variant too."""
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    setup_index(env, vec_algo='HNSW')
    blob = np.array([0.5, 0.5]).astype(np.float32).tobytes()
    response = env.cmd('FT.HYBRID', 'idx',
                       'SEARCH', 'shoes',
                       'VSIM', '@embedding', '$BLOB',
                       'RANGE', '4', 'RADIUS', '0.7', 'EPSILON', '0.5',
                       'COMBINE', 'LINEAR', '4', 'ALPHA', '0.6', 'BETA', '0.4',
                       'EXPLAINSCORE',
                       'PARAMS', '2', 'BLOB', blob)
    results, total = get_results_from_hybrid_response(response)
    env.assertGreater(total, 0)

    saw_matched = False
    for key, fields in results.items():
        _, _, tokens = _score_and_tokens(env, fields, key)
        joined = ' | '.join(tokens)

        # Outer line: "final score: 0.6000 * X + 0.4000 * Y = S" (or, for
        # docs that miss the vector branch, "0.6000 * X + 0 [vector: no match] = S").
        env.assertTrue(any(t.startswith('final score: 0.6000 * ')
                           and ' = ' in t
                           and (' + 0.4000 * ' in t or '0 [vector: no match]' in t)
                           for t in tokens),
                       message=f'missing LINEAR formula in outer line, got {joined!r}')
        env.assertTrue(any('Hybrid score (LINEAR: alpha=0.6000' in t
                           and 'beta=0.4000' in t for t in tokens),
                       message=f'missing LINEAR envelope in {joined!r}')
        env.assertTrue(any('vector branch (RANGE:' in t and 'radius=0.7000' in t
                           and 'epsilon=0.5' in t for t in tokens),
                       message=f'missing RANGE envelope in {joined!r}')
        env.assertTrue(any('matched within radius = ' in t for t in tokens),
                       message=f'missing "matched within radius" in {joined!r}')

        # text contribution is always present (every result has a text match).
        env.assertTrue(any('text contribution = 0.6000 *' in t for t in tokens),
                       message=f'missing text-contribution formula in {joined!r}')
        env.assertTrue(any(t.startswith('normalized text score = ') for t in tokens),
                       message=f'missing normalized text score in {joined!r}')

        # vector contribution only when the doc is within the RANGE radius.
        if any('matched within radius = true' in t for t in tokens):
            saw_matched = True
            env.assertTrue(any('vector contribution = 0.4000 *' in t for t in tokens),
                           message=f'missing vector-contribution formula in {joined!r}')
            env.assertTrue(any(t.startswith('normalized vector score = ') for t in tokens),
                           message=f'missing normalized vector score in {joined!r}')
    env.assertTrue(saw_matched,
                   message='expected at least one doc inside the RANGE radius')


def test_hybrid_explainscore_scorer_name_propagates():
    """An explicit SCORER on the SEARCH sub-query shows up in the "Text scorer:" label."""
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    setup_index(env)
    blob = np.array([0.5, 0.5]).astype(np.float32).tobytes()
    response = env.cmd('FT.HYBRID', 'idx',
                       'SEARCH', 'shoes', 'SCORER', 'TFIDF',
                       'VSIM', '@embedding', '$BLOB',
                       'EXPLAINSCORE',
                       'PARAMS', '2', 'BLOB', blob)
    results, total = get_results_from_hybrid_response(response)
    env.assertGreater(total, 0)

    for key, fields in results.items():
        _, _, tokens = _score_and_tokens(env, fields, key)
        joined = ' | '.join(tokens)
        env.assertTrue(any(t == 'Text scorer: TFIDF' for t in tokens),
                       message=f'expected "Text scorer: TFIDF" label, got {joined!r}')


def test_hybrid_explainscore_text_only_match_branch_placeholder():
    """When a doc matches text but not the vector RANGE, the vector branch
    shows "matched within radius = false", the per-branch sub-tree carries
    a "<no match>" placeholder, and the outer formula records the dropped
    term as "0 [vector: no match]"."""
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    setup_index(env, vec_algo='HNSW')
    # A very small radius so no vector match exists for any doc, but text matches.
    blob = np.array([10.0, 10.0]).astype(np.float32).tobytes()
    response = env.cmd('FT.HYBRID', 'idx',
                       'SEARCH', 'shoes',
                       'VSIM', '@embedding', '$BLOB',
                       'RANGE', '2', 'RADIUS', '0.001',
                       'EXPLAINSCORE',
                       'PARAMS', '2', 'BLOB', blob)
    results, total = get_results_from_hybrid_response(response)
    env.assertGreater(total, 0)

    saw_no_match = False
    for key, fields in results.items():
        _, _, tokens = _score_and_tokens(env, fields, key)
        if any('matched within radius = false' in t for t in tokens):
            saw_no_match = True
            env.assertTrue(any('<no match>' in t for t in tokens),
                           message=f'expected "<no match>" placeholder in {tokens!r}')
            env.assertTrue(any(t.startswith('final score: ')
                               and '0 [vector: no match]' in t for t in tokens),
                           message=f'expected dropped term in outer formula, got {tokens!r}')
            break
    env.assertTrue(saw_no_match,
                   message='expected at least one doc with vector branch missing')


# ----------------------------------------------------------------------------
# Structural snapshot helpers
#
# These let one test pin down the entire reply shape for a chosen document
# while staying robust against scorer-internal value drift. The template is a
# tree of literals + regex/predicate matchers; mismatches are reported with
# their JSON path and the offending actual fragment.
# ----------------------------------------------------------------------------


class _Re:
    """Regex matcher for a string (or bytes) leaf in a shape template."""
    def __init__(self, pattern):
        self.pattern = re.compile(pattern)

    def __call__(self, x):
        if isinstance(x, (bytes, bytearray)):
            x = x.decode('utf-8', errors='replace')
        return isinstance(x, str) and self.pattern.fullmatch(x) is not None

    def __repr__(self):
        return f'_Re({self.pattern.pattern!r})'


class _Near:
    """Predicate matcher for a numeric leaf (RESP2 sends doubles as strings).
    Wrapped in a class so mismatch reports get a useful repr."""
    def __init__(self, expected, tol=1e-9):
        self.expected = expected
        self.tol = tol

    def __call__(self, x):
        s = x.decode('utf-8', errors='replace') if isinstance(x, (bytes, bytearray)) else x
        try:
            return abs(float(s) - self.expected) <= self.tol
        except (TypeError, ValueError):
            return False

    def __repr__(self):
        return f'_Near({self.expected!r}, tol={self.tol!r})'


def _near(expected, tol=1e-9):
    return _Near(expected, tol)


def _norm(x):
    return x.decode('utf-8', errors='replace') if isinstance(x, (bytes, bytearray)) else x


def _to_jsonable(x):
    if isinstance(x, (bytes, bytearray)):
        return x.decode('utf-8', errors='replace')
    if isinstance(x, (list, tuple)):
        return [_to_jsonable(c) for c in x]
    return x


def _shape_errors(template, actual, path='$'):
    """Return a list of human-readable mismatches between template and actual."""
    # Order matters: strings and lists are also callables in Python's terms,
    # so check them first.
    if isinstance(template, str):
        return [] if _norm(actual) == template else [f'{path}: expected {template!r}, got {_norm(actual)!r}']
    if isinstance(template, list):
        if not isinstance(actual, (list, tuple)):
            return [f'{path}: expected list of len {len(template)}, got {type(actual).__name__}']
        errs = []
        if len(template) != len(actual):
            errs.append(f'{path}: expected list len {len(template)}, got len {len(actual)}')
        for i, (t, a) in enumerate(zip(template, actual)):
            errs.extend(_shape_errors(t, a, path=f'{path}[{i}]'))
        return errs
    if callable(template):
        if not template(actual):
            return [f'{path}: {_norm(actual)!r} did not satisfy {template!r}']
        return []
    return [f'{path}: unsupported template {template!r}']


def test_hybrid_explainscore_full_shape_rrf_knn_doc1():
    """Locked-down structural shape for an RRF+KNN reply.

    For the four-doc deterministic corpus, doc:1 ("red shoes" with vector
    [0,0]) lands at text rank 1 (BM25 ties broken by docID) and vector rank
    1 (all docs equidistant from query vector [0.5,0.5]; ties broken by
    docID). The template below pins down every node in the wrapper; the BM25
    sub-tree leaves use regex so the existing FT.SEARCH-side scorer numerics
    can evolve without breaking us.
    """
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    # Per-doc ranks aren't stable across shards (the hybrid merger assigns
    # ranks from its own consumption order, which depends on cursor reads).
    if env.isCluster():
        env.skip()
    setup_index(env)

    blob = np.array([0.5, 0.5]).astype(np.float32).tobytes()
    response = env.cmd('FT.HYBRID', 'idx',
                       'SEARCH', 'shoes',
                       'VSIM', '@embedding', '$BLOB',
                       'EXPLAINSCORE',
                       'PARAMS', '2', 'BLOB', blob)
    results, _ = get_results_from_hybrid_response(response)
    env.assertTrue('doc:1' in results, message=f'doc:1 missing from {list(results)}')
    fields = results['doc:1']
    env.assertTrue('score' in fields, message=f'no score field on doc:1: {fields}')

    # Expected RRF score: 1/(60+1) + 1/(60+1) = 2/61 = 0.0327868852…
    expected_rrf_score = 2.0 / 61.0
    decimal = r'[0-9]+(?:\.[0-9]+)?'

    expected_score_field = [
        _near(expected_rrf_score),                                  # raw score
        [                                                            # explain tree
            _Re(rf'final score: 1 / \(constant 60\.00 \+ rank 1\) \+ '
                rf'1 / \(constant 60\.00 \+ rank 1\) = {decimal}'),
            [
                [
                    'Hybrid score (RRF: window=20, constant=60.00)',
                    [
                        [   # text branch
                            'text rank = 1',
                            [
                                [
                                    'Text scorer: BM25STD',
                                    [
                                        [
                                            _Re(rf'Final BM25 : words BM25 {decimal} \* document score {decimal}'),
                                            [
                                                [
                                                    _Re(rf'\(Weight {decimal} \* children BM25 {decimal}\)'),
                                                    [
                                                        _Re(r'shoes: \(.+\)'),
                                                        _Re(r'\+shoe: \(.+\)'),
                                                    ],
                                                ],
                                            ],
                                        ],
                                    ],
                                ],
                            ],
                        ],
                        [   # vector branch
                            'vector branch (KNN)',
                            ['vector rank = 1'],
                        ],
                    ],
                ],
            ],
        ],
    ]

    errors = _shape_errors(expected_score_field, fields['score'])
    if errors:
        actual_pretty = json.dumps(_to_jsonable(fields['score']), indent=2)
        env.assertTrue(
            False,
            message='shape mismatch:\n  ' + '\n  '.join(errors) +
                    f'\n\nactual score field:\n{actual_pretty}')


def _bm25_subtree(decimal):
    """The standalone-side BM25STD subtree, used as a leaf in shape templates."""
    return [
        [
            _Re(rf'Final BM25 : words BM25 {decimal} \* document score {decimal}'),
            [
                [
                    _Re(rf'\(Weight {decimal} \* children BM25 {decimal}\)'),
                    [
                        _Re(r'shoes: \(.+\)'),
                        _Re(r'\+shoe: \(.+\)'),
                    ],
                ],
            ],
        ],
    ]


def _assert_shape(env, score_field, expected):
    errors = _shape_errors(expected, score_field)
    if errors:
        actual_pretty = json.dumps(_to_jsonable(score_field), indent=2)
        env.assertTrue(
            False,
            message='shape mismatch:\n  ' + '\n  '.join(errors) +
                    f'\n\nactual score field:\n{actual_pretty}')


def test_hybrid_explainscore_full_shape_rrf_range_doc1():
    """Locked-down structural shape for an RRF+RANGE reply.

    All four corpus docs sit inside the radius (squared L2 distance 0.5 from
    query [0.5,0.5] vs radius 0.7), so each doc has both a text and a vector
    rank. Ranks under HNSW range aren't pinned (the traversal order isn't
    distance-tied on ties), so the per-rank lines use regex matchers; the
    nesting itself is fully pinned.
    """
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    if env.isCluster():
        env.skip()
    setup_index(env, vec_algo='HNSW')

    blob = np.array([0.5, 0.5]).astype(np.float32).tobytes()
    response = env.cmd('FT.HYBRID', 'idx',
                       'SEARCH', 'shoes',
                       'VSIM', '@embedding', '$BLOB',
                       'RANGE', '4', 'RADIUS', '0.7', 'EPSILON', '0.5',
                       'EXPLAINSCORE',
                       'PARAMS', '2', 'BLOB', blob)
    results, _ = get_results_from_hybrid_response(response)
    env.assertTrue('doc:1' in results, message=f'doc:1 missing from {list(results)}')
    fields = results['doc:1']

    decimal = r'[0-9]+(?:\.[0-9]+)?'

    expected_score_field = [
        _Re(rf'{decimal}'),                                              # raw score
        [
            _Re(rf'final score: 1 / \(constant 60\.00 \+ rank {decimal}\) \+ '
                rf'1 / \(constant 60\.00 \+ rank {decimal}\) = {decimal}'),
            [
                [
                    'Hybrid score (RRF: window=20, constant=60.00)',
                    [
                        [   # text branch
                            _Re(rf'text rank = {decimal}'),
                            [
                                [
                                    'Text scorer: BM25STD',
                                    _bm25_subtree(decimal),
                                ],
                            ],
                        ],
                        [   # vector branch (RANGE)
                            'vector branch (RANGE: radius=0.7000, epsilon=0.5)',
                            [
                                'matched within radius = true',
                                _Re(rf'vector rank = {decimal}'),
                            ],
                        ],
                    ],
                ],
            ],
        ],
    ]
    _assert_shape(env, fields['score'], expected_score_field)


def test_hybrid_explainscore_full_shape_linear_range_doc1():
    """Locked-down structural shape for a LINEAR+RANGE reply.

    Same matched-by-radius corpus as the RRF+RANGE test. LINEAR adds the
    "normalized text score" sibling under the scorer node and the explicit
    "vector contribution" + "normalized vector score" rows under the vector
    branch — those extra children are what this test pins down.
    """
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    if env.isCluster():
        env.skip()
    setup_index(env, vec_algo='HNSW')

    blob = np.array([0.5, 0.5]).astype(np.float32).tobytes()
    response = env.cmd('FT.HYBRID', 'idx',
                       'SEARCH', 'shoes',
                       'VSIM', '@embedding', '$BLOB',
                       'RANGE', '4', 'RADIUS', '0.7', 'EPSILON', '0.5',
                       'COMBINE', 'LINEAR', '4', 'ALPHA', '0.7', 'BETA', '0.3',
                       'EXPLAINSCORE',
                       'PARAMS', '2', 'BLOB', blob)
    results, _ = get_results_from_hybrid_response(response)
    env.assertTrue('doc:1' in results, message=f'doc:1 missing from {list(results)}')
    fields = results['doc:1']

    decimal = r'[0-9]+(?:\.[0-9]+)?'

    expected_score_field = [
        _Re(rf'{decimal}'),                                              # raw score
        [
            _Re(rf'final score: 0\.7000 \* {decimal} \+ 0\.3000 \* {decimal} = {decimal}'),
            [
                [
                    'Hybrid score (LINEAR: alpha=0.7000, beta=0.3000, window=20)',
                    [
                        [   # text branch
                            _Re(rf'text contribution = 0\.7000 \* {decimal} = {decimal}'),
                            [
                                [
                                    'Text scorer: BM25STD',
                                    [
                                        _Re(rf'normalized text score = {decimal}'),
                                    ] + _bm25_subtree(decimal),
                                ],
                            ],
                        ],
                        [   # vector branch (RANGE)
                            'vector branch (RANGE: radius=0.7000, epsilon=0.5)',
                            [
                                'matched within radius = true',
                                _Re(rf'vector contribution = 0\.3000 \* {decimal} = {decimal}'),
                                _Re(rf'normalized vector score = {decimal}'),
                            ],
                        ],
                    ],
                ],
            ],
        ],
    ]
    _assert_shape(env, fields['score'], expected_score_field)


def test_hybrid_explainscore_full_shape_text_only_no_vector():
    """Single-branch match: text matches but the vector RANGE has no hits.

    Pins down the placeholder shape — the outer formula reports the dropped
    term as "0 [vector: no match]", and the vector branch carries
    "matched within radius = false" + "vector rank = <no match>" as siblings.
    """
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    if env.isCluster():
        env.skip()
    setup_index(env, vec_algo='HNSW')

    # Far-away query vector + tiny radius → no doc clears the RANGE filter.
    blob = np.array([10.0, 10.0]).astype(np.float32).tobytes()
    response = env.cmd('FT.HYBRID', 'idx',
                       'SEARCH', 'shoes',
                       'VSIM', '@embedding', '$BLOB',
                       'RANGE', '2', 'RADIUS', '0.001',
                       'EXPLAINSCORE',
                       'PARAMS', '2', 'BLOB', blob)
    results, _ = get_results_from_hybrid_response(response)
    env.assertTrue('doc:1' in results, message=f'doc:1 missing from {list(results)}')
    fields = results['doc:1']

    decimal = r'[0-9]+(?:\.[0-9]+)?'

    expected_score_field = [
        _Re(rf'{decimal}'),                                              # raw score
        [
            _Re(rf'final score: 1 / \(constant 60\.00 \+ rank {decimal}\) \+ '
                rf'0 \[vector: no match\] = {decimal}'),
            [
                [
                    'Hybrid score (RRF: window=20, constant=60.00)',
                    [
                        [   # text branch (present)
                            _Re(rf'text rank = {decimal}'),
                            [
                                [
                                    'Text scorer: BM25STD',
                                    _bm25_subtree(decimal),
                                ],
                            ],
                        ],
                        [   # vector branch (no match)
                            _Re(r'vector branch \(RANGE: radius=0\.0010\)'),
                            [
                                'matched within radius = false',
                                'vector rank = <no match>',
                            ],
                        ],
                    ],
                ],
            ],
        ],
    ]
    _assert_shape(env, fields['score'], expected_score_field)


def test_hybrid_explainscore_rejected_with_groupby():
    """EXPLAINSCORE is incompatible with GROUPBY: the grouper clears the
    per-document SearchResult (including the score_explain wrapper) and emits
    aggregate rows whose score_explain is empty, while the hybrid reply path
    still opens a [score, explain] array. The combination must be rejected at
    parse time."""
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    setup_index(env)
    blob = np.array([0.5, 0.5]).astype(np.float32).tobytes()
    env.expect('FT.HYBRID', 'idx',
               'SEARCH', 'shoes',
               'VSIM', '@embedding', '$BLOB',
               'EXPLAINSCORE',
               'GROUPBY', '1', '@description',
               'PARAMS', '2', 'BLOB', blob).error().contains(
                   'EXPLAINSCORE is not supported with GROUPBY')
