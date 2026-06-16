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

from RLTest import Env
from includes import *
from common import *


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
    """RRF + RANGE: vector branch envelope identifies RANGE with radius/epsilon."""
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

        env.assertTrue(any('Hybrid score (RRF:' in t for t in tokens),
                       message=f'missing RRF envelope in {joined!r}')
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
    """LINEAR + RANGE: combines the LINEAR envelope with a RANGE vector branch."""
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

    for key, fields in results.items():
        _, _, tokens = _score_and_tokens(env, fields, key)
        joined = ' | '.join(tokens)

        env.assertTrue(any('Hybrid score (LINEAR: alpha=0.6000' in t
                           and 'beta=0.4000' in t for t in tokens),
                       message=f'missing LINEAR envelope in {joined!r}')
        env.assertTrue(any('vector branch (RANGE:' in t and 'radius=0.7000' in t
                           and 'epsilon=0.5' in t for t in tokens),
                       message=f'missing RANGE envelope in {joined!r}')
        env.assertTrue(any('matched within radius = ' in t for t in tokens),
                       message=f'missing "matched within radius" in {joined!r}')


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
