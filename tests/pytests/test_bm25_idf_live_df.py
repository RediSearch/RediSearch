"""
Regression / flow tests for BM25 IDF using live document frequency on RAM indexes (MOD-13918).

Repeated in-place updates of the same hash key allocate new internal document ids while stale
postings linger until fork-GC. IDF must not use only the encoded inverted-list df, or BM25
scores drift until GC runs.
"""

from includes import *
from common import *


def test_bm25_score_stable_after_repeated_hash_replaces_pre_gc(env):
    """REGRESSION: BM25 search score is stable across many REPLACE updates before fork-GC.

    Without live df for IDF, encoded per-term `unique_docs` grows while `numDocuments` stays 1,
    skewing BM25. This flow holds fork-GC back, issues identical-text replaces on one document,
    and expects the BM25 score for a rare query term to match the initial measurement.
    """
    skipTest(cluster=True)
    skipTest(gc_no_fork=True)

    conn = getConnectionByEnv(env)

    # Keep fork GC from compacting postings during this test (staleness must persist).
    env.expect(config_cmd(), 'SET', 'FORK_GC_RUN_INTERVAL', '1000000000').ok()
    env.expect(config_cmd(), 'SET', 'FORK_GC_CLEAN_THRESHOLD', '100000000').ok()

    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 't', 'TEXT', 'rev', 'NUMERIC').ok()
    waitForIndex(env, 'idx')

    doc_id = 'doc_bm25_live_df'
    # Rare token so IDF is sensitive to (live) document frequency.
    text = 'alpha zzzunique_bm25_live_df_regression_token'
    token = 'zzzunique_bm25_live_df_regression_token'

    # `rev` bumps each replace so the indexer definitely runs (identical `t` alone may no-op).
    conn.execute_command('HSET', doc_id, 't', text, 'rev', 0)
    waitForIndex(env, 'idx')

    res0 = env.cmd(
        'FT.SEARCH',
        'idx',
        token,
        'SCORER',
        'BM25',
        'WITHSCORES',
        'NOCONTENT',
        'LIMIT',
        '0',
        '1',
    )
    env.assertEqual(res0[0], 1, message='single document should match')
    score0 = float(res0[2])

    n_replaces = 55
    for i in range(1, n_replaces + 1):
        conn.execute_command('HSET', doc_id, 't', text, 'rev', i)

    waitForIndex(env, 'idx')

    res1 = env.cmd(
        'FT.SEARCH',
        'idx',
        token,
        'SCORER',
        'BM25',
        'WITHSCORES',
        'NOCONTENT',
        'LIMIT',
        '0',
        '1',
    )
    env.assertEqual(res1[0], 1, message='document should still match after replaces')
    score1 = float(res1[2])

    env.assertAlmostEqual(
        score0,
        score1,
        delta=1e-4,
        message='BM25 score drifted pre-GC (likely IDF used stale inverted-index df)',
    )


def test_tfidf_score_stable_after_repeated_hash_replaces_pre_gc(env):
    """Same flow as BM25 but for the custom TFIDF scorer (also uses per-term IDF at iterator creation)."""
    skipTest(cluster=True)
    skipTest(gc_no_fork=True)

    conn = getConnectionByEnv(env)
    env.expect(config_cmd(), 'SET', 'FORK_GC_RUN_INTERVAL', '1000000000').ok()
    env.expect(config_cmd(), 'SET', 'FORK_GC_CLEAN_THRESHOLD', '100000000').ok()

    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 't', 'TEXT', 'rev', 'NUMERIC').ok()
    waitForIndex(env, 'idx')

    doc_id = 'doc_tfidf_live_df'
    text = 'bravo zzzunique_tfidf_live_df_regression_token'
    token = 'zzzunique_tfidf_live_df_regression_token'

    conn.execute_command('HSET', doc_id, 't', text, 'rev', 0)
    waitForIndex(env, 'idx')

    res0 = env.cmd(
        'FT.SEARCH',
        'idx',
        token,
        'SCORER',
        'TFIDF',
        'WITHSCORES',
        'NOCONTENT',
        'LIMIT',
        '0',
        '1',
    )
    env.assertEqual(res0[0], 1)
    score0 = float(res0[2])

    for i in range(1, 56):
        conn.execute_command('HSET', doc_id, 't', text, 'rev', i)

    waitForIndex(env, 'idx')

    res1 = env.cmd(
        'FT.SEARCH',
        'idx',
        token,
        'SCORER',
        'TFIDF',
        'WITHSCORES',
        'NOCONTENT',
        'LIMIT',
        '0',
        '1',
    )
    env.assertEqual(res1[0], 1)
    score1 = float(res1[2])

    env.assertAlmostEqual(score0, score1, delta=1e-4, message='TFIDF score drifted pre-GC')
