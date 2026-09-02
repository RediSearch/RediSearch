# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

import json

from common import *


def get_internal_id(env, key, idx='idx'):
    # internal_id is the primary observable for this feature: a document whose newly added
    # fields are all absent keeps its id (skipped), while a full reindex always goes through
    # the REPLACE path, which deletes the old doc-table entry and mints a new, larger id.
    docinfo = to_dict(env.cmd(debug_cmd(), 'DOCINFO', idx, key, 'REVEAL'))
    return docinfo['internal_id']


@skip(cluster=True)
def testAlterSkipUnchangedDocsHash(env):
    """A Hash document with none of the fields added by FT.ALTER keeps its internal id
    (skipped), while a document that has the added field is fully reindexed. Both remain
    correctly searchable on their old and new fields afterwards."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc:1', 'title', 'hello world')
    conn.execute_command('HSET', 'doc:2', 'title', 'goodbye world', 'tags', 'premium')

    id1_before = get_internal_id(env, 'doc:1')
    id2_before = get_internal_id(env, 'doc:2')

    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'tags', 'TAG').ok()
    waitForIndexFinishScan(env, 'idx')

    env.assertEqual(get_internal_id(env, 'doc:1'), id1_before)
    env.assertGreater(get_internal_id(env, 'doc:2'), id2_before)

    # The optimization must not change query results: the pre-existing field stays
    # searchable on both documents, and doc:2's replace-path reindex must have preserved it
    # alongside the newly indexed field.
    env.expect('FT.SEARCH', 'idx', '@title:hello', 'NOCONTENT').equal([1, 'doc:1'])
    env.expect('FT.SEARCH', 'idx', '@title:goodbye', 'NOCONTENT').equal([1, 'doc:2'])
    env.expect('FT.SEARCH', 'idx', '@tags:{premium}', 'NOCONTENT').equal([1, 'doc:2'])


@skip(cluster=True)
def testAlterSkipUnchangedDocsMultipleAddedFields(env):
    """FT.ALTER SCHEMA ADD with several fields in one command: a document is reindexed if it
    has ANY of them, and skipped only when it has NONE -- the range, not a single field, is
    what the probe covers."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc:1', 'title', 'apple')
    conn.execute_command('HSET', 'doc:2', 'title', 'banana', 'a', 'x')
    conn.execute_command('HSET', 'doc:3', 'title', 'cherry', 'b', '7')

    id1_before = get_internal_id(env, 'doc:1')
    id2_before = get_internal_id(env, 'doc:2')
    id3_before = get_internal_id(env, 'doc:3')

    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'a', 'TAG', 'b', 'NUMERIC').ok()
    waitForIndexFinishScan(env, 'idx')

    env.assertEqual(get_internal_id(env, 'doc:1'), id1_before)
    env.assertGreater(get_internal_id(env, 'doc:2'), id2_before)
    env.assertGreater(get_internal_id(env, 'doc:3'), id3_before)

    env.expect('FT.SEARCH', 'idx', '@title:apple', 'NOCONTENT').equal([1, 'doc:1'])
    env.expect('FT.SEARCH', 'idx', '@a:{x}', 'NOCONTENT').equal([1, 'doc:2'])
    env.expect('FT.SEARCH', 'idx', '@b:[7 7]', 'NOCONTENT').equal([1, 'doc:3'])


@skip(cluster=True)
def testAlterSkipUnchangedDocsHashAlias(env):
    """Presence must be resolved through the stored hash field path ('category'), not the
    query-facing alias introduced by 'AS cat' -- a document that happens to have a literal
    'cat' hash key but no 'category' key must still be skipped, matching how the full load
    resolves the field."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc:1', 'title', 'first', 'category', 'electronics')
    conn.execute_command('HSET', 'doc:2', 'title', 'second', 'cat', 'electronics')

    id1_before = get_internal_id(env, 'doc:1')
    id2_before = get_internal_id(env, 'doc:2')

    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'category', 'AS', 'cat', 'TAG').ok()
    waitForIndexFinishScan(env, 'idx')

    env.assertGreater(get_internal_id(env, 'doc:1'), id1_before)
    env.assertEqual(get_internal_id(env, 'doc:2'), id2_before)

    env.expect('FT.SEARCH', 'idx', '@cat:{electronics}', 'NOCONTENT').equal([1, 'doc:1'])
    env.expect('FT.SEARCH', 'idx', '@title:second', 'NOCONTENT').equal([1, 'doc:2'])


@skip(cluster=True)
def testAlterSkipUnchangedDocsJson(env):
    """JSON documents: presence is resolved by JSONPath, covering both a nested single-value
    path and a multi-value path. An empty match array counts as absent -- the probe follows
    the full load's len()-based check, so a present-but-empty array is not mistaken for a
    present field."""
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.title', 'AS', 'title', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('JSON.SET', 'doc:1', '$', json.dumps({'title': 'alpha'}))
    conn.execute_command('JSON.SET', 'doc:2', '$',
                          json.dumps({'title': 'bravo', 'meta': {'category': 'premium'}}))
    conn.execute_command('JSON.SET', 'doc:3', '$',
                          json.dumps({'title': 'charlie', 'items': [{'name': 'widget'}]}))
    conn.execute_command('JSON.SET', 'doc:4', '$', json.dumps({'title': 'delta', 'items': []}))

    id1_before = get_internal_id(env, 'doc:1')
    id2_before = get_internal_id(env, 'doc:2')
    id3_before = get_internal_id(env, 'doc:3')
    id4_before = get_internal_id(env, 'doc:4')

    env.expect('FT.ALTER', 'idx', 'SCHEMA',
               'ADD', '$.meta.category', 'AS', 'category', 'TAG',
               '$.items[*].name', 'AS', 'iname', 'TAG').ok()
    waitForIndexFinishScan(env, 'idx')

    env.assertEqual(get_internal_id(env, 'doc:1'), id1_before)
    env.assertGreater(get_internal_id(env, 'doc:2'), id2_before)
    env.assertGreater(get_internal_id(env, 'doc:3'), id3_before)
    env.assertEqual(get_internal_id(env, 'doc:4'), id4_before)

    env.expect('FT.SEARCH', 'idx', '@title:alpha', 'NOCONTENT').equal([1, 'doc:1'])
    env.expect('FT.SEARCH', 'idx', '@category:{premium}', 'NOCONTENT').equal([1, 'doc:2'])
    env.expect('FT.SEARCH', 'idx', '@iname:{widget}', 'NOCONTENT').equal([1, 'doc:3'])
    env.expect('FT.SEARCH', 'idx', '@title:delta', 'NOCONTENT').equal([1, 'doc:4'])


# The functions below cover every conservative fallback in design.md except one:
# Document_ProbeFieldsPresent's DOCUMENT_FIELDS_PROBE_FAILED result (an unexpected key type on
# an already-type-checked scan key, or a missing RedisJSON API) has no trigger reachable from a
# Python flow test -- both preconditions are enforced earlier in the scan before the probe ever
# runs. That per-document fallback is deliberately left uncovered here rather than faked.


@skip(cluster=True)
def testAlterSkipUnchangedDocsFallbackIndexMissing(env):
    """An added field with INDEXMISSING disables the shortcut for the whole scan: a document
    without the added field must still be fully reindexed. Asserting on doc:1 (which has no
    'status') rather than doc:2 is what makes this test meaningful -- if the shortcut were
    mistakenly still active, doc:1 is exactly the document it would wrongly skip."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc:1', 'title', 'alpha')
    conn.execute_command('HSET', 'doc:2', 'title', 'bravo', 'status', 'active')

    id1_before = get_internal_id(env, 'doc:1')
    id2_before = get_internal_id(env, 'doc:2')

    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'status', 'TAG', 'INDEXMISSING').ok()
    waitForIndexFinishScan(env, 'idx')

    env.assertGreater(get_internal_id(env, 'doc:1'), id1_before)
    env.assertGreater(get_internal_id(env, 'doc:2'), id2_before)


@skip(cluster=True)
def testAlterSkipUnchangedDocsFallbackSkipInitialScanAtCreate(env):
    """An index created with SKIPINITIALSCAN carries Index_SkipInitialScan for its lifetime,
    so a later FT.ALTER (even without SKIPINITIALSCAN) must run a full scan rather than the
    shortcut -- the index may still hold documents from before creation that were never
    backfilled, and the shortcut's skip decision assumes a document's existing index entries
    are already complete."""
    env.expect('FT.CREATE', 'idx', 'SKIPINITIALSCAN', 'SCHEMA', 'title', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    # Written after CREATE, so the keyspace-notification path indexes it synchronously and
    # gives it a known id; this does not exercise the skipped initial scan itself.
    conn.execute_command('HSET', 'doc:1', 'title', 'alpha')

    id1_before = get_internal_id(env, 'doc:1')

    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'status', 'TAG').ok()
    waitForIndexFinishScan(env, 'idx')

    # doc:1 has no 'status': only the fallback (not the shortcut) reindexes it.
    env.assertGreater(get_internal_id(env, 'doc:1'), id1_before)


@skip(cluster=True)
def testAlterSkipUnchangedDocsFallbackPriorSkipInitialScanAlter(env):
    """A prior 'FT.ALTER ... SKIPINITIALSCAN' that added a field without ever backfilling it
    sets Index_HasSkippedAlterScan; a later ALTER must run a full scan even though it did not
    itself request SKIPINITIALSCAN, since documents may still be missing that earlier field."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc:1', 'title', 'alpha')

    env.expect('FT.ALTER', 'idx', 'SKIPINITIALSCAN', 'SCHEMA', 'ADD', 'a', 'TAG').ok()

    id1_before = get_internal_id(env, 'doc:1')

    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'b', 'TAG').ok()
    waitForIndexFinishScan(env, 'idx')

    # doc:1 has neither 'a' nor 'b': only the fallback (not the shortcut) reindexes it.
    env.assertGreater(get_internal_id(env, 'doc:1'), id1_before)


@skip(cluster=True)
def testAlterSkipUnchangedDocsHistorySurvivesRdbReload(env):
    """Index_HasSkippedAlterScan is persisted through the existing IndexFlags RDB value: after
    a reload, a later ALTER must still fall back to a full scan instead of resuming the
    shortcut. Without this, a restart could silently leave documents missing a field an
    earlier 'SKIPINITIALSCAN' ALTER added but never backfilled."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc:1', 'title', 'alpha')

    env.expect('FT.ALTER', 'idx', 'SKIPINITIALSCAN', 'SCHEMA', 'ADD', 'a', 'TAG').ok()

    env.dumpAndReload()

    # Captured after the reload: reload rebuilds the doc table, so this is the baseline for
    # the next ALTER, not a comparison across the reload itself.
    id1_before = get_internal_id(env, 'doc:1')

    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'b', 'TAG').ok()
    waitForIndexFinishScan(env, 'idx')

    # doc:1 has neither 'a' nor 'b': the fallback must still be in effect after reload.
    env.assertGreater(get_internal_id(env, 'doc:1'), id1_before)


@skip(cluster=True)
def testAlterSkipUnchangedDocsFallbackActiveScan(env):
    """IndexSpec_ScanAndReindexForAlter's eligibility gate requires sp->scanner == NULL: a scan
    already registered on the spec -- even one paused before it starts running its scan proc,
    which is as 'pending' as a scan gets -- must force a concurrently scheduled ALTER onto the
    full-scan path. Pausing every newly constructed debug scanner before it runs keeps the
    first ALTER's scanner installed on the spec while the second ALTER's eligibility check
    executes, then both scanners are let run once pausing is turned off."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc:1', 'title', 'alpha')

    id1_before = get_internal_id(env, 'doc:1')

    env.expect(bgScanCommand(), 'SET_PAUSE_BEFORE_SCAN', 'true').ok()
    try:
        env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'a', 'TAG').ok()
        # 'NEW' means the debug scanner is constructed and installed as sp->scanner, but is
        # blocked before its first RM_Scan call -- exactly the "active or pending" state the
        # eligibility gate must see from the second ALTER below.
        waitForIndexStatus(env, 'NEW', 'idx')

        # Scheduled while the first ALTER's scan is still active/pending, so this one must
        # fall back to a full scan regardless of which field it adds.
        env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'b', 'TAG').ok()
        waitForIndexStatus(env, 'NEW', 'idx')
    finally:
        # Restore the debug controller before letting either scan actually run, so a later
        # test in this file does not inherit a paused scanner or debug-mode overhead.
        env.expect(bgScanCommand(), 'SET_PAUSE_BEFORE_SCAN', 'false').ok()
        env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
    waitForIndexFinishScan(env, 'idx')

    # doc:1 has neither 'a' nor 'b': only the fallback (not the shortcut) reindexes it.
    env.assertGreater(get_internal_id(env, 'doc:1'), id1_before)


@skip(cluster=True)
def testAlterSkipUnchangedDocsFallbackUnresolvedOOM(env):
    """A background scan that aborts on OOM leaves scan_failed_OOM set on the spec, which the
    eligibility gate reads directly; a later ALTER must run a full scan to finish the work the
    aborted scan left undone, even for a document missing the new ALTER's own added field.
    A single document is enough to trigger OOM on the very first (and only) scanned key once
    maxmemory is tightened, so this does not need the multi-document SET_PAUSE_ON_SCANNED_DOCS
    staging that tests/pytests/test_index_oom.py uses to control how much of a larger scan
    completes before OOM hits."""
    try:
        env.expect('FT.CONFIG', 'SET', '_BG_INDEX_MEM_PCT_THR', '80').ok()
        conn = getConnectionByEnv(env)
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT').ok()
        conn.execute_command('HSET', 'doc:1', 'title', 'alpha')

        env.expect(bgScanCommand(), 'SET_PAUSE_ON_OOM', 'true').ok()
        set_tight_maxmemory_for_oom(env, 0.85)

        # This ALTER's own scan is the one that hits OOM; doc:1 is not reindexed by it either
        # way, so its id is not meaningful until after the corrective full scan below.
        env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'a', 'TAG').ok()
        waitForIndexStatus(env, 'PAUSED_ON_OOM', 'idx')
    finally:
        # Unconditional, like the try/finally below and the sibling active-scan test: if
        # waitForIndexStatus above raised while the scanner was genuinely parked on the global
        # pause flag, skipping this would leave every later scan in this shared-server file
        # blocked on that same flag until its own TimeLimit fires.
        set_unlimited_maxmemory_for_oom(env)
        env.expect(bgScanCommand(), 'SET_PAUSE_ON_OOM', 'false').ok()
        env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
        env.expect('FT.CONFIG', 'SET', '_BG_INDEX_MEM_PCT_THR', '100').ok()
    waitForIndexFinishScan(env, 'idx')

    id1_before = get_internal_id(env, 'doc:1')

    env.expect(bgScanCommand(), 'SET_PAUSE_BEFORE_SCAN', 'true').ok()
    try:
        # Memory has recovered and the OOM pause is off, but scan_failed_OOM is still set from
        # the aborted scan above, so this ALTER must still take the full-scan fallback.
        env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'b', 'TAG').ok()
        waitForIndexStatus(env, 'NEW', 'idx')
    finally:
        env.expect(bgScanCommand(), 'SET_PAUSE_BEFORE_SCAN', 'false').ok()
        env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
    waitForIndexFinishScan(env, 'idx')

    # doc:1 has neither 'a' nor 'b': only the fallback (not the shortcut) reindexes it.
    env.assertGreater(get_internal_id(env, 'doc:1'), id1_before)


def testAlterSkipUnchangedDocsCoordinatorSearchCorrectness(env):
    """Task 3.5: a light, cluster-enabled counterpart to the id-based tests above. Internal ids
    are per-shard and not meaningfully comparable across a cluster, so this test asserts only
    search correctness -- the coordinator-fronted backfill must make exactly the right
    documents searchable on the newly added field, and pre-existing-field queries must be
    unaffected. Not skipped for cluster, so it runs standalone too."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc:1', 'title', 'alpha')
    conn.execute_command('HSET', 'doc:2', 'title', 'bravo', 'tags', 'premium')

    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'tags', 'TAG').ok()
    waitForIndexFinishScan(env, 'idx')

    env.expect('FT.SEARCH', 'idx', '@title:alpha', 'NOCONTENT').equal([1, 'doc:1'])
    env.expect('FT.SEARCH', 'idx', '@title:bravo', 'NOCONTENT').equal([1, 'doc:2'])
    env.expect('FT.SEARCH', 'idx', '@tags:{premium}', 'NOCONTENT').equal([1, 'doc:2'])


@skip(cluster=True)
def testAlterBackfillReplacementCountSparseVsDense(env):
    """Task 4.1/4.2: measure FT.ALTER SCHEMA ADD's selective backfill on a sparse workload
    (few documents carry the newly added field) and a dense one (all of them do). The
    replacement count is derived from the max_doc_id delta -- deterministic, since every
    REPLACE mints exactly one new, strictly increasing id (DocTable_Put's ++t->maxDocId) and a
    skipped document mints none -- and is the only thing asserted on. N=1000 matches the scale
    tests/pytests/test_index_oom.py already uses for its background-scan tests: large enough
    that the sparse case's per-document probe cost is not swamped by fixed overhead, small
    enough to stay a fast flow test. Elapsed time is printed for humans only; a threshold
    assertion would flake on a loaded CI host."""
    N = 1000
    SPARSE_STRIDE = 20  # 1 in 20 (5%) of documents carry the added field in the sparse case

    def run_case(idx_name, added_field, docs_with_field):
        env.expect('FT.CREATE', idx_name, 'SCHEMA', 'title', 'TEXT').ok()
        conn = getConnectionByEnv(env)
        for i in range(N):
            key = f'{idx_name}:doc:{i}'
            if i in docs_with_field:
                conn.execute_command('HSET', key, 'title', 'word', added_field, 'x')
            else:
                conn.execute_command('HSET', key, 'title', 'word')

        max_id_before = int(index_info(env, idx_name)['max_doc_id'])

        start = time.time()
        env.expect('FT.ALTER', idx_name, 'SCHEMA', 'ADD', added_field, 'TAG').ok()
        waitForIndexFinishScan(env, idx_name)
        elapsed = time.time() - start

        max_id_after = int(index_info(env, idx_name)['max_doc_id'])
        return max_id_after - max_id_before, elapsed

    sparse_docs_with_field = set(range(0, N, SPARSE_STRIDE))
    replaced_sparse, elapsed_sparse = run_case('idx_sparse', 'sparsefield', sparse_docs_with_field)
    env.assertEqual(replaced_sparse, len(sparse_docs_with_field), message=replaced_sparse)

    dense_docs_with_field = set(range(N))
    replaced_dense, elapsed_dense = run_case('idx_dense', 'densefield', dense_docs_with_field)
    env.assertEqual(replaced_dense, N, message=replaced_dense)

    print(f'[alter backfill] sparse: {len(sparse_docs_with_field)}/{N} replaced in '
          f'{elapsed_sparse:.3f}s; dense: {N}/{N} replaced in {elapsed_dense:.3f}s')
