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
