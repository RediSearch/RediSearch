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
    conn.execute_command('HSET', 'doc:3', 'title', 'empty', 'tags', '')

    id1_before = get_internal_id(env, 'doc:1')
    id2_before = get_internal_id(env, 'doc:2')
    id3_before = get_internal_id(env, 'doc:3')

    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'tags', 'TAG').ok()
    waitForIndexFinishScan(env, 'idx')

    env.assertEqual(get_internal_id(env, 'doc:1'), id1_before)
    env.assertGreater(get_internal_id(env, 'doc:2'), id2_before)
    env.assertGreater(get_internal_id(env, 'doc:3'), id3_before)

    # The optimization must not change query results: the pre-existing field stays
    # searchable on both documents, and doc:2's replace-path reindex must have preserved it
    # alongside the newly indexed field.
    env.expect('FT.SEARCH', 'idx', '@title:hello', 'NOCONTENT').equal([1, 'doc:1'])
    env.expect('FT.SEARCH', 'idx', '@title:goodbye', 'NOCONTENT').equal([1, 'doc:2'])
    env.expect('FT.SEARCH', 'idx', '@tags:{premium}', 'NOCONTENT').equal([1, 'doc:2'])
    env.expect('FT.SEARCH', 'idx', '@title:empty', 'NOCONTENT').equal([1, 'doc:3'])


@skip(cluster=True)
def testAlterSkipUnchangedDocsHashFilter(env):
    """Selective ALTER preserves FILTER membership while skipping only unchanged documents."""
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'FILTER', '@status == "active"',
               'SCHEMA', 'title', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc:1', 'title', 'alpha', 'status', 'active')
    conn.execute_command('HSET', 'doc:2', 'title', 'bravo', 'status', 'active',
                         'category', 'premium')
    conn.execute_command('HSET', 'doc:3', 'title', 'charlie', 'status', 'inactive',
                         'category', 'premium')

    expected_docs = toSortedFlatList([2, 'doc:1', 'doc:2'])
    env.assertEqual(toSortedFlatList(env.cmd('FT.SEARCH', 'idx', '*', 'NOCONTENT')),
                    expected_docs)
    id1_before = get_internal_id(env, 'doc:1')
    id2_before = get_internal_id(env, 'doc:2')

    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'category', 'TAG').ok()
    waitForIndexFinishScan(env, 'idx')

    env.assertEqual(get_internal_id(env, 'doc:1'), id1_before)
    env.assertGreater(get_internal_id(env, 'doc:2'), id2_before)
    env.assertEqual(toSortedFlatList(env.cmd('FT.SEARCH', 'idx', '*', 'NOCONTENT')),
                    expected_docs)
    env.expect('FT.SEARCH', 'idx', '@title:alpha', 'NOCONTENT').equal([1, 'doc:1'])
    env.expect('FT.SEARCH', 'idx', '@title:bravo', 'NOCONTENT').equal([1, 'doc:2'])
    env.expect('FT.SEARCH', 'idx', '@category:{premium}', 'NOCONTENT').equal([1, 'doc:2'])


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
def testAlterSkipUnchangedDocsAfterSkipInitialScanAtCreate(env):
    """CREATE's skipped documents are indexed by a selective ALTER only if selected."""
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc:1', 'title', 'alpha')
    conn.execute_command('HSET', 'doc:2', 'title', 'bravo', 'rating', '5')
    env.expect('FT.CREATE', 'idx', 'SKIPINITIALSCAN', 'SCHEMA', 'title', 'TEXT').ok()
    waitForIndexFinishScan(env, 'idx')
    env.expect('FT.SEARCH', 'idx', '*', 'NOCONTENT').equal([0])

    conn.execute_command('HSET', 'doc:3', 'title', 'charlie')
    id3_before = get_internal_id(env, 'doc:3')

    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'rating', 'NUMERIC').ok()
    waitForIndexFinishScan(env, 'idx')

    env.expect('FT.SEARCH', 'idx', '@title:alpha', 'NOCONTENT').equal([0])
    env.expect('FT.SEARCH', 'idx', '@title:bravo', 'NOCONTENT').equal([1, 'doc:2'])
    env.expect('FT.SEARCH', 'idx', '@rating:[5 5]', 'NOCONTENT').equal([1, 'doc:2'])
    env.expect('FT.SEARCH', 'idx', '@title:charlie', 'NOCONTENT').equal([1, 'doc:3'])
    env.assertEqual(get_internal_id(env, 'doc:3'), id3_before)

    conn.execute_command('HSET', 'doc:1', 'title', 'updated')
    env.expect('FT.SEARCH', 'idx', '@title:updated', 'NOCONTENT').equal([1, 'doc:1'])


@skip(cluster=True)
def testAlterSkipUnchangedDocsAfterSkipInitialScanAlter(env):
    """Earlier skipped fields are indexed only on documents selected by the current ALTER."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc:1', 'title', 'alpha', 'category', 'fiction')
    conn.execute_command('HSET', 'doc:2', 'title', 'bravo', 'rating', '5',
                         'category', 'fiction', 'author', 'alice')
    conn.execute_command('HSET', 'doc:3', 'title', 'charlie')
    conn.execute_command('HSET', 'doc:4', 'title', 'delta', 'author', 'alice')
    keys = ['doc:1', 'doc:2', 'doc:3', 'doc:4']
    ids_before = [get_internal_id(env, key) for key in keys]

    env.expect('FT.ALTER', 'idx', 'SKIPINITIALSCAN', 'SCHEMA', 'ADD', 'category', 'TAG').ok()
    env.expect('FT.ALTER', 'idx', 'SKIPINITIALSCAN', 'SCHEMA', 'ADD', 'author', 'TAG').ok()
    env.expect('FT.SEARCH', 'idx', '@category:{fiction}', 'NOCONTENT').equal([0])
    env.expect('FT.SEARCH', 'idx', '@author:{alice}', 'NOCONTENT').equal([0])
    env.assertEqual([get_internal_id(env, key) for key in keys], ids_before)

    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'rating', 'NUMERIC').ok()
    waitForIndexFinishScan(env, 'idx')

    ids_after = [get_internal_id(env, key) for key in keys]
    env.assertEqual(ids_after[0], ids_before[0])
    env.assertGreater(ids_after[1], ids_before[1])
    env.assertEqual(ids_after[2], ids_before[2])
    env.assertEqual(ids_after[3], ids_before[3])
    env.expect('FT.SEARCH', 'idx', '@title:alpha', 'NOCONTENT').equal([1, 'doc:1'])
    env.expect('FT.SEARCH', 'idx', '@title:charlie', 'NOCONTENT').equal([1, 'doc:3'])
    env.expect('FT.SEARCH', 'idx', '@rating:[5 5]', 'NOCONTENT').equal([1, 'doc:2'])
    env.expect('FT.SEARCH', 'idx', '@category:{fiction}', 'NOCONTENT').equal([1, 'doc:2'])
    env.expect('FT.SEARCH', 'idx', '@author:{alice}', 'NOCONTENT').equal([1, 'doc:2'])

    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'extra', 'TAG').ok()
    waitForIndexFinishScan(env, 'idx')
    env.assertEqual([get_internal_id(env, key) for key in keys], ids_after)


@skip(cluster=True)
def testAlterSkippedFieldsAcrossIndexes(env):
    """A selective scan on either index leaves documents lacking its new field untouched."""
    for idx in ['idx', 'other']:
        env.expect('FT.CREATE', idx, 'SCHEMA', 'title', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc:1', 'title', 'alpha', 'category', 'fiction')
    ids_before = [get_internal_id(env, 'doc:1', idx) for idx in ['idx', 'other']]

    env.expect('FT.ALTER', 'idx', 'SKIPINITIALSCAN', 'SCHEMA', 'ADD', 'category', 'TAG').ok()
    env.expect('FT.ALTER', 'other', 'SCHEMA', 'ADD', 'rating', 'NUMERIC').ok()
    waitForIndexFinishScan(env, 'other')
    env.assertEqual(get_internal_id(env, 'doc:1', 'other'), ids_before[1])
    env.expect('FT.SEARCH', 'idx', '@category:{fiction}', 'NOCONTENT').equal([0])

    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'rating', 'NUMERIC').ok()
    waitForIndexFinishScan(env, 'idx')
    env.assertEqual(get_internal_id(env, 'doc:1', 'idx'), ids_before[0])
    env.expect('FT.SEARCH', 'idx', '@category:{fiction}', 'NOCONTENT').equal([0])


@skip(cluster=True)
def testAlterSkippedFieldsJson(env):
    """A selected JSON document also indexes skipped fields using their paths and aliases."""
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', '$.title', 'AS', 'title', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('JSON.SET', 'doc:1', '$',
                         json.dumps({'title': 'alpha', 'meta': {'category': 'fiction'}}))
    conn.execute_command('JSON.SET', 'doc:2', '$',
                         json.dumps({'title': 'bravo', 'meta': {'category': 'fiction'},
                                     'rating': 5}))
    id1_before = get_internal_id(env, 'doc:1')
    id2_before = get_internal_id(env, 'doc:2')
    env.expect('FT.ALTER', 'idx', 'SKIPINITIALSCAN', 'SCHEMA', 'ADD',
               '$.meta.category', 'AS', 'category', 'TAG').ok()
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', '$.rating', 'AS', 'rating', 'NUMERIC').ok()
    waitForIndexFinishScan(env, 'idx')

    env.assertEqual(get_internal_id(env, 'doc:1'), id1_before)
    env.assertGreater(get_internal_id(env, 'doc:2'), id2_before)
    env.expect('FT.SEARCH', 'idx', '@category:{fiction}', 'NOCONTENT').equal([1, 'doc:2'])
    env.expect('FT.SEARCH', 'idx', '@rating:[5 5]', 'NOCONTENT').equal([1, 'doc:2'])
    env.expect('FT.SEARCH', 'idx', '@title:alpha', 'NOCONTENT').equal([1, 'doc:1'])


@skip(cluster=True)
def testAlterSkippedFieldOptionsDoNotForceFullScan(env):
    """Options on skipped fields do not disable a later ALTER's presence shortcut."""
    indexes = [('missing', 'INDEXMISSING'), ('sortable', 'SORTABLE')]
    for idx, _ in indexes:
        env.expect('FT.CREATE', idx, 'PREFIX', '1', f'{idx}:', 'SCHEMA', 'title', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    for idx, option in indexes:
        key = f'{idx}:1'
        selected_key = f'{idx}:2'
        conn.execute_command('HSET', key, 'title', 'alpha')
        conn.execute_command('HSET', selected_key, 'title', 'bravo', 'rating', '5')
        old_id = get_internal_id(env, key, idx)
        selected_id = get_internal_id(env, selected_key, idx)
        env.expect('FT.ALTER', idx, 'SKIPINITIALSCAN', 'SCHEMA', 'ADD', 'category', 'TAG', option).ok()
        env.expect('FT.ALTER', idx, 'SCHEMA', 'ADD', 'rating', 'NUMERIC').ok()
        waitForIndexFinishScan(env, idx)
        env.assertEqual(get_internal_id(env, key, idx), old_id)
        env.assertGreater(get_internal_id(env, selected_key, idx), selected_id)
        env.expect('FT.SEARCH', idx, '@title:alpha', 'NOCONTENT').equal([1, key])
        env.expect('FT.SEARCH', idx, '@rating:[5 5]', 'NOCONTENT').equal([1, selected_key])
        if option == 'INDEXMISSING':
            env.expect('FT.SEARCH', idx, 'ismissing(@category)', 'NOCONTENT',
                       'DIALECT', '2').equal([1, selected_key])


@skip(cluster=True)
def testAlterSkippedFieldsAddedDuringScan(env):
    """A skipped addition cannot widen an active scan's field range or a later ALTER's."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    keys = ['doc:1', 'doc:2']
    for key in keys:
        conn.execute_command('HSET', key, 'title', 'alpha', 'category', 'fiction')
    ids_before = [get_internal_id(env, key) for key in keys]

    env.expect(bgScanCommand(), 'SET_PAUSE_ON_SCANNED_DOCS', 1).ok()
    try:
        env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'rating', 'NUMERIC').ok()
        waitForIndexStatus(env, 'PAUSED', 'idx')
        env.expect('FT.ALTER', 'idx', 'SKIPINITIALSCAN', 'SCHEMA', 'ADD', 'category', 'TAG').ok()
    finally:
        env.expect(bgScanCommand(), 'SET_PAUSE_ON_SCANNED_DOCS', 0).ok()
        env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
    waitForIndexFinishScan(env, 'idx')
    env.assertEqual([get_internal_id(env, key) for key in keys], ids_before)
    env.expect('FT.SEARCH', 'idx', '@category:{fiction}', 'NOCONTENT').equal([0])

    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'extra', 'TAG').ok()
    waitForIndexFinishScan(env, 'idx')
    env.assertEqual([get_internal_id(env, key) for key in keys], ids_before)
    env.expect('FT.SEARCH', 'idx', '@category:{fiction}', 'NOCONTENT').equal([0])


@skip(cluster=True)
def testAlterSkippedFieldsIndexedOnReload(env):
    """RAM reload indexes skipped fields; a later ALTER can still skip unaffected documents."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc:1', 'title', 'alpha', 'category', 'fiction')
    env.expect('FT.ALTER', 'idx', 'SKIPINITIALSCAN', 'SCHEMA', 'ADD', 'category', 'TAG').ok()
    env.expect('FT.SEARCH', 'idx', '@category:{fiction}', 'NOCONTENT').equal([0])
    env.dumpAndReload()
    env.expect('FT.SEARCH', 'idx', '@category:{fiction}', 'NOCONTENT').equal([1, 'doc:1'])
    old_id = get_internal_id(env, 'doc:1')

    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'rating', 'NUMERIC').ok()
    waitForIndexFinishScan(env, 'idx')
    env.assertEqual(get_internal_id(env, 'doc:1'), old_id)


@skip(cluster=True)
def testAlterSkipUnchangedDocsEmptyKeyspace(env):
    """ALTER on an empty keyspace needs no scan and does not affect later scan selection."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT').ok()
    env.expect('FT.ALTER', 'idx', 'SKIPINITIALSCAN', 'SCHEMA', 'ADD', 'category', 'TAG').ok()
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'rating', 'NUMERIC').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc:1', 'title', 'alpha', 'category', 'fiction')
    old_id = get_internal_id(env, 'doc:1')

    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'extra', 'TAG').ok()
    waitForIndexFinishScan(env, 'idx')
    env.assertEqual(get_internal_id(env, 'doc:1'), old_id)


@skip(cluster=True)
def testAlterFailedAddDoesNotWidenScan(env):
    """A rolled-back schema addition must not widen the next ALTER's field range."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc:1', 'title', 'alpha', 'category', 'fiction')
    old_id = get_internal_id(env, 'doc:1')
    env.expect('FT.ALTER', 'idx', 'SKIPINITIALSCAN', 'SCHEMA', 'ADD',
               'category', 'TAG', 'category', 'TAG').error().contains('Duplicate field')

    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'rating', 'NUMERIC').ok()
    waitForIndexFinishScan(env, 'idx')
    env.assertEqual(get_internal_id(env, 'doc:1'), old_id)


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
    conn.execute_command('HSET', 'doc:2', 'title', 'bravo', 'a', 'present')

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

    env.expect('FT.SEARCH', 'idx', '@a:{present}', 'NOCONTENT').equal([1, 'doc:2'])
    id_after = get_internal_id(env, 'doc:2')
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'extra', 'TAG').ok()
    waitForIndexFinishScan(env, 'idx')
    env.assertEqual(get_internal_id(env, 'doc:2'), id_after)


@skip(cluster=True)
def testAlterSkipUnchangedDocsFallbackUnresolvedOOM(env):
    """An unresolved OOM forces a full scan, also indexing earlier skipped fields."""
    try:
        env.expect('FT.CONFIG', 'SET', '_BG_INDEX_MEM_PCT_THR', '80').ok()
        conn = getConnectionByEnv(env)
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT').ok()
        conn.execute_command('HSET', 'doc:1', 'title', 'alpha', 'category', 'fiction')
        conn.execute_command('HSET', 'doc:2', 'title', 'bravo')
        env.expect('FT.ALTER', 'idx', 'SKIPINITIALSCAN', 'SCHEMA', 'ADD', 'category', 'TAG').ok()

        env.expect(bgScanCommand(), 'SET_PAUSE_ON_OOM', 'true').ok()
        set_tight_maxmemory_for_oom(env, 0.85)

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

    id2_before = get_internal_id(env, 'doc:2')

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

    # doc:2 has no 'b': only the full-scan fallback reindexes it.
    env.assertGreater(get_internal_id(env, 'doc:2'), id2_before)
    env.expect('FT.SEARCH', 'idx', '@category:{fiction}', 'NOCONTENT').equal([1, 'doc:1'])

    id_after = get_internal_id(env, 'doc:1')
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'extra', 'TAG').ok()
    waitForIndexFinishScan(env, 'idx')
    env.assertEqual(get_internal_id(env, 'doc:1'), id_after)


def testAlterSkipUnchangedDocsCoordinatorSearchCorrectness(env):
    """Coordinator ALTER selects by new fields and reindexes each selected document fully."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', '{doc}:1', 'title', 'alpha', 'category', 'fiction')
    conn.execute_command('HSET', '{doc}:2', 'title', 'bravo', 'tags', 'premium',
                         'category', 'fiction')

    env.expect('FT.ALTER', 'idx', 'SKIPINITIALSCAN', 'SCHEMA', 'ADD', 'category', 'TAG').ok()
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'tags', 'TAG').ok()
    waitForIndexFinishScan(env, 'idx')

    env.expect('FT.SEARCH', 'idx', '@title:alpha', 'NOCONTENT').equal([1, '{doc}:1'])
    env.expect('FT.SEARCH', 'idx', '@title:bravo', 'NOCONTENT').equal([1, '{doc}:2'])
    env.expect('FT.SEARCH', 'idx', '@tags:{premium}', 'NOCONTENT').equal([1, '{doc}:2'])
    env.expect('FT.SEARCH', 'idx', '@category:{fiction}', 'NOCONTENT').equal([1, '{doc}:2'])


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
        # PREFIX confines each case to its own documents. Without it both indexes match every
        # hash in the keyspace, so the second case's index also backfills the first case's
        # documents and the max_doc_id delta stops describing this case's ALTER.
        prefix = f'{idx_name}:doc:'
        env.expect('FT.CREATE', idx_name, 'ON', 'HASH', 'PREFIX', '1', prefix,
                   'SCHEMA', 'title', 'TEXT').ok()
        conn = getConnectionByEnv(env)
        for i in range(N):
            key = f'{prefix}{i}'
            if i in docs_with_field:
                conn.execute_command('HSET', key, 'title', 'word', added_field, 'x')
            else:
                conn.execute_command('HSET', key, 'title', 'word')

        # Snapshot only once indexing has settled: ids minted by work still in flight from the
        # writes above would otherwise land inside the delta and be counted as ALTER
        # replacements.
        waitForIndexFinishScan(env, idx_name)
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


@skip(cluster=True)
def testAlterSkipUnchangedDocsFallbackSortable(env):
    """An added SORTABLE field disables the shortcut for the whole scan, because only the full
    reindex path rebuilds a document's sorting vector.

    A skipped document would keep a vector sized for the old schema while the schema has grown
    a sorting slot, and AddDocumentCtx_UpdateNoIndex reallocates a sorting vector only when it
    is zero-length -- so a write reaching that path with a short vector would panic inside
    RSSortingVector_Put*.

    What this test pins is the fallback, not that panic: doc:1 lacks 'rank' and is exactly what
    the shortcut would skip, so the id assertion fails against a build without the gate
    (verified: it reports "1 > 1"). The panic itself is not reproduced here. The HSET below
    takes the full-reindex path, which rebuilds the vector at the new width, so it stays as a
    guard that the retained document is still writable and searchable afterwards. Reaching
    AddDocumentCtx_UpdateNoIndex needs a sortables-only update such as
    FT.ADD ... REPLACE PARTIAL, which no test here exercises."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc:1', 'title', 'alpha')
    conn.execute_command('HSET', 'doc:2', 'title', 'bravo', 'rank', '1')

    id1_before = get_internal_id(env, 'doc:1')

    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'rank', 'NUMERIC', 'SORTABLE', 'NOINDEX').ok()
    waitForIndexFinishScan(env, 'idx')

    env.assertGreater(get_internal_id(env, 'doc:1'), id1_before)

    # Writes only the NOINDEX sortable field, so this takes the sortables-only update path that
    # writes straight into the retained vector at the new field's sortIdx.
    conn.execute_command('HSET', 'doc:1', 'rank', '7')
    env.assertEqual(env.cmd('PING'), True)
    env.assertEqual(toSortedFlatList(env.cmd('FT.SEARCH', 'idx', '@title:alpha', 'NOCONTENT')),
                    toSortedFlatList([1, 'doc:1']))
