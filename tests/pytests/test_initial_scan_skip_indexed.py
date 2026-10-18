# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from common import *

# The initial scan of a new index must not re-index a document that its keyspace notification
# already indexed while the scan was pending or running. get_internal_id (common.py) is the
# observable: a reindex goes through the replace path and assigns a new, larger id, so an
# unchanged id shows that the scan skipped the document.


@skip(cluster=True)
def testInitialScanSkipsDocsIndexedByNotification(env):
    """A document written while the initial scan is paused is indexed by its keyspace
    notification, and the scan then skips it instead of replacing it under a new doc id.
    Documents that existed before FT.CREATE and were not touched are still indexed by the scan."""
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc:old', 't', 'before')
    conn.execute_command('HSET', 'doc:updated', 't', 'stale')

    env.expect(bgScanCommand(), 'SET_PAUSE_BEFORE_SCAN', 'true').ok()
    try:
        env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
        # The scanner exists but has not started: writes now reach the index only through
        # their keyspace notifications.
        waitForIndexStatus(env, 'NEW', 'idx')
        conn.execute_command('HSET', 'doc:new', 't', 'during')
        conn.execute_command('HSET', 'doc:updated', 't', 'fresh')
        new_id = get_internal_id(env, 'doc:new')
        updated_id = get_internal_id(env, 'doc:updated')
    finally:
        env.expect(bgScanCommand(), 'SET_PAUSE_BEFORE_SCAN', 'false').ok()
        env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
    waitForIndexFinishScan(env, 'idx')

    # Skipped by the scan: same ids, and no replaced (deleted) doc ids left behind.
    env.assertEqual(get_internal_id(env, 'doc:new'), new_id)
    env.assertEqual(get_internal_id(env, 'doc:updated'), updated_id)
    # Three documents, each indexed exactly once.
    info = index_info(env, 'idx')
    env.assertEqual(int(info['num_docs']), 3, message=info)
    env.assertEqual(int(info['max_doc_id']), 3, message=info)

    # The untouched pre-existing document was indexed by the scan, and the update made during
    # the pause was not overwritten by the scan's view of the same key.
    env.expect('FT.SEARCH', 'idx', '@t:before', 'NOCONTENT').equal([1, 'doc:old'])
    env.expect('FT.SEARCH', 'idx', '@t:during', 'NOCONTENT').equal([1, 'doc:new'])
    env.expect('FT.SEARCH', 'idx', '@t:fresh', 'NOCONTENT').equal([1, 'doc:updated'])
    env.expect('FT.SEARCH', 'idx', '@t:stale', 'NOCONTENT').equal([0])


@skip(cluster=True)
def testSynUpdateReindexesIndexedDocs(env):
    """FT.SYNUPDATE reindexes documents that are already indexed so the new synonyms apply to
    them. It must not take the initial-scan shortcut that skips documents that already have a
    doc id."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc:1', 't', 'baby')
    id_before = get_internal_id(env, 'doc:1')

    env.expect('FT.SYNUPDATE', 'idx', 'g', 'baby', 'child').ok()
    waitForIndexFinishScan(env, 'idx')

    env.assertGreater(get_internal_id(env, 'doc:1'), id_before)
    env.expect('FT.SEARCH', 'idx', '@t:child', 'NOCONTENT').equal([1, 'doc:1'])


@skip(cluster=True)
def testAlterFullScanReindexesIndexedDocs(env):
    """A SORTABLE field forces FT.ALTER onto the full-scan path (see
    IndexSpec_ScanAndReindexForAlter), which must reindex every document, not skip the ones
    that already have a doc id."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc:1', 't', 'hello')
    id_before = get_internal_id(env, 'doc:1')

    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'n', 'NUMERIC', 'SORTABLE').ok()
    waitForIndexFinishScan(env, 'idx')

    env.assertGreater(get_internal_id(env, 'doc:1'), id_before)
    env.expect('FT.SEARCH', 'idx', '@t:hello', 'NOCONTENT').equal([1, 'doc:1'])
