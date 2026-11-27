# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from common import *
import platform
from time import sleep
import threading
import time

@skip(cluster=True)
def testBasicGC(env):
    set_thread_gc_config(env, clean_threshold=0)
    env.assertOk(env.cmd('ft.create', 'idx', 'ON', 'HASH',
                         'schema', 'title', 'text', 'id', 'numeric', 't', 'tag'))
    waitForIndex(env, 'idx')
    for i in range(101):
        env.assertOk(env.cmd('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields',
                             'title', 'hello world',
                             'id', '5',
                             't', 'tag1'))

    env.assertEqual(env.cmd(debug_cmd(), 'DUMP_INVIDX', 'idx', 'world'), [int(i) for i in range(1, 102)])
    env.assertEqual(env.cmd(debug_cmd(), 'DUMP_NUMIDX', 'idx', 'id'), [[int(i) for i in range(1, 102)]])
    env.assertEqual(env.cmd(debug_cmd(), 'DUMP_TAGIDX', 'idx', 't'), [['tag1', [int(i) for i in range(1, 102)]]])

    env.assertEqual(env.cmd('ft.del', 'idx', 'doc0'), 1)

    forceInvokeGC(env, 'idx')

    # check that the gc collected the deleted docs
    env.assertEqual(env.cmd(debug_cmd(), 'DUMP_INVIDX', 'idx', 'world'), [int(i) for i in range(2, 102)])
    env.assertEqual(env.cmd(debug_cmd(), 'DUMP_NUMIDX', 'idx', 'id'), [[int(i) for i in range(2, 102)]])
    env.assertEqual(env.cmd(debug_cmd(), 'DUMP_TAGIDX', 'idx', 't'), [['tag1', [int(i) for i in range(2, 102)]]])

@skip(cluster=True)
def testBasicGCWithEmptyInvIdx(env):
    if env.moduleArgs is not None and 'GC_POLICY LEGACY' in env.moduleArgs:
        # this test is not relevant for legacy gc cause its not squashing inverted index
        env.skip()
    set_thread_gc_config(env, clean_threshold=0)
    env.assertOk(env.cmd('ft.create', 'idx', 'ON', 'HASH', 'schema', 'title', 'text'))
    waitForIndex(env, 'idx')
    env.assertOk(env.cmd('ft.add', 'idx', 'doc1', 1.0, 'fields',
                         'title', 'hello world'))

    env.assertEqual(env.cmd(debug_cmd(), 'DUMP_INVIDX', 'idx', 'world'), [1])

    env.assertEqual(env.cmd('ft.del', 'idx', 'doc1'), 1)

    forceInvokeGC(env, 'idx')

    # check that the gc collected the deleted docs
    env.expect(debug_cmd(), 'DUMP_INVIDX', 'idx', 'world').error().contains('Can not find the inverted index')

@skip(cluster=True)
def testGCMissingFieldNumRecords(env):
    # Regression: deleting documents indexed in a missing-field (INDEXMISSING)
    # inverted index must not underflow the spec's num_records. Missing-field
    # entries are not counted in num_records on insertion, so the fork GC must
    # not subtract them on removal (it previously did, driving num_records
    # negative once enough missing-field docs were collected).
    set_thread_gc_config(env, clean_threshold=0)
    env.assertOk(env.cmd('ft.create', 'idx', 'schema', 't', 'text', 'm', 'tag', 'INDEXMISSING'))
    waitForIndex(env, 'idx')

    n = 100
    for i in range(n):
        # Even docs carry field `m`; odd docs are missing it (populating the
        # missing-field index for `m`).
        if i % 2 == 0:
            env.cmd('hset', f'doc{i}', 't', 'hello', 'm', 'x')
        else:
            env.cmd('hset', f'doc{i}', 't', 'hello')

    # Half the docs are missing `m`.
    env.assertEqual(env.cmd('ft.search', 'idx', 'ismissing(@m)', 'LIMIT', 0, 0, 'DIALECT', 2)[0], n // 2)
    before = int(index_info(env, 'idx')['num_records'])
    env.assertGreater(before, 0)

    # Delete everything and collect.
    for i in range(n):
        env.cmd('del', f'doc{i}')
    forceInvokeGC(env, 'idx')

    # num_records must return to 0 -- and above all must never be negative.
    after = int(index_info(env, 'idx')['num_records'])
    env.assertEqual(after, 0)
    env.assertGreaterEqual(after, 0)

@skip(cluster=True)
def testNumericGCIntensive(env):
    NumberOfDocs = 1000
    set_thread_gc_config(env, clean_threshold=0)
    env.assertOk(env.cmd('ft.create', 'idx', 'ON', 'HASH', 'schema', 'id', 'numeric'))
    waitForIndex(env, 'idx')

    for i in range(NumberOfDocs):
        env.assertOk(env.cmd('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields', 'id', '1'))

    for i in range(0, NumberOfDocs, 2):
        env.assertEqual(env.cmd('ft.del', 'idx', 'doc%d' % i), 1)

    for i in range(100):
        forceInvokeGC(env, 'idx')

    res = env.cmd(debug_cmd(), 'DUMP_NUMIDX', 'idx', 'id')
    for r1 in res:
        for r2 in r1:
            # if r2 is greater then 900 its on the last block and fork GC does not clean the last block
            env.assertTrue(r2 % 2 == 0 or r2 > 900)

@skip(cluster=True)
def testNumericCompleteGCAndRepopulation(env):
    """Test that after deleting all docs used for a numeric index through GC that the index is still usable"""
    set_thread_gc_config(env, clean_threshold=0)
    env.assertOk(env.cmd('ft.create', 'idx', 'ON', 'HASH', 'schema', 'id', 'numeric'))
    waitForIndex(env, 'idx')

    # Phase 1: Add initial documents
    InitialDocs = 100
    for i in range(InitialDocs):
        env.assertEqual(env.cmd('hset', 'doc%d' % i, 'id', str(i)), 1)

    # Verify initial state
    res = env.cmd(debug_cmd(), 'DUMP_NUMIDX', 'idx', 'id')
    # Numeric indices return nested structure with buckets
    all_doc_ids = []
    for bucket in res:
        all_doc_ids.extend(bucket)
    env.assertEqual(len(all_doc_ids), InitialDocs)

    # Phase 2: Delete all documents and run GC
    for i in range(InitialDocs):
        env.assertEqual(env.cmd('del', 'doc%d' % i), 1)

    forceInvokeGC(env, 'idx')

    # Verify index is empty - might return empty buckets or empty list
    res = env.cmd(debug_cmd(), 'DUMP_NUMIDX', 'idx', 'id')
    env.assertEqual(res, [[]])

    # Verify search returns no results
    search_res = env.cmd('ft.search', 'idx', '@id:[0 99]')
    env.assertEqual(search_res[0], 0)

    # Phase 3: Re-add documents with the same numeric values
    NewDocs = 50
    for i in range(NewDocs):
        env.assertEqual(env.cmd('hset', 'newdoc%d' % i, 'id', str(i)), 1)

    # Verify new documents are indexed
    res = env.cmd(debug_cmd(), 'DUMP_NUMIDX', 'idx', 'id')
    all_doc_ids = []
    for bucket in res:
        all_doc_ids.extend(bucket)
    env.assertEqual(len(all_doc_ids), NewDocs)

    # Verify search works
    search_res = env.cmd('ft.search', 'idx', '@id:[0 49]')
    env.assertEqual(search_res[0], NewDocs)

@skip(cluster=True)
def testNumericMergesTrees(env):
    """Test to check the numeric index trees merges nodes when half or more of all the nodes are empty"""
    set_thread_gc_config(env, clean_threshold=0)
    env.assertOk(env.cmd('ft.create', 'idx', 'ON', 'HASH', 'schema', 'id', 'numeric'))
    waitForIndex(env, 'idx')

    # Phase 1: Add initial documents
    InitialDocs = 255
    for i in range(InitialDocs):
        env.assertEqual(env.cmd('hset', 'doc%d' % i, 'id', str(i)), 1)

    # Verify initial state
    res = env.cmd(debug_cmd(), 'DUMP_NUMIDX', 'idx', 'id')
    env.assertEqual(len(res), 3)
    # Values in each node of the tree
    env.assertEqual(res[0], [int(i) for i in range(44, 256)])
    env.assertEqual(res[1], [int(i) for i in range(9, 44)])
    env.assertEqual(res[2], [int(i) for i in range(1, 9)])

    # Phase 2: Empty the last node
    for i in range(0, 8):
        env.assertEqual(env.cmd('del', 'doc%d' % i), 1)

    forceInvokeGC(env, 'idx')

    # Verify last node is empty, but still present
    # The tree isn't (yet) sparse enough to trigger a compaction
    res = env.cmd(debug_cmd(), 'DUMP_NUMIDX', 'idx', 'id')
    env.assertEqual(len(res), 3)
    env.assertEqual(res[0], [int(i) for i in range(44, 256)])
    env.assertEqual(res[1], [int(i) for i in range(9, 44)])
    env.assertEqual(res[2], [])

    # Phase 3: Make the second-to-last node empty to trigger a merge
    for i in range(8, 43):
        env.assertEqual(env.cmd('del', 'doc%d' % i), 1)

    forceInvokeGC(env, 'idx')

    # Verify nodes were merged
    res = env.cmd(debug_cmd(), 'DUMP_NUMIDX', 'idx', 'id')
    env.assertEqual(len(res), 1)
    env.assertEqual(res[0], [int(i) for i in range(44, 256)])

@skip(cluster=True)
def testGeoGCIntensive(env:Env):
    NumberOfDocs = 1000
    set_thread_gc_config(env, clean_threshold=0)
    env.expect('ft.create', 'idx', 'ON', 'HASH', 'schema', 'g', 'geo').ok()

    for i in range(NumberOfDocs):
        env.expect('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields', 'g', '12.34,56.78').ok()

    for i in range(0, NumberOfDocs, 2):
        env.expect('ft.del', 'idx', 'doc%d' % i).equal(1)

    forceInvokeGC(env)

    res = env.cmd(debug_cmd(), 'DUMP_NUMIDX', 'idx', 'g')
    for r1 in res:
        for r2 in r1:
            # if r2 is greater then 900 its on the last block and fork GC does not clean the last block
            env.assertTrue(r2 % 2 == 0 or r2 > 900)

@skip(cluster=True)
def testTagGC(env):
    NumberOfDocs = 101
    set_thread_gc_config(env, clean_threshold=0)
    env.assertOk(env.cmd('ft.create', 'idx', 'ON', 'HASH', 'schema', 't', 'tag'))
    waitForIndex(env, 'idx')

    for i in range(NumberOfDocs):
        env.assertOk(env.cmd('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields', 't', '1'))

    for i in range(0, NumberOfDocs, 2):
        env.assertEqual(env.cmd('ft.del', 'idx', 'doc%d' % i), 1)

    for i in range(100):
        # gc is random so we need to do it long enough times for it to work
        forceInvokeGC(env, 'idx')

    res = env.cmd(debug_cmd(), 'DUMP_TAGIDX', 'idx', 't')
    for r1 in res:
        for r2 in r1[1]:
            # if r2 is greater then 100 its on the last block and fork GC does not clean the last block
            env.assertTrue(r2 % 2 == 0 or r2 > 100)

@skip(cluster=True)
def testTagCompleteGCAndRepopulation(env):
    """Test that after deleting all docs used for a tag index through GC that the index is still usable"""
    set_thread_gc_config(env, clean_threshold=0)
    env.assertOk(env.cmd('ft.create', 'idx', 'ON', 'HASH', 'schema', 't', 'tag'))
    waitForIndex(env, 'idx')

    # Phase 1: Add initial documents
    InitialDocs = 100
    for i in range(InitialDocs):
        env.assertEqual(env.cmd('hset', 'doc%d' % i, 't', 'tag1'), 1)

    # Verify initial state
    res = env.cmd(debug_cmd(), 'DUMP_TAGIDX', 'idx', 't')
    env.assertEqual(len(res), 1)
    env.assertEqual(res[0][0], 'tag1')
    env.assertEqual(len(res[0][1]), InitialDocs)

    # Phase 2: Delete all documents and run GC
    for i in range(InitialDocs):
        env.assertEqual(env.cmd('del', 'doc%d' % i), 1)

    # forceInvokeGC waits for any in-progress save, but an Enterprise auto-bgsave
    # can start in the gap before the fork, making RedisModule_Fork fail with
    # EEXIST so the cycle collects nothing; retry until the tag index is emptied.
    # On OSS this collects on the first pass (no autosave races the fork).
    res = None
    for _ in range(100):
        forceInvokeGC(env, 'idx')
        res = env.cmd(debug_cmd(), 'DUMP_TAGIDX', 'idx', 't')
        if res == []:
            break

    # Verify tag index is empty
    env.assertEqual(res, [])

    # Verify search returns no results
    search_res = env.cmd('ft.search', 'idx', '@t:{tag1}')
    env.assertEqual(search_res[0], 0)

    # Phase 3: Re-add documents with the same tag
    NewDocs = 50
    for i in range(NewDocs):
        env.assertEqual(env.cmd('hset', 'newdoc%d' % i, 't', 'tag1'), 1)

    # Verify new documents are indexed
    res = env.cmd(debug_cmd(), 'DUMP_TAGIDX', 'idx', 't')
    env.assertEqual(len(res), 1)
    env.assertEqual(res[0][0], 'tag1')
    env.assertEqual(len(res[0][1]), NewDocs)

    # Verify search works
    search_res = env.cmd('ft.search', 'idx', '@t:{tag1}')
    env.assertEqual(search_res[0], NewDocs)

@skip(cluster=True)
def testDeleteEntireBlock(env):
    set_thread_gc_config(env, clean_threshold=0)
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH',
               'SCHEMA', 'test', 'TEXT', 'SORTABLE', 'test2', 'TEXT', 'SORTABLE', ).ok()
    waitForIndex(env, 'idx')
    # creating 5 blocks on 'checking' inverted index
    for i in range(700):
        env.expect('FT.ADD', 'idx', 'doc%d' % i, '1.0', 'FIELDS', 'test', 'checking', 'test2', 'checking%d' % i).ok()

    # delete docs in the middle of the inverted index, make sure the binary search are not broken
    for i in range(400, 501):
        env.expect('FT.DEL', 'idx', 'doc%d' % i).equal(1)
    res = env.cmd('FT.SEARCH', 'idx', '@test:checking @test2:checking250')
    env.assertEqual(res[0:2],[1, 'doc250'])
    env.assertEqual(set(res[2]), set(['test', 'checking', 'test2', 'checking250']))

    # actually clean the inverted index, make sure the binary search are not broken, check also after rdb reload
    for i in range(100):
        # gc is random so we need to do it long enough times for it to work
        forceInvokeGC(env, 'idx')
    for _ in env.reloadingIterator():
        waitForIndex(env, 'idx')
        res = env.cmd('FT.SEARCH', 'idx', '@test:checking @test2:checking250')
        env.assertEqual(res[0:2],[1, 'doc250'])
        env.assertEqual(set(res[2]), set(['test', 'checking', 'test2', 'checking250']))

@skip(cluster=True)
def testGCIntegrationWithRedisFork():
    env = Env(moduleArgs='GC_POLICY FORK')
    if env.env == 'existing-env':
        env.skip()
    if env.env == 'enterprise':
        env.skip()
    env.expect(config_cmd(), 'set', 'FORK_GC_CLEAN_THRESHOLD', 0).equal('OK')
    env.expect(config_cmd(), 'SET', 'FORKGC_SLEEP_BEFORE_EXIT', '4').ok()
    env.expect(config_cmd(), 'set', 'FORK_GC_CLEAN_THRESHOLD', 0).ok()
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'title', 'TEXT', 'SORTABLE').ok()
    waitForIndex(env, 'idx')
    env.expect('FT.ADD', 'idx', 'doc1', 1.0, 'FIELDS', 'title', 'hello world').ok()
    env.expect('bgsave').true()
    forceInvokeGC(env, 'idx')
    env.expect('bgsave').true()
    env.cmd(config_cmd(), 'SET', 'FORKGC_SLEEP_BEFORE_EXIT', '0')

@skip(cluster=True)
def testGCCleanupWithReplace(env):
    """Test that GC properly cleans old inverted index entries after
    REPLACE and REPLACE PARTIAL operations."""
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'title', 'TEXT', 'SORTABLE').ok()
    waitForIndex(env, 'idx')

    # Add docs with value 'foo', then replace all with 'foo1'
    for i in range(1000):
        env.expect('FT.ADD', 'idx', 'doc%d' % i, '1.0', 'FIELDS', 'title', 'foo').ok()

    for i in range(1000):
        env.expect('FT.ADD', 'idx', 'doc%d' % i, '1.0', 'REPLACE', 'FIELDS', 'title', 'foo1').ok()

    forceInvokeGC(env, 'idx')

    # Old 'foo' entries should be cleaned
    env.expect(debug_cmd(), 'DUMP_INVIDX', 'idx', 'foo').error().contains('Can not find the inverted index')

    # Replace partial: replace all with 'foo2', old 'foo1' entries should be cleaned
    for i in range(1000):
        env.expect('FT.ADD', 'idx', 'doc%d' % i, '1.0', 'REPLACE', 'PARTIAL', 'FIELDS', 'title', 'foo2').ok()

    forceInvokeGC(env, 'idx')

    env.expect(debug_cmd(), 'DUMP_INVIDX', 'idx', 'foo1').error().contains('Can not find the inverted index')

@skip(cluster=True)
def testGCShutDownOnExit(env):
    if env.env == 'existing-env' or env.env == 'enterprise' or platform.system() == 'Darwin':
        env.skip()
    env.expect(config_cmd(), 'set', 'FORK_GC_CLEAN_THRESHOLD', 0).equal('OK')
    env = Env(moduleArgs='GC_POLICY FORK FORKGC_SLEEP_BEFORE_EXIT 20')
    env.expect(config_cmd(), 'set', 'FORK_GC_CLEAN_THRESHOLD', 0).ok()
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'title', 'TEXT', 'SORTABLE').ok()
    waitForIndex(env, 'idx')
    env.expect(debug_cmd(), 'GC_FORCEBGINVOKE', 'idx').ok()
    env.stop()
    env.start()

    # make sure server started successfully
    env.cmd('flushall')
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'title', 'TEXT', 'SORTABLE').ok()
    waitForIndex(env, 'idx')

@skip(cluster=True)
def testGFreeEmpryTerms(env):
    if env.env == 'existing-env' or env.env == 'enterprise':
        env.skip()

    env = Env(moduleArgs='GC_POLICY FORK')
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 't', 'TEXT').ok()

    for i in range(200):
        env.expect('hset', 'doc%d'%i, 't', 'foo')

    for i in range(200):
        env.expect('del', 'doc%d'%i)

    env.expect(debug_cmd(), 'DUMP_TERMS', 'idx').equal(['foo'])
    forceInvokeGC(env, 'idx')
    env.expect(debug_cmd(), 'DUMP_TERMS', 'idx').equal([])

@skip(cluster=True)
def testAutoMemory_MOD_3951():
    env = Env(redisConfigFile=thread_gc_conf(clean_threshold=0))
    conn = getConnectionByEnv(env)

    # create index with filter
    conn.execute_command('FT.CREATE', 'idx', 'FILTER', '@t == "5"', 'SCHEMA', 't', 'TEXT')
    # add docs
    conn = getConnectionByEnv(env)
    for i in range(100):
        conn.execute_command('HSET', i, 't', i % 10)
    # delete 1 doc and trigger GC
    conn.execute_command('DEL', 0)
    forceInvokeGC(env, 'idx')
    # call alter to trigger rescan
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', '2nd', 'TEXT').equal('OK')

    # This test should catch some leaks on the sanitizer

def testConcurrentFTInfoDuringIndexDeletion(env):
    """
    Test that performs FT.INFO calls concurrently while indexes are being deleted
    and garbage collected. This tests the robustness of FT.INFO during GC operations.
    """
    # Configure GC to be more aggressive for testing
    set_thread_gc_config(env, clean_threshold=0)
    set_thread_gc_config(env, run_interval=100)  # Run GC more frequently

    # Number of indexes to create and test with
    num_indexes = 5
    num_docs = 1000

    # Create multiple indexes with different field types
    index_names = []
    for i in range(num_indexes):
        idx_name = f'test_idx_{i}'
        index_names.append(idx_name)

        # Create index with multiple field types to make it more substantial
        env.expect('FT.CREATE', idx_name, 'ON', 'HASH',
                   'SCHEMA',
                   'title', 'TEXT', 'SORTABLE',
                   'price', 'NUMERIC', 'SORTABLE',
                   'category', 'TAG').equal('OK')
        waitForIndex(env, idx_name)

    # Add documents to make the indexes substantial
    with env.getClusterConnectionIfNeeded() as conn:
        for j in range(num_docs):
            doc_id = f'doc_{j}'
            conn.execute_command('HSET', doc_id,
                                'title', f'Product {j}',
                                'price', j * 10.5,
                                'category', f'cat{j % 5}')

        # Verify all indexes are created and populated
        for idx_name in index_names:
            info = env.cmd('FT.INFO', idx_name)
            info_dict = {info[i]: info[i + 1] for i in range(0, len(info), 2)}
            env.assertEqual(int(info_dict['num_docs']), num_docs)

    # Shared variables for thread coordination
    results = {'info_calls': 0, 'errors': 0, 'successful_calls': 0}
    stop_threads = threading.Event()

    def ft_info_worker(idx_name):
        """Worker function that continuously calls FT.INFO on an index"""
        with env.getClusterConnectionIfNeeded() as local_conn:
            while not stop_threads.is_set():
                try:
                    info_result = local_conn.execute_command('FT.INFO', idx_name)
                    results['info_calls'] += 1
                    results['successful_calls'] += 1
                    # Small delay to prevent overwhelming the system
                    time.sleep(0.01)
                except Exception as e:
                    results['info_calls'] += 1
                    results['errors'] += 1
                    # Expected errors when index is being deleted:
                    # - "SEARCH_INDEX_NOT_FOUND Index not found"
                    error_msg = str(e).lower()
                    if 'index not found' in error_msg:
                        # These are expected errors during index deletion
                        pass
                    else:
                        # Unexpected error
                        env.assertTrue(False, message=f"Unexpected error in FT.INFO for {idx_name}: {e}")
                        break
                    time.sleep(0.01)

    # Start worker threads for each index
    threads = []
    for idx_name in index_names:
        thread = threading.Thread(target=ft_info_worker, args=(idx_name,))
        thread.daemon = True
        threads.append(thread)
        thread.start()

    # Let the threads run for a short time to establish baseline
    while results['info_calls'] < 10:
        time.sleep(0.1)

    # Delete all documents
    with env.getClusterConnectionIfNeeded() as local_conn:
        for i in range(num_docs):
            local_conn.execute_command('del', f'doc_{i}')
            if i % 100 == 0:
                for idx_name in index_names:
                    forceBGInvokeGC(env, idx_name)


    # Now delete the indexes while FT.INFO calls are running
    for idx_name in index_names:
        try:
            env.expect('FT.DROPINDEX', idx_name).equal('OK')
        except Exception as e:
            env.assertTrue(False, message=f"Unexpected error dropping index {idx_name}: {e}")

        # Force GC to clean up the deleted index
        try:
            forceBGInvokeGC(env, idx_name)
        except Exception as e:
            # Expected - index should be gone
            env.assertTrue('index not found' in str(e).lower(),
                          message=f"Unexpected error in GC for deleted index {idx_name}: {e}")
            pass

        # Small delay between deletions to spread out the work
        time.sleep(0.1)

    # Continue running FT.INFO calls for a bit longer to catch cleanup operations
    while results['errors'] < 10:
        time.sleep(0.1)

    # Stop all threads
    stop_threads.set()
    for thread in threads:
        thread.join(timeout=5.0)  # 5 second timeout for thread cleanup

    # Verify that we had at least some successful calls
    env.assertGreater(results['successful_calls'], 0,
                     message=f"Expected some successful FT.INFO calls, got {results['successful_calls']}")

    # Verify that all indexes are actually deleted
    for idx_name in index_names:
        try:
            env.cmd('FT.INFO', idx_name)
            env.assertTrue(False, f"Index {idx_name} should have been deleted")
        except Exception as e:
            # Expected - index should be gone
            env.assertTrue('index not found' in str(e).lower(),
                          message=f"Unexpected error for deleted index {idx_name}: {e}")
    env.expect(debug_cmd(), 'GC_WAIT_FOR_JOBS').equal('DONE')


# Fork-only: the thread GC policy neither forks nor skips cycles under memory pressure.
@skip(cluster=True)
def test_gc_oom():
    env = Env(moduleArgs='GC_POLICY FORK')
    env.expect(config_cmd(), 'SET', 'FORK_GC_CLEAN_THRESHOLD', '0').ok()
    env.expect(config_cmd(), 'SET', 'FORK_GC_RUN_INTERVAL', '30000').ok()
    num_docs = 10
    # Create index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    # Add some documents
    for i in range(num_docs):
        env.expect('HSET', f'doc{i}', 't', f'name{i}').equal(1)

    set_tight_maxmemory_for_oom(env)

    # Delete them all
    for i in range(num_docs):
        env.expect('DEL', f'doc{i}').equal(1)

    forceInvokeGC(env)

    # Verify no bytes collected by GC
    info = index_info(env)
    gc_dict = to_dict(info["gc_stats"])
    bytes_collected = int(gc_dict['bytes_collected'])
    env.assertEqual(bytes_collected, 0)

    # Increase memory and rerun GC
    set_unlimited_maxmemory_for_oom(env)
    forceInvokeGC(env)

    # Verify bytes collected by GC is more than 0
    info = index_info(env)
    gc_dict = to_dict(info["gc_stats"])
    bytes_collected = int(gc_dict['bytes_collected'])
    env.assertGreater(bytes_collected, 0)

@skip(cluster=True)
def test_gc_oom_replica_relaxed():
    """
    Test that GC runs on replicas even when maxmemory is exceeded.

    On replicas, the OOM check only considers max_process_mem (Enterprise limit),
    not maxmemory. In OSS, max_process_mem is not set, so GC should always run
    on replicas regardless of maxmemory setting.
    """
    # Set FORK_GC_CLEAN_THRESHOLD to 0 via module args since FT.CONFIG SET
    # cannot be executed on a read-only replica
    env = Env(useSlaves=True, forceTcp=True,
              moduleArgs='GC_POLICY FORK FORK_GC_CLEAN_THRESHOLD 0 FORK_GC_RUN_INTERVAL 30000')

    master = env.getConnection()
    slave = env.getSlaveConnection()

    # Verify connections work
    env.assertTrue(master.execute_command("PING"))
    env.assertTrue(slave.execute_command("PING"))

    # Wait for master and slave to be in sync
    env.expect('WAIT', '1', '10000').equal(1)

    num_docs = 10
    # Create index and add documents on master
    master.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT')
    for i in range(num_docs):
        master.execute_command('HSET', f'doc{i}', 't', f'name{i}')

    # Wait for sync
    master.execute_command('WAIT', '1', '10000')

    # Verify docs are synced to the slave
    slave_info = slave.execute_command('FT.INFO', 'idx')
    slave_info_dict = to_dict(slave_info)
    env.assertEqual(int(slave_info_dict['num_docs']), num_docs)

    # Delete docs on master, sync to slave
    for i in range(num_docs):
        master.execute_command('DEL', f'doc{i}')
    master.execute_command('WAIT', '1', '10000')

    # Get memory info from slave
    slave_memory_info = slave.execute_command('INFO', 'MEMORY')
    slave_memory = slave_memory_info['used_memory']

    # Set tight maxmemory on slave to simulate OOM condition based on maxmemory
    # This would block GC on master, but replica ignores maxmemory for GC OOM check
    # (only checks max_process_mem which is 0 in OSS, so used_memory_ratio = 0)
    slave.execute_command('CONFIG', 'SET', 'maxmemory', int(slave_memory * 0.99))

    # Enterprise standalone replication can leave a background save child
    # around briefly, which makes RedisModule_Fork fail with EEXIST; wait for
    # it to clear before invoking GC. On OSS this is a no-op poll (no autosave
    # races the fork).
    waitForRdbSaveToFinish(lambda: slave.execute_command('INFO', 'Persistence'), attempts=100, sleep_interval=0.1)

    bytes_collected = 0
    # A new autosave can start in the gap between the wait above and
    # GC_FORCEINVOKE below; retry a couple of times as a residual-race safety net.
    for _ in range(2):
        # Force GC on slave - should run despite maxmemory being exceeded
        # because replicas only check max_process_mem (which is 0 in OSS)
        slave.execute_command(debug_cmd(), 'GC_FORCEINVOKE', 'idx')

        # Verify bytes were collected by GC on the slave
        slave_info = slave.execute_command('FT.INFO', 'idx')
        slave_info_dict = to_dict(slave_info)
        gc_dict = to_dict(slave_info_dict["gc_stats"])
        bytes_collected = int(gc_dict['bytes_collected'])
        if bytes_collected > 0:
            break
        sleep(0.1)

    env.assertGreater(bytes_collected, 0,
        message="GC should run on replica even when maxmemory is exceeded")

@skip(cluster=True)
def testForceGCBypassesThreshold(env):
    """Test that GC_FORCEINVOKE (force=true) bypasses the clean threshold,
    while the periodic GC (force=false) respects it."""

    # High threshold so periodic GC (force=false) always skips, short interval so it attempts
    # soon. A Search Disk index runs the disk policy, which reads the FORK settings.
    set_thread_gc_config(env, clean_threshold=1000)
    set_thread_gc_config(env, run_interval=1)
    env.expect(config_cmd(), 'set', 'FORK_GC_CLEAN_THRESHOLD', 1000).equal('OK')
    env.expect(config_cmd(), 'set', 'FORK_GC_RUN_INTERVAL', 1).equal('OK')

    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'title', 'TEXT').ok()
    waitForIndex(env, 'idx')

    # Add 10 documents
    for i in range(10):
        env.expect('HSET', 'doc%d' % i, 'title', 'hello world').equal(1)

    # Delete 9 documents (well below threshold of 1000)
    for i in range(9):
        env.expect('DEL', 'doc%d' % i).equal(1)

    # Save the inverted index state before GC
    debug_rep = env.cmd(debug_cmd(), 'DUMP_INVIDX', 'idx', 'hello')

    # Every scheduled pass re-arms its timer from the main-thread one-shot, a skipped one too, so
    # two such arms mean at least one whole pass ran since the deletions.
    env.expect(debug_cmd(), 'GC_TIMER_ARMS', 'RESET').ok()
    arms = lambda: int(env.cmd(debug_cmd(), 'GC_TIMER_ARMS', 'FROM_ONESHOT'))
    wait_for_condition(lambda: (arms() >= 2, {'arms': arms()}), 'the periodic GC never ran')

    # Verify inverted index is unchanged (periodic GC skipped due to threshold)
    env.expect(debug_cmd(), 'DUMP_INVIDX', 'idx', 'hello').equal(debug_rep)

    # Force invoke bypasses threshold (uses force=true) and cleans the deleted entries
    forceInvokeGC(env, 'idx')
    env.expect(debug_cmd(), 'DUMP_INVIDX', 'idx', 'hello').equal([10])


# Regression for [MOD-15940](https://redislabs.atlassian.net/browse/MOD-15940)
@skip(cluster=True)
def testForkGCEmptyTagSuffixTrieMap_emptyValue_MOD_15996():
    env = Env(moduleArgs='GC_POLICY FORK FORK_GC_CLEAN_THRESHOLD 0')
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               't', 'TAG', 'INDEXEMPTY', 'WITHSUFFIXTRIE').ok()
    waitForIndex(env, 'idx')
    env.cmd('HSET', 'doc1', 't', '')
    env.expect('DEL', 'doc1').equal(1)
    forceInvokeGC(env, 'idx')
    env.expect('PING').equal(True)


@skip(cluster=True)
def testForkGCEmptyTextSuffixTrie_emptyValue():
    env = Env(moduleArgs='GC_POLICY FORK FORK_GC_CLEAN_THRESHOLD 0')
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               't', 'TEXT', 'INDEXEMPTY', 'WITHSUFFIXTRIE').ok()
    waitForIndex(env, 'idx')
    env.cmd('HSET', 'doc1', 't', '')
    env.expect('DEL', 'doc1').equal(1)
    forceInvokeGC(env, 'idx')
    env.expect('PING').equal(True)


# Periodic-GC scheduling tests. They assert on the per-index gc_stats cycle counter, and the
# ones that need to act on a run already in flight park it on the GCTaskStart sync point --
# the only way to hold a GC worker, and so the only way to drive the scheduling guards against
# a real in-flight run instead of racing one.
SYNC_POINT_GC_TASK_START = 'GCTaskStart'  # src/debug_commands.h
SYNC_POINT_GC_BEFORE_REARM_POST = 'GCBeforeRearmPost'  # src/debug_commands.h
SYNC_POINT_GC_BEFORE_FORK = 'GCBeforeFork'  # src/debug_commands.h
SYNC_POINT_GC_FORK_SLOT_BUSY = 'GCForkSlotBusy'  # src/debug_commands.h


def gcCycles(env, idx):
    return int(to_dict(index_info(env, idx)['gc_stats'])['total_cycles'])


def waitForGCCycles(env, idx, base, count=1, timeout=60):
    """Wait for `idx` to complete `count` cycles past `base`, returning the counter value that
    was actually observed. Callers assert on that value rather than re-reading: a re-read can
    pick up a later timer-driven cycle and turn an exact-count assertion into a flake."""
    seen = {}

    def check():
        seen['total_cycles'] = gcCycles(env, idx)
        return seen['total_cycles'] >= base + count, {'index': idx, 'base': base, **seen}

    wait_for_condition(check, f'GC stopped collecting for {idx}', timeout=timeout)
    return seen['total_cycles']


def armParkedGCRun(env, point=SYNC_POINT_GC_TASK_START):
    """Park the next periodic GC job on `point`, returning once it is parked -- so the caller
    knows RUN_PENDING is held and that index has no timer armed. At GCTaskStart the pass has not
    started yet; at GCBeforeRearmPost it has finished and only its re-arm is still owed."""
    env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
    # Auto-release after 10s: a stalled runner must not leave a GC worker parked for the rest of
    # the file, since the GC pool has a single thread and every other test here shares it.
    env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', point, 10000).ok()
    wait_for_condition(
        lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', point) == 1, {}),
        f'Timeout waiting for a GC job to reach {point}')


@skip(cluster=True)
def testGCPeriodicScheduleStopAndResume():
    """The periodic GC re-arms itself, stops on GC_STOP_SCHEDULE, and resumes on
    GC_CONTINUE_SCHEDULE. Almost every other GC test drives collection with FORCE_INVOKE,
    which never re-arms, so nothing else notices if the chain stops after its first pass."""
    # No garbage on purpose: a GC that finds nothing still has to re-arm. CLEAN_THRESHOLD 0
    # keeps those empty passes from short-circuiting ahead of the total_cycles counter.
    env = Env(redisConfigFile=thread_gc_conf(run_interval=1, clean_threshold=0))
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    # A second index nobody stops, used to time the negative window below instead of sleeping.
    # It is what keeps that window from passing for the wrong reason: a runner slow enough to
    # delay 'idx' past a fixed sleep delays 'ctl' just as much, so the window stretches with
    # the host rather than expiring before the timer it is meant to catch.
    env.expect('FT.CREATE', 'ctl', 'SCHEMA', 't', 'TEXT').ok()

    # Two cycles, not one: the second is the one that can only come from the chain re-arming.
    waitForGCCycles(env, 'idx', gcCycles(env, 'idx'), 2)

    env.expect(debug_cmd(), 'GC_STOP_SCHEDULE', 'idx').ok()
    env.cmd(debug_cmd(), 'GC_WAIT_FOR_JOBS')  # let the run already in flight finish
    stopped_at = gcCycles(env, 'idx')
    # Two cycles of the control index: direct evidence that GC timers did fire during this
    # window, so a flat counter on 'idx' can only mean its own scheduling stopped.
    waitForGCCycles(env, 'ctl', gcCycles(env, 'ctl'), 2)
    after_stop = gcCycles(env, 'idx')
    env.assertEqual(after_stop, stopped_at,
                    message=f'GC ran after GC_STOP_SCHEDULE: {stopped_at} -> {after_stop}')

    env.expect(debug_cmd(), 'GC_CONTINUE_SCHEDULE', 'idx').ok()
    waitForGCCycles(env, 'idx', stopped_at)


@skip(cluster=True)
def testGCScheduleCommandsOnIndexWithoutGC(env):
    """A TEMPORARY index gets no GCContext, and the GC commands must reject it rather than
    dereference the missing context. NOGC reaches the same branch in IndexSpec_StartGC."""
    env.expect('FT.CREATE', 'tmp_idx', 'TEMPORARY', 3600, 'SCHEMA', 't', 'TEXT').ok()

    for sub in ('GC_STOP_SCHEDULE', 'GC_CONTINUE_SCHEDULE', 'GC_FORCEBGINVOKE',
                'GC_FORCEINVOKE'):
        env.expect(debug_cmd(), sub, 'tmp_idx').error().contains(
            'GC is not available for this index')

    # A normal index in the same server still works, so the guard is not over-broad.
    env.expect('FT.CREATE', 'idx2', 'SCHEMA', 't', 'TEXT').ok()
    env.expect(debug_cmd(), 'GC_STOP_SCHEDULE', 'idx2').ok()
    env.expect(debug_cmd(), 'GC_CONTINUE_SCHEDULE', 'idx2').ok()


@skip(cluster=True)
def testGCStopScheduleDuringRun():
    """GC_STOP_SCHEDULE while a run is in flight: the completing run's own re-arm has to notice
    that collection was disabled under it. Nothing disarms a timer armed from there, so the
    chain would outlive the stop -- and after a drop it would arm one over a freed context."""
    env = Env(redisConfigFile=thread_gc_conf(run_interval=1, clean_threshold=0))
    skipIfNoEnableAssert(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()

    armParkedGCRun(env)
    before = gcCycles(env, 'idx')
    env.expect(debug_cmd(), 'GC_STOP_SCHEDULE', 'idx').ok()

    # Release the parked run into its re-arm, which now has to decline. SIGNAL also disarms the
    # sync point, so nothing parks again.
    env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', SYNC_POINT_GC_TASK_START)
    stopped_at = waitForGCCycles(env, 'idx', before, timeout=30)
    env.assertEqual(stopped_at, before + 1,
                    message=f'more than the parked run completed: {before} -> {stopped_at}')

    # Created after the sync point was disarmed, so this one never parks. Its cycles time the
    # window below -- evidence that GC timers fired in it while 'idx' stayed flat.
    env.expect('FT.CREATE', 'ctl', 'SCHEMA', 't', 'TEXT').ok()
    waitForGCCycles(env, 'ctl', gcCycles(env, 'ctl'), 2)
    after_stop = gcCycles(env, 'idx')
    env.assertEqual(after_stop, stopped_at,
                    message=f'the completing run re-armed despite the stop: '
                            f'{stopped_at} -> {after_stop}')
    env.expect('PING').equal(True)

@skip(cluster=True)
def testForcedGCFailsWhileARunOfTheIndexIsInFlight():
    """A forced run never overlaps another run of its index: while one is in flight it fails
    at once instead of waiting (a background one included, rather than being dropped), and works
    again once that run has finished."""
    env = Env(redisConfigFile=thread_gc_conf(run_interval=1, clean_threshold=0))
    skipIfNoEnableAssert(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()

    armParkedGCRun(env)
    before = gcCycles(env, 'idx')
    env.expect(debug_cmd(), 'GC_FORCEINVOKE', 'idx').error().contains(GC_RUN_IN_PROGRESS)
    env.expect(debug_cmd(), 'GC_FORCEBGINVOKE', 'idx').error().contains(GC_RUN_IN_PROGRESS)

    env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', SYNC_POINT_GC_TASK_START)
    waitForGCCycles(env, 'idx', before)
    forceInvokeGC(env, 'idx')
    env.assertGreaterEqual(gcCycles(env, 'idx'), before + 2)


# Regression for MOD-17309: the re-arm moved off the GC thread onto a main-thread one-shot.
@skip(cluster=True)
def testGCKeepsReschedulingItself_MOD_17309():
    """Every arm goes through the main-thread one-shot, and the chain sustains itself."""
    env = Env(redisConfigFile=thread_gc_conf(run_interval=1))
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()

    def arms(which):
        return int(env.cmd(debug_cmd(), 'GC_TIMER_ARMS', which))

    # Reset so the one main-thread arm from GCContext_Start at index creation is not counted.
    env.expect(debug_cmd(), 'GC_TIMER_ARMS', 'RESET').ok()

    def total():
        return arms('TOTAL')

    wait_for_condition(lambda: (total() >= 3, {'arms': total()}),
                       'GC stopped rescheduling itself', timeout=60)

    # Freeze the chain first: the two counters are read in separate round trips, so an arm
    # landing between them would fail this even though every arm went through the one-shot.
    env.expect(debug_cmd(), 'GC_STOP_SCHEDULE', 'idx').ok()
    env.cmd(debug_cmd(), 'GC_WAIT_FOR_JOBS')
    total_arms, oneshot_arms = arms('TOTAL'), arms('FROM_ONESHOT')
    env.assertEqual(total_arms, oneshot_arms,
                    message=f'a timer arm bypassed the main-thread one-shot: '
                            f'{total_arms} total vs {oneshot_arms} from one-shot')


@skip(cluster=True)
def testGCContinueScheduleDuringRun_MOD_17309():
    """GC_CONTINUE_SCHEDULE must not queue a second run while one is already in flight: two
    runs behind one RUN_PENDING bit let the first one's tail arm a timer while the second is
    still queued, and a second run that finds the index dropped then frees the GC under it."""
    env = Env(redisConfigFile=thread_gc_conf(run_interval=1, clean_threshold=0))
    skipIfNoEnableAssert(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()

    armParkedGCRun(env)
    before = gcCycles(env, 'idx')
    env.expect(debug_cmd(), 'GC_STOP_SCHEDULE', 'idx').ok()
    env.expect(debug_cmd(), 'GC_CONTINUE_SCHEDULE', 'idx').ok()
    # Whether CONTINUE queued a duplicate was already decided above. Stopping again before the
    # parked run is released leaves it unable to re-arm, so no timer can fire during the
    # assertion below -- a second cycle can then only mean a duplicate run.
    env.expect(debug_cmd(), 'GC_STOP_SCHEDULE', 'idx').ok()

    # Release the parked run. SIGNAL also disarms, so nothing parks again.
    env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', SYNC_POINT_GC_TASK_START)

    # GC_WAIT_FOR_JOBS returns once the GC pool has nothing queued or running, so it drains the
    # parked run and any duplicate queued behind it: no wall-clock window a stray cycle could
    # slip past.
    env.cmd(debug_cmd(), 'GC_WAIT_FOR_JOBS')
    completed = gcCycles(env, 'idx')
    env.assertEqual(completed, before + 1,
                    message=f'GC_CONTINUE_SCHEDULE queued a duplicate run: '
                            f'{before} -> {completed}')
    env.expect('PING').equal(True)


def queuedRunKeepsItsCleanThreshold(env, raiseThreshold, live_docs=0):
    """A run checks the clean threshold and ratio the index had when the run was queued. Raising
    either while the run waits reaches only the runs queued after the change. `env` runs the GC
    every second with a threshold of 0; `raiseThreshold` raises the threshold, or the ratio of
    the `live_docs` the index holds, above what is deleted."""
    skipIfNoEnableAssert(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    for i in range(live_docs):
        env.cmd('HSET', f'doc{i}', 't', 'hello')

    def oneShotArms():
        return int(env.cmd(debug_cmd(), 'GC_TIMER_ARMS', 'FROM_ONESHOT'))

    armParkedGCRun(env)  # queued with threshold 0
    before = gcCycles(env, 'idx')
    raiseThreshold()
    arms = oneShotArms()
    env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', SYNC_POINT_GC_TASK_START)

    # Nothing was deleted, so only the bar of 0 it was queued with lets the parked run collect.
    collected = waitForGCCycles(env, 'idx', before, timeout=30)
    env.assertEqual(collected, before + 1)

    # Each finished run re-arms once, so two more arms mean a run queued after the change has
    # finished too -- and it skipped, under the new threshold.
    wait_for_condition(lambda: (oneShotArms() >= arms + 2, {'arms': oneShotArms()}),
                       'GC stopped rescheduling itself', timeout=60)
    env.assertEqual(gcCycles(env, 'idx'), collected,
                    message='a run queued after the change collected below the new threshold')


@skip(cluster=True)
def testGCQueuedRunKeepsItsCleanThresholdFork():
    env = Env(moduleArgs='GC_POLICY FORK FORK_GC_RUN_INTERVAL 1 FORK_GC_CLEAN_THRESHOLD 0')
    queuedRunKeepsItsCleanThreshold(
        env, lambda: env.expect(config_cmd(), 'SET', 'FORK_GC_CLEAN_THRESHOLD', 1000).ok())


@skip(cluster=True)
def testGCQueuedRunKeepsItsCleanThresholdThread():
    env = Env(moduleArgs='GC_POLICY THREAD',
              redisConfigFile=thread_gc_conf(run_interval=1, clean_threshold=0))
    queuedRunKeepsItsCleanThreshold(
        env, lambda: env.expect('CONFIG', 'SET', 'search-thread-gc-clean-threshold', 1000).ok())


@skip(cluster=True)
def testGCQueuedRunKeepsItsCleanRatioFork():
    env = Env(moduleArgs='GC_POLICY FORK FORK_GC_RUN_INTERVAL 1 FORK_GC_CLEAN_THRESHOLD 0')
    queuedRunKeepsItsCleanThreshold(
        env, lambda: alterGC(env, 'idx', 'CLEAN_RATIO_PPM', 1_000_000).ok(), live_docs=5)


@skip(cluster=True)
def testGCQueuedRunKeepsItsCleanRatioThread():
    env = Env(moduleArgs='GC_POLICY THREAD',
              redisConfigFile=thread_gc_conf(run_interval=1, clean_threshold=0))
    queuedRunKeepsItsCleanThreshold(
        env, lambda: alterGC(env, 'idx', 'CLEAN_RATIO_PPM', 1_000_000).ok(), live_docs=5)


@skip(cluster=True)
def testGCTerminatesAfterDropWhileStopped(env):
    """A GC stopped by GC_STOP_SCHEDULE still has to terminate when its index is dropped.
    IndexSpec_Free does not free the GCContext -- it waits for a run to discover the drop --
    and a stopped GC has neither a timer nor a queued run, so nothing ever would. The context
    and its detached thread-safe context would leak for the life of the process.

    onTerm is what returns this index's logically-deleted docs to the global counter, so the
    counter dropping back to 0 is evidence the GC actually terminated."""
    logically_deleted = lambda: env.cmd('INFO', 'MODULES')['search_gc_total_docs_not_collected']

    # The GC INFO section is only rendered while at least one index exists, so keep one that is
    # never dropped -- otherwise the counter we assert on disappears just as it reaches 0. Give
    # both a prefix so only 'idx' indexes the document, and the count is unambiguously its own.
    env.expect('FT.CREATE', 'keep', 'PREFIX', 1, 'keep:', 'SCHEMA', 't', 'TEXT').ok()
    env.expect('FT.CREATE', 'idx', 'PREFIX', 1, 'doc:', 'SCHEMA', 't', 'TEXT').ok()
    env.expect(debug_cmd(), 'GC_STOP_SCHEDULE', 'idx').ok()
    env.cmd('HSET', 'doc:1', 't', 'hello')
    env.expect('DEL', 'doc:1').equal(1)
    env.assertEqual(logically_deleted(), 1)

    env.expect('FT.DROPINDEX', 'idx').ok()
    wait_for_condition(lambda: (logically_deleted() == 0, {'not_collected': logically_deleted()}),
                       'stopped GC never terminated after its index was dropped', timeout=30)


# The GC INFO section is suppressed once no index is left, so both tests below keep one index
# that is never dropped. It is created *after* the park so it cannot be the index that parked --
# the GC pool has a single thread, and nothing says which context's job reaches a sync point
# first -- and it is prefixed so the counter stays unambiguously the dropped index's.
def keepGCInfoSectionAlive(env):
    env.expect('FT.CREATE', 'keep', 'PREFIX', 1, 'keep:', 'SCHEMA', 't', 'TEXT').ok()


def logicallyDeletedDocs(env):
    return env.cmd('INFO', 'MODULES')['search_gc_total_docs_not_collected']


@skip(cluster=True)
def testGCTeardownAfterContinueAndDrop_MOD_17309():
    """A stop, a resume and a drop that all land while a run is parked must end in teardown. The
    drop finds RUN_PENDING held and hands the discovery to that run, so the parked pass is the
    only thing left that can free the context -- and onTerm returning this index's
    logically-deleted docs to the global counter is the evidence that it did."""
    env = Env(redisConfigFile=thread_gc_conf(run_interval=1, clean_threshold=0))
    skipIfNoEnableAssert(env)
    env.expect('FT.CREATE', 'idx', 'PREFIX', 1, 'doc:', 'SCHEMA', 't', 'TEXT').ok()

    armParkedGCRun(env)
    keepGCInfoSectionAlive(env)

    # Deleted while the pass is parked before its promote, so it cannot be collected: that pass
    # never reaches the fork, and no other run can start while it holds RUN_PENDING. Only
    # teardown can return the counter to 0.
    env.cmd('HSET', 'doc:1', 't', 'hello')
    env.expect('DEL', 'doc:1').equal(1)
    env.assertEqual(logicallyDeletedDocs(env), 1)

    # Arranged while the worker is still parked -- not raced against it. GC_CONTINUE_SCHEDULE
    # must not queue a second run behind the parked one, and the drop must not disarm anything.
    env.expect(debug_cmd(), 'GC_STOP_SCHEDULE', 'idx').ok()
    env.expect(debug_cmd(), 'GC_CONTINUE_SCHEDULE', 'idx').ok()
    env.expect('FT.DROPINDEX', 'idx').ok()
    env.assertEqual(logicallyDeletedDocs(env), 1,
                    message='the drop returned the docs on its own, so the wait below proves nothing')

    # Release it into a promote that now fails: the pass has to terminate instead of re-arming.
    env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', SYNC_POINT_GC_TASK_START)
    wait_for_condition(
        lambda: (logicallyDeletedDocs(env) == 0, {'not_collected': logicallyDeletedDocs(env)}),
        'the parked run never tore down its dropped index', timeout=30)
    env.expect('FT._LIST').equal(['keep'])


@skip(cluster=True)
def testGCTeardownWhenDropOvertakesPendingRearm_MOD_17309():
    """A stop and a drop that land after the pass finished, while only its re-arm one-shot is
    still owed, must still end in teardown. GCContext_BeginDrop defers to "a run in flight",
    which by then is just that pending post -- so the one-shot has to be able to arm the timer
    that discovers the unlink, which is why the drop re-enables collection before deciding."""
    env = Env(redisConfigFile=thread_gc_conf(run_interval=1, clean_threshold=0))
    skipIfNoEnableAssert(env)
    env.expect('FT.CREATE', 'idx', 'PREFIX', 1, 'doc:', 'SCHEMA', 't', 'TEXT').ok()

    armParkedGCRun(env, SYNC_POINT_GC_BEFORE_REARM_POST)
    keepGCInfoSectionAlive(env)

    # Deleted after that pass finished, so this pass cannot have collected it and no other run
    # can start while it holds RUN_PENDING: only teardown can return the counter to 0.
    env.cmd('HSET', 'doc:1', 't', 'hello')
    env.expect('DEL', 'doc:1').equal(1)
    env.assertEqual(logicallyDeletedDocs(env), 1)

    env.expect(debug_cmd(), 'GC_STOP_SCHEDULE', 'idx').ok()
    env.expect('FT.DROPINDEX', 'idx').ok()
    env.assertEqual(logicallyDeletedDocs(env), 1,
                    message='the drop returned the docs on its own, so the wait below proves nothing')

    # Let the post through. It re-arms on the main thread, and that timer's run is what has to
    # find the index gone and free the context.
    env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', SYNC_POINT_GC_BEFORE_REARM_POST)
    wait_for_condition(
        lambda: (logicallyDeletedDocs(env) == 0, {'not_collected': logicallyDeletedDocs(env)}),
        'a drop that overtook the pending re-arm never tore the context down', timeout=30)
    env.expect('FT._LIST').equal(['keep'])


# The run interval is read when a timer is armed, on the main thread. These start from a run
# interval of 1000s, so a second cycle or arm inside the test can only come from a new value.
# GC_STOP_SCHEDULE / GC_CONTINUE_SCHEDULE stand in for the waiting timer firing: CONTINUE queues
# a scheduled run at once, so no test waits out the old period.
def oneShotArms(env):
    return int(env.cmd(debug_cmd(), 'GC_TIMER_ARMS', 'FROM_ONESHOT'))


@skip(cluster=True)
def testGCIdleIndexPicksUpNewRunInterval():
    """An index below the clean threshold never forks, and used to keep the run interval it had
    when it last forked -- or was created -- however often the global value changed."""
    env = Env(moduleArgs='GC_POLICY FORK FORK_GC_RUN_INTERVAL 1000 FORK_GC_CLEAN_THRESHOLD 1000')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    env.expect(debug_cmd(), 'GC_STOP_SCHEDULE', 'idx').ok()
    env.expect(config_cmd(), 'SET', 'FORK_GC_RUN_INTERVAL', 1).ok()
    try:
        env.expect(debug_cmd(), 'GC_TIMER_ARMS', 'RESET').ok()
        # Skipped cycles leave total_cycles alone, so count re-arms instead. The first follows
        # the run CONTINUE queued; the next two need timers armed with the new interval.
        env.expect(debug_cmd(), 'GC_CONTINUE_SCHEDULE', 'idx').ok()
        wait_for_condition(lambda: (oneShotArms(env) >= 3, {'arms': oneShotArms(env)}),
                           'the idle index kept its old run interval', timeout=30)
    finally:
        env.expect(config_cmd(), 'SET', 'FORK_GC_RUN_INTERVAL', 1000).ok()


@skip(cluster=True)
def testGCRearmReadsIntervalChangedWhileRearmIsPending():
    """A change that lands after a pass finished but before its re-arm drains reaches that
    re-arm. The pass used to capture the interval before posting the re-arm."""
    env = Env(moduleArgs='GC_POLICY FORK FORK_GC_RUN_INTERVAL 1000 FORK_GC_CLEAN_THRESHOLD 0')
    skipIfNoEnableAssert(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    env.expect(debug_cmd(), 'GC_STOP_SCHEDULE', 'idx').ok()
    env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
    env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', SYNC_POINT_GC_BEFORE_REARM_POST, 10000).ok()
    env.expect(debug_cmd(), 'GC_CONTINUE_SCHEDULE', 'idx').ok()
    waitForSyncPoint(env, SYNC_POINT_GC_BEFORE_REARM_POST,
                     'Timeout waiting for the GC run to park before its re-arm')
    parked = gcCycles(env, 'idx')  # the parked pass is already counted

    env.expect(config_cmd(), 'SET', 'FORK_GC_RUN_INTERVAL', 1).ok()
    try:
        env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', SYNC_POINT_GC_BEFORE_REARM_POST)
        waitForGCCycles(env, 'idx', parked, 2, timeout=30)
    finally:
        env.expect(config_cmd(), 'SET', 'FORK_GC_RUN_INTERVAL', 1000).ok()


@skip(cluster=True)
def testGCScheduledForkFailureRetriesAtRetryInterval():
    """A scheduled cycle that cannot fork arms with search-fork-gc-retry-interval, not the run
    interval, until a fork succeeds -- read when the retry timer is armed. The retry interval
    changes after the fork failed and before its re-arm drains, where the fork-failure path
    used to fix it."""
    env = Env(moduleArgs='GC_POLICY FORK FORK_GC_RUN_INTERVAL 1000 '
                         'FORK_GC_RETRY_INTERVAL 1000 FORK_GC_CLEAN_THRESHOLD 0')
    skipIfNoEnableAssert(env)
    indexWithCollectableDocs(env)
    base = gcCycles(env, 'idx')
    env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
    env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', SYNC_POINT_GC_BEFORE_FORK, 10000).ok()
    env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', SYNC_POINT_GC_BEFORE_REARM_POST, 10000).ok()
    try:
        # The scheduled run parks before its fork, so the bgsave holds the slot when it tries.
        env.expect(debug_cmd(), 'GC_CONTINUE_SCHEDULE', 'idx').ok()
        waitForSyncPoint(env, SYNC_POINT_GC_BEFORE_FORK,
                         'Timeout waiting for the scheduled GC run to park before its fork')
        # 50 live keys at 0.1s each hold the slot for about 5s, several retries' worth.
        holdForkSlot(env, 100000)
        env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', SYNC_POINT_GC_BEFORE_FORK)
        waitForSyncPoint(env, SYNC_POINT_GC_BEFORE_REARM_POST,
                         'Timeout waiting for the failed GC run to park before its re-arm')
        env.assertEqual(gcCycles(env, 'idx'), base,
                        message='the scheduled fork did not fail, so nothing below tests the retry')

        env.expect(config_cmd(), 'SET', 'FORK_GC_RETRY_INTERVAL', 1).ok()
        env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', SYNC_POINT_GC_BEFORE_REARM_POST)
        # Once the bgsave ends, only a 1s retry timer brings the next cycle within the timeout.
        waitForGCCycles(env, 'idx', base, timeout=60)
    finally:
        # The bgsave keeps the delay it was forked with, so an early failure still has to wait
        # it out before the next test.
        env.cmd('CONFIG', 'SET', 'rdb-key-save-delay', '0')
        waitForRdbSaveToFinish(env)
        env.expect(config_cmd(), 'SET', 'FORK_GC_RETRY_INTERVAL', 1000).ok()


@skip(cluster=True)
def testGCTeardownWhenAForcedRunFindsTheIndexDroppedUnderAnArmedTimer():
    """A forced run queued while the periodic timer is armed, and released only after its index
    is dropped, frees the GC context once its promote fails. The timer must not survive that:
    the forced run disarms it when it is queued, so nothing fires on the freed context later."""
    # Long enough that the new index's first timer cannot fire before the forced run is queued.
    env = Env(redisConfigFile=thread_gc_conf(run_interval=2, clean_threshold=0))
    skipIfNoEnableAssert(env)
    env.expect('FT.CREATE', 'keep', 'PREFIX', 1, 'keep:', 'SCHEMA', 't', 'TEXT').ok()
    # Only the forced run below may reach the sync point.
    env.expect(debug_cmd(), 'GC_STOP_SCHEDULE', 'keep').ok()
    env.cmd(debug_cmd(), 'GC_WAIT_FOR_JOBS')
    env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
    env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', SYNC_POINT_GC_TASK_START, 10000).ok()

    # Creating the index arms its timer; the forced run is queued with that timer armed, and
    # parks before its promote.
    env.expect('FT.CREATE', 'idx', 'PREFIX', 1, 'doc:', 'SCHEMA', 't', 'TEXT').ok()
    env.cmd('HSET', 'doc:1', 't', 'hello')
    env.expect('DEL', 'doc:1').equal(1)
    env.expect(debug_cmd(), 'GC_FORCEBGINVOKE', 'idx').ok()
    waitForSyncPoint(env, SYNC_POINT_GC_TASK_START, 'the forced run never parked')
    env.assertEqual(logicallyDeletedDocs(env), 1)

    env.expect('FT.DROPINDEX', 'idx').ok()
    env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', SYNC_POINT_GC_TASK_START)
    wait_for_condition(
        lambda: (logicallyDeletedDocs(env) == 0, {'not_collected': logicallyDeletedDocs(env)}),
        'the forced run never tore down its dropped index', timeout=30)

    # Two cycles of an index created after the timer was armed: evidence that timer's period has
    # passed, so had it survived it would have fired on the freed context by now.
    env.expect('FT.CREATE', 'ctl', 'PREFIX', 1, 'ctl:', 'SCHEMA', 't', 'TEXT').ok()
    waitForGCCycles(env, 'ctl', gcCycles(env, 'ctl'), 2)
    env.expect('PING').equal(True)
    env.assertEqual(logicallyDeletedDocs(env), 0, message='the context was torn down twice')
    env.expect('FT._LIST').apply(sorted).equal(['ctl', 'keep'])


def waitForSyncPoint(env, name, message, timeout=30):
    wait_for_condition(
        lambda: (env.cmd(debug_cmd(), 'SYNC_POINT', 'IS_WAITING', name) == 1, {}),
        message, timeout=timeout)


def indexWithCollectableDocs(env):
    """An index with 50 deleted docs to collect and 50 live keys. The live half is what lets a
    delayed bgsave keep holding the fork slot, and GC_STOP_SCHEDULE leaves a forced cycle as
    the only one that can reach the sync points below."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    env.expect(debug_cmd(), 'GC_STOP_SCHEDULE', 'idx').ok()
    for i in range(100):
        env.expect('HSET', f'doc{i}', 't', 'hello').equal(1)
    for i in range(50):
        env.expect('DEL', f'doc{i}').equal(1)
    waitForRdbSaveToFinish(env)


def parkedForcedGC(env, timeout=None):
    """Start GC_FORCEINVOKE, which blocks, on its own connection, and return once its cycle is
    parked before the fork -- so the caller can take the slot out from under it. The returned
    dict gains 'reply' or 'error' once the thread finishes."""
    conn = env.getConnection()
    result = {}
    args = [debug_cmd(), 'GC_FORCEINVOKE', 'idx'] + ([timeout] if timeout is not None else [])

    def run():
        try:
            result['reply'] = conn.execute_command(*args)
        except Exception as e:
            result['error'] = str(e)

    thread = threading.Thread(target=run)
    thread.start()
    waitForSyncPoint(env, SYNC_POINT_GC_BEFORE_FORK,
                     'Timeout waiting for the forced GC to reach the pre-fork sync point')
    return thread, result


def holdForkSlot(env, key_delay_us):
    """Take Redis's single child slot with a bgsave slowed to key_delay_us per key saved."""
    env.expect('CONFIG', 'SET', 'rdb-key-save-delay', str(key_delay_us)).ok()
    env.cmd('BGSAVE')
    env.assertEqual(int(env.cmd('INFO', 'Persistence')['rdb_bgsave_in_progress']), 1)


# Fork-only: the thread GC policy never takes the fork slot.
@skip(cluster=True)
def testForcedGCWaitsForForkSlot():
    """A forced GC replying DONE has to mean it collected. Parking the cycle before its fork lets
    a bgsave take the only child slot first, and parking it again after the failed fork is what
    proves the EEXIST retry ran rather than the cycle winning the slot on a lucky schedule."""
    env = Env(moduleArgs='GC_POLICY FORK')
    skipIfNoEnableAssert(env)
    indexWithCollectableDocs(env)
    env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
    env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', SYNC_POINT_GC_BEFORE_FORK, 10000).ok()
    env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', SYNC_POINT_GC_FORK_SLOT_BUSY, 10000).ok()

    thread, forced = parkedForcedGC(env)
    try:
        holdForkSlot(env, 20000)
        env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', SYNC_POINT_GC_BEFORE_FORK)

        # The cycle only parks here after a fork failed with EEXIST, so arriving is the
        # assertion that the retry path ran.
        waitForSyncPoint(env, SYNC_POINT_GC_FORK_SLOT_BUSY,
                         'forced GC never hit a busy fork slot')
        env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', SYNC_POINT_GC_FORK_SLOT_BUSY)
    finally:
        thread.join(timeout=60)
        env.cmd('CONFIG', 'SET', 'rdb-key-save-delay', '0')

    env.assertFalse(thread.is_alive(), message='forced GC never returned')
    env.assertEqual(forced.get('reply'), 'DONE')
    env.assertGreater(int(to_dict(index_info(env)['gc_stats'])['bytes_collected']), 0)


# Fork-only: the thread GC policy never takes the fork slot.
@skip(cluster=True)
def testForcedGCReportsWhenItCannotFork():
    """A forced GC that never gets the slot has to say so. Replying DONE would be the original
    bug at a longer timescale: a collection reported but never made."""
    env = Env(moduleArgs='GC_POLICY FORK')
    skipIfNoEnableAssert(env)
    indexWithCollectableDocs(env)
    env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
    env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', SYNC_POINT_GC_BEFORE_FORK, 10000).ok()

    # The budget is this TIMEOUT less a margin, and both clocks start at submission -- so the
    # margin, not the timeout, is what this test's own setup races. TIMEOUT has to stay clear of
    # how long that setup can take on a slow host; the budget then expires while the child below
    # is still holding the slot.
    thread, forced = parkedForcedGC(env, timeout=2000)
    try:
        holdForkSlot(env, 40000)
        env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', SYNC_POINT_GC_BEFORE_FORK)
    finally:
        thread.join(timeout=60)
        env.cmd('CONFIG', 'SET', 'rdb-key-save-delay', '0')
        waitForRdbSaveToFinish(env)

    env.assertContains('GC_NO_FORK', forced.get('error', ''),
                       message=f'expected an error, got {forced}')


@skip(cluster=True, redis_less_than='7.2')  # 7.2 added eventloop_cycles
@env_spec(moduleArgs='GC_POLICY FORK FORK_GC_RUN_INTERVAL 2147484')  # the policy this interval sets
def testLongGCIntervalKeepsTheEventLoopIdle(env):
    """Redis truncates the ms until the next module timer to an `int` (MOD-21060). A GC timer
    armed past INT_MAX ms, left as the earliest module timer once another one fires, made the
    event loop wake on every pass -- 100% CPU on an idle server. Every delay this interval can
    draw truncates negative, but the loop only spins on a host up for longer than that negative
    value in ms (up to 24.86 days), since ae's clock counts from boot; on a younger host the
    timer stalls instead. So this catches a regression on long-lived hosts only, and
    GCTimerDelayTest in the C++ tests is the guard everywhere."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    # The other module timer: its expiry leaves idx's GC timer as the earliest one.
    env.expect('FT.CREATE', 'tmp', 'TEMPORARY', 1, 'SCHEMA', 't', 'TEXT').ok()
    wait_for_condition(lambda: ('tmp' not in env.cmd('FT._LIST'), {}),
                       'TEMPORARY index never expired', timeout=30)

    cycles = lambda: env.cmd('INFO', 'stats')['eventloop_cycles']
    before = cycles()
    time.sleep(1)
    spent = cycles() - before
    # An idle second takes about hz (10) cycles; a spinning loop takes millions.
    env.assertLess(spent, 1000, message=f'{spent} event-loop cycles in an idle second')


# ---------------------------------------------------------------------------------------------
# FT.ALTER <index> GC SET (MOD-20263). GC_TIMER_INFO reports the interval a timer was armed with,
# and GC_TIMER_ARMS TOTAL counts arms, so a restart -- or its absence -- is checked without
# waiting out a period. Most tests start from global values of 1000, so nothing fires on its own
# while they run and any shorter interval can only come from an override.

GC_MAX_INTERVAL_SEC = (2**63 - 1) // 4 // 1000 // 1000  # src/gc/gc.h

GC_GLOBAL_CONFIGS = {
    'FORK': ('search-fork-gc-run-interval', 'search-fork-gc-clean-threshold',
             'search-fork-gc-clean-ratio-ppm'),
    'THREAD': ('search-thread-gc-run-interval', 'search-thread-gc-clean-threshold',
               'search-gc-clean-ratio-ppm'),
}


def gcPolicyEnv(policy, run_interval=1000, clean_threshold=1000, **kwargs):
    """An env running `policy` with these global values for it."""
    if policy == 'FORK':
        return Env(moduleArgs=f'GC_POLICY FORK FORK_GC_RUN_INTERVAL {run_interval} '
                              f'FORK_GC_CLEAN_THRESHOLD {clean_threshold}', **kwargs)
    return Env(moduleArgs='GC_POLICY THREAD',
               redisConfigFile=thread_gc_conf(run_interval, clean_threshold), **kwargs)


def setGlobalGC(env, policy, run_interval=None, clean_threshold=None, clean_ratio_ppm=None):
    names = GC_GLOBAL_CONFIGS[policy]
    for name, value in zip(names, (run_interval, clean_threshold, clean_ratio_ppm)):
        if value is not None:
            verify_command_OK_on_all_shards(env, 'CONFIG', 'SET', name, value)


def otherPolicy(policy):
    return 'THREAD' if policy == 'FORK' else 'FORK'


def gcTimer(conn, idx):
    """FT.DEBUG GC_TIMER_INFO as a dict; `conn` is an env or a shard connection."""
    run = conn.cmd if hasattr(conn, 'cmd') else conn.execute_command
    reply = run(debug_cmd(), 'GC_TIMER_INFO', idx)
    return {reply[i]: reply[i + 1] for i in range(0, len(reply), 2)}


def timerArms(env):
    return int(env.cmd(debug_cmd(), 'GC_TIMER_ARMS', 'TOTAL'))


def assertArmedWith(env, idx, seconds, conn=None, message=''):
    """`idx` waits on a run-interval timer armed for `seconds`, with no more than the period
    and its jitter left."""
    timer = gcTimer(conn or env, idx)
    env.assertEqual(timer.get('type'), 'run_interval', message=f'{message}: {timer}')
    env.assertEqual(timer.get('interval_ms'), seconds * 1000, message=f'{message}: {timer}')
    env.assertLessEqual(timer.get('remaining_ms', 0), 2 * seconds * 1000,
                        message=f'{message}: {timer}')


def waitForRearm(env, idx, message):
    wait_for_condition(lambda: (gcTimer(env, idx).get('type') != 'none', gcTimer(env, idx)),
                       message, timeout=30)


def runScheduledCycle(env, idx):
    """Queue a scheduled run of `idx` now, as its timer firing would, and return once it has
    finished and re-armed. GC_CONTINUE_SCHEDULE queues the run at once; its re-arm is the only
    way back to a live timer, so the wait cannot pass before the run is done."""
    env.expect(debug_cmd(), 'GC_STOP_SCHEDULE', idx).ok()
    env.expect(debug_cmd(), 'GC_CONTINUE_SCHEDULE', idx).ok()
    waitForRearm(env, idx, f'the scheduled run of {idx} never re-armed')


def alterGC(env, idx, *settings):
    return env.expect('FT.ALTER', idx, 'GC', 'SET', *settings)


@skip(cluster=True)
def testGCAlterSyntax():
    env = gcPolicyEnv('FORK')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()

    # Any order and case, and the six-word form.
    alterGC(env, 'idx', 'RUN_INTERVAL', 10).ok()
    alterGC(env, 'idx', 'clean_threshold', 5, 'run_interval', 20).ok()
    assertArmedWith(env, 'idx', 20)
    alterGC(env, 'idx', 'RUN_INTERVAL', 'default', 'CLEAN_THRESHOLD', 'DEFAULT').ok()
    assertArmedWith(env, 'idx', 1000)
    env.expect('ft.alter', 'idx', 'gc', 'set', 'RUN_INTERVAL', 30).ok()
    assertArmedWith(env, 'idx', 30)

    # The longest interval a timer can be armed with, which it reaches in steps.
    alterGC(env, 'idx', 'RUN_INTERVAL', GC_MAX_INTERVAL_SEC).ok()
    timer = gcTimer(env, 'idx')
    env.assertEqual(timer['interval_ms'], GC_MAX_INTERVAL_SEC * 1000)
    env.assertGreaterEqual(timer['remaining_ms'], GC_MAX_INTERVAL_SEC * 1000 - 60000)
    alterGC(env, 'idx', 'RUN_INTERVAL', 30, 'CLEAN_THRESHOLD', 5).ok()
    for ratio in (0, 1_000_000, 'default'):
        alterGC(env, 'idx', 'clean_ratio_ppm', ratio).ok()

    # Shorter than six words: Redis's arity check, before the handler.
    for short in (['GC', 'SET'], ['GC', 'SET', 'RUN_INTERVAL'], ['GC', 'RUN_INTERVAL', 10]):
        env.expect('FT.ALTER', 'idx', *short).error().contains('wrong number of arguments')

    rejected = [
        (['GC', 'RUN_INTERVAL', 10, 'CLEAN_THRESHOLD'], 'GC must be followed by SET'),
        (['GC', 'RESET', 'RUN_INTERVAL', 10], 'GC must be followed by SET'),
        (['GC', 'SET', 'RUN_INTERVAL', 10, 'RUN_INTERVAL', 20], 'more than once'),
        (['GC', 'SET', 'RUN_INTERVAL', 10, 'RUN_INTERVAL', 'DEFAULT'], 'more than once'),
        (['GC', 'SET', 'RUN_INTERVAL', 10, 'CLEAN_THRESHOLD'], 'Missing value'),
        (['GC', 'SET', 'RUN_INTERVAL', 10, 'FOO', 1], 'Unknown GC setting'),
        (['GC', 'SET', 'CLEAN_RATIO', 10], 'Unknown GC setting'),
        (['GC', 'SET', 'RUN_INTERVAL', 0], 'RUN_INTERVAL'),
        (['GC', 'SET', 'RUN_INTERVAL', GC_MAX_INTERVAL_SEC + 1], str(GC_MAX_INTERVAL_SEC)),
        (['GC', 'SET', 'RUN_INTERVAL', -1], 'RUN_INTERVAL'),
        (['GC', 'SET', 'RUN_INTERVAL', '1.5'], 'RUN_INTERVAL'),
        (['GC', 'SET', 'RUN_INTERVAL', 'ten'], 'RUN_INTERVAL'),
        (['GC', 'SET', 'CLEAN_THRESHOLD', -1], 'CLEAN_THRESHOLD'),
        (['GC', 'SET', 'CLEAN_THRESHOLD', 2**63], 'CLEAN_THRESHOLD'),
        (['GC', 'SET', 'CLEAN_THRESHOLD', '1e3'], 'CLEAN_THRESHOLD'),
        (['GC', 'SET', 'CLEAN_RATIO_PPM', 1_000_001], 'from 0 to 1000000'),
        (['GC', 'SET', 'CLEAN_RATIO_PPM', -1], 'CLEAN_RATIO_PPM'),
        (['GC', 'SET', 'CLEAN_RATIO_PPM', '0.5'], 'CLEAN_RATIO_PPM'),
        (['GC', 'SET', 'CLEAN_RATIO_PPM', 5, 'CLEAN_RATIO_PPM', 'DEFAULT'], 'more than once'),
        # Every value is checked before any is applied, so the valid ones are not applied.
        (['GC', 'SET', 'RUN_INTERVAL', 100, 'CLEAN_THRESHOLD', -1], 'CLEAN_THRESHOLD'),
        (['GC', 'SET', 'CLEAN_THRESHOLD', 0, 'RUN_INTERVAL', 0], 'RUN_INTERVAL'),
        (['GC', 'SET', 'CLEAN_THRESHOLD', 0, 'CLEAN_RATIO_PPM', 1_000_001], 'CLEAN_RATIO_PPM'),
        (['SKIPINITIALSCAN', 'GC', 'SET', 'RUN_INTERVAL', 100], 'SKIPINITIALSCAN does not apply'),
        (['FOO', 'a', 'b', 'c'], 'ALTER must be followed by SCHEMA or GC'),
    ]
    arms = timerArms(env)
    for args, error in rejected:
        env.expect('FT.ALTER', 'idx', *args).error().contains(error)
    env.assertEqual(timerArms(env), arms, message='a rejected command restarted the timer')
    assertArmedWith(env, 'idx', 30, message='a rejected interval applied')
    # CLEAN_THRESHOLD 5 is still set: a scheduled run with nothing deleted does not collect.
    runScheduledCycle(env, 'idx')
    env.assertEqual(gcCycles(env, 'idx'), 0, message='a rejected threshold applied')

    # A keyword is matched by its whole length: a NUL byte does not end it early.
    arms = timerArms(env)
    for args, error in ((['GC\x00junk', 'SET', 'RUN_INTERVAL', 40], 'ALTER must be followed by SCHEMA or GC'),
                        (['GC', 'SET\x00junk', 'RUN_INTERVAL', 50], 'GC must be followed by SET')):
        env.expect('FT.ALTER', 'idx', *args).error().contains(error)
    env.assertEqual(timerArms(env), arms, message='a keyword with a NUL byte restarted the timer')
    assertArmedWith(env, 'idx', 30, message='a keyword with a NUL byte applied its interval')

    env.expect('FT.ALTER', 'missing', 'GC', 'SET', 'RUN_INTERVAL', 10).error().contains('Index not found')
    env.expect('FT.CREATE', 'tmp', 'TEMPORARY', 1000, 'SCHEMA', 't', 'TEXT').ok()
    alterGC(env, 'tmp', 'RUN_INTERVAL', 10).error().contains('TEMPORARY')
    # SCHEMA ADD is unaffected.
    env.expect('FT.ALTER', 'idx', 'SCHEMA', 'ADD', 'n', 'NUMERIC').ok()
    env.expect('FT.ALTER', 'idx', 'SKIPINITIALSCAN', 'SCHEMA', 'ADD', 'm', 'NUMERIC').ok()


def alterRestartsTheTimer(policy):
    env = gcPolicyEnv(policy)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    assertArmedWith(env, 'idx', 1000)

    def expectRestart(restarts, seconds, *settings):
        arms = timerArms(env)
        alterGC(env, 'idx', *settings).ok()
        env.assertEqual(timerArms(env), arms + int(restarts), message=str(settings))
        assertArmedWith(env, 'idx', seconds, message=str(settings))

    # Lowering a long interval restarts the waiting timer from the command.
    expectRestart(True, 10, 'RUN_INTERVAL', 10)
    # The armed value again, however often, leaves it alone, so it cannot postpone GC.
    for _ in range(3):
        expectRestart(False, 10, 'RUN_INTERVAL', 10)
    # A threshold change never touches the timer.
    expectRestart(False, 10, 'CLEAN_THRESHOLD', 5)
    # DEFAULT restarts only when the global value differs from the armed one.
    expectRestart(True, 1000, 'RUN_INTERVAL', 'DEFAULT')
    expectRestart(False, 1000, 'RUN_INTERVAL', 1000)
    expectRestart(False, 1000, 'RUN_INTERVAL', 'DEFAULT')

    # A CONFIG SET leaves the waiting timer alone; FT.ALTER to the same new value then restarts
    # it, because the timer was armed with the old global value.
    arms = timerArms(env)
    setGlobalGC(env, policy, run_interval=10)
    env.assertEqual(timerArms(env), arms)
    assertArmedWith(env, 'idx', 1000)
    expectRestart(True, 10, 'RUN_INTERVAL', 10)


@skip(cluster=True)
def testGCAlterRestartsTheTimerFork():
    alterRestartsTheTimer('FORK')


@skip(cluster=True)
def testGCAlterRestartsTheTimerThread():
    alterRestartsTheTimer('THREAD')


def overridesAndInheritance(policy):
    """Unset and DEFAULT values follow the active policy's global values, read when a timer is
    armed and when a run is queued; the other policy's values never reach the index."""
    env = gcPolicyEnv(policy)
    for idx in ('inherit', 'reset', 'override'):
        env.expect('FT.CREATE', idx, 'PREFIX', 1, f'{idx}:', 'SCHEMA', 't', 'TEXT').ok()
    alterGC(env, 'reset', 'RUN_INTERVAL', 7, 'CLEAN_THRESHOLD', 0).ok()
    alterGC(env, 'reset', 'RUN_INTERVAL', 'DEFAULT', 'CLEAN_THRESHOLD', 'DEFAULT').ok()
    alterGC(env, 'override', 'RUN_INTERVAL', 7, 'CLEAN_THRESHOLD', 1000).ok()

    setGlobalGC(env, policy, run_interval=20, clean_threshold=0)
    setGlobalGC(env, otherPolicy(policy), run_interval=30, clean_threshold=1000)
    for idx, interval, collects in (('inherit', 20, True), ('reset', 20, True),
                                    ('override', 7, False)):
        before = gcCycles(env, idx)
        runScheduledCycle(env, idx)
        env.assertEqual(gcCycles(env, idx), before + int(collects), message=idx)
        assertArmedWith(env, idx, interval, message=idx)


@skip(cluster=True)
def testGCOverridesAndInheritanceFork():
    overridesAndInheritance('FORK')


@skip(cluster=True)
def testGCOverridesAndInheritanceThread():
    overridesAndInheritance('THREAD')


def indexesProgressAsConfigured(policy):
    """Real timers, no forced runs: each index collects on its own interval and threshold."""
    env = gcPolicyEnv(policy)
    for idx in ('fast', 'waits', 'slow'):
        env.expect('FT.CREATE', idx, 'PREFIX', 1, f'{idx}:', 'SCHEMA', 't', 'TEXT').ok()
    env.cmd('HSET', 'waits:1', 't', 'hello')
    env.expect('DEL', 'waits:1').equal(1)
    alterGC(env, 'fast', 'RUN_INTERVAL', 1, 'CLEAN_THRESHOLD', 0).ok()
    alterGC(env, 'waits', 'RUN_INTERVAL', 1).ok()  # keeps the global threshold of 1000
    alterGC(env, 'slow', 'CLEAN_THRESHOLD', 0).ok()  # keeps the global interval of 1000

    waitForGCCycles(env, 'fast', 0, 3, timeout=30)
    env.assertEqual(gcCycles(env, 'waits'), 0, message='collected below its threshold')
    env.assertEqual(gcCycles(env, 'slow'), 0, message='ran before its interval')
    assertArmedWith(env, 'slow', 1000)

    alterGC(env, 'waits', 'CLEAN_THRESHOLD', 1).ok()
    waitForGCCycles(env, 'waits', 0, timeout=30)


@skip(cluster=True)
def testGCIndexesProgressAsConfiguredFork():
    indexesProgressAsConfigured('FORK')


@skip(cluster=True)
def testGCIndexesProgressAsConfiguredThread():
    indexesProgressAsConfigured('THREAD')


@skip(cluster=True)
def testGCCleanThresholdZeroFork():
    """Under FORK, threshold 0 forks every interval even with nothing deleted; an index
    beside it keeps the global threshold."""
    env = gcPolicyEnv('FORK')
    for idx in ('zero', 'global'):
        env.expect('FT.CREATE', idx, 'PREFIX', 1, f'{idx}:', 'SCHEMA', 't', 'TEXT').ok()
        env.cmd('HSET', f'{idx}:1', 't', 'hello')
    alterGC(env, 'zero', 'CLEAN_THRESHOLD', 0).ok()
    for _ in range(2):
        for idx, collects in (('zero', True), ('global', False)):
            before = gcCycles(env, idx)
            runScheduledCycle(env, idx)
            env.assertEqual(gcCycles(env, idx), before + int(collects), message=idx)


@skip(cluster=True)
def testGCThreadRatioAppliesOverTheIndexThreshold():
    """Under THREAD, search-gc-clean-ratio-ppm applies on top of the index's threshold: a
    threshold below the ratio's share of the index still waits for the ratio."""
    env = gcPolicyEnv('THREAD')
    env.expect('CONFIG', 'SET', 'search-gc-clean-ratio-ppm', 500000).ok()  # half the docs
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    for i in range(10):
        env.cmd('HSET', f'doc{i}', 't', 'hello')
    alterGC(env, 'idx', 'CLEAN_THRESHOLD', 1).ok()

    for i in range(2):
        env.expect('DEL', f'doc{i}').equal(1)
    runScheduledCycle(env, 'idx')
    env.assertEqual(gcCycles(env, 'idx'), 0, message='2 deleted of 8 passed a ratio of half')

    for i in range(2, 5):
        env.expect('DEL', f'doc{i}').equal(1)
    runScheduledCycle(env, 'idx')
    env.assertEqual(gcCycles(env, 'idx'), 1, message='5 deleted of 5 did not pass a ratio of half')


def indexWithDeletes(env, idx, docs=10, deleted=2):
    """`idx` over its own prefix with `docs` documents, `deleted` of them deleted. The documents
    are written once the index's initial scan is done: one the scan also indexed would count as
    updated and move the index toward its clean bar."""
    env.expect('FT.CREATE', idx, 'PREFIX', 1, f'{idx}:', 'SCHEMA', 't', 'TEXT').ok()
    waitForIndex(env, idx)
    for i in range(docs):
        env.cmd('HSET', f'{idx}:{i}', 't', 'hello')
    for i in range(deleted):
        env.expect('DEL', f'{idx}:{i}').equal(1)


def assertScheduledCycleCollects(env, idx, collects, message=''):
    before = gcCycles(env, idx)
    runScheduledCycle(env, idx)
    env.assertEqual(gcCycles(env, idx), before + int(collects), message=message or idx)


def ratioOverridesAndInheritance(policy):
    """With 2 of 10 documents deleted, 8 remain: a ratio of half waits for 4 deletions, one of a
    fifth for 1. The active policy's global ratio reaches an index without an override, the
    other policy's never does, and a later CONFIG SET reaches the next queued run."""
    env = gcPolicyEnv(policy, clean_threshold=1)
    setGlobalGC(env, policy, clean_ratio_ppm=500_000)
    setGlobalGC(env, otherPolicy(policy), clean_ratio_ppm=0)
    for idx in ('inherit', 'lower', 'zero', 'reset'):
        indexWithDeletes(env, idx)
    alterGC(env, 'lower', 'CLEAN_RATIO_PPM', 200_000).ok()
    alterGC(env, 'zero', 'CLEAN_RATIO_PPM', 0).ok()  # the threshold of 1 alone decides
    alterGC(env, 'reset', 'CLEAN_RATIO_PPM', 0).ok()
    alterGC(env, 'reset', 'CLEAN_RATIO_PPM', 'DEFAULT').ok()

    for idx, collects in (('inherit', False), ('lower', True), ('zero', True), ('reset', False)):
        assertScheduledCycleCollects(env, idx, collects)

    # An override of the threshold leaves the ratio on top of it.
    alterGC(env, 'inherit', 'CLEAN_THRESHOLD', 0).ok()
    assertScheduledCycleCollects(env, 'inherit', False, 'a threshold override dropped the ratio')

    setGlobalGC(env, policy, clean_ratio_ppm=0)
    setGlobalGC(env, otherPolicy(policy), clean_ratio_ppm=1_000_000)
    for idx in ('inherit', 'reset'):
        assertScheduledCycleCollects(env, idx, True, f'{idx} after CONFIG SET')


@skip(cluster=True)
def testGCRatioOverridesAndInheritanceFork():
    ratioOverridesAndInheritance('FORK')


@skip(cluster=True)
def testGCRatioOverridesAndInheritanceThread():
    ratioOverridesAndInheritance('THREAD')


@skip(cluster=True)
def testGCForkWithoutARatioKeepsItsThreshold():
    """search-fork-gc-clean-ratio-ppm is 0 by default, so FORK forks on its threshold alone,
    however small the share of the index that was deleted; THREAD's ratio does not reach it."""
    env = gcPolicyEnv('FORK', clean_threshold=1)
    env.expect('CONFIG', 'GET', 'search-fork-gc-clean-ratio-ppm').equal(
        ['search-fork-gc-clean-ratio-ppm', '0'])
    env.expect('CONFIG', 'SET', 'search-gc-clean-ratio-ppm', 1_000_000).ok()
    indexWithDeletes(env, 'idx', docs=20, deleted=1)
    assertScheduledCycleCollects(env, 'idx', True)


@skip(cluster=True)
def testGCRatioSurvivesDebugReload():
    """The ratio is saved with the index, and DEFAULT clears it in the saved index too. A reload
    rebuilds the index without its deleted documents, so the deletions come after it."""
    env = gcPolicyEnv('FORK', clean_threshold=1)
    for idx in ('kept', 'cleared'):
        indexWithDeletes(env, idx, deleted=0)
        alterGC(env, idx, 'CLEAN_RATIO_PPM', 1_000_000).ok()
    env.expect('DEBUG', 'RELOAD').ok()
    alterGC(env, 'cleared', 'CLEAN_RATIO_PPM', 'DEFAULT').ok()
    env.expect('DEBUG', 'RELOAD').ok()
    for idx in ('kept', 'cleared'):
        waitForIndex(env, idx)

    for idx, collects in (('kept', False), ('cleared', True)):
        for i in range(2):
            env.expect('DEL', f'{idx}:{i}').equal(1)
        assertScheduledCycleCollects(env, idx, collects)


def alterWhileARunIsInFlight(policy, point):
    """A run queued or running holds the context with no timer armed. FT.ALTER arms nothing
    then, and the run's re-arm picks the new interval up."""
    env = gcPolicyEnv(policy, clean_threshold=0)
    skipIfNoEnableAssert(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    env.expect(debug_cmd(), 'GC_STOP_SCHEDULE', 'idx').ok()
    env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
    env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', point, 10000).ok()
    env.expect(debug_cmd(), 'GC_CONTINUE_SCHEDULE', 'idx').ok()
    waitForSyncPoint(env, point, f'the GC run never parked at {point}')

    arms = timerArms(env)
    alterGC(env, 'idx', 'RUN_INTERVAL', 5, 'CLEAN_THRESHOLD', 7).ok()
    env.assertEqual(gcTimer(env, 'idx'), {'type': 'none'})
    env.assertEqual(timerArms(env), arms)

    env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', point)
    waitForRearm(env, 'idx', 'the parked run never re-armed')
    assertArmedWith(env, 'idx', 5)


@skip(cluster=True)
def testGCAlterWhileARunIsQueuedFork():
    alterWhileARunIsInFlight('FORK', SYNC_POINT_GC_TASK_START)


@skip(cluster=True)
def testGCAlterWhileARunIsQueuedThread():
    alterWhileARunIsInFlight('THREAD', SYNC_POINT_GC_TASK_START)


@skip(cluster=True)
def testGCAlterWhileARunAwaitsItsRearmFork():
    alterWhileARunIsInFlight('FORK', SYNC_POINT_GC_BEFORE_REARM_POST)


@skip(cluster=True)
def testGCAlterWhileARunAwaitsItsRearmThread():
    alterWhileARunIsInFlight('THREAD', SYNC_POINT_GC_BEFORE_REARM_POST)


def alterWhileStopped(policy):
    env = gcPolicyEnv(policy)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    env.expect(debug_cmd(), 'GC_STOP_SCHEDULE', 'idx').ok()
    alterGC(env, 'idx', 'RUN_INTERVAL', 5).ok()
    env.assertEqual(gcTimer(env, 'idx'), {'type': 'none'}, message='FT.ALTER restarted a stopped GC')
    env.expect(debug_cmd(), 'GC_CONTINUE_SCHEDULE', 'idx').ok()
    waitForRearm(env, 'idx', 'the resumed GC never re-armed')
    assertArmedWith(env, 'idx', 5)


@skip(cluster=True)
def testGCAlterWhileStoppedFork():
    alterWhileStopped('FORK')


@skip(cluster=True)
def testGCAlterWhileStoppedThread():
    alterWhileStopped('THREAD')


@skip(cluster=True)
def testGCAlterWhileWaitingToRetryAFork():
    """A retry timer is armed with the retry interval, so FT.ALTER leaves it alone; the run
    interval set while it waits applies once a fork succeeds."""
    env = Env(moduleArgs='GC_POLICY FORK FORK_GC_RUN_INTERVAL 1000 '
                         'FORK_GC_RETRY_INTERVAL 1000 FORK_GC_CLEAN_THRESHOLD 0')
    skipIfNoEnableAssert(env)
    indexWithCollectableDocs(env)
    base = gcCycles(env, 'idx')
    env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
    env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', SYNC_POINT_GC_BEFORE_FORK, 10000).ok()
    try:
        env.expect(debug_cmd(), 'GC_CONTINUE_SCHEDULE', 'idx').ok()
        waitForSyncPoint(env, SYNC_POINT_GC_BEFORE_FORK,
                         'Timeout waiting for the scheduled GC run to park before its fork')
        holdForkSlot(env, 100000)
        env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', SYNC_POINT_GC_BEFORE_FORK)
        wait_for_condition(lambda: (gcTimer(env, 'idx').get('type') == 'retry', gcTimer(env, 'idx')),
                           'the failed fork never armed a retry', timeout=30)
        env.assertEqual(gcCycles(env, 'idx'), base)

        arms = timerArms(env)
        alterGC(env, 'idx', 'RUN_INTERVAL', 5).ok()
        env.assertEqual(timerArms(env), arms, message='FT.ALTER restarted a retry timer')
        timer = gcTimer(env, 'idx')
        env.assertEqual((timer['type'], timer['interval_ms']), ('retry', 1000 * 1000))
    finally:
        env.cmd('CONFIG', 'SET', 'rdb-key-save-delay', '0')
        waitForRdbSaveToFinish(env)

    # The retry, now able to fork, collects and re-arms with the new run interval.
    runScheduledCycle(env, 'idx')
    env.assertEqual(gcCycles(env, 'idx'), base + 1)
    assertArmedWith(env, 'idx', 5)


@skip(cluster=True)
def testGCTeardownWhenDropOvertakesPendingRearmWithAnOverride():
    """As testGCTeardownWhenDropOvertakesPendingRearm_MOD_17309, with a run interval override
    far longer than the global one. The re-arm after the drop must not use the override of an
    index that is gone, or the teardown would wait out its period."""
    env = Env(redisConfigFile=thread_gc_conf(run_interval=1, clean_threshold=0))
    skipIfNoEnableAssert(env)
    env.expect('FT.CREATE', 'idx', 'PREFIX', 1, 'doc:', 'SCHEMA', 't', 'TEXT').ok()
    alterGC(env, 'idx', 'RUN_INTERVAL', 100000).ok()
    # Queue a run now rather than wait out the override.
    env.expect(debug_cmd(), 'GC_STOP_SCHEDULE', 'idx').ok()
    env.cmd(debug_cmd(), 'SYNC_POINT', 'CLEAR')
    env.expect(debug_cmd(), 'SYNC_POINT', 'ARM', SYNC_POINT_GC_BEFORE_REARM_POST, 10000).ok()
    env.expect(debug_cmd(), 'GC_CONTINUE_SCHEDULE', 'idx').ok()
    waitForSyncPoint(env, SYNC_POINT_GC_BEFORE_REARM_POST,
                     'the GC run never parked before its re-arm')
    keepGCInfoSectionAlive(env)

    env.cmd('HSET', 'doc:1', 't', 'hello')
    env.expect('DEL', 'doc:1').equal(1)
    env.assertEqual(logicallyDeletedDocs(env), 1)
    env.expect(debug_cmd(), 'GC_STOP_SCHEDULE', 'idx').ok()
    env.expect('FT.DROPINDEX', 'idx').ok()

    env.cmd(debug_cmd(), 'SYNC_POINT', 'SIGNAL', SYNC_POINT_GC_BEFORE_REARM_POST)
    wait_for_condition(
        lambda: (logicallyDeletedDocs(env) == 0, {'not_collected': logicallyDeletedDocs(env)}),
        'a drop that overtook the pending re-arm never tore the context down', timeout=30)
    env.expect('FT._LIST').equal(['keep'])


@skip(cluster=True)
def testGCAlterThroughAnAlias():
    """The alias names the index it points to when the command runs; an index it points to
    later does not get the override."""
    env = gcPolicyEnv('FORK')
    for idx in ('first', 'second'):
        env.expect('FT.CREATE', idx, 'PREFIX', 1, f'{idx}:', 'SCHEMA', 't', 'TEXT').ok()
    env.expect('FT.ALIASADD', 'alias', 'first').ok()
    alterGC(env, 'alias', 'RUN_INTERVAL', 10).ok()
    assertArmedWith(env, 'first', 10)
    env.expect('FT.ALIASUPDATE', 'alias', 'second').ok()
    runScheduledCycle(env, 'second')
    assertArmedWith(env, 'second', 1000)
    assertArmedWith(env, 'first', 10)


@skip(cluster=True)
def testGCRecreatedIndexStartsWithoutOverrides():
    env = gcPolicyEnv('FORK')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    env.cmd('HSET', 'doc1', 't', 'hello')
    alterGC(env, 'idx', 'RUN_INTERVAL', 10, 'CLEAN_THRESHOLD', 0).ok()
    env.expect('FT.DROPINDEX', 'idx').ok()
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    assertArmedWith(env, 'idx', 1000)
    runScheduledCycle(env, 'idx')
    env.assertEqual(gcCycles(env, 'idx'), 0, message="the dropped index's threshold applied")


@skip(cluster=True)
def testGCAlterWithoutGC():
    """A node started with search-no-gc accepts and stores the override, for a node that
    collects to use once the index reaches it (test_replicate.py follows it there)."""
    env = Env(moduleArgs='NOGC')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    alterGC(env, 'idx', 'RUN_INTERVAL', 10, 'CLEAN_THRESHOLD', 0).ok()
    alterGC(env, 'idx', 'RUN_INTERVAL', 'DEFAULT').ok()
    env.expect(debug_cmd(), 'GC_TIMER_INFO', 'idx').error().contains('GC is not available')
    env.expect('DEBUG', 'RELOAD').ok()
    env.expect('FT._LIST').equal(['idx'])


def overridesKept(env, idx, interval, message):
    """`idx` arms with `interval` and collects with nothing deleted, as a CLEAN_THRESHOLD 0
    override over a global threshold of 1000 makes it."""
    assertArmedWith(env, idx, interval, message=message)
    before = gcCycles(env, idx)
    runScheduledCycle(env, idx)
    env.assertEqual(gcCycles(env, idx), before + 1, message=f'{message}: threshold lost')


@skip(cluster=True)
def testGCOverridesSurviveDebugReload():
    env = gcPolicyEnv('FORK')
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    env.expect('FT.CREATE', 'plain', 'PREFIX', 1, 'plain:', 'SCHEMA', 't', 'TEXT').ok()
    alterGC(env, 'idx', 'RUN_INTERVAL', 77, 'CLEAN_THRESHOLD', 0).ok()
    env.expect('DEBUG', 'RELOAD').ok()
    overridesKept(env, 'idx', 77, 'DEBUG RELOAD')
    assertArmedWith(env, 'plain', 1000, message='an index without overrides gained one')


def waitForAofRewrite(env):
    wait_for_condition(
        lambda: (int(env.cmd('INFO', 'persistence')['aof_rewrite_in_progress']) == 0, {}),
        'BGREWRITEAOF never finished', timeout=60)


@skip(cluster=True)
def testGCOverridesSurviveAofReplay():
    """An FT.ALTER after the RDB preamble is replayed from the AOF; one before a BGREWRITEAOF
    is loaded from the preamble the rewrite writes."""
    env = gcPolicyEnv('FORK', useAof=True)
    if env.env == 'existing-env':
        env.skip()
    env.expect('CONFIG', 'SET', 'aof-use-rdb-preamble', 'yes').ok()
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    env.cmd('BGREWRITEAOF')
    waitForAofRewrite(env)

    alterGC(env, 'idx', 'RUN_INTERVAL', 77, 'CLEAN_THRESHOLD', 0).ok()
    env.expect('DEBUG', 'LOADAOF').ok()
    overridesKept(env, 'idx', 77, 'AOF replay')

    alterGC(env, 'idx', 'RUN_INTERVAL', 88).ok()
    env.cmd('BGREWRITEAOF')
    waitForAofRewrite(env)
    env.restartAndReload()
    overridesKept(env, 'idx', 88, 'BGREWRITEAOF and restart')


@skip(cluster=False)
def testGCAlterFansOutToEveryShard(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    shards = env.getOSSMasterNodesConnectionList()
    with env.getClusterConnectionIfNeeded() as conn:
        env.assertEqual(conn.execute_command('FT.ALTER', 'idx', 'GC', 'SET', 'RUN_INTERVAL', 77), 'OK')
        for i, shard in enumerate(shards):
            assertArmedWith(env, 'idx', 77, conn=shard, message=f'shard {i}')
        try:
            conn.execute_command('FT.ALTER', 'idx', 'GC', 'SET', 'RUN_INTERVAL', 0)
            env.assertTrue(False, message='an out-of-range value was accepted')
        except redis_exceptions.ResponseError as e:
            env.assertContains('RUN_INTERVAL', str(e))
        for i, shard in enumerate(shards):
            assertArmedWith(env, 'idx', 77, conn=shard, message=f'shard {i} after a rejected value')
