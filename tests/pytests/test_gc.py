
from common import *
import platform
from time import sleep
import threading
import time

@skip(cluster=True)
def testBasicGC(env):
    env.expect(config_cmd(), 'set', 'FORK_GC_CLEAN_THRESHOLD', 0).equal('OK')
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
        # this test is not relevent for legacy gc cause its not squeshing inverted index
        env.skip()
    env.expect(config_cmd(), 'set', 'FORK_GC_CLEAN_THRESHOLD', 0).equal('OK')
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
def testNumericGCIntensive(env):
    NumberOfDocs = 1000
    env.expect(config_cmd(), 'set', 'FORK_GC_CLEAN_THRESHOLD', 0).equal('OK')
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
def testGeoGCIntensive(env):
    NumberOfDocs = 1000
    env.expect(config_cmd(), 'set', 'FORK_GC_CLEAN_THRESHOLD', 0).equal('OK')
    env.assertOk(env.cmd('ft.create', 'idx', 'ON', 'HASH', 'schema', 'g', 'geo'))
    waitForIndex(env, 'idx')

    for i in range(NumberOfDocs):
        env.assertOk(env.cmd('ft.add', 'idx', 'doc%d' % i, 1.0, 'fields', 'g', '12.34,56.78'))

    for i in range(0, NumberOfDocs, 2):
        env.assertEqual(env.cmd('ft.del', 'idx', 'doc%d' % i), 1)

    for i in range(100):
        forceInvokeGC(env, 'idx')

    res = env.cmd(debug_cmd(), 'DUMP_NUMIDX', 'idx', 'g')
    for r1 in res:
        for r2 in r1:
            # if r2 is greater then 900 its on the last block and fork GC does not clean the last block
            env.assertTrue(r2 % 2 == 0 or r2 > 900)

@skip(cluster=True)
def testTagGC(env):
    NumberOfDocs = 101
    env.expect(config_cmd(), 'set', 'FORK_GC_CLEAN_THRESHOLD', 0).equal('OK')
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
def testDeleteEntireBlock(env):
    env.expect(config_cmd(), 'set', 'FORK_GC_CLEAN_THRESHOLD', 0).equal('OK')
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH',
               'SCHEMA', 'test', 'TEXT', 'SORTABLE', 'test2', 'TEXT', 'SORTABLE', ).ok()
    waitForIndex(env, 'idx')
    # creating 5 blocks on 'checking' inverted index
    for i in range(700):
        env.expect('FT.ADD', 'idx', 'doc%d' % i, '1.0', 'FIELDS', 'test', 'checking', 'test2', 'checking%d' % i).ok()

    # delete docs in the midle of the inverted index, make sure the binary search are not braken
    for i in range(400, 501):
        env.expect('FT.DEL', 'idx', 'doc%d' % i).equal(1)
    res = env.cmd('FT.SEARCH', 'idx', '@test:checking @test2:checking250')
    env.assertEqual(res[0:2],[1, 'doc250'])
    env.assertEqual(set(res[2]), set(['test', 'checking', 'test2', 'checking250']))

    # actually clean the inverted index, make sure the binary search are not braken, check also after rdb reload
    for i in range(100):
        # gc is random so we need to do it long enough times for it to work
        forceInvokeGC(env, 'idx')
    for _ in env.reloadingIterator():
        waitForIndex(env, 'idx')
        res = env.cmd('FT.SEARCH', 'idx', '@test:checking @test2:checking250')
        env.assertEqual(res[0:2],[1, 'doc250'])
        env.assertEqual(set(res[2]), set(['test', 'checking', 'test2', 'checking250']))

@skip(cluster=True)
def testGCIntegrationWithRedisFork(env):
    if env.env == 'existing-env':
        env.skip()
    if env.env == 'enterprise':
        env.skip()
    if env.cmd(config_cmd(), 'GET', 'GC_POLICY')[0][1] != 'fork':
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
def testGCThreshold(env):
    if env.env == 'existing-env':
        env.skip()

    env = Env(moduleArgs='GC_POLICY FORK FORK_GC_CLEAN_THRESHOLD 1000')
    env.expect('FT.CREATE', 'idx', 'ON', 'HASH', 'SCHEMA', 'title', 'TEXT', 'SORTABLE').ok()
    waitForIndex(env, 'idx')
    for i in range(1000):
        env.expect('FT.ADD', 'idx', 'doc%d' % i, '1.0', 'FIELDS', 'title', 'foo').ok()

    debug_rep = env.cmd(debug_cmd(), 'DUMP_INVIDX', 'idx', 'foo')

    for i in range(999):
        env.expect('FT.DEL', 'idx', 'doc%d' % i).equal(1)

    forceInvokeGC(env, 'idx')

    env.expect(debug_cmd(), 'DUMP_INVIDX', 'idx', 'foo').equal(debug_rep)

    env.expect('FT.DEL', 'idx', 'doc999').equal(1)

    forceInvokeGC(env, 'idx')

    env.expect(debug_cmd(), 'DUMP_INVIDX', 'idx', 'foo').error().contains('Can not find the inverted index')

    # retry with replace
    for i in range(1000):
        env.expect('FT.ADD', 'idx', 'doc%d' % i, '1.0', 'FIELDS', 'title', 'foo').ok()

    debug_rep = env.cmd(debug_cmd(), 'DUMP_INVIDX', 'idx', 'foo')

    for i in range(999):
        env.expect('FT.ADD', 'idx', 'doc%d' % i, '1.0', 'REPLACE', 'FIELDS', 'title', 'foo1').ok()

    forceInvokeGC(env, 'idx')

    env.expect(debug_cmd(), 'DUMP_INVIDX', 'idx', 'foo').equal(debug_rep)

    env.expect('FT.ADD', 'idx', 'doc999', '1.0', 'REPLACE', 'FIELDS', 'title', 'foo1').ok()

    forceInvokeGC(env, 'idx')

    env.expect(debug_cmd(), 'DUMP_INVIDX', 'idx', 'foo').error().contains('Can not find the inverted index')

    # retry with replace partial

    debug_rep = env.cmd(debug_cmd(), 'DUMP_INVIDX', 'idx', 'foo1')

    for i in range(999):
        env.expect('FT.ADD', 'idx', 'doc%d' % i, '1.0', 'REPLACE', 'PARTIAL', 'FIELDS', 'title', 'foo2').ok()

    forceInvokeGC(env, 'idx')

    env.expect(debug_cmd(), 'DUMP_INVIDX', 'idx', 'foo1').equal(debug_rep)

    env.expect('FT.ADD', 'idx', 'doc999', '1.0', 'REPLACE', 'PARTIAL', 'FIELDS', 'title', 'foo2').ok()

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
    env = Env(moduleArgs='FORK_GC_CLEAN_THRESHOLD 0')
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
    env.expect(config_cmd(), 'set', 'FORK_GC_CLEAN_THRESHOLD', 0).equal('OK')
    env.expect(config_cmd(), 'set', 'FORK_GC_RUN_INTERVAL', 100).equal('OK')  # Run GC more frequently

    # Number of indexes to create and test with
    num_indexes = 5
    docs_per_index = 100

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
                   'category', 'TAG',
                   'location', 'GEO').ok()
        waitForIndex(env, idx_name)

        # Add documents to make the indexes substantial
        conn = env.getConnection()
        for j in range(docs_per_index):
            doc_id = f'doc_{i}_{j}'
            conn.execute_command('HSET', doc_id,
                               'title', f'Product {i} {j}',
                               'price', j * 10.5,
                               'category', f'cat{j % 5}',
                               'location', f'{40.7128 + j * 0.001},{-74.0060 + j * 0.001}')

    # Verify all indexes are created and populated
    for idx_name in index_names:
        info = env.cmd('FT.INFO', idx_name)
        info_dict = {info[i]: info[i + 1] for i in range(0, len(info), 2)}
        env.assertEqual(int(info_dict['num_docs']), docs_per_index)

    # Shared variables for thread coordination
    results = {'info_calls': 0, 'errors': 0, 'successful_calls': 0}
    stop_threads = threading.Event()

    def ft_info_worker(idx_name):
        """Worker function that continuously calls FT.INFO on an index"""
        local_conn = env.getConnection()
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
                # - "Unknown index name"
                # - Connection errors during cleanup
                error_msg = str(e).lower()
                if 'unknown index' in error_msg or 'connection' in error_msg:
                    # These are expected errors during index deletion
                    pass
                else:
                    # Unexpected error - log it but don't fail the test
                    env.debugPrint(f"Unexpected error in FT.INFO for {idx_name}: {e}", force=True)
                time.sleep(0.01)

    # Start worker threads for each index
    threads = []
    for idx_name in index_names:
        thread = threading.Thread(target=ft_info_worker, args=(idx_name,))
        thread.daemon = True
        threads.append(thread)
        thread.start()

    # Let the threads run for a short time to establish baseline
    time.sleep(0.5)

    # Now delete the indexes while FT.INFO calls are running
    for idx_name in index_names:
        try:
            env.expect('FT.DROP', idx_name, 'DD').ok()  # DD = Delete Documents
            env.debugPrint(f"Dropped index {idx_name}", force=True)
        except Exception as e:
            env.debugPrint(f"Error dropping index {idx_name}: {e}", force=True)

        # Force GC to clean up the deleted index
        try:
            forceBGInvokeGC(env, idx_name)
        except Exception as e:
            # Expected - index might already be cleaned up
            pass

        # Small delay between deletions to spread out the work
        time.sleep(0.1)

    # Continue running FT.INFO calls for a bit longer to catch cleanup operations
    time.sleep(1.0)

    # Stop all threads
    stop_threads.set()
    for thread in threads:
        thread.join(timeout=5.0)  # 5 second timeout for thread cleanup

    # Verify that we made a reasonable number of FT.INFO calls
    env.assertGreater(results['info_calls'], 50,
                     f"Expected at least 50 FT.INFO calls, got {results['info_calls']}")

    # Verify that we had both successful calls and expected errors
    env.assertGreater(results['successful_calls'], 0,
                     f"Expected some successful FT.INFO calls, got {results['successful_calls']}")

    # Log the results for debugging
    env.debugPrint(f"FT.INFO test results: {results['info_calls']} total calls, "
                  f"{results['successful_calls']} successful, {results['errors']} errors", force=True)

    # Verify that all indexes are actually deleted
    for idx_name in index_names:
        try:
            env.cmd('FT.INFO', idx_name)
            env.assertTrue(False, f"Index {idx_name} should have been deleted")
        except Exception as e:
            # Expected - index should be gone
            env.assertTrue('unknown index' in str(e).lower() or 'no such index' in str(e).lower(),
                          f"Unexpected error for deleted index {idx_name}: {e}")

    env.debugPrint("Concurrent FT.INFO during index deletion test completed successfully", force=True)
