from RLTest import Env
from common import *
import threading
import time
import random

def test_MOD_11658_workers_reduction_under_load():
    """
    Test that changing WORKERS from high to 0 under load doesn't cause unresponsiveness.
    This test:
    1. Starts with WORKERS=8 (simulating QPF=8)
    2. Creates an index and loads data
    3. Runs concurrent queries in background threads (100 threads, no delays)
    4. Changes WORKERS to 0 (simulating QPF=0 change)
    5. Verifies the shard remains responsive

    """
    # This test requires coordinator mode (OSS cluster)

    # Start with 8 workers (simulating QPF=8)
    env = Env(moduleArgs='WORKERS 8', enableDebugCommand=True)

    # Create index with multiple field types to make queries more complex
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               'title', 'TEXT', 'WEIGHT', '2.0',
               'body', 'TEXT',
               'price', 'NUMERIC', 'SORTABLE',
               'category', 'TAG',
               'location', 'GEO').ok()

    # Load a significant amount of data to make queries take time
    conn = getConnectionByEnv(env)
    n_docs = 1000
    categories = ['electronics', 'books', 'clothing', 'food', 'toys']

    for i in range(n_docs):
        conn.execute_command('HSET', f'doc{i}',
                           'title', f'Product {i} title with searchable text',
                           'body', f'This is the body of document {i} with more searchable content',
                           'price', random.randint(10, 1000),
                           'category', random.choice(categories),
                           'location', f'{random.uniform(-90, 90)},{random.uniform(-180, 180)}')

    waitForIndex(env, 'idx')

    # Verify initial state
    initial_workers = env.cmd(config_cmd(), 'GET', 'WORKERS')
    env.assertEqual(initial_workers, [['WORKERS', '8']])

    # Flag to control query threads
    stop_queries = threading.Event()
    query_success_count = [0]  # Use list to allow modification in thread

    def run_queries():
        """Run various queries continuously until stopped"""
        local_conn = env.getConnection()
        while not stop_queries.is_set():
            # Mix of different query types
            queries = [
                ['FT.SEARCH', 'idx', '*', 'LIMIT', '0', '10'],
                ['FT.SEARCH', 'idx', 'searchable', 'LIMIT', '0', '10'],
                ['FT.SEARCH', 'idx', '@category:{electronics}', 'LIMIT', '0', '10'],
                ['FT.SEARCH', 'idx', '*', 'SORTBY', 'price', 'ASC', 'LIMIT', '0', '10'],
                ['FT.AGGREGATE', 'idx', '*', 'GROUPBY', '1', '@category', 'REDUCE', 'COUNT', '0', 'AS', 'count'],
            ]

            query = random.choice(queries)
            _ = local_conn.execute_command(*query)
            query_success_count[0] += 1

    # Start multiple query threads to simulate concurrent load
    # Increase thread count to maximize likelihood of hitting the race condition
    # The bug requires worker threads to be actively processing queries when
    # the config change happens
    num_query_threads = 20
    query_threads = []

    for i in range(num_query_threads):
        t = threading.Thread(target=run_queries, name=f'QueryThread-{i}')
        t.start()
        query_threads.append(t)

    # Let queries run for a bit to establish load
    # Increase time to ensure all threads are actively querying
    time.sleep(3)

    # Verify queries are running successfully
    initial_success = query_success_count[0]
    env.assertTrue(initial_success > 0, message="Queries should be running successfully before config change")

    # Now change WORKERS to 0 (simulating QPF change from 8 to 0)
    # This is the critical moment that triggers the bug
    env.debugPrint("Changing WORKERS from 8 to 0 while queries are running...", force=True)
    pre_count = query_success_count[0]
    env.debugPrint(f"Query success count before config change: {pre_count}", force=True)

    # The bug: This command may hang indefinitely if worker threads are blocked
    # waiting for coordinator connections that were stopped by MRConnManager_Shrink
    env.expect(config_cmd(), 'SET', 'WORKERS', '0').ok()

    # Verify the config change is reflected in the getter
    new_workers = env.cmd(config_cmd(), 'GET', 'WORKERS')
    env.assertEqual(new_workers, [['WORKERS', '0']])
    time.sleep(5)
    post_count = query_success_count[0]
    env.debugPrint(f"Query success count after config change: {post_count}", force=True)
    env.assertGreater(post_count, pre_count, message="Queries should continue running after config change")



    # Verify the config change took effect
    # TODO(Joan): How can I know if the threads actually disappeared
    # Critical test: Verify Redis is still responsive
    # This is where the bug manifests - Redis becomes unresponsive
    try:
        # Try to PING - this should work even with WORKERS=0
        ping_result = env.cmd('PING')
        env.assertContains(ping_result, ['PONG', True], message="Redis should respond to PING after WORKERS change")

        # Try a simple query - this should work on the main thread
        search_result = env.cmd('FT.SEARCH', 'idx', '*', 'LIMIT', '0', '1')
        env.assertTrue(search_result is not None, message="Search should work after WORKERS change")

        # Try to add a new document
        add_result = conn.execute_command('HSET', 'newdoc', 'title', 'test', 'price', '100', 'category', 'test')
        env.assertContains(add_result, [3, True], message="Should be able to add documents after WORKERS change")
    except Exception as e:
        env.debugPrint(f"CRITICAL: Redis became unresponsive after WORKERS change: {e}", force=True)
        raise AssertionError(f"Redis became unresponsive after WORKERS change (MOD-11658 reproduced): {e}")

    # Let queries continue for a bit with WORKERS=0
    time.sleep(2)

    # Stop query threads
    stop_queries.set()
    for t in query_threads:
        t.join(timeout=5)

    # Final verification: Redis should still be fully functional
    final_ping = env.cmd('PING')
    env.assertTrue(final_ping in ['PONG', True], message="Redis should still respond to PING at end of test")

    final_search = env.cmd('FT.SEARCH', 'idx', '*', 'LIMIT', '0', '5')
    env.assertTrue(final_search[0] > 0, message="Search should return results at end of test")

    env.debugPrint(f"Test completed. Total successful queries: {query_success_count[0]}", force=True)


@skip(cluster=False)
def test_MOD_11658_workers_reduction_sequence():
    """
    Test gradual reduction of workers to see if the issue is specific to large deltas.
    This test reduces workers gradually: 8 -> 4 -> 2 -> 1 -> 0
    """
    env = Env(moduleArgs='WORKERS 8', enableDebugCommand=True)

    # Create simple index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'text', 'TEXT').ok()

    # Add some documents
    conn = getConnectionByEnv(env)
    for i in range(100):
        conn.execute_command('HSET', f'doc{i}', 'text', f'document {i} with searchable content')

    waitForIndex(env, 'idx')

    # Test gradual reduction
    worker_sequence = [8, 4, 2, 1, 0]

    for workers in worker_sequence:
        env.debugPrint(f"Testing with WORKERS={workers}", force=True)

        if workers < 8:  # Skip first iteration (already at 8)
            env.expect(config_cmd(), 'SET', 'WORKERS', str(workers)).ok()

        # Verify config
        current = env.cmd(config_cmd(), 'GET', 'WORKERS')
        env.assertEqual(current, [['WORKERS', str(workers)]])

        # Verify responsiveness
        ping_result = env.cmd('PING')
        env.assertTrue(ping_result in ['PONG', True])

        # Run a query
        result = env.cmd('FT.SEARCH', 'idx', 'searchable', 'LIMIT', '0', '5')
        env.assertTrue(result[0] > 0, message="Search should work with WORKERS={}".format(workers))
        # Small delay between changes
        time.sleep(0.5)

    env.debugPrint("Gradual reduction test completed successfully", force=True)


@skip(cluster=False)
def test_MOD_11658_workers_zero_to_nonzero():
    """
    Test that increasing workers from 0 to a higher value also works correctly.
    This tests the reverse direction to ensure the connection pool expansion works.
    """
    # Start with WORKERS=0
    env = Env(moduleArgs='WORKERS 0', enableDebugCommand=True)

    # Create index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'text', 'TEXT').ok()

    # Add documents
    conn = getConnectionByEnv(env)
    for i in range(100):
        conn.execute_command('HSET', f'doc{i}', 'text', f'document {i}')

    waitForIndex(env, 'idx')

    # Verify initial state
    env.assertEqual(env.cmd(config_cmd(), 'GET', 'WORKERS'), [['WORKERS', '0']])

    # Query should work with WORKERS=0 (on main thread)
    result = env.cmd('FT.SEARCH', 'idx', '*', 'LIMIT', '0', '5')
    env.assertTrue(result[0] > 0)

    # Increase workers to 8
    env.expect(config_cmd(), 'SET', 'WORKERS', '8').ok()
    env.assertEqual(env.cmd(config_cmd(), 'GET', 'WORKERS'), [['WORKERS', '8']])

    # Verify still responsive
    ping_result = env.cmd('PING')
    env.assertTrue(ping_result in ['PONG', True])

    # Query should still work
    result = env.cmd('FT.SEARCH', 'idx', '*', 'LIMIT', '0', '5')
    env.assertTrue(result[0] > 0)

    env.debugPrint("Workers increase test completed successfully", force=True)
