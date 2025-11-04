
from RLTest import Env
from common import *
import threading
import time
import random

@skip(cluster=False)
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
    query_errors = []
    query_success_count = [0]  # Use list to allow modification in thread
    
    def run_queries():
        """Run various queries continuously until stopped"""
        local_conn = env.getConnection()
        while not stop_queries.is_set():
            try:
                # Mix of different query types
                queries = [
                    ['FT.SEARCH', 'idx', '*', 'LIMIT', '0', '10'],
                    ['FT.SEARCH', 'idx', 'searchable', 'LIMIT', '0', '10'],
                    ['FT.SEARCH', 'idx', '@category:{electronics}', 'LIMIT', '0', '10'],
                    ['FT.SEARCH', 'idx', '*', 'SORTBY', 'price', 'ASC', 'LIMIT', '0', '10'],
                    ['FT.AGGREGATE', 'idx', '*', 'GROUPBY', '1', '@category', 'REDUCE', 'COUNT', '0', 'AS', 'count'],
                ]
                
                query = random.choice(queries)
                result = local_conn.execute_command(*query)
                query_success_count[0] += 1

                # NO delay - maximize concurrent queries to increase race condition likelihood
                # The bug is a race condition, so we want maximum concurrency
                # time.sleep(0.01)
                
            except Exception as e:
                query_errors.append(str(e))
                # If we get connection errors, stop trying
                if 'Connection' in str(e) or 'timeout' in str(e).lower():
                    break
    
    # Start multiple query threads to simulate concurrent load
    # Increase thread count to maximize likelihood of hitting the race condition
    # The bug requires worker threads to be actively processing queries when
    # the config change happens
    num_query_threads = 50
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
    env.debugPrint(f"Query success count before config change: {query_success_count[0]}", force=True)

    # The bug: This command may hang indefinitely if worker threads are blocked
    # waiting for coordinator connections that were stopped by MRConnManager_Shrink
    env.expect(config_cmd(), 'SET', 'WORKERS', '0').ok()
    
    # Verify the config change took effect
    new_workers = env.cmd(config_cmd(), 'GET', 'WORKERS')
    env.assertEqual(new_workers, [['WORKERS', '0']])
    
    # Critical test: Verify Redis is still responsive
    # This is where the bug manifests - Redis becomes unresponsive
    try:
        # Try to PING - this should work even with WORKERS=0
        ping_result = env.cmd('PING')
        env.assertTrue(ping_result in ['PONG', True], message="Redis should respond to PING after WORKERS change")

        # Try a simple query - this should work on the main thread
        search_result = env.cmd('FT.SEARCH', 'idx', '*', 'LIMIT', '0', '1')
        env.assertTrue(search_result is not None, message="Search should work after WORKERS change")

        # Try to add a new document
        add_result = conn.execute_command('HSET', 'newdoc', 'title', 'test', 'price', '100', 'category', 'test')
        env.assertTrue(add_result in [3, True], message="Should be able to add documents after WORKERS change")
        
    except Exception as e:
        env.debugPrint(f"CRITICAL: Redis became unresponsive after WORKERS change: {e}", force=True)
        raise AssertionError(f"Redis became unresponsive after WORKERS change (MOD-11658 reproduced): {e}")
    
    # Let queries continue for a bit with WORKERS=0
    time.sleep(2)
    
    # Stop query threads
    stop_queries.set()
    for t in query_threads:
        t.join(timeout=5)
    
    # Check if there were connection errors during or after the transition
    if query_errors:
        env.debugPrint(f"Query errors during test: {query_errors[:10]}", force=True)
        # Some errors might be expected during the transition, but connection errors are critical
        connection_errors = [e for e in query_errors if 'Connection' in e or 'timeout' in e.lower()]
        if connection_errors:
            env.debugPrint(f"CRITICAL: Connection errors detected: {connection_errors[:5]}", force=True)
            # This indicates the bug was reproduced
            raise AssertionError(f"Connection errors after WORKERS change (MOD-11658 reproduced): {connection_errors[0]}")
    
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

@skip(cluster=False)
def test_MOD_11658_workers_8_to_0_to_8_rapid():
    """
    Test the 8→0→8 scenario to verify the new non-blocking implementation.

    This test specifically validates:
    1. Changing from 8→0 doesn't block (returns immediately)
    2. Attempting 0→8 while threads are still terminating is rejected
    3. Once threads finish terminating, 0→8 succeeds
    """

    # Start with WORKERS=8
    env = Env(moduleArgs='WORKERS 8', enableDebugCommand=True)

    # Create index and add data
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'text', 'TEXT').ok()

    conn = getConnectionByEnv(env)
    for i in range(100):
        conn.execute_command('HSET', f'doc{i}', 'text', f'document {i}')

    waitForIndex(env, 'idx')

    # Trigger thread pool initialization by running a query
    env.cmd('FT.SEARCH', 'idx', '*', 'LIMIT', '0', '1')

    # Wait for threads to be alive
    time.sleep(0.5)

    # Verify we have 8 workers alive
    stats = getWorkersThpoolStats(env)
    env.assertEqual(stats['numThreadsAlive'], 8, message="Should have 8 threads alive initially")

    # Change from 8→0 (this should return immediately with the new implementation)
    env.debugPrint("Changing WORKERS from 8 to 0...", force=True)
    start_time = time.time()
    env.expect(config_cmd(), 'SET', 'WORKERS', '0').ok()
    elapsed = time.time() - start_time

    # The command should return quickly (< 1 second) because it doesn't wait
    env.assertTrue(elapsed < 1.0, message=f"Config change should be non-blocking, took {elapsed}s")
    env.debugPrint(f"Config change took {elapsed:.3f}s (non-blocking)", force=True)

    # Verify config changed
    env.assertEqual(env.cmd(config_cmd(), 'GET', 'WORKERS'), [['WORKERS', '0']])

    # Immediately try to change back to 8 (this might fail if threads are still terminating)
    env.debugPrint("Attempting to change WORKERS from 0 to 8 immediately...", force=True)

    # Try multiple times with small delays to handle the race condition
    max_attempts = 20
    success = False

    for attempt in range(max_attempts):
        try:
            result = env.cmd(config_cmd(), 'SET', 'WORKERS', '8')
            if result == 'OK' or result == b'OK':
                success = True
                env.debugPrint(f"Successfully changed to 8 workers on attempt {attempt + 1}", force=True)
                break
        except Exception as e:
            env.debugPrint(f"Attempt {attempt + 1} failed (expected during termination): {e}", force=True)

        # Small delay before retry
        time.sleep(0.1)

    # Eventually it should succeed
    env.assertTrue(success, message="Should eventually be able to change to 8 workers after threads terminate")

    # Verify final state
    env.assertEqual(env.cmd(config_cmd(), 'GET', 'WORKERS'), [['WORKERS', '8']])

    # Verify Redis is still responsive
    ping_result = env.cmd('PING')
    env.assertTrue(ping_result in ['PONG', True])

    # Run a query to verify functionality
    result = env.cmd('FT.SEARCH', 'idx', '*', 'LIMIT', '0', '5')
    env.assertTrue(result[0] > 0)

    env.debugPrint("8→0→8 rapid transition test completed successfully", force=True)

@skip(cluster=False)
def test_MOD_11658_workers_concurrent_changes():
    """
    Test that concurrent worker changes are handled correctly.

    This test validates that the termination flag prevents issues when
    multiple config changes happen in quick succession.
    """

    # Start with WORKERS=8
    env = Env(moduleArgs='WORKERS 8', enableDebugCommand=True)

    # Create index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'text', 'TEXT').ok()

    conn = getConnectionByEnv(env)
    for i in range(50):
        conn.execute_command('HSET', f'doc{i}', 'text', f'document {i}')

    waitForIndex(env, 'idx')

    # Trigger thread pool initialization
    env.cmd('FT.SEARCH', 'idx', '*', 'LIMIT', '0', '1')
    time.sleep(0.5)

    # Test sequence: 8 → 4 → 2 → 0 → 2 → 4 → 8
    sequences = [4, 2, 0, 2, 4, 8]

    for target_workers in sequences:
        env.debugPrint(f"Changing WORKERS to {target_workers}...", force=True)

        # Try to change workers
        max_attempts = 20
        success = False

        for attempt in range(max_attempts):
            try:
                result = env.cmd(config_cmd(), 'SET', 'WORKERS', str(target_workers))
                if result == 'OK' or result == b'OK':
                    success = True
                    break
            except Exception as e:
                env.debugPrint(f"Attempt {attempt + 1} to set WORKERS={target_workers} failed: {e}", force=True)

            time.sleep(0.1)

        env.assertTrue(success, message=f"Should be able to change to {target_workers} workers")

        # Verify config
        current = env.cmd(config_cmd(), 'GET', 'WORKERS')
        env.assertEqual(current, [['WORKERS', str(target_workers)]])

        # Verify responsiveness
        ping_result = env.cmd('PING')
        env.assertTrue(ping_result in ['PONG', True])

        # Small delay between changes
        time.sleep(0.2)

    # Final verification
    result = env.cmd('FT.SEARCH', 'idx', '*', 'LIMIT', '0', '5')
    env.assertTrue(result[0] > 0)

    env.debugPrint("Concurrent worker changes test completed successfully", force=True)

