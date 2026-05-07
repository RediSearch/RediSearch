import threading
import time
from common import *

def test_query_while_flush():
    """
    Test scenario:
    1. Create index1 with 100 documents
    2. Start threads that continuously query the index
    3. Call FLUSHALL command
    4. Create index2 and start querying it
    5. Verify that:
       - Before FLUSHALL: Errors=0, Successes>0
       - After FLUSHALL: Errors>0, Successes==0 (for old queries)
       - New index queries work properly
    """
    env = Env(moduleArgs='WORKERS 2')
    env.expect('FT.CREATE', 'index1', 'ON', 'HASH', 'SCHEMA', 'text', 'TEXT').ok()

    # Add 100 documents to index1
    for i in range(100):
        env.getClusterConnectionIfNeeded().execute_command('HSET', f'doc:{i}', 'text', f'hello world document {i}')

    # Wait for indexing to complete
    waitForIndex(env, 'index1')

    # Verify index1 is working
    result = env.cmd('FT.SEARCH', 'index1', 'hello')
    env.assertEqual(result[0], 100)  # Should find all 100 documents

    # Replace the flushall_called flag with a threading event
    flushall_called = threading.Event()

    # Statistics tracking
    stats = {
        'before_flush_errors': 0,
        'before_flush_successes': 0,
        'after_flush_errors': 0,
        'after_flush_successes': 0,
        'stop_queries': False,
        'flush_completed': False
    }

    # Per-thread iteration counters, incremented at the end of each loop iteration.
    # Used to deterministically drain in-flight iterations after toggling state
    # (flushall_called / flush_completed): once every counter has advanced by at
    # least one, every worker has re-evaluated the state and no stale attribution
    # to the pre-flush counters can still be pending.
    num_threads = 5
    iteration_counts = [0] * num_threads

    def query_worker(stats, thread_id):
        """Worker thread that continuously queries the index"""
        local_conn = env.getClusterConnectionIfNeeded()

        while not stats['stop_queries']:
          try:
              # Query index1
              local_conn.execute_command('FT.SEARCH', 'index1', 'hello')

              if not flushall_called.is_set():
                # Check if flush has completed
                if not stats['flush_completed']:
                    stats['before_flush_successes'] += 1
                else:
                    stats['after_flush_successes'] += 1

          except Exception as e:
              # Check if flush has completed
              if not flushall_called.is_set():
                if not stats['flush_completed']:
                    stats['before_flush_errors'] += 1
                else:
                    stats['after_flush_errors'] += 1

          iteration_counts[thread_id] += 1
          # Small delay to avoid overwhelming the system
          time.sleep(0.001)

    # Start query threads (pass the event)
    threads = []
    for i in range(num_threads):
        thread = threading.Thread(
            target=query_worker,
            args=(stats, i),
        )
        threads.append(thread)
        thread.start()

    # Wait until query threads have accumulated some successes
    wait_for_condition(
        lambda: (stats['before_flush_successes'] > 0, stats),
        message='no successful pre-flush queries observed',
        timeout=30,
    )

    # Signal that flushall is about to be called
    flushall_called.set()
    # Wait for in-flight pre-flush attributions to drain: every worker must complete
    # at least one loop iteration after flushall_called.set(), guaranteeing each has
    # re-evaluated flushall_called.is_set() at the increment site with the flag set.
    snap = list(iteration_counts)
    wait_for_condition(
        lambda: (
            all(c > s for c, s in zip(iteration_counts, snap)),
            {'snap': snap, 'cur': list(iteration_counts)},
        ),
        message='not all workers completed an iteration after flushall_called.set()',
        timeout=10,
    )
    env.assertGreater(stats['before_flush_successes'], 0)
    env.assertEqual(stats['before_flush_errors'], 0)

    # Execute FLUSHALL
    env.cmd('FLUSHALL')

    # Mark flush as completed
    stats['flush_completed'] = True
    # Wait for any thread that already passed the `flushall_called.is_set()` check but has
    # not yet read `flush_completed` to finish its iteration, so we don't misattribute a
    # post-flush observation to the before-flush bucket when we later clear the event.
    # Same drain purpose as above, expressed against the per-thread iteration counters.
    snap = list(iteration_counts)
    wait_for_condition(
        lambda: (
            all(c > s for c, s in zip(iteration_counts, snap)),
            {'snap': snap, 'cur': list(iteration_counts)},
        ),
        message='not all workers completed an iteration after flush_completed=True',
        timeout=10,
    )
    flushall_called.clear()  # Reset the event
    # Create index2 and verify it works properly
    env.expect('FT.CREATE', 'index2', 'ON', 'HASH', 'SCHEMA', 'text', 'TEXT').ok()

    # Add some documents to index2
    for i in range(10):
        env.getClusterConnectionIfNeeded().execute_command('HSET', f'newdoc:{i}', 'text', f'new document {i}')

    # Wait for indexing to complete
    waitForIndex(env, 'index2')

    # Verify index2 works properly
    result = env.cmd('FT.SEARCH', 'index2', 'new')
    env.assertEqual(result[0], 10)  # Should find all 10 new documents

        # Stop query threads
    stats['stop_queries'] = True

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

    # Verify statistics before flush
    env.assertEqual(stats['before_flush_errors'], 0,
                   message="Should have no errors before FLUSHALL")
    env.assertGreater(stats['before_flush_successes'], 0,
                     message="Should have successes before FLUSHALL")

    # Verify statistics after flush
    env.assertGreater(stats['after_flush_errors'], 0,
                     message="Should have errors after FLUSHALL")
    env.assertEqual(stats['after_flush_successes'], 0,
                   message="Should have no successes after FLUSHALL for old index")

    # Verify old index1 is gone
    env.expect('FT.SEARCH', 'index1', 'hello').error().contains('SEARCH_INDEX_NOT_FOUND Index not found')
    env.debugPrint(f"  Before FLUSHALL - Errors: {stats['before_flush_errors']}, Successes: {stats['before_flush_successes']}")
    env.debugPrint(f"  After FLUSHALL - Errors: {stats['after_flush_errors']}, Successes: {stats['after_flush_successes']}")
