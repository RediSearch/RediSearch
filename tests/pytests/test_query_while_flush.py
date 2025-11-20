import threading
import time
from common import *

@skip(cluster=True)
def test_query_while_flush(env):
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
    env.expect('FT.CREATE', 'index1', 'ON', 'HASH', 'SCHEMA', 'text', 'TEXT').ok()

    # Add 100 documents to index1
    for i in range(100):
        env.cmd('HSET', f'doc:{i}', 'text', f'hello world document {i}')

    # Wait for indexing to complete
    waitForIndex(env, 'index1')

    # Verify index1 is working
    result = env.cmd('FT.SEARCH', 'index1', 'hello')
    env.assertEqual(result[0], 100)  # Should find all 100 documents

    # Statistics tracking
    stats = {
        'before_flush_errors': 0,
        'before_flush_successes': 0,
        'after_flush_errors': 0,
        'after_flush_successes': 0,
        'stop_queries': False,
        'flush_completed': False,
        'flushall_called': False
    }

    def query_worker(stats):
        """Worker thread that continuously queries the index"""
        local_conn = env.getClusterConnectionIfNeeded()

        while not stats['stop_queries']:
          try:
              # Query index1
              local_conn.execute_command('FT.SEARCH', 'index1', 'hello')

              if not stats['flushall_called']:
                # Check if flush has completed
                if not stats['flush_completed']:
                    stats['before_flush_successes'] += 1
                else:
                    stats['after_flush_successes'] += 1

          except Exception as e:
              # Check if flush has completed
              if not stats['flushall_called']:
                if not stats['flush_completed']:
                    stats['before_flush_errors'] += 1
                else:
                    stats['after_flush_errors'] += 1

          # Small delay to avoid overwhelming the system
          time.sleep(0.001)

    # Start 5 query threads
    num_threads = 5
    threads = []
    for i in range(num_threads):
        thread = threading.Thread(
            target=query_worker,
            args=(stats, ),
            daemon=True
        )
        threads.append(thread)
        thread.start()

    # Let queries run for a bit to accumulate some successes
    time.sleep(0.5)

    # Verify we have some successes before flush
    stats['flushall_called'] = True
    time.sleep(0.5)
    env.assertGreater(stats['before_flush_successes'], 0)
    env.assertEqual(stats['before_flush_errors'], 0)

    # Execute FLUSHALL
    env.cmd('FLUSHALL')

    # Mark flush as completed
    stats['flush_completed'] = True
    stats['flushall_called'] = False

    # Create index2 and verify it works properly
    env.expect('FT.CREATE', 'index2', 'ON', 'HASH', 'SCHEMA', 'text', 'TEXT').ok()

    # Add some documents to index2
    for i in range(10):
        env.cmd('HSET', f'newdoc:{i}', 'text', f'new document {i}')

    # Wait for indexing to complete
    waitForIndex(env, 'index2')

    # Verify index2 works properly
    result = env.cmd('FT.SEARCH', 'index2', 'new')
    env.assertEqual(result[0], 10)  # Should find all 10 new documents

        # Stop query threads
    stats['stop_queries'] = True

    # Wait for all threads to complete
    for thread in threads:
        thread.join(timeout=2.0)

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
    env.expect('FT.SEARCH', 'index1', 'hello').error().contains('No such index')
    env.debugPrint(f"  Before FLUSHALL - Errors: {stats['before_flush_errors']}, Successes: {stats['before_flush_successes']}")
    env.debugPrint(f"  After FLUSHALL - Errors: {stats['after_flush_errors']}, Successes: {stats['after_flush_successes']}")
