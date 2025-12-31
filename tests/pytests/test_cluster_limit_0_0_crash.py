"""
Test to reproduce the crash in cluster mode when executing FT.SEARCH idx * LIMIT 0 0

This test targets the crash documented in docs/crash.log with the following stack trace:
  DocTable_Borrow (doc_table.c:72)
  rpidxNext (result_processor.c:139)
  rpcountNext (result_processor.c:1152)
  startPipeline (aggregate_exec.c:383)

Root Cause Analysis:
1. Race condition: Documents being deleted/modified while search operations access their metadata
2. Memory corruption/logic leak: When the optimizer collects results, it takes a reference to the
   Document Metadata (DMD) and adds it to its heap. If the iterator is freed while the heap is not
   empty (e.g., on timeout or error), the results are freed without returning the borrowed reference
   to the DMD, causing a logic leak and potential access to freed memory.
3. The crash occurs specifically when accessing dmd->flags on a potentially corrupted/freed DMD

Test Strategy:
- Target cluster mode specifically (crash confirmed in cluster, not verified in standalone)
- Use FT.SEARCH idx * LIMIT 0 0 as the triggering command
- Create concurrent operations that modify/delete documents during searches
- Focus on the result processor pipeline (rpidxNext -> rpcountNext -> DocTable_Borrow)
- Include timeout scenarios that might trigger premature cleanup
- Test with JSON documents (customer's use case)
"""

from RLTest import Env
from common import *
import threading
import time
import random
import json


@skip(cluster=False)
def test_limit_0_0_with_concurrent_deletes():
    """
    Basic test: FT.SEARCH idx * LIMIT 0 0 with concurrent document deletions.
    This is the exact command from the crash report.
    Uses JSON documents (customer's use case).
    """
    env = Env(moduleArgs='MAXDOCTABLESIZE 100')

    # Create index on JSON with numeric fields and GEO field
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
               '$.num1', 'AS', 'num1', 'NUMERIC',
               '$.num2', 'AS', 'num2', 'NUMERIC',
               '$.num3', 'AS', 'num3', 'NUMERIC',
               '$.num4', 'AS', 'num4', 'NUMERIC',
               '$.geo1', 'AS', 'geo1', 'GEO').ok()

    # Insert JSON documents across all shards
    num_docs = 500
    with env.getClusterConnectionIfNeeded() as conn:
        for i in range(num_docs):
            doc = {
                'num1': i,
                'num2': i * 2,
                'num3': i * 3,
                'num4': i * 4,
                'geo1': f'{(i % 180) - 90},{(i % 360) - 180}'
            }
            conn.execute_command('JSON.SET', f'doc{i}', '$', json.dumps(doc))

    time.sleep(0.3)  # Allow indexing to complete

    errors = []
    stop_flag = threading.Event()

    def search_limit_0_0():
        """Continuously run FT.SEARCH idx * LIMIT 0 0"""
        with env.getClusterConnectionIfNeeded() as conn:
            iteration = 0
            while not stop_flag.is_set() and iteration < 1000:
                try:
                    # This is the exact command that triggers the crash
                    result = conn.execute_command('FT.SEARCH', 'idx', '*', 'LIMIT', '0', '0')
                    # Result should be [count] with no documents
                    if not isinstance(result, list) or len(result) < 1:
                        errors.append(('search', f'Unexpected result format: {result}'))
                    iteration += 1
                except Exception as e:
                    errors.append(('search', str(e)))
                    if 'crash' in str(e).lower() or 'segfault' in str(e).lower():
                        stop_flag.set()

    def delete_and_readd():
        """Delete and re-add JSON documents to create race conditions"""
        with env.getClusterConnectionIfNeeded() as conn:
            iteration = 0
            while not stop_flag.is_set() and iteration < 200:
                try:
                    # Delete a batch of documents
                    for i in range(iteration * 5, (iteration + 1) * 5):
                        doc_id = i % num_docs
                        conn.execute_command('DEL', f'doc{doc_id}')

                    # Immediately re-add them as JSON
                    for i in range(iteration * 5, (iteration + 1) * 5):
                        doc_id = i % num_docs
                        doc = {
                            'num1': doc_id,
                            'num2': doc_id * 2,
                            'num3': doc_id * 3,
                            'num4': doc_id * 4,
                            'geo1': f'{(doc_id % 180) - 90},{(doc_id % 360) - 180}'
                        }
                        conn.execute_command('JSON.SET', f'doc{doc_id}', '$', json.dumps(doc))
                    iteration += 1
                    time.sleep(0.01)  # Small delay to allow searches to interleave
                except Exception as e:
                    errors.append(('delete', str(e)))

    # Start multiple search threads and delete threads
    threads = []
    for _ in range(4):
        t = threading.Thread(target=search_limit_0_0)
        threads.append(t)
        t.start()

    for _ in range(2):
        t = threading.Thread(target=delete_and_readd)
        threads.append(t)
        t.start()

    # Wait for all threads to complete
    for t in threads:
        t.join(timeout=30)

    stop_flag.set()

    # Check for errors
    if errors:
        env.assertTrue(False, message=f"Encountered {len(errors)} errors: {errors[:5]}")

    env.expect('FT.DROP', 'idx').ok()


@skip(cluster=False)
def test_limit_0_0_with_timeout():
    """
    Test LIMIT 0 0 with timeout to trigger premature iterator cleanup.
    This tests the scenario where the iterator is freed while the heap still has borrowed DMD references.
    Uses JSON documents (customer's use case).
    """
    env = Env(moduleArgs='MAXDOCTABLESIZE 50 ON_TIMEOUT FAIL TIMEOUT 10')

    # Create index on JSON with numeric and GEO fields
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
               '$.num1', 'AS', 'num1', 'NUMERIC',
               '$.num2', 'AS', 'num2', 'NUMERIC',
               '$.num3', 'AS', 'num3', 'NUMERIC',
               '$.num4', 'AS', 'num4', 'NUMERIC',
               '$.geo1', 'AS', 'geo1', 'GEO').ok()

    # Insert many JSON documents to increase processing time
    num_docs = 2000
    with env.getClusterConnectionIfNeeded() as conn:
        for i in range(num_docs):
            doc = {
                'num1': i,
                'num2': i * 2,
                'num3': i * 3,
                'num4': i * 4,
                'geo1': f'{(i % 180) - 90},{(i % 360) - 180}'
            }
            conn.execute_command('JSON.SET', f'doc{i}', '$', json.dumps(doc))

    time.sleep(0.5)

    errors = []
    stop_flag = threading.Event()

    def search_with_short_timeout():
        """Run searches with very short timeout to trigger cleanup during processing"""
        with env.getClusterConnectionIfNeeded() as conn:
            iteration = 0
            while not stop_flag.is_set() and iteration < 500:
                try:
                    # Use TIMEOUT parameter to force timeout during processing
                    conn.execute_command('FT.SEARCH', 'idx', '*', 'LIMIT', '0', '0', 'TIMEOUT', '1')  # 1ms timeout
                    iteration += 1
                except Exception as e:
                    # Timeout errors are expected, but crashes are not
                    if 'timeout' not in str(e).lower():
                        errors.append(('search_timeout', str(e)))
                    if 'crash' in str(e).lower() or 'segfault' in str(e).lower():
                        stop_flag.set()
                    iteration += 1

    def concurrent_deletes():
        """Delete documents while searches are timing out"""
        with env.getClusterConnectionIfNeeded() as conn:
            iteration = 0
            while not stop_flag.is_set() and iteration < 100:
                try:
                    for i in range(10):
                        doc_id = (iteration * 10 + i) % num_docs
                        conn.execute_command('DEL', f'doc{doc_id}')
                    iteration += 1
                    time.sleep(0.02)
                except Exception as e:
                    errors.append(('delete', str(e)))

    # Start threads
    threads = []
    for _ in range(6):
        t = threading.Thread(target=search_with_short_timeout)
        threads.append(t)
        t.start()

    for _ in range(2):
        t = threading.Thread(target=concurrent_deletes)
        threads.append(t)
        t.start()

    for t in threads:
        t.join(timeout=30)

    stop_flag.set()

    # Filter out expected timeout errors
    critical_errors = [e for e in errors if 'timeout' not in str(e[1]).lower()]
    if critical_errors:
        env.assertTrue(False, message=f"Encountered {len(critical_errors)} critical errors: {critical_errors[:5]}")

    env.expect('FT.DROP', 'idx').ok()


@skip(cluster=False)
def test_limit_0_0_extreme_loop():
    """
    Extreme stress test: Rapid-fire LIMIT 0 0 queries with continuous document churn.
    This maximizes the chance of hitting the race condition.
    Uses JSON documents (customer's use case).
    """
    env = Env(moduleArgs='MAXDOCTABLESIZE 30')

    # Create index on JSON
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
               '$.num1', 'AS', 'num1', 'NUMERIC',
               '$.num2', 'AS', 'num2', 'NUMERIC',
               '$.num3', 'AS', 'num3', 'NUMERIC',
               '$.num4', 'AS', 'num4', 'NUMERIC',
               '$.geo1', 'AS', 'geo1', 'GEO').ok()

    # Smaller dataset for faster churn
    num_docs = 300
    with env.getClusterConnectionIfNeeded() as conn:
        for i in range(num_docs):
            doc = {
                'num1': i,
                'num2': i * 2,
                'num3': i * 3,
                'num4': i * 4,
                'geo1': f'{(i % 180) - 90},{(i % 360) - 180}'
            }
            conn.execute_command('JSON.SET', f'doc{i}', '$', json.dumps(doc))

    time.sleep(0.2)

    errors = []
    stop_flag = threading.Event()
    iterations = {'search': 0, 'delete': 0}

    def hammer_search():
        """Hammer with LIMIT 0 0 queries - no delays"""
        with env.getClusterConnectionIfNeeded() as conn:
            while not stop_flag.is_set() and iterations['search'] < 2000:
                try:
                    conn.execute_command('FT.SEARCH', 'idx', '*', 'LIMIT', '0', '0')
                    iterations['search'] += 1
                except Exception as e:
                    errors.append(('search', str(e)))
                    if 'crash' in str(e).lower() or 'segfault' in str(e).lower():
                        stop_flag.set()

    def hammer_delete():
        """Rapidly delete and re-add JSON documents - no delays"""
        with env.getClusterConnectionIfNeeded() as conn:
            while not stop_flag.is_set() and iterations['delete'] < 500:
                try:
                    # Delete a batch
                    batch_start = (iterations['delete'] * 10) % num_docs
                    for i in range(batch_start, batch_start + 10):
                        conn.execute_command('DEL', f'doc{i % num_docs}')

                    # Immediately re-add as JSON
                    for i in range(batch_start, batch_start + 10):
                        doc_id = i % num_docs
                        doc = {
                            'num1': doc_id,
                            'num2': doc_id * 2,
                            'num3': doc_id * 3,
                            'num4': doc_id * 4,
                            'geo1': f'{(doc_id % 180) - 90},{(doc_id % 360) - 180}'
                        }
                        conn.execute_command('JSON.SET', f'doc{doc_id}', '$', json.dumps(doc))
                    iterations['delete'] += 1
                except Exception as e:
                    errors.append(('delete', str(e)))

    # Many threads for maximum contention
    threads = []
    for _ in range(8):
        t = threading.Thread(target=hammer_search)
        threads.append(t)
        t.start()

    for _ in range(4):
        t = threading.Thread(target=hammer_delete)
        threads.append(t)
        t.start()

    for t in threads:
        t.join(timeout=30)

    stop_flag.set()

    if errors:
        env.assertTrue(False, message=f"Encountered {len(errors)} errors after {iterations['search']} searches: {errors[:5]}")

    env.expect('FT.DROP', 'idx').ok()


@skip(cluster=False)
def test_limit_0_0_with_sortby_optimizer():
    """
    Test LIMIT 0 0 with SORTBY to trigger the optimizer path.
    The optimizer collects results into a heap with borrowed DMD references.
    If the iterator is freed prematurely (timeout/error), the heap may not be properly cleaned.
    Uses JSON documents (customer's use case).
    """
    env = Env(moduleArgs='MAXDOCTABLESIZE 50')

    # Create index on JSON with sortable numeric fields
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
               '$.num1', 'AS', 'num1', 'NUMERIC', 'SORTABLE',
               '$.num2', 'AS', 'num2', 'NUMERIC', 'SORTABLE',
               '$.num3', 'AS', 'num3', 'NUMERIC',
               '$.num4', 'AS', 'num4', 'NUMERIC',
               '$.geo1', 'AS', 'geo1', 'GEO').ok()

    num_docs = 1000
    with env.getClusterConnectionIfNeeded() as conn:
        for i in range(num_docs):
            doc = {
                'num1': i,
                'num2': i * 2,
                'num3': i * 3,
                'num4': i * 4,
                'geo1': f'{(i % 180) - 90},{(i % 360) - 180}'
            }
            conn.execute_command('JSON.SET', f'doc{i}', '$', json.dumps(doc))

    time.sleep(0.3)

    errors = []
    stop_flag = threading.Event()

    def search_with_sortby():
        """Search with SORTBY to trigger optimizer heap allocation"""
        with env.getClusterConnectionIfNeeded() as conn:
            iteration = 0
            while not stop_flag.is_set() and iteration < 1000:
                try:
                    # SORTBY triggers the optimizer which uses a heap to collect results
                    # Even with LIMIT 0 0, the optimizer may collect results before limiting
                    conn.execute_command('FT.SEARCH', 'idx', '*', 'SORTBY', 'num1', 'ASC', 'LIMIT', '0', '0')
                    iteration += 1
                except Exception as e:
                    errors.append(('search_sortby', str(e)))
                    if 'crash' in str(e).lower() or 'segfault' in str(e).lower():
                        stop_flag.set()

    def search_with_sortby_and_timeout():
        """Search with SORTBY and timeout to trigger cleanup during heap processing"""
        with env.getClusterConnectionIfNeeded() as conn:
            iteration = 0
            while not stop_flag.is_set() and iteration < 500:
                try:
                    conn.execute_command('FT.SEARCH', 'idx', '*', 'SORTBY', 'num2', 'DESC',
                                       'LIMIT', '0', '0', 'TIMEOUT', '1')
                    iteration += 1
                except Exception as e:
                    # Timeout is expected
                    if 'timeout' not in str(e).lower():
                        errors.append(('search_sortby_timeout', str(e)))
                    if 'crash' in str(e).lower() or 'segfault' in str(e).lower():
                        stop_flag.set()
                    iteration += 1

    def delete_documents():
        """Delete documents while optimizer is collecting results"""
        with env.getClusterConnectionIfNeeded() as conn:
            iteration = 0
            while not stop_flag.is_set() and iteration < 150:
                try:
                    for i in range(10):
                        doc_id = (iteration * 10 + i) % num_docs
                        conn.execute_command('DEL', f'doc{doc_id}')
                    iteration += 1
                    time.sleep(0.02)
                except Exception as e:
                    errors.append(('delete', str(e)))

    # Start threads
    threads = []
    for _ in range(4):
        t = threading.Thread(target=search_with_sortby)
        threads.append(t)
        t.start()

    for _ in range(3):
        t = threading.Thread(target=search_with_sortby_and_timeout)
        threads.append(t)
        t.start()

    for _ in range(2):
        t = threading.Thread(target=delete_documents)
        threads.append(t)
        t.start()

    for t in threads:
        t.join(timeout=30)

    stop_flag.set()

    # Filter out expected timeout errors
    critical_errors = [e for e in errors if 'timeout' not in str(e[1]).lower()]
    if critical_errors:
        env.assertTrue(False, message=f"Encountered {len(critical_errors)} critical errors: {critical_errors[:5]}")

    env.expect('FT.DROP', 'idx').ok()


@skip(cluster=False)
def test_force_memory_reuse_crash():
    """
    Test that forces memory reuse patterns that could expose use-after-free bugs.
    Uses a very small MAXDOCTABLESIZE to force hash collisions and chain traversal.
    Uses JSON documents (customer's use case).
    """
    env = Env(moduleArgs='MAXDOCTABLESIZE 10')

    # Create index on JSON
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
               '$.num1', 'AS', 'num1', 'NUMERIC',
               '$.num2', 'AS', 'num2', 'NUMERIC',
               '$.num3', 'AS', 'num3', 'NUMERIC',
               '$.num4', 'AS', 'num4', 'NUMERIC',
               '$.geo1', 'AS', 'geo1', 'GEO').ok()

    # Many documents with small table size = many hash collisions
    num_docs = 200
    with env.getClusterConnectionIfNeeded() as conn:
        for i in range(num_docs):
            doc = {
                'num1': i,
                'num2': i * 2,
                'num3': i * 3,
                'num4': i * 4,
                'geo1': f'{(i % 180) - 90},{(i % 360) - 180}'
            }
            conn.execute_command('JSON.SET', f'doc{i}', '$', json.dumps(doc))

    time.sleep(0.2)

    errors = []
    stop_flag = threading.Event()

    def rapid_search():
        """Rapid searches to access DMD during chain traversal"""
        with env.getClusterConnectionIfNeeded() as conn:
            iteration = 0
            while not stop_flag.is_set() and iteration < 1500:
                try:
                    conn.execute_command('FT.SEARCH', 'idx', '*', 'LIMIT', '0', '0')
                    iteration += 1
                except Exception as e:
                    errors.append(('search', str(e)))
                    if 'crash' in str(e).lower():
                        stop_flag.set()

    def rapid_churn():
        """Rapid delete/add JSON documents to force DMD allocation/deallocation"""
        with env.getClusterConnectionIfNeeded() as conn:
            iteration = 0
            while not stop_flag.is_set() and iteration < 300:
                try:
                    # Delete and immediately re-add to force memory reuse
                    doc_id = iteration % num_docs
                    conn.execute_command('DEL', f'doc{doc_id}')
                    doc = {
                        'num1': doc_id,
                        'num2': doc_id * 2,
                        'num3': doc_id * 3,
                        'num4': doc_id * 4,
                        'geo1': f'{(doc_id % 180) - 90},{(doc_id % 360) - 180}'
                    }
                    conn.execute_command('JSON.SET', f'doc{doc_id}', '$', json.dumps(doc))
                    iteration += 1
                except Exception as e:
                    errors.append(('churn', str(e)))

    # Maximum threads for maximum contention
    threads = []
    for _ in range(10):
        t = threading.Thread(target=rapid_search)
        threads.append(t)
        t.start()

    for _ in range(5):
        t = threading.Thread(target=rapid_churn)
        threads.append(t)
        t.start()

    for t in threads:
        t.join(timeout=30)

    stop_flag.set()

    if errors:
        env.assertTrue(False, message=f"Encountered {len(errors)} errors: {errors[:5]}")

    env.expect('FT.DROP', 'idx').ok()

