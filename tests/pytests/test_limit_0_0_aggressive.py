"""
AGGRESSIVE test to reproduce LIMIT 0 0 crash with maximum race condition pressure.

This test uses:
- Very small document set (50 docs) for high collision rate
- Many threads (20 search, 10 delete) for maximum contention
- No delays between operations - continuous hammering
- Targets same 10 documents repeatedly
- Uses JSON documents (customer's use case)
- SORTBY to trigger optimizer heap path
"""

from RLTest import Env
from common import *
import threading
import time
import json


@skip(cluster=False)
def test_aggressive_limit_0_0_crash():
    """
    MAXIMUM AGGRESSION: Hammer LIMIT 0 0 with extreme concurrent deletion pressure.
    """
    env = Env(moduleArgs='MAXDOCTABLESIZE 10')  # Very small table = more collisions

    # Create index on JSON with sortable fields
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
               '$.num1', 'AS', 'num1', 'NUMERIC', 'SORTABLE',
               '$.num2', 'AS', 'num2', 'NUMERIC', 'SORTABLE',
               '$.num3', 'AS', 'num3', 'NUMERIC',
               '$.geo1', 'AS', 'geo1', 'GEO').ok()

    # VERY SMALL dataset - only 50 docs for maximum collision
    num_docs = 50
    with env.getClusterConnectionIfNeeded() as conn:
        for i in range(num_docs):
            doc = {
                'num1': i,
                'num2': i * 2,
                'num3': i * 3,
                'geo1': f'{(i % 180) - 90},{(i % 360) - 180}'
            }
            conn.execute_command('JSON.SET', f'doc{i}', '$', json.dumps(doc))

    time.sleep(0.2)

    errors = []
    stop_flag = threading.Event()
    counters = {'search': 0, 'delete': 0, 'sortby': 0}

    def hammer_limit_0_0():
        """Hammer LIMIT 0 0 with NO delays"""
        with env.getClusterConnectionIfNeeded() as conn:
            while not stop_flag.is_set() and counters['search'] < 5000:
                try:
                    conn.execute_command('FT.SEARCH', 'idx', '*', 'LIMIT', '0', '0')
                    counters['search'] += 1
                except Exception as e:
                    errors.append(('search', str(e)))
                    if 'crash' in str(e).lower() or 'segfault' in str(e).lower():
                        stop_flag.set()
                        break

    def hammer_sortby_limit_0_0():
        """Hammer SORTBY LIMIT 0 0 to trigger optimizer heap path"""
        with env.getClusterConnectionIfNeeded() as conn:
            while not stop_flag.is_set() and counters['sortby'] < 3000:
                try:
                    # SORTBY forces optimizer to use heap
                    conn.execute_command('FT.SEARCH', 'idx', '*', 'SORTBY', 'num1', 'ASC', 'LIMIT', '0', '0')
                    counters['sortby'] += 1
                except Exception as e:
                    errors.append(('sortby', str(e)))
                    if 'crash' in str(e).lower() or 'segfault' in str(e).lower():
                        stop_flag.set()
                        break

    def hammer_delete_same_docs():
        """Rapidly delete and re-add THE SAME 10 documents - maximum collision"""
        with env.getClusterConnectionIfNeeded() as conn:
            while not stop_flag.is_set() and counters['delete'] < 1000:
                try:
                    # Target only docs 0-9 for maximum contention
                    target_doc = counters['delete'] % 10
                    
                    # Delete
                    conn.execute_command('DEL', f'doc{target_doc}')
                    
                    # Immediately re-add
                    doc = {
                        'num1': target_doc,
                        'num2': target_doc * 2,
                        'num3': target_doc * 3,
                        'geo1': f'{(target_doc % 180) - 90},{(target_doc % 360) - 180}'
                    }
                    conn.execute_command('JSON.SET', f'doc{target_doc}', '$', json.dumps(doc))
                    
                    counters['delete'] += 1
                    # NO SLEEP - continuous hammering
                except Exception as e:
                    errors.append(('delete', str(e)))

    # MAXIMUM THREADS
    threads = []
    
    # 20 search threads
    for i in range(20):
        t = threading.Thread(target=hammer_limit_0_0, name=f'search-{i}')
        threads.append(t)
        t.start()
    
    # 10 sortby threads (optimizer path)
    for i in range(10):
        t = threading.Thread(target=hammer_sortby_limit_0_0, name=f'sortby-{i}')
        threads.append(t)
        t.start()
    
    # 10 delete threads
    for i in range(10):
        t = threading.Thread(target=hammer_delete_same_docs, name=f'delete-{i}')
        threads.append(t)
        t.start()

    # Run for 10 seconds or until crash
    time.sleep(10)
    stop_flag.set()

    # Wait for all threads
    for t in threads:
        t.join(timeout=2)

    print(f"\nCounters: search={counters['search']}, sortby={counters['sortby']}, delete={counters['delete']}")
    print(f"Total errors: {len(errors)}")
    
    if errors:
        print(f"First 10 errors: {errors[:10]}")
        # Check for crash-related errors
        crash_errors = [e for e in errors if 'crash' in str(e[1]).lower() or 'segfault' in str(e[1]).lower()]
        if crash_errors:
            env.assertTrue(False, message=f"CRASH DETECTED: {crash_errors}")

    env.expect('FT.DROP', 'idx').ok()


@skip(cluster=False)
def test_aggressive_with_timeout():
    """
    Aggressive test with timeout to force premature cleanup.
    """
    env = Env(moduleArgs='MAXDOCTABLESIZE 10 ON_TIMEOUT FAIL TIMEOUT 10')

    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
               '$.num1', 'AS', 'num1', 'NUMERIC', 'SORTABLE',
               '$.num2', 'AS', 'num2', 'NUMERIC').ok()

    # Small dataset
    num_docs = 30
    with env.getClusterConnectionIfNeeded() as conn:
        for i in range(num_docs):
            doc = {'num1': i, 'num2': i * 2}
            conn.execute_command('JSON.SET', f'doc{i}', '$', json.dumps(doc))

    time.sleep(0.1)

    errors = []
    stop_flag = threading.Event()

    def search_with_timeout():
        """Search with 1ms timeout to force cleanup during processing"""
        with env.getClusterConnectionIfNeeded() as conn:
            iteration = 0
            while not stop_flag.is_set() and iteration < 2000:
                try:
                    conn.execute_command('FT.SEARCH', 'idx', '*', 'SORTBY', 'num1', 
                                       'LIMIT', '0', '0', 'TIMEOUT', '1')
                    iteration += 1
                except Exception as e:
                    if 'timeout' not in str(e).lower():
                        errors.append(('timeout_search', str(e)))
                    if 'crash' in str(e).lower():
                        stop_flag.set()
                    iteration += 1

    def rapid_delete():
        """Delete same docs repeatedly"""
        with env.getClusterConnectionIfNeeded() as conn:
            iteration = 0
            while not stop_flag.is_set() and iteration < 500:
                try:
                    doc_id = iteration % 10
                    conn.execute_command('DEL', f'doc{doc_id}')
                    doc = {'num1': doc_id, 'num2': doc_id * 2}
                    conn.execute_command('JSON.SET', f'doc{doc_id}', '$', json.dumps(doc))
                    iteration += 1
                except Exception as e:
                    errors.append(('delete', str(e)))

    threads = []
    for _ in range(15):
        t = threading.Thread(target=search_with_timeout)
        threads.append(t)
        t.start()

    for _ in range(8):
        t = threading.Thread(target=rapid_delete)
        threads.append(t)
        t.start()

    time.sleep(8)
    stop_flag.set()

    for t in threads:
        t.join(timeout=2)

    critical_errors = [e for e in errors if 'timeout' not in str(e[1]).lower()]
    if critical_errors:
        print(f"Critical errors: {critical_errors[:10]}")

    env.expect('FT.DROP', 'idx').ok()

