"""
Flow test for vecsim_disk - tests disk-based HNSW vector indexes through Redis.

This test verifies the full integration path:
  Redis → RediSearch → redisearch_disk (Rust) → vecsim_disk (C++) → HNSWDiskIndex

Prerequisites:
  - Redis with bigredis support and SpeedB driver (bs_speedb.so)
  - See vecsim_disk/STRUCTURE.md for build instructions

Run from flow-tests directory:
    python runtests.py -t test_vecsim_disk.py
"""

import random
import struct
import time

import pytest

from vecsim_common import create_float32_vector, get_vecsim_info


def test_create_hnsw_disk_index(redis_env):
    """Test creating an HNSW disk index with all required parameters."""
    conn = redis_env.getConnection()

    # Disk mode requires: M, EF_CONSTRUCTION, EF_RUNTIME, RERANK (no defaults allowed)
    result = conn.execute_command(
        'FT.CREATE', 'idx', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA',
        'vec', 'VECTOR', 'HNSW', '13',
        'TYPE', 'FLOAT32',
        'DIM', '4',
        'DISTANCE_METRIC', 'L2',
        'M', '16',
        'EF_CONSTRUCTION', '200',
        'EF_RUNTIME', '10',
        'RERANK'
    )
    assert result == 'OK', f"FT.CREATE failed: {result}"

    # Verify the index is on disk
    info = get_vecsim_info(conn, 'idx', 'vec')
    assert info.get('IS_DISK') == 1, f"Expected IS_DISK=1, got {info.get('IS_DISK')}"


def test_add_vectors_and_knn_query(redis_env):
    """Test adding vectors and performing KNN queries with disk-based HNSW."""
    conn = redis_env.getConnection()

    # Create index with all required disk parameters
    conn.execute_command(
        'FT.CREATE', 'idx', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA',
        'vec', 'VECTOR', 'HNSW', '13',
        'TYPE', 'FLOAT32',
        'DIM', '4',
        'DISTANCE_METRIC', 'L2',
        'M', '16',
        'EF_CONSTRUCTION', '200',
        'EF_RUNTIME', '10',
        'RERANK'
    )

    # Add vectors
    vectors = [
        ('doc1', [1.0, 0.0, 0.0, 0.0]),
        ('doc2', [0.0, 1.0, 0.0, 0.0]),
        ('doc3', [0.0, 0.0, 1.0, 0.0]),
        ('doc4', [1.0, 1.0, 0.0, 0.0]),
    ]
    for doc_id, vec in vectors:
        conn.execute_command('HSET', doc_id, 'vec', create_float32_vector(vec))

    # KNN query - find 2 nearest neighbors to [1,0,0,0]
    query_vec = create_float32_vector([1.0, 0.0, 0.0, 0.0])
    result = conn.execute_command(
        'FT.SEARCH', 'idx', '*=>[KNN 2 @vec $blob AS dist]',
        'PARAMS', '2', 'blob', query_vec,
        'SORTBY', 'dist',
        'RETURN', '1', 'dist',
        'DIALECT', '2'
    )

    # Verify we get 2 results (KNN 2 requested)
    count = result[0]
    assert count == 2, f"Expected 2 results, got {count}"

    # Verify doc1 is first (exact match with distance 0)
    # Result may be str or bytes depending on redis-py version
    first_doc = result[1]
    if isinstance(first_doc, bytes):
        first_doc = first_doc.decode()
    assert first_doc == 'doc1', f"Expected doc1 first, got {first_doc}"

@pytest.mark.skip('flaky')
def test_delete_vector_and_requery(redis_env):
    """Test that deleted vectors are excluded from query results."""
    conn = redis_env.getConnection()

    # Create index with all required disk parameters
    conn.execute_command(
        'FT.CREATE', 'idx', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA',
        'vec', 'VECTOR', 'HNSW', '13',
        'TYPE', 'FLOAT32',
        'DIM', '4',
        'DISTANCE_METRIC', 'L2',
        'M', '16',
        'EF_CONSTRUCTION', '200',
        'EF_RUNTIME', '10',
        'RERANK'
    )

    # Add vectors
    vectors = [
        ('doc1', [1.0, 0.0, 0.0, 0.0]),
        ('doc2', [0.0, 1.0, 0.0, 0.0]),
        ('doc4', [1.0, 1.0, 0.0, 0.0]),
    ]
    for doc_id, vec in vectors:
        conn.execute_command('HSET', doc_id, 'vec', create_float32_vector(vec))

    query_vec = create_float32_vector([1.0, 0.0, 0.0, 0.0])

    # Delete doc1 (the exact match)
    conn.execute_command('DEL', 'doc1')

    # Query again - doc4 should now be first (L2 distance = 1.0)
    result = conn.execute_command(
        'FT.SEARCH', 'idx', '*=>[KNN 2 @vec $blob AS dist]',
        'PARAMS', '2', 'blob', query_vec,
        'SORTBY', 'dist',
        'RETURN', '1', 'dist',
        'DIALECT', '2'
    )

    first_doc = result[1].decode() if isinstance(result[1], bytes) else result[1]
    assert first_doc == 'doc4', f"Expected doc4 after delete, got {first_doc}"


def test_drop_index(redis_env):
    """Test dropping an HNSW disk index."""
    conn = redis_env.getConnection()

    # Create index with all required disk parameters
    conn.execute_command(
        'FT.CREATE', 'idx', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA',
        'vec', 'VECTOR', 'HNSW', '13',
        'TYPE', 'FLOAT32',
        'DIM', '4',
        'DISTANCE_METRIC', 'L2',
        'M', '16',
        'EF_CONSTRUCTION', '200',
        'EF_RUNTIME', '10',
        'RERANK'
    )

    # Drop index with DD (delete documents)
    result = conn.execute_command('FT.DROPINDEX', 'idx', 'DD')
    assert result == 'OK', f"FT.DROPINDEX failed: {result}"


def test_create_disk_index_rejects_flat_algorithm(redis_env):
    """Test that FLAT algorithm is rejected in disk mode."""
    conn = redis_env.getConnection()

    with pytest.raises(Exception) as exc_info:
        conn.execute_command(
            'FT.CREATE', 'idx', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA',
            'vec', 'VECTOR', 'FLAT', '6',
            'TYPE', 'FLOAT32',
            'DIM', '4',
            'DISTANCE_METRIC', 'L2'
        )
    assert 'Disk index does not support FLAT algorithm' in str(exc_info.value)


def test_create_disk_index_rejects_float64(redis_env):
    """Test that FLOAT64 type is rejected in disk mode."""
    conn = redis_env.getConnection()

    with pytest.raises(Exception) as exc_info:
        conn.execute_command(
            'FT.CREATE', 'idx', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA',
            'vec', 'VECTOR', 'HNSW', '13',
            'TYPE', 'FLOAT64',
            'DIM', '4',
            'DISTANCE_METRIC', 'L2',
            'M', '16',
            'EF_CONSTRUCTION', '200',
            'EF_RUNTIME', '10',
            'RERANK'
        )
    assert 'Disk index does not support FLOAT64 vector type' in str(exc_info.value)


def test_create_disk_index_requires_m_parameter(redis_env):
    """Test that M parameter is mandatory in disk mode (has default in RAM)."""
    conn = redis_env.getConnection()

    with pytest.raises(Exception) as exc_info:
        conn.execute_command(
            'FT.CREATE', 'idx', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA',
            'vec', 'VECTOR', 'HNSW', '11',
            'TYPE', 'FLOAT32',
            'DIM', '4',
            'DISTANCE_METRIC', 'L2',
            'EF_CONSTRUCTION', '200',
            'EF_RUNTIME', '10',
            'RERANK'
        )
    assert 'Disk HNSW index requires M parameter' in str(exc_info.value)


def test_create_disk_index_requires_ef_construction(redis_env):
    """Test that EF_CONSTRUCTION is mandatory in disk mode (has default in RAM)."""
    conn = redis_env.getConnection()

    with pytest.raises(Exception) as exc_info:
        conn.execute_command(
            'FT.CREATE', 'idx', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA',
            'vec', 'VECTOR', 'HNSW', '11',
            'TYPE', 'FLOAT32',
            'DIM', '4',
            'DISTANCE_METRIC', 'L2',
            'M', '16',
            'EF_RUNTIME', '10',
            'RERANK'
        )
    assert 'Disk HNSW index requires EF_CONSTRUCTION parameter' in str(exc_info.value)


def test_create_disk_index_requires_ef_runtime(redis_env):
    """Test that EF_RUNTIME is mandatory in disk mode (has default in RAM)."""
    conn = redis_env.getConnection()

    with pytest.raises(Exception) as exc_info:
        conn.execute_command(
            'FT.CREATE', 'idx', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA',
            'vec', 'VECTOR', 'HNSW', '11',
            'TYPE', 'FLOAT32',
            'DIM', '4',
            'DISTANCE_METRIC', 'L2',
            'M', '16',
            'EF_CONSTRUCTION', '200',
            'RERANK'
        )
    assert 'Disk HNSW index requires EF_RUNTIME parameter' in str(exc_info.value)


def test_create_disk_index_requires_rerank(redis_env):
    """Test that RERANK parameter is mandatory in disk mode."""
    conn = redis_env.getConnection()

    with pytest.raises(Exception) as exc_info:
        conn.execute_command(
            'FT.CREATE', 'idx', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA',
            'vec', 'VECTOR', 'HNSW', '12',
            'TYPE', 'FLOAT32',
            'DIM', '4',
            'DISTANCE_METRIC', 'L2',
            'M', '16',
            'EF_CONSTRUCTION', '200',
            'EF_RUNTIME', '10'
        )
    assert 'Disk HNSW index requires RERANK parameter' in str(exc_info.value)


@pytest.mark.skip(reason="Long-running test for memory analysis - run manually")
def test_memory_usage_after_deletes(redis_env):
    """Test that memory usage decreases after bulk deletes.

    This test verifies that the data structure shrinking mechanisms work correctly:
    - holes_ shrinks via shrink_to_fit()
    - labelToIdLookup_ rehashes when load_factor drops below 25%
    - idToMetaData and nodeLocks_ shrink via shrinkUnusedCapacity()
    - vectors container shrinks via removeElement() for tail deletes
    """
    conn = redis_env.getConnection()

    # Create index with higher dimension for meaningful memory usage
    dim = 128
    conn.execute_command(
        'FT.CREATE', 'idx', 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA',
        'vec', 'VECTOR', 'HNSW', '13',
        'TYPE', 'FLOAT32',
        'DIM', str(dim),
        'DISTANCE_METRIC', 'L2',
        'M', '16',
        'EF_CONSTRUCTION', '200',
        'EF_RUNTIME', '10',
        'RERANK'
    )

    import threading

    # Background monitoring thread
    stop_monitor = threading.Event()
    monitor_data = {'phase': 'INIT', 'memory_before': 0}

    def monitor_memory():
        start_time = time.time()
        print(f"\n{'='*80}")
        print(f"{'TIME':>6} | {'PHASE':<12} | {'MEMORY':>12} | {'INDEX_SIZE':>10} | {'DROP':>8}")
        print(f"{'='*80}")
        while not stop_monitor.is_set():
            try:
                info = get_vecsim_info(conn, 'idx', 'vec')
                mem = info.get('MEMORY', 0)
                idx_size = info.get('INDEX_SIZE', 0)
                elapsed = time.time() - start_time
                phase = monitor_data['phase']
                mem_before = monitor_data['memory_before']
                if mem_before > 0:
                    drop_pct = f"{(mem_before - mem) / mem_before * 100:.1f}%"
                else:
                    drop_pct = "N/A"
                print(f"{elapsed:>5.1f}s | {phase:<12} | {mem:>12,} | {idx_size:>10} | {drop_pct:>8}")
            except Exception:
                pass
            time.sleep(0.2)

    monitor_thread = threading.Thread(target=monitor_memory, daemon=True)
    monitor_thread.start()

    # Add many vectors
    n_vectors = 20000
    monitor_data['phase'] = 'INSERTING'
    for i in range(n_vectors):
        vec = [float(i % 100) / 100.0] * dim
        conn.execute_command('HSET', f'doc{i}', 'vec', create_float32_vector(vec))

    # Wait for async indexing to complete
    monitor_data['phase'] = 'INDEX_WAIT'
    while True:
        info = get_vecsim_info(conn, 'idx', 'vec')
        if info.get('INDEX_SIZE', 0) == n_vectors:
            break
        time.sleep(0.5)

    info_before = get_vecsim_info(conn, 'idx', 'vec')
    memory_before = info_before.get('MEMORY', 0)
    index_size_before = info_before.get('INDEX_SIZE', 0)
    monitor_data['memory_before'] = memory_before

    assert index_size_before == n_vectors, f"Expected {n_vectors} vectors, got {index_size_before}"
    assert memory_before > 0, "Memory should be positive after adding vectors"

    # Delete most vectors (90%)
    n_delete = int(n_vectors * 0.9)
    expected_remaining = n_vectors - n_delete
    monitor_data['phase'] = 'DELETING'
    for i in range(n_delete):
        conn.execute_command('DEL', f'doc{i}')

    # Wait for async delete finalization
    monitor_data['phase'] = 'DELETE_WAIT'
    while True:
        info = get_vecsim_info(conn, 'idx', 'vec')
        if info.get('INDEX_SIZE', 0) == expected_remaining:
            break
        time.sleep(0.5)

    # Let it settle
    time.sleep(2)
    monitor_data['phase'] = 'DONE'
    time.sleep(1)

    stop_monitor.set()
    monitor_thread.join(timeout=2)
    print(f"{'='*80}\n")

    info_after = get_vecsim_info(conn, 'idx', 'vec')
    memory_after = info_after.get('MEMORY', 0)
    index_size_after = info_after.get('INDEX_SIZE', 0)

    expected_remaining = n_vectors - n_delete
    assert index_size_after == expected_remaining, \
        f"Expected {expected_remaining} vectors, got {index_size_after}"

    # Memory should decrease by at least 70% after deleting 90% of vectors
    # Not exactly 90% because: fixed overhead, HNSW graph edges, storage metadata
    # This verifies our shrinking mechanisms work (holes_, labelToIdLookup_, vectors, etc.)
    memory_drop_ratio = (memory_before - memory_after) / memory_before
    assert memory_drop_ratio >= 0.70, \
        f"Expected at least 70% memory drop, got {memory_drop_ratio*100:.1f}% " \
        f"(before={memory_before}, after={memory_after})"

    # Verify search still works on remaining vectors
    query_vec = create_float32_vector([float(n_delete % 100) / 100.0] * dim)
    result = conn.execute_command(
        'FT.SEARCH', 'idx', '*=>[KNN 5 @vec $blob AS dist]',
        'PARAMS', '2', 'blob', query_vec,
        'SORTBY', 'dist',
        'RETURN', '1', 'dist',
        'DIALECT', '2'
    )
    count = result[0]
    assert count >= 1, f"Expected at least 1 result, got {count}"



# ============================================================================
# Delete Flow Tests - MOD-13797
# ============================================================================


def test_delete_nonexistent_label(new_hnsw_disk_index):
    """Test that deleting a non-existent label returns 0 and causes no error."""
    conn = new_hnsw_disk_index

    # Add one vector
    conn.execute_command('HSET', 'doc1', 'vec', create_float32_vector([1.0, 0.0, 0.0, 0.0]))

    # Delete non-existent document - should return 0
    result = conn.execute_command('DEL', 'nonexistent')
    assert result == 0, f"Expected 0 for deleting nonexistent key, got {result}"

    # Original doc should still be searchable
    query_vec = create_float32_vector([1.0, 0.0, 0.0, 0.0])
    result = conn.execute_command(
        'FT.SEARCH', 'idx', '*=>[KNN 1 @vec $blob AS dist]',
        'PARAMS', '2', 'blob', query_vec,
        'SORTBY', 'dist',
        'RETURN', '1', 'dist',
        'DIALECT', '2'
    )
    assert result[0] == 1, f"Expected 1 result, got {result[0]}"


def test_delete_idempotent(new_hnsw_disk_index):
    """Test that deleting the same label twice is idempotent - second delete returns 0."""
    conn = new_hnsw_disk_index

    conn.execute_command('HSET', 'doc1', 'vec', create_float32_vector([1.0, 0.0, 0.0, 0.0]))

    # First delete - should return 1
    result1 = conn.execute_command('DEL', 'doc1')
    assert result1 == 1, f"Expected 1 for first delete, got {result1}"

    # Second delete - should return 0 (key no longer exists)
    result2 = conn.execute_command('DEL', 'doc1')
    assert result2 == 0, f"Expected 0 for second delete, got {result2}"


def test_delete_all_vectors(new_hnsw_disk_index):
    """Test that deleting all vectors results in empty search results."""
    conn = new_hnsw_disk_index

    # Add several vectors
    for i in range(10):
        vec = [float(i) / 10.0, 0.0, 0.0, 0.0]
        conn.execute_command('HSET', f'doc{i}', 'vec', create_float32_vector(vec))

    # Delete all vectors
    for i in range(10):
        conn.execute_command('DEL', f'doc{i}')

    # Wait for async delete finalization
    time.sleep(2)

    # Search should return 0 results
    query_vec = create_float32_vector([0.5, 0.0, 0.0, 0.0])
    result = conn.execute_command(
        'FT.SEARCH', 'idx', '*=>[KNN 5 @vec $blob AS dist]',
        'PARAMS', '2', 'blob', query_vec,
        'SORTBY', 'dist',
        'RETURN', '1', 'dist',
        'DIALECT', '2'
    )
    assert result[0] == 0, f"Expected 0 results after deleting all vectors, got {result[0]}"



def test_delete_then_insert_same_label(new_hnsw_disk_index):
    """Test that deleting a vector and re-inserting with same label works (ID reuse)."""
    conn = new_hnsw_disk_index

    # Insert original vector
    conn.execute_command('HSET', 'doc1', 'vec', create_float32_vector([1.0, 0.0, 0.0, 0.0]))

    # Delete it
    conn.execute_command('DEL', 'doc1')
    time.sleep(1)  # Wait for async finalization

    # Re-insert with different vector
    conn.execute_command('HSET', 'doc1', 'vec', create_float32_vector([0.0, 1.0, 0.0, 0.0]))

    # Query - should find the new vector
    query_vec = create_float32_vector([0.0, 1.0, 0.0, 0.0])
    result = conn.execute_command(
        'FT.SEARCH', 'idx', '*=>[KNN 1 @vec $blob AS dist]',
        'PARAMS', '2', 'blob', query_vec,
        'SORTBY', 'dist',
        'RETURN', '1', 'dist',
        'DIALECT', '2'
    )
    assert result[0] == 1, f"Expected 1 result, got {result[0]}"
    first_doc = result[1].decode() if isinstance(result[1], bytes) else result[1]
    assert first_doc == 'doc1', f"Expected doc1, got {first_doc}"


def test_insert_after_delete_different_label(new_hnsw_disk_index):
    """Test that inserting a new vector after deletes works correctly."""
    conn = new_hnsw_disk_index

    # Insert vectors
    conn.execute_command('HSET', 'doc1', 'vec', create_float32_vector([1.0, 0.0, 0.0, 0.0]))
    conn.execute_command('HSET', 'doc2', 'vec', create_float32_vector([0.0, 1.0, 0.0, 0.0]))

    # Delete doc1
    conn.execute_command('DEL', 'doc1')
    time.sleep(1)

    # Insert new vector with different label
    conn.execute_command('HSET', 'doc3', 'vec', create_float32_vector([0.0, 0.0, 1.0, 0.0]))

    # Query for doc3
    query_vec = create_float32_vector([0.0, 0.0, 1.0, 0.0])
    result = conn.execute_command(
        'FT.SEARCH', 'idx', '*=>[KNN 1 @vec $blob AS dist]',
        'PARAMS', '2', 'blob', query_vec,
        'SORTBY', 'dist',
        'RETURN', '1', 'dist',
        'DIALECT', '2'
    )
    assert result[0] >= 1, f"Expected at least 1 result, got {result[0]}"
    first_doc = result[1].decode() if isinstance(result[1], bytes) else result[1]
    assert first_doc == 'doc3', f"Expected doc3 as nearest, got {first_doc}"


def test_delete_all_then_insert(new_hnsw_disk_index):
    """Test that the index recovers correctly after deleting everything and inserting new vectors."""
    conn = new_hnsw_disk_index

    # Insert initial vectors
    for i in range(5):
        conn.execute_command('HSET', f'doc{i}', 'vec', create_float32_vector([float(i), 0.0, 0.0, 0.0]))

    # Delete all
    for i in range(5):
        conn.execute_command('DEL', f'doc{i}')
    time.sleep(2)

    # Insert new vectors
    for i in range(5, 10):
        conn.execute_command('HSET', f'doc{i}', 'vec', create_float32_vector([float(i), 0.0, 0.0, 0.0]))

    # Query should find the new vectors
    query_vec = create_float32_vector([7.0, 0.0, 0.0, 0.0])
    result = conn.execute_command(
        'FT.SEARCH', 'idx', '*=>[KNN 3 @vec $blob AS dist]',
        'PARAMS', '2', 'blob', query_vec,
        'SORTBY', 'dist',
        'RETURN', '1', 'dist',
        'DIALECT', '2'
    )
    assert result[0] == 3, f"Expected 3 results, got {result[0]}"
    first_doc = result[1].decode() if isinstance(result[1], bytes) else result[1]
    assert first_doc == 'doc7', f"Expected doc7 as nearest to 7.0, got {first_doc}"



def test_delete_entry_point(new_hnsw_disk_index):
    """Test that search still works after deleting the entry point."""
    conn = new_hnsw_disk_index

    # Add multiple vectors - first one becomes entry point
    conn.execute_command('HSET', 'doc0', 'vec', create_float32_vector([0.0, 0.0, 0.0, 0.0]))
    for i in range(1, 10):
        conn.execute_command('HSET', f'doc{i}', 'vec', create_float32_vector([float(i), 0.0, 0.0, 0.0]))

    # Delete the first vector (likely entry point)
    conn.execute_command('DEL', 'doc0')
    time.sleep(1)

    # Search should still work and find other vectors
    query_vec = create_float32_vector([5.0, 0.0, 0.0, 0.0])
    result = conn.execute_command(
        'FT.SEARCH', 'idx', '*=>[KNN 3 @vec $blob AS dist]',
        'PARAMS', '2', 'blob', query_vec,
        'SORTBY', 'dist',
        'RETURN', '1', 'dist',
        'DIALECT', '2'
    )
    assert result[0] >= 1, f"Expected at least 1 result after deleting entry point, got {result[0]}"


def test_delete_all_but_one(new_hnsw_disk_index):
    """Test that the single remaining vector is findable after deleting all others."""
    conn = new_hnsw_disk_index

    # Add vectors
    for i in range(10):
        conn.execute_command('HSET', f'doc{i}', 'vec', create_float32_vector([float(i), 0.0, 0.0, 0.0]))

    # Delete all but one (keep doc5)
    for i in range(10):
        if i != 5:
            conn.execute_command('DEL', f'doc{i}')
    time.sleep(2)

    # Query - should find doc5
    query_vec = create_float32_vector([5.0, 0.0, 0.0, 0.0])
    result = conn.execute_command(
        'FT.SEARCH', 'idx', '*=>[KNN 3 @vec $blob AS dist]',
        'PARAMS', '2', 'blob', query_vec,
        'SORTBY', 'dist',
        'RETURN', '1', 'dist',
        'DIALECT', '2'
    )
    assert result[0] == 1, f"Expected exactly 1 result, got {result[0]}"
    first_doc = result[1].decode() if isinstance(result[1], bytes) else result[1]
    assert first_doc == 'doc5', f"Expected doc5, got {first_doc}"


def test_delete_connected_nodes(new_hnsw_disk_index):
    """Test that remaining vectors are still found after deleting their neighbors."""
    conn = new_hnsw_disk_index

    # Add cluster of nearby vectors
    for i in range(20):
        vec = [float(i) / 10.0, 0.0, 0.0, 0.0]
        conn.execute_command('HSET', f'doc{i}', 'vec', create_float32_vector(vec))

    # Delete alternating vectors (removes many graph connections)
    for i in range(0, 20, 2):
        conn.execute_command('DEL', f'doc{i}')
    time.sleep(2)

    # Query should still find remaining vectors
    query_vec = create_float32_vector([1.0, 0.0, 0.0, 0.0])
    result = conn.execute_command(
        'FT.SEARCH', 'idx', '*=>[KNN 5 @vec $blob AS dist]',
        'PARAMS', '2', 'blob', query_vec,
        'SORTBY', 'dist',
        'RETURN', '1', 'dist',
        'DIALECT', '2'
    )
    # Should find the odd-numbered vectors
    assert result[0] >= 5, f"Expected at least 5 results, got {result[0]}"


def test_index_size_decrements(new_hnsw_disk_index):
    """Test that FT.INFO shows correct count after deletes."""
    conn = new_hnsw_disk_index

    # Add vectors
    for i in range(10):
        conn.execute_command('HSET', f'doc{i}', 'vec', create_float32_vector([float(i), 0.0, 0.0, 0.0]))

    info = get_vecsim_info(conn, 'idx', 'vec')
    assert info.get('INDEX_SIZE') == 10, f"Expected INDEX_SIZE=10 after insert, got {info.get('INDEX_SIZE')}"

    # Delete half
    for i in range(5):
        conn.execute_command('DEL', f'doc{i}')
    time.sleep(2)

    info = get_vecsim_info(conn, 'idx', 'vec')
    assert info.get('INDEX_SIZE') == 5, f"Expected INDEX_SIZE=5 after deletes, got {info.get('INDEX_SIZE')}"



def test_search_accuracy_after_deletes(new_hnsw_disk_index):
    """Test that recall remains acceptable after multiple deletes."""
    conn = new_hnsw_disk_index

    # Add 100 vectors
    random.seed(42)
    vectors = {}
    for i in range(100):
        vec = [random.uniform(-1, 1) for _ in range(4)]
        vectors[f'doc{i}'] = vec
        conn.execute_command('HSET', f'doc{i}', 'vec', create_float32_vector(vec))

    # Delete 50 random vectors
    docs_to_delete = random.sample(list(vectors.keys()), 50)
    for doc in docs_to_delete:
        conn.execute_command('DEL', doc)
        del vectors[doc]
    time.sleep(3)

    # Remaining vectors
    remaining = list(vectors.keys())

    # Query with a known remaining vector - it should find itself at distance ~0
    test_doc = remaining[0]
    test_vec = vectors[test_doc]
    query_vec = create_float32_vector(test_vec)
    result = conn.execute_command(
        'FT.SEARCH', 'idx', '*=>[KNN 5 @vec $blob AS dist]',
        'PARAMS', '2', 'blob', query_vec,
        'SORTBY', 'dist',
        'RETURN', '1', 'dist',
        'DIALECT', '2'
    )
    assert result[0] >= 1, f"Expected at least 1 result, got {result[0]}"
    first_doc = result[1].decode() if isinstance(result[1], bytes) else result[1]
    assert first_doc == test_doc, f"Expected {test_doc} as exact match, got {first_doc}"
