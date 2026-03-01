"""
Flow tests for throttle mechanism in disk-based vector indexes.

These tests verify that the throttle callbacks (EnablePostponeClients / DisablePostponeClients)
are invoked correctly when the flat buffer fills up and frees space.

The throttle mechanism:
1. When flat buffer reaches flatBufferLimit, EnablePostponeClients is called
2. Write operations are postponed (clients block) until space is freed
3. When worker threads move vectors from flat buffer to HNSW, DisablePostponeClients is called
4. Postponed writes resume

Test configuration:
- Use small TIERED_HNSW_BUFFER_LIMIT (e.g., 10) to easily trigger throttling
- Use WORKERS to control background processing
- Tests use @pytest.mark.module_args to pass custom module arguments
"""

import threading
import time

import pytest

from vecsim_common import create_float32_vector, get_vecsim_info


# Buffer limit for throttle tests - small to easily trigger throttling
THROTTLE_BUFFER_LIMIT = 10

# Module args for throttle tests
THROTTLE_MODULE_ARGS = f'WORKERS 2 TIERED_HNSW_BUFFER_LIMIT {THROTTLE_BUFFER_LIMIT}'


def get_info_section(conn, section: str) -> dict:
    """Get a section from INFO command as a dict."""
    # INFO command returns a dict when decodeResponses=True
    info = conn.execute_command('INFO', section)
    if isinstance(info, dict):
        return info
    # Fallback for string response
    result = {}
    for line in info.split('\n'):
        line = line.strip()
        if ':' in line and not line.startswith('#'):
            key, value = line.split(':', 1)
            try:
                result[key] = int(value)
            except ValueError:
                result[key] = value
    return result


def wait_for_condition(condition_fn, timeout_sec: float, poll_interval_sec: float = 0.05):
    """
    Poll condition_fn() until it returns True or timeout expires.

    Args:
        condition_fn: Callable that returns True when condition is met
        timeout_sec: Maximum time to wait
        poll_interval_sec: Time between polls

    Returns:
        True if condition was met, False if timeout expired
    """
    start = time.monotonic()
    while time.monotonic() - start < timeout_sec:
        if condition_fn():
            return True
        time.sleep(poll_interval_sec)
    return False


@pytest.mark.module_args(THROTTLE_MODULE_ARGS)
def test_throttle_lifecycle(redis_env):
    """
    Test the full throttle lifecycle: enable -> postpone -> disable -> resume.

    Steps:
    1. Create disk HNSW index with small buffer limit (TIERED_HNSW_BUFFER_LIMIT=10)
    2. Pause workers so vectors accumulate in flat buffer
    3. Add vectors to fill the buffer (triggers EnablePostponeClients)
    4. Verify throttle is enabled via INFO metric
    5. Start a write that will be postponed
    6. Verify client is postponed via INFO metric
    7. Resume workers (moves vectors to HNSW, triggers DisablePostponeClients)
    8. Verify write completes and throttle is disabled
    """
    conn = redis_env.getConnection()
    buffer_limit = THROTTLE_BUFFER_LIMIT

    # Verify throttle is initially disabled
    # big_module_postpone_enabled_count is in INFO bigredis section
    bigredis_info = get_info_section(conn, 'bigredis')
    assert bigredis_info.get('big_module_postpone_enabled_count', -1) == 0, \
        "Throttle should be disabled initially"

    # big_postponed_clients_module is in INFO bigredis-stats section
    bigredis_stats = get_info_section(conn, 'bigredis-stats')
    initial_postponed_count = bigredis_stats.get('big_postponed_clients_module')
    assert initial_postponed_count == 0, f"Expected 0 postponed clients initially, got {initial_postponed_count}"

    # Create disk HNSW index
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

    # Pause workers so vectors accumulate in flat buffer
    conn.execute_command('_FT.DEBUG', 'WORKERS', 'PAUSE')

    # Add vectors to fill the buffer (last one triggers throttle)
    for i in range(buffer_limit):
        vec = create_float32_vector([float(i), 0.0, 0.0, 0.0])
        conn.execute_command('HSET', f'doc{i}', 'vec', vec)

    # Verify flat buffer is full
    info = get_vecsim_info(conn, 'idx', 'vec')
    frontend_info = info.get('FRONTEND_INDEX', {})
    frontend_size = frontend_info.get('INDEX_SIZE', 0)
    assert frontend_size == buffer_limit, \
        f"Expected flat buffer size {buffer_limit}, got {frontend_size}"

    # Verify throttle is now enabled (counter == 1)
    bigredis_info = get_info_section(conn, 'bigredis')

    assert bigredis_info.get('big_module_postpone_enabled_count') == 1, \
        f"Throttle should be enabled (counter=1), got {bigredis_info.get('big_module_postpone_enabled_count')}"

    # Start a background thread to add one more vector (should be postponed)
    write_completed = threading.Event()

    def blocked_write():
        conn2 = redis_env.getConnection()
        vec = create_float32_vector([100.0, 0.0, 0.0, 0.0])
        conn2.execute_command('HSET', 'doc_blocked', 'vec', vec, 'num', 1)
        write_completed.set()

    write_thread = threading.Thread(target=blocked_write)
    write_thread.start()

    # Wait for the client to be postponed (cumulative count increases)
    postponed = wait_for_condition(
        lambda: get_info_section(conn, 'bigredis-stats').get('big_postponed_clients_module', 0) == 1,
        timeout_sec=5.0
    )
    assert postponed, "Client should be postponed when buffer is full"

    # Verify write has NOT completed (it's blocked)
    assert not write_completed.is_set(), "Write should be postponed while buffer is full"

    # Verify read commands are processed
    result = conn.execute_command('EXISTS', 'doc0')
    assert result == 1, "EXISTS should return 1"

    info = conn.execute_command('FT.INFO', 'idx')
    assert info is not None, "FT.INFO should return info"

    # Resume workers - they will move vectors from flat buffer to HNSW
    conn.execute_command('_FT.DEBUG', 'WORKERS', 'RESUME')

    # Wait for write to complete (with timeout)
    completed = wait_for_condition(
        lambda: write_completed.is_set(),
        timeout_sec=30.0
    )
    assert completed, "Write should complete after workers free space"

    write_thread.join(timeout=1.0)

    # Verify the blocked document was added
    result = conn.execute_command('HGET', 'doc_blocked', 'num')
    assert result is not None, "Blocked document should be added after throttle disabled"

    # Verify throttle is disabled (counter == 0)
    bigredis_info = get_info_section(conn, 'bigredis')
    assert bigredis_info.get('big_module_postpone_enabled_count') == 0, \
        f"Throttle should be disabled (counter=0), got {bigredis_info.get('big_module_postpone_enabled_count')}"

@pytest.mark.module_args(THROTTLE_MODULE_ARGS)
def test_delete_from_flat_buffer(redis_env):
    """
    Test that delete operations work for vectors in flat buffer.

    This verifies that vectors in the flat buffer (not yet moved to HNSW)
    can be deleted correctly.

    NOTE: Skipped until deleteVector is implemented for disk indexes.
    """
    conn = redis_env.getConnection()
    num_vectors = THROTTLE_BUFFER_LIMIT

    # Create disk HNSW index
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

    # Pause workers so vectors stay in flat buffer
    conn.execute_command('_FT.DEBUG', 'WORKERS', 'PAUSE')

    # Add some vectors
    for i in range(num_vectors):
        vec = create_float32_vector([float(i), 0.0, 0.0, 0.0])
        conn.execute_command('HSET', f'doc{i}', 'vec', vec, 'num', i)

    # Check vectors are in flat buffer
    info = get_vecsim_info(conn, 'idx', 'vec')
    frontend_info = info.get('FRONTEND_INDEX', {})
    frontend_size = frontend_info.get('INDEX_SIZE', 0)
    assert frontend_size == num_vectors, f"Expected {num_vectors} vectors in flat buffer, got {frontend_size}"

    # Delete a document from flat buffer
    result = conn.execute_command('DEL', 'doc0')
    assert result == 1, "DEL should succeed"

    # Verify vector is deleted
    result = conn.execute_command('HGET', 'doc0', 'vec')
    assert result is None, "Deleted document should not exist"

    # Add a new vector after delete
    vec = create_float32_vector([100.0, 0.0, 0.0, 0.0])
    conn.execute_command('HSET', 'doc_new', 'vec', vec, 'num', 100)

    # Verify the new document was added
    result = conn.execute_command('HGET', 'doc_new', 'num')
    assert result is not None, "New document should be added"

    # Resume workers to clean up
    conn.execute_command('_FT.DEBUG', 'WORKERS', 'RESUME')

@pytest.mark.module_args(THROTTLE_MODULE_ARGS)
def test_drop_index_with_full_flat_buffer_disables_throttle(redis_env):
    """Test that dropping an index with a full flat buffer properly disables throttle.

    When the flat buffer is at capacity, EnableThrottle has been called.
    If we drop the index before workers drain it, the destructor must call
    DisableThrottle to balance the global counter.
    """
    conn = redis_env.getConnection()

    # Verify throttle is initially disabled
    bigredis_info = get_info_section(conn, 'bigredis')
    assert bigredis_info.get('big_module_postpone_enabled_count', -1) == 0, \
        "Throttle should be disabled initially"

    # Pause workers so jobs don't execute
    conn.execute_command('_FT.DEBUG', 'WORKERS', 'PAUSE')

    # Create index
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

    # Fill flat buffer (triggers EnableThrottle on last vector)
    for i in range(THROTTLE_BUFFER_LIMIT):
        vec = create_float32_vector([float(i), 0.0, 0.0, 0.0])
        conn.execute_command('HSET', f'doc{i}', 'vec', vec)

    # Verify throttle is now enabled
    bigredis_info = get_info_section(conn, 'bigredis')
    assert bigredis_info.get('big_module_postpone_enabled_count') == 1, \
        f"Throttle should be enabled (counter=1), got {bigredis_info.get('big_module_postpone_enabled_count')}"

    # Drop index while buffer is full - destructor should call DisableThrottle
    result = conn.execute_command('FT.DROPINDEX', 'idx', 'DD')
    assert result == 'OK'

    # Verify throttle is disabled after drop
    bigredis_info = get_info_section(conn, 'bigredis')
    assert bigredis_info.get('big_module_postpone_enabled_count') == 0, \
        f"Throttle should be disabled after drop (counter=0), got {bigredis_info.get('big_module_postpone_enabled_count')}"

    # Verify writes work while workers are still paused (proves throttle is truly disabled)
    conn.execute_command('HSET', 'test_doc', 'foo', 'bar')
    result = conn.execute_command('HGET', 'test_doc', 'foo')
    assert result is not None, "Write should succeed after throttle disabled (workers still paused)"

    # Resume workers
    conn.execute_command('_FT.DEBUG', 'WORKERS', 'RESUME')
