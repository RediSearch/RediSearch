"""
Basic flow tests for INFO command metrics collection.

These tests verify that the INFO search command properly reports metrics
for disk-based indexes.
"""

from common import *

# Column family metric fields (shared by doc_table and inverted_index)
CF_METRIC_FIELDS = [
    'num_immutable_memtables',
    'num_immutable_memtables_flushed',
    'mem_table_flush_pending',
    'active_memtable_size',
    'size_all_mem_tables',
    'num_entries_active_memtable',
    'num_entries_imm_memtables',
    'num_deletes_active_memtable',
    'num_deletes_imm_memtables',
    'compaction_pending',
    'num_running_compactions',
    'num_running_flushes',
    'estimate_pending_compaction_bytes',
    'estimate_num_keys',
    'estimate_live_data_size',
    'live_sst_files_size',
    'num_live_versions',
    'estimate_table_readers_mem',
]

# Doc table specific fields (in addition to CF metrics)
DOC_TABLE_EXTRA_FIELDS = [
    'deleted_ids_count',
    'async_total_reads_requested',
    'async_reads_found',
    'async_reads_not_found',
    'async_reads_errors',
    'async_reads_expired',
]

def with_overrides(base, **overrides):
    """Return a copy of base dict with overrides applied."""
    return {**base, **overrides}

def make_cf_metrics():
    """Create a column family metrics dict with all fields defaulting to 0."""
    return {field: 0 for field in CF_METRIC_FIELDS}

def make_doc_table_metrics():
    """Create a doc table metrics dict with all fields defaulting to 0."""
    return {field: 0 for field in CF_METRIC_FIELDS + DOC_TABLE_EXTRA_FIELDS}

def test_info_search_basic(redis_env):
    """
    Tests basic `INFO` fields for RediSearch on Flex.
    """

    # Test before creating any indexes.
    info_before = redis_env.cmd('INFO', 'search')
    expected_doc_table = make_doc_table_metrics()
    expected_inverted_index = make_cf_metrics()
    redis_env.assertEqual(info_before['search_disk_doc_table'], expected_doc_table)
    redis_env.assertEqual(info_before['search_disk_text_inverted_index'], expected_inverted_index)

    # --------------------------- Create an index ------------------------------
    redis_env.cmd("FT.CREATE", "idx", "SKIPINITIALSCAN", "SCHEMA", "t", "TEXT")

    # Test after creating an index, before population.
    info_after_create = redis_env.cmd('INFO', 'search')

    # We now have one index, so index memory and disk memtable sizes should be > 0,
    # but there should still be no keys/entries on disk.
    redis_env.assertEqual(info_after_create['search_number_of_indexes'], 1)
    redis_env.assertGreater(
        info_after_create['search_used_memory_indexes'],
        info_before['search_used_memory_indexes'],
    )

    # Doc table metrics
    expected_doc_table = with_overrides(expected_doc_table,
        active_memtable_size=2048, size_all_mem_tables=2048, num_live_versions=1)
    redis_env.assertEqual(info_after_create['search_disk_doc_table'], expected_doc_table)

    # Inverted index metrics
    expected_inverted_index = with_overrides(expected_inverted_index,
        active_memtable_size=2048, size_all_mem_tables=2048, compaction_pending=1, num_live_versions=1)
    redis_env.assertEqual(info_after_create['search_disk_text_inverted_index'], expected_inverted_index)

    # ------------------------- Populate the index -----------------------------
    # Add some documents to the db
    n_docs = 1000
    conn = redis_env.getConnection()
    for i in range(n_docs):
        conn.execute_command('HSET', f'doc_{i}', 't', 'foo')

    # Test a populated index.
    info_after_pop = redis_env.cmd('INFO', 'search')

    # After population, index memory and disk entry/key counts should increase.
    redis_env.assertGreater(
        info_after_pop['search_used_memory_indexes'],
        info_after_create['search_used_memory_indexes'],
    )

    # Doc table metrics
    # Note: memtable sizes can vary across platforms due to SpeedB/RocksDB
    # internal allocation patterns, so we just check they're positive.
    disk_doc_after_pop = info_after_pop['search_disk_doc_table']
    redis_env.assertGreater(disk_doc_after_pop['active_memtable_size'], 0)
    redis_env.assertGreater(disk_doc_after_pop['size_all_mem_tables'], 0)
    expected_doc_table = with_overrides(expected_doc_table,
        active_memtable_size=disk_doc_after_pop['active_memtable_size'],
        size_all_mem_tables=disk_doc_after_pop['size_all_mem_tables'],
        num_entries_active_memtable=1000, estimate_num_keys=1000)
    redis_env.assertEqual(disk_doc_after_pop, expected_doc_table)

    # Inverted index metrics
    disk_inv_after_pop = info_after_pop['search_disk_text_inverted_index']
    redis_env.assertGreater(disk_inv_after_pop['active_memtable_size'], 0)
    redis_env.assertGreater(disk_inv_after_pop['size_all_mem_tables'], 0)
    expected_inverted_index = with_overrides(expected_inverted_index,
        active_memtable_size=disk_inv_after_pop['active_memtable_size'],
        size_all_mem_tables=disk_inv_after_pop['size_all_mem_tables'],
        num_entries_active_memtable=1000, estimate_num_keys=1000)
    redis_env.assertEqual(disk_inv_after_pop, expected_inverted_index)

# TODO: Add tests with:
#   * deletion (depends on MOD-13306).
#   * Compaction/flush.
#   * Disk usage, once we can initiate a flush via a debug command, or control the cache size more easily.
