"""
Basic flow tests for INFO command metrics collection.

These tests verify that the INFO search command properly reports metrics
for disk-based indexes.
"""

from common import *

def test_info_search_basic(redis_env):
    """
    Tests basic `INFO` fields for RediSearch on Flex.
    """

    # Test before creating any indexes.
    info_before = redis_env.cmd('INFO', 'search')
    expected_both = {
        'num_immutable_memtables': 0,
        'num_immutable_memtables_flushed': 0,
        'mem_table_flush_pending': 0,
        'active_memtable_size': 0,
        'size_all_mem_tables': 0,
        'num_entries_active_memtable': 0,
        'num_entries_imm_memtables': 0,
        'num_deletes_active_memtable': 0,
        'num_deletes_imm_memtables': 0,
        'compaction_pending': 0,
        'num_running_compactions': 0,
        'num_running_flushes': 0,
        'estimate_pending_compaction_bytes': 0,
        'estimate_num_keys': 0,
        'estimate_live_data_size': 0,
        'live_sst_files_size': 0,
        'num_live_versions': 0,
        'estimate_table_readers_mem': 0
    }
    redis_env.assertEqual(info_before['search_disk_doc_table'], expected_both)
    redis_env.assertEqual(info_before['search_disk_text_inverted_index'], expected_both)

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

    disk_doc_after_create = info_after_create['search_disk_doc_table']
    # Doc table metrics
    expected_dt = {
        'num_immutable_memtables': 0,
        'num_immutable_memtables_flushed': 0,
        'mem_table_flush_pending': 0,
        'active_memtable_size': 2048,
        'size_all_mem_tables': 2048,
        'num_entries_active_memtable': 0,
        'num_entries_imm_memtables': 0,
        'num_deletes_active_memtable': 0,
        'num_deletes_imm_memtables': 0,
        'compaction_pending': 0,
        'num_running_compactions': 0,
        'num_running_flushes': 0,
        'estimate_pending_compaction_bytes': 0,
        'estimate_num_keys': 0,
        'estimate_live_data_size': 0,
        'live_sst_files_size': 0,
        'num_live_versions': 1,
        'estimate_table_readers_mem': 0
    }
    redis_env.assertEqual(disk_doc_after_create, expected_dt)

    # Inverted index metrics
    disk_inv_after_create = info_after_create['search_disk_text_inverted_index']
    expected_ii = {
        'num_immutable_memtables': 0,
        'num_immutable_memtables_flushed': 0,
        'mem_table_flush_pending': 0,
        'active_memtable_size': 2048,
        'size_all_mem_tables': 2048,
        'num_entries_active_memtable': 0,
        'num_entries_imm_memtables': 0,
        'num_deletes_active_memtable': 0,
        'num_deletes_imm_memtables': 0,
        'compaction_pending': 0,
        'num_running_compactions': 0,
        'num_running_flushes': 0,
        'estimate_pending_compaction_bytes': 0,
        'estimate_num_keys': 0,
        'estimate_live_data_size': 0,
        'live_sst_files_size': 0,
        'num_live_versions': 1,
        'estimate_table_readers_mem': 0
    }
    redis_env.assertEqual(disk_inv_after_create, expected_ii)

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
    disk_doc_after_pop = info_after_pop['search_disk_doc_table']
    expected_dt = {
        'num_immutable_memtables': 0,
        'num_immutable_memtables_flushed': 0,
        'mem_table_flush_pending': 0,
        'active_memtable_size': 1050624,
        'size_all_mem_tables': 1050624,
        'num_entries_active_memtable': 1000,
        'num_entries_imm_memtables': 0,
        'num_deletes_active_memtable': 0,
        'num_deletes_imm_memtables': 0,
        'compaction_pending': 0,
        'num_running_compactions': 0,
        'num_running_flushes': 0,
        'estimate_pending_compaction_bytes': 0,
        'estimate_num_keys': 1000,
        'estimate_live_data_size': 0,
        'live_sst_files_size': 0,
        'num_live_versions': 1,
        'estimate_table_readers_mem': 0
    }
    redis_env.assertEqual(disk_doc_after_pop, expected_dt)

    # Inverted index metrics
    disk_inv_after_pop = info_after_pop['search_disk_text_inverted_index']
    expected_ii = {
        'num_immutable_memtables': 0,
        'num_immutable_memtables_flushed': 0,
        'mem_table_flush_pending': 0,
        'active_memtable_size': 1050624,
        'size_all_mem_tables': 1050624,
        'num_entries_active_memtable': 1000,
        'num_entries_imm_memtables': 0,
        'num_deletes_active_memtable': 0,
        'num_deletes_imm_memtables': 0,
        'compaction_pending': 0,
        'num_running_compactions': 0,
        'num_running_flushes': 0,
        'estimate_pending_compaction_bytes': 0,
        'estimate_num_keys': 1000,
        'estimate_live_data_size': 0,
        'live_sst_files_size': 0,
        'num_live_versions': 1,
        'estimate_table_readers_mem': 0
    }
    redis_env.assertEqual(disk_inv_after_pop, expected_ii)

# TODO: Add tests with:
#   * deletion (depends on MOD-13306).
#   * Compaction/flush.
#   * Disk usage, once we can initiate a flush via a debug command, or control the cache size more easily.
