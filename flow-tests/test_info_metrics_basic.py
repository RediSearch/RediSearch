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

# Compaction cumulative metrics (inverted index dict only)
COMPACTION_METRIC_FIELDS = [
    'compaction_total_cycles',
    'compaction_total_ms_run',
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

def make_inverted_index_metrics():
    """Inverted index dict = CF metrics + compaction metrics (all default 0)."""
    return {field: 0 for field in CF_METRIC_FIELDS + COMPACTION_METRIC_FIELDS}


def make_doc_table_metrics():
    """Create a doc table metrics dict with all fields defaulting to 0."""
    return {field: 0 for field in CF_METRIC_FIELDS + DOC_TABLE_EXTRA_FIELDS}

def test_info_search_basic(redis_env):
    """
    Tests basic `INFO` fields for RediSearch on Flex.
    """

    # Test before creating any indexes.
    info_before = redis_env.cmd('INFO', 'search')
    redis_env.assertNotIn('search_disk_doc_table', info_before)
    redis_env.assertNotIn('search_disk_text_inverted_index', info_before)
    redis_env.assertNotIn('search_disk_tag_inverted_index', info_before)
    redis_env.assertNotIn('search_used_memory_indexes', info_before)
    expected_doc_table = make_doc_table_metrics()
    expected_text_inverted_index = make_inverted_index_metrics()
    expected_tag_inverted_index = make_inverted_index_metrics()

    # --------------------------- Create an index ------------------------------
    redis_env.cmd("FT.CREATE", "idx", "SKIPINITIALSCAN", "SCHEMA", "t", "TEXT", "o", "TAG", "f", "TAG", "u", "TAG")

    # Test after creating an index, before population.
    info_after_create = redis_env.cmd('INFO', 'search')

    # We now have one index, so index memory and disk memtable sizes should be > 0,
    # but there should still be no keys/entries on disk.
    redis_env.assertEqual(info_after_create['search_number_of_indexes'], 1)

    # Doc table metrics
    # TODO(MOD-14101): Flaky - num_running_compactions sometimes 1 instead of 0
    # expected_doc_table = with_overrides(expected_doc_table,
    #     active_memtable_size=2048, size_all_mem_tables=2048, num_live_versions=1)
    # redis_env.assertEqual(info_after_create['search_disk_doc_table'], expected_doc_table)

    # Text inverted index metrics
    expected_text_inverted_index = with_overrides(expected_text_inverted_index,
        active_memtable_size=2048, size_all_mem_tables=2048, compaction_pending=1, num_live_versions=1)
    redis_env.assertEqual(info_after_create['search_disk_text_inverted_index'], expected_text_inverted_index)

    # Tag inverted index metrics - all zeros before indexing (lazy-initialized)
    redis_env.assertEqual(info_after_create['search_disk_tag_inverted_index'], expected_tag_inverted_index)

    # ------------------------- Populate the index -----------------------------
    # Add some documents to the db
    n_docs = 1000
    conn = redis_env.getConnection()
    for i in range(n_docs):
        conn.execute_command('HSET', f'doc_{i}', 't', 'foo', 'o', 'bar', 'f', 'baz', 'u', 'qux')

    # Test a populated index.
    info_after_pop = redis_env.cmd('INFO', 'search')

    # After population, index memory and disk entry/key counts should increase.
    redis_env.assertGreater(
        info_after_pop['search_used_memory_indexes'],
        info_after_create['search_used_memory_indexes'],
    )

    # Doc table metrics
    # TODO(MOD-14101): Flaky - num_live_versions sometimes 1 instead of 0
    # disk_doc_after_pop = info_after_pop['search_disk_doc_table']
    # redis_env.assertGreater(disk_doc_after_pop['active_memtable_size'], 0)
    # redis_env.assertGreater(disk_doc_after_pop['size_all_mem_tables'], 0)
    # expected_doc_table = with_overrides(expected_doc_table,
    #     active_memtable_size=disk_doc_after_pop['active_memtable_size'],
    #     size_all_mem_tables=disk_doc_after_pop['size_all_mem_tables'],
    #     num_entries_active_memtable=1000, estimate_num_keys=1000)
    # redis_env.assertEqual(disk_doc_after_pop, expected_doc_table)

    # Text inverted index metrics
    disk_text_inv_after_pop = info_after_pop['search_disk_text_inverted_index']
    redis_env.assertGreater(disk_text_inv_after_pop['active_memtable_size'], 0)
    redis_env.assertGreater(disk_text_inv_after_pop['size_all_mem_tables'], 0)
    expected_text_inverted_index = with_overrides(expected_text_inverted_index,
        active_memtable_size=disk_text_inv_after_pop['active_memtable_size'],
        size_all_mem_tables=disk_text_inv_after_pop['size_all_mem_tables'],
        num_entries_active_memtable=1000, estimate_num_keys=1000)
    redis_env.assertEqual(disk_text_inv_after_pop, expected_text_inverted_index)

    # Tag inverted index metrics (3 tag fields × 1000 docs = 3000 entries)
    disk_tag_inv_after_pop = info_after_pop['search_disk_tag_inverted_index']
    redis_env.assertGreater(disk_tag_inv_after_pop['active_memtable_size'], 0)
    redis_env.assertGreater(disk_tag_inv_after_pop['size_all_mem_tables'], 0)
    expected_tag_inverted_index = with_overrides(expected_tag_inverted_index,
        active_memtable_size=disk_tag_inv_after_pop['active_memtable_size'],
        size_all_mem_tables=disk_tag_inv_after_pop['size_all_mem_tables'],
        num_entries_active_memtable=n_docs * 3,  # 3 tag fields
        estimate_num_keys=n_docs * 3,
        compaction_pending=3,  # 3 tag column families
        num_live_versions=3)
    redis_env.assertEqual(disk_tag_inv_after_pop, expected_tag_inverted_index)

def test_compaction_metrics(redis_env):
    """
    Verifies that compaction cumulative metrics (cycles, ms_run)
    start at zero and increase as expected after each GC_FORCEINVOKE.
    """
    conn = redis_env.getConnection()

    # Create index
    conn.execute_command(
        'FT.CREATE', 'idx', 'ON', 'HASH', 'SKIPINITIALSCAN',
        'SCHEMA', 'title', 'TEXT', 'tag', 'TAG'
    )

    # Add documents
    n_docs = 10000
    for i in range(n_docs):
        conn.execute_command('HSET', f'doc_{i}', 'title', f'hello world term_{i}', 'tag', f'tag_{i % 10}')

    waitForIndex(redis_env, 'idx')

    # Compaction metrics should be zero before any compaction has run
    info_after_create = redis_env.cmd('INFO', 'search')
    inv_initial = info_after_create['search_disk_text_inverted_index']
    redis_env.assertEqual(inv_initial['compaction_total_cycles'], 0,
                          message='compaction_total_cycles should be 0 upon index creation')
    redis_env.assertEqual(inv_initial['compaction_total_ms_run'], 0,
                          message='compaction_total_ms_run should be 0 upon index creation')

    # First GC invocation, trigger a compaction cycle
    result = redis_env.cmd('_FT.DEBUG', 'GC_FORCEINVOKE', 'idx')
    redis_env.assertEqual(result, 'DONE')

    info_after_first_gc = redis_env.cmd('INFO', 'search')
    inv = info_after_first_gc['search_disk_text_inverted_index']
    cycles_after_first = inv['compaction_total_cycles']
    ms_after_first = inv['compaction_total_ms_run']

    redis_env.assertGreaterEqual(cycles_after_first, 1, message='At least one compaction cycle after first GC')
    redis_env.assertGreaterEqual(ms_after_first, 0, message='compaction_total_ms_run should be >= 0')

    # Delete many documents so that second compaction has work to do
    n_delete = 5000
    for i in range(n_delete):
        conn.execute_command('DEL', f'doc_{i}')

    # Second GC invocation, trigger a compaction cycle
    result = redis_env.cmd('_FT.DEBUG', 'GC_FORCEINVOKE', 'idx')
    redis_env.assertEqual(result, 'DONE')

    info_after_second_gc = redis_env.cmd('INFO', 'search')
    inv2 = info_after_second_gc['search_disk_text_inverted_index']
    cycles_after_second = inv2['compaction_total_cycles']
    ms_after_second = inv2['compaction_total_ms_run']

    # Compaction cycles: exactly one more after second GC invocation
    redis_env.assertEqual(
        cycles_after_second, cycles_after_first + 1,
        message=f'compaction_total_cycles should grow by 1: before={cycles_after_first}, after={cycles_after_second}'
    )

    # Total ms run should increase
    redis_env.assertGreaterEqual(
        ms_after_second, ms_after_first,
        message=f'compaction_total_ms_run should increase: before={ms_after_first}, after={ms_after_second}'
    )

# TODO: Add tests with:
#   * deletion (depends on MOD-13306).
#   * Disk usage, once we can initiate a flush via a debug command, or control the cache size more easily.
