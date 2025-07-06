from common import *
from test_index_oom import *

# These are commented out tests that were moved from test_index_oom.py
# They may need to be uncommented and fixed in the future

@skip(cluster=True)
def test_pseudo_enterprise_oom_retry_drop(env):
  oom_pseudo_enterprise_config(env)

  num_docs = 100
  for i in range(num_docs):
    env.expect('HSET', f'doc{i}', 'name', f'name{i}').equal(1)

  num_docs_scanned = num_docs//4
  env.expect(bgScanCommand(), 'SET_PAUSE_ON_SCANNED_DOCS', num_docs_scanned).ok()
  env.expect(bgScanCommand(), 'SET_PAUSE_BEFORE_OOM_RETRY', 'true').ok()
  for run in ['without', 'with']: # Run the test with and without increasing memory
    idx = f'idx{run}'
    # Create an index
    env.expect('FT.CREATE', idx, 'SCHEMA', 'name', 'TEXT').ok()
    waitForIndexPauseScan(env, idx)

    # At this point num_docs_scanned were scanned
    # Now we set the tight memory limit
    set_tight_maxmemory_for_oom(env, 0.85)

    # Resume PAUSE ON SCANNED DOCS
    env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
    # Wait for OOM
    waitForIndexStatus(env, 'PAUSED_BEFORE_OOM_RETRY',idx)
    # At this point the scan should be paused before OOM retry
    # If we are in the first run, we don't increase the memory
    if run == 'with':
      # Increase memory during the pause, emulating resource allocation
      set_unlimited_maxmemory_for_oom(env)
    # Drop the index
    env.expect(f'FT.DROPINDEX', idx).ok()
    # Resume PAUSE BEFORE OOM RETRY
    env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()

    # Validate that the index was dropped
    env.expect('ft._list').equal([])
    # Reset memory for next run
    set_unlimited_maxmemory_for_oom(env)

@skip(cluster=True)
def test_pseudo_enterprise_oom_retry_alter_success(env):
  oom_pseudo_enterprise_config(env)

  num_docs = 100
  for i in range(num_docs):
    env.expect('HSET', f'doc{i}', 'name', f'name{i}', 'hello', f'hello{i}').equal(2)

  # env.expect(bgScanCommand(), 'SET_PAUSE_ON_OOM', 'true').ok()
  num_docs_scanned = num_docs//4
  env.expect(bgScanCommand(), 'SET_PAUSE_ON_SCANNED_DOCS', num_docs_scanned).ok()
  env.expect(bgScanCommand(), 'SET_PAUSE_BEFORE_OOM_RETRY', 'true').ok()

  idx = f'idx'
# Create an index
  env.expect('FT.CREATE', idx, 'SCHEMA', 'name', 'TEXT').ok()
  waitForIndexPauseScan(env, idx)

  # At this point num_docs_scanned were scanned
  # Now we set the tight memory limit
  set_tight_maxmemory_for_oom(env, 0.85)

  # Resume PAUSE ON SCANNED DOCS
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
  # Wait for OOM
  waitForIndexStatus(env, 'PAUSED_BEFORE_OOM_RETRY',idx)
  # At this point the scan should be paused before OOM retry
  # Increase memory during the pause, emulating resource allocation
  set_unlimited_maxmemory_for_oom(env)
  # Remove pause configs
  env.expect(bgScanCommand(), 'SET_PAUSE_ON_SCANNED_DOCS', 0).ok()
  env.expect(bgScanCommand(), 'SET_PAUSE_BEFORE_OOM_RETRY', 'false').ok()

  env.expect(f'FT.ALTER', idx, 'SCHEMA', 'ADD', 'hello', 'TEXT').ok()
  # Resume PAUSE BEFORE OOM RETRY
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()

  # Verify that the indexing finished
  waitForIndexFinishScan(env, idx)
  # Verify that all docs were indexed
  docs_in_index = get_index_num_docs(env)
  index_errors = get_index_errors_dict(env)
  env.assertEqual(docs_in_index, num_docs)
  # Verify index BG indexing status is OK
  env.assertEqual(index_errors[bgIndexingStatusStr], 'OK')

@skip(cluster=True)
def test_pseudo_enterprise_oom_retry_alter_failure(env):
  oom_pseudo_enterprise_config(env)

  num_docs = 100
  for i in range(num_docs):
    env.expect('HSET', f'doc{i}', 'name', f'name{i}', 'hello', f'hello{i}').equal(2)

  num_docs_scanned = num_docs//4
  env.expect(bgScanCommand(), 'SET_PAUSE_ON_SCANNED_DOCS', num_docs_scanned).ok()
  env.expect(bgScanCommand(), 'SET_PAUSE_BEFORE_OOM_RETRY', 'true').ok()
  env.expect(bgScanCommand(), 'SET_PAUSE_ON_OOM', 'true').ok()

  idx = f'idx'
  # Create an index
  env.expect('FT.CREATE', idx, 'SCHEMA', 'name', 'TEXT').ok()
  waitForIndexPauseScan(env, idx)

  # At this point num_docs_scanned were scanned
  # Now we set the tight memory limit
  set_tight_maxmemory_for_oom(env, 0.85)

  # Resume PAUSE ON SCANNED DOCS
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
  # Wait for OOM
  waitForIndexStatus(env, 'PAUSED_BEFORE_OOM_RETRY',idx)
  # At this point the scan should be paused before OOM retry

  # Remove pause configs
  env.expect(bgScanCommand(), 'SET_PAUSE_BEFORE_OOM_RETRY', 'false').ok()

  # # Increase memory during the pause, to enable the ft.alter command
  set_unlimited_maxmemory_for_oom(env)
  env.expect(f'FT.ALTER', idx, 'SCHEMA', 'ADD', 'hello', 'TEXT').ok()
  # Resume PAUSE BEFORE OOM RETRY
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()

  # The scan should cancel due to the ft.alter command
  # For the new scan, at this point, num_docs_scanned were scanned
  waitForIndexPauseScan(env, idx)
  # Set again the limit to 85% to trigger OOM (removed to enable the ft.alter command)
  set_tight_maxmemory_for_oom(env, 0.85)

  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
  # The scan should OOM
  waitForIndexStatus(env, 'PAUSED_ON_OOM', idx)
  # Resume PAUSE ON OOM
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()

  # Verify that the indexing finished
  waitForIndexFinishScan(env, idx)
  # Verify OOM status
  error_dict = get_index_errors_dict(env)
  env.assertEqual(error_dict[bgIndexingStatusStr], OOMfailureStr)

@skip(cluster=False)
def test_pseudo_enterprise_cluster_oom_retry_success(env):
    # Let background indexing go up to 80 % of Redis' limit
    verify_command_OK_on_all_shards(
        env, '_FT.CONFIG SET _BG_INDEX_MEM_PCT_THR 80')
    # 1-second grace so the test doesn't take too long
    verify_command_OK_on_all_shards(
        env, '_FT.CONFIG SET _BG_INDEX_OOM_PAUSE_TIME 1')

    conn = getConnectionByEnv(env)
    docs_per_shard = 1_000
    total_docs = docs_per_shard * env.shardsCount
    for i in range(total_docs):
        conn.execute_command('HSET', f'doc{i}', 'name', f'name{i}')

    # Instrument the scanner on every shard
    run_command_on_all_shards(env,
        f'{bgScanCommand()} SET_PAUSE_ON_OOM true')
    run_command_on_all_shards(env,
        f'{bgScanCommand()} SET_PAUSE_ON_SCANNED_DOCS {docs_per_shard//4}')
    run_command_on_all_shards(env,
        f'{bgScanCommand()} SET_PAUSE_BEFORE_OOM_RETRY true')

    idx = 'idx'
    conn.execute_command('FT.CREATE', idx, 'SCHEMA', 'name', 'TEXT')

    # Pause after the first chunk of documents
    allShards_waitForIndexPauseScan(env, idx)

    # Drop memory to 85 % of the configured threshold
    allShards_set_tight_maxmemory_for_oom(env, 0.85)

    # Resume - this will push every shard into PAUSED_BEFORE_OOM_RETRY
    run_command_on_all_shards(env,
        f'{bgScanCommand()} SET_BG_INDEX_RESUME')
    allShards_waitForIndexStatus(env, 'PAUSED_BEFORE_OOM_RETRY', idx)

    # While paused, free memory so the retry can succeed
    allShards_set_unlimited_maxmemory_for_oom(env)

    # Resume again - indexing should now complete
    run_command_on_all_shards(env,
        f'{bgScanCommand()} SET_BG_INDEX_RESUME')
    allShards_waitForIndexFinishScan(env, idx)

    indexed_num_docs = get_index_num_docs(env, idx=idx)
    index_errors = get_index_errors_dict(env, idx=idx)
    env.assertEqual(indexed_num_docs, total_docs)
    env.assertEqual(index_errors[bgIndexingStatusStr], 'OK')
    # Every shard's failure counter must stay at 0
    for shard_id in range(1, env.shardsCount + 1):
        failures = env.getConnection(shard_id).execute_command(
            'INFO', 'modules')['search_OOM_indexing_failures_indexes_count']
        env.assertEqual(failures, 0)

@skip(cluster=False)
def test_pseudo_enterprise_cluster_oom_retry_failure(env):
    verify_command_OK_on_all_shards(
        env, '_FT.CONFIG SET _BG_INDEX_MEM_PCT_THR 80')
    verify_command_OK_on_all_shards(
        env, '_FT.CONFIG SET _BG_INDEX_OOM_PAUSE_TIME 1')

    conn = getConnectionByEnv(env)
    docs_per_shard = 1_000
    total_docs = docs_per_shard * env.shardsCount
    for i in range(total_docs):
        conn.execute_command('HSET', f'doc{i}', 'name', f'name{i}')

    run_command_on_all_shards(env,
        f'{bgScanCommand()} SET_PAUSE_ON_OOM true')
    run_command_on_all_shards(env,
        f'{bgScanCommand()} SET_PAUSE_ON_SCANNED_DOCS {docs_per_shard//4}')
    run_command_on_all_shards(env,
        f'{bgScanCommand()} SET_PAUSE_BEFORE_OOM_RETRY true')

    idx = 'idx'
    conn.execute_command('FT.CREATE', idx, 'SCHEMA', 'name', 'TEXT')

    # Pause after first docs chunk, then tighten memory
    allShards_waitForIndexPauseScan(env, idx)
    allShards_set_tight_maxmemory_for_oom(env, 0.85)

    # Resume - shards pause *before* OOM retry
    run_command_on_all_shards(env,
        f'{bgScanCommand()} SET_BG_INDEX_RESUME')
    allShards_waitForIndexStatus(env, 'PAUSED_BEFORE_OOM_RETRY', idx)

    # Resume again with memory still tight -> PAUSED_ON_OOM
    run_command_on_all_shards(env,
        f'{bgScanCommand()} SET_BG_INDEX_RESUME')
    allShards_waitForIndexStatus(env, 'PAUSED_ON_OOM', idx)

    # One last resume - the second OOM turns into failure
    run_command_on_all_shards(env,
        f'{bgScanCommand()} SET_BG_INDEX_RESUME')
    allShards_waitForIndexFinishScan(env, idx)

    errors = get_index_errors_dict(env, idx=idx)
    env.assertEqual(errors[bgIndexingStatusStr], 'OOM failure')
    # Shards must report exactly one failed index each
    for shard_id in range(1, env.shardsCount + 1):
        failures = env.getConnection(shard_id).execute_command(
            'INFO', 'modules')['search_OOM_indexing_failures_indexes_count']
        env.assertEqual(failures, 1)

@skip(cluster=True)
def test_unlimited_memory_thrs(env):
  # Set the threshold to 0
  env.expect('FT.CONFIG', 'SET', '_BG_INDEX_MEM_PCT_THR', '0').ok()
  # Set pause before scan
  env.expect(bgScanCommand(), 'SET_PAUSE_BEFORE_SCAN', 'true').ok()
  # insert 100 docs
  for i in range(100):
      env.expect('HSET', f'doc{i}', 'name', f'name{i}').equal(1)
  # Create an index
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
  # Wait for pause before scan
  waitForIndexStatus(env, 'NEW')
  # Set maxmemory to be equal to used memory
  set_tight_maxmemory_for_oom(env, 1.0)
  # Resume indexing
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
  # Verify that the indexing finished even though we reached OOM
  waitForIndexFinishScan(env, 'idx')
  # Verify that all docs were indexed
  docs_in_index = get_index_num_docs(env)
  env.assertEqual(docs_in_index, 100)
