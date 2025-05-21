from common import *

# Global variables
bgIndexingStatusStr = "background indexing status"
indexing_failures_str = 'indexing failures'
last_indexing_error_key_str = 'last indexing error key'
last_indexing_error_str = 'last indexing error'
OOM_indexing_failure_str = 'Index background scan did not complete due to OOM. New documents will not be indexed.'
OOMfailureStr = "OOM failure"

def get_memory_consumption_ratio(env):
  used_memory = env.cmd('INFO', 'MEMORY')['used_memory']
  max_memory = env.cmd('INFO', 'MEMORY')['maxmemory']
  return used_memory/max_memory

def get_index_errors_dict(env, idx = 'idx'):
  info = index_info(env, idx)
  error_dict = to_dict(info["Index Errors"])
  return error_dict
def get_index_num_docs(env, idx = 'idx'):
  info = index_info(env, idx)
  num_docs = info['num_docs']
  return num_docs

def oom_test_config(env):
  # Set the memory limit to 80% so it can be tested without colliding with redis memory limit
  env.expect('FT.CONFIG', 'SET', '_BG_INDEX_MEM_PCT_THR', '80').ok()

def oom_pseudo_enterprise_config(env):
  oom_test_config(env)
  # Set the pause time to 1 second so we can test the retry
  env.expect('FT.CONFIG', 'SET', '_BG_INDEX_OOM_PAUSE_TIME', '1').ok()

@skip(cluster=True)
def test_stop_background_indexing_on_low_mem(env):
  oom_test_config(env)

  num_docs = 1000
  for i in range(num_docs):
      env.expect('HSET', f'doc{i}', 'name', f'name{i}').equal(1)

  # Set pause on OOM
  env.expect(bgScanCommand(), 'SET_PAUSE_ON_OOM', 'true').ok()
  # Set pause after quarter of the docs were scanned
  num_docs_scanned = num_docs//4
  env.expect(bgScanCommand(), 'SET_PAUSE_ON_SCANNED_DOCS', num_docs_scanned).ok()

  # Create an index
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
  waitForIndexPauseScan(env, 'idx')

  # At this point num_docs_scanned were scanned
  # Now we set the tight memory limit
  set_tight_maxmemory_for_oom(env, 0.85)
  # After we resume, an OOM should trigger
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
  # Wait for OOM
  waitForIndexStatus(env, 'PAUSED_ON_OOM','idx')
  # Resume the indexing
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
  # Wait for the indexing to finish
  waitForIndexFinishScan(env, 'idx')
  # Verify that only num_docs_scanned were indexed
  docs_in_index = get_index_num_docs(env)
  env.assertEqual(docs_in_index, num_docs_scanned)
  # Verify that used_memory is close to 80% (config set) of maxmemory
  memory_ratio = get_memory_consumption_ratio(env)
  env.assertAlmostEqual(memory_ratio, 0.85, delta=0.1)

@skip(cluster=True)
def test_stop_indexing_low_mem_verbosity(env):
  # Change to resp3
  env = Env(protocol=3)
  oom_test_config(env)

  # Create OOM
  num_docs = 10
  for i in range(num_docs):
      env.expect('HSET', f'doc{i}', 'name', f'name{i}').equal(1)
  # Set pause on OOM
  env.expect(bgScanCommand(), 'SET_PAUSE_ON_OOM', 'true').ok()
  # Set pause before scanning
  env.expect(bgScanCommand(), 'SET_PAUSE_ON_SCANNED_DOCS', 1).ok()
  # Create an index
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
  # Wait for pause before scanning
  waitForIndexPauseScan(env, 'idx')
  # Set tight memory limit
  set_tight_maxmemory_for_oom(env, 0.85)
  # Resume indexing
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
  # Wait for OOM
  waitForIndexStatus(env, 'PAUSED_ON_OOM','idx')
  # Resume the indexing
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
  # Wait for the indexing to finish
  waitForIndexFinishScan(env, 'idx')

  # Verify ft info
  error_dict = get_index_errors_dict(env)

  expected_error_dict = {
                        indexing_failures_str: 1, # 1 OOM error
                        last_indexing_error_str:  "Used memory is more than 80 percent of max memory, cancelling the scan",
                        bgIndexingStatusStr: OOMfailureStr,
                        }
  # Last indexing error key is not checked because it is not deterministic
  # OOM is triggered after the first doc, the second doc is not indexed
  assertEqual_dicts_on_intersection(env, error_dict, expected_error_dict)

  # Verify info metric
  # Only one index was created
  index_oom_count = env.cmd('INFO', 'modules')['search_OOM_indexing_failures_indexes_count']
  env.assertEqual(index_oom_count, 1)

  # Check verbosity of HSET after OOM
  env.expect('HSET', 'NewDoc', 'name', f'DoNotIndex').equal(1)
  # Verify that the new doc was not indexed
  docs_in_index = get_index_num_docs(env)
  env.assertEqual(docs_in_index, 1)

  error_dict = get_index_errors_dict(env)
  # Assert error dict
  expected_error_dict = {
                        indexing_failures_str: expected_error_dict[indexing_failures_str]+1, # Add 1 to the count
                        last_indexing_error_str: OOM_indexing_failure_str,
                        last_indexing_error_key_str: 'NewDoc', # OOM error triggered by the new doc
                        bgIndexingStatusStr: OOMfailureStr,
                        }
  env.assertEqual(error_dict, expected_error_dict)

  # Update a doc indexed
  # The doc should not be reindexed
  env.expect('HSET', 'doc0', 'name', 'newName').equal(0)
  docs_in_index = get_index_num_docs(env)
  env.assertEqual(docs_in_index, 1)
  # Verify Index Errors
  error_dict = get_index_errors_dict(env)
  # Assert error dict
  expected_error_dict = {
                        indexing_failures_str: expected_error_dict[indexing_failures_str]+1, # Add 1 to the count
                        last_indexing_error_str: OOM_indexing_failure_str,
                        last_indexing_error_key_str: 'doc0', # OOM error triggered by the new doc
                        bgIndexingStatusStr: OOMfailureStr,
                        }
  env.assertEqual(error_dict, expected_error_dict)
  # Check resp3 warning for OOM
  res = env.cmd('FT.SEARCH', 'idx','*')
  warning = res['warning'][0]
  env.assertEqual(warning, 'Index contains partial data due to an indexing failure caused by insufficient memory')
  # Check resp3 warning in FT.PROFILE
  res = env.cmd('FT.PROFILE', 'idx', 'SEARCH','QUERY', '*')
  warning = res['Results']['warning'][0]
  env.assertEqual(warning, 'Index contains partial data due to an indexing failure caused by insufficient memory')

@skip(cluster=True)
def test_idx_delete_during_bg_indexing(env):
  oom_test_config(env)

  # Test deleting an index while it is being indexed in the background
  n_docs = 10000
  for i in range(n_docs):
    env.expect('HSET', f'doc{i}', 't', f'hello{i}').equal(1)

  # Set pause before indexing
  env.expect(bgScanCommand(), 'SET_PAUSE_BEFORE_SCAN', 'true').ok()
  # Create an index with a text field.
  env.expect('ft.create', 'idx', 'SCHEMA', 't', 'text').ok()
  waitForIndexStatus(env, 'NEW')
  # Delete the index
  env.expect('ft.dropindex', 'idx').ok()
  # Resume indexing
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
  # Check that the index does not exist
  env.expect('ft._list').equal([])

  env.expect(bgScanCommand(), 'SET_PAUSE_ON_SCANNED_DOCS', n_docs//2).ok()

  env.expect('ft.create', 'idx', 'SCHEMA', 't', 'text').ok()
  waitForIndexStatus(env, 'NEW')
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()

  waitForIndexPauseScan(env, 'idx')
  # Delete the index
  env.expect('ft.dropindex', 'idx').ok()
  # Resume indexing
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
  # After the following line, the background indexing should be completed
  env.expect(bgScanCommand(), 'TERMINATE_BG_POOL').ok()
  # Check that the index does not exist
  env.expect('ft._list').equal([])

@skip(cluster=True)
def test_delete_docs_during_bg_indexing(env):
  oom_test_config(env)

  # Test deleting docs while they are being indexed in the background
  # Using a large number of docs to make sure the test is not flaky
  n_docs = 10000
  for i in range(n_docs):
    env.expect('HSET', f'doc{i}', 't', f'hello{i}').equal(1)

  # Set delta to 100
  delta = n_docs//100

  env.expect(config_cmd(), 'SET', 'FORK_GC_CLEAN_THRESHOLD', '0').ok()
  env.expect(config_cmd(), 'SET', 'FORK_GC_RUN_INTERVAL', '30000').ok()
  env.expect(bgScanCommand(), 'SET_PAUSE_ON_OOM', 'true').ok()

  # Set pause before indexing
  env.expect(bgScanCommand(), 'SET_PAUSE_BEFORE_SCAN', 'true').ok()

  idx_str = 'idx'
  env.expect('ft.create', idx_str, 'SCHEMA', 't', 'text').ok()
  waitForIndexStatus(env, 'NEW')
  # Set tight memory limit
  set_tight_maxmemory_for_oom(env, 0.85)

  # Delete the 1000 first docs
  for i in range(n_docs//10):
    env.expect('DEL', f'doc{i}').equal(1)
  forceInvokeGC(env, idx = idx_str)

  # Resume indexing
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
  waitForIndexStatus(env, 'PAUSED_ON_OOM', idx_str)
  # Resume the indexing
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
  # Wait for the indexing to finish
  waitForIndexFinishScan(env, idx_str)

  # Verify OOM status
  error_dict = get_index_errors_dict(env, idx_str)
  env.assertEqual(error_dict[bgIndexingStatusStr], OOMfailureStr)



@skip(cluster=True)
def test_change_config_during_bg_indexing(env):
  oom_test_config(env)

  # Test deleting docs while they are being indexed in the background
  # Using a large number of docs to make sure the test is not flaky
  n_docs = 10000
  for i in range(n_docs):
    env.expect('HSET', f'doc{i}', 't', f'hello{i}').equal(1)

  env.expect('FT.CONFIG', 'SET', '_BG_INDEX_MEM_PCT_THR', '70').ok()
  # Set pause after half of the docs were scanned
  env.expect(bgScanCommand(), 'SET_PAUSE_ON_SCANNED_DOCS', n_docs//10).ok()
  env.expect(bgScanCommand(), 'SET_PAUSE_ON_OOM', 'true').ok()

  # Create an index with a text field.
  env.expect('ft.create', 'idx', 'SCHEMA', 't', 'text').ok()
  waitForIndexPauseScan(env, 'idx')
  # Set tight memory limit
  set_tight_maxmemory_for_oom(env, 0.7)
  # Change the memory limit
  env.expect('FT.CONFIG', 'SET', '_BG_INDEX_MEM_PCT_THR', '80').ok()
  # Resume indexing
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()

  waitForIndexStatus(env, 'PAUSED_ON_OOM','idx')
  # Resume the indexing
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
  # Wait for the indexing to finish
  waitForIndexFinishScan(env, 'idx')
  # Verify memory consumption
  memory_ratio = get_memory_consumption_ratio(env)
  env.assertAlmostEqual(memory_ratio, 0.85, delta=0.1)

def test_cluster_oom_all_shards(env):
  # Change the memory limit to 80% so it can be tested without redis memory limit taking effect
  verify_command_OK_on_all_shards(env,' '.join(['_FT.CONFIG', 'SET', '_BG_INDEX_MEM_PCT_THR', '80']))

  conn = getConnectionByEnv(env)
  n_docs_per_shard = 1000
  n_docs = n_docs_per_shard * env.shardsCount
  for i in range(n_docs):
    res = conn.execute_command('HSET', f'doc{i}', 't', f'text{i}')
    env.assertEqual(res, 1)

  # Set pause on OOM for all shards
  pause_on_oom_cmd = ' '.join([bgScanCommand(),' SET_PAUSE_ON_OOM', 'true'])
  run_command_on_all_shards(env, pause_on_oom_cmd)
  # Set pause on half of the docs for all shards
  pause_on_scanned_docs_cmd = ' '.join([bgScanCommand(),' SET_PAUSE_ON_SCANNED_DOCS', str(n_docs_per_shard//50)])
  run_command_on_all_shards(env, pause_on_scanned_docs_cmd)
  # Set pause before scan for all shards
  pause_before_scan_cmd = ' '.join([bgScanCommand(),' SET_PAUSE_BEFORE_SCAN', 'true'])
  run_command_on_all_shards(env, pause_before_scan_cmd)

  # Create an index
  idx_str = 'idx'
  res = conn.execute_command('FT.CREATE', idx_str, 'SCHEMA', 'txt', 'TEXT')
  env.assertEqual(res, 'OK')
  # Wait for pause before scan
  allShards_waitForIndexStatus(env, 'NEW', idx_str)
  # Resume all shards
  resume_cmd = ' '.join([bgScanCommand(),' SET_BG_INDEX_RESUME'])
  run_command_on_all_shards(env, resume_cmd)
  # Wait for pause on docs scanned
  allShards_waitForIndexPauseScan(env, idx_str)
  # Set tight memory limit for all shards
  allShards_set_tight_maxmemory_for_oom(env, 0.85)
  run_command_on_all_shards(env, resume_cmd)
  # Wait for OOM on all shards
  allShards_waitForIndexStatus(env, 'PAUSED_ON_OOM', idx_str)
  # Resume all shards
  run_command_on_all_shards(env, resume_cmd)
  # Resume all shards
  # Wait for finish scan on all shards
  allShards_waitForIndexFinishScan(env, idx_str)

  # Verify OOM status
  error_dict = get_index_errors_dict(env, idx_str)
  env.assertEqual(error_dict[bgIndexingStatusStr], OOMfailureStr)
  # Verify all shards individual OOM status
  for shard_id in range(1, env.shardsCount + 1):
    res = env.getConnection(shard_id).execute_command('INFO', 'modules')['search_OOM_indexing_failures_indexes_count']
    env.assertEqual(res,1)


def test_cluster_oom_single_shard(env):
  # Change the memory limit to 80% so it can be tested without redis memory limit taking effect
  verify_command_OK_on_all_shards(env,' '.join(['_FT.CONFIG', 'SET', '_BG_INDEX_MEM_PCT_THR', '80']))

  conn = getConnectionByEnv(env)
  n_docs_per_shard = 1000
  n_docs = n_docs_per_shard * env.shardsCount
  for i in range(n_docs):
    res = conn.execute_command('HSET', f'doc{i}', 't', f'text{i}')
    env.assertEqual(res, 1)

  oom_shard_id =  env.shardsCount

  # Set pause on OOM for all shards
  pause_on_oom_cmd = ' '.join([bgScanCommand(),' SET_PAUSE_ON_OOM', 'true'])
  run_command_on_all_shards(env, pause_on_oom_cmd)
  # Set pause on half of the docs for all shards
  pause_on_scanned_docs_cmd = ' '.join([bgScanCommand(),' SET_PAUSE_ON_SCANNED_DOCS', str(n_docs_per_shard//50)])
  run_command_on_all_shards(env, pause_on_scanned_docs_cmd)
  # Set pause before scan for all shards
  pause_before_scan_cmd = ' '.join([bgScanCommand(),' SET_PAUSE_BEFORE_SCAN', 'true'])
  run_command_on_all_shards(env, pause_before_scan_cmd)

  # Create an index
  idx_str = 'idx'
  res = conn.execute_command('FT.CREATE', idx_str, 'SCHEMA', 'txt', 'TEXT')
  env.assertEqual(res, 'OK')
  # Wait for pause before scan
  allShards_waitForIndexStatus(env, 'NEW', idx_str)
  # Resume all shards
  resume_cmd = ' '.join([bgScanCommand(),' SET_BG_INDEX_RESUME'])
  run_command_on_all_shards(env, resume_cmd)
  # Wait for pause on docs scanned
  allShards_waitForIndexPauseScan(env, idx_str)
  # Set tight memory limit for one shard
  shard_set_tight_maxmemory_for_oom(env, oom_shard_id, 0.85)
  # Resume all shards
  run_command_on_all_shards(env, resume_cmd)
  # Wait for OOM on shard
  shard_waitForIndexStatus(env, oom_shard_id, 'PAUSED_ON_OOM', idx_str)
  # Resume OOM shards
  env.getConnection(oom_shard_id).execute_command(bgScanCommand(), 'SET_BG_INDEX_RESUME')

  # Wait for finish scan on all shards
  allShards_waitForIndexFinishScan(env, idx_str)

  error_dict = get_index_errors_dict(env, idx_str)
  env.assertEqual(error_dict[bgIndexingStatusStr], OOMfailureStr)
  # Verify all shards individual OOM status
  # Cannot use FT.INFO on a specific shard, so we use the info metric
  for shard_id in range(1, env.shardsCount):
    res = env.getConnection(shard_id).execute_command('INFO', 'modules')['search_OOM_indexing_failures_indexes_count']
    env.assertEqual(res, 0)
  # Verify the shard that triggered OOM
  res = env.getConnection(oom_shard_id).execute_command('INFO', 'modules')['search_OOM_indexing_failures_indexes_count']
  env.assertEqual(res, 1)

@skip(cluster=True, no_json=True)
def test_oom_json(env):
  oom_test_config(env)

  num_docs = 10
  for i in range(num_docs):
      env.expect('JSON.SET', f'jsonDoc{i}', '.', '{"name":"jsonName"}').ok()
  # Set pause on OOM
  env.expect(bgScanCommand(), 'SET_PAUSE_ON_OOM', 'true').ok()
  # Set pause before scanning
  env.expect(bgScanCommand(), 'SET_PAUSE_ON_SCANNED_DOCS', 1).ok()
  # Create an index
  env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA', 'name', 'TEXT').ok()
  # Wait for pause before scanning
  waitForIndexPauseScan(env, 'idx')
  # Set tight memory limit
  set_tight_maxmemory_for_oom(env, 0.85)
  # Resume indexing
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
  # Wait for OOM
  waitForIndexStatus(env, 'PAUSED_ON_OOM','idx')
  # Resume the indexing
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
  # Wait for the indexing to finish
  waitForIndexFinishScan(env, 'idx')

  # Verify ft info
  error_dict = get_index_errors_dict(env)

  expected_error_dict = {
                        indexing_failures_str: 1, # 1 OOM error
                        last_indexing_error_str:  "Used memory is more than 80 percent of max memory, cancelling the scan",
                        bgIndexingStatusStr: OOMfailureStr,
                        }
  # Last indexing error key is not checked because it is not deterministic
  # OOM is triggered after the first doc, the second doc is not indexed
  assertEqual_dicts_on_intersection(env, error_dict, expected_error_dict)

  # Check verbosity of json.set after OOM
  env.expect('JSON.SET', 'jsonDoc', '.', '{"name":"jsonName"}').ok()
  # Verify that the new doc was not indexed
  docs_in_index = get_index_num_docs(env)
  env.assertEqual(docs_in_index, 1)
  # Verify Index Errors
  error_dict = get_index_errors_dict(env)
  # Assert error dict
  expected_error_dict = {
                        indexing_failures_str: expected_error_dict[indexing_failures_str]+1, # Add 1 to the count
                        last_indexing_error_str: OOM_indexing_failure_str,
                        last_indexing_error_key_str: 'jsonDoc', # OOM error triggered by the new doc
                        bgIndexingStatusStr: OOMfailureStr,
                        }
  env.assertEqual(error_dict, expected_error_dict)

@skip(cluster=True)
def test_oom_100_percent(env):
  # Test the default behavior of 100% memory limit w.r.t redis memory limit (also 100%)
  n_docs = 100
  for i in range(n_docs):
    env.expect('HSET', f'doc{i}', 't', f'hello{i}').equal(1)
  set_tight_maxmemory_for_oom(env)

  # set pause on OOM
  env.expect(bgScanCommand(), 'SET_PAUSE_ON_OOM', 'true').ok()
  # Set pause before scanning
  env.expect(bgScanCommand(), 'SET_PAUSE_BEFORE_SCAN', 'true').ok()

  # Create an index, should trigger redis level OOM
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').error().contains("command not allowed when used memory > 'maxmemory'.")
  set_unlimited_maxmemory_for_oom(env)
  # Create an index, should not trigger OOM
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
  # Wait for pause before scanning
  waitForIndexStatus(env, 'NEW', 'idx')
  # Set tight memory limit
  set_tight_maxmemory_for_oom(env)
  # Resume indexing
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
  # Wait for OOM
  waitForIndexStatus(env, 'PAUSED_ON_OOM','idx')
  # Resume the indexing
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
  # Wait for the indexing to finish
  waitForIndexFinishScan(env, 'idx')
  # Verify OOM status
  error_dict = get_index_errors_dict(env)
  env.assertEqual(error_dict[bgIndexingStatusStr], OOMfailureStr)

@skip(cluster=True)
def test_pseudo_enterprise_oom_retry_success(env):
  oom_pseudo_enterprise_config(env)

  num_docs = 1000
  for i in range(num_docs):
    env.expect('HSET', f'doc{i}', 'name', f'name{i}').equal(1)

  env.expect(bgScanCommand(), 'SET_PAUSE_ON_OOM', 'true').ok()
  num_docs_scanned = num_docs//4
  env.expect(bgScanCommand(), 'SET_PAUSE_ON_SCANNED_DOCS', num_docs_scanned).ok()
  env.expect(bgScanCommand(), 'SET_PAUSE_BEFORE_OOM_RETRY', 'true').ok()

  # Create an index
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
  waitForIndexPauseScan(env, 'idx')
  # At this point num_docs_scanned were scanned
  # Now we set the tight memory limit
  set_tight_maxmemory_for_oom(env, 0.85)

  # Resume PAUSE ON SCANNED DOCS
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
  # Wait for OOM
  waitForIndexStatus(env, 'PAUSED_BEFORE_OOM_RETRY','idx')
  # At this point the scan should be paused before OOM retry
  # Increase memory during the pause, emulating resource allocation
  set_unlimited_maxmemory_for_oom(env)
  # Resume PAUSE BEFORE OOM RETRY
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()

  # Verify that the indexing finished
  waitForIndexFinishScan(env, 'idx')
  # Verify that all docs were indexed
  docs_in_index = get_index_num_docs(env)
  index_errors = get_index_errors_dict(env)
  env.assertEqual(docs_in_index, num_docs)
  # Verify index BG indexing status is OK
  env.assertEqual(index_errors[bgIndexingStatusStr], 'OK')

@skip(cluster=True)
def test_pseudo_enterprise_oom_retry_failure(env):
  oom_pseudo_enterprise_config(env)

  num_docs = 1000
  for i in range(num_docs):
    env.expect('HSET', f'doc{i}', 'name', f'name{i}').equal(1)

  env.expect(bgScanCommand(), 'SET_PAUSE_ON_OOM', 'true').ok()
  num_docs_scanned = num_docs//4
  env.expect(bgScanCommand(), 'SET_PAUSE_ON_SCANNED_DOCS', num_docs_scanned).ok()
  env.expect(bgScanCommand(), 'SET_PAUSE_BEFORE_OOM_RETRY', 'true').ok()

  # Create an index
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
  waitForIndexPauseScan(env, 'idx')

  # At this point num_docs_scanned were scanned
  # Now we set the tight memory limit
  set_tight_maxmemory_for_oom(env, 0.85)

  # Resume PAUSE ON SCANNED DOCS
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
  waitForIndexStatus(env, 'PAUSED_BEFORE_OOM_RETRY','idx')
  # Resume PAUSE BEFORE OOM RETRY
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
  # Since we are not increasing the memory, the scan should be paused on OOM
  waitForIndexStatus(env, 'PAUSED_ON_OOM','idx')
  # Resume PAUSE ON OOM
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()

  # Wait for the indexing to finish
  waitForIndexFinishScan(env, 'idx')
  # Verify OOM status
  error_dict = get_index_errors_dict(env)
  env.assertEqual(error_dict[bgIndexingStatusStr], OOMfailureStr)

@skip(cluster=True)
def test_pseudo_enterprise_oom_multiple_retry_success(env):
  oom_pseudo_enterprise_config(env)

  num_docs = 1000
  for i in range(num_docs):
    env.expect('HSET', f'doc{i}', 'name', f'name{i}').equal(1)

  env.expect(bgScanCommand(), 'SET_PAUSE_BEFORE_OOM_RETRY', 'true').ok()

  runs = 2
  run = 1

  num_docs_scanned = num_docs//(runs*2)
  env.expect(bgScanCommand(), 'SET_PAUSE_ON_SCANNED_DOCS', num_docs_scanned).ok()

  # Create an index
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()

  while run <= runs:

    waitForIndexPauseScan(env, 'idx')
    # At this point num_docs_scanned were scanned
    # Now we set the tight memory limit
    set_tight_maxmemory_for_oom(env, 0.85)


    # Update the number of scanned docs to pause on for the next run
    if run < runs:
      num_docs_scanned = ((run+1) * num_docs)//(runs*2)
      env.expect(bgScanCommand(), 'SET_PAUSE_ON_SCANNED_DOCS', num_docs_scanned).ok()
      env.expect(bgScanCommand(), 'DEBUG_SCANNER_UPDATE_CONFIG', 'idx').ok()

    # Resume PAUSE ON SCANNED DOCS
    env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
    # Wait for OOM
    waitForIndexStatus(env, 'PAUSED_BEFORE_OOM_RETRY','idx')
    # At this point the scan should be paused before OOM retry
    # Increase memory during the pause, emulating resource allocation
    set_unlimited_maxmemory_for_oom(env)
    # Resume PAUSE BEFORE OOM RETRY
    env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
    run+=1

  # Verify that the indexing finished
  waitForIndexFinishScan(env, 'idx')
  # Verify that all docs were indexed
  docs_in_index = get_index_num_docs(env)
  index_errors = get_index_errors_dict(env)
  env.assertEqual(docs_in_index, num_docs)
  # Verify index BG indexing status is OK
  env.assertEqual(index_errors[bgIndexingStatusStr], 'OK')

@skip(cluster=True)
def test_pseudo_enterprise_oom_multiple_retry_failure(env):
  oom_pseudo_enterprise_config(env)

  num_docs = 1000
  for i in range(num_docs):
    env.expect('HSET', f'doc{i}', 'name', f'name{i}').equal(1)

  env.expect(bgScanCommand(), 'SET_PAUSE_ON_OOM', 'true').ok()
  env.expect(bgScanCommand(), 'SET_PAUSE_BEFORE_OOM_RETRY', 'true').ok()

  runs = 2
  run = 1

  num_docs_scanned = num_docs//(runs*2)
  env.expect(bgScanCommand(), 'SET_PAUSE_ON_SCANNED_DOCS', num_docs_scanned).ok()

  # Create an index
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()

  while run <= runs:

    waitForIndexPauseScan(env, 'idx')
    # At this point num_docs_scanned were scanned
    # Now we set the tight memory limit
    set_tight_maxmemory_for_oom(env, 0.85)

    # Update the number of scanned docs to pause on for the next run
    if run < runs:
      num_docs_scanned = ((run+1) * num_docs)//(runs*2)
      env.expect(bgScanCommand(), 'SET_PAUSE_ON_SCANNED_DOCS', num_docs_scanned).ok()
      env.expect(bgScanCommand(), 'DEBUG_SCANNER_UPDATE_CONFIG', 'idx').ok()

    # Resume PAUSE ON SCANNED DOCS
    env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
    # Wait for OOM
    waitForIndexStatus(env, 'PAUSED_BEFORE_OOM_RETRY','idx')
    # At this point the scan should be paused before OOM retry

    # Increase memory during the pause, emulating resource allocation, only if not in the last run
    if run < runs:
      set_unlimited_maxmemory_for_oom(env)
    # Resume PAUSE BEFORE OOM RETRY
    env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
    run+=1

  # Since we are not increasing the memory, the scan should be paused on OOM
  waitForIndexStatus(env, 'PAUSED_ON_OOM','idx')
  # Resume PAUSE ON OOM
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()

  # Wait for the indexing to finish
  waitForIndexFinishScan(env, 'idx')
  # Verify OOM status
  error_dict = get_index_errors_dict(env)
  env.assertEqual(error_dict[bgIndexingStatusStr], OOMfailureStr)

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

def test_pseudo_enterprise_cluster_oom_retry_success(env):
    # Let background indexing go up to 80 % of Redis' limit
    verify_command_OK_on_all_shards(
        env, '_FT.CONFIG SET _BG_INDEX_MEM_PCT_THR 80')
    # 1-second grace so the test doesn’t take too long
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

    # Resume – this will push every shard into PAUSED_BEFORE_OOM_RETRY
    run_command_on_all_shards(env,
        f'{bgScanCommand()} SET_BG_INDEX_RESUME')
    allShards_waitForIndexStatus(env, 'PAUSED_BEFORE_OOM_RETRY', idx)

    # While paused, free memory so the retry can succeed
    allShards_set_unlimited_maxmemory_for_oom(env)

    # Resume again – indexing should now complete
    run_command_on_all_shards(env,
        f'{bgScanCommand()} SET_BG_INDEX_RESUME')
    allShards_waitForIndexFinishScan(env, idx)

    indexed_num_docs = get_index_num_docs(env, idx=idx)
    index_errors = get_index_errors_dict(env, idx=idx)
    env.assertEqual(indexed_num_docs, total_docs)
    env.assertEqual(index_errors[bgIndexingStatusStr], 'OK')
    # Every shard’s failure counter must stay at 0
    for shard_id in range(1, env.shardsCount + 1):
        failures = env.getConnection(shard_id).execute_command(
            'INFO', 'modules')['search_OOM_indexing_failures_indexes_count']
        env.assertEqual(failures, 0)

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

    # Resume – shards pause *before* OOM retry
    run_command_on_all_shards(env,
        f'{bgScanCommand()} SET_BG_INDEX_RESUME')
    allShards_waitForIndexStatus(env, 'PAUSED_BEFORE_OOM_RETRY', idx)

    # Resume again with memory still tight → PAUSED_ON_OOM
    run_command_on_all_shards(env,
        f'{bgScanCommand()} SET_BG_INDEX_RESUME')
    allShards_waitForIndexStatus(env, 'PAUSED_ON_OOM', idx)

    # One last resume – the second OOM turns into failure
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
