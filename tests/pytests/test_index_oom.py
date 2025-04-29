from common import *

# Global variables
bgIndexingStatusStr = "background indexing status"
indexing_failures_str = 'indexing failures'
last_indexing_error_key_str = 'last indexing error key'
last_indexing_error_str = 'last indexing error'
OOM_indexing_failure_str = 'Index background scan failed due to OOM. New documents will not be indexed.'
OOMfailureStr = "OOM failure"


@skip(cluster=True)
def test_stop_background_indexing_on_low_mem(env):
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
    set_tight_maxmemory_for_oom(env)
    # After we resume, an OOM should trigger
    env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
    # Wait for OOM
    waitForIndexStatus(env, 'PAUSED_ON_OOM','idx')
    # Resume the indexing
    env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
    # Wait for the indexing to finish
    waitForIndexFinishScan(env, 'idx')
    # Verify that only num_docs_scanned were indexed
    docs_in_index = index_info(env)['num_docs']
    env.assertEqual(docs_in_index, num_docs_scanned)
    # Verify that used_memory is close to 80% (default) of maxmemory
    used_memory = env.cmd('INFO', 'MEMORY')['used_memory']
    max_memory = env.cmd('INFO', 'MEMORY')['maxmemory']
    memory_ratio = used_memory / max_memory
    env.assertAlmostEqual(memory_ratio, 0.8, delta=0.1)

@skip(cluster=True)
def test_stop_indexing_low_mem_verbosity(env):
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
  set_tight_maxmemory_for_oom(env)
  # Resume indexing
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
  # Wait for OOM
  waitForIndexStatus(env, 'PAUSED_ON_OOM','idx')
  # Resume the indexing
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
  # Wait for the indexing to finish
  waitForIndexFinishScan(env, 'idx')

  # Verify ft info
  info = index_info(env)
  error_dict = to_dict(info["Index Errors"])

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
  info = index_info(env)
  # Verify that the new doc was not indexed
  docs_in_index = info['num_docs']
  env.assertEqual(docs_in_index, 1)

  error_dict = to_dict(info["Index Errors"])
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
  info = index_info(env)
  docs_in_index = info['num_docs']
  env.assertEqual(docs_in_index, 1)
  # Verify Index Errors
  error_dict = to_dict(info["Index Errors"])
  # Assert error dict
  expected_error_dict = {
                        indexing_failures_str: expected_error_dict[indexing_failures_str]+1, # Add 1 to the count
                        last_indexing_error_str: OOM_indexing_failure_str,
                        last_indexing_error_key_str: 'doc0', # OOM error triggered by the new doc
                        bgIndexingStatusStr: OOMfailureStr,
                        }
  env.assertEqual(error_dict, expected_error_dict)

@skip(cluster=True)
def test_idx_delete_during_bg_indexing(env):
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
  set_tight_maxmemory_for_oom(env)

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
  info = index_info(env,idx = idx_str)
  error_dict = to_dict(info["Index Errors"])
  bgIndexingStatusStr = "background indexing status"
  OOMfailureStr = "OOM failure"
  env.assertEqual(error_dict[bgIndexingStatusStr], OOMfailureStr)



@skip(cluster=True)
def test_change_config_during_bg_indexing(env):
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
  used_memory = env.cmd('INFO', 'MEMORY')['used_memory']
  max_memory = env.cmd('INFO', 'MEMORY')['maxmemory']
  memory_ratio = used_memory / max_memory
  env.assertAlmostEqual(memory_ratio, 0.8, delta=0.1)

@skip(cluster=True)
def test_oom_query_error(env):
  idx_name = 'idx'
  error_querys_star = ['SEARCH', 'AGGREGATE', 'TAGVALS', 'MGET']
  queries_params = {
                    'PROFILE': f'{idx_name} SEARCH QUERY * ',
                    'SYNDUMP': f'{idx_name}',
                    'ALTER': f'{idx_name} SCHEMA ADD field1 TEXT',
                  }
  queries_params.update({query: f'{idx_name} *' for query in error_querys_star})
  # Using a large number of docs to make sure the test is not flaky
  n_docs = 10000
  for i in range(n_docs):
    env.expect('HSET', f'doc{i}', 't', f'hello{i}').equal(1)
  # Set pause before indexing
  env.expect(bgScanCommand(), 'SET_PAUSE_BEFORE_SCAN', 'true').ok()
  # Set pause on OOM
  env.expect(bgScanCommand(), 'SET_PAUSE_ON_OOM', 'true').ok()
  # Create an index with a text field.
  env.expect('ft.create', idx_name, 'SCHEMA', 't', 'text').ok()
  waitForIndexStatus(env, 'NEW', idx_name)
  # Set tight memory limit
  set_tight_maxmemory_for_oom(env)
  # Resume indexing
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
  # Wait for OOM
  waitForIndexStatus(env, 'PAUSED_ON_OOM', idx_name)
  # Resume the indexing
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
  # Wait for the indexing to finish
  waitForIndexFinishScan(env, idx_name)

  for query,param in queries_params.items():
    parsed_query = f'FT.{query} {param}'
    env.expect(parsed_query).error().equal(f'Background scan for index {idx_name} failed due to OOM.'
                                           ' Queries cannot be executed on an incomplete index.')

  # Test FT.DEBUG FT.SEARCH/AGGREGATE command
  for query_type in ['SEARCH', 'AGGREGATE']:
    parsed_query = f'_FT.DEBUG FT.{query_type} {idx_name} * TIMEOUT_AFTER_N 3 DEBUG_PARAMS_COUNT 2 '
    env.expect(parsed_query).error().equal(f'Background scan for index {idx_name} failed due to OOM.'
                                           ' Queries cannot be executed on an incomplete index.')

  # Verify ft info possible
  env.expect('FT.INFO', idx_name).noError()
  # Verify ft dropindex possible
  env.expect('FT.DROPINDEX', idx_name).ok()

def test_cluster_oom_all_shards(env):
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
  allShards_set_tight_maxmemory_for_oom(env)
  run_command_on_all_shards(env, resume_cmd)
  # Wait for OOM on all shards
  allShards_waitForIndexStatus(env, 'PAUSED_ON_OOM', idx_str)
  # Resume all shards
  run_command_on_all_shards(env, resume_cmd)
  # Resume all shards
  # Wait for finish scan on all shards
  allShards_waitForIndexFinishScan(env, idx_str)

  # Verify OOM status
  info = index_info(env,idx = idx_str)
  error_dict = to_dict(info["Index Errors"])
  bgIndexingStatusStr = "background indexing status"
  OOMfailureStr = "OOM failure"
  env.assertEqual(error_dict[bgIndexingStatusStr], OOMfailureStr)
  # Verify all shards individual OOM status
  for shard_id in range(1, env.shardsCount + 1):
    res = env.getConnection(shard_id).execute_command('INFO', 'modules')['search_OOM_indexing_failures_indexes_count']
    env.assertEqual(res,1)


def test_cluster_oom_single_shard(env):
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
  shard_set_tight_maxmemory_for_oom(env, oom_shard_id)
  # Resume all shards
  run_command_on_all_shards(env, resume_cmd)
  # Wait for OOM on shard
  shard_waitForIndexStatus(env, oom_shard_id, 'PAUSED_ON_OOM', idx_str)
  # Resume OOM shards
  env.getConnection(oom_shard_id).execute_command(bgScanCommand(), 'SET_BG_INDEX_RESUME')

  # Wait for finish scan on all shards
  allShards_waitForIndexFinishScan(env, idx_str)


  info = index_info(env,idx = idx_str)
  error_dict = to_dict(info["Index Errors"])
  bgIndexingStatusStr = "background indexing status"
  OOMfailureStr = "OOM failure"
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
  set_tight_maxmemory_for_oom(env)
  # Resume indexing
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
  # Wait for OOM
  waitForIndexStatus(env, 'PAUSED_ON_OOM','idx')
  # Resume the indexing
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME').ok()
  # Wait for the indexing to finish
  waitForIndexFinishScan(env, 'idx')

  # Verify ft info
  info = index_info(env)
  error_dict = to_dict(info["Index Errors"])

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
  info = index_info(env)
  docs_in_index = info['num_docs']
  env.assertEqual(docs_in_index, 1)
  # Verify Index Errors
  error_dict = to_dict(info["Index Errors"])
  # Assert error dict
  expected_error_dict = {
                        indexing_failures_str: expected_error_dict[indexing_failures_str]+1, # Add 1 to the count
                        last_indexing_error_str: OOM_indexing_failure_str,
                        last_indexing_error_key_str: 'jsonDoc', # OOM error triggered by the new doc
                        bgIndexingStatusStr: OOMfailureStr,
                        }
  env.assertEqual(error_dict, expected_error_dict)
