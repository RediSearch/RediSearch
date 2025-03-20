from common import *

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
    env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME','true').ok()
    # Wait for OOM
    waitForIndexStatus(env, 'PAUSED_ON_OOM','idx')
    # Resume the indexing
    env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME','true').ok()
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
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME','true').ok()
  # Wait for OOM
  waitForIndexStatus(env, 'PAUSED_ON_OOM','idx')
  # Resume the indexing
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME','true').ok()
  # Wait for the indexing to finish
  waitForIndexFinishScan(env, 'idx')

  # Verify ft info
  info = index_info(env)
  error_dict = to_dict(info["Index Errors"])
  bgIndexingStatusStr = "background indexing status"
  OOMfailureStr = "OOM failure"
  env.assertEqual(error_dict[bgIndexingStatusStr], OOMfailureStr)

  # Verify info metric
  # Only one index was created
  index_oom_count = env.cmd('INFO', 'modules')['search_OOM_indexing_failures_indexes_count']
  env.assertEqual(index_oom_count, 1)

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
  env.expect('ft.drop', 'idx').ok()
  # Resume indexing
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME', 'true').ok()
  # Check that the index does not exist
  env.expect('ft._list').equal([])

  env.expect(bgScanCommand(), 'SET_PAUSE_ON_SCANNED_DOCS', n_docs//2).ok()

  env.expect('ft.create', 'idx', 'SCHEMA', 't', 'text').ok()
  waitForIndexStatus(env, 'NEW')
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME', 'true').ok()

  waitForIndexPauseScan(env, 'idx')
  # Delete the index
  env.expect('ft.drop', 'idx').ok()
  # Resume indexing
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME', 'true').ok()
  # After the following line, the background indexing should be completed
  env.expect(bgScanCommand(), 'TERMINATE_BG_POOL').ok()
  # Check that the index does not exist
  env.expect('ft._list').equal([])
  #

@skip(cluster=True)
def test_delete_docs_during_bg_indexing(env):
  # Test deleting docs while they are being indexed in the background
  # Using a large number of docs to make sure the test is not flaky
  n_docs = 10000
  for i in range(n_docs):
    env.expect('HSET', f'doc{i}', 't', f'hello{i}').equal(1)

  env.expect(config_cmd(), 'SET', 'FORK_GC_CLEAN_THRESHOLD', '0').ok()
  env.expect(config_cmd(), 'SET', 'FORK_GC_RUN_INTERVAL', '30000').ok()
  env.expect(bgScanCommand(), 'SET_PAUSE_ON_OOM', 'true').ok()

  # Set pause after half of the docs were scanned
  env.expect(bgScanCommand(), 'SET_PAUSE_ON_SCANNED_DOCS', n_docs//2).ok()
  # Create an index with a text field.
  env.expect('ft.create', 'idx', 'SCHEMA', 't', 'text').ok()
  waitForIndexPauseScan(env, 'idx')
  # Set tight memory limit
  set_tight_maxmemory_for_oom(env)
  # Delete the 1000 first docs
  for i in range(n_docs//10):
    env.expect('DEL', f'doc{i}').equal(1)
  forceInvokeGC(env)
  # Resume indexing
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME', 'true').ok()
  waitForIndexStatus(env, 'PAUSED_ON_OOM','idx')
  # Resume the indexing
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME','true').ok()
  # Wait for the indexing to finish
  waitForIndexFinishScan(env, 'idx')
  # Verify OOM status
  info = index_info(env)
  error_dict = to_dict(info["Index Errors"])
  bgIndexingStatusStr = "background indexing status"
  OOMfailureStr = "OOM failure"
  env.assertEqual(error_dict[bgIndexingStatusStr], OOMfailureStr)
  # Verify that close to n_docs//2 docs were indexed
  docs_in_index = info['num_docs']
  env.assertAlmostEqual(docs_in_index, n_docs//2, delta=100)

@skip(cluster=True)
def test_change_config_during_bg_indexing(env):
  # Test deleting docs while they are being indexed in the background
  # Using a large number of docs to make sure the test is not flaky
  n_docs = 10000
  for i in range(n_docs):
    env.expect('HSET', f'doc{i}', 't', f'hello{i}').equal(1)

  env.expect('FT.CONFIG', 'SET', '_INDEX_MEM_PERCENT', '70').ok()
  # Set pause after half of the docs were scanned
  env.expect(bgScanCommand(), 'SET_PAUSE_ON_SCANNED_DOCS', n_docs//10).ok()
  env.expect(bgScanCommand(), 'SET_PAUSE_ON_OOM', 'true').ok()

  # Create an index with a text field.
  env.expect('ft.create', 'idx', 'SCHEMA', 't', 'text').ok()
  waitForIndexPauseScan(env, 'idx')
  # Set tight memory limit
  set_tight_maxmemory_for_oom(env, 0.7)
  # Change the memory limit
  env.expect('FT.CONFIG', 'SET', '_INDEX_MEM_PERCENT', '80').ok()
  # Resume indexing
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME', 'true').ok()

  waitForIndexStatus(env, 'PAUSED_ON_OOM','idx')
  # Resume the indexing
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME','true').ok()
  # Wait for the indexing to finish
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
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME', 'true').ok()
  # Wait for OOM
  waitForIndexStatus(env, 'PAUSED_ON_OOM', idx_name)
  # Resume the indexing
  env.expect(bgScanCommand(), 'SET_BG_INDEX_RESUME','true').ok()
  # Wait for the indexing to finish
  waitForIndexFinishScan(env, idx_name)

  for query,param in queries_params.items():
    parsed_query = f'FT.{query} {param}'
    env.expect(parsed_query).error().equal(f'{idx_name}: \
    Index background scan failed due to OOM. Queries cannot be executed on an incomplete index.')

  # Verify ft info possible
  env.expect('FT.INFO', idx_name).noError()
  # Verify ft drop possible
  env.expect('FT.DROP', idx_name).ok()
