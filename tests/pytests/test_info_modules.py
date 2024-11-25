from common import *
from RLTest import Env
import redis
from inspect import currentframe

def info_modules_to_dict(conn):
  res = conn.execute_command('INFO MODULES')
  info = dict()
  section_name = ""
  for line in res.splitlines():
    if line:
      if line.startswith('#'):
        section_name = line[2:]
        info[section_name] = dict()
      else:
        data = line.split(':', 1)
        info[section_name][data[0]] = data[1]
  return info

def get_search_field_info(type: str, count: int, index_errors: int = 0, **kwargs):
  # Base info
  info = {
    type: str(count),
    'IndexErrors': str(index_errors),
    **{key: str(value) for key, value in kwargs.items()}
  }
  return info

def field_info_to_dict(info):
  return {key: value for field in info.split(',') for key, value in [field.split('=')]}

def testInfoModulesBasic(env):
  conn = env.getConnection()

  idx1 = 'idx1'
  idx2 = 'idx2'
  idx3 = 'idx3'

  env.expect('FT.CREATE', idx1, 'STOPWORDS', 3, 'TLV', 'summer', '2020',
                                'SCHEMA', 'title', 'TEXT', 'SORTABLE',
                                          'body', 'TEXT', 'NOINDEX',
                                          'id', 'NUMERIC',
                                          'subject location', 'GEO',
                                          'geom', 'GEOSHAPE', 'SORTABLE'
                                          ).ok()

  env.expect('FT.CREATE', idx2, 'LANGUAGE', 'french', 'NOOFFSETS', 'NOFREQS',
                                'PREFIX', 2, 'TLV:', 'NY:',
                                'SCHEMA', 't1', 'TAG', 'CASESENSITIVE', 'SORTABLE',
                                          'T2', 'AS', 't2', 'TAG',
                                          'id', 'NUMERIC', 'NOINDEX',
                                          'geom', 'GEOSHAPE', 'NOINDEX'
                                          ).ok()

  env.expect('FT.CREATE', idx3, 'SCHEMA', 'vec_flat', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '128', 'DISTANCE_METRIC', 'L2',
                                          'vec_hnsw', 'VECTOR', 'HNSW', '14', 'TYPE', 'FLOAT32', 'DIM', '128', 'DISTANCE_METRIC', 'L2',
                                          'INITIAL_CAP', '10000', 'M', '40', 'EF_CONSTRUCTION', '250', 'EF_RUNTIME', '20').ok()

  info = info_modules_to_dict(conn)
  env.assertEqual(info['search_index']['search_number_of_indexes'], '3')
  fieldsInfo = info['search_fields_statistics']
  env.assertEqual(field_info_to_dict(fieldsInfo['search_fields_text']), get_search_field_info('Text', 2, Sortable=1, NoIndex=1))
  env.assertEqual(field_info_to_dict(fieldsInfo['search_fields_tag']), get_search_field_info('Tag', 2, Sortable=1, CaseSensitive=1))
  env.assertEqual(field_info_to_dict(fieldsInfo['search_fields_numeric']), get_search_field_info('Numeric', 2, NoIndex=1))
  env.assertEqual(field_info_to_dict(fieldsInfo['search_fields_geo']), get_search_field_info('Geo', 1))
  env.assertEqual(field_info_to_dict(fieldsInfo['search_fields_vector']), get_search_field_info('Vector', 2, Flat=1, HNSW=1))
  env.assertEqual(field_info_to_dict(fieldsInfo['search_fields_geoshape']), get_search_field_info('Geoshape', 2, Sortable=1 ,NoIndex=1))

  configInfo = info['search_runtime_configurations']
  env.assertEqual(configInfo['search_minimal_term_prefix'], '2')
  env.assertEqual(configInfo['search_gc_scan_size'], '100')

  # idx1Info = info['search_info_' + idx1]
  # env.assertTrue('search_stop_words' in idx1Info)
  # env.assertTrue('search_field_4' in idx1Info)
  # env.assertEqual(idx1Info['search_field_2'], 'identifier=body,attribute=body,type=TEXT,WEIGHT=1,NOINDEX=ON')
  # env.assertEqual(idx1Info['search_stop_words'], '"tlv","summer","2020"')

  # idx2Info = info['search_info_' + idx2]
  # env.assertTrue('search_stop_words' not in idx2Info)
  # env.assertTrue('prefixes="TLV:","NY:"' in idx2Info['search_index_definition'])
  # env.assertTrue('default_language=' in idx2Info['search_index_definition'])
  # env.assertEqual(idx2Info['search_field_2'], 'identifier=T2,attribute=t2,type=TAG,SEPARATOR=","')


def testInfoModulesAlter(env):
  conn = env.getConnection()
  idx1 = 'idx1'

  env.expect('FT.CREATE', idx1, 'SCHEMA', 'title', 'TEXT', 'SORTABLE').ok()
  env.expect('FT.ALTER', idx1, 'SCHEMA', 'ADD', 'n', 'NUMERIC', 'NOINDEX', 'geom', 'GEOSHAPE', 'SORTABLE').ok()

  info = info_modules_to_dict(conn)
  env.assertEqual(info['search_index']['search_number_of_indexes'], '1')

  fieldsInfo = info['search_fields_statistics']
  env.assertEqual(field_info_to_dict(fieldsInfo['search_fields_text']), get_search_field_info('Text', 1, Sortable=1))
  env.assertEqual(field_info_to_dict(fieldsInfo['search_fields_numeric']), get_search_field_info('Numeric', 1, NoIndex=1))
  env.assertEqual(field_info_to_dict(fieldsInfo['search_fields_geoshape']), get_search_field_info('Geoshape', 1, Sortable=1))

  # idx1Info = info['search_info_' + idx1]
  # env.assertEqual(idx1Info['search_field_2'], 'identifier=n,attribute=n,type=NUMERIC,NOINDEX=ON')


def testInfoModulesDrop(env):
  conn = env.getConnection()
  idx1 = 'idx1'
  idx2 = 'idx2'

  env.expect('FT.CREATE', idx1, 'STOPWORDS', 3, 'TLV', 'summer', '2020',
                                'SCHEMA', 'title', 'TEXT', 'SORTABLE',
                                          'body', 'TEXT').ok()

  env.expect('FT.CREATE', idx2, 'SCHEMA', 'title', 'TEXT', 'SORTABLE',
                                          'body', 'TEXT',
                                          'id', 'NUMERIC', 'NOINDEX').ok()

  env.expect('FT.DROP', idx2).ok()

  info = info_modules_to_dict(conn)
  env.assertEqual(info['search_index']['search_number_of_indexes'], '1')

  fieldsInfo = info['search_fields_statistics']
  env.assertEqual(field_info_to_dict(fieldsInfo['search_fields_text']), get_search_field_info('Text', 2, Sortable=1))
  env.assertFalse('search_fields_numeric' in fieldsInfo) # no numeric fields since we removed idx2


def testInfoModulesAfterReload(env):
  conn = env.getConnection()
  idx1 = 'idx1'

  env.expect('FT.CREATE', idx1, 'SCHEMA', 'age', 'NUMERIC', 'SORTABLE',
                                          'geo', 'GEO', 'SORTABLE', 'NOINDEX',
                                          'body', 'TAG', 'NOINDEX').ok()

  for _ in env.retry_with_rdb_reload():
    info = info_modules_to_dict(conn)
    env.assertEqual(info['search_index']['search_number_of_indexes'], '1')

    fieldsInfo = info['search_fields_statistics']
    env.assertFalse('search_fields_text' in fieldsInfo) # no text fields
    env.assertEqual(field_info_to_dict(fieldsInfo['search_fields_numeric']), get_search_field_info('Numeric', 1, Sortable=1))
    env.assertEqual(field_info_to_dict(fieldsInfo['search_fields_geo']), get_search_field_info('Geo', 1, Sortable=1, NoIndex=1))
    env.assertEqual(field_info_to_dict(fieldsInfo['search_fields_tag']), get_search_field_info('Tag', 1, NoIndex=1))

# This tests relies on shard info, which depends on the hashes in the *shard*.
# In cluster mode, hashes might be stored in different shards, and the shard we call INFO for,
# will not be aware of the index failures they cause.
@skip(cluster=True)
def test_redis_info_errors():

  env = Env(moduleArgs='DEFAULT_DIALECT 2')
  conn = getConnectionByEnv(env)

  # Create two indices
  env.cmd('FT.CREATE', 'idx1', 'SCHEMA', 'n', 'NUMERIC')
  env.cmd('FT.CREATE', 'idx2', 'SCHEMA', 'n2', 'NUMERIC')

  expected = {
    'number_of_indexes': 2,
    'fields_numeric_count': 2,
    'idx1_errors': 0,
    'idx2_errors': 0,
  }

  def validate_info_output(message):

    # Call `INFO` and check that the index is there
    res = conn.execute_command('INFO', 'MODULES')

    env.assertEqual(res['search_number_of_indexes'], expected['number_of_indexes'], message=message + " failed in number of indexes")
    env.assertEqual(res['search_fields_numeric']['Numeric'], expected['fields_numeric_count'], message=message + " failed in number of numeric fields")
    expected_total_errors = expected['idx1_errors'] + expected['idx2_errors']

    # field level errors count
    env.assertEqual(res['search_fields_numeric']['IndexErrors'], expected_total_errors, message=message + " failed in number of numeric:IndexErrors")

    # Index level errors count
    env.assertEqual(res['search_errors_indexing_failures'], expected_total_errors, message=message + " failed in number of IndexErrors")
    env.assertEqual(res['search_errors_for_index_with_max_failures'], max(expected['idx1_errors'], expected['idx2_errors']), message=message + " failed in max number of IndexErrors")

  # Add a document we will fail to index in both indices
  conn.execute_command('HSET', f'doc:1', 'n', f'meow', 'n2', f'meow')
  expected['idx1_errors'] += 1
  expected['idx2_errors'] += 1
  validate_info_output(message='fail both indices')

  # Add a document that we will fail to index in idx1, and succeed in idx2
  conn.execute_command('HSET', f'doc:2', 'n', f'meow', 'n2', '4')
  expected['idx1_errors'] += 1
  validate_info_output(message='fail one index')

  # Add the failing field to idx2
  # expect that the error count will increase due to bg indexing of 2 documents with invalid numeric values.
  conn.execute_command('FT.ALTER', 'idx2', 'SCHEMA', 'ADD', 'n', 'NUMERIC')
  waitForIndex(env, 'idx2')
  expected['fields_numeric_count'] += 1
  expected['idx2_errors'] += 2
  validate_info_output(message='add failing field to idx2')

  # Drop one index and expect the errors counter to decrease
  conn.execute_command('FT.DROPINDEX', 'idx1')
  expected['number_of_indexes'] -= 1
  expected['fields_numeric_count'] -= 1
  expected['idx1_errors'] = 0
  validate_info_output(message='drop one index')

@skip(cluster=True)
def test_redis_info_errors_json():

  env = Env(moduleArgs='DEFAULT_DIALECT 2')
  conn = getConnectionByEnv(env)

  # Create two indices
  env.cmd('FT.CREATE', 'idx1', 'ON', 'JSON', 'SCHEMA', '$.n', 'AS', 'n', 'NUMERIC')
  env.cmd('FT.CREATE', 'idx2', 'ON', 'JSON', 'SCHEMA', '$.n2', 'AS', 'n2', 'NUMERIC')

  expected = {
    'number_of_indexes': 2,
    'fields_numeric_count': 2,
    'idx1_errors': 0,
    'idx2_errors': 0,
  }

  def validate_info_output(message):

    # Call `INFO` and check that the index is there
    res = conn.execute_command('INFO', 'MODULES')

    env.assertEqual(res['search_number_of_indexes'], expected['number_of_indexes'], message=message + " failed in number of indexes")
    env.assertEqual(res['search_fields_numeric']['Numeric'], expected['fields_numeric_count'], message=message + " failed in number of numeric fields")
    expected_total_errors = expected['idx1_errors'] + expected['idx2_errors']

    # field level errors count
    env.assertEqual(res['search_fields_numeric']['IndexErrors'], expected_total_errors, message=message + " failed in number of numeric:IndexErrors")

    # Index level errors count
    env.assertEqual(res['search_errors_indexing_failures'], expected_total_errors, message=message + " failed in number of IndexErrors")
    env.assertEqual(res['search_errors_for_index_with_max_failures'], max(expected['idx1_errors'], expected['idx2_errors']), message=message + " failed in max number of IndexErrors")

  # Add a document we will fail to index in both indices
  json_val = r'{"n":"meow","n2":"meow"}'
  env.expect('JSON.SET', 'doc:1', '$', json_val).ok()
  expected['idx1_errors'] += 1
  expected['idx2_errors'] += 1
  validate_info_output(message='fail both indices')

  # Add a document that we will fail to index in idx1, and succeed in idx2
  json_val = r'{"n":"meow","n2":4}'
  env.expect('JSON.SET', 'doc:2', '$', json_val).ok()
  expected['idx1_errors'] += 1
  validate_info_output(message='fail one index')

  # Add the failing field to idx2
  # expect that the error count will increase due to bg indexing of 2 documents with invalid numeric values.
  conn.execute_command('FT.ALTER', 'idx2', 'SCHEMA', 'ADD', '$.n', 'AS', 'n', 'NUMERIC')
  waitForIndex(env, 'idx2')
  expected['fields_numeric_count'] += 1
  expected['idx2_errors'] += 2
  validate_info_output(message='add failing field to idx2')

  # Drop one index and expect the errors counter to decrease
  conn.execute_command('FT.DROPINDEX', 'idx1')
  expected['number_of_indexes'] -= 1
  expected['fields_numeric_count'] -= 1
  expected['idx1_errors'] = 0
  validate_info_output(message='drop one index')

#ensure update with alter and drop index
def test_redis_info():
  """Tests that the Redis `INFO` command works as expected"""

  env = Env(moduleArgs='DEFAULT_DIALECT 2')
  conn = getConnectionByEnv(env)

  # Create an index
  env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'txt', 'TEXT', 'tag', 'TAG', 'SORTABLE')

  # Add some data
  n_docs = 10000
  for i in range(n_docs):
    conn.execute_command('HSET', f'h{i}', 'txt', f'hello{i}', 'tag', f'tag{i}')

  # Call `INFO` and check that the index is there
  res = env.cmd('INFO', 'MODULES')

  env.assertEqual(res['search_number_of_indexes'], 1)

  # ========== Field statistics ==========
  # amanzonlinux:2 install redis version '5.1.0a1' which has different output
  if redis.__version__ >= '5.0.5' and redis.__version__ != '5.1.0a1':
    env.assertEqual(res['search_fields_text']['Text'], 1)
  else:
    env.assertEqual(res['search_fields_text'], 'Text=1')

  env.assertEqual(res['search_fields_tag']['Tag'], 1)

  # ========== Memory statistics ==========
  env.assertGreater(res['search_used_memory_indexes'], 0)
  env.assertGreater(res['search_used_memory_indexes_human'], 0)
  env.assertGreater(res['search_largest_memory_index'], 0)
  env.assertGreater(res['search_largest_memory_index_human'], 0)
  env.assertGreater(res['search_smallest_memory_index'], 0)
  env.assertGreater(res['search_smallest_memory_index_human'], 0)
  env.assertEqual(res['search_used_memory_vector_index'], 0)
  # env.assertGreater(res['search_total_indexing_time'], 0)   # Introduces flakiness

  # ========== Cursors statistics ==========
  env.assertEqual(res['search_global_idle'], 0)
  env.assertEqual(res['search_global_total'], 0)

  # ========== GC statistics ==========
  env.assertEqual(res['search_bytes_collected'], 0)
  env.assertEqual(res['search_total_cycles'], 0)
  env.assertEqual(res['search_total_ms_run'], 0)
  env.assertEqual(res['search_total_docs_not_collected_by_gc'], 0)
  env.assertEqual(res['search_marked_deleted_vectors'], 0)

  # ========== Dialect statistics ==========
  env.assertEqual(res['search_dialect_1'], 0)
  env.assertEqual(res['search_dialect_2'], 0)
  env.assertEqual(res['search_dialect_3'], 0)
  env.assertEqual(res['search_dialect_4'], 0)

  # ========== Errors statistics ==========
  env.assertEqual(res['search_errors_indexing_failures'], 0)
  env.assertEqual(res['search_errors_for_index_with_max_failures'], 0)

  # Create a cursor
  res = env.cmd('FT.AGGREGATE', 'idx', '*', 'WITHCURSOR')

  # Dispatch a query
  env.cmd('FT.SEARCH', 'idx', '*')

  # Call `INFO` and check that the data is updated accordingly
  res = env.cmd('INFO', 'MODULES')
  # On cluster mode, we have shard cursor on each master shard for the
  # aggregation command, yet the `INFO` command is per-shard, so the master shard
  # we enquery has 2 cursors (coord & shard).
  env.assertEqual(res['search_global_idle'], 1 if not env.isCluster() else 2)
  env.assertEqual(res['search_global_total'], 1 if not env.isCluster() else 2)
  env.assertEqual(res['search_dialect_2'], 1)

  # Delete all docs
  for i in range(n_docs):
    conn.execute_command('DEL', f'h{i}')

  # Force-invoke the GC
  forceInvokeGC(env)

  # Call `INFO` and check that the data is updated accordingly
  res = env.cmd('INFO', 'MODULES')
  env.assertGreater(res['search_bytes_collected'], 0)
  env.assertGreater(res['search_total_cycles'], 0)
  env.assertGreater(res['search_total_ms_run'], 0)


def test_counting_queries(env: Env):
  # Create an index
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC').ok()
  # Add some data
  n_docs = 10
  with env.getClusterConnectionIfNeeded() as con:
    for i in range(n_docs):
      con.execute_command('HSET', i, 'n', i)

  # Initiate counters
  queries_counter = 0
  query_commands_counter = 0
  def check_counters():
    line_number = currentframe().f_back.f_lineno
    info = env.cmd('INFO', 'MODULES')
    env.assertEqual(info['search_total_queries_processed'], queries_counter, message=f'line {line_number}')
    env.assertEqual(info['search_total_query_commands'], query_commands_counter, message=f'line {line_number}')

  # Call `INFO` and check that the counters are 0
  check_counters()

  env.cmd('FT.SEARCH', 'idx', '*')
  queries_counter += 1
  query_commands_counter += 1

  # Both counters should be updated
  check_counters()

  env.cmd('FT.AGGREGATE', 'idx', '*')
  queries_counter += 1
  query_commands_counter += 1

  # Both counters should be updated
  check_counters()

  _, cursor = env.cmd('FT.AGGREGATE', 'idx', '*', 'WITHCURSOR', 'COUNT', (n_docs // 2) + 1)
  env.assertNotEqual(cursor, 0) # Cursor is not done
  queries_counter += 1
  query_commands_counter += 1

  # Both counters should be updated
  check_counters()

  _, cursor = env.cmd('FT.CURSOR', 'READ', 'idx', cursor)
  env.assertEqual(cursor, 0) # Cursor is done
  query_commands_counter += 1 # Another query command, but not a unique query

  # Only the query commands counter should be updated
  check_counters()

  # Call commands that do not count as queries

  # Search with a non-existing index
  env.expect('FT.SEARCH', 'idx2', '*').error()
  check_counters()

  # Search with a syntax error
  env.expect('FT.SEARCH', 'idx', '(*').error()
  check_counters()

  # Aggregate with a non-existing index
  env.expect('FT.AGGREGATE', 'idx2', '*').error()
  check_counters()

  # Aggregate with a syntax error
  env.expect('FT.AGGREGATE', 'idx', '(*').error()
  check_counters()

  # Cursor read with a non-existing cursor
  env.expect('FT.CURSOR', 'READ', 'idx', '123').error()
  check_counters()

  if env.isCluster() and env.shardsCount > 1:
    # Verify that the counters are updated correctly on a cluster
    # We expect all the counters to sum up to the total number of queries

    for i in range(1, env.shardsCount + 1):
      env.getConnection(i).execute_command('FT.SEARCH', 'idx', '*')

    queries_counter += env.shardsCount
    query_commands_counter += env.shardsCount

    actual_queries_counter = 0
    actual_query_commands_counter = 0
    for i in range(1, env.shardsCount + 1):
      info = env.getConnection(i).execute_command('INFO', 'MODULES')
      actual_queries_counter += info['search_total_queries_processed']
      actual_query_commands_counter += info['search_total_query_commands']

    env.assertEqual(actual_queries_counter, queries_counter)
    env.assertEqual(actual_query_commands_counter, query_commands_counter)

  # Validate we count the execution time of the query (with any command)
  timeout = 300 # 5 minutes
  total_query_execution_time = lambda: env.cmd('INFO', 'MODULES')['search_total_query_execution_time_ms']
  with TimeLimit(timeout, 'FT.SEARCH'):
    cur_time_count = total_query_execution_time()
    while total_query_execution_time() == cur_time_count:
      env.cmd('FT.SEARCH', 'idx', '*')

  with TimeLimit(timeout, 'FT.AGGREGATE'):
    cur_time_count = total_query_execution_time()
    while total_query_execution_time() == cur_time_count:
      env.cmd('FT.AGGREGATE', 'idx', '*')

  with TimeLimit(timeout, 'FT.CURSOR READ'):
    cursor = 0
    cur_time_count = total_query_execution_time()
    while total_query_execution_time() == cur_time_count:
      if cursor == 0:
        _, cursor = env.cmd('FT.AGGREGATE', 'idx', '*', 'WITHCURSOR', 'COUNT', 1)
        cur_time_count = total_query_execution_time()
      _, cursor = env.cmd('FT.CURSOR', 'READ', 'idx', cursor)


@skip(noWorkers=True)
def test_counting_queries_BG():
  env = Env(moduleArgs='WORKER_THREADS 2 MT_MODE MT_MODE_FULL')
  test_counting_queries(env)


@skip(cluster=True, noWorkers=True)
def test_redis_info_modules_vecsim():
  env = Env(moduleArgs='WORKER_THREADS 2 MT_MODE MT_MODE_FULL')
  env.expect(config_cmd(), 'SET', 'FORK_GC_CLEAN_THRESHOLD', '0').ok()
  set_doc = lambda: env.expect('HSET', '1', 'vec', '????????')
  field_infos = lambda: [to_dict(env.cmd(debug_cmd(), 'VECSIM_INFO', f'idx{i}', 'vec')) for i in range(1, 4)]
  env.expect('FT.CREATE', 'idx1', 'SCHEMA', 'vec', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()
  env.expect('FT.CREATE', 'idx2', 'SCHEMA', 'vec', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()
  env.expect('FT.CREATE', 'idx3', 'SCHEMA', 'vec', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()

  set_doc().equal(1) # Add a document for the first time
  # Busy wait until the new vector is indexed to HNSW in both indexes.
  while True:
    infos = field_infos()
    # Once the backend index is no longer empty for ALL fields, we can continue to the test (async indexing is done).
    # print([to_dict(f_info['BACKEND_INDEX'])['INDEX_SIZE'] == 0 for f_info in infos[:2]].any())
    # return
    if all([to_dict(f_info['BACKEND_INDEX'])['INDEX_SIZE'] > 0 for f_info in infos[:2]]):
      break


  info = env.cmd('INFO', 'MODULES')
  field_infos = field_infos()
  env.assertEqual(info['search_used_memory_vector_index'], sum(field_info['MEMORY'] for field_info in field_infos))
  env.assertEqual(info['search_marked_deleted_vectors'], 0)

  [env.expect('FT.DEBUG', 'GC_STOP_SCHEDULE', f'idx{i}').ok() for i in range(1, 4)]   # Stop the gc
  set_doc().equal(0) # Add (override) the document for the second time

  info = env.cmd('INFO', 'MODULES')
  field_infos = [to_dict(env.cmd(debug_cmd(), 'VECSIM_INFO', f'idx{i}', 'vec')) for i in range(1, 4)]
  env.assertEqual(info['search_used_memory_vector_index'], sum(field_info['MEMORY'] for field_info in field_infos))
  env.assertEqual(info['search_marked_deleted_vectors'], 2) # 2 vectors were marked as deleted (1 for each index)
  env.assertEqual(to_dict(field_infos[0]['BACKEND_INDEX'])['NUMBER_OF_MARKED_DELETED'], 1)
  env.assertEqual(to_dict(field_infos[1]['BACKEND_INDEX'])['NUMBER_OF_MARKED_DELETED'], 1)

  [env.expect('FT.DEBUG', 'GC_CONTINUE_SCHEDULE', f'idx{i}').ok() for i in range(1, 4)]   # resume gc
  [forceInvokeGC(env, f'idx{i}') for i in range(1, 4)]

  info = env.cmd('INFO', 'MODULES')
  field_infos = [to_dict(env.cmd(debug_cmd(), 'VECSIM_INFO', f'idx{i}', 'vec')) for i in range(1, 4)]
  env.assertEqual(info['search_used_memory_vector_index'], sum(field_info['MEMORY'] for field_info in field_infos))
  env.assertEqual(info['search_marked_deleted_vectors'], 0)
  env.assertEqual(to_dict(field_infos[0]['BACKEND_INDEX'])['NUMBER_OF_MARKED_DELETED'], 0)
  env.assertEqual(to_dict(field_infos[1]['BACKEND_INDEX'])['NUMBER_OF_MARKED_DELETED'], 0)

@skip(cluster=True)
def test_indexes_logically_deleted_docs(env):
  # Set these values to manually control the GC, ensuring that the GC will not run automatically since the run intervall
  # is > 8h (5 mintues is the hard limit for a test).
  env.expect(config_cmd(), 'SET', 'FORK_GC_CLEAN_THRESHOLD', '0').ok()
  env.expect(config_cmd(), 'SET', 'FORK_GC_RUN_INTERVAL', '30000').ok()
  set_doc = lambda doc_id: env.expect('HSET', doc_id, 'text', 'some text', 'tag', 'tag1', 'num', 1)
  get_logically_deleted_docs = lambda: env.cmd('INFO', 'MODULES')['search_total_docs_not_collected_by_gc']

  # Init state
  env.assertEqual(get_logically_deleted_docs(), 0)

  # Create one index and one document, then delete the document (logically)
  num_fields = 3
  env.expect('FT.CREATE', 'idx1', 'SCHEMA', 'text', 'TEXT', 'tag', 'TAG', 'num', 'NUMERIC').ok()
  env.expect(debug_cmd(), 'GC_STOP_SCHEDULE', 'idx1').ok()  # Stop GC for this index to keep the deleted docs
  set_doc(f'doc:1').equal(num_fields)
  env.assertEqual(get_logically_deleted_docs(), 0)
  env.expect('DEL', 'doc:1').equal(1)
  env.assertEqual(get_logically_deleted_docs(), 1)

  # Create another index, expect that the deleted document will not be indexed.
  env.expect('FT.CREATE', 'idx2', 'SCHEMA', 'text', 'TEXT', 'tag', 'TAG', 'num', 'NUMERIC').ok()
  env.assertEqual(get_logically_deleted_docs(), 1)

  # Add another document that should be indexed into both indexes, then deleted it (logically) and expect
  # it will be accounted twice.
  set_doc(f'doc:2').equal(num_fields)
  env.cmd('DEL', 'doc:2')
  env.assertEqual(get_logically_deleted_docs(), 3)

  # Drop first index, expect that the deleted documents in this index will not be accounted anymore when releasing the GC.
  # We run in a transaction, to ensure that the GC will not run until the "dropindex" commmand is executed from
  # the main thread (otherwise, we would have released the main thread between the commands and the GC could run before
  # the dropindex command. Though it won't impact correctness, we fail to test the desired scenario)
  env.expect('MULTI').ok()
  env.cmd(debug_cmd(), 'test_cpp_ra', 'idx1')
  env.cmd('FT.DROPINDEX', 'idx1')
  env.expect('EXEC').equal(['OK', 'OK'])
  env.expect(debug_cmd(), 'GC_WAIT_FOR_JOBS').equal('DONE')  # Wait for the gc to finish
  env.assertEqual(get_logically_deleted_docs(), 1)

  # Run GC, expect that the deleted document will not be accounted anymore.
  forceInvokeGC(env, idx='idx2')
  env.assertEqual(get_logically_deleted_docs(), 0)
