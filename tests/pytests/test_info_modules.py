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
  get_field_infos = lambda: [to_dict(env.cmd(debug_cmd(), 'VECSIM_INFO', f'idx{i}', 'vec')) for i in range(1, 4)]
  # Busy wait until the new vector has moved completely to HNSW in both indexes.
  def wait_for_vector_to_move_to_hnsw():
    while True:
      field_infos = get_field_infos()
      # Once the frontend index is empty for ALL fields, we can continue to the test (async indexing is done).
      if all([to_dict(f_info['FRONTEND_INDEX'])['INDEX_SIZE'] == 0 for f_info in field_infos[:2]]):
        break

  env.expect('FT.CREATE', 'idx1', 'SCHEMA', 'vec', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()
  env.expect('FT.CREATE', 'idx2', 'SCHEMA', 'vec', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()
  env.expect('FT.CREATE', 'idx3', 'SCHEMA', 'vec', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()

  set_doc().equal(1) # Add a document for the first time
  wait_for_vector_to_move_to_hnsw()

  info = env.cmd('INFO', 'MODULES')
  field_infos = get_field_infos()
  env.assertEqual(info['search_used_memory_vector_index'], sum(field_info['MEMORY'] for field_info in field_infos))
  # Validate that vector indexes are accounted in the total index memory
  env.assertGreater(info['search_used_memory_indexes'], info['search_used_memory_vector_index'])
  env.assertEqual(info['search_marked_deleted_vectors'], 0)

  [env.expect('FT.DEBUG', 'GC_STOP_SCHEDULE', f'idx{i}').ok() for i in range(1, 4)]   # Stop the gc
  set_doc().equal(0) # Add (override) the document with a new one
  wait_for_vector_to_move_to_hnsw()

  info = env.cmd('INFO', 'MODULES')
  field_infos = get_field_infos()
  env.assertEqual(info['search_used_memory_vector_index'], sum(field_info['MEMORY'] for field_info in field_infos))
  env.assertEqual(info['search_marked_deleted_vectors'], 2) # 2 vectors were marked as deleted (1 for each index)
  env.assertEqual(to_dict(field_infos[0]['BACKEND_INDEX'])['NUMBER_OF_MARKED_DELETED'], 1)
  env.assertEqual(to_dict(field_infos[1]['BACKEND_INDEX'])['NUMBER_OF_MARKED_DELETED'], 1)

  [env.expect('FT.DEBUG', 'GC_CONTINUE_SCHEDULE', f'idx{i}').ok() for i in range(1, 4)]   # resume gc
  [forceInvokeGC(env, f'idx{i}') for i in range(1, 4)]

  info = env.cmd('INFO', 'MODULES')
  field_infos = get_field_infos()
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
  env.cmd(debug_cmd(), 'GC_CONTINUE_SCHEDULE', 'idx1')
  env.cmd('FT.DROPINDEX', 'idx1')
  env.expect('EXEC').equal(['OK', 'OK'])
  env.expect(debug_cmd(), 'GC_WAIT_FOR_JOBS').equal('DONE')  # Wait for the gc to finish
  env.assertEqual(get_logically_deleted_docs(), 1)

  # Run GC, expect that the deleted document will not be accounted anymore.
  forceInvokeGC(env, idx='idx2')
  env.assertEqual(get_logically_deleted_docs(), 0)

@skip(cluster=True)
def test_indexing_metrics(env: Env):
  env.cmd('HSET', 'doc:1', 'text', 'hello world')
  n_indexes = 3

  # Create indexes in a transaction, and at the end of the transaction
  # call `info` and verify we observe that all the indexes are currently indexing
  with env.getConnection().pipeline(transaction=True) as pipe:
    for i in range(n_indexes):
      pipe.execute_command('FT.CREATE', f'idx{i}', 'SCHEMA', 'text', 'TEXT')
    pipe.execute_command('INFO', 'MODULES')
    res = pipe.execute()

  # Verify that all the indexes are currently indexing
  env.assertEqual(res[:n_indexes], ['OK'] * n_indexes)

  # Verify that the INFO command returns the correct indexing status
  env.assertEqual(res[-1]['search_number_of_indexes'], n_indexes)
  env.assertEqual(res[-1]['search_number_of_active_indexes'], n_indexes)
  env.assertEqual(res[-1]['search_number_of_active_indexes_indexing'], n_indexes)
  env.assertEqual(res[-1]['search_total_active_write_threads'], 1) # 1 write operation by the BG indexer thread

SEARCH_PREFIX = 'search_'

########
# Multi Threaded Stats tests
########

MULTI_THREADING_SECTION = f'{SEARCH_PREFIX}multi_threading'
ACTIVE_IO_THREADS_METRIC = f'{SEARCH_PREFIX}active_io_threads'
ACTIVE_WORKER_THREADS_METRIC = f'{SEARCH_PREFIX}active_worker_threads'
ACTIVE_COORD_THREADS_METRIC = f'{SEARCH_PREFIX}active_coord_threads'
WORKERS_LOW_PRIORITY_PENDING_JOBS_METRIC = f'{SEARCH_PREFIX}workers_low_priority_pending_jobs'
WORKERS_HIGH_PRIORITY_PENDING_JOBS_METRIC = f'{SEARCH_PREFIX}workers_high_priority_pending_jobs'

def test_initial_multi_threading_stats(env):
  conn = getConnectionByEnv(env)
  # Setup: Create index with some data
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT', 'age', 'NUMERIC').ok()
  for i in range(10):
    conn.execute_command('HSET', f'doc{i}', 'name', f'name{i}', 'age', i)

  # Phase 1: Verify multi_threading section exists and active_io_threads starts at 0
  info_dict = info_modules_to_dict(env)

  # Verify multi_threading section exists
  env.assertTrue(MULTI_THREADING_SECTION in info_dict,
                 message="multi_threading section should exist in INFO MODULES")

  # Verify all expected fields exist
  env.assertTrue(ACTIVE_IO_THREADS_METRIC in info_dict[MULTI_THREADING_SECTION],
                 message=f"{ACTIVE_IO_THREADS_METRIC} field should exist in multi_threading section")
  env.assertTrue(ACTIVE_WORKER_THREADS_METRIC in info_dict[MULTI_THREADING_SECTION],
                 message=f"{ACTIVE_WORKER_THREADS_METRIC} field should exist in multi_threading section")

  # Verify all fields initialized to 0.
  env.assertEqual(info_dict[MULTI_THREADING_SECTION][ACTIVE_IO_THREADS_METRIC], '0',
                 message=f"{ACTIVE_IO_THREADS_METRIC} should be 0 when idle")
  env.assertEqual(info_dict[MULTI_THREADING_SECTION][ACTIVE_WORKER_THREADS_METRIC], '0',
                 message=f"{ACTIVE_WORKER_THREADS_METRIC} should be 0 when idle")
  # There's no deterministic way to test active_io_threads increases while a query is running,
  # we test it in unit tests.

# NOTE: Currently query debug pause mechanism only supports pausing one query at a time, and is not supported on cluster.
@skip(cluster=True, noWorkers=True)
def test_active_worker_threads():
    env = Env(moduleArgs='WORKER_THREADS 2 MT_MODE MT_MODE_FULL')
    num_queries = 1
    conn = getConnectionByEnv(env)

    # Create index and add test data
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC').ok()
    for i in range(10):
        conn.execute_command('HSET', f'doc{i}', 'n', i)

    # Verify active_worker_threads starts at 0
    info_dict = info_modules_to_dict(conn)
    env.assertEqual(info_dict[MULTI_THREADING_SECTION][ACTIVE_WORKER_THREADS_METRIC], '0',
                    message=f"{ACTIVE_WORKER_THREADS_METRIC} should be 0 when idle")

    # Define callback for testing a specific query type
    def _test_query_type(query_type):
        query_threads = []
        query_results = []

        # Launch num_queries queries in background threads, paused at Index RP
        for i in range(num_queries):
            result_list = []
            query_results.append(result_list)
            t = threading.Thread(
                target=call_and_store,
                args=(runDebugQueryCommandPauseBeforeRPAfterN,
                      (env, [query_type, 'idx', '*'], 'Index', 0),
                      result_list),
                daemon=True
            )
            query_threads.append(t)
            t.start()

        # Wait for all queries to be paused
        with TimeLimit(120):
            while not getIsRPPaused(env):
                time.sleep(0.1)

        # Verify active_worker_threads == num_queries
        info_dict = info_modules_to_dict(conn)
        env.assertEqual(info_dict[MULTI_THREADING_SECTION][ACTIVE_WORKER_THREADS_METRIC], str(num_queries),
                        message=f"{query_type}: {ACTIVE_WORKER_THREADS_METRIC} should be {num_queries} when {num_queries} queries are paused")

        # Resume all queries
        setPauseRPResume(env)

        # Wait for all query threads to complete
        for t in query_threads:
            t.join()

        # Drain worker thread pool to ensure all jobs complete
        env.expect(debug_cmd(), 'WORKER_THREADS', 'DRAIN').ok()

        # Verify active_worker_threads returns to 0
        info_dict = info_modules_to_dict(conn)
        env.assertEqual(info_dict[MULTI_THREADING_SECTION][ACTIVE_WORKER_THREADS_METRIC], '0',
                        message=f"{query_type}: active_worker_threads should return to 0 after queries complete")

    # Test both query types
    _test_query_type('FT.SEARCH')
    _test_query_type('FT.AGGREGATE')

def _test_pending_jobs_metrics(env, command_type):
    """
    Parameters:
        - env: Test environment (works for both SA and cluster)
    """

    # --- STEP 1: SETUP ---
    # assert we have enough workers configured on all shards
    for i, conn in enumerate(env.getOSSMasterNodesConnectionList()):
        shard_workers = int(conn.execute_command(config_cmd(), 'GET', 'WORKER_THREADS')[0][1])
        env.assertGreaterEqual(shard_workers, 1, message=f"shard {i} has {shard_workers} worker threads, expected at least 1")

    # Define variables
    num_vectors = 10 * env.shardsCount  # Number of vectors to index (creates low priority jobs)
    num_queries = 3   # Number of queries to execute (creates high priority jobs)
    dim = 4
    vector_field = DEFAULT_VECTOR_FIELD_NAME
    index_name = 'idx'

    # --- STEP 2: VERIFY INITIAL STATE (metrics = 0) ---
    for conn in env.getOSSMasterNodesConnectionList():
        info_dict = info_modules_to_dict(conn)
        env.assertEqual(info_dict[MULTI_THREADING_SECTION][WORKERS_LOW_PRIORITY_PENDING_JOBS_METRIC], '0')
        env.assertEqual(info_dict[MULTI_THREADING_SECTION][WORKERS_HIGH_PRIORITY_PENDING_JOBS_METRIC], '0')

    #  --- STEP 3: PAUSE WORKERS THREAD POOL ---
    # Pause workers to prevent jobs from executing
    run_command_on_all_shards(env, debug_cmd(), 'WORKER_THREADS', 'PAUSE')

    # --- STEP 4: CREATE INDEX AND INDEX VECTORS (creates workers_low_priority_pending_jobs) ---
    # Create index with HNSW and load vectors (HNSW creates background indexing jobs which are low priority)
    env.expect('FT.CREATE', index_name, 'SCHEMA', vector_field, 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', dim, 'DISTANCE_METRIC', 'L2').ok()
    load_vectors_to_redis(env, num_vectors, 0, dim, vec_field_name=vector_field)

    def check_indexing_jobs_pending():
        num_shards = env.shardsCount
        all_shards_ready = [False] * num_shards
        state = {
          'indexing_jobs_pending': [0] * num_shards,
          'expected_indexing_jobs': [0] * num_shards,
          'workers_stats': [{}] * num_shards,
        }

        for i, con in enumerate(env.getOSSMasterNodesConnectionList()):
          # Expected low_priority_pending_jobs = con.dbsize() (number of vectors on this shard)
          expected_indexing_jobs = con.execute_command('DBSIZE')

          shard_stats = info_modules_to_dict(con)
          indexing_jobs_pending = int(shard_stats[MULTI_THREADING_SECTION][WORKERS_LOW_PRIORITY_PENDING_JOBS_METRIC])

          all_shards_ready[i] = (expected_indexing_jobs == indexing_jobs_pending)
          state['expected_indexing_jobs'][i] = expected_indexing_jobs
          state['indexing_jobs_pending'][i] = indexing_jobs_pending
          state['workers_stats'][i] = {f'shard {i}': to_dict(con.execute_command(debug_cmd(), 'WORKER_THREADS', 'stats'))}
        return all(all_shards_ready), state

    wait_for_condition(check_indexing_jobs_pending, "wait_for_workers_low_priority_jobs_pending")

    # --- STEP 5: EXECUTE QUERIES (creates high_priority_pending_jobs) ---
    # Launch num_queries queries in background threads
    # Queries will be queued as high-priority jobs but not executed (workers paused)

    query_threads = launch_cmds_in_bg_with_exception_check(env, [f'FT.{command_type}', index_name, '*'], num_queries)
    if query_threads is None:
        run_command_on_all_shards(env, debug_cmd(), 'WORKER_THREADS', 'RESUME')
        return

    # --- STEP 6: WAIT FOR THREADPOOL STATS TO UPDATE (jobs queued) ---
    # Wait for the threadpool stats to reflect the expected pending jobs
    def check_queries_jobs_pending():
        num_shards = env.shardsCount
        all_shards_ready = [False] * num_shards
        expected_queries_jobs = num_queries
        state = {
          'queries_jobs_pending': [0] * num_shards,
          'expected_queries_jobs': [expected_queries_jobs] * num_shards,
          'workers_stats': [{}] * num_shards,
        }

        for i, con in enumerate(env.getOSSMasterNodesConnectionList()):

          shard_stats = info_modules_to_dict(con)
          queries_pending_jobs = int(shard_stats[MULTI_THREADING_SECTION][WORKERS_HIGH_PRIORITY_PENDING_JOBS_METRIC])

          all_shards_ready[i] = (expected_queries_jobs == queries_pending_jobs)
          state['queries_jobs_pending'][i] = queries_pending_jobs
          state['expected_queries_jobs'][i] = expected_queries_jobs
          state['workers_stats'][i] = {f'shard {i}': to_dict(con.execute_command(debug_cmd(), 'WORKER_THREADS', 'stats'))}
        return all(all_shards_ready), state

    wait_for_condition(check_queries_jobs_pending, "wait_for_high_priority_jobs_pending")

    # --- STEP 7: RESUME WORKERS AND DRAIN ---
    # Resume workers:
    run_command_on_all_shards(env, debug_cmd(), 'WORKER_THREADS', 'RESUME')

    # Wait for all query threads to complete:
    for t in query_threads:
        t.join(timeout=30)

    # Drain worker thread pool to ensure all jobs complete:
    run_command_on_all_shards(env, debug_cmd(), 'WORKER_THREADS', 'DRAIN')

    # --- STEP 8: VERIFY METRICS RETURN TO 0 ---
    # Wait for metrics to return to 0 (job callback finished before stats update)
    def check_reset_metrics():
        num_shards = env.shardsCount
        all_shards_ready = [False] * num_shards
        state = {
          'workers_low_priority_jobs_pending': [-1] * num_shards,
          'workers_high_priority_jobs_pending': [-1] * num_shards,
          'workers_stats': [{}] * num_shards,
        }

        for i, con in enumerate(env.getOSSMasterNodesConnectionList()):

          shard_stats = info_modules_to_dict(con)
          queries_jobs_pending = int(shard_stats[MULTI_THREADING_SECTION][WORKERS_HIGH_PRIORITY_PENDING_JOBS_METRIC])
          background_indexing_jobs_pending = int(shard_stats[MULTI_THREADING_SECTION][WORKERS_LOW_PRIORITY_PENDING_JOBS_METRIC])

          all_shards_ready[i] = (queries_jobs_pending == 0 and background_indexing_jobs_pending == 0)
          state['workers_low_priority_jobs_pending'][i] = background_indexing_jobs_pending
          state['workers_high_priority_jobs_pending'][i] = queries_jobs_pending
          state['workers_stats'][i] = {f'shard {i}': to_dict(con.execute_command(debug_cmd(), 'WORKER_THREADS', 'stats'))}
        return all(all_shards_ready), state

    wait_for_condition(check_reset_metrics, "wait_for_workers_pending_jobs_metric_reset")

@skip(noWorkers=True)
def test_pending_jobs_metrics_search():
  env = Env(moduleArgs='DEFAULT_DIALECT 2 WORKER_THREADS 2 MT_MODE MT_MODE_FULL')
  _test_pending_jobs_metrics(env, 'SEARCH')

@skip(noWorkers=True)
def test_pending_jobs_metrics_aggregate():
  env = Env(moduleArgs='DEFAULT_DIALECT 2 WORKER_THREADS 2 MT_MODE MT_MODE_FULL')
  _test_pending_jobs_metrics(env, 'AGGREGATE')
