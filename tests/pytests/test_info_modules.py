from common import *
from RLTest import Env
import redis
from inspect import currentframe
import numpy as np


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
                                          'INITIAL_CAP', '10000', 'M', '40', 'EF_CONSTRUCTION', '250', 'EF_RUNTIME', '20',
                                          'vec_svs_vamana', 'VECTOR', 'SVS-VAMANA', '6', 'TYPE', 'FLOAT32', 'DIM', '128', 'DISTANCE_METRIC', 'L2',
                                          'vec_svs_vamana_COMPRESSED', 'VECTOR', 'SVS-VAMANA', '8', 'TYPE', 'FLOAT32', 'DIM', '128', 'DISTANCE_METRIC', 'L2',
                                          'COMPRESSION', 'LVQ4').ok()

  info = info_modules_to_dict(conn)
  env.assertEqual(info['search_indexes']['search_number_of_indexes'], '3')
  fieldsInfo = info['search_fields_statistics']
  env.assertEqual(field_info_to_dict(fieldsInfo['search_fields_text']), get_search_field_info('Text', 2, Sortable=1, NoIndex=1))
  env.assertEqual(field_info_to_dict(fieldsInfo['search_fields_tag']), get_search_field_info('Tag', 2, Sortable=1, CaseSensitive=1))
  env.assertEqual(field_info_to_dict(fieldsInfo['search_fields_numeric']), get_search_field_info('Numeric', 2, NoIndex=1))
  env.assertEqual(field_info_to_dict(fieldsInfo['search_fields_geo']), get_search_field_info('Geo', 1))
  env.assertEqual(field_info_to_dict(fieldsInfo['search_fields_vector']), get_search_field_info('Vector', 4, Flat=1, HNSW=1, SVS_VAMANA=2, SVS_VAMANA_Compressed=1))
  env.assertEqual(field_info_to_dict(fieldsInfo['search_fields_geoshape']), get_search_field_info('Geoshape', 2, Sortable=1 ,NoIndex=1))

  configInfo = info['search_runtime_configurations']
  env.assertEqual(configInfo['search_minimal_term_prefix'], '2')
  env.assertEqual(configInfo['search_gc_scan_size'], '100')
  env.assertEqual(configInfo['search_bm25std_tanh_factor'], '4')

  garbage_collector_info = info['search_garbage_collector']
  env.assertEqual(garbage_collector_info['search_gc_bytes_collected'], '0')
  env.assertEqual(garbage_collector_info['search_gc_total_cycles'], '0')
  env.assertEqual(garbage_collector_info['search_gc_total_ms_run'], '0')
  env.assertEqual(garbage_collector_info['search_gc_total_docs_not_collected'], '0')
  env.assertEqual(garbage_collector_info['search_gc_marked_deleted_vectors'], '0')

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
  env.assertEqual(info['search_indexes']['search_number_of_indexes'], '1')

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
  env.assertEqual(info['search_indexes']['search_number_of_indexes'], '1')

  fieldsInfo = info['search_fields_statistics']
  env.assertEqual(field_info_to_dict(fieldsInfo['search_fields_text']), get_search_field_info('Text', 2, Sortable=1))
  env.assertFalse('search_fields_numeric' in fieldsInfo) # no numeric fields since we removed idx2


def testInfoModulesAfterReload(env):
  conn = env.getConnection()
  idx1 = 'idx1'

  env.expect('FT.CREATE', idx1, 'SCHEMA', 'age', 'NUMERIC', 'SORTABLE',
                                          'geo', 'GEO', 'SORTABLE', 'NOINDEX',
                                          'body', 'TAG', 'NOINDEX').ok()

  for _ in env.reloadingIterator():
    info = info_modules_to_dict(conn)
    env.assertEqual(info['search_indexes']['search_number_of_indexes'], '1')

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

@skip(cluster=True, no_json=True)
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
  env.assertEqual(res['search_global_idle_user'], 0)
  env.assertEqual(res['search_global_idle_internal'], 0)
  env.assertEqual(res['search_global_total_user'], 0)
  env.assertEqual(res['search_global_total_internal'], 0)

  # ========== GC statistics ==========
  env.assertEqual(res['search_gc_bytes_collected'], 0)
  env.assertEqual(res['search_gc_total_cycles'], 0)
  env.assertEqual(res['search_gc_total_ms_run'], 0)
  env.assertEqual(res['search_gc_total_docs_not_collected'], 0)
  env.assertEqual(res['search_gc_marked_deleted_vectors'], 0)

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
  env.assertEqual(res['search_global_idle_user'], 1)
  env.assertEqual(res['search_global_total_user'], 1)
  env.assertEqual(res['search_global_idle_internal'], 1 if env.isCluster() else 0)
  env.assertEqual(res['search_global_total_internal'], 1 if env.isCluster() else 0)

  env.assertEqual(res['search_dialect_2'], 1)

  # Delete all docs
  for i in range(n_docs):
    conn.execute_command('DEL', f'h{i}')

  # Force-invoke the GC
  forceInvokeGC(env)

  # Call `INFO` and check that the data is updated accordingly
  res = env.cmd('INFO', 'MODULES')
  env.assertGreater(res['search_gc_bytes_collected'], 0)
  env.assertGreater(res['search_gc_total_cycles'], 0)
  env.assertGreater(res['search_gc_total_ms_run'], 0)


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


def test_counting_queries_BG():
  env = Env(moduleArgs='WORKERS 2')
  test_counting_queries(env)


@skip(cluster=True)
def test_redis_info_modules_vecsim():
  env = Env(moduleArgs='WORKERS 2')
  env.expect(config_cmd(), 'SET', 'FORK_GC_CLEAN_THRESHOLD', '0').ok()
  set_doc = lambda: env.expect('HSET', '1', 'vec', '????')

  env.expect('FT.CREATE', 'idx1', 'SCHEMA', 'vec', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT16', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()
  env.expect('FT.CREATE', 'idx2', 'SCHEMA', 'vec', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT16', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()
  env.expect('FT.CREATE', 'idx3', 'SCHEMA', 'vec', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT16', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()
  env.expect('FT.CREATE', 'idx4', 'SCHEMA', 'vec', 'VECTOR', 'SVS-VAMANA', '6', 'TYPE', 'FLOAT16', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()

  set_doc().equal(1) # Add a document for the first time
  env.expect(debug_cmd(), 'WORKERS', 'DRAIN').ok()

  info = env.cmd('INFO', 'MODULES')
  field_infos = [to_dict(env.cmd(debug_cmd(), 'VECSIM_INFO', f'idx{i}', 'vec')) for i in range(1, 5)]
  env.assertEqual(info['search_used_memory_vector_index'], sum(field_info['MEMORY'] for field_info in field_infos))
  # Validate that vector indexes are accounted in the total index memory
  env.assertGreater(info['search_used_memory_indexes'], info['search_used_memory_vector_index'])
  env.assertEqual(info['search_gc_marked_deleted_vectors'], 0)

  env.expect(debug_cmd(), 'WORKERS', 'PAUSE').ok()
  set_doc().equal(0) # Add (override) the document for the second time

  info = env.cmd('INFO', 'MODULES')
  field_infos = [to_dict(env.cmd(debug_cmd(), 'VECSIM_INFO', f'idx{i}', 'vec')) for i in range(1, 5)]
  env.assertEqual(info['search_used_memory_vector_index'], sum(field_info['MEMORY'] for field_info in field_infos))

  # Todo: account for deleted vector in SVS-VAMANA as well
  env.assertEqual(info['search_gc_marked_deleted_vectors'], 2) # 2 vectors were marked as deleted (1 for each hnsw index)
  env.assertEqual(to_dict(field_infos[0]['BACKEND_INDEX'])['NUMBER_OF_MARKED_DELETED'], 1)
  env.assertEqual(to_dict(field_infos[1]['BACKEND_INDEX'])['NUMBER_OF_MARKED_DELETED'], 1)

  env.expect(debug_cmd(), 'WORKERS', 'RESUME').ok()
  [forceInvokeGC(env, f'idx{i}') for i in range(1, 5)]

  info = env.cmd('INFO', 'MODULES')
  field_infos = [to_dict(env.cmd(debug_cmd(), 'VECSIM_INFO', f'idx{i}', 'vec')) for i in range(1, 5)]
  env.assertEqual(info['search_used_memory_vector_index'], sum(field_info['MEMORY'] for field_info in field_infos))
  env.assertEqual(info['search_gc_marked_deleted_vectors'], 0)
  env.assertEqual(to_dict(field_infos[0]['BACKEND_INDEX'])['NUMBER_OF_MARKED_DELETED'], 0)
  env.assertEqual(to_dict(field_infos[1]['BACKEND_INDEX'])['NUMBER_OF_MARKED_DELETED'], 0)

@skip(cluster=True)
def test_indexes_logically_deleted_docs(env):
  # Set these values to manually control the GC, ensuring that the GC will not run automatically since the run interval
  # is > 8h (5 minutes is the hard limit for a test).
  env.expect(config_cmd(), 'SET', 'FORK_GC_CLEAN_THRESHOLD', '0').ok()
  env.expect(config_cmd(), 'SET', 'FORK_GC_RUN_INTERVAL', '30000').ok()
  set_doc = lambda doc_id: env.expect('HSET', doc_id, 'text', 'some text', 'tag', 'tag1', 'num', 1)
  get_logically_deleted_docs = lambda: env.cmd('INFO', 'MODULES')['search_gc_total_docs_not_collected']

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
  # We run in a transaction, to ensure that the GC will not run until the "dropindex" command is executed from
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

SYNTAX_ERROR = "Parsing/Syntax error for query string"
ARGS_ERROR = "Error parsing query/aggregation arguments"

SEARCH_PREFIX = 'search_'
WARN_ERR_SECTION = f'{SEARCH_PREFIX}warnings_and_errors'

SEARCH_SHARD_PREFIX = 'search_shard_'
SYNTAX_ERROR_SHARD_METRIC = f"{SEARCH_SHARD_PREFIX}total_query_errors_syntax"
ARGS_ERROR_SHARD_METRIC = f"{SEARCH_SHARD_PREFIX}total_query_errors_arguments"

COORD_WARN_ERR_SECTION = WARN_ERR_SECTION.replace(SEARCH_PREFIX, 'search_coordinator_')

SEARCH_COORD_PREFIX = 'search_coord_'
SYNTAX_ERROR_COORD_METRIC = f"{SEARCH_COORD_PREFIX}total_query_errors_syntax"
ARGS_ERROR_COORD_METRIC = f"{SEARCH_COORD_PREFIX}total_query_errors_arguments"

# Expect env and conn so we can assert
def _verify_metrics_not_changed(env, conn, prev_info_dict: dict, ignored_metrics : list):
  info_dict = info_modules_to_dict(conn)
  for section in [WARN_ERR_SECTION, COORD_WARN_ERR_SECTION]:
    for metric in info_dict[section]:
      if metric in ignored_metrics:
        continue
      env.assertEqual(info_dict[section][metric], prev_info_dict[section][metric], message = f"Metric {metric} changed")

def _common_warnings_errors_test_scenario(env):
  """Common setup for warnings and errors tests"""
  # Create index
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'text', 'TEXT').ok()
  # Create doc
  env.expect('HSET', 'doc:1', 'text', 'hello world').equal(1)
  # Create vector index for hybrid
  env.expect('FT.CREATE', 'idx_vec', 'PREFIX', '1', 'vec:', 'SCHEMA', 'vector', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()
  # Create doc for hybrid
  env.expect('HSET', 'vec:1', 'vector', np.array([0.0, 0.0]).astype(np.float32).tobytes()).equal(1)

class testWarningsAndErrorsStandalone:
  """Test class for warnings and errors metrics in standalone mode"""

  def __init__(self):
    skipTest(cluster=True)
    self.env = Env()
    _common_warnings_errors_test_scenario(self.env)
    self.prev_info_dict = info_modules_to_dict(self.env)

  def setUp(self):
    self.prev_info_dict = info_modules_to_dict(self.env)

  def test_syntax_errors_SA(self):
    # Standalone shards are considered as shards in the info metrics

    # Test syntax errors
    self.env.expect('FT.SEARCH', 'idx', 'hello world:').error().contains('Syntax error at offset')
    # Test counter
    info_dict = info_modules_to_dict(self.env)
    syntax_error_count = info_dict[WARN_ERR_SECTION][SYNTAX_ERROR_SHARD_METRIC]
    self.env.assertEqual(syntax_error_count, '1')
    # Test syntax errors in aggregate
    self.env.expect('FT.AGGREGATE', 'idx', 'hello world:').error().contains('Syntax error at offset')
    # Test counter
    info_dict = info_modules_to_dict(self.env)
    syntax_error_count = info_dict[WARN_ERR_SECTION][SYNTAX_ERROR_SHARD_METRIC]
    self.env.assertEqual(syntax_error_count, '2')
    # Test syntax errors in hybrid
    self.env.expect('FT.HYBRID', 'idx_vec', 'SEARCH', 'hello world:', 'VSIM', '@vector', '0').error().contains('Syntax error at offset')
    # Test counter
    info_dict = info_modules_to_dict(self.env)
    syntax_error_count = info_dict[WARN_ERR_SECTION][SYNTAX_ERROR_SHARD_METRIC]
    self.env.assertEqual(syntax_error_count, '3')

    # Test other metrics not changed
    tested_in_this_test = [SYNTAX_ERROR_SHARD_METRIC]
    _verify_metrics_not_changed(self.env, self.env, self.prev_info_dict, tested_in_this_test)

  def test_args_errors_SA(self):
    # Standalone shards are considered as shards in the info metrics

    # Test args errors
    self.env.expect('FT.SEARCH', 'idx', 'hello world', 'LIMIT', 0, 0, 'MEOW').error().contains('Unknown argument')
    # Test counter
    info_dict = info_modules_to_dict(self.env)
    args_error_count = info_dict[WARN_ERR_SECTION][ARGS_ERROR_SHARD_METRIC]
    self.env.assertEqual(args_error_count, '1')
    # Test args errors in aggregate
    self.env.expect('FT.AGGREGATE', 'idx', 'hello world', 'LIMIT', 0, 0, 'MEOW').error().contains('Unknown argument')
    # Test counter
    info_dict = info_modules_to_dict(self.env)
    args_error_count = info_dict[WARN_ERR_SECTION][ARGS_ERROR_SHARD_METRIC]
    self.env.assertEqual(args_error_count, '2')
    # Test args errors in hybrid
    self.env.expect('FT.HYBRID', 'idx_vec', 'SEARCH', 'hello world', 'VSIM', '@vector', '0', 'LIMIT', 0, 0, 'MEOW').error().contains('Unknown argument')
    # Test counter
    info_dict = info_modules_to_dict(self.env)
    args_error_count = info_dict[WARN_ERR_SECTION][ARGS_ERROR_SHARD_METRIC]
    self.env.assertEqual(args_error_count, '3')

    # Test other metrics not changed
    tested_in_this_test = [ARGS_ERROR_SHARD_METRIC]
    _verify_metrics_not_changed(self.env, self.env, self.prev_info_dict, tested_in_this_test)

  def test_no_error_queries_SA(self):
    # Standalone shards are considered as coordinator in the info metrics

    # Check no error queries not affecting any metric
    before_info_dict = info_modules_to_dict(self.env)
    self.env.expect('FT.SEARCH', 'idx', 'hello world').noError()
    after_info_dict = info_modules_to_dict(self.env)

    self.env.assertEqual(before_info_dict[WARN_ERR_SECTION], after_info_dict[WARN_ERR_SECTION])
    self.env.assertEqual(before_info_dict[COORD_WARN_ERR_SECTION], after_info_dict[COORD_WARN_ERR_SECTION])

    # Test no error queries in aggregate
    self.env.expect('FT.AGGREGATE', 'idx', 'hello world').noError()
    after_info_dict = info_modules_to_dict(self.env)

    self.env.assertEqual(before_info_dict[WARN_ERR_SECTION], after_info_dict[WARN_ERR_SECTION])
    self.env.assertEqual(before_info_dict[COORD_WARN_ERR_SECTION], after_info_dict[COORD_WARN_ERR_SECTION])

    # Test no error queries in hybrid
    self.env.expect('FT.HYBRID', 'idx_vec', 'SEARCH', 'hello world', 'VSIM', '@vector', np.array([0.0, 0.0]).astype(np.float32).tobytes()).noError()
    after_info_dict = info_modules_to_dict(self.env)
    self.env.assertEqual(before_info_dict[WARN_ERR_SECTION], after_info_dict[WARN_ERR_SECTION])
    self.env.assertEqual(before_info_dict[COORD_WARN_ERR_SECTION], after_info_dict[COORD_WARN_ERR_SECTION])


def _common_warnings_errors_cluster_test_scenario(env):
  """Common setup for warnings and errors cluster tests"""
  # Create index
  env.expect('FT.CREATE', 'idx', 'SCHEMA', 'text', 'TEXT').ok()
  # Create doc
  conn = getConnectionByEnv(env)
  conn.execute_command('HSET', 'doc:1', 'text', 'hello world')
  # Create vector index for hybrid
  env.expect('FT.CREATE', 'idx_vec', 'PREFIX', '1', 'vec:', 'SCHEMA', 'vector', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()
  # Create doc for hybrid
  conn.execute_command('HSET', 'vec:1', 'vector', np.array([0.0, 0.0]).astype(np.float32).tobytes())

class testWarningsAndErrorsCluster:
  """Test class for warnings and errors metrics in cluster mode with RESP2"""

  def __init__(self):
    skipTest(cluster=False)
    self.env = Env()
    _common_warnings_errors_cluster_test_scenario(self.env)
    self.shards_prev_info_dict = {}
    self.coord_prev_info_dict = info_modules_to_dict(self.env)
    # Init all shards
    for i in range(1, self.env.shardsCount + 1):
      verify_shard_init(self.env.getConnection(i))
      self.shards_prev_info_dict[i] = info_modules_to_dict(self.env.getConnection(i))

  def setUp(self):
    self.coord_prev_info_dict = info_modules_to_dict(self.env)
    # Init all shards
    for i in range(1, self.env.shardsCount + 1):
      self.shards_prev_info_dict[i] = info_modules_to_dict(self.env.getConnection(i))

  def _verify_metrics_not_changes_all_shards(self, ignored_metrics : list):
    # Verify shards (coord is one of the shards as well)
    for shardId in range(1, self.env.shardsCount + 1):
      _verify_metrics_not_changed(self.env, self.env.getConnection(shardId), self.shards_prev_info_dict[shardId], ignored_metrics)

  def test_syntax_errors_cluster(self):
    # In cluster mode, syntax errors are only tracked at shard level

    # Test syntax errors for shard level syntax error
    self.env.expect('FT.SEARCH', 'idx', 'hello world:').error().contains('Syntax error at offset')
    # Test counter on each shard
    for shardId in range(1, self.env.shardsCount + 1):
      shard_conn = self.env.getConnection(shardId)
      info_dict = info_modules_to_dict(shard_conn)
      syntax_error_count = info_dict[WARN_ERR_SECTION][SYNTAX_ERROR_SHARD_METRIC]
      self.env.assertEqual(syntax_error_count, '1', message=f"Shard {shardId} has wrong syntax error count")
    # Check coord metric unchanged
    # Syntax error in FT.SEARCH are not checked on the coordinator
    info_dict = info_modules_to_dict(self.env)
    coord_syntax_error_count = info_dict[COORD_WARN_ERR_SECTION][SYNTAX_ERROR_COORD_METRIC]
    self.env.assertEqual(coord_syntax_error_count, '0')

    # Test syntax errors in aggregate
    self.env.expect('FT.AGGREGATE', 'idx', 'hello world:').error().contains('Syntax error at offset')
    # Test counter on each shard
    for shardId in range(1, self.env.shardsCount + 1):
      shard_conn = self.env.getConnection(shardId)
      info_dict = info_modules_to_dict(shard_conn)
      syntax_error_count = info_dict[WARN_ERR_SECTION][SYNTAX_ERROR_SHARD_METRIC]
      self.env.assertEqual(syntax_error_count, '2', message=f"Shard {shardId} has wrong syntax error count")
    # Check coord metric unchanged
    # Syntax error in FT.AGGREGATE are not checked on the coordinator
    info_dict = info_modules_to_dict(self.env)
    coord_syntax_error_count = info_dict[COORD_WARN_ERR_SECTION][SYNTAX_ERROR_COORD_METRIC]
    self.env.assertEqual(coord_syntax_error_count, '0')

    # Test syntax errors in hybrid
    # Syntax errors in the hybrid command are only counted on the coordinator.
    self.env.expect('FT.HYBRID', 'idx_vec', 'SEARCH', 'hello world:', 'VSIM', '@vector', '0').error().contains('Syntax error at offset')
    # Test counter on each shard unchanged
    for shardId in range(1, self.env.shardsCount + 1):
      shard_conn = self.env.getConnection(shardId)
      info_dict = info_modules_to_dict(shard_conn)
      syntax_error_count = info_dict[WARN_ERR_SECTION][SYNTAX_ERROR_SHARD_METRIC]
      self.env.assertEqual(syntax_error_count, '2', message=f"Shard {shardId} has wrong syntax error count")
    # Check coord metric
    info_dict = info_modules_to_dict(self.env)
    coord_syntax_error_count = info_dict[COORD_WARN_ERR_SECTION][SYNTAX_ERROR_COORD_METRIC]
    self.env.assertEqual(coord_syntax_error_count, '1')

    # Test other metrics not changed
    tested_in_this_test = [SYNTAX_ERROR_SHARD_METRIC, SYNTAX_ERROR_COORD_METRIC]
    self._verify_metrics_not_changes_all_shards(tested_in_this_test)

  def test_args_errors_cluster(self):

    # Check args error metric before adding any errors on each shard
    for shardId in range(1, self.env.shardsCount + 1):
      shard_conn = self.env.getConnection(shardId)
      info_dict = info_modules_to_dict(shard_conn)
      args_error_count = info_dict[WARN_ERR_SECTION][ARGS_ERROR_SHARD_METRIC]
      self.env.assertEqual(args_error_count, '0', message=f"Shard {shardId} has wrong initial args error count")
      args_error_count = info_dict[COORD_WARN_ERR_SECTION][ARGS_ERROR_COORD_METRIC]
      self.env.assertEqual(args_error_count, '0', message=f"Shard {shardId} has wrong initial args error count")

    # Test args errors that are counted in the shards
    self.env.expect('FT.SEARCH', 'idx', 'hello world', 'LIMIT', 0, 10, 'MEOW').error().contains('Unknown argument')
    # Test counter on each shard
    for shardId in range(1, self.env.shardsCount + 1):
      shard_conn = self.env.getConnection(shardId)
      info_dict = info_modules_to_dict(shard_conn)
      args_error_count = info_dict[WARN_ERR_SECTION][ARGS_ERROR_SHARD_METRIC]
      self.env.assertEqual(args_error_count, '1', message=f"Shard {shardId} has wrong args error count")
    # Check coord metric unchanged
    info_dict = info_modules_to_dict(self.env)
    coord_args_error_count = info_dict[COORD_WARN_ERR_SECTION][ARGS_ERROR_COORD_METRIC]
    self.env.assertEqual(coord_args_error_count, '0')

    #### Should fail when a bug (MOD-12465) is fixed
    #### When fixed, should decrease the shard arg count and increase the coord arg count
    # Test args errors that are counted in the coord
    self.env.expect('FT.SEARCH', 'idx', 'hello world', 'LIMIT', 'A', 0, 'MEOW').error().contains('Unknown argument')
    # Test counter on each shard
    for shardId in range(1, self.env.shardsCount + 1):
      shard_conn = self.env.getConnection(shardId)
      info_dict = info_modules_to_dict(shard_conn)
      args_error_count = info_dict[WARN_ERR_SECTION][ARGS_ERROR_SHARD_METRIC]
      self.env.assertEqual(args_error_count, '2', message=f"Shard {shardId} has wrong args error count")
    # Check coord metric unchanged
    info_dict = info_modules_to_dict(self.env)
    coord_args_error_count = info_dict[COORD_WARN_ERR_SECTION][ARGS_ERROR_COORD_METRIC]
    self.env.assertEqual(coord_args_error_count, '0')

    # Test arg error that is updated only in coord
    self.env.expect('FT.SEARCH', 'idx', 'hello world', 'DIALECT').error().contains('Need an argument for DIALECT')
    # Test counter on each shard (should not change)
    for shardId in range(1, self.env.shardsCount + 1):
      shard_conn = self.env.getConnection(shardId)
      info_dict = info_modules_to_dict(shard_conn)
      args_error_count = info_dict[WARN_ERR_SECTION][ARGS_ERROR_SHARD_METRIC]
      self.env.assertEqual(args_error_count, '2', message=f"Shard {shardId} has wrong args error count")
    # Check coord metric (should change)
    info_dict = info_modules_to_dict(self.env)
    coord_args_error_count = info_dict[COORD_WARN_ERR_SECTION][ARGS_ERROR_COORD_METRIC]
    self.env.assertEqual(coord_args_error_count, '1')

    # Test args errors in aggregate
    # All args errors in FT.AGGREGATE should be (de facto) counted on the coordinator
    self.env.expect('FT.AGGREGATE', 'idx', 'hello world', 'LIMIT', 0, 0, 'MEOW').error().contains('Unknown argument')
    # Test counter on each shard
    for shardId in range(1, self.env.shardsCount + 1):
      shard_conn = self.env.getConnection(shardId)
      info_dict = info_modules_to_dict(shard_conn)
      args_error_count = info_dict[WARN_ERR_SECTION][ARGS_ERROR_SHARD_METRIC]
      self.env.assertEqual(args_error_count, '2', message=f"Shard {shardId} has wrong args error count")
    # Check coord metric
    info_dict = info_modules_to_dict(self.env)
    coord_args_error_count = info_dict[COORD_WARN_ERR_SECTION][ARGS_ERROR_COORD_METRIC]
    self.env.assertEqual(coord_args_error_count, '2')

    # Test args errors in hybrid
    # All args errors in FT.HYBRID are counted on the coordinator
    self.env.expect('FT.HYBRID', 'idx_vec', 'SEARCH', 'hello world', 'VSIM', '@vector', '0', 'LIMIT', 0, 0, 'MEOW').error().contains('Unknown argument')
    # Test counter on each shard
    for shardId in range(1, self.env.shardsCount + 1):
      shard_conn = self.env.getConnection(shardId)
      info_dict = info_modules_to_dict(shard_conn)
      args_error_count = info_dict[WARN_ERR_SECTION][ARGS_ERROR_SHARD_METRIC]
      self.env.assertEqual(args_error_count, '2', message=f"Shard {shardId} has wrong args error count")
    # Check coord metric
    info_dict = info_modules_to_dict(self.env)
    coord_args_error_count = info_dict[COORD_WARN_ERR_SECTION][ARGS_ERROR_COORD_METRIC]
    self.env.assertEqual(coord_args_error_count, '3')

    # Test other metrics not changed
    tested_in_this_test = [ARGS_ERROR_SHARD_METRIC, ARGS_ERROR_COORD_METRIC]
    self._verify_metrics_not_changes_all_shards(tested_in_this_test)

  def test_no_error_queries_cluster(self):
    # Check no error queries not affecting any metric on each shard
    before_info_dicts = []
    for shardId in range(1, self.env.shardsCount + 1):
      shard_conn = self.env.getConnection(shardId)
      before_info_dicts.append(info_modules_to_dict(shard_conn))

    self.env.expect('FT.SEARCH', 'idx', 'hello world').noError()
    for shardId in range(1, self.env.shardsCount + 1):
      shard_conn = self.env.getConnection(shardId)
      after_info_dict = info_modules_to_dict(shard_conn)
      before_warn_err = before_info_dicts[shardId - 1][WARN_ERR_SECTION]
      after_warn_err = after_info_dict[WARN_ERR_SECTION]
      self.env.assertEqual(before_warn_err, after_warn_err, message=f"Shard {shardId} has wrong warnings/errors section after no-error query")
      before_coord_warn_err = before_info_dicts[shardId - 1][COORD_WARN_ERR_SECTION]
      after_coord_warn_err = after_info_dict[COORD_WARN_ERR_SECTION]
      self.env.assertEqual(before_coord_warn_err, after_coord_warn_err, message=f"Shard {shardId} has wrong coordinator warnings/errors section after no-error query")

    # Test no error queries in aggregate
    self.env.expect('FT.AGGREGATE', 'idx', 'hello world').noError()
    for shardId in range(1, self.env.shardsCount + 1):
      shard_conn = self.env.getConnection(shardId)
      after_info_dict = info_modules_to_dict(shard_conn)
      before_warn_err = before_info_dicts[shardId - 1][WARN_ERR_SECTION]
      after_warn_err = after_info_dict[WARN_ERR_SECTION]
      self.env.assertEqual(before_warn_err, after_warn_err, message=f"Shard {shardId} has wrong warnings/errors section after no-error aggregate query")
      before_coord_warn_err = before_info_dicts[shardId - 1][COORD_WARN_ERR_SECTION]
      after_coord_warn_err = after_info_dict[COORD_WARN_ERR_SECTION]
      self.env.assertEqual(before_coord_warn_err, after_coord_warn_err, message=f"Shard {shardId} has wrong coordinator warnings/errors section after no-error aggregate query")

    # Test no error queries in hybrid
    self.env.expect('FT.HYBRID', 'idx_vec', 'SEARCH', 'hello world', 'VSIM', '@vector', np.array([0.0, 0.0]).astype(np.float32).tobytes()).noError()
    for shardId in range(1, self.env.shardsCount + 1):
      shard_conn = self.env.getConnection(shardId)
      after_info_dict = info_modules_to_dict(shard_conn)
      before_warn_err = before_info_dicts[shardId - 1][WARN_ERR_SECTION]
      after_warn_err = after_info_dict[WARN_ERR_SECTION]
      self.env.assertEqual(before_warn_err, after_warn_err, message=f"Shard {shardId} has wrong warnings/errors section after no-error hybrid query")
      before_coord_warn_err = before_info_dicts[shardId - 1][COORD_WARN_ERR_SECTION]
      after_coord_warn_err = after_info_dict[COORD_WARN_ERR_SECTION]
      self.env.assertEqual(before_coord_warn_err, after_coord_warn_err, message=f"Shard {shardId} has wrong coordinator warnings/errors section after no-error hybrid query")

def test_errors_and_warnings_init(env):
  # Verify fields in metric are initialized properly
  info_dict = info_modules_to_dict(env)
  for metric in [WARN_ERR_SECTION, COORD_WARN_ERR_SECTION]:
    for field in info_dict[metric]:
      env.assertEqual(info_dict[metric][field], '0')
