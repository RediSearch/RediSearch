from common import *
from RLTest import Env
import redis
from inspect import currentframe
import numpy as np
from vecsim_utils import (
    DEFAULT_FIELD_NAME,
    DEFAULT_INDEX_NAME,
    set_up_database_with_vectors,
)

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
TIMEOUT_ERROR_SHARD_METRIC = f"{SEARCH_SHARD_PREFIX}total_query_errors_timeout"
TIMEOUT_WARNING_SHARD_METRIC = f"{SEARCH_SHARD_PREFIX}total_query_warnings_timeout"
OOM_ERROR_SHARD_METRIC = f"{SEARCH_SHARD_PREFIX}total_query_errors_oom"
OOM_WARNING_SHARD_METRIC = f"{SEARCH_SHARD_PREFIX}total_query_warnings_oom"
MAXPREFIXEXPANSIONS_WARNING_SHARD_METRIC = f"{SEARCH_SHARD_PREFIX}total_query_warnings_max_prefix_expansions"

COORD_WARN_ERR_SECTION = WARN_ERR_SECTION.replace(SEARCH_PREFIX, 'search_coordinator_')

SEARCH_COORD_PREFIX = 'search_coord_'
SYNTAX_ERROR_COORD_METRIC = f"{SEARCH_COORD_PREFIX}total_query_errors_syntax"
ARGS_ERROR_COORD_METRIC = f"{SEARCH_COORD_PREFIX}total_query_errors_arguments"
TIMEOUT_ERROR_COORD_METRIC = f"{SEARCH_COORD_PREFIX}total_query_errors_timeout"
TIMEOUT_WARNING_COORD_METRIC = f"{SEARCH_COORD_PREFIX}total_query_warnings_timeout"
OOM_ERROR_COORD_METRIC = f"{SEARCH_COORD_PREFIX}total_query_errors_oom"
OOM_WARNING_COORD_METRIC = f"{SEARCH_COORD_PREFIX}total_query_warnings_oom"
MAXPREFIXEXPANSIONS_WARNING_COORD_METRIC = f"{SEARCH_COORD_PREFIX}total_query_warnings_max_prefix_expansions"

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
  env.expect('FT.CREATE', 'idx', 'PREFIX', '1', 'doc:', 'SCHEMA', 'text', 'TEXT').ok()
  # Create vector index for hybrid
  env.expect('FT.CREATE', 'idx_vec', 'PREFIX', '1', 'vec:', 'SCHEMA', 'text', 'TEXT', 'vector', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()
  # Create doc
  env.expect('HSET', 'doc:1', 'text', 'hello world').equal(1)
  # Create docs for hybrid
  env.expect('HSET', 'vec:1', 'vector', np.array([1.0, 0.0]).astype(np.float32).tobytes(), 'text', 'hello world1').equal(2)
  env.expect('HSET', 'vec:2', 'vector', np.array([0.0, 1.0]).astype(np.float32).tobytes(), 'text', 'hello world2').equal(2)

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
    # Standalone shards are considered as coordinator in the info metrics

    # Test syntax errors
    self.env.expect('FT.SEARCH', 'idx', 'hello world:').error().contains('Syntax error at offset')
    # Test counter
    info_dict = info_modules_to_dict(self.env)
    syntax_error_count = info_dict[COORD_WARN_ERR_SECTION][SYNTAX_ERROR_COORD_METRIC]
    self.env.assertEqual(syntax_error_count, '1')
    # Test syntax errors in aggregate
    self.env.expect('FT.AGGREGATE', 'idx', 'hello world:').error().contains('Syntax error at offset')
    # Test counter
    info_dict = info_modules_to_dict(self.env)
    syntax_error_count = info_dict[COORD_WARN_ERR_SECTION][SYNTAX_ERROR_COORD_METRIC]
    self.env.assertEqual(syntax_error_count, '2')
    # Test syntax errors in hybrid
    self.env.expect('FT.HYBRID', 'idx_vec', 'SEARCH', 'hello world:', 'VSIM', '@vector', '0').error().contains('Syntax error at offset')
    # Test counter
    info_dict = info_modules_to_dict(self.env)
    syntax_error_count = info_dict[COORD_WARN_ERR_SECTION][SYNTAX_ERROR_COORD_METRIC]
    self.env.assertEqual(syntax_error_count, '3')

    # Test other metrics not changed
    tested_in_this_test = [SYNTAX_ERROR_COORD_METRIC]
    _verify_metrics_not_changed(self.env, self.env, self.prev_info_dict, tested_in_this_test)

  def test_args_errors_SA(self):
    # Standalone shards are considered as coordinator in the info metrics

    # Test args errors
    self.env.expect('FT.SEARCH', 'idx', 'hello world', 'LIMIT', 0, 0, 'MEOW').error().contains('Unknown argument')
    # Test counter
    info_dict = info_modules_to_dict(self.env)
    args_error_count = info_dict[COORD_WARN_ERR_SECTION][ARGS_ERROR_COORD_METRIC]
    self.env.assertEqual(args_error_count, '1')
    # Test args errors in aggregate
    self.env.expect('FT.AGGREGATE', 'idx', 'hello world', 'LIMIT', 0, 0, 'MEOW').error().contains('Unknown argument')
    # Test counter
    info_dict = info_modules_to_dict(self.env)
    args_error_count = info_dict[COORD_WARN_ERR_SECTION][ARGS_ERROR_COORD_METRIC]
    self.env.assertEqual(args_error_count, '2')
    # Test args errors in hybrid
    self.env.expect('FT.HYBRID', 'idx_vec', 'SEARCH', 'hello world', 'VSIM', '@vector', '0', 'LIMIT', 0, 0, 'MEOW').error().contains('Unknown argument')
    # Test counter
    info_dict = info_modules_to_dict(self.env)
    args_error_count = info_dict[COORD_WARN_ERR_SECTION][ARGS_ERROR_COORD_METRIC]
    self.env.assertEqual(args_error_count, '3')

    # Test other metrics not changed
    tested_in_this_test = [ARGS_ERROR_COORD_METRIC]
    _verify_metrics_not_changed(self.env, self.env, self.prev_info_dict, tested_in_this_test)

  def test_timeout_SA(self):
    # Standalone shards are considered as coordinator in the info metrics

    # ---------- Timeout Errors ----------
    self.env.expect(config_cmd(), 'SET', 'ON_TIMEOUT', 'FAIL').ok()
    before_info_dict_err = info_modules_to_dict(self.env)
    base_err = int(before_info_dict_err[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])

    # Test timeout error in FT.SEARCH
    self.env.expect(debug_cmd(), 'FT.SEARCH', 'idx', '*',
                    'TIMEOUT_AFTER_N', 0, 'DEBUG_PARAMS_COUNT', 2).error().contains('Timeout limit was reached')
    info_dict = info_modules_to_dict(self.env)
    self.env.assertEqual(info_dict[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC], str(base_err + 1))

    # Test timeout error in FT.AGGREGATE
    self.env.expect(debug_cmd(), 'FT.AGGREGATE', 'idx', '*',
                    'TIMEOUT_AFTER_N', 0, 'DEBUG_PARAMS_COUNT', 2).error().contains('Timeout limit was reached')
    info_dict = info_modules_to_dict(self.env)
    self.env.assertEqual(info_dict[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC], str(base_err + 2))

    # Test timeout error in FT.HYBRID (single shard debug)
    #### Test needs to be fixed (should return error, metric should increment by 1)
    self.env.expect(debug_cmd(), 'FT.HYBRID', 'idx_vec', 'SEARCH', 'hello world',
                    'VSIM', '@vector', np.array([0.0, 0.0]).astype(np.float32).tobytes(),
                    'TIMEOUT_AFTER_N_SEARCH', 0, 'DEBUG_PARAMS_COUNT', 2).noError()
    info_dict = info_modules_to_dict(self.env)
    self.env.assertEqual(info_dict[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC], str(base_err + 2))

    # ---------- Timeout Warnings ----------
    self.env.expect(config_cmd(), 'SET', 'ON_TIMEOUT', 'RETURN').ok()
    before_info_dict = info_modules_to_dict(self.env)
    base_warn = int(before_info_dict[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])

    # Test timeout warning in FT.SEARCH
    self.env.expect(debug_cmd(), 'FT.SEARCH', 'idx', '*',
                    'TIMEOUT_AFTER_N', 0, 'DEBUG_PARAMS_COUNT', 2).noError()
    info_dict = info_modules_to_dict(self.env)
    self.env.assertEqual(info_dict[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC], str(base_warn + 1))

    # Test timeout warning in FT.AGGREGATE
    self.env.expect(debug_cmd(), 'FT.AGGREGATE', 'idx', '*',
                    'TIMEOUT_AFTER_N', 0, 'DEBUG_PARAMS_COUNT', 2).noError()
    info_dict = info_modules_to_dict(self.env)
    self.env.assertEqual(info_dict[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC], str(base_warn + 2))

    # Test timeout warning in FT.HYBRID (single shard debug)
    ### Needs to be fixed
    ### Ignores the timeout and doesn't return a warning
    query_vec = np.array([1.2, 0.2]).astype(np.float32).tobytes()
    res = self.env.cmd(
        debug_cmd(), 'FT.HYBRID', 'idx_vec',
        'SEARCH', 'hello world',
        'VSIM', '@vector', '$BLOB',
        'PARAMS', '2', 'BLOB', query_vec,
        'TIMEOUT_AFTER_N_SEARCH', '1',
        'DEBUG_PARAMS_COUNT', '2'
    )
    warnings_idx = res.index('warnings')
    #### FIX : when the issue is fixed, res[warnings_idx+1] should be equal to timeout warning
    self.env.assertEqual(res[warnings_idx+1], [])
    info_dict = info_modules_to_dict(self.env)
    #### FIX : when the issue is fixed, this should be equal to base_warn + 3
    self.env.assertEqual(info_dict[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC], str(base_warn + 2))

    # Test other metrics not changed
    tested_in_this_test = [TIMEOUT_WARNING_COORD_METRIC, TIMEOUT_ERROR_COORD_METRIC]
    _verify_metrics_not_changed(self.env, self.env, before_info_dict, tested_in_this_test)

  def test_oom_errors_SA(self):
    # Standalone shards are considered as coordinator in the info metrics

    # ---------- OOM Errors ----------
    self.env.expect(config_cmd(), 'SET', 'ON_OOM', 'FAIL').ok()
    before_info_dict_err = info_modules_to_dict(self.env)
    base_err = int(before_info_dict_err[COORD_WARN_ERR_SECTION][OOM_ERROR_COORD_METRIC])

    self.env.expect('CONFIG', 'SET', 'maxmemory', '1').ok()
    # Test OOM error in FT.SEARCH
    self.env.expect('FT.SEARCH', 'idx', 'hello world').error().contains('Not enough memory available to execute the query')
    info_dict = info_modules_to_dict(self.env)
    self.env.assertEqual(info_dict[COORD_WARN_ERR_SECTION][OOM_ERROR_COORD_METRIC], str(base_err + 1))
    # Test OOM error in FT.AGGREGATE
    self.env.expect('FT.AGGREGATE', 'idx', 'hello world').error().contains('Not enough memory available to execute the query')
    info_dict = info_modules_to_dict(self.env)
    self.env.assertEqual(info_dict[COORD_WARN_ERR_SECTION][OOM_ERROR_COORD_METRIC], str(base_err + 2))
    # Test OOM error in FT.HYBRID (single shard debug)
    query_vec = np.array([1.2, 0.2]).astype(np.float32).tobytes()
    self.env.expect('FT.HYBRID', 'idx_vec', 'SEARCH', 'hello world', 'VSIM', '@vector', query_vec).error().contains('Not enough memory available to execute the query')
    info_dict = info_modules_to_dict(self.env)
    self.env.assertEqual(info_dict[COORD_WARN_ERR_SECTION][OOM_ERROR_COORD_METRIC], str(base_err + 3))

    # ---------- OOM Warnings ----------
    self.env.expect(config_cmd(), 'SET', 'ON_OOM', 'RETURN').ok()
    before_info_dict = info_modules_to_dict(self.env)
    base_warn = int(before_info_dict[COORD_WARN_ERR_SECTION][OOM_WARNING_COORD_METRIC])

    # Test OOM warning in FT.SEARCH
    self.env.expect('FT.SEARCH', 'idx', 'hello world').noError()
    info_dict = info_modules_to_dict(self.env)
    self.env.assertEqual(info_dict[COORD_WARN_ERR_SECTION][OOM_WARNING_COORD_METRIC], str(base_warn + 1))
    # Test OOM warning in FT.AGGREGATE
    self.env.expect('FT.AGGREGATE', 'idx', 'hello world').noError()
    info_dict = info_modules_to_dict(self.env)
    self.env.assertEqual(info_dict[COORD_WARN_ERR_SECTION][OOM_WARNING_COORD_METRIC], str(base_warn + 2))
    # Test OOM warning in FT.HYBRID (single shard debug)
    query_vec = np.array([1.2, 0.2]).astype(np.float32).tobytes()
    self.env.expect('FT.HYBRID', 'idx_vec', 'SEARCH', 'hello world', 'VSIM', '@vector', query_vec).noError()
    info_dict = info_modules_to_dict(self.env)
    self.env.assertEqual(info_dict[COORD_WARN_ERR_SECTION][OOM_WARNING_COORD_METRIC], str(base_warn + 3))

    self.env.expect('CONFIG', 'SET', 'maxmemory', '0').ok()
    # Test other metrics not changed
    tested_in_this_test = [OOM_ERROR_COORD_METRIC, OOM_WARNING_COORD_METRIC]
    _verify_metrics_not_changed(self.env, self.env, before_info_dict, tested_in_this_test)

  def test_max_prefix_expansions_SA(self):
      # Standalone shards are considered as coordinator in the info metrics

      # ---------- Max Prefix Expansions Warnings ----------
      # Save original config
      original_max_prefix_expansions = self.env.cmd(config_cmd(), 'GET', 'MAXPREFIXEXPANSIONS')[0][1]

      # Add more documents with different words starting with "hell" to trigger prefix expansion
      self.env.expect('HSET', 'doc:2', 'text', 'helloworld').equal(1)
      self.env.expect('HSET', 'doc:3', 'text', 'hellfire').equal(1)
      self.env.expect('HSET', 'vec:3', 'vector', np.array([0.5, 0.5]).astype(np.float32).tobytes(), 'text', 'helloworld').equal(2)
      self.env.expect('HSET', 'vec:4', 'vector', np.array([0.3, 0.7]).astype(np.float32).tobytes(), 'text', 'hellfire').equal(2)

      self.env.expect(config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', '1').ok()
      before_info_dict = info_modules_to_dict(self.env)
      base_warn = int(before_info_dict[COORD_WARN_ERR_SECTION][MAXPREFIXEXPANSIONS_WARNING_COORD_METRIC])

      # Test max prefix expansions warning in FT.SEARCH
      # "hell*" will match "hello", "helloworld", "hellfire" - 3 terms, but limit is 1
      self.env.expect('FT.SEARCH', 'idx', '@text:hell*').noError()
      info_dict = info_modules_to_dict(self.env)
      self.env.assertEqual(info_dict[COORD_WARN_ERR_SECTION][MAXPREFIXEXPANSIONS_WARNING_COORD_METRIC], str(base_warn + 1))

      # Test max prefix expansions warning in FT.AGGREGATE
      # "hello*" will match "hello", "helloworld" - 2 terms, but limit is 1
      self.env.expect('FT.AGGREGATE', 'idx', 'hello*').noError()
      info_dict = info_modules_to_dict(self.env)
      self.env.assertEqual(info_dict[COORD_WARN_ERR_SECTION][MAXPREFIXEXPANSIONS_WARNING_COORD_METRIC], str(base_warn + 2))

      # Test max prefix expansions warning in FT.HYBRID
      # "hello*" will match "hello", "helloworld" - 2 terms, but limit is 1
      query_vec = np.array([1.2, 0.2]).astype(np.float32).tobytes()
      self.env.expect('FT.HYBRID', 'idx_vec', 'SEARCH', 'hello*', 'VSIM', '@vector', query_vec).noError()
      info_dict = info_modules_to_dict(self.env)
      self.env.assertEqual(info_dict[COORD_WARN_ERR_SECTION][MAXPREFIXEXPANSIONS_WARNING_COORD_METRIC], str(base_warn + 3))

      # Clean up: Remove extra documents and restore original config
      self.env.expect('DEL', 'doc:2', 'doc:3', 'vec:3', 'vec:4').equal(4)
      self.env.expect(config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', original_max_prefix_expansions).ok()

      # Test other metrics not changed
      tested_in_this_test = [MAXPREFIXEXPANSIONS_WARNING_COORD_METRIC]
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
  env.expect('FT.CREATE', 'idx', 'PREFIX', '1', 'doc:', 'SCHEMA', 'text', 'TEXT').ok()
  # Create vector index for hybrid
  env.expect('FT.CREATE', 'idx_vec', 'PREFIX', '1', 'vec:', 'SCHEMA', 'vector', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()
  # Create doc
  conn = getConnectionByEnv(env)
  # Insert enough docs s.t each shard will timeout
  docs_per_shard = 3
  for i in range(docs_per_shard * env.shardsCount):
    conn.execute_command('HSET', f'doc:{i}', 'text', f'hello world {i}')

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
      self.env.assertEqual(syntax_error_count, '1',
                           message=f"Shard {shardId} has wrong syntax error count")
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
      self.env.assertEqual(syntax_error_count, '2',
                           message=f"Shard {shardId} has wrong syntax error count")
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
      self.env.assertEqual(syntax_error_count, '2',
                           message=f"Shard {shardId} has wrong syntax error count")
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
      self.env.assertEqual(args_error_count, '0',
                           message=f"Shard {shardId} has wrong initial args error count")
      args_error_count = info_dict[COORD_WARN_ERR_SECTION][ARGS_ERROR_COORD_METRIC]
      self.env.assertEqual(args_error_count, '0',
                           message=f"Shard {shardId} has wrong initial args error count")

    # Test args errors that are counted in the shards
    self.env.expect('FT.SEARCH', 'idx', 'hello world', 'LIMIT', 0, 10, 'MEOW').error().contains('Unknown argument')
    # Test counter on each shard
    for shardId in range(1, self.env.shardsCount + 1):
      shard_conn = self.env.getConnection(shardId)
      info_dict = info_modules_to_dict(shard_conn)
      args_error_count = info_dict[WARN_ERR_SECTION][ARGS_ERROR_SHARD_METRIC]
      self.env.assertEqual(args_error_count, '1',
                           message=f"Shard {shardId} has wrong args error count")
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
      self.env.assertEqual(args_error_count, '2',
                           message=f"Shard {shardId} has wrong args error count")
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
      self.env.assertEqual(args_error_count, '2',
                           message=f"Shard {shardId} has wrong args error count")
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
      self.env.assertEqual(args_error_count, '2',
                           message=f"Shard {shardId} has wrong args error count")
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
      self.env.assertEqual(args_error_count, '2',
                           message=f"Shard {shardId} has wrong args error count")
    # Check coord metric
    info_dict = info_modules_to_dict(self.env)
    coord_args_error_count = info_dict[COORD_WARN_ERR_SECTION][ARGS_ERROR_COORD_METRIC]
    self.env.assertEqual(coord_args_error_count, '3')

    # Test other metrics not changed
    tested_in_this_test = [ARGS_ERROR_SHARD_METRIC, ARGS_ERROR_COORD_METRIC]
    self._verify_metrics_not_changes_all_shards(tested_in_this_test)

  def test_timeout_cluster(self):
    # In cluster mode, test both shard-level and coordinator-level timeouts.
    # HYBRID debug is not supported in cluster.

    # ---------- Timeout Errors ----------
    allShards_change_timeout_policy(self.env, 'FAIL')

    coord_before_err = info_modules_to_dict(self.env)
    shards_before_err = {i: info_modules_to_dict(self.env.getConnection(i)) for i in range(1, self.env.shardsCount + 1)}
    base_err_coord = int(coord_before_err[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC])
    base_err_shards = {i: int(shards_before_err[i][WARN_ERR_SECTION][TIMEOUT_ERROR_SHARD_METRIC]) for i in shards_before_err}

    # Test timeout error in FT.SEARCH (shards)
    self.env.expect(debug_cmd(), 'FT.SEARCH', 'idx', '*',
                    'TIMEOUT_AFTER_N', 0, 'DEBUG_PARAMS_COUNT', 2).error().contains('Timeout limit was reached')
    # Shards: +1 each
    for shardId in range(1, self.env.shardsCount + 1):
      info_dict = info_modules_to_dict(self.env.getConnection(shardId))
      self.env.assertEqual(info_dict[WARN_ERR_SECTION][TIMEOUT_ERROR_SHARD_METRIC], str(base_err_shards[shardId] + 1),
                           message=f"Shard {shardId} SEARCH timeout error should be +1")
    # Coord: +1
    info_coord = info_modules_to_dict(self.env)
    self.env.assertEqual(info_coord[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC], str(base_err_coord + 1),
                         message="Coordinator timeout error should be +1 after FT.SEARCH")

    # Test timeout error in FT.AGGREGATE (shards only via INTERNAL_ONLY)
    self.env.expect(debug_cmd(), 'FT.AGGREGATE', 'idx', '*',
                    'TIMEOUT_AFTER_N', 1, 'INTERNAL_ONLY', 'DEBUG_PARAMS_COUNT', 3).error().contains('Timeout limit was reached')
    # Shards: +1 each again (total +2)
    for shardId in range(1, self.env.shardsCount + 1):
      info_dict = info_modules_to_dict(self.env.getConnection(shardId))
      self.env.assertEqual(info_dict[WARN_ERR_SECTION][TIMEOUT_ERROR_SHARD_METRIC], str(base_err_shards[shardId] + 2),
                           message=f"Shard {shardId} AGG INTERNAL_ONLY timeout error should be +2 total")
    # Coord: +2
    info_coord = info_modules_to_dict(self.env)
    self.env.assertEqual(info_coord[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC], str(base_err_coord + 2),
                         message="Coordinator timeout error should be +1 after AGG INTERNAL_ONLY")

    # Test timeout error in FT.AGGREGATE (coordinator)
    self.env.expect(debug_cmd(), 'FT.AGGREGATE', 'idx', '*',
                    'TIMEOUT_AFTER_N', 0, 'DEBUG_PARAMS_COUNT', 2).error().contains('Timeout limit was reached')
    # Shards: +3 (timeout is returned by the coord, but each shard still times out)
    for shardId in range(1, self.env.shardsCount + 1):
      info_dict = info_modules_to_dict(self.env.getConnection(shardId))
      self.env.assertEqual(info_dict[WARN_ERR_SECTION][TIMEOUT_ERROR_SHARD_METRIC], str(base_err_shards[shardId] + 3),
                           message=f"Shard {shardId} AGG coordinator timeout error should be +3 total")
    # Coord: +3
    info_coord = info_modules_to_dict(self.env)
    self.env.assertEqual(info_coord[COORD_WARN_ERR_SECTION][TIMEOUT_ERROR_COORD_METRIC], str(base_err_coord + 3),
                         message="Coordinator timeout error should be +1 after AGG coordinator timeout")

    # ---------- Timeout Warnings ----------
    allShards_change_timeout_policy(self.env, 'RETURN')

    coord_before_warn = info_modules_to_dict(self.env)
    shards_before_warn = {i: info_modules_to_dict(self.env.getConnection(i)) for i in range(1, self.env.shardsCount + 1)}
    base_warn_coord = int(coord_before_warn[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])
    base_warn_shards = {i: int(shards_before_warn[i][WARN_ERR_SECTION][TIMEOUT_WARNING_SHARD_METRIC]) for i in shards_before_warn}

    # Test timeout warning in FT.SEARCH (shards)
    self.env.expect(debug_cmd(), 'FT.SEARCH', 'idx', '*',
                    'TIMEOUT_AFTER_N', 0, 'DEBUG_PARAMS_COUNT', 2).noError()
    # Shards: +1 each
    for shardId in range(1, self.env.shardsCount + 1):
      info_dict = info_modules_to_dict(self.env.getConnection(shardId))
      self.env.assertEqual(info_dict[WARN_ERR_SECTION][TIMEOUT_WARNING_SHARD_METRIC], str(base_warn_shards[shardId] + 1),
                           message=f"Shard {shardId} SEARCH timeout warning should be +1")
    # Coord: unchanged
    info_coord = info_modules_to_dict(self.env)
    self.env.assertEqual(info_coord[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC], str(base_warn_coord),
                         message="Coordinator timeout warning should not change after FT.SEARCH")

    # Test other metrics not changed (on shards)
    tested_in_this_test = [TIMEOUT_ERROR_SHARD_METRIC, TIMEOUT_WARNING_SHARD_METRIC, TIMEOUT_ERROR_COORD_METRIC, TIMEOUT_WARNING_COORD_METRIC]
    self._verify_metrics_not_changes_all_shards(tested_in_this_test)

  def test_oom_errors_cluster_in_coord(self):
    # Error/Warnings in Coordinator only
    # Set OOM policy to fail
    self.env.expect(config_cmd(), 'SET', 'ON_OOM', 'FAIL').ok()
    coord_before_err = info_modules_to_dict(self.env)
    base_err_coord = int(coord_before_err[COORD_WARN_ERR_SECTION][OOM_ERROR_COORD_METRIC])
    base_warn_coord = int(coord_before_err[COORD_WARN_ERR_SECTION][OOM_WARNING_COORD_METRIC])
    # Set maxmemory to 1 to trigger OOM
    self.env.expect('CONFIG', 'SET', 'maxmemory', '1').ok()

    # Test OOM error in FT.SEARCH
    self.env.expect('FT.SEARCH', 'idx', 'hello world').error().contains('Not enough memory available to execute the query')
    # Coord: +1
    info_coord = info_modules_to_dict(self.env)
    self.env.assertEqual(info_coord[COORD_WARN_ERR_SECTION][OOM_ERROR_COORD_METRIC], str(base_err_coord + 1),
                         message="Coordinator OOM error should be +1 after FT.SEARCH")
    # Shards: unchanged
    for shardId in range(1, self.env.shardsCount + 1):
      info_dict = info_modules_to_dict(self.env.getConnection(shardId))
      self.env.assertEqual(info_dict[WARN_ERR_SECTION][OOM_ERROR_SHARD_METRIC], '0',
                           message=f"Shard {shardId} OOM error should not change after FT.SEARCH")

    # Test OOM error in FT.AGGREGATE
    self.env.expect('FT.AGGREGATE', 'idx', 'hello world').error().contains('Not enough memory available to execute the query')
    # Coord: +1
    info_coord = info_modules_to_dict(self.env)
    self.env.assertEqual(info_coord[COORD_WARN_ERR_SECTION][OOM_ERROR_COORD_METRIC], str(base_err_coord + 2),
                         message="Coordinator OOM error should be +1 after FT.AGGREGATE")
    # Shards: unchanged
    for shardId in range(1, self.env.shardsCount + 1):
      info_dict = info_modules_to_dict(self.env.getConnection(shardId))
      self.env.assertEqual(info_dict[WARN_ERR_SECTION][OOM_ERROR_SHARD_METRIC], '0',
                           message=f"Shard {shardId} OOM error should not change after FT.AGGREGATE")

    # Test OOM error in FT.HYBRID
    query_vector = np.array([1.2, 0.2]).astype(np.float32).tobytes()
    self.env.expect('FT.HYBRID', 'idx_vec', 'SEARCH', 'hello world', 'VSIM', '@vector', query_vector).error().contains('Not enough memory available to execute the query')
    # Coord: +1
    info_coord = info_modules_to_dict(self.env)
    self.env.assertEqual(info_coord[COORD_WARN_ERR_SECTION][OOM_ERROR_COORD_METRIC], str(base_err_coord + 3),
                         message="Coordinator OOM error should be +1 after FT.HYBRID")
    # Shards: unchanged
    for shardId in range(1, self.env.shardsCount + 1):
      info_dict = info_modules_to_dict(self.env.getConnection(shardId))
      self.env.assertEqual(info_dict[WARN_ERR_SECTION][OOM_ERROR_SHARD_METRIC], '0',
                           message=f"Shard {shardId} OOM error should not change after FT.HYBRID")

    # Test warnings
    # Set policy to return
    self.env.expect(config_cmd(), 'SET', 'ON_OOM', 'return').ok()
    # Test warning in FT.SEARCH
    self.env.expect('FT.SEARCH', 'idx', 'hello world').noError()
    # Coord: +1
    info_coord = info_modules_to_dict(self.env)
    self.env.assertEqual(info_coord[COORD_WARN_ERR_SECTION][OOM_WARNING_COORD_METRIC], str(base_warn_coord + 1),
                         message="Coordinator OOM warning should be +1 after FT.SEARCH")
    # Shards: unchanged
    for shardId in range(1, self.env.shardsCount + 1):
      info_dict = info_modules_to_dict(self.env.getConnection(shardId))
      self.env.assertEqual(info_dict[WARN_ERR_SECTION][OOM_WARNING_SHARD_METRIC], '0',
                           message=f"Shard {shardId} OOM warning should not change after FT.SEARCH")

    # Test warning in FT.AGGREGATE
    self.env.expect('FT.AGGREGATE', 'idx', 'hello world').noError()
    # Coord: +1
    info_coord = info_modules_to_dict(self.env)
    self.env.assertEqual(info_coord[COORD_WARN_ERR_SECTION][OOM_WARNING_COORD_METRIC], str(base_warn_coord + 2),
                         message="Coordinator OOM warning should be +1 after FT.AGGREGATE")
    # Shards: unchanged
    for shardId in range(1, self.env.shardsCount + 1):
      info_dict = info_modules_to_dict(self.env.getConnection(shardId))
      self.env.assertEqual(info_dict[WARN_ERR_SECTION][OOM_WARNING_SHARD_METRIC], '0',
                           message=f"Shard {shardId} OOM warning should not change after FT.AGGREGATE")

    # Test warning in FT.HYBRID
    query_vector = np.array([1.2, 0.2]).astype(np.float32).tobytes()
    self.env.expect('FT.HYBRID', 'idx_vec', 'SEARCH', 'hello world', 'VSIM', '@vector', query_vector).noError()
    # Coord: +1
    info_coord = info_modules_to_dict(self.env)
    self.env.assertEqual(info_coord[COORD_WARN_ERR_SECTION][OOM_WARNING_COORD_METRIC], str(base_warn_coord + 3),
                         message="Coordinator OOM warning should be +1 after FT.HYBRID")
    # Shards: unchanged
    for shardId in range(1, self.env.shardsCount + 1):
      info_dict = info_modules_to_dict(self.env.getConnection(shardId))
      self.env.assertEqual(info_dict[WARN_ERR_SECTION][OOM_WARNING_SHARD_METRIC], '0',
                           message=f"Shard {shardId} OOM warning should not change after FT.HYBRID")

    self.env.expect('CONFIG', 'SET', 'maxmemory', '0').ok()

    # Test other metrics not changed
    tested_in_this_test = [OOM_ERROR_COORD_METRIC, OOM_WARNING_COORD_METRIC]
    self._verify_metrics_not_changes_all_shards(tested_in_this_test)

  def test_oom_errors_cluster_in_shards(self):
    # Error/Warnings in Shards only
    # Set OOM policy to fail
    allShards_change_oom_policy(self.env, 'FAIL')
    # Set maxmemory to 1 to trigger OOM
    allShards_change_maxmemory_low(self.env)
    # Set unlimited maxmemory for coord
    set_unlimited_maxmemory_for_oom(self.env)

    coord_before_err = info_modules_to_dict(self.env)
    base_err_coord = int(coord_before_err[COORD_WARN_ERR_SECTION][OOM_ERROR_COORD_METRIC])
    base_warn_coord = int(coord_before_err[COORD_WARN_ERR_SECTION][OOM_WARNING_COORD_METRIC])
    shards_before_err = {i: info_modules_to_dict(self.env.getConnection(i)) for i in range(1, self.env.shardsCount + 1)}
    base_err_shards = {i: int(shards_before_err[i][WARN_ERR_SECTION][OOM_ERROR_SHARD_METRIC]) for i in shards_before_err}
    base_warn_shards = {i: int(shards_before_err[i][WARN_ERR_SECTION][OOM_WARNING_SHARD_METRIC]) for i in shards_before_err}

    # Test OOM error in FT.SEARCH
    self.env.expect('FT.SEARCH', 'idx', 'hello world').error().contains('Not enough memory available to execute the query')
    # Coord: +1
    info_coord = info_modules_to_dict(self.env)
    self.env.assertEqual(info_coord[COORD_WARN_ERR_SECTION][OOM_ERROR_COORD_METRIC], str(base_err_coord + 1),
                         message="Coordinator OOM error should be +1 after FT.SEARCH")
    # Shards: +1 each (besides shard 1 which is coord)
    shards_metrics = [info_modules_to_dict(self.env.getConnection(i))[WARN_ERR_SECTION][OOM_ERROR_SHARD_METRIC] for i in range(1, self.env.shardsCount + 1)]
    self.env.assertEqual(shards_metrics.count(str(base_err_shards[1] + 1)), 2,
                         message="Wrong number of shards with OOM error +1 after FT.SEARCH")

    # Test OOM error in FT.AGGREGATE
    self.env.expect('FT.AGGREGATE', 'idx', 'hello world').error().contains('Not enough memory available to execute the query')
    # Coord: +1
    info_coord = info_modules_to_dict(self.env)
    self.env.assertEqual(info_coord[COORD_WARN_ERR_SECTION][OOM_ERROR_COORD_METRIC], str(base_err_coord + 2),
                         message="Coordinator OOM error should be +1 after FT.AGGREGATE")
    # Shards: +1 each (besides shard 1 which is coord)
    shards_metrics = [info_modules_to_dict(self.env.getConnection(i))[WARN_ERR_SECTION][OOM_ERROR_SHARD_METRIC] for i in range(1, self.env.shardsCount + 1)]
    self.env.assertEqual(shards_metrics.count(str(base_err_shards[1] + 2)), 2,
                         message="Wrong number of shards with OOM error +1 after FT.AGGREGATE")

    # Test OOM error in FT.HYBRID
    query_vector = np.array([1.2, 0.2]).astype(np.float32).tobytes()
    self.env.expect('FT.HYBRID', 'idx_vec', 'SEARCH', 'hello world', 'VSIM', '@vector', query_vector).error().contains('Not enough memory available to execute the query')
    # Coord: +1
    info_coord = info_modules_to_dict(self.env)
    self.env.assertEqual(info_coord[COORD_WARN_ERR_SECTION][OOM_ERROR_COORD_METRIC], str(base_err_coord + 3),
                         message="Coordinator OOM error should be +1 after FT.HYBRID")
    # Shards: +1 each (besides shard 1 which is coord)
    shards_metrics = [info_modules_to_dict(self.env.getConnection(i))[WARN_ERR_SECTION][OOM_ERROR_SHARD_METRIC] for i in range(1, self.env.shardsCount + 1)]
    self.env.assertEqual(shards_metrics.count(str(base_err_shards[1] + 3)), 2,
                         message="Wrong number of shards with OOM error +1 after FT.HYBRID")

    # Test warnings
    # Set policy to return
    allShards_change_oom_policy(self.env, 'RETURN')
    # Test warning in FT.SEARCH
    self.env.expect('FT.SEARCH', 'idx', 'hello world').noError()

    # Coord: unchanged (Coord doesn't count warnings since resp2 doesn't return warnings)
    info_coord = info_modules_to_dict(self.env)
    self.env.assertEqual(info_coord[COORD_WARN_ERR_SECTION][OOM_WARNING_COORD_METRIC], str(base_warn_coord),
                         message="Coordinator OOM warning should not change after FT.SEARCH")
    # Shards: +1 each (besides shard 1 which is coord)
    shards_metrics = [info_modules_to_dict(self.env.getConnection(i))[WARN_ERR_SECTION][OOM_WARNING_SHARD_METRIC] for i in range(1, self.env.shardsCount + 1)]
    self.env.assertEqual(shards_metrics.count(str(base_warn_shards[1] + 1)), 2,
                         message="Wrong number of shards with OOM warning +1 after FT.SEARCH")

    # Test warning in FT.AGGREGATE
    self.env.expect('FT.AGGREGATE', 'idx', 'hello world').noError()
    # Coord: unchanged (Coord doesn't count warnings since resp2 doesn't return warnings)
    info_coord = info_modules_to_dict(self.env)
    self.env.assertEqual(info_coord[COORD_WARN_ERR_SECTION][OOM_WARNING_COORD_METRIC], str(base_warn_coord),
                         message="Coordinator OOM warning should not change after FT.AGGREGATE")
    # Shards: +1 each (besides shard 1 which is coord)
    shards_metrics = [info_modules_to_dict(self.env.getConnection(i))[WARN_ERR_SECTION][OOM_WARNING_SHARD_METRIC] for i in range(1, self.env.shardsCount + 1)]
    self.env.assertEqual(shards_metrics.count(str(base_warn_shards[1] + 2)), 2,
                         message="Wrong number of shards with OOM warning +1 after FT.AGGREGATE")
    # Test warning in FT.HYBRID
    query_vector = np.array([1.2, 0.2]).astype(np.float32).tobytes()
    self.env.expect('FT.HYBRID', 'idx_vec', 'SEARCH', 'hello world', 'VSIM', '@vector', query_vector).noError()
    # Coord: +1
    info_coord = info_modules_to_dict(self.env)
    self.env.assertEqual(info_coord[COORD_WARN_ERR_SECTION][OOM_WARNING_COORD_METRIC], str(base_warn_coord + 1),
                         message="Coordinator OOM warning should be +1 after FT.HYBRID")
    # Shards: +1 each (besides shard 1 which is coord)
    shards_metrics = [info_modules_to_dict(self.env.getConnection(i))[WARN_ERR_SECTION][OOM_WARNING_SHARD_METRIC] for i in range(1, self.env.shardsCount + 1)]
    self.env.assertEqual(shards_metrics.count(str(base_warn_shards[1] + 3)), 2,
                         message="Wrong number of shards with OOM warning +1 after FT.HYBRID")

    allShards_set_unlimited_maxmemory_for_oom(self.env)

    # Test other metrics not changed
    tested_in_this_test = [OOM_ERROR_COORD_METRIC, OOM_WARNING_COORD_METRIC, OOM_ERROR_SHARD_METRIC, OOM_WARNING_SHARD_METRIC]
    self._verify_metrics_not_changes_all_shards(tested_in_this_test)

  def test_max_prefix_expansions_cluster(self):
      # In cluster mode, maxprefixexpansion warnings are tracked at shard level
      # and propagated to coordinator

      # ---------- Max Prefix Expansions Warnings ----------
      # Save original config for all shards but last
      original_max_prefix_expansions = {}
      for shardId in range(1, self.env.shardsCount):
        shard_conn = self.env.getConnection(shardId)
        original_max_prefix_expansions[shardId] = shard_conn.execute_command(config_cmd(), 'GET', 'MAXPREFIXEXPANSIONS')[0][1]
        shard_conn.execute_command(config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', '1')

      # Insert documents so all shards have enough documents to trigger max prefix expansions warning
      docs_per_shard = 100
      total_docs = docs_per_shard * (self.env.shardsCount)
      conn = getConnectionByEnv(self.env)
      for i in range(total_docs):
        conn.execute_command('HSET', f'doc:maxprefix:{i}', 'text', f'helloworld{i}')
        # For vector index
        conn.execute_command('HSET', f'vec:maxprefix:{i}', 'text', f'helloworld{i}', 'vector', np.array([0.0, 0.0]).astype(np.float32).tobytes())

      # Trigger max prefix expansions warning in FT.SEARCH
      self.env.expect('FT.SEARCH', 'idx', '@text:hell*').noError()
      # Shards: +1 each besides last shard (which doesn't have enough docs to trigger warning)
      for shardId in range(1, self.env.shardsCount):
        info_dict = info_modules_to_dict(self.env.getConnection(shardId))
        self.env.assertEqual(info_dict[WARN_ERR_SECTION][MAXPREFIXEXPANSIONS_WARNING_SHARD_METRIC], '1',
                            message=f"Shard {shardId} max prefix expansions warning should be +1 after FT.SEARCH")
      # Last shard: unchanged
      info_dict = info_modules_to_dict(self.env.getConnection(self.env.shardsCount))
      self.env.assertEqual(info_dict[WARN_ERR_SECTION][MAXPREFIXEXPANSIONS_WARNING_SHARD_METRIC], '0',
                          message=f"Last shard max prefix expansions warning should not change after FT.SEARCH")

      # Coord: Unchanged (Coord doesn't count warnings in ft.search since resp2 doesn't return warnings)
      info_coord = info_modules_to_dict(self.env)
      base_warn_coord = int(info_coord[COORD_WARN_ERR_SECTION][MAXPREFIXEXPANSIONS_WARNING_COORD_METRIC])
      self.env.assertEqual(info_coord[COORD_WARN_ERR_SECTION][MAXPREFIXEXPANSIONS_WARNING_COORD_METRIC], str(base_warn_coord),
                          message="Coordinator max prefix expansions warning should not change after FT.SEARCH")

      # Trigger max prefix expansions warning in FT.AGGREGATE
      self.env.expect('FT.AGGREGATE', 'idx', '@text:hell*').noError()
      # Shards: +1 each besides last shard (which doesn't have enough docs to trigger warning)
      for shardId in range(1, self.env.shardsCount):
        info_dict = info_modules_to_dict(self.env.getConnection(shardId))
        self.env.assertEqual(info_dict[WARN_ERR_SECTION][MAXPREFIXEXPANSIONS_WARNING_SHARD_METRIC], '2',
                            message=f"Shard {shardId} max prefix expansions warning should be +1 after FT.AGGREGATE")
      # Last shard: unchanged
      info_dict = info_modules_to_dict(self.env.getConnection(self.env.shardsCount))
      self.env.assertEqual(info_dict[WARN_ERR_SECTION][MAXPREFIXEXPANSIONS_WARNING_SHARD_METRIC], '0',
                          message=f"Last shard max prefix expansions warning should not change after FT.AGGREGATE")

      # Coord: unchanged (Coord doesn't count warnings in ft.aggregate since resp2 doesn't return warnings)
      info_coord = info_modules_to_dict(self.env)
      self.env.assertEqual(info_coord[COORD_WARN_ERR_SECTION][MAXPREFIXEXPANSIONS_WARNING_COORD_METRIC], str(base_warn_coord),
                            message="Coordinator max prefix expansions warning should not change after FT.AGGREGATE")

      # Trigger max prefix expansions warning in FT.HYBRID is not supported yet in cluster mode
      # Change test when FT.HYBRID max prefix expansion warnings is supported in cluster mode
      query_vector = np.array([1.2, 0.2]).astype(np.float32).tobytes()
      res = self.env.cmd('FT.HYBRID', 'idx_vec', 'SEARCH', 'hell*', 'VSIM', '@vector', query_vector)
      # Verify *no* warning is returned in ft.hybrid response
      warnings_idx = res.index('warnings') + 1
      self.env.assertFalse('Max prefix expansions limit was reached' in res[warnings_idx])

      # Shards: should be +1 each besides last shard (which doesn't have enough docs to trigger warning)
      # But since we don't support warnings in ft.hybrid in cluster mode, we don't expect any change
      for shardId in range(1, self.env.shardsCount):
        info_dict = info_modules_to_dict(self.env.getConnection(shardId))
        self.env.assertEqual(info_dict[WARN_ERR_SECTION][MAXPREFIXEXPANSIONS_WARNING_SHARD_METRIC], '2',
                            message=f"Shard {shardId} max prefix expansions warning should not change after FT.HYBRID")

      # Coord: should be +1 since we don't support warnings in ft.hybrid in cluster mode
      # Change test when FT.HYBRID max prefix expansion warnings is supported in cluster mode
      info_coord = info_modules_to_dict(self.env)
      self.env.assertEqual(info_coord[COORD_WARN_ERR_SECTION][MAXPREFIXEXPANSIONS_WARNING_COORD_METRIC], str(base_warn_coord),
                          message="Coordinator max prefix expansions warning should not change after FT.HYBRID")

      # Restore original max prefix expansions
      for shardId in range(1, self.env.shardsCount):
        shard_conn = self.env.getConnection(shardId)
        shard_conn.execute_command(config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', original_max_prefix_expansions[shardId])

      # Remove test data
      for i in range(total_docs):
        conn.execute_command('DEL', f'doc:maxprefix:{i}')
        conn.execute_command('DEL', f'vec:maxprefix:{i}')

      # Test other metrics not changed
      tested_in_this_test = [MAXPREFIXEXPANSIONS_WARNING_SHARD_METRIC, MAXPREFIXEXPANSIONS_WARNING_COORD_METRIC]
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
      self.env.assertEqual(before_warn_err, after_warn_err,
                           message=f"Shard {shardId} has wrong warnings/errors section after no-error query")
      before_coord_warn_err = before_info_dicts[shardId - 1][COORD_WARN_ERR_SECTION]
      after_coord_warn_err = after_info_dict[COORD_WARN_ERR_SECTION]
      self.env.assertEqual(before_coord_warn_err, after_coord_warn_err,
                           message=f"Shard {shardId} has wrong coordinator warnings/errors section after no-error query")

    # Test no error queries in aggregate
    self.env.expect('FT.AGGREGATE', 'idx', 'hello world').noError()
    for shardId in range(1, self.env.shardsCount + 1):
      shard_conn = self.env.getConnection(shardId)
      after_info_dict = info_modules_to_dict(shard_conn)
      before_warn_err = before_info_dicts[shardId - 1][WARN_ERR_SECTION]
      after_warn_err = after_info_dict[WARN_ERR_SECTION]
      self.env.assertEqual(before_warn_err, after_warn_err,
                           message=f"Shard {shardId} has wrong warnings/errors section after no-error aggregate query")
      before_coord_warn_err = before_info_dicts[shardId - 1][COORD_WARN_ERR_SECTION]
      after_coord_warn_err = after_info_dict[COORD_WARN_ERR_SECTION]
      self.env.assertEqual(before_coord_warn_err, after_coord_warn_err,
                           message=f"Shard {shardId} has wrong coordinator warnings/errors section after no-error aggregate query")

    # Test no error queries in hybrid
    self.env.expect('FT.HYBRID', 'idx_vec', 'SEARCH', 'hello world', 'VSIM', '@vector', np.array([0.0, 0.0]).astype(np.float32).tobytes()).noError()
    for shardId in range(1, self.env.shardsCount + 1):
      shard_conn = self.env.getConnection(shardId)
      after_info_dict = info_modules_to_dict(shard_conn)
      before_warn_err = before_info_dicts[shardId - 1][WARN_ERR_SECTION]
      after_warn_err = after_info_dict[WARN_ERR_SECTION]
      self.env.assertEqual(before_warn_err, after_warn_err,
                           message=f"Shard {shardId} has wrong warnings/errors section after no-error hybrid query")
      before_coord_warn_err = before_info_dicts[shardId - 1][COORD_WARN_ERR_SECTION]
      after_coord_warn_err = after_info_dict[COORD_WARN_ERR_SECTION]
      self.env.assertEqual(before_coord_warn_err, after_coord_warn_err,
                           message=f"Shard {shardId} has wrong coordinator warnings/errors section after no-error hybrid query")

def test_errors_and_warnings_init(env):
  # Verify fields in metric are initialized properly
  info_dict = info_modules_to_dict(env)
  for metric in [WARN_ERR_SECTION, COORD_WARN_ERR_SECTION]:
    for field in info_dict[metric]:
      env.assertEqual(info_dict[metric][field], '0')

@skip(cluster=False)
def test_warnings_metric_count_timeout_cluster_in_shards_resp3(env):
  env = Env(protocol=3)

  allShards_change_timeout_policy(env, 'RETURN')

  # Create index
  env.expect('FT.CREATE', 'idx', 'PREFIX', '1', 'doc:', 'SCHEMA', 'text', 'TEXT').ok()
  # Create doc
  conn = getConnectionByEnv(env)
  # Insert enough docs s.t each shard will timeout
  docs_per_shard = 3
  for i in range(docs_per_shard * env.shardsCount):
    conn.execute_command('HSET', f'doc:{i}', 'text', f'hello world {i}')

  before_info_dicts = {}
  for shardId in range(1, env.shardsCount + 1):
    shard_conn = env.getConnection(shardId)
    before_info_dicts[shardId] = info_modules_to_dict(shard_conn)

  coord_before_info_dict = info_modules_to_dict(env)

  # Test coord metric update after debug ft.search (not tested with resp2)
  env.expect(debug_cmd(), 'FT.SEARCH', 'idx', '*', 'TIMEOUT_AFTER_N', 1, 'DEBUG_PARAMS_COUNT', 2).noError()

  # Check coord metric + 1
  after_info_dict = info_modules_to_dict(env)
  before_warn_err = int(coord_before_info_dict[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])
  after_warn_err = int(after_info_dict[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])
  env.assertEqual(after_warn_err, before_warn_err + 1,
                   message="Coordinator timeout warning should be +1 after FT.SEARCH")

  # Test debug aggregate with and without internal only
  env.expect(debug_cmd(), 'FT.AGGREGATE', 'idx', '*', 'TIMEOUT_AFTER_N', 0, 'DEBUG_PARAMS_COUNT', 2).noError()

  # Verify timeout warning was counted on coordinator
  after_info_dict = info_modules_to_dict(env)
  before_warn_err = int(coord_before_info_dict[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])
  after_warn_err = int(after_info_dict[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])
  env.assertEqual(after_warn_err, before_warn_err + 2,
                   message="Coordinator timeout warning should be +1 after FT.AGGREGATE")

  # Test with internal only
  env.expect(debug_cmd(), 'FT.AGGREGATE', 'idx', '*', 'TIMEOUT_AFTER_N', 1, 'INTERNAL_ONLY', 'DEBUG_PARAMS_COUNT', 3).noError()

  # Since the cursor is not depleted after 1 read, the coord might sent another read to the shards
  # which might trigger more metric increments (until reaching EOF)
  for shardId in range(1, env.shardsCount + 1):
    shard_conn = env.getConnection(shardId)
    after_info_dict = info_modules_to_dict(shard_conn)
    before_warn_err = int(before_info_dicts[shardId][WARN_ERR_SECTION][TIMEOUT_WARNING_SHARD_METRIC])
    after_warn_err = int(after_info_dict[WARN_ERR_SECTION][TIMEOUT_WARNING_SHARD_METRIC])
    env.assertGreaterEqual(after_warn_err, before_warn_err + 3,
                           message=f"Shard {shardId} timeout warning should be at least +1 after FT.AGGREGATE with INTERNAL_ONLY")

  # So, we check just the coord's metric
  after_info_dict = info_modules_to_dict(env)
  before_warn_err = int(coord_before_info_dict[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])
  after_warn_err = int(after_info_dict[COORD_WARN_ERR_SECTION][TIMEOUT_WARNING_COORD_METRIC])
  env.assertEqual(after_warn_err, before_warn_err + 3,
                   message="Coordinator timeout warning should be +2 after FT.AGGREGATE with INTERNAL_ONLY")

  # Check other metric unchanged
  tested_in_this_test = [TIMEOUT_WARNING_SHARD_METRIC, TIMEOUT_WARNING_COORD_METRIC]
  for shardId in range(1, env.shardsCount + 1):
    shard_conn = env.getConnection(shardId)
    _verify_metrics_not_changed(env, shard_conn, before_info_dicts[shardId], tested_in_this_test)

@skip(cluster=False)
def test_warnings_metric_count_oom_cluster_in_shards_resp3():
  # Test OOM warnings in shards only with RESP3
  env  = Env(protocol=3)
  _common_warnings_errors_cluster_test_scenario(env)
  # Set OOM policy to return
  allShards_change_oom_policy(env, 'RETURN')
  allShards_change_maxmemory_low(env)
  # Set unlimited maxmemory for coord
  set_unlimited_maxmemory_for_oom(env)
  # Test warning in FT.SEARCH
  res = env.cmd('FT.SEARCH', 'idx', 'hello world')
  env.assertEqual(res['warning'][0], 'One or more shards failed to execute the query due to insufficient memory')
  # Coord: +1
  info_coord = info_modules_to_dict(env)
  env.assertEqual(info_coord[COORD_WARN_ERR_SECTION][OOM_WARNING_COORD_METRIC], '1')

  # Test warning in FT.AGGREGATE
  # FT.AGGREGATE doesn't return warning in cluster for empty results
  res = env.cmd('FT.AGGREGATE', 'idx', 'hello world')
  # TODO - Check warning in FT.AGGREGATE when empty results are handled correctly
  # The following asserts should fail when empty results are handled correctly
  env.assertEqual(res['warning'], [])
  # Coord: +1
  info_coord = info_modules_to_dict(env)
  env.assertEqual(info_coord[COORD_WARN_ERR_SECTION][OOM_WARNING_COORD_METRIC], '1')

@skip(cluster=False)
def test_warnings_metric_count_maxprefixexpansions_cluster_resp3():
  # Test max prefix expansions warnings in shards and coord with RESP3
  env  = Env(protocol=3)

  # Create index and add documents
  _common_warnings_errors_cluster_test_scenario(env)
  # Add more documents with different words starting with "hell" to trigger prefix expansion
  conn = getConnectionByEnv(env)
  docs_per_shard = 100
  total_docs = docs_per_shard * (env.shardsCount)
  for i in range(total_docs):
    conn.execute_command('HSET', f'doc:maxprefix:{i}', 'text', f'hello{i}')

  # Set max prefix expansions to 1 in all shards
  for shardId in range(1, env.shardsCount + 1):
    shard_conn = env.getConnection(shardId)
    shard_conn.execute_command(config_cmd(), 'SET', 'MAXPREFIXEXPANSIONS', '1')

  # Test warning in FT.SEARCH
  res = env.cmd('FT.SEARCH', 'idx', '@text:hell*')
  env.assertEqual(res['warning'][0], 'Max prefix expansions limit was reached')
  # Coord: +1
  info_coord = info_modules_to_dict(env)
  env.assertEqual(info_coord[COORD_WARN_ERR_SECTION][MAXPREFIXEXPANSIONS_WARNING_COORD_METRIC], '1')
  # Test warning in FT.AGGREGATE
  res = env.cmd('FT.AGGREGATE', 'idx', '@text:hell*')
  env.assertEqual(res['warning'][0], 'Max prefix expansions limit was reached')
  # Coord: +1
  info_coord = info_modules_to_dict(env)
  env.assertEqual(info_coord[COORD_WARN_ERR_SECTION][MAXPREFIXEXPANSIONS_WARNING_COORD_METRIC], '2')

########
# Multi Threaded Stats tests
########

MULTI_THREADING_SECTION = f'{SEARCH_PREFIX}multi_threading'
ACTIVE_IO_THREADS_METRIC = f'{SEARCH_PREFIX}active_io_threads'
ACTIVE_WORKER_THREADS_METRIC = f'{SEARCH_PREFIX}active_worker_threads'
ACTIVE_COORD_THREADS_METRIC = f'{SEARCH_PREFIX}active_coord_threads'
WORKERS_LOW_PRIORITY_PENDING_JOBS_METRIC = f'{SEARCH_PREFIX}workers_low_priority_pending_jobs'
WORKERS_HIGH_PRIORITY_PENDING_JOBS_METRIC = f'{SEARCH_PREFIX}workers_high_priority_pending_jobs'
COORD_HIGH_PRIORITY_PENDING_JOBS_METRIC = f'{SEARCH_PREFIX}coord_high_priority_pending_jobs'

def test_active_io_threads_stats(env):
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

  # Verify all fields initialized to 0.
  env.assertEqual(info_dict[MULTI_THREADING_SECTION][ACTIVE_IO_THREADS_METRIC], '0',
                 message=f"{ACTIVE_IO_THREADS_METRIC} should be 0 when idle")
  # There's no deterministic way to test active_io_threads increases while a query is running,
  # we test it in unit tests.

# --- Helper Function (to be shared by both SA and Cluster tests) ---
# NOTE: Currently query debug pause mechanism only supports pausing one query at a time.
def _test_active_worker_threads(env, num_queries):
    env.assertEqual(num_queries, 1, message="Currently only supports pausing one query at a time")
    """
    Helper function to test active_worker_threads metric with paused queries.

    Args:
        env: Test environment
        num_queries: Number of queries to pause.
                     NOTE: Currently query debug pause mechanism only supports pausing one query at a time.
    """
    conn = getConnectionByEnv(env)

    # Setup: Ensure workers are configured (need at least num_queries workers)
    run_command_on_all_shards(env, config_cmd(), 'SET', 'WORKERS', num_queries)

    # Create index and add test data
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC').ok()
    for i in range(10):
        conn.execute_command('HSET', f'doc{i}', 'n', i)

    # Verify active_worker_threads and coord threads start at 0
    for i, con in enumerate(env.getOSSMasterNodesConnectionList()):
        info_dict = info_modules_to_dict(con)
        env.assertEqual(info_dict[MULTI_THREADING_SECTION][ACTIVE_WORKER_THREADS_METRIC], '0',
                       message=f"shard {i}: {ACTIVE_WORKER_THREADS_METRIC} should be 0 when idle")
        env.assertEqual(info_dict[MULTI_THREADING_SECTION][ACTIVE_COORD_THREADS_METRIC], '0',
                       message=f"shard {i}: {ACTIVE_COORD_THREADS_METRIC} should be 0 when idle")

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
                      (env, [query_type, 'idx', '*'], 'Index', 0, ['INTERNAL_ONLY']),
                      result_list),
                daemon=True
            )
            query_threads.append(t)
            t.start()

        # Wait for all queries to be paused
        with TimeLimit(120):
            while not all(allShards_getIsRPPaused(env)):
                time.sleep(0.1)

        # Verify active_worker_threads == num_queries
        for i, con in enumerate(env.getOSSMasterNodesConnectionList()):
            info_dict = info_modules_to_dict(con)
            env.assertEqual(info_dict[MULTI_THREADING_SECTION][ACTIVE_WORKER_THREADS_METRIC], str(num_queries),
                           message=f"shard {i}: {query_type}: {ACTIVE_WORKER_THREADS_METRIC} should be {num_queries} when {num_queries} queries are paused")

        # If this is cluster, and FT.AGGREGATE, verify active_coord_threads == num_queries
        if env.isCluster() and query_type == 'FT.AGGREGATE':
          info_dict = info_modules_to_dict(env)
          env.assertEqual(info_dict[MULTI_THREADING_SECTION][ACTIVE_COORD_THREADS_METRIC], str(num_queries),
                         message=f"coordinator: {query_type}: {ACTIVE_COORD_THREADS_METRIC} should be {num_queries} when {num_queries} queries are paused")

        # Resume all queries
        allShards_setPauseRPResume(env)

        # Wait for all query threads to complete
        for t in query_threads:
            t.join()

        # Drain worker thread pool to ensure all jobs complete
        run_command_on_all_shards(env, debug_cmd(), 'WORKERS', 'DRAIN')

        # Verify active_worker_threads returns to 0
        for i, con in enumerate(env.getOSSMasterNodesConnectionList()):
            info_dict = info_modules_to_dict(con)
            env.assertEqual(info_dict[MULTI_THREADING_SECTION][ACTIVE_WORKER_THREADS_METRIC], '0',
                           message=f"shard {i}: {query_type}: {ACTIVE_WORKER_THREADS_METRIC} should return to 0 after queries complete")

    # Test both query types
    _test_query_type('FT.SEARCH')
    _test_query_type('FT.AGGREGATE')

def test_active_worker_threads(env):
    num_queries = 1
    _test_active_worker_threads(env, num_queries)

def _test_pending_jobs_metrics(env, command_type):
    """
    Parameters:
        - env: Test environment (works for both SA and cluster)
    """

    # --- STEP 1: SETUP ---
    # Configure WORKERS (we just need workers enabled, e.g., 2)
    run_command_on_all_shards(env, config_cmd(), 'SET', 'WORKERS', '2')

    # Define variables
    num_vectors = 10 * env.shardsCount  # Number of vectors to index (creates low priority jobs)
    num_queries = 3   # Number of queries to execute (creates high priority jobs)
    dim = 4
    vector_field = DEFAULT_FIELD_NAME
    index_name = 'idx'

    # --- STEP 2: VERIFY INITIAL STATE (metrics = 0) ---
    for conn in env.getOSSMasterNodesConnectionList():
        info_dict = info_modules_to_dict(conn)
        env.assertEqual(info_dict[MULTI_THREADING_SECTION][WORKERS_LOW_PRIORITY_PENDING_JOBS_METRIC], '0')
        env.assertEqual(info_dict[MULTI_THREADING_SECTION][WORKERS_HIGH_PRIORITY_PENDING_JOBS_METRIC], '0')

    #  --- STEP 3: PAUSE WORKERS THREAD POOL ---
    # Pause workers to prevent jobs from executing
    run_command_on_all_shards(env, debug_cmd(), 'WORKERS', 'PAUSE')

    # --- STEP 4: CREATE INDEX AND INDEX VECTORS (creates workers_low_priority_pending_jobs) ---
    # Create index with HNSW and load vectors (HNSW creates background indexing jobs which are low priority)
    set_up_database_with_vectors(env, dim, num_vectors, index_name=index_name,
                                             field_name=vector_field, datatype='FLOAT32',
                                             metric='L2', alg='HNSW')

    def check_indexing_jobs_pending():
        num_shards = env.shardsCount
        all_shards_ready = [False] * num_shards
        state = {
          'indexing_jobs_pending': [0] * num_shards,
          'expected_indexing_jobs': [0] * num_shards,
        }

        for i, con in enumerate(env.getOSSMasterNodesConnectionList()):
          # Expected low_priority_pending_jobs = con.dbsize() (number of vectors on this shard)
          expected_indexing_jobs = con.execute_command('DBSIZE')

          shard_stats = info_modules_to_dict(con)
          indexing_jobs_pending = int(shard_stats[MULTI_THREADING_SECTION][WORKERS_LOW_PRIORITY_PENDING_JOBS_METRIC])

          all_shards_ready[i] = (expected_indexing_jobs == indexing_jobs_pending)
          state['expected_indexing_jobs'][i] = expected_indexing_jobs
          state['indexing_jobs_pending'][i] = indexing_jobs_pending
        return all(all_shards_ready), state

    wait_for_condition(check_indexing_jobs_pending, "wait_for_workers_low_priority_jobs_pending")

    # --- STEP 5: EXECUTE QUERIES (creates high_priority_pending_jobs) ---
    # Launch num_queries queries in background threads
    # Queries will be queued as high-priority jobs but not executed (workers paused)

    query_threads = []
    query_results = []

    def run_query(query_id):
        conn = getConnectionByEnv(env)
        try:
            result = conn.execute_command(f'FT.{command_type}', index_name, '*')
            query_results.append((query_id, 'success', result))
        except Exception as e:
            query_results.append((query_id, 'error', e))

    for i in range(num_queries):
        t = threading.Thread(target=run_query, args=(i,))
        query_threads.append(t)
        t.start()

    # Give threads a moment to start and attempt to queue their queries
    time.sleep(0.1)

    # Check if any queries failed immediately (before being queued)
    for query_id, status, result in query_results:
        if status == 'error':
            env.assertTrue(False, message=f"Query {query_id} failed immediately: {result}")

    # --- STEP 6: WAIT FOR THREADPOOL STATS TO UPDATE (jobs queued) ---
    # Wait for the threadpool stats to reflect the expected pending jobs
    def check_queries_jobs_pending():
        num_shards = env.shardsCount
        all_shards_ready = [False] * num_shards
        expected_queries_jobs = num_queries
        state = {
          'queries_jobs_pending': [0] * num_shards,
          'expected_queries_jobs': [expected_queries_jobs] * num_shards,
        }

        for i, con in enumerate(env.getOSSMasterNodesConnectionList()):

          shard_stats = info_modules_to_dict(con)
          queries_pending_jobs = int(shard_stats[MULTI_THREADING_SECTION][WORKERS_HIGH_PRIORITY_PENDING_JOBS_METRIC])

          all_shards_ready[i] = (expected_queries_jobs == queries_pending_jobs)
          state['queries_jobs_pending'][i] = queries_pending_jobs
          state['expected_queries_jobs'][i] = expected_queries_jobs
        return all(all_shards_ready), state

    wait_for_condition(check_queries_jobs_pending, "wait_for_high_priority_jobs_pending")

    # --- STEP 7: RESUME WORKERS AND DRAIN ---
    # Resume workers:
    run_command_on_all_shards(env, debug_cmd(), 'WORKERS', 'RESUME')

    # Wait for all query threads to complete:
    for t in query_threads:
        t.join(timeout=30)

    # Drain worker thread pool to ensure all jobs complete:
    run_command_on_all_shards(env, debug_cmd(), 'WORKERS', 'DRAIN')

    # --- STEP 8: VERIFY METRICS RETURN TO 0 ---
    # Wait for metrics to return to 0 (job callback finished before stats update)
    def check_reset_metrics():
        num_shards = env.shardsCount
        all_shards_ready = [False] * num_shards
        state = {
          'workers_low_priority_jobs_pending': [-1] * num_shards,
          'workers_high_priority_jobs_pending': [-1] * num_shards,
        }

        for i, con in enumerate(env.getOSSMasterNodesConnectionList()):

          shard_stats = info_modules_to_dict(con)
          queries_jobs_pending = int(shard_stats[MULTI_THREADING_SECTION][WORKERS_HIGH_PRIORITY_PENDING_JOBS_METRIC])
          background_indexing_jobs_pending = int(shard_stats[MULTI_THREADING_SECTION][WORKERS_LOW_PRIORITY_PENDING_JOBS_METRIC])

          all_shards_ready[i] = (queries_jobs_pending == 0 and background_indexing_jobs_pending == 0)
          state['workers_low_priority_jobs_pending'][i] = background_indexing_jobs_pending
          state['workers_high_priority_jobs_pending'][i] = queries_jobs_pending
        return all(all_shards_ready), state

    wait_for_condition(check_reset_metrics, "wait_for_workers_pending_jobs_metric_reset")

def test_pending_jobs_metrics_search():
  env = Env(moduleArgs='DEFAULT_DIALECT 2')
  _test_pending_jobs_metrics(env, 'SEARCH')

def test_pending_jobs_metrics_aggregate():
  env = Env(moduleArgs='DEFAULT_DIALECT 2')
  _test_pending_jobs_metrics(env, 'AGGREGATE')

# The metric is increased when the following commands are executed in cluster env:
# - FT.SEARCH
# - FT.AGGREGATE
# - FT.CURSOR *
# - FT.HYBRID

class TestCoordHighPriorityPendingJobs(object):
  def __init__(self):
    if not CLUSTER:
        raise SkipTest()
    self.env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(self.env)
    num_docs = 10 * self.env.shardsCount
    self.dim = 2
    set_up_database_with_vectors(self.env, self.dim, num_docs, index_name=DEFAULT_INDEX_NAME,
                                             field_name=DEFAULT_FIELD_NAME, datatype='FLOAT32',
                                             metric='L2', alg='FLAT', additional_schema_args=['t', 'TEXT'])

    # Add some text to the documents
    for i in range(num_docs):
      conn.execute_command('HSET', f'doc:{i}', 't', f'hello')

    # VERIFY INITIAL STATE (metric = 0) ---
    info_dict = info_modules_to_dict(self.env)
    self.env.assertEqual(info_dict[MULTI_THREADING_SECTION][COORD_HIGH_PRIORITY_PENDING_JOBS_METRIC], '0')


  def verify_coord_high_priority_pending_jobs(self, command_type, num_commands_per_type, search_threads):
    # --- VERIFY METRIC INCREASED ---
    def check_coord_pending_jobs():
      info_dict = info_modules_to_dict(self.env)
      pending_jobs = int(info_dict[MULTI_THREADING_SECTION][COORD_HIGH_PRIORITY_PENDING_JOBS_METRIC])
      return (pending_jobs == num_commands_per_type), {'pending_jobs': pending_jobs, 'expected': num_commands_per_type}

    wait_for_condition(check_coord_pending_jobs, f"wait_for_coord_pending_jobs_{command_type}")
    # --- RESUME COORD_THREADS ---
    self.env.expect(debug_cmd(), 'COORD_THREADS', 'RESUME').ok()
    # --- WAIT FOR ALL THREADS TO COMPLETE ---
    for t in search_threads:
        t.join(timeout=30)
    # --- VERIFY METRIC DECREASED TO 0 ---
    def check_coord_pending_jobs_reset():
        info_dict = info_modules_to_dict(self.env)
        pending_jobs = int(info_dict[MULTI_THREADING_SECTION][COORD_HIGH_PRIORITY_PENDING_JOBS_METRIC])
        return (pending_jobs == 0), {'pending_jobs': pending_jobs}

    wait_for_condition(check_coord_pending_jobs_reset, f"wait_for_coord_pending_jobs_reset_{command_type}")

  def _test_coord_high_priority_pending_jobs(self, command_type):
    env = self.env
    num_commands_per_type = 3  # Number of commands to execute for each command type

    env.expect(debug_cmd(), 'COORD_THREADS', 'PAUSE').ok()

    search_threads = launch_cmds_in_bg_with_exception_check(self.env, [f'FT.{command_type}', DEFAULT_INDEX_NAME, '*'], num_commands_per_type)
    if search_threads is None:
      env.expect(debug_cmd(), 'COORD_THREADS', 'RESUME').ok()
      return

    self.verify_coord_high_priority_pending_jobs(command_type, num_commands_per_type, search_threads)

  def test_coord_high_priority_pending_jobs_search(self):
    self._test_coord_high_priority_pending_jobs('SEARCH')

  def test_coord_high_priority_pending_jobs_aggregate(self):
    self._test_coord_high_priority_pending_jobs('AGGREGATE')

  def test_coord_high_priority_pending_jobs_cursor(self):
    # Use COUNT parameter with low value so cursor won't be depleted at first execution
    _, cursor_id = self.env.cmd('FT.AGGREGATE', DEFAULT_INDEX_NAME, '*', 'LOAD', '1', '@t', 'WITHCURSOR', 'COUNT', '2')
    self.env.assertNotEqual(cursor_id, 0, message="Cursor should not be depleted")
    num_commands_per_type = 1  # Number of commands to execute for each command type

    self.env.expect(debug_cmd(), 'COORD_THREADS', 'PAUSE').ok()
    search_threads = launch_cmds_in_bg_with_exception_check(self.env, ['FT.CURSOR', 'READ', DEFAULT_INDEX_NAME, cursor_id], num_commands_per_type)
    if search_threads is None:
      self.env.expect(debug_cmd(), 'COORD_THREADS', 'RESUME').ok()
      return

    self.verify_coord_high_priority_pending_jobs('CURSOR', num_commands_per_type, search_threads)
  # Skipping due to a leak in HYBRID queries
  # enable once MOD-12859 is fixed
  def test_coord_high_priority_pending_jobs_hybrid(self):
    raise SkipTest()
    num_commands_per_type = 3  # Number of commands to execute for each command type
    query_vector = np.array([1.0] * self.dim).astype(np.float32).tobytes()

    self.env.expect(debug_cmd(), 'COORD_THREADS', 'PAUSE').ok()

    hybrid_threads = launch_cmds_in_bg_with_exception_check(self.env, ['FT.HYBRID', DEFAULT_INDEX_NAME, 'SEARCH', 'hello',
                                 'VSIM', f'@{DEFAULT_FIELD_NAME}', query_vector], num_commands_per_type)
    if hybrid_threads is None:
      self.env.expect(debug_cmd(), 'COORD_THREADS', 'RESUME').ok()
      return

    self.verify_coord_high_priority_pending_jobs('HYBRID', num_commands_per_type, hybrid_threads)
