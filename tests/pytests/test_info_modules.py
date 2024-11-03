from common import *
from RLTest import Env
import redis

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


def testInfoModulesBasic(env):
  conn = env.getConnection()

  idx1 = 'idx1'
  idx2 = 'idx2'
  idx3 = 'idx3'

  env.expect('FT.CREATE', idx1, 'STOPWORDS', 3, 'TLV', 'summer', '2020',
                                'SCHEMA', 'title', 'TEXT', 'SORTABLE',
                                          'body', 'TEXT', 'NOINDEX',
                                          'id', 'NUMERIC',
                                          'subject location', 'GEO').ok()

  env.expect('FT.CREATE', idx2, 'LANGUAGE', 'french', 'NOOFFSETS', 'NOFREQS',
                                'PREFIX', 2, 'TLV:', 'NY:',
                                'SCHEMA', 't1', 'TAG', 'CASESENSITIVE', 'SORTABLE',
                                          'T2', 'AS', 't2', 'TAG',
                                          'id', 'NUMERIC', 'NOINDEX').ok()

  env.expect('FT.CREATE', idx3, 'SCHEMA', 'vec_flat', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '128', 'DISTANCE_METRIC', 'L2',
                                          'vec_hnsw', 'VECTOR', 'HNSW', '14', 'TYPE', 'FLOAT32', 'DIM', '128', 'DISTANCE_METRIC', 'L2',
                                          'INITIAL_CAP', '10000', 'M', '40', 'EF_CONSTRUCTION', '250', 'EF_RUNTIME', '20').ok()

  info = info_modules_to_dict(conn)
  env.assertEqual(info['search_index']['search_number_of_indexes'], '3')

  fieldsInfo = info['search_fields_statistics']
  env.assertEqual(fieldsInfo['search_fields_text'], 'Text=2,Sortable=1,NoIndex=1')
  env.assertEqual(fieldsInfo['search_fields_tag'], 'Tag=2,Sortable=1,CaseSensitive=1')
  env.assertEqual(fieldsInfo['search_fields_numeric'], 'Numeric=2,NoIndex=1')
  env.assertEqual(fieldsInfo['search_fields_geo'], 'Geo=1')
  env.assertEqual(fieldsInfo['search_fields_vector'], 'Vector=2,Flat=1,HNSW=1')

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
  env.expect('FT.ALTER', idx1, 'SCHEMA', 'ADD', 'n', 'NUMERIC', 'NOINDEX').ok()

  info = info_modules_to_dict(conn)
  env.assertEqual(info['search_index']['search_number_of_indexes'], '1')

  fieldsInfo = info['search_fields_statistics']
  env.assertEqual(fieldsInfo['search_fields_text'], 'Text=1,Sortable=1')
  env.assertEqual(fieldsInfo['search_fields_numeric'], 'Numeric=1,NoIndex=1')

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
  env.assertEqual(fieldsInfo['search_fields_text'], 'Text=2,Sortable=1')
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
    env.assertEqual(fieldsInfo['search_fields_numeric'], 'Numeric=1,Sortable=1')
    env.assertEqual(fieldsInfo['search_fields_geo'], 'Geo=1,Sortable=1,NoIndex=1')
    env.assertEqual(fieldsInfo['search_fields_tag'], 'Tag=1,NoIndex=1')

def test_redis_info():
  """Tests that the Redis `INFO` command works as expected"""

  env = Env(moduleArgs='DEFAULT_DIALECT 2')
  conn = getConnectionByEnv(env)

  # Create an index
  env.cmd('FT.CREATE', 'idx', 'SCHEMA', 'txt', 'TEXT', 'tag', 'TAG', 'SORTABLE')

  # Add some data
  n_docs = 10000
  for i in range(n_docs):
    conn.execute_command('HSET', f'h{i}', 'txt', f'hello{i}', 'tag', 'tag{i}')

  # Call `INFO` and check that the index is there
  res = env.cmd('INFO', 'MODULES')

  env.assertEqual(res['search_number_of_indexes'], 1)
  # amanzonlinux:2 install redis version '5.1.0a1' which has different output
  if redis.__version__ >= '5.0.5' and redis.__version__ != '5.1.0a1':
    env.assertEqual(res['search_fields_text']['Text'], 1)
  else:
    env.assertEqual(res['search_fields_text'], 'Text=1')
  
  env.assertEqual(res['search_fields_tag']['Tag'], 1)
  env.assertEqual(res['search_fields_tag']['Sortable'], 1)
  env.assertGreater(res['search_used_memory_indexes'], 0)
  env.assertGreater(res['search_used_memory_indexes_human'], 0)
  env.assertGreater(res['search_min_memory_index'], 0)
  env.assertGreater(res['search_min_memory_index_human'], 0)
  env.assertGreater(res['search_max_memory_index'], 0)
  env.assertGreater(res['search_max_memory_index_human'], 0)
  # env.assertGreater(res['search_total_indexing_time'], 0)   # Introduces flakiness
  env.assertEqual(res['search_global_idle'], 0)
  env.assertEqual(res['search_global_total'], 0)
  env.assertEqual(res['search_bytes_collected'], 0)
  env.assertEqual(res['search_total_cycles'], 0)
  env.assertEqual(res['search_total_ms_run'], 0)
  env.assertEqual(res['search_dialect_1'], 0)
  env.assertEqual(res['search_dialect_2'], 0)
  env.assertEqual(res['search_dialect_3'], 0)
  env.assertEqual(res['search_dialect_4'], 0)

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
  forceInvokeGC(env, 'idx')

  # Wait for GC to finish
  time.sleep(2)

  # Call `INFO` and check that the data is updated accordingly
  res = env.cmd('INFO', 'MODULES')
  env.assertGreater(res['search_bytes_collected'], 0)
  env.assertGreater(res['search_total_cycles'], 0)
  env.assertGreater(res['search_total_ms_run'], 0)
