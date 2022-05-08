from common import *
from RLTest import Env


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
        data = line.split(':')
        info[section_name][data[0]] = data[1]
  return info


def testInfoModulesBasic(env):
  conn = env.getConnection()

  idx1 = 'idx1'
  idx2 = 'idx2'
  idx3 = 'idx3'

  env.expect('FT.CREATE', idx1, 'STOPWORDS', 3, 'TLV', 'summer', '2020',
                                'SCHEMA', 'title', 'TEXT', 'SORTABLE',
                                          'body', 'TEXT',
                                          'id', 'NUMERIC',
                                          'subject location', 'GEO').ok()

  env.expect('FT.CREATE', idx2, 'LANGUAGE', 'french', 'NOOFFSETS', 'NOFREQS',
                                'PREFIX', 2, 'TLV:', 'S',
                                'SCHEMA', 't1', 'TAG', 'CASESENSITIVE', 'SORTABLE',
                                          'T2', 'AS', 't2', 'TAG',
                                          'id', 'NUMERIC', 'NOINDEX').ok()

  env.expect('FT.CREATE', idx3, 'SCHEMA', 'vec_flat', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '128', 'DISTANCE_METRIC', 'L2',
                                          'vec_hnsw', 'VECTOR', 'HNSW', '14', 'TYPE', 'FLOAT32', 'DIM', '128', 'DISTANCE_METRIC', 'L2',
                                          'INITIAL_CAP', '1000000', 'M', '40', 'EF_CONSTRUCTION', '250', 'EF_RUNTIME', '20').ok()

  info = info_modules_to_dict(conn)
  env.assertEqual(info['search_index']['search_number_of_indexes'], '3')

  fieldsInfo = info['search_fields_statistics']
  env.assertEqual(fieldsInfo['search_fields_text'], 'Text=2,Sortable=1')
  env.assertEqual(fieldsInfo['search_fields_tag'], 'Tag=2,Sortable=1,CaseSensitive=1')
  env.assertEqual(fieldsInfo['search_fields_numeric'], 'Numeric=1,NoIndex=1')
  env.assertEqual(fieldsInfo['search_fields_geo'], 'Geo=1')
  env.assertEqual(fieldsInfo['search_fields_vector'], 'Vector=2,Flat=1,HSNW=1')

  configInfo = info['search_run_time_configs']
  env.assertEqual(configInfo['search_minimal_term_prefix'], '2')
  env.assertEqual(configInfo['search_query_timeout_ms'], '500')
  env.assertEqual(configInfo['search_gc_scan_size'], '100')

  idx1Info = info['search_info_' + idx1]
  env.assertTrue('search_stop_words' in idx1Info)
  env.assertTrue('search_field_4' in idx1Info)
  env.assertEqual(idx1Info['search_field_2'], 'identifier=body,attribute=body,type=TEXT,WEIGHT=1')

  idx2Info = info['search_info_' + idx2]
  env.assertTrue('search_stop_words' not in idx2Info)
  env.assertTrue('prefixes=' in idx2Info['search_index_definition'])
  env.assertTrue('default_language=' in idx2Info['search_index_definition'])
  env.assertEqual(idx2Info['search_field_2'], 'identifier=T2,attribute=t2,type=TAG,SEPARATOR=","')


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

  idx1Info = info['search_info_' + idx1]
  env.assertEqual(idx1Info['search_field_2'], 'identifier=n,attribute=n,type=NUMERIC,NOINDEX=ON')
