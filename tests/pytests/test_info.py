from common import getConnectionByEnv, waitForIndex, sortedResults, toSortedFlatList, forceInvokeGC
from RLTest import Env
from time import sleep


def ft_info_to_dict(env, idx):
  res = env.execute_command('ft.info', idx)
  return {res[i]: res[i + 1] for i in range(0, len(res), 2)}

# The output for this test can be used for recreating documentation for `FT.INFO`
def testInfo(env):
  env.skip()
  count = 345678
  conn = env.getConnection()
  pl = conn.pipeline()

  idx = 'wikipedia'

  for i in range(count):
    geo = '1.23456,1.' + str(i / float(count))
    pl.execute_command('HSET', 'doc%d' % i, 'title', 'hello%d' % i,
                                            'body', '%dhello%dworld%dhow%dare%dyou%dtoday%d' % (i, i, i, i, i, i, i),
                                            'n', i / 17.0,
                                            'geo', geo)
    if i % 10000 == 0:
      pl.execute()
  pl.execute()

  env.expect('FT.CREATE', idx, 'STOPWORDS', 3, 'TLV', 'summer', '2020',
                               'SCHEMA', 'title', 'TEXT', 'SORTABLE',
                                         'body', 'TEXT',
                                         'id', 'NUMERIC',
                                         'subject location', 'GEO').ok()

  waitForIndex(env, idx)

  for i in range(count):
    pl.execute_command('DEL', 'doc%d' % i)
    if i % 10000 == 0:
      pl.execute()
      forceInvokeGC(env, idx)
  pl.execute()

  #  GC stats
  for i in range(25):
    forceInvokeGC(env, idx)

  # cursor stats
  #query = ['FT.AGGREGATE', idx, '*', 'WITHCURSOR']
  #res = env.cmd(*query)
  #env.cmd('FT.CURSOR', 'READ', idx, str(res[1]))

  #print info


def test_numeric_info(env):

  env.execute_command('ft.create', 'idx1', 'SCHEMA', 'n', 'numeric')
  env.execute_command('ft.create', 'idx2', 'SCHEMA', 'n', 'numeric', 'SORTABLE')
  env.execute_command('ft.create', 'idx3', 'SCHEMA', 'n', 'numeric', 'SORTABLE', 'UNF')
  env.execute_command('ft.create', 'idx4', 'SCHEMA', 'n', 'numeric', 'SORTABLE', 'NOINDEX')
  env.execute_command('ft.create', 'idx5', 'SCHEMA', 'n', 'numeric', 'SORTABLE', 'UNF', 'NOINDEX')

  res1 = ft_info_to_dict(env, 'idx1')['attributes']
  res2 = ft_info_to_dict(env, 'idx2')['attributes']
  res3 = ft_info_to_dict(env, 'idx3')['attributes']
  res4 = ft_info_to_dict(env, 'idx4')['attributes']
  res5 = ft_info_to_dict(env, 'idx5')['attributes']

  exp1 = [['identifier', 'n', 'attribute', 'n', 'type', 'NUMERIC']]
  exp2 = [['identifier', 'n', 'attribute', 'n', 'type', 'NUMERIC', 'SORTABLE', 'UNF']]
  exp3 = [['identifier', 'n', 'attribute', 'n', 'type', 'NUMERIC', 'SORTABLE', 'UNF', 'NOINDEX']]

  env.assertEqual(res1, exp1)  # Nothing special about the numeric field
  env.assertEqual(res2, exp2)  # Numeric field is sortable, and automatically UNF
  env.assertEqual(res3, exp2)  # Numeric field is sortable, and explicitly UNF
  env.assertEqual(res4, exp3)  # Numeric field is sortable, explicitly NOINDEX, and automatically UNF
  env.assertEqual(res5, exp3)  # Numeric field is sortable, explicitly NOINDEX, and explicitly UNF


############################### indexing failures #####################################

def get_field_spec_dict(info_command_output):
  field_spec_list = info_command_output['attributes'][0]
  return {field_spec_list[i]: field_spec_list[i + 1] for i in range(0, len(field_spec_list), 2)}

def test_vector_index_failures(env):
  con = getConnectionByEnv(env)
  # Create a vector index.
  env.expect('ft.create', 'idx', 'SCHEMA', 'v', 'VECTOR', 'FLAT', 6, 'DIM', 2, 'TYPE', 'FLOAT32', 'DISTANCE_METRIC', 'COSINE').ok()

  # Insert two documents, one with a valid vector and one with an invalid vector. The invalid vector is too short.
  # On cluster, both documents should be set in different shards, so the coordinator should get the error from the
  # first document and the second document should be indexed successfully.
  # The index should contain only the valid vector.

  con.execute_command('hset', 'doc{1}', 'v', 'aaaa')
  con.execute_command('hset', 'doc{2}', 'v', 'aaaaaaaa')

  info = ft_info_to_dict(env, 'idx')
  env.assertEqual(info['num_docs'], '1')

  field_spec_dict = get_field_spec_dict(info)

  env.assertEqual(field_spec_dict['field_indexing_failures'], '1')
  env.assertEqual(field_spec_dict['last_indexing_error_key'], 'doc{1}')
  env.assertEqual(field_spec_dict['last_indexing_error'], 'Could not add vector with blob size 4 (expected size 8)')

  env.assertEqual(info['hash_indexing_failures'], '1')
  env.assertEqual(info['last_indexing_error_key'], 'doc{1}')
  env.assertEqual(info['last_indexing_error'], 'Could not add vector with blob size 4 (expected size 8)')

def test_vector_indexing_with_json(env):
  con = getConnectionByEnv(env)
  # Create a vector index.
  env.expect('ft.create', 'idx', 'ON', 'JSON', 'SCHEMA', '$.v', 'VECTOR', 'FLAT', 6, 'DIM', 2, 'TYPE', 'FLOAT32', 'DISTANCE_METRIC', 'COSINE').ok()

  # Insert a document with a valid vector as a JSON.
  con.execute_command('JSON.SET', 'doc{1}', '.', '{"v": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]}')

  info = ft_info_to_dict(env, 'idx')
  env.assertEqual(info['num_docs'], '1')

  field_spec_dict = get_field_spec_dict(info)

  env.assertEqual(field_spec_dict['field_indexing_failures'], '0')
  env.assertEqual(field_spec_dict['last_indexing_error_key'], '')
  env.assertEqual(field_spec_dict['last_indexing_error'], '')

  env.assertEqual(info['hash_indexing_failures'], '0')
  env.assertEqual(info['last_indexing_error_key'], '')
  env.assertEqual(info['last_indexing_error'], '')

def test_numeric_index_failures(env):
  con = getConnectionByEnv(env)
  # Create a numeric index.
  env.expect('ft.create', 'idx', 'SCHEMA', 'n', 'numeric').ok()

  # Insert two documents, one with a valid numeric and one with an invalid numeric. The invalid numeric is a string.
  # On cluster, both documents should be set in different shards, so the coordinator should get the error from the
  # first document and the second document should be indexed successfully.
  # The index should contain only the valid numeric.

  con.execute_command('hset', 'doc{1}', 'n', 'aaaa')
  con.execute_command('hset', 'doc{2}', 'n', '1')

  info = ft_info_to_dict(env, 'idx')
  env.assertEqual(info['num_docs'], '1')

  field_spec_dict = get_field_spec_dict(info)

  env.assertEqual(field_spec_dict['field_indexing_failures'], '1')
  env.assertEqual(field_spec_dict['last_indexing_error_key'], 'doc{1}')
  env.assertEqual(field_spec_dict['last_indexing_error'], 'Invalid numeric value: \'aaaa\'')

  env.assertEqual(info['hash_indexing_failures'], '1')
  env.assertEqual(info['last_indexing_error_key'], 'doc{1}')
  env.assertEqual(info['last_indexing_error'], 'Invalid numeric value: \'aaaa\'')


def test_geo_index_failures(env):
    con = getConnectionByEnv(env)
    # Create a geo index.
    env.expect('ft.create', 'idx', 'SCHEMA', 'g', 'geo').ok()

    # Insert two documents, one with a valid geo and one with an invalid geo. The invalid geo is a string.
    # On cluster, both documents should be set in different shards, so the coordinator should get the error from the
    # first document and the second document should be indexed successfully.
    # The index should contain only the valid geo.

    con.execute_command('hset', 'doc{1}', 'g', 'aaaa')
    con.execute_command('hset', 'doc{2}', 'g', '1,1')

    info = ft_info_to_dict(env, 'idx')
    env.assertEqual(info['num_docs'], '1')

    field_spec_dict = get_field_spec_dict(info)

    env.assertEqual(field_spec_dict['field_indexing_failures'], '1')
    env.assertEqual(field_spec_dict['last_indexing_error_key'], 'doc{1}')
    env.assertEqual(field_spec_dict['last_indexing_error'], 'Invalid geo string')

    
    env.assertEqual(info['hash_indexing_failures'], '1')
    env.assertEqual(info['last_indexing_error_key'], 'doc{1}')
    env.assertEqual(info['last_indexing_error'], 'Invalid geo string')

    con.execute_command('hset', 'doc{3}', 'g', '1000,1000')


    info = ft_info_to_dict(env, 'idx')
    env.assertEqual(info['num_docs'], '1')

    field_spec_dict = get_field_spec_dict(info)

    env.assertEqual(field_spec_dict['field_indexing_failures'], '2')
    env.assertEqual(field_spec_dict['last_indexing_error_key'], 'doc{3}')
    env.assertEqual(field_spec_dict['last_indexing_error'], 'Invalid geo coordinates: 1000.000000, 1000.000000')

    env.assertEqual(info['hash_indexing_failures'], '2')
    env.assertEqual(info['last_indexing_error_key'], 'doc{3}')
    env.assertEqual(info['last_indexing_error'], 'Invalid geo coordinates: 1000.000000, 1000.000000')

# TODO: Talk with Omer about this test

# def test_geoshape_index_failures(env):
#     con = getConnectionByEnv(env)
#     # Create a geoshape index.

#     env.expect('FT.CREATE', 'idx', 'SCHEMA', 'geom', 'GEOSHAPE', 'FLAT').ok()
  
#     con.execute_command('HSET', 'doc{1}', 'geom', 'POLIKON(()())')
#     con.execute_command('HSET', 'doc{2}', 'geom', 'POLYGON((0 0, 1 1, 2 2, 0 0))')

#     info = ft_info_to_dict(env, 'idx')
#     env.assertEqual(info['num_docs'], '1')

#     field_spec_dict = get_field_spec_dict(info)

#     env.assertEqual(field_spec_dict['field_indexing_failures'], '1')
#     env.assertEqual(field_spec_dict['last_indexing_error_key'], 'doc{1}')
#     env.assertEqual(field_spec_dict['last_indexing_error'], 'Invalid geoshape string')

#     env.assertEqual(info['hash_indexing_failures'], '1')
#     env.assertEqual(info['last_indexing_error_key'], 'doc{1}')
#     env.assertEqual(info['last_indexing_error'], 'Invalid geoshape string')




