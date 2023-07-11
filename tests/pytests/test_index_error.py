from common import getConnectionByEnv, ft_info_to_dict



############################### indexing failures #####################################

def get_field_stats_dict(info_command_output, index = 0):
  field_spec_list = info_command_output['Field statistics'][index]
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

  field_spec_dict = get_field_stats_dict(info)

  env.assertEqual(field_spec_dict['field_indexing_failures'], '1')
  env.assertEqual(field_spec_dict['last_indexing_error_key'], 'doc{1}')
  env.assertEqual(field_spec_dict['last_indexing_error'], 'Could not add vector with blob size 4 (expected size 8)')

  env.assertEqual(info['indexing_failures'], '1')
  env.assertEqual(info['last_indexing_error_key'], 'doc{1}')
  env.assertEqual(info['last_indexing_error'], 'Could not add vector with blob size 4 (expected size 8)')

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

  field_spec_dict = get_field_stats_dict(info)

  env.assertEqual(field_spec_dict['field_indexing_failures'], '1')
  env.assertEqual(field_spec_dict['last_indexing_error_key'], 'doc{1}')
  env.assertEqual(field_spec_dict['last_indexing_error'], 'Invalid numeric value: \'aaaa\'')

  env.assertEqual(info['indexing_failures'], '1')
  env.assertEqual(info['last_indexing_error_key'], 'doc{1}')
  env.assertEqual(info['last_indexing_error'], 'Invalid numeric value: \'aaaa\'')


def test_mixed_index_failures(env):
  con = getConnectionByEnv(env)
  # Create a mixed index.
  env.expect('ft.create', 'idx', 'SCHEMA', 'n', 'numeric', 'v', 'VECTOR', 'FLAT', 6, 'DIM', 2, 'TYPE', 'FLOAT32', 'DISTANCE_METRIC', 'COSINE').ok()

  # Insert two documents, one with a valid numeric and one with an invalid numeric. The invalid numeric is a string.
  # On cluster, both documents should be set in different shards, so the coordinator should get the error from the
  # first document and the second document should be indexed successfully.
  # The index should contain only the valid numeric.
  
  con.execute_command('hset', 'doc{1}', 'n', 'aaaa', 'v', 'aaaaaaaa')
  con.execute_command('hset', 'doc{2}', 'n', '1', 'v', 'aaaaaaaa')

  info = ft_info_to_dict(env, 'idx')
  env.assertEqual(info['num_docs'], '1')

  field_spec_dict = get_field_stats_dict(info, 0)

  env.assertEqual(field_spec_dict['field_indexing_failures'], '1')
  env.assertEqual(field_spec_dict['last_indexing_error_key'], 'doc{1}')
  env.assertEqual(field_spec_dict['last_indexing_error'], 'Invalid numeric value: \'aaaa\'')

  env.assertEqual(info['indexing_failures'], '1')
  env.assertEqual(info['last_indexing_error_key'], 'doc{1}')
  env.assertEqual(info['last_indexing_error'], 'Invalid numeric value: \'aaaa\'')

  con.flushall()
  env.expect('ft.create', 'idx', 'SCHEMA', 'n', 'numeric', 'v', 'VECTOR', 'FLAT', 6, 'DIM', 2, 'TYPE', 'FLOAT32', 'DISTANCE_METRIC', 'COSINE').ok()

  # Insert two documents, one with a valid vector and one with an invalid vector. The invalid vector is a string.
  # On cluster, both documents should be set in different shards, so the coordinator should get the error from the
  # first document and the second document should be indexed successfully.
  # The index should contain only the valid vector.

  con.execute_command('hset', 'doc{1}', 'n', '1', 'v', 'aaaa')
  con.execute_command('hset', 'doc{2}', 'n', '1', 'v', 'aaaaaaaa')

  info = ft_info_to_dict(env, 'idx')
  env.assertEqual(info['num_docs'], '1')

  field_spec_dict = get_field_stats_dict(info, 1)

  env.assertEqual(field_spec_dict['field_indexing_failures'], '1')
  env.assertEqual(field_spec_dict['last_indexing_error_key'], 'doc{1}')
  env.assertEqual(field_spec_dict['last_indexing_error'], 'Could not add vector with blob size 4 (expected size 8)')

  env.assertEqual(info['indexing_failures'], '1')
  env.assertEqual(info['last_indexing_error_key'], 'doc{1}')
  env.assertEqual(info['last_indexing_error'], 'Could not add vector with blob size 4 (expected size 8)')

  


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

    field_spec_dict = get_field_stats_dict(info)

    env.assertEqual(field_spec_dict['field_indexing_failures'], '1')
    env.assertEqual(field_spec_dict['last_indexing_error_key'], 'doc{1}')
    env.assertEqual(field_spec_dict['last_indexing_error'], 'Invalid geo string')

    
    env.assertEqual(info['indexing_failures'], '1')
    env.assertEqual(info['last_indexing_error_key'], 'doc{1}')
    env.assertEqual(info['last_indexing_error'], 'Invalid geo string')

    con.execute_command('hset', 'doc{3}', 'g', '1000,1000')


    info = ft_info_to_dict(env, 'idx')
    env.assertEqual(info['num_docs'], '1')

    field_spec_dict = get_field_stats_dict(info)

    env.assertEqual(field_spec_dict['field_indexing_failures'], '2')
    env.assertEqual(field_spec_dict['last_indexing_error_key'], 'doc{3}')
    env.assertEqual(field_spec_dict['last_indexing_error'], 'Invalid geo coordinates: 1000.000000, 1000.000000')

    env.assertEqual(info['indexing_failures'], '2')
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

#     env.assertEqual(info['indexing_failures'], '1')
#     env.assertEqual(info['last_indexing_error_key'], 'doc{1}')
#     env.assertEqual(info['last_indexing_error'], 'Invalid geoshape string')

###################### JSON failues ######################

def test_vector_indexing_with_json(env):
  con = getConnectionByEnv(env)
  # Create a vector index.
  env.expect('ft.create', 'idx', 'ON', 'JSON', 'SCHEMA', '$.v', 'VECTOR', 'FLAT', 6, 'DIM', 2, 'TYPE', 'FLOAT32', 'DISTANCE_METRIC', 'COSINE').ok()

  # Insert a document with a valid vector as a JSON.
  con.execute_command('JSON.SET', 'doc{1}', '.', '{"v": [1.0, 2.0, 3.0]}')

  info = ft_info_to_dict(env, 'idx')
  env.assertEqual(info['num_docs'], '0')

  field_spec_dict = get_field_stats_dict(info)

  # Important:
  # For the time being, JSON field preprocess is in different code path than the hash field preprocess.
  # Therefore, the JSON field failure statistics are not updated.
  # This test is to make sure that the JSON field failure statistics are updated in the future, when the code paths are merged
  # so it'll break once the good behavior is implemented.
  env.assertEqual(field_spec_dict['field_indexing_failures'], '0')
  env.assertEqual(field_spec_dict['last_indexing_error_key'], 'NA')
  env.assertEqual(field_spec_dict['last_indexing_error'], 'NA')

  env.assertEqual(info['indexing_failures'], '1')
  env.assertEqual(info['last_indexing_error_key'], 'doc{1}')
  env.assertEqual(info['last_indexing_error'], 'Invalid vector length. Expected 2, got 3')


