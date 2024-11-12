from common import getConnectionByEnv, index_info, to_dict



# String constants for the info command output.

indexing_failures_str = 'indexing failures'
last_indexing_error_key_str = 'last indexing error key'
last_indexing_error_str = 'last indexing error'
index_errors_str = 'Index Errors'

def get_field_stats_dict(info_command_output, index = 0):
  return to_dict(info_command_output['field statistics'][index])

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

  for _ in env.reloadingIterator():
    info = index_info(env)
    env.assertEqual(int(info['num_docs']), 1)

    expected_error_dict = {
                            indexing_failures_str: 1,
                            last_indexing_error_str: 'Could not add vector with blob size 4 (expected size 8)',
                            last_indexing_error_key_str: 'doc{1}'
                          }

    field_spec_dict = get_field_stats_dict(info)
    error_dict = to_dict(field_spec_dict["Index Errors"])
    env.assertEqual(error_dict, expected_error_dict)

    error_dict = to_dict(info["Index Errors"])
    env.assertEqual(error_dict, expected_error_dict)


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

  for _ in env.reloadingIterator():
    info = index_info(env)
    env.assertEqual(int(info['num_docs']), 1)

    expected_error_dict = {
                            indexing_failures_str: 1,
                            last_indexing_error_str: 'Invalid numeric value: \'aaaa\'',
                            last_indexing_error_key_str: 'doc{1}'
                          }


    field_spec_dict = get_field_stats_dict(info)
    error_dict = to_dict(field_spec_dict["Index Errors"])

    env.assertEqual(error_dict, expected_error_dict)

    error_dict = to_dict(info["Index Errors"])
    env.assertEqual(error_dict, expected_error_dict)


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

  for _ in env.reloadingIterator():
    info = index_info(env)
    env.assertEqual(int(info['num_docs']), 1)

    expected_error_dict = {
                            indexing_failures_str: 1,
                            last_indexing_error_str: 'Invalid numeric value: \'aaaa\'',
                            last_indexing_error_key_str: 'doc{1}'
                          }

    field_spec_dict = get_field_stats_dict(info, 0)
    error_dict = to_dict(field_spec_dict["Index Errors"])
    env.assertEqual(error_dict, expected_error_dict)

    error_dict = to_dict(info["Index Errors"])
    env.assertEqual(error_dict, expected_error_dict)

  con.flushall()
  env.expect('ft.create', 'idx', 'SCHEMA', 'n', 'numeric', 'v', 'VECTOR', 'FLAT', 6, 'DIM', 2, 'TYPE', 'FLOAT32', 'DISTANCE_METRIC', 'COSINE').ok()

  # Insert two documents, one with a valid vector and one with an invalid vector. The invalid vector is a string.
  # On cluster, both documents should be set in different shards, so the coordinator should get the error from the
  # first document and the second document should be indexed successfully.
  # The index should contain only the valid vector.

  con.execute_command('hset', 'doc{1}', 'n', '1', 'v', 'aaaa')
  con.execute_command('hset', 'doc{2}', 'n', '1', 'v', 'aaaaaaaa')

  for _ in env.reloadingIterator():
    info = index_info(env)
    env.assertEqual(int(info['num_docs']), 1)

    expected_error_dict = {
                            indexing_failures_str: 1,
                            last_indexing_error_str: 'Could not add vector with blob size 4 (expected size 8)',
                            last_indexing_error_key_str: 'doc{1}'
                          }

    field_spec_dict = get_field_stats_dict(info, 1)
    error_dict = to_dict(field_spec_dict["Index Errors"])

    env.assertEqual(error_dict, expected_error_dict)

    error_dict = to_dict(info["Index Errors"])
    env.assertEqual(error_dict, expected_error_dict)


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

  for _ in env.reloadingIterator():
    info = index_info(env)
    env.assertEqual(int(info['num_docs']), 1)

    expected_error_dict = {
                            indexing_failures_str: 1,
                            last_indexing_error_str: 'Invalid geo string',
                            last_indexing_error_key_str: 'doc{1}'
                          }

    field_spec_dict = get_field_stats_dict(info)
    error_dict = to_dict(field_spec_dict["Index Errors"])

    env.assertEqual(error_dict, expected_error_dict)

    error_dict = to_dict(info["Index Errors"])
    env.assertEqual(error_dict, expected_error_dict)

  con.flushall()

  env.expect('ft.create', 'idx', 'SCHEMA', 'g', 'geo').ok()

  # Insert two documents, one with a valid geo and one with an invalid geo. The invalid geo is a string.
  # On cluster, both documents should be set in different shards, so the coordinator should get the error from the
  # first document and the second document should be indexed successfully.
  # The index should contain only the valid geo.


  con.execute_command('hset', 'doc{1}', 'g', '1000,1000')
  con.execute_command('hset', 'doc{2}', 'g', '1,1')

  expected_error_dict = {
                          indexing_failures_str: 1,
                          last_indexing_error_str: 'Invalid geo coordinates: 1000.000000, 1000.000000',
                          last_indexing_error_key_str: 'doc{1}'
                        }

  for _ in env.reloadingIterator():
    info = index_info(env)
    env.assertEqual(int(info['num_docs']), 1)

    field_spec_dict = get_field_stats_dict(info)
    error_dict = to_dict(field_spec_dict["Index Errors"])

    env.assertEqual(error_dict, expected_error_dict)

    error_dict = to_dict(info["Index Errors"])
    env.assertEqual(error_dict, expected_error_dict)


# TODO: Talk with Omer about this test

# def test_geoshape_index_failures(env):
#   con = getConnectionByEnv(env)
#   # Create a geoshape index.

#   env.expect('FT.CREATE', 'idx', 'SCHEMA', 'geom', 'GEOSHAPE', 'FLAT').ok()

#   con.execute_command('HSET', 'doc{1}', 'geom', 'POLIKON(()())')
#   con.execute_command('HSET', 'doc{2}', 'geom', 'POLYGON((0 0, 1 1, 2 2, 0 0))')

#   for _ in env.reloadingIterator():
#     info = index_info(env)
#     env.assertEqual(int(info['num_docs']), 2)

#     field_spec_dict = get_field_stats_dict(info)

#     env.assertEqual(field_spec_dict['indexing failures'], '1')
#     env.assertEqual(field_spec_dict['last indexing error key'], 'doc{1}')
#     env.assertEqual(field_spec_dict['last indexing error'], 'Invalid geoshape string')

#     env.assertEqual(info['indexing_failures'], '1')
#     env.assertEqual(info['last indexing error key'], 'doc{1}')
#     env.assertEqual(info['last indexing error'], 'Invalid geoshape string')

def test_partial_doc_index_failures(env):
  # Create an index with a text field as the first field and a numeric field as the second field.
  env.expect('ft.create', 'idx', 'SCHEMA', 't', 'text', 'n', 'numeric').ok()
  # Create a document with no text field and an invalid numeric field.
  env.expect('HSET', 'doc', 'n', 'banana').equal(1)

  expected_text_stats = ['identifier', 't', 'attribute', 't', 'Index Errors',
                         ['indexing failures', 0, 'last indexing error', 'N/A', 'last indexing error key', 'N/A']]
  excepted_numeric_stats = ['identifier', 'n', 'attribute', 'n', 'Index Errors',
                            ['indexing failures', 1, 'last indexing error', "Invalid numeric value: 'banana'", 'last indexing error key', 'doc']]
  for _ in env.reloadingIterator():
    info = index_info(env)
    env.assertEqual(int(info['num_docs']), 0)
    env.assertEqual(info['field statistics'][0], expected_text_stats)
    env.assertEqual(info['field statistics'][1], excepted_numeric_stats)

def test_multiple_index_failures(env):
    # Create 2 indices with a different schema order.
    env.expect('ft.create', 'idx1', 'SCHEMA', 'n1', 'numeric', 'n2', 'numeric').ok()
    env.expect('ft.create', 'idx2', 'SCHEMA', 'n2', 'numeric', 'n1', 'numeric').ok()

    # Create a document with two fields containing invalid numeric values.
    env.expect('HSET', 'doc', 'n1', 'banana', 'n2', 'meow').equal(2)

    index_to_errors_strings = {'idx1': 'banana', 'idx2': 'meow'}
    for _ in env.reloadingIterator():

      for idx in ['idx1', 'idx2']:
        info = index_info(env, idx)

        expected_error_dict = {
            indexing_failures_str: 1,
            last_indexing_error_str: f"Invalid numeric value: '{index_to_errors_strings[idx]}'",
            last_indexing_error_key_str: 'doc'
        }

        # Both indices contain one error for the same document.
        error_dict = to_dict(info["Index Errors"])
        env.assertEqual(error_dict, expected_error_dict)


        # Each index failed to index the doc due to the first failing field in the schema.
        index_to_failed_field = {'idx1': 'n1', 'idx2': 'n2'}
        index_to_ok_field = {'idx1': 'n2', 'idx2': 'n1'}
        expected_failed_field_stats = [
            'identifier', index_to_failed_field[idx], 'attribute', index_to_failed_field[idx], 'Index Errors',
            ['indexing failures', 1, 'last indexing error',
            f"Invalid numeric value: '{index_to_errors_strings[idx]}'",
            'last indexing error key', 'doc']
        ]

        expected_no_error_field_stats = [
            'identifier', index_to_ok_field[idx], 'attribute', index_to_ok_field[idx], 'Index Errors',
            ['indexing failures', 0, 'last indexing error', 'N/A', 'last indexing error key', 'N/A']
        ]

        env.assertEqual(info['num_docs'], 0)
        env.assertEqual(info['field statistics'][0], expected_failed_field_stats)
        env.assertEqual(info['field statistics'][1], expected_no_error_field_stats)


###################### JSON failures ######################

def test_vector_indexing_with_json(env):
  con = getConnectionByEnv(env)
  # Create a vector index.
  env.expect('ft.create', 'idx', 'ON', 'JSON', 'SCHEMA', '$.v', 'VECTOR', 'FLAT', 6, 'DIM', 2, 'TYPE', 'FLOAT32', 'DISTANCE_METRIC', 'COSINE').ok()

  # Insert a document with a valid but too long vector as a JSON.
  con.execute_command('JSON.SET', 'doc{1}', '.', '{"v": [1.0, 2.0, 3.0]}')

  for _ in env.reloadingIterator():
    info = index_info(env)
    env.assertEqual(int(info['num_docs']), 0)

    expected_error_dict = {
                            indexing_failures_str: 1,
                            last_indexing_error_str: 'Invalid vector length. Expected 2, got 3',
                            last_indexing_error_key_str: 'doc{1}'
                          }

    field_spec_dict = get_field_stats_dict(info)
    field_error_dict = to_dict(field_spec_dict["Index Errors"])
    env.assertEqual(field_error_dict, expected_error_dict)

    error_dict = to_dict(info["Index Errors"])
    env.assertEqual(error_dict, expected_error_dict)

@skip(no_json=True)
def test_multiple_index_failures_json(env):
    # Create 2 indices with a different schema order.
    env.expect('ft.create', 'idx1', 'ON', 'JSON', 'SCHEMA', '$.n1', 'AS', 'n1', 'numeric', '$.n2', 'AS', 'n2', 'numeric').ok()
    env.expect('ft.create', 'idx2', 'ON', 'JSON', 'SCHEMA', '$.n2', 'AS', 'n2', 'numeric', '$.n1', 'AS', 'n1', 'numeric').ok()

    # Create a document with two fields containing invalid numeric values.
    json_val = r'{"n1":"banana","n2":"meow"}'
    env.expect('JSON.SET', 'doc', '$', json_val).ok()

    for _ in env.reloadingIterator():
      for idx in ['idx1', 'idx2']:
          info = index_info(env, idx)

          expected_error_dict = {
              indexing_failures_str: 1,
              last_indexing_error_str: f"Invalid JSON type: String type can represent only TEXT, TAG, GEO or GEOMETRY field",
              last_indexing_error_key_str: 'doc'
          }

          # Both indices contain one error for the same document.
          error_dict = to_dict(info["Index Errors"])
          env.assertEqual(error_dict, expected_error_dict)


          # Each index failed to index the doc due to the first failing field in the schema.
          index_to_failed_field = {'idx1': 'n1', 'idx2': 'n2'}
          index_to_ok_field = {'idx1': 'n2', 'idx2': 'n1'}
          expected_failed_field_stats = [
              'identifier', f"$.{index_to_failed_field[idx]}", 'attribute', index_to_failed_field[idx], 'Index Errors',
              ['indexing failures', 1, 'last indexing error',
              f"Invalid JSON type: String type can represent only TEXT, TAG, GEO or GEOMETRY field",
              'last indexing error key', 'doc']
          ]

          expected_no_error_field_stats = [
              'identifier', f"$.{index_to_ok_field[idx]}", 'attribute', index_to_ok_field[idx], 'Index Errors',
              ['indexing failures', 0, 'last indexing error', 'N/A', 'last indexing error key', 'N/A']
          ]

          env.assertEqual(info['num_docs'], 0)
          env.assertEqual(info['field statistics'][0], expected_failed_field_stats)
          env.assertEqual(info['field statistics'][1], expected_no_error_field_stats)
