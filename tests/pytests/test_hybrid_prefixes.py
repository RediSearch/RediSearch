from common import *

@skip(cluster=False, min_shards=2)
def test_hybrid_incompatibleIndex(env):
    """Tests that we get an error if we try to query an index with a different
    schema than the one used in the query"""

    # Connect to two shards
    first_conn = env.getConnection(0)
    second_conn = env.getConnection(1)

    # Create an index
    index_name = 'idx'
    env.expect(f'FT.CREATE {index_name} PREFIX 1 h: SCHEMA description TEXT embedding VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2').ok

    def modify_index(conn, index_name, prefixes):
        # Promote the connection to an internal one, such that we can execute internal (shard-local) commands
        conn.execute_command('DEBUG', 'MARK-INTERNAL-CLIENT')
        # Connect to a shard, and create an index with a different schema, but
        # the same name
        res = conn.execute_command('_FT.DROPINDEX', index_name)
        env.assertEqual(res, 'OK')
        res = conn.execute_command('_FT.CREATE', index_name, 'PREFIX', len(prefixes), *prefixes, 'SCHEMA', 'description', 'TEXT', 'embedding', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2')
        env.assertEqual(res, 'OK')

    modify_index(first_conn, index_name, ['k:'])

    # Query via the cluster connection, such that we will get the mismatch error
    commands = [
        ['FT.HYBRID', index_name, 'SEARCH', 'text', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', '0']
    ]

    # Run commands on second shard (different index prefixes -> error)
    for command in commands:
        try:
            second_conn.execute_command(*command)
            env.assertTrue(False)
        except Exception as e:
            env.assertContains("Index mismatch: Shard index is different than queried index", str(e))

    # Also for an index with a different amount of prefixes
    modify_index(first_conn, index_name, ['h:', 'k:'])
    # Run commands on second shard (different index prefixes -> error)
    for command in commands:
        try:
            second_conn.execute_command(*command)
            env.assertTrue(False)
        except Exception as e:
            env.assertContains("Index mismatch: Shard index is different than queried index", str(e))


@skip(cluster=False, min_shards=2)
def test_hybrid_compatibleIndex(env):
    """Tests that we get results when querying an index with compatible prefixes across shards"""

    # Connect to two shards
    first_conn = env.getConnection(0)
    second_conn = env.getConnection(1)

    # Create an index with compatible prefixes across shards
    index_name = 'idx'
    env.expect(f'FT.CREATE {index_name} PREFIX 1 h: SCHEMA description TEXT embedding VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2').ok

    # Add test data to both shards using cluster connection
    conn = env.getClusterConnectionIfNeeded()

    # Add documents with h: prefix that should be indexed
    test_docs = [
        ('h:doc:1', 'red shoes', [0.0, 0.0]),
        ('h:doc:2', 'blue running shoes', [1.0, 0.0]),
        ('h:doc:3', 'running gear', [0.0, 1.0]),
        ('h:doc:4', 'green shoes', [1.0, 1.0])
    ]

    for doc_id, description, vector in test_docs:
        vector_data = create_np_array_typed(vector, 'FLOAT32')
        conn.execute_command('HSET', doc_id, 'description', description, 'embedding', vector_data.tobytes())

    # Query vector for similarity search
    query_vector = create_np_array_typed([0.5, 0.5], 'FLOAT32').tobytes()

    # Test hybrid queries that should succeed with compatible indices
    hybrid_commands = [
        ['FT.HYBRID', index_name, 'SEARCH', 'shoes', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector],
        ['FT.HYBRID', index_name, 'SEARCH', 'running', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector],
        ['FT.HYBRID', index_name, 'SEARCH', '*', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', query_vector]
    ]

    # Execute queries and verify they return results without errors
    for command in hybrid_commands:
      # Use env.cmd instead of conn.execute_command for cluster compatibility
      response = env.cmd(*command)
      # Verify we get a valid response structure
      env.assertTrue(isinstance(response, list))
      env.assertTrue(len(response) >= 4)  # Should have format, results, etc.

      # Extract results using the common utility function
      results, count = get_results_from_hybrid_response(response)

      # Verify we get some results (at least one document should match)
      env.assertGreater(count, 0)
      env.assertEqual(count, len(results.keys()))

      # Verify all returned documents have the expected prefix
      for doc_key in results.keys():
          env.assertTrue(doc_key.startswith('h:doc:'))
