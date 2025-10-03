from common import *
from includes import *


def setup_hybrid_test_data(env):
    """Setup test data based on the provided scenario"""
    # Create index with text and vector fields
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               'description', 'TEXT',
               'embedding', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()

    # Add test documents with embeddings
    conn = getConnectionByEnv(env)
    conn.execute_command('HSET', 'doc:1{hash_tag}', 'description', 'red shoes',
                        'embedding', create_np_array_typed([0.0, 0.0], 'FLOAT32').tobytes())
    conn.execute_command('HSET', 'doc:2{hash_tag}', 'description', 'red running shoes',
                        'embedding', create_np_array_typed([1.0, 0.0], 'FLOAT32').tobytes())
    conn.execute_command('HSET', 'doc:3{hash_tag}', 'description', 'running gear',
                        'embedding', create_np_array_typed([0.0, 1.0], 'FLOAT32').tobytes())
    conn.execute_command('HSET', 'doc:4{hash_tag}', 'description', 'blue shoes',
                        'embedding', create_np_array_typed([1.0, 1.0], 'FLOAT32').tobytes())

    # Mark as internal client for _FT.HYBRID command
    env.cmd('DEBUG', 'MARK-INTERNAL-CLIENT')


def read_cursor_completely_resp3(env, index_name, cursor_id, batch_callback=None):
    """Read all results from a cursor and return them (RESP 3 format)

    Args:
        env: Test environment
        index_name: Name of the index
        cursor_id: Cursor ID to read from
        batch_callback: Optional function called for each batch with (batch_results, cursor_response)

    Returns:
        list: All results from the cursor as dicts with '__key' and optionally 'score' fields
    """
    if cursor_id == 0:
        return []

    all_results = []
    current_cursor = cursor_id

    while current_cursor != 0:
        cursor_response = env.cmd('FT.CURSOR', 'READ', index_name, current_cursor)
        # RESP 3 format: [{'results': [...], ...}, cursor_id]
        results_dict = cursor_response[0]
        current_cursor = cursor_response[1]
        batch_results = results_dict['results']

        # Call batch callback if provided
        if batch_callback:
            batch_callback(batch_results, cursor_response)

        # Extract document keys and scores from cursor results
        for result in batch_results:
            score = result.get('score')
            key = result.get('extra_attributes', {}).get('__key', '')
            all_results.append({
                        'key': key,
                        'score': score
                    })

    return sorted(all_results, key=lambda x: x['key'] if isinstance(x, dict) else x)


def read_cursor_completely_resp2(env, index_name, cursor_id, batch_callback=None):
    """Read all results from a cursor and return them (RESP 2 format)

    Args:
        env: Test environment
        index_name: Name of the index
        cursor_id: Cursor ID to read from
        batch_callback: Optional function called for each batch with (batch_results, cursor_response)

    Returns:
        list: All results from the cursor as document key strings (RESP 2 doesn't include scores)
    """
    if cursor_id == 0:
        return []

    all_results = []
    current_cursor = cursor_id

    while current_cursor != 0:
        cursor_response = env.cmd('FT.CURSOR', 'READ', index_name, current_cursor)

        # RESP 2 format: [[count, result1, result2, ...], next_cursor_id]
        results_array = cursor_response[0]
        current_cursor = cursor_response[1]
        batch_results = results_array[1:]  # Skip the count at index 0

        # Call batch callback if provided
        if batch_callback:
            batch_callback(batch_results, cursor_response)

        # Extract document keys from cursor results (RESP 2 doesn't include scores)
        for result in batch_results:
            result_dict = dict(zip(result[::2], result[1::2]))
            key = result_dict.get('__key')
            if key is not None:
                all_results.append(key)

    return sorted(all_results)


def read_cursor_completely(env, index_name, cursor_id, batch_callback=None):
    """Read all results from a cursor and return them (auto-detect RESP format)

    Args:
        env: Test environment
        index_name: Name of the index
        cursor_id: Cursor ID to read from
        batch_callback: Optional function called for each batch with (batch_results, cursor_response)

    Returns:
        list: All results from the cursor as dicts with '__key' and optionally 'score' fields
    """
    # Use RESP 3 by default since that's what most tests use
    if hasattr(env, 'protocol') and env.protocol == 2:
        return read_cursor_completely_resp2(env, index_name, cursor_id, batch_callback)
    else:
        return read_cursor_completely_resp3(env, index_name, cursor_id, batch_callback)


@skip(cluster=True)
def test_basic_hybrid_internal_withcursor(env):
    """Test basic _FT.HYBRID command with WITHCURSOR functionality

    Expected behavior when fixed:
    - Should return a map with VSIM and SEARCH cursor IDs
    - Format: ['VSIM', cursor_id, 'SEARCH', cursor_id]
    - Both cursor IDs should be valid integers/strings
    """
    setup_hybrid_test_data(env)

    # Execute _FT.HYBRID command with WITHCURSOR using direct vector specification
    query_vec = create_np_array_typed([0.0, 0.0], 'FLOAT32')
    result = env.cmd('_FT.HYBRID', 'idx', 'SEARCH', '@description:running',
                     'VSIM', '@embedding', query_vec.tobytes(), 'WITHCURSOR')

    # Should return a map with VSIM and SEARCH cursor IDs
    env.assertTrue(isinstance(result, list))
    env.assertTrue(len(result) > 0)

    # Convert list to dict for easier access
    result_dict = dict(zip(result[::2], result[1::2]))

    # Should have VSIM and SEARCH cursor IDs
    env.assertIn('VSIM', result_dict)
    env.assertIn('SEARCH', result_dict)

    # Both cursor IDs should be valid integers
    vsim_cursor = result_dict['VSIM']
    search_cursor = result_dict['SEARCH']
    env.assertTrue(isinstance(vsim_cursor, (int, str)))
    env.assertTrue(isinstance(search_cursor, (int, str)))


@skip(cluster=True)
def test_hybrid_internal_with_count_parameter(env):
    """Test _FT.HYBRID with WITHCURSOR and COUNT parameter"""
    setup_hybrid_test_data(env)

    # Execute with COUNT parameter set to 2 using direct vector specification
    count_param = 2
    query_vec = create_np_array_typed([0.0, 0.0], 'FLOAT32')
    result = env.cmd('_FT.HYBRID', 'idx', 'SEARCH', '@description:running',
                     'VSIM', '@embedding', query_vec.tobytes(), 'WITHCURSOR', 'COUNT', str(count_param))

    # Should return a map with cursor IDs
    env.assertTrue(isinstance(result, list))
    result_dict = dict(zip(result[::2], result[1::2]))

    # Should have both cursor types
    env.assertIn('VSIM', result_dict)
    env.assertIn('SEARCH', result_dict)

    # Test reading from cursors with COUNT parameter using callback
    def validate_batch_size(batch_results, _cursor_response):
        """Callback to validate that each batch respects the COUNT parameter"""
        # The key test: number of results in each batch should be <= COUNT parameter
        env.assertTrue(len(batch_results) <= count_param)

    for cursor_id in result_dict.values():
        if cursor_id != 0:  # Only test active cursors
            # Use common function with callback to validate COUNT behavior
            results = read_cursor_completely(env, 'idx', cursor_id, validate_batch_size)
            env.assertTrue(isinstance(results, list))


@skip(cluster=True)
def test_hybrid_internal_cursor_interaction(env):
    """Test reading from both VSIM and SEARCH cursors and compare with equivalent FT.SEARCH commands"""
    setup_hybrid_test_data(env)

    # Execute the hybrid command with cursors using direct vector specification
    query_vec = create_np_array_typed([1.0, 0.0], 'FLOAT32')
    hybrid_result = env.cmd('_FT.HYBRID', 'idx', 'SEARCH', '@description:shoes',
                           'VSIM', '@embedding', query_vec.tobytes(), 'WITHCURSOR')

    # Should return a map with cursor IDs
    env.assertTrue(isinstance(hybrid_result, list))
    result_dict = dict(zip(hybrid_result[::2], hybrid_result[1::2]))

    # Should have both cursor types
    env.assertIn('VSIM', result_dict)
    env.assertIn('SEARCH', result_dict)

    # Get expected results from equivalent individual FT.SEARCH commands
    # For text search - just get document keys
    text_search_result = env.cmd('FT.SEARCH', 'idx', '@description:shoes', 'DIALECT', '2', 'RETURN', '0')

    # For vector search - return only keys to avoid binary data issues
    vector_search_result = env.cmd('FT.SEARCH', 'idx', '*=>[KNN 10 @embedding $vec_param]', 'DIALECT', '2',
                                  'PARAMS', '2', 'vec_param', query_vec.tobytes(), 'RETURN', '0')

    # Extract document keys from expected results (RETURN 0 format)
    def extract_doc_keys(search_result):
        """Extract document keys from FT.SEARCH result format with RETURN 0"""
        if len(search_result) < 2:
            return []

        doc_keys = []
        # Skip the count (first element), remaining elements are just document keys
        for i in range(1, len(search_result)):
            doc_keys.append(search_result[i])
        return sorted(doc_keys)

    expected_text_docs = extract_doc_keys(text_search_result)
    expected_vector_docs = extract_doc_keys(vector_search_result)

    # Read from cursors and collect results using common function
    cursor_results = {}
    for cursor_type, cursor_id in result_dict.items():
        cursor_results[cursor_type] = read_cursor_completely(env, 'idx', cursor_id)

    # Compare cursor results with expected FT.SEARCH results
    if 'SEARCH' in cursor_results:
        env.assertEqual(cursor_results['SEARCH'], expected_text_docs)

    if 'VSIM' in cursor_results:
        env.assertEqual(cursor_results['VSIM'], expected_vector_docs)


@skip(cluster=True)
def test_hybrid_internal_cursor_with_scores():
    """Test reading from both VSIM and SEARCH cursors with WITHSCORES and compare with equivalent FT.SEARCH commands"""
    env = Env(protocol=3, moduleArgs='DEFAULT_DIALECT 2')
    setup_hybrid_test_data(env)

    # Execute the hybrid command with cursors
    query_vec = create_np_array_typed([1.0, 0.0], 'FLOAT32')
    hybrid_cursor_dict = env.cmd('_FT.HYBRID', 'idx', 'SEARCH', '@description:shoes',
                           'VSIM', '@embedding', '$vec_param', 'KNN', '2', 'K', '10',
                           'WITHCURSOR', 'WITHSCORES',
                           'PARAMS', '2', 'vec_param', query_vec.tobytes())

    # Should return a map with cursor IDs
    env.assertTrue(isinstance(hybrid_cursor_dict, dict))

    # Should have both cursor types
    env.assertIn('VSIM', hybrid_cursor_dict)
    env.assertIn('SEARCH', hybrid_cursor_dict)


    # Read from cursors and collect results using common function
    cursor_results = {}
    for cursor_type, cursor_id in hybrid_cursor_dict.items():
        cursor_results[cursor_type] = read_cursor_completely(env, 'idx', cursor_id)


    for cursor_type, cursor_result in cursor_results.items():
        for result in cursor_result:
            env.assertIn('key', result)
            env.assertIn('doc', result['key'])
            env.assertIn('score', result)
            env.assertTrue(isinstance(result['score'], (int, float)))



@skip(cluster=True)
def test_hybrid_internal_with_params(env):
    """Test _FT.HYBRID with WITHCURSOR and PARAMS functionality"""
    setup_hybrid_test_data(env)

    # Test with PARAMS for both text and vector parts
    query_vec = create_np_array_typed([1.0, 0.0], 'FLOAT32')

    # Execute hybrid command with direct vector specification (keeping text param)
    hybrid_result = env.cmd('_FT.HYBRID', 'idx', 'SEARCH', '@description:($term)',
                           'VSIM', '@embedding', query_vec.tobytes(), 'WITHCURSOR',
                           'PARAMS', '2', 'term', 'shoes')

    # Should return cursor map
    env.assertTrue(isinstance(hybrid_result, list))
    result_dict = dict(zip(hybrid_result[::2], hybrid_result[1::2]))
    env.assertIn('VSIM', result_dict)
    env.assertIn('SEARCH', result_dict)

    # Get expected results from equivalent parameterized FT.SEARCH commands
    text_search_result = env.cmd('FT.SEARCH', 'idx', '@description:($term)', 'DIALECT', '2',
                                'PARAMS', '2', 'term', 'shoes', 'RETURN', '0')
    vector_search_result = env.cmd('FT.SEARCH', 'idx', '*=>[KNN 10 @embedding $vec_param]', 'DIALECT', '2',
                                  'PARAMS', '2', 'vec_param', query_vec.tobytes(), 'RETURN', '0')

    # Extract expected document keys
    def extract_doc_keys(search_result):
        return sorted(search_result[1:]) if len(search_result) > 1 else []

    expected_text_docs = extract_doc_keys(text_search_result)
    expected_vector_docs = extract_doc_keys(vector_search_result)

    # Read cursor results and compare with expected results
    cursor_results = {}
    for cursor_type, cursor_id in result_dict.items():
        cursor_results[cursor_type] = read_cursor_completely(env, 'idx', cursor_id)

    # Verify that parameterized queries work correctly
    env.assertEqual(cursor_results['SEARCH'], expected_text_docs)
    env.assertEqual(cursor_results['VSIM'], expected_vector_docs)


@skip(cluster=True)
def test_hybrid_internal_error_cases(env):
    """Test error cases with _FT.HYBRID (without WITHCURSOR)"""
    setup_hybrid_test_data(env)

    # Test with non-existent index using direct vector specification
    query_vec = create_np_array_typed([0.0, 0.0], 'FLOAT32')
    env.expect('_FT.HYBRID', 'nonexistent', 'SEARCH', '@description:running',
               'VSIM', '@embedding', query_vec.tobytes()).error().contains('No such index nonexistent')

    # Test with invalid vector field using direct vector specification
    env.expect('_FT.HYBRID', 'idx', 'SEARCH', '@description:running',
               'VSIM', '@nonexistent', query_vec.tobytes()).error().contains('Unknown field `nonexistent`')


@skip(cluster=True)
def test_hybrid_internal_cursor_limit(env):
    """Test _FT.HYBRID cursor limit per shard

    A single _FT.HYBRID command tries to create two cursors (VSIM and SEARCH).
    When INDEX_CURSOR_LIMIT is set to 1, this should fail with 'Failed to allocate enough cursors' error.
    """
    # Set cursor limit to 1 for this test
    env.cmd('CONFIG', 'SET', 'search-index-cursor-limit', '1')

    setup_hybrid_test_data(env)

    # _FT.HYBRID command should fail because it tries to create 2 cursors but limit is 1
    query_vec = create_np_array_typed([0.0, 0.0], 'FLOAT32')
    env.expect('_FT.HYBRID', 'idx', 'SEARCH', '@description:running',
               'VSIM', '@embedding', query_vec.tobytes(), 'WITHCURSOR').error().contains('INDEX_CURSOR_LIMIT of 1 has been reached for an index')


@skip(cluster=True)
def test_hybrid_internal_empty_search_results(env):
    """Test _FT.HYBRID when search subquery returns no results

    This test verifies behavior when the text search part finds no matching documents,
    while the vector similarity part can still return results.
    """
    setup_hybrid_test_data(env)

    # Search for a term that doesn't exist in any document
    query_vec = create_np_array_typed([0.0, 0.0], 'FLOAT32')
    hybrid_result = env.cmd('_FT.HYBRID', 'idx', 'SEARCH', '@description:nonexistent',
                           'VSIM', '@embedding', query_vec.tobytes(), 'WITHCURSOR')

    # Should return a map with cursor IDs
    env.assertTrue(isinstance(hybrid_result, list))
    result_dict = dict(zip(hybrid_result[::2], hybrid_result[1::2]))

    # Should have both cursor types
    env.assertIn('VSIM', result_dict)
    env.assertIn('SEARCH', result_dict)

    # Verify that text search returns no results
    text_search_result = env.cmd('FT.SEARCH', 'idx', '@description:nonexistent', 'DIALECT', '2', 'RETURN', '0')
    env.assertEqual(text_search_result[0], 0)  # Should have 0 results

    # Verify that vector search still returns results
    vector_search_result = env.cmd('FT.SEARCH', 'idx', '*=>[KNN 10 @embedding $vec_param]', 'DIALECT', '2',
                                  'PARAMS', '2', 'vec_param', query_vec.tobytes(), 'RETURN', '0')
    env.assertTrue(vector_search_result[0] > 0)  # Should have results

    # Read from cursors and verify behavior
    cursor_results = {}
    for cursor_type, cursor_id in result_dict.items():
        cursor_results[cursor_type] = read_cursor_completely(env, 'idx', cursor_id)

    # SEARCH cursor should return empty results
    env.assertEqual(cursor_results['SEARCH'], [])

    # VSIM cursor should return some results
    env.assertTrue(len(cursor_results['VSIM']) > 0)
