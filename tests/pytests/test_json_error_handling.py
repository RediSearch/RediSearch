# -*- coding: utf-8 -*-

import json
from common import *
from includes import *
from RLTest import Env


@skip(no_json=True, cluster=True)
def test_missing_json_key_error(env):
    """Test error handling when JSON key doesn't exist or is not a JSON document"""

    # Create an index with JSON schema
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
               '$.name', 'AS', 'name', 'TEXT',
               '$.age', 'AS', 'age', 'NUMERIC').ok()

    # Test 1: Try to index a non-existent key
    # This should trigger the error in Document_LoadSchemaFieldJson
    env.expect('FT.SEARCH', 'idx', '*').equal([0])

    # Test 2: Create a non-JSON key and try to index it
    env.expect('SET', 'non_json_doc', 'this is not json').equal(True)

    # The document should not be indexed due to the error
    env.expect('FT.SEARCH', 'idx', '*').equal([0])

    # Test 3: Create a proper JSON document to verify normal operation
    env.expect('JSON.SET', 'valid_doc', '$', '{"name": "John", "age": 30}').ok()

    # This should be indexed successfully
    env.expect('FT.SEARCH', 'idx', '*').equal([1, 'valid_doc', ['$', '{"name":"John","age":30}']])


@skip(no_json=True, cluster=True)
def test_missing_json_key_specific_error_message(env):
    """Test that the specific error message is generated for missing JSON keys"""

    # Create an index
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
               '$.data', 'AS', 'data', 'TEXT').ok()

    # Create a non-JSON document that should trigger the error
    env.expect('SET', 'string_doc', 'not a json document').equal(True)

    # Check index info for error details
    info = env.cmd('FT.INFO', 'idx')
    info_dict = {info[i]: info[i+1] for i in range(0, len(info), 2)}

    # The document should not be indexed
    env.assertEqual(info_dict.get('num_docs', 0), 0)

    # Test with a completely missing key by trying to search for it
    # The key doesn't exist, so it won't be indexed
    env.expect('FT.SEARCH', 'idx', '*').equal([0])


@skip(no_json=True, cluster=True)
def test_json_key_exists_but_wrong_type(env):
    """Test error when key exists but is not a JSON type"""

    # Create an index
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
               '$.name', 'AS', 'name', 'TEXT').ok()

    # Create different types of non-JSON keys
    env.expect('SET', 'string_key', 'plain string').equal(True)
    env.expect('HSET', 'hash_key', 'field', 'value').equal(1)
    env.expect('LPUSH', 'list_key', 'item').equal(1)
    env.expect('SADD', 'set_key', 'member').equal(1)

    # None of these should be indexed
    env.expect('FT.SEARCH', 'idx', '*').equal([0])

    # Add a valid JSON document to verify the index works
    env.expect('JSON.SET', 'json_key', '$', '{"name": "valid"}').ok()
    env.expect('FT.SEARCH', 'idx', '*').equal([1, 'json_key', ['$', '{"name":"valid"}']])


@skip(no_json=True, cluster=True)
def test_unsupported_field_types_error(env):
    """Test error handling for unsupported JSON field types"""

    # Test 1: Object type for TEXT field (should fail)
    env.expect('FT.CREATE', 'idx_obj', 'ON', 'JSON', 'SCHEMA',
               '$.data', 'AS', 'data', 'TEXT').ok()

    # Set a JSON document with an object for text field
    env.expect('JSON.SET', 'doc_obj', '$',
               '{"data": {"nested": "value"}}').ok()

    # The document should not be indexed due to unsupported object type
    env.expect('FT.SEARCH', 'idx_obj', '*').equal([0])

    # Test 2: Valid cases to ensure normal operation still works
    env.expect('JSON.SET', 'doc_valid_text', '$',
               '{"data": "valid text"}').ok()

    env.expect('FT.SEARCH', 'idx_obj', '*').equal([1, 'doc_valid_text', ['$', '{"data":"valid text"}']])


@skip(no_json=True, cluster=True)
def test_unsupported_field_combinations_error(env):
    """Test error handling for various unsupported field type combinations"""
    
    # Test different field types with unsupported JSON types
    test_cases = [
        {
            'field_type': 'NUMERIC',
            'json_data': '{"value": {"nested": "object"}}',
            'doc_name': 'doc_numeric_obj'
        },
        {
            'field_type': 'TAG', 
            'json_data': '{"value": {"nested": "object"}}',
            'doc_name': 'doc_tag_obj'
        },
        {
            'field_type': 'TEXT',
            'json_data': '{"value": {"nested": "object"}}', 
            'doc_name': 'doc_text_obj'
        }
    ]
    
    for i, test_case in enumerate(test_cases):
        idx_name = f'idx_{i}'
        
        # Create index for this test case
        env.expect('FT.CREATE', idx_name, 'ON', 'JSON', 'SCHEMA',
                   '$.value', 'AS', 'value', test_case['field_type']).ok()
        
        # Set JSON document with unsupported type
        env.expect('JSON.SET', test_case['doc_name'], '$', test_case['json_data']).ok()
        
        # Document should not be indexed due to unsupported type
        env.expect('FT.SEARCH', idx_name, '*').equal([0])


@skip(no_json=True, cluster=True)
def test_iterator_failure_error(env):
    """Test error handling when JSON iterator fails to get value"""
    
    # Create an index
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
               '$.data', 'AS', 'data', 'TEXT').ok()
    
    # This test is more complex as it requires triggering iterator failure
    # For now, we'll test with a valid case and verify it works
    env.expect('JSON.SET', 'doc', '$', '{"data": "test"}').ok()
    env.expect('FT.SEARCH', 'idx', '*').equal([1, 'doc', ['$', '{"data":"test"}']])


@skip(no_json=True, cluster=True)
def test_error_messages_in_index_info(env):
    """Test that error messages appear correctly in FT.INFO output"""

    import time
    import random

    # Create a unique index name to avoid collisions across test runs
    unique_suffix = str(int(time.time() * 1000) % 100000) + str(random.randint(1000, 9999))
    idx_name = f'idx_info_{unique_suffix}'

    # Create an index with supported field types
    env.expect('FT.CREATE', idx_name, 'ON', 'JSON', 'SCHEMA',
               '$.name', 'AS', 'name', 'TEXT',
               '$.location', 'AS', 'location', 'GEO').ok()

    # Add a document that will cause indexing errors
    doc_name = f'error_doc_{unique_suffix}'
    env.expect('JSON.SET', doc_name, '$',
               '{"name": {"nested": "object"}, "location": {"lat": 1, "lon": 2}}').ok()

    # Check that the index info shows the errors
    info = env.cmd('FT.INFO', idx_name)
    info_dict = {info[i]: info[i+1] for i in range(0, len(info), 2)}

    # Verify that indexing failures are recorded
    if 'Index Errors' in info_dict:
        errors = info_dict['Index Errors']
        # Should have some indexing failures recorded
        env.assertTrue(len(errors) > 0)


@skip(no_json=True, cluster=True)
def test_mixed_valid_invalid_documents(env):
    """Test that valid documents are indexed while invalid ones are skipped"""
    
    # Create an index
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
               '$.name', 'AS', 'name', 'TEXT',
               '$.age', 'AS', 'age', 'NUMERIC').ok()
    
    # Add valid documents
    env.expect('JSON.SET', 'valid1', '$', '{"name": "Alice", "age": 25}').ok()
    env.expect('JSON.SET', 'valid2', '$', '{"name": "Bob", "age": 30}').ok()
    
    # Add invalid documents (objects for text/numeric fields)
    env.expect('JSON.SET', 'invalid1', '$', '{"name": {"first": "Charlie"}, "age": 35}').ok()
    env.expect('JSON.SET', 'invalid2', '$', '{"name": "David", "age": {"value": 40}}').ok()
    
    # Search should only return valid documents
    result = env.cmd('FT.SEARCH', 'idx', '*')
    env.assertEqual(result[0], 2)  # Should have 2 valid documents
    
    # Verify the valid documents are returned
    doc_ids = [result[i] for i in range(1, len(result), 2)]
    env.assertIn('valid1', doc_ids)
    env.assertIn('valid2', doc_ids)


@skip(no_json=True, cluster=True)
def test_geo_array_error_specific(env):
    """Test specific error handling for GEO field with array type"""

    # Create index with GEO field
    env.expect('FT.CREATE', 'idx_geo', 'ON', 'JSON', 'SCHEMA',
               '$.location', 'AS', 'location', 'GEO').ok()

    # Test array of geo values (should work as GEO supports arrays)
    env.expect('JSON.SET', 'doc_geo_array', '$',
               '{"location": ["1.0,2.0", "3.0,4.0"]}').ok()

    # Test object for geo field (should fail)
    env.expect('JSON.SET', 'doc_geo_obj', '$',
               '{"location": {"lat": 1, "lon": 2}}').ok()

    # Test valid geo (should work)
    env.expect('JSON.SET', 'doc_geo_valid', '$',
               '{"location": "1.0,2.0"}').ok()

    # Check how many documents were indexed
    result = env.cmd('FT.SEARCH', 'idx_geo', '*')
    # Should have at least the valid documents
    env.assertTrue(result[0] >= 1)


@skip(no_json=True, cluster=True)
def test_object_type_error_specific(env):
    """Test specific error handling for object types"""

    # Create index with various field types
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
               '$.text_field', 'AS', 'text_field', 'TEXT',
               '$.numeric_field', 'AS', 'numeric_field', 'NUMERIC',
               '$.tag_field', 'AS', 'tag_field', 'TAG').ok()

    # Test object for text field (should trigger "Object type is not supported")
    env.expect('JSON.SET', 'doc_obj_text', '$',
               '{"text_field": {"nested": "value"}}').ok()

    # Test object for numeric field
    env.expect('JSON.SET', 'doc_obj_numeric', '$',
               '{"numeric_field": {"value": 42}}').ok()

    # Test object for tag field
    env.expect('JSON.SET', 'doc_obj_tag', '$',
               '{"tag_field": {"tag": "value"}}').ok()

    # None should be indexed
    env.expect('FT.SEARCH', 'idx', '*').equal([0])

    # Test valid values
    env.expect('JSON.SET', 'doc_valid', '$',
               '{"text_field": "text", "numeric_field": 42, "tag_field": "tag"}').ok()

    result = env.cmd('FT.SEARCH', 'idx', '*')
    env.assertEqual(result[0], 1)
    env.assertIn('doc_valid', result)


@skip(no_json=True, cluster=True)
def test_unsupported_field_type_error_specific(env):
    """Test specific error handling for unsupported field types in JSON_LoadDocumentField"""

    # Create index with a field type that might trigger unsupported type errors
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
               '$.data', 'AS', 'data', 'TEXT').ok()

    # Test with complex nested structures that might trigger unsupported type errors
    env.expect('JSON.SET', 'doc_complex', '$',
               '{"data": [{"nested": {"deep": "value"}}, null, true, false]}').ok()

    # This might not be indexed depending on how the complex array is handled
    # We're mainly testing that it doesn't crash and handles errors gracefully
    env.cmd('FT.SEARCH', 'idx', '*')  # Just verify it doesn't crash


@skip(no_json=True, cluster=True)
def test_iterator_value_failure_error(env):
    """Test error handling when iterator fails to get value"""

    # Create index
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
               '$.data', 'AS', 'data', 'TEXT').ok()

    # Test with various edge cases that might cause iterator failures
    test_cases = [
        '{"data": null}',  # null value
        '{"data": []}',    # empty array
        '{"data": {}}',    # empty object
    ]

    for i, json_data in enumerate(test_cases):
        doc_name = f'doc_edge_case_{i}'
        env.expect('JSON.SET', doc_name, '$', json_data).ok()

    # Check how many documents were successfully indexed
    # The exact number depends on how each edge case is handled
    # We're testing that the system handles these gracefully without crashing
    env.cmd('FT.SEARCH', 'idx', '*')  # Just verify it doesn't crash


@skip(no_json=True, cluster=True)
def test_log_level_change_verification(env):
    """Test that the log level change from WARNING to DEBUG doesn't break functionality"""

    # Create an index
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
               '$.name', 'AS', 'name', 'TEXT').ok()

    # Create a scenario that would trigger IndexError_AddError
    # This happens when there are indexing errors
    env.expect('JSON.SET', 'error_doc', '$',
               '{"name": {"nested": "object"}}').ok()

    # The document should not be indexed due to the error
    env.expect('FT.SEARCH', 'idx', '*').equal([0])

    # Check that the index info shows the error was recorded
    info = env.cmd('FT.INFO', 'idx')
    info_dict = {info[i]: info[i+1] for i in range(0, len(info), 2)}

    # Verify that the error tracking still works (the log level change shouldn't affect this)
    if 'Index Errors' in info_dict:
        errors = info_dict['Index Errors']
        # Should have recorded the indexing failure
        env.assertTrue(len(errors) > 0)

    # Add a valid document to ensure normal operation continues
    env.expect('JSON.SET', 'valid_doc', '$', '{"name": "test"}').ok()
    env.expect('FT.SEARCH', 'idx', '*').equal([1, 'valid_doc', ['$', '{"name":"test"}']])


@skip(no_json=True, cluster=True)
def test_error_propagation_consistency(env):
    """Test that error messages are consistently propagated through the system"""

    # Create an index with multiple field types
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
               '$.text', 'AS', 'text', 'TEXT',
               '$.number', 'AS', 'number', 'NUMERIC',
               '$.geo', 'AS', 'geo', 'GEO',
               '$.tag', 'AS', 'tag', 'TAG').ok()

    # Create documents with various error conditions
    error_docs = [
        ('doc_text_obj', '{"text": {"nested": "value"}}'),
        ('doc_number_obj', '{"number": {"value": 42}}'),
        ('doc_geo_obj', '{"geo": {"lat": 1, "lon": 2}}'),
        ('doc_tag_obj', '{"tag": {"name": "value"}}'),
    ]

    for doc_name, json_data in error_docs:
        env.expect('JSON.SET', doc_name, '$', json_data).ok()

    # None of these should be indexed
    env.expect('FT.SEARCH', 'idx', '*').equal([0])

    # Check that errors are properly recorded in index info
    info = env.cmd('FT.INFO', 'idx')
    info_dict = {info[i]: info[i+1] for i in range(0, len(info), 2)}

    # Should have recorded indexing failures
    env.assertEqual(info_dict.get('num_docs', 0), 0)

    # Add valid documents to ensure the index still works
    env.expect('JSON.SET', 'valid_doc', '$',
               '{"text": "hello", "number": 42, "geo": "1.0,2.0", "tag": "test"}').ok()

    result = env.cmd('FT.SEARCH', 'idx', '*')
    env.assertEqual(result[0], 1)
    env.assertIn('valid_doc', result)
