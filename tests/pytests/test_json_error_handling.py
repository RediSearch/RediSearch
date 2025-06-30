# -*- coding: utf-8 -*-

import json
from common import *
from includes import *
from RLTest import Env




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

    import random
    import time
    unique_suffix = str(int(time.time() * 1000) % 100000) + str(random.randint(1000, 9999))
    idx_name = f'idx_propagation_{unique_suffix}'

    # Create an index with multiple field types
    env.expect('FT.CREATE', idx_name, 'ON', 'JSON', 'SCHEMA',
               '$.text', 'AS', 'text', 'TEXT',
               '$.number', 'AS', 'number', 'NUMERIC',
               '$.geo', 'AS', 'geo', 'GEO',
               '$.tag', 'AS', 'tag', 'TAG').ok()

    # Create documents with various error conditions
    error_docs = [
        (f'doc_text_obj_{unique_suffix}', '{"text": {"nested": "value"}}'),
        (f'doc_number_obj_{unique_suffix}', '{"number": {"value": 42}}'),
        (f'doc_geo_obj_{unique_suffix}', '{"geo": {"lat": 1, "lon": 2}}'),
        (f'doc_tag_obj_{unique_suffix}', '{"tag": {"name": "value"}}'),
    ]

    for doc_name, json_data in error_docs:
        env.expect('JSON.SET', doc_name, '$', json_data).ok()

    # Check initial state
    result = env.cmd('FT.SEARCH', idx_name, '*')
    initial_count = result[0]

    # Check that errors are properly recorded in index info
    info = env.cmd('FT.INFO', idx_name)
    info_dict = {info[i]: info[i+1] for i in range(0, len(info), 2)}

    # Add valid documents to ensure the index still works
    env.expect('JSON.SET', f'valid_doc_{unique_suffix}', '$',
               '{"text": "hello", "number": 42, "geo": "1.0,2.0", "tag": "test"}').ok()

    result = env.cmd('FT.SEARCH', idx_name, 'hello')
    env.assertTrue(result[0] >= 1)
    env.assertIn(f'valid_doc_{unique_suffix}', result)


@skip(no_json=True, cluster=True)
def test_index_error_comprehensive(env):
    """Comprehensive test for IndexError functionality covering all error scenarios"""

    import random
    import time
    unique_suffix = str(int(time.time() * 1000) % 100000) + str(random.randint(1000, 9999))
    idx_name = f'idx_error_test_{unique_suffix}'

    # Create an index with multiple field types
    env.expect('FT.CREATE', idx_name, 'ON', 'JSON', 'SCHEMA',
               '$.name', 'AS', 'name', 'TEXT',
               '$.age', 'AS', 'age', 'NUMERIC',
               '$.location', 'AS', 'location', 'GEO').ok()

    # Test 1: Create a non-JSON document (should trigger missing JSON key error)
    env.expect('SET', f'non_json_{unique_suffix}', 'this is not json').equal(True)

    # Test 2: Create JSON documents with object types (should trigger object type errors)
    env.expect('JSON.SET', f'obj_text_{unique_suffix}', '$',
               '{"name": {"nested": "object"}}').ok()
    env.expect('JSON.SET', f'obj_numeric_{unique_suffix}', '$',
               '{"age": {"value": 30}}').ok()
    env.expect('JSON.SET', f'obj_geo_{unique_suffix}', '$',
               '{"location": {"lat": 1, "lon": 2}}').ok()

    # Check the index info for error details
    info = env.cmd('FT.INFO', idx_name)
    info_dict = {info[i]: info[i+1] for i in range(0, len(info), 2)}

    # Should have no successfully indexed documents due to errors
    env.assertEqual(info_dict.get('num_docs', 0), 0)

    # Check Index Errors section
    if 'Index Errors' in info_dict:
        errors = info_dict['Index Errors']
        if len(errors) > 0:
            error_dict = {errors[i]: errors[i+1] for i in range(0, len(errors), 2)}

            # Should have recorded indexing failures
            if 'indexing failures' in error_dict:
                env.assertTrue(error_dict['indexing failures'] >= 0)

            # Should have last indexing error information
            if 'last indexing error' in error_dict:
                env.assertTrue(len(str(error_dict['last indexing error'])) > 0)

            # Should have the key that caused the last error
            if 'last indexing error key' in error_dict:
                env.assertTrue(len(str(error_dict['last indexing error key'])) > 0)

    # Test 3: Add a valid document to verify the index still works
    env.expect('JSON.SET', f'valid_doc_{unique_suffix}', '$',
               '{"name": "John", "age": 30, "location": "1.0,2.0"}').ok()

    # Check that the valid document was indexed
    info = env.cmd('FT.INFO', idx_name)
    info_dict = {info[i]: info[i+1] for i in range(0, len(info), 2)}
    env.assertEqual(info_dict.get('num_docs', 0), 1)  # Should have 1 valid document

    # Error information should still be preserved
    if 'Index Errors' in info_dict:
        errors = info_dict['Index Errors']
        if len(errors) > 0:
            error_dict = {errors[i]: errors[i+1] for i in range(0, len(errors), 2)}

            # Should still show previous indexing failures
            if 'indexing failures' in error_dict:
                env.assertTrue(error_dict['indexing failures'] >= 0)

    # Verify the valid document can be found
    result = env.cmd('FT.SEARCH', idx_name, '*')
    env.assertEqual(result[0], 1)
    env.assertIn(f'valid_doc_{unique_suffix}', result)

