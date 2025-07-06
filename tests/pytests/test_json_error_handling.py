from common import *

@skip(no_json=True, cluster=True)
def test_json_indexing_error_message(env):
    """Simple test to cause an indexing error and verify the error message is recorded"""

    # Create an index expecting a TEXT field
    env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'SCHEMA',
               '$.name', 'AS', 'name', 'TEXT').ok()

    # Create a document that will cause an indexing error
    # (providing an object instead of a text value)
    env.expect('JSON.SET', 'error_doc', '$',
               '{"name": {"nested": "object"}}').ok()

    # Get index info to check for errors
    info = env.cmd('FT.INFO', 'idx')
    info_dict = {info[i]: info[i+1] for i in range(0, len(info), 2)}

    # Verify that an error was recorded
    env.assertIn('Index Errors', info_dict)
    errors = info_dict['Index Errors']
    error_dict = {errors[i]: errors[i+1] for i in range(0, len(errors), 2)}

    # Check that we have indexing failures
    env.assertIn('indexing failures', error_dict)
    env.assertTrue(error_dict['indexing failures'] > 0)

    # Check that the error message is recorded
    env.assertIn('last indexing error', error_dict)
    error_message = error_dict['last indexing error']

    # Verify the error message contains expected content
    # (This is where you would check for your newly written error message)
    env.assertTrue(len(error_message) > 0)
    print(f"Error message: {error_message}")

    # Check that the error key is recorded
    env.assertIn('last indexing error key', error_dict)
    env.assertEqual(error_dict['last indexing error key'], 'error_doc')


@skip(no_json=True, cluster=True)
def test_geometry_field_array_error(env):
    """Test that GEOMETRY fields reject array types with proper error message"""
    # Skip this test if GEOMETRY field type is not supported
    try:
        env.expect('FT.CREATE', 'geo_test_idx', 'ON', 'JSON', 'SCHEMA',
                   '$.test', 'AS', 'test', 'GEOMETRY').ok()
        env.cmd('FT.DROPINDEX', 'geo_test_idx')
    except Exception as e:
        if 'Invalid field type' in str(e):
            skipTest()  # Skip test if GEOMETRY is not supported
        else:
            raise
    """Test that GEOMETRY fields reject array types with proper error message"""

    # Create an index with a GEOMETRY field
    env.expect('FT.CREATE', 'geo_idx', 'ON', 'JSON', 'SCHEMA',
               '$.location', 'AS', 'location', 'GEOMETRY').ok()

    # Try to index a document with an array for the geometry field
    env.expect('JSON.SET', 'geo_error_doc', '$',
               '{"location": [1, 2, 3]}').ok()

    # Get index info to check for errors
    info = env.cmd('FT.INFO', 'geo_idx')
    info_dict = {info[i]: info[i+1] for i in range(0, len(info), 2)}

    # Verify that an error was recorded
    env.assertIn('Index Errors', info_dict)
    errors = info_dict['Index Errors']
    error_dict = {errors[i]: errors[i+1] for i in range(0, len(errors), 2)}

    # Check that we have indexing failures
    env.assertIn('indexing failures', error_dict)
    env.assertTrue(error_dict['indexing failures'] > 0)

    # Check that the error message is recorded and contains expected text
    env.assertIn('last indexing error', error_dict)
    error_message = error_dict['last indexing error']
    env.assertIn('GEOMETRY field does not support array type', error_message)

    # Check that the error key is recorded
    env.assertIn('last indexing error key', error_dict)
    env.assertEqual(error_dict['last indexing error key'], 'geo_error_doc')


@skip(no_json=True, cluster=True)
def test_vector_field_empty_array_error(env):
    """Test that VECTOR fields reject empty arrays with proper error message"""

    # Create an index with a VECTOR field
    env.expect('FT.CREATE', 'vec_idx', 'ON', 'JSON', 'SCHEMA',
               '$.embedding', 'AS', 'embedding', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '3', 'DISTANCE_METRIC', 'L2').ok()

    # Try to index a document with an empty array for the vector field
    env.expect('JSON.SET', 'vec_error_doc', '$',
               '{"embedding": []}').ok()

    # Get index info to check for errors
    info = env.cmd('FT.INFO', 'vec_idx')
    info_dict = {info[i]: info[i+1] for i in range(0, len(info), 2)}

    # Verify that an error was recorded
    env.assertIn('Index Errors', info_dict)
    errors = info_dict['Index Errors']
    error_dict = {errors[i]: errors[i+1] for i in range(0, len(errors), 2)}

    # Check that we have indexing failures
    env.assertIn('indexing failures', error_dict)
    env.assertTrue(error_dict['indexing failures'] > 0)

    # Check that the error message is recorded and contains expected text
    env.assertIn('last indexing error', error_dict)
    error_message = error_dict['last indexing error']
    env.assertIn('Empty array for vector field on JSON document', error_message)

    # Check that the error key is recorded
    env.assertIn('last indexing error key', error_dict)
    env.assertEqual(error_dict['last indexing error key'], 'vec_error_doc')


@skip(no_json=True, cluster=True)
def test_numeric_field_invalid_array_elements(env):
    """Test that NUMERIC fields reject arrays with non-numeric elements"""

    # Create an index with a NUMERIC field
    env.expect('FT.CREATE', 'num_idx', 'ON', 'JSON', 'SCHEMA',
               '$.scores', 'AS', 'scores', 'NUMERIC').ok()

    # Try to index a document with an array containing non-numeric elements
    env.expect('JSON.SET', 'num_error_doc', '$',
               '{"scores": [1, 2, "invalid", 4]}').ok()

    # Get index info to check for errors
    info = env.cmd('FT.INFO', 'num_idx')
    info_dict = {info[i]: info[i+1] for i in range(0, len(info), 2)}

    # Verify that an error was recorded
    env.assertIn('Index Errors', info_dict)
    errors = info_dict['Index Errors']
    error_dict = {errors[i]: errors[i+1] for i in range(0, len(errors), 2)}

    # Check that we have indexing failures
    env.assertIn('indexing failures', error_dict)
    env.assertTrue(error_dict['indexing failures'] > 0)

    # Check that the error message is recorded and contains expected text
    env.assertIn('last indexing error', error_dict)
    error_message = error_dict['last indexing error']
    env.assertIn('NUMERIC fields can only contain numeric or nulls', error_message)

    # Check that the error key is recorded
    env.assertIn('last indexing error key', error_dict)
    env.assertEqual(error_dict['last indexing error key'], 'num_error_doc')


@skip(no_json=True, cluster=True)
def test_text_field_invalid_array_elements(env):
    """Test that TEXT fields reject arrays with non-string elements"""

    # Create an index with a TEXT field
    env.expect('FT.CREATE', 'text_idx', 'ON', 'JSON', 'SCHEMA',
               '$.tags', 'AS', 'tags', 'TEXT').ok()

    # Try to index a document with an array containing non-string elements
    env.expect('JSON.SET', 'text_error_doc', '$',
               '{"tags": ["valid", 123, "another_valid"]}').ok()

    # Get index info to check for errors
    info = env.cmd('FT.INFO', 'text_idx')
    info_dict = {info[i]: info[i+1] for i in range(0, len(info), 2)}

    # Verify that an error was recorded
    env.assertIn('Index Errors', info_dict)
    errors = info_dict['Index Errors']
    error_dict = {errors[i]: errors[i+1] for i in range(0, len(errors), 2)}

    # Check that we have indexing failures
    env.assertIn('indexing failures', error_dict)
    env.assertTrue(error_dict['indexing failures'] > 0)

    # Check that the error message is recorded and contains expected text
    env.assertIn('last indexing error', error_dict)
    error_message = error_dict['last indexing error']
    env.assertIn('TEXT/TAG fields can only contain strings or nulls', error_message)

    # Check that the error key is recorded
    env.assertIn('last indexing error key', error_dict)
    env.assertEqual(error_dict['last indexing error key'], 'text_error_doc')


@skip(no_json=True, cluster=True)
def test_tag_field_invalid_array_elements(env):
    """Test that TAG fields reject arrays with non-string elements"""

    # Create an index with a TAG field
    env.expect('FT.CREATE', 'tag_idx', 'ON', 'JSON', 'SCHEMA',
               '$.categories', 'AS', 'categories', 'TAG').ok()

    # Try to index a document with an array containing non-string elements
    env.expect('JSON.SET', 'tag_error_doc', '$',
               '{"categories": ["electronics", {"nested": "object"}, "books"]}').ok()

    # Get index info to check for errors
    info = env.cmd('FT.INFO', 'tag_idx')
    info_dict = {info[i]: info[i+1] for i in range(0, len(info), 2)}

    # Verify that an error was recorded
    env.assertIn('Index Errors', info_dict)
    errors = info_dict['Index Errors']
    error_dict = {errors[i]: errors[i+1] for i in range(0, len(errors), 2)}

    # Check that we have indexing failures
    env.assertIn('indexing failures', error_dict)
    env.assertTrue(error_dict['indexing failures'] > 0)

    # Check that the error message is recorded and contains expected text
    env.assertIn('last indexing error', error_dict)
    error_message = error_dict['last indexing error']
    env.assertIn('TEXT/TAG fields can only contain strings or nulls', error_message)

    # Check that the error key is recorded
    env.assertIn('last indexing error key', error_dict)
    env.assertEqual(error_dict['last indexing error key'], 'tag_error_doc')


@skip(no_json=True, cluster=True)
def test_vector_field_wrong_dimension(env):
    """Test that VECTOR fields reject arrays with wrong dimensions"""

    # Create an index with a VECTOR field expecting 3 dimensions
    env.expect('FT.CREATE', 'vec_dim_idx', 'ON', 'JSON', 'SCHEMA',
               '$.embedding', 'AS', 'embedding', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '3', 'DISTANCE_METRIC', 'L2').ok()

    # Try to index a document with wrong vector dimension (5 instead of 3)
    env.expect('JSON.SET', 'vec_dim_error_doc', '$',
               '{"embedding": [1.0, 2.0, 3.0, 4.0, 5.0]}').ok()

    # Get index info to check for errors
    info = env.cmd('FT.INFO', 'vec_dim_idx')
    info_dict = {info[i]: info[i+1] for i in range(0, len(info), 2)}

    # Verify that an error was recorded
    env.assertIn('Index Errors', info_dict)
    errors = info_dict['Index Errors']
    error_dict = {errors[i]: errors[i+1] for i in range(0, len(errors), 2)}

    # Check that we have indexing failures
    env.assertIn('indexing failures', error_dict)
    env.assertTrue(error_dict['indexing failures'] > 0)

    # Check that the error message is recorded and contains expected text
    env.assertIn('last indexing error', error_dict)
    error_message = error_dict['last indexing error']
    env.assertIn('Invalid vector length. Expected 3, got 5', error_message)

    # Check that the error key is recorded
    env.assertIn('last indexing error key', error_dict)
    env.assertEqual(error_dict['last indexing error key'], 'vec_dim_error_doc')


@skip(no_json=True, cluster=True)
def test_vector_field_invalid_element_type(env):
    """Test that VECTOR fields reject arrays with invalid element types"""

    # Create an index with a VECTOR field
    env.expect('FT.CREATE', 'vec_type_idx', 'ON', 'JSON', 'SCHEMA',
               '$.embedding', 'AS', 'embedding', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '3', 'DISTANCE_METRIC', 'L2').ok()

    # Try to index a document with invalid vector element types (string instead of number)
    env.expect('JSON.SET', 'vec_type_error_doc', '$',
               '{"embedding": [1.0, "invalid", 3.0]}').ok()

    # Get index info to check for errors
    info = env.cmd('FT.INFO', 'vec_type_idx')
    info_dict = {info[i]: info[i+1] for i in range(0, len(info), 2)}

    # Verify that an error was recorded
    env.assertIn('Index Errors', info_dict)
    errors = info_dict['Index Errors']
    error_dict = {errors[i]: errors[i+1] for i in range(0, len(errors), 2)}

    # Check that we have indexing failures
    env.assertIn('indexing failures', error_dict)
    env.assertTrue(error_dict['indexing failures'] > 0)

    # Check that the error message is recorded and contains expected text
    env.assertIn('last indexing error', error_dict)
    error_message = error_dict['last indexing error']
    env.assertIn('Invalid vector element at index', error_message)

    # Check that the error key is recorded
    env.assertIn('last indexing error key', error_dict)
    env.assertEqual(error_dict['last indexing error key'], 'vec_type_error_doc')


@skip(no_json=True, cluster=True)
def test_json_type_validation_errors(env):
    """Test JSON type validation errors from FieldSpec_CheckJsonType"""

    # Test 1: String type for NUMERIC field (should fail)
    env.expect('FT.CREATE', 'type_val_idx1', 'ON', 'JSON', 'SCHEMA',
               '$.value', 'AS', 'value', 'NUMERIC').ok()

    env.expect('JSON.SET', 'type_val_error_doc1', '$',
               '{"value": "not_a_number"}').ok()

    info = env.cmd('FT.INFO', 'type_val_idx1')
    info_dict = {info[i]: info[i+1] for i in range(0, len(info), 2)}
    env.assertIn('Index Errors', info_dict)
    errors = info_dict['Index Errors']
    error_dict = {errors[i]: errors[i+1] for i in range(0, len(errors), 2)}
    env.assertTrue(error_dict['indexing failures'] > 0)
    error_message = error_dict['last indexing error']
    env.assertIn('Invalid JSON type: String type can represent only TEXT, TAG, GEO or GEOMETRY field', error_message)

    # Test 2: Boolean type for NUMERIC field (should fail)
    env.expect('FT.CREATE', 'type_val_idx2', 'ON', 'JSON', 'SCHEMA',
               '$.flag', 'AS', 'flag', 'NUMERIC').ok()

    env.expect('JSON.SET', 'type_val_error_doc2', '$',
               '{"flag": true}').ok()

    info = env.cmd('FT.INFO', 'type_val_idx2')
    info_dict = {info[i]: info[i+1] for i in range(0, len(info), 2)}
    env.assertIn('Index Errors', info_dict)
    errors = info_dict['Index Errors']
    error_dict = {errors[i]: errors[i+1] for i in range(0, len(errors), 2)}
    env.assertTrue(error_dict['indexing failures'] > 0)
    error_message = error_dict['last indexing error']
    env.assertIn('Invalid JSON type: Boolean type can be represent only TAG field', error_message)


@skip(no_json=True, cluster=True)
def test_object_type_validation_errors(env):
    """Test JSON object type validation errors"""

    # Test: Object type for TEXT field (should fail)
    env.expect('FT.CREATE', 'obj_val_idx', 'ON', 'JSON', 'SCHEMA',
               '$.data', 'AS', 'data', 'TEXT').ok()

    env.expect('JSON.SET', 'obj_val_error_doc', '$',
               '{"data": {"nested": "object"}}').ok()

    info = env.cmd('FT.INFO', 'obj_val_idx')
    info_dict = {info[i]: info[i+1] for i in range(0, len(info), 2)}
    env.assertIn('Index Errors', info_dict)
    errors = info_dict['Index Errors']
    error_dict = {errors[i]: errors[i+1] for i in range(0, len(errors), 2)}
    env.assertTrue(error_dict['indexing failures'] > 0)
    error_message = error_dict['last indexing error']
    env.assertIn('Invalid JSON type: Object type can represent only GEOMETRY field', error_message)


@skip(no_json=True, cluster=True)
def test_array_type_geometry_validation(env):
    """Test that arrays are properly rejected for GEOMETRY fields during type validation"""
    # Skip this test if GEOMETRY field type is not supported
    try:
        env.expect('FT.CREATE', 'geo_test_idx2', 'ON', 'JSON', 'SCHEMA',
                   '$.test', 'AS', 'test', 'GEOMETRY').ok()
        env.cmd('FT.DROPINDEX', 'geo_test_idx2')
    except Exception as e:
        if 'Invalid field type' in str(e):
            skipTest()  # Skip test if GEOMETRY is not supported
        else:
            raise
    """Test that arrays are properly rejected for GEOMETRY fields during type validation"""

    # Create an index with a GEOMETRY field
    env.expect('FT.CREATE', 'geo_array_idx', 'ON', 'JSON', 'SCHEMA',
               '$.shape', 'AS', 'shape', 'GEOMETRY').ok()

    # Try to index a document with an array for the geometry field
    env.expect('JSON.SET', 'geo_array_error_doc', '$',
               '{"shape": [{"type": "Point", "coordinates": [1, 2]}]}').ok()

    # Get index info to check for errors
    info = env.cmd('FT.INFO', 'geo_array_idx')
    info_dict = {info[i]: info[i+1] for i in range(0, len(info), 2)}

    # Verify that an error was recorded
    env.assertIn('Index Errors', info_dict)
    errors = info_dict['Index Errors']
    error_dict = {errors[i]: errors[i+1] for i in range(0, len(errors), 2)}

    # Check that we have indexing failures
    env.assertIn('indexing failures', error_dict)
    env.assertTrue(error_dict['indexing failures'] > 0)

    # Check that the error message is recorded and contains expected text
    env.assertIn('last indexing error', error_dict)
    error_message = error_dict['last indexing error']
    # This should trigger the type validation error first
    env.assertIn('Invalid JSON type: Array type cannot represent GEOMETRY field', error_message)

    # Check that the error key is recorded
    env.assertIn('last indexing error key', error_dict)
    env.assertEqual(error_dict['last indexing error key'], 'geo_array_error_doc')

