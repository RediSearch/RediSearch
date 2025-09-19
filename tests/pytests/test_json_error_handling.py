from common import *

@skip(no_json=True, cluster=True)
def test_geometry_field_array_type_validation_error(env):
    """Test GEOMETRY field array rejection at type validation level

    Note: The specific error 'GEOMETRY field does not support array type'
    from JSON_StoreInDocField (line 602) is currently unreachable because
    FieldSpec_CheckJsonType (line 134) catches arrays for GEOMETRY fields first.
    This test validates the type validation error that actually occurs.
    """

    # Create an index with a GEOSHAPE field
    env.expect('FT.CREATE', 'geo_idx', 'ON', 'JSON', 'SCHEMA',
               '$.location', 'AS', 'location', 'GEOSHAPE').ok()

    # Try to index a document with an array for the geometry field
    # This will trigger the type validation error, not the field processing error
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

    # Check that we get the type validation error (not the field processing error)
    env.assertIn('last indexing error', error_dict)
    error_message = error_dict['last indexing error']
    env.assertIn('Invalid JSON type: Array type cannot represent GEOMETRY field', error_message)

    # Check that the error key is recorded
    env.assertIn('last indexing error key', error_dict)
    env.assertEqual(error_dict['last indexing error key'], 'geo_error_doc')


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
def test_json_type_validation_errors(env):
    """Test JSON type validation errors from FieldSpec_CheckJsonType"""

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

    # Check that the error key is recorded
    env.assertIn('last indexing error key', error_dict)
    env.assertEqual(error_dict['last indexing error key'], 'obj_val_error_doc')

