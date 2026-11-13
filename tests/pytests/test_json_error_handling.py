# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

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
    env.assertIn('Invalid JSON type: Object type can represent only GEOMETRY or SPARSEVECTOR fields', error_message)

    # Check that the error key is recorded
    env.assertIn('last indexing error key', error_dict)
    env.assertEqual(error_dict['last indexing error key'], 'obj_val_error_doc')



@skip(no_json=True, cluster=True)
def test_populated_array_freed_when_later_field_fails(env):
    """A populated multi-value field loads, then a later schema field fails, so
    the document is discarded. The stored array, and for SORTABLE the multi-value
    sorting value built at load time, must be freed: a leak is only reported
    under the sanitizer job."""

    conn = getConnectionByEnv(env)
    # (field, type, array, query matching the array)
    arrays = [
        ('tags', 'TAG', '["foo","bar"]', '@tags:{foo}'),
        ('nums', 'NUMERIC', '[1,2,3]', '@nums:[1 3]'),
    ]
    cases = [(name, ftype, arr, query, path, sortable)
             for name, ftype, arr, query in arrays
             for path in (f'$.{name}', f'$.{name}[*]')
             for sortable in ([], ['SORTABLE'])]

    for name, ftype, arr, query, path, sortable in cases:
        msg = f'{path} {ftype} {" ".join(sortable)}'
        # Fields load in schema order: the array field must precede `n`.
        env.expect('FT.CREATE', 'idx', 'ON', 'JSON', 'PREFIX', '1', 'doc:',
                   'SCHEMA', path, 'AS', name, ftype, *sortable,
                   '$.n', 'AS', 'n', 'NUMERIC').ok()

        # Control: the same array indexes when `n` is valid.
        conn.execute_command('JSON.SET', 'doc:ok', '$', f'{{"{name}": {arr}, "n": 1}}')
        env.expect('FT.SEARCH', 'idx', query, 'NOCONTENT').equal([1, 'doc:ok'], message=msg)

        conn.execute_command('JSON.SET', 'doc:bad', '$', f'{{"{name}": {arr}, "n": "invalid"}}')
        info = index_info(env, 'idx')
        env.assertEqual(info['hash_indexing_failures'], 1, message=msg)
        errors = to_dict(info['Index Errors'])
        env.assertEqual(errors['last indexing error key'], 'doc:bad', message=msg)
        env.assertContains('String type can represent only', errors['last indexing error'],
                           message=msg)
        env.expect('FT.SEARCH', 'idx', query, 'NOCONTENT').equal([1, 'doc:ok'], message=msg)
        env.flush()
