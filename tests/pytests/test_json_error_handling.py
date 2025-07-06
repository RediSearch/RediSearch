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

