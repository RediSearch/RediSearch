"""
Step definitions for RediSearchDisk RDB save/load tests.
"""
from common import *

@then(parsers.parse('the debug command "{command}" should return {expected_value:d}'))
def verify_debug_command_returns_int(redis_env, command, expected_value):
    """Verify that a debug command returns the expected integer value."""
    # Parse the command string (e.g., "_FT.DEBUG GET_MAX_DOC_ID idx")
    cmd_parts = command.split()
    result = redis_env.cmd(*cmd_parts)
    redis_env.assertEqual(result, expected_value)


@then(parsers.parse('the debug command "{command}" should return an empty array'))
def verify_debug_command_returns_empty_array(redis_env, command):
    """Verify that a debug command returns an empty array."""
    cmd_parts = command.split()
    result = redis_env.cmd(*cmd_parts)
    redis_env.assertEqual(result, [])


@then(parsers.parse('the debug command "{command}" should contain doc ID {doc_id:d}'))
def verify_debug_command_contains_single_id(redis_env, command, doc_id):
    """Verify that a debug command result contains a specific doc ID."""
    cmd_parts = command.split()
    result = redis_env.cmd(*cmd_parts)
    redis_env.assertTrue(isinstance(result, list))
    redis_env.assertIn(doc_id, result)
    redis_env.assertEqual(len(result), 1)


@then(parsers.parse('the max doc-id for "{index}" should be {expected_value:d}'))
def verify_max_doc_id(redis_env, index, expected_value):
    """Verify that the max doc-id for an index is as expected."""
    cmd_parts = ["_FT.DEBUG", "GET_MAX_DOC_ID", index]
    result = redis_env.cmd(*cmd_parts)
    redis_env.assertEqual(result, expected_value)


@then(parsers.parse('the deleted-ids for "{index}" should be {ids}'))
def verify_deleted_ids(redis_env, index, ids):
    """Verify that the deleted-ids set for an index contains specific IDs."""
    cmd_parts = ["_FT.DEBUG", "DUMP_DELETED_IDS", index]
    result = redis_env.cmd(*cmd_parts)
    redis_env.assertTrue(isinstance(result, list))

    if ids == "empty" or ids == "[]":
        redis_env.assertEqual(result, [])
        return

    # Parse the comma-separated list of IDs
    expected_ids = sorted([int(id_str.strip()) for id_str in ids[1:-1].split(',')])
    actual_ids = sorted(result)
    redis_env.assertEqual(actual_ids, expected_ids)
