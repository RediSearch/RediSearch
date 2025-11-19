from common import *

def test_search_error_stats_tracking(env):
    """
    Test SEARCH_ error format and Redis errorstats tracking.
    Validates both error message format and Redis error statistics.
    """
    conn = env.getConnection()

    # Test 1: SEARCH_INDEX_NOT_FOUND error format
    try:
        conn.execute_command('FT.SEARCH', 'nonexistent_index', '*')
        env.assertTrue(False, message="Expected SEARCH_INDEX_NOT_FOUND error but command succeeded")
    except Exception as e:
        error_msg = str(e)
        env.assertTrue(error_msg.startswith('SEARCH_INDEX_NOT_FOUND:'),
                      message=f"Error should start with 'SEARCH_INDEX_NOT_FOUND:' but got: {error_msg}")

    # Test 2: SEARCH_ARG_UNRECOGNIZED error format
    conn.execute_command('FT.CREATE', 'test_idx', 'SCHEMA', 'title', 'TEXT')
    try:
        conn.execute_command('FT.DROPINDEX', 'test_idx', 'INVALID_ARG')
        env.assertTrue(False, "Expected SEARCH_ARG_UNRECOGNIZED error but command succeeded")
    except Exception as e:
        error_msg = str(e)
        env.assertTrue(error_msg.startswith('SEARCH_ARG_UNRECOGNIZED:'),
                      message=f"Error should start with 'SEARCH_ARG_UNRECOGNIZED:' but got: {error_msg}")

    # Test 3: Validate Redis errorstats tracking with correct counts
    final_stats = conn.execute_command('INFO', 'errorstats')
    # Check SEARCH_INDEX_NOT_FOUND appears with count=1
    index_error_key = None
    for key in final_stats.keys():
        if key.startswith('errorstat_SEARCH_INDEX_NOT_FOUND_'):
            index_error_key = key
            break

    env.assertTrue(index_error_key is not None, message="SEARCH_INDEX_NOT_FOUND should appear in errorstats")
    env.assertEqual(final_stats[index_error_key], 1, message="SEARCH_INDEX_NOT_FOUND count should be 1")

    # Check SEARCH_ARG_UNRECOGNIZED appears with count=1
    arg_error_key = None
    for key in final_stats.keys():
        if key.startswith('errorstat_SEARCH_ARG_UNRECOGNIZED_'):
            arg_error_key = key
            break

    env.assertTrue(arg_error_key is not None, message="SEARCH_ARG_UNRECOGNIZED should appear in errorstats")
    env.assertEqual(final_stats[arg_error_key], 1, message="SEARCH_ARG_UNRECOGNIZED count should be 1")
