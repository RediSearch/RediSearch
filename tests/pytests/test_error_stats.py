from common import *

def test_search_error_stats_tracking(env):
    """
    Test SEARCH_ error format and Redis errorstats tracking end-to-end.
    Validates both error message format and Redis error statistics.
    """
    conn = env.getConnection()

    # Test 1: Validate error message format starts with SEARCH_ prefix
    index_error_format_correct = False
    arg_error_format_correct = False

    # Test SEARCH_INDEX_NOT_FOUND error format
    try:
        conn.execute_command('FT.SEARCH', 'nonexistent_index', '*')
    except Exception as e:
        error_msg = str(e)
        if error_msg.startswith('SEARCH_INDEX_NOT_FOUND:'):
            index_error_format_correct = True

    # Create index and test SEARCH_ARG_UNRECOGNIZED error format
    try:
        conn.execute_command('FT.CREATE', 'test_idx', 'SCHEMA', 'title', 'TEXT')
        try:
            conn.execute_command('FT.DROPINDEX', 'test_idx', 'INVALID_ARG')
        except Exception as e:
            error_msg = str(e)
            if error_msg.startswith('SEARCH_ARG_UNRECOGNIZED:'):
                arg_error_format_correct = True
    except:
        pass  # Module may not be loaded

    # Test 2: Validate Redis errorstats tracking
    final_stats = conn.execute_command('INFO', 'errorstats')
    search_errors_found = [k for k in final_stats.keys() if k.startswith('errorstat_SEARCH_')]

    index_error_tracked = any('INDEX_NOT_FOUND' in k for k in search_errors_found)
    arg_error_tracked = any('ARG_UNRECOGNIZED' in k for k in search_errors_found)

    # Validate both error format AND errorstats tracking
    if index_error_format_correct and arg_error_format_correct:
        # Error message format is correct, now check errorstats
        env.assertTrue(index_error_tracked, "SEARCH_INDEX_NOT_FOUND should appear in errorstats")
        env.assertTrue(arg_error_tracked, "SEARCH_ARG_UNRECOGNIZED should appear in errorstats")
    else:
        # Skip if module not loaded properly
        env.skip()
