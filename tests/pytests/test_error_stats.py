from common import *

def test_search_error_stats_tracking(env):
    """
    Test SEARCH_ error format and Redis errorstats tracking.
    Validates both error message format and Redis error statistics.
    """
    # Test 1: SEARCH_INDEX_NOT_FOUND error format
    env.expect('FT.SEARCH', 'nonexistent_index', '*') \
        .error().apply(lambda e: str(e).startswith('SEARCH_INDEX_NOT_FOUND:')).true()

    # Test 2: SEARCH_ARG_UNRECOGNIZED error format
    env.expect('FT.CREATE', 'test_idx', 'SCHEMA', 'title', 'TEXT').ok()
    env.expect('FT.DROPINDEX', 'test_idx', 'INVALID_ARG') \
        .error().apply(lambda e: str(e).startswith('SEARCH_ARG_UNRECOGNIZED:')).true()

    # Test 3: Validate Redis errorstats tracking with correct counts
    final_stats = env.cmd('INFO', 'errorstats')
    # Check SEARCH_INDEX_NOT_FOUND appears with count=1
    index_error_key = None
    for key in final_stats.keys():
        if key.startswith('errorstat_SEARCH_INDEX_NOT_FOUND_'):
            index_error_key = key
            break

    env.assertTrue(index_error_key is not None, message="SEARCH_INDEX_NOT_FOUND should appear in errorstats")
    env.assertEqual(final_stats[index_error_key]['count'], 1, message="SEARCH_INDEX_NOT_FOUND count should be 1")

    # Check SEARCH_ARG_UNRECOGNIZED appears with count=1
    arg_error_key = None
    for key in final_stats.keys():
        if key.startswith('errorstat_SEARCH_ARG_UNRECOGNIZED_'):
            arg_error_key = key
            break

    env.assertTrue(arg_error_key is not None, message="SEARCH_ARG_UNRECOGNIZED should appear in errorstats")
    env.assertEqual(final_stats[arg_error_key]['count'], 1, message="SEARCH_ARG_UNRECOGNIZED count should be 1")
