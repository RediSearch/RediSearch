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

    env.assertTrue(index_error_key is not None,
                   message="SEARCH_INDEX_NOT_FOUND should appear in errorstats")
    env.assertEqual(final_stats[index_error_key]['count'], 1,
                    message="SEARCH_INDEX_NOT_FOUND count should be 1")

    # Check SEARCH_ARG_UNRECOGNIZED appears with count=1
    arg_error_key = None
    for key in final_stats.keys():
        if key.startswith('errorstat_SEARCH_ARG_UNRECOGNIZED_'):
            arg_error_key = key
            break

    env.assertTrue(arg_error_key is not None,
                   message="SEARCH_ARG_UNRECOGNIZED should appear in errorstats")

    # In cluster mode with multiple shards, FT.DROPINDEX uses
    # MastersFanoutCommandHandler which fans out _FT.DROPINDEX to all shards
    # without coordinator-level argument validation.
    # The error counting behavior is:
    # - Single shard / standalone: command executed locally, 1 error returned
    #   to client → count=1
    # - Multi-shard cluster: the coordinator executes _FT.DROPINDEX locally
    #   (1 error) AND returns an error to the client via allOKReducer
    #   (1 more error) → count=2 on coordinator
    # Note: Remote shards each count 1 error, but INFO errorstats only shows
    # the coordinator's stats.
    if env.isCluster() and env.shardsCount > 1:
        expected_count = 2
    else:
        expected_count = 1

    env.assertEqual(final_stats[arg_error_key]['count'], expected_count,
                    message=f"SEARCH_ARG_UNRECOGNIZED count should be {expected_count}")
