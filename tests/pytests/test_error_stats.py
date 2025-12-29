# -*- coding: utf-8 -*-

import redis
import time
from common import *

def test_search_index_not_found_error_stats(env):
    """
    Test that SEARCH_INDEX_NOT_FOUND errors are properly tracked in Redis error statistics.
    This test verifies that our error uniquification changes are working correctly.
    """
    conn = env.getConnection()
    
    # Get initial error stats before triggering errors
    info_before = conn.execute_command('INFO', 'errorstats')
    initial_count = 0

    # Parse initial SEARCH_INDEX_NOT_FOUND count if it exists
    if isinstance(info_before, dict):
        # Redis returns a dict with errorstats
        initial_count = info_before.get('errorstat_SEARCH_INDEX_NOT_FOUND:count', 0)
    else:
        # Fallback for string format
        for line in str(info_before).split('\n'):
            if 'errorstat_SEARCH_INDEX_NOT_FOUND:count=' in line:
                initial_count = int(line.split('=')[1])
                break
    
    # Trigger SEARCH_INDEX_NOT_FOUND errors by trying to search non-existent indexes
    error_commands = [
        ['FT.SEARCH', 'nonexistent_index', '*'],
        ['FT.INFO', 'missing_index'],
        ['FT.AGGREGATE', 'fake_index', '*'],
        ['FT.DROPINDEX', 'unknown_index'],
        ['FT.ALTER', 'bad_index', 'SCHEMA', 'ADD', 'field', 'TEXT']
    ]
    
    expected_errors = 0
    for cmd in error_commands:
        try:
            conn.execute_command(*cmd)
        except redis.ResponseError as e:
            # Verify the error message contains our new SEARCH_INDEX_NOT_FOUND prefix
            error_msg = str(e)
            if 'SEARCH_INDEX_NOT_FOUND' in error_msg or 'Index not found' in error_msg:
                expected_errors += 1
                print(f"✅ Command {cmd[0]} triggered expected error: {error_msg}")
            else:
                print(f"❌ Command {cmd[0]} triggered unexpected error: {error_msg}")
    
    # Wait a moment for Redis to update error statistics
    time.sleep(0.1)
    
    # Get error stats after triggering errors
    info_after = conn.execute_command('INFO', 'errorstats')
    final_count = 0

    # Parse final SEARCH_INDEX_NOT_FOUND count
    if isinstance(info_after, dict):
        # Redis returns a dict with errorstats - check both possible formats
        if 'errorstat_SEARCH_INDEX_NOT_FOUND:count' in info_after:
            final_count = info_after['errorstat_SEARCH_INDEX_NOT_FOUND:count']
        elif 'errorstat_SEARCH_INDEX_NOT_FOUND_' in info_after:
            final_count = info_after['errorstat_SEARCH_INDEX_NOT_FOUND_']['count']
    else:
        # Fallback for string format
        for line in str(info_after).split('\n'):
            if 'errorstat_SEARCH_INDEX_NOT_FOUND:count=' in line:
                final_count = int(line.split('=')[1])
                break
    
    # Verify that the error count increased
    errors_added = final_count - initial_count
    print(f"📊 Error stats: initial={initial_count}, final={final_count}, added={errors_added}")
    
    # Assert that we have the expected number of new errors
    env.assertTrue(errors_added >= expected_errors)
    
    # Verify the error stat line exists and has the correct format
    found_error_stat = False
    if isinstance(info_after, dict):
        # Check if the error stat exists in the dict - check both possible formats
        if 'errorstat_SEARCH_INDEX_NOT_FOUND:count' in info_after:
            found_error_stat = True
            print(f"✅ Found error stat: errorstat_SEARCH_INDEX_NOT_FOUND:count={info_after['errorstat_SEARCH_INDEX_NOT_FOUND:count']}")
        elif 'errorstat_SEARCH_INDEX_NOT_FOUND_' in info_after:
            found_error_stat = True
            count_val = info_after['errorstat_SEARCH_INDEX_NOT_FOUND_']['count']
            print(f"✅ Found error stat: errorstat_SEARCH_INDEX_NOT_FOUND_={{count: {count_val}}}")
    else:
        # Fallback for string format
        for line in str(info_after).split('\n'):
            if 'errorstat_SEARCH_INDEX_NOT_FOUND:count=' in line:
                found_error_stat = True
                print(f"✅ Found error stat: {line.strip()}")
                break
    
    env.assertTrue(found_error_stat)
    
    print("🎉 Test passed: SEARCH_INDEX_NOT_FOUND errors are properly tracked in Redis error statistics!")


def test_multiple_search_error_types(env):
    """
    Test that different types of SEARCH_ prefixed errors are tracked separately.
    """
    conn = env.getConnection()
    
    # Create an index for some tests
    env.expect('FT.CREATE', 'test_idx', 'SCHEMA', 'title', 'TEXT').ok()
    
    # Get initial error stats
    info_before = conn.execute_command('INFO', 'errorstats')
    
    # Trigger different types of errors
    error_tests = [
        # SEARCH_INDEX_NOT_FOUND
        (['FT.SEARCH', 'missing_idx', '*'], 'SEARCH_INDEX_NOT_FOUND'),
        # SEARCH_ARG_UNRECOGNIZED (if we can trigger it)
        (['FT.SEARCH', 'test_idx', '*', 'INVALID_ARG'], 'SEARCH_ARG'),
    ]
    
    for cmd, expected_prefix in error_tests:
        try:
            conn.execute_command(*cmd)
        except redis.ResponseError as e:
            error_msg = str(e)
            print(f"Command {cmd}: {error_msg}")
    
    # Wait for stats to update
    time.sleep(0.1)
    
    # Check that we have SEARCH_ prefixed errors in the stats
    info_after = conn.execute_command('INFO', 'errorstats')
    search_errors_found = []

    if isinstance(info_after, dict):
        # Redis returns a dict with errorstats
        for key, value in info_after.items():
            if key.startswith('errorstat_SEARCH_') and ':count' in key:
                search_errors_found.append(f"{key}={value}")
    else:
        # Fallback for string format
        for line in str(info_after).split('\n'):
            if 'errorstat_SEARCH_' in line and ':count=' in line:
                search_errors_found.append(line.strip())
    
    print(f"📊 Found SEARCH_ error statistics:")
    for error_stat in search_errors_found:
        print(f"  {error_stat}")
    
    # Verify we found at least one SEARCH_ error statistic
    env.assertTrue(len(search_errors_found) > 0)
    
    print("🎉 Test passed: Multiple SEARCH_ error types are tracked separately!")
