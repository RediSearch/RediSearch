"""
Flow test for disk path placement.

This test verifies that RediSearch disk storage is placed in the correct directory:
- The bigredis-path config (e.g., /path/to/flash/bigstore-1) is used to compute the disk path
- The disk path is: bigredis-path + "/redisearch" (e.g., /path/to/flash/bigstore-1/redisearch)
- Each index database is at: {disk_path}_{index_name}_{doc_type}
"""

from common import *

def get_expected_disk_path(bigredis_path: str, idx_name: str, doc_type: str) -> str:
    """
    Compute the expected disk path from bigredis-path.

    This mirrors the Rust compute_disk_path function:
    - Append "/redisearch_{idx_name}_{doc_type}" to bigredis-path

    Example: /tmp/test/redis.big -> /tmp/test/redis.big/redisearch_idx_hash
    """
    return str(Path(bigredis_path) / f"redisearch_{idx_name}_{doc_type}")


def get_bigredis_path(redis_env) -> str:
    """Get the bigredis-path configuration from Redis."""
    result = redis_env.cmd('CONFIG', 'GET', 'bigredis-path')
    if not result or len(result) < 2:
        raise ValueError("bigredis-path not configured")

    return result[1]


def test_disk_database_placed_in_correct_directory(redis_env):
    """
    Test that the disk database is created in the correct directory.

    The expected path is: bigredis-path/redisearch_{index_name}_{doc_type}
    """
    conn = redis_env.getConnection()

    # Get the bigredis-path configuration
    bigredis_path = get_bigredis_path(redis_env)

    # Compute the expected disk path
    index_name = "test_path_idx"
    expected_disk_path = get_expected_disk_path(bigredis_path, index_name, "hash")

    # Create an index - this should create the database in the expected location
    # SKIPINITIALSCAN is required for Flex indexes
    result = conn.execute_command(
        'FT.CREATE', index_name, 'ON', 'HASH', 'SKIPINITIALSCAN', 'SCHEMA',
        'title', 'TEXT'
    )
    redis_env.assertEqual(result, 'OK', message=f"FT.CREATE failed: {result}")

    # The database path should be: {expected_disk_path}_{index_name}_hash
    expected_db_path = f"{expected_disk_path}"

    waitForIndex(redis_env, index_name)

    # Verify the database directory exists
    redis_env.assertTrue(
        os.path.exists(expected_db_path),
        message=f"Database directory not found at expected path: {expected_db_path}\n"
                f"bigredis-path: {bigredis_path}\n"
                f"Expected disk path: {expected_disk_path}"
    )

    # Verify it's a directory (SpeedB creates a directory for the database)
    redis_env.assertTrue(
        os.path.isdir(expected_db_path),
        message=f"Expected a directory at {expected_db_path}, but it's not a directory"
    )
