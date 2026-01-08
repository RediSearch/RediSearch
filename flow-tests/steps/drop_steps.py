"""
Step definitions for RediSearchDisk index deletion tests.
"""
from common import *

@when(parsers.parse('I drop the index "{index_name}"'))
def drop_index(redis_env, index_name):
    """Drop an index."""
    redis_env.expect('FT.DROPINDEX', index_name).ok()


@then(parsers.parse('the index "{index_name}" should not exist'))
def index_should_not_exist(redis_env, index_name):
    """Verify that an index does not exist."""
    # Try to get info about the index - should fail
    try:
        redis_env.cmd('FT.INFO', index_name)
        assert False, f"Index {index_name} should not exist but FT.INFO succeeded"
    except Exception:
        # Expected - index doesn't exist
        pass


@then(parsers.parse('the index "{index_name}" database files should be deleted'))
def database_files_should_be_deleted(redis_env, index_name):
    """Verify that the database files for an index have been deleted from disk."""
    # Get the bigredis-path from redis config
    # This is where RediSearchDisk stores its database files
    bigredis_path_result = redis_env.cmd('CONFIG', 'GET', 'bigredis-path')
    if not bigredis_path_result or len(bigredis_path_result) < 2:
        # Fallback to dir if bigredis-path is not set
        bigredis_path = redis_env.cmd('CONFIG', 'GET', 'dir')[1]
    else:
        bigredis_path = bigredis_path_result[1]

    # Convert bytes to string if necessary
    if isinstance(bigredis_path, bytes):
        bigredis_path = bigredis_path.decode('utf-8')

    # The index database directory pattern is: {bigredis_path}_{index_name}_{doc_type}
    # The Rust code appends the suffix directly to the base path (not as a subdirectory)
    # For example: /tmp/bigredis_myindex_hash (not /tmp/bigredis/myindex_hash)
    index_db_path_hash = f"{bigredis_path}_{index_name}_hash"
    index_db_path_json = f"{bigredis_path}_{index_name}_json"

    # Check that neither path exists
    assert not os.path.exists(index_db_path_hash), \
        f"Database files for index {index_name} (HASH) should be deleted but found at {index_db_path_hash}"
    assert not os.path.exists(index_db_path_json), \
        f"Database files for index {index_name} (JSON) should be deleted but found at {index_db_path_json}"

