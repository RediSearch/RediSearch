"""
Step definitions for RediSearchDisk RDB save/load tests.
"""
from common import *

@when('I restart Redis and reload the RDB')
def restart_and_reload(redis_env):
    """Restart Redis to reload from RDB file."""
    # RLTest's restartAndReload() will:
    # 1. Save current state to RDB
    # 2. Stop Redis
    # 3. Start Redis again (which loads from RDB)
    # 4. Wait for the module to be ready
    redis_env.restartAndReload()

    # Configure search cluster settings
    # Get the connection to execute commands
    conn = redis_env.getConnection()

    configure_search_cluster_single_shard(conn, redis_env.port)
