"""
Step definitions for RediSearchDisk RDB save/load tests.
"""
from common import *

@when('I restart Redis and reload the RDB')
def restart_and_reload(redis_env):
    """Restart Redis to reload from RDB file."""
    # Reload using SST files (so the correct context-flags are set)
    redis_env.cmd('DEBUG', 'RELOAD', 'HOT')

    # Configure search cluster settings
    # Get the connection to execute commands
    conn = redis_env.getConnection()

    configure_search_cluster_single_shard(conn, redis_env.port)
