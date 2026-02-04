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


@when(parsers.parse('I apply reload policy "{reload_policy}"'))
def apply_reload_policy(redis_env, reload_policy):
    """Apply a reload policy.

    Supported policies:
    - "none": No reload, continue with current state
    - "rdb": Restart Redis and reload from RDB
    """
    if reload_policy == "none":
        # No-op: continue with current state
        pass
    elif reload_policy == "rdb":
        # Reload using SST files (so the correct context-flags are set)
        redis_env.cmd('DEBUG', 'RELOAD', 'HOT')

        # Configure search cluster settings
        conn = redis_env.getConnection()
        configure_search_cluster_single_shard(conn, redis_env.port)
    else:
        raise ValueError(f"Unknown reload policy: {reload_policy}. Supported: none, rdb")
