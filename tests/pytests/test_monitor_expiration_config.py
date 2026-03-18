import time
from common import *

"""
Tests for the search-monitor-expiration config parameter.
This config controls whether indexes track key and field expiration
(set via EXPIRE, EXPIREAT, HEXPIRE, etc.) and filter out expired
documents and fields from search results.
"""

@skip(cluster=True, redis_less_than="7.2")
def test_monitor_expiration_config_default():
    """Test that the config defaults to 'yes' (enabled)."""
    env = Env(noDefaultModuleArgs=True)
    if env.env == 'existing-env':
        env.skip()
    env.expect('CONFIG', 'GET', 'search-monitor-expiration').equal(
        ['search-monitor-expiration', 'yes'])


@skip(cluster=True, redis_less_than="7.2")
def test_monitor_expiration_disable_at_runtime():
    """Test disabling expiration monitoring at runtime cleans up TTL tables."""
    env = Env(noDefaultModuleArgs=True)
    if env.env == 'existing-env':
        env.skip()
    conn = env.getConnection()

    # Use lazy expire to control when documents actually expire
    conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')

    # Create index with default config (monitoring enabled)
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT')
    conn.execute_command('HSET', 'doc1', 't', 'hello')
    conn.execute_command('HSET', 'doc2', 't', 'world')

    # Expire doc1
    conn.execute_command('PEXPIRE', 'doc1', 1)
    time.sleep(0.015)

    # With monitoring enabled, expired doc should be filtered from results
    res = conn.execute_command('FT.SEARCH', 'idx', '*', 'NOCONTENT')
    env.assertEqual(res, [1, 'doc2'])

    # Disable monitoring at runtime
    conn.execute_command('CONFIG', 'SET', 'search-monitor-expiration', 'no')
    env.expect('CONFIG', 'GET', 'search-monitor-expiration').equal(
        ['search-monitor-expiration', 'no'])

    # After disabling, expired docs should appear in results (TTL table cleared)
    # Note: The doc is still lazily expired in Redis, but we no longer track it
    res = conn.execute_command('FT.SEARCH', 'idx', '*', 'NOCONTENT')
    # Both docs appear since TTL tracking is disabled
    env.assertEqual(res[0], 2)


@skip(cluster=True, redis_less_than="7.2")
def test_monitor_expiration_new_index_respects_config():
    """Test that new indexes respect the current config value."""
    env = Env(noDefaultModuleArgs=True)
    if env.env == 'existing-env':
        env.skip()
    conn = env.getConnection()

    # Use lazy expire
    conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')

    # Disable monitoring before creating index
    conn.execute_command('CONFIG', 'SET', 'search-monitor-expiration', 'no')

    # Create index - should not track expirations
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT')
    conn.execute_command('HSET', 'doc1', 't', 'hello')
    conn.execute_command('HSET', 'doc2', 't', 'world')

    # Expire doc1
    conn.execute_command('PEXPIRE', 'doc1', 1)
    time.sleep(0.015)

    # Both docs should appear (no expiration filtering)
    res = conn.execute_command('FT.SEARCH', 'idx', '*', 'NOCONTENT')
    env.assertEqual(res[0], 2)


@skip(cluster=True, redis_less_than="7.2")
def test_monitor_expiration_enable_at_runtime():
    """Test enabling expiration monitoring at runtime for existing indexes."""
    env = Env(noDefaultModuleArgs=True)
    if env.env == 'existing-env':
        env.skip()
    conn = env.getConnection()

    # Use lazy expire
    conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')

    # Start with monitoring disabled
    conn.execute_command('CONFIG', 'SET', 'search-monitor-expiration', 'no')

    # Create index
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT')
    conn.execute_command('HSET', 'doc1', 't', 'hello')

    # Enable monitoring
    conn.execute_command('CONFIG', 'SET', 'search-monitor-expiration', 'yes')
    env.expect('CONFIG', 'GET', 'search-monitor-expiration').equal(
        ['search-monitor-expiration', 'yes'])

    # Add new document and expire it
    conn.execute_command('HSET', 'doc2', 't', 'world')
    conn.execute_command('PEXPIRE', 'doc2', 1)
    time.sleep(0.015)

    # New expired doc should be filtered (monitoring now enabled)
    res = conn.execute_command('FT.SEARCH', 'idx', '*', 'NOCONTENT')
    env.assertEqual(res, [1, 'doc1'])


@skip(cluster=True, redis_less_than="7.2")
def test_monitor_expiration_multiple_indexes():
    """Test that config change affects all existing indexes."""
    env = Env(noDefaultModuleArgs=True)
    if env.env == 'existing-env':
        env.skip()
    conn = env.getConnection()

    conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')

    # Create two indexes with monitoring enabled
    conn.execute_command('FT.CREATE', 'idx1', 'PREFIX', '1', 'a:', 'SCHEMA', 't', 'TEXT')
    conn.execute_command('FT.CREATE', 'idx2', 'PREFIX', '1', 'b:', 'SCHEMA', 't', 'TEXT')

    conn.execute_command('HSET', 'a:doc1', 't', 'hello')
    conn.execute_command('HSET', 'b:doc1', 't', 'world')

    conn.execute_command('PEXPIRE', 'a:doc1', 1)
    conn.execute_command('PEXPIRE', 'b:doc1', 1)
    time.sleep(0.015)

    # Both indexes should filter expired docs
    env.assertEqual(conn.execute_command('FT.SEARCH', 'idx1', '*', 'NOCONTENT'), [0])
    env.assertEqual(conn.execute_command('FT.SEARCH', 'idx2', '*', 'NOCONTENT'), [0])

    # Disable monitoring - should affect both indexes
    conn.execute_command('CONFIG', 'SET', 'search-monitor-expiration', 'no')

    # Both indexes should now show the expired docs
    env.assertEqual(conn.execute_command('FT.SEARCH', 'idx1', '*', 'NOCONTENT')[0], 1)
    env.assertEqual(conn.execute_command('FT.SEARCH', 'idx2', '*', 'NOCONTENT')[0], 1)

