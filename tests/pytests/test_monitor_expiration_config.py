import time
from common import *

"""
Tests for the search-monitor-expiration config parameter.
This config controls whether indexes track key and field expiration
(set via EXPIRE, EXPIREAT, HEXPIRE, etc.) and filter out expired
documents and fields from search results.

When disabled, the TTL dict is kept alive but new writes are stopped.
Pre-existing TTL data remains valid and continues to be checked at
query time. Re-enabling restores monitoring for new expirations;
expirations set during the disabled window are missed.
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
def test_monitor_expiration_disable_preserves_existing_ttl():
    """Test that disabling monitoring preserves pre-existing TTL data."""
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

    # Expire doc1 while monitoring is enabled
    conn.execute_command('PEXPIRE', 'doc1', 1)
    time.sleep(0.015)

    # With monitoring enabled, expired doc should be filtered from results
    res = conn.execute_command('FT.SEARCH', 'idx', '*', 'NOCONTENT')
    env.assertEqual(res, [1, 'doc2'])

    # Disable monitoring — pre-existing TTL data is kept
    conn.execute_command('CONFIG', 'SET', 'search-monitor-expiration', 'no')

    # doc1 still filtered because its TTL data persists
    res = conn.execute_command('FT.SEARCH', 'idx', '*', 'NOCONTENT')
    env.assertEqual(res, [1, 'doc2'])


@skip(cluster=True, redis_less_than="7.2")
def test_monitor_expiration_disable_stops_new_tracking():
    """Test that new expirations are not tracked while monitoring is disabled."""
    env = Env(noDefaultModuleArgs=True)
    if env.env == 'existing-env':
        env.skip()
    conn = env.getConnection()

    # Use lazy expire to control when documents actually expire
    conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')

    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT')
    conn.execute_command('HSET', 'doc1', 't', 'hello')

    # Disable monitoring before setting expiration
    conn.execute_command('CONFIG', 'SET', 'search-monitor-expiration', 'no')

    # Add and expire a new doc while monitoring is disabled
    conn.execute_command('HSET', 'doc2', 't', 'world')
    conn.execute_command('PEXPIRE', 'doc2', 1)
    time.sleep(0.015)

    # doc2 should appear because its expiration was not tracked
    res = conn.execute_command('FT.SEARCH', 'idx', '*', 'NOCONTENT')
    env.assertEqual(res[0], 2)


@skip(cluster=True, redis_less_than="7.2")
def test_monitor_expiration_new_index_respects_config():
    """Test that new indexes respect the current config value."""
    env = Env(noDefaultModuleArgs=True)
    if env.env == 'existing-env':
        env.skip()
    conn = env.getConnection()

    # Use lazy expire to control when documents actually expire
    conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')

    # Disable monitoring before creating index
    conn.execute_command('CONFIG', 'SET', 'search-monitor-expiration', 'no')

    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT')
    conn.execute_command('HSET', 'doc1', 't', 'hello')
    conn.execute_command('HSET', 'doc2', 't', 'world')

    conn.execute_command('PEXPIRE', 'doc1', 1)
    time.sleep(0.015)

    # Both docs should appear (no expiration tracking)
    res = conn.execute_command('FT.SEARCH', 'idx', '*', 'NOCONTENT')
    env.assertEqual(res[0], 2)


@skip(cluster=True, redis_less_than="7.2")
def test_monitor_expiration_enable_at_runtime():
    """Test enabling expiration monitoring at runtime for existing indexes."""
    env = Env(noDefaultModuleArgs=True)
    if env.env == 'existing-env':
        env.skip()
    conn = env.getConnection()

    # Use lazy expire to control when documents actually expire
    conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')

    # Start with monitoring disabled
    conn.execute_command('CONFIG', 'SET', 'search-monitor-expiration', 'no')

    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT')
    conn.execute_command('HSET', 'doc1', 't', 'hello')

    # Enable monitoring
    conn.execute_command('CONFIG', 'SET', 'search-monitor-expiration', 'yes')
    env.expect('CONFIG', 'GET', 'search-monitor-expiration').equal(
        ['search-monitor-expiration', 'yes'])

    # New expirations should be tracked after re-enable
    conn.execute_command('HSET', 'doc2', 't', 'world')
    conn.execute_command('PEXPIRE', 'doc2', 1)
    time.sleep(0.015)

    res = conn.execute_command('FT.SEARCH', 'idx', '*', 'NOCONTENT')
    env.assertEqual(res, [1, 'doc1'])


@skip(cluster=True, redis_less_than="7.2")
def test_monitor_expiration_multiple_indexes():
    """Test that config change affects all existing indexes."""
    env = Env(noDefaultModuleArgs=True)
    if env.env == 'existing-env':
        env.skip()
    conn = env.getConnection()

    # Use lazy expire to control when documents actually expire
    conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')

    # Create two indexes with monitoring enabled
    conn.execute_command('FT.CREATE', 'idx1', 'PREFIX', '1', 'a:', 'SCHEMA', 't', 'TEXT')
    conn.execute_command('FT.CREATE', 'idx2', 'PREFIX', '1', 'b:', 'SCHEMA', 't', 'TEXT')

    conn.execute_command('HSET', 'a:doc1', 't', 'hello')
    conn.execute_command('HSET', 'b:doc1', 't', 'world')

    conn.execute_command('PEXPIRE', 'a:doc1', 1)
    conn.execute_command('PEXPIRE', 'b:doc1', 1)
    time.sleep(0.015)

    env.assertEqual(conn.execute_command('FT.SEARCH', 'idx1', '*', 'NOCONTENT'), [0])
    env.assertEqual(conn.execute_command('FT.SEARCH', 'idx2', '*', 'NOCONTENT'), [0])

    # Disable monitoring — pre-existing TTL data is preserved
    conn.execute_command('CONFIG', 'SET', 'search-monitor-expiration', 'no')

    # Expired docs remain filtered (their TTL data persists)
    env.assertEqual(conn.execute_command('FT.SEARCH', 'idx1', '*', 'NOCONTENT'), [0])
    env.assertEqual(conn.execute_command('FT.SEARCH', 'idx2', '*', 'NOCONTENT'), [0])

    # New expirations while disabled are NOT tracked
    conn.execute_command('HSET', 'a:doc2', 't', 'foo')
    conn.execute_command('HSET', 'b:doc2', 't', 'bar')
    conn.execute_command('PEXPIRE', 'a:doc2', 1)
    conn.execute_command('PEXPIRE', 'b:doc2', 1)
    time.sleep(0.015)

    # New expired docs appear because their expirations were not tracked
    env.assertEqual(conn.execute_command('FT.SEARCH', 'idx1', '*', 'NOCONTENT')[0], 1)
    env.assertEqual(conn.execute_command('FT.SEARCH', 'idx2', '*', 'NOCONTENT')[0], 1)

