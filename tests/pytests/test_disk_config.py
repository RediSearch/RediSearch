from common import *

"""
Tests for search-disk-drop-read-cache and search-disk-use-direct-reads module configs.

These configs control SpeedB OS page-cache behaviour for disk indexes:
  - search-disk-drop-read-cache: whether to drop the OS read cache after each read
  - search-disk-use-direct-reads: whether to use O_DIRECT for reads

Both are immutable (load-time only) and default to 'no'.  They are RSE-only knobs;
there is no fallback to any Flex bigredis-driver setting (decision per MOD-15866 review).
"""


@skip(cluster=True, redis_less_than="8.8")
def test_disk_config_defaults():
    """Both configs default to 'no' when no module arg is provided."""
    env = Env(noDefaultModuleArgs=True)
    if env.env == 'existing-env':
        env.skip()
    env.expect('CONFIG', 'GET', 'search-disk-drop-read-cache').equal(
        ['search-disk-drop-read-cache', 'no'])
    env.expect('CONFIG', 'GET', 'search-disk-use-direct-reads').equal(
        ['search-disk-use-direct-reads', 'no'])


@skip(cluster=True, redis_less_than="8.8")
def test_disk_config_immutable(env):
    """Both configs are immutable — runtime CONFIG SET must be rejected."""
    env.expect('CONFIG', 'SET', 'search-disk-drop-read-cache', 'yes').error()
    env.expect('CONFIG', 'SET', 'search-disk-use-direct-reads', 'yes').error()


@skip(cluster=True, redis_less_than="8.8")
def test_disk_config_startup_drop_read_cache_yes():
    """search-disk-drop-read-cache=yes set at load time is reflected in CONFIG GET."""
    env = Env(moduleArgs='search-disk-drop-read-cache yes', noDefaultModuleArgs=True)
    if env.env == 'existing-env':
        env.skip()
    env.expect('CONFIG', 'GET', 'search-disk-drop-read-cache').equal(
        ['search-disk-drop-read-cache', 'yes'])
    env.stop()


@skip(cluster=True, redis_less_than="8.8")
def test_disk_config_startup_drop_read_cache_no():
    """search-disk-drop-read-cache=no set at load time is reflected in CONFIG GET."""
    env = Env(moduleArgs='search-disk-drop-read-cache no', noDefaultModuleArgs=True)
    if env.env == 'existing-env':
        env.skip()
    env.expect('CONFIG', 'GET', 'search-disk-drop-read-cache').equal(
        ['search-disk-drop-read-cache', 'no'])
    env.stop()


@skip(cluster=True, redis_less_than="8.8")
def test_disk_config_startup_use_direct_reads_yes():
    """search-disk-use-direct-reads=yes set at load time is reflected in CONFIG GET."""
    env = Env(moduleArgs='search-disk-use-direct-reads yes', noDefaultModuleArgs=True)
    if env.env == 'existing-env':
        env.skip()
    env.expect('CONFIG', 'GET', 'search-disk-use-direct-reads').equal(
        ['search-disk-use-direct-reads', 'yes'])
    env.stop()


@skip(cluster=True, redis_less_than="8.8")
def test_disk_config_startup_use_direct_reads_no():
    """search-disk-use-direct-reads=no set at load time is reflected in CONFIG GET."""
    env = Env(moduleArgs='search-disk-use-direct-reads no', noDefaultModuleArgs=True)
    if env.env == 'existing-env':
        env.skip()
    env.expect('CONFIG', 'GET', 'search-disk-use-direct-reads').equal(
        ['search-disk-use-direct-reads', 'no'])
    env.stop()


@skip(cluster=True, redis_less_than="8.8")
def test_disk_config_startup_both():
    """Both configs can be set together at load time."""
    env = Env(moduleArgs='search-disk-drop-read-cache yes search-disk-use-direct-reads yes',
              noDefaultModuleArgs=True)
    if env.env == 'existing-env':
        env.skip()
    env.expect('CONFIG', 'GET', 'search-disk-drop-read-cache').equal(
        ['search-disk-drop-read-cache', 'yes'])
    env.expect('CONFIG', 'GET', 'search-disk-use-direct-reads').equal(
        ['search-disk-use-direct-reads', 'yes'])
    env.stop()
