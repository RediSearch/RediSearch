from common import *

"""
Tests for search-disk-drop-read-cache and search-disk-use-direct-reads module configs.

These configs control SpeedB OS page-cache behaviour for disk indexes:
  - search-disk-drop-read-cache: whether to drop the OS read cache after each read
  - search-disk-use-direct-reads: whether to use O_DIRECT for reads

Both are immutable (load-time only) and default to 'no'.

Per MOD-15866, the config hierarchy is:
  1. Explicit RSE value (search-disk-drop-read-cache / search-disk-use-direct-reads).
  2. Flex fallback for drop_read_cache only: bigredis-driver-allow_os_buffer read via CONFIG GET
     at SearchDisk_Initialize time.  allow_os_buffer=0 → drop_read_cache=true (inverted).
     Active only when search-disk-drop-read-cache is NOT explicitly set.
     Not testable here without a live Flex environment.
  3. SpeedB default: false (OS caching enabled).

Note: bigredis-driver-use_direct_reads does not exist in bs_rocksdb.c, so
search-disk-use-direct-reads has no Flex fallback (tiers 1 and 3 only).
"""


@skip(cluster=True, redis_less_than="7.2")
def test_disk_config_defaults():
    """Both configs default to 'no' when no module arg is provided."""
    env = Env(noDefaultModuleArgs=True)
    if env.env == 'existing-env':
        env.skip()
    env.expect('CONFIG', 'GET', 'search-disk-drop-read-cache').equal(
        ['search-disk-drop-read-cache', 'no'])
    env.expect('CONFIG', 'GET', 'search-disk-use-direct-reads').equal(
        ['search-disk-use-direct-reads', 'no'])


@skip(cluster=True, redis_less_than="7.2")
def test_disk_config_immutable(env):
    """Both configs are immutable — runtime CONFIG SET must be rejected."""
    env.expect('CONFIG', 'SET', 'search-disk-drop-read-cache', 'yes').error()
    env.expect('CONFIG', 'SET', 'search-disk-use-direct-reads', 'yes').error()


@skip(cluster=True, redis_less_than="7.2")
def test_disk_config_startup_drop_read_cache_explicit_yes():
    """Explicit yes at load time is reflected in CONFIG GET and bypasses Flex fallback."""
    env = Env(moduleArgs='search-disk-drop-read-cache yes', noDefaultModuleArgs=True)
    if env.env == 'existing-env':
        env.skip()
    env.expect('CONFIG', 'GET', 'search-disk-drop-read-cache').equal(
        ['search-disk-drop-read-cache', 'yes'])
    env.stop()


@skip(cluster=True, redis_less_than="7.2")
def test_disk_config_startup_drop_read_cache_explicit_no():
    """Explicit no at load time is honoured and prevents Flex fallback from overriding it."""
    env = Env(moduleArgs='search-disk-drop-read-cache no', noDefaultModuleArgs=True)
    if env.env == 'existing-env':
        env.skip()
    env.expect('CONFIG', 'GET', 'search-disk-drop-read-cache').equal(
        ['search-disk-drop-read-cache', 'no'])
    env.stop()


@skip(cluster=True, redis_less_than="7.2")
def test_disk_config_startup_use_direct_reads():
    """search-disk-use-direct-reads=yes set at load time is reflected in CONFIG GET."""
    env = Env(moduleArgs='search-disk-use-direct-reads yes', noDefaultModuleArgs=True)
    if env.env == 'existing-env':
        env.skip()
    env.expect('CONFIG', 'GET', 'search-disk-use-direct-reads').equal(
        ['search-disk-use-direct-reads', 'yes'])
    env.stop()


@skip(cluster=True, redis_less_than="7.2")
def test_disk_config_startup_use_direct_reads_explicit_no():
    """Explicit no for use-direct-reads is honoured (tiers 1 and 3 only, no Flex fallback)."""
    env = Env(moduleArgs='search-disk-use-direct-reads no', noDefaultModuleArgs=True)
    if env.env == 'existing-env':
        env.skip()
    env.expect('CONFIG', 'GET', 'search-disk-use-direct-reads').equal(
        ['search-disk-use-direct-reads', 'no'])
    env.stop()


@skip(cluster=True, redis_less_than="7.2")
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
