"""
Step definitions for RediSearchDisk document expiration tests.
"""
from common import *
import time


@then(parsers.parse('the index should have {count:d} expired async read'))
@then(parsers.parse('the index should have {count:d} expired async reads'))
def verify_expired_async_reads(redis_env, count):
    """Verify the async_reads_expired count in the search INFO."""
    info = redis_env.cmd('INFO', 'search')
    actual_count = info['search_disk_doc_table']['async_reads_expired']
    assert actual_count == count, f"Expected {count} expired async reads, got {actual_count}"


@when(parsers.parse('I set expiration on "{key}" to {seconds:d} seconds'))
def set_expiration(redis_env, key, seconds):
    """Set expiration on a document key using Redis EXPIRE command."""
    redis_env.cmd('EXPIRE', key, seconds)


@when(parsers.parse('I set expiration on "{key}" to {milliseconds:d} milliseconds'))
def set_expiration_ms(redis_env, key, milliseconds):
    """Set expiration on a document key using Redis PEXPIRE command."""
    redis_env.cmd('PEXPIRE', key, milliseconds)


@when(parsers.parse('I wait {seconds:d} seconds'))
def wait_seconds(seconds):
    """Wait for specified number of seconds."""
    time.sleep(seconds)


@when(parsers.parse('I wait {milliseconds:d} milliseconds'))
def wait_milliseconds(milliseconds):
    """Wait for specified number of milliseconds."""
    time.sleep(milliseconds / 1000.0)


@when('I disable active expiration')
def disable_active_expiration(redis_env):
    """Disable active expiration so keys only expire when accessed (lazy expiration)."""
    redis_env.cmd('DEBUG', 'SET-ACTIVE-EXPIRE', '0')


@when('I enable active expiration')
def enable_active_expiration(redis_env):
    """Enable active expiration so Redis actively expires keys."""
    redis_env.cmd('DEBUG', 'SET-ACTIVE-EXPIRE', '1')
