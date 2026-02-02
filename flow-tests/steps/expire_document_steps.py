"""
Step definitions for RediSearchDisk document expiration tests.
"""
from common import *
import time


@when(parsers.parse('I set expiration on "{key}" to {seconds:d} seconds'))
def set_expiration(redis_env, key, seconds):
    """Set expiration on a document key using Redis EXPIRE command."""
    redis_env.cmd('EXPIRE', key, seconds)


@when(parsers.parse('I wait {seconds:d} seconds'))
def wait_seconds(seconds):
    """Wait for specified number of seconds."""
    time.sleep(seconds)
