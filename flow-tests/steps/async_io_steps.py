"""
Step definitions for RediSearchDisk async IO control tests.
"""
from pytest_bdd import when, then, parsers
from common import *


@when('I disable async disk IO')
def disable_async_disk_io(redis_env):
    """Disable async disk IO using FT.DEBUG DISK_IO_CONTROL."""
    redis_env.cmd('_FT.DEBUG', 'DISK_IO_CONTROL', 'disable')


@when('I enable async disk IO')
def enable_async_disk_io(redis_env):
    """Enable async disk IO using FT.DEBUG DISK_IO_CONTROL."""
    redis_env.cmd('_FT.DEBUG', 'DISK_IO_CONTROL', 'enable')


@then(parsers.parse('async disk IO status should be "{expected_status}"'))
def verify_async_disk_io_status(redis_env, expected_status):
    """Verify the async disk IO status."""
    result = redis_env.cmd('_FT.DEBUG', 'DISK_IO_CONTROL', 'status')
    # Response format is "Async I/O: <status>", check that expected_status is in result
    redis_env.assertContains(expected_status, result)

