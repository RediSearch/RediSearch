"""
Async IO flow tests for RediSearchDisk module.

This module tests that the async IO path can be controlled via FT.DEBUG DISK_IO_CONTROL
and that both async and sync read paths work correctly.
"""
from pytest_bdd import scenarios

# Import step definitions to ensure they're registered
from steps import basic_steps  # noqa: F401
from steps import async_io_steps  # noqa: F401

# Load all scenarios from the async_io feature file
scenarios('features/async_io.feature')

