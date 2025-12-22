"""
Basic functionality tests for RediSearchDisk module.

This module loads scenarios from the basic.feature file and uses
step definitions from the steps package.
"""
from pytest_bdd import scenarios

# Import step definitions to ensure they're registered
from steps import basic_steps  # noqa: F401

# Load all scenarios from the basic feature file
scenarios('features/basic.feature')

