"""
Wildcard iterator tests for RediSearchDisk module.

This module loads scenarios from the wildcard.feature file and uses
step definitions from the steps package.
"""
from pytest_bdd import scenarios

# Import step definitions to ensure they're registered
from steps import basic_steps  # noqa: F401

# Load all scenarios from the wildcard feature file
scenarios('features/wildcard.feature')

