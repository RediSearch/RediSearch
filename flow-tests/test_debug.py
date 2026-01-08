"""
Debug commands' functionality tests for RediSearchDisk module.

This module loads scenarios from the debug.feature file and uses step
definitions from the steps package.
"""
from pytest_bdd import scenarios

# Load all scenarios from the basic feature file
scenarios('features/debug.feature')

