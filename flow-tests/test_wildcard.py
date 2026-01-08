"""
Wildcard iterator tests for RediSearchDisk module.

This module loads scenarios from the wildcard.feature file and uses
step definitions from the steps package.
"""
from pytest_bdd import scenarios

# Load all scenarios from the wildcard feature file
scenarios('features/wildcard.feature')

