"""
Document expiration functionality tests for RediSearchDisk module.

This module loads scenarios from the expire_document.feature file and uses step definitions
from the steps package.
"""
from pytest_bdd import scenarios

# Load all scenarios from the expire document feature file
scenarios('features/expire_document.feature')
