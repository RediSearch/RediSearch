"""
Test file for drop.feature scenarios.
This file is required by pytest-bdd to discover and run the feature file.
"""
from pytest_bdd import scenarios

# Load all scenarios from the drop.feature file
scenarios('features/drop.feature')

