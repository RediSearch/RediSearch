"""
Compaction (GC) and scoring stats tests for RediSearchDisk module.

This module loads scenarios from the compaction.feature file and uses
step definitions to verify that GC correctly updates scoring statistics.
"""
from pytest_bdd import scenarios

# Import step definitions to ensure they're registered
from steps import compaction_steps, scoring_steps, basic_steps, delete_document_steps  # noqa: F401

# Load all scenarios from the compaction feature file
scenarios('features/compaction.feature')

