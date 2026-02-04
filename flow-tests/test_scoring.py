"""
Scoring functionality tests for RediSearchDisk module.

This module loads scenarios from the scoring.feature file and uses
step definitions from the steps package to test BM25 and TFIDF scorers.
"""
from pytest_bdd import scenarios

# Import step definitions to ensure they're registered
from steps import scoring_steps, rdb_steps  # noqa: F401

# Load all scenarios from the scoring feature file
scenarios('features/scoring.feature')
