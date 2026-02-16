"""
Test filter expressions with missing fields.

This tests the fix for the issue where expression evaluation order affects results
when fields are missing from the document. The behavior should be consistent
regardless of the order of sub-expressions in AND/OR operations.
"""

from RLTest import Env
from common import *

@skip(cluster=True)
def test_filter_and_missing_field_order(env):
    """Test that AND filter expressions work consistently regardless of order when fields are missing"""
    conn = getConnectionByEnv(env)

    # Create index with filter: @d1==0 && @d2==0
    env.expect('FT.CREATE', 'idx1', 'FILTER', '@d1==0 && @d2==0',
               'SCHEMA', 'd1', 'NUMERIC', 'd2', 'NUMERIC').ok()

    # Create index with filter in reverse order: @d2==0 && @d1==0
    env.expect('FT.CREATE', 'idx2', 'FILTER', '@d2==0 && @d1==0',
               'SCHEMA', 'd1', 'NUMERIC', 'd2', 'NUMERIC').ok()

    # Add document with only d2 field (d1 is missing)
    conn.execute_command('HSET', 'h1', 'd2', '1')

    # Both indexes should have the same result (0 documents)
    # because d2=1 (not 0) and d1 is missing (treated as false)
    result1 = env.cmd('FT.SEARCH', 'idx1', '*')
    result2 = env.cmd('FT.SEARCH', 'idx2', '*')

    env.assertEqual(result1, [0])
    env.assertEqual(result2, [0])
    env.assertEqual(result1, result2)

@skip(cluster=True)
def test_filter_or_missing_field_order(env):
    """Test that OR filter expressions work consistently regardless of order when fields are missing"""
    conn = getConnectionByEnv(env)

    # Create index with filter: @d1==0 || @d2==0
    env.expect('FT.CREATE', 'idx1', 'FILTER', '@d1==0 || @d2==0',
               'SCHEMA', 'd1', 'NUMERIC', 'd2', 'NUMERIC').ok()

    # Create index with filter in reverse order: @d2==0 || @d1==0
    env.expect('FT.CREATE', 'idx2', 'FILTER', '@d2==0 || @d1==0',
               'SCHEMA', 'd1', 'NUMERIC', 'd2', 'NUMERIC').ok()

    # Add document with only d2=1 (d1 is missing)
    conn.execute_command('HSET', 'h1', 'd2', '1')

    # Both indexes should have the same result (0 documents)
    # because d2==1 is true (even though d1 is missing)
    result1 = env.cmd('FT.SEARCH', 'idx1', '*')
    result2 = env.cmd('FT.SEARCH', 'idx2', '*')

    env.assertEqual(result1[0], 0)
    env.assertEqual(result2[0], 0)

@skip(cluster=True)
def test_filter_both_fields_missing(env):
    """Test AND and OR filters when both fields are missing"""
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx1', 'FILTER', '@d1==0 && @d2==0',
               'SCHEMA', 'd1', 'NUMERIC', 'd2', 'NUMERIC').ok()
    env.expect('FT.CREATE', 'idx2', 'FILTER', '@d1==1 || @d2==1',
               'SCHEMA', 'd1', 'NUMERIC', 'd2', 'NUMERIC').ok()

    # Add document with neither d1 nor d2
    conn.execute_command('HSET', 'h1', 'other', 'value')

    # Should have 0 documents because both fields are missing (treated as false)
    result = env.cmd('FT.SEARCH', 'idx1', '*')
    env.assertEqual(result, [0])
    result = env.cmd('FT.SEARCH', 'idx2', '*')
    env.assertEqual(result, [0])

@skip(cluster=True)
def test_filter_and_one_field_matches(env):
    """Test AND filter when one field matches and one is missing"""
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx1', 'FILTER', '@d1==5 && @d2==5',
               'SCHEMA', 'd1', 'NUMERIC', 'd2', 'NUMERIC').ok()

    env.expect('FT.CREATE', 'idx2', 'FILTER', '@d2==5 && @d1==5',
               'SCHEMA', 'd1', 'NUMERIC', 'd2', 'NUMERIC').ok()

    # Add document with only d2=5 (d1 is missing)
    conn.execute_command('HSET', 'h1', 'd2', '5')

    # Both should have 0 documents because d1 is missing (treated as false)
    result1 = env.cmd('FT.SEARCH', 'idx1', '*')
    result2 = env.cmd('FT.SEARCH', 'idx2', '*')

    env.assertEqual(result1, [0])
    env.assertEqual(result2, [0])
    env.assertEqual(result1, result2)

@skip(cluster=True)
def test_filter_or_one_field_matches(env):
    """Test OR filter when one field matches and one is missing"""
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx1', 'FILTER', '@d1==5 || @d2==5',
               'SCHEMA', 'd1', 'NUMERIC', 'd2', 'NUMERIC').ok()

    env.expect('FT.CREATE', 'idx2', 'FILTER', '@d2==5 || @d1==5',
               'SCHEMA', 'd1', 'NUMERIC', 'd2', 'NUMERIC').ok()

    # Add document with only d2=5 (d1 is missing)
    conn.execute_command('HSET', 'h1', 'd2', '5')

    # Both should have 1 document because d2==5 is true
    result1 = env.cmd('FT.SEARCH', 'idx1', '*')
    result2 = env.cmd('FT.SEARCH', 'idx2', '*')

    env.assertEqual(result1[0], 1)
    env.assertEqual(result1[1], 'h1')
    env.assertEqual(result2[0], 1)
    env.assertEqual(result2[1], 'h1')
    env.assertEqual(result1, result2)

# Tests with reverse order: documents first, then index creation

@skip(cluster=True)
def test_filter_and_missing_field_order_docs_first(env):
    """Test AND filter with documents added before index creation"""
    conn = getConnectionByEnv(env)

    # Add document with only d2 field (d1 is missing) BEFORE creating indexes
    conn.execute_command('HSET', 'h1', 'd2', '1')

    # Create index with filter: @d1==0 && @d2==0
    env.expect('FT.CREATE', 'idx1', 'FILTER', '@d1==0 && @d2==0',
               'SCHEMA', 'd1', 'NUMERIC', 'd2', 'NUMERIC').ok()

    # Create index with filter in reverse order: @d2==0 && @d1==0
    env.expect('FT.CREATE', 'idx2', 'FILTER', '@d2==0 && @d1==0',
               'SCHEMA', 'd1', 'NUMERIC', 'd2', 'NUMERIC').ok()

    # Both indexes should have the same result (0 documents)
    result1 = env.cmd('FT.SEARCH', 'idx1', '*')
    result2 = env.cmd('FT.SEARCH', 'idx2', '*')

    env.assertEqual(result1, [0])
    env.assertEqual(result2, [0])
    env.assertEqual(result1, result2)

@skip(cluster=True)
def test_filter_or_missing_field_order_docs_first(env):
    """Test OR filter with documents added before index creation"""
    conn = getConnectionByEnv(env)

    # Add document with only d2=1 (d1 is missing) BEFORE creating indexes
    conn.execute_command('HSET', 'h1', 'd2', '1')

    # Create index with filter: @d1==1 || @d2==1
    env.expect('FT.CREATE', 'idx1', 'FILTER', '@d1==1 || @d2==1',
               'SCHEMA', 'd1', 'NUMERIC', 'd2', 'NUMERIC').ok()

    # Create index with filter in reverse order: @d2==1 || @d1==1
    env.expect('FT.CREATE', 'idx2', 'FILTER', '@d2==1 || @d1==1',
               'SCHEMA', 'd1', 'NUMERIC', 'd2', 'NUMERIC').ok()

    # Both indexes should have the same result (1 document)
    result1 = env.cmd('FT.SEARCH', 'idx1', '*')
    result2 = env.cmd('FT.SEARCH', 'idx2', '*')

    env.assertEqual(result1[0], 1)
    env.assertEqual(result1[1], 'h1')
    env.assertEqual(result2[0], 1)
    env.assertEqual(result2[1], 'h1')
    env.assertEqual(result1, result2)

@skip(cluster=True)
def test_filter_both_fields_missing_docs_first(env):
    """Test AND and OR filters when both fields are missing, documents added before index creation"""
    conn = getConnectionByEnv(env)

    # Add document with neither d1 nor d2 BEFORE creating index
    conn.execute_command('HSET', 'h1', 'other', 'value')

    env.expect('FT.CREATE', 'idx1', 'FILTER', '@d1==0 && @d2==0',
               'SCHEMA', 'd1', 'NUMERIC', 'd2', 'NUMERIC').ok()
    env.expect('FT.CREATE', 'idx2', 'FILTER', '@d1==1 || @d2==1',
               'SCHEMA', 'd1', 'NUMERIC', 'd2', 'NUMERIC').ok()

    # Should have 0 documents because both fields are missing (treated as false)
    result = env.cmd('FT.SEARCH', 'idx1', '*')
    env.assertEqual(result, [0])
    result = env.cmd('FT.SEARCH', 'idx2', '*')
    env.assertEqual(result, [0])



@skip(cluster=True)
def test_filter_and_one_field_matches_docs_first(env):
    """Test AND filter when one field matches and one is missing, documents added before index creation"""
    conn = getConnectionByEnv(env)

    # Add document with only d2=5 (d1 is missing) BEFORE creating indexes
    conn.execute_command('HSET', 'h1', 'd2', '5')

    env.expect('FT.CREATE', 'idx1', 'FILTER', '@d1==5 && @d2==5',
               'SCHEMA', 'd1', 'NUMERIC', 'd2', 'NUMERIC').ok()

    env.expect('FT.CREATE', 'idx2', 'FILTER', '@d2==5 && @d1==5',
               'SCHEMA', 'd1', 'NUMERIC', 'd2', 'NUMERIC').ok()

    # Both should have 0 documents because d1 is missing (treated as false)
    result1 = env.cmd('FT.SEARCH', 'idx1', '*')
    result2 = env.cmd('FT.SEARCH', 'idx2', '*')

    env.assertEqual(result1, [0])
    env.assertEqual(result2, [0])
    env.assertEqual(result1, result2)

@skip(cluster=True)
def test_filter_or_one_field_matches_docs_first(env):
    """Test OR filter when one field matches and one is missing, documents added before index creation"""
    conn = getConnectionByEnv(env)

    # Add document with only d2=5 (d1 is missing) BEFORE creating indexes
    conn.execute_command('HSET', 'h1', 'd2', '5')

    env.expect('FT.CREATE', 'idx1', 'FILTER', '@d1==5 || @d2==5',
               'SCHEMA', 'd1', 'NUMERIC', 'd2', 'NUMERIC').ok()

    env.expect('FT.CREATE', 'idx2', 'FILTER', '@d2==5 || @d1==5',
               'SCHEMA', 'd1', 'NUMERIC', 'd2', 'NUMERIC').ok()

    # Both should have 1 document because d2==5 is true
    result1 = env.cmd('FT.SEARCH', 'idx1', '*')
    result2 = env.cmd('FT.SEARCH', 'idx2', '*')

    env.assertEqual(result1[0], 1)
    env.assertEqual(result1[1], 'h1')
    env.assertEqual(result2[0], 1)
    env.assertEqual(result2[1], 'h1')
    env.assertEqual(result1, result2)
