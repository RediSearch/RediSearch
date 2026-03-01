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

    waitForIndex(env, 'idx1')
    waitForIndex(env, 'idx2')

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

    waitForIndex(env, 'idx1')
    waitForIndex(env, 'idx2')

    # Both should have 1 document because d2==5 is true
    result1 = env.cmd('FT.SEARCH', 'idx1', '*')
    result2 = env.cmd('FT.SEARCH', 'idx2', '*')

    env.assertEqual(result1[0], 1)
    env.assertEqual(result1[1], 'h1')
    env.assertEqual(result2[0], 1)
    env.assertEqual(result2[1], 'h1')
    env.assertEqual(result1, result2)


# Tests for type mismatches in filter expressions

@skip(cluster=True)
def test_filter_type_mismatch_numeric_comparison(env):
    """Test that type mismatches in numeric comparisons are handled correctly"""
    conn = getConnectionByEnv(env)

    # Create index with numeric comparison filter
    env.expect('FT.CREATE', 'idx', 'FILTER', '@price > 100',
               'SCHEMA', 'price', 'TEXT', 'name', 'TEXT').ok()

    # Add document with a non-numeric string in price field
    conn.execute_command('HSET', 'h1', 'price', 'hello', 'name', 'test')

    # Document should NOT be indexed because 'hello' > 100 should fail/return false
    result = env.cmd('FT.SEARCH', 'idx', '*')
    env.assertEqual(result, [0], message="'hello' > 100 should fail, doc not indexed")

    # Add document with valid numeric price
    conn.execute_command('HSET', 'h2', 'price', '150', 'name', 'test2')

    # This document SHOULD be indexed
    result = env.cmd('FT.SEARCH', 'idx', '*')
    env.assertEqual(result[0], 1, message="150 > 100 should pass")
    env.assertEqual(result[1], 'h2', message="Only h2 should be indexed")

    # Add document with price below threshold
    conn.execute_command('HSET', 'h3', 'price', '50', 'name', 'test3')
    result = env.cmd('FT.SEARCH', 'idx', '*')
    env.assertEqual(result[0], 1, message="50 > 100 is false, only h2 indexed")
    env.assertEqual(result[1], 'h2')


@skip(cluster=True)
def test_filter_comparison_operators_with_missing_fields(env):
    """Test various comparison operators with missing fields"""
    conn = getConnectionByEnv(env)

    # Test less than
    env.expect('FT.CREATE', 'idx_lt', 'FILTER', '@val < 10',
               'SCHEMA', 'val', 'NUMERIC').ok()

    # Test greater than
    env.expect('FT.CREATE', 'idx_gt', 'FILTER', '@val > 10',
               'SCHEMA', 'val', 'NUMERIC').ok()

    # Test less than or equal
    env.expect('FT.CREATE', 'idx_le', 'FILTER', '@val <= 10',
               'SCHEMA', 'val', 'NUMERIC').ok()

    # Test greater than or equal
    env.expect('FT.CREATE', 'idx_ge', 'FILTER', '@val >= 10',
               'SCHEMA', 'val', 'NUMERIC').ok()

    # Test not equal
    env.expect('FT.CREATE', 'idx_ne', 'FILTER', '@val != 10',
               'SCHEMA', 'val', 'NUMERIC').ok()

    # Test equal
    env.expect('FT.CREATE', 'idx_eq', 'FILTER', '@val == 10',
               'SCHEMA', 'val', 'NUMERIC').ok()

    # Add document without 'val' field
    conn.execute_command('HSET', 'h1', 'other', 'data')

    # All indexes should have 0 documents (missing field treated as comparison failure)
    for idx in ['idx_lt', 'idx_gt', 'idx_le', 'idx_ge', 'idx_ne', 'idx_eq']:
        result = env.cmd('FT.SEARCH', idx, '*')
        env.assertEqual(result, [0], message=f"Index {idx} should have 0 docs when field is missing")

    # Now add documents with actual values to verify the filters work correctly
    conn.execute_command('HSET', 'h_lt', 'val', '5')   # 5 < 10 is true
    conn.execute_command('HSET', 'h_gt', 'val', '15')  # 15 > 10 is true
    conn.execute_command('HSET', 'h_eq', 'val', '10')  # 10 == 10 is true

    # Verify each index now has the correct document
    result = env.cmd('FT.SEARCH', 'idx_lt', '*')
    env.assertEqual(result[0], 1, message="idx_lt should have 1 doc (5 < 10)")
    env.assertEqual(result[1], 'h_lt')

    result = env.cmd('FT.SEARCH', 'idx_gt', '*')
    env.assertEqual(result[0], 1, message="idx_gt should have 1 doc (15 > 10)")
    env.assertEqual(result[1], 'h_gt')

    result = env.cmd('FT.SEARCH', 'idx_le', '*')
    env.assertEqual(result[0], 2, message="idx_le should have 2 docs (5 <= 10, 10 <= 10)")
    env.assertContains('h_lt', result)
    env.assertContains('h_eq', result)

    result = env.cmd('FT.SEARCH', 'idx_ge', '*')
    env.assertEqual(result[0], 2, message="idx_ge should have 2 docs (15 >= 10, 10 >= 10)")
    env.assertContains('h_gt', result)
    env.assertContains('h_eq', result)

    result = env.cmd('FT.SEARCH', 'idx_ne', '*')
    env.assertEqual(result[0], 2, message="idx_ne should have 2 docs (5 != 10, 15 != 10)")
    env.assertContains('h_lt', result)
    env.assertContains('h_gt', result)

    result = env.cmd('FT.SEARCH', 'idx_eq', '*')
    env.assertEqual(result[0], 1, message="idx_eq should have 1 doc (10 == 10)")
    env.assertEqual(result[1], 'h_eq')


@skip(cluster=True)
def test_filter_nested_expressions_with_missing_fields(env):
    """Test nested AND/OR expressions with some fields missing"""
    conn = getConnectionByEnv(env)

    # Complex filter: (@a==1 && @b==2) || (@c==3 && @d==4)
    env.expect('FT.CREATE', 'idx', 'FILTER', '(@a==1 && @b==2) || (@c==3 && @d==4)',
               'SCHEMA', 'a', 'NUMERIC', 'b', 'NUMERIC', 'c', 'NUMERIC', 'd', 'NUMERIC').ok()

    # Document with only a=1, b=2 (c and d missing) - should match first clause
    conn.execute_command('HSET', 'h1', 'a', '1', 'b', '2')
    result = env.cmd('FT.SEARCH', 'idx', '*')
    env.assertEqual(result[0], 1, message="h1 should match (@a==1 && @b==2)")
    env.assertEqual(result[1], 'h1')

    # Document with only c=3, d=4 (a and b missing) - should match second clause
    conn.execute_command('HSET', 'h2', 'c', '3', 'd', '4')
    result = env.cmd('FT.SEARCH', 'idx', '*')
    env.assertEqual(result[0], 2, message="h2 should match (@c==3 && @d==4)")
    env.assertContains('h1', result)
    env.assertContains('h2', result)

    # Document with only a=1 (b, c, d missing) - should NOT match
    conn.execute_command('HSET', 'h3', 'a', '1')
    result = env.cmd('FT.SEARCH', 'idx', '*')
    env.assertEqual(result[0], 2, message="h3 should NOT match - b is missing so first clause fails")
    env.assertContains('h1', result)
    env.assertContains('h2', result)

    # Document with all fields but wrong values - should NOT match
    conn.execute_command('HSET', 'h4', 'a', '9', 'b', '9', 'c', '9', 'd', '9')
    result = env.cmd('FT.SEARCH', 'idx', '*')
    env.assertEqual(result[0], 2, message="h4 should NOT match - wrong values")
    env.assertContains('h1', result)
    env.assertContains('h2', result)

    # Document matching both clauses - should match
    conn.execute_command('HSET', 'h5', 'a', '1', 'b', '2', 'c', '3', 'd', '4')
    result = env.cmd('FT.SEARCH', 'idx', '*')
    env.assertEqual(result[0], 3, message="h5 should match - both clauses true")
    env.assertContains('h1', result)
    env.assertContains('h2', result)
    env.assertContains('h5', result)


@skip(cluster=True)
def test_filter_type_mismatch_equality(env):
    """Test type mismatch in equality comparisons"""
    conn = getConnectionByEnv(env)

    # Filter comparing numeric field to number
    env.expect('FT.CREATE', 'idx', 'FILTER', '@count == 5',
               'SCHEMA', 'count', 'TEXT').ok()

    # Add document with string that can be parsed as number
    conn.execute_command('HSET', 'h1', 'count', '5')
    result = env.cmd('FT.SEARCH', 'idx', '*')
    env.assertEqual(result[0], 1, message="'5' == 5 should match")
    env.assertEqual(result[1], 'h1')

    # Add document with string that cannot be parsed as number
    conn.execute_command('HSET', 'h2', 'count', 'five')
    result = env.cmd('FT.SEARCH', 'idx', '*')
    env.assertEqual(result[0], 1, message="'five' == 5 should NOT match")
    env.assertEqual(result[1], 'h1')

    # Add document with different numeric value
    conn.execute_command('HSET', 'h3', 'count', '10')
    result = env.cmd('FT.SEARCH', 'idx', '*')
    env.assertEqual(result[0], 1, message="'10' == 5 should NOT match")
    env.assertEqual(result[1], 'h1')


@skip(cluster=True)
def test_filter_comparing_two_missing_fields(env):
    """Test that comparing two missing fields returns false (not true like NULL == NULL)"""
    conn = getConnectionByEnv(env)

    # Filter that compares two fields directly
    env.expect('FT.CREATE', 'idx_eq', 'FILTER', '@field1 == @field2',
               'SCHEMA', 'field1', 'NUMERIC', 'field2', 'NUMERIC').ok()

    env.expect('FT.CREATE', 'idx_ne', 'FILTER', '@field1 != @field2',
               'SCHEMA', 'field1', 'NUMERIC', 'field2', 'NUMERIC').ok()

    # Add document where both fields are missing
    conn.execute_command('HSET', 'h1', 'other', 'data')

    # Both comparisons should return false when both fields are missing
    # (missing property NULL is different from literal NULL)
    result_eq = env.cmd('FT.SEARCH', 'idx_eq', '*')
    env.assertEqual(result_eq, [0], message="@field1 == @field2 should be false when both are missing")

    result_ne = env.cmd('FT.SEARCH', 'idx_ne', '*')
    env.assertEqual(result_ne, [0], message="@field1 != @field2 should be false when both are missing")

    # Add document where both fields exist and are equal
    conn.execute_command('HSET', 'h2', 'field1', '5', 'field2', '5')
    result_eq = env.cmd('FT.SEARCH', 'idx_eq', '*')
    env.assertEqual(result_eq[0], 1, message="@field1 == @field2 should be true when both are 5")
    env.assertEqual(result_eq[1], 'h2')

    result_ne = env.cmd('FT.SEARCH', 'idx_ne', '*')
    env.assertEqual(result_ne, [0], message="@field1 != @field2 should still be false (5 != 5 is false)")

    # Add document where both fields exist and are not equal
    conn.execute_command('HSET', 'h3', 'field1', '5', 'field2', '10')
    result_eq = env.cmd('FT.SEARCH', 'idx_eq', '*')
    env.assertEqual(result_eq[0], 1, message="@field1 == @field2 still only h2 (5 == 10 is false)")
    env.assertEqual(result_eq[1], 'h2')

    result_ne = env.cmd('FT.SEARCH', 'idx_ne', '*')
    env.assertEqual(result_ne[0], 1, message="@field1 != @field2 should be true for h3 (5 != 10)")
    env.assertEqual(result_ne[1], 'h3')

    # Add document where only one field exists
    conn.execute_command('HSET', 'h4', 'field1', '5')
    result_eq = env.cmd('FT.SEARCH', 'idx_eq', '*')
    env.assertEqual(result_eq[0], 1, message="Missing field2 - comparison should fail")
    env.assertEqual(result_eq[1], 'h2')

    result_ne = env.cmd('FT.SEARCH', 'idx_ne', '*')
    env.assertEqual(result_ne[0], 1, message="Missing field2 - comparison should fail")
    env.assertEqual(result_ne[1], 'h3')
