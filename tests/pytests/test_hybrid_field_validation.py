from RLTest import Env
from includes import *
from common import *
import numpy as np
import json

def setup_basic_index(env):
    """Setup basic index with test data for field validation tests"""
    conn = env.getClusterConnectionIfNeeded()
    env.expect('FT.CREATE idx SCHEMA description TEXT embedding VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2 category TAG price NUMERIC').noError()


def setup_json_index(env):
    """Setup JSON index with test data for JSON path validation tests"""
    conn = env.getClusterConnectionIfNeeded()
    env.expect('FT.CREATE json_idx ON JSON SCHEMA $.description AS description TEXT $.embedding AS embedding VECTOR FLAT 6 TYPE FLOAT32 DIM 2 DISTANCE_METRIC L2 $.category AS category TAG $.price AS price NUMERIC').noError()
    

LOAD_ERROR_MSG = 'Missing prefix: name requires \'@\' prefix, JSON path require \'$\' prefix'


def test_hybrid_load_requires_at_prefix(env):
    """Test that FT.HYBRID LOAD requires @ prefix for field names"""
    setup_basic_index(env)
    query_vector = np.array([0.0, 0.0]).astype(np.float32).tobytes()

    # Test LOAD with missing @ prefix - should fail
    env.expect(
        'FT.HYBRID', 'idx',
        'SEARCH', 'shoes',
        'VSIM', '@embedding', '$query_vec',
        'PARAMS', '2', 'query_vec', query_vector,
        'LOAD', '1', 'description'  # Missing @ prefix
    ).error().contains(LOAD_ERROR_MSG)

    # Test LOAD with @ prefix - should succeed
    env.expect(
        'FT.HYBRID', 'idx',
        'SEARCH', 'shoes',
        'VSIM', '@embedding', '$query_vec',
        'PARAMS', '2', 'query_vec', query_vector,
        'LOAD', '1', '@description'  # With @ prefix
    ).noError()

    # Test LOAD with multiple fields, one missing @ prefix - should fail
    env.expect(
        'FT.HYBRID', 'idx',
        'SEARCH', 'shoes',
        'VSIM', '@embedding', '$query_vec',
        'PARAMS', '2', 'query_vec', query_vector,
        'LOAD', '2', '@description', 'category'  # Second field missing @ prefix
    ).error().contains(LOAD_ERROR_MSG)

    # Test LOAD with multiple fields, all with @ prefix - should succeed
    env.expect(
        'FT.HYBRID', 'idx',
        'SEARCH', 'shoes',
        'VSIM', '@embedding', '$query_vec',
        'PARAMS', '2', 'query_vec', query_vector,
        'LOAD', '2', '@description', '@category'  # Both with @ prefix
    ).noError()


def test_hybrid_load_allows_dollar_for_json_paths(env):
    """Test that FT.HYBRID LOAD allows $ prefix for JSON paths"""
    setup_json_index(env)
    query_vector = np.array([0.0, 0.0]).astype(np.float32).tobytes()

    # Test LOAD with $ prefix for JSON path - should succeed
    env.expect(
        'FT.HYBRID', 'json_idx',
        'SEARCH', 'shoes',
        'VSIM', '@embedding', '$query_vec',
        'PARAMS', '2', 'query_vec', query_vector,
        'LOAD', '1', '$.description'  # With $ prefix for JSON path
    ).noError()

    # Test LOAD with mixed @ and $ prefixes - should succeed
    env.expect(
        'FT.HYBRID', 'json_idx',
        'SEARCH', 'shoes',
        'VSIM', '@embedding', '$query_vec',
        'PARAMS', '2', 'query_vec', query_vector,
        'LOAD', '2', '@description', '$.category'  # Mixed prefixes
    ).noError()

    # Test LOAD with $ prefix for nested JSON path - should succeed
    env.expect(
        'FT.HYBRID', 'json_idx',
        'SEARCH', 'shoes',
        'VSIM', '@embedding', '$query_vec',
        'PARAMS', '2', 'query_vec', query_vector,
        'LOAD', '1', '$.price'  # Nested JSON path
    ).noError()


def test_hybrid_apply_requires_at_prefix(env):
    """Test that FT.HYBRID APPLY requires @ prefix for field references"""
    setup_basic_index(env)
    query_vector = np.array([0.0, 0.0]).astype(np.float32).tobytes()

    # Test APPLY with missing @ prefix in expression - should fail
    env.expect(
        'FT.HYBRID', 'idx',
        'SEARCH', 'shoes',
        'VSIM', '@embedding', '$query_vec',
        'PARAMS', '2', 'query_vec', query_vector,
        'LOAD', '1', '@price',
        'APPLY', 'price * 2', 'AS', 'double_price'  # Missing @ prefix in expression
    ).error().contains('Unknown symbol \'price\'')

    # Test APPLY with @ prefix in expression - should succeed
    env.expect(
        'FT.HYBRID', 'idx',
        'SEARCH', 'shoes',
        'VSIM', '@embedding', '$query_vec',
        'PARAMS', '2', 'query_vec', query_vector,
        'LOAD', '1', '@price',
        'APPLY', '@price * 2', 'AS', 'double_price'  # With @ prefix in expression
    ).noError()


def test_hybrid_filter_requires_at_prefix(env):
    """Test that FT.HYBRID FILTER requires @ prefix for field references"""
    setup_basic_index(env)
    query_vector = np.array([0.0, 0.0]).astype(np.float32).tobytes()

    # Test FILTER with missing @ prefix in expression - should fail
    env.expect(
        'FT.HYBRID', 'idx',
        'SEARCH', 'shoes',
        'VSIM', '@embedding', '$query_vec',
        'PARAMS', '2', 'query_vec', query_vector,
        'LOAD', '1', '@price',
        'FILTER', 'price > 120'  # Missing @ prefix in expression
    ).error().contains('Unknown symbol \'price\'')

    # Test FILTER with @ prefix in expression - should succeed
    env.expect(
        'FT.HYBRID', 'idx',
        'SEARCH', 'shoes',
        'VSIM', '@embedding', '$query_vec',
        'PARAMS', '2', 'query_vec', query_vector,
        'LOAD', '1', '@price',
        'FILTER', '@price > 120'  # With @ prefix in expression
    ).noError()


def test_hybrid_sortby_requires_at_prefix(env):
    """Test that FT.HYBRID SORTBY requires @ prefix for field names"""
    setup_basic_index(env)
    query_vector = np.array([0.0, 0.0]).astype(np.float32).tobytes()

    # Test SORTBY with missing @ prefix - should fail
    env.expect(
        'FT.HYBRID', 'idx',
        'SEARCH', 'shoes',
        'VSIM', '@embedding', '$query_vec',
        'PARAMS', '2', 'query_vec', query_vector,
        'LOAD', '1', '@price',
        'SORTBY', '2', 'price', 'DESC'  # Missing @ prefix
    ).error().contains('"Missing prefix: name requires \'@\' prefix, JSON path require \'$\' prefix, got: price in SORTBY"')

    # Test SORTBY with @ prefix - should succeed
    env.expect(
        'FT.HYBRID', 'idx',
        'SEARCH', 'shoes',
        'VSIM', '@embedding', '$query_vec',
        'PARAMS', '2', 'query_vec', query_vector,
        'LOAD', '1', '@price',
        'SORTBY', '2', '@price', 'DESC'  # With @ prefix
    ).noError()


def test_hybrid_load_star_works(env):
    """Test that FT.HYBRID LOAD * works without field validation"""
    setup_basic_index(env)
    query_vector = np.array([0.0, 0.0]).astype(np.float32).tobytes()

    # Test LOAD * - should succeed without field validation
    env.expect(
        'FT.HYBRID', 'idx',
        'SEARCH', 'shoes',
        'VSIM', '@embedding', '$query_vec',
        'PARAMS', '2', 'query_vec', query_vector,
        'LOAD', '*'  # Load all fields
    ).noError()


def test_hybrid_special_fields_work(env):
    """Test that FT.HYBRID field prefix validation works with special fields like __key and __score"""
    setup_basic_index(env)
    query_vector = np.array([0.0, 0.0]).astype(np.float32).tobytes()

    # Test LOAD with __key - should fail
    env.expect(
        'FT.HYBRID', 'idx',
        'SEARCH', 'shoes',
        'VSIM', '@embedding', '$query_vec',
        'PARAMS', '2', 'query_vec', query_vector,
        'LOAD', '1', '__key'
    ).error().contains(LOAD_ERROR_MSG)

    # Test LOAD with __score - should fail
    env.expect(
        'FT.HYBRID', 'idx',
        'SEARCH', 'shoes',
        'VSIM', '@embedding', '$query_vec',
        'PARAMS', '2', 'query_vec', query_vector,
        'LOAD', '1', '__score'  # Special field without @ prefix
    ).error().contains(LOAD_ERROR_MSG)

    # Test LOAD with @__key - should succeed
    env.expect(
        'FT.HYBRID', 'idx',
        'SEARCH', 'shoes',
        'VSIM', '@embedding', '$query_vec',
        'PARAMS', '2', 'query_vec', query_vector,
        'LOAD', '1', '@__key'
    ).noError()

    # Test LOAD with @__score - should succeed
    env.expect(
        'FT.HYBRID', 'idx',
        'SEARCH', 'shoes',
        'VSIM', '@embedding', '$query_vec',
        'PARAMS', '2', 'query_vec', query_vector,
        'LOAD', '1', '@__score'  # Special field without @ prefix
    ).noError()


def test_hybrid_groupby_requires_at_prefix(env):
    """Test that FT.HYBRID GROUPBY requires @ prefix for field names"""
    setup_basic_index(env)
    query_vector = np.array([0.0, 0.0]).astype(np.float32).tobytes()

    # Test GROUPBY with missing @ prefix - should fail
    env.expect(
        'FT.HYBRID', 'idx',
        'SEARCH', 'shoes',
        'VSIM', '@embedding', '$query_vec',
        'PARAMS', '2', 'query_vec', query_vector,
        'LOAD', '1', '@category',
        'GROUPBY', '1', 'category', 'REDUCE', 'COUNT', '0', 'AS', 'count'  # Missing @ prefix
    ).error().contains('Bad arguments for GROUPBY: Unknown property `category`. Did you mean `@category`?')

    # Test GROUPBY with @ prefix - should succeed
    env.expect(
        'FT.HYBRID', 'idx',
        'SEARCH', 'shoes',
        'VSIM', '@embedding', '$query_vec',
        'PARAMS', '2', 'query_vec', query_vector,
        'LOAD', '1', '@category',
        'GROUPBY', '1', '@category', 'REDUCE', 'COUNT', '0', 'AS', 'count'  # With @ prefix
    ).noError()

    # Test GROUPBY with multiple fields, one missing @ prefix - should fail
    env.expect(
        'FT.HYBRID', 'idx',
        'SEARCH', 'shoes',
        'VSIM', '@embedding', '$query_vec',
        'PARAMS', '2', 'query_vec', query_vector,
        'LOAD', '2', '@category', '@price',
        'GROUPBY', '2', '@category', 'price', 'REDUCE', 'COUNT', '0', 'AS', 'count'  # Second field missing @ prefix
    ).error().contains('Bad arguments for GROUPBY: Unknown property `price`. Did you mean `@price`?')

    # Test GROUPBY with multiple fields, all with @ prefix - should succeed
    env.expect(
        'FT.HYBRID', 'idx',
        'SEARCH', 'shoes',
        'VSIM', '@embedding', '$query_vec',
        'PARAMS', '2', 'query_vec', query_vector,
        'LOAD', '2', '@category', '@price',
        'GROUPBY', '2', '@category', '@price', 'REDUCE', 'COUNT', '0', 'AS', 'count'  # Both with @ prefix
    ).noError()


def test_hybrid_groupby_reduce_requires_at_prefix(env):
    """Test that FT.HYBRID GROUPBY REDUCE requires @ prefix for field references"""
    setup_basic_index(env)
    query_vector = np.array([0.0, 0.0]).astype(np.float32).tobytes()

    # Test REDUCE with missing @ prefix in field reference - should fail
    env.expect(
        'FT.HYBRID', 'idx',
        'SEARCH', 'shoes',
        'VSIM', '@embedding', '$query_vec',
        'PARAMS', '2', 'query_vec', query_vector,
        'LOAD', '1', '@price',
        'GROUPBY', '1', '@category', 'REDUCE', 'SUM', '1', 'price', 'AS', 'total_price'  # Missing @ prefix in REDUCE
    ).error().contains('Missing prefix: name requires \'@\' prefix, JSON path require \'$\' prefix, got: price in SUM')

    # Test REDUCE with @ prefix in field reference - should succeed
    env.expect(
        'FT.HYBRID', 'idx',
        'SEARCH', 'shoes',
        'VSIM', '@embedding', '$query_vec',
        'PARAMS', '2', 'query_vec', query_vector,
        'LOAD', '1', '@price',
        'GROUPBY', '1', '@category', 'REDUCE', 'SUM', '1', '@price', 'AS', 'total_price'  # With @ prefix in REDUCE
    ).noError()

    # Test REDUCE with multiple operations, one missing @ prefix - should fail
    env.expect(
        'FT.HYBRID', 'idx',
        'SEARCH', 'shoes',
        'VSIM', '@embedding', '$query_vec',
        'PARAMS', '2', 'query_vec', query_vector,
        'LOAD', '1', '@price',
        'GROUPBY', '1', '@category',
        'REDUCE', 'COUNT', '0', 'AS', 'count',
        'REDUCE', 'AVG', '1', 'price', 'AS', 'avg_price'  # Missing @ prefix in second REDUCE
    ).error().contains('Missing prefix: name requires \'@\' prefix, JSON path require \'$\' prefix, got: price in AVG')

    # Test REDUCE with multiple operations, all with @ prefix - should succeed
    env.expect(
        'FT.HYBRID', 'idx',
        'SEARCH', 'shoes',
        'VSIM', '@embedding', '$query_vec',
        'PARAMS', '2', 'query_vec', query_vector,
        'LOAD', '1', '@price',
        'GROUPBY', '1', '@category',
        'REDUCE', 'COUNT', '0', 'AS', 'count',
        'REDUCE', 'AVG', '1', '@price', 'AS', 'avg_price'  # With @ prefix in both REDUCE operations
    ).noError()
