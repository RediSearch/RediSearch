"""
Tests for SHARD_K_RATIO parameter in FT.HYBRID command.

SHARD_K_RATIO optimizes KNN query execution in distributed/cluster environments
by controlling how many results each shard returns using the formula:
    effectiveK = max(K/#shards, ceil(K × ratio))

Valid ratio range: (0.0, 1.0] (exclusive 0, inclusive 1)
Default ratio: 1.0 (no optimization - each shard returns full K results)
"""

from common import *


def ValidateHybridError(env, res, expected_error_message, message="", depth=1):
    """Helper to validate error response from FT.HYBRID command"""
    env.assertTrue(res.errorRaised, message=message, depth=depth+1)
    env.assertContains(expected_error_message, res.res, message=message, depth=depth+1)


def test_shard_k_ratio_parameter_validation():
    """Test SHARD_K_RATIO parameter validation and error handling for FT.HYBRID."""
    env = Env(moduleArgs='DEFAULT_DIALECT 2')

    dim = 2
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', dim, 'DISTANCE_METRIC', 'L2',
               't', 'TEXT').ok()

    conn = getConnectionByEnv(env)
    vec = create_np_array_typed([1.0] * dim)
    conn.execute_command('HSET', 'doc:1', 'v', vec.tobytes(), 't', 'hello world')

    query_vec = create_np_array_typed([2.0] * dim)

    # Test invalid ratio values - below minimum, above maximum, negative
    # Error message is "Invalid shard k ratio value" from ValidateShardKRatio
    invalid_ratios = [0.0, -0.1, 1.1, 2.0, 0, 7]
    for ratio in invalid_ratios:
        res = env.expect('FT.HYBRID', 'idx', '2', 'SEARCH', 'hello',
                         'VSIM', '@v', '$BLOB', 'KNN', '2', 'K', '5',
                         'SHARD_K_RATIO', str(ratio),
                         'PARAMS', '2', 'BLOB', query_vec.tobytes())
        ValidateHybridError(env, res, "Invalid shard k ratio value",
                           message=f"FT.HYBRID expected error for invalid shard k ratio: {ratio}")

    # Test non-numeric value
    res = env.expect('FT.HYBRID', 'idx', '2', 'SEARCH', 'hello',
                     'VSIM', '@v', '$BLOB', 'KNN', '2', 'K', '5',
                     'SHARD_K_RATIO', 'invalid',
                     'PARAMS', '2', 'BLOB', query_vec.tobytes())
    ValidateHybridError(env, res, "Invalid shard k ratio value",
                       message="FT.HYBRID expected error for non-numeric shard k ratio")


def test_shard_k_ratio_missing_value():
    """Test SHARD_K_RATIO with missing value for FT.HYBRID."""
    env = Env(moduleArgs='DEFAULT_DIALECT 2')

    dim = 2
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', dim, 'DISTANCE_METRIC', 'L2',
               't', 'TEXT').ok()

    conn = getConnectionByEnv(env)
    vec = create_np_array_typed([1.0] * dim)
    conn.execute_command('HSET', 'doc:1', 'v', vec.tobytes(), 't', 'hello world')

    query_vec = create_np_array_typed([2.0] * dim)

    # Test missing value - SHARD_K_RATIO followed by PARAMS (which is not a valid ratio)
    # Error message is "Invalid shard k ratio value" when PARAMS is parsed as the ratio value
    res = env.expect('FT.HYBRID', 'idx', '2', 'SEARCH', 'hello',
                     'VSIM', '@v', '$BLOB', 'KNN', '2', 'K', '5',
                     'SHARD_K_RATIO',
                     'PARAMS', '2', 'BLOB', query_vec.tobytes())
    ValidateHybridError(env, res, "Invalid shard k ratio value",
                       message="FT.HYBRID expected error for missing SHARD_K_RATIO value")


def test_shard_k_ratio_duplicate():
    """Test duplicate SHARD_K_RATIO parameter for FT.HYBRID."""
    env = Env(moduleArgs='DEFAULT_DIALECT 2')

    dim = 2
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', dim, 'DISTANCE_METRIC', 'L2',
               't', 'TEXT').ok()

    conn = getConnectionByEnv(env)
    vec = create_np_array_typed([1.0] * dim)
    conn.execute_command('HSET', 'doc:1', 'v', vec.tobytes(), 't', 'hello world')

    query_vec = create_np_array_typed([2.0] * dim)

    # Test duplicate SHARD_K_RATIO
    # Note: The parser handles the second SHARD_K_RATIO as an unknown argument
    res = env.expect('FT.HYBRID', 'idx', '2', 'SEARCH', 'hello',
                     'VSIM', '@v', '$BLOB', 'KNN', '2', 'K', '5',
                     'SHARD_K_RATIO', '0.5', 'SHARD_K_RATIO', '0.8',
                     'PARAMS', '2', 'BLOB', query_vec.tobytes())
    ValidateHybridError(env, res, "SHARD_K_RATIO",
                       message="FT.HYBRID expected error for duplicate SHARD_K_RATIO")


def test_shard_k_ratio_valid_values():
    """Test SHARD_K_RATIO with valid values for FT.HYBRID."""
    env = Env(moduleArgs='DEFAULT_DIALECT 2')

    dim = 2
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', dim, 'DISTANCE_METRIC', 'L2',
               't', 'TEXT').ok()

    conn = getConnectionByEnv(env)
    # Create enough documents for meaningful tests
    for i in range(10):
        vec = create_np_array_typed([float(i)] * dim)
        conn.execute_command('HSET', f'doc:{i}', 'v', vec.tobytes(), 't', f'hello world {i}')

    query_vec = create_np_array_typed([5.0] * dim)

    # Test valid ratio values - should all succeed
    valid_ratios = [0.1, 0.5, 0.9, 1.0]
    for ratio in valid_ratios:
        res = env.cmd('FT.HYBRID', 'idx', '2', 'SEARCH', 'hello',
                      'VSIM', '@v', '$BLOB', 'KNN', '2', 'K', '5',
                      'SHARD_K_RATIO', str(ratio),
                      'PARAMS', '2', 'BLOB', query_vec.tobytes())
        # Verify we get a valid response - check total_results is present (RESP2 format)
        # Response format: [total_results, doc1, attrs1, doc2, attrs2, ...]
        env.assertIsNotNone(res, message=f"Expected response for ratio {ratio}")
        env.assertGreaterEqual(len(res), 1, message=f"Expected at least total_results for ratio {ratio}")


def test_shard_k_ratio_with_filter():
    """Test SHARD_K_RATIO combined with FILTER clause."""
    env = Env(moduleArgs='DEFAULT_DIALECT 2')

    dim = 2
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', dim, 'DISTANCE_METRIC', 'L2',
               't', 'TEXT',
               'tag', 'TAG').ok()

    conn = getConnectionByEnv(env)
    for i in range(10):
        vec = create_np_array_typed([float(i)] * dim)
        tag = 'even' if i % 2 == 0 else 'odd'
        conn.execute_command('HSET', f'doc:{i}', 'v', vec.tobytes(), 't', f'hello world {i}', 'tag', tag)

    query_vec = create_np_array_typed([5.0] * dim)

    # Test SHARD_K_RATIO with FILTER
    res = env.cmd('FT.HYBRID', 'idx', '2', 'SEARCH', 'hello',
                  'VSIM', '@v', '$BLOB', 'KNN', '2', 'K', '5',
                  'FILTER', '@tag:{even}',
                  'SHARD_K_RATIO', '0.5',
                  'PARAMS', '2', 'BLOB', query_vec.tobytes())
    # Verify we get a valid response
    env.assertIsNotNone(res, message="Expected valid response with FILTER and SHARD_K_RATIO")
    env.assertGreaterEqual(len(res), 1, message="Expected at least total_results with FILTER and SHARD_K_RATIO")


def test_shard_k_ratio_with_yield_score_as():
    """Test SHARD_K_RATIO combined with YIELD_SCORE_AS clause."""
    env = Env(moduleArgs='DEFAULT_DIALECT 2')

    dim = 2
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', dim, 'DISTANCE_METRIC', 'L2',
               't', 'TEXT').ok()

    conn = getConnectionByEnv(env)
    for i in range(10):
        vec = create_np_array_typed([float(i)] * dim)
        conn.execute_command('HSET', f'doc:{i}', 'v', vec.tobytes(), 't', f'hello world {i}')

    query_vec = create_np_array_typed([5.0] * dim)

    # Test SHARD_K_RATIO with YIELD_SCORE_AS
    res = env.cmd('FT.HYBRID', 'idx', '2', 'SEARCH', 'hello',
                  'VSIM', '@v', '$BLOB', 'KNN', '2', 'K', '5',
                  'YIELD_SCORE_AS', 'my_score',
                  'SHARD_K_RATIO', '0.8',
                  'PARAMS', '2', 'BLOB', query_vec.tobytes())
    # Verify we get a valid response
    env.assertIsNotNone(res, message="Expected valid response with YIELD_SCORE_AS and SHARD_K_RATIO")
    env.assertGreaterEqual(len(res), 1, message="Expected at least total_results with YIELD_SCORE_AS and SHARD_K_RATIO")

