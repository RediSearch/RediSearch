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


def test_shard_k_ratio_result_count():
    """Test that SHARD_K_RATIO returns the expected number of results.

    Similar to test_query in test_shard_window_ratio.py - verifies that
    we get K results with various ratio values. This doesn't verify
    the internal effectiveK calculation (requires FT.PROFILE), but ensures
    SHARD_K_RATIO doesn't break query execution.

    By using a SEARCH query that matches zero documents, only the VSIM
    subquery results are returned, so K directly controls the output count.
    """
    env = Env(moduleArgs='DEFAULT_DIALECT 2')

    dim = 2
    datatype = 'FLOAT32'
    k = 100
    num_docs = k * max(env.shardsCount, 1) * 3  # ensure we have enough results in each shard

    # Create index with vector and text fields
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               'v', 'VECTOR', 'FLAT', '6', 'TYPE', datatype, 'DIM', dim, 'DISTANCE_METRIC', 'L2',
               't', 'TEXT').ok()

    conn = getConnectionByEnv(env)
    # Create documents with vector field only (no matching text for SEARCH)
    for i in range(1, num_docs + 1):
        vec = create_np_array_typed([float(i % 100)] * dim)
        conn.execute_command('HSET', f'doc{i}', 'v', vec.tobytes(), 't', 'some text')

    query_vec = create_random_np_array_typed(dim, datatype)

    min_shard_ratio = 1 / float(max(env.shardsCount, 1))
    ratios = [min_shard_ratio, 0.01, 0.5, 0.9, 1.0]

    for ratio in ratios:
        # Test FT.HYBRID with SHARD_K_RATIO
        # Use a SEARCH query that matches zero docs, so only VSIM results are returned
        # Use LIMIT and WINDOW to ensure we can get K results (defaults are 10 and 20)
        res = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'nonexistent_term_xyz',
                      'VSIM', '@v', '$BLOB', 'KNN', '2', 'K', str(k),
                      'SHARD_K_RATIO', str(ratio),
                      'COMBINE', 'RRF', '2', 'WINDOW', str(k),
                      'LIMIT', '0', str(k),
                      'PARAMS', '2', 'BLOB', query_vec.tobytes())

        # Response format: ['total_results', N, 'results', [...], 'warnings', [], 'execution_time', ...]
        # Results are in res[3]
        actual_result_count = len(res[3])

        env.assertEqual(actual_result_count, k,
                       message=f"FT.HYBRID with K={k}, ratio={ratio}: expected {k} results, got {actual_result_count}")


def test_shard_k_ratio_small_k():
    """Test SHARD_K_RATIO with small K values (1, 2, 3).

    By using a SEARCH query that matches zero documents, only the VSIM
    subquery results are returned, so K directly controls the output count.
    """
    env = Env(moduleArgs='DEFAULT_DIALECT 2')

    dim = 2
    datatype = 'FLOAT32'

    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               'v', 'VECTOR', 'FLAT', '6', 'TYPE', datatype, 'DIM', dim, 'DISTANCE_METRIC', 'L2',
               't', 'TEXT').ok()

    conn = getConnectionByEnv(env)
    for i in range(1, 11):
        vec = create_np_array_typed([float(i)] * dim)
        conn.execute_command('HSET', f'doc{i}', 'v', vec.tobytes(), 't', 'some text')

    query_vec = create_random_np_array_typed(dim, datatype)

    ratio = 0.5

    # Test small K values with a non-matching SEARCH query
    for k in [1, 2, 3]:
        res = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'nonexistent_term_xyz',
                      'VSIM', '@v', '$BLOB', 'KNN', '2', 'K', str(k),
                      'SHARD_K_RATIO', str(ratio),
                      'PARAMS', '2', 'BLOB', query_vec.tobytes())

        # Response format: ['total_results', N, 'results', [...], 'warnings', [], 'execution_time', ...]
        actual_result_count = len(res[3])
        env.assertEqual(actual_result_count, k,
                       message=f"FT.HYBRID with K={k}: expected {k} results, got {actual_result_count}")


@skip(cluster=False)  # Only relevant for cluster mode
def test_shard_k_ratio_per_shard_verification():
    """Test that SHARD_K_RATIO actually limits results per shard.

    This test verifies the SHARD_K_RATIO behavior by:
    1. Creating documents with hash tags to control shard distribution
       ({shard:0}, {shard:1}, {shard:3} go to different shards)
    2. Adding a TAG field that identifies which shard the document belongs to
    3. Running FT.HYBRID with SHARD_K_RATIO that limits effectiveK per shard
    4. Counting results by shard_tag to verify each shard only returned the
       limited number

    Without FT.PROFILE for FT.HYBRID, this is a way to verify that
    SHARD_K_RATIO is actually affecting per-shard behavior.
    """
    env = Env(moduleArgs='DEFAULT_DIALECT 2')

    # This test requires exactly 3 shards due to hardcoded hash tags
    num_shards = 3
    if env.shardsCount != num_shards:
        env.skip()

    conn = getConnectionByEnv(env)

    dim = 2
    datatype = 'FLOAT32'

    # We want enough docs per shard to verify limiting works
    # With 5 docs per shard and effectiveK = 3, we can verify limiting
    docs_per_shard = 5
    effective_k_per_shard = 3

    # Calculate K and ratio to achieve effectiveK = 3 per shard
    # effectiveK = max(K/#shards, ceil(K × ratio))
    # With ratio = 1/num_shards and K = 3 * num_shards:
    # effectiveK = max(3, ceil(3)) = 3
    k = effective_k_per_shard * num_shards  # K = 9
    ratio = effective_k_per_shard / k  # ratio = 3/9 = 1/3

    # Create index with vector, text, and tag fields
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               'v', 'VECTOR', 'FLAT', '6', 'TYPE', datatype, 'DIM', dim, 'DISTANCE_METRIC', 'L2',
               't', 'TEXT',
               'shard_tag', 'TAG').ok()

    # Hash tags that distribute to different shards in a 3-shard cluster
    shard_hash_tags = ['{shard:0}', '{shard:1}', '{shard:3}']

    # Create documents with hash tags to control shard distribution
    # Use the same vector values across all shards so distances are uniform
    for shard_idx in range(num_shards):
        hash_tag = shard_hash_tags[shard_idx]
        shard_tag = f'shard{shard_idx}'
        for doc_idx in range(docs_per_shard):
            doc_key = f'{hash_tag}:doc{doc_idx}'
            # Use doc_idx only (not shard_idx) so all shards have same vector distribution
            vec = create_np_array_typed([float(doc_idx)] * dim)
            conn.execute_command('HSET', doc_key, 'v', vec.tobytes(),
                               't', 'some text', 'shard_tag', shard_tag)

    # Verify each shard has the expected number of documents
    for shard_idx, shard_conn in enumerate(env.getOSSMasterNodesConnectionList()):
        keys = shard_conn.execute_command('KEYS', '*')
        env.assertGreaterEqual(len(keys), docs_per_shard,
                              message=f"Shard {shard_idx} should have at least {docs_per_shard} keys, got {len(keys)}")

    query_vec = create_random_np_array_typed(dim, datatype)

    # Run FT.HYBRID with SHARD_K_RATIO
    # Use non-matching SEARCH query so only VSIM results are returned
    res = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'nonexistent_term_xyz',
                  'VSIM', '@v', '$BLOB', 'KNN', '2', 'K', str(k),
                  'SHARD_K_RATIO', str(ratio),
                  'COMBINE', 'RRF', '2', 'WINDOW', str(k),
                  'LIMIT', '0', str(k),
                  'PARAMS', '2', 'BLOB', query_vec.tobytes())

    # Response format: ['total_results', N, 'results', [...], 'warnings', [],
    # 'execution_time', ...]
    results = res[3]

    # Count results by shard (extracted from key name which contains hash tag)
    # Result format: ['__key', '{shard:X}:docY', '__score', '...', ...]
    import re
    shard_counts = {}
    for result in results:
        # Find the key value (after '__key')
        doc_key = None
        for i in range(0, len(result), 2):
            if result[i] == '__key':
                doc_key = result[i + 1]
                break

        if doc_key:
            # Extract shard from key name: '{shard:X}:docY' -> 'shard:X'
            # The hash tag is between { and }
            match = re.search(r'\{([^}]+)\}', doc_key)
            if match:
                shard_tag = match.group(1)  # e.g., 'shard:0', 'shard:1', 'shard:3'
                shard_counts[shard_tag] = shard_counts.get(shard_tag, 0) + 1

    # Verify each shard returned at most effective_k_per_shard results
    for shard_tag, count in shard_counts.items():
        env.assertLessEqual(count, effective_k_per_shard,
                           message=f"Shard {shard_tag} returned {count} results, expected at most {effective_k_per_shard}")

    # Verify we got results from all 3 shards
    env.assertEqual(len(shard_counts), num_shards,
                   message=f"Should have results from all {num_shards} shards, got {len(shard_counts)}")

