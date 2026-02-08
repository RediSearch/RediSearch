"""
Tests for SHARD_K_RATIO parameter in FT.HYBRID command.

SHARD_K_RATIO optimizes KNN query execution in distributed/cluster environments
by controlling how many results each shard returns using the formula:
    effectiveK = max(K/#shards, ceil(K × ratio))

Valid ratio range: (0.0, 1.0] (exclusive 0, inclusive 1)
Default ratio: 1.0 (no optimization - each shard returns full K results)
"""

from common import (
    skip,
    Env,
    getConnectionByEnv,
    create_np_array_typed,
    create_random_np_array_typed,
)


def _validate_individual_shard_results(env, profile_response, k, expected_effective_k):
    """Validate that each shard processed the expected number of results.

    For FT.HYBRID profile, the structure is:
    - profile_response[8] = ['Shards', [shard_profiles...], 'Coordinator', coordinator_profile]
    - Each shard_profile = ['Shard ID', id, 'SEARCH', [...], 'VSIM', vsim_profile]
    - vsim_profile contains 'Result processors profile' with Index RP first
    """
    shard_profiles = profile_response[8][1]

    env.assertEqual(len(shard_profiles), env.shardsCount, depth=1,
                   message="Validate shards count in profile")

    # Parse each shard's results
    for i, shard_profile in enumerate(shard_profiles):
        # shard profile has the following structure:
        # ['Shard ID', id, 'SEARCH', [...], 'VSIM', vsim_profile]
        vsim_profile = shard_profile[5]  # VSIM profile is at index 5
        result_processors_profile = vsim_profile[7]

        # Index RP is always first
        index_rp_profile = result_processors_profile[0]
        # Result processors profile has the following structure:
        # ['Type', 'Index', 'Results processed', 5]
        shard_result_count = index_rp_profile[3]
        env.assertEqual(
            shard_result_count, expected_effective_k, depth=1,
            message=f"Shard {i} expected {expected_effective_k} results, got {shard_result_count}")


def _validate_hybrid_error(env, res, expected_error_message, message="", depth=1):
    """Helper to validate error response from FT.HYBRID command"""
    env.assertTrue(res.errorRaised, message=message, depth=depth + 1)
    env.assertContains(expected_error_message, res.res, message=message, depth=depth + 1)


def setup_basic_index(env, dim=2, docs_per_shard=None, uniform_vectors=True):
    """
    Create a basic index with optional sharded document distribution.

    Args:
        env: The test environment
        dim: Vector dimension (default: 2)
        docs_per_shard:
            Optional list of integers specifying how many documents each shard
            should contain (e.g., [1, 1, 5]).
            If None, creates a single document for simple tests.
    """
    env.expect(
        'FT.CREATE', 'idx', 'SCHEMA',
        'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', dim,
            'DISTANCE_METRIC', 'L2',
        't', 'TEXT',
        'tag', 'TAG',
        'shard_tag', 'TAG').ok()

    if docs_per_shard is not None:
        setup_sharded_documents(env, docs_per_shard, dim, uniform_vectors)
    else:
        conn = getConnectionByEnv(env)
        vec = create_np_array_typed([1.0] * dim)
        conn.execute_command('HSET', 'doc:1', 'v', vec.tobytes(), 't', 'hello')


def setup_sharded_documents(env, docs_per_shard, dim, uniform_vectors=True):
    """
    Set up documents distributed across shards according to a specified target
    distribution.

    This helper creates documents with hash tags to control shard distribution
    in a 3-shard cluster. Each document includes a vector field, text field, and
    shard_tag field for tracking which shard it belongs to.

    Args:
        env: The test environment
        docs_per_shard: List of integers specifying how many documents
                        each shard should contain (e.g., [1, 1, 5])
        dim: Vector dimension

    Note:
        - Uses hardcoded hash tags ['{shard:0}', '{shard:1}', '{shard:3}']
          for 3-shard clusters
        - Vector values are based on doc_idx only (not shard_idx) to ensure
          uniform distribution across shards
        - Verifies each shard has the expected number of documents
    """
    # Hash tags that distribute to different shards in a 3-shard cluster
    shard_hash_tags = ['{shard:0}', '{shard:1}', '{shard:3}']
    conn = getConnectionByEnv(env)
    # Create documents with hash tags to control shard distribution
    # Use the same vector values across all shards so distances are uniform
    for shard_idx in range(3):
        hash_tag = shard_hash_tags[shard_idx]
        shard_tag_value = hash_tag.strip('{}')  # e.g., 'shard:0'
        for doc_idx in range(docs_per_shard[shard_idx]):
            doc_key = f'{hash_tag}:doc{doc_idx}'

            if uniform_vectors:
                # Use doc_idx only (not shard_idx) so all shards have same
                # vector distribution
                vector = ([float(doc_idx)] * dim)
            else:
                # Use doc_idx + shard_idx offset so each shard has unique vector
                # distribution
                vector = ([float(doc_idx) + 5 * shard_idx] * dim)

            vec = create_np_array_typed(vector)
            conn.execute_command('HSET', doc_key, 'v', vec.tobytes(),
                                 't', 'some text',
                                 'shard_tag', shard_tag_value)

    # Verify each shard has the expected number of documents
    for shard_idx, shard_conn in enumerate(env.getOSSMasterNodesConnectionList()):
        keys = shard_conn.execute_command('KEYS', '*')
        env.assertEqual(
            len(keys), docs_per_shard[shard_idx],
            message=f"Shard {shard_idx} should have {docs_per_shard[shard_idx]} keys, got {len(keys)}")


def test_shard_k_ratio_parameter_validation():
    """Test SHARD_K_RATIO parameter validation and error handling for FT.HYBRID."""
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    setup_basic_index(env)
    query_vec = create_np_array_typed([2.0] * 2)

    # Test invalid ratio values - below minimum, above maximum, negative
    # Error message is "Invalid shard k ratio value" from ValidateShardKRatio
    invalid_ratios = [0.0, -0.1, 1.1, 2.0, 0, 7]
    for ratio in invalid_ratios:
        res = env.expect('FT.HYBRID', 'idx', '2', 'SEARCH', 'hello',
                         'VSIM', '@v', '$BLOB',
                           'KNN', '4', 'K', '5', 'SHARD_K_RATIO', ratio,
                         'PARAMS', '2', 'BLOB', query_vec.tobytes())
        _validate_hybrid_error(
            env, res, "Invalid shard k ratio value",
            message=f"FT.HYBRID expected error for invalid shard k ratio: {ratio}")

    # Test non-numeric value
    res = env.expect('FT.HYBRID', 'idx', '2', 'SEARCH', 'hello',
                     'VSIM', '@v', '$BLOB',
                        'KNN', '4', 'K', '5', 'SHARD_K_RATIO', 'invalid',
                     'PARAMS', '2', 'BLOB', query_vec.tobytes())
    _validate_hybrid_error(
        env, res, "Invalid shard k ratio value",
        message="FT.HYBRID expected error for non-numeric shard k ratio")


def test_shard_k_ratio_missing_value():
    """Test SHARD_K_RATIO with missing value for FT.HYBRID."""
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    setup_basic_index(env)
    query_vec = create_np_array_typed([2.0] * 2)

    # Test missing value - SHARD_K_RATIO followed by PARAMS (which is not a
    # valid ratio)
    res = env.expect('FT.HYBRID', 'idx', '2', 'SEARCH', 'hello',
                     'VSIM', '@v', '$BLOB',
                        'KNN', '4', 'K', '5', 'SHARD_K_RATIO',
                     'PARAMS', '2', 'BLOB', query_vec.tobytes())
    _validate_hybrid_error(
        env, res, "Invalid shard k ratio value",
        message="FT.HYBRID expected error for missing SHARD_K_RATIO value")


def test_shard_k_ratio_duplicate():
    """Test duplicate SHARD_K_RATIO parameter for FT.HYBRID."""
    env = Env(moduleArgs='DEFAULT_DIALECT 2')

    setup_basic_index(env)
    query_vec = create_np_array_typed([2.0] * 2)

    # Test duplicate SHARD_K_RATIO
    # Note: The parser handles the second SHARD_K_RATIO as an unknown argument
    res = env.expect('FT.HYBRID', 'idx', '2', 'SEARCH', 'hello',
                     'VSIM', '@v', '$BLOB',
                        'KNN', '6', 'K', '5', 'SHARD_K_RATIO', '0.5',
                            'SHARD_K_RATIO', '0.8',
                     'PARAMS', '2', 'BLOB', query_vec.tobytes())
    _validate_hybrid_error(
        env, res, "SHARD_K_RATIO",
        message="FT.HYBRID expected error for duplicate SHARD_K_RATIO")


def test_shard_k_ratio_small_k():
    """Test SHARD_K_RATIO with small K values (1, 2, 3).

    By using a SEARCH query that matches zero documents, only the VSIM
    subquery results are returned, so K directly controls the output count.
    """
    env = Env(moduleArgs='DEFAULT_DIALECT 2')

    dim = 2
    setup_basic_index(env, dim)

    conn = getConnectionByEnv(env)
    for i in range(1, 11):
        vec = create_np_array_typed([float(i)] * dim)
        conn.execute_command('HSET', f'doc{i}', 'v', vec.tobytes(), 't', 'some text')

    query_vec = create_random_np_array_typed(dim, 'FLOAT32')

    ratio = 0.5

    # Test small K values with a non-matching SEARCH query
    for k in [1, 2, 3]:
        res = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'nonexistent_term_xyz',
                      'VSIM', '@v', '$BLOB',
                        'KNN', '4', 'K', k, 'SHARD_K_RATIO', ratio,
                      'PARAMS', '2', 'BLOB', query_vec.tobytes())

        # Response format: ['total_results', N, 'results', [...], ...]
        actual_result_count = len(res[3])
        env.assertEqual(actual_result_count, k,
                       message=f"FT.HYBRID with K={k}: expected {k} results, got {actual_result_count}")


@skip(cluster=False)  # Only relevant for cluster mode
def test_shard_k_ratio_profile_verification():
    """Test SHARD_K_RATIO using FT.PROFILE to verify effectiveK per shard.

    This test uses FT.PROFILE HYBRID to verify that SHARD_K_RATIO actually
    limits the number of documents processed per shard according to the formula:
        effectiveK = max(K/#shards, ceil(K × ratio))
    """
    env = Env(moduleArgs='DEFAULT_DIALECT 2 _PRINT_PROFILE_CLOCK false')

    # This test requires exactly 3 shards due to hardcoded hash tags
    num_shards = 3
    if env.shardsCount != num_shards:
        env.skip()

    dim = 2
    k = 15  # Request 15 results total
    query_vec = create_np_array_typed([5.0] * dim)

    # Test different ratio values and verify effectiveK per shard
    # effectiveK = max(K/#shards, ceil(K × ratio))
    test_cases = [
        # (ratio, expected_effectiveK)
        (1.0, k),              # ratio=1.0: effectiveK = max(15/3, ceil(15*1.0)) = max(5, 15) = 15
        (0.5, 8),              # ratio=0.5: effectiveK = max(15/3, ceil(15*0.5)) = max(5, 8) = 8
        (0.2, 5),              # ratio=0.2: effectiveK = max(15/3, ceil(15*0.2)) = max(5, 3) = 5
        (1.0 / num_shards, 5), # ratio=1/3: effectiveK = max(15/3, ceil(15*0.33)) = max(5, 5) = 5
    ]

    for uniform_vectors in [True, False]:
        # Set up index with enough docs per shard
        setup_basic_index(env, dim, [k] * num_shards, uniform_vectors)
        for ratio, expected_effective_k in test_cases:
            # Run FT.PROFILE HYBRID with SHARD_K_RATIO
            res = env.cmd(
                'FT.PROFILE', 'idx', 'HYBRID', 'QUERY',
                'SEARCH', 'nonexistent_term_xyz',
                'VSIM', '@v', '$BLOB',
                    'KNN', '4', 'K', k, 'SHARD_K_RATIO', ratio,
                'PARAMS', '2', 'BLOB', query_vec.tobytes())

            _validate_individual_shard_results(env, res, k, expected_effective_k)
        env.flush()


@skip(cluster=False)  # Only relevant for cluster mode
def test_shard_k_ratio_insufficient_docs():
    """Test SHARD_K_RATIO when not all shards have enough documents.

    Tests the scenario where some shards don't have enough docs to return
    effectiveK results. When a shard has fewer documents than effectiveK,
    it returns all available documents. When a shard has more documents than
    effectiveK, it should return only effectiveK results.

    Target distribution: [1, 1, 3] docs per shard (5 total)
    With K=5, ratio=0.1:
      effectiveK = max(5/3, ceil(5*0.1)) = max(2, 1) = 2
    Expected results per shard:
      - Shard 0: 1 (all available docs, less than effectiveK)
      - Shard 1: 1 (all available docs, less than effectiveK)
      - Shard 2: 2 (limited by effectiveK, even though 3 docs available)
    Total expected: 1 + 1 + 2 = 4 results
    """
    env = Env(moduleArgs='DEFAULT_DIALECT 2')

    # This test requires exactly 3 shards due to hardcoded hash tags
    num_shards = 3
    if env.shardsCount != num_shards:
        env.skip()

    dim = 2
    k = 5  # Request 5 results
    ratio = 0.1

    # Set up index and documents: [1, 1, 5] docs per shard (unequal distribution)
    target_docs_per_shard = [1, 1, 5]
    setup_basic_index(env, dim, target_docs_per_shard)

    # Fixed query vector for reproducibility
    query_vec = create_np_array_typed([0.5] * dim)

    # Use non-matching SEARCH query so only VSIM results are returned
    # Use GROUPBY @shard_tag with REDUCE COUNT to count results per shard
    res = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'nonexistent_term_xyz',
                  'VSIM', '@v', '$BLOB',
                    'KNN', '4', 'K', k, 'SHARD_K_RATIO', ratio,
                  'LOAD', '1', '@shard_tag',
                  'GROUPBY', '1', '@shard_tag',
                  'REDUCE', 'COUNT', '0', 'AS', 'count',
                  'SORTBY', '2', '@shard_tag', 'ASC',
                  'PARAMS', '2', 'BLOB', query_vec.tobytes())

    # Response format: ['total_results', N, 'results', [...], ...]
    # With GROUPBY, results are grouped rows like:
    #                   [{'shard_tag': 'shard:0', 'count': '1'}, ...]
    results = res[3]

    # Expected effectiveK = max(5/3, ceil(5*0.1)) = max(2, 1) = 2
    # - Shard 0 with 1 doc returns 1 (all available, less than effectiveK)
    # - Shard 1 with 1 doc returns 1 (all available, less than effectiveK)
    # - Shard 2 with 3 docs should return only 2 (limited by effectiveK)
    expected_results = [
        ['shard_tag', 'shard:0', 'count', '1'],
        ['shard_tag', 'shard:1', 'count', '1'],
        ['shard_tag', 'shard:3', 'count', '2']
    ]
    env.assertEqual(results, expected_results)

    # Total expected: 1 + 1 + 2 = 4 (same as FT.SEARCH/FT.AGGREGATE)
    expected_result_count = 4
    total_count = sum(int(row[3]) for row in results)
    env.assertEqual(
        total_count, expected_result_count,
        message=f"FT.HYBRID with SHARD_K_RATIO: expected {expected_result_count} results, got {total_count}")
