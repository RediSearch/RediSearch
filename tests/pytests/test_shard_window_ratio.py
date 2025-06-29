"""
Shard window ratio test suite
"""

import math
from common import *
from common import to_dict
from includes import *


def setup_aggregate_test_data(env, index_name, num_docs=120, dim=2):
    """Shared setup function for FT.AGGREGATE tests with consistent data."""
    # Use exact pattern from working test_ft_aggregate_basic
    conn = getConnectionByEnv(env)

    # Create index with vector and metadata fields - use FLAT like working test
    conn.execute_command('FT.CREATE', index_name, 'SCHEMA',
                        'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32',
                        'DIM', dim, 'DISTANCE_METRIC', 'L2',
                        'category', 'TAG',
                        'priority', 'NUMERIC')

    # Add test data distributed across shards using exact pattern from working test
    categories = ['urgent', 'normal', 'low']

    # Create documents with even distribution across multiple shards
    # Use multiple hash slots to ensure even distribution across all shards
    hash_slots = ["{0}", "{1}", "{2}", "{3}", "{4}", "{5}"]

    for i in range(1, num_docs + 1):
        # Cycle through hash slots for even distribution
        hash_slot = hash_slots[i % len(hash_slots)]
        doc_key = f"doc{i}{hash_slot}"

        vector = create_np_array_typed([i] * dim, 'FLOAT32')
        category = categories[i % 3]
        priority = (i % 10) + 1
        conn.execute_command('HSET', doc_key,
                'v', vector.tobytes(),
                'category', category,
                'priority', priority)

    waitForIndex(env, index_name)

    # Return consistent query vector using simple pattern like working test
    query_vec = create_np_array_typed([0] * dim, 'FLOAT32')
    return query_vec


def test_shard_window_ratio_parameter_validation():
    """Test parameter validation and error handling for shard window ratio."""
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)
    
    dim = 2
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', dim, 'DISTANCE_METRIC', 'L2').ok()
    
    # Add a test document
    vector = create_np_array_typed([1] * dim, 'FLOAT32')
    conn.execute_command('HSET', 'doc1', 'v', vector.tobytes())
    
    waitForIndex(env, 'idx')

    query_vec = create_np_array_typed([1] * dim, 'FLOAT32')

    # Test invalid ratio values
    invalid_ratios = [0.0, -0.1, 1.1, 2.0, "invalid"]
    
    for ratio in invalid_ratios:
        # Should return error for invalid ratios
        env.expect('FT.SEARCH', 'idx',
                  f'*=>[KNN 10 @v $query_vec]=>{{$shard_window_ratio: {ratio}}}',
                  'PARAMS', 2, 'query_vec', query_vec.tobytes()).error()
    
    # Test malformed syntax scenarios
    env.debugPrint("\n=== Testing malformed syntax scenarios ===", force=True)
    
    malformed_queries = [
        # Missing closing brace
        '*=>[KNN 10 @v $query_vec]=>{$shard_window_ratio: 0.5',
        # Missing colon
        '*=>[KNN 10 @v $query_vec]=>{$shard_window_ratio 0.5}',
    ]
    
    for malformed_query in malformed_queries:
        env.expect('FT.SEARCH', 'idx', malformed_query,
                  'PARAMS', 2, 'query_vec', query_vec.tobytes()).error()


    # Test invalid ratio values for FT.AGGREGATE
    for ratio in invalid_ratios:
        # Should return error for invalid ratios
        env.expect('FT.AGGREGATE', 'idx',
                  f'*=>[KNN 10 @v $query_vec]=>{{$shard_window_ratio: {ratio}}}',
                  'PARAMS', 2, 'query_vec', query_vec.tobytes()).error()

    # Test malformed syntax scenarios for FT.AGGREGATE
    for i, malformed_query in enumerate(malformed_queries):
        env.debugPrint(f"Testing malformed query {i}: {malformed_query}", force=True)
        env.expect('FT.AGGREGATE', 'idx', malformed_query,
                  'PARAMS', 2, 'query_vec', query_vec.tobytes()).error()



def test_ft_profile_shard_result_validation_scenarios():
    """Test comprehensive scenarios for shard window ratio validation."""
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    # Create index
    dim = 64
    env.expect('FT.CREATE', 'idx_scenarios', 'SCHEMA',
               'v', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', dim, 'DISTANCE_METRIC', 'L2').ok()

    # Add test documents - ensure we have enough for all test scenarios
    num_docs = 2000  # Increased to ensure we always have enough results
    p = conn.pipeline(transaction=False)
    for i in range(1, num_docs + 1):
        vector = create_np_array_typed([i % 100] * dim, 'FLOAT32')
        p.execute_command('HSET', f'doc{i}', 'v', vector.tobytes())
    p.execute()

    waitForIndex(env, 'idx_scenarios')

    query_vec = create_np_array_typed([50] * dim, 'FLOAT32')

    # Test scenarios with different characteristics
    # Expected results depend on cluster vs standalone mode
    scenarios = [
        {
            'k': 3,
            'ratio': 0.3,
            'expected_effective_k': 1,
            'expected_total_results_cluster': 3,  # User always gets K results in cluster
            'expected_total_results_standalone': 1,  # effectiveK in standalone
        },
        {
            'k': 10,
            'ratio': 0.3,
            'expected_effective_k': 3,
            'expected_total_results_cluster': 9,  # 3 effectiveK × 3 shards = 9 max possible
            'expected_total_results_standalone': 3,  # effectiveK in standalone
        },
        {
            'k': 20,
            'ratio': 0.5,
            'expected_effective_k': 10,
            'expected_total_results_cluster': 20,  # User always gets K results in cluster
            'expected_total_results_standalone': 10,  # effectiveK in standalone
        },
        {
            'k': 15,
            'ratio': 1.0,
            'expected_effective_k': 15,
            'expected_total_results_cluster': 15,  # User always gets K results in cluster
            'expected_total_results_standalone': 15,  # effectiveK in standalone (same as K)
        }
    ]

    for scenario in scenarios:
        k = scenario['k']
        ratio = scenario['ratio']

        profile_res = env.cmd('FT.PROFILE', 'idx_scenarios', 'SEARCH', 'QUERY',
                             f'*=>[KNN {k} @v $query_vec]=>{{$shard_window_ratio: {ratio}}}',
                             'PARAMS', 2, 'query_vec', query_vec.tobytes(),
                             'RETURN', 1, '__v_score')

        results, profile_info = profile_res
        env.assertGreater(results[0], 0, message=f"No results for {scenario}")

        # Validate final result count from coordinator
        # With shard window ratio optimization, we expect fewer results than K
        # Handle both RESP2 (list format) and RESP3 (dict format)
        if isinstance(results, dict):
            actual_result_count = results['total_results']
        else:
            actual_result_count = results[0]

        # Use expected result count based on cluster vs standalone mode
        if env.isCluster():
            expected_total = scenario['expected_total_results_cluster']
            mode_desc = f"effectiveK={scenario['expected_effective_k']} × {env.shardsCount} shards"
        else:
            expected_total = scenario['expected_total_results_standalone']
            mode_desc = f"effectiveK={scenario['expected_effective_k']} (standalone)"

        # Debug output to see all scenario results
        env.debugPrint(f"Scenario {scenario}: K={k}, ratio={ratio}, {mode_desc}, expected={expected_total}, got={actual_result_count}")

        env.assertEqual(actual_result_count, expected_total,
                       message=f"With K={k}, ratio={ratio}: expected {expected_total} results ({mode_desc}), got {actual_result_count}")

        # Validate shard result processing for this scenario
        profile_dict = to_dict(profile_info)
        _validate_individual_shard_results(env, profile_dict, scenario)



def test_ft_aggregate_shard_window_ratio_profile():
    """Test FT.AGGREGATE with shard window ratio and profile metrics"""
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    env.debugPrint("\n=== Testing FT.AGGREGATE with shard window ratio ===", force=True)

    dim = 2  
    query_vec = setup_aggregate_test_data(env, 'idx_agg', num_docs=120, dim=dim)
    k = 30  
    ratio = 0.4  # Each shard should return ~12 results instead of 30

    # Create working query vector for all tests
    working_query_vec = create_np_array_typed([0] * dim, 'FLOAT32')

    conn = getConnectionByEnv(env)

    # FT.AGGREGATE with shard window ratio
    agg_query = "*=>[KNN " + str(k) + " @v $BLOB]=>{$shard_window_ratio: " + str(ratio) + "; $yield_distance_as: dist}"
    agg_res = conn.execute_command("FT.AGGREGATE", "idx_agg", agg_query,
                                  "PARAMS", 2, "BLOB", working_query_vec.tobytes(),
                                  "LIMIT", 0, k + 1)

    # Handle both RESP2 (list format) and RESP3 (dict format)
    if isinstance(agg_res, dict):
        actual_agg_count = agg_res['total_results']
    else:
        actual_agg_count = len(agg_res) - 1

    # Expected results depend on cluster vs standalone mode
    if env.isCluster():
        expected_agg_count = k  # Coordinator should return exactly K results in cluster
        mode_desc = "cluster mode"
    else:
        expected_agg_count = max(1, math.ceil(k * ratio))  # effectiveK in standalone
        mode_desc = "standalone mode"

    env.assertEqual(actual_agg_count, expected_agg_count,
                   message=f"FT.AGGREGATE in {mode_desc} should return {expected_agg_count} results, got {actual_agg_count}")


    # Test FT.PROFILE with FT.AGGREGATE and shard window ratio

    profile_query = "*=>[KNN " + str(k) + " @v $BLOB]=>{$shard_window_ratio: " + str(ratio) + "; $yield_distance_as: dist}"
    profile_res = env.cmd('FT.PROFILE', 'idx_agg', 'AGGREGATE', 'QUERY', profile_query,
                         'PARAMS', 2, 'BLOB', working_query_vec.tobytes())

    results, profile_info = profile_res
    env.assertGreater(results[0], 0, message="Profile should return results")

    # Validate final result count from coordinator for FT.PROFILE AGGREGATE
    # Handle both RESP2 (list format) and RESP3 (dict format)
    if isinstance(results, dict):
        actual_profile_count = results['total_results']
    else:
        actual_profile_count = len(results) - 1

    # Expected results depend on cluster vs standalone mode
    if env.isCluster():
        expected_profile_count = k  # Coordinator should return exactly K results in cluster
        mode_desc = "cluster mode"
    else:
        expected_profile_count = max(1, math.ceil(k * ratio))  # effectiveK in standalone
        mode_desc = "standalone mode"

    env.assertEqual(actual_profile_count, expected_profile_count,
                     message=f"FT.PROFILE AGGREGATE in {mode_desc} should return {expected_profile_count} results, got {actual_profile_count}")

    # Validate profile structure and metrics
    profile_dict = to_dict(profile_info)

    # Validate consistent profile structure
    env.assertEqual(set(profile_dict.keys()), {'Shards', 'Coordinator'},
                   message="FT.AGGREGATE profile should have exactly 'Shards' and 'Coordinator' sections")

    # Extract and validate coordinator section
    coordinator_section = profile_dict['Coordinator']
    env.assertIsInstance(coordinator_section, list,
                       message="Coordinator section should be a list")

    # Convert to dict and validate shard window ratio metrics are present
    coord_dict = {}
    for i in range(0, len(coordinator_section), 2):
        if i + 1 < len(coordinator_section):
            coord_dict[coordinator_section[i]] = coordinator_section[i + 1]

    # Validate optimization effectiveness using individual shard results
    # (coordinator shard_window_ratio metrics have been removed)
    basic_scenario = {
        'k': k,
        'ratio': ratio,
        'expected_effective_k': max(1, math.ceil(k * ratio)),
    }
    _validate_individual_shard_results(env, profile_dict, basic_scenario)


def test_ft_aggregate_profile_shard_result_validation_scenarios():
    """Test FT.PROFILE with FT.AGGREGATE and shard window ratio across different result scenarios"""
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    env.debugPrint("\n=== Testing FT.AGGREGATE shard result validation scenarios ===", force=True)

    # Use shared setup function to ensure consistent data
    dim = 2  # Use same dimension as working test
    query_vec = setup_aggregate_test_data(env, 'idx_agg_scenarios', num_docs=120, dim=dim)

    # Test scenarios with different characteristics for FT.AGGREGATE
    # Expected results depend on cluster vs standalone mode
    scenarios = [
        {
            'k': 6,
            'ratio': 0.5,
            'expected_effective_k': 3,
            'expected_total_results_cluster': 6,  # User always gets K results in cluster
            'expected_total_results_standalone': 3,  # effectiveK in standalone
        },
        {
            'k': 30,
            'ratio': 0.3,
            'expected_effective_k': 9,
            'expected_total_results_cluster': 27,  # 9 effectiveK × 3 shards = 27 max possible
            'expected_total_results_standalone': 9,  # effectiveK in standalone
        },
        {
            'k': 24,
            'ratio': 0.4,
            'expected_effective_k': 10,
            'expected_total_results_cluster': 24,  # User always gets K results in cluster
            'expected_total_results_standalone': 10,  # effectiveK in standalone
        },
        {
            'k': 10,
            'ratio': 1.0,
            'expected_effective_k': 10,
            'expected_total_results_cluster': 10,  # User always gets K results in cluster
            'expected_total_results_standalone': 10,  # effectiveK in standalone (same as K)
        }
    ]

    for scenario in scenarios:
        k = scenario['k']
        ratio = scenario['ratio']

        # Test 1: Basic FT.AGGREGATE with shard window ratio and profile
        profile_res = env.cmd('FT.PROFILE', 'idx_agg_scenarios', 'AGGREGATE', 'QUERY',
                             f'*=>[KNN {k} @v $query_vec]=>{{$shard_window_ratio: {ratio}; $yield_distance_as: dist}}',
                             'PARAMS', 2, 'query_vec', query_vec.tobytes(),
                             'LOAD', 3, '@category', '@priority', '@dist')

        results, profile_info = profile_res
        env.assertGreater(results[0], 0, message=f"No results for {scenario}")

        # Validate final result count from coordinator (same as FT.SEARCH scenarios)
        # Handle both RESP2 (list format) and RESP3 (dict format)
        if isinstance(results, dict):
            actual_result_count = results['total_results']
        else:
            actual_result_count = len(results) - 1  # FT.AGGREGATE format: [num_fields, result1, result2, ...]

        # Use expected result count based on cluster vs standalone mode
        if env.isCluster():
            expected_total = scenario['expected_total_results_cluster']
            mode_desc = f"effectiveK={scenario['expected_effective_k']} × {env.shardsCount} shards"
        else:
            expected_total = scenario['expected_total_results_standalone']
            mode_desc = f"effectiveK={scenario['expected_effective_k']} (standalone)"

        # Debug output to see all scenario results
        env.debugPrint(f"FT.AGGREGATE Scenario {scenario}: K={k}, ratio={ratio}, {mode_desc}, expected={expected_total}, got={actual_result_count}")

        env.assertEqual(actual_result_count, expected_total,
                       message=f"FT.AGGREGATE: With K={k}, ratio={ratio}: expected {expected_total} results ({mode_desc}), got {actual_result_count}")

        # Validate profile structure
        profile_dict = to_dict(profile_info)

        # Validate profile structure - should have consistent format
        env.assertEqual(set(profile_dict.keys()), {'Shards', 'Coordinator'},
                       message="Profile should have exactly 'Shards' and 'Coordinator' sections")

        # Validate optimization effectiveness using individual shard results
        # (coordinator shard_window_ratio metrics have been removed)
        _validate_individual_shard_results(env, profile_dict, scenario)




def _validate_individual_shard_results(env, profile_dict, scenario):
    """Validate individual shard results from FT.PROFILE output"""

    # Extract individual shard information from the Shards section
    env.assertIn('Shards', profile_dict, message="Profile should contain Shards section")

    shards_section = profile_dict['Shards']
    env.assertIsInstance(shards_section, list, message="Shards section should be a list")

    shard_result_counts = []
    expected_effective_k = scenario['expected_effective_k']
    num_shards = len(shards_section)

    # Parse each shard's results
    for i, shard_data in enumerate(shards_section):
        env.assertIsInstance(shard_data, list, message=f"Shard {i} data should be a list")

        # Convert shard data to dict
        shard_dict = {}
        for j in range(0, len(shard_data), 2):
            if j + 1 < len(shard_data):
                shard_dict[shard_data[j]] = shard_data[j + 1]

        # Look for result count in Iterators profile Counter (the standard location)
        result_count = None

        if 'Iterators profile' in shard_dict:
            iterators_profile = shard_dict['Iterators profile']
            if isinstance(iterators_profile, list):
                # Convert to dict and look for Counter (which represents result count)
                iter_dict = {}
                for j in range(0, len(iterators_profile), 2):
                    if j + 1 < len(iterators_profile):
                        iter_dict[iterators_profile[j]] = iterators_profile[j + 1]

                # Look for Counter which represents the number of results processed
                if 'Counter' in iter_dict:
                    result_count = int(iter_dict['Counter'])

        # Require that we find a valid result count - be strict
        env.assertIsNotNone(result_count,
                           message=f"Could not find result count in shard {i} Iterators profile")
        env.assertGreaterEqual(result_count, 0,
                              message=f"Invalid result count {result_count} in shard {i}")

        shard_result_counts.append(result_count)

    # Validate shard-level behavior - we should have results from all shards
    env.assertEqual(len(shard_result_counts), num_shards,
                   message=f"Should have result counts from all {num_shards} shards")

    # Simple, direct validation - each shard should process exactly the expected effective K
    for i, count in enumerate(shard_result_counts):
        env.assertEqual(count, expected_effective_k,
                       message=f"Shard {i} should process exactly {expected_effective_k} results, got {count}")
