from common import *
from vecsim_utils import set_up_database_with_vectors, create_random_np_array_typed

def calculate_effective_k(original_k, ratio, num_shards):
    """Calculate effective K using the PRD formula: max(top_k/#shards, ceil(top_k × ratio))"""

    if num_shards == 1:
        return original_k  # In standalone mode, shard_k_ratio is ignored

    # Calculate minimum K per shard to ensure we can return full original_k results
    # Use ceiling division: (original_k + num_shards - 1) // num_shards
    min_k_per_shard = (original_k + num_shards - 1) // num_shards

    # Calculate ratio-based K per shard
    ratio_k_per_shard = math.ceil(original_k * ratio)


    # Apply PRD formula: max(top_k/#shards, ceil(top_k × ratio))
    return max(ratio_k_per_shard, min_k_per_shard)

def ValidateError(env, res: Query, expected_error_message, message="", depth=1):
    env.assertTrue(res.errorRaised, message=message, depth=depth)
    env.assertContains(expected_error_message, res.res, message=message, depth=depth)

def _validate_individual_shard_results(env, profile_dict, k, ratio, scenario_description):

    shards_section = profile_dict['Shards']

    env.assertEqual(len(shards_section), env.shardsCount, depth=1, message=f"Validate shards count in profile_dict['Shards']")

    # Calculate expected results per shard
    effective_k = calculate_effective_k(k, ratio, env.shardsCount)

    # Parse each shard's results
    for i, shard in enumerate(shards_section):

        index_rp_profile = shard['Result processors profile'][0] #index_rp is always first

            # Look for Counter which represents the number of results processed
        shard_result_count = index_rp_profile['Counter']
        env.assertEqual(shard_result_count, effective_k,
        message=f"In scenario {scenario_description}: With k={k}, ratio: {ratio}, Shard {i} expected {effective_k} results, got {shard_result_count}", depth=1)

@skip(cluster=False) # shard_k_ratio is ignored is SA
def test_shard_k_ratio_parameter_validation():
    """Test parameter validation and error handling for shard k ratio."""
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    dim = 1
    datatype = 'FLOAT32'
    set_up_database_with_vectors(env, dim, num_docs=1, index_name='idx', datatype='FLOAT32')

    query_vec = create_random_np_array_typed(dim, datatype)

    # Test invalid ratio values
    invalid_ratios = [0.0, -0.1, 1.1, 2.0, 0, 7, "invalid"]

    malformed_queries = [
        {"error": "Missing closing brace",
         "query": '*=>[KNN 10 @v $query_vec]=>{$shard_k_ratio: 0.5'},
        {"error":'Missing colon',
            "query": '*=>[KNN 10 @v $query_vec]=>{$shard_k_ratio 0.5}'},
    ]
    for cmd in ['FT.SEARCH', 'FT.AGGREGATE']:
        for ratio in invalid_ratios:
            # Should return error for invalid ratios
            res = env.expect(cmd, 'idx',
                      f'*=>[KNN 10 @v $query_vec]=>{{$shard_k_ratio: {ratio}}}',
                      'PARAMS', 2, 'query_vec', query_vec.tobytes(), 'nocontent')
            ValidateError(env, res, message=f"{cmd} expected error for invalid shard k ratio: {ratio}",
                          expected_error_message="Invalid shard k ratio value")


        for malformed_query in malformed_queries:
            res = env.expect(cmd, 'idx', malformed_query['query'],
                    'PARAMS', 2, 'query_vec', query_vec.tobytes(), 'nocontent')
            ValidateError(env, res,
                        message=f"{cmd} expected error for {malformed_query['error']} in: {malformed_query['query']}",
                        expected_error_message=f"Syntax error")

def test_ft_profile_shard_result_validation_scenarios():
    """Test comprehensive scenarios for shard window ratio validation."""
    env = Env(moduleArgs='DEFAULT_DIALECT 2', protocol=3)

    dim = 1
    datatype = 'FLOAT32'
    k = 100
    num_docs = k * env.shardsCount * 3 # ensure we always have enough results in each shard
    set_up_database_with_vectors(env, dim, num_docs=num_docs, index_name='idx', datatype='FLOAT32')

    query_vec = create_random_np_array_typed(dim, datatype)

    # Test scenarios with different characteristics
    # effectiveK = max(top_k/#shards, ceil(top_k × ratio))
    # - In cluster mode: coordinator returns exactly K results to user, shards process effectiveK
    # - In standalone mode: k_ratio is ignored, and we always return K results
    min_shard_ratio = 1 / float(env.shardsCount)
    ratios = [min_shard_ratio, 0.01, 0.9, 1.0]  # Valid ratios


    for ratio in ratios:
        k_param_style_command_args = {
            "k_as_literal": [f'*=>[KNN {k} @v $query_vec]=>{{$shard_k_ratio: {ratio}}}',
                                        'PARAMS', 2, 'query_vec', query_vec.tobytes(),],

            "k_in_param": [f'*=>[KNN $k @v $query_vec]=>{{$shard_k_ratio: {ratio}}}',
                                        'PARAMS', 4, 'query_vec', query_vec.tobytes(), 'k', k,]
        }
        for k_style, command_args in k_param_style_command_args.items():
            for cmd in ['SEARCH', 'AGGREGATE']:
                # Determine expected results based on deployment mode
                profile_res = env.cmd('FT.PROFILE', 'idx', f'{cmd}', 'QUERY',
                                    *command_args,
                                    'nocontent', "LIMIT", 0, k + 1)

                _validate_individual_shard_results(env, profile_res['Profile'], k, ratio, scenario_description=f"{cmd} {k_style}")

                # Validate final result count
                actual_result_count = len(profile_res['Results']['results'])

                env.assertEqual(actual_result_count, k,
                            message=f"{cmd} With K={k}, ratio={ratio}: expected {k} results, got {actual_result_count}")

def test_k_0():
    env = Env(moduleArgs='DEFAULT_DIALECT 2')

    dim = 1
    datatype = 'FLOAT32'
    set_up_database_with_vectors(env, dim, num_docs=10, index_name='idx', datatype='FLOAT32')

    query_vec = create_random_np_array_typed(dim, datatype)

    k = 0
    ratio = 0.5
    query = f'*=>[KNN {k} @v $query_vec]=>{{$shard_k_ratio: {ratio}}}'
    params_and_args = ["PARAMS", 2, "query_vec", query_vec.tobytes(), "LIMIT", 0, k + 1]

    res = env.cmd('FT.SEARCH', "idx", query, *params_and_args, "return", 1, "__v_score")
    env.assertEqual(len(res[1:]), 0)

    res = env.cmd('FT.AGGREGATE', "idx", query, *params_and_args, "load", 1, "__v_score")
    env.assertEqual(len(res[1:]), 0)

def test_query():
    """Test FT.AGGREGATE with shard k ratio and profile metrics"""
    env = Env(moduleArgs='DEFAULT_DIALECT 2')
    conn = getConnectionByEnv(env)

    dim = 2
    datatype = 'FLOAT32'
    k = 100
    num_docs = k * env.shardsCount * 3 # ensure we always have enough results in each shard

    set_up_database_with_vectors(env, dim, num_docs=num_docs, index_name='idx', datatype='FLOAT32', additional_schema_args=['n', 'NUMERIC'])

    # Add numeric field to each document
    for i in range(1, num_docs + 1):
        conn.execute_command('HSET', f'doc{i}', 'n', i)

    query_vec = create_random_np_array_typed(dim, datatype)

    min_shard_ratio = 1 / float(env.shardsCount)
    ratios = [min_shard_ratio, 0.01, 0.9, 1.0]  # Valid ratios

    def validate_len(command, query, actual_result_count):
        env.assertEqual(actual_result_count, k,
                    message=f"{command} for query {query} with k={k}, ratio={ratio}: expected {k} results, got {actual_result_count}", depth=1)

    for ratio in ratios:
        # Test simple query
        # k as parameter
        query = f'*=>[KNN $k_costume @v $query_vec]=>{{$shard_k_ratio: {ratio}}}'
        params_and_args = ["PARAMS", 4, "query_vec", query_vec.tobytes(), "k_costume", k, "LIMIT", 0, k + 1]

        res = env.cmd('FT.SEARCH', "idx", query, *params_and_args, "nocontent")
        validate_len("FT.SEARCH", query, len(res[1:]))

        res = env.cmd('FT.AGGREGATE', "idx", query, *params_and_args)
        validate_len("FT.AGGREGATE", query, len(res[1:]))

        # k as literal
        query = f'*=>[KNN {k} @v $query_vec]=>{{$shard_k_ratio: {ratio}}}'
        params_and_args = ["PARAMS", 2, "query_vec", query_vec.tobytes(), "LIMIT", 0, k + 1]

        res = env.cmd('FT.SEARCH', "idx", query, *params_and_args, "nocontent")
        validate_len("FT.SEARCH", query, len(res[1:]))

        res = env.cmd('FT.AGGREGATE', "idx", query, *params_and_args)
        validate_len("FT.AGGREGATE", query, len(res[1:]))


        # Additional args in query

        query = f'*=>[KNN {k} @v $query_vec]=>{{$shard_k_ratio: {ratio}; $yield_distance_as: dist}}'
        # reuse previous params_and_args
        res = env.cmd('FT.SEARCH', "idx", query, *params_and_args, "return", 1, "dist")
        validate_len("FT.SEARCH", query, len(res[1:]) // 2)

        for i in range(k):
            env.assertTrue('dist' in res[1 + 1 + i * 2][0], message=f"Missing 'dist' field in result {i}")

        res = env.cmd('FT.AGGREGATE', "idx", query, *params_and_args, "LOAD", 1, "dist")
        validate_len("FT.AGGREGATE", query, len(res[1:]))
        for i in range(k):
            env.assertTrue('dist' in res[1 + i][0], message=f"Missing 'dist' field in result {i}")

        # Hybrid query
        query = f'@n:[0 inf]=>[KNN {k} @v $query_vec]=>{{$shard_k_ratio: {ratio}}}'
        # reuse previous params_and_args
        res = env.cmd('FT.SEARCH', "idx", query, *params_and_args, "nocontent")
        validate_len("FT.SEARCH", query, len(res[1:]))

        res = env.cmd('FT.AGGREGATE', "idx", query, *params_and_args)
        validate_len("FT.AGGREGATE", query, len(res[1:]))

@skip(cluster=False)  # Only relevant for cluster mode
def test_insufficient_docs_per_shard():
    """Test scenario where not all shards have enough docs to return ceil(k/num_shards) results"""
    env = Env(moduleArgs='DEFAULT_DIALECT 2')

    # This test is using hardcoded shard distribution, so it only works with 3 shards
    num_shards = 3
    if env.shardsCount != num_shards:
        env.skip()

    conn = getConnectionByEnv(env)

    dim = 2
    datatype = 'FLOAT32'
    k = 5  # Request 5 results
    effectiveK = (k + num_shards - 1) // num_shards
    # Set up database with 10 documents initially
    num_initial_docs = 20
    set_up_database_with_vectors(env, dim, num_initial_docs, 'idx', datatype)
    query_vec = create_random_np_array_typed(dim, datatype)

    # The database contains k(5) results in total.
    # However, since in this case effectiveK = 2, some shards won't have enough results,
    # and effectiveK is not enough to close the gap with the larger shards results.
    target_keys_in_shard = [1, 1, 3]
    # In total we will get: 1 + 1 + effectiveK(2) = 4 results
    expected_k = 4

    # Reduce keys in each shard to target count
    for i, shard_conn in enumerate(env.getOSSMasterNodesConnectionList()):
        keys = shard_conn.execute_command('KEYS', '*')
        shard_keys_count = len(keys)
        env.assertGreaterEqual(shard_keys_count, target_keys_in_shard[i], message=f"Shard {i} doesn't have enough keys")
        keys_to_delete = shard_keys_count - target_keys_in_shard[i]

        for i in range(keys_to_delete):
            conn.execute_command('DEL', keys[i])

    query = f'*=>[KNN {k} @v $query_vec]=>{{$shard_k_ratio: 0.1}}' # smaller ratio than min_shard_ratio
    params_and_args = ["PARAMS", 2, "query_vec", query_vec.tobytes(), "LIMIT", 0, k + 1]

    res = env.cmd('FT.SEARCH', "idx", query, *params_and_args, "nocontent")
    env.assertEqual(len(res[1:]), expected_k)

    res = env.cmd('FT.AGGREGATE', "idx", query, *params_and_args)
    env.assertEqual(len(res[1:]), expected_k)
