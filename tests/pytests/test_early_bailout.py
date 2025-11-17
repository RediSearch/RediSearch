from common import *

# Test early bailout and empty results for FT.SEARCH, FT.AGGREGATE, FT.HYBRID
# In SA setting
# Currently, only OOM `return` policy initiates early bailout

OOM_QUERY_ERROR = "Not enough memory available to execute the query"
COORD_OOM_WARNING = "One or more shards failed to execute the query due to insufficient memory"
SHARD_OOM_WARNING = "Shard failed to execute the query due to insufficient memory"

def remove_keys_with_phrases(data, phrases):
    if isinstance(data, dict):
        new_dict = {}
        for key, value in data.items():
            # Check if key contains any phrase (case-insensitive)
            if not any(phrase.lower() in key.lower() for phrase in phrases):
                new_dict[key] = remove_keys_with_phrases(value, phrases)
        return new_dict

    elif isinstance(data, list):
        # Recurse into lists
        return [remove_keys_with_phrases(item, phrases) for item in data]

    else:
        # Base case: leave primitive values unchanged
        return data

def remove_keys_with_phrases_from_list(lst, phrases):
    def match(key):
        return any(p.lower() in str(key).lower() for p in phrases)

    result = []
    for i in range(0, len(lst), 2):
        key = lst[i]
        value = lst[i + 1] if i + 1 < len(lst) else None
        if not match(key):
            # If value is another list, recurse
            if isinstance(value, list):
                value = remove_keys_with_phrases_from_list(value, phrases)
            result.extend([key, value])
    return result

class TestEarlyBailoutEmptyResultsSA_Resp2:
    def __init__(self):
        skipTest(cluster=True)
        self.env = Env(protocol=2)
        self.env.expect('FT.CREATE', 'empty', 'PREFIX', '1', 'NonExistingPrefix:', 'SCHEMA', 't', 'TEXT').ok()
        self.env.expect('FT.CREATE', 'empty_hybrid', 'PREFIX', '1', 'NonExistingPrefix:', 'SCHEMA',
            'description', 'TEXT',
            'embedding', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()
        self.env.expect('FT.CREATE', 'not_empty', 'SCHEMA', 't', 'TEXT').ok()
        self.env.expect('FT.CREATE', 'not_empty_hybrid', 'SCHEMA',
            'description', 'TEXT',
            'embedding', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()

        self.env.expect('HSET', 'doc', 't', 'hello').equal(1)

        # Make sure the empty index returns empty results and not_empty returns 1 result
        res = self.env.cmd('FT.SEARCH', 'empty', '*')
        self.env.assertEqual(res[0], 0)
        res = self.env.cmd('FT.SEARCH', 'not_empty', '*')
        self.env.assertEqual(res[0], 1)

    def setUp(self):
        pass
    def tearDown(self):
        set_unlimited_maxmemory_for_oom(self.env)

    def test_early_bailout_search_resp2(self):

        query_params_to_check = [
            ['*'],
            ['*', 'DIALECT', '2'],
            ['*', 'DIALECT', '3'],
            ['*', 'DIALECT', '4'],
            ['*', 'WITHOUTCOUNT', 'DIALECT', '4']
        ]

        empty_results = {}

        for query_params in query_params_to_check:
            empty_results[' '.join(query_params)] = self.env.cmd('FT.SEARCH', 'empty', *query_params)


        change_oom_policy(self.env, 'return')
        # Change maxmemory to 1 to trigger OOM
        self.env.expect('CONFIG', 'SET', 'maxmemory', '1').ok()

        for query_params in query_params_to_check:
            res = self.env.cmd('FT.SEARCH', 'not_empty', *query_params)
            empty = empty_results[' '.join(query_params)]
            self.env.assertEqual(res, empty, message = 'Failed for query params: ' + ' '.join(query_params))

    def test_early_bailout_aggregate_resp2(self):

        query_params_to_check = [
            ['*'],
            ['*', 'DIALECT', '2'],
            ['*', 'DIALECT', '3'],
            ['*', 'DIALECT', '4'],
            ['*', 'WITHOUTCOUNT', 'DIALECT', '4'],
            ['*', 'WITHCURSOR'],
        ]

        empty_results = {}

        for query_params in query_params_to_check:
            empty_results[' '.join(query_params)] = self.env.cmd('FT.AGGREGATE', 'empty', *query_params)


        change_oom_policy(self.env, 'return')
        # Change maxmemory to 1 to trigger OOM
        self.env.expect('CONFIG', 'SET', 'maxmemory', '1').ok()


        for query_params in query_params_to_check:
            res = self.env.cmd('FT.AGGREGATE', 'not_empty', *query_params)
            empty = empty_results[' '.join(query_params)]
            if 'WITHCURSOR' in query_params:
                res = res[0]
                empty = empty[0]
            self.env.assertEqual(res, empty, message = 'Failed for query params: ' + ' '.join(query_params))


    def test_early_bailout_hybrid_resp2(self):

        query_params_to_check = [
            ['SEARCH', '*', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', '0'],
        ]

        empty_results = {}

        for query_params in query_params_to_check:
            empty_results[' '.join(query_params)] = self.env.cmd('FT.HYBRID', 'empty_hybrid', *query_params)

        change_oom_policy(self.env, 'return')
        # Change maxmemory to 1 to trigger OOM
        self.env.expect('CONFIG', 'SET', 'maxmemory', '1').ok()


        for query_params in query_params_to_check:
            res = self.env.cmd('FT.HYBRID', 'not_empty_hybrid', *query_params)
            empty = empty_results[' '.join(query_params)]
            # Assert OOM warning exists
            self.env.assertEqual(res[5][0], SHARD_OOM_WARNING)
            self.env.assertEqual(empty[5], [])
            # Clear warnings from results
            del res[5]
            del empty[5]
            # Clear execution time from results
            del res[6]
            del empty[6]

            self.env.assertEqual(res, empty, message = 'Failed for query params: ' + ' '.join(query_params))

    def test_early_bailout_profile_resp2(self):
        query_params_to_check = [
            ['SEARCH', 'QUERY', '*'],
            ['SEARCH', 'LIMITED', 'QUERY', '*'],
            ['AGGREGATE', 'QUERY', '*'],
            ['AGGREGATE', 'LIMITED', 'QUERY', '*'],
        ]
        empty_results = {}
        for query_params in query_params_to_check:
            empty_results[' '.join(query_params)] = self.env.cmd('FT.PROFILE', 'empty', *query_params)


        change_oom_policy(self.env, 'return')
        # Change maxmemory to 1 to trigger OOM
        self.env.expect('CONFIG', 'SET', 'maxmemory', '1').ok()

        for query_params in query_params_to_check:
            res = self.env.cmd('FT.PROFILE', 'not_empty', *query_params)
            empty = empty_results[' '.join(query_params)]
            # Clear time related fields from results
            res = remove_keys_with_phrases_from_list(res, ['time', 'Warning','Iterators profile', 'Result processors profile'])
            empty = remove_keys_with_phrases_from_list(empty, ['time', 'Warning','Iterators profile', 'Result processors profile'])
            self.env.assertEqual(res, empty, message = 'Failed for query params: ' + ' '.join(query_params))

    def test_args_error_when_oom_resp2(self):
        # OOM should override args errors and return empty results
        change_oom_policy(self.env, 'return')
        # Change maxmemory to 1 to trigger OOM
        self.env.expect('CONFIG', 'SET', 'maxmemory', '1').ok()

        # Test FT.SEARCH with args error
        res = self.env.cmd('FT.SEARCH', 'idx', 'hello world', 'LIMIT', 0, 0, 'MEOW')
        self.env.assertEqual(res[0], 0)
        # Test FT.AGGREGATE with args error
        res = self.env.cmd('FT.AGGREGATE', 'idx', 'hello world', 'LIMIT', 0, 0, 'MEOW')
        self.env.assertEqual(res[0], 0)
        # Test FT.HYBRID with args error
        res = self.env.cmd('FT.HYBRID', 'idx_vec', 'SEARCH', 'hello world', 'VSIM', '@vector', '0', 'LIMIT', 0, 0, 'MEOW')
        self.env.assertEqual(res[1], 0)

class TestEarlyBailoutEmptyResultsSA_Resp3:
    def __init__(self):
        skipTest(cluster=True)
        self.env = Env(protocol=3)
        self.env.expect('FT.CREATE', 'empty', 'PREFIX', '1', 'NonExistingPrefix:', 'SCHEMA', 't', 'TEXT').ok()
        self.env.expect('FT.CREATE', 'empty_hybrid', 'PREFIX', '1', 'NonExistingPrefix:', 'SCHEMA',
            'description', 'TEXT',
            'embedding', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()
        self.env.expect('FT.CREATE', 'not_empty', 'SCHEMA', 't', 'TEXT').ok()
        self.env.expect('FT.CREATE', 'not_empty_hybrid', 'SCHEMA',
            'description', 'TEXT',
            'embedding', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()

        self.env.expect('HSET', 'doc', 't', 'hello').equal(1)

        # Make sure the empty index returns empty results and not_empty returns 1 result
        total_results = self.env.cmd('FT.SEARCH', 'empty', '*')['total_results']
        self.env.assertEqual(total_results, 0)
        total_results = self.env.cmd('FT.SEARCH', 'not_empty', '*')['total_results']
        self.env.assertEqual(total_results, 1)

    def setUp(self):
        pass
    def tearDown(self):
        set_unlimited_maxmemory_for_oom(self.env)

    def test_early_bailout_search_resp3(self):

        query_params_to_check = [
            ['*'],
            ['*', 'DIALECT', '2'],
            ['*', 'DIALECT', '3'],
            ['*', 'DIALECT', '4'],
            ['*', 'WITHOUTCOUNT', 'DIALECT', '4']
        ]

        empty_results = {}

        for query_params in query_params_to_check:
            empty_results[' '.join(query_params)] = self.env.cmd('FT.SEARCH', 'empty', *query_params)


        change_oom_policy(self.env, 'return')
        # Change maxmemory to 1 to trigger OOM
        self.env.expect('CONFIG', 'SET', 'maxmemory', '1').ok()

        for query_params in query_params_to_check:
            res = self.env.cmd('FT.SEARCH', 'not_empty', *query_params)
            empty = empty_results[' '.join(query_params)]
            # Assert res has OOM warning
            self.env.assertEqual(res['warning'][0], SHARD_OOM_WARNING)
            self.env.assertEqual(empty['warning'], [])
            # Clear warnings from res
            del res['warning']
            del empty['warning']
            # Assert dicts equal
            self.env.assertEqual(res, empty, message = 'Failed for query params: ' + ' '.join(query_params))

    def test_early_bailout_aggregate_resp3(self):

        query_params_to_check = [
            ['*'],
            ['*', 'DIALECT', '2'],
            ['*', 'DIALECT', '3'],
            ['*', 'DIALECT', '4'],
            ['*', 'WITHOUTCOUNT', 'DIALECT', '4'],
            ['*', 'WITHCURSOR'],
        ]

        empty_results = {}

        for query_params in query_params_to_check:
            empty_results[' '.join(query_params)] = self.env.cmd('FT.AGGREGATE', 'empty', *query_params)


        change_oom_policy(self.env, 'return')
        # Change maxmemory to 1 to trigger OOM
        self.env.expect('CONFIG', 'SET', 'maxmemory', '1').ok()


        for query_params in query_params_to_check:
            res = self.env.cmd('FT.AGGREGATE', 'not_empty', *query_params)
            empty = empty_results[' '.join(query_params)]
            if 'WITHCURSOR' in query_params:
                res = res[0]
                empty = empty[0]

            # Assert OOM warning exists
            self.env.assertEqual(res['warning'][0], SHARD_OOM_WARNING)
            self.env.assertEqual(empty['warning'], [])
            # Clear warnings from results
            del res['warning']
            del empty['warning']
            self.env.assertEqual(res, empty, message = 'Failed for query params: ' + ' '.join(query_params))


    def test_early_bailout_hybrid_resp3(self):

        query_params_to_check = [
            ['SEARCH', '*', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', '0'],
        ]

        empty_results = {}

        for query_params in query_params_to_check:
            empty_results[' '.join(query_params)] = self.env.cmd('FT.HYBRID', 'empty_hybrid', *query_params)

        change_oom_policy(self.env, 'return')
        # Change maxmemory to 1 to trigger OOM
        self.env.expect('CONFIG', 'SET', 'maxmemory', '1').ok()


        for query_params in query_params_to_check:
            res = self.env.cmd('FT.HYBRID', 'not_empty_hybrid', *query_params)
            empty = empty_results[' '.join(query_params)]
            # Assert OOM warning exists
            self.env.assertEqual(res['warnings'][0], SHARD_OOM_WARNING)
            self.env.assertEqual(empty['warnings'], [])
            # Clear warnings from results
            del res['warnings']
            del empty['warnings']
            # Clear execution time from results
            del res['execution_time']
            del empty['execution_time']
            self.env.assertEqual(res, empty, message = 'Failed for query params: ' + ' '.join(query_params))

    def test_early_bailout_profile_resp3(self):
        query_params_to_check = [
            ['SEARCH', 'QUERY', '*'],
            ['SEARCH', 'LIMITED', 'QUERY', '*'],
            ['AGGREGATE', 'QUERY', '*'],
            ['AGGREGATE', 'LIMITED', 'QUERY', '*'],
        ]
        empty_results = {}
        for query_params in query_params_to_check:
            empty_results[' '.join(query_params)] = self.env.cmd('FT.PROFILE', 'empty', *query_params)


        change_oom_policy(self.env, 'return')
        # Change maxmemory to 1 to trigger OOM
        self.env.expect('CONFIG', 'SET', 'maxmemory', '1').ok()

        for query_params in query_params_to_check:
            res = self.env.cmd('FT.PROFILE', 'not_empty', *query_params)
            empty = empty_results[' '.join(query_params)]
            # Assert res has OOM warning
            self.env.assertEqual(res['Results']['warning'][0], SHARD_OOM_WARNING)
            self.env.assertEqual(empty['Results']['warning'], [])

            # Clear time related fields from results
            res = remove_keys_with_phrases(res, ['time', 'Warning','Iterators profile', 'Result processors profile'])
            empty = remove_keys_with_phrases(empty, ['time', 'Warning','Iterators profile', 'Result processors profile'])

            # Assert dicts equal
            self.env.assertEqual(res, empty, message = 'Failed for query params: ' + ' '.join(query_params))

# Test early bailout and empty results for FT.SEARCH, FT.AGGREGATE, FT.HYBRID
# In Coordinator setting
class TestEarlyBailoutEmptyResultsCoord_Resp2:
    def __init__(self):
        skipTest(cluster=False)
        self.env = Env(shardsCount=3, protocol=2)
        self.env.expect('FT.CREATE', 'empty', 'PREFIX', '1', 'NonExistingPrefix:', 'SCHEMA', 't', 'TEXT').ok()
        self.env.expect('FT.CREATE', 'empty_hybrid', 'PREFIX', '1', 'NonExistingPrefix:', 'SCHEMA',
            'description', 'TEXT',
            'embedding', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()
        self.env.expect('FT.CREATE', 'not_empty', 'SCHEMA', 't', 'TEXT').ok()
        self.env.expect('FT.CREATE', 'not_empty_hybrid', 'SCHEMA',
            'description', 'TEXT',
            'embedding', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()

        self.env.expect('HSET', 'doc', 't', 'hello').equal(1)

        # Make sure the empty index returns empty results and not_empty returns 1 result
        res = self.env.cmd('FT.SEARCH', 'empty', '*')
        self.env.assertEqual(res[0], 0)
        res = self.env.cmd('FT.SEARCH', 'not_empty', '*')
        self.env.assertEqual(res[0], 1)

    def setUp(self):
        pass
    def tearDown(self):
        allShards_set_unlimited_maxmemory_for_oom(self.env)

    def test_early_bailout_search_resp2(self):

        query_params_to_check = [
            ['*'],
            ['*', 'DIALECT', '2'],
            ['*', 'DIALECT', '3'],
            ['*', 'DIALECT', '4'],
            ['*', 'WITHOUTCOUNT', 'DIALECT', '4']
        ]

        empty_results = {}

        for query_params in query_params_to_check:
            empty_results[' '.join(query_params)] = self.env.cmd('FT.SEARCH', 'empty', *query_params)


        allShards_change_oom_policy(self.env, 'return')
        # Change maxmemory on all shards to 1
        allShards_change_maxmemory_low(self.env)

        for query_params in query_params_to_check:
            res = self.env.cmd('FT.SEARCH', 'not_empty', *query_params)
            empty = empty_results[' '.join(query_params)]
            self.env.assertEqual(res, empty, message = 'Failed for query params: ' + ' '.join(query_params))

    def test_early_bailout_aggregate_resp2(self):

        query_params_to_check = [
            ['*'],
            ['*', 'DIALECT', '2'],
            ['*', 'DIALECT', '3'],
            ['*', 'DIALECT', '4'],
            ['*', 'WITHOUTCOUNT', 'DIALECT', '4'],
            ['*', 'WITHCURSOR'],
        ]

        empty_results = {}

        for query_params in query_params_to_check:
            empty_results[' '.join(query_params)] = self.env.cmd('FT.AGGREGATE', 'empty', *query_params)


        allShards_change_oom_policy(self.env, 'return')
        # Change maxmemory on all shards to 1
        allShards_change_maxmemory_low(self.env)

        for query_params in query_params_to_check:
            res = self.env.cmd('FT.AGGREGATE', 'not_empty', *query_params)
            empty = empty_results[' '.join(query_params)]
            if 'WITHCURSOR' in query_params:
                res = res[0]
                empty = empty[0]
            self.env.assertEqual(res, empty, message = 'Failed for query params: ' + ' '.join(query_params))


    def test_early_bailout_hybrid_resp2(self):

        query_params_to_check = [
            ['SEARCH', '*', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', '0'],
        ]

        empty_results = {}

        for query_params in query_params_to_check:
            empty_results[' '.join(query_params)] = self.env.cmd('FT.HYBRID', 'empty_hybrid', *query_params)

        allShards_change_oom_policy(self.env, 'return')
        # Change maxmemory on all shards to 1
        allShards_change_maxmemory_low(self.env)

        for query_params in query_params_to_check:
            res = self.env.cmd('FT.HYBRID', 'not_empty_hybrid', *query_params)
            empty = empty_results[' '.join(query_params)]
            # Assert OOM warning exists
            self.env.assertEqual(res[5][0], COORD_OOM_WARNING)
            self.env.assertEqual(empty[5], [])
            # Clear warnings from results
            del res[5]
            del empty[5]
            # Clear execution time from results
            del res[6]
            del empty[6]

            self.env.assertEqual(res, empty, message = 'Failed for query params: ' + ' '.join(query_params))

    def test_early_bailout_profile_resp2(self):
        query_params_to_check = [
            ['SEARCH', 'QUERY', '*'],
            ['SEARCH', 'LIMITED', 'QUERY', '*'],
            ['AGGREGATE', 'QUERY', '*'],
            ['AGGREGATE', 'LIMITED', 'QUERY', '*'],
        ]
        empty_results = {}
        for query_params in query_params_to_check:
            empty_results[' '.join(query_params)] = self.env.cmd('FT.PROFILE', 'empty', *query_params)


        allShards_change_oom_policy(self.env, 'return')
        # Change maxmemory on all shards to 1
        allShards_change_maxmemory_low(self.env)

        for query_params in query_params_to_check:
            res = self.env.cmd('FT.PROFILE', 'not_empty', *query_params)
            empty = empty_results[' '.join(query_params)]
            # Clear time related fields from results
            res = remove_keys_with_phrases_from_list(res, ['time', 'Warning','Iterators profile', 'Result processors profile'])
            empty = remove_keys_with_phrases_from_list(empty, ['time', 'Warning','Iterators profile', 'Result processors profile'])
            self.env.assertEqual(res, empty, message = 'Failed for query params: ' + ' '.join(query_params))

    def test_syntax_error_not_oom_resp2(self):
        # Test that args errors return empty results (not OOM) when policy is return
        allShards_change_oom_policy(self.env, 'return')
        # Change maxmemory on all shards to 1
        allShards_change_maxmemory_low(self.env)

        # Test FT.SEARCH with args error
        res = self.env.cmd('FT.SEARCH', 'idx', 'hello world', 'LIMIT', 0, 0, 'MEOW')
        self.env.assertEqual(res[0], 0)
        # Test FT.AGGREGATE with args error
        res = self.env.cmd('FT.AGGREGATE', 'idx', 'hello world', 'LIMIT', 0, 0, 'MEOW')
        self.env.assertEqual(res[0], 0)
        # Test FT.HYBRID with args error
        res = self.env.cmd('FT.HYBRID', 'idx_vec', 'SEARCH', 'hello world', 'VSIM', '@vector', '0', 'LIMIT', 0, 0, 'MEOW')
        self.env.assertEqual(res[1], 0)

# Test early bailout and empty results for FT.SEARCH, FT.AGGREGATE, FT.HYBRID
# In Coordinator setting
class TestEarlyBailoutEmptyResultsCoord_Resp3:
    def __init__(self):
        skipTest(cluster=False)
        self.env = Env(shardsCount=3, protocol=3)
        self.env.expect('FT.CREATE', 'empty', 'PREFIX', '1', 'NonExistingPrefix:', 'SCHEMA', 't', 'TEXT').ok()
        self.env.expect('FT.CREATE', 'empty_hybrid', 'PREFIX', '1', 'NonExistingPrefix:', 'SCHEMA',
            'description', 'TEXT',
            'embedding', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()
        self.env.expect('FT.CREATE', 'not_empty', 'SCHEMA', 't', 'TEXT').ok()
        self.env.expect('FT.CREATE', 'not_empty_hybrid', 'SCHEMA',
            'description', 'TEXT',
            'embedding', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()

        self.env.expect('HSET', 'doc', 't', 'hello').equal(1)

        # Make sure the empty index returns empty results and not_empty returns 1 result
        total_results = self.env.cmd('FT.SEARCH', 'empty', '*')['total_results']
        self.env.assertEqual(total_results, 0)
        total_results = self.env.cmd('FT.SEARCH', 'not_empty', '*')['total_results']
        self.env.assertEqual(total_results, 1)

    def setUp(self):
        pass
    def tearDown(self):
        allShards_set_unlimited_maxmemory_for_oom(self.env)

    def test_early_bailout_search_resp3(self):

        query_params_to_check = [
            ['*'],
            ['*', 'DIALECT', '2'],
            ['*', 'DIALECT', '3'],
            ['*', 'DIALECT', '4'],
            ['*', 'WITHOUTCOUNT', 'DIALECT', '4']
        ]

        empty_results = {}

        for query_params in query_params_to_check:
            empty_results[' '.join(query_params)] = self.env.cmd('FT.SEARCH', 'empty', *query_params)


        allShards_change_oom_policy(self.env, 'return')
        # Change maxmemory on all shards to 1
        allShards_change_maxmemory_low(self.env)

        for query_params in query_params_to_check:
            res = self.env.cmd('FT.SEARCH', 'not_empty', *query_params)
            empty = empty_results[' '.join(query_params)]
            # Assert res has OOM warning
            self.env.assertEqual(res['warning'], COORD_OOM_WARNING)
            self.env.assertEqual(empty['warning'], [])
            # Clear warnings from res
            del res['warning']
            del empty['warning']
            # Assert dicts equal
            self.env.assertEqual(res, empty, message = 'Failed for query params: ' + ' '.join(query_params))

    def test_early_bailout_aggregate_resp3(self):

        query_params_to_check = [
            ['*'],
            ['*', 'DIALECT', '2'],
            ['*', 'DIALECT', '3'],
            ['*', 'DIALECT', '4'],
            ['*', 'WITHOUTCOUNT', 'DIALECT', '4'],
            ['*', 'WITHCURSOR'],
        ]

        empty_results = {}

        for query_params in query_params_to_check:
            empty_results[' '.join(query_params)] = self.env.cmd('FT.AGGREGATE', 'empty', *query_params)


        allShards_change_oom_policy(self.env, 'return')
        # Change maxmemory on all shards to 1
        allShards_change_maxmemory_low(self.env)

        for query_params in query_params_to_check:
            res = self.env.cmd('FT.AGGREGATE', 'not_empty', *query_params)
            empty = empty_results[' '.join(query_params)]
            if 'WITHCURSOR' in query_params:
                res = res[0]
                empty = empty[0]

            # Assert OOM warning exists
            self.env.assertEqual(res['warning'][0], COORD_OOM_WARNING)
            self.env.assertEqual(empty['warning'], [])
            # Clear warnings from results
            del res['warning']
            del empty['warning']
            self.env.assertEqual(res, empty, message = 'Failed for query params: ' + ' '.join(query_params))


    def test_early_bailout_hybrid_resp3(self):

        query_params_to_check = [
            ['SEARCH', '*', 'VSIM', '@embedding', '$BLOB', 'PARAMS', '2', 'BLOB', '0'],
        ]

        empty_results = {}

        for query_params in query_params_to_check:
            empty_results[' '.join(query_params)] = self.env.cmd('FT.HYBRID', 'empty_hybrid', *query_params)

        allShards_change_oom_policy(self.env, 'return')
        # Change maxmemory on all shards to 1
        allShards_change_maxmemory_low(self.env)

        for query_params in query_params_to_check:
            res = self.env.cmd('FT.HYBRID', 'not_empty_hybrid', *query_params)
            empty = empty_results[' '.join(query_params)]
            # Assert OOM warning exists
            self.env.assertEqual(res['warnings'][0], COORD_OOM_WARNING)
            self.env.assertEqual(empty['warnings'], [])
            # Clear warnings from results
            del res['warnings']
            del empty['warnings']
            # Clear execution time from results
            del res['execution_time']
            del empty['execution_time']
            self.env.assertEqual(res, empty, message = 'Failed for query params: ' + ' '.join(query_params))

    def test_early_bailout_profile_resp3(self):
        query_params_to_check = [
            ['SEARCH', 'QUERY', '*'],
            ['SEARCH', 'LIMITED', 'QUERY', '*'],
            ['AGGREGATE', 'QUERY', '*'],
            ['AGGREGATE', 'LIMITED', 'QUERY', '*'],
        ]
        empty_results = {}
        for query_params in query_params_to_check:
            empty_results[' '.join(query_params)] = self.env.cmd('FT.PROFILE', 'empty', *query_params)


        allShards_change_oom_policy(self.env, 'return')
        # Change maxmemory on all shards to 1
        allShards_change_maxmemory_low(self.env)

        for query_params in query_params_to_check:
            res = self.env.cmd('FT.PROFILE', 'not_empty', *query_params)
            empty = empty_results[' '.join(query_params)]
            # Assert res has OOM warning
            res_warning = res['Results']['warning']
            if isinstance(res_warning, list) and res_warning:
                res_warning = res_warning[0]
            self.env.assertEqual(res_warning , COORD_OOM_WARNING)
            empty_warning = empty['Results']['warning']
            if isinstance(empty_warning, list) and empty_warning:
                empty_warning = empty_warning[0]
            self.env.assertEqual(empty_warning , [])

            # Clear time related fields from results
            res = remove_keys_with_phrases(res, ['time', 'Warning','Iterators profile', 'Result processors profile', 'Shards'])
            empty = remove_keys_with_phrases(empty, ['time', 'Warning','Iterators profile', 'Result processors profile', 'Shards'])

            # Assert dicts equal
            self.env.assertEqual(res, empty, message = 'Failed for query params: ' + ' '.join(query_params))
