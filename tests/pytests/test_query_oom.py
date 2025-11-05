from common import *
import threading
import psutil
import numpy as np
from redis.exceptions import ResponseError

OOM_QUERY_ERROR = "Not enough memory available to execute the query"
OOM_WARNING = "One or more shards failed to execute the query due to insufficient memory"

def change_oom_policy(env, policy):
    env.expect(config_cmd(), 'SET', 'ON_OOM', policy).ok()

def shard_change_oom_policy(env, shardId, policy):
    res = env.getConnection(shardId).execute_command(config_cmd(), 'SET', 'ON_OOM', policy)
    env.assertEqual(res, 'OK')

def allShards_change_oom_policy(env, policy):
    for shardId in range(1, env.shardsCount + 1):
        shard_change_oom_policy(env, shardId, policy)

def allShards_change_maxmemory_low(env):
    for shardId in range(1, env.shardsCount + 1):
        res = env.getConnection(shardId).execute_command('config', 'set', 'maxmemory', 1)
        env.assertEqual(res, 'OK')

def run_cmd_expect_oom(env, query_args):
    env.expect(*query_args).error().contains(OOM_QUERY_ERROR)

def pid_cmd(conn):
    return conn.execute_command('info', 'server')['process_id']

def get_all_shards_pid(env):
    for shardId in range(1, env.shardsCount + 1):
        conn = env.getConnection(shardId)
        yield pid_cmd(conn)

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


def _common_test_scenario(env):
    # Create an index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
    # Add a document
    env.expect('HSET', 'doc', 'name', 'hello').equal(1)
    # Change maxmemory to 1 to trigger OOM
    env.expect('CONFIG', 'SET', 'maxmemory', '1').ok()

def _common_cluster_test_scenario(env):
    conn = getConnectionByEnv(env)
    # Create an index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT', 'SORTABLE').ok()

    n_docs_per_shard = 100
    n_docs = n_docs_per_shard * env.shardsCount
    for i in range(n_docs):
        conn.execute_command('HSET', f'doc{i}', 'name', f'hello{i}')

    # Change maxmemory to 1 to trigger OOM
    env.expect('CONFIG', 'SET', 'maxmemory', '1').ok()

    return n_docs

# Test ignore policy
@skip(cluster=True)
def test_query_oom_ignore(env):

    _common_test_scenario(env)

    change_oom_policy(env, 'ignore')

    res = env.cmd('FT.SEARCH', 'idx', '*')
    env.assertEqual(res, [1, 'doc', ['name', 'hello']])
    res = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@name')
    env.assertEqual(res[0], 1)

# Test ignore policy in cluster
@skip(cluster=False)
def test_query_oom_cluster_ignore(env):

    n_docs = _common_cluster_test_scenario(env)

    allShards_change_oom_policy(env, 'ignore')

    res = env.cmd('FT.SEARCH', 'idx', '*')
    env.assertEqual(res[0] , n_docs)
    res = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@name')
    env.assertEqual(len(res), n_docs + 1)

@skip(cluster=True)
def test_query_oom_fail_standalone(env):

    change_oom_policy(env, 'fail')

    _common_test_scenario(env)

    # Verify query fails
    env.expect('FT.SEARCH', 'idx', '*').error().contains(OOM_QUERY_ERROR)
    # Verify aggregation query fails
    env.expect('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@name').error().contains(OOM_QUERY_ERROR)

# Test OOM error returned from shards (only for fail)
@skip(cluster=False)
def test_query_oom_cluster_shards_error():
    env  = Env(shardsCount=3)

    # Set OOM policy to fail on all shards
    allShards_change_oom_policy(env, 'fail')

    _ = _common_cluster_test_scenario(env)

    # Change maxmemory on all shards to 1
    allShards_change_maxmemory_low(env)
    # Change back coord maxmemory to 0
    set_unlimited_maxmemory_for_oom(env)

    # Verify query fails
    env.expect('FT.SEARCH', 'idx', '*').error().contains(OOM_QUERY_ERROR)
    # Verify aggregation query fails with sorting so it has to wait for all shards
    env.expect('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@name', 'SORTBY', 2, '@name', 'ASC').error().contains(OOM_QUERY_ERROR)

# Test OOM error returned from shards (only for fail), enforcing first reply from non-error shard
@skip(cluster=False, asan=True)
def test_query_oom_cluster_shards_error_first_reply():
    # Workers is necessary to make sure the query is not finished before we resume the shards
    env  = Env(shardsCount=3, moduleArgs='WORKERS 1')

    # Set OOM policy to fail on all shards
    allShards_change_oom_policy(env, 'fail')

    _ = _common_cluster_test_scenario(env)

    # Change maxmemory on all shards to 1
    allShards_change_maxmemory_low(env)
    # Change back coord maxmemory to 0
    set_unlimited_maxmemory_for_oom(env)

    coord_pid = pid_cmd(env.con)
    shards_pid = list(get_all_shards_pid(env))
    shards_pid.remove(coord_pid)

    # Pause all shards processes
    shards_p = [psutil.Process(pid) for pid in shards_pid]
    for shard_p in shards_p:
        shard_p.suspend()
    with TimeLimit(60, 'Timeout while waiting for shards to pause'):
        while any(shard_p.status() != psutil.STATUS_STOPPED for shard_p in shards_p):
            time.sleep(0.1)

    # We need to call the queries in MT so the paused query won't block the test
    query_result = []

    # Build threads
    query_args = ['FT.AGGREGATE', 'idx', '*']

    t_query = threading.Thread(
        target=call_and_store,
        args=(run_cmd_expect_oom,
            (env, query_args),
            query_result),
        daemon=True
    )

    # Start the query and the pause-check in parallel
    t_query.start()

    with TimeLimit(60, 'Timeout while waiting for worker to be created'):
        while getWorkersThpoolStats(env)['numThreadsAlive'] == 0:
            time.sleep(0.1)

    env.expect(debug_cmd(), 'WORKERS', 'drain').ok()
    stats = getWorkersThpoolStats(env)
    env.assertEqual(stats['totalJobsDone'], 1)
    # If here, we know the coordinator got the first reply.

    # Let's resume the shards
    for shard_p in shards_p:
        shard_p.resume()
    # consider any non-stopped state as “resumed”
    with TimeLimit(60, 'Timeout while waiting for shards to resume'):
        while any(shard_p.status() == psutil.STATUS_STOPPED for shard_p in shards_p):
            time.sleep(0.1)
    # Wait for the query to finish
    t_query.join()

# Test OOM error on coordinator, 'fail' policy
@skip(cluster=False)
def test_query_oom_cluster_coord_error():
    env  = Env(shardsCount=3)

    _ = _common_cluster_test_scenario(env)
    # Note: the coord's maxmemory is changed in the function above

    allShards_change_oom_policy(env, 'fail')
    # Verify query fails
    env.expect('FT.SEARCH', 'idx', '*').error().contains(OOM_QUERY_ERROR)
    # Verify aggregation query fails
    env.expect('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@name').error().contains(OOM_QUERY_ERROR)

# Test OOM error returned from shards (only for return)
@skip(cluster=False)
def test_query_oom_cluster_shards_return():
    env  = Env(shardsCount=3)

    # Set OOM policy to return on all shards
    allShards_change_oom_policy(env, 'return')

    _ = _common_cluster_test_scenario(env)

    # Change maxmemory on all shards to 1
    allShards_change_maxmemory_low(env)
    # Change back coord maxmemory to 0
    set_unlimited_maxmemory_for_oom(env)

    # Note - only the coordinator shard will return results

    # Get num keys in coordinator shard
    n_keys = len(env.cmd('KEYS', '*'))

    # Verify partial results in search/aggregate
    res = env.cmd('FT.SEARCH', 'idx', '*')
    env.assertEqual(res[0] , n_keys)
    res = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@name')
    env.assertEqual(len(res), n_keys + 1)


def _common_hybrid_test_scenario(env):
    """Common setup for hybrid OOM tests"""
    # Create an index with text and vector fields
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               'description', 'TEXT',
               'embedding', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()

    # Add test document
    env.expect('HSET', 'doc:1', 'description', 'red shoes', 'embedding', np.array([0.0, 0.0]).astype(np.float32).tobytes()).equal(2)
    # Change maxmemory to 1 to trigger OOM
    env.expect('CONFIG', 'SET', 'maxmemory', '1').ok()

def _common_hybrid_cluster_test_scenario(env):
    """Common setup for hybrid OOM tests in cluster"""
    conn = getConnectionByEnv(env)
    # Create an index
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
            'description', 'TEXT',
            'embedding', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()


    n_docs_per_shard = 100
    n_docs = n_docs_per_shard * env.shardsCount
    for i in range(n_docs):
        conn.execute_command('HSET', f'doc{i}', 'description', f'hello{i}', 'embedding', np.array([np.random.rand(), np.random.rand()]).astype(np.float32).tobytes())

    # Change maxmemory to 1 to trigger OOM
    env.expect('CONFIG', 'SET', 'maxmemory', '1').ok()

    return n_docs

# Test ignore policy for FT.HYBRID
@skip(cluster=True)
def test_hybrid_oom_ignore(env):
    _common_hybrid_test_scenario(env)

    change_oom_policy(env, 'ignore')

    query_vector = np.array([1.2, 0.2]).astype(np.float32).tobytes()
    res = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'shoes', 'VSIM', '@embedding', query_vector)

    # Should get results despite OOM condition
    env.assertEqual(res[1], 1)

# Test fail policy for FT.HYBRID in standalone
@skip(cluster=True)
def test_hybrid_oom_fail_standalone(env):
    change_oom_policy(env, 'fail')

    _common_hybrid_test_scenario(env)

    query_vector = np.array([1.2, 0.2]).astype(np.float32).tobytes()

    # Verify hybrid query fails
    env.expect('FT.HYBRID', 'idx', 'SEARCH', 'shoes', 'VSIM', '@embedding', query_vector).error().contains(OOM_QUERY_ERROR)

# Test OOM error on coordinator for FT.HYBRID, 'fail' policy
@skip(cluster=False)
def test_hybrid_oom_cluster_coord_error():
    env = Env(shardsCount=3)

    _ = _common_hybrid_cluster_test_scenario(env)
    # Note: the coord's maxmemory is changed in the function above

    # Test with 'fail' policy on all shards
    allShards_change_oom_policy(env, 'fail')

    query_vector = np.array([1.2, 0.2]).astype(np.float32).tobytes()

    # Verify hybrid query fails
    env.expect('FT.HYBRID', 'idx', 'SEARCH', 'shoes', 'VSIM', '@embedding', query_vector).error().contains(OOM_QUERY_ERROR)

# Test OOM error returned from shards for FT.HYBRID (only for fail)
@skip(cluster=False)
def test_hybrid_oom_cluster_shards_error():
    env = Env(shardsCount=3)

    # Set OOM policy to fail on all shards
    allShards_change_oom_policy(env, 'fail')

    _ = _common_hybrid_cluster_test_scenario(env)

    # Change maxmemory on all shards to 1
    allShards_change_maxmemory_low(env)
    # Change back coord maxmemory to 0
    set_unlimited_maxmemory_for_oom(env)

    query_vector = np.array([1.2, 0.2]).astype(np.float32).tobytes()

    # Verify hybrid query fails
    env.expect('FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM', '@embedding', query_vector, 'COMBINE', 'RRF', '2', 'WINDOW', '1000').error().contains(OOM_QUERY_ERROR)

# Test OOM error returned from shards for FT.HYBRID (only for return)
# @skip(cluster=False)
def test_hybrid_oom_cluster_shards_return():
    env = Env(shardsCount=3, enableDebugCommand=True, protocol=3)

    # Set OOM policy to return on all shards
    allShards_change_oom_policy(env, 'return')

    _common_hybrid_cluster_test_scenario(env)

    # Change maxmemory on all shards to 1
    allShards_change_maxmemory_low(env)
    # Change back coord maxmemory to 0
    set_unlimited_maxmemory_for_oom(env)

    query_vector = np.array([1.2, 0.2]).astype(np.float32).tobytes()

    # Note - only the coordinator shard will return results

    # Get num keys in coordinator shard
    n_keys = len(env.cmd('KEYS', '*'))

    # Verify partial results in hybrid search
    res = env.cmd('FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM', '@embedding', query_vector, 'COMBINE', 'RRF', '2', 'WINDOW', '1000')
    env.assertEqual(res[1] , n_keys)

    # Testing warnings verbosity
    # Resp3
    env.assertEqual(res['warnings'][0], OOM_WARNING)

    # Resp2
    env.cmd('HELLO', 2)
    res2 = env.cmd('FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM', '@embedding', query_vector, 'COMBINE', 'RRF', '2', 'WINDOW', '1000')
    env.assertEqual(res2[5][0], OOM_WARNING)

# Test early bailout and empty results for FT.SEARCH, FT.AGGREGATE, FT.HYBRID
# In SA setting
# This test can be in a file of its own, since it's not related to OOM
# But currently, only OOM `return` policy initiates early bailout
class TestEarlyBailoutEmptyResultsSA_Resp2:
    def __init__(self):
        skipTest(cluster=True)
        self.env = Env(protocol=2)
        self.env.expect('HSET', 'doc', 't', 'hello').equal(1)
        self.env.expect('FT.CREATE', 'empty', 'PREFIX', '1', 'NonExistingPrefix:', 'SCHEMA', 't', 'TEXT').ok()
        self.env.expect('FT.CREATE', 'empty_hybrid', 'PREFIX', '1', 'NonExistingPrefix:', 'SCHEMA',
            'description', 'TEXT',
            'embedding', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()
        self.env.expect('FT.CREATE', 'not_empty', 'SCHEMA', 't', 'TEXT').ok()
        self.env.expect('FT.CREATE', 'not_empty_hybrid', 'SCHEMA',
            'description', 'TEXT',
            'embedding', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()

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
            self.env.assertEqual(res[5][0], OOM_WARNING)
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

# Test early bailout and empty results for FT.SEARCH, FT.AGGREGATE, FT.HYBRID
# In SA setting
# This test can be in a file of its own, since it's not related to OOM
# But currently, only OOM `return` policy initiates early bailout
class TestEarlyBailoutEmptyResultsSA_Resp3:
    def __init__(self):
        skipTest(cluster=True)
        self.env = Env(protocol=3)
        self.env.expect('HSET', 'doc', 't', 'hello').equal(1)
        self.env.expect('FT.CREATE', 'empty', 'PREFIX', '1', 'NonExistingPrefix:', 'SCHEMA', 't', 'TEXT').ok()
        self.env.expect('FT.CREATE', 'empty_hybrid', 'PREFIX', '1', 'NonExistingPrefix:', 'SCHEMA',
            'description', 'TEXT',
            'embedding', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()
        self.env.expect('FT.CREATE', 'not_empty', 'SCHEMA', 't', 'TEXT').ok()
        self.env.expect('FT.CREATE', 'not_empty_hybrid', 'SCHEMA',
            'description', 'TEXT',
            'embedding', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()

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
            self.env.assertEqual(res['warning'][0], OOM_WARNING)
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
            self.env.assertEqual(res['warning'][0], OOM_WARNING)
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
            self.env.assertEqual(res['warnings'][0], OOM_WARNING)
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
            self.env.assertEqual(res['Results']['warning'][0], OOM_WARNING)
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
        self.env.expect('HSET', 'doc', 't', 'hello').equal(1)
        self.env.expect('FT.CREATE', 'empty', 'PREFIX', '1', 'NonExistingPrefix:', 'SCHEMA', 't', 'TEXT').ok()
        self.env.expect('FT.CREATE', 'empty_hybrid', 'PREFIX', '1', 'NonExistingPrefix:', 'SCHEMA',
            'description', 'TEXT',
            'embedding', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()
        self.env.expect('FT.CREATE', 'not_empty', 'SCHEMA', 't', 'TEXT').ok()
        self.env.expect('FT.CREATE', 'not_empty_hybrid', 'SCHEMA',
            'description', 'TEXT',
            'embedding', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()

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
            self.env.assertEqual(res[5][0], OOM_WARNING)
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

# Test early bailout and empty results for FT.SEARCH, FT.AGGREGATE, FT.HYBRID
# In Coordinator setting
class TestEarlyBailoutEmptyResultsCoord_Resp3:
    def __init__(self):
        skipTest(cluster=False)
        self.env = Env(shardsCount=3, protocol=3)
        self.env.expect('HSET', 'doc', 't', 'hello').equal(1)
        self.env.expect('FT.CREATE', 'empty', 'PREFIX', '1', 'NonExistingPrefix:', 'SCHEMA', 't', 'TEXT').ok()
        self.env.expect('FT.CREATE', 'empty_hybrid', 'PREFIX', '1', 'NonExistingPrefix:', 'SCHEMA',
            'description', 'TEXT',
            'embedding', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()
        self.env.expect('FT.CREATE', 'not_empty', 'SCHEMA', 't', 'TEXT').ok()
        self.env.expect('FT.CREATE', 'not_empty_hybrid', 'SCHEMA',
            'description', 'TEXT',
            'embedding', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2', 'DISTANCE_METRIC', 'L2').ok()

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
            self.env.assertEqual(res['warning'], OOM_WARNING)
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
            self.env.assertEqual(res['warning'][0], OOM_WARNING)
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
            self.env.assertEqual(res['warnings'][0], OOM_WARNING)
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
            self.env.assertEqual(res_warning , OOM_WARNING)
            empty_warning = empty['Results']['warning']
            if isinstance(empty_warning, list) and empty_warning:
                empty_warning = empty_warning[0]
            self.env.assertEqual(empty_warning , [])

            # Clear time related fields from results
            res = remove_keys_with_phrases(res, ['time', 'Warning','Iterators profile', 'Result processors profile', 'Shards'])
            empty = remove_keys_with_phrases(empty, ['time', 'Warning','Iterators profile', 'Result processors profile', 'Shards'])

            # Assert dicts equal
            self.env.assertEqual(res, empty, message = 'Failed for query params: ' + ' '.join(query_params))
