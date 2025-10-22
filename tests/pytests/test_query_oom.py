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


def _common_test_scenario(env):
    # Create an index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
    # Add a document
    env.expect('HSET', 'doc', 'name', 'hello').equal(1)
    # Change maxmemory to 1
    env.expect('CONFIG', 'SET', 'maxmemory', '1').ok()

def _common_cluster_test_scenario(env):
    conn = getConnectionByEnv(env)
    # Create an index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT', 'SORTABLE').ok()

    n_docs_per_shard = 100
    n_docs = n_docs_per_shard * env.shardsCount
    for i in range(n_docs):
        conn.execute_command('HSET', f'doc{i}', 'name', f'hello{i}')

    # Change maxmemory to 1
    env.expect('CONFIG', 'SET', 'maxmemory', '1').ok()

    return n_docs

# Test ignore policy
@skip(cluster=True)
def test_query_oom_ignore(env):

    _common_test_scenario(env)

    # The test should ignore OOM since 'ignore' is the default config
    # TODO : change/ remove test if default config is changed
    res = env.cmd('FT.SEARCH', 'idx', '*')
    env.assertEqual(res, [1, 'doc', ['name', 'hello']])
    res = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@name')
    env.assertEqual(res[0], 1)

# Test ignore policy in cluster
@skip(cluster=False)
def test_query_oom_cluster_ignore(env):

    n_docs = _common_cluster_test_scenario(env)

    # The test should ignore OOM since 'ignore' is the default config
    # TODO : change/remove test if default config is changed

    res = env.cmd('FT.SEARCH', 'idx', '*')
    env.assertEqual(res[0] , n_docs)
    res = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@name')
    env.assertEqual(len(res), n_docs + 1)

@skip(cluster=True)
def test_query_oom_standalone(env):
    # Change oom policy to fail
    # TODO : Change if default value is changed
    change_oom_policy(env, 'fail')

    _common_test_scenario(env)

    # Verify query fails
    env.expect('FT.SEARCH', 'idx', '*').error().contains(OOM_QUERY_ERROR)
    # Verify aggregation query fails
    env.expect('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@name').error().contains(OOM_QUERY_ERROR)

    # Since we are in a standalone env, the test should fail also if the config is 'return'
    change_oom_policy(env, 'return')
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

# Test OOM error on coordinator (fail or return)
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

    # Set OOM policy to fail on all shards
    allShards_change_oom_policy(env, 'return')
    # Verify query fails
    env.expect('FT.SEARCH', 'idx', '*').error().contains(OOM_QUERY_ERROR)
    # Verify aggregation query fails
    env.expect('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@name').error().contains(OOM_QUERY_ERROR)

# Test OOM error returned from shards (only for return)
@skip(cluster=False)
def test_query_oom_cluster_shards_return():
    env  = Env(shardsCount=3)

    # Set OOM policy to fail on all shards
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

    # Change maxmemory to 1
    env.expect('CONFIG', 'SET', 'maxmemory', '1').ok()

    return n_docs

# Test ignore policy for FT.HYBRID
@skip(cluster=True)
def test_hybrid_oom_ignore(env):
    _common_hybrid_test_scenario(env)

    # The test should ignore OOM since 'ignore' is the default config
    # TODO : change/ remove test if default config is changed

    query_vector = np.array([1.2, 0.2]).astype(np.float32).tobytes()
    res = env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'shoes', 'VSIM', '@embedding', query_vector)

    # Should get results despite OOM condition
    env.assertEqual(res[1], 1)

# Test fail policy for FT.HYBRID in standalone
@skip(cluster=True)
def test_hybrid_oom_standalone(env):
    # Change oom policy to fail
    # TODO : Change if default value is changed
    change_oom_policy(env, 'fail')

    _common_hybrid_test_scenario(env)

    query_vector = np.array([1.2, 0.2]).astype(np.float32).tobytes()

    # Verify hybrid query fails
    env.expect('FT.HYBRID', 'idx', 'SEARCH', 'shoes', 'VSIM', '@embedding', query_vector).error().contains(OOM_QUERY_ERROR)

    # Since we are in a standalone env, the test should fail also if the config is 'return'
    change_oom_policy(env, 'return')
    # Verify hybrid query fails
    env.expect('FT.HYBRID', 'idx', 'SEARCH', 'shoes', 'VSIM', '@embedding', query_vector).error().contains(OOM_QUERY_ERROR)

# Test OOM error on coordinator for FT.HYBRID (fail or return)
# under each policy, test the query fails on the coordinator at the beginning of the query
# therefore we expect the query to fail with OOM_QUERY_ERROR both fail and return policies
def test_hybrid_oom_cluster_coord_error():
    env = Env(shardsCount=3)

    _ = _common_hybrid_cluster_test_scenario(env)
    # Note: the coord's maxmemory is changed in the function above

    # Test with 'fail' policy on all shards
    allShards_change_oom_policy(env, 'fail')

    query_vector = np.array([1.2, 0.2]).astype(np.float32).tobytes()

    # Verify hybrid query fails
    env.expect('FT.HYBRID', 'idx', 'SEARCH', 'shoes', 'VSIM', '@embedding', query_vector).error().contains(OOM_QUERY_ERROR)

    # Test with 'return' policy on all shards
    allShards_change_oom_policy(env, 'return')
    # Verify hybrid query fails
    env.expect('FT.HYBRID', 'idx', 'SEARCH', 'shoes', 'VSIM', '@embedding', query_vector).error().contains(OOM_QUERY_ERROR)

# Test OOM error returned from shards for FT.HYBRID (only for fail)
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
def test_hybrid_oom_cluster_shards_return():
    env = Env(shardsCount=3, enableDebugCommand=True)

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

# Test OOM error returned from shards for FT.HYBRID (enforcing first reply from non-error shard)
def test_hybrid_oom_cluster_shards_error_first_reply():
    # Workers is necessary to make sure the query is not finished before we resume the shards
    env = Env(shardsCount=3, moduleArgs='WORKERS 1', enableDebugCommand=True)

    # enable unstable features so we have the special loader
    enable_unstable_features(env)

    # Set OOM policy to fail on all shards
    allShards_change_oom_policy(env, 'fail')

    _ = _common_hybrid_cluster_test_scenario(env)

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

    query_vector = np.array([1.2, 0.2]).astype(np.float32).tobytes()

    # LOAD @__key to skip the safe loader
    query_args = ['FT.HYBRID', 'idx', 'SEARCH', 'hello', 'VSIM', '@embedding', query_vector, 'LOAD', 1, '@__key']

    # Build threads
    t_query = threading.Thread(
        target=call_and_store,
        args=(run_cmd_expect_oom,
            (env, query_args),
            query_result),
        daemon=True
    )

    # Start the query and the pause-check in parallel
    t_query.start()

    env.expect(debug_cmd(), 'WORKERS', 'drain').ok()
    stats = getWorkersThpoolStats(env)
    env.assertEqual(stats['totalJobsDone'], 1)
    # If here, we know the coordinator got the first reply.

    # Let's resume the shards
    for shard_p in shards_p:
        shard_p.resume()
    # consider any non-stopped state as "resumed"
    with TimeLimit(10, 'Timeout while waiting for shards to resume'):
        while any(shard_p.status() == psutil.STATUS_STOPPED for shard_p in shards_p):
            time.sleep(0.1)
    # Wait for the query to finish
    t_query.join()

# Test verbosity when partial results are returned for PROFILE and RESP3
@skip(cluster=False)
def test_oom_verbosity_cluster_return(env):
    env  = Env(shardsCount=3, protocol=3)

    # Set OOM policy to fail on all shards
    allShards_change_oom_policy(env, 'return')

    _ = _common_cluster_test_scenario(env)

    # Change maxmemory on all shards to 1
    allShards_change_maxmemory_low(env)
    # Change back coord maxmemory to 0
    set_unlimited_maxmemory_for_oom(env)

    # Note - only the coordinator shard will return results

    # RESP3

    # Search Profile
    res = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '*')
    env.assertEqual(res['Results']['warning'], OOM_WARNING)
    shards_error_lst = [str(shard_err) for shard_err in res['Profile']['Shards'] if isinstance(shard_err, ResponseError)]
    # Since we don't know the order of responses, we need to count 2 errors
    env.assertEqual(shards_error_lst.count(OOM_QUERY_ERROR), 2)

    # Aggregate Profile
    res = env.cmd('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*')
    env.assertEqual(res['Results']['warning'][0], OOM_WARNING)
    env.assertEqual(res['Profile']['Coordinator']['Warning'], OOM_WARNING)
    shards_error_lst = [str(shard_err) for shard_err in res['Profile']['Shards'] if isinstance(shard_err, ResponseError)]
    # Since we don't know the order of responses, we need to count 2 errors
    env.assertEqual(shards_error_lst.count(OOM_QUERY_ERROR), 2)

    # FT.SEARCH
    res = env.cmd('FT.SEARCH', 'idx', '*')
    env.assertEqual(res['warning'], OOM_WARNING)

    # FT.AGGREGATE
    res = env.cmd('FT.AGGREGATE', 'idx', '*')
    env.assertEqual(res['warning'][0], OOM_WARNING)

    # RESP2
    env.cmd('HELLO', 2)

    # Search Profile
    res = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '*')
    shards_lst = [str(shard) for shard in res[1][1]]
    # Since we don't know the order of responses, we need to count 2 errors
    env.assertEqual(shards_lst.count(OOM_QUERY_ERROR), 2)

    # Aggregate Profile
    res = env.cmd('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*')
    coord_res = res[1][3]
    warning_idx = coord_res.index('Warning') + 1
    env.assertEqual(coord_res[warning_idx], OOM_WARNING)
    shards_error_lst = [str(shard_err) for shard_err in res[1][1] if isinstance(shard_err, ResponseError)]
    # Since we don't know the order of responses, we need to count 2 errors
    env.assertEqual(shards_error_lst.count(OOM_QUERY_ERROR), 2)
