from common import *
import threading

OOM_QUERY_ERROR = "Not enough memory available to execute the query"

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

# Helper to call a function and push its return value into a list
def _call_and_store(fn, args, out_list):
    out_list.append(fn(*args))

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

def runDebugQueryCommand_expect_oom(env, query_cmd, debug_params):
    # Use the helper function to build the argument list
    args = parseDebugQueryCommandArgs(query_cmd, debug_params)
    return env.expect(debug_cmd(), *args).error().contains(OOM_QUERY_ERROR)

def runDebugQueryCommandPauseBeforeRPAfterN_expect_oom(env, query_cmd, rp_type, pause_after_n, extra_args=None):
    debug_params = ['PAUSE_BEFORE_RP_N', rp_type, pause_after_n]
    if extra_args:
        debug_params.extend(extra_args)
    return runDebugQueryCommand_expect_oom(env, query_cmd, debug_params)

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

    for config_return in [False, True]:
        # Since we are in a standalone env, the test should fail also if the config is 'return'
        if config_return:
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
    env.expect('FT.SEARCH', 'idx', '*', 'SORTBY', 'name', 'ASC').error().contains(OOM_QUERY_ERROR)
    # Verify aggregation query fails with sorting so it has to wait for all shards
    env.expect('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@name', 'SORTBY', 2, '@name', 'ASC').error().contains(OOM_QUERY_ERROR)

# Test OOM error returned from shards (only for fail), enforcing first reply from non-error shard
# @skip(cluster=False)
def test_query_oom_cluster_shards_error_first_reply():
    env  = Env(shardsCount=3, moduleArgs='WORKERS 1')

    # Set OOM policy to fail on all shards
    allShards_change_oom_policy(env, 'return')

    _ = _common_cluster_test_scenario(env)

    # Change maxmemory on all shards to 1
    allShards_change_maxmemory_low(env)
    # Change back coord maxmemory to 0
    set_unlimited_maxmemory_for_oom(env)

    ##### Use debug command to enforce first reply from non-error shard #####

    # We need to call the queries in MT so the paused query won't block the test
    query_result = []
    # Build threads
    query_args = ['FT.AGGREGATE', 'idx', '*', 'LIMIT', 0, 120]
    t_query = threading.Thread(
        target=_call_and_store,
        args=(runDebugQueryCommandPauseBeforeRPAfterN_expect_oom,
            (env, query_args, 'Index', 0, ['INTERNAL_ONLY']),
            query_result),
        daemon=True
    )
    # Start the query and the pause-check in parallel
    t_query.start()
    # Wait for the coordinator to be paused
    while False in allShards_getIsRPPaused(env):
        time.sleep(0.1)

    # Resume the coordinator
    setPauseRPResume(env)
    t_query.join()
    allShards_setPauseRPResume(env, 2)
    # Now, the result from the coordinator is the first to arrive, which is not an error
    # Resume the rest of the shards
    print(query_result)

# Test OOM error returned from shards (only for fail) with Sorting
@skip(cluster=False)
def test_query_oom_cluster_shards_sorting_error():
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
    # Verify aggregation query fails
    env.expect('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@name').error().contains(OOM_QUERY_ERROR)

# Test OOM error on coordinator (fail or return)
@skip(cluster=False)
def test_query_oom_cluster_coord_error():
    env  = Env(shardsCount=3)

    _ = _common_cluster_test_scenario(env)
    # Note: the coord's maxmemory is changed in the function above

    for policy in ['fail', 'return']:
        # Set OOM policy to fail on all shards
        allShards_change_oom_policy(env, policy)

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

    n_docs = _common_cluster_test_scenario(env)

    # Change maxmemory on all shards to 1
    allShards_change_maxmemory_low(env)
    # Change back coord maxmemory to 0
    set_unlimited_maxmemory_for_oom(env)

    # Verify partial results in search/aggregate
    res = env.cmd('FT.SEARCH', 'idx', '@name:*hello*')
    env.assertLess(res[0] , n_docs)
    res = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@name')
    env.assertLess(len(res), n_docs + 1)
