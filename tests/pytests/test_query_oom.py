from common import *
import threading
import psutil

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

def pauseAfterNetworkRP_expect_oom(env, query_args, pause_after_n):
    env.expect(debug_cmd(), *query_args, 'PAUSE_BEFORE_RP_N', 'Network', pause_after_n, 'DEBUG_PARAMS_COUNT', 3).error().contains(OOM_QUERY_ERROR)

def run_cmd_expect_oom(env, query_args):
    env.expect(*query_args).error().contains(OOM_QUERY_ERROR)

def pid_cmd(conn):
    return conn.execute_command('info', 'server')['process_id']

def get_all_shards_pid(env):
    for shardId in range(1, env.shardsCount + 1):
        conn = env.getConnection(shardId)
        yield pid_cmd(conn)

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
    env.expect('FT.SEARCH', 'idx', '*').error().contains(OOM_QUERY_ERROR)
    # Verify aggregation query fails with sorting so it has to wait for all shards
    env.expect('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@name', 'SORTBY', 2, '@name', 'ASC').error().contains(OOM_QUERY_ERROR)

# Test OOM error returned from shards (only for fail), enforcing first reply from non-error shard
@skip(cluster=False)
def test_query_oom_cluster_shards_error_first_reply():
    env  = Env(shardsCount=3, moduleArgs='WORKERS 1')

    # Set OOM policy to fail on all shards
    allShards_change_oom_policy(env, 'fail')

    _ = _common_cluster_test_scenario(env)

    # Change maxmemory on all shards to 1
    allShards_change_maxmemory_low(env)
    # Change back coord maxmemory to 0
    set_unlimited_maxmemory_for_oom(env)

    coord_pid = pid_cmd(getConnectionByEnv(env))
    shards_pid = list(get_all_shards_pid(env))
    shards_pid.remove(coord_pid)

    # Pause all shards processes
    for pid in shards_pid:
        shard_p = psutil.Process(pid)
        shard_p.suspend()
        while shard_p.status() != psutil.STATUS_STOPPED:
            time.sleep(0.1)

    # Helper to call a function and push its return value into a list
    def _call_and_store(fn, args, out_list):
        out_list.append(fn(*args))

    # We need to call the queries in MT so the paused query won't block the test
    query_result = []

    # Build threads
    query_args = ['FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@t']

    t_query = threading.Thread(
        target=_call_and_store,
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
    for pid in shards_pid:
        shard_p = psutil.Process(pid)
        shard_p.resume()
        while shard_p.status() != psutil.STATUS_RUNNING:
            time.sleep(0.1)

    t_query.join()


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

    # Note - only the coordinator shard will return results

    # Verify partial results in search/aggregate
    res = env.cmd('FT.SEARCH', 'idx', '*')
    env.assertLess(res[0] , n_docs)
    res = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@name')
    env.assertLess(len(res), n_docs + 1)
