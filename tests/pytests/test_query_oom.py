from common import *
import threading
import psutil
import numpy as np
from redis.exceptions import ResponseError

OOM_QUERY_ERROR = "Not enough memory available to execute the query"
OOM_WARNING = "One or more shards failed to execute the query due to insufficient memory"

def run_cmd_expect_oom(env, query_args):
    env.expect(*query_args).error().contains(OOM_QUERY_ERROR)

def run_cmd(env, query_args):
    return (env.cmd(*query_args))

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

class testOomStandaloneBehavior:

    def __init__(self):
        skipTest(cluster=True)
        self.env = Env()
        _common_test_scenario(self.env)
        # Init all shards
        verify_shard_init(self.env.getConnection())

    def test_query_oom_ignore(self):
        change_oom_policy(self.env, 'ignore')
        res = self.env.cmd('FT.SEARCH', 'idx', '*')
        self.env.assertEqual(res, [1, 'doc', ['name', 'hello']])
        res = self.env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@name')
        self.env.assertEqual(res, [1, ['name', 'hello']])

    def test_query_oom_fail(self):
        change_oom_policy(self.env, 'fail')
        run_cmd_expect_oom(self.env, ['FT.SEARCH', 'idx', '*'])
        run_cmd_expect_oom(self.env, ['FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@name'])

    def test_query_oom_return(self):
        change_oom_policy(self.env, 'return')
        # Should return empty results
        res = self.env.cmd('FT.SEARCH', 'idx', '*')
        self.env.assertEqual(res, [0])
        res = self.env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@name')
        self.env.assertEqual(res, [0])

class testOomClusterBehavior:
    def __init__(self):
        skipTest(cluster=False)
        self.env = Env(shardsCount=3)
        self.n_docs = _common_cluster_test_scenario(self.env)
        allShards_change_maxmemory_low(self.env)
        # Init all shards
        for i in range(self.env.shardsCount):
            verify_shard_init(self.env.getConnection(i))


    def tearDown(self):
        self.env.expect('CONFIG', 'SET', 'maxmemory', '1').ok()

    def test_query_oom_ignore(self):
        allShards_change_oom_policy(self.env, 'ignore')
        res = self.env.cmd('FT.SEARCH', 'idx', '*')
        self.env.assertEqual(res[0] , self.n_docs)
        res = self.env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@name')
        self.env.assertEqual(len(res), self.n_docs + 1)

    def test_query_oom_coord_fail(self):
        # Test coord failing with OOM
        allShards_change_oom_policy(self.env, 'fail')
        # Test class invariant - coord maxmemory is 1
        run_cmd_expect_oom(self.env, ['FT.SEARCH', 'idx', '*'])
        run_cmd_expect_oom(self.env, ['FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@name'])

    def test_query_oom_shards_fail(self):
        # Test coord passing OOM but shards failing with OOM
        allShards_change_oom_policy(self.env, 'fail')
        # Change back coord maxmemory to 0 so it doesn't fail
        set_unlimited_maxmemory_for_oom(self.env)
        run_cmd_expect_oom(self.env, ['FT.SEARCH', 'idx', '*'])
        run_cmd_expect_oom(self.env, ['FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@name'])

    def test_query_oom_shards_return(self):
        # Test coord passing OOM, but shards returning empty results due to OOM
        allShards_change_oom_policy(self.env, 'return')
        # Change back coord maxmemory to 0
        set_unlimited_maxmemory_for_oom(self.env)
        # Note - only the coordinator shard will return results
        n_keys = len(self.env.cmd('KEYS', '*'))
        # Verify partial results in search/aggregate
        res = self.env.cmd('FT.SEARCH', 'idx', '*')
        self.env.assertEqual(res[0] , n_keys)
        res = self.env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@name')
        self.env.assertEqual(len(res), n_keys + 1)

# Test OOM error returned from shards (only for fail), enforcing first reply from non-error shard
# Test has specific environment requirements, so it's left out of the test class
@skip(cluster=False, asan=True)
def test_query_oom_cluster_shards_error_first_reply():
    # Workers is necessary to make sure the query is not finished before we resume the shards
    env  = Env(shardsCount=3, moduleArgs='WORKERS 1')

    # Init all shards
    for i in range(env.shardsCount):
        verify_shard_init(env.getConnection(i))

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

class testOomHybridStandaloneBehavior:

    def __init__(self):
        skipTest(cluster=True)
        self.env = Env()
        _common_hybrid_test_scenario(self.env)
        verify_shard_init(self.env.getConnection())

    def test_hybrid_oom_ignore(self):
        change_oom_policy(self.env, 'ignore')
        query_vector = np.array([1.2, 0.2]).astype(np.float32).tobytes()
        res = self.env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'shoes', 'VSIM', '@embedding', query_vector)
        # Should get results despite OOM condition
        self.env.assertEqual(res[1], 1)

    def test_hybrid_oom_fail(self):
        change_oom_policy(self.env, 'fail')
        query_vector = np.array([1.2, 0.2]).astype(np.float32).tobytes()
        run_cmd_expect_oom(self.env, ['FT.HYBRID', 'idx', 'SEARCH', 'shoes', 'VSIM', '@embedding', query_vector])

    def test_hybrid_oom_return(self):
        change_oom_policy(self.env, 'return')
        query_vector = np.array([1.2, 0.2]).astype(np.float32).tobytes()
        # Should return empty results
        res = self.env.cmd('FT.HYBRID', 'idx', 'SEARCH', 'shoes', 'VSIM', '@embedding', query_vector)
        self.env.assertEqual(res[1], 0)

class testOomHybridClusterBehavior:
    def __init__(self):
        skipTest(cluster=False)
        self.env = Env(shardsCount=3)
        self.n_docs = _common_hybrid_cluster_test_scenario(self.env)
        allShards_change_maxmemory_low(self.env)
        # Init all shards
        for i in range(self.env.shardsCount):
            verify_shard_init(self.env.getConnection(i))

    def tearDown(self):
        self.env.expect('CONFIG', 'SET', 'maxmemory', '1').ok()

    def test_hybrid_oom_ignore(self):
        allShards_change_oom_policy(self.env, 'ignore')
        query_vector = np.array([1.2, 0.2]).astype(np.float32).tobytes()
        res = self.env.cmd('FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM', '@embedding', query_vector, 'COMBINE', 'RRF', '2', 'WINDOW', '1000')
        self.env.assertEqual(res[1], self.n_docs)

    def test_hybrid_oom_coord_fail(self):
        # Test coord failing with OOM
        allShards_change_oom_policy(self.env, 'fail')
        query_vector = np.array([1.2, 0.2]).astype(np.float32).tobytes()
        # Test class invariant - coord maxmemory is 1
        run_cmd_expect_oom(self.env, ['FT.HYBRID', 'idx', 'SEARCH', 'shoes', 'VSIM', '@embedding', query_vector])

    def test_hybrid_oom_shards_fail(self):
        # Test coord passing OOM but shards failing with OOM
        allShards_change_oom_policy(self.env, 'fail')
        allShards_change_maxmemory_low(self.env)
        # Change back coord maxmemory to 0 so it doesn't fail
        set_unlimited_maxmemory_for_oom(self.env)
        query_vector = np.array([1.2, 0.2]).astype(np.float32).tobytes()
        run_cmd_expect_oom(self.env, ['FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM', '@embedding', query_vector, 'COMBINE', 'RRF', '2', 'WINDOW', '1000'])

    def test_hybrid_oom_shards_return(self):
        # Test coord passing OOM, but shards returning empty results due to OOM
        allShards_change_oom_policy(self.env, 'return')
        allShards_change_maxmemory_low(self.env)
        # Change back coord maxmemory to 0
        set_unlimited_maxmemory_for_oom(self.env)
        query_vector = np.array([1.2, 0.2]).astype(np.float32).tobytes()
        # Note - only the coordinator shard will return results
        n_keys = len(self.env.cmd('KEYS', '*'))
        # Verify partial results in hybrid search
        res = self.env.cmd('FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM', '@embedding', query_vector, 'COMBINE', 'RRF', '2', 'WINDOW', '1000')
        self.env.assertEqual(res[1] , n_keys)
        # Testing warnings verbosity
        self.env.assertEqual(res[5][0], OOM_WARNING)

@skip(cluster=False)
def test_oom_verbosity_cluster_return(env):
    env  = Env(shardsCount=3, protocol=3)

    # Init all shards
    for i in range(env.shardsCount):
        verify_shard_init(env.getConnection(i))

    # Set OOM policy to fail on all shards
    allShards_change_oom_policy(env, 'return')

    _ = _common_cluster_test_scenario(env)

    # Change maxmemory on all shards to 1
    allShards_change_maxmemory_low(env)
    # Change back coord maxmemory to 0
    set_unlimited_maxmemory_for_oom(env)

    # Note - only the coordinator shard will return results

    # RESP3

    # FT.SEARCH
    res = env.cmd('FT.SEARCH', 'idx', '*')
    env.assertEqual(res['warning'][0], OOM_WARNING)

    # TODO - Check warnings in FT.AGGREGATE when empty results are handled correctly

    # Search Profile
    res = env.cmd('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', '*')
    env.assertEqual(res['Results']['warning'][0], OOM_WARNING)
    shards_warning_lst = [shard_profile['Warning'] for shard_profile in res['Profile']['Shards']]
    # Since we don't know the order of responses, we need to count 2 errors
    env.assertEqual(shards_warning_lst.count(OOM_WARNING), 2)

    # Aggregate Profile
    res = env.cmd('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*')
    # TODO - Check 'Results' and 'Coordinator' warning when empty results are handled correctly
    shards_warning_lst = [shard_profile['Warning'] for shard_profile in res['Profile']['Shards']]
    # Since we don't know the order of responses, we need to count 2 errors
    env.assertEqual(shards_warning_lst.count(OOM_WARNING), 2)

    # RESP2
    env.cmd('HELLO', 2)

    # Aggregate Profile
    res = env.cmd('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*')
    # TODO - Check coordinator warning when empty results are handled correctly
    shards_warning_lst = [shard_res[9] for shard_res in res[1][1]]
    # Since we don't know the order of responses, we need to count 2 errors
    env.assertEqual(shards_warning_lst.count(OOM_WARNING), 2)

# Test OOM error returned from shards (only for return), enforcing first reply from non-error shard
# For FT.HYBRID - to make sure that the faulty shards don't affect the cursor mappings
# Test has specific environment requirements, so it's left out of the test class
@skip(cluster=False, asan=True)
def test_hybrid_query_oom_cluster_shards_error_first_reply():
    # Workers is necessary to make sure the query is not finished before we resume the shards
    env  = Env(shardsCount=3, moduleArgs='WORKERS 1')

    # Init all shards
    for i in range(env.shardsCount):
        verify_shard_init(env.getConnection(i))

    # Set OOM policy to return on all shards
    allShards_change_oom_policy(env, 'return')

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

    # Build threads
    query_vector = np.array([1.2, 0.2]).astype(np.float32).tobytes()
    query_args = ['FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM', '@embedding', query_vector, 'COMBINE', 'RRF', '2', 'WINDOW', '1000']

    t_query = threading.Thread(
        target=call_and_store,
        args=(run_cmd,
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

    # Note - only the coordinator shard will return results
    n_keys = len(env.cmd('KEYS', '*'))
    env.assertEqual(query_result[0][1] , n_keys)
    # Testing warnings verbosity
    env.assertEqual(query_result[0][5][0], OOM_WARNING)
