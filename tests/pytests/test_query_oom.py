from common import *

OOM_QUERY_ERROR = "Not enough memory available to execute the query"

def change_oom_policy(env, policy):
    env.expect(config_cmd(), 'SET', 'ON_OOM', policy).ok()

# Test ignore policy
@skip(cluster=True)
def test_query_oom_ignore(env):
    # Create an index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
    # Add a document
    env.expect('HSET', 'doc', 'name', 'hello').equal(1)

    # Change maxmemory to 1
    env.expect('CONFIG', 'SET', 'maxmemory', '1').ok()

    # The test should ignore OOM since 'ignore' is the default config
    # TODO : change/ remove test if default config is changed
    res = env.cmd('FT.SEARCH', 'idx', '*')
    env.assertEqual(res, [1, 'doc', ['name', 'hello']])
    res = env.cmd('FT.AGGREGATE', 'idx', '*')
    env.assertEqual(res[0], 1)

@skip(cluster=True)
def test_query_oom_fail(env):
    # Change oom policy to fail
    # TODO : Change if default value is changed
    change_oom_policy(env, 'fail')

    # Create an index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
    # Add a document
    env.expect('HSET', 'doc', 'name', 'hello').equal(1)

    # Change maxmemory to 1
    env.expect('CONFIG', 'SET', 'maxmemory', '1').ok()

    for config_return in [False, True]:
        # Since we are in a standalone env, the test should fail also if the config is 'return'
        if config_return:
            change_oom_policy(env, 'return')
        # Verify query fails
        env.expect('FT.SEARCH', 'idx', '*').error().contains(OOM_QUERY_ERROR)
        # Verify aggregation query fails
        env.expect('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@name').error().contains(OOM_QUERY_ERROR)

# Temp test to be removed when cluster env is supported
@skip(cluster=False)
def test_query_oom_cluster_ignore():
    # Create cluster with more than 1 so we are in a cluster env
    env  = Env(shardsCount=3)
    # Change oom policy to fail
    # TODO : Change if default value is changed
    change_oom_policy(env, 'fail')

    # Create an index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
    # Add a document
    env.expect('HSET', 'doc', 'name', 'hello').equal(1)

    # Change maxmemory to 1
    env.expect('CONFIG', 'SET', 'maxmemory', '1').ok()

    # The test should ignore OOM since we are in a cluster env
    # TODO : change/ remove test if cluster env is supported

    res = env.cmd('FT.SEARCH', 'idx', '*')
    env.assertEqual(res, [1, 'doc', ['name', 'hello']])
    res = env.cmd('FT.AGGREGATE', 'idx', '*')
    env.assertEqual(res[0], 1)

# Temp test to be removed when cluster env is supported
@skip(cluster=False)
def test_query_oom_single_shard_fail():
    # Create cluster with 1 shard
    env  = Env(shardsCount=1)
    # Change oom policy to fail
    # TODO : Change if default value is changed
    change_oom_policy(env, 'fail')

    # Create an index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
    # Add a document
    env.expect('HSET', 'doc', 'name', 'hello').equal(1)

    # Change maxmemory to 1
    env.expect('CONFIG', 'SET', 'maxmemory', '1').ok()

    # Verify query fails
    env.expect('FT.SEARCH', 'idx', '*').error().contains(OOM_QUERY_ERROR)
    # Verify aggregation query fails
    env.expect('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@name').error().contains(OOM_QUERY_ERROR)
