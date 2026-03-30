from common import *

@skip(cluster=False)
def testInfo(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'SORTABLE', 'v', 'VECTOR', 'HNSW', '6', 'TYPE', 'FLOAT32', 'DIM', '2','DISTANCE_METRIC', 'L2').ok()
    for i in range (100):
        conn.execute_command('HSET', i, 't', 'Hello world!', 'v', 'abcdefgh')

    idx_info = index_info(env, 'idx')
    env.assertLess(float(idx_info['inverted_sz_mb']), 1)
    env.assertLess(float(idx_info['offset_vectors_sz_mb']), 1)
    env.assertLess(float(idx_info['doc_table_size_mb']), 1)
    env.assertLess(float(idx_info['sortable_values_size_mb']), 1)
    env.assertLess(float(idx_info['key_table_size_mb']), 1)
    env.assertLess(float(idx_info['vector_index_sz_mb']), 1)
    env.assertGreater(float(idx_info['inverted_sz_mb']), 0)
    env.assertGreater(float(idx_info['offset_vectors_sz_mb']), 0)
    env.assertGreater(float(idx_info['doc_table_size_mb']), 0)
    env.assertGreater(float(idx_info['sortable_values_size_mb']), 0)
    env.assertGreater(float(idx_info['key_table_size_mb']), 0)
    env.assertGreater(float(idx_info['vector_index_sz_mb']), 0)

@skip(cluster=True)
def test_required_fields(env):
    # Testing coordinator<-> shard `_REQUIRED_FIELDS` protocol
    env.expect('ft.create', 'idx', 'schema', 't', 'text').ok()
    env.cmd('HSET', '0', 't', 'hello')
    env.expect('ft.search', 'idx', 'hello', '_REQUIRED_FIELDS').error()
    env.expect('ft.search', 'idx', 'hello', '_REQUIRED_FIELDS', '2', 't').error()
    env.expect('ft.search', 'idx', 'hello', '_REQUIRED_FIELDS', '1', 't').equal([1, '0', '$hello', ['t', 'hello']])
    env.expect('ft.search', 'idx', 'hello', 'nocontent', 'SORTBY', 't', '_REQUIRED_FIELDS', '1', 't').equal([1, '0', '$hello'])
    # Field is not in Rlookup, will not load
    env.expect('ft.search', 'idx', 'hello', 'nocontent', '_REQUIRED_FIELDS', '1', 't').equal([1, '0', None])


def check_info_commandstats(env, cmd):
    res = env.cmd('INFO', 'COMMANDSTATS')
    env.assertGreater(res['cmdstat_' + cmd]['usec'], res['cmdstat__' + cmd]['usec'])

@skip(cluster=False, redis_less_than="6.2.0")
def testCommandStatsOnRedis(env):
    # This test checks the total time spent on the Coordinator is greater then
    # on a single shard

    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'SORTABLE').ok()
    # _FT.CREATE is not called. No option to test

    for i in range(100):
        conn.execute_command('HSET', i, 't', 'Hello world!')

    env.expect('FT.SEARCH', 'idx', 'hello', 'LIMIT', 0, 0).equal([100])
    check_info_commandstats(env, 'FT.SEARCH')

    env.expect('FT.AGGREGATE', 'idx', 'hello', 'LIMIT', 0, 0).noError()
    check_info_commandstats(env, 'FT.AGGREGATE')

    conn.execute_command('FT.INFO', 'idx')
    check_info_commandstats(env, 'FT.INFO')

@skip(cluster=False)
def testPendingCommands():
    num_io_threads = 2 # Multiple IO threads for an edge case in `SHARD_CONNECTION_STATES`
    env = Env(moduleArgs=f'SEARCH_IO_THREADS {num_io_threads}')
    verify_shard_init(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    max_pending_commands = 50

    # Run each command `max_pending_commands` times, to verify they are not pending
    # after they are executed, and that the coordinator is still responsive.
    for _ in range(max_pending_commands * num_io_threads):
        env.cmd(config_cmd(), 'SET', 'CONN_PER_SHARD', '0')
    env.expect('FT.SEARCH', 'idx', 'hello').equal([0])

    for _ in range(max_pending_commands * num_io_threads):
        env.cmd(debug_cmd(), 'SHARD_CONNECTION_STATES')
    env.expect('FT.SEARCH', 'idx', 'hello').equal([0])

    for _ in range(max_pending_commands * num_io_threads):
        env.cmd('SEARCH.CLUSTERINFO')
    env.expect('FT.SEARCH', 'idx', 'hello').equal([0])

def test_curly_brackets(env):
    conn = getConnectionByEnv(env)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'SORTABLE').ok()

    conn.execute_command('HSET', 'foo{bar}', 't', 'Hello world!')
    env.expect('ft.search', 'idx', 'hello').equal([1, 'foo{bar}', ['t', 'Hello world!']])
    env.expect('ft.aggregate', 'idx', 'hello', 'LOAD', 1, '__key').equal([1, ['__key', 'foo{bar}']])

@skip(cluster=False)
def test_MOD_3540(env):
    # check server does not crash when MAX argument for SORTBY is greater than 10
    conn = getConnectionByEnv(env)

    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT')
    for i in range(100):
        conn.execute_command('HSET', i, 't', i)

    env.expect('FT.SEARCH', 'idx', '*', 'SORTBY', 't', 'DESC', 'MAX', '20')

def test_error_propagation_from_shards(env):
    """Tests that errors from the shards are propagated properly to the
    coordinator, for both `FT.SEARCH` and `FT.AGGREGATE` commands.
    We check the following errors:
    1. Non-existing index.
    2. Bad query.

    * Timeouts are handled and tested separately.
    """

    SkipOnNonCluster(env)

    # indexing an index that doesn't exist (today revealed only in the shards)
    env.expect('FT.AGGREGATE', 'idx', '*').error().contains('SEARCH_INDEX_NOT_FOUND Index not found')
    env.expect('FT.SEARCH', 'idx', '*').error().contains('SEARCH_INDEX_NOT_FOUND Index not found')

    # Bad query
    # create the index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    env.expect('FT.AGGREGATE', 'idx', '**').error().contains('Syntax error')
    env.expect('FT.SEARCH', 'idx', '**').error().contains('Syntax error')

    # Other stuff that are being checked only on the shards (FYI):
    #   1. The language requested in the command.
    #   2. The scorer requested in the command.
    #   3. Parameters evaluation

@skip(cluster=False, min_shards=2)
def test_index_missing_on_one_shard(env):
    """Tests that we get an error if the index is missing on one shard.
    """

    first_conn = env.getConnection(0)

    # Create an index on all shards
    index_name = 'idx'
    env.expect(
        'FT.CREATE', index_name, 'SCHEMA', 'n', 'NUMERIC', 't', 'TEXT',
        'v', 'VECTOR', 'FLAT', 6, 'TYPE', 'FLOAT32', 'DIM', 2,
        'DISTANCE_METRIC', 'L2').ok()

    # Drop the index on only one shard (without recreating it)
    first_conn.execute_command('DEBUG', 'MARK-INTERNAL-CLIENT')
    first_conn.execute_command('_FT.DROPINDEX', index_name)

    error_msg = f'SEARCH_INDEX_NOT_FOUND Index not found: {index_name}'

    # Query via the shard connection
    try:
        first_conn.execute_command('_FT.INFO', index_name)
        env.assertTrue(False) # Should not reach this point
    except Exception as e:
        env.assertContains(error_msg, str(e))

    # Query via the cluster connection
    env.expect('FT.SEARCH', index_name, '*').error().contains(error_msg)
    env.expect('FT.AGGREGATE', index_name, '*').error().contains(error_msg)
    env.expect('FT.HYBRID', index_name, 'SEARCH', '*',
               'VSIM', '@v', '$BLOB', 'PARAMS', '2', 'BLOB', 'aaaabbbb')\
                .error().contains(error_msg)
    env.expect('FT.SYNUPDATE', index_name, '1', 'a', 'b')\
                .error().contains(error_msg)
    env.expect('FT.ALTER', index_name, 'SCHEMA', 'ADD', 'n2', 'NUMERIC')\
                .error().contains(error_msg)
    # FT.TAGVALS: query the shard directly (not via coordinator) to ensure we hit
    # the shard where the index is missing, avoiding non-deterministic fanout behavior.
    try:
        first_conn.execute_command('_FT.TAGVALS', index_name, 'n')
        env.assertTrue(False, message="TAGVALS should have failed with index not found")
    except Exception as e:
        env.assertContains(error_msg, str(e))
    env.expect('FT.MGET', index_name, 'doc1').error().contains(error_msg)
    env.expect('FT.DROP', index_name).error().contains(error_msg)

@skip(cluster=False)
def test_timeout():
    """Tests that timeouts are handled properly by the coordinator.
    We check that the coordinator returns a timeout error when the timeout is
    reached in the shards or in the coordinator itself.
    """

    env = Env(moduleArgs='DEFAULT_DIALECT 2 ON_TIMEOUT FAIL TIMEOUT 1')
    conn = getConnectionByEnv(env)

    # Create the index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'title', 'TEXT').ok()

    # Populate the database with many documents (more docs --> less flakiness)
    n_docs = 25000 * env.shardsCount
    for i in range(n_docs):
        conn.execute_command('HSET', i ,'t1', str(i))

    # No client cursor
    env.expect(
        'FT.AGGREGATE', 'idx', '*', 'LOAD', '2', '@t1', '@__key', 'APPLY',
        '@t1 ^ @t1', 'AS', 't1exp', 'groupby', '2', '@t1', '@t1exp', 'REDUCE',
        'tolist', '1', '@__key', 'AS', 'keys', 'timeout', '1'
    ).error().contains('SEARCH_TIMEOUT Timeout limit was reached')

    # Client cursor mid execution
    env.expect(
        'FT.AGGREGATE', 'idx', '*', 'LOAD', '*', 'WITHCURSOR', 'COUNT', n_docs,
        'timeout', '1'
    ).error().contains('SEARCH_TIMEOUT Timeout limit was reached')

    # FT.SEARCH
    env.expect(
        'FT.SEARCH', 'idx', '*', 'LIMIT', '0', n_docs, 'timeout', '1'
    ).error().contains('SEARCH_TIMEOUT Timeout limit was reached')

@skip(cluster=False, min_shards=2)
def test_mod_6287(env):
    """Tests that the coordinator does not crash on aggregations with cursors,
    when some of the shards return an error while the others don't. Specifically,
    such a scenario depicted in PR #4324 results in a crash since the `depleted`
    and `pending` flags/counter were not aligned."""

    conn = getConnectionByEnv(env)
    con2 = env.getConnection(2)

    # Create an index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC').ok()

    # Populate the database with enough documents to make sure that each shard
    # will get at least 2 `_FT.CURSOR READ` commands from the coordinator, and
    # still have more docs to return.
    # Each such command pulls 1000 docs from each shard, so 2500 should work.
    n_docs = 2500 * env.shardsCount
    for i in range(n_docs):
        conn.execute_command('HSET', i, 'n', i)

    # Dispatch an aggregate with cursor command to the coordinator
    res, cid = env.cmd('FT.AGGREGATE', 'idx', '*', 'LOAD', '*', 'WITHCURSOR')
    received = len(res)-1

    # Delete a shard cursor (of shard 2 in this case) so that once the coordinator
    # sends a `CURSOR READ` command to that shard, it will return an error.
    # Now (after PR 6287), the command for the errored shard will be set as
    # `depleted`, such that the `depleted` shards will be aligned with the
    # `pending` counter.
    con2.execute_command(debug_cmd(), 'DELETE_LOCAL_CURSORS')

    # Dispatch an `FT.CURSOR READ` command that will request for more results from the shards
    # This results in the crash solved by #6287
    res, cid = env.cmd('FT.CURSOR', 'READ', 'idx', cid, 'COUNT', n_docs - received)
    env.assertEqual(cid, 0)

    # Send another command to make sure that the coordinator is healthy
    res = env.cmd('FT.AGGREGATE', 'idx', '*', 'LIMIT', '0', str(n_docs))
    env.assertEqual(len(res)-1, n_docs)

def test_single_shard_optimization():
    env = Env(shardsCount=1) # Either standalone or cluster with 1 shard

    # Create the index
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT', 'n', 'NUMERIC').ok()
    with env.getClusterConnectionIfNeeded() as conn:
        for i in range(100):
            conn.execute_command('HSET', i, 't', 'Hello world!', 'n', i)

    # Search
    env.expect('FT.SEARCH', 'idx', 'hello', 'SORTBY', 'n', 'RETURN', 0).equal([100] + [str(i) for i in range(10)])

    # Aggregate
    env.expect('FT.AGGREGATE', 'idx', 'hello', 'SORTBY', '1', '@n').equal([100] + [['n', str(i)] for i in range(10)])

    # Cursor
    res, cid = env.cmd('FT.AGGREGATE', 'idx', 'hello', 'SORTBY', '1', '@n', 'LIMIT', '0', '19', 'WITHCURSOR', 'COUNT', '10')
    env.assertEqual(res[1:], [['n', str(i)] for i in range(10)])
    env.assertNotEqual(cid, 0)
    res, cid = env.cmd('FT.CURSOR', 'READ', 'idx', cid)
    env.assertEqual(res[1:], [['n', str(i)] for i in range(10, 19)])
    env.assertEqual(cid, 0)

    # Profile
    env.expect('FT.PROFILE', 'idx', 'SEARCH', 'QUERY', 'hello').noError().apply(lambda res: to_dict(res[1])['Coordinator']).equal([])
    # A simple validation that we get a standalone error response
    env.expect('FT.PROFILE', 'idx', 'SEARCH', 'hello', 'world').error().contains('The QUERY keyword is expected')
    # Verify that PROFILE does not support WITHCURSOR
    env.expect('FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', 'hello', 'WITHCURSOR').error().contains('FT.PROFILE does not support cursor')

    # SpellCheck
    env.expect('FT.SPELLCHECK', 'idx', 'hell').equal([['TERM', 'hell', [['1', 'hello']]]])



def _set_all_shards_unreachable(env: Env):
    """Set topology so all shards point to unreachable addresses (port 9)."""
    env.expect('SEARCH.CLUSTERSET',
               'MYID', '1',
               'RANGES', '2',
               'SHARD', '1', 'SLOTRANGE', '0', '8191',
               'ADDR', '127.0.0.1:9', 'MASTER',
               'SHARD', '2', 'SLOTRANGE', '8192', '16383',
               'ADDR', '127.0.0.1:9', 'MASTER'
    ).ok()
    # Wait for the new topology to be applied
    wait_for_condition(
        lambda: (env.cmd('SEARCH.CLUSTERINFO')[5][0][7] == 9, {}),
        'Failed waiting for topology to be applied'
    )


def _set_one_shard_unreachable(env: Env):
    """Set topology so one shard is reachable and one points to an unreachable address."""
    # Get the real shard address before we modify the topology
    cluster_info = env.cmd('SEARCH.CLUSTERINFO')
    # cluster_info[5] is the shards array, [0] is first shard, [7] is port, [5] is host
    real_port = cluster_info[5][0][7]
    real_host = cluster_info[5][0][5]

    env.expect('SEARCH.CLUSTERSET',
               'MYID', '1',
               'RANGES', '2',
               'SHARD', '1', 'SLOTRANGE', '0', '8191',
               'ADDR', f'{real_host}:{real_port}', 'MASTER',
               'SHARD', '2', 'SLOTRANGE', '8192', '16383',
               'ADDR', '127.0.0.1:9', 'MASTER'
    ).ok()
    # Wait for the new topology to be applied (check that any shard has port 9)
    wait_for_condition(
        lambda: (any(shard[7] == 9 for shard in env.cmd('SEARCH.CLUSTERINFO')[5]), {}),
        'Failed waiting for topology to be applied'
    )


def _test_all_queries_fail_on_unreachable_shard(env: Env, scenario: str):
    """Test that FT.SEARCH, FT.AGGREGATE, and FT.HYBRID all return an error."""
    # FT.SEARCH returns an error (does not hang)
    with TimeLimit(5, f'FT.SEARCH hung ({scenario})'):
        env.expect('FT.SEARCH', 'idx', '*').error().contains('Could not send query to cluster')

    # FT.SEARCH with cursor returns an error (does not hang)
    with TimeLimit(5, f'FT.SEARCH WITHCURSOR hung ({scenario})'):
        env.expect('FT.SEARCH', 'idx', '*', 'WITHCURSOR').error().contains('Could not send query to cluster')

    # FT.AGGREGATE returns an error (does not hang)
    with TimeLimit(5, f'FT.AGGREGATE hung ({scenario})'):
        env.expect('FT.AGGREGATE', 'idx', '*').error().contains('Could not send query to cluster')

    # FT.AGGREGATE with cursor returns an error (does not hang)
    with TimeLimit(5, f'FT.AGGREGATE WITHCURSOR hung ({scenario})'):
        env.expect('FT.AGGREGATE', 'idx', '*', 'WITHCURSOR').error().contains('Could not send query to cluster')

    # FT.HYBRID returns an error (does not hang)
    with TimeLimit(5, f'FT.HYBRID hung ({scenario})'):
        env.expect('FT.HYBRID', 'idx',
                   'SEARCH', '*',
                   'VSIM', '@v', '$BLOB',
                   'PARAMS', '2', 'BLOB', 'abcdefgh'
        ).error().contains('Could not send query to cluster')


@skip(cluster=False, min_shards=2)
def test_queries_fail_on_all_shards_unreachable(env: Env):
    """Test that all query commands (FT.SEARCH, FT.AGGREGATE, FT.HYBRID) return an error
    when all shards are unreachable, rather than hanging indefinitely.

    When MRCluster_SendCommand fails (REDIS_ERR) during the initial fanout, the error
    must be routed through the user callback so that:
    - FT.SEARCH: The reducer receives the error and returns it to the client
    - FT.AGGREGATE: The error is pushed to the channel and consumed by rpnetNext
    - FT.HYBRID: The processCursorMappingCallback increments responseCount and signals
      the condition variable, allowing ProcessHybridCursorMappings to unblock
    """
    # Create an index and add data before breaking topology
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               't', 'TEXT',
               'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2',
               'DISTANCE_METRIC', 'L2').ok()
    conn = getConnectionByEnv(env)
    for i in range(10):
        conn.execute_command('HSET', f'doc{i}', 't', f'hello{i}', 'v', 'abcdefgh')

    # Pause topology refresh so our invalid topology stays in effect
    env.expect(debug_cmd(), 'PAUSE_TOPOLOGY_UPDATER').ok()
    # Set validation timeout to 1ms so we don't wait for unreachable shards
    env.expect(config_cmd(), 'SET', 'TOPOLOGY_VALIDATION_TIMEOUT', '1').ok()

    _set_all_shards_unreachable(env)
    _test_all_queries_fail_on_unreachable_shard(env, 'all shards unreachable')


@skip(cluster=False, min_shards=2)
def test_queries_fail_on_one_shard_unreachable(env: Env):
    """Test that all query commands (FT.SEARCH, FT.AGGREGATE, FT.HYBRID) return an error
    when one shard is unreachable, rather than hanging indefinitely or returning partial results.
    """
    # Create an index and add data before breaking topology
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               't', 'TEXT',
               'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2',
               'DISTANCE_METRIC', 'L2').ok()
    conn = getConnectionByEnv(env)
    for i in range(10):
        conn.execute_command('HSET', f'doc{i}', 't', f'hello{i}', 'v', 'abcdefgh')

    # Pause topology refresh so our invalid topology stays in effect
    env.expect(debug_cmd(), 'PAUSE_TOPOLOGY_UPDATER').ok()
    # Set validation timeout to 1ms so we don't wait for unreachable shards
    env.expect(config_cmd(), 'SET', 'TOPOLOGY_VALIDATION_TIMEOUT', '1').ok()

    _set_one_shard_unreachable(env)
    _test_all_queries_fail_on_unreachable_shard(env, 'one shard unreachable')


def _get_shard_cursor_count(conn, idx='idx'):
    """Get cursor count from a shard using _FT.INFO (internal, shard-local command)."""
    info = conn.execute_command('_FT.INFO', idx)
    info_dict = to_dict(info)
    cursor_stats = to_dict(info_dict.get('cursor_stats', {}))
    return cursor_stats.get('global_total', 0)


@skip(cluster=False, min_shards=2)
def test_hybrid_cursor_cleanup_on_partial_failure(env: Env):
    """Test that FT.HYBRID cleans up cursors on reachable shards when one shard is unreachable.

    When FT.HYBRID's mapping phase succeeds on some shards but fails on others (e.g., unreachable),
    the coordinator must send _FT.CURSOR DEL commands to clean up cursors on the successful shards.
    """
    # Create an index with vector field for FT.HYBRID
    env.expect('FT.CREATE', 'idx', 'SCHEMA',
               't', 'TEXT',
               'v', 'VECTOR', 'FLAT', '6', 'TYPE', 'FLOAT32', 'DIM', '2',
               'DISTANCE_METRIC', 'L2').ok()
    conn = getConnectionByEnv(env)
    for i in range(10):
        conn.execute_command('HSET', f'doc{i}', 't', f'hello{i}', 'v', 'abcdefgh')

    # Get a direct connection to shard 1 before modifying topology
    shard1_conn = env.getConnection(shardId=1)
    shard1_conn.execute_command('DEBUG', 'MARK-INTERNAL-CLIENT')

    # Record cursor count before breaking topology
    cursors_before = _get_shard_cursor_count(shard1_conn)

    # Pause topology refresh so our invalid topology stays in effect
    env.expect(debug_cmd(), 'PAUSE_TOPOLOGY_UPDATER').ok()
    # Set validation timeout to 1ms so we don't wait for unreachable shards
    env.expect(config_cmd(), 'SET', 'TOPOLOGY_VALIDATION_TIMEOUT', '1').ok()

    _set_one_shard_unreachable(env)

    # Run FT.HYBRID - it will create cursors on shard 1 (reachable) during mapping phase,
    # but fail overall because shard 2 is unreachable. The fix should send DEL commands.
    with TimeLimit(5, 'FT.HYBRID hung'):
        env.expect('FT.HYBRID', 'idx',
                   'SEARCH', '*',
                   'VSIM', '@v', '$BLOB',
                   'PARAMS', '2', 'BLOB', 'abcdefgh'
        ).error().contains('Could not send query to cluster')

    # Verify cursors on the reachable shard were cleaned up (cursor count returns to original)
    wait_for_condition(
        lambda: (_get_shard_cursor_count(shard1_conn) == cursors_before, {}),
        'Cursors were not cleaned up on reachable shard after FT.HYBRID failure'
    )
