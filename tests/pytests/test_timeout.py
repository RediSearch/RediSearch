from common import *


def verifyTimeoutResultsResp3(env, res, expected_results_count, message="", depth=0):
    env.assertEqual(len(res["results"]), expected_results_count, depth=depth+1, message=message + " unexpected results count")
    VerifyTimeoutWarningResp3(env, res, depth=depth+1, message=message + " unexpected results count")


def _blocked_callback_count(env):
    return int(env.cmd(debug_cmd(), 'QUERY_CONTROLLER',
                       'GET_BLOCKED_REPLY_CALLBACK_COUNT'))


def _blocked_onfree_count(env):
    return int(env.cmd(debug_cmd(), 'QUERY_CONTROLLER',
                       'GET_BLOCKED_REQUEST_ONFREE_COUNT'))


def _test_return_background_stages_partial_results(protocol):
    """RETURN stages partial RESP bytes while retaining the reply callback."""
    env = Env(protocol=protocol, enableDebugCommand=True,
              moduleArgs='WORKERS 1 ON_TIMEOUT RETURN')
    skipIfNoEnableAssert(env)
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE').ok()
    for i in range(5):
        conn.execute_command('HSET', f'doc:{i}', 'n', i)
    waitForIndex(env, 'idx')

    callbacks_before = _blocked_callback_count(env)
    onfree_before = _blocked_onfree_count(env)
    query_args = parseDebugQueryCommandArgs(
        ['FT.AGGREGATE', 'idx', '*',
         'SORTBY', '2', '@n', 'ASC', 'LOAD', '1', '@n'],
        ['TIMEOUT_AFTER_N', 2],
    )
    query_result = conn.execute_command(debug_cmd(), *query_args)

    # A duplicate callback reply would be observed as a ghost response here.
    env.assertEqual(conn.execute_command('PING'), True)

    # The client receives the worker-staged bytes only after Redis invokes the
    # registered reply callback and the cycle's free callback.
    env.assertEqual(_blocked_callback_count(env), callbacks_before + 1)
    env.assertEqual(_blocked_onfree_count(env), onfree_before + 1)

    if protocol == 2:
        env.assertEqual(query_result, [2, ['n', '0'], ['n', '1']])
    else:
        env.assertEqual(query_result, {
            'attributes': [],
            'warning': ['Timeout limit was reached'],
            'total_results': 2,
            'format': 'STRING',
            'results': [
                {'extra_attributes': {'n': '0'}, 'values': []},
                {'extra_attributes': {'n': '1'}, 'values': []},
            ],
        })


@skip(cluster=True)
def test_return_background_stages_partial_results_resp2():
    """Exercise callback-preserving worker staging under RESP2."""
    _test_return_background_stages_partial_results(2)


@skip(cluster=True)
def test_return_background_stages_partial_results_resp3():
    """Exercise callback-preserving worker staging under RESP3."""
    _test_return_background_stages_partial_results(3)


@skip(cluster=True)
def test_return_background_stages_search_reply():
    """FT.SEARCH RETURN stages its reply and still invokes the callback."""
    env = Env(protocol=3, enableDebugCommand=True,
              moduleArgs='WORKERS 1 ON_TIMEOUT RETURN')
    skipIfNoEnableAssert(env)
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE').ok()
    for i in range(5):
        conn.execute_command('HSET', f'doc:{i}', 'n', i)
    waitForIndex(env, 'idx')

    callbacks = _blocked_callback_count(env)
    onfree = _blocked_onfree_count(env)
    result = conn.execute_command(
        'FT.SEARCH', 'idx', '*', 'SORTBY', 'n', 'ASC',
        'RETURN', 1, 'n', 'LIMIT', 0, 5,
    )
    env.assertEqual(result['total_results'], 5)
    env.assertEqual(len(result['results']), 5)
    env.assertEqual(conn.execute_command('PING'), True)
    env.assertEqual(_blocked_callback_count(env), callbacks + 1)
    env.assertEqual(_blocked_onfree_count(env), onfree + 1)


@skip(cluster=True)
def test_return_background_stages_cursor_cycles():
    """Initial and follow-up cursor chunks each retain exactly one callback."""
    env = Env(protocol=3, enableDebugCommand=True,
              moduleArgs='WORKERS 1 ON_TIMEOUT RETURN')
    skipIfNoEnableAssert(env)
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE').ok()
    for i in range(5):
        conn.execute_command('HSET', f'doc:{i}', 'n', i)
    waitForIndex(env, 'idx')

    callbacks = _blocked_callback_count(env)
    onfree = _blocked_onfree_count(env)
    result, cursor = conn.execute_command(
        'FT.AGGREGATE', 'idx', '*', 'SORTBY', '2', '@n', 'ASC',
        'LOAD', '1', '@n', 'WITHCURSOR', 'COUNT', 2,
    )
    env.assertEqual(len(result['results']), 2)
    env.assertNotEqual(cursor, 0)
    received = len(result['results'])
    callbacks += 1
    onfree += 1
    env.assertEqual(_blocked_callback_count(env), callbacks)
    env.assertEqual(_blocked_onfree_count(env), onfree)

    while cursor:
        result, cursor = conn.execute_command('FT.CURSOR', 'READ', 'idx', cursor, 'COUNT', 2)
        received += len(result['results'])
        callbacks += 1
        onfree += 1
        env.assertEqual(_blocked_callback_count(env), callbacks)
        env.assertEqual(_blocked_onfree_count(env), onfree)

    env.assertEqual(received, 5)
    env.assertEqual(conn.execute_command('PING'), True)

# skip on cluster since there might not be enough documents in each shard to reach the RP_INDEX timeout limit counter.
@skip(cluster=True)
def testEmptyResult():
    env = Env(protocol=3, moduleArgs='ON_TIMEOUT RETURN')
    conn = getConnectionByEnv(env)

    # Create the index
    env.expect('FT.CREATE idx SCHEMA n numeric').ok()

    # Populate the index
    num_docs = 150
    for i in range(num_docs):
        conn.execute_command('HSET', f'doc{i}' ,'n', i)

    # Before the bug fix, the first doc caused timeout and returned as an empty valid result. Since we reset the timeout counter of RP_INDEX,
    # The next call to the query pipeline we will continue iterating over the results until EOF is reached or for another TIMEOUT_COUNTER_LIMIT reads.
    # Now, upon timeout, the reply ends with no further calls to the query pipeline.
    res = env.cmd('_ft.debug', 'FT.AGGREGATE', 'idx', '*', 'load', '1', '@n', 'LIMIT', 99, 110, 'TIMEOUT_AFTER_N', 99, 'DEBUG_PARAMS_COUNT', 2)

    verifyTimeoutResultsResp3(env, res, 0)

# This test purpose it to verify that a cursor with limit (a pager), and some reads that result in timeout,
# will be depleted once the sum of all the read results is equal to the limit.
# Before the bug fix, the pager would decrease its counter for every 'Next' call to its upstream result processor.
# Even though the upstream result processor returned might return an error or a timeout, without any new result.
# As a result, with every cursor read resulted in a timeout, the pager would decrease its counter by 1, leading to a total
# results count of limit - timedout_cursor_reads.
def TestLimitWithCursor():
    env = Env(protocol=3, moduleArgs='ON_TIMEOUT RETURN')
    conn = getConnectionByEnv(env)
    # Create the index
    env.expect('FT.CREATE idx SCHEMA n numeric').ok()

    # Populate the index
    num_docs = 150
    for i in range(num_docs):
        conn.execute_command('HSET', f'doc{i}' ,'n', i)

    # query with timeout
    timeout_res_count = num_docs // 4
    res, cursor = env.cmd('_ft.debug', 'FT.AGGREGATE', 'idx', '*', 'WITHCURSOR', 'COUNT', num_docs, 'LIMIT', 0, num_docs, 'TIMEOUT_AFTER_N', timeout_res_count, 'DEBUG_PARAMS_COUNT', 2)
    total_res = len(res["results"])

    while (cursor):
        res, cursor = env.cmd('FT.CURSOR', 'READ', 'idx', cursor)
        total_res += len(res["results"])
    # before the bug fix we got total_res = limit - cursor_reads
    env.assertEqual(total_res, num_docs, message="unexpected results count")


def test_search_debug_zero_params_count():
    """Test that DEBUG_PARAMS_COUNT 0 returns an error for FT.SEARCH.
    Include a dummy debug param so we pass the arity check (argc >= 7).
    """
    env = Env(enableDebugCommand=True)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC').ok()
    env.expect(debug_cmd(), 'FT.SEARCH', 'idx', '*',
               'TIMEOUT_AFTER_N', '100',
               'DEBUG_PARAMS_COUNT', '0').error().contains('Invalid DEBUG_PARAMS_COUNT count')


def test_aggregate_debug_zero_params_count():
    """Test that DEBUG_PARAMS_COUNT 0 returns an error for FT.AGGREGATE.
    Include a dummy debug param so we pass the arity check (argc >= 7).
    """
    env = Env(enableDebugCommand=True)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC').ok()
    env.expect(debug_cmd(), 'FT.AGGREGATE', 'idx', '*',
               'TIMEOUT_AFTER_N', '100',
               'DEBUG_PARAMS_COUNT', '0').error().contains('Invalid DEBUG_PARAMS_COUNT count')
