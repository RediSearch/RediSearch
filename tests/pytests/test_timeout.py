from common import *

# skip on cluster since there might not be enough documents in each shard to reach the RP_INDEX timeout limit counter.
@skip(cluster=True)
def testEmptyResult():
    env = Env(moduleArgs='ON_TIMEOUT RETURN')
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
    res = env.cmd(debug_cmd(), 'FT.AGGREGATE', 'idx', '*', 'load', '1', '@n', 'LIMIT', 99, 110, 'TIMEOUT_AFTER_N', 99, 'DEBUG_PARAMS_COUNT', 2)

    verifyResultLen(env, res, 0, mode="AGG")

# Based on this page https://redislabs.atlassian.net/wiki/spaces/DX/pages/edit-v2/5153554508,
# detailing all the timeout scnarios and their expected results.
def TestAllScenarios():
    env = Env(moduleArgs='ON_TIMEOUT FAIL')
    # Create the index
    env.expect('FT.CREATE idx SCHEMA n numeric').ok()
    conn = getConnectionByEnv(env)
    # Populate the index
    num_docs = 150
    for i in range(num_docs):
        conn.execute_command('HSET', f'doc{i}' ,'n', i)

    # ================ FAIL POLICY =================

    """Test WITHCURSOR BASIC + FAIL policy"""
    # scenario: timeout on first pass
    # expected: [0, 0]
    timeout_res_count = 0
    res, cursor = env.cmd(debug_cmd(), 'FT.AGGREGATE', 'idx', '*', 'WITHCURSOR', 'load', '1', '@n', 'TIMEOUT_AFTER_N', timeout_res_count, 'DEBUG_PARAMS_COUNT', 2)
    env.assertEqual(res, [0])
    env.assertEqual(cursor, 0)
    # scenario: timeout on later pass
    # expected: [[ANY, <doc>, <doc> .. ], 0]
    timeout_res_count = 3
    res, cursor = env.cmd(debug_cmd(), 'FT.AGGREGATE', 'idx', '*', 'WITHCURSOR', 'load', '1', '@n', 'TIMEOUT_AFTER_N', timeout_res_count, 'DEBUG_PARAMS_COUNT', 2)
    # skip doc count since it is unpredictable
    env.assertEqual(res[1:], [['n', str(i)] for i in range(timeout_res_count)])
    env.assertEqual(cursor, 0)

    """Test WITHCURSOR + SORTER + FAIL policy"""
    # Sorter logic includes timeout policy validation. On FAIL, propagates timeout without returning any results.
    # scenario: timeout on first pass. timeout on later pass is unreachable.
    # expected: [timeout_res_count, 0]
    timeout_res_count = 3
    res, cursor = env.cmd(debug_cmd(), 'FT.AGGREGATE', 'idx', '*', 'WITHCURSOR', 'sortby', '1', '@n', 'load', '1', '@n', 'TIMEOUT_AFTER_N', timeout_res_count, 'DEBUG_PARAMS_COUNT', 2)
    env.assertEqual(res, [timeout_res_count])
    env.assertEqual(cursor, 0)

    """Test WITHCURSOR + GROUPER + FAIL policy"""
    # Grouper drops accumulated results regardless the policy and propagates timeout
    # scenario: timeout on first pass. timeout on later pass is unreachable.
    # expected: [timeout_res_count, 0]
    res, cursor = env.cmd(debug_cmd(), 'FT.AGGREGATE', 'idx', '*', 'WITHCURSOR', 'load', '1', '@n', 'groupby', '1', '@n', 'TIMEOUT_AFTER_N', timeout_res_count, 'DEBUG_PARAMS_COUNT', 2)
    env.assertEqual(res, [timeout_res_count])
    env.assertEqual(cursor, 0)

    """Test No cursor BASIC + FAIL policy"""
    # scenario: timeout on first pass
    # expected: ["Timeout limit was reached"]
    timeout_res_count = 0
    res = env.cmd(debug_cmd(), 'FT.AGGREGATE', 'idx', '*', 'load', '1', '@n', 'TIMEOUT_AFTER_N', timeout_res_count, 'DEBUG_PARAMS_COUNT', 2)
    env.assertEqual(res, ["Timeout limit was reached"])
    # scenario: timeout on later pass
    # expected: [ANY, <doc>, <doc> .. ]
    timeout_res_count = 3
    res = env.cmd(debug_cmd(), 'FT.AGGREGATE', 'idx', '*', 'load', '1', '@n', 'TIMEOUT_AFTER_N', timeout_res_count, 'DEBUG_PARAMS_COUNT', 2)
    # skip doc count since it is unpredictable
    env.assertEqual(res[1:], [['n', str(i)] for i in range(timeout_res_count)])

    """Test No cursor + SORTER + FAIL policy"""
    # scenario: timeout on first pass, timeout on later pass is unreachable.
    # expected: ["Timeout limit was reached"]
    timeout_res_count = 4
    res = env.cmd(debug_cmd(), 'FT.AGGREGATE', 'idx', '*', 'load', '1', '@n', 'sortby', '1', '@n', 'TIMEOUT_AFTER_N', timeout_res_count, 'DEBUG_PARAMS_COUNT', 2)
    env.assertEqual(res, ["Timeout limit was reached"])
    # assuming FT.SEARCH always includes a sorter
    res = env.cmd(debug_cmd(), 'FT.SEARCH', 'idx', '*', 'TIMEOUT_AFTER_N', timeout_res_count, 'DEBUG_PARAMS_COUNT', 2)
    env.assertEqual(res, ["Timeout limit was reached"])

    """Test No cursor + GROUPER + FAIL policy"""
    # scenario: timeout on first pass, timeout on later pass is unreachable.
    # expected: ["Timeout limit was reached"]
    timeout_res_count = 4
    res = env.cmd(debug_cmd(), 'FT.AGGREGATE', 'idx', '*', 'load', '1', '@n', 'groupby', '1', '@n', 'TIMEOUT_AFTER_N', timeout_res_count, 'DEBUG_PARAMS_COUNT', 2)
    env.assertEqual(res, ["Timeout limit was reached"])

    # ================ RETURN POLICY =================
    env.cmd('FT.CONFIG', 'SET', 'ON_TIMEOUT', 'RETURN')

    """Test WITHCURSOR BASIC + RETURN policy"""
    # Behaves same as FAIL: accumulate results in the reply until rp->Next() returns timeout.
    # scenario: timeout on first pass
    # expected: [0, 0]
    timeout_res_count = 0
    res, cursor = env.cmd(debug_cmd(), 'FT.AGGREGATE', 'idx', '*', 'WITHCURSOR', 'load', '1', '@n', 'TIMEOUT_AFTER_N', timeout_res_count, 'DEBUG_PARAMS_COUNT', 2)
    env.assertEqual(res, [0])
    env.assertEqual(cursor, 0)
    # scenario: timeout on later pass
    # expected: [[ANY, <doc>, <doc> .. ], 0]
    timeout_res_count = 3
    res, cursor = env.cmd(debug_cmd(), 'FT.AGGREGATE', 'idx', '*', 'WITHCURSOR', 'load', '1', '@n', 'TIMEOUT_AFTER_N', timeout_res_count, 'DEBUG_PARAMS_COUNT', 2)
    # skip doc count since it is unpredictable
    env.assertEqual(res[1:], [['n', str(i)] for i in range(timeout_res_count)])
    env.assertEqual(cursor, 0)

    """Test WITHCURSOR + SORTER + RETURN policy"""
    # Sorter logic includes timeout policy validation. On RETURN, switch to yield, then EOF
    # scenario: timeout on first pass. timeout on later pass is unreachable.
    # expected: [timeout_res_count, 0]
    timeout_res_count = 3
    res, cursor = env.cmd(debug_cmd(), 'FT.AGGREGATE', 'idx', '*', 'WITHCURSOR', 'sortby', '1', '@n', 'load', '1', '@n', 'TIMEOUT_AFTER_N', timeout_res_count, 'DEBUG_PARAMS_COUNT', 2)
    env.assertEqual(res[0], timeout_res_count)
    env.assertEqual(res[1:], [['n', str(i)] for i in range(timeout_res_count)])
    env.assertEqual(cursor, 0)

    """Test WITHCURSOR + GROUPER + RETURN policy"""
    # Grouper drops accumulated results regardless the policy and propagates timeout
    # scenario: timeout on first pass. timeout on later pass is unreachable.
    # expected: [timeout_res_count, 0]
    res, cursor = env.cmd(debug_cmd(), 'FT.AGGREGATE', 'idx', '*', 'WITHCURSOR', 'load', '1', '@n', 'groupby', '1', '@n', 'TIMEOUT_AFTER_N', timeout_res_count, 'DEBUG_PARAMS_COUNT', 2)
    env.assertEqual(res, [timeout_res_count])
    env.assertEqual(cursor, 0)

    """Test No cursor BASIC + RETURN policy"""
    # scenario: timeout on first pass
    # expected: empty result
    timeout_res_count = 0
    res = env.cmd(debug_cmd(), 'FT.AGGREGATE', 'idx', '*', 'load', '1', '@n', 'TIMEOUT_AFTER_N', timeout_res_count, 'DEBUG_PARAMS_COUNT', 2)
    env.assertEqual(res, [0])
    # scenario: timeout on later pass
    # expected: [ANY, <doc>, <doc> .. ]
    timeout_res_count = 3
    res = env.cmd(debug_cmd(), 'FT.AGGREGATE', 'idx', '*', 'load', '1', '@n', 'TIMEOUT_AFTER_N', timeout_res_count, 'DEBUG_PARAMS_COUNT', 2)
    # skip doc count since it is unpredictable
    env.assertEqual(res[1:], [['n', str(i)] for i in range(timeout_res_count)])

    """Test No cursor + SORTER + RETURN policy"""
    # Sorter logic includes timeout policy validation. On RETURN, switch to yield, then EOF
    # scenario: timeout on first pass. timeout on later pass is unreachable.
    # expected: [timeout_res_count, 0]
    timeout_res_count = 4
    res = env.cmd(debug_cmd(), 'FT.AGGREGATE', 'idx', '*', 'load', '1', '@n', 'sortby', '1', '@n', 'TIMEOUT_AFTER_N', timeout_res_count, 'DEBUG_PARAMS_COUNT', 2)
    env.assertEqual(res[0], timeout_res_count)
    env.assertEqual(res[1:], [['n', str(i)] for i in range(timeout_res_count)])
    # assuming FT.SEARCH always includes a sorter
    res = env.cmd(debug_cmd(), 'FT.SEARCH', 'idx', '*', 'TIMEOUT_AFTER_N', timeout_res_count, 'DEBUG_PARAMS_COUNT', 2)
    env.assertEqual(res[0], timeout_res_count)
    expected_res = [f"doc{i}" if j % 2 == 0 else ['n', str(i)]  for i in range(timeout_res_count) for j in range(2)]
    env.assertEqual(res[1:], expected_res)

    """Test No cursor + GROUPER + FAIL policy"""
    # scenario: timeout on first pass, timeout on later pass is unreachable.
    # expected: ["Timeout limit was reached"]
    timeout_res_count = 4
    res = env.cmd(debug_cmd(), 'FT.AGGREGATE', 'idx', '*', 'load', '1', '@n', 'groupby', '1', '@n', 'TIMEOUT_AFTER_N', timeout_res_count, 'DEBUG_PARAMS_COUNT', 2)
    env.assertEqual(res, [timeout_res_count])
