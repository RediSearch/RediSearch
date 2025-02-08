from common import *

def verifyTimeoutResultsResp3(env, res, expected_results_count, message="", depth=0):
    env.assertEqual(len(res["results"]), expected_results_count, depth=depth+1, message=message + " unexpected results count")
    VerifyTimeoutWarningResp3(env, res, depth=depth+1, message=message + " unexpected results count")

def testEmptyResult():
    env = Env(protocol=3, moduleArgs='ON_TIMEOUT RETURN')
    conn = getConnectionByEnv(env)

    # Create the index
    env.expect('FT.CREATE idx SCHEMA n numeric').ok()

    # Populate the index
    num_docs = 150
    for i in range(num_docs):
        conn.execute_command('HSET', f'doc{i}' ,'n', i)

    # Before the bug fix, the first doc caused timeout and returns as an empty valid. Since we reset the timeout counter of RP_INDEX,
    # The next call to the query pipeline we will continue iterating over the results until EOF is reached or for another TIMEOUT_COUNTER_LIMIT reads.
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
