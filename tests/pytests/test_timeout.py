from common import *

# Test
def testEmptyResult():
    env = Env(protocol=3)

    conn = getConnectionByEnv(env)

    # Create the index
    env.expect('FT.CREATE idx SCHEMA n numeric').ok()

    # Populate the index
    num_docs = 150
    for i in range(num_docs):
        conn.execute_command('HSET', f'doc{i}' ,'n', i)

    def VerifyEmptyResults(res):
        env.assertFalse(res["results"], message=f"expected 0 results, got {res["results"]}")

        # verify timeout warning
        VerifyTimeoutWarningResp3(env, res)

    res = env.cmd('_ft.debug', 'FT.AGGREGATE', 'idx', '*', 'load', '1', '@n', 'TIMEOUT_AFTER_N', 0, 'DEBUG_PARAMS_COUNT', 2)

    VerifyEmptyResults(res)

    # Before the bug fix, the first doc caused timeout and returns empty. Since we reset the timeout counter of RP_INDEX,
    # The next call to the query pipeline we will continue iterating over the results until EOF is reached or for another TIMEOUT_COUNTER_LIMIT reads.
    res = env.cmd('_ft.debug', 'FT.AGGREGATE', 'idx', '*', 'load', '1', '@n', 'LIMIT', 99, 110, 'TIMEOUT_AFTER_N', 99, 'DEBUG_PARAMS_COUNT', 2)

    VerifyEmptyResults(res)
