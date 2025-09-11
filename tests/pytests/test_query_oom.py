import threading
from common import *
from math import ceil

# Based on src/result_processor.c
OOM_COUNTER_LIMIT = 100
# Query OOM error
OOM_QUERY_ERROR = "Query caused memory usage to exceed the configured limit"

# Helper to call a function and push its return value into a list
def _call_and_store(fn, args, out_list):
    out_list.append(fn(*args))

def _expected_results(pause_after_n):
    count = ceil(pause_after_n / OOM_COUNTER_LIMIT)
    count +=1
    return count * OOM_COUNTER_LIMIT - 1

def _test_oom_before_rp(env, rp_type, pause_after_n, fail_on_oom):
    query_result = []

    # Prepare thread
    t_query = threading.Thread(
        target=_call_and_store,
        args=(runDebugQueryCommandPauseBeforeRPAfterN,
            (env, ['FT.SEARCH', 'idx', '*'], rp_type, pause_after_n, fail_on_oom),
            query_result),
        daemon=True
    )

    # Call debug search command with PAUSE_BEFORE_RP_N
    t_query.start()

    # Wait to the query to be paused
    while getIsRPPaused(env) != 1:
        time.sleep(0.1)

    # Simulate OOM (Can be low as we like)
    set_tight_maxmemory_for_oom(env, 2)

    # Resume the query
    setPauseRPResume(env)
    # Wait for the query to finish
    t_query.join()

    if fail_on_oom:
        # Verify the query returned OOM
        query_result[0].contains(OOM_QUERY_ERROR)
    else:
        # Verify amount of results
        # Assuming the OOM happened before loader changed to yeild mode
        env.assertEqual(query_result[0][0], _expected_results(pause_after_n))

    # Reset memory
    set_unlimited_maxmemory_for_oom(env)

@skip(cluster=True)
def test_query_oom_by_rp(env):
    RPS_TO_TEST = ['Index']

    # Set workers to 1 to make sure the query can be paused
    env.expect('FT.CONFIG', 'SET', 'WORKERS', 1).ok()

    # Create 1000 docs
    num_docs = 1000
    for i in range(num_docs):
        env.expect('HSET', f'doc{i}', 'name', f'name{i}').equal(1)

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT').ok()
    # Wait for index to finish scan
    waitForIndex(env, 'idx')

    for rp_type in RPS_TO_TEST:
        # Test OOM before rp
        _test_oom_before_rp(env, rp_type, 100, False)
        # Change config to fail on OOM
        env.expect('FT.CONFIG', 'SET', 'ON_OOM', 'FAIL').ok()
        # Test OOM before rp
        _test_oom_before_rp(env, rp_type, 100, True)
        # Revert config
        env.expect('FT.CONFIG', 'SET', 'ON_OOM', 'RETURN').ok()
