# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

from common import *

def _verify_return_timeout_prefix(protocol):
    # The reply protocol and RETURN policy are essential to this regression.
    env = Env(protocol=protocol, moduleArgs='ON_TIMEOUT RETURN TIMEOUT 0')
    env.expect('FT.CREATE', 'prefix_idx', 'SCHEMA', 'n', 'NUMERIC').ok()
    conn = getConnectionByEnv(env)
    for i in range(6):
        conn.execute_command('HSET', f'prefix:{i}', 'n', i)
    try:
        for oom_policy in ('return', 'fail'):
            env.expect('CONFIG', 'SET', 'search-on-oom', oom_policy).ok()
            result = env.cmd(debug_cmd(), 'FT.AGGREGATE', 'prefix_idx', '*',
                             'LOAD', 1, '@n', 'LIMIT', 0, 6,
                             'TIMEOUT_AFTER_N', 3, 'DEBUG_PARAMS_COUNT', 2)
            if protocol == 2:
                # Streaming RESP2 publishes its count before subsequent Next calls.
                count = 1 if oom_policy == 'return' else 3
                env.assertEqual(result, [count, ['n', '0'], ['n', '1'], ['n', '2']])
            else:
                env.assertEqual(result, {
                    'attributes': [],
                    'format': 'STRING',
                    'results': [{'extra_attributes': {'n': str(i)}, 'values': []}
                                for i in range(3)],
                    'total_results': 3,
                    'warning': ['Timeout limit was reached'],
                })
    finally:
        env.expect('CONFIG', 'SET', 'search-on-oom', 'return').ok()


@skip(cluster=True)
def test_return_timeout_prefix_resp2():
    """RETURN preserves the prefix in both streaming and buffered RESP2 replies."""
    _verify_return_timeout_prefix(2)


@skip(cluster=True)
def test_return_timeout_prefix_resp3():
    """RETURN preserves the prefix and warning in both RESP3 execution modes."""
    _verify_return_timeout_prefix(3)


@skip(cluster=True)
def test_return_timeout_drains_sorter_with_offset():
    """After Next folds, Drain preserves partial-heap ordering, OFFSET and LIMIT."""
    env = Env(protocol=3, moduleArgs='ON_TIMEOUT RETURN TIMEOUT 0')
    env.expect('FT.CREATE', 'sort_idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE').ok()
    conn = getConnectionByEnv(env)
    for i in range(6):
        conn.execute_command('HSET', f'sort:{i}', 'n', i)
    try:
        for oom_policy in ('return', 'fail'):
            env.expect('CONFIG', 'SET', 'search-on-oom', oom_policy).ok()
            result = env.cmd(debug_cmd(), 'FT.AGGREGATE', 'sort_idx', '*',
                             'SORTBY', 2, '@n', 'DESC', 'LIMIT', 1, 1,
                             'TIMEOUT_AFTER_N', 3, 'DEBUG_PARAMS_COUNT', 2)
            env.assertEqual(result, {
                'attributes': [], 'format': 'STRING', 'total_results': 3,
                'results': [{'extra_attributes': {'n': '1'}, 'values': []}],
                'warning': ['Timeout limit was reached'],
            })
    finally:
        env.expect('CONFIG', 'SET', 'search-on-oom', 'return').ok()

def verifyTimeoutResultsResp3(env, res, expected_results_count, message="", depth=0):
    env.assertEqual(len(res["results"]), expected_results_count, depth=depth+1, message=message + " unexpected results count")
    VerifyTimeoutWarningResp3(env, res, depth=depth+1, message=message + " unexpected results count")


@skip(cluster=True)
def test_return_timeout_drains_normalized_scores():
    """Drain applies the committed maximum and keeps timeout when LIMIT fills."""
    env = Env(protocol=3, moduleArgs='ON_TIMEOUT RETURN TIMEOUT 0')
    env.expect('FT.CREATE', 'norm_idx', 'SCHEMA', 't', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    for i in range(6):
        conn.execute_command('HSET', f'norm:{i}', 't', 'hello')
    result = env.cmd(debug_cmd(), 'FT.AGGREGATE', 'norm_idx', 'hello',
                     'ADDSCORES', 'SCORER', 'BM25STD.NORM', 'LIMIT', 0, 2,
                     'TIMEOUT_AFTER_N', 3, 'DEBUG_PARAMS_COUNT', 2)
    env.assertEqual(result, {
        'attributes': [], 'format': 'STRING', 'total_results': 3,
        'results': [{'extra_attributes': {'__score': '1'}, 'values': []}] * 2,
        'warning': ['Timeout limit was reached'],
    })

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


@skip(cluster=True)
def testCursorDeadlineIsNotStaleOnResume():
    """A cursor read is measured against its own deadline, not an earlier read's.

    `runCursor` re-arms `sctx->time.timeout` before every read. Before this fix the clock-based
    timeout checker captured its deadline when the iterator tree was built, so from the second
    read onwards the iterators measured against the *first* read's deadline and reported a
    timeout for one the pipeline had just extended - and because the NOT iterator latches itself
    to EOF on timeout, the cursor then ended, dropping the documents it still owed.

    Only the clock checker was affected. FAIL and RETURN_STRICT poll the blocked-client flag,
    re-read on every probe, and those are exactly the policies for which `runCursor` skips the
    re-arm - so RETURN was both the only policy that re-armed and the only one that captured.

    Reaching it needs an iterator that probes the clock (NOT, NOT-optimized, geo-shape) and a run
    of skipped documents long enough to reach the amortization limit, which is why it went
    unnoticed on small indexes.
    """
    env = Env(protocol=3, moduleArgs='ON_TIMEOUT RETURN')
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'text', 'TEXT').ok()

    # doc:0 matches `-common` straight away, so the first page is served without scanning.
    # The 6000 documents after it are all skipped by the NOT iterator on the *second* page, which
    # is more than the 5000-probe amortization limit, so the second page is the one that consults
    # the clock. The two trailing documents are the results the second page owes.
    skipped = 6000
    with conn.pipeline(transaction=False) as p:
        p.execute_command('HSET', 'doc:0', 'text', 'rare')
        for i in range(1, skipped + 1):
            p.execute_command('HSET', f'doc:{i}', 'text', 'common')
        p.execute_command('HSET', f'doc:{skipped + 1}', 'text', 'rare')
        p.execute_command('HSET', f'doc:{skipped + 2}', 'text', 'rare')
        p.execute()

    # Generous enough that scanning the skipped run cannot legitimately exhaust it on a loaded
    # runner or an instrumented build - a real timeout here would be indistinguishable from the
    # regression. The sleep below scales with it, so the pre-fix deadline is expired either way.
    query_timeout_ms = 2000
    res, cursor = env.cmd('FT.AGGREGATE', 'idx', '-common', 'LOAD', '1', '@__key',
                          'WITHCURSOR', 'COUNT', '1', 'TIMEOUT', query_timeout_ms)
    env.assertEqual([d['extra_attributes']['__key'] for d in res['results']], ['doc:0'])
    env.assertNotEqual(cursor, 0)

    # Outlive the deadline this request started with. The pipeline re-arms its own deadline for the
    # next read, so the read below has its full budget - only a captured deadline would be expired.
    time.sleep(query_timeout_ms / 1000 * 1.5)

    remaining = []
    while cursor != 0:
        res, cursor = env.cmd('FT.CURSOR', 'READ', 'idx', cursor)
        env.assertEqual(res.get('warning', []), [],
                        message="a cursor read gets a fresh deadline, so nothing should time out")
        remaining.extend(d['extra_attributes']['__key'] for d in res['results'])

    env.assertEqual(remaining, [f'doc:{skipped + 1}', f'doc:{skipped + 2}'],
                    message="documents past the skipped run must still be served")
