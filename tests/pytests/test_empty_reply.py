from common import *

class TestEmptyReplyWarnings:
    """
    Tests for MOD-12640: Coordinator should propagate warnings from empty shard replies.

    Before the fix, when shards returned empty results with a timeout warning,
    the coordinator ignored the warning. After the fix, processWarningsAndCleanup()
    is called for empty replies, returning RS_RESULT_TIMEDOUT, which propagates
    the timeout to the coordinator.
    """

    def __init__(self):
        # Cluster mode, RESP3
        self.env = Env(protocol=3)
        skipTest(cluster=False)
        # Create simple index
        self.env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
        # Add docs so shards have data (but TIMEOUT_AFTER_N 0 will return empty)
        conn = getConnectionByEnv(self.env)
        for i in range(20):
            conn.execute_command('HSET', f'doc{i}', 't', f'hello{i}')

    def testEmptyReplyTimeoutWarningAggregate(self):
        """
        Test 1: Empty reply with timeout warning - FT.AGGREGATE

        TIMEOUT_AFTER_N 0 INTERNAL_ONLY causes shards to return empty + timeout warning.
        Verify the warning is propagated to the response.
        """
        query = ['FT.AGGREGATE', 'idx', '*', 'TIMEOUT', 0]
        res = runDebugQueryCommandTimeoutAfterN(self.env, query, 0, internal_only=True)

        # Should have 0 results
        self.env.assertEqual(len(res['results']), 0,
                             message="Expected 0 results with TIMEOUT_AFTER_N 0")
        # Should have timeout warning (propagated from shards via the fix)
        VerifyTimeoutWarningResp3(self.env, res,
                                  message="Empty reply should propagate timeout warning")

    def testEmptyReplyTimeoutWarningProfileAggregate(self):
        """
        Test 2: Empty reply with timeout warning - FT.PROFILE AGGREGATE

        Verify coordinator gets timeout from shard's empty reply.
        Before MOD-12640 fix: Coordinator wouldn't get timeout from empty shard replies.
        After MOD-12640 fix: processWarningsAndCleanup returns RS_RESULT_TIMEDOUT,
        which sets req->has_timedout, so coordinator profile shows timeout.
        """
        query = ['FT.PROFILE', 'idx', 'AGGREGATE', 'QUERY', '*', 'TIMEOUT', 0]
        res = runDebugQueryCommandTimeoutAfterN(self.env, query, 0, internal_only=True)

        # Results should have timeout warning
        VerifyTimeoutWarningResp3(self.env, res['Results'],
                                  message=f"Results should have timeout warning, res: {res}")

        # Coordinator SHOULD have timeout warning (propagated via RS_RESULT_TIMEDOUT)
        coord_warning = res['Profile']['Coordinator']['Warning']
        self.env.assertContains('Timeout', coord_warning,
                                message=f"Coordinator should have timeout warning from shard's empty reply, res: {res}")

    def testEmptyReplyMaxPrefixExpansionsWarning(self):
        """
        Empty reply with max prefix expansions warning.
        Verifies coordinator propagates warning even when result is empty.
        """
        # Set max prefix expansions to 1 on all shards
        run_command_on_all_shards(self.env, f'{config_cmd()} SET MAXPREFIXEXPANSIONS 1')

        # Query: hell* triggers max prefix warning, @t:world doesn't exist -> empty result + warning
        res = self.env.cmd('FT.AGGREGATE', 'idx', '@t:hell* @t:world')
        self.env.assertEqual(len(res['results']), 0,
                             message=f"Expected empty results, got: {res}")
        self.env.assertGreaterEqual(len(res['warning']), 1,
                                    message=f"Expected max prefix expansion warning, got: {res}")
        self.env.assertContains('Max prefix expansions', res['warning'][0],
                                message=f"Expected max prefix expansion warning, got: {res}")

@skip(cluster=False)
def testEmptyReplyTimeoutResp2():
    """
    RESP2 empty reply with timeout - verify handeled correctly.
    """
    env = Env(protocol=2)
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    for i in range(20):
        conn.execute_command('HSET', f'doc{i}', 't', f'hello{i}')
    query = ['FT.AGGREGATE', 'idx', '*', 'TIMEOUT', 0]
    # This should not crash - RESP2 uses forced coordinator timeout
    res = runDebugQueryCommandTimeoutAfterN(env, query, 0, internal_only=True)
    env.assertEqual(res, [0], message=res)


@skip(cluster=False)
def testEmptyReplyIndexingOomWarning():
    """
    Empty reply with indexing OOM warning.
    Trigger indexing OOM, then query for non-existent term -> empty result + warning.
    Verifies coordinator propagates indexing failure warning even when result is empty.
    """
    env = Env(protocol=3)
    partial_results_warning = 'Index contains partial data due to an indexing failure caused by insufficient memory'

    # Set memory threshold to 80%
    verify_command_OK_on_all_shards(env, '_FT.CONFIG SET _BG_INDEX_MEM_PCT_THR 80')

    conn = getConnectionByEnv(env)
    n_docs_per_shard = 100
    n_docs = n_docs_per_shard * env.shardsCount
    for i in range(n_docs):
        conn.execute_command('HSET', f'doc{i}', 't', f'hello{i}')

    # Set pause configs on all shards
    run_command_on_all_shards(env, f'{bgScanCommand()} SET_PAUSE_ON_OOM true')
    run_command_on_all_shards(env, f'{bgScanCommand()} SET_PAUSE_BEFORE_SCAN true')

    # Create index
    conn.execute_command('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT')
    allShards_waitForIndexStatus(env, 'NEW', 'idx')

    # Set tight memory BEFORE resuming -> OOM will trigger immediately
    allShards_set_tight_maxmemory_for_oom(env, 0.85)
    run_command_on_all_shards(env, f'{bgScanCommand()} SET_BG_INDEX_RESUME')
    allShards_waitForIndexStatus(env, 'PAUSED_ON_OOM', 'idx')

    # Resume -> finish with OOM status
    run_command_on_all_shards(env, f'{bgScanCommand()} SET_BG_INDEX_RESUME')
    allShards_waitForIndexFinishScan(env, 'idx')

    # Query for non-existent term -> empty result + indexing OOM warning
    res = env.cmd('FT.AGGREGATE', 'idx', '@t:nonexistent_term_xyz')
    env.assertEqual(len(res['results']), 0,
                    message=f"Expected empty results, got: {res}")
    env.assertGreaterEqual(len(res['warning']), 1,
                           message=f"Expected indexing OOM warning, got: {res}")
    env.assertEqual(res['warning'][0], partial_results_warning,
                    message=f"Expected indexing OOM warning, got: {res}")
