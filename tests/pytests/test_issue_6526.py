from common import *
import threading


def _run_query_with_reindex_during_load(query):
    # A dedicated environment is required for the background safe-loader path and its debug hook.
    if not MT_BUILD:
        raise SkipTest('MT_BUILD is not set')
    env = Env(enableDebugCommand=True, moduleArgs='WORKERS 1')
    conn = getConnectionByEnv(env)

    env.expect(
        'FT.CREATE', 'idx', 'ON', 'HASH', 'PREFIX', '1', 'doc:',
        'SCHEMA', 'n', 'NUMERIC', 'title', 'TEXT', 'SORTABLE'
    ).ok()
    env.cmd(config_cmd(), 'SET', 'TIMEOUT', '0')
    conn.execute_command('HSET', 'doc:1', 'n', '1', 'title', 'one')
    conn.execute_command('HSET', 'doc:2', 'n', '2', 'title', 'two')
    waitForIndex(env, 'idx')

    args = parseDebugQueryCommandArgs(query, ['PAUSE_BEFORE_SAFE_LOADER_GIL'])
    query_conn = env.getConnection()
    outcome = []

    def run_query():
        try:
            outcome.append(query_conn.execute_command(debug_cmd(), *args))
        except Exception as error:
            outcome.append(error)

    thread = threading.Thread(target=run_query, daemon=True)
    thread.start()

    def loader_paused_or_query_finished():
        paused = getIsRPPaused(env)
        return paused == 1 or bool(outcome), {
            'paused': paused,
            'outcome': repr(outcome),
        }

    wait_for_condition(
        loader_paused_or_query_finished,
        'Timeout waiting for the safe loader to release the spec lock'
    )
    env.assertEqual(outcome, [], message='query finished before reaching the safe loader pause')

    try:
        # Replacing doc:1 pops the metadata buffered by the query and assigns a new doc ID.
        conn.execute_command('HSET', 'doc:1', 'n', '1', 'title', 'one-reindexed')
    finally:
        setPauseRPResume(env)

    thread.join(timeout=10)
    env.assertFalse(thread.is_alive(), message='query did not resume after loader signal')
    env.assertEqual(len(outcome), 1, message=outcome)
    env.assertFalse(isinstance(outcome[0], Exception), message=outcome[0])
    return env, outcome[0]


@skip(cluster=True)
def test_aggregate_drops_doc_reindexed_during_load():
    """FT.AGGREGATE must not emit an empty row for a doc invalidated before LOAD."""
    env, result = _run_query_with_reindex_during_load(
        ['FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@n']
    )

    total, rows = result[0], result[1:]
    invalid_rows = [row for row in rows if row is None or row == []]
    env.assertEqual(invalid_rows, [], message=f'invalid aggregate row: {result}')
    env.assertEqual(rows, [['n', '2']], message=result)
    env.assertEqual(total, len(rows), message=result)


@skip(cluster=True)
def test_aggregate_groupby_drops_doc_reindexed_during_load():
    """GROUPBY must not subtract an upstream loader drop from its recomputed group count."""
    env, result = _run_query_with_reindex_during_load(
        ['FT.AGGREGATE', 'idx', '*', 'LOAD', '1', '@n', 'GROUPBY', '1', '@n']
    )

    total, rows = result[0], result[1:]
    env.assertEqual(rows, [['n', '2']], message=result)
    env.assertEqual(total, len(rows), message=result)


@skip(cluster=True)
def test_search_optimizer_drops_doc_reindexed_during_load():
    """The optimizer must fold the loader-drop correction before applying LIMIT."""
    env, result = _run_query_with_reindex_during_load(
        ['FT.SEARCH', 'idx', '*', 'WITHOUTCOUNT', 'LIMIT', '0', '10']
    )

    total, flat_results = result[0], result[1:]
    pairs = list(zip(flat_results[0::2], flat_results[1::2]))
    env.assertEqual([fields for _, fields in pairs if fields is None], [], message=result)
    env.assertEqual([key for key, _ in pairs], ['doc:2'], message=result)
    env.assertEqual(total, len(pairs), message=result)


@skip(cluster=True)
def test_safe_loader_preserves_lazy_expiration_row():
    """A Redis 6/7 lazy-expiration load failure must keep the legacy row and total."""
    if not MT_BUILD:
        raise SkipTest('MT_BUILD is not set')
    env = Env(enableDebugCommand=True, moduleArgs='WORKERS 1')
    conn = getConnectionByEnv(env)

    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT').ok()
    conn.execute_command('HSET', 'doc1', 't', 'bar')
    conn.execute_command('HSET', 'doc2', 't', 'arr')

    conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '0')
    try:
        # A longer TTL avoids the pre-7.2 PEXPIRE time-sampling race.
        conn.execute_command('PEXPIRE', 'doc1', 1000)
        conn.execute_command('DEBUG', 'SLEEP', 1.1)
        res = conn.execute_command('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@t')
        env.assertEqual(res, [2, None, ['t', 'arr']])
    finally:
        conn.execute_command('DEBUG', 'SET-ACTIVE-EXPIRE', '1')
