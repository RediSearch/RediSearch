# Copyright (c) 2006-Present, Redis Ltd.
# All rights reserved.
#
# Licensed under your choice of the Redis Source Available License 2.0
# (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
# GNU Affero General Public License v3 (AGPLv3).

"""The query memory checkpoints, driven by `_FT.DEBUG MEMORY_PRESSURE AT_CALL <n>`, which reports
pressure on the n-th checkpoint call of the process. The fake pressure works on any Redis, so these
tests do not depend on the server exporting the memory API."""

from common import *

OOM_QUERY_ERROR = "Not enough memory available to execute the query"
SHARD_OOM_WARNING = "One or more shards failed to execute the query due to insufficient memory"
COORD_OOM_WARNING = "Coordinator failed to execute the query due to insufficient memory"
# FT.SEARCH and FT.AGGREGATE read memory before building the iterators, after them, and after the
# pipeline; the pipeline checkpoints count from there.
BUILD_CHECKPOINTS = 3
N_DOCS = 10

SEARCH = ('FT.SEARCH', 'idx', '*', 'NOCONTENT')
AGGREGATE = ('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@n')


def memory_pressure(env, *args):
    return env.cmd(debug_cmd(), 'MEMORY_PRESSURE', *args)


def checkpoint_calls(env):
    return memory_pressure(env, 'STATUS')[5]


def run_at_call(env, n, *query):
    """Runs `query` with the n-th checkpoint call from now reporting pressure."""
    memory_pressure(env, 'AT_CALL', n)
    try:
        return env.cmd(*query)
    finally:
        memory_pressure(env, 'RESET')


def expect_oom_error_at_call(env, n, *query):
    memory_pressure(env, 'AT_CALL', n)
    try:
        env.expect(*query).error().contains(OOM_QUERY_ERROR)
    finally:
        memory_pressure(env, 'RESET')


class oom_policy:
    """Sets ON_OOM on every shard for a block and restores RETURN after it."""
    def __init__(self, env, policy):
        self.env, self.policy = env, policy

    def __enter__(self):
        allShards_change_oom_policy(self.env, self.policy)

    def __exit__(self, *exc):
        allShards_change_oom_policy(self.env, 'return')


def worker_counts(env):
    """Yields 0 and 2 after setting WORKERS to each, and restores 0 afterwards."""
    try:
        for workers in (0, 2):
            set_workers(env, workers)
            yield workers
    finally:
        set_workers(env, 0)


def create_index(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'n', 'NUMERIC', 'SORTABLE', 't', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    for i in range(N_DOCS):
        conn.execute_command('HSET', f'doc{i}', 'n', i, 't', f'hello{i}')


def expect_empty_with_oom_warning(env, res):
    env.assertEqual(res['results'], [], message=res)
    env.assertEqual(res['warning'], [COORD_OOM_WARNING], message=res)


@skip(cluster=True)
@env_spec(protocol=3)
def test_build_checkpoints_stop_before_execution(env):
    """Every build stage reads memory once: under pressure ON_OOM RETURN replies an empty result
    with the OOM warning and ON_OOM FAIL the OOM error, before any row is read, with and without
    workers. The first call past the build stages is a runtime checkpoint (the reply loop's,
    before the second row), so it truncates instead."""
    create_index(env)
    for workers in worker_counts(env):
        for at_call in range(1, BUILD_CHECKPOINTS + 1):
            for query in (SEARCH, AGGREGATE):
                expect_empty_with_oom_warning(env, run_at_call(env, at_call, *query))
                with oom_policy(env, 'fail'):
                    expect_oom_error_at_call(env, at_call, *query)
                with oom_policy(env, 'ignore'):
                    env.assertEqual(len(run_at_call(env, at_call, *query)['results']), N_DOCS)

        res = run_at_call(env, BUILD_CHECKPOINTS + 1, *AGGREGATE)
        env.assertEqual(res['warning'], [COORD_OOM_WARNING], message=res)
        env.assertEqual(len(res['results']), 1, message=res)


@skip(cluster=True)
@env_spec(protocol=2)
def test_build_checkpoint_stop_in_resp2(env):
    """RESP2 has no warning channel: a query stopped at build replies an empty result."""
    create_index(env)
    env.assertEqual(run_at_call(env, 1, *SEARCH), [0])
    env.assertEqual(run_at_call(env, 1, *AGGREGATE), [0])


@skip(cluster=True)
@env_spec(protocol=3)
def test_build_checkpoint_stop_is_not_a_timeout(env):
    """Under ON_TIMEOUT FAIL a build-time OOM stop still follows ON_OOM, inline (WORKERS 0, clock
    deadline) and in the background (blocked-client timeout)."""
    create_index(env)
    env.expect(config_cmd(), 'SET', 'ON_TIMEOUT', 'FAIL').ok()
    try:
        for workers in worker_counts(env):
            for at_call in range(1, BUILD_CHECKPOINTS + 1):
                expect_empty_with_oom_warning(env, run_at_call(env, at_call, *AGGREGATE))
    finally:
        env.expect(config_cmd(), 'SET', 'ON_TIMEOUT', 'RETURN').ok()


@skip(cluster=True)
def test_build_checkpoint_stop_ends_the_cursor(env):
    """A cursor stopped at build returns no rows and is not left open; under ON_OOM FAIL the
    query fails and no cursor is created."""
    create_index(env)
    res, cursor = run_at_call(env, 1, *AGGREGATE, 'WITHCURSOR', 'COUNT', 2)
    env.assertEqual(res, [0])
    env.assertEqual(cursor, 0)
    with oom_policy(env, 'fail'):
        expect_oom_error_at_call(env, 1, *AGGREGATE, 'WITHCURSOR', 'COUNT', 2)
    env.assertEqual(to_dict(to_dict(env.cmd('FT.INFO', 'idx'))['cursor_stats'])['index_total'], 0)


@skip(cluster=True)
def test_explain_does_not_read_memory(env):
    """FT.EXPLAIN builds a plan it never runs, so pressure neither fails it nor counts as a
    checkpoint call."""
    create_index(env)
    with oom_policy(env, 'fail'):
        memory_pressure(env, 'AT_CALL', 1)
        try:
            env.assertContains('NUMERIC', env.cmd('FT.EXPLAIN', 'idx', '@n:[1 2]'))
            env.assertEqual(checkpoint_calls(env), 0)
        finally:
            memory_pressure(env, 'RESET')


def create_hybrid_index(env):
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 't', 'TEXT',
               'v', 'VECTOR', 'FLAT', 6, 'TYPE', 'FLOAT32', 'DIM', 2, 'DISTANCE_METRIC', 'L2').ok()
    conn = getConnectionByEnv(env)
    for i in range(N_DOCS):
        conn.execute_command('HSET', f'doc{i}', 't', f'hello{i}',
                             'v', np.array([i, 0]).astype(np.float32).tobytes())


HYBRID = ('FT.HYBRID', 'idx', 'SEARCH', '*', 'VSIM', '@v', '$blob',
          'PARAMS', 2, 'blob', np.array([0, 0]).astype(np.float32).tobytes())


def hybrid_result_count(res):
    return len(res[res.index('results') + 1])


@skip(cluster=True)
def test_hybrid_build_checkpoint_fails_before_execution(env):
    """Under ON_OOM FAIL, FT.HYBRID reads memory after building each subquery, and pressure at
    either fails the query with the OOM error before it runs. Under ON_OOM RETURN a stop there
    replies the OOM warning instead."""
    create_hybrid_index(env)
    full = hybrid_result_count(env.cmd(*HYBRID))
    env.assertGreater(full, 0)
    with oom_policy(env, 'fail'):
        for workers in worker_counts(env):
            # Calls from 3 on are runtime checkpoints, which fail it the same way.
            for at_call in (1, 2, 3):
                expect_oom_error_at_call(env, at_call, *HYBRID)
    # Under ON_OOM RETURN a subquery stopped at build reads nothing and the query replies the
    # OOM warning.
    res = run_at_call(env, 1, *HYBRID)
    env.assertEqual(res[res.index('warnings') + 1], [COORD_OOM_WARNING], message=res)


@skip(cluster=False)
@env_spec(protocol=3)
def test_build_checkpoint_stop_on_shards_reaches_the_coordinator(env):
    """Shards stopped at build answer like shards stopped later in the pipeline: the coordinator
    turns their OOM warning or error into its own ON_OOM outcome."""
    create_index(env)

    def at_first_shard_call(*query):
        run_command_on_all_shards(env, debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', 1)
        try:
            return env.cmd(*query)
        finally:
            run_command_on_all_shards(env, debug_cmd(), 'MEMORY_PRESSURE', 'RESET')

    expect_empty_with_oom_warning(env, at_first_shard_call(*AGGREGATE))
    # The FT.SEARCH coordinator replies the shards' wording.
    res = at_first_shard_call(*SEARCH)
    env.assertEqual(res['results'], [], message=res)
    env.assertEqual(res['warning'], [SHARD_OOM_WARNING], message=res)

    with oom_policy(env, 'fail'):
        for query in (AGGREGATE, SEARCH):
            run_command_on_all_shards(env, debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', 1)
            try:
                env.expect(*query).error().contains(OOM_QUERY_ERROR)
            finally:
                run_command_on_all_shards(env, debug_cmd(), 'MEMORY_PRESSURE', 'RESET')

    res = env.cmd(*AGGREGATE)
    env.assertEqual(len(res['results']), N_DOCS, message=res)


def ids(res):
    return [r['id'] for r in res['results']]


def first_rows_desc(k):
    """The rows a stopped query holds when it stopped after reading k rows, in doc id order,
    sorted by n descending."""
    return [f'doc{i}' for i in reversed(range(k))]


TIMEOUT_POLICIES = ('RETURN', 'FAIL', 'RETURN-STRICT')


def timeout_policies(env):
    """Yields each ON_TIMEOUT policy after setting it, and restores RETURN afterwards."""
    try:
        for policy in TIMEOUT_POLICIES:
            env.expect(config_cmd(), 'SET', 'ON_TIMEOUT', policy).ok()
            yield policy
    finally:
        env.expect(config_cmd(), 'SET', 'ON_TIMEOUT', 'RETURN').ok()


SORTED_SEARCH = ('FT.SEARCH', 'idx', '*', 'NOCONTENT', 'SORTBY', 'n', 'DESC', 'LIMIT', 0, N_DOCS)
SORTED_AGGREGATE = ('FT.AGGREGATE', 'idx', '*', 'SORTBY', 2, '@n', 'DESC', 'MAX', N_DOCS)


@skip(cluster=True)
@env_spec(protocol=3)
def test_sorter_stop_yields_its_sorted_prefix(env):
    """A sorter stopped while filling its heap replies the rows it holds, sorted, with the OOM
    warning, under every timeout policy (the timeout policy decides only what a timeout keeps),
    with and without workers. ON_OOM FAIL replies the OOM error."""
    create_index(env)
    k = 4
    for policy in timeout_policies(env):
        for workers in worker_counts(env):
            res = run_at_call(env, BUILD_CHECKPOINTS + k, *SORTED_SEARCH)
            env.assertEqual(ids(res), first_rows_desc(k), message=(policy, workers, res))
            env.assertEqual(res['warning'], [COORD_OOM_WARNING], message=(policy, workers, res))

            res = run_at_call(env, BUILD_CHECKPOINTS + k, *SORTED_AGGREGATE)
            env.assertEqual([r['extra_attributes']['n'] for r in res['results']],
                            [str(i) for i in reversed(range(k))], message=(policy, workers, res))
            env.assertEqual(res['warning'], [COORD_OOM_WARNING], message=(policy, workers, res))

            with oom_policy(env, 'fail'):
                expect_oom_error_at_call(env, BUILD_CHECKPOINTS + k, *SORTED_SEARCH)
                expect_oom_error_at_call(env, BUILD_CHECKPOINTS + k, *SORTED_AGGREGATE)
            with oom_policy(env, 'ignore'):
                res = run_at_call(env, BUILD_CHECKPOINTS + k, *SORTED_SEARCH)
                env.assertEqual(ids(res), first_rows_desc(N_DOCS), message=(policy, workers, res))


@skip(cluster=True)
@env_spec(protocol=2)
def test_sorter_stop_in_resp2(env):
    """RESP2 replies the sorted prefix without a warning."""
    create_index(env)
    k = 4
    res = run_at_call(env, BUILD_CHECKPOINTS + k, *SORTED_SEARCH)
    env.assertEqual(res, [k] + first_rows_desc(k), message=res)


@skip(cluster=True)
@env_spec(protocol=3)
def test_normalizer_stop_normalizes_by_the_partial_max(env):
    """BM25STD.NORM pools every match to find the max score. Stopped while pooling, it replies the
    pooled rows normalized by the max among them, under every timeout policy."""
    env.expect('FT.CREATE', 'idx', 'SCHEMA', 'body', 'TEXT').ok()
    conn = getConnectionByEnv(env)
    for i in range(N_DOCS):
        conn.execute_command('HSET', f'doc{i}', 'body', ' '.join(['hello'] * (i + 1) + ['x'] * 5))
    query = ('FT.SEARCH', 'idx', 'hello', 'SCORER', 'BM25STD.NORM', 'WITHSCORES', 'NOCONTENT',
             'LIMIT', 0, N_DOCS)
    k = 4
    for policy in timeout_policies(env):
        for workers in worker_counts(env):
            res = run_at_call(env, BUILD_CHECKPOINTS + k, *query)
            env.assertEqual(sorted(ids(res)), [f'doc{i}' for i in range(k)],
                            message=(policy, workers, res))
            env.assertEqual(res['results'][0]['score'], 1.0, message=(policy, workers, res))
            env.assertEqual(res['warning'], [COORD_OOM_WARNING], message=(policy, workers, res))

            with oom_policy(env, 'fail'):
                expect_oom_error_at_call(env, BUILD_CHECKPOINTS + k, *query)
            with oom_policy(env, 'ignore'):
                res = run_at_call(env, BUILD_CHECKPOINTS + k, *query)
                env.assertEqual(len(res['results']), N_DOCS, message=(policy, workers, res))


GROUPBY_QUERIES = (
    ('FT.AGGREGATE', 'idx', '*', 'GROUPBY', 1, '@n', 'REDUCE', 'COUNT', 0, 'AS', 'c'),
    ('FT.AGGREGATE', 'idx', '*', 'GROUPBY', 0,
     'REDUCE', 'TOLIST', 1, '@n', 'AS', 'list',
     'REDUCE', 'COUNT_DISTINCT', 1, '@n', 'AS', 'distinct',
     'REDUCE', 'QUANTILE', 2, '@n', 0.5, 'AS', 'median',
     'REDUCE', 'COLLECT', 3, 'FIELDS', 1, '@n', 'AS', 'rows'),
)


@skip(cluster=True)
@env_spec(protocol=3)
def test_grouper_stop_yields_no_partial_groups(env):
    """A GROUPBY stopped while accumulating replies no groups: ON_OOM RETURN an empty result with
    the OOM warning, ON_OOM FAIL the OOM error, under every timeout policy. It counts rows, not
    groups, so a stop also lands while rows only grow existing groups."""
    create_index(env)
    k = 4
    for policy in timeout_policies(env):
        for workers in worker_counts(env):
            for query in GROUPBY_QUERIES:
                full = env.cmd(*query)
                env.assertGreater(len(full['results']), 0)
                expect_empty_with_oom_warning(env, run_at_call(env, BUILD_CHECKPOINTS + k, *query))
                with oom_policy(env, 'fail'):
                    expect_oom_error_at_call(env, BUILD_CHECKPOINTS + k, *query)
                with oom_policy(env, 'ignore'):
                    env.assertEqual(run_at_call(env, BUILD_CHECKPOINTS + k, *query)['results'],
                                    full['results'], message=(policy, workers, query))


@skip(cluster=False)
@env_spec(protocol=3)
def test_shard_grouper_stop_leaves_the_other_shards_groups(env):
    """In a cluster each shard groups its own rows. A shard stopped while grouping replies none
    and warns; as for a shard timeout, the coordinator merges the other shards' groups, so the
    aggregates miss the stopped shard's rows, and replies its OOM warning."""
    create_index(env)
    conn = getConnectionByEnv(env)
    total = 60
    for i in range(N_DOCS, total):
        conn.execute_command('HSET', f'doc{i}', 'n', i % 3, 't', f'hello{i}')
    query = ('FT.AGGREGATE', 'idx', '*', 'GROUPBY', 0, 'REDUCE', 'COUNT', 0, 'AS', 'c')
    # Shard 2's process hosts no coordinator pipeline, so its calls are its own pipeline's.
    shard = env.getConnection(2)
    shard.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'AT_CALL', BUILD_CHECKPOINTS + 1)
    try:
        res = env.cmd(*query)
    finally:
        shard.execute_command(debug_cmd(), 'MEMORY_PRESSURE', 'RESET')
    env.assertEqual(res['warning'], [COORD_OOM_WARNING], message=res)
    env.assertLess(int(res['results'][0]['extra_attributes']['c']), total, message=res)


@skip(cluster=True)
@env_spec(protocol=3)
def test_sorter_stop_over_cursor_reads(env):
    """A sorted prefix longer than a cursor read is read to its end, and the read that ends it
    carries the OOM warning and closes the cursor."""
    create_index(env)
    k = 4
    memory_pressure(env, 'AT_CALL', BUILD_CHECKPOINTS + k)
    try:
        res, cursor = env.cmd(*SORTED_AGGREGATE, 'WITHCURSOR', 'COUNT', 3)
    finally:
        memory_pressure(env, 'RESET')
    rows = [r['extra_attributes']['n'] for r in res['results']]
    env.assertEqual(res['warning'], [], message=res)
    env.assertNotEqual(cursor, 0)
    res, cursor = env.cmd('FT.CURSOR', 'READ', 'idx', cursor)
    rows += [r['extra_attributes']['n'] for r in res['results']]
    env.assertEqual(rows, [str(i) for i in reversed(range(k))])
    env.assertEqual(res['warning'], [COORD_OOM_WARNING], message=res)
    env.assertEqual(cursor, 0)


@skip(cluster=False)
@env_spec(protocol=3)
def test_shard_sorter_stop_reaches_the_coordinator(env):
    """Shards stopped while sorting reply their sorted prefix with the OOM warning; the FT.SEARCH
    coordinator merges the prefixes and passes the warning on, or fails under ON_OOM FAIL. Only
    the shards without the coordinator are armed, since pressure is counted per process."""
    create_index(env)
    shards = [env.getConnection(i) for i in range(2, env.shardsCount + 1)]

    def arm(*args):
        for shard in shards:
            shard.execute_command(debug_cmd(), 'MEMORY_PRESSURE', *args)

    arm('AT_CALL', BUILD_CHECKPOINTS + 1)
    try:
        res = env.cmd(*SORTED_SEARCH)
    finally:
        arm('RESET')
    env.assertLess(len(res['results']), N_DOCS, message=res)
    env.assertEqual(res['warning'], [SHARD_OOM_WARNING], message=res)

    with oom_policy(env, 'fail'):
        arm('AT_CALL', BUILD_CHECKPOINTS + 1)
        try:
            env.expect(*SORTED_SEARCH).error().contains(OOM_QUERY_ERROR)
        finally:
            arm('RESET')


LOADED_AGGREGATE = ('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@t')
LOADED_SEARCH = ('FT.SEARCH', 'idx', '*', 'RETURN', 1, 't', 'LIMIT', 0, N_DOCS)


def loaded_values(res):
    return sorted(r['extra_attributes']['t'] for r in res['results'])


def hellos(rows):
    return sorted(f'hello{i}' for i in rows)


@skip(cluster=True)
@env_spec(protocol=3)
def test_safe_loader_stop_yields_the_loaded_prefix(env):
    """With workers, loads go through the safe loader, which buffers rows and then loads them all
    under the GIL. Stopped while buffering it loads and replies what it buffered; stopped while
    loading it replies the rows it loaded. Both with the OOM warning, under every timeout policy;
    ON_OOM FAIL replies the OOM error."""
    create_index(env)
    set_workers(env, 2)
    try:
        for policy in timeout_policies(env):
            # The buffer checkpoint runs per buffered row, the load checkpoint per loaded row:
            # the 4th call stops the buffer at 4 rows, call N_DOCS + 3 the load after 2.
            for at_call, rows in ((4, range(4)), (N_DOCS + 3, range(2))):
                res = run_at_call(env, BUILD_CHECKPOINTS + at_call, *LOADED_AGGREGATE)
                env.assertEqual(loaded_values(res), hellos(rows), message=(policy, at_call, res))
                env.assertEqual(res['warning'], [COORD_OOM_WARNING], message=(policy, res))
                # Rows the stop left unloaded still matched: the total counts the rows read.
                env.assertEqual(res['total_results'], len(rows) if at_call == 4 else N_DOCS,
                                message=(policy, at_call, res))

                with oom_policy(env, 'fail'):
                    expect_oom_error_at_call(env, BUILD_CHECKPOINTS + at_call, *LOADED_AGGREGATE)
                with oom_policy(env, 'ignore'):
                    res = run_at_call(env, BUILD_CHECKPOINTS + at_call, *LOADED_AGGREGATE)
                    env.assertEqual(loaded_values(res), hellos(range(N_DOCS)),
                                    message=(policy, at_call, res))

            # FT.SEARCH sorts by score before loading, and the sorter checkpoints each row first.
            res = run_at_call(env, BUILD_CHECKPOINTS + 2 * N_DOCS + 3, *LOADED_SEARCH)
            env.assertEqual(loaded_values(res), hellos(range(2)), message=(policy, res))
            env.assertEqual(res['warning'], [COORD_OOM_WARNING], message=(policy, res))
    finally:
        set_workers(env, 0)


@skip(cluster=True)
@env_spec(protocol=3)
def test_safe_loader_stop_on_the_last_buffered_row_ends_the_cursor(env):
    """A stop found on the row that fills a cursor read's buffer is not lost: that read replies its
    rows, and the next one ends the cursor with the OOM warning."""
    create_index(env)
    set_workers(env, 2)
    try:
        memory_pressure(env, 'AT_CALL', BUILD_CHECKPOINTS + 4)
        try:
            res, cursor = env.cmd(*LOADED_AGGREGATE, 'WITHCURSOR', 'COUNT', 4)
        finally:
            memory_pressure(env, 'RESET')
        env.assertEqual(loaded_values(res), hellos(range(4)), message=res)
        env.assertEqual(res['warning'], [], message=res)
        env.assertNotEqual(cursor, 0)
        res, cursor = env.cmd('FT.CURSOR', 'READ', 'idx', cursor)
        env.assertEqual(res['results'], [], message=res)
        env.assertEqual(res['warning'], [COORD_OOM_WARNING], message=res)
        env.assertEqual(cursor, 0)
    finally:
        set_workers(env, 0)


@skip(cluster=True)
@env_spec(protocol=3)
def test_withcount_depleter_stop_keeps_the_depleted_rows(env):
    """FT.AGGREGATE WITHCOUNT without SORTBY or GROUPBY depletes every match up front, whatever
    LIMIT says. Stopped while depleting it replies the rows depleted so far with the OOM warning,
    under every timeout policy; ON_OOM FAIL replies the OOM error."""
    create_index(env)
    query = ('FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 1, '@n')
    k = 4
    for policy in timeout_policies(env):
        res = run_at_call(env, BUILD_CHECKPOINTS + k, *query)
        env.assertEqual(sorted(r['extra_attributes']['n'] for r in res['results']),
                        [str(i) for i in range(k)], message=(policy, res))
        env.assertEqual(res['warning'], [COORD_OOM_WARNING], message=(policy, res))
        # The count is of the rows read before the stop.
        env.assertEqual(res['total_results'], k, message=(policy, res))
        with oom_policy(env, 'fail'):
            expect_oom_error_at_call(env, BUILD_CHECKPOINTS + k, *query)


@skip(cluster=True)
@env_spec(protocol=3)
def test_withcount_depleter_stop_over_cursor_reads(env):
    """Depleted rows read over several cursor reads keep coming, and the read that ends them
    carries the OOM warning and closes the cursor."""
    create_index(env)
    k = 4
    memory_pressure(env, 'AT_CALL', BUILD_CHECKPOINTS + k)
    try:
        res, cursor = env.cmd('FT.AGGREGATE', 'idx', '*', 'WITHCOUNT', 'LOAD', 1, '@n',
                              'WITHCURSOR', 'COUNT', 3)
    finally:
        memory_pressure(env, 'RESET')
    rows = [r['extra_attributes']['n'] for r in res['results']]
    env.assertEqual(res['warning'], [], message=res)
    env.assertNotEqual(cursor, 0)
    res, cursor = env.cmd('FT.CURSOR', 'READ', 'idx', cursor)
    rows += [r['extra_attributes']['n'] for r in res['results']]
    env.assertEqual(sorted(rows), [str(i) for i in range(k)], message=res)
    env.assertEqual(res['warning'], [COORD_OOM_WARNING], message=res)
    env.assertEqual(cursor, 0)


APPLY = ('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@t', 'APPLY', 'upper(@t)', 'AS', 'u')
FILTER = ('FT.AGGREGATE', 'idx', '*', 'LOAD', 1, '@t', 'FILTER', 'startswith(@t, "hello")')


@skip(cluster=True)
@env_spec(protocol=3)
def test_expression_stop_ends_the_stream(env):
    """APPLY and FILTER keep their string results for the whole query, so they check memory per
    row. Stopped while rows still stream from the index, the stream ends with the rows already
    produced and the OOM warning; ON_OOM FAIL replies the OOM error. With workers the safe loader
    has read every row before the first row reaches the expression, so under RETURN nothing is
    cut and the complete result has no warning; only FAIL cuts what it holds."""
    create_index(env)
    k = 4
    for workers in worker_counts(env):
        # With workers the safe loader buffers and loads every row first. The expression's calls
        # alternate with the reply loop's (or AggregateResults'), so its k-th is 2k - 1 later.
        at_call = BUILD_CHECKPOINTS + (2 * N_DOCS if workers else 0) + 2 * k - 1
        for query in (APPLY, FILTER):
            res = run_at_call(env, at_call, *query)
            if workers:
                env.assertEqual(len(res['results']), N_DOCS, message=(query, res))
                env.assertEqual(res['warning'], [], message=(query, res))
            else:
                env.assertEqual(len(res['results']), k - 1, message=(query, res))
                env.assertEqual(res['warning'], [COORD_OOM_WARNING], message=(query, res))
            with oom_policy(env, 'fail'):
                expect_oom_error_at_call(env, at_call, *query)
            with oom_policy(env, 'ignore'):
                env.assertEqual(len(run_at_call(env, at_call, *query)['results']), N_DOCS)
